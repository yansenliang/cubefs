#include <stdlib.h>
#include <malloc.h>
#include <unistd.h>
#include "cbrdma.h"
#include "common.h"

#define CBRDMA_DATA_MAGIC           0xFEFEFEFE

extern net_env_t        *g_net_env;

void* _cbrdma_get_send_buff(connect_t *conn, int32_t size, int64_t dead_line, int32_t *ret_size) {
    int64_t now = get_time_ns();
    void*   send_data = NULL;
    buffer_t *buff = NULL;
    int start = 0;
    int block_cnt = 0;
    block_header_t* head = NULL;
    block_tail_t*   tail = NULL;
    uint64_t nofity_value = 0;

    *ret_size = 0;
    pthread_spin_lock(&conn->spin_lock);
    if (conn->state != CONN_ST_CONNECTED) {
        pthread_spin_unlock(&conn->spin_lock);
        LOG_WITH_LIMIT(ERROR, now, 60, 6, 1, "conn(%lu-%p) get send buff failed, stat is closing[state:%d]", conn->nd, conn, conn->state);
        *ret_size = -1;
        return NULL;
    }

    while (1) {
        //预留一个槽位
        if (conn->send_win_size < 2) {

            if (conn->state != CONN_ST_CONNECTED) {
                pthread_spin_unlock(&conn->spin_lock);
                LOG(ERROR, "conn(%lu-%p) get send buff failed, stat is closing[state:%d]", conn->nd, conn, conn->state);
                *ret_size = -1;
                return NULL;
            }

            pthread_spin_unlock(&conn->spin_lock);

            now = get_time_ns();
            if (dead_line == -1) {
                //time out;
                LOG_WITH_LIMIT(ERROR, now, 60, 6, 2, "conn(%lu-%p) get send buff timeout, deadline:%ld, now:%ld", conn->nd, conn, dead_line, now);
                return NULL;
            }

            if (conn->efd > 0 && conn->worker->w_pid != pthread_self()) {
                read(conn->efd, &nofity_value, 8);
            } else {
                usleep(10);
            }
            
            if (now >= dead_line) {
                //time out;
                LOG_WITH_LIMIT(ERROR, now, 60, 6, 3, "conn(%lu-%p) get send buff timeout, deadline:%ld, now:%ld", conn->nd, conn, dead_line, now);
                return NULL;
            }
            pthread_spin_lock(&conn->spin_lock);
            continue;
        }
        break;
    }

    buff = list_head(&conn->send_free_list, buffer_t, node);
    send_data = buff->data;
    head = (block_header_t*) send_data;
    start = buff->index;
    head->block_start = start;
    head->seq = conn->send_cnt;

    while(conn->send_win_size > 1 && *ret_size < size && buff->index >= start) {
        list_del(&buff->node);
        *ret_size += conn->send_block_size;
        list_add_tail(&conn->send_wait_free_list, &buff->node);
        block_cnt++;
        conn->send_win_size--;
        buff->state = BUFF_ST_APP_INUSE;
        conn->buff_ref++;
        conn->send_cnt++;
#ifdef _LIST_CHECK_
        assert(buff->index == (conn->send_head_index % conn->send_block_cnt));
#endif
        conn->send_head_index++;
        buff = list_head(&conn->send_free_list, buffer_t, node);
    }

    *ret_size -= (sizeof(block_header_t) + sizeof(block_tail_t));
    *ret_size = (*ret_size > size) ? size : *ret_size;

    head->send_wind_size = conn->send_win_size;
    head->recv_ack_cnt = conn->recv_ack_cnt;
    head->len = *ret_size;
    pthread_spin_unlock(&conn->spin_lock);

    head->magic = CBRDMA_DATA_MAGIC;
    head->magic2 = CBRDMA_DATA_MAGIC;
    head->block_cnt = block_cnt;

    send_data = send_data + sizeof(block_header_t);
    tail = (block_tail_t*)(send_data + head->len);
    memcpy(tail, head, sizeof(block_header_t));

    buff = conn->send_buff + head->block_start;
    LOG(DEBUG,"conn(%lu-%p) get send buff (buff: %p, %u, block:[index:%d, %p])", conn->nd, conn, send_data, head->len, start, buff->data);
    return send_data;
}

void* cbrdma_get_send_buff(uint64_t nd, int32_t size, int64_t timeout_us, int32_t *ret_size) {
    worker_t *worker = NULL;
    connect_t *conn  = NULL;
    void* data_ptr = NULL;
    int64_t dead_line = 0;
    int64_t now = get_time_ns();
    get_worker_and_connect_by_nd(nd, &worker, &conn, GET_CONN_WIT_REF);
    if (conn == NULL) {
        //already closed;
        LOG_WITH_LIMIT(ERROR, now, 60, 6, 1, "conn(%lu) already closed", nd);
        *ret_size = -1;
        return NULL;
    }

    dead_line = timeout_us == -1 ? timeout_us : get_time_ns() + timeout_us * 1000;

    data_ptr = _cbrdma_get_send_buff(conn, size, dead_line, ret_size);
    conn_del_ref(conn);

    return data_ptr;
}

static void _cbrdma_check_send_buff(connect_t *conn, void* data_ptr, uint32_t len) {
    block_header_t* head = NULL;
    block_tail_t  * tail = NULL;
    void*       buff_ptr = NULL;
    buffer_t *last_block = NULL;
    int       block_cnt  = 0;
    uint32_t  real_len   = 0;

    assert(conn != NULL);

    buff_ptr = (data_ptr - sizeof(block_header_t));
    head     = (block_header_t*) buff_ptr;
    tail  = data_ptr + head->len;
    real_len = len + sizeof(block_header_t) + sizeof(block_tail_t);

    assert(head->magic  == CBRDMA_DATA_MAGIC);
    assert(head->magic2 == CBRDMA_DATA_MAGIC);
    assert(head->len >= len);

    assert(tail->magic  == CBRDMA_DATA_MAGIC);
    assert(tail->magic2 == CBRDMA_DATA_MAGIC);
    assert(tail->len == head->len);
    assert(head->block_cnt + head->block_start <= conn->send_block_cnt);

    block_cnt = head->block_cnt;
    if (head->len != len) {
        tail = (block_tail_t*) (data_ptr + len);
        head->len = len;
        memcpy(tail, head, sizeof(block_header_t));
        block_cnt = real_len / conn->send_block_size;
        block_cnt += real_len % conn->send_block_size ? 1 : 0;
    }

    last_block = conn->send_buff + (block_cnt + head->block_start - 1);
    last_block->type |= SEND_LAST_BLOCK_BIT;

    return;
}

static void _cbrdma_release_send_buff(connect_t* conn, void* data_ptr) {
    void*       buff_ptr = NULL;
    int            start = 0;
    block_header_t* head = NULL;
    buffer_t*       buff = NULL;

    pthread_spin_lock(&conn->spin_lock);
    buff_ptr = (data_ptr - sizeof(block_header_t));
    head     = (block_header_t*) buff_ptr;
    start    = head->block_start;

    for (int i = 0; i < head->block_cnt; i++) {
        buff = conn->send_buff + (i + start);
        buff->state = BUFF_ST_FREE;
        conn->buff_ref--;
    }
    pthread_spin_unlock(&conn->spin_lock);

    return;
}

static int _cbrdma_send(worker_t *worker, connect_t *conn, void* data_ptr, uint32_t len) {
    uint32_t total_size = len + sizeof(block_header_t) + sizeof(block_tail_t);
    buffer_t* buff      = NULL;
    int             ret = 0;
    int           index = 0;
    block_header_t *head = NULL;

    assert(worker != NULL);
    assert(conn != NULL);

    _cbrdma_check_send_buff(conn, data_ptr, len);
    head = (block_header_t*) (data_ptr - sizeof(block_header_t));

    pthread_spin_lock(&conn->spin_lock);
    if (conn->state != CONN_ST_CONNECTED) {
        pthread_spin_unlock(&conn->spin_lock);
        _cbrdma_release_send_buff(conn, data_ptr);
        LOG(ERROR,"conn(%lu-%p) closing[state:%d]", conn->nd, conn, conn->state);
        return -1;
    }

    head->recv_ack_cnt = conn->recv_ack_cnt;
    for (index = 0; index < head->block_cnt && total_size > 0; ) {
        buff = conn->send_buff + (head->block_start + index);
        index++;

        buff->send_len = total_size >= conn->send_block_size ? conn->send_block_size : total_size;
        ret = post_send_data(conn, buff, buff->send_len);
        if (ret != 0) {
            goto failed;
        }
        total_size -= buff->send_len;
    }

    pthread_spin_unlock(&conn->spin_lock);
    return 1;

failed:
    //todo wait close
    for (; index < head->block_cnt; index++) {
        buff = conn->send_buff + (head->block_start + index);
        buff->state = BUFF_ST_FREE;
        conn->buff_ref--;
    }
    pthread_spin_unlock(&conn->spin_lock);
    LOG(ERROR, "conn(%lu-%p) send failed, cur send size:%u, cur send sq:%lu, cur recv size:%u, peer send win size:%u, err:%d-%d",
                     conn->nd, conn, conn->send_win_size, conn->send_cnt, conn->recv_win_size, conn->peer_send_wind_size, ret, errno);
    conn_notify_error(conn);

    conn_close(worker, conn);
    return -1;
}

//buff len not used, need wait peer recv data free
int cbrdma_send(uint64_t nd, void* buff, uint32_t len) {
    worker_t *worker = NULL;
    connect_t *conn  = NULL;
    int ret = 0;
    _get_worker_and_connect_by_nd(nd, &worker, &conn, GET_CONN_WIT_REF);
    if (conn == NULL) {
        //already closed;
        LOG(ERROR,"conn(%lu) already closed", nd);
        return -1;
    }

    ret = _cbrdma_send(worker, conn, buff, len);
    conn_del_ref(conn);
    return ret;
}

static int conn_auto_reply_ack(connect_t *conn) {
    int ret = 0;

    if (list_is_empty(&conn->send_free_list)) {
        return 0;
    }

    assert(conn->auto_ack == NULL);
    buffer_t *buff = list_extract_head(&conn->send_free_list, buffer_t, node);
    block_header_t *head = (block_header_t*) buff->data;
    block_tail_t   *tail = (block_tail_t*) (buff->data + sizeof(block_header_t));
    conn->auto_ack = buff;

    list_add_tail(&conn->send_wait_free_list, &buff->node);
    buff->state = BUFF_ST_APP_INUSE;
    conn->buff_ref++;
    conn->send_win_size--;

    head->len = 0;
    head->block_cnt = 1;
    head->magic = CBRDMA_DATA_MAGIC;
    head->magic2 = CBRDMA_DATA_MAGIC;
    head->block_start = buff->index;
    head->recv_ack_cnt = conn->recv_ack_cnt;
    head->send_wind_size = conn->send_win_size;
    head->seq = conn->send_cnt;
    conn->send_cnt++;

#ifdef _LIST_CHECK_
    assert(buff->index == (conn->send_head_index % conn->send_block_cnt));
#endif
    conn->send_head_index++;

    memcpy(tail, head, sizeof(block_header_t));
    ret = post_send_data(conn, buff, sizeof(block_header_t) + sizeof(block_tail_t));

    if (ret != 0) {
        buff->state = BUFF_ST_FREE;
        conn->buff_ref--;
    }
    return ret;
}

static int check_recv_data(connect_t *conn, buffer_t *buff) {
    block_header_t* head = NULL;
    block_tail_t*   tail = NULL;

    if (buff == NULL) {
        return -1;
    }

    head = (block_header_t*) buff->data;
    if (head->magic != CBRDMA_DATA_MAGIC) {
        return -1;
    }
    tail = (block_tail_t*) (buff->data + sizeof(block_header_t) + head->len);
    if (tail->magic != CBRDMA_DATA_MAGIC || tail->magic2 != CBRDMA_DATA_MAGIC) {
        return -1;
    }

    assert(head->seq == tail->seq);
    assert(head->len == tail->len);
    assert(head->block_cnt == tail->block_cnt);
    return 0;
}

static int _cbrdma_release_recv_data(connect_t *conn, void* data_ptr) {
    block_header_t* head = NULL;
    buffer_t* buff = NULL;
    int close_flag = 0;
    int count = 0;
    int offset = 0;
    int64_t now = get_time_ns();

    assert(conn != NULL);
    assert(data_ptr != NULL);

    head = (block_header_t*) (data_ptr - sizeof(block_header_t));
    assert(head->block_start < conn->recv_block_cnt);
    buff = conn->recv_buff + head->block_start;
    assert(check_recv_data(conn, buff) == 0);
    count = head->block_cnt;
    offset = head->block_start;

    pthread_spin_lock(&conn->spin_lock);
    //遍历recv buff 数组
    for (int i = 0; i < count; i++) {
        buff = conn->recv_buff + (offset + i);
        buff->state = BUFF_ST_FREE;
        conn->buff_ref--;
        memset(buff->data, 0, conn->recv_block_size);
    }

    //将已经free加入到free list
    for (int i = 0; i < conn->recv_block_cnt; i++) {
        buff = conn->recv_buff + ((conn->recv_tail_index) % conn->recv_block_cnt);
#ifdef _LIST_CHECK_
        assert(buff->index == (conn->recv_tail_index % conn->recv_block_cnt));
#endif
        if (buff->state != BUFF_ST_FREE) {
            break;
        }
        list_add_tail(&conn->recv_free_list, &buff->node);
        conn->recv_win_size++;
        buff->state = BUFF_ST_POLLING_INUSE;
        conn->recv_tail_index++;
        conn->recv_ack_cnt++;
    }

    if (conn->recv_cur == NULL && !list_is_empty(&conn->recv_free_list)) {
        conn->recv_cur = list_head(&conn->recv_free_list, buffer_t, node);
    }

    if (conn->recv_win_size > (conn->peer_send_wind_size * 2)
        && conn->state == CONN_ST_CONNECTED
         && conn->auto_ack == NULL
         && conn->recv_ack_cnt > (conn->last_auto_seq + conn->recv_win_size / 2)) {
        //auto reply ack
        close_flag = conn_auto_reply_ack(conn);
        LOG_WITH_LIMIT(ERROR, now, 60, 6, 1, "conn(%lu-%p) reply auto ack , cur recv size:%u, peer send win size:%u, recv seq:%lu", conn->nd, conn, conn->recv_win_size, conn->peer_send_wind_size, conn->recv_cnt);
        if (close_flag != 0) {
            conn->auto_ack = NULL;
            LOG(ERROR, "conn(%lu-%p) reply auto ack failed, cur send size:%u, cur send sq:%lu, cur recv size:%u, peer send win size:%u, err:%d-%d",
                     conn->nd, conn, conn->send_win_size, conn->send_cnt, conn->recv_win_size, conn->peer_send_wind_size, close_flag, errno);
        }
    }
    pthread_spin_unlock(&conn->spin_lock);

    if (close_flag) {
        conn_notify_error(conn);
        conn_close(conn->worker, conn);
    }
    return 0;
}

int cbrdma_release_recv_buff(uint64_t nd, void *data_ptr) {
    worker_t *worker = NULL;
    connect_t *conn  = NULL;
    int ret = 0;
    _get_worker_and_connect_by_nd(nd, &worker, &conn, GET_CONN_WIT_REF);
    if (conn == NULL) {
        //already closed;
        LOG(ERROR,"conn(%lu) already closed", nd);
        return -1;
    }

    ret = _cbrdma_release_recv_data(conn, data_ptr);
    conn_del_ref(conn);
    return ret;
}

void conn_deal_ctl_cmd(connect_t *conn) {
    meta_t *meta = (void*) conn->recv_meta->data;
    LOG(INFO, " conn:%p deal cmd:%d ", conn, meta->cmd);
    switch (meta->cmd) {
        case CMD_REG_RECV_BUFF:
            //reg send buff
            conn->send_buff = malloc(sizeof(buffer_t) * meta->count);
            if (conn->send_buff == NULL) {
                goto failed;
            }
            if(conn_reg_data_buff(conn, meta->size, meta->count, conn->mem_type, conn->send_buff) != 0) {
                goto failed;
            }

            conn->send_block_cnt = meta->count;
            conn->send_block_size = meta->size;
            conn_bind_remote_recv_key(conn, meta);

            LOG(INFO, "conn(%lu-%p) reg send block success, size %u, count: %u", conn->nd, conn, meta->size, meta->count);
            if (conn->server) {
                pthread_spin_lock(&conn->spin_lock);
                set_conn_state(conn, CONN_ST_CONNECTED);
                pthread_spin_unlock(&conn->spin_lock);
                conn->server->accept_cb(conn->server->nd, conn->nd, conn->server->context);
            }
            break;
        default:
            LOG(INFO, "conn:%p recv unknown cmd:%u", conn, meta->cmd);
            assert(0);
    }
    /*todo release recv meta*/
    return;

failed:
    disconnect(conn->nd);
    return;
}

void process_recv_meta(struct ibv_wc *wc, connect_t *conn, int64_t now) {
    assert(conn != NULL);

    conn_deal_ctl_cmd(conn);

    pthread_spin_lock(&conn->spin_lock);
    set_conn_state(conn, CONN_ST_CONNECTED);
    if (conn->recv_cur == NULL && !list_is_empty(&conn->recv_free_list)) {
        conn->recv_cur = list_head(&conn->recv_free_list, buffer_t, node);
    }
    conn->recv_time = now;
    pthread_spin_unlock(&conn->spin_lock);

    pthread_spin_lock(&conn->worker->lock);
    list_add_tail(&conn->worker->conn_list, &conn->worker_node);
    pthread_spin_unlock(&conn->worker->lock);
    return;
}

int process_send_meta(struct ibv_wc *wc, connect_t *conn) {
    return 0;
}

int process_send_data(struct ibv_wc *wc, complete_msg_t *msg, connect_t *conn, buffer_t *buff) {
    int          tail_off = 0;
    block_tail_t    *tail = NULL;
    buffer_t *start_block = NULL;

    assert(conn != NULL);

    if (!(buff->type & SEND_LAST_BLOCK_BIT)) {
        //do nothing
        return 0;
    }

    pthread_spin_lock(&conn->spin_lock);
    if (conn->state != CONN_ST_CONNECTED) {
        pthread_spin_unlock(&conn->spin_lock);
        return 0;
    }
    pthread_spin_unlock(&conn->spin_lock);

    tail_off = buff->send_len - sizeof(block_tail_t);
    tail = (block_tail_t*)(buff->data + tail_off);
    start_block = conn->send_buff + tail->block_start;

    conn->send_cb_cnt++;
    msg->op     = CBRDMA_OP_SEND;
    msg->status = 1;
    msg->length = tail->len;
    msg->buff   = start_block->data + sizeof(block_header_t);
    msg->nd     = conn->nd;
    msg->user_context = conn->context;
    return 1;
}

int on_completion(struct ibv_wc *wc_arr, complete_msg_t *msg_arr, int len, int64_t now) {
    int msg_index = 0;
    for (int i = 0; i < len; i++) {
        struct ibv_wc *wc = wc_arr + i;
        complete_msg_t *msg = msg_arr + msg_index;
        buffer_t *buff = (buffer_t*) wc->wr_id;
        connect_t* conn  = buff->context;
        worker_t* worker = conn->worker;

        pthread_spin_lock(&conn->spin_lock);
        if (conn->auto_ack == buff) {
            conn->auto_ack = NULL;
        }

        if (buff->state == BUFF_ST_APP_INUSE) {
            conn->buff_ref--;
            buff->state = BUFF_ST_FREE;
        }
        pthread_spin_unlock(&conn->spin_lock);

        if (wc->status != IBV_WC_SUCCESS) {
            LOG(ERROR, "conn:(%lu-%p) failed:%d %s, recv :%lu", conn->nd, conn, wc->status, ibv_wc_status_str(wc->status), conn->recv_cnt);
            if (conn->state == CONN_ST_CONNECTED) {
                conn_notify_error(conn);
                conn_close(worker, conn);
            } else {
                disconnect(conn->nd);
            }
            continue;
        }

        LOG(DEBUG, "op code:%d, status:%d, %d", wc->opcode, wc->status, wc->byte_len);
        switch (wc->opcode)
        {
        case IBV_WC_RECV: //128
            /* code */
            process_recv_meta(wc, conn, now);
            break;
        case IBV_WC_SEND:  //0
            process_send_meta(wc, conn);
            break;
        case IBV_WC_RDMA_WRITE:   //1
            if (process_send_data(wc, msg, conn, buff) != 0) {
                msg_index++;
            }
            break;
        default:
            //error no support
            LOG(ERROR, "not support wc->opcode:%d", wc->opcode);
            assert(0);
            break;
        }
    }
    return msg_index;
}

void process_close_list(worker_t *worker) {
    list_link_t retry_list;
    list_link_t task_list;
    int64_t now = 0;

    list_head_init(&retry_list);
    list_head_init(&task_list);
    now = get_time_ns();

    pthread_spin_lock(&worker->lock);
    list_splice_tail(&task_list, &worker->close_list);
    list_head_init(&worker->close_list);
    pthread_spin_unlock(&worker->lock);

    while (!list_is_empty(&task_list)) {
        connect_t *conn = list_head(&task_list, connect_t, close_node);
        if (conn != NULL) {
            pthread_spin_lock(&conn->spin_lock);
            list_del(&conn->close_node);
            if (!conn->is_app_closed || conn->ref || conn->buff_ref || conn->state != CONN_ST_DISCONNECTED) {
                if (conn->state == CONN_ST_DISCONNECTED) {
                    release_rdma(conn);
                }
                //wait next
                list_add_tail(&retry_list, &conn->close_node);
                pthread_spin_unlock(&conn->spin_lock);
                if (now - conn->close_start > CLOSE_TIME_OUT) {
                    LOG_WITH_LIMIT(ERROR, now, 60, 6, 1, "conn(%lu-%p) close timeout, is_app_closed:%d, ref:%u, buffer_ref:%u, state:%s",
                                   conn->nd, conn, conn->is_app_closed, conn->ref, conn->buff_ref, conn_state_names[conn->state]);
                }
            } else {
                LOG(INFO, "conn(%lu-%p) release and del it from close_task_list", conn->nd, conn);
                set_conn_state(conn, CONN_ST_CLOSED);
                pthread_spin_unlock(&conn->spin_lock);
                del_conn_from_worker(conn->nd, conn->worker, conn->worker->closing_nd_map);
                release_rdma(conn);
                release_buffer(conn);
                conn_notify_closed(conn);
                free(conn);
            }
        }
    }

    pthread_spin_lock(&worker->lock);
    list_splice_tail(&worker->close_list, &retry_list);
    pthread_spin_unlock(&worker->lock);
    return;
}

static int update_recv_cur(connect_t* conn, block_header_t* head, int64_t now) {
    uint16_t    recv_ack_cnt = 0;
    buffer_t *buff;
    uint64_t    notify_value = 1;

    assert(head != NULL);

    pthread_spin_lock(&conn->spin_lock);
    for(int i = 0; i < head->block_cnt; i++) {
        buff = list_extract_head(&conn->recv_free_list, buffer_t, node);
#ifdef _LIST_CHECK_
        assert(buff->index == (conn->recv_head_index % conn->recv_block_cnt));
#endif
        conn->recv_win_size--;
        buff->state = BUFF_ST_APP_INUSE;
        conn->buff_ref++;
        conn->recv_head_index++;
    }
    conn->recv_time = now;

    conn->recv_cur = NULL;
    if (!list_is_empty(&conn->recv_free_list)) {
        conn->recv_cur = list_head(&conn->recv_free_list, buffer_t, node);
    }

    conn->peer_send_wind_size = head->send_wind_size;

    recv_ack_cnt = head->recv_ack_cnt >= conn->peer_ack_cnt ? (head->recv_ack_cnt - conn->peer_ack_cnt) : (MAX_PORT_DEPTH_SIZE - conn->peer_ack_cnt + head->recv_ack_cnt + 1);
    if (recv_ack_cnt > conn->recv_block_cnt) {
        recv_ack_cnt = 0;
    } else {
        conn->peer_ack_cnt = head->recv_ack_cnt;
    }

    for (int i = 0; i < recv_ack_cnt; i++) {
        buff = list_extract_head(&conn->send_wait_free_list, buffer_t, node);
        if (buff->state == BUFF_ST_APP_INUSE) {
            buff->state = BUFF_ST_FREE;
            conn->buff_ref--;
        }
        list_add_tail(&conn->send_free_list, &buff->node);
        memset(buff->data, 0, conn->send_block_size);
#ifdef _LIST_CHECK_
        assert(buff->index == (conn->send_tail_index % conn->send_block_cnt));
#endif
        conn->send_tail_index++;
    }

    if (conn->send_win_size < 1) {
        write(conn->efd, &notify_value, 8);
    }

    conn->send_win_size += recv_ack_cnt;
    conn->recv_cnt      += head->block_cnt;
    pthread_spin_unlock(&conn->spin_lock);

    return 0;
}

int process_recv_data(worker_t *worker, complete_msg_t* msgs, int len, int64_t now) {
    list_link_t retry_list;
    list_link_t task_list;
    list_link_t recv_list;
    connect_t*  conn     = NULL;
    int msg_index        = 0;
    int count            = 0;
    complete_msg_t* msg  = NULL;
    buffer_t*     buff   = NULL;
    block_header_t* head = NULL;
    uint64_t notify_value = 1;
    int64_t  recv_dead_line = -1;

    list_head_init(&retry_list);
    list_head_init(&task_list);
    list_head_init(&recv_list);
    pthread_spin_lock(&worker->lock);
    list_splice_tail(&task_list, &worker->conn_list);
    list_head_init(&worker->conn_list);
    pthread_spin_unlock(&worker->lock);

    while(msg_index < len && (!list_is_empty(&task_list)) && count < 128) {
        count++;
        recv_dead_line = -1;
        conn = list_extract_head(&task_list, connect_t, worker_node);
        pthread_spin_lock(&conn->spin_lock);

        if (now - conn->last_notify > 1000 && conn->efd > 0) {
            write(conn->efd, &notify_value, 8);
            conn->last_notify = now;
        }

        if (conn->state != CONN_ST_CONNECTED) {
            pthread_spin_unlock(&conn->spin_lock);
            continue;
        }

        if ((now - conn->last_req_time > ONE_SEC_IN_NS || conn->peer_send_wind_size < 2) && conn->auto_ack == NULL && conn->recv_ack_cnt != conn->last_auto_seq) {
           conn_auto_reply_ack(conn);
        }

        buff = conn->recv_cur;
        if (conn->recv_timeout_ns != -1) {
            recv_dead_line = conn->recv_time + conn->recv_timeout_ns;
        }
        pthread_spin_unlock(&conn->spin_lock);

        // timeout
        // timeout_ns = -1; without set recv timeout
        // recv_time = 0; no recv
        if (recv_dead_line != -1 && now > recv_dead_line) {
            //recv timeout err wait close
            LOG_WITH_LIMIT(ERROR, now, 60, 6, 1, "conn(%lu-%p) recv timeout", conn->nd, conn);
            conn_notify_error(conn);
            continue;
        }

        if (check_recv_data(conn, buff) != 0) {
            list_add_tail(&retry_list, &conn->worker_node);
            continue;
        }

        head = (block_header_t*)buff->data;
        if (update_recv_cur(conn, head, now) == 0) {
            list_add_tail(&recv_list, &conn->worker_node);
        }

        head->block_start = buff->index;
        if (head->len == 0) {
            //inner msg
            _cbrdma_release_recv_data(conn, buff->data + sizeof(block_header_t));
            continue;
        }

        msg = msgs + msg_index;
        msg_index++;

        msg->op     = CBRDMA_OP_RECV;
        msg->status = 1;
        msg->length = head->len;
        msg->buff   = buff->data + sizeof(block_header_t);
        msg->nd     = conn->nd;
        msg->user_context = conn->context;
    }

    pthread_spin_lock(&worker->lock);
    //todo add before
    list_splice_tail(&worker->conn_list, &task_list);
    //add tail
    list_splice_tail(&worker->conn_list, &retry_list);
    list_splice_tail(&worker->conn_list, &recv_list);
    pthread_spin_unlock(&worker->lock);

    return msg_index;
}

//return msg numer
int cbrdma_worker_poll(uint32_t worker_id, complete_msg_t* msgs, int msg_len) {
    worker_t  *worker = g_net_env->worker + (worker_id % g_net_env->worker_num);
    struct ibv_cq *cq = worker->cq;
    int    ret        = 0;
    struct ibv_wc wc[CQ_CNT_PER_POLL];
    int  deal_len = msg_len > CQ_CNT_PER_POLL ? CQ_CNT_PER_POLL : msg_len;
    int  recv_msg_cnt = 0;

    if (worker->w_pid == 0) {
        worker->w_pid = pthread_self();
    }

    if (worker->run_cycle % 1000 == 0) {
        worker->last_active_time = get_time_ns();
    }
    worker->run_cycle++;

    memset(wc, 0, sizeof(wc));

    //close
    process_close_list(worker);

    recv_msg_cnt = process_recv_data(worker, msgs, msg_len / 2, worker->last_active_time);

    deal_len = (msg_len - recv_msg_cnt) > CQ_CNT_PER_POLL ? CQ_CNT_PER_POLL : (msg_len - recv_msg_cnt);
    if (deal_len == 0) {
        return recv_msg_cnt;
    }

    ret = ibv_poll_cq(cq, deal_len, wc);
    if (ret <= 0) {
        //error
        ret = recv_msg_cnt > 0 ? recv_msg_cnt : ret;
        return ret;
    }

    //error
    ret = on_completion(wc, msgs + recv_msg_cnt, ret, worker->last_active_time);

    return ret + recv_msg_cnt;
}
