#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <assert.h>
#include <unistd.h>
#include <errno.h>
#include <linux/if_link.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <ifaddrs.h>
#include "common.h"

#define LINE_STR_LEN        128
#define LOG_LINE_MAX_LEN    1024

log_handler_cb_t g_log_handler;
on_disconnected_cb_t    g_disconnected_handler;
on_error_cb_t    g_error_handler;
on_closed_cb_t    g_closed_handler;
net_env_t        *g_net_env;


const char * conn_state_names[CONN_ST_MAX] = {"CONN_ST_INIT", "CONN_ST_CONNECTING", "CONN_ST_CONNECTED", "CONN_ST_ERROR",
    "CONN_ST_CLOSING", "CONN_ST_DISCONNECTED", "CONN_ST_CLOSED"};

int64_t get_time_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000000000 + ts.tv_nsec;
}

void deal_log(int level, const char* func, int line, const char* fmt, ...) {
    char buf[LOG_LINE_MAX_LEN] = {0};
    va_list args;

    if (level < g_net_env->log_level) {
        return;
    }

    snprintf(buf, LOG_LINE_MAX_LEN, "[libcbrdma.so] [%lu] [%s:%d] ", pthread_self(), func, line);
    va_start(args, fmt);
    vsprintf(buf + strlen(buf) , fmt, args);
    va_end(args);
    if (g_log_handler) {
        g_log_handler(ERROR, buf, strlen(buf));
    }
    return;
}

const char *cbrdma_net_status_string(int16_t status) { return NULL; }

static int get_eth_dev_name_by_ip(char* local_ip, char dev_name[], int len) {
    struct ifaddrs *ifaddr;
    int family, s;
    char host[NI_MAXHOST];
    int ret = 0;

    if (getifaddrs(&ifaddr) == -1) {
        LOG(ERROR, "getifaddrs failed: %d", errno);
        ret = errno;
        return ret;
    }

    ret = -1;
    for (struct ifaddrs *ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL) {
            continue;
        }

        family = ifa->ifa_addr->sa_family;
        if (family == AF_INET || family == AF_INET6) {
            s = getnameinfo(ifa->ifa_addr,
                    (family == AF_INET) ? sizeof(struct sockaddr_in) :
                                          sizeof(struct sockaddr_in6),
                    host, NI_MAXHOST,
                    NULL, 0, NI_NUMERICHOST);
            if (s != 0) {
                LOG(ERROR, "getnameinfo() failed: %d\n", errno);
                ret = errno;
                break;
            }
            if( !strcmp(host, local_ip)) {
                strncpy(dev_name, ifa->ifa_name, strlen(ifa->ifa_name) > len ? len : strlen(ifa->ifa_name));
                ret = 0;
                break;
            }
        }
    }

    freeifaddrs(ifaddr);
    return ret;
}

int get_rdma_dev_name_by_ip(char* local_ip, char rdma_name[], int len) {
    int  ret = 0;
    char dev_name[DEV_NAME_LEN]  = {0};
    char str_line[LINE_STR_LEN] = {0};
    FILE* fp = NULL;

    ret = get_eth_dev_name_by_ip(local_ip, dev_name, DEV_NAME_LEN);
    if (ret != 0) {
        return ret;
    }

    fp = popen("ibdev2netdev", "r");
    if (fp == NULL) {
        LOG(ERROR, "can not find ibdev2netdev cmd, failed:%d", errno);
        ret = errno;
        return ret;
    }

    while (fgets(str_line, sizeof(str_line), fp) != NULL) {
        if (strstr(str_line, dev_name) != NULL) {
            int rdma_len = (strchr(str_line, ' ') - str_line);
            strncpy(rdma_name, str_line, rdma_len > len ? len : rdma_len);
            ret = 0;
            break;
        }
    }

    pclose(fp);
    return ret;
}

uint64_t allocate_nd(int type) {
    int id_index = ID_GEN_CTRL;
    union conn_nd_union id;

    id_index += 1;
    id.nd_.worker_id = __sync_fetch_and_add((g_net_env->id_gen + id_index), 1) & 0xFF;
    id.nd_.type = type & 0xFF;
    id.nd_.m1 = 'c';
    id.nd_.m2 = 'b';

    id.nd_.id = __sync_fetch_and_add((g_net_env->id_gen + ID_GEN_MAX - 1), 1);
    return id.nd;
}

//export to lib
void cbrdma_parse_nd(uint64_t nd, int *id, int * worker_id, int * is_server, int * is_active) {
    *id = (nd & 0xFFFFFFFF);
    *worker_id = ((nd >> 32) & 0xFF);
    uint8_t type  = (((nd >> 32) & 0xFF00) >> 8);
    *is_server = type & 0x80;
    *is_active = type & 0x40;
}

worker_t* get_worker_by_nd(uint64_t nd) {
    int worker_id = ((nd) >>CONN_ID_BIT_LEN) % g_net_env->worker_num;
    return g_net_env->worker + worker_id;
}

int add_conn_to_worker(connect_t * conn, worker_t * worker, khash_t(map) *hmap) {
    int ret = 0;
    pthread_spin_lock(&worker->nd_map_lock);
    ret = hashmap_put(hmap, conn->nd, (uint64_t)conn);
    if (hmap == worker->nd_map)
        worker->qp_cnt++;
    pthread_spin_unlock(&worker->nd_map_lock);
    return ret >= 0;
}

int del_conn_from_worker(uint64_t nd, worker_t * worker, khash_t(map) *hmap) {
    int ret = 0;
    pthread_spin_lock(&worker->nd_map_lock);
    ret = hashmap_del(hmap, nd);
    if (hmap == worker->closing_nd_map)
        worker->qp_cnt--;
    pthread_spin_unlock(&worker->nd_map_lock);
    return ret >= 0;
}

void get_worker_and_connect_by_nd(uint64_t nd, worker_t ** worker, connect_t** conn, int8_t with_ref) {
    *worker = get_worker_by_nd(nd);
    pthread_spin_lock(&(*worker)->nd_map_lock);
    *conn = (connect_t *)hashmap_get((*worker)->nd_map, nd);
    if (*conn != NULL && with_ref) {
        pthread_spin_lock(&(*conn)->spin_lock);
        (*conn)->ref++;
        pthread_spin_unlock(&(*conn)->spin_lock);
    }
    pthread_spin_unlock(&(*worker)->nd_map_lock);
}

void _get_worker_and_connect_by_nd(uint64_t nd, worker_t** worker, connect_t** conn, int8_t with_ref) {
    *worker = get_worker_by_nd(nd);
    pthread_spin_lock(&(*worker)->nd_map_lock);
    *conn = (connect_t *)hashmap_get((*worker)->nd_map, nd);
    if (*conn == NULL) {
        *conn = (connect_t *)hashmap_get((*worker)->closing_nd_map, nd);
    }
    if (*conn != NULL && with_ref) {
        pthread_spin_lock(&(*conn)->spin_lock);
        if ((*conn)->state == CONN_ST_CLOSED) {
            pthread_spin_unlock(&(*conn)->spin_lock);
            pthread_spin_unlock(&(*worker)->nd_map_lock);
            *conn = NULL;
            return;
        }

        (*conn)->ref++;
        pthread_spin_unlock(&(*conn)->spin_lock);
    }
    pthread_spin_unlock(&(*worker)->nd_map_lock);
}

static inline void conn_close_efd_with_lock(connect_t *conn) {
    if (conn == NULL) {
        return;
    }

    pthread_spin_lock(&conn->spin_lock);
    if (conn->efd > 0) {
        close(conn->efd);
        conn->efd = -1;
    }

    if (conn->state == CONN_ST_CONNECTED) {
        set_conn_state(conn, CONN_ST_ERROR);
    }

    pthread_spin_unlock(&conn->spin_lock);
    return;
}

void conn_notify_disconnect(connect_t *conn) {
    if (conn == NULL) {
        return;
    }
    LOG(INFO, "conn(%lu-%p) notify disconnect", conn->nd, conn);
    conn_close_efd_with_lock(conn);
    if (g_disconnected_handler != NULL) {
        g_disconnected_handler(conn->nd, conn->context);
    }
}

void conn_notify_error(connect_t *conn) {
    if (conn == NULL) {
        return;
    }
    LOG(INFO, "conn(%lu-%p) notify error", conn->nd, conn);
    conn_close_efd_with_lock(conn);
    if (g_error_handler != NULL) {
        g_error_handler(conn->nd, conn->context);
    }
}

void conn_notify_closed(connect_t *conn) {
    if (conn == NULL) {
        return;
    }
    LOG(INFO, "conn(%lu-%p) notify disconnect", conn->nd, conn);
    conn_close_efd_with_lock(conn);
    if (g_closed_handler != NULL) {
        g_closed_handler(conn->nd, conn->context);
    }
}

static inline void init_buff(buffer_t* buff, connect_t* conn, int index, int type) {
    buff->context = (void*) conn;
    buff->index = index;
    list_head_init(&buff->node);
    buff->type = type;
    return;
}

int reg_meta_data(connect_t *conn, buffer_t* meta) {
    meta->data = malloc(sizeof(meta_t));
    if (meta == NULL) {
        return -1;
    }
    LOG(INFO, "meta malloc:%p, size:%d", meta->data, sizeof(meta_t));
    memset(meta->data, 0, sizeof(meta_t));
    meta->context = (void*) conn;
    meta->mr = ibv_reg_mr(conn->worker->pd, meta->data, sizeof(meta_t), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (meta->mr == NULL) {
        free(meta->data);
        meta->data = NULL;
        return -1;
    }
    LOG(INFO, "ibv_reg_mr meta malloc:%p, size:%d", meta->data, sizeof(meta_t));
    return 0;
}

int conn_reg_data_buff(connect_t * conn, uint32_t block_size, uint32_t block_cnt, int mem_type, buffer_t * buff_array) {
    void* data_buff    = NULL;
    worker_t *worker   = conn->worker;

    data_buff = malloc(block_size * block_cnt);
    if (data_buff == NULL) {
        LOG(ERROR, "conn(%lu-%p) malloc recv data failed, errno:%d", conn->nd, conn, errno);
        return -1;
    }

    for (int i = 0; i < block_cnt; i++) {
        buffer_t *buff = buff_array + i;
        init_buff(buff, conn, i, 0);
        buff->data = data_buff + (i * block_size);

        buff->mr = ibv_reg_mr(worker->pd, buff->data, block_size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
        if (buff->mr == NULL) {
            LOG(ERROR, "conn(%lu-%p) reg recv buff failed, errno:%d", conn->nd, conn, errno);
            goto failed;
        }

        LOG(INFO, "conn(%lu-%p) buff:%p ibv_reg_mr mr: %p, data:%p", conn->nd, conn, buff, buff->mr, buff->data);
    }

    return 0;

failed:
    for (int i = 0; i < block_cnt; i++) {
        buffer_t *buff = buff_array + i;
        if (buff->mr != NULL)
            ibv_dereg_mr(buff->mr);
        buff->mr = NULL;
    }
    free(data_buff);
    return -1;
}

int conn_bind_remote_recv_key(connect_t *conn, meta_t* rmeta) {
    assert(conn != NULL);
    assert(rmeta != NULL);
    assert(conn->send_buff != NULL);

    for (int i = 0; i < rmeta->count; i++) {
        buffer_t *buff = conn->send_buff + i;
        buff->peer_rkey = rmeta->rkey[i];
        buff->peer_addr = rmeta->addr[i];
        list_add_tail(&conn->send_free_list, &buff->node);
        conn->send_win_size++;
    }

    return 0;
}

int dereg_buffer(connect_t * conn, buffer_t *buff_arr, int len) {
    int ret = 0;
    for (int i = 0; i < len; i++) {
        buffer_t *buff = buff_arr + i;
        if (buff->state == BUFF_ST_APP_INUSE) {
            LOG(INFO, "conn(%lu-%p) buff:%p mr:%p wait app free", conn->nd, conn, buff, buff->mr);
            ret = -1;
            break;
        }

        if (buff->mr != NULL) {
            ret = ibv_dereg_mr(buff->mr);
            if (ret == 0) {
                buff->mr = NULL;
            }
            LOG(INFO, "conn(%lu-%p) buff:%p mr:%p ibv_dereg_mr return: %d, errno:%d", conn->nd, conn, buff, buff->mr, ret, errno);
            break;
        }
    }
    
    return ret;
}

void dereg_all_buffer(connect_t * conn) {
    if (dereg_buffer(conn, conn->send_meta, 1) == 0
        && dereg_buffer(conn, conn->recv_meta, 1) == 0
        && dereg_buffer(conn, conn->send_buff, conn->send_block_cnt) == 0
        && dereg_buffer(conn, conn->recv_buff, conn->recv_block_cnt) == 0) {
        conn->buff_ref = 0;
    }
}

void release_rdma(connect_t * conn) {
    if (conn->qp != NULL && conn->id != NULL) {
        rdma_destroy_qp(conn->id);
        LOG(INFO, "conn(%lu-%p) rdma_destroy_qp:%p", conn->nd, conn, conn->id);
    }

    if (conn->id != NULL) {
        rdma_destroy_id(conn->id);
        LOG(INFO, "conn(%lu-%p) rdma_destroy_id:%p", conn->nd, conn, conn->id);
    }

    conn->id = NULL;
    conn->qp = NULL;

    dereg_all_buffer(conn);
}

int free_buffer(buffer_t *buff) {
    if (buff == NULL) {
        return 0;
    }

    if (buff->data != NULL)
        free(buff->data);

    buff->mr   = NULL;
    buff->data = NULL;
    free(buff);
    return 0;
}

void release_buffer(connect_t * conn) {
    free_buffer(conn->send_meta);
    conn->send_meta = NULL;
    LOG(INFO, "free send_meta:%p", conn);

    free_buffer(conn->recv_meta);
    conn->recv_meta = NULL;
    LOG(INFO, "free recv_meta:%p", conn);

    free_buffer(conn->send_buff);
    conn->send_buff = NULL;
    LOG(INFO, "free send_buff:%p", conn);

    free_buffer(conn->recv_buff);
    conn->recv_buff = NULL;
    LOG(INFO, "free recv_buff:%p", conn);
}

int conn_close(worker_t* worker, connect_t* conn) {
    int ret = 0;
    uint64_t notify_value = 1;
    pthread_spin_lock(&conn->spin_lock);

     if (conn->efd > 0) {
        write(conn->efd, &notify_value, 8);
        close(conn->efd);
        conn->efd = -1;
    }

    conn->is_app_closed = 1;
    if (conn->state != CONN_ST_CLOSED && conn->state != CONN_ST_DISCONNECTED) {
        if (conn->state != CONN_ST_CLOSING && conn->id != NULL) {
            ret = rdma_disconnect(conn->id);
        }
        set_conn_state(conn, CONN_ST_CLOSING);
    }

    if (conn->close_start == 0) {
        conn->close_start = get_time_ns();
    }
    LOG(INFO, "conn(%lu-%p) rdma_disconnect:%p", conn->nd, conn, conn->id);

    pthread_spin_unlock(&conn->spin_lock);

    del_conn_from_worker(conn->nd, worker, worker->nd_map);
    add_conn_to_worker(conn, worker, worker->closing_nd_map);

    return ret;
}

//触发rdma_disconnect，放入等待队列
int disconnect(uint64_t nd) {
    LOG(INFO, "disconnect:%lu", nd);
    worker_t * worker = NULL;//get_worker_by_nd(nd);
    connect_t * conn  = NULL;//get_connect_by_nd(nd);
    int ret = 0;

    get_worker_and_connect_by_nd(nd, &worker, &conn, GET_CONN_WIT_REF);
    if (conn == NULL) return 0;
    conn->is_app_closed = 1;
    ret = conn_close(worker, conn);
    conn_del_ref(conn);
    return ret;
}

void client_build_reg_recv_buff_cmd(connect_t* conn) {
    meta_t *meta = (meta_t*) conn->send_meta->data;
    meta->cmd    = CMD_REG_RECV_BUFF;
    meta->size   = conn->recv_block_size;
    meta->count  = conn->recv_block_cnt;
    for (int i = 0; i < meta->count; i++) {
        buffer_t* recv_buff = conn->recv_buff + i;
        meta->addr[i]   =  (uintptr_t)recv_buff->mr->addr;
        meta->rkey[i]   = recv_buff->mr->rkey;
        recv_buff->state = BUFF_ST_POLLING_INUSE;
        list_add_tail(&conn->recv_free_list, &recv_buff->node);
        conn->recv_win_size++;
    }

    LOG(INFO, "reg client recv buffer to size:%u, count:%u, 0 rkey:%u, 0 raddr:%lu", meta->size, meta->count, meta->rkey[0], meta->addr[0]);
    return;
}

int post_recv_meta(connect_t *conn) {
    struct ibv_recv_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;
    int ret = 1;

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t)conn->recv_meta;
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    sge.addr = (uintptr_t)conn->recv_meta->data;
    sge.length = sizeof(meta_t);
    sge.lkey = conn->recv_meta->mr->lkey;

    ret = ibv_post_recv(conn->qp, &wr, &bad_wr);
    if (ret == 0) {
        conn->buff_ref++;
        conn->recv_meta->state = BUFF_ST_APP_INUSE;
    }
    return ret;
}

int post_send_meta(connect_t *conn) {
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;
    int ret = 1;

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t)conn->send_meta;
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_FENCE;

    sge.addr = (uintptr_t)conn->send_meta->data;
    sge.length = sizeof(meta_t);
    sge.lkey = conn->send_meta->mr->lkey;
    ret = ibv_post_send(conn->qp, &wr, &bad_wr);
    if (ret == 0) {
        conn->buff_ref++;
        conn->send_meta->state = BUFF_ST_APP_INUSE;
    }
    return ret;
}

int post_send_data(connect_t *conn, buffer_t *buff, uint32_t len) {
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;
    int ret = 1;

    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uintptr_t)buff;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_FENCE;
    wr.wr.rdma.remote_addr = buff->peer_addr;
    wr.wr.rdma.rkey = buff->peer_rkey;

    if (len != 0) {
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.next    = NULL;

        sge.addr   = (uintptr_t)buff->data;
        sge.length = len;
        sge.lkey   = buff->mr->lkey;
    }

    ret = ibv_post_send(conn->qp, &wr, &bad_wr);
    if (ret == 0) {
        conn->post_send_cnt++;
        conn->last_auto_seq = conn->recv_ack_cnt;
        conn->last_req_time = get_time_ns();
    }
    return ret;
}
