#include "cbrdma.h"
#include "common.h"
#include "list.h"
#include "hashmap.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <numa.h>
#include <pthread.h>
#include <stdint.h>
#include <linux/types.h>
#include <sys/time.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/eventfd.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>


#define MIN_CQE_NUM                             1024
#define WQ_DEPTH            8
#define WQ_SG_DEPTH         2

#define CBRDMA_MIN_BLOCK_SIZE 64
#define CBRDMA_MIN_BLOCK_CNT  4

static void build_qp_attr(struct ibv_cq *cq, struct ibv_qp_init_attr *qp_attr);
static void on_addr_resolved(struct rdma_cm_id *id);
static void on_route_resolved(struct rdma_cm_id *conn_id);
static void on_connected(struct rdma_cm_id *id);
static void on_accept(struct rdma_cm_id* listen_id, struct rdma_cm_id* id);
static void on_disconnected(struct rdma_cm_id* id);

extern log_handler_cb_t g_log_handler;
extern on_disconnected_cb_t    g_disconnected_handler;
extern on_error_cb_t    g_error_handler;
extern on_closed_cb_t    g_closed_handler;
extern net_env_t        *g_net_env;

const int TIMEOUT_IN_MS = 500; /* ms */

//创建pd, cq, 发送队列
static int init_worker(worker_t *worker) {
    int ret = 0;
    worker->pd = ibv_alloc_pd(g_net_env->ctx);
    if (worker->pd == NULL) {
        LOG(ERROR, "alloc pd failed, errno:%d", errno);
        return 0;
    }
    LOG(INFO, "ibv_alloc_pd:%p", worker->pd);
    worker->cq = ibv_create_cq(g_net_env->ctx, MIN_CQE_NUM, NULL, NULL/*worker->comp_channel*/, 0);
    if (worker->cq == NULL) {
        //return assert,ignore resource free
        LOG(ERROR, "create cq failed, errno:%d", errno);
        return 0;
    }
    LOG(INFO, "ibv_create_cq:%p", worker->cq);

    ret = pthread_spin_init(&(worker->nd_map_lock), PTHREAD_PROCESS_SHARED);
    if (ret != 0) {
        LOG(ERROR, "init worker spin lock failed, err:%d", ret);
        return 0;
    }

    worker->nd_map = hashmap_create();

    worker->closing_nd_map = hashmap_create();

    list_head_init(&worker->conn_list);
    list_head_init(&worker->close_list);

    ret = pthread_spin_init(&(worker->lock), PTHREAD_PROCESS_SHARED);
    if (ret != 0) {
        LOG(ERROR, "init worker spin task_lock failed, err:%d", ret);
        return 0;
    }

    return 1;
}

static void destroy_worker(worker_t *worker) {
    if (worker->nd_map != NULL) {
        hashmap_destroy(worker->nd_map);
        worker->nd_map = NULL;
    }

    if (worker->closing_nd_map != NULL) {
        hashmap_destroy(worker->closing_nd_map);
        worker->closing_nd_map = NULL;
    }

    pthread_spin_destroy(&worker->nd_map_lock);
    pthread_spin_destroy(&worker->lock);

    if (worker->cq != NULL) {
        if (ibv_destroy_cq(worker->cq)) {
            LOG(ERROR, "Failed to destroy completion queue cleanly, %d \n", -errno);
            // we continue anyways;
        }
        LOG(INFO, "ibv_destroy_cq:%p", worker->cq);
        worker->cq = NULL;
    }

    if (worker->pd != NULL) {
        if (ibv_dealloc_pd(worker->pd)) {
            LOG(ERROR, "Failed to destroy client protection domain cleanly, %d \n", -errno);
            // we continue anyways;
        }
        LOG(INFO, "ibv_dealloc_pd:%p", worker->pd);
        worker->pd = NULL;
    }
}

void cbrdma_init_config(cbrdma_config_t * config) {
    memset(config, 0, sizeof(cbrdma_config_t));
    config->numa_node = -1;
    config->worker_num = 1;
    config->max_msg_cnt_per_poll = 16;
    config->log_level = INFO;
    config->conn_timeout_ms = 1000;
}

static void process_net_event(int event_type, struct rdma_cm_id *listen_id, struct rdma_cm_id *conn_id) {
    LOG(INFO, "process_net_event:%d->%s", event_type, rdma_event_str(event_type));
    switch(event_type) {
        /*active op*/
        case RDMA_CM_EVENT_ADDR_RESOLVED:
            on_addr_resolved(conn_id);
            break;
        case RDMA_CM_EVENT_ROUTE_RESOLVED:
            on_route_resolved(conn_id);
            break;
        case RDMA_CM_EVENT_ADDR_ERROR:
        case RDMA_CM_EVENT_ROUTE_ERROR:
        case RDMA_CM_EVENT_REJECTED:
        case RDMA_CM_EVENT_UNREACHABLE:
            on_disconnected(conn_id);
            break;
        case RDMA_CM_EVENT_CONNECT_RESPONSE:
            LOG(ERROR, "event channel received: acitve recv conn resp event");
            assert(event_type == 0);        //assert
            break;

        /*passive op*/
        case RDMA_CM_EVENT_CONNECT_REQUEST:
            on_accept(listen_id, conn_id);
            break;

        /*both*/
        case RDMA_CM_EVENT_ESTABLISHED:
            on_connected(conn_id);
            break;
        case RDMA_CM_EVENT_CONNECT_ERROR:
        case RDMA_CM_EVENT_DISCONNECTED:
            on_disconnected(conn_id);
            break;
        case RDMA_CM_EVENT_TIMEWAIT_EXIT:
            //do nothing
            break;
        /*not support*/
        case RDMA_CM_EVENT_DEVICE_REMOVAL:
        case RDMA_CM_EVENT_MULTICAST_JOIN:
        case RDMA_CM_EVENT_MULTICAST_ERROR:
        case RDMA_CM_EVENT_ADDR_CHANGE:
            LOG(ERROR, "event channel received:unspport event:%d", event_type);
            assert(event_type == 0);        //assert
            break;
        default :
            LOG(ERROR, "event channel received:unknown event:%d", event_type);
            assert(event_type == 0);        //assert
            break;

    }
}

void* net_event_loop(void* ctx) {
    net_env_t *env = (net_env_t *) ctx;
    struct rdma_cm_event *evt;
    while (rdma_get_cm_event(env->event_channel, &evt) == 0) {
        struct rdma_cm_id *conn_id   = evt->id;
        struct rdma_cm_id *listen_id = evt->listen_id;
        int    event_type            = evt->event;

        rdma_ack_cm_event(evt);
        process_net_event(event_type, listen_id, conn_id);
    }
    return NULL;
}

//初始化cbrdma_env_t，rdma_get_devices获取ibv_ctx, 创建event_channel, 初始化worker
int cbrdma_init(cbrdma_config_t * config) {
    g_disconnected_handler = config->on_disconnected_func;
    g_error_handler = config->on_error_func;
    g_closed_handler = config->on_closed_func;
    g_log_handler   = config->log_handler_func;

    int len = sizeof(net_env_t) + config->worker_num * sizeof(worker_t);
    g_net_env = (net_env_t*) cbrdma_malloc(len);
    if (g_net_env == NULL) {
        LOG(ERROR, "init env failed: no enouth memory");
        goto error;
    }
    g_net_env->worker_num = config->worker_num;
    g_net_env->log_level  = config->log_level;
    list_head_init(&g_net_env->server_list);

    if (pthread_spin_init(&(g_net_env->server_lock), PTHREAD_PROCESS_SHARED) != 0) {
        LOG(ERROR, "init gnet_env->server_lock spin lock failed");
        goto error;
    }

    g_net_env->all_devs = rdma_get_devices(&g_net_env->ib_dev_cnt);
    if (g_net_env->all_devs == NULL) {
        LOG(ERROR, "init env failed: no enouth memory");
        goto error;
    }
    LOG(INFO, "rdma_get_devices find ib_dev_cnt:%d", g_net_env->ib_dev_cnt);

    char rdma_dev_name[DEV_NAME_LEN] = {0};
    if (get_rdma_dev_name_by_ip(config->str_local_ip, rdma_dev_name, DEV_NAME_LEN) != 0) {
        LOG(ERROR, "get rdma dev name failed");
        goto error;
    }
    LOG(INFO, "get_rdma_dev_name_by_ip(%s):%s", config->str_local_ip, rdma_dev_name);

    struct ibv_context* tmp;
    for (int i = 0; i < g_net_env->ib_dev_cnt; i++) {
        tmp = g_net_env->all_devs[i];
        if (strncmp(rdma_dev_name, tmp->device->name, strlen(rdma_dev_name)) == 0) {
            g_net_env->ctx = tmp;
            break;
        }
    }

    if (g_net_env->ctx == NULL) {
        LOG(ERROR, "can not find rdma dev");
        goto error;
    }

    g_net_env->event_channel = rdma_create_event_channel();
    pthread_create(&g_net_env->event_loop_thread, NULL, net_event_loop, g_net_env);

    for (int i = 0; i < g_net_env->worker_num; i++) {
        LOG(INFO, "init_worker(%d)", i);
        g_net_env->worker[i].id = i;
        if (init_worker(g_net_env->worker + i) <= 0) {
            LOG(ERROR, "init env failed: init worker[%d] failed\n", i);
            goto error;
        }
    }

    return 1;
error:
    cbrdma_destroy();
    return 0;
}

void cbrdma_destroy() {
    LOG(INFO, "cbrdma_destroy");
    for (int i = 0; i < g_net_env->worker_num; i++) {
        destroy_worker(g_net_env->worker + i);
    }

     if (g_net_env->event_channel != NULL) {
        rdma_destroy_event_channel(g_net_env->event_channel);
        g_net_env->event_channel = NULL;
    }

    if (g_net_env->event_loop_thread > 0) {
        if (pthread_join(g_net_env->event_loop_thread, NULL)) {
            LOG(ERROR, "pthread_join(g_net_env->event_loop_thread) failed");
            g_net_env->event_loop_thread = 0;
        }
    }

    if (g_net_env->all_devs != NULL) {
        rdma_free_devices(g_net_env->all_devs);
        g_net_env->all_devs = NULL;
    }

    pthread_spin_destroy(&g_net_env->server_lock);

    if (g_net_env != NULL) {
        free(g_net_env);
        g_net_env = NULL;
    }
}

static void add_server_to_env(server_t* server) {
    pthread_spin_lock(&g_net_env->server_lock);
    list_add_tail(&g_net_env->server_list, &server->server_node);
    ++g_net_env->server_cnt;
    pthread_spin_unlock(&g_net_env->server_lock);
    return;
}

static void del_server_from_env(server_t* server) {
    pthread_spin_lock(&g_net_env->server_lock);
    list_del(&server->server_node);
    --g_net_env->server_cnt;
    pthread_spin_unlock(&g_net_env->server_lock);
    return;
}

static int add_conn_to_server(connect_t * conn, server_t * server) {
    int ret = 0;
    conn->server = server;
    pthread_spin_lock(&server->conn_list_lock);
    list_add_tail(&server->conn_list, &conn->server_node);
    server->conn_cnt++;
    pthread_spin_unlock(&server->conn_list_lock);
    return ret >= 0;
}

static int del_conn_from_server(connect_t * conn, server_t * server) {
    int ret = 0;
    if (server == NULL) {
        return 0;
    }

    pthread_spin_lock(&server->conn_list_lock);
    list_del(&conn->server_node);
    server->conn_cnt--;
    pthread_spin_unlock(&server->conn_list_lock);
    return ret >= 0;
}

//初始化server_t, 调用rdma_listen
int cbrdma_listen(const char * ip, uint16_t port, uint32_t recv_block_size, uint32_t recv_block_cnt, int mem_type, accept_conn_cb_t accept_cb, void* server_context, uint64_t *nd) {
    if (recv_block_size < CBRDMA_MIN_BLOCK_SIZE) {
        LOG(ERROR, "recv_block_size < %d", CBRDMA_MIN_BLOCK_SIZE);
        return 0;
    }

    if (recv_block_cnt < CBRDMA_MIN_BLOCK_CNT) {
        LOG(ERROR, "recv_block_cnt < %d", CBRDMA_MIN_BLOCK_CNT);
        return 0;
    }

    LOG(INFO, "cbrdma_listen(%s, %d, %d, %d, %d, accept_cb, %p, nd)", ip, port, recv_block_size, recv_block_cnt, mem_type, server_context);
    int ret = 0;
    server_t* server = (server_t*) cbrdma_malloc(sizeof(server_t));
    if (server == NULL) {
        LOG(ERROR, "create server failed: malloc failed\n");
        return 0;
    }

    server->nd = allocate_nd(CONN_SERVER_BIT);
    server->def_recv_block_size = recv_block_size;
    server->def_recv_block_cnt  = recv_block_cnt;
    server->mem_type = (uint8_t)mem_type;
    server->accept_cb = accept_cb;
    server->port = port;
    server->context = server_context;

    struct sockaddr_in server_sockaddr;
    bzero(&server_sockaddr, sizeof server_sockaddr);
    server_sockaddr.sin_family = AF_INET; /* standard IP NET address */
    if(!inet_pton(AF_INET, ip, &(server_sockaddr.sin_addr))) {
        LOG(ERROR, "Invalid IP:[%s] \n", ip);
        return 0;
    }
    //server_sockaddr.sin_addr.s_addr = htonl(INADDR_ANY); /* passed address */
    server_sockaddr.sin_port = htons(port);

    list_head_init(&server->server_node);

    ret = pthread_spin_init(&(server->conn_list_lock), PTHREAD_PROCESS_SHARED);
    if (ret != 0) {
        LOG(ERROR, "init server spin lock failed, err:%d", ret);
        goto err_free;
    }

    list_head_init(&server->conn_list);

    *nd = server->nd;
    ret = rdma_create_id(g_net_env->event_channel, &server->listen_id, server, RDMA_PS_TCP);
    if (ret != 0) {
        LOG(ERROR, "rdma create id failed, errno:%d", errno);
        goto err_free;
    }
    ret = rdma_bind_addr(server->listen_id, (struct sockaddr*)&server_sockaddr);
    if (ret != 0) {
        LOG(ERROR, "rdma bind failed, errno:%d", errno);
        goto err_free_id;
    }
    ret = rdma_listen(server->listen_id, 10);
    if (ret != 0) {
        LOG(ERROR, "rdma bind failed, errno:%d", errno);
        goto err_free_id;
    }

    add_server_to_env(server);
    return 1;

err_free_id:
    rdma_destroy_id(server->listen_id);
err_free:
    pthread_spin_destroy(&server->conn_list_lock);
    free(server);
    return 0;
}

static int close_server(uint64_t listen_nd) {
    LOG(INFO, "close_server:%ld", listen_nd);
    server_t * p = NULL, *n = NULL;
    pthread_spin_lock(&g_net_env->server_lock);
    list_for_each_safe(p, n, &g_net_env->server_list, server_node) {
        if (p->nd == listen_nd) {
            list_del(&p->server_node);
            break;
        }
    }
    pthread_spin_unlock(&g_net_env->server_lock);

    if (p == NULL) return 0;

    pthread_spin_destroy(&p->conn_list_lock);
    if (p->listen_id != 0) {
        rdma_destroy_id(p->listen_id);
    }
    cbrdma_free(p);
    return 1;
}

static int reg_connect_mem(connect_t *conn, uint32_t block_size, uint32_t block_cnt) {
    //init data buff
    int ret = 0;
    ret = conn_reg_data_buff(conn, block_size, block_cnt, conn->mem_type, conn->recv_buff);
    if (ret != 0) {
        LOG(ERROR, "client reg recv data failed, errno:%d", errno);
        return -1;
    }

    ret = reg_meta_data(conn, conn->send_meta);
    if (ret != 0) {
        LOG(ERROR, "client reg recv meta data failed, errno:%d", errno);
        return -1;
    }

    client_build_reg_recv_buff_cmd(conn);

    ret = reg_meta_data(conn, conn->recv_meta);
    if (ret != 0) {
        LOG(ERROR, "client reg send meta data failed, errno:%d", errno);
        return -1;
    }

    return 0;
}

static connect_t* init_connection(uint64_t nd, uint32_t recv_block_cnt) {
    int ret = 0;
    connect_t *conn = (connect_t*) cbrdma_malloc(sizeof(connect_t));
    if (conn == NULL) {
        LOG(ERROR, "create conn mem obj failed");
        return NULL;
    }
    LOG(INFO, "malloc connect_t:%p", conn);

    conn->nd = nd;
    conn->worker = get_worker_by_nd(conn->nd);
    conn->efd    = eventfd(0, 0);

    ret = pthread_spin_init(&(conn->spin_lock), PTHREAD_PROCESS_SHARED);
    if (ret != 0) {
        LOG(ERROR, "init conn spin lock failed, err:%d", ret);
        cbrdma_free(conn);
        return 0;
    }

    set_conn_state(conn, CONN_ST_CONNECTING);

    list_head_init(&conn->server_node);
    list_head_init(&conn->worker_node);
    list_head_init(&conn->close_node);

    list_head_init(&conn->recv_free_list);

    list_head_init(&conn->send_free_list);
    list_head_init(&conn->send_wait_free_list);

    conn->recv_meta = (buffer_t *)cbrdma_malloc(sizeof(buffer_t));
    if (conn->recv_meta == NULL) {
        LOG(ERROR, "no enouth memory\n");
        goto err;
    }
    LOG(INFO, "conn(%lu-%p) malloc recv_meta:%p", conn->nd, conn, conn->recv_meta);

    conn->send_meta = (buffer_t *)cbrdma_malloc(sizeof(buffer_t));
    if (conn->send_meta == NULL) {
        LOG(ERROR, "no enouth memory\n");
        goto err;
    }
    LOG(INFO, "conn(%lu-%p) malloc send_meta:%p", conn->nd, conn, conn->send_meta);

    conn->recv_buff = (buffer_t *)cbrdma_malloc(sizeof(buffer_t) * recv_block_cnt);
    if (conn->recv_buff == NULL) {
        LOG(ERROR, "no enouth memory\n");
        goto err;
    }
    LOG(INFO, "conn(%lu-%p) malloc recv_buff:%p, count:%u", conn->nd, conn, conn->recv_buff, recv_block_cnt);

    conn->recv_timeout_ns = 2 * ONE_SEC_IN_NS;
    conn->recv_cur = NULL;
    return conn;
err:
    release_buffer(conn);
    cbrdma_free(conn);
    return NULL;
}

int cbrdma_connect(const char* ip, uint16_t port, uint32_t recv_block_size, uint32_t recv_block_cnt, int mem_type, int64_t deadline, void *user_context, uint64_t *nd) {

    if (recv_block_size < CBRDMA_MIN_BLOCK_SIZE) {
        LOG(ERROR, "recv_block_size < %d", CBRDMA_MIN_BLOCK_SIZE);
        return 0;
    }

    if (recv_block_cnt < CBRDMA_MIN_BLOCK_CNT) {
        LOG(ERROR, "recv_block_cnt < %d", CBRDMA_MIN_BLOCK_CNT);
        return 0;
    }

    LOG(INFO, "cbrdma_connect(ip:%s, port:%d, blcok(size:%u, cnt:%u) memtype:%d, ctx:%p, *nd)", ip, port, recv_block_size, recv_block_cnt, mem_type, user_context);
    int ret = 0;
    struct sockaddr_in server_sockaddr;
    int64_t now   = 0;
    uint8_t state = 0;
    int64_t start = get_time_ns();
    uint64_t notify_value = 0;

    deadline = deadline * 1000;
    bzero(&server_sockaddr, sizeof server_sockaddr);
    server_sockaddr.sin_family = AF_INET; /* standard IP NET address */

    if(!inet_pton(AF_INET, ip, &(server_sockaddr.sin_addr))) {
        LOG(ERROR, "Invalid IP:[%s] \n", ip);
        return 0;
    }
    server_sockaddr.sin_port = htons(port);

    *nd = allocate_nd(CONN_ACTIVE_BIT);
    connect_t * conn = init_connection(*nd, recv_block_cnt);
    if (conn == NULL) {
        LOG(ERROR, "init_connection return null\n");
        return 0;
    }
    conn->mem_type = mem_type;
    conn->recv_block_size = recv_block_size;
    conn->recv_block_cnt = recv_block_cnt;
    ret = reg_connect_mem(conn, recv_block_size, recv_block_cnt);
    if (ret != 0) {
        LOG(ERROR, "rdma reg mem failed, err:%d", errno);
        goto err_free_resource;
    }

    ret = rdma_create_id(g_net_env->event_channel, &conn->id, (void*)(conn->nd), RDMA_PS_TCP);
    if (ret != 0) {
        LOG(ERROR, "rdma create id failed, err:%d", errno);
        goto err_free_resource;
    }
    LOG(INFO, "conn(%lu-%p) create cmid:%p", conn->nd, conn, conn->id);

    ret = rdma_resolve_addr(conn->id, NULL, (struct sockaddr*)&server_sockaddr, TIMEOUT_IN_MS);
    if (ret != 0) {
        LOG(ERROR, "rdma solve addr failed, err:%d", errno);
        goto err_free_id;
    }

    conn->context = user_context;
    add_conn_to_worker(conn, conn->worker, conn->worker->nd_map);

    while (1) {
        pthread_spin_lock(&conn->spin_lock);
        state = conn->state;
        pthread_spin_unlock(&conn->spin_lock);
        if (state == CONN_ST_CONNECTED) {
            break;
        }

        if (state == CONN_ST_CONNECTING) {
            now = get_time_ns();
            if ((now - start) > deadline) {
                goto err_timeout;
            }
            if (conn->efd > 0 && conn->worker->w_pid != pthread_self()) {
                read(conn->efd, &notify_value, 8);
            } else {
                usleep(10);
            }
            continue;
        }
        goto err_timeout;
    }

    return 1;

err_free_id:
    if (rdma_destroy_id(conn->id)) {
        LOG(ERROR, "Failed to destroy rdma id cleanly, %d \n", -errno);
    }
    LOG(INFO, "rdma_destroy_id(%p) for connect", conn->id);
err_free_resource:
    release_buffer(conn);
    cbrdma_free(conn);
    conn = NULL;
    return 0;

err_timeout:
    disconnect(*nd);
    return 0;
}

static void build_qp_attr(struct ibv_cq *cq, struct ibv_qp_init_attr *qp_attr) {
    memset(qp_attr, 0, sizeof(*qp_attr));

    qp_attr->send_cq = cq;
    qp_attr->recv_cq = cq;
    qp_attr->qp_type = IBV_QPT_RC;

    qp_attr->cap.max_send_wr = WQ_DEPTH;
    qp_attr->cap.max_recv_wr = WQ_DEPTH;
    qp_attr->cap.max_send_sge = WQ_SG_DEPTH;
    qp_attr->cap.max_recv_sge = WQ_SG_DEPTH;
}


static void on_addr_resolved(struct rdma_cm_id *id) {
    LOG(INFO, "on_addr_resolved:%p", id);
    int ret = 0;
    connect_t *conn = NULL;
    worker_t  *worker = NULL;
    _get_worker_and_connect_by_nd((uintptr_t) id->context, &worker, &conn, GET_CONN_WIT_REF);
    if (conn == NULL)  {
        //already closed
        return;
    }

    ret = rdma_resolve_route(id, TIMEOUT_IN_MS);
    if (ret != 0) {
        LOG(ERROR, "conn(%lu-%p) resolve failed, errno:%d, call on_disconnected(%p)", conn->nd, conn, errno, conn->id);
        disconnect(conn->nd);
        conn_del_ref(conn);
        return;
    }

    LOG(INFO, "conn(%lu-%p) addr resolved", conn->nd, conn);
    conn_del_ref(conn);
    return;
}

static void on_route_resolved(struct rdma_cm_id *conn_id) {
    LOG(INFO, "on_route_resolved:%p", conn_id);
    connect_t *conn = NULL;
    worker_t  *worker = NULL;
    _get_worker_and_connect_by_nd((uintptr_t) conn_id->context, &worker, &conn, GET_CONN_WIT_REF);
    if (conn == NULL)  {
        //already closed
        return;
    }

    struct ibv_qp_init_attr qp_attr;
    build_qp_attr(conn->worker->cq, &qp_attr);

    int ret = rdma_create_qp(conn->id, conn->worker->pd, &qp_attr);
    if (ret != 0) {
        LOG(ERROR, "rdma rdma create qp failed, err:%d, call on_disconnected(%p)", errno, conn->id);
        disconnect(conn->nd);
        conn_del_ref(conn);
        return;
    }
    conn->qp = conn->id->qp;
    LOG(INFO, "rdma_create_qp:%p", conn->qp);

    pthread_spin_lock(&conn->spin_lock);
    post_recv_meta(conn);
    pthread_spin_unlock(&conn->spin_lock);

    struct rdma_conn_param cm_params;
    memset(&cm_params, 0, sizeof(cm_params));
    ret = rdma_connect(conn_id, &cm_params);
    if (ret) {
        LOG(INFO, "Failed to connect to remote host , errno: %d, call on_disconnected(%p)", -errno, conn->id);
        disconnect(conn->nd);
    }
    LOG(INFO, "conn(%lu-%p) rdma connect, cmid:%p", conn->nd, conn, conn_id);
    conn_del_ref(conn);
}

static void on_connected(struct rdma_cm_id *id) {
    connect_t *conn = NULL;
    worker_t  *worker = NULL;
    _get_worker_and_connect_by_nd((uintptr_t) id->context, &worker, &conn, GET_CONN_WIT_REF);
    if (conn == NULL)  {
        //already closed
        return;
    }

    pthread_spin_lock(&conn->spin_lock);
    if (conn->state >= CONN_ST_ERROR) {
        pthread_spin_unlock(&conn->spin_lock);

        LOG(ERROR, "conn(%lu-%p) on_connected, already closed, do nothing", conn->nd, conn);
        conn_del_ref(conn);
        return;
    }
    post_send_meta(conn);
    pthread_spin_unlock(&conn->spin_lock);

    LOG(INFO, "conn(%lu-%p) on_connected; conn finished", conn->nd, conn);
    conn_del_ref(conn);
    return;
}

static void on_disconnected(struct rdma_cm_id* id) {
    connect_t *conn = NULL;
    worker_t  *worker = NULL;
    server_t  *server = NULL;
    int is_onclose = 0;

    _get_worker_and_connect_by_nd((uintptr_t) id->context, &worker, &conn, GET_CONN_WIT_REF);
    if (conn == NULL)  {
        //already closed
        return;
    }

    LOG(INFO, "conn(%lu-%p) proccess disconnected event, close begin", conn->nd, conn);
    pthread_spin_lock(&worker->lock);
    pthread_spin_lock(&conn->spin_lock);

    if (conn->is_app_closed == 0) {
        is_onclose = 1;
    }

    if (conn->close_start == 0) {
        conn->close_start = get_time_ns();
    }

    server = conn->server;
    conn->server = NULL;

    if (list_is_empty(&conn->close_node)) {
        list_add_tail(&worker->close_list, &conn->close_node);
        LOG(INFO, "conn(%lu-%p) add to close_task_list", conn->nd, conn);
    }

    set_conn_state(conn, CONN_ST_DISCONNECTED);
    pthread_spin_unlock(&conn->spin_lock);
    pthread_spin_unlock(&worker->lock);

    del_conn_from_server(conn, server);

    if (is_onclose) {
        conn_notify_disconnect(conn);
    }

    conn_del_ref(conn);
}

void on_accept(struct rdma_cm_id* listen_id, struct rdma_cm_id* id) {
    LOG(INFO, "on_accept:%p/%p", listen_id, id);
    int    ret = 0;
    server_t * server = listen_id->context;
    uint64_t accept_nd = allocate_nd(0);//server passive connection
    connect_t * conn = init_connection(accept_nd, server->def_recv_block_cnt);
    if (conn == NULL) {
        LOG(ERROR, "init_connection return null");
        rdma_reject(id, NULL, 0);
    }
    conn->mem_type = server->mem_type;
    conn->recv_block_size = server->def_recv_block_size;
    conn->recv_block_cnt  = server->def_recv_block_cnt;

    ret = reg_connect_mem(conn, server->def_recv_block_size, server->def_recv_block_cnt);
    if (ret != 0) {
        LOG(ERROR, "rdma reg mem failed, err:%d", errno);
        rdma_reject(id, NULL, 0);
        goto err;
    }

    struct ibv_qp_init_attr qp_attr;
    build_qp_attr(conn->worker->cq, &qp_attr);
    ret = rdma_create_qp(id, conn->worker->pd, &qp_attr);
    if (ret != 0) {
        LOG(ERROR, "conn(%lu-%p) create qp failed, errno:%d", conn->nd, conn, errno);
        rdma_reject(id, NULL, 0);
        goto err;
    }

    id->context = (void*)conn->nd;
    conn->qp = id->qp;
    LOG(INFO, "conn(%lu-%p) rdma_create_qp:%p", conn->nd, conn, conn->qp);
    post_recv_meta(conn);
    LOG(INFO, "conn(%lu-%p) post recv", conn->nd, conn);

    struct rdma_conn_param  cm_params;
    memset(&cm_params, 0, sizeof(cm_params));
    ret = rdma_accept(id, &cm_params);
    if (ret != 0) {
        LOG(ERROR, "accept conn:%p, rdma accept failed, errno:%d", conn, errno);
        rdma_reject(id, NULL, 0);
        goto err1;
    }
    LOG(INFO, "conn(%lu-%p) rdma_accept cmid:%p", conn->nd, conn, id);

    add_conn_to_server(conn, server);
    add_conn_to_worker(conn, conn->worker, conn->worker->nd_map);
    conn->id = id;
    return;
err1:
    rdma_destroy_qp(id);
    LOG(INFO, "rdma_destroy_qp:%p", conn->id);
err:
    release_buffer(conn);
    cbrdma_free(conn);
    return;
}

void cbrdma_set_user_context(uint64_t nd, void * user_context) {
    LOG(INFO, "cbrdma_set_user_context(%lu, %p)", nd, user_context);
    worker_t *worker = NULL;
    connect_t * conn = NULL;
    get_worker_and_connect_by_nd(nd, &worker, &conn, GET_CONN_WIT_REF);
    if (conn == NULL) {
        return;
    }
    conn->context = user_context;
    conn_del_ref(conn);
    return;
}

void cbrdma_set_send_timeout_us(uint64_t nd, int64_t timeout_us) {
    LOG(INFO, "cbrdma_set_send_timeout_us(%lu, %ld)", nd, timeout_us);
    worker_t *worker = NULL;
    connect_t * conn = NULL;
    get_worker_and_connect_by_nd(nd, &worker, &conn, GET_CONN_WIT_REF);
    if (conn == NULL) {
        return;
    }
    pthread_spin_lock(&conn->spin_lock);
    if (timeout_us > 0) {
        conn->send_timeout_ns = timeout_us * 1000;
    } else {
        conn->send_timeout_ns = -1;
    }

    pthread_spin_unlock(&conn->spin_lock);
    conn_del_ref(conn);
    return;
}

void cbrdma_set_recv_timeout_us(uint64_t nd, int64_t timeout_us) {
    LOG(INFO, "cbrdma_set_recv_timeout_us(%lu, %ld)", nd, timeout_us);
    worker_t *worker = NULL;
    connect_t * conn = NULL;
    get_worker_and_connect_by_nd(nd, &worker, &conn, GET_CONN_WIT_REF);
    if (conn == NULL) {
        return;
    }
    pthread_spin_lock(&conn->spin_lock);
    if (timeout_us > 0) {
        conn->recv_timeout_ns = timeout_us * 1000;
    } else {
        conn->recv_timeout_ns = -1;
    }

    pthread_spin_unlock(&conn->spin_lock);
    conn_del_ref(conn);
    return;
}

static void get_addr_by_scokaddr(struct sockaddr* addr, char* str_addr, int len) {
    struct sockaddr_in* addr_in = NULL;
    char ip[INET6_ADDRSTRLEN] = {0};
    struct sockaddr_in6* addr_in6 = NULL;
    if (addr == NULL) {
        return;
    }

    switch (addr->sa_family) {
        case AF_INET6:
            addr_in6 = (struct sockaddr_in6*)addr;
            inet_ntop(AF_INET6, &(addr_in6->sin6_addr), ip, INET6_ADDRSTRLEN);
            snprintf(str_addr, len - 1, "%s:%d", ip,  ntohs(addr_in6->sin6_port));
            break;
        case AF_INET:
            addr_in = (struct sockaddr_in*)addr;
            snprintf(str_addr, len - 1, "%s:%d", inet_ntoa(addr_in->sin_addr),  ntohs(addr_in->sin_port));
            break;
        default:
            LOG(ERROR, "unkonwn addr type:%p, %d", addr, addr->sa_family);
            break;
    }

    return;
}

void cbrdma_get_src_addr(uint64_t nd, char* src_addr, int len) {
    LOG(INFO, "cbrdma_get_src_addr(%lu, %p, %d)", nd, src_addr, len);
    worker_t *worker = NULL;
    connect_t * conn = NULL;
    struct sockaddr * addr = NULL;

    get_worker_and_connect_by_nd(nd, &worker, &conn, GET_CONN_WIT_REF);
    if (conn == NULL) {
        return;
    }

    addr = rdma_get_local_addr(conn->id);
    if (addr != NULL) {

    }

    get_addr_by_scokaddr(addr, src_addr, len);
    conn_del_ref(conn);
    return;
}

void cbrdma_get_dst_addr(uint64_t nd, char* dst_addr, int len) {
    LOG(INFO, "cbrdma_get_dst_addr(%lu, %p, %d)", nd, dst_addr, len);
    worker_t *worker = NULL;
    connect_t * conn = NULL;
    struct sockaddr * addr = NULL;

    get_worker_and_connect_by_nd(nd, &worker, &conn, GET_CONN_WIT_REF);
    if (conn == NULL) {
        return;
    }

    addr = rdma_get_peer_addr(conn->id);
    if (addr != NULL) {

    }

    get_addr_by_scokaddr(addr, dst_addr, len);
    conn_del_ref(conn);
    return;
}

void cbrdma_set_log_level(int level) {
    g_net_env->log_level = level;
}

//close a connection or a server
void cbrdma_close(uint64_t nd) {
    LOG(INFO, "cbrdma_close:%ld", nd);
    int id = 0, worker_id = 0, is_server = 0, is_active = 0;
    cbrdma_parse_nd(nd, &id, &worker_id, &is_server, &is_active);
    if (is_server) {
        close_server(nd);
    } else {
        disconnect(nd);
    }
}

void cbrdma_net_monitor(cbrdma_metrics_t *m) {
    memset(m, 0, sizeof(cbrdma_metrics_t));
    m->server_cnt = g_net_env->server_cnt;
    m->worker_cnt = g_net_env->worker_num;

    //no need lock
    for (int i = 0; i < g_net_env->worker_num; ++i) {
        m->qp_cnt += g_net_env->worker[i].qp_cnt;
    }

    server_t * p = NULL, *n = NULL;
    pthread_spin_lock(&g_net_env->server_lock);
    list_for_each_safe(p, n, &g_net_env->server_list, server_node) {
        m->qp_cnt += p->conn_cnt;
    }
    pthread_spin_unlock(&g_net_env->server_lock);
}

void cbrdma_get_conn_counter(uint64_t nd, conn_counter_t *info) {
    worker_t * worker = NULL;//get_worker_by_nd(nd);
    connect_t * conn  = NULL;//get_connect_by_nd(nd);
    get_worker_and_connect_by_nd(nd, &worker, &conn, GET_CONN_WIT_REF);
    if (conn == NULL) return;

    pthread_spin_lock(&conn->spin_lock);
    info->send_post_cnt = conn->post_send_cnt;
    info->send_ack_cnt  = conn->send_ack_cnt;
    info->send_cb_cnt   = conn->send_cb_cnt;
    info->send_win_size = conn->send_win_size;

    info->recv_cnt      = conn->recv_cnt;
    info->recv_ack_cnt  = conn->recv_ack_cnt;
    info->recv_win_size = conn->recv_win_size;

    info->peer_ack_cnt   = conn->peer_ack_cnt;
    info->peer_send_size = conn->peer_send_wind_size;
    pthread_spin_unlock(&conn->spin_lock);

    conn_del_ref(conn);
    return;
}

