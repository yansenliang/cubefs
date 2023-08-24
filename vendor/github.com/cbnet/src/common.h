#ifndef _COMMON_H_
#define _COMMON_H_
#include <stdint.h>
#include <assert.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include "list.h"
#include "hashmap.h"
#include "cbrdma.h"

#ifdef CBRDMA_WITH_DEBUG
#define _LIST_CHECK_
#endif

#define CONN_ID_BIT_LEN         32
#define WORKER_ID_BIT_LEN       8
#define CONN_TYPE_BIT_LEN       8
#define CONN_ID_MASK_BIT_LEN    8

#define IP_STRING_LEN           16
#define DEV_NAME_LEN            64
#define CQ_CNT_PER_POLL         16
#define GET_CONN_WIT_REF        1

#define COND_TIMEOUT_NS             500 * 1000 * 1000           //500ms
#define ONE_SEC_IN_NS               1000000000L
#define CLOSE_TIME_OUT              10 * ONE_SEC_IN_NS

#define set_conn_state(conn, t) do {\
        uint8_t old = conn->state;                                        \
        conn->state = t;                                                  \
        LOG(INFO, "conn[%d] state: %s-->%s", conn->nd, conn_state_names[old], conn_state_names[t]);\
    }while(0)

#define conn_add_ref(conn) do {\
        pthread_spin_lock(&conn->spin_lock);    \
        if (conn->ref > 0) {                    \
            conn->ref++;                        \
        };                                      \
        pthread_spin_unlock(&conn->spin_lock);    \
    }while(0)

#define conn_del_ref(conn) do {\
        pthread_spin_lock(&conn->spin_lock);    \
        if (conn->ref > 0) {                    \
            conn->ref--;                        \
        };                                      \
        pthread_spin_unlock(&conn->spin_lock);    \
    }while(0)


typedef enum conn_state_enum_t {
    CONN_ST_INIT,
    CONN_ST_CONNECTING, //执行rdma连接建立，元数据交换
    CONN_ST_CONNECTED,  //元数据交换完成
    CONN_ST_ERROR,      //主动改变发送buffer大小，元数据交换
    CONN_ST_CLOSING,    //连接已建立，开始执行关闭
    CONN_ST_DISCONNECTED, //连接已彻底断开
    CONN_ST_CLOSED,       //连接资源已释放完毕
    CONN_ST_MAX
} conn_state_t;

typedef enum buff_state_enum_t {
    BUFF_ST_FREE,
    BUFF_ST_APP_INUSE, 
    BUFF_ST_POLLING_INUSE,
    BUFF_ST_MAX
} buff_state_t;

extern const char *conn_state_names[CONN_ST_MAX];

typedef enum conn_ctl_cmd_enum_t {
    CMD_REG_RECV_BUFF,
    CMD_EXCHANGE_RECV_BUFF_REQ,
    CMD_EXCHANGE_RECV_BUFF_RSP
} ctl_cmd_t;

typedef enum buff_type_bit_mask {
    SEND_LAST_BLOCK_BIT = 1 << 7,
    SEND_TYPE_BIT = 1 << 4,
} buff_type_mask;

typedef enum conn_type_bit_mask {
    CONN_SERVER_BIT = 1 << 7,
    CONN_ACTIVE_BIT = 1 << 6,
} conn_type_mask;

typedef enum id_gen_enum {
    ID_GEN_CTRL,
    ID_GEN_DATA,
    ID_GEN_MAX
} IdGenEnum;

struct conn_nd_t {
    uint64_t id:CONN_ID_BIT_LEN;                //32
    uint64_t worker_id:WORKER_ID_BIT_LEN;       //8
    uint64_t type:CONN_TYPE_BIT_LEN;            //8 RDMA/TCP Server/Conn ACtive/Passive
    uint64_t m2:CONN_ID_MASK_BIT_LEN;           //8 b
    uint64_t m1:CONN_ID_MASK_BIT_LEN;           //8 c
};

union conn_nd_union {
    struct conn_nd_t nd_;
    uint64_t nd;
};

typedef struct worker_st {
    struct ibv_pd      *pd;
    struct ibv_cq      *cq;

    struct ibv_comp_channel   *comp_channel;        // with cq
    pthread_t  cq_poller_thread;

    pthread_spinlock_t nd_map_lock;
    khash_t(map)       *nd_map;
    khash_t(map)       *closing_nd_map;

    //send task list
    pthread_spinlock_t lock;
    list_link_t        conn_list;
    list_link_t        close_list;

    uint8_t          id;
    uint32_t         qp_cnt;
    pthread_t        w_pid;

    uint64_t         run_cycle;
    int64_t          last_active_time;
} worker_t;

typedef struct net_env_st {
    uint8_t             worker_num;
    uint8_t             log_level;
    int8_t              pad[6];

    struct ibv_context  **all_devs;
    struct ibv_context  *ctx;

    struct rdma_event_channel *event_channel;
    pthread_t                 event_loop_thread;

    uint32_t            server_cnt;
    int32_t             ib_dev_cnt;
    pthread_spinlock_t  server_lock;
    list_link_t         server_list;
    uint32_t            id_gen[ID_GEN_MAX];
    worker_t            worker[];
} net_env_t;

typedef struct server_context_t {
    uint64_t nd;
    uint32_t def_recv_block_size;
    uint32_t def_recv_block_cnt;
    uint32_t conn_cnt;
    int16_t  port;
    uint8_t  mem_type;
    uint8_t  pad[5];

    list_link_t        server_node;
    pthread_spinlock_t conn_list_lock;
    list_link_t        conn_list;
    struct rdma_cm_id  *listen_id;
    worker_t           *worker;
    void               *context;
    accept_conn_cb_t   accept_cb;
} server_t;

//exchange rdma addr to peer
typedef struct meta_message_st {
    uint32_t id;
    uint32_t cmd;
    uint32_t size;
    uint32_t count;
    uint32_t rkey[MAX_PORT_DEPTH_SIZE];
    uint64_t addr[MAX_PORT_DEPTH_SIZE];
} meta_t;

typedef struct block_header_st {
    uint32_t magic;
    uint32_t seq;  //send cnt
    uint32_t len;
    uint16_t send_wind_size;
    uint16_t recv_ack_cnt;
    uint16_t block_cnt;
    uint16_t block_start;           //接收后更改成本端的recv index
    uint32_t magic2;
} block_header_t;

typedef block_header_t   block_tail_t;

//buff header + data    PACKAGE_LEN + DATA_LEN
typedef struct buffer_st {
    list_link_t   node;
    int32_t       index;

    uint32_t       peer_rkey;
    uint64_t       peer_addr;
    struct ibv_mr *mr;
    void          *context;

    uint8_t         type;           // 最后一个block设置为user 信息，oncom 需要处理
    uint8_t         worker_id;
    uint8_t         state;            // buff，返回app值1， release 值0， 扫描lian
    uint8_t         pad;
    uint32_t        send_len;
    void           *data;           // header+body+tail
} buffer_t;

typedef struct conn_context_t {
    uint64_t        nd;
    void*           context;

    struct rdma_cm_id *id;
    struct ibv_qp     *qp;
    worker_t          *worker;
    server_t          *server;

    list_link_t   worker_node;
    list_link_t   server_node;
    list_link_t   close_node;

    /*连接完毕后释放*/
    buffer_t      *recv_meta;
    buffer_t      *send_meta;

    pthread_spinlock_t spin_lock;
    int             efd;
    int64_t         last_notify;
    uint32_t        ref;
    uint32_t        buff_ref;
    uint8_t         mem_type;
    uint8_t         state;                   //INIT   CONNECTING   CONNECTED   CLOSING   CLOSED  内部状态（ EXCHANGE ) //TODO 状态改变如何保护
    int8_t          is_app_closed;
    uint8_t         pad[5];



    buffer_t      *recv_buff;             //MAX SIZE define by meta buffer size
    buffer_t      *recv_cur;
    uint32_t      recv_block_size;
    uint32_t      recv_block_cnt;
    list_link_t   recv_free_list;
    uint16_t      recv_win_size;
    uint16_t      peer_send_wind_size;
    uint16_t      recv_head_index;          //free list游标检测， 出队
    uint16_t      recv_tail_index;          //入队，控制wait_list是否可以入队

    int64_t       recv_time;                 //free list 出队计时，数据达到截止
    int64_t       recv_timeout_ns;
    uint64_t      recv_cnt;
    uint64_t      recv_ack_cnt;              //需要返回给对端

    buffer_t      *send_buff;               //MAX SIZE define by meta buffer size
    buffer_t      *auto_ack;                //防止wq写满，每次只允许一个auto ack
    uint64_t      last_auto_seq;            // 防止过多auto ack， 至少等待1/2 recv 窗口
    int64_t       last_req_time;
    uint32_t      send_block_size;
    uint32_t      send_block_cnt;
    list_link_t   send_free_list;
    list_link_t   send_wait_free_list;
    uint16_t      send_win_size;
    uint16_t      send_head_index;          //free list 游标检测，出队
    uint16_t      send_tail_index;          //入队
    uint8_t       pad2[2];
    int64_t       send_time;               //内部消息不进行更新， free list出队计时， cq消息回复截止
    uint16_t      peer_ack_cnt;

    uint64_t      send_cnt;               //出队free list时++，填充到header seq
    uint64_t      post_send_cnt;
    uint64_t      send_ack_cnt;
    uint64_t      send_cb_cnt;

    int64_t       close_start;
} connect_t;

void (*log_handler_cb) (int level, char* line, int len);

#define LOG(level, fmt, ...)  deal_log(level, __FUNCTION__, __LINE__, fmt,  ##__VA_ARGS__)

#define LOG_WITH_LIMIT(_level, _now, _time, _log_count, _id, _fmt, ...) \
    static int64_t log_dur_##_id = 0; \
    do { \
        if (_now - log_dur_##_id >  ((ONE_SEC_IN_NS * _time) / _log_count)) { \
            log_dur_##_id = _now;\
            LOG(_level, _fmt, ## __VA_ARGS__); \
        }\
    } while (0)


int64_t get_time_ns();
void deal_log(int level, const char* func, int line, const char* fmt, ...);
int get_rdma_dev_name_by_ip(char* local_ip, char rdma_name[], int len);

uint64_t allocate_nd(int type);
worker_t*  get_worker_by_nd(uint64_t nd);
int add_conn_to_worker(connect_t * conn, worker_t * worker, khash_t(map) *hmap);
int del_conn_from_worker(uint64_t nd, worker_t * worker, khash_t(map) *hmap);
void get_worker_and_connect_by_nd(uint64_t nd, worker_t ** worker, connect_t** conn, int8_t with_ref);
void _get_worker_and_connect_by_nd(uint64_t nd, worker_t** worker, connect_t** conn, int8_t with_ref);

void conn_notify_disconnect(connect_t *conn);
void conn_notify_error(connect_t *conn);
void conn_notify_closed(connect_t *conn);

int reg_meta_data(connect_t *conn, buffer_t* meta_ptr);
int conn_reg_data_buff(connect_t *conn, uint32_t blcok_size, uint32_t block_cnt, int mem_type, buffer_t * buff);
int conn_bind_remote_recv_key(connect_t *conn, meta_t* rmeta);
void release_rdma(connect_t * conn);
void release_buffer(connect_t * conn);

int conn_close(worker_t* worker, connect_t* conn);
int disconnect(uint64_t nd);

void client_build_reg_recv_buff_cmd(connect_t* conn);
int post_recv_meta(connect_t *conn);
int post_send_meta(connect_t *conn);
int post_send_data(connect_t *conn, buffer_t *buff, uint32_t len);

#endif
