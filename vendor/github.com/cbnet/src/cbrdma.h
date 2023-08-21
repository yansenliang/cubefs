#ifndef _CBRDMA_LIB_H_
#define _CBRDMA_LIB_H_

#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

    #define MAX_IP_LEN                        128
    #define MAX_PORT_DEPTH_SIZE               0xFFFF

    typedef int (*accept_conn_cb_t)(uint64_t server, uint64_t accept_nd, void * user_context);
    typedef void (*on_disconnected_cb_t)(uint64_t nd, void * context);
    typedef void (*on_error_cb_t)(uint64_t nd, void * context);
    typedef void (*on_closed_cb_t)(uint64_t nd, void * context);
    typedef void (*log_handler_cb_t) (int level, char* line, int len);

    /* change with op_names */
    typedef enum cbrdma_op_type {
        CBRDMA_OP_SEND,               //
        CBRDMA_OP_RECV,               //
        CBRDMA_OPTYPE_MAX          // 哨兵
    } cbrdma_op_type_t;

    typedef enum cbrdma_mem_type {
        CBRDMA_MEMORY,
        CBRDMA_GPU_MEM,
        CBRDMA_MEMTYPE_MAX          // 哨兵
    } cbrdma_mem_type_t;

    typedef enum log_level_enum_t {
        TRACE,
        DEBUG,
        INFO,
        WARN,
        ERROR,
        FATAL
    } log_level_t;

    typedef struct netlib_metrics {
        uint64_t ts;
        uint32_t server_cnt;
        uint32_t worker_cnt;
        uint32_t qp_cnt;
    } cbrdma_metrics_t;

    typedef struct cbrdma_config {
        int8_t   numa_node;
        uint8_t   worker_num;
        uint8_t   max_msg_cnt_per_poll;
        uint8_t   log_level;
        int8_t   pad;
        uint32_t conn_timeout_ms;
        char     str_local_ip[MAX_IP_LEN];

        on_disconnected_cb_t    on_disconnected_func;
        on_error_cb_t on_error_func;
        on_closed_cb_t on_closed_func;
        log_handler_cb_t log_handler_func;
    } cbrdma_config_t;

    typedef struct complete_msg {
        uint64_t      nd;               // net 维护的描述符, 指针类型，可以强转成ServerWorrkInfo或connect_nd_t，通过ndtype区分
        void*         user_context;
        void*         buff;
        uint32_t      length;         //发送长度//接收长度//元数据大小
        int32_t       status;           // op 操作结果 0:成功, negative: 失败 参考 ucs_status_t, other: 参考 net_status_t
        int8_t        op;               // 操作码
        uint8_t       pad[7];
    } complete_msg_t;

    typedef struct connect_counter_st {
        uint64_t recv_cnt;
        uint64_t recv_ack_cnt;

        uint64_t send_post_cnt;
        uint64_t send_ack_cnt;
        uint64_t send_cb_cnt;

        uint16_t send_win_size;
        uint16_t recv_win_size;
        uint16_t peer_send_size;
        uint16_t peer_ack_cnt;
    }conn_counter_t;

    void cbrdma_init_config(cbrdma_config_t *config);

    int cbrdma_init(cbrdma_config_t *config);

    void cbrdma_destroy();


    int cbrdma_listen(const char * ip, uint16_t port, uint32_t recv_block_size, uint32_t recv_block_cnt, int mem_type, accept_conn_cb_t cb, void* server_context, uint64_t *nd);

    int cbrdma_connect(const char* ip, uint16_t port, uint32_t recv_block_size, uint32_t recv_block_cnt, int mem_type, int64_t timeout_us, void *user_context, uint64_t *nd);

    //0 : timeout retry;
    //<0: err occur
    //>0: data size
    void* cbrdma_get_send_buff(uint64_t nd, uint32_t size, int64_t timeout_us, int32_t *ret_size);

    int cbrdma_send(uint64_t nd, void* buff, uint32_t len);

    int cbrdma_release_recv_buff(uint64_t nd, void *data_ptr);

    void cbrdma_close(uint64_t nd);

    void cbrdma_set_log_level(int level);

    //在用户服务端接受连接时，将用户上下文跟nd关联起来
    void cbrdma_set_user_context(uint64_t nd, void * user_context);

    //return msg numer
    int cbrdma_worker_poll(uint32_t worker_id, complete_msg_t* msgs, int msg_len);

    void net_monitor(cbrdma_metrics_t *m);

    void parse_nd(uint64_t nd, int *id, int * worker_id, int * is_server, int * is_active);

    const char *net_status_string(int16_t status);

    void get_conn_counter(uint64_t nd, conn_counter_t* counter);

    int64_t get_time_ns();

#ifdef __cplusplus
}
#endif

#endif
