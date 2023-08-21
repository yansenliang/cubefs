#include <iostream>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <thread>
#include <mutex>
#include "cbrdma.h"

using namespace std;

DEFINE_string(t, "server", "server or client");
DEFINE_int32(clinum, 1, "client num");
DEFINE_int32(numa, 0, "numa_node");
DEFINE_uint32(worker, 1, "worker num");
DEFINE_string(local_ip, "", "local_ip used to bind rdma_dev");
DEFINE_string(ip, "", "ip");
DEFINE_uint32(port, 1233, "port");
DEFINE_uint32(poll_msgs, 16, "max_msg_cnt_per_poll");
DEFINE_uint32(msg_size, 16, "pingpong msg_size");
DEFINE_uint32(loop_count, -1, "pingpong count");

typedef struct server_context {
    int running;
    uint64_t nd;
    uint32_t accept_conn_cnt;
    uint32_t send_msg_size;
} server_context_t;

typedef struct client_context {
    int running;
    uint32_t send_msg_size;
    uint32_t recv_count;
    uint32_t send_count;
    void * recv_buff;
    void * send_buff;
    uint32_t recv_buff_size;
    uint32_t send_buff_size;
    server_context_t * srv;
} client_context_t;

int server_on_send(uint64_t nd, uint32_t send_size, int status, void * user_context) {
    client_context_t * ctx = (client_context_t *)user_context;
    if(status <= 0) {
        LOG(ERROR) << "on_send status:" << status;
        CHECK(cbrdma_close(nd)) << "cbrdma_close failed";
        ctx->running = 0;
        return 0;
    }

    ctx->send_count++;

    CHECK(ctx->send_buff);
    memset(ctx->send_buff, 0, send_size);

    if (cbrdma_send(nd, ctx->send_msg_size) <= 0) {
        LOG(ERROR) << "cbrdma_send failed";
        ctx->running = 0;
        return 0;
    }

    return 1;
}


int client_on_send(uint64_t nd, uint32_t send_size, int status, void * user_context) {
    client_context_t * ctx = (client_context_t *)user_context;
    if(status <= 0 || ctx->send_count > 20000) {
        LOG(ERROR) << "on_send status:" << status;
        CHECK(cbrdma_close(nd)) << "cbrdma_close failed";
        ctx->running = 0;
        return 0;
    }

    ctx->send_count++;

    CHECK(ctx->send_buff);
    memset(ctx->send_buff, 0, send_size);

    if (cbrdma_send(nd, ctx->send_msg_size) <= 0) {
        LOG(ERROR) << "cbrdma_send failed";
        ctx->running = 0;
        return 0;
    }

    if (ctx->send_count == 100000) {
        ctx->send_msg_size = 128;

        if (cbrdma_chg_send_buff_size(nd, 128)) {
            LOG(ERROR) << "cbrdma_reg_send_buff(" << nd << "," << ctx->send_msg_size << ") failed";
            return 0;
        }
    }

    return 1;
}

int on_send(uint64_t nd, uint32_t send_size, int status, void * user_context) {
    int id, worker_id, is_server, is_active;
    parse_nd(nd, &id, &worker_id, &is_server, &is_active);
    if (is_active) {
        return client_on_send(nd, send_size, status, user_context);
    } else {
        return server_on_send(nd, send_size, status, user_context);
    }
}

int on_recv(uint64_t nd, void* recv_buff, uint32_t recv_size, int status, void * user_context) {
    client_context_t * ctx = (client_context_t *)user_context;
    if(status <= 0) {
        LOG(ERROR) << "on_recv status:" << status;
        ctx->running = 0;
        return 0;
    }

    ctx->recv_count++;

    cbrdma_prepare_recv(nd);

    int id, worker_id, is_server, is_active;
    parse_nd(nd, &id, &worker_id, &is_server, &is_active);
    if (!is_active && ctx->recv_count == 1) {
        CHECK(ctx->send_buff);
        memcpy(ctx->send_buff, recv_buff, recv_size);

        if (cbrdma_send(nd, ctx->send_msg_size) <= 0) {
            LOG(ERROR) << "cbrdma_send failed";
            ctx->running = 0;
            return 0;
        }
    }

    return 1;
}

int accept_cb(uint64_t server_nd, uint64_t accept_nd, void * server_context) {
    server_context_t * srv_ctx = (server_context_t *)server_context;
    srv_ctx->accept_conn_cnt++;

    client_context_t * ctx = (client_context_t *)malloc(sizeof(client_context_t));
    memset(ctx, 0, sizeof(client_context_t));
    ctx->running = 1;
    ctx->send_msg_size = srv_ctx->send_msg_size;

    ctx->send_buff = cbrdma_get_send_buff(accept_nd, &ctx->send_buff_size);
    if (ctx->send_buff < 0) {
        printf("cbrdma_get_send_buff failed\n");
        return 0;
    }

    if(ctx->send_msg_size > ctx->send_buff_size) {
        if (!cbrdma_chg_send_buff_size(accept_nd, ctx->send_msg_size)) {
            LOG(ERROR) << "cbrdma_chg_send_buff_size(" << accept_nd << "," << ctx->send_msg_size << ") failed";
            return 0;
        }

        ctx->send_buff = cbrdma_get_send_buff(accept_nd, &ctx->send_buff_size);
        if (ctx->send_buff < 0) {
            printf("cbrdma_get_send_buff failed\n");
            return 0;
        }
    }

    cbrdma_set_user_context(accept_nd, ctx);

    return 1;
}

void start_ppserver() {
    server_context_t ctx = {1, 0, 0, FLAGS_msg_size};
    int ret = 0;
    char ip[MAX_IP_LEN];
    memset(ip, 0, MAX_IP_LEN);
    if (FLAGS_ip.empty()) {
        memcpy(ip, FLAGS_local_ip.c_str(), strlen(FLAGS_local_ip.c_str()));
    } else {
        memcpy(ip, FLAGS_ip.c_str(), strlen(FLAGS_ip.c_str()));
    }
    ret = cbrdma_listen(ip, FLAGS_port, 16, 0, accept_cb, &ctx, &ctx.nd);
    if (ret <= 0) {
        LOG(ERROR) << "cbrdma_listen(" << FLAGS_ip << "," << FLAGS_port << ") failed";
        return;
    }

    while(ctx.running) {
        LOG(INFO) << "accept_conn_cnt:" << ctx.accept_conn_cnt;
        this_thread::sleep_for(chrono::seconds(1));
    }
}

void start_ppclient(int index) {
    LOG(INFO) << "start client:" << index ;

    client_context_t * ctx = (client_context_t *)malloc(sizeof(client_context_t));
    memset(ctx, 0, sizeof(client_context_t));
    ctx->running = 1;
    ctx->send_msg_size = FLAGS_msg_size;

    int ret = 0;
    uint64_t nd = 0;
    ret = cbrdma_connect(FLAGS_ip.c_str(), FLAGS_port, FLAGS_msg_size, 0, ctx, &nd);
    if (ret <= 0) {
        LOG(ERROR) << "cbrdma_connect(" << FLAGS_ip << "," << FLAGS_port << ") failed";
        return;
    }

    ctx->send_buff = cbrdma_get_send_buff(nd, &ctx->send_buff_size);
    if (ctx->send_buff < 0) {
        printf("cbrdma_get_send_buff failed\n");
        return;
    }

    if(ctx->send_msg_size > ctx->send_buff_size) {
        if (!cbrdma_chg_send_buff_size(nd, ctx->send_msg_size)) {
            LOG(ERROR) << "cbrdma_chg_send_buff_size(" << nd << "," << ctx->send_msg_size << ") failed";
            return;
        }

        ctx->send_buff = cbrdma_get_send_buff(nd, &ctx->send_buff_size);
        if (ctx->send_buff < 0) {
            printf("cbrdma_get_send_buff failed\n");
            return;
        }
    }

    memset(ctx->send_buff, 0, ctx->send_msg_size);
    if (cbrdma_send(nd, ctx->send_msg_size) <= 0) {
        LOG(ERROR) << "cbrdma_send failed";
        return;
    }

    while(ctx->running) {
        LOG(INFO) << "send:" << ctx->send_count;
        this_thread::sleep_for(chrono::seconds(1));
    }
}


void polling(int worker_id) {
    while(1) {
    complete_msg_t msgs[FLAGS_poll_msgs];
    int cnt = cbrdma_worker_poll(worker_id, msgs, FLAGS_poll_msgs);
    for(int i = 0; i < cnt; ++i) {
        complete_msg_t * msg = &msgs[i];
        switch(msg->op) {
        case SEND:
            CHECK(on_send(msg->nd, msg->length, msg->status, msg->user_context));
            break;
        case RECV:
            CHECK(on_recv(msg->nd, msg->buff, msg->length, msg->status, msg->user_context));
            break;
        default:
            CHECK(0) << "not support op:" << msg->op;
            break;
        }
    }
    }
}

void disconnected_cb(uint64_t nd, void * context) {
    client_context_t * ctx = (client_context_t *) context;
    CHECK(ctx != NULL);
    if (ctx->srv) {
        LOG(INFO) << "server[" << ctx->srv->nd << "] find connection[" << nd << "] closed";
        ctx->srv->accept_conn_cnt--;
    }

    free(ctx);
}

void log_cb(int level, char* line, int len) {
    LOG(INFO) << line;
}

//GLOG_log_dir=. GLOG_stderrthreshold=1 GLOG_logbuflevel=-1
//./demo -log_dir=. -logtostderr=1 -local_ip=11.97.91.151
//./demo -log_dir=. -logtostderr=1 -t=cli -local_ip=11.97.91.151 -ip=11.97.91.151 -port=1233
int main(int argc, char *argv[]) {
#ifdef NDEBUG
    cout << "define NDEBUG";
#else
    cout << "no define NDEBUG";
#endif
    google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, true);

    LOG(INFO) << "local_ip=[" << FLAGS_local_ip << "] "
              << "t=[" << FLAGS_t << "] clinum=[" << FLAGS_clinum << "] "
              << "numa=[" << FLAGS_numa << "] worker=[" << FLAGS_worker << "] "
              << "ip=[" << FLAGS_ip << "] port=[" << FLAGS_port << "] "
              << "msg_size=[" << FLAGS_msg_size << "] loop_count=[" << FLAGS_loop_count << "]";

    cbrdma_config_t conf;
    cbrdma_init_config(&conf);
    memcpy(conf.str_local_ip, FLAGS_local_ip.c_str(), strlen(FLAGS_local_ip.c_str()));
    conf.numa_node = FLAGS_numa;
    conf.worker_num = FLAGS_worker;
    conf.max_msg_cnt_per_poll = FLAGS_poll_msgs;
    conf.conn_timeout_ms = 1000;
    conf.on_disconnected_func = disconnected_cb;
    conf.log_handler_func = log_cb;

    if (!cbrdma_init(&conf)) {
        LOG(ERROR) << "cbrdma_init failed!";
        return 1;
    }

    vector<thread> worker_ths;
    worker_ths.reserve(conf.worker_num);
    for ( int i = 0; i < conf.worker_num; ++i ) {
        worker_ths.emplace_back(std::thread(polling, i));
    }

    if ( FLAGS_t == "server") {
        start_ppserver();
    } else {
        vector<thread> ths;
        ths.reserve(FLAGS_clinum);
        for ( int i = 0; i < FLAGS_clinum; ++i ) {
            ths.emplace_back(std::thread(start_ppclient, i));
        }

        for ( int i = 0; i < FLAGS_clinum; ++i ) {
            ths[i].join();
        }
    }

    cbrdma_destroy();

    google::ShutDownCommandLineFlags();
    google::ShutdownGoogleLogging();
    return 0;
}
