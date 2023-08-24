#include <iostream>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <thread>
#include <mutex>
#include "cbrdma.h"
#include <sys/eventfd.h>
#include <sstream>

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
DEFINE_uint32(recv_block_size, 1024, "recv_block_size");
DEFINE_uint32(recv_block_cnt, 128, "recv_block_cnt");

int test_running = 1;

typedef struct server_context {
    int running;
    uint64_t nd;
    uint32_t accept_conn_cnt;
} server_context_t;

typedef struct client_context {
    int running;
    uint32_t recv_count;
    uint32_t send_count;
    uint32_t old_send_count;

    uint32_t loop_count;

    int event_fd;
    server_context_t * srv;
} client_context_t;

int on_send(uint64_t nd, uint32_t send_size, int status, void * user_context) {
    int id, worker_id, is_server, is_active;
    cbrdma_parse_nd(nd, &id, &worker_id, &is_server, &is_active);

    client_context_t * ctx = (client_context_t *)user_context;
    if(status <= 0 || (is_active && ctx->send_count >= ctx->loop_count) ) {
        LOG(ERROR) << "on_send status:" << status;
        cbrdma_close(nd);
        ctx->running = 0;
        return 0;
    }

    ctx->send_count++;
    return 1;
}

int on_recv(uint64_t nd, void* recv_buff, uint32_t recv_size, int status, void * user_context) {
    int id, worker_id, is_server, is_active;
    cbrdma_parse_nd(nd, &id, &worker_id, &is_server, &is_active);

    client_context_t * ctx = (client_context_t *)user_context;
    if(status <= 0) {
        LOG(ERROR) << "on_recv status:" << status;
        cbrdma_close(nd);
        ctx->running = 0;
        return 0;
    }

    ctx->recv_count++;


    int32_t send_buff_size = 0;
    void * send_buff = cbrdma_get_send_buff(nd, recv_size, 10, &send_buff_size);
    if (uint32_t(send_buff_size) != recv_size) {
        LOG(ERROR) << "cbrdma_get_send_buff failed";
        return 0;
    }

    memcpy(send_buff, recv_buff, send_buff_size);
    cbrdma_release_recv_buff(nd, recv_buff);

    if (cbrdma_send(nd, send_buff, send_buff_size) <= 0) {
        LOG(ERROR) << "cbrdma_send failed";
        cbrdma_close(nd);
        ctx->running = 0;
        return 1;
    }

    return 1;
}

int accept_cb(uint64_t server_nd, uint64_t accept_nd, void * server_context) {
    server_context_t * srv_ctx = (server_context_t *)server_context;
    srv_ctx->accept_conn_cnt++;

    client_context_t * ctx = (client_context_t *)malloc(sizeof(client_context_t));
    memset(ctx, 0, sizeof(client_context_t));
    ctx->running = 1;

    cbrdma_set_user_context(accept_nd, ctx);
    ctx->srv = srv_ctx;
    return 1;
}

void start_ppserver() {
    server_context_t ctx = {1, 0, 0};
    int ret = 0;
    char ip[MAX_IP_LEN];
    memset(ip, 0, MAX_IP_LEN);
    if (FLAGS_ip.empty()) {
        memcpy(ip, FLAGS_local_ip.c_str(), strlen(FLAGS_local_ip.c_str()));
    } else {
        memcpy(ip, FLAGS_ip.c_str(), strlen(FLAGS_ip.c_str()));
    }
    ret = cbrdma_listen(ip, FLAGS_port, FLAGS_recv_block_size, FLAGS_recv_block_cnt, 0, accept_cb, &ctx, &ctx.nd);
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

    ctx->event_fd = eventfd(0, 0);
    if (ctx->event_fd < 0) {
        LOG(ERROR) << "open event fd failed";
        return;
    }

    ctx->loop_count = FLAGS_loop_count;
    ctx->running = 1;

    int ret = 0;
    uint64_t nd = 0;
    ret = cbrdma_connect(FLAGS_ip.c_str(), FLAGS_port, FLAGS_recv_block_size, FLAGS_recv_block_cnt, 0, 10000, ctx, &nd);
    if (ret <= 0) {
        LOG(ERROR) << "cbrdma_connect(" << FLAGS_ip << "," << FLAGS_port << ") failed";
        return;
    }

    int32_t send_buff_size = 0;
    void * send_buff = cbrdma_get_send_buff(nd, FLAGS_msg_size, 10000, &send_buff_size);
    if (uint32_t(send_buff_size) != FLAGS_msg_size) {
        LOG(ERROR) << "cbrdma_get_send_buff failed";
        return;
    }

    memset(send_buff, 0, send_buff_size);
    if (cbrdma_send(nd, send_buff, send_buff_size) <= 0) {
        cbrdma_close(nd);
        ctx->running = 0;
        return;
    }

    while(ctx->running) {
        int sec = 1;
        int speed = (ctx->send_count - ctx->old_send_count) * 2 / sec;
        stringstream ss;
        ss << "total:" << ctx->send_count << " msg_size:" << FLAGS_msg_size
           << " speed: " << speed << " tps";
        if (speed > 0) {
            ss << " delay: " << 1000000.0 / speed << " us";
        }
        LOG(INFO) << ss.str();
        ctx->old_send_count = ctx->send_count;
        this_thread::sleep_for(chrono::seconds(sec));
    }
}


void polling(int worker_id) {
    while(test_running) {
        complete_msg_t msgs[FLAGS_poll_msgs];
        int cnt = cbrdma_worker_poll(worker_id, msgs, FLAGS_poll_msgs);
        for(int i = 0; i < cnt; ++i) {
            complete_msg_t * msg = &msgs[i];
            switch(msg->op) {
            case CBRDMA_OP_SEND:
                if (!on_send(msg->nd, msg->length, msg->status, msg->user_context)) {
                    LOG(ERROR) << "on_send failed";
                }
                break;
            case CBRDMA_OP_RECV:
                if (!on_recv(msg->nd, msg->buff, msg->length, msg->status, msg->user_context)) {
                    LOG(ERROR) << "on_recv failed";
                }
                break;
            default:
                CHECK(0) << "not support op:" << msg->op;
                break;
            }
        }
    }
}

void disconnected_cb(uint64_t nd, void * context) {
    LOG(INFO) << "disconnected_cb:" << nd;
    cbrdma_close(nd);
}

void error_cb(uint64_t nd, void * context) {
    LOG(INFO) << "error_cb:" << nd;
    cbrdma_close(nd);
}

void closed_cb(uint64_t nd, void * context) {
    LOG(INFO) << "closed_cb:" << nd;
    client_context_t * ctx = (client_context_t *) context;
    CHECK(ctx != NULL);
    if (ctx->srv) {
        LOG(INFO) << "server[" << ctx->srv->nd << "] find connection[" << nd << "] closed";
        ctx->srv->accept_conn_cnt--;
    }

    free(ctx);
    LOG(INFO) << "free(" << ctx << ")";
}

void log_cb(int level, char* line, int len) {
    LOG(INFO) << line;
}

//GLOG_log_dir=. GLOG_stderrthreshold=1 GLOG_logbuflevel=-1
//./demo -log_dir=. -logtostderr=1 -local_ip=11.97.91.151 -recv_block_size=1024 -recv_block_cnt=8
//./demo -log_dir=. -logtostderr=1 -t=cli -local_ip=11.97.91.182 -ip=11.97.91.151 -port=1233 -recv_block_size=1024 -recv_block_cnt=8
int main(int argc, char *argv[]) {
#ifdef NDEBUG
    cout << "define NDEBUG";
#else
    cout << "no define NDEBUG";
#endif
    google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, true);

    LOG(INFO) << "local_ip=[" << FLAGS_local_ip  << "] "
              << "t=[" << FLAGS_t << "] clinum=[" << FLAGS_clinum << "] "
              << "numa=[" << FLAGS_numa << "] worker=[" << FLAGS_worker << "] "
              << "ip=[" << FLAGS_ip << "] port=[" << FLAGS_port << "] "
              << "msg_size=[" << FLAGS_msg_size << "] loop_count=[" << FLAGS_loop_count << "]"
              << "recv_block_size=[" << FLAGS_recv_block_size << "] recv_block_cnt=[" << FLAGS_recv_block_cnt << "]";

    test_running = 1;

    cbrdma_config_t conf;
    cbrdma_init_config(&conf);
    memcpy(conf.str_local_ip, FLAGS_local_ip.c_str(), strlen(FLAGS_local_ip.c_str()));
    conf.numa_node = FLAGS_numa;
    conf.worker_num = FLAGS_worker;
    conf.max_msg_cnt_per_poll = FLAGS_poll_msgs;
    conf.conn_timeout_ms = 1000;
    conf.on_disconnected_func = disconnected_cb;
    conf.on_error_func = error_cb;
    conf.on_closed_func = closed_cb;
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

    //test_running = 0;
    for ( int i = 0; i < conf.worker_num; ++i ) {
        worker_ths[i].join();
    }

    cbrdma_destroy();

    google::ShutDownCommandLineFlags();
    google::ShutdownGoogleLogging();
    return 0;
}
