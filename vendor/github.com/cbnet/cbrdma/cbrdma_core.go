// +build rdma

package cbrdma

//#cgo CFLAGS: -I. -I../src
//#cgo LDFLAGS: -L. -L../src  -lcbrdma -lrdmacm -libverbs -lpthread
//
//#include <stdint.h>
//#include <unistd.h>
//#include <sys/eventfd.h>
//#include "cbrdma.h"
//int  ServerOnAccept(uint64_t server, uint64_t new_conn, void* context);
//void  NetOnDisconnectedCb(uint64_t nd, void* context);
//void  NetOnClosedCb(uint64_t nd, void* context);
//void LogCbFunc(int level, char* line, int len);
/*
static inline int open_event_fd() {
   return eventfd(0, EFD_SEMAPHORE);
}

static inline int wait_event(int fd) {
	uint64_t value = 0;
	return read(fd, &value, 8);
}

static inline int notify_event(int fd, int flag) {
	if (flag == 0) {
		uint64_t value = 1;
		return write(fd, &value, 8);
	} else {
		close(fd);
	}
	return 0;
}
*/
import "C"
import (
	"container/list"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"
)

func (conn *RDMAConn) GetCounter() *ConnCounter {
	var Cinfo C.conn_counter_t
	info := &ConnCounter{}
	C.get_conn_counter(conn.connPtr, &Cinfo)

	info.PostSend = uint64(Cinfo.send_post_cnt)
	info.SendAck = uint64(Cinfo.send_ack_cnt)
	info.SendCb = uint64(Cinfo.send_cb_cnt)

	info.RecvCnt = uint64(Cinfo.recv_cnt)
	info.RecvAck = uint64(Cinfo.recv_ack_cnt)

	info.SendWin = uint16(Cinfo.send_win_size)
	info.RecvWin = uint16(Cinfo.recv_win_size)
	info.RSendWin = uint16(Cinfo.peer_send_size)
	info.RRecAck = uint16(Cinfo.peer_ack_cnt)
	return info
}

func (conn *RDMAConn) OnRecvCB(buff []byte, recvLen int, status int) {
	if conn == nil {
		return
	}

	if conn.onRecv != nil {
		conn.onRecv(conn, buff, recvLen, status)
		C.cbrdma_release_recv_buff(conn.connPtr, unsafe.Pointer(&buff[0]))
		return
	}

	if status < 0 {
		C.notify_event(conn.rFd, -1)
		conn.onDisconnected(conn)
		return
	}

	conn.mu.Lock()
	conn.recvMsgList.PushBack(&RecvMsg{buff, recvLen})
	conn.mu.Unlock()

	C.notify_event(conn.rFd, 0)
	return
}

func (conn *RDMAConn) OnSendCB(sendLen int, status int) {
	if conn == nil {
		return
	}

	if conn.onSend != nil {
		conn.onSend(conn, sendLen, status)
		return
	}

	if status < 0 {
		C.notify_event(conn.wFd, -1)
		conn.onDisconnected(conn)
		return
	}

	C.notify_event(conn.wFd, 0)
	return
}

func (conn *RDMAConn) Init(recvFunc OnRecvFunc, sendFunc OnSendFunc, disconnectedFunc OnDisconnectedFunc, closedFunc OnClosedFunc, ctx unsafe.Pointer) {
	conn.ctx = ctx
	conn.onRecv = recvFunc
	conn.onSend = sendFunc
	conn.onDisconnected = disconnectedFunc
	conn.onError = disconnectedFunc
	conn.onClosed = closedFunc

	if conn.onRecv == nil {
		conn.rFd = C.open_event_fd()
		conn.recvMsgList = list.New()
	}

	if conn.onSend == nil {
		conn.wFd = C.open_event_fd()
	}

}

func (conn *RDMAConn) Dial(ip string, port int, recvSize int, recvCnt int, memType int) error {
	cIp := C.CString(ip)
	defer C.free(unsafe.Pointer(cIp))
	ret := C.cbrdma_connect(cIp, C.uint16_t(port), C.uint32_t(recvSize), C.uint32_t(recvCnt), C.int(memType), 10000, unsafe.Pointer(conn), &conn.connPtr)
	if ret <= 0 {
		return fmt.Errorf("create connect %s:%d failed", ip, port)
	}

	conn.connType = CONN_TYPE_ACTIVE
	atomic.StoreInt32(&conn.state, CONN_ST_CONNECTED)
	conn.remoteAddr = RDMAAddr{addr: ip + strconv.Itoa(port - 10000)}
	gConnMap.Store(conn.connPtr, conn)
	return nil
}

func (conn *RDMAConn) GetSendBuf(len int, timeout_us int) ([]byte, error) {
	var buffSize C.int32_t
	dataPtr := C.cbrdma_get_send_buff(conn.connPtr, C.uint32_t(len), C.int64_t(timeout_us), &buffSize)
	if buffSize == 0 {
		return nil, RetryErr
	}

	if dataPtr == C.NULL {
		return nil, fmt.Errorf("conn(%d-%p) get send buff failed", uint64(conn.connPtr), conn)
	}
	sendBuff := CbuffToSlice(dataPtr, int(buffSize))
	return sendBuff, nil
}

func (conn *RDMAConn) Write(buff []byte) (int, error) {
	if err := conn.Send(buff, len(buff)); err != nil {
		return -1, err
	}

	return len(buff), nil
}

func (conn *RDMAConn) Send(buff []byte, length int) (err error) {
	ret := C.cbrdma_send(conn.connPtr, unsafe.Pointer(&buff[0]), C.uint32_t(length))
	if ret <= 0 {
		err = fmt.Errorf("conn(%d) send failed", conn.connPtr)
	}

	if conn.onSend == nil {
		if value := C.wait_event(conn.wFd); value <= 0 {
			return fmt.Errorf("send failed")
		}
	}
	return nil
}

func (conn *RDMAConn) Read([]byte) (int, error) {
	if conn.onRecv != nil {
		return 0, nil
	}

	if value := C.wait_event(conn.rFd); value <= 0 {
		return -1, fmt.Errorf("recv failed")
	}

	return 0, nil
}

func (conn *RDMAConn) GetRecvBuff() ([]byte, int) {
	if conn.onRecv != nil {
		return nil, 0
	}

	conn.mu.RLock()
	msgElem := conn.recvMsgList.Front()
	conn.recvMsgList.Remove(msgElem)
	conn.mu.RUnlock()
	msg := msgElem.Value.(*RecvMsg)
	return msg.dataPatr, msg.len
}

func (conn *RDMAConn) ReleaseRecvBuffer(buff []byte) (err error) {
	if buff == nil || len(buff) == 0 {
		return nil
	}

	C.cbrdma_release_recv_buff(conn.connPtr, unsafe.Pointer(&buff[0]))
	return
}

func (conn *RDMAConn) LocalAddr() net.Addr {
	return &conn.localAddr
}

func (conn *RDMAConn) RemoteAddr() net.Addr {
	return &conn.remoteAddr
}

func (conn *RDMAConn)  SetDeadline(t time.Time) error {
	return nil
}

func (conn *RDMAConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (conn *RDMAConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func (conn *RDMAConn) Close() error {

	if _, exist := gConnMap.LoadAndDelete(conn.connPtr); !exist {
		//already clsoed
		return nil
	}

	if conn.rFd > 0 {
		C.close(conn.rFd)
		conn.rFd = -1
	}

	if conn.wFd > 0 {
		C.close(conn.wFd)
		conn.wFd = -1
	}

	conn.mu.Lock()
	for elem := conn.recvMsgList.Front(); elem != nil; elem = elem.Next() {
		msg := elem.Value.(*RecvMsg)
		C.cbrdma_release_recv_buff(conn.connPtr, unsafe.Pointer(&msg.dataPatr[0]))
	}
	conn.recvMsgList.Init()
	conn.mu.Unlock()

	C.cbrdma_close(conn.connPtr)
	return nil
}

//export ServerOnAccept
func ServerOnAccept(serverNd C.uint64_t, connNd C.uint64_t, context *C.void) C.int {
	server := (*RDMAServer)(unsafe.Pointer(context))

	conn := server.OnAccept(server)
	if conn == nil {
		return 0
	}

	//if conn.onRecv == nil {
	//	conn.onRecv = server.DefOnRecv
	//}
	//
	//if conn.onSend == nil {
	//	conn.onSend = server.DefOnSend
	//}

	//if conn.onClosed == nil {
	//	conn.onClosed = server.DefOnClosed
	//}
	//
	//if conn.onDisconnected == nil {
	//	conn.onDisconnected = server.DefOnDisconnected
	//}

	atomic.StoreInt32(&conn.state, CONN_ST_CONNECTED)
	conn.connPtr = connNd
	C.cbrdma_set_user_context(connNd, unsafe.Pointer(conn))
	gConnMap.Store(conn.connPtr, conn)

	return 1
}

func NewServer(onAccept OnAcceptFunc, onRecv OnRecvFunc, onSend OnSendFunc, onDisconnected OnDisconnectedFunc, onClosed OnClosedFunc, ctx unsafe.Pointer) *RDMAServer {
	server := new(RDMAServer)
	server.OnAccept = onAccept
	server.DefOnRecv = onRecv
	server.DefOnSend = onSend
	server.DefOnDisconnected = onDisconnected
	server.DefOnError = onDisconnected
	server.DefOnClosed = onClosed
	server.ctx = ctx
	return server
}

func (server *RDMAServer) Init(onAccept OnAcceptFunc, onRecv OnRecvFunc, onSend OnSendFunc, onDisconnected OnDisconnectedFunc, onClosed OnClosedFunc, ctx unsafe.Pointer) {
	server.OnAccept = onAccept
	server.DefOnRecv = onRecv
	server.DefOnSend = onSend
	server.DefOnDisconnected = onDisconnected
	server.DefOnError = onDisconnected
	server.DefOnClosed = onClosed
	server.ctx = ctx
}

func (server *RDMAServer) Listen(ip string, port int, defRecvSize int, defRecvCnt int, memType int) error {
	cIp := C.CString(ip)
	defer C.free(unsafe.Pointer(cIp))
	server.ListernIp = ip
	server.Port = port
	ret := C.cbrdma_listen(cIp, C.uint16_t(port), C.uint32_t(defRecvSize), C.uint32_t(defRecvCnt), C.int(memType),
		C.accept_conn_cb_t(C.ServerOnAccept), unsafe.Pointer(server), &server.serverPtr)
	if ret <= 0 {
		return fmt.Errorf("create server %s:%d failed", ip, port)
	}
	return nil
}

func (server *RDMAServer) Close() error {
	C.cbrdma_close(server.serverPtr)
	return nil
}

func workerPoll(workerId int) {
	msgs := C.malloc(C.sizeof_complete_msg_t * 16)

	runtime.LockOSThread()
	for {
		var num C.int = 0
		num = C.cbrdma_worker_poll(C.uint32_t(workerId), (*C.complete_msg_t)(msgs), 16)
		if num < 0 {
			//net close exit
			break
		}

		for i := C.int(0); i < num; i++ {
			m := (*C.complete_msg_t)(unsafe.Pointer(uintptr(unsafe.Pointer(msgs)) + uintptr(C.sizeof_complete_msg_t*C.int(i))))
			ctx, ok := gConnMap.Load(m.nd)
			if !ok {
				if m.op == C.CBRDMA_OP_RECV {
					C.cbrdma_release_recv_buff(m.nd, m.buff)
				}
				continue
			}

			conn := ctx.(*RDMAConn)

			switch m.op {
			case C.CBRDMA_OP_SEND:
				conn.OnSendCB( int(m.length), int(m.status))
			case C.CBRDMA_OP_RECV:
				recvBuff := CbuffToSlice(m.buff, int(m.length))
				conn.OnRecvCB( recvBuff, int(m.length), int(m.status))
			default:
				//unspport
			}
		}
		atomic.AddUint64(&pollcnt, 1)
	}
}

func SetNetLogLevel(level int) {
	C.cbrdma_set_log_level(C.int(level))
	return
}

//export LogCbFunc
func LogCbFunc(level C.int, buff *C.char, length C.int) {
	goStr := C.GoString(buff)
	if gLogHandler == nil {
		return
	}
	switch level {
	case C.DEBUG:
		gLogHandler.Debug(goStr)
	case C.INFO:
		gLogHandler.Info(goStr)
	case C.WARN:
		gLogHandler.Warn(goStr)
	case C.ERROR:
		gLogHandler.Error(goStr)
	case C.FATAL:
		gLogHandler.Error(goStr)
	}
}

//export NetOnDisconnectedCb
func NetOnDisconnectedCb(nd C.uint64_t, userContext *C.void) {
	if _, exist := gConnMap.Load(nd); !exist {
		C.cbrdma_close(nd)
		return
	}

	conn := (*RDMAConn)(unsafe.Pointer(userContext))
	atomic.StoreInt32(&conn.state, CONN_ST_CLOSE)
	if conn.onDisconnected != nil {
		conn.onDisconnected(conn)
	}
	return
}

//export NetOnClosedCb
func NetOnClosedCb(nd C.uint64_t, userContext *C.void) {
	if _, exist := gConnMap.Load(nd); !exist {
		C.cbrdma_close(nd)
		return
	}

	conn := (*RDMAConn)(unsafe.Pointer(userContext))
	if conn.onClosed != nil {
		conn.onClosed(conn)
	}
	return
}

func InitNetEnv(workerNum, logLevel, numaNode int, localIp string, logHandler NetLogger) (int, error) {
	gLogHandler = logHandler
	config := C.cbrdma_config_t{}
	C.cbrdma_init_config(&config)

	config.numa_node = C.int8_t(numaNode)
	config.worker_num = C.uint8_t(workerNum)
	config.log_level = C.uint8_t(logLevel)
	config.log_handler_func = C.log_handler_cb_t(C.LogCbFunc)
	config.on_disconnected_func = C.on_disconnected_cb_t(C.NetOnDisconnectedCb)
	config.on_error_func = C.on_error_cb_t(C.NetOnDisconnectedCb)
	config.on_closed_func = C.on_closed_cb_t(C.NetOnClosedCb)
	gLogHandler.Error("init net env(%d, %d, %d, %s, %p)", workerNum, logLevel, numaNode, localIp, logHandler)

	for i := 0; i < len(localIp) && i < int(C.MAX_IP_LEN); i++ {
		config.str_local_ip[i] = C.char(localIp[i])
	}

	ret := C.cbrdma_init(&config)
	if ret <= 0 {
		return -1, fmt.Errorf("init net env failed")
	}

	for i := 0; i < workerNum; i++ {
		go workerPoll(i)
	}
	return 0, nil
}

func DestoryNetEnv() {
	C.cbrdma_destroy()
	return
}
