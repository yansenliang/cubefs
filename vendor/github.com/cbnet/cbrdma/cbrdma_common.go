package cbrdma

//#include <stdint.h>
import "C"
import (
    "container/list"
    "time"
    "unsafe"
    "reflect"
)
import "sync"
import "fmt"

type OnRecvFunc func(conn *RDMAConn, buffer []byte, recvLen int, status int)
type OnSendFunc func(conn *RDMAConn, sendLen int, status int)
type OnDisconnectedFunc func(conn *RDMAConn)
type OnClosedFunc func(conn *RDMAConn)
type OnAcceptFunc func(server *RDMAServer) (conn *RDMAConn)

const (
    MAX_ADDR_LEN = 64
)
const (
    CONN_TYPE_ACTIVE = (1 << iota)
)

const (
    CONN_ST_CLOSE     = 0
    CONN_ST_CONNECTED = 1
)

const (
    RDMA_NetWorker_STR  = "rdma"
)

var RetryErr = fmt.Errorf("Retry")

var pollcnt uint64 = 0

type NetLogger interface {
    Debug(format string, v ...interface{})
    Info(format string, v ...interface{})
    Warn(format string, v ...interface{})
    Error(format string, v ...interface{})
    Flush()
}

type RecvMsg struct {
    dataPatr []byte
    len      int
}

type RDMAAddr struct {
    addr   string
}

type RDMAConn struct {
    connPtr  C.uint64_t
    connType int
    state    int32

    //user cb
    onRecv OnRecvFunc
    onSend OnSendFunc

    onDisconnected OnDisconnectedFunc
    onError OnDisconnectedFunc
    onClosed OnClosedFunc

    mu          sync.RWMutex
    recvMsgList *list.List
    rFd         C.int
    wFd         C.int

    recvDeadLine time.Duration
    sendDeadLine time.Duration

    localAddr  RDMAAddr
    remoteAddr RDMAAddr

    ctx unsafe.Pointer
}

type RDMAServer struct {
    serverPtr C.uint64_t
    ListernIp string
    Port      int
    OnAccept  OnAcceptFunc

    //def conn cb
    DefOnRecv  OnRecvFunc
    DefOnSend  OnSendFunc
    DefOnDisconnected OnDisconnectedFunc
    DefOnError OnDisconnectedFunc
    DefOnClosed OnClosedFunc
    ctx        unsafe.Pointer
}

type ConnCounter struct {
    PostSend uint64
    SendAck  uint64
    SendCb   uint64

    RecvCnt uint64
    RecvAck uint64

    SendWin  uint16
    RecvWin  uint16
    RRecAck  uint16
    RSendWin uint16
}

var gLogHandler NetLogger

var gConnMap    sync.Map
//var gConnMap    map[C.uint64_t]unsafe.Pointer

func CbuffToSlice(ptr unsafe.Pointer, length int) []byte {
    var buffer []byte

    hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
    hdr.Data = uintptr(ptr)
    hdr.Len = int(length)
    hdr.Cap = int(length)

    return buffer
}

func (r *RDMAAddr) Network() string {
    return RDMA_NetWorker_STR
}

func (r *RDMAAddr) String() string {
    return r.addr
}

func (conn *RDMAConn) GetConnType() int {
    return conn.connType
}

func (conn *RDMAConn) GetNd() uint64 {
    return uint64(conn.connPtr)
}

func (conn *RDMAConn) GetUserContext() unsafe.Pointer {
    return conn.ctx
}

func (server *RDMAServer) GetUserContext() unsafe.Pointer {
    return server.ctx
}

