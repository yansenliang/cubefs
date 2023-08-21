// +build rdma_fake

package cbrdma

import "fmt"
import "unsafe"

func (conn *RDMAConn) Init(recvFunc OnRecvFunc, sendFunc OnSendFunc, disconnectedFunc OnDisconnectedFunc, closedFunc OnClosedFunc, ctx unsafe.Pointer) {
    return
}

func (conn *RDMAConn) Dial(ip string, port int, recvSize int, recvCnt int, memType int) error {
    return fmt.Errorf("not support rdma")
}

func (conn *RDMAConn) GetSendBuf(length int, int2 int) ([]byte, error) {
    return nil, fmt.Errorf("not support rdma")
}

func (conn *RDMAConn) Write(p []byte) (int, error) {
    return -1, fmt.Errorf("not support rdma")
}

func (conn *RDMAConn) Send(buff[]byte, length int) (err error) {
    return fmt.Errorf("not support rdma")
}

func (conn *RDMAConn) Read([]byte) (int, error) {
    return 0, nil
}

func (conn *RDMAConn) GetRecvBuff() ([]byte, int) {
    return nil, 0
}

func (conn *RDMAConn) ReleaseRecvBuffer(buff []byte) (err error) {
    return
}

func (conn *RDMAConn) Close() error {
    return fmt.Errorf("not support rdma")
}

func (conn *RDMAConn) GetCounter() *ConnCounter {
    info := &ConnCounter{}
    return info
}

func NewServer(onAccept OnAcceptFunc, onRecv OnRecvFunc, onSend OnSendFunc, onDisconnected OnDisconnectedFunc, onClosed OnClosedFunc, ctx unsafe.Pointer) *RDMAServer {
    return nil
}
func (server *RDMAServer) Init(onAccept OnAcceptFunc, onRecv OnRecvFunc, onSend OnSendFunc, onDisconnected OnDisconnectedFunc, onClosed OnClosedFunc, ctx unsafe.Pointer) {
    return
}

func (server *RDMAServer) Listen(ip string, port int, defualtRecvSize int, recvCnt int, memType int) error {
    return nil
}

func (server *RDMAServer) Close() error {
    return nil
}

func SetNetLogLevel(level int) {
    return
}


func InitNetEnv(workerNum, logLevel, numaNode int, localIp string, logHandler NetLogger) (int, error) {

    return 0, nil
}

func DestoryNetEnv() {
    return
}
