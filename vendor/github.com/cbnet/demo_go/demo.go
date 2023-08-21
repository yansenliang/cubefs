package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"../cbrdma"
)

type logHandler struct {
	Fp *os.File
}

type CbConnect struct {
	ConnType int
	cbrdma.RDMAConn
	recvCnt  uint64
	sendCnt  uint64
	start    time.Time
	needSend uint32
}

type CbServer struct {
	cbrdma.RDMAServer
	connMap     map[uint64]*CbConnect
}

var workerCnt int
var logLevel int
var numNode int
var recvSize int
var recvCnt int
var clientCnt int
var syncFlag int
var ip string
var port string
var mode string
var wg sync.WaitGroup
var logger *logHandler
var localIp string
var connMap map[uint64]*CbConnect
var mu sync.Mutex

func (handler *logHandler) Debug(format string, v ...interface{}) {
	if handler.Fp != nil {
		handler.Fp.WriteString(fmt.Sprintf(format, v...))
		handler.Fp.WriteString("\n")
	}
}

func (handler *logHandler) Info(format string, v ...interface{}) {
	if handler.Fp != nil {
		handler.Fp.WriteString(fmt.Sprintf(format, v...))
		handler.Fp.WriteString("\n")
	}
}

func (handler *logHandler) Warn(format string, v ...interface{}) {
	if handler.Fp != nil {
		handler.Fp.WriteString(fmt.Sprintf(format, v...))
		handler.Fp.WriteString("\n")
	}
}

func (handler *logHandler) Error(format string, v ...interface{}) {
	if handler.Fp != nil {
		handler.Fp.WriteString(fmt.Sprintf(format, v...))
		handler.Fp.WriteString("\n")
	}
}

func onRecv(ctx *cbrdma.RDMAConn, buffer []byte, recvLen int, status int) {
	conn := (*CbConnect)(ctx.GetUserContext())

	conn.recvCnt++

	if conn.recvCnt%100000 == 0 {
		cost := time.Since(conn.start)
		logger.Info("conn(%d-%p) recv %d, cost:%v, per op:%v", conn.GetNd(), conn, conn.recvCnt, cost, cost/(time.Duration(conn.recvCnt)))
	}
	return
}

func onSend(ctx *cbrdma.RDMAConn, sendLen int, status int) {
	conn := (*CbConnect)(ctx.GetUserContext())
	buff, err := conn.GetSendBuf(recvSize - 64, -1)
	if err != nil {
		atomic.StoreUint32(&conn.needSend, 1)
		return
	}

	if err = conn.Send(buff, cap(buff)); err != nil {
		logger.Info("send failed:%s", err.Error())
		panic(fmt.Sprint("conn(%d-%p) send failed, err:%s", conn.GetNd(), conn, err.Error()))
		return
	}
	conn.sendCnt++
	return
}

func onDisconnected(conn *cbrdma.RDMAConn) {
	conn.Close()
	return
}

func onClosed(conn *cbrdma.RDMAConn) {
	//TODO
}

func AcceptCbFunc(server *cbrdma.RDMAServer) *cbrdma.RDMAConn {
	s := (*CbServer)(server.Ctx)
	conn := &CbConnect{}
	conn.Init(onRecv, onSend, onDisconnected, onClosed, unsafe.Pointer(conn))
	s.connMap[conn.GetNd()] = conn
	conn.ConnType = 2
	conn.needSend = 1
	conn.start = time.Now()

	go func(c *CbConnect) {
		cnt := 0
		for {
			cnt++
			if cnt%1000000 == 0 {
				info := conn.GetCounter()
				logger.Info("conn(%d-%p) send count:%d recv count:%d, counter:%v", conn.GetNd(), conn, c.sendCnt, c.recvCnt, info)
			}

			if atomic.LoadUint32(&conn.needSend) == 1 {
				buff, err := conn.GetSendBuf(recvSize - 64, -1)
				if err != nil {
					if err == cbrdma.RetryErr {
						continue
					}
					return
				}
				if err = conn.Send(buff, cap(buff)); err != nil {
					panic(fmt.Sprint("conn(%d-%p) send failed, err:%s", conn.GetNd(), conn, err.Error()))
				}
				atomic.StoreUint32(&conn.needSend, 0)
			}

			time.Sleep(time.Microsecond * 5)
		}

	}(conn)
	return &conn.RDMAConn
}

func SyncAcceptCbFunc(server *cbrdma.RDMAServer) *cbrdma.RDMAConn {
	s := (*CbServer)(server.Ctx)
	conn := &CbConnect{}
	conn.Init(nil, nil, onDisconnected, onClosed, unsafe.Pointer(conn))
	s.connMap[conn.GetNd()] = conn
	conn.start = time.Now()

	go func(syncConn *CbConnect) {
		for {
			if _, err := syncConn.Read(nil); err != nil {
				logger.Error("recv failed:%s", err.Error())
				syncConn.Close()
				return
			}
			recvBuff, _ := syncConn.GetRecvBuff()
			syncConn.ReleaseRecvBuffer(recvBuff)

			buff, err := syncConn.GetSendBuf(recvSize - 64, -1)
			if err != nil {
				if err == cbrdma.RetryErr {
					continue
				}
				return
			}
			if err := syncConn.Send(buff, cap(buff)); err != nil {
				logger.Error("send failed:%s", err.Error())
				syncConn.Close()
				return
			}

			syncConn.recvCnt++
			syncConn.sendCnt++

			if syncConn.recvCnt%100000 == 0 {
				cost := time.Since(syncConn.start)
				logger.Info("conn(%d-%p) recv %d, cost:%v, per op:%v", conn.GetNd(), conn, conn.recvCnt, cost, cost/(time.Duration(syncConn.recvCnt)))
			}
		}
	}(conn)

	go func(c *CbConnect) {
		for {
			oldSendCnt := c.sendCnt
			time.Sleep(time.Second)
			if oldSendCnt != c.sendCnt {
				continue
			}
			info := conn.GetCounter()

			logger.Info("conn(%d-%p) send count:%d recv count:%d, counter:%v", conn.GetNd(), conn, c.sendCnt, c.recvCnt, info)
		}

	}(conn)

	return &conn.RDMAConn
}

func startAsyncClient() {
	iPort, _ := strconv.Atoi(port)
	conn := &CbConnect{}
	conn.Init(onRecv, onSend, onDisconnected, onClosed, unsafe.Pointer(conn))
	if err := conn.Dial(ip, iPort, recvSize, recvCnt, 0); err != nil {
		logger.Error("conn(%s:%d) dial failed:%s", ip, iPort, err.Error())
		return
	}
	connMap[conn.GetNd()] = conn
	conn.start = time.Now()
	conn.needSend = 1

	go func(c *CbConnect) {
		cnt := 0
		for {
			cnt++
			if cnt%1000000 == 0 {
				info := conn.GetCounter()
				logger.Info("conn(%d-%p) send count:%d recv count:%d, counter:%v", conn.GetNd(), conn, c.sendCnt, c.recvCnt, info)
			}

			if atomic.LoadUint32(&conn.needSend) == 1 {
				buff, err := conn.GetSendBuf(recvSize - 64, -1)
				if err != nil {
					if err == cbrdma.RetryErr {
						continue
					}
					return
				}
				if err = conn.Send(buff, cap(buff)); err != nil {
					panic(fmt.Sprint("conn(%d-%p) send failed, err:%s", conn.GetNd(), conn, err.Error()))
				}
				atomic.StoreUint32(&conn.needSend, 0)
			}

			time.Sleep(time.Microsecond * 5)
		}

	}(conn)
}

func startSyncClient() {
	iPort, _ := strconv.Atoi(port)
	conn := &CbConnect{}
	conn.Init(nil, nil, onDisconnected, onClosed, unsafe.Pointer(conn))
	if err := conn.Dial(ip, iPort, recvSize, recvCnt, 0); err != nil {
		logger.Error("conn(%s:%d) dial failed:%s", ip, iPort, err.Error())
		return
	}
	mu.Lock()
	connMap[conn.GetNd()] = conn
	mu.Unlock()
	conn.start = time.Now()
	go func(c *CbConnect) {
		for {
			oldSendCnt := c.sendCnt
			time.Sleep(time.Second)
			if oldSendCnt != c.sendCnt {
				continue
			}
			info := conn.GetCounter()

			logger.Info("conn(%d-%p) send count:%d recv count:%d, counter:%v", conn.GetNd(), conn, c.sendCnt, c.recvCnt, info)
		}

	}(conn)
	for {
		buff, err := conn.GetSendBuf(recvSize - 64, -1)
		if err != nil {
			if err == cbrdma.RetryErr {
				continue
			}
			return
		}

		if err := conn.Send(buff, cap(buff)); err != nil {
			logger.Error("send failed:%s", err.Error())
			return
		}
		conn.sendCnt++

		if _, err := conn.Read(nil); err != nil {
			logger.Error("recv failed:%s", err.Error())
			return
		}
		recvBuff, _ := conn.GetRecvBuff()
		conn.ReleaseRecvBuffer(recvBuff)

		conn.recvCnt++
		if conn.recvCnt%100000 == 0 {
			cost := time.Since(conn.start)
			logger.Info("conn(%d-%p) recv %d, cost:%v, per op:%v", conn.GetNd(), conn, conn.recvCnt, cost, cost/(time.Duration(conn.recvCnt)))
		}
	}
}

func startClient() {
	for i := 0; i < clientCnt; i++ {
		if syncFlag == 1 {
			go startSyncClient()
		} else {
			go startAsyncClient()
		}
		wg.Add(1)
	}
	wg.Wait()
}

func startServer() {
	server := &CbServer{connMap: make(map[uint64]*CbConnect)}
	server.Init(AcceptCbFunc, onRecv, onSend, onDisconnected, onClosed, unsafe.Pointer(server))
	if syncFlag == 1 {
		server.OnAccept = SyncAcceptCbFunc
	}
	iPort, _ := strconv.Atoi(port)
	server.Ctx = unsafe.Pointer(server)
	wg.Add(1)
	if err := server.Listen(ip, iPort, recvSize, recvCnt, 0); err != nil {
		logger.Error("server(%s:%d) listen failed:%s\n", ip, iPort)
	}
	for {
		logger.Info("Server(%s:%d) has %d conn", ip, iPort, len(server.connMap))
		time.Sleep(time.Second * 30)
	}
	wg.Wait()
}

func main() {
	flag.IntVar(&workerCnt, "w", 1, "worker count, default 1")
	flag.IntVar(&logLevel, "l", 1, "log level, default 1")
	flag.IntVar(&numNode, "numa", -1, "numa node, default -1")
	flag.IntVar(&recvSize, "m", 4096, "msg size, unit B, default 4KB")
	flag.IntVar(&recvCnt, "recvCnt", 1024, "msg count, 1024")
	flag.IntVar(&clientCnt, "n", 1, "client count, default 1")
	flag.IntVar(&syncFlag, "sync", 0, "sync, async 0, sync 1, default 0 async")
	flag.StringVar(&ip, "i", "0.0.0.0", "ip default 0.0.00")
	flag.StringVar(&port, "p", "2233", "port default 2233")
	flag.StringVar(&mode, "c", "client", "default client otherwise server")
	flag.StringVar(&localIp, "loc", "127.0.0.1", "local ip that bond rdma dev")
	flag.Parse()

	profNetListener, _ := net.Listen("tcp", ":3000")
	go http.Serve(profNetListener, http.DefaultServeMux)

	connMap = make(map[uint64]*CbConnect)
	logger = &logHandler{Fp: os.Stdout}
	cbrdma.InitNetEnv(workerCnt, logLevel, numNode, localIp, logger)
	if mode == "client" {
		startClient()
	} else {
		startServer()
	}
}
