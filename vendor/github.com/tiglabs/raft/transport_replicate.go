// Copyright 2018 The tiglabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cbnet/cbrdma"
	"github.com/tiglabs/raft/logger"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/util"
)

type replicateTransport struct {
	config       *TransportConfig
	raftServer   *RaftServer
	listener     net.Listener
	rdmaListener *cbrdma.RDMAServer
	curSnapshot int32
	mu          sync.RWMutex
	senders     map[uint64]*transportSender
	rdmaConn    map[uint64]*cbrdma.RDMAConn
	stopc       chan struct{}

	queueNum    uint64
	rcvQs       []*util.CircleQueue
}

func RDMAServerAccpetCb (server *cbrdma.RDMAServer) (conn *cbrdma.RDMAConn) {
	conn = &cbrdma.RDMAConn{}
	conn.Init(RDMAServerRecvCb, nil, RDMAServerDisconnectCb, RDMAServerCloseCb, server.GetUserContext())
	repl := (*replicateTransport) (server.GetUserContext())
	repl.mu.Lock()
	repl.rdmaConn[conn.GetNd()] = conn
	repl.mu.Unlock()
	return
}

func RDMAServerRecvCb(conn *cbrdma.RDMAConn, datas []byte, recvLen int, status int) {
	transport := (*replicateTransport) (conn.GetUserContext())
	stop := false
	defer func() {
		if status != 0 || stop {
			conn.Close()
			transport.mu.Lock()
			delete(transport.rdmaConn, conn.GetNd())
			transport.mu.Unlock()
			return
		}
	}()

	if status != 0 {
		return
	}
	//stop channel有消息则停止执行rcv请求
	select {
	case <-transport.stopc:
		stop = true
		return
	default:
	}

	buf := make([]byte, len(datas))
	copy(buf, datas)
	//ucxnet_go.UcxnetBufFree(datas)
	dataType := proto.GetDataType(buf)
	if dataType == proto.ReqMsgSnapShot {
		panic("recv err")
	} else {
		if len(buf) <= 20 {
			logger.Error("recv repliac data(%d), data:%v\n", len(buf), buf)
		}
		err := transport.EnqueueRecvData(buf)
		if err != nil {
			logger.Error("!!!Oops, recv repliac drop data(%d), data:%v\n", len(buf), buf)
		}
	}
	return
}

func RDMAServerSendCb(conn *cbrdma.RDMAConn, sendLen int, status int) {
	return
}

func RDMAServerDisconnectCb(conn *cbrdma.RDMAConn) {
	transport := (*replicateTransport) (conn.GetUserContext())
	conn.Close()
	transport.mu.Lock()
	delete(transport.rdmaConn, conn.GetNd())
	transport.mu.Unlock()
	return
}

func RDMAServerCloseCb(conn *cbrdma.RDMAConn) {
	transport := (*replicateTransport) (conn.GetUserContext())
	conn.Close()
	transport.mu.Lock()
	delete(transport.rdmaConn, conn.GetNd())
	transport.mu.Unlock()
	return
}

func newReplicateTransport(raftServer *RaftServer, config *TransportConfig) (*replicateTransport, error) {
	var (
		listener net.Listener
		err      error
	    rcvQs []*util.CircleQueue
	)

	queueNum := uint64(4)
	for i := 0; i < transportListenRetryMaxCount; i++ {
		listener, err = net.Listen("tcp", config.ReplicateAddr)
		if err != nil {
			time.Sleep(transportListenRetryInterval)
			continue
		}
		break
	}
	if err != nil {
		return nil, err
	}

	t := &replicateTransport{
		config:     config,
		raftServer: raftServer,
		listener:   listener,
		senders:    make(map[uint64]*transportSender),
		stopc:      make(chan struct{}),
		queueNum:  config.QueueCnt,
		rdmaConn:   make(map[uint64]*cbrdma.RDMAConn),
	}

	rdmaListener := cbrdma.NewServer(RDMAServerAccpetCb, RDMAServerRecvCb, RDMAServerSendCb, RDMAServerDisconnectCb,
		RDMAServerCloseCb, unsafe.Pointer(t))
	strIp := strings.Split(config.ReplicateRDMAAddr, ":")[0]
	Port, _  := strconv.Atoi(strings.Split(config.ReplicateRDMAAddr, ":")[1])
	if err = rdmaListener.Listen(strIp, Port, 128 * KB, 8, 0); err != nil {
		listener.Close()
		return nil, err
	}

	for i := uint64(0); i < queueNum; i++ {
		var rcvQ util.CircleQueue
		rcvQ.Init(1024)
		t.replrun(&rcvQ)
		rcvQs = append(rcvQs, &rcvQ)
	}
	t.rcvQs = rcvQs
	t.rdmaListener = rdmaListener
	logger.Info("repl server start success")
	return t, nil
}

func (t *replicateTransport) stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	select {
	case <-t.stopc:
		return
	default:
		close(t.stopc)
		t.listener.Close()
		for _, s := range t.senders {
			s.stop()
		}
	}
}

func (t *replicateTransport) send(m *proto.Message) {
	s := t.getSender(m.To)
	s.send(m)
}

func (t *replicateTransport) getSender(nodeId uint64) *transportSender {
	t.mu.RLock()
	sender, ok := t.senders[nodeId]
	t.mu.RUnlock()
	if ok {
		return sender
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if sender, ok = t.senders[nodeId]; !ok {
		sender = newTransportSender(nodeId, uint64(t.config.MaxReplConcurrency), t.config.SendBufferSize, Replicate, t.config.Resolver)
		t.senders[nodeId] = sender
	}
	return sender
}

func (t *replicateTransport) sendSnapshot(m *proto.Message, rs *snapshotStatus) {
	var (
		conn *util.ConnTimeout
		err  error
	)
	defer func() {
		atomic.AddInt32(&t.curSnapshot, -1)
		rs.respond(err)
		if conn != nil {
			conn.Close()
		}
		if err != nil {
			logger.Error("[Transport] %v send snapshot to %v failed error is: %v.", m.ID, m.To, err)
		} else if logger.IsEnableWarn() {
			logger.Warn("[Transport] %v send snapshot to %v successful.", m.ID, m.To)
		}
	}()

	if atomic.AddInt32(&t.curSnapshot, 1) > int32(t.config.MaxSnapConcurrency) {
		err = fmt.Errorf("snapshot concurrency exceed the limit %v.", t.config.MaxSnapConcurrency)
		return
	}

	if conn, err = getConn(context.Background(), m.To, Replicate, t.config.Resolver, 10*time.Minute, 1*time.Minute); err != nil {
		err = fmt.Errorf("can not get connection to %v: %v", m.To, err)
		return
	}

	// send snapshot header message
	bufWr := util.NewBufferWriter(conn, 1*MB)
	if err = m.Encode(bufWr); err != nil {
		return
	}
	if err = bufWr.Flush(); err != nil {
		return
	}

	// send snapshot data
	var (
		data      []byte
		loopCount = 0
		sizeBuf   = make([]byte, 4)
	)
	for err == nil {
		loopCount = loopCount + 1
		if loopCount > 16 {
			loopCount = 0
			runtime.Gosched()
		}

		select {
		case <-rs.stopCh:
			err = fmt.Errorf("raft has shutdown.")

		default:
			data, err = m.Snapshot.Next()
			if len(data) > 0 {
				// write block size
				binary.BigEndian.PutUint32(sizeBuf, uint32(len(data)))
				if _, err = bufWr.Write(sizeBuf); err == nil {
					_, err = bufWr.Write(data)
				}
			}
		}
	}

	// write end flag and flush
	if err != nil && err != io.EOF {
		return
	}
	binary.BigEndian.PutUint32(sizeBuf, 0)
	if _, err = bufWr.Write(sizeBuf); err != nil {
		return
	}
	if err = bufWr.Flush(); err != nil {
		return
	}

	// wait response
	err = nil
	resp := make([]byte, 1)
	io.ReadFull(conn, resp)
	if resp[0] != 1 {
		err = fmt.Errorf("follower response failed.")
	}
}

func (t *replicateTransport) start() {
	util.RunWorkerUtilStop("replicateTransport->start", func() {
		for {
			select {
			case <-t.stopc:
				return
			default:
				conn, err := t.listener.Accept()
				if err != nil {
					continue
				}
				t.handleConn(util.NewConnTimeout(conn))
			}
		}
	}, t.stopc)
}

func (t *replicateTransport) handleConn(conn *util.ConnTimeout) {
	util.RunWorker("replicateTransport->handleConn", func() {
		defer conn.Close()

		//loopCount := 0
		bufRd := util.NewBufferReader(conn, 64*KB)
		for {
			//loopCount = loopCount + 1
			//if loopCount > 16 {
			//	loopCount = 0
			//	//runtime.Gosched()
			//}

			select {
			case <-t.stopc:
				return
			default:
				msg, err := receiveMessage(bufRd)
				if err != nil {
					return
				}
				if msg.Type == proto.ReqMsgSnapShot {
					if err := t.handleSnapshot(msg, conn, bufRd); err != nil {
						return
					}
				} else {
					t.raftServer.reciveMessage(msg)
				}
			}
		}
	})
}

var snap_ack = []byte{1}

func (t *replicateTransport) handleSnapshot(m *proto.Message, conn *util.ConnTimeout, bufRd *util.BufferReader) error {
	conn.SetReadTimeout(time.Minute)
	conn.SetWriteTimeout(15 * time.Second)
	bufRd.Grow(1 * MB)
	req := newSnapshotRequest(m, bufRd)
	t.raftServer.reciveSnapshot(req)

	// wait snapshot result
	if err := req.response(); err != nil {
		logger.Error("[Transport] handle snapshot request from %v error: %v.", m.From, err)
		return err
	}

	_, err := conn.Write(snap_ack)
	return err
}

func (t *replicateTransport) EnqueueRecvData(data []byte) (err error){
	id := proto.GetRaftId(data)
	idx := 0
	if t.queueNum > 1 {
		idx = int(id & (t.queueNum - 1))
	}
	for i := 0; i < 16; i++ {
		err = t.rcvQs[idx].Enqueue(unsafe.Pointer(&(data)))
		if err == nil {
			return nil
		}
	}
	return
}

func (t *replicateTransport) replrun(rcvQ *util.CircleQueue) {
	var loopRunfun = func() {
		loopCount := 0
		//var rcvWaitC chan struct{}
		var n, i uint32
		datas := make([]unsafe.Pointer, 16)
		for {
			loopCount = loopCount + 1
			if loopCount > 8 {
				loopCount = 0
				runtime.Gosched()
			}

			n = rcvQ.Dequeues(16, datas)
			for i = 0; i < n; i++ {
				msg := proto.GetMessage()
				msg.DecodeForRDMA(*(*[]byte)(datas[i]))
				t.raftServer.reciveMessage(msg)
			}

			select {
			case <-t.stopc:
				return
			default:
			}

		}
	}

	util.RunWorkerUtilStop("replica loop", loopRunfun, t.stopc)
}
