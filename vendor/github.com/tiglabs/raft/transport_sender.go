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
	"net"
	"io"
	"runtime"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/cbnet/cbrdma"
	"github.com/tiglabs/raft/logger"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/util"
)

type unreachableReporter func(uint64)

type transportSender struct {
	nodeID      uint64
	concurrency uint64
	senderType  []SocketType
	resolver    SocketResolver
	inputc      []chan *proto.Message
	send        func(msg *proto.Message)
	mu          sync.Mutex
	isStop      bool
	stopc       chan struct{}
	modec       []chan SocketType
	modeWg      sync.WaitGroup
}

func newTransportSender(nodeID, concurrency uint64, buffSize int, senderType SocketType, resolver SocketResolver) *transportSender {
	sender := &transportSender{
		nodeID:      nodeID,
		concurrency: concurrency,
		senderType:  make([]SocketType, concurrency),
		resolver:    resolver,
		inputc:      make([]chan *proto.Message, concurrency),
		stopc:       make(chan struct{}),
		modec:       make([]chan SocketType, concurrency),
	}

	addr, _ := resolver.NodeAddress(nodeID, senderType)
	logger.Info("new sender[%d] concurrency:%d sendType:%v addr:%v", nodeID, concurrency, senderType, addr)
	for i := uint64(0); i < concurrency; i++ {
		sender.senderType[i] = senderType
		sender.inputc[i] = make(chan *proto.Message, buffSize)
		sender.modec[i] = make (chan SocketType, 1)
		sender.loopSend(int(i))
	}

	if (concurrency & (concurrency - 1)) == 0 {
		sender.send = func(msg *proto.Message) {
			idx := 0
			if concurrency > 1 {
				idx = int(msg.ID&concurrency - 1)
			}
			sender.inputc[idx] <- msg
		}
	} else {
		sender.send = func(msg *proto.Message) {
			idx := 0
			if concurrency > 1 {
				idx = int(msg.ID % concurrency)
			}
			sender.inputc[idx] <- msg
		}
	}
	return sender
}

func (s *transportSender) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isStop = true
	select {
	case <-s.stopc:
		return
	default:
		close(s.stopc)
	}
}

func (s *transportSender) changeSocketType(mode SocketType) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isStop {
		return
	}
	needWaitCnt := 0
	for i := uint64(0); i < s.concurrency; i++ {
		if s.senderType[i] != mode {
			s.modeWg.Add(1)
			s.modec[i] <- mode
			needWaitCnt++
		}
	}

	if needWaitCnt != 0 {
		s.modeWg.Wait()
	}
	return
}


func createNewConn(nodeID uint64, mode SocketType, resolver SocketResolver, rdTime, wrTime time.Duration) (writer io.Writer, err error){
	if mode.IsRDMAMode() {
		var addr, ip, portstr string
		var port int
		conn := &cbrdma.RDMAConn{}
		conn.Init(nil, nil, nil, nil, unsafe.Pointer(conn))
		if addr, err = resolver.NodeAddress(nodeID, mode); err != nil {
			return
		}

		if ip, portstr, err = net.SplitHostPort(addr); err != nil {
			return
		}

		if port, err = strconv.Atoi(portstr); err != nil {
			return
		}

		if err = conn.Dial(ip, port, 128 *KB, 8, 0); err != nil {
			return
		}

		writer = conn
		logger.Info("create new rdma conn,-->%s ", addr)
		return
	}

	//go net
	var buffWr *util.BufferWriter
	var conn *util.ConnTimeout
	if conn, err = getConn(context.Background(), nodeID, mode, resolver, rdTime, wrTime); err != nil {
		logger.Error("[Transport] get connection [%v] to [%v] failed: %v", mode, nodeID, err)
		return nil, err
	}
	buffWr = util.NewBufferWriter(conn, 128 * KB)
	writer = buffWr
	return
}

func closeConn(mode SocketType, writer io.Writer) (err error) {
	if mode.IsRDMAMode() {
		conn := writer.(*cbrdma.RDMAConn)
		err = conn.Close()
		return
	}
	buffW := writer.(*util.BufferWriter)
	conn := buffW.GetWriter().(*util.ConnTimeout)
	err = conn.Close()
	return
}

func (s *transportSender) SendMsg(index int, mode SocketType, conn unsafe.Pointer) (error)  {
	if mode.IsTCPMode() {
		return nil
	}

	if mode.IsRDMAMode() {
		return nil
	}

	return nil
}

func batchSendMsgGo(recvc chan *proto.Message,writer io.Writer, msg *proto.Message) (err error) {
	bufWr := writer.(*util.BufferWriter)
	if err = msg.Encode(bufWr); err != nil {
		bufWr.Flush()
		proto.ReturnMessage(msg)
		return
	}
	proto.ReturnMessage(msg)

	defer func() {
		if err == nil {
			err = bufWr.Flush()
		}
	}()

	for i := 0; i < 16; i++ {
		select {
		case msg = <-recvc:
			err = msg.Encode(bufWr)
			proto.ReturnMessage(msg)
			if err != nil {
				bufWr.Flush()
				return
			}
		default:
			return
		}
	}
	return
}

func batchSendMsgRDMA(recvc chan *proto.Message, conn *cbrdma.RDMAConn, msg *proto.Message) (err error) {

	err = msg.EncodeForRDMA(conn)
	if err != nil {
		return
	}
	proto.ReturnMessage(msg)
	for i := 0; i < 16; i++ {
		select {
		case msg = <-recvc:
			err = msg.EncodeForRDMA(conn)
			if err != nil {
				return
			}
			proto.ReturnMessage(msg)
		default:
		}
	}

	return
}

func batchSendMsg(recvc chan *proto.Message, conn io.Writer, msg *proto.Message, mode SocketType) (err error) {
	if mode.IsTCPMode() {
		//go net
		return batchSendMsgGo(recvc, conn, msg)
	}

	if mode.IsRDMAMode() {
		// rdma
		return batchSendMsgRDMA(recvc, conn.(*cbrdma.RDMAConn), msg)
	}
	return
}

func (s *transportSender) loopSend(index int) {
	var loopSendFunc = func() {
		var conn io.Writer
		var err error
		curMode := s.senderType[index]
		loopCount := 0
		lastRdmaErr := int64(0)

		inputc := s.inputc[index]

		if conn, err = createNewConn( s.nodeID, curMode, s.resolver, 0, 2*time.Second); err != nil {
			logger.Error("[Transport] get connection [%v] to [%v] failed: %v", s.senderType, s.nodeID, err)
		}

		defer func() {
			if conn != nil {
				closeConn(curMode, conn)
			}
		}()

		for {
			loopCount = loopCount + 1
			if loopCount > 8 {
				loopCount = 0
				runtime.Gosched()
			}

			select {
			case <-s.stopc:
				return
			case mode :=  <-s.modec[index]:
				if mode == ReplicateRDMA && curMode == Replicate {
					//tcp --> RDMA
					if time.Now().Unix() - lastRdmaErr < defaultFailBackUCXSec {
						s.modeWg.Done()
						continue
					}
				}
				if mode != curMode {
					//mode change, close current conn wait next new conn
					if conn != nil {
						closeConn(curMode, conn)
					}
					s.senderType[index] = mode
					curMode = mode
					conn = nil
				}
				s.modeWg.Done()

			case msg := <-s.inputc[index]://inputc:
				if conn == nil {
					if conn, err = createNewConn( s.nodeID, curMode, s.resolver, 0, 2*time.Second); err != nil {
						logger.Error("[Transport] get connection [%v] to [%v] failed: %v", s.senderType, s.nodeID, err)
						proto.ReturnMessage(msg)
						// reset chan
						for {
							select {
							case msg := <-s.inputc[index]:
								proto.ReturnMessage(msg)
								continue
							default:
							}
							break
						}
						time.Sleep(50 * time.Millisecond)
						continue
					}
				}
				err = batchSendMsg(inputc, conn, msg, curMode)
				if err != nil {
					addr, _ := s.resolver.NodeAddress(s.nodeID, curMode)
					logger.Error("[Transport] send message[%d] to %v[%v] error:[%v].", s.senderType, s.nodeID, addr, err)
					closeConn(curMode, conn)
					if curMode == ReplicateRDMA {
						curMode = Replicate
						lastRdmaErr = time.Now().Unix()
						s.senderType[index] = curMode
					}
					conn = nil
					continue
				}
			}
		}
	}
	util.RunWorkerUtilStop("transportSender->loopSend", loopSendFunc, s.stopc)
}

func getConn(ctx context.Context, nodeID uint64, socketType SocketType, resolver SocketResolver, rdTime, wrTime time.Duration) (conn *util.ConnTimeout, err error) {

	var addr string

	defer func() {
		if err != nil {
			conn = nil
		}
	}()

	if addr, err = resolver.NodeAddress(nodeID, socketType); err != nil {
		return
	}

	if conn, err = util.DialTimeout(addr, 2*time.Second); err != nil {
		return
	}

	conn.SetReadTimeout(rdTime)
	conn.SetWriteTimeout(wrTime)
	logger.Info("create new tcp conn,-->%s ", addr)
	return
}
