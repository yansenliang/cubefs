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

package util

import (
	"errors"
	"time"
	"sync"
	"unsafe"
)

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
)

const time_format = "2006-01-02 15:04:05.000"

type Uint64Slice []uint64

func (p Uint64Slice) Len() int           { return len(p) }
func (p Uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func Min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func Max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func FormatDate(t time.Time) string {
	return t.Format(time_format)
}

func FormatTimestamp(t int64) string {
	if t <= 0 {
		return ""
	}
	return time.Unix(0, t).Format(time_format)
}

const (
	WaitCircleQueue = iota
	NotWaitCircleQueue
)

var (
	QueueFullErr = errors.New("the queue is full.")
)

type CircleQueue struct {
	head    uint32
	tail    uint32
	size    uint32
	usedCnt uint32
	msk     uint32
	mux     sync.Mutex
	//notEmptyC chan struct{}
	//notFullC  chan struct{}
	buffer    []unsafe.Pointer
	queueType int
}

func roundup_power_two(size uint32) uint32 {
	var resize uint32
	resize = 1 << 1
	if (size & (size - 1)) == 0 {
		return size
	}
	for {
		if (size >> 1) == 0 {
			break
		}
		size = size >> 1
		resize = resize << 1
	}
	return resize
}

func (q *CircleQueue) Init(size uint32) {
	resize := roundup_power_two(size)
	q.buffer = make([]unsafe.Pointer, resize)
	q.size = resize
	q.msk = resize - 1
	q.head = 0
	q.tail = 0
	q.usedCnt = 0
	//q.notEmptyC = make(chan struct{}, 1)
	//q.notFullC = make(chan struct{}, 1)
	return
}

func (q *CircleQueue) Size() uint32 {
	q.mux.Lock()
	defer q.mux.Unlock()
	return q.usedCnt
}

func (q *CircleQueue) Full() bool {

	if q.usedCnt == q.size {
		return true
	}

	return false
}

func (q *CircleQueue) Empty() bool {
	if q.usedCnt == 0 {
		return true
	}
	return false
}

func (q *CircleQueue) Enqueue(data unsafe.Pointer) error {
	//var emptyToNot bool
	if q.Full() {
		//logger.Warn("CircleQueue.Enqueue: full!")
		return QueueFullErr
	}

	q.mux.Lock()
	/*
		defer func() {
			q.mux.Unlock()

				if emptyToNot {
					//logger.Warn("CircleQueue.Enqueue: empty to not!")
					q.notEmptyC <- struct{}{}
				}

			//logger.Warn("CircleQueue.enqueue: %d, %d, %d!",q.head, q.tail, q.usedCnt)

		}()
	*/
	if q.Full() {
		//logger.Warn("CircleQueue.Enqueue: lock full!")
		q.mux.Unlock()
		return QueueFullErr
	}
	//clearChannelMsg(q.notFullC)
	/*
		if q.usedCnt == 0 {

			emptyToNot = true

		}
	*/

	index := q.tail & q.msk

	q.buffer[index] = data
	q.tail += 1
	q.usedCnt++
	q.mux.Unlock()
	return nil
}

func clearChannelMsg(c chan struct{}) {
	for {
		select {
		case <-c:
		default:
			return
		}
	}
}

func (q *CircleQueue) Dequeue() unsafe.Pointer {
	//var fullToNot bool
	if q.Empty() {
		//logger.Warn("CircleQueue.Dequeue: empty!")
		return nil
	}

	q.mux.Lock()
	/*
		defer func() {
			q.mux.Unlock()
				if fullToNot {
					logger.Warn("CircleQueue.Dequeue: full to not!")
					close(q.notFullC)
					q.notFullC = make(chan struct{},1)
				}

			//logger.Warn("CircleQueue.dequeue: %d, %d, %di!",q.head, q.tail, q.usedCnt)
		}()
	*/
	if q.Empty() {
		//logger.Warn("CircleQueue.Dequeue: lock empty!")
		q.mux.Unlock()
		return nil
	}

	//clearChannelMsg(q.notEmptyC)
	/*
		if q.usedCnt == q.size {
			fullToNot = true
		}
	*/
	index := q.head & q.msk
	msg := q.buffer[index]
	q.buffer[index] = nil
	q.head++
	q.usedCnt--
	q.mux.Unlock()
	return msg
}

func (q *CircleQueue) Dequeues(max uint32, results []unsafe.Pointer) uint32 {
	//var fullToNot bool
	resultLen := uint32(len(results))
	if resultLen == 0 {
		return 0
	}
	if q.Empty() {
		//logger.Warn("CircleQueue.Dequeue: empty!")
		return 0

	}

	q.mux.Lock()
	/*
		defer func() {
			q.mux.Unlock()

			if fullToNot {
				logger.Warn("CircleQueue.Dequeue: full to not!")
				close(q.notFullC)
				q.notFullC = make(chan struct{},1)
			}

			//logger.Warn("CircleQueue.dequeues: %d, %d, %di!",q.head, q.tail, q.usedCnt)
		}()
	*/

	if q.Empty() {
		//logger.Warn("CircleQueue.Dequeue: lock empty!")
		q.mux.Unlock()
		return 0

	}
	/*
		if q.usedCnt == q.size {
			fullToNot = true
		}
	*/
	//	clearChannelMsg(q.notEmptyC)

	dequeueNum := max
	if q.usedCnt < max {
		dequeueNum = q.usedCnt
	}

	if resultLen < dequeueNum {
		dequeueNum = resultLen
	}

	index := q.head & q.msk
	var i uint32
	if (index + dequeueNum) > q.size {
		for i = 0; index < q.size; i++ {
			results[i] = q.buffer[index]
			q.buffer[index] = nil
			index++
		}
		index = 0
		for ; i < dequeueNum; i++ {
			results[i] = q.buffer[index]
			q.buffer[index] = nil
			index++
		}
	} else {
		for ; i < dequeueNum; i++ {
			results[i] = q.buffer[index]
			q.buffer[index] = nil
			index++
		}
	}
	//logger.Warn("CircleQueue.Dequeues:begin  %d, %d, %d, dequueNum-%d!",q.head, q.tail, q.usedCnt,dequeueNum)
	q.usedCnt -= dequeueNum
	q.head += dequeueNum
	//logger.Warn("CircleQueue.Dequeues:end  %d, %d, %d, dequueNum-%d!",q.head, q.tail, q.usedCnt,dequeueNum)
	q.mux.Unlock()
	return dequeueNum
}