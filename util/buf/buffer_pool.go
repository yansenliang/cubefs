package buf

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/util"
	"golang.org/x/time/rate"
)

const (
	HeaderBufferPoolSize = 8192
	InvalidLimit         = 0
)

var ReadBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 32*1024)
		return b
	},
}

const (
	BufferTypeHeader    = 0
	BufferTypeNormal    = 1
	BufferTypeHeaderVer = 2
	BufferTypeRepair    = 3
)

var (
	tinyBuffersTotalLimit    int64 = 4096
	NormalBuffersTotalLimit  int64
	HeadBuffersTotalLimit    int64
	HeadVerBuffersTotalLimit int64
	RepairBuffersTotalLimit  int64
)

var (
	tinyBuffersCount    int64
	normalBuffersCount  int64
	headBuffersCount    int64
	headVerBuffersCount int64
	repairBuffersCount  int64
)

var (
	normalBufAllocId  uint64
	headBufAllocId    uint64
	headBufVerAllocId uint64
	repairBufAllocId  uint64
)

var (
	normalBufFreecId uint64
	headBufFreeId    uint64
	headBufVerFreeId uint64
	repairBufFreeId  uint64
)

var (
	buffersRateLimit        = rate.NewLimiter(rate.Limit(16), 16)
	normalBuffersRateLimit  = rate.NewLimiter(rate.Limit(16), 16)
	headBuffersRateLimit    = rate.NewLimiter(rate.Limit(16), 16)
	headVerBuffersRateLimit = rate.NewLimiter(rate.Limit(16), 16)
	repairBuffersRateLimit  = rate.NewLimiter(rate.Limit(16), 16)
)

func NewTinyBufferPool() *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			if atomic.LoadInt64(&tinyBuffersCount) >= tinyBuffersTotalLimit {
				ctx := context.Background()
				buffersRateLimit.Wait(ctx)
			}
			return make([]byte, util.DefaultTinySizeLimit)
		},
	}
}

func NewHeadVerBufferPool() *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			if HeadVerBuffersTotalLimit != InvalidLimit && atomic.LoadInt64(&headVerBuffersCount) >= HeadVerBuffersTotalLimit {
				ctx := context.Background()
				headVerBuffersRateLimit.Wait(ctx)
			}
			return make([]byte, util.PacketHeaderVerSize)
		},
	}
}

func NewHeadBufferPool() *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			if HeadBuffersTotalLimit != InvalidLimit && atomic.LoadInt64(&headBuffersCount) >= HeadBuffersTotalLimit {
				ctx := context.Background()
				headBuffersRateLimit.Wait(ctx)
			}
			return make([]byte, util.PacketHeaderSize)
		},
	}
}

func NewNormalBufferPool() *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			if NormalBuffersTotalLimit != InvalidLimit && atomic.LoadInt64(&normalBuffersCount) >= NormalBuffersTotalLimit {
				ctx := context.Background()
				normalBuffersRateLimit.Wait(ctx)
			}
			return make([]byte, util.BlockSize)
		},
	}
}

func NewRepiarBufferPool() *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			if RepairBuffersTotalLimit != InvalidLimit && atomic.LoadInt64(&repairBuffersCount) >= RepairBuffersTotalLimit {
				ctx := context.Background()
				repairBuffersRateLimit.Wait(ctx)
			}
			return make([]byte, util.RepairReadBlockSize)
		},
	}
}

// BufferPool defines the struct of a buffered pool with 4 objects.
type BufferPool struct {
	headPools    []chan []byte
	headVerPools []chan []byte
	normalPools  []chan []byte
	repairPools  []chan []byte
	tinyPool     *sync.Pool
	headPool     *sync.Pool
	normalPool   *sync.Pool
	headVerPool  *sync.Pool
	repairPool   *sync.Pool
}

var slotCnt = uint64(16)

// NewBufferPool returns a new buffered pool.
func NewBufferPool() (bufferP *BufferPool) {
	bufferP = &BufferPool{}
	bufferP.headPools = make([]chan []byte, slotCnt)
	bufferP.normalPools = make([]chan []byte, slotCnt)
	bufferP.headVerPools = make([]chan []byte, slotCnt)
	bufferP.repairPools = make([]chan []byte, slotCnt)
	for i := 0; i < int(slotCnt); i++ {
		bufferP.headPools[i] = make(chan []byte, HeaderBufferPoolSize/slotCnt)
		bufferP.headVerPools[i] = make(chan []byte, HeaderBufferPoolSize/slotCnt)
		bufferP.normalPools[i] = make(chan []byte, HeaderBufferPoolSize/slotCnt)
		bufferP.repairPools[i] = make(chan []byte, HeaderBufferPoolSize/slotCnt)
	}
	bufferP.tinyPool = NewTinyBufferPool()
	bufferP.headPool = NewHeadBufferPool()
	bufferP.headVerPool = NewHeadVerBufferPool()
	bufferP.normalPool = NewNormalBufferPool()
	bufferP.repairPool = NewRepiarBufferPool()
	return bufferP
}

func (bufferP *BufferPool) getHead(id uint64) (data []byte) {
	select {
	case data = <-bufferP.headPools[id%slotCnt]:
		return
	default:
		return bufferP.headPool.Get().([]byte)
	}
}

func (bufferP *BufferPool) getHeadVer(id uint64) (data []byte) {
	select {
	case data = <-bufferP.headVerPools[id%slotCnt]:
		return
	default:
		return bufferP.headVerPool.Get().([]byte)
	}
}

func (bufferP *BufferPool) getNormal(id uint64) (data []byte) {
	select {
	case data = <-bufferP.normalPools[id%slotCnt]:
		return
	default:
		return bufferP.normalPool.Get().([]byte)
	}
}

func (bufferP *BufferPool) getRepair(id uint64) (data []byte) {
	select {
	case data = <-bufferP.repairPools[id%slotCnt]:
		return
	default:
		return bufferP.repairPool.Get().([]byte)
	}
}

// Get returns the data based on the given size. Different size corresponds to different object in the pool.
func (bufferP *BufferPool) Get(size int) (data []byte, err error) {
	if size == util.PacketHeaderSize {
		atomic.AddInt64(&headBuffersCount, 1)
		id := atomic.AddUint64(&headBufAllocId, 1)
		return bufferP.getHead(id), nil
	} else if size == util.PacketHeaderVerSize {
		atomic.AddInt64(&headVerBuffersCount, 1)
		id := atomic.AddUint64(&headBufVerAllocId, 1)
		return bufferP.getHeadVer(id), nil
	} else if size == util.BlockSize {
		atomic.AddInt64(&normalBuffersCount, 1)
		id := atomic.AddUint64(&normalBufAllocId, 1)
		return bufferP.getNormal(id), nil
	} else if size == util.RepairReadBlockSize {
		atomic.AddInt64(&repairBuffersCount, 1)
		id := atomic.AddUint64(&repairBufAllocId, 1)
		return bufferP.getRepair(id), nil
	} else if size == util.DefaultTinySizeLimit {
		atomic.AddInt64(&tinyBuffersCount, 1)
		return bufferP.tinyPool.Get().([]byte), nil
	}
	return nil, fmt.Errorf("can only support 45 or 65536 bytes")
}

func (bufferP *BufferPool) putHead(index int, data []byte) {
	select {
	case bufferP.headPools[index] <- data:
		return
	default:
		bufferP.headPool.Put(data) // nolint: staticcheck
	}
}

func (bufferP *BufferPool) putHeadVer(index int, data []byte) {
	select {
	case bufferP.headVerPools[index] <- data:
		return
	default:
		bufferP.headVerPool.Put(data) // nolint: staticcheck
	}
}

func (bufferP *BufferPool) putNormal(index int, data []byte) {
	select {
	case bufferP.normalPools[index] <- data:
		return
	default:
		bufferP.normalPool.Put(data) // nolint: staticcheck
	}
}

func (bufferP *BufferPool) putRepair(index int, data []byte) {
	select {
	case bufferP.repairPools[index] <- data:
		return
	default:
		bufferP.repairPool.Put(data) // nolint: staticcheck
	}
}

// Put puts the given data into the buffer pool.
func (bufferP *BufferPool) Put(data []byte) {
	if data == nil {
		return
	}
	size := len(data)
	if size == util.PacketHeaderSize {
		atomic.AddInt64(&headBuffersCount, -1)
		id := atomic.AddUint64(&headBufFreeId, 1)
		bufferP.putHead(int(id%slotCnt), data)
	} else if size == util.PacketHeaderVerSize {
		atomic.AddInt64(&headVerBuffersCount, -1)
		id := atomic.AddUint64(&headBufVerFreeId, 1)
		bufferP.putHeadVer(int(id%slotCnt), data)
	} else if size == util.BlockSize {
		atomic.AddInt64(&normalBuffersCount, -1)
		id := atomic.AddUint64(&normalBufFreecId, 1)
		bufferP.putNormal(int(id%slotCnt), data)
	} else if size == util.RepairReadBlockSize {
		atomic.AddInt64(&repairBuffersCount, -1)
		id := atomic.AddUint64(&repairBufFreeId, 1)
		bufferP.putRepair(int(id%slotCnt), data)
	} else if size == util.DefaultTinySizeLimit {
		bufferP.tinyPool.Put(data) // nolint: staticcheck
		atomic.AddInt64(&tinyBuffersCount, -1)
	}
}
