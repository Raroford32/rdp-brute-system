package pool

import (
	"sync"
)

// MemoryPool manages reusable memory buffers to reduce GC pressure
type MemoryPool struct {
	small  sync.Pool // 1KB buffers
	medium sync.Pool // 16KB buffers  
	large  sync.Pool // 64KB buffers
}

// NewMemoryPool creates a new memory pool
func NewMemoryPool() *MemoryPool {
	return &MemoryPool{
		small: sync.Pool{
			New: func() interface{} {
				buf := make([]byte, 1024)
				return &buf
			},
		},
		medium: sync.Pool{
			New: func() interface{} {
				buf := make([]byte, 16*1024)
				return &buf
			},
		},
		large: sync.Pool{
			New: func() interface{} {
				buf := make([]byte, 64*1024)
				return &buf
			},
		},
	}
}

// GetBuffer retrieves a buffer of the requested size
func (mp *MemoryPool) GetBuffer(size int) *[]byte {
	switch {
	case size <= 1024:
		return mp.small.Get().(*[]byte)
	case size <= 16*1024:
		return mp.medium.Get().(*[]byte)
	default:
		return mp.large.Get().(*[]byte)
	}
}

// PutBuffer returns a buffer to the pool
func (mp *MemoryPool) PutBuffer(buf *[]byte) {
	if buf == nil {
		return
	}
	
	size := cap(*buf)
	// Clear the buffer before returning to pool
	*buf = (*buf)[:0]
	
	switch {
	case size <= 1024:
		mp.small.Put(buf)
	case size <= 16*1024:
		mp.medium.Put(buf)
	default:
		mp.large.Put(buf)
	}
}

// ByteSlicePool manages byte slices of varying sizes
type ByteSlicePool struct {
	pools map[int]*sync.Pool
	mu    sync.RWMutex
}

// NewByteSlicePool creates a new byte slice pool
func NewByteSlicePool() *ByteSlicePool {
	return &ByteSlicePool{
		pools: make(map[int]*sync.Pool),
	}
}

// Get retrieves a byte slice of at least the requested size
func (bsp *ByteSlicePool) Get(size int) []byte {
	// Round up to nearest power of 2
	allocSize := 1
	for allocSize < size {
		allocSize <<= 1
	}
	
	bsp.mu.RLock()
	pool, exists := bsp.pools[allocSize]
	bsp.mu.RUnlock()
	
	if !exists {
		bsp.mu.Lock()
		pool, exists = bsp.pools[allocSize]
		if !exists {
			pool = &sync.Pool{
				New: func() interface{} {
					return make([]byte, allocSize)
				},
			}
			bsp.pools[allocSize] = pool
		}
		bsp.mu.Unlock()
	}
	
	buf := pool.Get().([]byte)
	return buf[:size]
}

// Put returns a byte slice to the pool
func (bsp *ByteSlicePool) Put(buf []byte) {
	if len(buf) == 0 {
		return
	}
	
	// Find the appropriate pool based on capacity
	allocSize := 1
	for allocSize < cap(buf) {
		allocSize <<= 1
	}
	
	bsp.mu.RLock()
	pool, exists := bsp.pools[allocSize]
	bsp.mu.RUnlock()
	
	if exists {
		// Clear the slice before returning
		buf = buf[:0]
		pool.Put(buf)
	}
}