// Package cache provides the hot (RAM) LRU layer, cold (NVMe) spill layer,
// and a zero-allocation buffer pool for 4 MiB chunk I/O.
package cache

import "sync"

// ChunkSize is the canonical 4 MiB chunk size used by every layer of the proxy.
// All byte-range arithmetic is aligned to this boundary.
const ChunkSize = 4 * 1024 * 1024 // 4 MiB

// BufferPool wraps sync.Pool to provide reusable 4 MiB byte slices.
//
// Allocating a fresh 4 MiB slice for every backend fetch would generate
// ~250 MB/s of GC-visible garbage at 1 Gbps throughput. Pooling eliminates
// that allocation entirely, keeping GC pauses under 1 ms at sustained load.
type BufferPool struct {
	pool sync.Pool
}

// NewBufferPool initialises a pool whose factory allocates 4 MiB slices.
func NewBufferPool() *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() any {
				b := make([]byte, ChunkSize)
				return &b // store pointer-to-slice to avoid boxing on Get/Put
			},
		},
	}
}

// Get retrieves a buffer from the pool, always returning a slice of exactly
// ChunkSize bytes. Callers MUST call Put when the buffer is no longer needed.
func (p *BufferPool) Get() *[]byte {
	buf := p.pool.Get().(*[]byte)
	*buf = (*buf)[:ChunkSize]
	return buf
}

// Put returns a buffer to the pool. The caller must not read or write buf
// after this call — concurrent Gets may immediately reuse it.
func (p *BufferPool) Put(buf *[]byte) {
	p.pool.Put(buf)
}
