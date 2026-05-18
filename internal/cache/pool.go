package cache

import "sync"

const ChunkSize = 4 * 1024 * 1024

type BufferPool struct {
	pool sync.Pool
}

func NewBufferPool() *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() any {
				b := make([]byte, ChunkSize)
				return &b
			},
		},
	}
}

func (p *BufferPool) Get() *[]byte {
	buf := p.pool.Get().(*[]byte)
	*buf = (*buf)[:ChunkSize]
	return buf
}

func (p *BufferPool) Put(buf *[]byte) {
	p.pool.Put(buf)
}
