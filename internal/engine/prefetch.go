package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/nadsanket7/go-predictive-proxy/internal/cache"
	"github.com/nadsanket7/go-predictive-proxy/internal/metrics"
	"go.uber.org/zap"
)

// PrefetchConfig controls the worker pool and look-ahead window.
type PrefetchConfig struct {
	// Workers is the number of concurrent background fetch goroutines.
	// Each worker maintains its own backend connection from the pool.
	Workers int

	// QueueDepth is the capacity of the bounded job channel. When full,
	// Schedule drops jobs silently, providing natural backpressure without
	// ever blocking the request-serving goroutine.
	QueueDepth int

	// LookAhead is the number of chunks ahead of the current position that
	// Schedule will enqueue per trigger event.
	LookAhead int
}

// prefetchJob describes a single chunk that should be speculatively cached.
type prefetchJob struct {
	objectKey  string
	chunkIndex uint64
}

// BackendFetcher is the interface the Prefetcher uses to load chunks from the
// origin. proxy.Backend implements this interface; defining it here in the
// engine package breaks the engine→proxy import cycle.
type BackendFetcher interface {
	FetchChunk(ctx context.Context, objectKey string, chunkIndex uint64, buf *[]byte) (int, error)
}

// Prefetcher manages a fixed-size pool of worker goroutines that speculatively
// load upcoming chunks into the hot cache before the client requests them.
//
// Backpressure design: Schedule performs a non-blocking channel send. If the
// channel is full (all workers busy), the excess jobs are dropped and counted
// in the PrefetchDropped metric rather than stalling the caller.
type Prefetcher struct {
	cfg      PrefetchConfig
	backend  BackendFetcher
	hot      *cache.HotCache
	pool     *cache.BufferPool
	log      *zap.Logger
	reg      *metrics.Registry
	jobs     chan prefetchJob
	wg       sync.WaitGroup
	cancelFn context.CancelFunc
}

// NewPrefetcher constructs a Prefetcher. Call Start to launch workers.
func NewPrefetcher(
	cfg PrefetchConfig,
	backend BackendFetcher,
	hot *cache.HotCache,
	pool *cache.BufferPool,
	log *zap.Logger,
	reg *metrics.Registry,
) *Prefetcher {
	return &Prefetcher{
		cfg:     cfg,
		backend: backend,
		hot:     hot,
		pool:    pool,
		log:     log,
		reg:     reg,
		jobs:    make(chan prefetchJob, cfg.QueueDepth),
	}
}

// Start launches the configured number of worker goroutines. It is safe to
// call only once; call Stop before calling Start again.
func (p *Prefetcher) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancelFn = cancel
	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}
	p.log.Info("prefetch pool started", zap.Int("workers", p.cfg.Workers), zap.Int("queue_depth", p.cfg.QueueDepth))
}

// Stop signals all workers to finish their current job and exit, then waits
// for the pool to drain. Pending jobs in the channel are abandoned.
func (p *Prefetcher) Stop() {
	p.cancelFn()
	close(p.jobs)
	p.wg.Wait()
	p.log.Info("prefetch pool stopped")
}

// Schedule enqueues up to LookAhead prefetch jobs starting at startChunk.
// It is intentionally non-blocking: jobs are dropped if the channel is full.
func (p *Prefetcher) Schedule(objectKey string, startChunk uint64) {
	for i := 0; i < p.cfg.LookAhead; i++ {
		job := prefetchJob{objectKey: objectKey, chunkIndex: startChunk + uint64(i)}
		select {
		case p.jobs <- job:
			p.reg.PrefetchQueued.Inc()
			p.reg.PrefetchQueueLength.Inc()
		default:
			p.reg.PrefetchDropped.Inc()
		}
	}
}

// QueueDepth returns the number of jobs currently waiting in the channel.
func (p *Prefetcher) QueueDepth() int { return len(p.jobs) }

// worker is the main loop for a single prefetch goroutine. It exits when
// either ctx is cancelled or the jobs channel is closed by Stop.
func (p *Prefetcher) worker(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-p.jobs:
			if !ok {
				return
			}
			p.reg.PrefetchQueueLength.Dec()
			p.fetchAndCache(ctx, job)
		}
	}
}

// fetchAndCache fetches a single chunk from the backend and stores it in the
// hot cache. It is a no-op if the chunk is already cached.
func (p *Prefetcher) fetchAndCache(ctx context.Context, job prefetchJob) {
	key := cache.ChunkKey{ObjectKey: job.objectKey, ChunkIndex: job.chunkIndex}

	// Short-circuit: avoid a backend round-trip if another goroutine already
	// cached this chunk (e.g., the client issued the request before we ran).
	if _, hit := p.hot.Get(key); hit {
		return
	}

	buf := p.pool.Get()
	defer p.pool.Put(buf)

	n, err := p.backend.FetchChunk(ctx, job.objectKey, job.chunkIndex, buf)
	if err != nil {
		// Log at Warn rather than Error: prefetch failures are non-fatal; the
		// next real request for this chunk will fall through to the backend.
		p.log.Warn("prefetch fetch failed",
			zap.String("object", job.objectKey),
			zap.Uint64("chunk", job.chunkIndex),
			zap.Error(err),
		)
		p.reg.PrefetchErrors.Inc()
		return
	}

	// Copy out of the pool buffer into a heap-owned slice before returning the
	// buffer. The hot cache takes ownership of the heap slice.
	owned := make([]byte, n)
	copy(owned, (*buf)[:n])
	p.hot.Put(key, owned)
	p.reg.PrefetchHits.Inc()

	p.log.Debug("prefetch stored",
		zap.String("object", job.objectKey),
		zap.Uint64("chunk", job.chunkIndex),
		zap.String("bytes", fmt.Sprintf("%d", n)),
	)
}
