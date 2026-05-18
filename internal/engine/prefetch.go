package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/nadsanket7/go-predictive-proxy/internal/cache"
	"github.com/nadsanket7/go-predictive-proxy/internal/metrics"
	"go.uber.org/zap"
)

type PrefetchConfig struct {
	Workers    int
	QueueDepth int
	LookAhead  int
}

type prefetchJob struct {
	objectKey  string
	chunkIndex uint64
}

type BackendFetcher interface {
	FetchChunk(ctx context.Context, objectKey string, chunkIndex uint64, buf *[]byte) (int, error)
}

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

func (p *Prefetcher) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancelFn = cancel
	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}
	p.log.Info("prefetch pool started", zap.Int("workers", p.cfg.Workers), zap.Int("queue_depth", p.cfg.QueueDepth))
}

func (p *Prefetcher) Stop() {
	p.cancelFn()
	close(p.jobs)
	p.wg.Wait()
	p.log.Info("prefetch pool stopped")
}

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

func (p *Prefetcher) QueueDepth() int { return len(p.jobs) }

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

func (p *Prefetcher) fetchAndCache(ctx context.Context, job prefetchJob) {
	key := cache.ChunkKey{ObjectKey: job.objectKey, ChunkIndex: job.chunkIndex}

	if _, hit := p.hot.Get(key); hit {
		return
	}

	buf := p.pool.Get()
	defer p.pool.Put(buf)

	n, err := p.backend.FetchChunk(ctx, job.objectKey, job.chunkIndex, buf)
	if err != nil {
		p.log.Warn("prefetch fetch failed",
			zap.String("object", job.objectKey),
			zap.Uint64("chunk", job.chunkIndex),
			zap.Error(err),
		)
		p.reg.PrefetchErrors.Inc()
		return
	}

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
