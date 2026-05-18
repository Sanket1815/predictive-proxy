package engine

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

type TrackerConfig struct {
	StreakThreshold               int
	VelocityThresholdChunksPerSec float64
}

type readState struct {
	lastChunkIndex uint64
	lastReadAt     time.Time
	streakLen      int
	velocityEWMA   float64
}

type VelocityTracker struct {
	cfg        TrackerConfig
	prefetcher *Prefetcher
	log        *zap.Logger

	mu     sync.Mutex
	states map[string]*readState
}

func NewVelocityTracker(cfg TrackerConfig, p *Prefetcher, log *zap.Logger) *VelocityTracker {
	return &VelocityTracker{
		cfg:        cfg,
		prefetcher: p,
		log:        log,
		states:     make(map[string]*readState, 256),
	}
}

func (t *VelocityTracker) Record(objectKey string, chunkIndex uint64) {
	now := time.Now()

	t.mu.Lock()
	st, exists := t.states[objectKey]
	if !exists {
		t.states[objectKey] = &readState{
			lastChunkIndex: chunkIndex,
			lastReadAt:     now,
			streakLen:      1,
		}
		t.mu.Unlock()
		return
	}

	isSequential := chunkIndex == st.lastChunkIndex+1
	elapsed := now.Sub(st.lastReadAt).Seconds()

	if isSequential && elapsed > 0 {
		const alpha = 0.3
		instant := 1.0 / elapsed
		if st.velocityEWMA == 0 {
			st.velocityEWMA = instant
		} else {
			st.velocityEWMA = alpha*instant + (1-alpha)*st.velocityEWMA
		}
		st.streakLen++
	} else {
		st.streakLen = 1
		st.velocityEWMA *= 0.5
	}

	st.lastChunkIndex = chunkIndex
	st.lastReadAt = now

	shouldPrefetch := st.streakLen >= t.cfg.StreakThreshold &&
		st.velocityEWMA >= t.cfg.VelocityThresholdChunksPerSec
	velocity := st.velocityEWMA
	t.mu.Unlock()

	if shouldPrefetch {
		t.log.Debug("sequential streak detected — scheduling prefetch",
			zap.String("object", objectKey),
			zap.Uint64("next_chunk", chunkIndex+1),
			zap.Float64("velocity_chunks_per_sec", velocity),
			zap.Int("streak_len", st.streakLen),
		)
		t.prefetcher.Schedule(objectKey, chunkIndex+1)
	}
}

func (t *VelocityTracker) Evict(objectKey string) {
	t.mu.Lock()
	delete(t.states, objectKey)
	t.mu.Unlock()
}
