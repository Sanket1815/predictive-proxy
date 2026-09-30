package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nadsanket7/go-predictive-proxy/internal/cache"
	"github.com/nadsanket7/go-predictive-proxy/internal/engine"
	"github.com/nadsanket7/go-predictive-proxy/internal/metrics"
	"go.uber.org/zap"
)

// TierHeader tells the client which tier served the slowest chunk of its request.
const TierHeader = "X-Cache-Tier"

// Tiers from fastest to slowest. "inflight" means the chunk was already being
// fetched (usually by the prefetcher) and the request waited on that fetch.
var tierRank = map[string]int{"hot": 0, "cold": 1, "inflight": 2, "backend": 3}

type HandlerConfig struct {
	HotCache  *cache.HotCache
	ColdCache *cache.ColdCache
	Backend   *Backend
	Pool      *cache.BufferPool
	Tracker   *engine.VelocityTracker
	Metrics   *metrics.Registry
	Logger    *zap.Logger
}

type Handler struct {
	cfg HandlerConfig
}

func NewHandler(cfg HandlerConfig) *Handler {
	return &Handler{cfg: cfg}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	objectKey := strings.TrimPrefix(r.URL.Path, "/")
	if objectKey == "" {
		http.Error(w, "missing object key in path", http.StatusBadRequest)
		return
	}

	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" && r.Method == http.MethodGet {
		h.proxyFull(w, r, objectKey)
		return
	}

	size, err := h.cfg.Backend.ObjectSize(r.Context(), objectKey)
	if err != nil {
		h.cfg.Logger.Error("object size lookup failed", zap.String("object", objectKey), zap.Error(err))
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}

	// Plain HEAD: video players, PDF viewers and analytic engines use it to
	// learn the file size before issuing ranged reads.
	if rangeHeader == "" {
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Accept-Ranges", "bytes")
		w.WriteHeader(http.StatusOK)
		return
	}

	startByte, endByte, ok := resolveByteRange(rangeHeader, size)
	if !ok {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}

	contentLength := endByte - startByte + 1
	chunkStart := uint64(startByte / cache.ChunkSize)
	chunkEnd := uint64(endByte / cache.ChunkSize)

	// Resolve every chunk before writing headers so the tier header reflects
	// the slowest chunk and a backend failure can still become a 502.
	chunks := make([][]byte, 0, chunkEnd-chunkStart+1)
	cacheTier := "hot"
	for chunkIdx := chunkStart; chunkIdx <= chunkEnd; chunkIdx++ {
		key := cache.ChunkKey{ObjectKey: objectKey, ChunkIndex: chunkIdx}
		chunkData, tier, err := h.resolveChunk(r.Context(), key)
		if err != nil {
			h.cfg.Logger.Error("chunk resolution failed",
				zap.String("object", objectKey),
				zap.Uint64("chunk", chunkIdx),
				zap.Error(err),
			)
			http.Error(w, "upstream error", http.StatusBadGateway)
			return
		}
		if tierRank[tier] > tierRank[cacheTier] {
			cacheTier = tier
		}
		chunks = append(chunks, chunkData)
		h.cfg.Tracker.Record(objectKey, chunkIdx)
	}

	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", startByte, endByte, size))
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set(TierHeader, cacheTier)
	w.WriteHeader(http.StatusPartialContent)

	var bytesServed int64
	if r.Method == http.MethodGet {
		for i, chunkData := range chunks {
			chunkByteBase := int64(chunkStart+uint64(i)) * cache.ChunkSize
			sliceStart := int64(0)
			sliceEnd := int64(len(chunkData))

			if startByte > chunkByteBase {
				sliceStart = startByte - chunkByteBase
			}
			if endByte-chunkByteBase+1 < sliceEnd {
				sliceEnd = endByte - chunkByteBase + 1
			}
			if sliceStart >= sliceEnd {
				break
			}

			n, writeErr := w.Write(chunkData[sliceStart:sliceEnd])
			bytesServed += int64(n)
			if writeErr != nil {
				return
			}
		}
	}

	elapsed := time.Since(start).Seconds()
	h.cfg.Metrics.BytesServed.Add(float64(bytesServed))
	h.cfg.Metrics.RequestLatency.WithLabelValues(cacheTier).Observe(elapsed)
	h.cfg.Metrics.RequestsTotal.WithLabelValues("206", cacheTier).Inc()

	h.cfg.Logger.Debug("range request served",
		zap.String("object", objectKey),
		zap.Int64("bytes", bytesServed),
		zap.String("tier", cacheTier),
		zap.Float64("latency_ms", elapsed*1000),
	)
}

func (h *Handler) resolveChunk(ctx context.Context, key cache.ChunkKey) ([]byte, string, error) {
	if data, ok := h.cfg.HotCache.Get(key); ok {
		h.cfg.Metrics.CacheHitTotal.Inc()
		return data, "hot", nil
	}
	h.cfg.Metrics.CacheMissTotal.Inc()

	if data, ok := h.cfg.ColdCache.Get(key); ok {
		h.cfg.Metrics.ColdCacheHitTotal.Inc()
		h.cfg.HotCache.Put(key, data)
		return data, "cold", nil
	}

	buf := h.cfg.Pool.Get()
	defer h.cfg.Pool.Put(buf)

	n, joined, err := h.cfg.Backend.FetchChunkShared(ctx, key.ObjectKey, key.ChunkIndex, buf)
	if err != nil {
		return nil, "backend", fmt.Errorf("backend fetch chunk %d of %q: %w", key.ChunkIndex, key.ObjectKey, err)
	}

	owned := make([]byte, n)
	copy(owned, (*buf)[:n])
	if joined {
		// The leader (prefetcher or another request) caches it.
		return owned, "inflight", nil
	}
	h.cfg.HotCache.Put(key, owned)
	return owned, "backend", nil
}

func (h *Handler) proxyFull(w http.ResponseWriter, r *http.Request, objectKey string) {
	body, contentType, err := h.cfg.Backend.GetObjectStream(r.Context(), objectKey)
	if err != nil {
		h.cfg.Logger.Error("full-object proxy failed",
			zap.String("object", objectKey),
			zap.Error(err),
		)
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	defer body.Close()

	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, body)
}

// resolveByteRange turns a Range header into an inclusive [start, end] clamped
// to the object size. It handles "bytes=X-Y", open-ended "bytes=X-" and suffix
// "bytes=-N" forms. ok is false when the range is malformed or unsatisfiable.
func resolveByteRange(header string, size int64) (start, end int64, ok bool) {
	start, end, suffix, ok := parseByteRange(header)
	if !ok || size <= 0 {
		return 0, 0, false
	}
	if suffix {
		if end == 0 {
			return 0, 0, false
		}
		start = max(0, size-end)
		return start, size - 1, true
	}
	if start >= size {
		return 0, 0, false
	}
	if end < 0 || end >= size {
		end = size - 1
	}
	return start, end, true
}

// parseByteRange parses a single-range header. For a suffix range ("bytes=-N")
// suffix is true and end holds N. For an open-ended range end is -1.
func parseByteRange(header string) (start, end int64, suffix, ok bool) {
	const prefix = "bytes="
	if len(header) <= len(prefix) || header[:len(prefix)] != prefix {
		return 0, 0, false, false
	}
	rest := header[len(prefix):]
	hyphen := strings.IndexByte(rest, '-')
	if hyphen < 0 {
		return 0, 0, false, false
	}

	var err error
	if hyphen == 0 {
		end, err = strconv.ParseInt(rest[1:], 10, 64)
		if err != nil || end < 0 {
			return 0, 0, false, false
		}
		return 0, end, true, true
	}

	start, err = strconv.ParseInt(rest[:hyphen], 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false, false
	}
	if hyphen+1 < len(rest) {
		end, err = strconv.ParseInt(rest[hyphen+1:], 10, 64)
		if err != nil || end < start {
			return 0, 0, false, false
		}
	} else {
		end = -1
	}
	return start, end, false, true
}
