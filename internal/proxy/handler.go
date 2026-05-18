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
	if rangeHeader == "" {
		h.proxyFull(w, r, objectKey)
		return
	}

	startByte, endByte, ok := parseByteRange(rangeHeader)
	if !ok || endByte < 0 {
		h.proxyFull(w, r, objectKey)
		return
	}
	if startByte > endByte {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}

	contentLength := endByte - startByte + 1
	chunkStart := uint64(startByte / cache.ChunkSize)
	chunkEnd := uint64(endByte / cache.ChunkSize)

	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/*", startByte, endByte))
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusPartialContent)

	if r.Method == http.MethodHead {
		return
	}

	var cacheTier string
	var bytesServed int64

	for chunkIdx := chunkStart; chunkIdx <= chunkEnd; chunkIdx++ {
		key := cache.ChunkKey{ObjectKey: objectKey, ChunkIndex: chunkIdx}

		chunkData, tier, err := h.resolveChunk(r.Context(), key)
		if err != nil {
			h.cfg.Logger.Error("chunk resolution failed",
				zap.String("object", objectKey),
				zap.Uint64("chunk", chunkIdx),
				zap.Error(err),
			)
			return
		}
		if cacheTier == "" {
			cacheTier = tier
		}

		chunkByteBase := int64(chunkIdx) * cache.ChunkSize
		sliceStart := int64(0)
		sliceEnd := int64(len(chunkData))

		if startByte > chunkByteBase {
			sliceStart = startByte - chunkByteBase
		}
		chunkByteEnd := chunkByteBase + int64(len(chunkData)) - 1
		if endByte < chunkByteEnd {
			sliceEnd = endByte - chunkByteBase + 1
		}

		n, writeErr := w.Write(chunkData[sliceStart:sliceEnd])
		bytesServed += int64(n)
		if writeErr != nil {
			return
		}

		h.cfg.Tracker.Record(objectKey, chunkIdx)
	}

	elapsed := time.Since(start).Seconds()
	if cacheTier == "" {
		cacheTier = "backend"
	}
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

	n, err := h.cfg.Backend.FetchChunk(ctx, key.ObjectKey, key.ChunkIndex, buf)
	if err != nil {
		return nil, "backend", fmt.Errorf("backend fetch chunk %d of %q: %w", key.ChunkIndex, key.ObjectKey, err)
	}

	owned := make([]byte, n)
	copy(owned, (*buf)[:n])
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

func parseByteRange(header string) (start, end int64, ok bool) {
	const prefix = "bytes="
	if len(header) <= len(prefix) || header[:len(prefix)] != prefix {
		return 0, 0, false
	}
	rest := header[len(prefix):]
	hyphen := strings.IndexByte(rest, '-')
	if hyphen < 0 {
		return 0, 0, false
	}

	var err error
	if hyphen > 0 {
		start, err = strconv.ParseInt(rest[:hyphen], 10, 64)
		if err != nil || start < 0 {
			return 0, 0, false
		}
	}
	if hyphen+1 < len(rest) {
		end, err = strconv.ParseInt(rest[hyphen+1:], 10, 64)
		if err != nil || end < start {
			return 0, 0, false
		}
	} else {
		end = -1
	}
	return start, end, true
}
