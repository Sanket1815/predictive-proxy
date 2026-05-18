// Package metrics exposes Prometheus telemetry and optional eBPF kernel
// instrumentation for the predictive proxy.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry holds every counter, gauge, and histogram exported by the proxy.
// All fields are safe for concurrent use after NewRegistry returns.
type Registry struct {
	// Hot-cache counters
	CacheHitTotal     prometheus.Counter
	CacheMissTotal    prometheus.Counter
	ColdCacheHitTotal prometheus.Counter

	// Prefetch counters and queue gauge
	PrefetchQueued      prometheus.Counter
	PrefetchDropped     prometheus.Counter
	PrefetchHits        prometheus.Counter
	PrefetchErrors      prometheus.Counter
	PrefetchQueueLength prometheus.Gauge

	// Per-request counters and histograms
	RequestsTotal  *prometheus.CounterVec
	BytesServed    prometheus.Counter
	RequestLatency *prometheus.HistogramVec

	reg *prometheus.Registry
}

// NewRegistry creates and registers all proxy metrics with a dedicated
// Prometheus registry (not the global default) for isolation in tests.
func NewRegistry() *Registry {
	reg := prometheus.NewRegistry()

	r := &Registry{
		CacheHitTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_cache_hot_hits_total",
			Help: "Total hot-cache (RAM) hits.",
		}),
		CacheMissTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_cache_hot_misses_total",
			Help: "Total hot-cache misses (fell through to cold or backend).",
		}),
		ColdCacheHitTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_cache_cold_hits_total",
			Help: "Total cold-cache (NVMe) hits.",
		}),
		PrefetchQueued: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_prefetch_enqueued_total",
			Help: "Total prefetch jobs enqueued.",
		}),
		PrefetchDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_prefetch_dropped_total",
			Help: "Total prefetch jobs dropped due to a full queue (backpressure).",
		}),
		PrefetchHits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_prefetch_completed_total",
			Help: "Total prefetch jobs that successfully populated the cache.",
		}),
		PrefetchErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_prefetch_errors_total",
			Help: "Total prefetch jobs that failed due to backend errors.",
		}),
		PrefetchQueueLength: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "proxy_prefetch_queue_length",
			Help: "Current number of jobs waiting in the prefetch channel.",
		}),
		RequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "proxy_requests_total",
			Help: "Total proxy requests by HTTP status code and cache tier.",
		}, []string{"status", "cache_tier"}),
		BytesServed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_bytes_served_total",
			Help: "Total bytes written to downstream clients.",
		}),
		RequestLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "proxy_request_duration_seconds",
			Help: "End-to-end request latency, bucketed by cache tier.",
			// 14 buckets: 1 ms → ~8 s. Covers NVMe cold-cache latency (1–5 ms)
			// through slow backend fetches (1–8 s) in a single histogram.
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		}, []string{"cache_tier"}),
		reg: reg,
	}

	reg.MustRegister(
		r.CacheHitTotal,
		r.CacheMissTotal,
		r.ColdCacheHitTotal,
		r.PrefetchQueued,
		r.PrefetchDropped,
		r.PrefetchHits,
		r.PrefetchErrors,
		r.PrefetchQueueLength,
		r.RequestsTotal,
		r.BytesServed,
		r.RequestLatency,
	)

	return r
}

// Handler returns an http.Handler that serves the Prometheus metrics endpoint.
// Mount this on a separate port from the proxy to avoid exposing metrics
// publicly.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{Registry: r.reg})
}
