package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

type Registry struct {
	CacheHitTotal     prometheus.Counter
	CacheMissTotal    prometheus.Counter
	ColdCacheHitTotal prometheus.Counter

	PrefetchQueued      prometheus.Counter
	PrefetchDropped     prometheus.Counter
	PrefetchHits        prometheus.Counter
	PrefetchErrors      prometheus.Counter
	PrefetchQueueLength prometheus.Gauge

	RequestsTotal  *prometheus.CounterVec
	BytesServed    prometheus.Counter
	RequestLatency *prometheus.HistogramVec

	reg *prometheus.Registry
}

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
			Name:    "proxy_request_duration_seconds",
			Help:    "End-to-end request latency, bucketed by cache tier.",
			// 50µs … ~6.5s: resolves sub-millisecond hot hits as well as slow S3 tails.
			Buckets: prometheus.ExponentialBuckets(0.00005, 2, 18),
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

func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{Registry: r.reg})
}

func (r *Registry) Snapshot() map[string]float64 {
	mfs, err := r.reg.Gather()
	if err != nil {
		return nil
	}
	result := make(map[string]float64)
	for _, mf := range mfs {
		name := mf.GetName()
		for _, m := range mf.GetMetric() {
			key := name
			if lbls := m.GetLabel(); len(lbls) > 0 {
				parts := ""
				for _, lp := range lbls {
					if parts != "" {
						parts += ","
					}
					parts += lp.GetValue()
				}
				key += "{" + parts + "}"
			}
			switch mf.GetType() {
			case dto.MetricType_COUNTER:
				result[key] = m.GetCounter().GetValue()
			case dto.MetricType_GAUGE:
				result[key] = m.GetGauge().GetValue()
			case dto.MetricType_HISTOGRAM:
				result[key+"#count"] = float64(m.GetHistogram().GetSampleCount())
				result[key+"#sum"] = m.GetHistogram().GetSampleSum()
			}
		}
	}
	return result
}
