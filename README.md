# go-predictive-proxy

A production-grade, predictive reverse proxy written in Go. It sits between an analytics engine issuing HTTP Range Requests and a Wasabi/S3 object store, using a dynamic chunk-based hot/cold caching algorithm and a velocity-driven prefetch engine to eliminate read amplification and tail latency.

---

## Architecture

```
Analytics Engine
      │  Range: bytes=X-Y
      ▼
┌─────────────────────────────────────────────────────────────┐
│                   proxy.Handler (handler.go)                │
│   parseByteRange → chunkStart…chunkEnd → resolveChunk loop  │
│                                                             │
│  ┌──────────────┐  miss  ┌──────────────┐  miss            │
│  │  HotCache    │───────▶│  ColdCache   │────────────────┐ │
│  │  (RAM LRU)   │        │  (NVMe SSD)  │                │ │
│  │  256 shards  │◀───────│  flat files  │                │ │
│  │  sync.Mutex  │promote │  FNV-sharded │                │ │
│  └──────────────┘        └──────────────┘                │ │
│        ▲  evict                                           ▼ │
│        │  (async goroutine)                    Backend.FetchChunk │
│        └──────────────────────────────────────────────────┘ │
│                                                             │
│  VelocityTracker  ──triggers──▶  Prefetcher (worker pool)  │
│  (EWMA, streak)                  (bounded channel, fan-out) │
└─────────────────────────────────────────────────────────────┘
      │                                     │
      │  TC egress hook (eBPF)              │ AWS SDK v2
      ▼                                     ▼
 Kernel RTT / retransmit maps          Wasabi / S3
```

### Key design decisions

| Requirement | Implementation |
|---|---|
| Zero-allocation chunk I/O | `sync.Pool` of `*[]byte` (4 MiB); pool stores pointer-to-slice to avoid boxing |
| Low lock-contention cache | 256-shard LRU; each shard has its own `sync.Mutex`; FNV-1a hash selects shard |
| LRU eviction to NVMe | `container/list` doubly-linked list + `map[ChunkKey]*list.Element`; evicted chunks written atomically via tmp-rename |
| Predictive prefetch | EWA velocity tracker fires fan-out to a bounded `chan prefetchJob`; non-blocking sends drop jobs when the channel is full |
| Range header parsing | Manual `strconv.ParseInt` on sliced strings; no regexp, no reflection |
| Kernel observability | TC egress eBPF program records per-backend TCP RTT and retransmits into LRU hash maps; read from user-space via `cilium/ebpf` |
| Backend connection pool | Custom `http.Transport` with `MaxIdleConnsPerHost = MaxConns`, `DisableCompression = true` |

---

## Directory structure

```
go-predictive-proxy/
├── cmd/proxy/main.go          # Wiring + OS signal handling
├── internal/
│   ├── cache/
│   │   ├── pool.go            # sync.Pool 4 MiB buffer pool
│   │   ├── lru.go             # 256-shard RAM LRU cache
│   │   └── disk.go            # NVMe cold cache (tmp-rename writes)
│   ├── engine/
│   │   ├── predictor.go       # EWMA velocity tracker
│   │   └── prefetch.go        # Bounded fan-out worker pool
│   ├── proxy/
│   │   ├── handler.go         # HTTP handler + Range parser
│   │   └── backend.go         # AWS SDK v2 S3/Wasabi client
│   └── metrics/
│       ├── prometheus.go      # Counter/gauge/histogram registry
│       ├── ebpf.go            # TC eBPF loader (linux build tag)
│       └── ebpf_stub.go       # No-op stubs for non-Linux
├── ebpf/
│   ├── tc_tracker.c           # BPF TC egress classifier (C)
│   └── bpf_bpfel.go           # Generated Go bindings (bpf2go)
├── configs/proxy.config.yaml
├── deployments/
│   ├── Dockerfile             # Multi-stage scratch build
│   └── docker-compose.yml     # Proxy + Prometheus + Grafana
└── go.mod
```

---

## Getting started

### Prerequisites

- Go 1.22+
- Docker (for the compose stack)
- Linux ≥ 5.10 with `CAP_BPF` + `CAP_NET_ADMIN` (eBPF only; set `ebpf.enabled: false` to skip)
- clang-14 + llvm-strip (eBPF C compilation; handled inside the Dockerfile)

### Local run (without Docker)

```bash
# 1. Resolve dependencies
cd go-predictive-proxy
go mod tidy

# 2. Set credentials
export WASABI_ACCESS_KEY_ID=...
export WASABI_SECRET_ACCESS_KEY=...

# 3. Edit configs/proxy.config.yaml — set disk_path to a writable directory
#    and ebpf.enabled: false on non-Linux dev machines.

# 4. Build and run
go build -o proxy ./cmd/proxy
./proxy -config configs/proxy.config.yaml
```

### Docker Compose

```bash
cd go-predictive-proxy
WASABI_ACCESS_KEY_ID=... WASABI_SECRET_ACCESS_KEY=... docker compose -f deployments/docker-compose.yml up --build
```

- Proxy: http://localhost:8080
- Prometheus: http://localhost:9091
- Grafana: http://localhost:3000 (admin / admin)

### eBPF code generation

After modifying `ebpf/tc_tracker.c`, regenerate Go bindings:

```bash
go generate ./ebpf/...
```

This requires `bpf2go`, `clang`, and `llvm-strip` on `$PATH`.

---

## Configuration reference

| Key | Default | Description |
|---|---|---|
| `server.port` | `8080` | Proxy listen port |
| `server.metrics_port` | `9090` | Prometheus /metrics port |
| `cache.ram_capacity_bytes` | `8589934592` | Total hot-cache RAM budget (8 GiB) |
| `cache.disk_path` | `/var/cache/…/cold` | NVMe mount point for cold cache |
| `backend.endpoint` | Wasabi US-East-1 | Override for MinIO or AWS S3 |
| `backend.max_conns` | `512` | TCP connection pool size |
| `engine.prefetch_workers` | `16` | Background fetch goroutines |
| `engine.look_ahead_chunks` | `8` | Chunks prefetched per trigger (32 MiB) |
| `engine.streak_threshold` | `3` | Sequential reads before prefetch arms |
| `engine.velocity_threshold_chunks_per_sec` | `0.5` | Minimum EWMA velocity to trigger |

---

## Metrics reference

| Metric | Type | Description |
|---|---|---|
| `proxy_cache_hot_hits_total` | Counter | RAM cache hits |
| `proxy_cache_hot_misses_total` | Counter | RAM cache misses |
| `proxy_cache_cold_hits_total` | Counter | NVMe cache hits (promotions) |
| `proxy_prefetch_enqueued_total` | Counter | Jobs sent to prefetch channel |
| `proxy_prefetch_dropped_total` | Counter | Jobs dropped due to backpressure |
| `proxy_prefetch_completed_total` | Counter | Successful speculative fetches |
| `proxy_prefetch_queue_length` | Gauge | Current prefetch channel depth |
| `proxy_requests_total` | CounterVec | By `{status, cache_tier}` |
| `proxy_bytes_served_total` | Counter | Total bytes written to clients |
| `proxy_request_duration_seconds` | HistogramVec | Latency by `{cache_tier}` |

---

## Performance tuning

**RAM cache is the critical knob.** The hit ratio is the single biggest lever on
end-to-end latency. Aim for `proxy_cache_hot_hits_total / (hits + misses) > 0.90`
for sequential analytic workloads. Size `ram_capacity_bytes` to hold at least
2× the working set of the busiest concurrent scan.

**Prefetch look-ahead vs. cache churn.** Each `look_ahead_chunks` unit consumes
one 4 MiB backend fetch and one hot-cache slot. If `proxy_prefetch_completed_total`
is high but the downstream hit ratio is not improving, reduce `look_ahead_chunks`
— you are fetching chunks that expire from the LRU before the client reaches them.

**Backend connection pool.** Set `max_conns` to the Wasabi per-IP connection
limit for your account tier. Exceeding it causes connection resets; under-sizing
it serialises concurrent prefetch workers.

**eBPF RTT feedback.** When `IsSlow()` returns true for the backend IP (RTT > 50 ms),
consider wiring the prefetcher to increase `LookAhead` dynamically to compensate
for the higher network latency.

---

## License

MIT
