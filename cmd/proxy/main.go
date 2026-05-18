// Package main is the entrypoint for the predictive reverse proxy.
//
// It wires together all internal components — buffer pool, hot/cold cache,
// velocity tracker, prefetch engine, proxy handler, and metrics — then starts
// the HTTP servers and blocks until the process receives SIGINT or SIGTERM,
// at which point it performs a graceful shutdown with a 30-second timeout.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nadsanket7/go-predictive-proxy/internal/cache"
	"github.com/nadsanket7/go-predictive-proxy/internal/engine"
	"github.com/nadsanket7/go-predictive-proxy/internal/metrics"
	"github.com/nadsanket7/go-predictive-proxy/internal/proxy"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

func main() {
	cfgPath := flag.String("config", "configs/proxy.config.yaml", "path to config file")
	flag.Parse()

	log, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to init logger: %v\n", err)
		os.Exit(1)
	}
	defer log.Sync() //nolint:errcheck

	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		log.Fatal("config load failed", zap.String("path", *cfgPath), zap.Error(err))
	}

	// ── Buffer pool ──────────────────────────────────────────────────────────
	bufPool := cache.NewBufferPool()

	// ── Cold (NVMe) cache ────────────────────────────────────────────────────
	coldCache, err := cache.NewColdCache(cfg.Cache.DiskPath)
	if err != nil {
		log.Fatal("cold cache init failed", zap.Error(err))
	}

	// ── Eviction bridge: hot → cold ──────────────────────────────────────────
	// The onEvict callback is called inside the shard lock, so it must be
	// non-blocking. We launch a goroutine per eviction; for production workloads
	// a bounded worker pool (e.g., 4–8 workers draining a channel) is preferred
	// to cap goroutine creation under eviction bursts.
	evictOnHot := func(k cache.ChunkKey, data []byte) {
		go func() {
			if putErr := coldCache.Put(k, data); putErr != nil {
				log.Warn("cold cache eviction write failed",
					zap.String("object", k.ObjectKey),
					zap.Uint64("chunk", k.ChunkIndex),
					zap.Error(putErr),
				)
			}
		}()
	}

	// ── Hot (RAM) LRU cache ──────────────────────────────────────────────────
	hotCache := cache.NewHotCache(cfg.Cache.RAMCapacityBytes, evictOnHot)

	// ── Prometheus metrics registry ──────────────────────────────────────────
	reg := metrics.NewRegistry()

	// ── Backend connection pool ──────────────────────────────────────────────
	backend, err := proxy.NewBackend(proxy.BackendConfig{
		Endpoint:        cfg.Backend.Endpoint,
		Region:          cfg.Backend.Region,
		AccessKeyID:     os.ExpandEnv(cfg.Backend.AccessKeyID),
		SecretAccessKey: os.ExpandEnv(cfg.Backend.SecretAccessKey),
		Bucket:          cfg.Backend.Bucket,
		MaxConns:        cfg.Backend.MaxConns,
	}, log)
	if err != nil {
		log.Fatal("backend init failed", zap.Error(err))
	}

	// ── Prefetch worker pool ─────────────────────────────────────────────────
	prefetcher := engine.NewPrefetcher(engine.PrefetchConfig{
		Workers:    cfg.Engine.PrefetchWorkers,
		QueueDepth: cfg.Engine.PrefetchQueueDepth,
		LookAhead:  cfg.Engine.LookAheadChunks,
	}, backend, hotCache, bufPool, log, reg)
	prefetcher.Start()
	defer prefetcher.Stop()

	// ── Velocity tracker ─────────────────────────────────────────────────────
	tracker := engine.NewVelocityTracker(engine.TrackerConfig{
		StreakThreshold:               cfg.Engine.StreakThreshold,
		VelocityThresholdChunksPerSec: cfg.Engine.VelocityThresholdChunksPerSec,
	}, prefetcher, log)

	// ── Proxy handler ────────────────────────────────────────────────────────
	handler := proxy.NewHandler(proxy.HandlerConfig{
		HotCache:  hotCache,
		ColdCache: coldCache,
		Backend:   backend,
		Pool:      bufPool,
		Tracker:   tracker,
		Metrics:   reg,
		Logger:    log,
	})

	proxySrv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:      handler,
		ReadTimeout:  time.Duration(cfg.Server.ReadTimeoutSec) * time.Second,
		WriteTimeout: time.Duration(cfg.Server.WriteTimeoutSec) * time.Second,
		IdleTimeout:  time.Duration(cfg.Server.IdleTimeoutSec) * time.Second,
	}

	metricsSrv := &http.Server{
		Addr:        fmt.Sprintf(":%d", cfg.Server.MetricsPort),
		Handler:     reg.Handler(),
		ReadTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("metrics server started", zap.Int("port", cfg.Server.MetricsPort))
		if serveErr := metricsSrv.ListenAndServe(); serveErr != nil && serveErr != http.ErrServerClosed {
			log.Fatal("metrics server error", zap.Error(serveErr))
		}
	}()

	go func() {
		log.Info("proxy server started", zap.Int("port", cfg.Server.Port))
		if serveErr := proxySrv.ListenAndServe(); serveErr != nil && serveErr != http.ErrServerClosed {
			log.Fatal("proxy server error", zap.Error(serveErr))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Info("shutdown signal received", zap.String("signal", sig.String()))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if shutErr := proxySrv.Shutdown(ctx); shutErr != nil {
		log.Error("proxy server shutdown error", zap.Error(shutErr))
	}
	if shutErr := metricsSrv.Shutdown(ctx); shutErr != nil {
		log.Error("metrics server shutdown error", zap.Error(shutErr))
	}
	log.Info("shutdown complete")
}

// ── Config types (YAML-mapped) ───────────────────────────────────────────────

type appConfig struct {
	Server  serverCfg  `yaml:"server"`
	Cache   cacheCfg   `yaml:"cache"`
	Backend backendCfg `yaml:"backend"`
	Engine  engineCfg  `yaml:"engine"`
}

type serverCfg struct {
	Port            int `yaml:"port"`
	MetricsPort     int `yaml:"metrics_port"`
	ReadTimeoutSec  int `yaml:"read_timeout_sec"`
	WriteTimeoutSec int `yaml:"write_timeout_sec"`
	IdleTimeoutSec  int `yaml:"idle_timeout_sec"`
}

type cacheCfg struct {
	RAMCapacityBytes int64  `yaml:"ram_capacity_bytes"`
	DiskPath         string `yaml:"disk_path"`
}

type backendCfg struct {
	Endpoint        string `yaml:"endpoint"`
	Region          string `yaml:"region"`
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	Bucket          string `yaml:"bucket"`
	MaxConns        int    `yaml:"max_conns"`
}

type engineCfg struct {
	PrefetchWorkers               int     `yaml:"prefetch_workers"`
	PrefetchQueueDepth            int     `yaml:"prefetch_queue_depth"`
	LookAheadChunks               int     `yaml:"look_ahead_chunks"`
	StreakThreshold               int     `yaml:"streak_threshold"`
	VelocityThresholdChunksPerSec float64 `yaml:"velocity_threshold_chunks_per_sec"`
}

func loadConfig(path string) (*appConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", path, err)
	}
	defer f.Close()

	var cfg appConfig
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode %q: %w", path, err)
	}
	return &cfg, nil
}
