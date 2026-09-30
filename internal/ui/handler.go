package ui

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/nadsanket7/go-predictive-proxy/internal/metrics"
	"go.uber.org/zap"
)

//go:embed index.html
var indexHTML []byte

type Config struct {
	ProxyPort int
	Endpoint  string
	Region    string
	Bucket    string
}

type Handler struct {
	mux       *http.ServeMux
	cfg       Config
	reg       *metrics.Registry
	log       *zap.Logger
	proxyAddr string
}

func NewHandler(cfg Config, reg *metrics.Registry, log *zap.Logger) *Handler {
	h := &Handler{
		mux:       http.NewServeMux(),
		cfg:       cfg,
		reg:       reg,
		log:       log,
		proxyAddr: fmt.Sprintf("http://localhost:%d", cfg.ProxyPort),
	}
	h.mux.HandleFunc("/", h.serveIndex)
	h.mux.HandleFunc("/api/config", h.serveConfig)
	h.mux.HandleFunc("/api/metrics", h.serveMetrics)
	h.mux.HandleFunc("/api/fetch", h.serveFetch)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(indexHTML)
}

func (h *Handler) serveConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"proxy_port": h.cfg.ProxyPort,
		"endpoint":   h.cfg.Endpoint,
		"region":     h.cfg.Region,
		"bucket":     h.cfg.Bucket,
	})
}

func (h *Handler) serveMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(h.reg.Snapshot())
}

type fetchRequest struct {
	ObjectKey  string `json:"object_key"`
	RangeStart int64  `json:"range_start"`
	RangeEnd   int64  `json:"range_end"`
	UseRange   bool   `json:"use_range"`
}

type fetchResponse struct {
	StatusCode int     `json:"status_code"`
	Bytes      int64   `json:"bytes"`
	LatencyMs  float64 `json:"latency_ms"`
	CacheTier  string  `json:"cache_tier"`
	Error      string  `json:"error,omitempty"`
}

var proxyClient = &http.Client{Timeout: 60 * time.Second}

func (h *Handler) serveFetch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req fetchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}
	if req.ObjectKey == "" {
		http.Error(w, "object_key required", http.StatusBadRequest)
		return
	}

	before := h.reg.Snapshot()

	url := fmt.Sprintf("%s/%s", h.proxyAddr, req.ObjectKey)
	httpReq, err := http.NewRequestWithContext(r.Context(), http.MethodGet, url, nil)
	if err != nil {
		writeJSON(w, fetchResponse{Error: err.Error()})
		return
	}
	if req.UseRange {
		httpReq.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", req.RangeStart, req.RangeEnd))
	}

	start := time.Now()
	resp, err := proxyClient.Do(httpReq)
	latencyMs := time.Since(start).Seconds() * 1000
	if err != nil {
		writeJSON(w, fetchResponse{Error: err.Error()})
		return
	}
	defer resp.Body.Close()
	n, _ := io.Copy(io.Discard, resp.Body)

	after := h.reg.Snapshot()

	tier := "backend"
	if after["proxy_cache_hot_hits_total"] > before["proxy_cache_hot_hits_total"] {
		tier = "hot (RAM)"
	} else if after["proxy_cache_cold_hits_total"] > before["proxy_cache_cold_hits_total"] {
		tier = "cold (NVMe)"
	}

	writeJSON(w, fetchResponse{
		StatusCode: resp.StatusCode,
		Bytes:      n,
		LatencyMs:  latencyMs,
		CacheTier:  tier,
	})
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
