package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type sample struct {
	Phase     string
	Kind      string
	Object    string
	Offset    int64
	Length    int64
	Seq       int
	Tier      string
	TTFB      time.Duration
	Latency   time.Duration
	StartedAt time.Duration // since phase start
	Err       string
}

type reader interface {
	read(ctx context.Context, key string, off, length int64) (tier string, ttfb time.Duration, n int64, err error)
}

func runBench(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	var sf s3Flags
	sf.register(fs)
	proxyURL := fs.String("proxy", "http://localhost:8080", "proxy base URL")
	phases := fs.String("phases", "direct,proxy-cold,proxy-warm",
		"comma list: direct (S3, no proxy), proxy-cold (first pass through a freshly started proxy), proxy-warm (repeat pass)")
	sessions := fs.Int("sessions", 300, "student sessions to replay per phase")
	students := fs.Int("students", 16, "concurrent students (clients)")
	maxBytes := fs.Int64("max-bytes-per-session", 48<<20, "cap on bytes a single session reads (0 = none)")
	videoChunk := fs.Int64("video-chunk", 2<<20, "video segment request size")
	pdfChunk := fs.Int64("pdf-chunk", 256<<10, "PDF viewer range size")
	downloadBuf := fs.Int64("download-chunk", 4<<20, "range size for slide/dataset downloads")
	zipf := fs.Float64("zipf", 1.1, "course popularity skew (higher = more concentrated on top courses)")
	seed := fs.Int64("seed", 42, "workload RNG seed (same seed => identical requests every phase)")
	outDir := fs.String("out", filepath.Join("bench-results", time.Now().Format("20060102-150405")), "output directory")
	fs.Parse(args)

	ctx := context.Background()
	client, err := sf.client(ctx)
	if err != nil {
		return err
	}
	objs, err := listObjects(ctx, client, sf.bucket, sf.prefix)
	if err != nil {
		return err
	}
	if len(objs) == 0 {
		return fmt.Errorf("no objects under s3://%s/%s — run `bench seed` first", sf.bucket, sf.prefix)
	}

	work := buildSessions(objs, workloadConfig{
		sessions: *sessions, maxBytes: *maxBytes, videoChunk: *videoChunk, pdfChunk: *pdfChunk,
		downloadBuf: *downloadBuf, zipfS: *zipf, seed: *seed,
	})
	var reqs int
	var bytesPerPass, datasetBytes int64
	distinct := map[string]bool{}
	for _, s := range work {
		reqs += len(s.reqs)
		distinct[s.obj.key] = true
		for _, r := range s.reqs {
			bytesPerPass += r.length
		}
	}
	for _, o := range objs {
		datasetBytes += o.size
	}
	fmt.Printf("dataset:  %d files, %.2f GB\n", len(objs), float64(datasetBytes)/1e9)
	fmt.Printf("workload: %d sessions over %d distinct files, %d requests, %.2f GB per pass, %d concurrent students\n",
		len(work), len(distinct), reqs, float64(bytesPerPass)/1e9, *students)

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		return err
	}
	csvFile, err := os.Create(filepath.Join(*outDir, "requests.csv"))
	if err != nil {
		return err
	}
	defer csvFile.Close()
	cw := csv.NewWriter(csvFile)
	cw.Write([]string{"phase", "kind", "object", "offset", "length", "seq", "tier", "ttfb_us", "latency_us", "started_ms", "error"})

	summary := Summary{
		Config: map[string]any{
			"bucket": sf.bucket, "region": sf.region, "prefix": sf.prefix,
			"dataset_files": len(objs), "dataset_bytes": datasetBytes,
			"sessions": len(work), "distinct_files": len(distinct), "requests_per_pass": reqs, "bytes_per_pass": bytesPerPass,
			"students": *students, "max_bytes_per_session": *maxBytes, "video_chunk": *videoChunk,
			"pdf_chunk": *pdfChunk, "download_chunk": *downloadBuf, "zipf": *zipf, "seed": *seed,
			"started": time.Now().UTC().Format(time.RFC3339),
		},
	}

	for _, phase := range strings.Split(*phases, ",") {
		phase = strings.TrimSpace(phase)
		var r reader
		switch phase {
		case "direct":
			r = &s3Reader{client: client, bucket: sf.bucket}
		case "proxy-cold", "proxy-warm":
			pr := newProxyReader(*proxyURL, *students)
			if err := pr.ping(ctx, work[0].obj.key); err != nil {
				return fmt.Errorf("phase %s: proxy not reachable at %s: %w", phase, *proxyURL, err)
			}
			r = pr
		default:
			return fmt.Errorf("unknown phase %q", phase)
		}

		fmt.Printf("\n== %s ==\n", phase)
		samples, wall := runPhase(ctx, phase, r, work, *students)
		for _, s := range samples {
			cw.Write([]string{s.Phase, s.Kind, s.Object, strconv.FormatInt(s.Offset, 10), strconv.FormatInt(s.Length, 10),
				strconv.Itoa(s.Seq), s.Tier, strconv.FormatInt(s.TTFB.Microseconds(), 10),
				strconv.FormatInt(s.Latency.Microseconds(), 10), strconv.FormatInt(s.StartedAt.Milliseconds(), 10), s.Err})
		}
		cw.Flush()

		ps := summarize(phase, samples, wall)
		summary.Phases = append(summary.Phases, ps)
		printPhase(ps)
	}

	f, err := os.Create(filepath.Join(*outDir, "summary.json"))
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(summary); err != nil {
		return err
	}
	fmt.Printf("\nresults: %s\n", *outDir)
	return nil
}

func runPhase(ctx context.Context, phase string, r reader, work []session, students int) ([]sample, time.Duration) {
	var (
		mu      sync.Mutex
		samples []sample
		wg      sync.WaitGroup
		next    = make(chan session)
		done    int
		total   int
	)
	for _, s := range work {
		total += len(s.reqs)
	}
	phaseStart := time.Now()

	stopProgress := make(chan struct{})
	go func() {
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-stopProgress:
				return
			case <-tick.C:
				mu.Lock()
				d := done
				mu.Unlock()
				fmt.Printf("  %d/%d requests (%.0f%%) %s\n", d, total, 100*float64(d)/float64(total), time.Since(phaseStart).Round(time.Second))
			}
		}
	}()

	for w := 0; w < students; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sess := range next {
				for _, rq := range sess.reqs {
					start := time.Now()
					tier, ttfb, n, err := r.read(ctx, sess.obj.key, rq.off, rq.length)
					s := sample{
						Phase: phase, Kind: sess.kind, Object: sess.obj.key, Offset: rq.off, Length: n, Seq: rq.seq,
						Tier: tier, TTFB: ttfb, Latency: time.Since(start), StartedAt: start.Sub(phaseStart),
					}
					if err == nil && n != rq.length {
						err = fmt.Errorf("short read: got %d of %d bytes", n, rq.length)
					}
					if err != nil {
						s.Err = err.Error()
					}
					mu.Lock()
					samples = append(samples, s)
					done++
					mu.Unlock()
				}
			}
		}()
	}
	for _, s := range work {
		next <- s
	}
	close(next)
	wg.Wait()
	close(stopProgress)
	return samples, time.Since(phaseStart)
}

// s3Reader is the no-proxy baseline: the portal fetching from S3 itself.
type s3Reader struct {
	client *s3.Client
	bucket string
}

func (s *s3Reader) read(ctx context.Context, key string, off, length int64) (string, time.Duration, int64, error) {
	start := time.Now()
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, off+length-1)),
	})
	if err != nil {
		return "s3", 0, 0, err
	}
	ttfb := time.Since(start)
	defer out.Body.Close()
	n, err := io.Copy(io.Discard, out.Body)
	return "s3", ttfb, n, err
}

type proxyReader struct {
	base   string
	client *http.Client
}

func newProxyReader(base string, concurrency int) *proxyReader {
	return &proxyReader{
		base: strings.TrimRight(base, "/"),
		client: &http.Client{
			Timeout: 2 * time.Minute,
			Transport: &http.Transport{
				MaxIdleConns:        concurrency * 2,
				MaxIdleConnsPerHost: concurrency * 2,
				DisableCompression:  true,
			},
		},
	}
}

// ping issues one small ranged read so an unreachable or misconfigured proxy
// fails fast instead of producing a phase full of errors.
func (p *proxyReader) ping(ctx context.Context, key string) error {
	_, _, _, err := p.read(ctx, key, 0, 1)
	return err
}

func (p *proxyReader) read(ctx context.Context, key string, off, length int64) (string, time.Duration, int64, error) {
	var ttfb time.Duration
	start := time.Now()
	trace := &httptrace.ClientTrace{GotFirstResponseByte: func() { ttfb = time.Since(start) }}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), http.MethodGet, p.base+"/"+key, nil)
	if err != nil {
		return "", 0, 0, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, off+length-1))
	resp, err := p.client.Do(req)
	if err != nil {
		return "", 0, 0, err
	}
	defer resp.Body.Close()
	n, err := io.Copy(io.Discard, resp.Body)
	// Set by the proxy when it reports which cache tier served the request.
	tier := resp.Header.Get("X-Cache-Tier")
	if tier == "" {
		tier = "proxy"
	}
	if resp.StatusCode != http.StatusPartialContent {
		return tier, ttfb, n, errors.New(resp.Status)
	}
	return tier, ttfb, n, err
}

// ---- summary ----

type Stats struct {
	Count  int     `json:"count"`
	Errors int     `json:"errors"`
	P50    float64 `json:"p50_ms"`
	P90    float64 `json:"p90_ms"`
	P99    float64 `json:"p99_ms"`
	P999   float64 `json:"p999_ms"`
	Max    float64 `json:"max_ms"`
	Mean   float64 `json:"mean_ms"`
	TTFB50 float64 `json:"ttfb_p50_ms"`
	TTFB99 float64 `json:"ttfb_p99_ms"`
}

type PhaseSummary struct {
	Phase     string           `json:"phase"`
	WallSec   float64          `json:"wall_seconds"`
	Bytes     int64            `json:"bytes"`
	MBps      float64          `json:"throughput_mb_s"`
	All       Stats            `json:"all"`
	Kinds     map[string]Stats `json:"kinds"`
	Tiers     map[string]Stats `json:"tiers"`
	Histogram []int            `json:"histogram"` // counts per HistEdges bucket (upper edge, ms)
	HistEdges []float64        `json:"histogram_edges_ms"`
	BySeq     []float64        `json:"video_p50_ms_by_segment"` // p50 of the i-th segment of a video run
}

type Summary struct {
	Config map[string]any `json:"config"`
	Phases []PhaseSummary `json:"phases"`
}

func summarize(phase string, samples []sample, wall time.Duration) PhaseSummary {
	ps := PhaseSummary{Phase: phase, WallSec: wall.Seconds(), Kinds: map[string]Stats{}, Tiers: map[string]Stats{}}
	byKind := map[string][]sample{}
	byTier := map[string][]sample{}
	bySeq := map[int][]sample{}
	for _, s := range samples {
		ps.Bytes += s.Length
		byKind[s.Kind] = append(byKind[s.Kind], s)
		if s.Err == "" {
			byTier[s.Tier] = append(byTier[s.Tier], s)
		}
		if s.Kind == "video" && s.Seq >= 0 {
			bySeq[s.Seq] = append(bySeq[s.Seq], s)
		}
	}
	ps.MBps = float64(ps.Bytes) / 1e6 / wall.Seconds()
	ps.All = stats(samples)
	for k, ss := range byKind {
		ps.Kinds[k] = stats(ss)
	}
	for t, ss := range byTier {
		ps.Tiers[t] = stats(ss)
	}

	// Log-spaced buckets from 20µs to 20s, 8 per decade.
	for e := math.Log10(0.02); e <= math.Log10(20000)+1e-9; e += 1.0 / 8 {
		ps.HistEdges = append(ps.HistEdges, math.Pow(10, e))
	}
	ps.Histogram = make([]int, len(ps.HistEdges))
	for _, s := range samples {
		if s.Err != "" {
			continue
		}
		ms := float64(s.Latency.Microseconds()) / 1000
		i := min(sort.SearchFloat64s(ps.HistEdges, ms), len(ps.Histogram)-1)
		ps.Histogram[i]++
	}

	for i := 0; ; i++ {
		// Stop once too few runs reach this depth to be meaningful.
		if len(bySeq[i]) < 5 {
			break
		}
		ps.BySeq = append(ps.BySeq, stats(bySeq[i]).P50)
	}
	return ps
}

func stats(ss []sample) Stats {
	var st Stats
	var lat, ttfb []float64
	for _, s := range ss {
		st.Count++
		if s.Err != "" {
			st.Errors++
			continue
		}
		lat = append(lat, float64(s.Latency.Microseconds())/1000)
		ttfb = append(ttfb, float64(s.TTFB.Microseconds())/1000)
	}
	if len(lat) == 0 {
		return st
	}
	sort.Float64s(lat)
	sort.Float64s(ttfb)
	var sum float64
	for _, v := range lat {
		sum += v
	}
	st.Mean = sum / float64(len(lat))
	st.P50 = pct(lat, 0.50)
	st.P90 = pct(lat, 0.90)
	st.P99 = pct(lat, 0.99)
	st.P999 = pct(lat, 0.999)
	st.Max = lat[len(lat)-1]
	st.TTFB50 = pct(ttfb, 0.50)
	st.TTFB99 = pct(ttfb, 0.99)
	return st
}

func pct(sorted []float64, q float64) float64 {
	i := int(math.Ceil(q*float64(len(sorted)))) - 1
	return sorted[max(0, min(i, len(sorted)-1))]
}

func printPhase(ps PhaseSummary) {
	fmt.Printf("  %d requests, %d errors, %.1f MB/s, %.0fs wall\n", ps.All.Count, ps.All.Errors, ps.MBps, ps.WallSec)
	fmt.Printf("  %-14s %7s %10s %10s %10s %10s %10s\n", "", "count", "p50 ms", "p90 ms", "p99 ms", "max ms", "ttfb p50")
	row := func(name string, s Stats) {
		if s.Count == 0 {
			return
		}
		fmt.Printf("  %-14s %7d %10.3f %10.3f %10.3f %10.3f %10.3f\n", name, s.Count, s.P50, s.P90, s.P99, s.Max, s.TTFB50)
	}
	row("all", ps.All)
	kinds := make([]string, 0, len(ps.Kinds))
	for k := range ps.Kinds {
		kinds = append(kinds, k)
	}
	sort.Slice(kinds, func(i, j int) bool { return kindOrder(kinds[i]) < kindOrder(kinds[j]) })
	for _, k := range kinds {
		row(k, ps.Kinds[k])
	}
	for _, t := range []string{"hot", "cold", "inflight", "backend", "proxy", "s3"} {
		row("tier:"+t, ps.Tiers[t])
	}
}
