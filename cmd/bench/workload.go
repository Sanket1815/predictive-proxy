package main

import (
	"math"
	"math/rand"
	"sort"
	"strings"
)

type rangeReq struct {
	off, length int64
	// seq is the position within an uninterrupted sequential run (0 = first
	// read after opening or seeking); -1 for reads that are not part of one.
	seq int
}

// session is one student interaction with one file, replayed identically in
// every phase so direct-to-S3 and proxied numbers are comparable.
type session struct {
	obj  object
	kind string
	reqs []rangeReq
}

type workloadConfig struct {
	sessions    int
	maxBytes    int64 // cap on bytes read per session (0 = none)
	videoChunk  int64 // HTML5 players fetch ~1–2 MiB segments
	pdfChunk    int64 // PDF.js range size
	downloadBuf int64 // download managers / browsers
	zipfS       float64
	seed        int64
}

// buildSessions models portal traffic:
//   - course popularity is Zipf over course rank (a few intro courses dominate);
//   - within a course, early lectures are watched far more than later ones;
//   - video: play from start in videoChunk segments; many students drop off
//     early, some seek once to a later point and keep watching;
//   - pdf: viewer reads the trailer/xref at the end, then pages from the start;
//   - slides/dataset: full sequential download.
func buildSessions(objs []object, cfg workloadConfig) []session {
	rng := rand.New(rand.NewSource(cfg.seed))

	byCourse := map[string]map[string][]object{}
	for _, o := range objs {
		c := courseOf(o.key)
		if byCourse[c] == nil {
			byCourse[c] = map[string][]object{}
		}
		byCourse[c][o.kind()] = append(byCourse[c][o.kind()], o)
	}
	courses := make([]string, 0, len(byCourse))
	for c := range byCourse {
		courses = append(courses, c)
	}
	sort.Strings(courses) // "01-…" first => most popular
	courseW := make([]float64, len(courses))
	for i := range courses {
		courseW[i] = 1 / math.Pow(float64(i+1), cfg.zipfS)
	}

	kindW := map[string]float64{"video": 0.55, "pdf": 0.25, "slides": 0.10, "dataset": 0.10, "other": 0.05}

	var out []session
	for len(out) < cfg.sessions {
		files := byCourse[courses[pickWeighted(rng, courseW)]]
		var kinds []string
		var kw []float64
		for k, fs := range files {
			if len(fs) > 0 {
				kinds = append(kinds, k)
			}
		}
		sort.Strings(kinds)
		for _, k := range kinds {
			kw = append(kw, kindW[k])
		}
		kind := kinds[pickWeighted(rng, kw)]

		fs := files[kind]
		sort.Slice(fs, func(i, j int) bool { return fs[i].key < fs[j].key })
		fw := make([]float64, len(fs))
		for i := range fs {
			// Lecture 1 is watched ~3x more than lecture 9.
			fw[i] = 1 / math.Sqrt(float64(i+1))
		}
		o := fs[pickWeighted(rng, fw)]

		s := session{obj: o, kind: kind}
		switch kind {
		case "video":
			s.reqs = videoReads(rng, o.size, cfg.videoChunk)
		case "pdf":
			s.reqs = pdfReads(rng, o.size, cfg.pdfChunk)
		default:
			s.reqs = sequential(0, o.size, cfg.downloadBuf, 0)
		}
		s.reqs = capBytes(s.reqs, cfg.maxBytes)
		if len(s.reqs) > 0 {
			out = append(out, s)
		}
	}
	return out
}

func videoReads(rng *rand.Rand, size, chunk int64) []rangeReq {
	// Fraction watched: exponential-ish drop-off, mean ~45%, some finish.
	watch := math.Min(1, 0.08+rng.ExpFloat64()*0.37)
	end := int64(float64(size) * watch)
	if rng.Float64() < 0.2 {
		// Watch the opening, then skip ahead and keep watching from there.
		first := min(end, chunk*int64(2+rng.Intn(4)))
		seekTo := first + rng.Int63n(max(1, size-first))
		seekTo -= seekTo % chunk
		reqs := sequential(0, first, chunk, 0)
		return append(reqs, sequential(seekTo, min(size, seekTo+(end-first)), chunk, 0)...)
	}
	return sequential(0, max(end, min(size, chunk)), chunk, 0)
}

func pdfReads(rng *rand.Rand, size, chunk int64) []rangeReq {
	tail := min(size, 64<<10)
	reqs := []rangeReq{{off: size - tail, length: tail, seq: -1}}
	pages := int64(2 + rng.Intn(14))
	return append(reqs, sequential(0, min(size, pages*chunk), chunk, 0)...)
}

func sequential(from, to, chunk int64, seq0 int) []rangeReq {
	var reqs []rangeReq
	for off, i := from, seq0; off < to; off, i = off+chunk, i+1 {
		reqs = append(reqs, rangeReq{off: off, length: min(chunk, to-off), seq: i})
	}
	return reqs
}

func capBytes(reqs []rangeReq, maxBytes int64) []rangeReq {
	if maxBytes <= 0 {
		return reqs
	}
	var total int64
	for i, r := range reqs {
		total += r.length
		if total > maxBytes {
			return reqs[:max(1, i)]
		}
	}
	return reqs
}

func pickWeighted(rng *rand.Rand, w []float64) int {
	var sum float64
	for _, v := range w {
		sum += v
	}
	x := rng.Float64() * sum
	for i, v := range w {
		if x < v {
			return i
		}
		x -= v
	}
	return len(w) - 1
}

func kindOrder(k string) int {
	return strings.Index("video pdf slides dataset other", k)
}
