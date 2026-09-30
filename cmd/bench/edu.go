package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Course titles for the imagined portal. Order is popularity rank: the bench
// workload gives earlier courses more students (Zipf), as real catalogs do.
var courseTitles = []string{
	"intro-to-programming-python", "data-structures-and-algorithms", "machine-learning-foundations",
	"calculus-i", "linear-algebra", "statistics-for-data-science", "web-development-fullstack",
	"database-systems", "operating-systems", "computer-networks", "deep-learning",
	"discrete-mathematics", "physics-mechanics", "organic-chemistry", "microeconomics",
	"financial-accounting", "digital-marketing", "project-management", "cloud-computing-aws",
	"cybersecurity-essentials", "english-academic-writing", "world-history-modern",
	"psychology-101", "biology-cell-and-molecular", "ux-design-principles",
	"mobile-app-development", "computer-vision", "natural-language-processing",
	"business-analytics", "public-speaking",
}

var lectureTopics = []string{
	"introduction", "core-concepts", "worked-examples", "deep-dive", "case-study",
	"lab-walkthrough", "problem-solving", "advanced-topics", "review-session", "exam-prep",
	"guest-lecture", "project-kickoff",
}

type eduFile struct {
	key  string
	size int64
}

// buildCatalog lays out a portal:
//
//	<prefix>courses/<NN>-<course>/syllabus.pdf
//	<prefix>courses/<NN>-<course>/lectures/<LL>-<topic>.mp4        lecture recordings
//	<prefix>courses/<NN>-<course>/notes/<LL>-<topic>.pdf           lecture notes
//	<prefix>courses/<NN>-<course>/slides/course-deck.pptx
//	<prefix>courses/<NN>-<course>/resources/dataset.zip            (some courses)
//
// Sizes are drawn from ranges typical of lecture content; scale multiplies them.
func buildCatalog(prefix string, courses int, scale float64, seed int64) []eduFile {
	rng := rand.New(rand.NewSource(seed))
	mb := func(lo, hi float64) int64 { return int64((lo + rng.Float64()*(hi-lo)) * scale * 1e6) }

	var files []eduFile
	for c := 0; c < courses; c++ {
		dir := fmt.Sprintf("%scourses/%02d-%s/", prefix, c+1, courseTitles[c%len(courseTitles)])
		files = append(files, eduFile{dir + "syllabus.pdf", mb(0.2, 1.2)})
		lectures := 6 + rng.Intn(5)
		for l := 0; l < lectures; l++ {
			topic := lectureTopics[l%len(lectureTopics)]
			// 720p lecture capture ~ 3–5 MB/min; 5–15 minute segments.
			files = append(files, eduFile{fmt.Sprintf("%slectures/%02d-%s.mp4", dir, l+1, topic), mb(15, 60)})
			files = append(files, eduFile{fmt.Sprintf("%snotes/%02d-%s.pdf", dir, l+1, topic), mb(0.5, 6)})
		}
		files = append(files, eduFile{dir + "slides/course-deck.pptx", mb(8, 35)})
		if rng.Float64() < 0.4 {
			files = append(files, eduFile{dir + "resources/dataset.zip", mb(40, 160)})
		}
	}
	return files
}

func runSeed(args []string) error {
	fset := flag.NewFlagSet("seed", flag.ExitOnError)
	var sf s3Flags
	sf.register(fset)
	courses := fset.Int("courses", 16, "number of synthetic courses (0 = only -include-dir files)")
	scale := fset.Float64("scale", 1.0, "multiplier on file sizes (~340 MB per course at 1.0)")
	seed := fset.Int64("seed", 7, "catalog RNG seed")
	includeDir := fset.String("include-dir", "", "optional folder of real course material (videos, PDFs, slides) uploaded as-is under <prefix>uploads/")
	parallel := fset.Int("parallel", 4, "concurrent uploads")
	dryRun := fset.Bool("dry-run", false, "print the catalog and total size without uploading")
	fset.Parse(args)

	type upload struct {
		key  string
		size int64
		open func() (io.ReadSeeker, func(), error)
	}
	var ups []upload
	for _, f := range buildCatalog(sf.prefix, *courses, *scale, *seed) {
		f := f
		ups = append(ups, upload{f.key, f.size, func() (io.ReadSeeker, func(), error) {
			return newSynthFile(f.key, f.size), func() {}, nil
		}})
	}
	if *includeDir != "" {
		err := filepath.WalkDir(*includeDir, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			info, err := d.Info()
			if err != nil || info.Size() == 0 {
				return err
			}
			rel, _ := filepath.Rel(*includeDir, path)
			ups = append(ups, upload{sf.prefix + "uploads/" + filepath.ToSlash(rel), info.Size(),
				func() (io.ReadSeeker, func(), error) {
					f, err := os.Open(path)
					if err != nil {
						return nil, nil, err
					}
					return f, func() { f.Close() }, nil
				}})
			return nil
		})
		if err != nil {
			return fmt.Errorf("scan %s: %w", *includeDir, err)
		}
	}

	var total int64
	kinds := map[string]int{}
	for _, u := range ups {
		total += u.size
		kinds[object{key: u.key}.kind()]++
	}
	fmt.Printf("catalog: %d files, %.2f GB  %v\n", len(ups), float64(total)/1e9, kinds)
	if *dryRun {
		for _, u := range ups {
			fmt.Printf("  %8.1f MB  %s\n", float64(u.size)/1e6, u.key)
		}
		return nil
	}

	ctx := context.Background()
	client, err := sf.client(ctx)
	if err != nil {
		return err
	}
	existing := map[string]int64{}
	if objs, err := listObjects(ctx, client, sf.bucket, sf.prefix); err == nil {
		for _, o := range objs {
			existing[o.key] = o.size
		}
	}

	var (
		mu       sync.Mutex
		done     int
		sent     int64
		firstErr error
		wg       sync.WaitGroup
		jobs     = make(chan upload)
		start    = time.Now()
	)
	for w := 0; w < *parallel; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for u := range jobs {
				err := func() error {
					if existing[u.key] == u.size {
						return nil
					}
					body, closeFn, err := u.open()
					if err != nil {
						return err
					}
					defer closeFn()
					_, err = client.PutObject(ctx, &s3.PutObjectInput{
						Bucket:        aws.String(sf.bucket),
						Key:           aws.String(u.key),
						Body:          body,
						ContentLength: aws.Int64(u.size),
						ContentType:   aws.String(contentType(u.key)),
					})
					if err != nil {
						return fmt.Errorf("upload s3://%s/%s: %w", sf.bucket, u.key, err)
					}
					return nil
				}()
				mu.Lock()
				done++
				if err != nil && firstErr == nil {
					firstErr = err
				}
				if err == nil {
					sent += u.size
				}
				fmt.Printf("[%d/%d] %7.1f MB  %s  (%.1f MB/s)\n", done, len(ups), float64(u.size)/1e6, u.key,
					float64(sent)/1e6/time.Since(start).Seconds())
				mu.Unlock()
			}
		}()
	}
	for _, u := range ups {
		mu.Lock()
		stop := firstErr != nil
		mu.Unlock()
		if stop {
			break
		}
		jobs <- u
	}
	close(jobs)
	wg.Wait()
	return firstErr
}

func contentType(key string) string {
	switch (object{key: key}).kind() {
	case "video":
		return "video/mp4"
	case "pdf":
		return "application/pdf"
	case "slides":
		return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
	case "dataset":
		return "application/zip"
	}
	return "application/octet-stream"
}

// synthFile is a deterministic, seekable, incompressible stand-in for course
// media. Latency depends on size and byte count, not content, so random bytes
// behave like real video/PDF payloads on the wire (and defeat any compression).
// A real format header is overlaid so tools recognise the file type.
type synthFile struct {
	seed   uint64
	size   int64
	pos    int64
	header []byte
}

func newSynthFile(key string, size int64) *synthFile {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(key); i++ {
		h = (h ^ uint64(key[i])) * 1099511628211
	}
	var hdr []byte
	switch (object{key: key}).kind() {
	case "video":
		hdr = []byte("\x00\x00\x00\x20ftypisom\x00\x00\x02\x00isomiso2avc1mp41")
	case "pdf":
		hdr = []byte("%PDF-1.7\n%\xe2\xe3\xcf\xd3\n")
	case "slides", "dataset":
		hdr = []byte("PK\x03\x04")
	}
	return &synthFile{seed: h, size: size, header: hdr}
}

func splitmix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func (s *synthFile) Read(p []byte) (int, error) {
	if s.pos >= s.size {
		return 0, io.EOF
	}
	if rem := s.size - s.pos; int64(len(p)) > rem {
		p = p[:rem]
	}
	for i := range p {
		off := s.pos + int64(i)
		if off < int64(len(s.header)) {
			p[i] = s.header[off]
			continue
		}
		p[i] = byte(splitmix(s.seed^uint64(off>>3)) >> (8 * (off & 7)))
	}
	s.pos += int64(len(p))
	return len(p), nil
}

func (s *synthFile) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = s.pos + offset
	case io.SeekEnd:
		abs = s.size + offset
	}
	if abs < 0 {
		return 0, errors.New("synthFile: negative position")
	}
	s.pos = abs
	return abs, nil
}

// courseOf returns the course folder of a key ("" for files outside courses/),
// used to model course popularity.
func courseOf(key string) string {
	i := strings.Index(key, "courses/")
	if i < 0 {
		return ""
	}
	rest := key[i+len("courses/"):]
	if j := strings.IndexByte(rest, '/'); j > 0 {
		return rest[:j]
	}
	return ""
}
