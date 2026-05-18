package cache

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// ColdCache stores evicted chunks on a local NVMe SSD as flat binary files.
//
// Layout: <baseDir>/<2-char shard>/<16-char key hash>/<chunk-index>
//
// Two-level directory sharding prevents single-directory inode exhaustion.
// With 256 top-level shards each holding up to 65 536 sub-directories, the
// layout supports ~16 million distinct objects without fs performance cliffs.
//
// Write atomicity: data is written to a .tmp sibling and renamed into place,
// so a crash mid-write leaves at most an orphaned temp file — no corrupt chunks.
type ColdCache struct {
	baseDir string
}

// NewColdCache creates (or reopens) a ColdCache rooted at baseDir.
func NewColdCache(baseDir string) (*ColdCache, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("cold cache: create base dir %q: %w", baseDir, err)
	}
	return &ColdCache{baseDir: baseDir}, nil
}

// Put writes data to disk atomically. An in-flight crash leaves only an orphaned
// .tmp file; subsequent writes for the same key overwrite it cleanly.
func (c *ColdCache) Put(k ChunkKey, data []byte) error {
	p := c.chunkPath(k)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return fmt.Errorf("cold cache mkdir %q: %w", k.ObjectKey, err)
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("cold cache write %q chunk %d: %w", k.ObjectKey, k.ChunkIndex, err)
	}
	if err := os.Rename(tmp, p); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("cold cache rename %q chunk %d: %w", k.ObjectKey, k.ChunkIndex, err)
	}
	return nil
}

// Get reads a chunk from disk. Returns nil, false if the chunk is absent.
// The returned slice is freshly allocated by os.ReadFile; ownership passes
// to the caller (and then to HotCache.Put without a second copy).
func (c *ColdCache) Get(k ChunkKey) ([]byte, bool) {
	data, err := os.ReadFile(c.chunkPath(k))
	if err != nil {
		return nil, false
	}
	return data, true
}

// Delete removes a cached chunk. Idempotent: a missing file is not an error.
func (c *ColdCache) Delete(k ChunkKey) error {
	err := os.Remove(c.chunkPath(k))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// chunkPath maps a ChunkKey to an absolute filesystem path.
func (c *ColdCache) chunkPath(k ChunkKey) string {
	h := fnv1aHash64(k.ObjectKey)
	raw := [8]byte{
		byte(h >> 56), byte(h >> 48), byte(h >> 40), byte(h >> 32),
		byte(h >> 24), byte(h >> 16), byte(h >> 8), byte(h),
	}
	hashHex := make([]byte, 16)
	hex.Encode(hashHex, raw[:])
	shard := string(hashHex[:2])
	return filepath.Join(c.baseDir, shard, string(hashHex), strconv.FormatUint(k.ChunkIndex, 10))
}

// fnv1aHash64 computes a 64-bit FNV-1a hash of s for directory path sharding.
func fnv1aHash64(s string) uint64 {
	const (
		offset64 uint64 = 14695981039346656037
		prime64  uint64 = 1099511628211
	)
	h := offset64
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}
