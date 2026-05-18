package cache

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

type ColdCache struct {
	baseDir string
}

func NewColdCache(baseDir string) (*ColdCache, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("cold cache: create base dir %q: %w", baseDir, err)
	}
	return &ColdCache{baseDir: baseDir}, nil
}

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

func (c *ColdCache) Get(k ChunkKey) ([]byte, bool) {
	data, err := os.ReadFile(c.chunkPath(k))
	if err != nil {
		return nil, false
	}
	return data, true
}

func (c *ColdCache) Delete(k ChunkKey) error {
	err := os.Remove(c.chunkPath(k))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

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
