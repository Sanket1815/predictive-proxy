package cache

import (
	"container/list"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

// numShards is the number of independent LRU buckets. 256 shards reduce
// write-lock contention by ~256× versus a single global mutex, while keeping
// memory overhead negligible (~3 KB of map/list headers per shard).
const numShards = 256

// ChunkKey uniquely identifies a cached chunk: the object's storage path
// combined with its zero-based 4 MiB chunk index.
type ChunkKey struct {
	ObjectKey  string
	ChunkIndex uint64
}

// cacheEntry is the value type stored inside each shard's eviction list.
type cacheEntry struct {
	key  ChunkKey
	data []byte
}

// shard is a self-contained, mutex-isolated LRU segment.
type shard struct {
	mu        sync.Mutex // Write lock on every Get (MoveToFront); RWMutex buys nothing here.
	items     map[ChunkKey]*list.Element
	evictList *list.List
	capacity  int64 // max bytes this shard may hold
	used      int64 // bytes currently stored
	onEvict   func(ChunkKey, []byte)
}

// HotCache is a 256-shard in-memory LRU cache for 4 MiB chunks.
// Chunks evicted by the LRU policy are forwarded to the onEvict callback,
// which is typically a cold-cache (NVMe) write. The callback is invoked
// inside the shard lock — keep it non-blocking (launch a goroutine or send
// to a buffered channel).
type HotCache struct {
	shards [numShards]*shard
	hits   atomic.Int64
	misses atomic.Int64
}

// NewHotCache creates a sharded LRU cache with the given total RAM budget.
// capacityBytes is split evenly across all shards.
func NewHotCache(capacityBytes int64, onEvict func(ChunkKey, []byte)) *HotCache {
	c := &HotCache{}
	perShard := capacityBytes / numShards
	for i := range c.shards {
		c.shards[i] = &shard{
			items:     make(map[ChunkKey]*list.Element, 64),
			evictList: list.New(),
			capacity:  perShard,
			onEvict:   onEvict,
		}
	}
	return c
}

// Get returns the chunk data for k and promotes it to the MRU position.
// Returns nil, false on a cache miss. The returned slice is owned by the
// cache; callers must not modify it.
func (c *HotCache) Get(k ChunkKey) ([]byte, bool) {
	s := c.shardFor(k)
	s.mu.Lock()
	el, ok := s.items[k]
	if !ok {
		s.mu.Unlock()
		c.misses.Add(1)
		return nil, false
	}
	s.evictList.MoveToFront(el)
	data := el.Value.(*cacheEntry).data
	s.mu.Unlock()
	c.hits.Add(1)
	return data, true
}

// Put inserts or replaces chunk data for k. Ownership of data transfers to
// the cache; callers must not modify data after Put returns. LRU eviction
// runs synchronously inside the call to reclaim space before inserting.
func (c *HotCache) Put(k ChunkKey, data []byte) {
	s := c.shardFor(k)
	size := int64(len(data))

	s.mu.Lock()
	defer s.mu.Unlock()

	if el, ok := s.items[k]; ok {
		// Update existing entry in-place, adjusting the used-bytes counter.
		s.evictList.MoveToFront(el)
		old := el.Value.(*cacheEntry)
		s.used -= int64(len(old.data))
		old.data = data
		s.used += size
		return
	}

	for s.used+size > s.capacity && s.evictList.Len() > 0 {
		s.evictTail()
	}

	entry := &cacheEntry{key: k, data: data}
	el := s.evictList.PushFront(entry)
	s.items[k] = el
	s.used += size
}

// Hits returns the cumulative hot-cache hit count.
func (c *HotCache) Hits() int64 { return c.hits.Load() }

// Misses returns the cumulative hot-cache miss count.
func (c *HotCache) Misses() int64 { return c.misses.Load() }

// HitRatio returns the fraction of Get calls that were hits [0, 1].
func (c *HotCache) HitRatio() float64 {
	h, m := c.hits.Load(), c.misses.Load()
	if h+m == 0 {
		return 0
	}
	return float64(h) / float64(h+m)
}

// shardFor maps a ChunkKey to one of the 256 shards via FNV-1a hashing.
func (c *HotCache) shardFor(k ChunkKey) *shard {
	return c.shards[fnv1aHash32(k)%numShards]
}

// evictTail removes the least-recently-used entry from the shard.
// Must be called with s.mu held.
func (s *shard) evictTail() {
	el := s.evictList.Back()
	if el == nil {
		return
	}
	entry := el.Value.(*cacheEntry)
	s.evictList.Remove(el)
	delete(s.items, entry.key)
	s.used -= int64(len(entry.data))
	if s.onEvict != nil {
		s.onEvict(entry.key, entry.data)
	}
}

// fnv1aHash32 computes a 32-bit FNV-1a hash of a ChunkKey without heap
// allocation: string bytes are hashed directly, the chunk index is mixed in
// via a fixed 8-byte little-endian encoding on the stack.
func fnv1aHash32(k ChunkKey) uint32 {
	const (
		offset32 uint32 = 2166136261
		prime32  uint32 = 16777619
	)
	h := offset32
	for i := 0; i < len(k.ObjectKey); i++ {
		h ^= uint32(k.ObjectKey[i])
		h *= prime32
	}
	var idx [8]byte
	binary.LittleEndian.PutUint64(idx[:], k.ChunkIndex)
	for _, b := range idx {
		h ^= uint32(b)
		h *= prime32
	}
	return h
}
