package cache

import (
	"container/list"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const numShards = 256

type ChunkKey struct {
	ObjectKey  string
	ChunkIndex uint64
}

type cacheEntry struct {
	key  ChunkKey
	data []byte
}

type shard struct {
	mu        sync.Mutex
	items     map[ChunkKey]*list.Element
	evictList *list.List
	capacity  int64
	used      int64
	onEvict   func(ChunkKey, []byte)
}

type HotCache struct {
	shards [numShards]*shard
	hits   atomic.Int64
	misses atomic.Int64
}

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

func (c *HotCache) Put(k ChunkKey, data []byte) {
	s := c.shardFor(k)
	size := int64(len(data))

	s.mu.Lock()
	defer s.mu.Unlock()

	if el, ok := s.items[k]; ok {
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

func (c *HotCache) Hits() int64 { return c.hits.Load() }

func (c *HotCache) Misses() int64 { return c.misses.Load() }

func (c *HotCache) HitRatio() float64 {
	h, m := c.hits.Load(), c.misses.Load()
	if h+m == 0 {
		return 0
	}
	return float64(h) / float64(h+m)
}

func (c *HotCache) shardFor(k ChunkKey) *shard {
	return c.shards[fnv1aHash32(k)%numShards]
}

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
