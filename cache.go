package kv

import "github.com/zeebo/xxh3"

type Cache struct {
	shards []shard
}

func New(size int) *Cache {
	if size <= 0 {
		size = 1
	}

	numRings := (size + shardSize - 1) / shardSize

	return &Cache{
		shards: make([]shard, numRings),
	}
}

// Set() sets k, v into the cache.
// KV pairs with total length exceeding (64KB - 4B) are silently dropped.
//
// k and v are safe to be modified after Set() returns
func (c *Cache) Set(k, v []byte) {
	h := xxh3.Hash(k)
	c.shards[h%uint64(len(c.shards))].set(k, v, h)
}

// Get() copies value for key k into dst[:len(value)].
// A new slice will be allocated if dst is nil or not long enough.
// Returns nil if not found.
func (c *Cache) Get(dst, k []byte) (res []byte) {
	h := xxh3.Hash(k)
	res, _ = c.shards[h%uint64(len(c.shards))].get(dst, k, h, true)
	return
}

// HasGet() copies value for key k into dst[:len(value)] if pair exists.
// A new slice will be allocated if dst is nil or not long enough.
// Returns nil, false if not found.
//
// HasGet() is equal to Get() in performance.
func (c *Cache) HasGet(dst, k []byte) (res []byte, found bool) {
	h := xxh3.Hash(k)
	res, _ = c.shards[h%uint64(len(c.shards))].get(dst, k, h, true)
	return
}

// Has() checks whether key k exists in cache.
//
// Has() is slightly cheaper than HasGet() and Get().
func (c *Cache) Has(k []byte) (found bool) {
	h := xxh3.Hash(k)
	_, found = c.shards[h%uint64(len(c.shards))].get(nil, k, h, false)
	return
}

// Del() deletes KV pair from cache with key k.
func (c *Cache) Del(k []byte) {
	h := xxh3.Hash(k)
	c.shards[h%uint64(len(c.shards))].del(h)
}

// Reset removes all KV pairs from cache
func (c *Cache) Reset() {
	for i := range len(c.shards) {
		c.shards[i].reset()
	}
}

type Iterator struct {
	c        *Cache
	si       *shardIter
	shardIdx int
}

func (c *Cache) Iterator() *Iterator {
	return &Iterator{
		c: c,
		si: &shardIter{
			s: &shard{},
		},
		shardIdx: -1,
	}
}

// GetNext() copies key and value of next KV pair into kDst[:len(key)] and
// vDst[:len(value)] if next pair exists.
// New slices will be allocated if is nil or not long enough.
// Returns nil, nil, false when there are no more pairs.
func (it *Iterator) GetNext(kDst, vDst []byte) ([]byte, []byte, bool) {
	for {
		if k, v, ok := it.si.getNext(kDst, vDst); ok {
			return k, v, true
		} else {
			it.shardIdx++
			if it.shardIdx >= len(it.c.shards) { // No more shards
				return nil, nil, false
			}

			it.si = it.c.shards[it.shardIdx].iterator()
		}
	}
}
