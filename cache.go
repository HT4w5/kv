package kv

import "github.com/zeebo/xxh3"

const (
	numBuckets = 512
)

// Cache is a thread-safe, memory KV cache.
//
// Cache supports a hard memory cap and auto memory deallocation
// under low pressure.
//
// Actual memory consumption might be slightly higher than configured size.
type Cache struct {
	buckets [512]bucket
}

// New() creates a new Cache instance.
//
// size
func New(size int) *Cache {
	bucketSize := (max(size, 1) + numBuckets - 1) / numBuckets

	c := &Cache{}
	for i := range numBuckets {
		c.buckets[i].init(bucketSize)
	}

	return c
}

// Set() sets k, v into the cache.
// KV pairs with total length exceeding (64KB - 4B) are silently dropped.
//
// k and v are safe to be modified after Set() returns
func (c *Cache) Set(k, v []byte) {
	h := xxh3.Hash(k)
	c.buckets[h%numBuckets].set(k, v, h)
}

// Get() appends value for key k to dst.
// Returns nil if not found.
func (c *Cache) Get(dst, k []byte) (res []byte) {
	h := xxh3.Hash(k)
	res, _ = c.buckets[h%numBuckets].get(dst, k, h, true)
	return
}

// HasGet() appends value for key k to dst if pair exists.
// Returns nil, false if not found.
//
// HasGet() is equal to Get() in performance.
func (c *Cache) HasGet(dst, k []byte) (res []byte, found bool) {
	h := xxh3.Hash(k)
	res, found = c.buckets[h%numBuckets].get(dst, k, h, true)
	return
}

// Has() checks whether key k exists in cache.
//
// Has() is slightly cheaper than HasGet() and Get().
func (c *Cache) Has(k []byte) (found bool) {
	h := xxh3.Hash(k)
	_, found = c.buckets[h%numBuckets].get(nil, k, h, false)
	return
}

// Del() deletes KV pair from cache with key k.
func (c *Cache) Del(k []byte) {
	h := xxh3.Hash(k)
	c.buckets[h%numBuckets].del(h)
}

// Reset removes all KV pairs from cache
func (c *Cache) Reset() {
	for i := range len(c.buckets) {
		c.buckets[i].reset()
	}
}

type Iterator struct {
	c         *Cache
	bi        *bucketIter
	bucketIdx int
}

// Iterator() creates a new Iterator instance.
func (c *Cache) Iterator() *Iterator {
	return &Iterator{
		c: c,
		bi: &bucketIter{
			bkt: &bucket{},
		},
		bucketIdx: -1,
	}
}

// GetNext() copies key and value of next KV pair into kDst[:len(key)] and
// vDst[:len(value)] if next pair exists.
// New slices will be allocated if is nil or not long enough.
// Returns nil, nil, false when there are no more pairs.
//
// Pairs acquired by one Iterator are not guaranteed to be consistent in time.
// Iteration could pick up pairs inserted after creation of Iterator.
//
// GetNext() does not increment the Gets stat.
func (it *Iterator) GetNext(kDst, vDst []byte) ([]byte, []byte, bool) {
	for {
		if k, v, ok := it.bi.getNext(kDst, vDst); ok {
			return k, v, true
		} else {
			it.bucketIdx++
			if it.bucketIdx >= len(it.c.buckets) { // No more shards
				return nil, nil, false
			}

			it.bi = it.c.buckets[it.bucketIdx].iterator()
		}
	}
}
