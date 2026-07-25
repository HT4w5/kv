// LLM usage: tests are generated with deepseek-v4-pro and modified.
package kv_test

import (
	"sync"
	"testing"

	kv "github.com/HT4w5/kv"
)

func TestCache_SetGet(t *testing.T) {
	c := kv.New(1 << 20) // 1MB, enough for one shard
	c.Set([]byte("hello"), []byte("world"))

	v := c.Get(nil, []byte("hello"))
	if string(v) != "world" {
		t.Fatalf("expected 'world', got %q", v)
	}
}

func TestCache_Has(t *testing.T) {
	c := kv.New(1 << 20)
	c.Set([]byte("foo"), []byte("bar"))

	if !c.Has([]byte("foo")) {
		t.Fatal("Has returned false for existing key")
	}
	if c.Has([]byte("nonexistent")) {
		t.Fatal("Has returned true for missing key")
	}
}

func TestCache_HasGet(t *testing.T) {
	c := kv.New(1 << 20)

	// Found: returns value and true.
	v, found := c.HasGet(nil, []byte("key"))
	if found || v != nil {
		t.Fatal("HasGet returned true/non-nil for missing key")
	}

	c.Set([]byte("key"), []byte("val"))
	v, found = c.HasGet(nil, []byte("key"))
	if !found {
		t.Fatal("HasGet returned false for existing key")
	}
	if string(v) != "val" {
		t.Fatalf("expected 'val', got %q", v)
	}

	// Pre-allocated dst large enough.
	buf := make([]byte, 10)
	v, found = c.HasGet(buf, []byte("key"))
	if !found || string(v) != "val" {
		t.Fatalf("HasGet with pre-allocated dst: found=%v, val=%q", found, v)
	}

	// Pre-allocated dst too small -> should allocate.
	small := make([]byte, 1)
	v, found = c.HasGet(small, []byte("key"))
	if !found || string(v) != "val" {
		t.Fatalf("HasGet with small dst: found=%v, val=%q", found, v)
	}

	// Empty key, empty value.
	c.Set([]byte{}, []byte{})
	v, found = c.HasGet(nil, []byte{})
	if !found || len(v) != 0 {
		t.Fatalf("HasGet for empty/empty: found=%v, len=%d", found, len(v))
	}
}

func TestCache_Overwrite(t *testing.T) {
	c := kv.New(1 << 20)
	c.Set([]byte("key"), []byte("v1"))
	c.Set([]byte("key"), []byte("v2"))

	v := c.Get(nil, []byte("key"))
	if string(v) != "v2" {
		t.Fatalf("expected 'v2', got %q", v)
	}
}

func TestCache_Del(t *testing.T) {
	c := kv.New(1 << 20)
	c.Set([]byte("key"), []byte("val"))

	// Delete existing key.
	c.Del([]byte("key"))
	if c.Has([]byte("key")) {
		t.Fatal("Has returned true after Del")
	}
	if v := c.Get(nil, []byte("key")); v != nil {
		t.Fatalf("Get returned %q after Del, want nil", v)
	}

	// Deleting a non-existent key should not panic.
	c.Del([]byte("nonexistent"))
}

func TestCache_OversizedIgnored(t *testing.T) {
	const sz = 1 << 16 // shardSize

	c := kv.New(sz) // single shard

	// Exactly at limit: 4 + 1 + (sz-5) = sz -> rejected.
	bigV := make([]byte, sz-5)
	c.Set([]byte("k"), bigV)
	if c.Has([]byte("k")) {
		t.Fatal("oversized KV (at limit) was stored")
	}

	// One byte under limit: 4 + 1 + (sz-6) = sz-1 -> accepted.
	smallV := make([]byte, sz-6)
	c.Set([]byte("x"), smallV)
	if !c.Has([]byte("x")) {
		t.Fatal("valid KV (under limit) was not stored")
	}
	if len(c.Get(nil, []byte("x"))) != len(smallV) {
		t.Fatal("Get returned wrong length for valid KV")
	}
}

func TestCache_IteratorEmpty(t *testing.T) {
	c := kv.New(1 << 20)
	it := c.Iterator()
	k, v, ok := it.GetNext(nil, nil)
	if ok || k != nil || v != nil {
		t.Fatalf("expected nil,nil,false from empty iterator, got %q,%q,%v", k, v, ok)
	}
}

func TestCache_Iterator(t *testing.T) {
	c := kv.New(1 << 20)
	pairs := map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
	}
	for k, v := range pairs {
		c.Set([]byte(k), []byte(v))
	}

	seen := make(map[string]string)
	it := c.Iterator()
	for {
		k, v, ok := it.GetNext(nil, nil)
		if !ok {
			break
		}
		seen[string(k)] = string(v)
	}

	if len(seen) != len(pairs) {
		t.Fatalf("expected %d entries, got %d", len(pairs), len(seen))
	}
	for k, want := range pairs {
		if got := seen[k]; got != want {
			t.Fatalf("for key %q: expected %q, got %q", k, want, got)
		}
	}
}

func TestCache_IteratorSkipsDeleted(t *testing.T) {
	c := kv.New(1 << 20)
	c.Set([]byte("keep"), []byte("val"))
	c.Set([]byte("del"), []byte("tmp"))
	c.Del([]byte("del"))

	seen := make(map[string]string)
	it := c.Iterator()
	for {
		k, v, ok := it.GetNext(nil, nil)
		if !ok {
			break
		}
		seen[string(k)] = string(v)
	}

	if seen["del"] != "" {
		t.Fatal("iterator returned deleted key")
	}
	if seen["keep"] != "val" {
		t.Fatal("iterator did not return remaining key")
	}
	if len(seen) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(seen))
	}
}

func TestCache_WrapEviction(t *testing.T) {
	// Single shard so all entries compete for one 64KB ring buffer.
	c := kv.New(1)

	// Each entry: 4 + 1 (key) + 1024 (val) = 1029 bytes.
	// Need ~64 entries to exceed 64KB; write 80 to force multiple wraps.
	const n = 80
	v := make([]byte, 1024)
	for i := range n {
		k := []byte{byte(i)}
		v[0] = byte(i)
		c.Set(k, v)
	}

	// Oldest entries should be evicted; recent ones should survive.
	if c.Has([]byte{0}) {
		t.Fatal("oldest entry should have been evicted")
	}
	if !c.Has([]byte{byte(n - 1)}) {
		t.Fatal("newest entry should survive")
	}

	// At least one wrap should have occurred.
	s := c.Stats()
	if s.Wraps == 0 {
		t.Fatal("expected at least one ring buffer wrap")
	}
}

func TestCache_Reset(t *testing.T) {
	c := kv.New(1 << 20)
	c.Set([]byte("a"), []byte("1"))
	c.Set([]byte("b"), []byte("2"))

	c.Reset()

	// Data cleared.
	if c.Has([]byte("a")) || c.Has([]byte("b")) {
		t.Fatal("Has returned true after Reset")
	}
	if v := c.Get(nil, []byte("a")); v != nil {
		t.Fatalf("Get returned %q after Reset, want nil", v)
	}

	// Iterator empty.
	it := c.Iterator()
	if _, _, ok := it.GetNext(nil, nil); ok {
		t.Fatal("iterator returned entry after Reset")
	}

	// Cache usable after Reset.
	c.Set([]byte("c"), []byte("3"))
	if !c.Has([]byte("c")) {
		t.Fatal("cache unusable after Reset")
	}
}

func TestCache_AutoDeallocation(t *testing.T) {
	// Single shard for deterministic behavior.
	c := kv.New(1)

	c.Set([]byte("key"), []byte("val"))

	// Delete the only key -> shard should deallocate.
	c.Del([]byte("key"))
	s := c.Stats()
	if s.Deallocations == 0 {
		t.Fatal("expected deallocation after deleting last key in shard")
	}

	// Set again -> should allocate from pool.
	c.Set([]byte("key2"), []byte("val2"))
	s = c.Stats()
	if s.Allocations <= 1 {
		t.Fatal("expected re-allocation after inserting into deallocated shard")
	}
	if !c.Has([]byte("key2")) {
		t.Fatal("re-allocated shard should store key")
	}
}

func TestCache_ConcurrentSetGet(t *testing.T) {
	c := kv.New(1 << 20)
	const goroutines = 8
	const iters = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func(id int) {
			defer wg.Done()
			k := []byte{byte(id)}
			v := make([]byte, 64)
			v[0] = byte(id)
			for range iters {
				c.Set(k, v)
				got := c.Get(nil, k)
				if len(got) > 0 && got[0] != byte(id) {
					t.Errorf("wrong value: expected %d, got %d", id, got[0])
				}
				c.Has(k)
			}
		}(g)
	}
	wg.Wait()
}

func TestCache_ConcurrentIterator(t *testing.T) {
	c := kv.New(1 << 20)

	// Pre-populate.
	for i := range 100 {
		c.Set([]byte{byte(i)}, []byte{byte(i)})
	}

	var wg sync.WaitGroup

	// Mutator goroutines.
	for g := range 4 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			k := []byte{byte(id + 100)}
			v := []byte{byte(id + 100)}
			for range 200 {
				c.Set(k, v)
				c.Del(k)
			}
		}(g)
	}

	// Iterator goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 100 {
			it := c.Iterator()
			for {
				_, _, ok := it.GetNext(nil, nil)
				if !ok {
					break
				}
			}
		}
	}()

	wg.Wait()
}

func TestCache_EmptyKeyValue(t *testing.T) {
	c := kv.New(1 << 20)

	// Empty key, non-empty value.
	c.Set([]byte{}, []byte("val"))
	if !c.Has([]byte{}) {
		t.Fatal("Has returned false for empty key")
	}
	if string(c.Get(nil, []byte{})) != "val" {
		t.Fatal("wrong value for empty key")
	}

	// Non-empty key, empty value.
	c.Set([]byte("k"), []byte{})
	if !c.Has([]byte("k")) {
		t.Fatal("Has returned false for key with empty value")
	}
	if v := c.Get(nil, []byte("k")); len(v) != 0 {
		t.Fatalf("expected empty value, got %q", v)
	}

	// Both empty.
	c.Set([]byte{}, []byte{})
	if !c.Has([]byte{}) {
		t.Fatal("Has returned false for empty key and empty value")
	}
}

func TestCache_Stats(t *testing.T) {
	c := kv.New(1)

	// Sets.
	c.Set([]byte("a"), []byte("1"))
	c.Set([]byte("b"), []byte("2"))
	s := c.Stats()
	if s.Sets != 2 {
		t.Fatalf("expected Sets=2, got %d", s.Sets)
	}

	// Hits.
	c.Has([]byte("a"))      // gets+1, hit
	c.Get(nil, []byte("b")) // gets+1, hit
	s = c.Stats()
	if s.Gets != 2 {
		t.Fatalf("expected Gets=2, got %d", s.Gets)
	}
	if s.Misses != 0 {
		t.Fatalf("expected Misses=0, got %d", s.Misses)
	}

	// Misses.
	c.Has([]byte("missing"))      // gets+1, miss+1
	c.Get(nil, []byte("missing")) // gets+1, miss+1
	s = c.Stats()
	if s.Gets != 4 {
		t.Fatalf("expected Gets=4, got %d", s.Gets)
	}
	if s.Misses != 2 {
		t.Fatalf("expected Misses=2, got %d", s.Misses)
	}

	// Allocated (single shard, initialized).
	if s.Allocated != 1<<16 {
		t.Fatalf("expected Allocated=%d, got %d", 1<<16, s.Allocated)
	}
}
