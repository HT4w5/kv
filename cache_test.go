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
