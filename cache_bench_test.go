package kv_test

import (
	"fmt"
	"testing"

	kv "github.com/HT4w5/kv"
)

// These benchmarks are modified from
// https://github.com/VictoriaMetrics/fastcache/blob/v1.13.3/fastcache_timing_test.go,
// licensed under The MIT License
//
// The MIT License (MIT)
//
// Copyright (c) 2018 VictoriaMetrics
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// Modified by HT4w5 in 2026.

var sizes = []int{(1 << 16) * 512, (1024 * 1024 * 1024)}

func BenchmarkCacheSet(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("Size-%d", n), func(b *testing.B) {
			benchmarkCacheSet(b, n)
		})
	}
}

func BenchmarkCacheGet(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("Size-%d", n), func(b *testing.B) {
			benchmarkCacheGet(b, n)
		})
	}
}

func BenchmarkCacheHas(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("Size-%d", n), func(b *testing.B) {
			benchmarkCacheHas(b, n)
		})
	}
}

func BenchmarkCacheSetGet(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("Size-%d", n), func(b *testing.B) {
			benchmarkCacheSetGet(b, n)
		})
	}
}

func BenchmarkCacheIterator(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("Size-%d", n), func(b *testing.B) {
			benchmarkCacheIterator(b, n)
		})
	}
}

func benchmarkCacheSet(b *testing.B, size int) {
	c := kv.New(size)
	defer c.Reset()

	b.ReportAllocs()
	// SetBytes should reflect the size of data handled per iteration (key + value)
	b.SetBytes(4 + 4)
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		v := []byte("xyza")
		for pb.Next() {
			// Remove the 'items' loop to measure single operation latency
			k[0]++
			if k[0] == 0 {
				k[1]++
			}
			c.Set(k, v)
		}
	})
}

func benchmarkCacheGet(b *testing.B, size int) {
	c := kv.New(size)
	defer c.Reset()
	// Pre-fill the cache
	k := []byte("\x00\x00\x00\x00")
	v := []byte("xyza")
	for i := 0; i < b.N; i++ {
		k[0]++
		if k[0] == 0 {
			k[1]++
		}
		c.Set(k, v)
	}

	b.ReportAllocs()
	b.SetBytes(4 + 4)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var buf []byte
		k := []byte("\x00\x00\x00\x00")
		for pb.Next() {
			k[0]++
			if k[0] == 0 {
				k[1]++
			}

			buf = c.Get(buf[:0], k)
			if len(buf) == 0 {
				b.Fatal("BUG: missing value")
			}
		}
	})
}

func benchmarkCacheHas(b *testing.B, size int) {
	const items = 1 << 16
	c := kv.New(size)
	defer c.Reset()
	k := []byte("\x00\x00\x00\x00")
	for i := 0; i < items; i++ {
		k[0]++
		if k[0] == 0 {
			k[1]++
		}
		c.Set(k, nil)
	}

	b.ReportAllocs()
	b.SetBytes(4) // Key only
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		for pb.Next() {
			k[0]++
			if k[0] == 0 {
				k[1]++
			}
			if !c.Has(k) {
				b.Fatal("BUG: missing value")
			}
		}
	})
}

func benchmarkCacheSetGet(b *testing.B, size int) {
	c := kv.New(size)
	defer c.Reset()
	b.ReportAllocs()
	b.SetBytes(16)
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		v := []byte("xyza")
		var buf []byte
		for pb.Next() {
			k[0]++
			if k[0] == 0 {
				k[1]++
			}

			c.Set(k, v)
			buf, _ = c.HasGet(buf[:0], k)
			if len(buf) == 0 {
				b.Fatal("BUG: invalid value")
			}
		}
	})
}

func benchmarkCacheIterator(b *testing.B, size int) {
	c := kv.New(size)
	defer c.Reset()
	k := []byte("\x00\x00\x00\x00")
	v := []byte("xyza")
	for range b.N {
		k[0]++
		if k[0] == 0 {
			k[1]++
		}
		c.Set(k, v)
	}

	it := c.Iterator()
	b.ReportAllocs()
	b.SetBytes(8)
	b.ResetTimer()
	for range b.N {
		_, _, _ = it.GetNext(nil, nil)
	}
}

// These benchmarks are original.
func BenchmarkCacheSetDelete(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("Size-%d", n), func(b *testing.B) {
			benchmarkCacheSetDelete(b, n)
		})
	}
}

func benchmarkCacheSetDelete(b *testing.B, size int) {
	c := kv.New(size)
	defer c.Reset()
	b.ReportAllocs()
	b.SetBytes(16)
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		v := []byte("xyza")
		for pb.Next() {
			k[0]++
			if k[0] == 0 {
				k[1]++
			}

			c.Set(k, v)
			c.Del(k)
		}
	})
}
