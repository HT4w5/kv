package kv

import (
	"bytes"
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/zeebo/xxh3"
)

const (
	shardSize = 1 << 16 // 64KB
)

type shard struct {
	idxMap map[uint64]uint32
	b      []byte
	mu     sync.Mutex
	idx    uint32

	gets          atomic.Uint64
	sets          atomic.Uint64
	misses        atomic.Uint64
	wraps         atomic.Uint64
	collisions    atomic.Uint64
	allocations   atomic.Uint64
	deallocations atomic.Uint64
	_             [24]byte
}

// Caller must acquire write lock.
func (s *shard) init() {
	s.allocations.Add(1)
	s.b = getShardBuffer()
	s.idxMap = getShardMap()
	s.idx = 0
}

func isOneIterationBefore(pairIdx, shardIdx uint32) bool {
	return int32(pairIdx-shardIdx) < 0
}

func (s *shard) set(k, v []byte, h uint64) {
	s.sets.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.b == nil {
		s.init()
	}

	if 4+len(k)+len(v) >= shardSize {
		// Payload too large
		return
	}

	s.idxMap[h] = s.idx

	oldPtr := uint16(s.idx)
	ptr := oldPtr

	ptr = ringWriteUint16(ptr, s.b, uint16(len(k)))
	ptr = ringWriteUint16(ptr, s.b, uint16(len(v)))
	ptr = ringWriteTo(ptr, s.b, k)
	ptr = ringWriteTo(ptr, s.b, v)

	if ptr < oldPtr {
		// Wrap
		s.idx = ((s.idx ^ (1 << 31)) & (1 << 31)) | (uint32(ptr) &^ (1 << 31))
		newCap := 0
		for _, idx := range s.idxMap {
			if isOneIterationBefore(idx, s.idx) {
				newCap++
			}
		}
		newMap := make(map[uint64]uint32, newCap)
		for k, idx := range s.idxMap {
			if isOneIterationBefore(idx, s.idx) {
				newMap[k] = idx
			}
		}
		s.idxMap = newMap
		s.wraps.Add(1)
	} else {
		s.idx = (s.idx & (1 << 31)) | uint32(ptr)
	}
}

func (s *shard) get(dst, k []byte, h uint64, copy bool) ([]byte, bool) {
	s.gets.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.b == nil {
		s.misses.Add(1)
		return nil, false
	}

	idx, ok := s.idxMap[h]
	if !ok {
		s.misses.Add(1)
		return nil, false
	}

	if isOneIterationBefore(idx, s.idx) {
		ptr := uint16(idx)
		var kLen, vLen uint16
		var ok bool

		ptr, kLen = ringReadUint16(ptr, s.b)
		if kLen != uint16(len(k)) {
			s.collisions.Add(1)
			s.misses.Add(1)
			delete(s.idxMap, h)
			return nil, false
		}
		ptr, vLen = ringReadUint16(ptr, s.b)
		ptr, ok = ringEqual(ptr, s.b, k)
		if !ok {
			s.collisions.Add(1)
			s.misses.Add(1)
			delete(s.idxMap, h)
			return nil, false
		}

		if copy {
			dst = dst[:0]
			_, dst = ringReadTo(ptr, s.b, int(vLen), dst)
			return dst, true
		} else {
			return nil, true
		}
	}

	s.misses.Add(1)
	return nil, false
}

func ringReadUint16(ptr uint16, b []byte) (newPtr, v uint16) {
	if ptr <= shardSize-2 {
		return ptr + 2, binary.LittleEndian.Uint16(b[ptr:])
	}

	var buf [2]byte
	buf[0] = b[ptr]
	buf[1] = b[0]
	return 1, binary.LittleEndian.Uint16(buf[:])
}

func ringWriteUint16(ptr uint16, b []byte, v uint16) (newPtr uint16) {
	if ptr <= shardSize-2 {
		binary.LittleEndian.PutUint16(b[ptr:], v)
		return ptr + 2
	}

	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], v)
	b[ptr] = buf[0]
	b[0] = buf[1]
	return 1
}

func ringEqual(ptr uint16, b []byte, v []byte) (newPtr uint16, ok bool) {
	rem := shardSize - int(ptr)

	if rem > len(v) {
		// No fragmentation
		return ptr + uint16(len(v)), bytes.Equal(b[ptr:ptr+uint16(len(v))], v)
	}

	newPtr = uint16(len(v) - rem)

	if !bytes.Equal(b[ptr:], v[:rem]) {
		ok = false
		return
	}

	ok = bytes.Equal(b[:newPtr], v[rem:])
	return
}

func ringHash(ptr uint16, b []byte, n int) (newPtr uint16, hash uint64) {
	hasher := xxh3.New()
	rem := shardSize - int(ptr)

	if rem > n {
		// No fragmentation
		hasher.Write(b[ptr : ptr+uint16(n)])
		return ptr + uint16(n), hasher.Sum64()
	}

	newPtr = uint16(n - rem)

	hasher.Write(b[ptr:])
	hasher.Write(b[:newPtr])

	return newPtr, hasher.Sum64()
}

func ringReadTo(ptr uint16, b []byte, n int, dst []byte) (newPtr uint16, res []byte) {
	rem := shardSize - int(ptr)

	if rem > n {
		// No fragmentation
		return ptr + uint16(n), append(dst, b[ptr:ptr+uint16(n)]...)
	}

	newPtr = uint16(n - rem)

	dst = append(dst, b[ptr:]...)
	return newPtr, append(dst, b[:newPtr]...)
}

func ringWriteTo(ptr uint16, b []byte, v []byte) (newPtr uint16) {
	rem := shardSize - int(ptr)

	if rem > len(v) {
		// No fragmentation
		newPtr = ptr + uint16(copy(b[ptr:], v))
		return
	}

	newPtr = uint16(len(v) - rem)

	copy(b[ptr:], v[:rem])
	copy(b, v[rem:])
	return
}

func (s *shard) del(h uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.b == nil {
		return
	}
	delete(s.idxMap, h)
	if len(s.idxMap) == 0 {
		s.deallocations.Add(1)
		putShardBuffer(s.b)
		putShardMap(s.idxMap)
		s.b = nil
		s.idxMap = nil
	}
}

func (s *shard) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.b != nil {
		putShardBuffer(s.b)
		putShardMap(s.idxMap)
		s.b = nil
		s.idxMap = nil
	}

	s.gets.Store(0)
	s.sets.Store(0)
	s.misses.Store(0)
	s.wraps.Store(0)
	s.collisions.Store(0)
	s.allocations.Store(0)
	s.deallocations.Store(0)
}

func (s *shard) iterator() *shardIter {
	s.mu.Lock()
	defer s.mu.Unlock()

	it := &shardIter{
		s: s,
		pairs: make([]struct {
			hash uint64
			idx  uint32
		}, 0, len(s.idxMap)),
		i: -1,
	}

	for h, idx := range s.idxMap {
		it.pairs = append(it.pairs, struct {
			hash uint64
			idx  uint32
		}{
			hash: h,
			idx:  idx,
		})
	}

	return it
}

type shardIter struct {
	s     *shard
	pairs []struct {
		hash uint64
		idx  uint32
	}
	i int
}

func (it *shardIter) getNext(kDst, vDst []byte) (kRes []byte, vRes []byte, ok bool) {
	it.s.mu.Lock()
	defer it.s.mu.Unlock()

	for {
		it.i++
		if it.i >= len(it.pairs) {
			return nil, nil, false
		}

		h, idx := it.pairs[it.i].hash, it.pairs[it.i].idx

		if isOneIterationBefore(idx, it.s.idx) {
			ptr := uint16(idx)
			var kLen, vLen uint16

			ptr, kLen = ringReadUint16(ptr, it.s.b)
			ptr, vLen = ringReadUint16(ptr, it.s.b)

			if _, hash := ringHash(ptr, it.s.b, int(kLen)); hash != h {
				continue
			}

			kDst = kDst[:0]
			ptr, kDst = ringReadTo(ptr, it.s.b, int(kLen), kDst)
			vDst = vDst[:0]
			ptr, vDst = ringReadTo(ptr, it.s.b, int(vLen), vDst)
			return kDst, vDst, true
		}
	}
}
