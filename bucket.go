package kv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zeebo/xxh3"
)

const (
	chunkSize    = 1 << 16 // 64KB
	vacuumFactor = 2
)

var chunkPool = sync.Pool{
	New: func() any {
		return &chunk{
			b: make([]byte, chunkSize),
		}
	},
}

type chunk struct {
	b        []byte
	useCount int
}

type bucket struct {
	mu     sync.RWMutex
	idxMap map[uint64]uint64 // hash -> ring index
	chunks []*chunk
	idx    uint64

	statGets          atomic.Int64
	statSets          atomic.Int64
	statMisses        atomic.Int64
	statCollisions    atomic.Int64
	statVacuums       atomic.Int64
	statAllocations   atomic.Int64
	statDeallocations atomic.Int64
	statAllocated     atomic.Int64
}

func (b *bucket) init(size int) {
	numChunks := (size + chunkSize - 1) / chunkSize
	b.idxMap = make(map[uint64]uint64)
	b.chunks = make([]*chunk, numChunks)
}

const (
	localIdxMask = (1 << 16) - 1
	chunkIdxMask = (1 << 47) - 1
	wrapMask     = 1 << 63
)

func localIdx(idx uint64) int {
	return int(idx & localIdxMask)
}

func chunkIdx(idx uint64) int {
	return int((idx >> 16) & chunkIdxMask)
}

func wrap(idx uint64) uint64 {
	return idx & wrapMask
}

func buildIdx(wrap uint64, localIdx, chunkIdx int) uint64 {
	return (wrap & wrapMask) | ((uint64(chunkIdx) & chunkIdxMask) << 16) | (uint64(localIdx) & localIdxMask)
}

// whether idx is still valid when bucket is at bktIdx
func isValid(idx uint64, bktIdx uint64) bool {
	return int64(idx-bktIdx) < 0
}

func (bkt *bucket) set(k, v []byte, h uint64) {
	dataLen := 4 + len(k) + len(v)
	if dataLen >= chunkSize {
		// too large
		return
	}

	bkt.statSets.Add(1)

	bkt.mu.Lock()
	defer bkt.mu.Unlock()

	locIdx := localIdx(bkt.idx)
	chkIdx := chunkIdx(bkt.idx)
	wrp := wrap(bkt.idx)

	if chunkSize-locIdx < dataLen {
		// Start from next chunk
		locIdx = 0
		chkIdx++
	}

	if chkIdx == len(bkt.chunks) {
		// clean overwritten entires in idxMap
		prevMapSize := len(bkt.idxMap)
		newMapSize := 0
		for h, idx := range bkt.idxMap {
			if isValid(idx, bkt.idx) {
				newMapSize++
			} else {
				delete(bkt.idxMap, h)
				chkIdx := chunkIdx(idx)
				chk := bkt.chunks[chkIdx]
				if chk == nil {
					continue
				}
				chk.useCount--
				if chk.useCount == 0 {
					bkt.statDeallocations.Add(1)
					bkt.statAllocated.Add(-chunkSize)
					chunkPool.Put(chk)
					bkt.chunks[chkIdx] = nil
				}
			}
		}
		if prevMapSize > newMapSize*vacuumFactor {
			bkt.statVacuums.Add(1)
			newMap := make(map[uint64]uint64, newMapSize)
			maps.Copy(newMap, bkt.idxMap)
			bkt.idxMap = newMap
		}

		locIdx = 0
		chkIdx = 0
		wrp ^= wrapMask
	}

	oldIdx, ok := bkt.idxMap[h]
	if ok {
		oldChkIdx := chunkIdx(oldIdx)
		oldChk := bkt.chunks[oldChkIdx]
		if oldChk != nil {
			oldChk.useCount--
			if oldChk.useCount == 0 {
				bkt.statDeallocations.Add(1)
				bkt.statAllocated.Add(-chunkSize)
				chunkPool.Put(oldChk)
				bkt.chunks[oldChkIdx] = nil
			}
		}
	}

	bkt.idxMap[h] = buildIdx(wrp, locIdx, chkIdx)

	chk := bkt.chunks[chkIdx]
	if chk == nil {
		bkt.statAllocations.Add(1)
		bkt.statAllocated.Add(chunkSize)
		chk = chunkPool.Get().(*chunk)
		bkt.chunks[chkIdx] = chk
	}
	binary.LittleEndian.PutUint16(chk.b[locIdx:], uint16(len(k)))
	locIdx += 2
	binary.LittleEndian.PutUint16(chk.b[locIdx:], uint16(len(v)))
	locIdx += 2
	copy(chk.b[locIdx:], k)
	locIdx += len(k)
	copy(chk.b[locIdx:], v)
	locIdx += len(v)
	chk.useCount++

	if locIdx == chunkSize {
		locIdx = 0
		chkIdx++
	}

	bkt.idx = buildIdx(wrp, locIdx, chkIdx)

}

func (bkt *bucket) get(dst, k []byte, h uint64, copy bool) ([]byte, bool) {
	bkt.statGets.Add(1)
	bkt.mu.RLock()
	defer bkt.mu.RUnlock()

	idx, ok := bkt.idxMap[h]
	if !ok {
		bkt.statMisses.Add(1)
		return nil, false
	}

	if isValid(idx, bkt.idx) {
		locIdx := localIdx(idx)
		chkIdx := chunkIdx(idx)

		if chkIdx >= len(bkt.chunks) {
			// corrupt record: chunk index out of bounds
			return nil, false
		}

		chk := bkt.chunks[chkIdx]
		if chk == nil {
			// corrupt record: chunk should have been allocated
			return nil, false
		}

		if chunkSize-locIdx < 4 {
			// corrupt record: data should be at least 4 bytes before end of chunk
			return nil, false
		}

		kLen := int(binary.LittleEndian.Uint16(chk.b[locIdx:]))
		vLen := int(binary.LittleEndian.Uint16(chk.b[locIdx+2:]))
		dataLen := 4 + kLen + vLen

		if chunkSize-locIdx < dataLen {
			// corrupt record: data should be at least dataLen bytes before end of chunk
			return nil, false
		}

		locIdx += 4

		if !bytes.Equal(k, chk.b[locIdx:locIdx+kLen]) {
			// collision: two different keys have the same hash
			bkt.statCollisions.Add(1)
			return nil, false
		}

		locIdx += kLen

		if copy {
			dst = append(dst, chk.b[locIdx:locIdx+vLen]...)
			return dst, true
		} else {
			return nil, true
		}
	}

	bkt.statMisses.Add(1)
	return nil, false
}

func (bkt *bucket) del(h uint64) {
	bkt.mu.Lock()
	defer bkt.mu.Unlock()

	idx, ok := bkt.idxMap[h]
	if !ok {
		return
	}

	delete(bkt.idxMap, h)

	chkIdx := chunkIdx(idx)
	if chkIdx >= len(bkt.chunks) {
		// corrupt record: chunk index out of bounds
		return
	}

	chk := bkt.chunks[chkIdx]
	if chk != nil {
		chk.useCount--
		if chk.useCount == 0 {
			bkt.statDeallocations.Add(1)
			bkt.statAllocated.Add(-chunkSize)
			chunkPool.Put(chk)
			bkt.chunks[chkIdx] = nil
		}
	}
}

func (bkt *bucket) reset() {
	bkt.mu.Lock()
	defer bkt.mu.Unlock()

	clear(bkt.idxMap)
	bkt.idx = 0
	for i := range len(bkt.chunks) {
		chk := bkt.chunks[i]
		if chk != nil {
			chk.useCount = 0
			chunkPool.Put(chk)
			bkt.chunks[i] = nil
		}
	}

	bkt.statGets.Store(0)
	bkt.statSets.Store(0)
	bkt.statMisses.Store(0)
	bkt.statCollisions.Store(0)
	bkt.statVacuums.Store(0)
	bkt.statAllocations.Store(0)
	bkt.statDeallocations.Store(0)
	bkt.statAllocated.Store(0)
}

func (bkt *bucket) iterator() *bucketIter {
	bkt.mu.RLock()
	defer bkt.mu.RUnlock()

	it := &bucketIter{
		bkt: bkt,
		elems: make([]struct {
			hash uint64
			idx  uint64
		}, 0, len(bkt.idxMap)),
		elemIdx: -1,
	}

	for hash, idx := range bkt.idxMap {
		it.elems = append(it.elems, struct {
			hash uint64
			idx  uint64
		}{
			hash: hash,
			idx:  idx,
		})
	}

	return it
}

type bucketIter struct {
	bkt   *bucket
	elems []struct {
		hash uint64
		idx  uint64
	}
	elemIdx int
}

func (it *bucketIter) getNext(kDst, vDst []byte) (kRes, vRes []byte, ok bool) {
	it.bkt.mu.RLock()
	defer it.bkt.mu.RUnlock()

	for {
		it.elemIdx++
		if it.elemIdx >= len(it.elems) {
			return
		}

		hash, idx := it.elems[it.elemIdx].hash, it.elems[it.elemIdx].idx
		if isValid(idx, it.bkt.idx) {
			locIdx := localIdx(idx)
			chkIdx := chunkIdx(idx)
			if chkIdx >= len(it.bkt.chunks) {
				// stale record: chunk index out of bounds
				continue
			}
			chk := it.bkt.chunks[chkIdx]
			if chk == nil {
				// stale record: chunk not allocated
				continue
			}

			if chunkSize-locIdx < 4 {
				// corrupt record: data should be at least 4 bytes before end of chunk
				continue
			}

			kLen := int(binary.LittleEndian.Uint16(chk.b[locIdx:]))
			vLen := int(binary.LittleEndian.Uint16(chk.b[locIdx+2:]))
			dataLen := 4 + kLen + vLen

			if chunkSize-locIdx < dataLen {
				// corrupt record: data should be at least dataLen bytes before end of chunk
				continue
			}

			locIdx += 4

			if xxh3.Hash(chk.b[locIdx:locIdx+kLen]) != hash {
				// collision: two different keys have the same hash
				continue
			}

			kRes = append(kDst, chk.b[locIdx:locIdx+kLen]...)
			locIdx += kLen
			vRes = append(vDst, chk.b[locIdx:locIdx+vLen]...)
			ok = true
			return
		}
	}
}

type debugIdxMapEntry struct {
	Hash    uint64
	Idx     uint64
	LocIdx  int
	ChkIdx  int
	Wrap    bool
	KLen    int
	VLen    int
	Key     string
	Corrupt bool
	Remark  string
}

type debugChunkEntry struct {
	Idx      int
	Allocted bool
	UseCount int
}

func (bkt *bucket) debugDump() {
	// Verify every pair in idxMap to find stale entries.
	f, err := os.Create("bucket-debug-dump-" + time.Now().String())
	if err != nil {
		fmt.Printf("debugDump: failed to open file: %v", err)
		return
	}
	defer f.Close()
	bw := bufio.NewWriter(f)
	defer bw.Flush()

	enc := json.NewEncoder(bw)
	enc.SetIndent("", "")

	fmt.Fprintf(bw, `{"idx": %d,"idxMap":[`, bkt.idx)

	i := 0
	for hash, idx := range bkt.idxMap {
		if i > 0 {
			bw.WriteByte(',')
		}
		i++

		entry := debugIdxMapEntry{
			Hash: hash,
			Idx:  idx,
		}

		func() {
			if !isValid(idx, bkt.idx) {
				entry.Remark = "filtered"
				return
			}

			locIdx := localIdx(idx)
			chkIdx := chunkIdx(idx)
			wrp := wrap(idx)

			entry.LocIdx = locIdx
			entry.ChkIdx = chkIdx
			entry.Wrap = wrp == wrapMask

			if chunkSize-locIdx < 4 {
				entry.Corrupt = true
				entry.Remark = fmt.Sprintf("locIdx too close to chunk border: %d", locIdx)
				return
			}

			if chkIdx >= len(bkt.chunks) {
				entry.Corrupt = true
				entry.Remark = fmt.Sprintf("chkIdx out of bounds: %d", chkIdx)
				return
			}

			chk := bkt.chunks[chkIdx]
			if chk == nil {
				entry.Corrupt = true
				entry.Remark = fmt.Sprintf("chunk %d not allocated", chkIdx)
			}

			kLen := int(binary.LittleEndian.Uint16(chk.b[locIdx:]))
			vLen := int(binary.LittleEndian.Uint16(chk.b[locIdx+2:]))

			entry.KLen = kLen
			entry.VLen = vLen

			if chunkSize-locIdx < 4+kLen+vLen {
				entry.Corrupt = true
				entry.Remark = fmt.Sprintf("locIdx too close to chunk border: %d", locIdx)
				return
			}

			entry.Key = string(chk.b[locIdx+4 : locIdx+4+kLen])

			if hash != xxh3.Hash(chk.b[locIdx+4:locIdx+4+kLen]) {
				entry.Corrupt = true
				entry.Remark = "key hash mismatch"
			}
		}()

		if err := enc.Encode(entry); err != nil {
			fmt.Printf("debugDump: %v", err)
		}
	}

	fmt.Fprint(bw, `],"chunks":[`)

	for idx, chk := range bkt.chunks {
		if idx > 0 {
			bw.WriteByte(',')
		}

		entry := debugChunkEntry{
			Idx:      idx,
			Allocted: chk != nil,
		}

		if chk != nil {
			entry.UseCount = int(chk.useCount)
		}

		if err := enc.Encode(entry); err != nil {
			fmt.Printf("debugDump: %v", err)
		}
	}

	fmt.Fprint(bw, `]}`)
}
