package kv

import (
	"bytes"
	"encoding/binary"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/zeebo/xxh3"
)

func TestBucketWrap(t *testing.T) {
	var bkt bucket

	bkt.init(1) // 64K

	set := func(k, v []byte) {
		bkt.set(k, v, xxh3.Hash(k))
	}

	firstKey := []byte("first key")
	set(firstKey, firstKey)

	var seed [32]byte
	rnd := rand.NewChaCha8(seed)

	for range 60000 {
		var k [4]byte
		var v [4]byte
		rnd.Read(k[:])
		rnd.Read(v[:])
		set(k[:], v[:])
	}

	got, ok := bkt.get(nil, firstKey, xxh3.Hash(firstKey), true)
	if ok {
		t.Errorf("first key was not overwritten; got value: %s", got)
	}

	order := make([]int, 0, 100)
	for i := range 100 {
		order = append(order, i)
	}

	rand.New(rnd).Shuffle(100, func(i, j int) { order[i], order[j] = order[j], order[i] })

	for _, i := range order {
		k := []byte(strconv.Itoa(i))
		set(k, k)
	}

	for i := range 100 {
		k := []byte(strconv.Itoa(i))
		got, ok := bkt.get(nil, k, xxh3.Hash(k), true)
		if !ok {
			t.Errorf("key not found: %s", k)
		}
		if !bytes.Equal(k, got) {
			t.Errorf("value corrupt: expect %s, got %s", k, got)
		}
	}

	var stats Stats
	bkt.loadStats(&stats)
	t.Logf("stats: %+v", stats)
}

func TestBucketWrapClean(t *testing.T) {
	var bkt bucket

	bkt.init(chunkSize * 10) // 64K

	set := func(k, v []byte) {
		bkt.set(k, v, xxh3.Hash(k))
	}

	var seed [32]byte
	rndSrc := rand.NewChaCha8(seed)
	rnd := rand.New(rndSrc)

	buf := make([]byte, 2048)
	for i := range 60000 {
		k := "should-overwrite-" + strconv.Itoa(i)
		rndSrc.Read(buf)
		set([]byte(k), buf[:max(1, rnd.IntN(2048))])
	}

	for i := range 60000 {
		k := "new-padding-" + strconv.Itoa(i)
		rndSrc.Read(buf)
		set([]byte(k), buf[:max(1, rnd.IntN(2048))])
	}

	for i := range 60000 {
		k := "should-overwrite-" + strconv.Itoa(i)
		idx, ok := bkt.idxMap[xxh3.Hash([]byte(k))]
		if ok {
			t.Errorf("key %q should have been overwritten; got idx %d", k, idx)
		}
	}
}

func TestBucketDeallocation(t *testing.T) {
	var bkt bucket

	bkt.init(chunkSize * 16)

	set := func(k, v []byte) {
		bkt.set(k, v, xxh3.Hash(k))
	}

	var seed [32]byte
	rnd := rand.NewChaCha8(seed)

	order := make([]int, 0, 100)
	for i := range 100 {
		order = append(order, i)
	}

	rand.New(rnd).Shuffle(100, func(i, j int) { order[i], order[j] = order[j], order[i] })

	buf := make([]byte, 1145)
	for _, i := range order {
		var key [8]byte
		binary.BigEndian.PutUint64(buf, uint64(i))
		binary.BigEndian.PutUint64(key[:], uint64(i))
		set(key[:], buf)
	}

	for i := range 100 {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(i))
		bkt.del(xxh3.Hash(key[:]))
	}

	var stats Stats
	bkt.loadStats(&stats)

	t.Logf("stats: %+v", stats)

	if stats.Allocated != 0 {
		t.Errorf("allocated is not 0: %d", stats.Allocated)
	}
}
