package kv

import "sync"

var shardBufferPool = sync.Pool{}

func getShardBuffer() []byte {
	v := shardBufferPool.Get()
	if v == nil {
		return make([]byte, shardSize)
	}
	return v.([]byte)
}

func putShardBuffer(buf []byte) {
	shardBufferPool.Put(buf)
}

var shardMapPool = sync.Pool{}

func getShardMap() map[uint64]uint32 {
	m := shardMapPool.Get()
	if m == nil {
		m = make(map[uint64]uint32)
	}
	return m.(map[uint64]uint32)
}

func putShardMap(m map[uint64]uint32) {
	clear(m)
	shardMapPool.Put(m)
}
