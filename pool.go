package kv

import "sync"

var shardBufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, shardSize)
		return &b
	},
}

func getShardBuffer() *[]byte {
	return shardBufferPool.Get().(*[]byte)
}

func putShardBuffer(buf *[]byte) {
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
