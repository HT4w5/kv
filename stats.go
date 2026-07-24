package kv

// Stats represents global stats of Cache.
type Stats struct {
	// Number of Get() calls (including Has() but not Iterator.GetNext()).
	Gets uint64
	// Number of Set() calls.
	Sets          uint64
	Misses        uint64
	Wraps         uint64
	Collisions    uint64
	Allocations   uint64
	Deallocations uint64
	Allocated     uint64
}

func (s *shard) loadStats(stats *Stats) {
	stats.Gets += s.gets.Load()
	stats.Sets += s.sets.Load()
	stats.Misses += s.misses.Load()
	stats.Wraps += s.wraps.Load()
	stats.Collisions += s.collisions.Load()
	stats.Allocations += s.allocations.Load()
	stats.Deallocations += s.deallocations.Load()
	if s.idxMap != nil {
		stats.Allocated += shardSize
	}
}

// Stats() acquires global stats of Cache.
func (c *Cache) Stats() Stats {
	var stats Stats
	for i := range len(c.shards) {
		c.shards[i].loadStats(&stats)
	}
	return stats
}
