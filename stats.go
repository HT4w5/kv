package kv

// Stats represents global stats of Cache.
type Stats struct {
	// Number of Get() calls (including Has() but not Iterator.GetNext()).
	Gets int64
	// Number of Set() calls.
	Sets int64
	// Number of Get() misses (including Has() but not Iterator.GetNext()).
	Misses int64
	// Number of Get() hash collisions (including Has() but not Iterator.GetNext()).
	Collisions int64
	// Number of map re-creations.
	Vacuums int64
	// Number of chunk allocations.
	Allocations int64
	// Number of chunk deallocations.
	Deallocations int64
	// Size of allocated chunks.
	Allocated int64
}

func (bkt *bucket) loadStats(stats *Stats) {
	stats.Gets += bkt.statGets.Load()
	stats.Sets += bkt.statSets.Load()
	stats.Misses += bkt.statMisses.Load()
	stats.Collisions += bkt.statCollisions.Load()
	stats.Vacuums += bkt.statVacuums.Load()
	stats.Allocations += bkt.statAllocations.Load()
	stats.Deallocations += bkt.statDeallocations.Load()
	stats.Allocated += bkt.statAllocated.Load()
}

// Stats() acquires global stats of Cache.
func (c *Cache) Stats() Stats {
	var stats Stats
	for i := range numBuckets {
		c.buckets[i].loadStats(&stats)
	}
	return stats
}
