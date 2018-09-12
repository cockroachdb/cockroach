// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package raftentry

import (
	"container/list"

	"go.etcd.io/etcd/raft/raftpb"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

var (
	metaEntryCacheSize = metric.Metadata{
		Name:        "raft.entrycache.size",
		Help:        "Number of Raft entries in the Raft entry cache",
		Measurement: "Entry Count",
		Unit:        metric.Unit_COUNT,
	}
	metaEntryCacheBytes = metric.Metadata{
		Name:        "raft.entrycache.bytes",
		Help:        "Aggregate size of all Raft entries in the Raft entry cache",
		Measurement: "Entry Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaEntryCacheAccesses = metric.Metadata{
		Name:        "raft.entrycache.accesses",
		Help:        "Number of cache lookups in the Raft entry cache",
		Measurement: "Accesses",
		Unit:        metric.Unit_COUNT,
	}
	metaEntryCacheHits = metric.Metadata{
		Name:        "raft.entrycache.hits",
		Help:        "Number of successful cache lookups in the Raft entry cache",
		Measurement: "Hits",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics is the set of metrics for the raft entry cache.
type Metrics struct {
	Size     *metric.Gauge
	Bytes    *metric.Gauge
	Accesses *metric.Counter
	Hits     *metric.Counter
}

func makeMetrics() Metrics {
	return Metrics{
		Size:     metric.NewGauge(metaEntryCacheSize),
		Bytes:    metric.NewGauge(metaEntryCacheBytes),
		Accesses: metric.NewCounter(metaEntryCacheAccesses),
		Hits:     metric.NewCounter(metaEntryCacheHits),
	}
}

// Cache maintains a global cache of Raft group log entries. The cache mostly
// prevents unnecessary reads from disk of recently-written log entries between
// log append and application to the FSM.
//
// This cache stores entries with sideloaded proposals inlined (i.e. ready to be
// sent to followers).
type Cache struct {
	mu       syncutil.Mutex
	parts    map[roachpb.RangeID]*partition
	lru      list.List
	size     uint64 // count of all entries
	bytes    uint64 // aggregate size of all entries
	maxBytes uint64
	metrics  Metrics
}

// NB: partitions could maintain their own lock, allowing for a partitioned
// locking scheme where Cache only needs to hold a read lock when a partition is
// performing internal modifications. However, a global write lock would still
// be necessary to maintain the linked-list to enforce the LRU policy. Since we
// don't expect internal modifications to partitions to be slow due to the
// constant-time operations on the entry hashmap, this doesn't appear to be
// worth it.
type partition struct {
	r     roachpb.RangeID
	ents  map[uint64]raftpb.Entry
	el    *list.Element
	bytes uint64 // aggregate size of all entries in partition
}

// NewCache returns a new Cache instance.
func NewCache(maxBytes uint64) *Cache {
	return &Cache{
		parts:    make(map[roachpb.RangeID]*partition),
		maxBytes: maxBytes,
		metrics:  makeMetrics(),
	}
}

// Add adds the slice of Raft entries to the cache.
func (c *Cache) Add(r roachpb.RangeID, ents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get the partition. Create one if missing.
	p, ok := c.parts[r]
	if !ok {
		p = &partition{
			r:    r,
			ents: make(map[uint64]raftpb.Entry, len(ents)),
		}
		p.el = c.lru.PushFront(p)
		c.parts[r] = p
	} else {
		c.lru.MoveToFront(p.el)
	}

	// Add entries to the current partition.
	for _, e := range ents {
		s := uint64(e.Size())

		// Check if we're replacing an existing entry.
		if oldE, ok := p.ents[e.Index]; ok {
			// NB: it's possible that we're replacing an entry with a larger one
			// that puts us over the size limit. We could delete the old entry
			// and not insert the new one, but that would make this code more
			// complicated, so it's not worth it to prevent that rare case.
			s -= uint64(oldE.Size())
			p.ents[e.Index] = e
			p.bytes += s
			c.bytes += s
		} else {
			// If this new entry would push us above the byte limit, evict
			// partitions to try to get below the limit.
			for c.bytes+s > c.maxBytes && c.lru.Len() > 1 {
				e := c.lru.Back()
				ep := c.lru.Remove(e).(*partition)
				c.bytes -= ep.bytes
				c.size -= uint64(len(ep.ents))
				delete(c.parts, ep.r)
			}
			// If this will still push us above the limit, we must be the only
			// partition. Break if this isn't the first entry in this partition.
			if c.bytes+s > c.maxBytes {
				if c.lru.Len() != 1 {
					panic("unreachable")
				}
				if len(p.ents) > 0 {
					break
				}
			}

			p.ents[e.Index] = e
			c.size++
			p.bytes += s
			c.bytes += s
		}
	}
	c.updateGauges()
}

// Clear deletes entries between [lo, hi) for specified range.
func (c *Cache) Clear(r roachpb.RangeID, lo, hi uint64) {
	if lo >= hi {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	p, ok := c.parts[r]
	if !ok {
		return
	}
	// Don't `c.lru.MoveToFront(p.el)`. Clearing from a partition
	// is not considered an "access" with respect to the LRU policy.

	// Delete entries in range. Maintain stats.
	for i := lo; i < hi && len(p.ents) > 0; i++ {
		if e, ok := p.ents[i]; ok {
			delete(p.ents, i)
			c.size--
			c.bytes -= uint64(e.Size())
		}
	}
	c.updateGauges()

	// Delete the partition if it's now empty.
	if len(p.ents) == 0 {
		c.lru.Remove(p.el)
		delete(c.parts, p.r)
	}
}

// Get returns the entry for the specified index and true for the second return
// value. If the index is not present in the cache, false is returned.
func (c *Cache) Get(r roachpb.RangeID, idx uint64) (raftpb.Entry, bool) {
	c.metrics.Accesses.Inc(1)
	c.mu.Lock()
	defer c.mu.Unlock()

	p, ok := c.parts[r]
	if !ok {
		return raftpb.Entry{}, false
	}
	c.lru.MoveToFront(p.el)

	e, ok := p.ents[idx]
	if ok {
		c.metrics.Hits.Inc(1)
	}
	return e, ok
}

// Scan returns entries between [lo, hi) for specified range. If any entries are
// returned for the specified indexes, they will start with index lo and proceed
// sequentially without gaps until 1) all entries exclusive of hi are fetched,
// 2) fetching another entry would add up to more than maxBytes of data, or 3) a
// cache miss occurs. The returned size reflects the size of the returned
// entries.
func (c *Cache) Scan(
	ents []raftpb.Entry, r roachpb.RangeID, lo, hi, maxBytes uint64,
) (_ []raftpb.Entry, bytes uint64, nextIdx uint64, exceededMaxBytes bool) {
	c.metrics.Accesses.Inc(1)
	c.mu.Lock()
	defer c.mu.Unlock()

	p, ok := c.parts[r]
	if !ok {
		return ents, 0, lo, false
	}
	c.lru.MoveToFront(p.el)

	for nextIdx = lo; nextIdx < hi && !exceededMaxBytes; nextIdx++ {
		e, ok := p.ents[nextIdx]
		if !ok {
			// Break if there are any gaps.
			break
		}
		s := uint64(e.Size())
		if bytes+s > maxBytes {
			exceededMaxBytes = true
			if len(ents) > 0 {
				break
			}
		}
		bytes += s
		ents = append(ents, e)
	}
	if nextIdx == hi || exceededMaxBytes {
		// We only consider an access a "hit" if it returns all requested
		// entries or stops short because of a maximum bytes limit.
		c.metrics.Hits.Inc(1)
	}
	return ents, bytes, nextIdx, exceededMaxBytes
}

// Metrics returns a struct which contains metrics for the cache.
func (c *Cache) Metrics() Metrics { return c.metrics }

func (c *Cache) updateGauges() {
	c.metrics.Bytes.Update(int64(c.bytes))
	c.metrics.Size.Update(int64(c.size))
}
