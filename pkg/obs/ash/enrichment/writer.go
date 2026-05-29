// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package enrichment

import (
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
)

// shardCount is the number of independent concurrentWriteBuffers the
// writer fans incoming records out to. Picking a power of two lets the
// hash-to-shard step use a cheap mask. 16 matches the value txnidcache
// uses and is a reasonable starting point until benchmarking suggests
// otherwise.
const shardCount = 16

// writer fans records out across a fixed number of concurrent shards.
// Each shard owns an independent block, so unrelated executions on
// different goroutines almost never contend with one another.
type writer struct {
	st     *cluster.Settings
	shards [shardCount]*concurrentWriteBuffer
}

func newWriter(st *cluster.Settings, sink blockSink) *writer {
	w := &writer{st: st}
	for i := 0; i < shardCount; i++ {
		w.shards[i] = newConcurrentWriteBuffer(sink)
	}
	return w
}

// record routes the (id, attrs) pair to the shard chosen by hashing id.
// It is safe to call concurrently and is a no-op when the cache is
// disabled by cluster setting.
func (w *writer) record(id clusterunique.ID, attrs Attributes) {
	if !Enabled.Get(&w.st.SV) || CacheLimit.Get(&w.st.SV) == 0 {
		return
	}
	w.shards[shardIdx(id)].record(id, attrs)
}

// drain forces every shard's in-flight block to the sink. Useful in
// tests and as a hook before reads that must observe recent writes.
func (w *writer) drain() {
	for i := 0; i < shardCount; i++ {
		w.shards[i].drain()
	}
}

// shardIdx picks a shard for id. The low 64 bits of a clusterunique.ID
// already mix the HLC logical counter with the SQL instance ID, which
// is plenty of entropy for shard selection.
func shardIdx(id clusterunique.ID) int {
	return int(id.Lo & (shardCount - 1))
}
