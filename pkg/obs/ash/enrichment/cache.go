// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package enrichment implements the per-execution attribute cache that
// backs ASH (Active Session History) sample enrichment.
//
// At a high level: every SQL execution writes its identifying
// attributes (user, database, app name, plan gist, etc.) into the
// cache keyed by the execution's clusterunique.ID. The sampler later
// associates each sample with that same ID, and a background enricher
// reads attributes back out — either from the local cache (for samples
// produced on the gateway) or via an RPC to the gateway that owns the
// ID.
//
// The cache is shaped after pkg/sql/contention/txnidcache: writes are
// absorbed by 16 sharded, lock-free buffers (concurrentWriteBuffer);
// each full buffer drops a block into a buffered channel; a background
// goroutine drains that channel into a FIFO-evicted map (fifoCache).
// Reads hit the map directly.
//
//	+-----------+        +-----------+
//	| executor  | -----> |  writer   |
//	|           |        |  (16x     |
//	|           |        |  shards)  |
//	+-----------+        +-----+-----+
//	                           |
//	                           v
//	                       blockCh
//	                           |
//	                           v
//	                     +-----+-----+
//	                     | fifoCache |  <----- GetExecution
//	                     +-----------+
package enrichment

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// blockSink receives full blocks from the write buffers and is
// responsible for getting them to the FIFO store. The cache implements
// it via a buffered channel drained by a background goroutine.
type blockSink interface {
	push(*block)
}

// channelSize bounds the number of pending blocks between the writers
// and the ingest goroutine. Keeping this small means a slow ingest
// goroutine will eventually back-pressure writers, but in steady state
// the channel is rarely more than a block or two deep.
const channelSize = 128

// Cache stores per-execution Attributes keyed by clusterunique.ID. It
// is safe for concurrent use. The intended lifecycle is one Cache per
// SQL server (tenant); the cache is started once at server bring-up
// and stopped via the parent stopper.
type Cache struct {
	st *cluster.Settings

	blockCh chan *block
	closeCh chan struct{}

	store   *fifoCache
	writer  *writer
	metrics *Metrics
}

var _ blockSink = (*Cache)(nil)

// NewCache returns a Cache wired to the supplied cluster settings and
// metrics. The returned Cache is inert until Start is called.
func NewCache(st *cluster.Settings, metrics *Metrics) *Cache {
	c := &Cache{
		st:      st,
		metrics: metrics,
		blockCh: make(chan *block, channelSize),
		closeCh: make(chan struct{}),
	}
	c.store = newFIFOCache(func() int64 { return CacheLimit.Get(&st.SV) })
	c.writer = newWriter(st, c)
	return c
}

// Start launches the background goroutine that moves blocks from the
// write buffers into the FIFO store. It is safe to call Start more than
// once; subsequent calls are no-ops. The goroutine exits when the
// supplied stopper quiesces.
func (c *Cache) Start(ctx context.Context, stopper *stop.Stopper) {
	err := stopper.RunAsyncTask(ctx, "ash-enrichment-ingest", func(ctx context.Context) {
		for {
			select {
			case b := <-c.blockCh:
				c.store.add(b)
			case <-stopper.ShouldQuiesce():
				close(c.closeCh)
				return
			}
		}
	})
	if err != nil {
		close(c.closeCh)
	}
}

// Enabled reports whether the enrichment subsystem is currently
// activated by cluster setting. Callers on the gateway hot path should
// check this before constructing the Attributes literal so the argument
// evaluation is skipped when the feature is off — even a literal whose
// fields are all cheap to evaluate still costs a function call and
// (when escape analysis can't prove otherwise) a stack frame.
// PutExecution and GetExecution also short-circuit on this, so Enabled
// is an optimization for callers, not a correctness requirement.
func (c *Cache) Enabled() bool {
	return Enabled.Get(&c.st.SV) && CacheLimit.Get(&c.st.SV) > 0
}

// PutExecution records the attributes for an execution. It is a no-op
// when the enrichment subsystem is disabled by cluster setting. Writes
// become visible to GetExecution asynchronously after the writer's
// in-flight block is flushed, either by filling up or by an explicit
// DrainWriteBuffer.
func (c *Cache) PutExecution(id clusterunique.ID, attrs Attributes) {
	if !c.Enabled() {
		return
	}
	c.metrics.CachePutCount.Inc(1)
	c.writer.record(id, attrs)
}

// GetExecution returns the attributes recorded for id, or false if id
// is not in the cache. It is a no-op (returning false) when the
// enrichment subsystem is disabled.
func (c *Cache) GetExecution(id clusterunique.ID) (Attributes, bool) {
	if !c.Enabled() {
		return Attributes{}, false
	}
	c.metrics.CacheGetCount.Inc(1)
	a, ok := c.store.get(id)
	if !ok {
		c.metrics.CacheGetMissCount.Inc(1)
		return Attributes{}, false
	}
	return a, true
}

// DrainWriteBuffer flushes any in-flight blocks in the writer's sharded
// buffers to the FIFO store. The flush is synchronous with respect to
// the writer but the store ingestion remains asynchronous, so callers
// must still poll GetExecution until the value appears.
func (c *Cache) DrainWriteBuffer() {
	c.writer.drain()
}

// Size returns the current number of entries in the FIFO store. This
// excludes writes that are still buffered in the writer shards.
func (c *Cache) Size() int64 {
	return c.store.size()
}

// push implements blockSink. It is called by the write buffers when a
// block fills up. If the cache has been shut down the block is silently
// dropped — the alternative would be to block indefinitely on a closed
// pipeline.
func (c *Cache) push(b *block) {
	select {
	case c.blockCh <- b:
	case <-c.closeCh:
	}
}
