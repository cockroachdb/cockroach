// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/contention/contentionutils"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// enrichmentBlockSize is the number of entries per write buffer block.
// Each EnrichmentEntry is ~96 bytes, so 32 entries ≈ 3KB per block.
const enrichmentBlockSize = 32

const enrichmentShardCount = 16

const enrichmentChannelSize = 128

// globalEnrichmentCounter generates unique enrichment IDs via
// atomic increment. ID 0 is the "no enrichment" sentinel.
var globalEnrichmentCounter atomic.Uint64

// NextEnrichmentID returns a new globally unique enrichment ID.
func NextEnrichmentID() uint64 {
	return globalEnrichmentCounter.Add(1)
}

// EnrichmentData holds the attributes resolved from an enrichment ID.
type EnrichmentData struct {
	AppName   string
	User      string
	Database  string
	SessionID clusterunique.ID
	TxnID     uuid.UUID
	PlanHash  uint64
}

// EnrichmentEntry pairs an enrichment ID with its data for cache writes.
type EnrichmentEntry struct {
	EnrichmentID uint64
	EnrichmentData
}

func (e *EnrichmentEntry) valid() bool {
	return e.EnrichmentID != 0
}

// enrichmentBlock is a fixed-size array of entries used as the unit of
// write batching and FIFO eviction.
type enrichmentBlock [enrichmentBlockSize]EnrichmentEntry

var enrichmentBlockPool = &sync.Pool{
	New: func() any { return &enrichmentBlock{} },
}

// --- FIFO cache ---

type enrichmentBlockListNode struct {
	block *enrichmentBlock
	next  *enrichmentBlockListNode
}

var enrichmentNodePool = &sync.Pool{
	New: func() any { return &enrichmentBlockListNode{} },
}

type enrichmentFIFOCache struct {
	maxEntries int

	mu struct {
		syncutil.RWMutex
		data     map[uint64]EnrichmentData
		numNodes int
		head     *enrichmentBlockListNode
		tail     *enrichmentBlockListNode
	}
}

func newEnrichmentFIFOCache(maxEntries int) *enrichmentFIFOCache {
	c := &enrichmentFIFOCache{maxEntries: maxEntries}
	c.mu.data = make(map[uint64]EnrichmentData)
	return c
}

func (c *enrichmentFIFOCache) add(b *enrichmentBlock) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range b {
		if !b[i].valid() {
			break
		}
		c.mu.data[b[i].EnrichmentID] = b[i].EnrichmentData
	}
	node := enrichmentNodePool.Get().(*enrichmentBlockListNode)
	node.block = b
	if c.mu.head == nil {
		c.mu.head = node
	} else {
		c.mu.tail.next = node
	}
	c.mu.tail = node
	c.mu.numNodes++
	c.maybeEvictLocked()
}

func (c *enrichmentFIFOCache) get(id uint64) (EnrichmentData, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	data, ok := c.mu.data[id]
	return data, ok
}

func (c *enrichmentFIFOCache) size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.mu.data)
}

func (c *enrichmentFIFOCache) maybeEvictLocked() {
	for len(c.mu.data) > c.maxEntries {
		if c.mu.head == nil {
			return
		}
		c.mu.numNodes--
		node := c.mu.head
		c.mu.head = node.next
		if c.mu.head == nil {
			c.mu.tail = nil
		}
		for i := 0; i < enrichmentBlockSize; i++ {
			if !node.block[i].valid() {
				break
			}
			delete(c.mu.data, node.block[i].EnrichmentID)
		}
		*node.block = enrichmentBlock{}
		enrichmentBlockPool.Put(node.block)
		*node = enrichmentBlockListNode{}
		enrichmentNodePool.Put(node)
	}
}

// --- Concurrent write buffer (per shard) ---

type enrichmentBlockSink interface {
	pushBlock(*enrichmentBlock)
}

type enrichmentWriteBuffer struct {
	guard struct {
		*contentionutils.ConcurrentBufferGuard
		block *enrichmentBlock
	}
	sink enrichmentBlockSink
}

func newEnrichmentWriteBuffer(sink enrichmentBlockSink) *enrichmentWriteBuffer {
	wb := &enrichmentWriteBuffer{sink: sink}
	wb.guard.block = enrichmentBlockPool.Get().(*enrichmentBlock)
	wb.guard.ConcurrentBufferGuard = contentionutils.NewConcurrentBufferGuard(
		func() int64 { return enrichmentBlockSize },
		func(_ int64) {
			wb.sink.pushBlock(wb.guard.block)
			wb.guard.block = enrichmentBlockPool.Get().(*enrichmentBlock)
		},
	)
	return wb
}

func (wb *enrichmentWriteBuffer) record(entry EnrichmentEntry) {
	wb.guard.AtomicWrite(func(idx int64) {
		wb.guard.block[idx] = entry
	})
}

func (wb *enrichmentWriteBuffer) drain() {
	wb.guard.ForceSync()
}

// --- Sharded writer ---

type enrichmentWriter struct {
	shards [enrichmentShardCount]*enrichmentWriteBuffer
}

func newEnrichmentWriter(sink enrichmentBlockSink) *enrichmentWriter {
	w := &enrichmentWriter{}
	for i := range w.shards {
		w.shards[i] = newEnrichmentWriteBuffer(sink)
	}
	return w
}

func (w *enrichmentWriter) record(entry EnrichmentEntry) {
	shard := entry.EnrichmentID % enrichmentShardCount
	w.shards[shard].record(entry)
}

func (w *enrichmentWriter) drain() {
	for i := range w.shards {
		w.shards[i].drain()
	}
}

// --- EnrichmentCache ---

// EnrichmentCache maps enrichment IDs to enrichment data. It is designed
// for high write throughput using sharded concurrent write buffers that
// flush through a channel to a FIFO-evicting backing store.
//
// Architecture (same as txnidcache.Cache):
//
//	connExecutor ---Record---> sharded write buffers
//	                              |
//	                              v  (block flush via channel)
//	                           FIFO cache (map + block linked list)
//	                              ^
//	                              |  (Lookup)
//	                           ASH Sampler
type EnrichmentCache struct {
	blockCh chan *enrichmentBlock
	closeCh chan struct{}
	store   *enrichmentFIFOCache
	writer  *enrichmentWriter
}

var _ enrichmentBlockSink = (*EnrichmentCache)(nil)

// globalEnrichmentCache is the process-wide enrichment cache singleton.
var globalEnrichmentCache atomic.Pointer[EnrichmentCache]
var initEnrichmentCacheOnce sync.Once

// NewEnrichmentCache creates a new EnrichmentCache with the given
// maximum entry count.
func NewEnrichmentCache(maxEntries int) *EnrichmentCache {
	c := &EnrichmentCache{
		blockCh: make(chan *enrichmentBlock, enrichmentChannelSize),
		closeCh: make(chan struct{}),
	}
	c.store = newEnrichmentFIFOCache(maxEntries)
	c.writer = newEnrichmentWriter(c)
	return c
}

// Start launches the background goroutine that ingests blocks from
// the write shards into the FIFO cache.
func (c *EnrichmentCache) Start(ctx context.Context, stopper *stop.Stopper) error {
	err := stopper.RunAsyncTask(ctx, "ash-enrichment-cache", func(ctx context.Context) {
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
	return err
}

// Record writes an enrichment entry into a sharded write buffer.
// The entry is batched and asynchronously flushed to the backing
// cache. This is the hot-path write method called per statement
// execution.
func (c *EnrichmentCache) Record(entry EnrichmentEntry) {
	if entry.EnrichmentID == 0 {
		return
	}
	c.writer.record(entry)
}

// StoreImmediate writes entries directly into the FIFO cache under
// the write lock, bypassing the sharded writer. The entries are
// immediately available for Lookup. Use this for small batches
// (e.g., remote resolution results) where immediate visibility is
// required.
func (c *EnrichmentCache) StoreImmediate(entries map[uint64]EnrichmentData) {
	if len(entries) == 0 {
		return
	}
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	for id, data := range entries {
		c.store.mu.data[id] = data
	}
	c.store.maybeEvictLocked()
}

// Lookup returns the enrichment data for the given ID, if present.
func (c *EnrichmentCache) Lookup(id uint64) (EnrichmentData, bool) {
	return c.store.get(id)
}

// Size returns the number of entries in the cache.
func (c *EnrichmentCache) Size() int {
	return c.store.size()
}

// DrainWriteBuffer flushes all pending entries from the write shards
// through the channel into the backing cache. Intended for tests.
func (c *EnrichmentCache) DrainWriteBuffer() {
	c.writer.drain()
}

// pushBlock implements enrichmentBlockSink.
func (c *EnrichmentCache) pushBlock(b *enrichmentBlock) {
	select {
	case c.blockCh <- b:
	case <-c.closeCh:
	}
}

// InitGlobalEnrichmentCache creates and starts the process-wide
// enrichment cache. Idempotent: only the first call creates the cache.
func InitGlobalEnrichmentCache(ctx context.Context, maxEntries int, stopper *stop.Stopper) error {
	var initErr error
	initEnrichmentCacheOnce.Do(func() {
		c := NewEnrichmentCache(maxEntries)
		initErr = c.Start(ctx, stopper)
		if initErr == nil {
			globalEnrichmentCache.Store(c)
		}
	})
	return initErr
}

// GlobalEnrichmentCache returns the process-wide enrichment cache,
// or nil if not initialized.
func GlobalEnrichmentCache() *EnrichmentCache {
	return globalEnrichmentCache.Load()
}

// ResetGlobalEnrichmentCacheForTesting resets the global enrichment
// cache so subsequent tests can re-initialize it.
func ResetGlobalEnrichmentCacheForTesting() {
	globalEnrichmentCache.Store(nil)
	initEnrichmentCacheOnce = sync.Once{}
}
