// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package enrichment

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/contention/contentionutils"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// fifoCache is the backing store for the enrichment cache. It holds
// entries in a map for O(1) lookup and chains the blocks the entries
// arrived in on a singly-linked list for FIFO eviction. When the
// caller-supplied capacity is exceeded, blocks are removed from the
// head of the list and their entries deleted from the map.
//
// Reads (get) take the read lock; ingest (add) takes the write lock.
// Both paths are short, so contention is unlikely in practice — most
// pressure is absorbed upstream by the sharded writer.
type fifoCache struct {
	capacity contentionutils.CapacityLimiter

	mu struct {
		syncutil.RWMutex
		data     map[clusterunique.ID]Attributes
		eviction blockList
	}
}

// blockListNode is a list node holding a reference to the block whose
// entries were inserted together. Nodes are pooled.
type blockListNode struct {
	*block
	next *blockListNode
}

var nodePool = &sync.Pool{
	New: func() interface{} { return &blockListNode{} },
}

// blockList is a singly-linked list of blockListNodes used to implement
// FIFO eviction at block granularity (rather than per-entry, which
// would force the eviction path to maintain a separate linked list).
type blockList struct {
	head *blockListNode
	tail *blockListNode
}

func newFIFOCache(capacity contentionutils.CapacityLimiter) *fifoCache {
	c := &fifoCache{capacity: capacity}
	c.mu.data = make(map[clusterunique.ID]Attributes)
	return c
}

// add inserts every valid entry in b into the cache and threads b onto
// the eviction list. After add returns the caller must not reuse b: the
// fifoCache owns it and will return it to the pool when the block is
// evicted.
func (c *fifoCache) add(b *block) {
	c.mu.Lock()
	defer c.mu.Unlock()

	added := 0
	for i := range b {
		if !b[i].valid() {
			break
		}
		c.mu.data[b[i].Key] = b[i].Attrs
		added++
	}
	if added == 0 {
		// Empty block — nothing to track, recycle immediately.
		*b = block{}
		blockPool.Put(b)
		return
	}
	c.mu.eviction.addNode(b)
	c.maybeEvictLocked()
}

// get returns the attributes recorded for id, or false if id is not in
// the cache.
func (c *fifoCache) get(id clusterunique.ID) (Attributes, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	a, ok := c.mu.data[id]
	return a, ok
}

// size returns the current number of live entries in the cache.
func (c *fifoCache) size() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return int64(len(c.mu.data))
}

func (c *fifoCache) maybeEvictLocked() {
	for int64(len(c.mu.data)) > c.capacity() {
		node := c.mu.eviction.removeFront()
		if node == nil {
			return
		}
		c.evictNodeLocked(node)
	}
}

func (c *fifoCache) evictNodeLocked(node *blockListNode) {
	for i := range node.block {
		if !node.block[i].valid() {
			break
		}
		delete(c.mu.data, node.block[i].Key)
	}
	*node.block = block{}
	blockPool.Put(node.block)
	*node = blockListNode{}
	nodePool.Put(node)
}

func (e *blockList) addNode(b *block) {
	n := nodePool.Get().(*blockListNode)
	n.block = b
	if e.head == nil {
		e.head = n
	} else {
		e.tail.next = n
	}
	e.tail = n
}

func (e *blockList) removeFront() *blockListNode {
	if e.head == nil {
		return nil
	}
	front := e.head
	e.head = front.next
	if e.head == nil {
		e.tail = nil
	}
	return front
}
