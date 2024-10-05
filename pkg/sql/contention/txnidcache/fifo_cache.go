// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnidcache

import (
	"sync"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/contention/contentionutils"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

var nodePool = &sync.Pool{
	New: func() interface{} {
		return &blockListNode{}
	},
}

type fifoCache struct {
	capacity contentionutils.CapacityLimiter

	mu struct {
		syncutil.RWMutex

		data map[uuid.UUID]appstatspb.TransactionFingerprintID

		eviction blockList
	}
}

type blockListNode struct {
	*block
	next *blockListNode
}

// blockList is a singly-linked list of blocks. The list is used to
// implement FIFO eviction.
type blockList struct {
	numNodes int
	head     *blockListNode
	tail     *blockListNode
}

// newFifoCache takes a function which returns a capacity in bytes.
func newFIFOCache(capacity contentionutils.CapacityLimiter) *fifoCache {
	c := &fifoCache{
		capacity: capacity,
	}

	c.mu.data = make(map[uuid.UUID]appstatspb.TransactionFingerprintID)
	c.mu.eviction = blockList{}
	return c
}

// add insert a block into the fifoCache.
// N.B. After Add() returns, the input block should not be reused.
func (c *fifoCache) add(b *block) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range b {
		if !b[i].Valid() {
			break
		}

		c.mu.data[b[i].TxnID] = b[i].TxnFingerprintID
	}
	c.mu.eviction.addNode(b)
	c.maybeEvictLocked()
}

func (c *fifoCache) get(txnID uuid.UUID) (appstatspb.TransactionFingerprintID, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	fingerprintID, found := c.mu.data[txnID]
	return fingerprintID, found
}

func (c *fifoCache) size() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.sizeLocked()
}

func (c *fifoCache) sizeLocked() int64 {
	return int64(c.mu.eviction.numNodes)*
		((entrySize*blockSize)+int64(unsafe.Sizeof(blockListNode{}))) +
		int64(len(c.mu.data))*entrySize
}

func (c *fifoCache) maybeEvictLocked() {
	for c.sizeLocked() > c.capacity() {
		node := c.mu.eviction.removeFront()
		if node == nil {
			return
		}
		c.evictNodeLocked(node)
	}
}

// evictNodeLocked deletes all entries in the block from the internal map.
func (c *fifoCache) evictNodeLocked(node *blockListNode) {
	for i := 0; i < blockSize; i++ {
		if !node.block[i].Valid() {
			break
		}

		delete(c.mu.data, node.block[i].TxnID)
	}

	*node.block = block{}
	blockPool.Put(node.block)
	*node = blockListNode{}
	nodePool.Put(node)
}

func (e *blockList) addNode(b *block) {
	newNode := nodePool.Get().(*blockListNode)
	newNode.block = b
	if e.head == nil {
		e.head = newNode
	} else {
		e.tail.next = newNode
	}
	e.tail = newNode
	e.numNodes++
}

func (e *blockList) removeFront() *blockListNode {
	if e.head == nil {
		return nil
	}

	e.numNodes--
	removedBlock := e.head
	e.head = e.head.next
	if e.head == nil {
		e.tail = nil
	}
	return removedBlock
}
