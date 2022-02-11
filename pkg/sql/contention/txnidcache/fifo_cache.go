// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package txnidcache

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/contention/contentionutils"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

var nodePool = &sync.Pool{
	New: func() interface{} {
		return &evictionNode{}
	},
}

type fifoCache struct {
	capacity contentionutils.CapacityLimiter

	mu struct {
		syncutil.RWMutex

		data map[uuid.UUID]roachpb.TransactionFingerprintID

		eviction evictionList
	}
}

type evictionNode struct {
	messageBlock
	next *evictionNode
}

// evictionList is a singly-linked list of messageBlocks. The list is used to
// implement FIFO eviction.
type evictionList struct {
	head *evictionNode
	tail *evictionNode

	// tailBlockIdx is an index pointing into the next empty slot in messageBlock
	// stored in the tail pointer.
	tailBlockIdx int
}

func newFIFOCache(capacity contentionutils.CapacityLimiter) *fifoCache {
	c := &fifoCache{
		capacity: capacity,
	}

	c.mu.data = make(map[uuid.UUID]roachpb.TransactionFingerprintID)
	c.mu.eviction = evictionList{}
	return c
}

// Add insert a messageBlock into the fifoCache.
//
// N.B. After Add() returns, the input messageBlock should not be reused. The
// fifoCache sometimes directly hijack the input messageBlock to form the
// eviction list and often even directly overwrite its content to minimize
// memory allocation.
func (c *fifoCache) Add(block *messageBlock) {
	c.mu.Lock()
	defer c.mu.Unlock()

	blockSize := 0
	for i := range block {
		if !block[i].valid() {
			break
		}

		c.mu.data[block[i].TxnID] = block[i].TxnFingerprintID
		blockSize++
	}

	c.mu.eviction.insertBlock(block, blockSize)

	// Zeros out the block and put it back into the blockPool.
	*block = messageBlock{}
	blockPool.Put(block)

	c.maybeEvictLocked()
}

func (c *fifoCache) Get(txnID uuid.UUID) (roachpb.TransactionFingerprintID, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	fingerprintID, found := c.mu.data[txnID]
	return fingerprintID, found
}

func (c *fifoCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.mu.data)
}

func (c *fifoCache) maybeEvictLocked() {
	for int64(len(c.mu.data)) > c.capacity() {
		node := c.mu.eviction.removeFront()
		if node == nil {
			return
		}

		c.evictNodeLocked(node)

		// Zero out the node and put it back into the pool.
		*node = evictionNode{}
		nodePool.Put(node)
	}
}

// evictNodeLocked deletes all entries in the block from the internal map.
func (c *fifoCache) evictNodeLocked(node *evictionNode) {
	for i := 0; i < messageBlockSize; i++ {
		if !node.messageBlock[i].valid() {
			break
		}

		delete(c.mu.data, node.messageBlock[i].TxnID)
	}
}

// insertBlock insert the messageBlock into the eviction list.
func (e *evictionList) insertBlock(block *messageBlock, blockSize int) {
	// Adding the new block when the list is empty.
	if e.head == nil && e.tail == nil {
		newNode := nodePool.Get().(*evictionNode)
		copy(newNode.messageBlock[:], block[:blockSize])

		e.head = newNode
		e.tail = e.head
		e.tailBlockIdx = blockSize

		return
	}

	// Handle when the tail block is full.
	if e.tail.isFull() {
		e.appendBlockToNonEmptyList(block, blockSize, 0 /* offset */)
		return
	}

	// Handle when the tail block is partially full.
	remainingTailBlockCapacity := messageBlockSize - e.tailBlockIdx
	if remainingTailBlockCapacity <= 0 {
		// Sanity check.
		panic(errors.AssertionFailedf("bug"))
	}

	entriesToCopy := blockSize
	if remainingTailBlockCapacity < blockSize {
		// The remaining capacity in the tail block is insufficient to store all
		// the new information. In this case, we fill up the tail block first,
		// then append the remaining of the new block to the linked list.
		entriesToCopy = remainingTailBlockCapacity
	}

	copy(e.tail.messageBlock[e.tailBlockIdx:], block[:entriesToCopy])
	e.tailBlockIdx += entriesToCopy

	// If the new block is not fully copied.
	if remainingBlockSize := blockSize - entriesToCopy; remainingBlockSize > 0 {
		e.appendBlockToNonEmptyList(block, remainingBlockSize, entriesToCopy /* offset */)
	}
}

func (e *evictionList) appendBlockToNonEmptyList(block *messageBlock, blockSize, offset int) {
	newTail := nodePool.Get().(*evictionNode)
	copy(newTail.messageBlock[:], block[offset:offset+blockSize])
	e.tail.next = newTail
	e.tail = newTail
	e.tailBlockIdx = blockSize
}

func (e *evictionList) removeFront() *evictionNode {
	if e.head == nil {
		return nil
	}

	removedBlock := e.head
	e.head = e.head.next
	return removedBlock
}
