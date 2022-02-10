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

type fifoCache struct {
	pool     *sync.Pool
	capacity contentionutils.CapacityLimiter

	mu struct {
		syncutil.RWMutex

		data map[uuid.UUID]roachpb.TransactionFingerprintID

		eviction evictionList
	}
}

type evictionNode struct {
	block *messageBlock
	next  *evictionNode
}

// evictionList is a singly-linked list of messageBlocks. The list is used to
// implement FIFO eviction.
type evictionList struct {
	head *evictionNode
	tail *evictionNode

	// tailBlockIdx is an index pointing into the next empty slot in messageBlock
	// stored in the tail pointer.
	tailBlockIdx int

	scratchZeroBlock messageBlock
}

func newFIFOCache(pool *sync.Pool, capacity contentionutils.CapacityLimiter) *fifoCache {
	c := &fifoCache{
		pool:     pool,
		capacity: capacity,
	}

	c.mu.data = make(map[uuid.UUID]roachpb.TransactionFingerprintID)
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

	if blockHijacked := c.mu.eviction.insertBlock(block, blockSize); !blockHijacked {
		*block = messageBlock{}
		c.pool.Put(block)
	}
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
		block := c.mu.eviction.removeFront()
		if block == nil {
			return
		}

		c.evictBlockLocked(block)

		// Zero out the block and put it back into the pool.
		*block = messageBlock{}
		c.pool.Put(block)
	}
}

// evictBlockLocked deletes all entries in the block from the internal map.
func (c *fifoCache) evictBlockLocked(block *messageBlock) {
	for i := 0; i < messageBlockSize; i++ {
		if !block[i].valid() {
			break
		}

		delete(c.mu.data, block[i].TxnID)
	}
}

// insertBlock insert the messageBlock into the eviction list. If the eviction
// list does not have enough capacity to store all contents in the input block,
// then the new block is hijacked, and it is used as the new block in the
// eviction list.
func (e *evictionList) insertBlock(block *messageBlock, blockSize int) (blockHijacked bool) {
	// Adding the new block when the list is empty.
	if e.head == nil && e.tail == nil {
		e.head = &evictionNode{
			block: block,
			next:  nil,
		}
		e.tail = e.head
		e.tailBlockIdx = blockSize
		return true /* blockHijacked */
	}

	// Handle when the tail block is full.
	if e.tail.block.isFull() {
		e.appendBlockToNonEmptyList(block, blockSize)
		return true /* blockHijacked */
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

	copy(e.tail.block[e.tailBlockIdx:], block[:entriesToCopy])
	e.tailBlockIdx += entriesToCopy

	// If the new block is not fully copied.
	if remainingBlockSize := blockSize - entriesToCopy; remainingBlockSize > 0 {
		// Currently, the new block looks something like this:
		//
		//         remaining
		//         v~~~~~~~~~
		// [ 1  2  3  4  5  6  _  _  _   _ ]
		//   ^~~~              ^~~~~~~~~~~
		//   entries copied    empty
		//
		//
		// Step 1, copy the remaining section forward:
		//
		//               this is remaining.
		//               v~~~
		// [ 3  4  5  6  5  6  _  _  _   _ ]
		//   ^~~~~~~~~~
		//   this portion gets overwrite
		//
		// Step 2, zero out the remaining section:
		//
		// [ 3  4  5  6  _  _  _  _  _   _ ]
		//               ^~~~
		//               zero'ed out
		//
		// Step 3, append the block to the linked list.

		// Step 1.
		copy(block[:remainingBlockSize], block[entriesToCopy:entriesToCopy+remainingBlockSize])

		// Step 2.
		copy(block[remainingBlockSize:], e.scratchZeroBlock[:remainingBlockSize])

		// Step 3.
		e.appendBlockToNonEmptyList(block, remainingBlockSize)
	} else {
		// If the eviction list had enough capacity to store all the input block,
		// then the input block is not hijacked. Caller is responsible for freeing
		// the block.
		return false /* blockHijacked */
	}

	return true /* blockHijacked */
}

func (e *evictionList) appendBlockToNonEmptyList(block *messageBlock, blockSize int) {
	newTail := &evictionNode{
		block: block,
		next:  nil,
	}
	e.tail.next = newTail
	e.tail = newTail
	e.tailBlockIdx = blockSize
}

func (e *evictionList) removeFront() *messageBlock {
	if e.head == nil {
		return nil
	}

	removedBlock := e.head
	e.head = e.head.next
	return removedBlock.block
}
