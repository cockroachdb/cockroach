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
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
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

		data map[uuid.UUID]roachpb.TransactionFingerprintID

		eviction blockList
	}
}

type blockListNode struct {
	block
	next *blockListNode
}

// blockList is a singly-linked list of blocks. The list is used to
// implement FIFO eviction.
type blockList struct {
	head *blockListNode
	tail *blockListNode

	// tailIdx is an index pointing into the next empty slot in block
	// stored in the tail pointer.
	tailIdx int
}

func newFIFOCache(capacity contentionutils.CapacityLimiter) *fifoCache {
	c := &fifoCache{
		capacity: capacity,
	}

	c.mu.data = make(map[uuid.UUID]roachpb.TransactionFingerprintID)
	c.mu.eviction = blockList{}
	return c
}

// add insert a block into the fifoCache.
// N.B. After Add() returns, the input block should not be reused.
func (c *fifoCache) add(b *block) {
	c.mu.Lock()
	defer c.mu.Unlock()

	blockSize := 0
	for i := range b {
		if !b[i].Valid() {
			break
		}

		c.mu.data[b[i].TxnID] = b[i].TxnFingerprintID
		blockSize++
	}

	c.mu.eviction.append(b[:blockSize])

	// Zeros out the block and put it back into the blockPool.
	*b = block{}
	blockPool.Put(b)

	c.maybeEvictLocked()
}

func (c *fifoCache) get(txnID uuid.UUID) (roachpb.TransactionFingerprintID, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	fingerprintID, found := c.mu.data[txnID]
	return fingerprintID, found
}

func (c *fifoCache) size() int {
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
		*node = blockListNode{}
		nodePool.Put(node)
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
}

func (e *blockList) append(block []contentionpb.ResolvedTxnID) {
	block = e.appendToTail(block)
	for len(block) > 0 {
		e.addNode()
		block = e.appendToTail(block)
	}
}

func (e *blockList) addNode() {
	newNode := nodePool.Get().(*blockListNode)
	if e.head == nil {
		e.head = newNode
	} else {
		e.tail.next = newNode
	}
	e.tail = newNode
	e.tailIdx = 0
}

func (e *blockList) appendToTail(
	block []contentionpb.ResolvedTxnID,
) (remaining []contentionpb.ResolvedTxnID) {
	if e.head == nil {
		return block
	}
	toCopy := blockSize - e.tailIdx
	if toCopy > len(block) {
		toCopy = len(block)
	}
	copy(e.tail.block[e.tailIdx:], block[:toCopy])
	e.tailIdx += toCopy
	return block[toCopy:]
}

func (e *blockList) removeFront() *blockListNode {
	if e.head == nil {
		return nil
	}

	removedBlock := e.head
	e.head = e.head.next
	return removedBlock
}
