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

type evictionList struct {
	head *evictionNode
	tail *evictionNode
}

func newFIFOCache(pool *sync.Pool, capacity contentionutils.CapacityLimiter) *fifoCache {
	c := &fifoCache{
		pool:     pool,
		capacity: capacity,
	}

	c.mu.data = make(map[uuid.UUID]roachpb.TransactionFingerprintID)

	return c
}

func (c *fifoCache) add(block *messageBlock) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range block {
		if !block[i].valid() {
			break
		}

		c.mu.data[block[i].TxnID] = block[i].TxnFingerprintID
	}

	c.mu.eviction.insertBlock(block)
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

func (c *fifoCache) evictBlockLocked(block *messageBlock) {
	for i := 0; i < messageBlockSize; i++ {
		if !block[i].valid() {
			break
		}

		delete(c.mu.data, block[i].TxnID)
	}
}

func (e *evictionList) insertBlock(block *messageBlock) {
	if e.head == nil && e.tail == nil {
		e.head = &evictionNode{
			block: block,
			next:  nil,
		}
		e.tail = e.head
		return
	}

	newTail := &evictionNode{
		block: block,
		next:  nil,
	}
	e.tail.next = newTail
	e.tail = newTail
}

func (e *evictionList) removeFront() *messageBlock {
	if e.head == nil {
		return nil
	}

	removedBlock := e.head
	e.head = e.head.next
	return removedBlock.block
}
