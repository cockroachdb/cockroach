// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/contention/contentionutils"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const stripInitialCapacity = 128

type strip struct {
	syncutil.RWMutex
	data     map[uuid.UUID]roachpb.TransactionFingerprintID
	capacity contentionutils.CapacityLimiter
}

var _ reader = &strip{}
var _ resolver = &strip{}

func newStrip(capacity contentionutils.CapacityLimiter) *strip {
	c := &strip{
		data:     make(map[uuid.UUID]roachpb.TransactionFingerprintID, stripInitialCapacity),
		capacity: capacity,
	}
	return c
}

// tryInsertBlock takes two arguments:
// * block: the messageBlock that will be inserted into the strip.
// * blockStartingOffset: the offset into the block the strip will start reading
//                        at.
// The strip will starting reading the block from blockStartingOffset until
// either:
// * the strip is full: in this case, if the block is not fully consumed,
//   tryInsertBlock returns the next unread blockIdx into the messageBlock
//   and the boolean variable indicating that the block is not fully consumed.
// * the block is fully consumed: this can happen in two scenarios:
//   1. the messageBlock is fully populated, and we consumed the entirety
//      of the block.
//   2. the messageBlock is partially populated, and we have observed an invalid
//      ResolvedTxnID.
//   The returning blockIdx is insufficient for the caller to decide of the
//   block has been fully consumed, hence the caller relies on the second
//   returned boolean variable.
func (c *strip) tryInsertBlock(
	block messageBlock, blockStartingOffset int,
) (endingOffset int, more bool) {
	capn := c.capacity()
	c.Lock()
	defer c.Unlock()

	blockIdx := blockStartingOffset
	for ; blockIdx < messageBlockSize && block[blockIdx].Valid() && int64(len(c.data)) < capn; blockIdx++ {
		c.data[block[blockIdx].TxnID] = block[blockIdx].TxnFingerprintID
	}
	return blockIdx, blockIdx < messageBlockSize && block[blockIdx].Valid()
}

// Lookup implements the reader interface.
func (c *strip) Lookup(txnID uuid.UUID) (roachpb.TransactionFingerprintID, bool) {
	c.RLock()
	defer c.RUnlock()
	txnFingerprintID, found := c.data[txnID]
	return txnFingerprintID, found
}

// Resolve implements the reader interface.
func (c *strip) resolve(
	txnIDs map[uuid.UUID]struct{}, result []contentionpb.ResolvedTxnID,
) []contentionpb.ResolvedTxnID {
	c.RLock()
	defer c.RUnlock()

	for txnID := range txnIDs {
		if txnFingerprintID, ok := c.data[txnID]; ok {
			result = append(result, contentionpb.ResolvedTxnID{
				TxnID:            txnID,
				TxnFingerprintID: txnFingerprintID,
			})
			delete(txnIDs, txnID)
		}
	}

	return result
}

func (c *strip) clear() {
	c.Lock()
	defer c.Unlock()

	// Instead of reallocating a new map, range-loop with delete can trigger
	// golang compiler's optimization to use `memclr`.
	// See: https://github.com/golang/go/issues/20138.
	for key := range c.data {
		delete(c.data, key)
	}
}
