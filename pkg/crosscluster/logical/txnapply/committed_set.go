// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnapply

import (
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// committedSet tracks which transactions have been committed by an applier.
// It combines a resolvedTime watermark with a set of individually completed
// transactions whose timestamps are above the watermark. A transaction is
// considered resolved if its timestamp is at or below the resolvedTime, or
// if it appears in completedTxns. When the resolvedTime advances,
// completedTxns entries that fall at or below the new watermark are pruned.
type committedSet struct {
	resolvedTime  hlc.Timestamp
	completedTxns map[ldrdecoder.TxnID]struct{}
}

// makeCommittedSet returns an initialized committedSet.
func makeCommittedSet() committedSet {
	return committedSet{
		completedTxns: make(map[ldrdecoder.TxnID]struct{}),
	}
}

// IsResolved returns true if the given txn is resolved — either its
// timestamp is at or below the resolvedTime, or it is in completedTxns.
func (c *committedSet) IsResolved(txn ldrdecoder.TxnID) bool {
	if txn.Timestamp.LessEq(c.resolvedTime) {
		return true
	}
	_, ok := c.completedTxns[txn]
	return ok
}

// IsResolvedAt returns true if the given timestamp is at or below the
// resolvedTime.
func (c *committedSet) IsResolvedAt(ts hlc.Timestamp) bool {
	return ts.LessEq(c.resolvedTime)
}

// ResolvedTime returns the current resolvedTime watermark.
func (c *committedSet) ResolvedTime() hlc.Timestamp {
	return c.resolvedTime
}

// Resolve records a completed txn. If the txn's timestamp is above the
// current resolvedTime, it is added to completedTxns for individual
// tracking.
func (c *committedSet) Resolve(txn ldrdecoder.TxnID) {
	if !txn.Timestamp.LessEq(c.resolvedTime) {
		c.completedTxns[txn] = struct{}{}
	}
}

// UpdateResolvedTime advances the resolvedTime watermark. The watermark
// only moves forward; if ts is not after the current resolvedTime, this
// is a no-op. Transactions whose timestamps fall at or below the new
// resolvedTime are pruned from completedTxns.
func (c *committedSet) UpdateResolvedTime(ts hlc.Timestamp) {
	if !c.resolvedTime.Less(ts) {
		return
	}
	c.resolvedTime = ts
	for completed := range c.completedTxns {
		if completed.Timestamp.LessEq(ts) {
			delete(c.completedTxns, completed)
		}
	}
}
