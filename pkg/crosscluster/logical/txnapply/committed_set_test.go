// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnapply

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

func txnID(wall int64, applier int32) ldrdecoder.TxnID {
	return ldrdecoder.TxnID{
		Timestamp: hlc.Timestamp{WallTime: wall},
		ApplierID: ldrdecoder.ApplierID(applier),
	}
}

func TestCommittedSet(t *testing.T) {
	t.Run("initial state", func(t *testing.T) {
		cs := makeCommittedSet()
		require.True(t, cs.ResolvedTime().IsEmpty())
		require.False(t, cs.IsResolved(txnID(1, 0)))
		require.False(t, cs.IsResolvedAt(hlc.Timestamp{WallTime: 1}))
	})

	t.Run("resolve individual txn", func(t *testing.T) {
		cs := makeCommittedSet()
		txn := txnID(10, 1)
		require.False(t, cs.IsResolved(txn))

		cs.Resolve(txn)
		require.True(t, cs.IsResolved(txn))

		// Different txn at same timestamp but different applier is not resolved.
		require.False(t, cs.IsResolved(txnID(10, 2)))
	})

	t.Run("update resolved time", func(t *testing.T) {
		cs := makeCommittedSet()
		cs.UpdateResolvedTime(hlc.Timestamp{WallTime: 5})
		require.Equal(t, hlc.Timestamp{WallTime: 5}, cs.ResolvedTime())

		// Txn at or below watermark is resolved without explicit Resolve.
		require.True(t, cs.IsResolved(txnID(5, 0)))
		require.True(t, cs.IsResolved(txnID(3, 0)))
		require.True(t, cs.IsResolvedAt(hlc.Timestamp{WallTime: 5}))

		// Txn above watermark is not resolved.
		require.False(t, cs.IsResolved(txnID(6, 0)))
		require.False(t, cs.IsResolvedAt(hlc.Timestamp{WallTime: 6}))
	})

	t.Run("backward resolved time is no-op", func(t *testing.T) {
		cs := makeCommittedSet()
		cs.UpdateResolvedTime(hlc.Timestamp{WallTime: 10})
		cs.UpdateResolvedTime(hlc.Timestamp{WallTime: 5})
		require.Equal(t, hlc.Timestamp{WallTime: 10}, cs.ResolvedTime())
	})

	t.Run("resolve below watermark is no-op", func(t *testing.T) {
		cs := makeCommittedSet()
		cs.UpdateResolvedTime(hlc.Timestamp{WallTime: 10})

		// Resolve a txn at the watermark — should not be added to map.
		txn := txnID(10, 1)
		cs.Resolve(txn)
		require.Len(t, cs.completedTxns, 0)

		// It's still resolved because it's at the watermark.
		require.True(t, cs.IsResolved(txn))
	})

	t.Run("update resolved time prunes completed txns", func(t *testing.T) {
		cs := makeCommittedSet()
		cs.Resolve(txnID(5, 0))
		cs.Resolve(txnID(10, 0))
		cs.Resolve(txnID(15, 0))
		require.Len(t, cs.completedTxns, 3)

		// Advance watermark past the first two txns.
		cs.UpdateResolvedTime(hlc.Timestamp{WallTime: 10})
		require.Len(t, cs.completedTxns, 1)

		// The pruned txns are still resolved (via watermark).
		require.True(t, cs.IsResolved(txnID(5, 0)))
		require.True(t, cs.IsResolved(txnID(10, 0)))

		// The remaining txn is still resolved via completedTxns.
		require.True(t, cs.IsResolved(txnID(15, 0)))
	})
}
