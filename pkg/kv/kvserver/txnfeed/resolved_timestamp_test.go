// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func ts(walltime int64) hlc.Timestamp {
	return hlc.Timestamp{WallTime: walltime}
}

func TestResolvedTimestampEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	var rts resolvedTimestamp

	// Before Init, Get returns zero.
	require.True(t, rts.Get().IsEmpty())

	// Init with empty queue and no closed TS: resolved is zero.
	require.False(t, rts.Init(ctx))
	require.True(t, rts.Get().IsEmpty())

	// Forward closed TS: resolved advances to closed TS.
	require.True(t, rts.ForwardClosedTS(ctx, ts(10)))
	require.Equal(t, ts(10), rts.Get())
}

func TestResolvedTimestampSingleRecord(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	var rts resolvedTimestamp

	rts.Init(ctx)
	rts.ForwardClosedTS(ctx, ts(5))
	require.Equal(t, ts(5), rts.Get())

	// Add a record at ts(10). The closed TS is 5, which is less than the
	// record, so the resolved TS stays at 5.
	txnID := uuid.MakeV4()
	require.False(t, rts.ConsumeRecordWritten(ctx, txnID, ts(10)))
	require.Equal(t, ts(5), rts.Get())

	// Forward closed TS to 20. The record at ts(10) holds back resolved
	// to ts(10).Prev() = ts(9) with logical truncated to 0.
	require.True(t, rts.ForwardClosedTS(ctx, ts(20)))
	require.Equal(t, ts(9), rts.Get())

	// Commit the transaction. Resolved TS catches up to closed TS.
	require.True(t, rts.ConsumeCommitted(ctx, txnID))
	require.Equal(t, ts(20), rts.Get())
}

func TestResolvedTimestampMultipleRecords(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	var rts resolvedTimestamp

	rts.Init(ctx)

	txnA := uuid.MakeV4()
	txnB := uuid.MakeV4()
	txnC := uuid.MakeV4()

	// Add records before advancing the closed TS past them (matching the
	// real invariant that records are created above the closed TS).
	rts.ConsumeRecordWritten(ctx, txnA, ts(30))
	rts.ConsumeRecordWritten(ctx, txnB, ts(20))
	rts.ConsumeRecordWritten(ctx, txnC, ts(50))
	rts.ForwardClosedTS(ctx, ts(100))

	// Resolved = min(100, 20-1) = 19.
	require.Equal(t, ts(19), rts.Get())
	require.Equal(t, 3, rts.txnQ.Len())

	// Remove the oldest (txnB). Now oldest is txnA at ts(30).
	// Resolved = min(100, 30-1) = 29.
	require.True(t, rts.ConsumeAborted(ctx, txnB))
	require.Equal(t, ts(29), rts.Get())

	// Remove txnA. Now oldest is txnC at ts(50).
	// Resolved = min(100, 50-1) = 49.
	require.True(t, rts.ConsumeCommitted(ctx, txnA))
	require.Equal(t, ts(49), rts.Get())

	// Remove txnC. No unresolved records, resolved = closedTS = 100.
	require.True(t, rts.ConsumeCommitted(ctx, txnC))
	require.Equal(t, ts(100), rts.Get())
}

func TestResolvedTimestampForwardExisting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	var rts resolvedTimestamp

	rts.Init(ctx)

	txnA := uuid.MakeV4()
	txnB := uuid.MakeV4()

	// Add records before advancing closed TS past them.
	rts.ConsumeRecordWritten(ctx, txnA, ts(20))
	rts.ConsumeRecordWritten(ctx, txnB, ts(30))
	rts.ForwardClosedTS(ctx, ts(100))
	require.Equal(t, ts(19), rts.Get())

	// Forward txnA's timestamp past txnB. Now txnB is oldest.
	require.True(t, rts.ConsumeRecordWritten(ctx, txnA, ts(40)))
	require.Equal(t, ts(29), rts.Get())

	// Forward txnA again but to same timestamp (no-op).
	require.False(t, rts.ConsumeRecordWritten(ctx, txnA, ts(40)))
	require.Equal(t, ts(29), rts.Get())
}

func TestResolvedTimestampRemoveUnknown(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	var rts resolvedTimestamp

	rts.Init(ctx)
	rts.ForwardClosedTS(ctx, ts(10))

	// Removing a transaction that was never tracked is a no-op.
	require.False(t, rts.ConsumeCommitted(ctx, uuid.MakeV4()))
	require.False(t, rts.ConsumeAborted(ctx, uuid.MakeV4()))
	require.Equal(t, ts(10), rts.Get())
}

func TestResolvedTimestampNotInitialized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	var rts resolvedTimestamp

	// Before Init, operations don't advance the resolved timestamp.
	require.False(t, rts.ForwardClosedTS(ctx, ts(10)))
	require.True(t, rts.Get().IsEmpty())

	txnID := uuid.MakeV4()
	require.False(t, rts.ConsumeRecordWritten(ctx, txnID, ts(5)))
	require.True(t, rts.Get().IsEmpty())

	// Init computes the resolved TS from the current state.
	require.True(t, rts.Init(ctx))
	require.Equal(t, ts(4), rts.Get())
}

// TestResolvedTimestampPreInitRemovals verifies that transactions committed
// or aborted before initialization are tracked and skipped during init scan
// processing.
func TestResolvedTimestampPreInitRemovals(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	var rts resolvedTimestamp

	txnA := uuid.MakeV4()
	txnB := uuid.MakeV4()
	txnC := uuid.MakeV4()

	// Before init, add a record and then commit/abort transactions.
	rts.ConsumeRecordWritten(ctx, txnA, ts(10))
	rts.ConsumeCommitted(ctx, txnB)
	rts.ConsumeAborted(ctx, txnC)

	// txnB and txnC should be in pre-init removals.
	require.True(t, rts.wasRemovedBeforeInit(txnB))
	require.True(t, rts.wasRemovedBeforeInit(txnC))
	require.False(t, rts.wasRemovedBeforeInit(txnA))

	// After Init, preInitRemovals is cleared.
	rts.Init(ctx)
	require.False(t, rts.wasRemovedBeforeInit(txnB))
	require.False(t, rts.wasRemovedBeforeInit(txnC))
}

func TestUnresolvedTxnRecordQueueOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var q unresolvedTxnRecordQueue

	txnA := uuid.MakeV4()
	txnB := uuid.MakeV4()
	txnC := uuid.MakeV4()

	q.Add(txnC, ts(30))
	q.Add(txnA, ts(10))
	q.Add(txnB, ts(20))

	require.Equal(t, 3, q.Len())

	// Oldest should be txnA at ts(10).
	oldest := q.Oldest()
	require.Equal(t, txnA, oldest.txnID)
	require.Equal(t, ts(10), oldest.writeTimestamp)

	// Remove oldest, next should be txnB at ts(20).
	q.Remove(txnA)
	oldest = q.Oldest()
	require.Equal(t, txnB, oldest.txnID)
	require.Equal(t, ts(20), oldest.writeTimestamp)

	// Remove oldest, next should be txnC at ts(30).
	q.Remove(txnB)
	oldest = q.Oldest()
	require.Equal(t, txnC, oldest.txnID)
	require.Equal(t, ts(30), oldest.writeTimestamp)

	// Remove last, queue is empty.
	q.Remove(txnC)
	require.Equal(t, 0, q.Len())
	require.Nil(t, q.Oldest())
}

func TestUnresolvedTxnRecordQueueSameTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var q unresolvedTxnRecordQueue

	// Use deterministic UUIDs to test tie-breaking.
	txnA := uuid.FromStringOrNil("00000000-0000-0000-0000-000000000001")
	txnB := uuid.FromStringOrNil("00000000-0000-0000-0000-000000000002")

	q.Add(txnB, ts(10))
	q.Add(txnA, ts(10))

	// txnA should come first due to UUID ordering.
	require.Equal(t, txnA, q.Oldest().txnID)
}
