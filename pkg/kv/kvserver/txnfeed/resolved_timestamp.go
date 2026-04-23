// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/container/heap"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// resolvedTimestamp tracks the resolved timestamp for the TxnFeed processor.
//
// The resolved timestamp is the minimum of the closed timestamp and one
// below the write timestamp of the oldest unresolved transaction record in
// the tracked key range (with the logical part truncated to zero). An
// unresolved transaction record is one in a non-finalized state (PENDING or
// STAGING) that has not yet been committed or aborted.
//
// The resolved timestamp provides the guarantee that no future
// TxnFeedCommitted events will be emitted for transactions with commit
// timestamps at or below the resolved timestamp and anchor keys within the
// tracked range.
//
// This relies on a critical invariant: transaction records must be created
// with a WriteTimestamp above the closed timestamp. This is enforced by
// EndTxn (which returns WriteTooOldError) and HeartbeatTxn (which silently
// bumps). Without this, the resolved timestamp would need to regress when a
// new record appears below the closed timestamp.
type resolvedTimestamp struct {
	init       bool
	closedTS   hlc.Timestamp
	resolvedTS hlc.Timestamp
	txnQ       unresolvedTxnRecordQueue

	// preInitRemovals tracks txn IDs that were committed or aborted before
	// the resolved timestamp was initialized. When the init scan completes,
	// these txns are skipped to avoid re-adding already-finalized
	// transactions. Cleared on Init.
	preInitRemovals map[uuid.UUID]struct{}
}

// Init marks the resolved timestamp as initialized and performs the initial
// computation. Returns true if the resolved timestamp is non-zero after
// initialization.
//
// Before Init is called, live events (ConsumeRecordWritten, ConsumeCommitted,
// ConsumeAborted, ForwardClosedTS) are accepted and tracked, but recompute
// is a no-op so no checkpoints are emitted. Committed/aborted txns are
// recorded in preInitRemovals so they can be filtered from the init scan
// results. Init clears preInitRemovals after the scan results are applied.
func (rts *resolvedTimestamp) Init(ctx context.Context) bool {
	rts.init = true
	rts.preInitRemovals = nil
	return rts.recompute(ctx)
}

// IsInit returns whether the resolved timestamp has been initialized.
func (rts *resolvedTimestamp) IsInit() bool {
	return rts.init
}

// Get returns the current value of the resolved timestamp.
func (rts *resolvedTimestamp) Get() hlc.Timestamp {
	return rts.resolvedTS
}

// ForwardClosedTS advances the closed timestamp and recomputes the resolved
// timestamp. Returns true if the resolved timestamp advanced.
func (rts *resolvedTimestamp) ForwardClosedTS(ctx context.Context, closedTS hlc.Timestamp) bool {
	if rts.closedTS.Forward(closedTS) {
		return rts.recompute(ctx)
	}
	return false
}

// ConsumeRecordWritten tracks a non-finalized transaction record. If the
// transaction is already tracked, its timestamp is forwarded. Returns true
// if the resolved timestamp advanced (which can happen if a tracked
// transaction's timestamp moves forward past the current oldest).
func (rts *resolvedTimestamp) ConsumeRecordWritten(
	ctx context.Context, txnID uuid.UUID, writeTS hlc.Timestamp,
) bool {
	if rts.txnQ.Add(txnID, writeTS) {
		return rts.recompute(ctx)
	}
	return false
}

// ConsumeCommitted removes a committed transaction from tracking. Returns
// true if the resolved timestamp advanced.
func (rts *resolvedTimestamp) ConsumeCommitted(ctx context.Context, txnID uuid.UUID) bool {
	if !rts.init {
		rts.recordPreInitRemoval(txnID)
	}
	if rts.txnQ.Remove(txnID) {
		return rts.recompute(ctx)
	}
	return false
}

// ConsumeAborted removes an aborted transaction from tracking. Returns true
// if the resolved timestamp advanced.
func (rts *resolvedTimestamp) ConsumeAborted(ctx context.Context, txnID uuid.UUID) bool {
	if !rts.init {
		rts.recordPreInitRemoval(txnID)
	}
	if rts.txnQ.Remove(txnID) {
		return rts.recompute(ctx)
	}
	return false
}

// recordPreInitRemoval records a txn ID that was committed or aborted before
// initialization. The init scan uses this to skip txns that have already been
// finalized by live events.
func (rts *resolvedTimestamp) recordPreInitRemoval(txnID uuid.UUID) {
	if rts.preInitRemovals == nil {
		rts.preInitRemovals = make(map[uuid.UUID]struct{})
	}
	rts.preInitRemovals[txnID] = struct{}{}
}

// wasRemovedBeforeInit returns true if the given txn was committed or
// aborted before the resolved timestamp was initialized.
func (rts *resolvedTimestamp) wasRemovedBeforeInit(txnID uuid.UUID) bool {
	_, ok := rts.preInitRemovals[txnID]
	return ok
}

// recompute computes the resolved timestamp based on the closed timestamp
// and the oldest unresolved transaction record. Returns true if the resolved
// timestamp moved forward.
func (rts *resolvedTimestamp) recompute(ctx context.Context) bool {
	if !rts.IsInit() {
		return false
	}
	if rts.closedTS.Less(rts.resolvedTS) {
		log.KvExec.Fatalf(ctx,
			"closed timestamp below resolved timestamp: %s < %s",
			rts.closedTS, rts.resolvedTS)
	}
	newTS := rts.closedTS

	if txn := rts.txnQ.Oldest(); txn != nil {
		if txn.writeTimestamp.LessEq(rts.resolvedTS) {
			log.KvExec.Fatalf(ctx,
				"unresolved txn equal to or below resolved timestamp: %s <= %s",
				txn.writeTimestamp, rts.resolvedTS)
		}
		// The transaction's write timestamp cannot be resolved yet, so
		// the resolved timestamp must be strictly below it.
		txnTS := txn.writeTimestamp.Prev()
		newTS.Backward(txnTS)
	}
	// Truncate the logical part. It might have come from a Prev call
	// above, and it's dangerous to start pushing things above
	// Logical=MaxInt32.
	newTS.Logical = 0

	if newTS.Less(rts.resolvedTS) {
		log.KvExec.Fatalf(ctx,
			"resolved timestamp regression, was %s, recomputed as %s",
			rts.resolvedTS, newTS)
	}
	return rts.resolvedTS.Forward(newTS)
}

// unresolvedTxnRecord represents a transaction record that is in a
// non-finalized state (PENDING or STAGING).
type unresolvedTxnRecord struct {
	txnID          uuid.UUID
	writeTimestamp hlc.Timestamp
	// index is the position of this record in the min-heap, maintained by
	// the heap.Interface methods.
	index int
}

// unresolvedTxnRecordHeap implements heap.Interface for unresolvedTxnRecord
// pointers. Transactions are ordered by writeTimestamp so the oldest
// unresolved transaction rises to the top.
type unresolvedTxnRecordHeap []*unresolvedTxnRecord

func (h unresolvedTxnRecordHeap) Len() int { return len(h) }

func (h unresolvedTxnRecordHeap) Less(i, j int) bool {
	if h[i].writeTimestamp == h[j].writeTimestamp {
		return bytes.Compare(
			h[i].txnID.GetBytes(), h[j].txnID.GetBytes()) < 0
	}
	return h[i].writeTimestamp.Less(h[j].writeTimestamp)
}

func (h unresolvedTxnRecordHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

func (h *unresolvedTxnRecordHeap) Push(rec *unresolvedTxnRecord) {
	rec.index = len(*h)
	*h = append(*h, rec)
}

func (h *unresolvedTxnRecordHeap) Pop() *unresolvedTxnRecord {
	old := *h
	n := len(old)
	rec := old[n-1]
	rec.index = -1 // for safety
	old[n-1] = nil // for gc
	*h = old[:n-1]
	return rec
}

// unresolvedTxnRecordQueue tracks unresolved transaction records using a
// map for O(1) lookup and a min-heap for O(1) access to the oldest record.
// Unlike the rangefeed unresolvedIntentQueue, there is no reference counting
// because each transaction has at most one record.
type unresolvedTxnRecordQueue struct {
	txns    map[uuid.UUID]*unresolvedTxnRecord
	minHeap unresolvedTxnRecordHeap
}

// Len returns the number of unresolved transactions being tracked.
func (q *unresolvedTxnRecordQueue) Len() int {
	return len(q.minHeap)
}

// Oldest returns the oldest unresolved transaction record, or nil if the
// queue is empty.
func (q *unresolvedTxnRecordQueue) Oldest() *unresolvedTxnRecord {
	if q.Len() == 0 {
		return nil
	}
	return q.minHeap[0]
}

// Add adds or updates an unresolved transaction record. If the transaction
// is already tracked, its timestamp is forwarded (useful for heartbeats
// re-emitting the record). Returns true if the oldest record's timestamp
// may have changed (i.e., if this was a new record or the oldest record
// was updated).
func (q *unresolvedTxnRecordQueue) Add(txnID uuid.UUID, writeTS hlc.Timestamp) bool {
	if q.txns == nil {
		q.txns = make(map[uuid.UUID]*unresolvedTxnRecord)
	}
	if existing, ok := q.txns[txnID]; ok {
		if existing.writeTimestamp.Forward(writeTS) {
			heap.Fix[*unresolvedTxnRecord](&q.minHeap, existing.index)
			return true
		}
		return false
	}
	rec := &unresolvedTxnRecord{
		txnID:          txnID,
		writeTimestamp: writeTS,
	}
	q.txns[txnID] = rec
	heap.Push[*unresolvedTxnRecord](&q.minHeap, rec)
	return true
}

// Remove removes a transaction from the queue. Returns true if the
// transaction was found and removed, indicating that the oldest record may
// have changed. Returns false if the transaction was not being tracked
// (no-op).
func (q *unresolvedTxnRecordQueue) Remove(txnID uuid.UUID) bool {
	rec, ok := q.txns[txnID]
	if !ok {
		return false
	}
	delete(q.txns, txnID)
	heap.Remove[*unresolvedTxnRecord](&q.minHeap, rec.index)
	return true
}
