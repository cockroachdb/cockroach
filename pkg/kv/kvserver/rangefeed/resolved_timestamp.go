// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"bytes"
	"container/heap"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// A rangefeed's "resolved timestamp" is defined as the timestamp at which no
// future updates will be emitted to the feed at or before. The timestamp is
// monotonically increasing and is communicated through RangeFeedCheckpoint
// notifications whenever it changes.
//
// The resolved timestamp is closely tied to a Range's closed timestamp, but
// these concepts are not the same. Fundamentally, a closed timestamp is a
// property of a Range that restricts its state such that no "visible" data
// mutations are permitted at equal or earlier timestamps. This enables the
// guarantee that if the closed timestamp conditions are met, a follower replica
// has all state necessary to satisfy reads at or before the CT. On the other
// hand, a resolved timestamp is a property of a rangefeed that restrict its
// state such that no value notifications will be emitted at equal or earlier
// timestamps. The key difference here is that data mutations are allowed on a
// Range beneath a closed timestamp as long as they are not externally
// "visible". This is not true of the resolved timestamp because a rangefeed is
// driven directly off the state of a Range through its Raft log updates. As
// such, all changes to a Range beneath a given timestamp that will end up
// published on a rangefeed, "visible" or not, must be made to the Range before
// its resolved timestamp can be advanced to that timestamp.
//
// This distinction becomes interesting when considering how committed
// transaction intents are published as rangefeed events. Because the rangefeed
// is driven off the Raft log, these events are published when the intents are
// resolved, not when their corresponding transactions are committed. This leads
// to an important case with unresolved intents where a Range's closed timestamp
// and its corresponding rangefeed's resolved timestamp diverge because the
// closed timestamp advances past the timestamp of the unresolved intents. This
// is permitted because intent resolution of an intent which is part of a
// committed or aborted transaction does not change the visible state of a
// range. In effect, this means that a range can "close" timestamp t2 before
// resolving an intent for a transaction at timestamp t1, where t1 < t2.
// However, the rangefeed cannot "resolve" timestamp t2 until after the event
// for the committed intent is published. This creates a scenario where the
// Range's closed timestamp could have advanced to t2 while the corresponding
// rangefeed's resolved timestamp lags behind at some time earlier than t1,
// waiting for the intent resolution to result in a rangefeed publication.
//
// It follows that the closed timestamp mechanism is a necessary, but not
// sufficient, solution to creating a resolved timstamp. The closed timestamp is
// a necessary basis for the resolved timestamp because without it there could
// never be any guarantee about new changes to a range. However, the closed
// timestamp is not sufficient and could not replace the resolved timestamp
// directly because it does not wait for all changes to result in rangefeed
// notifications before advancing. In order to provide the proper guarantees for
// the resolved timestamp, it must be computed as the minimum of the closed
// timestamp and the timestamp of the earliest "unresolved intent" (see below)
// in the rangefeed's corresponding range of keys. This relies on one implicit
// assumption that is important enough to state: the closed timestamp must
// ensure that no unresolved intents will appear at timestamps that it has
// already closed off. Naively this property will hold, but it requires that
// changes to the closed timestamp and the tracking of unresolved intents be
// handled carefully and in a total order.
type resolvedTimestamp struct {
	init       bool
	closedTS   hlc.Timestamp
	resolvedTS hlc.Timestamp
	intentQ    unresolvedIntentQueue
}

func makeResolvedTimestamp() resolvedTimestamp {
	return resolvedTimestamp{
		intentQ: makeUnresolvedIntentQueue(),
	}
}

// Get returns the current value of the resolved timestamp.
func (rts *resolvedTimestamp) Get() hlc.Timestamp {
	return rts.resolvedTS
}

// Init informs the resolved timestamp that it has been provided all unresolved
// intents within its key range that may have timestamps lower than the initial
// closed timestamp. Once initialized, the resolvedTimestamp can begin operating
// in its steady state. The method returns whether this caused the resolved
// timestamp to move forward.
func (rts *resolvedTimestamp) Init() bool {
	rts.init = true
	// Once the resolvedTimestamp is initialized, all prior written intents
	// should be accounted for, so reference counts for transactions that
	// would drop below zero will all be due to aborted transactions. These
	// can all be ignored.
	rts.intentQ.AllowNegRefCount(false)
	return rts.recompute()
}

// IsInit returns whether the resolved timestamp is initialized.
func (rts *resolvedTimestamp) IsInit() bool {
	return rts.init
}

// ForwardClosedTS indicates that the closed timestamp that serves as the basis
// for the resolved timestamp has advanced. The method returns whether this
// caused the resolved timestamp to move forward.
func (rts *resolvedTimestamp) ForwardClosedTS(newClosedTS hlc.Timestamp) bool {
	if rts.closedTS.Forward(newClosedTS) {
		return rts.recompute()
	}
	rts.assertNoChange()
	return false
}

// ConsumeLogicalOp informs the resolved timestamp of the occupance of a logical
// operation within its range of tracked keys. This allows the structure to
// update its internal intent tracking to reflect the change. The method returns
// whether this caused the resolved timestamp to move forward.
func (rts *resolvedTimestamp) ConsumeLogicalOp(op enginepb.MVCCLogicalOp) bool {
	if rts.consumeLogicalOp(op) {
		return rts.recompute()
	}
	rts.assertNoChange()
	return false
}

func (rts *resolvedTimestamp) consumeLogicalOp(op enginepb.MVCCLogicalOp) bool {
	switch t := op.GetValue().(type) {
	case *enginepb.MVCCWriteValueOp:
		rts.assertOpAboveRTS(op, t.Timestamp)
		return false

	case *enginepb.MVCCWriteIntentOp:
		rts.assertOpAboveRTS(op, t.Timestamp)
		return rts.intentQ.IncRef(t.TxnID, t.TxnKey, t.TxnMinTimestamp, t.Timestamp)

	case *enginepb.MVCCUpdateIntentOp:
		return rts.intentQ.UpdateTS(t.TxnID, t.Timestamp)

	case *enginepb.MVCCCommitIntentOp:
		return rts.intentQ.DecrRef(t.TxnID, t.Timestamp)

	case *enginepb.MVCCAbortIntentOp:
		// An aborted intent does not necessarily indicate an aborted
		// transaction. An AbortIntent operation can be the result of an intent
		// that was written only in an earlier epoch being resolved after its
		// transaction committed in a later epoch. Don't make any assumptions
		// about the transaction other than to decrement its reference count.
		return rts.intentQ.DecrRef(t.TxnID, hlc.Timestamp{})

	case *enginepb.MVCCAbortTxnOp:
		// Unlike the previous case, an aborted transaction does indicate
		// that none of the transaction's intents will ever be committed.
		// This means that we can stop tracking the transaction entirely.
		// Doing so is critical to ensure forward progress of the resolved
		// timestamp in situtations where the oldest transaction on a range
		// is abandoned and the locations of its intents are unknown.
		//
		// However, the transaction may also still be writing, updating, and
		// resolving (aborting) its intents, so we need to be careful with
		// how we handle any future operations from this transaction. There
		// are three different operations we could see the zombie transaction
		// perform:
		//
		// - MVCCWriteIntentOp: it could write another intent. This could result
		//     in "reintroducing" the transaction to the queue. We allow this
		//     to happen and rely on pushing the transaction again, eventually
		//     evicting the transaction from the queue for good.
		//
		//     Just like any other transaction, this new intent will necessarily
		//     be pushed above the closed timestamp, so we don't need to worry
		//     about resolved timestamp regressions.
		//
		// - MVCCUpdateIntentOp: it could update one of its intents. If we're
		//     not already tracking the transaction then the queue will ignore
		//     the intent update.
		//
		// - MVCCAbortIntentOp: it could resolve one of its intents as aborted.
		//     This is the most likely case. Again, if we're not already tracking
		//     the transaction then the queue will ignore the intent abort.
		//
		if !rts.IsInit() {
			// We ignore MVCCAbortTxnOp operations until the queue is
			// initialized. This is necessary because we allow txn reference
			// counts to drop below zero before the queue is initialized and
			// expect that all reference count decrements be balanced by a
			// corresponding reference count increment.
			//
			// We could remove this restriction if we evicted all transactions
			// with negative reference counts after initialization, but this is
			// easier and more clear.
			return false
		}
		return rts.intentQ.Del(t.TxnID)

	default:
		panic(errors.AssertionFailedf("unknown logical op %T", t))
	}
}

// recompute computes the resolved timestamp based on its respective closed
// timestamp and the in-flight intents that it is tracking. The method returns
// whether this caused the resolved timestamp to move forward.
func (rts *resolvedTimestamp) recompute() bool {
	if !rts.IsInit() {
		return false
	}
	if rts.closedTS.Less(rts.resolvedTS) {
		panic(fmt.Sprintf("closed timestamp below resolved timestamp: %s < %s",
			rts.closedTS, rts.resolvedTS))
	}
	newTS := rts.closedTS

	// Take into account the intents that haven't been yet resolved - their
	// timestamps cannot be resolved yet.
	if txn := rts.intentQ.Oldest(); txn != nil {
		if txn.timestamp.LessEq(rts.resolvedTS) {
			panic(fmt.Sprintf("unresolved txn equal to or below resolved timestamp: %s <= %s",
				txn.timestamp, rts.resolvedTS))
		}
		// txn.timestamp cannot be resolved, so the resolved timestamp must be Prev.
		txnTS := txn.timestamp.Prev()
		newTS.Backward(txnTS)
	}
	// Truncate the logical part. It might have come from a Prev call above, and
	// it's dangerous to start pushing things above Logical=MaxInt32.
	newTS.Logical = 0

	if newTS.Less(rts.resolvedTS) {
		panic(fmt.Sprintf("resolved timestamp regression, was %s, recomputed as %s",
			rts.resolvedTS, newTS))
	}
	return rts.resolvedTS.Forward(newTS)
}

// assertNoChange asserts that a recomputation of the resolved timestamp does
// not change its value. A violation of this assertion would indicate a logic
// error in the resolvedTimestamp implementation.
func (rts *resolvedTimestamp) assertNoChange() {
	before := rts.resolvedTS
	changed := rts.recompute()
	if changed || !before.EqOrdering(rts.resolvedTS) {
		panic(fmt.Sprintf("unexpected resolved timestamp change on recomputation, "+
			"was %s, recomputed as %s", before, rts.resolvedTS))
	}
}

// assertOpAboveTimestamp asserts that this operation is at a larger timestamp
// than the current resolved timestamp. A violation of this assertion would
// indicate a failure of the closed timestamp mechanism.
func (rts *resolvedTimestamp) assertOpAboveRTS(op enginepb.MVCCLogicalOp, opTS hlc.Timestamp) {
	if opTS.LessEq(rts.resolvedTS) {
		panic(fmt.Sprintf("resolved timestamp %s equal to or above timestamp of operation %v",
			rts.resolvedTS, op))
	}
}

// An "unresolved intent" in the context of the rangefeed primitive is an intent
// that may at some point in the future result in a RangeFeedValue publication.
// Based on this definition, there are three possible states that an extent
// intent can be in while fitting the requirement to be an "unresolved intent":
// 1. part of a PENDING transaction
// 2. part of a STAGING transaction that has not been explicitly committed yet
// 3. part of a COMMITTED transaction but not yet resolved due to the asynchronous
//    nature of intent resolution
// Notably, this means that an intent that exists but that is known to be part
// of an ABORTED transaction is not considered "unresolved", even if it has yet
// to be cleaned up. In the context of rangefeeds, the intent's fate is resolved
// to never result in a RangeFeedValue publication.
//
// Defining unresolved intents in this way presents two paths for an unresolved
// intent to become resolved (and thus decrement the unresolvedTxn's ref count).
// An unresolved intent can become resolved if:
// 1. it is COMMITTED or ABORTED through the traditional intent resolution
//    process.
// 2. it's transaction is observed to be ABORTED, meaning that it is by
//    definition resolved even if it has yet to be cleaned up by the intent
//    resolution process.
//
// An unresolvedTxn is a transaction that has one or more unresolved intents on
// a given range. The structure itself maintains metadata about the transaction
// along with a reference count of the number of unresolved intents created by
// the transaction on a given range.
type unresolvedTxn struct {
	txnID           uuid.UUID
	txnKey          roachpb.Key
	txnMinTimestamp hlc.Timestamp
	timestamp       hlc.Timestamp
	refCount        int // count of unresolved intents

	// The index of the item in the unresolvedTxnHeap, maintained by the
	// heap.Interface methods.
	index int
}

// asTxnMeta returns a TxnMeta representation of the unresolved transaction.
func (t *unresolvedTxn) asTxnMeta() enginepb.TxnMeta {
	return enginepb.TxnMeta{
		ID:             t.txnID,
		Key:            t.txnKey,
		MinTimestamp:   t.txnMinTimestamp,
		WriteTimestamp: t.timestamp,
	}
}

// unresolvedTxnHeap implements heap.Interface and holds unresolvedTxns.
// Transactions are prioritized based on their timestamp such that the oldest
// unresolved transaction will rise to the top of the heap.
type unresolvedTxnHeap []*unresolvedTxn

func (h unresolvedTxnHeap) Len() int { return len(h) }

func (h unresolvedTxnHeap) Less(i, j int) bool {
	// container/heap constructs a min-heap by default, so prioritize the txn
	// with the smaller timestamp. Break ties by comparing IDs to establish a
	// total order.
	if h[i].timestamp.EqOrdering(h[j].timestamp) {
		return bytes.Compare(h[i].txnID.GetBytes(), h[j].txnID.GetBytes()) < 0
	}
	return h[i].timestamp.Less(h[j].timestamp)
}

func (h unresolvedTxnHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

func (h *unresolvedTxnHeap) Push(x interface{}) {
	n := len(*h)
	txn := x.(*unresolvedTxn)
	txn.index = n
	*h = append(*h, txn)
}

func (h *unresolvedTxnHeap) Pop() interface{} {
	old := *h
	n := len(old)
	txn := old[n-1]
	txn.index = -1 // for safety
	old[n-1] = nil // for gc
	*h = old[0 : n-1]
	return txn
}

// unresolvedIntentQueue tracks all unresolved intents that exist within the key
// bounds of a range. It does so by tracking every transaction that contains at
// least one unresolved intent on the range. For each of these transactions, the
// queue maintains a count of the number of unresolved intents it contains on
// the range. By doing so and watching for when the count drops to zero, the
// queue can determine when a transaction is no longer unresolved.
//
// The queue maintains an ordering of transactions by timestamp. This allows it
// to determine the oldest unresolved intent that it's tracking and by extension
// the earliest possible time that a RangeFeedValue can be emitted at. Combined
// with a closed timestamp, which guarantees that no transactions can write new
// intents at or beneath it, a resolved timestamp can be constructed.
type unresolvedIntentQueue struct {
	txns             map[uuid.UUID]*unresolvedTxn
	minHeap          unresolvedTxnHeap
	allowNegRefCount bool
}

func makeUnresolvedIntentQueue() unresolvedIntentQueue {
	return unresolvedIntentQueue{
		txns:             make(map[uuid.UUID]*unresolvedTxn),
		allowNegRefCount: true,
	}
}

// Len returns the number of transactions being tracked.
func (uiq *unresolvedIntentQueue) Len() int {
	return uiq.minHeap.Len()
}

// Oldest returns the oldest transaction that is being tracked in the
// unresolvedIntentQueue, or nil if the queue is empty. If two transactions have
// the same timestamp, breaks the tie by returning the transaction with the ID
// that sorts first.
func (uiq *unresolvedIntentQueue) Oldest() *unresolvedTxn {
	if uiq.Len() == 0 {
		return nil
	}
	return uiq.minHeap[0]
}

// Before returns all transactions that have timestamps before a certain
// timestamp. It does so in O(n) time, where n is the number of matching
// transactions, NOT the total number of transactions being tracked. The
// resulting transactions will not be in sorted order.
func (uiq *unresolvedIntentQueue) Before(ts hlc.Timestamp) []*unresolvedTxn {
	var txns []*unresolvedTxn
	var collect func(int)
	collect = func(i int) {
		if len(uiq.minHeap) > i && uiq.minHeap[i].timestamp.Less(ts) {
			txns = append(txns, uiq.minHeap[i])
			collect((2 * i) + 1) // left child
			collect((2 * i) + 2) // right child
		}
	}
	collect(0)
	return txns
}

// IncRef increments the reference count of the specified transaction. It
// returns whether the update advanced the timestamp of the oldest transaction
// in the queue.
func (uiq *unresolvedIntentQueue) IncRef(
	txnID uuid.UUID, txnKey roachpb.Key, txnMinTS, ts hlc.Timestamp,
) bool {
	return uiq.updateTxn(txnID, txnKey, txnMinTS, ts, +1)
}

// DecrRef decrements the reference count of the specified transaction. It
// returns whether the update advanced the timestamp of the oldest transaction
// in the queue.
func (uiq *unresolvedIntentQueue) DecrRef(txnID uuid.UUID, ts hlc.Timestamp) bool {
	return uiq.updateTxn(txnID, nil, hlc.Timestamp{}, ts, -1)
}

// UpdateTS updates the timestamp of the specified transaction without modifying
// its intent reference count. It returns whether the update advanced the
// timestamp of the oldest transaction in the queue.
func (uiq *unresolvedIntentQueue) UpdateTS(txnID uuid.UUID, ts hlc.Timestamp) bool {
	return uiq.updateTxn(txnID, nil, hlc.Timestamp{}, ts, 0)
}

func (uiq *unresolvedIntentQueue) updateTxn(
	txnID uuid.UUID, txnKey roachpb.Key, txnMinTS, ts hlc.Timestamp, delta int,
) bool {
	txn, ok := uiq.txns[txnID]
	if !ok {
		if delta == 0 || (delta < 0 && !uiq.allowNegRefCount) {
			// Unknown txn.
			return false
		}

		// Add new txn to the queue.
		txn = &unresolvedTxn{
			txnID:           txnID,
			txnKey:          txnKey,
			txnMinTimestamp: txnMinTS,
			timestamp:       ts,
			refCount:        delta,
		}
		uiq.txns[txn.txnID] = txn
		heap.Push(&uiq.minHeap, txn)

		// Adding a new txn can't advance the queue's earliest timestamp.
		return false
	}

	// Will changes to the txn advance the queue's earliest timestamp?
	wasMin := txn.index == 0

	txn.refCount += delta
	if txn.refCount == 0 || (txn.refCount < 0 && !uiq.allowNegRefCount) {
		// Remove txn from the queue.
		// NB: the txn.refCount < 0 case is not exercised by the external
		// interface of this type because currently |delta| <= 1, but it
		// is included for robustness.
		delete(uiq.txns, txn.txnID)
		heap.Remove(&uiq.minHeap, txn.index)
		return wasMin
	}

	// Forward the txn's timestamp. Need to fix heap if timestamp changes.
	if txn.timestamp.Forward(ts) {
		heap.Fix(&uiq.minHeap, txn.index)
		return wasMin
	}
	return false
}

// Del removes the transaction from the queue. It returns whether the update had
// an effect on the oldest transaction in the queue.
func (uiq *unresolvedIntentQueue) Del(txnID uuid.UUID) bool {
	// This implementation is logically equivalent to the following, but
	// it avoids underflow conditions:
	//  return uiq.updateTxn(txnID, nil, hlc.Timestamp{}, hlc.Timestamp{}, math.MinInt64)

	txn, ok := uiq.txns[txnID]
	if !ok {
		// Unknown txn.
		return false
	}

	// Will deleting the txn advance the queue's earliest timestamp?
	wasMin := txn.index == 0

	// Remove txn from the queue.
	delete(uiq.txns, txn.txnID)
	heap.Remove(&uiq.minHeap, txn.index)
	return wasMin
}

// AllowNegRefCount instruts the unresolvedIntentQueue on whether or not to
// allow the reference count on transactions to drop below zero. If disallowed,
// the method also asserts that all unresolved intent refcounts for transactions
// currently in the queue are positive. Assertion takes O(n) time, where n is
// the total number of transactions being tracked in the queue.
func (uiq *unresolvedIntentQueue) AllowNegRefCount(b bool) {
	if !b {
		// Assert that the queue is currently in compliance.
		uiq.assertOnlyPositiveRefCounts()
	}
	uiq.allowNegRefCount = b
}

func (uiq *unresolvedIntentQueue) assertOnlyPositiveRefCounts() {
	for _, txn := range uiq.txns {
		if txn.refCount <= 0 {
			panic(fmt.Sprintf("negative refcount %d for txn %+v", txn.refCount, txn))
		}
	}
}
