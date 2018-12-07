// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rangefeed

import (
	"bytes"
	"container/heap"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	rts.intentQ.assertPositiveRefCounts()
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
		return rts.intentQ.IncRef(t.TxnID, t.TxnKey, t.Timestamp)

	case *enginepb.MVCCUpdateIntentOp:
		return rts.intentQ.UpdateTS(t.TxnID, t.Timestamp)

	case *enginepb.MVCCCommitIntentOp:
		return rts.intentQ.DecrRef(t.TxnID, t.Timestamp)

	case *enginepb.MVCCAbortIntentOp:
		// If the resolved timestamp has been initialized then we can remove the
		// txn from the queue immediately after we observe that it has been
		// aborted. However, if the resolved timestamp has not yet been
		// initialized then we decrement from the txn's reference count. This
		// permits the refcount to drop below 0, which is important to handle
		// correctly when events may be out of order. For instance, before the
		// resolved timestamp is initialized, it's possible that we see an
		// intent get aborted before we see it in the first place. If we didn't
		// let the refcount drop below 0, we would risk leaking a reference.
		if rts.IsInit() {
			return rts.intentQ.Del(t.TxnID)
		}
		return rts.intentQ.DecrRef(t.TxnID, hlc.Timestamp{})

	default:
		panic(fmt.Sprintf("unknown logical op %T", t))
	}
}

// recompute computes the resolved timestamp based on its respective closed
// timestamp and the in-flight intents that it is tracking. The method returns
// whether this caused the resolved timestamp to move forward.
func (rts *resolvedTimestamp) recompute() bool {
	if !rts.IsInit() {
		return false
	}
	newTS := rts.closedTS
	if txn := rts.intentQ.Oldest(); txn != nil {
		txnTS := txn.timestamp.FloorPrev()
		if txnTS.Less(newTS) {
			newTS = txnTS
		}
	}
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
	if changed || (before != rts.resolvedTS) {
		panic(fmt.Sprintf("unexpected resolved timestamp change on recomputation, "+
			"was %s, recomputed as %s", before, rts.resolvedTS))
	}
}

// assertOpAboveTimestamp asserts that this operation is at a larger timestamp
// than the current resolved timestamp. A violation of this assertion would
// indicate a failure of the closed timestamp mechanism.
func (rts *resolvedTimestamp) assertOpAboveRTS(op enginepb.MVCCLogicalOp, opTS hlc.Timestamp) {
	if !rts.resolvedTS.Less(opTS) {
		panic(fmt.Sprintf("resolved timestamp %s equal to or above timestamp of operation %v",
			rts.resolvedTS, op))
	}
}

// An "unresolved intent" in the context of the rangefeed primitive is an intent
// that may at some point in the future result in a RangeFeedValue publication.
// Based on this definition, there are two possible states that an extent intent
// can be in while fitting the requirement to be an "unresolved intent":
// 1. part of a PENDING transaction
// 2. part of a COMMITTED transaction but not yet resolved due to the
//     asynchronous nature of intent resolution
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
	txnID     uuid.UUID
	txnKey    roachpb.Key
	timestamp hlc.Timestamp
	refCount  int // count of unresolved intents

	// The index of the item in the unresolvedTxnHeap, maintained by the
	// heap.Interface methods.
	index int
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
	if h[i].timestamp == h[j].timestamp {
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
	txns    map[uuid.UUID]*unresolvedTxn
	minHeap unresolvedTxnHeap
}

func makeUnresolvedIntentQueue() unresolvedIntentQueue {
	return unresolvedIntentQueue{
		txns: make(map[uuid.UUID]*unresolvedTxn),
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
	txnID uuid.UUID, txnKey roachpb.Key, ts hlc.Timestamp,
) bool {
	return uiq.updateTxn(txnID, txnKey, ts, +1)
}

// DecrRef decrements the reference count of the specified transaction. It
// returns whether the update advanced the timestamp of the oldest transaction
// in the queue.
func (uiq *unresolvedIntentQueue) DecrRef(txnID uuid.UUID, ts hlc.Timestamp) bool {
	return uiq.updateTxn(txnID, nil, ts, -1)
}

// UpdateTS updates the timestamp of the specified transaction without modifying
// its intent reference count. It returns whether the update advanced the
// timestamp of the oldest transaction in the queue.
func (uiq *unresolvedIntentQueue) UpdateTS(txnID uuid.UUID, ts hlc.Timestamp) bool {
	return uiq.updateTxn(txnID, nil, ts, 0)
}

func (uiq *unresolvedIntentQueue) updateTxn(
	txnID uuid.UUID, txnKey roachpb.Key, ts hlc.Timestamp, delta int,
) bool {
	txn, ok := uiq.txns[txnID]
	if !ok {
		// Unknown txn.
		if delta == 0 {
			return false
		}

		// Add new txn to the queue.
		txn = &unresolvedTxn{
			txnID:     txnID,
			txnKey:    txnKey,
			timestamp: ts,
			refCount:  delta,
		}
		uiq.txns[txn.txnID] = txn
		heap.Push(&uiq.minHeap, txn)

		// Adding a new txn can't advance the queue's earliest timestamp.
		return false
	}

	// Will changes to the txn advance the queue's earliest timestamp?
	wasMin := txn.index == 0

	txn.refCount += delta
	if txn.refCount == 0 {
		// Remove txn from the queue.
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

// assertPositiveRefCounts asserts that all unresolved intent refcounts for
// transactions in the unresolvedIntentQueue are positive. Assertion takes O(n)
// time, where n is the total number of transactions being tracked in the queue.
func (uiq *unresolvedIntentQueue) assertPositiveRefCounts() {
	for _, txn := range uiq.txns {
		if txn.refCount <= 0 {
			panic(fmt.Sprintf("unexpected txn refcount %d for txn %+v in unresolvedIntentQueue",
				txn.refCount, txn))
		}
	}
}
