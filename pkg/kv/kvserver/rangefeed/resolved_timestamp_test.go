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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestUnresolvedIntentQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	uiq := makeUnresolvedIntentQueue()

	// Test empty queue.
	require.Equal(t, 0, uiq.Len())
	require.Nil(t, uiq.Oldest())
	require.Nil(t, uiq.Before(hlc.MinTimestamp))
	require.Nil(t, uiq.Before(hlc.MaxTimestamp))

	// Increment a non-existent txn.
	txn1 := uuid.MakeV4()
	txn1Key := roachpb.Key("key1")
	txn1TS := hlc.Timestamp{WallTime: 1}
	txn1MinTS := hlc.Timestamp{WallTime: 0, Logical: 4}
	adv := uiq.IncRef(txn1, txn1Key, txn1MinTS, txn1TS)
	require.False(t, adv)
	require.Equal(t, 1, uiq.Len())
	require.Equal(t, txn1, uiq.Oldest().txnID)
	require.Equal(t, txn1Key, uiq.Oldest().txnKey)
	require.Equal(t, txn1MinTS, uiq.Oldest().txnMinTimestamp)
	require.Equal(t, txn1TS, uiq.Oldest().timestamp)
	require.Equal(t, 1, uiq.Oldest().refCount)
	require.Equal(t, 0, len(uiq.Before(hlc.Timestamp{WallTime: 0})))
	require.Equal(t, 0, len(uiq.Before(hlc.Timestamp{WallTime: 1})))
	require.Equal(t, 1, len(uiq.Before(hlc.Timestamp{WallTime: 2})))
	require.NotPanics(t, func() { uiq.assertOnlyPositiveRefCounts() })

	// Decrement a non-existent txn.
	txn2 := uuid.MakeV4()
	txn2TS := hlc.Timestamp{WallTime: 3}
	adv = uiq.DecrRef(txn2, txn2TS)
	require.False(t, adv)
	require.Equal(t, 2, uiq.Len())
	require.Equal(t, txn1, uiq.Oldest().txnID)
	require.Equal(t, 0, len(uiq.Before(hlc.Timestamp{WallTime: 1})))
	require.Equal(t, 1, len(uiq.Before(hlc.Timestamp{WallTime: 3})))
	require.Equal(t, 2, len(uiq.Before(hlc.Timestamp{WallTime: 5})))
	require.Panics(t, func() { uiq.assertOnlyPositiveRefCounts() })

	// Update a non-existent txn.
	txn3 := uuid.MakeV4()
	adv = uiq.UpdateTS(txn3, hlc.Timestamp{WallTime: 5})
	require.False(t, adv)
	require.Equal(t, 2, uiq.Len())

	// Delete a non-existent txn.
	txn4 := uuid.MakeV4()
	adv = uiq.Del(txn4)
	require.False(t, adv)
	require.Equal(t, 2, uiq.Len())

	// Update txn1 with a smaller timestamp.
	adv = uiq.UpdateTS(txn1, hlc.Timestamp{WallTime: 0})
	require.False(t, adv)
	require.Equal(t, 2, uiq.Len())
	require.Equal(t, txn1, uiq.Oldest().txnID)
	require.Equal(t, txn1TS, uiq.Oldest().timestamp)

	// Update txn1 with an equal timestamp.
	adv = uiq.UpdateTS(txn1, hlc.Timestamp{WallTime: 1})
	require.False(t, adv)
	require.Equal(t, 2, uiq.Len())
	require.Equal(t, txn1, uiq.Oldest().txnID)
	require.Equal(t, txn1TS, uiq.Oldest().timestamp)

	// Update txn1 with a larger timestamp. Should advance.
	newTxn1TS := hlc.Timestamp{WallTime: 2}
	adv = uiq.UpdateTS(txn1, newTxn1TS)
	require.True(t, adv)
	require.Equal(t, 2, uiq.Len())
	require.Equal(t, txn1, uiq.Oldest().txnID)
	require.Equal(t, newTxn1TS, uiq.Oldest().timestamp)
	require.Equal(t, 0, len(uiq.Before(hlc.Timestamp{WallTime: 2})))
	require.Equal(t, 1, len(uiq.Before(hlc.Timestamp{WallTime: 3})))
	require.Equal(t, 2, len(uiq.Before(hlc.Timestamp{WallTime: 4})))

	// Update txn1 with a much larger timestamp. txn2 new oldest.
	newTxn1TS = hlc.Timestamp{WallTime: 4}
	adv = uiq.UpdateTS(txn1, newTxn1TS)
	require.True(t, adv)
	require.Equal(t, 2, uiq.Len())
	require.Equal(t, txn2, uiq.Oldest().txnID)
	require.Equal(t, txn2TS, uiq.Oldest().timestamp)
	// Negative ref count ok. Indicates a re-ordering of events, which is
	// expected during the catch up scan and supported by the queue.
	require.Equal(t, -1, uiq.Oldest().refCount)
	require.Equal(t, 0, len(uiq.Before(hlc.Timestamp{WallTime: 2})))
	require.Equal(t, 0, len(uiq.Before(hlc.Timestamp{WallTime: 3})))
	require.Equal(t, 1, len(uiq.Before(hlc.Timestamp{WallTime: 4})))
	require.Equal(t, 2, len(uiq.Before(hlc.Timestamp{WallTime: 5})))

	// Increase txn1's ref count while increasing timestamp.
	newTxn1TS = hlc.Timestamp{WallTime: 5}
	adv = uiq.IncRef(txn1, txn1Key, txn1MinTS, newTxn1TS)
	require.False(t, adv)
	require.Equal(t, 2, uiq.Len())
	require.Equal(t, 2, uiq.txns[txn1].refCount)
	require.Equal(t, newTxn1TS, uiq.txns[txn1].timestamp)

	// Decrease txn1's ref count while increasing timestamp.
	newTxn1TS = hlc.Timestamp{WallTime: 6}
	adv = uiq.DecrRef(txn1, newTxn1TS)
	require.False(t, adv)
	require.Equal(t, 2, uiq.Len())
	require.Equal(t, 1, uiq.txns[txn1].refCount)
	require.Equal(t, newTxn1TS, uiq.txns[txn1].timestamp)

	// Add new txn at much higher timestamp. Immediately delete.
	txn5 := uuid.MakeV4()
	txn5TS := hlc.Timestamp{WallTime: 10}
	adv = uiq.IncRef(txn5, nil, txn5TS, txn5TS)
	require.False(t, adv)
	require.Equal(t, 3, uiq.Len())
	require.Equal(t, txn2, uiq.Oldest().txnID)
	adv = uiq.Del(txn5)
	require.False(t, adv)
	require.Equal(t, 2, uiq.Len())

	// Increase txn2's ref count, which results in deletion. txn1 new oldest.
	adv = uiq.IncRef(txn2, nil, txn2TS, txn2TS)
	require.True(t, adv)
	require.Equal(t, 1, uiq.Len())
	require.Equal(t, txn1, uiq.Oldest().txnID)
	require.Equal(t, newTxn1TS, uiq.Oldest().timestamp)
	require.NotPanics(t, func() { uiq.assertOnlyPositiveRefCounts() })

	// Decrease txn1's ref count. Should be empty again.
	adv = uiq.DecrRef(txn1, hlc.Timestamp{})
	require.True(t, adv)
	require.Equal(t, 0, uiq.Len())

	// Add new txn. Immediately decrement ref count. Should be empty again.
	txn6 := uuid.MakeV4()
	txn6TS := hlc.Timestamp{WallTime: 20}
	adv = uiq.IncRef(txn6, nil, txn6TS, txn6TS)
	require.False(t, adv)
	require.Equal(t, 1, uiq.Len())
	require.Equal(t, txn6, uiq.Oldest().txnID)
	adv = uiq.DecrRef(txn6, hlc.Timestamp{})
	require.True(t, adv)
	require.Equal(t, 0, uiq.Len())

	// Instruct the queue to disallow negative ref counts.
	uiq.AllowNegRefCount(false)

	// Decrease txn1's ref count. Should ignore because ref count
	// would go negative.
	adv = uiq.DecrRef(txn1, hlc.Timestamp{})
	require.False(t, adv)
	require.Equal(t, 0, uiq.Len())
}

func TestResolvedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rts := makeResolvedTimestamp()
	rts.Init()

	// Test empty resolved timestamp.
	require.Equal(t, hlc.Timestamp{}, rts.Get())

	// Add an intent. No closed timestamp so no resolved timestamp.
	txn1 := uuid.MakeV4()
	fwd := rts.ConsumeLogicalOp(writeIntentOp(txn1, hlc.Timestamp{WallTime: 10}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{}, rts.Get())

	// Add another intent. No closed timestamp so no resolved timestamp.
	txn2 := uuid.MakeV4()
	fwd = rts.ConsumeLogicalOp(writeIntentOp(txn2, hlc.Timestamp{WallTime: 12}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{}, rts.Get())

	// Set a closed timestamp. Resolved timestamp advances.
	fwd = rts.ForwardClosedTS(hlc.Timestamp{WallTime: 5})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 5}, rts.Get())

	// Write intent at earlier timestamp. Assertion failure.
	require.Panics(t, func() {
		rts.ConsumeLogicalOp(writeIntentOp(uuid.MakeV4(), hlc.Timestamp{WallTime: 3}))
	})

	// Write value at earlier timestamp. Assertion failure.
	require.Panics(t, func() {
		rts.ConsumeLogicalOp(writeValueOp(hlc.Timestamp{WallTime: 4}))
	})

	// Write value at later timestamp. No effect on resolved timestamp.
	fwd = rts.ConsumeLogicalOp(writeValueOp(hlc.Timestamp{WallTime: 6}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 5}, rts.Get())

	// Forward closed timestamp. Resolved timestamp advances to the timestamp of
	// the earliest intent.
	fwd = rts.ForwardClosedTS(hlc.Timestamp{WallTime: 15})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 9}, rts.Get())

	// Update the timestamp of txn2. No effect on the resolved timestamp.
	fwd = rts.ConsumeLogicalOp(updateIntentOp(txn2, hlc.Timestamp{WallTime: 18}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 9}, rts.Get())

	// Update the timestamp of txn1. Resolved timestamp moves forward.
	fwd = rts.ConsumeLogicalOp(updateIntentOp(txn1, hlc.Timestamp{WallTime: 20}))
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 15}, rts.Get())

	// Forward closed timestamp to same time as earliest intent.
	fwd = rts.ForwardClosedTS(hlc.Timestamp{WallTime: 18})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 17}, rts.Get())

	// Write intent for earliest txn at same timestamp. No change.
	fwd = rts.ConsumeLogicalOp(writeIntentOp(txn2, hlc.Timestamp{WallTime: 18}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 17}, rts.Get())

	// Write intent for earliest txn at later timestamp. Resolved
	// timestamp moves forward.
	fwd = rts.ConsumeLogicalOp(writeIntentOp(txn2, hlc.Timestamp{WallTime: 25}))
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 18}, rts.Get())

	// Forward closed timestamp. Resolved timestamp moves to earliest intent.
	fwd = rts.ForwardClosedTS(hlc.Timestamp{WallTime: 30})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 19}, rts.Get())

	// First transaction aborted. Resolved timestamp moves to next earliest
	// intent.
	fwd = rts.ConsumeLogicalOp(abortTxnOp(txn1))
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 24}, rts.Get())
	fwd = rts.ConsumeLogicalOp(abortIntentOp(txn1))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 24}, rts.Get())

	// Third transaction at higher timestamp. No effect.
	txn3 := uuid.MakeV4()
	fwd = rts.ConsumeLogicalOp(writeIntentOp(txn3, hlc.Timestamp{WallTime: 30}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 24}, rts.Get())
	fwd = rts.ConsumeLogicalOp(writeIntentOp(txn3, hlc.Timestamp{WallTime: 31}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 24}, rts.Get())

	// Third transaction aborted. No effect.
	fwd = rts.ConsumeLogicalOp(abortTxnOp(txn3))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 24}, rts.Get())
	fwd = rts.ConsumeLogicalOp(abortIntentOp(txn3))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 24}, rts.Get())
	fwd = rts.ConsumeLogicalOp(abortIntentOp(txn3))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 24}, rts.Get())

	// Fourth transaction at higher timestamp. No effect.
	txn4 := uuid.MakeV4()
	fwd = rts.ConsumeLogicalOp(writeIntentOp(txn4, hlc.Timestamp{WallTime: 45}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 24}, rts.Get())

	// Fourth transaction committed. No effect.
	fwd = rts.ConsumeLogicalOp(commitIntentOp(txn4, hlc.Timestamp{WallTime: 45}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 24}, rts.Get())

	// Second transaction observes one intent being resolved at timestamp
	// above closed time. Resolved timestamp moves to closed timestamp.
	fwd = rts.ConsumeLogicalOp(commitIntentOp(txn2, hlc.Timestamp{WallTime: 35}))
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 30}, rts.Get())

	// Forward closed timestamp. Resolved timestamp moves to earliest intent.
	fwd = rts.ForwardClosedTS(hlc.Timestamp{WallTime: 40})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 34}, rts.Get())

	// Second transaction observes another intent being resolved at timestamp
	// below closed time. Still one intent left.
	fwd = rts.ConsumeLogicalOp(commitIntentOp(txn2, hlc.Timestamp{WallTime: 35}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 34}, rts.Get())

	// Second transaction observes final intent being resolved at timestamp
	// below closed time. Resolved timestamp moves to closed timestamp.
	fwd = rts.ConsumeLogicalOp(commitIntentOp(txn2, hlc.Timestamp{WallTime: 35}))
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 40}, rts.Get())

	// Fifth transaction at higher timestamp. No effect.
	txn5 := uuid.MakeV4()
	fwd = rts.ConsumeLogicalOp(writeIntentOp(txn5, hlc.Timestamp{WallTime: 45}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 40}, rts.Get())
	fwd = rts.ConsumeLogicalOp(writeIntentOp(txn5, hlc.Timestamp{WallTime: 46}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 40}, rts.Get())

	// Forward closed timestamp. Resolved timestamp moves to earliest intent.
	fwd = rts.ForwardClosedTS(hlc.Timestamp{WallTime: 50})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 45}, rts.Get())

	// Fifth transaction bumps epoch and re-writes one of its intents. Resolved
	// timestamp moves to the new transaction timestamp.
	fwd = rts.ConsumeLogicalOp(updateIntentOp(txn5, hlc.Timestamp{WallTime: 47}))
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 46}, rts.Get())

	// Fifth transaction committed, but only one of its intents was written in
	// its final epoch. Resolved timestamp moves forward after observing the
	// first intent committing at a higher timestamp and moves to the closed
	// timestamp after observing the second intent aborting.
	fwd = rts.ConsumeLogicalOp(commitIntentOp(txn5, hlc.Timestamp{WallTime: 49}))
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 48}, rts.Get())
	fwd = rts.ConsumeLogicalOp(abortIntentOp(txn5))
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 50}, rts.Get())
}

func TestResolvedTimestampNoClosedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rts := makeResolvedTimestamp()
	rts.Init()

	// Add a value. No closed timestamp so no resolved timestamp.
	fwd := rts.ConsumeLogicalOp(writeValueOp(hlc.Timestamp{WallTime: 1}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{}, rts.Get())

	// Add an intent. No closed timestamp so no resolved timestamp.
	txn1 := uuid.MakeV4()
	fwd = rts.ConsumeLogicalOp(writeIntentOp(txn1, hlc.Timestamp{WallTime: 1}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{}, rts.Get())

	// Update intent. No closed timestamp so no resolved timestamp.
	fwd = rts.ConsumeLogicalOp(updateIntentOp(txn1, hlc.Timestamp{WallTime: 2}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{}, rts.Get())

	// Add another intent. No closed timestamp so no resolved timestamp.
	txn2 := uuid.MakeV4()
	fwd = rts.ConsumeLogicalOp(writeIntentOp(txn2, hlc.Timestamp{WallTime: 3}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{}, rts.Get())

	// Abort the first intent. No closed timestamp so no resolved timestamp.
	fwd = rts.ConsumeLogicalOp(abortIntentOp(txn1))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{}, rts.Get())

	// Commit the second intent. No closed timestamp so no resolved timestamp.
	fwd = rts.ConsumeLogicalOp(commitIntentOp(txn2, hlc.Timestamp{WallTime: 3}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{}, rts.Get())
}

func TestResolvedTimestampNoIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rts := makeResolvedTimestamp()
	rts.Init()

	// Set a closed timestamp. Resolved timestamp advances.
	fwd := rts.ForwardClosedTS(hlc.Timestamp{WallTime: 1})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 1}, rts.Get())

	// Forward closed timestamp. Resolved timestamp advances.
	fwd = rts.ForwardClosedTS(hlc.Timestamp{WallTime: 3})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 3}, rts.Get())

	// Smaller closed timestamp. Resolved timestamp does not advance.
	fwd = rts.ForwardClosedTS(hlc.Timestamp{WallTime: 2})
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 3}, rts.Get())

	// Equal closed timestamp. Resolved timestamp does not advance.
	fwd = rts.ForwardClosedTS(hlc.Timestamp{WallTime: 3})
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 3}, rts.Get())

	// Forward closed timestamp. Resolved timestamp advances.
	fwd = rts.ForwardClosedTS(hlc.Timestamp{WallTime: 4})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 4}, rts.Get())
}

func TestResolvedTimestampInit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("CT Before Init", func(t *testing.T) {
		rts := makeResolvedTimestamp()

		// Set a closed timestamp. Not initialized so no resolved timestamp.
		fwd := rts.ForwardClosedTS(hlc.Timestamp{WallTime: 5})
		require.False(t, fwd)
		require.Equal(t, hlc.Timestamp{}, rts.Get())

		// Init. Resolved timestamp moves to closed timestamp.
		fwd = rts.Init()
		require.True(t, fwd)
		require.Equal(t, hlc.Timestamp{WallTime: 5}, rts.Get())
	})
	t.Run("No CT Before Init", func(t *testing.T) {
		rts := makeResolvedTimestamp()

		// Add an intent. Not initialized so no resolved timestamp.
		txn1 := uuid.MakeV4()
		fwd := rts.ConsumeLogicalOp(writeIntentOp(txn1, hlc.Timestamp{WallTime: 3}))
		require.False(t, fwd)
		require.Equal(t, hlc.Timestamp{}, rts.Get())

		// Init. Resolved timestamp undefined.
		fwd = rts.Init()
		require.False(t, fwd)
		require.Equal(t, hlc.Timestamp{}, rts.Get())
	})
	t.Run("Write Before Init", func(t *testing.T) {
		rts := makeResolvedTimestamp()

		// Add an intent. Not initialized so no resolved timestamp.
		txn1 := uuid.MakeV4()
		fwd := rts.ConsumeLogicalOp(writeIntentOp(txn1, hlc.Timestamp{WallTime: 3}))
		require.False(t, fwd)
		require.Equal(t, hlc.Timestamp{}, rts.Get())

		// Set a closed timestamp. Not initialized so no resolved timestamp.
		fwd = rts.ForwardClosedTS(hlc.Timestamp{WallTime: 5})
		require.False(t, fwd)
		require.Equal(t, hlc.Timestamp{}, rts.Get())

		// Init. Resolved timestamp moves below first unresolved intent.
		fwd = rts.Init()
		require.True(t, fwd)
		require.Equal(t, hlc.Timestamp{WallTime: 2}, rts.Get())
	})
	t.Run("Abort + Write Before Init", func(t *testing.T) {
		rts := makeResolvedTimestamp()

		// Abort an intent. Not initialized so no resolved timestamp.
		txn1 := uuid.MakeV4()
		fwd := rts.ConsumeLogicalOp(abortIntentOp(txn1))
		require.False(t, fwd)
		require.Equal(t, hlc.Timestamp{}, rts.Get())

		// Abort that intent's transaction. Not initialized so no-op.
		fwd = rts.ConsumeLogicalOp(abortTxnOp(txn1))
		require.False(t, fwd)
		require.Equal(t, hlc.Timestamp{}, rts.Get())

		// Later, write an intent for the same transaction. This should cancel
		// out with the out-of-order intent abort operation. If this abort hadn't
		// allowed the unresolvedTxn's ref count to drop below 0, this would
		// have created a reference that would never be cleaned up.
		fwd = rts.ConsumeLogicalOp(writeIntentOp(txn1, hlc.Timestamp{WallTime: 3}))
		require.False(t, fwd)
		require.Equal(t, hlc.Timestamp{}, rts.Get())

		// Set a closed timestamp. Not initialized so no resolved timestamp.
		fwd = rts.ForwardClosedTS(hlc.Timestamp{WallTime: 5})
		require.False(t, fwd)
		require.Equal(t, hlc.Timestamp{}, rts.Get())

		// Init. Resolved timestamp moves to closed timestamp.
		fwd = rts.Init()
		require.True(t, fwd)
		require.Equal(t, hlc.Timestamp{WallTime: 5}, rts.Get())
	})
	t.Run("Abort Before Init, No Write", func(t *testing.T) {
		rts := makeResolvedTimestamp()

		// Abort an intent. Not initialized so no resolved timestamp.
		txn1 := uuid.MakeV4()
		fwd := rts.ConsumeLogicalOp(abortIntentOp(txn1))
		require.False(t, fwd)
		require.Equal(t, hlc.Timestamp{}, rts.Get())

		// Init. Negative txn ref count causes panic. Init should not have
		// been called because an intent must not have been accounted for.
		require.Panics(t, func() { rts.Init() })
	})
}

func TestResolvedTimestampTxnAborted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rts := makeResolvedTimestamp()
	rts.Init()

	// Set a closed timestamp. Resolved timestamp advances.
	fwd := rts.ForwardClosedTS(hlc.Timestamp{WallTime: 5})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 5}, rts.Get())

	// Add an intent for a new transaction.
	txn1 := uuid.MakeV4()
	fwd = rts.ConsumeLogicalOp(writeIntentOp(txn1, hlc.Timestamp{WallTime: 10}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 5}, rts.Get())

	// Set a new closed timestamp. Resolved timestamp advances.
	fwd = rts.ForwardClosedTS(hlc.Timestamp{WallTime: 15})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 9}, rts.Get())

	// Abort txn1 after a periodic txn push. Resolved timestamp advances.
	fwd = rts.ConsumeLogicalOp(abortTxnOp(txn1))
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 15}, rts.Get())

	// Update one of txn1's intents. Should be ignored.
	fwd = rts.ConsumeLogicalOp(updateIntentOp(txn1, hlc.Timestamp{WallTime: 20}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 15}, rts.Get())

	// Abort one of txn1's intents. Should be ignored.
	fwd = rts.ConsumeLogicalOp(abortIntentOp(txn1))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 15}, rts.Get())

	// Write another intent as txn1. Should add txn1 back into queue.
	// This will eventually require another txn push to evict.
	fwd = rts.ConsumeLogicalOp(writeIntentOp(txn1, hlc.Timestamp{WallTime: 20}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 15}, rts.Get())

	// Set a new closed timestamp. Resolved timestamp advances, but only up to
	// the timestamp of txn1's intent, which we fail remember is uncommittable.
	fwd = rts.ForwardClosedTS(hlc.Timestamp{WallTime: 25})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 19}, rts.Get())

	// Abort txn1 again after another periodic push. Resolved timestamp advances.
	fwd = rts.ConsumeLogicalOp(abortTxnOp(txn1))
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 25}, rts.Get())
}

// Test that things go well when the closed timestamp has non-zero logical part.
func TestClosedTimestampLogicalPart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rts := makeResolvedTimestamp()
	rts.Init()

	// Set a new closed timestamp. Resolved timestamp advances.
	fwd := rts.ForwardClosedTS(hlc.Timestamp{WallTime: 10, Logical: 2})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 10, Logical: 0}, rts.Get())

	// Add an intent for a new transaction.
	txn1 := uuid.MakeV4()
	fwd = rts.ConsumeLogicalOp(writeIntentOp(txn1, hlc.Timestamp{WallTime: 10, Logical: 4}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 10, Logical: 0}, rts.Get())

	// Set a new closed timestamp. Resolved timestamp doesn't advance, since it
	// could only theoretically advance up to 10.4, and it doesn't do logical
	// parts.
	fwd = rts.ForwardClosedTS(hlc.Timestamp{WallTime: 11, Logical: 6})
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 10, Logical: 0}, rts.Get())

	// Abort txn1. Resolved timestamp advances.
	fwd = rts.ConsumeLogicalOp(abortTxnOp(txn1))
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 11, Logical: 0}, rts.Get())

	// Create an intent one tick above the closed ts. Resolved timestamp doesn't
	// advance. This tests the case where the closed timestamp has a logical part,
	// and an intent is in the next wall tick; this used to cause an issue because
	// of the rounding logic.
	txn2 := uuid.MakeV4()
	fwd = rts.ConsumeLogicalOp(writeIntentOp(txn2, hlc.Timestamp{WallTime: 12, Logical: 7}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 11, Logical: 0}, rts.Get())
}
