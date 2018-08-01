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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	adv := uiq.IncRef(txn1, txn1Key, txn1TS)
	require.False(t, adv)
	require.Equal(t, 1, uiq.Len())
	require.Equal(t, txn1, uiq.Oldest().txnID)
	require.Equal(t, txn1Key, uiq.Oldest().txnKey)
	require.Equal(t, txn1TS, uiq.Oldest().timestamp)
	require.Equal(t, 1, uiq.Oldest().refCount)
	require.Equal(t, 0, len(uiq.Before(hlc.Timestamp{WallTime: 0})))
	require.Equal(t, 0, len(uiq.Before(hlc.Timestamp{WallTime: 1})))
	require.Equal(t, 1, len(uiq.Before(hlc.Timestamp{WallTime: 2})))

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
	adv = uiq.IncRef(txn1, nil, newTxn1TS)
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
	adv = uiq.IncRef(txn5, nil, hlc.Timestamp{WallTime: 10})
	require.False(t, adv)
	require.Equal(t, 3, uiq.Len())
	require.Equal(t, txn2, uiq.Oldest().txnID)
	adv = uiq.Del(txn5)
	require.False(t, adv)
	require.Equal(t, 2, uiq.Len())

	// Delete txn2. txn1 new oldest.
	adv = uiq.Del(txn2)
	require.True(t, adv)
	require.Equal(t, 1, uiq.Len())
	require.Equal(t, txn1, uiq.Oldest().txnID)
	require.Equal(t, newTxn1TS, uiq.Oldest().timestamp)

	// Decrease txn1's ref count. Should be empty again.
	adv = uiq.DecrRef(txn1, hlc.Timestamp{})
	require.True(t, adv)
	require.Equal(t, 0, uiq.Len())
}

func TestResolvedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rts := makeResolvedTimestamp()

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
	fwd = rts.ConsumeLogicalOp(abortIntentOp(txn1))
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 24}, rts.Get())

	// Third transaction at higher timestamp. No effect.
	txn3 := uuid.MakeV4()
	fwd = rts.ConsumeLogicalOp(writeIntentOp(txn3, hlc.Timestamp{WallTime: 40}))
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 24}, rts.Get())

	// Third transaction aborted. No effect.
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
}

func TestResolvedTimestampNoClosedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rts := makeResolvedTimestamp()

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
