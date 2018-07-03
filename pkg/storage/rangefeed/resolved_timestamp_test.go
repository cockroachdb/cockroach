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

	// WIP.
}
