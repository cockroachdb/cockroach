// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gc

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestGCIterator exercises the GC iterator by writing data to the underlying
// engine and then validating the state of the iterator as it iterates that
// data.
func TestGCIterator(t *testing.T) {
	// dataItem represents a version in the storage engine and optionally a
	// corresponding transaction which will make the MVCCKeyValue an intent.
	type dataItem struct {
		kv  storage.MVCCKeyValue
		txn *roachpb.Transaction
		kvR storage.MVCCRangeKey
	}
	makeTS := func(ts int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: ts * time.Nanosecond.Nanoseconds()}
	}
	// makeDataItem is a shorthand to construct dataItems.
	makeDataItem := func(k roachpb.Key, val []byte, ts int64, txn *roachpb.Transaction) dataItem {
		return dataItem{
			kv: storage.MVCCKeyValue{
				Key: storage.MVCCKey{
					Key:       k,
					Timestamp: makeTS(ts),
				},
				Value: val,
			},
			txn: txn,
		}
	}
	makeRangeTombstone := func(start, end roachpb.Key, ts int64) dataItem {
		return dataItem{
			kvR: storage.MVCCRangeKey{
				StartKey:  start,
				EndKey:    end,
				Timestamp: makeTS(ts),
			},
		}
	}
	// makeLiteralDistribution adapts dataItems for use with the data distribution
	// infrastructure.
	makeLiteralDataDistribution := func(items ...dataItem) dataDistribution {
		return func() (storage.MVCCKeyValue, storage.MVCCRangeKey, *roachpb.Transaction, bool) {
			if len(items) == 0 {
				return storage.MVCCKeyValue{}, storage.MVCCRangeKey{}, nil, false
			}
			item := items[0]
			defer func() { items = items[1:] }()
			return item.kv, item.kvR, item.txn, true
		}
	}
	// stateExpectations are expectations about the state of the iterator.
	type stateExpectations struct {
		cur, next, afterNext int
		isNewest             bool
		isIntent             bool
		isNotValue           bool
		tombstoneTS          hlc.Timestamp
	}
	// notation to mark that an iterator state element as either nil or metadata.
	const (
		isNil = -1
		isMD  = -2
	)
	// exp is a shorthand to construct state expectations.
	exp := func(cur, next, afterNext int, isNewest, isIntent, isNotValue bool,
		tombstoneTS hlc.Timestamp,
	) stateExpectations {
		return stateExpectations{
			cur: cur, next: next, afterNext: afterNext,
			isNewest:    isNewest,
			isIntent:    isIntent,
			isNotValue:  isNotValue,
			tombstoneTS: tombstoneTS,
		}
	}
	vals := uniformValueDistribution(3, 5, 0, rand.New(rand.NewSource(1)))
	tablePrefix := keys.SystemSQLCodec.TablePrefix(42)
	desc := roachpb.RangeDescriptor{
		StartKey: roachpb.RKey(tablePrefix),
		EndKey:   roachpb.RKey(tablePrefix.PrefixEnd()),
	}
	keyA := append(tablePrefix[0:len(tablePrefix):len(tablePrefix)], 'a')
	keyB := append(tablePrefix[0:len(tablePrefix):len(tablePrefix)], 'b')
	keyC := append(tablePrefix[0:len(tablePrefix):len(tablePrefix)], 'c')
	makeTxn := func() *roachpb.Transaction {
		txn := roachpb.Transaction{}
		txn.Key = keyA
		txn.ID = uuid.MakeV4()
		txn.Status = roachpb.PENDING
		return &txn
	}

	type testCase struct {
		name         string
		data         []dataItem
		expectations []stateExpectations
		gcThreshold  hlc.Timestamp
	}
	// checkExpectations tests whether the state of the iterator matches the
	// expectation.
	checkExpectations := func(
		t *testing.T, data []dataItem, ex stateExpectations, s gcIteratorState,
	) {
		check := func(ex int, role string, kv *storage.MVCCKeyValue) {
			switch {
			case ex >= 0:
				require.EqualValues(t, &data[ex].kv, kv, "unexpected data for %s at index %d", role, ex)
			case ex == isNil:
				require.Nil(t, kv)
			case ex == isMD:
				require.False(t, kv.Key.IsValue())
			}
		}
		check(ex.cur, "cur", s.cur)
		check(ex.next, "next", s.next)
		check(ex.afterNext, "after", s.afterNext)
		require.Equal(t, ex.tombstoneTS, s.lastTombstone, "unexpected last tombstone timestamp")
		require.Equal(t, ex.isNewest, s.curIsNewest())
		require.Equal(t, ex.isIntent, s.curIsIntent())
	}
	makeTest := func(tc testCase) func(t *testing.T) {
		return func(t *testing.T) {
			eng := storage.NewDefaultInMemForTesting()
			defer eng.Close()
			ds := makeLiteralDataDistribution(tc.data...)
			ds.setupTest(t, eng, desc)
			snap := eng.NewSnapshot()
			defer snap.Close()
			it := makeGCIterator(&desc, snap, tc.gcThreshold)
			defer it.close()
			expectations := tc.expectations
			for i, ex := range expectations {
				t.Run(fmt.Sprint(i), func(t *testing.T) {
					s, ok := it.state()
					require.True(t, ok)
					checkExpectations(t, tc.data, ex, s)
				})
				it.step()
			}
		}
	}
	noTS := hlc.Timestamp{}
	// shorthands for convenient notation
	di := makeDataItem
	rts := makeRangeTombstone
	for _, tc := range []testCase{
		{
			name: "basic",
			data: []dataItem{
				di(keyA, vals(), 2, nil),
				di(keyA, vals(), 11, nil),
				di(keyA, vals(), 14, nil),
				di(keyB, vals(), 3, nil),
				di(keyC, vals(), 7, makeTxn()),
			},
			expectations: []stateExpectations{
				exp(4, isMD, isNil, false, true, false, noTS),
				exp(isMD, isNil, isNil, false, false, true, noTS),
				exp(3, isNil, isNil, true, false, false, noTS),
				exp(0, 1, 2, false, false, false, noTS),
				exp(1, 2, isNil, false, false, false, noTS),
				exp(2, isNil, isNil, true, false, false, noTS),
			},
		},
		{
			name: "range tombstones range ts in future",
			data: []dataItem{
				di(keyA, vals(), 2, nil),       // 0
				di(keyB, vals(), 3, nil),       // 1
				di(keyC, vals(), 7, makeTxn()), // 2
				rts(keyA, keyB, 10),            // -
				di(keyA, vals(), 11, nil),      // 4
				di(keyA, vals(), 14, nil),      // 5
			},
			expectations: []stateExpectations{
				exp(2, isMD, isNil, false, true, false, noTS),
				exp(isMD, isNil, isNil, false, false, true, noTS),
				exp(1, isNil, isNil, true, false, false, noTS),
				exp(0, 4, 5, false, false, false, noTS),
				exp(4, 5, isNil, false, false, false, noTS),
				exp(5, isNil, isNil, true, false, false, noTS),
			},
			gcThreshold: makeTS(7),
		},
		{
			name: "range tombstones with ts",
			data: []dataItem{
				di(keyA, vals(), 2, nil),       // 0
				di(keyB, vals(), 3, nil),       // 1
				di(keyC, vals(), 7, makeTxn()), // 2
				rts(keyA, keyB, 10),            // -
				di(keyA, vals(), 11, nil),      // 4
				di(keyA, vals(), 14, nil),      // 5
			},
			expectations: []stateExpectations{
				exp(2, isMD, isNil, false, true, false, noTS),
				exp(isMD, isNil, isNil, false, false, true, noTS),
				exp(1, isNil, isNil, true, false, false, noTS),
				exp(0, 4, 5, false, false, false, makeTS(10)),
				exp(4, 5, isNil, false, false, false, makeTS(10)),
				exp(5, isNil, isNil, true, false, false, makeTS(10)),
			},
			gcThreshold: makeTS(10),
		},
		{
			name: "multiple range tombstones",
			data: []dataItem{
				rts(keyA, keyB, 1),             // -
				di(keyA, vals(), 2, nil),       // 1
				di(keyB, vals(), 3, nil),       // 2
				rts(keyA, keyB, 7),             // -
				di(keyC, vals(), 7, makeTxn()), // 4
				rts(keyA, keyC, 10),            // -
				di(keyA, vals(), 11, nil),      // 6
				di(keyA, vals(), 14, nil),      // 7
			},
			expectations: []stateExpectations{
				exp(4, isMD, isNil, false, true, false, noTS),
				exp(isMD, isNil, isNil, false, false, true, noTS),
				exp(2, isNil, isNil, true, false, false, noTS),
				exp(1, 6, 7, false, false, false, makeTS(7)),
				exp(6, 7, isNil, false, false, false, makeTS(7)),
				exp(7, isNil, isNil, true, false, false, makeTS(7)),
			},
			gcThreshold: makeTS(9),
		},
	} {
		t.Run(tc.name, makeTest(tc))
	}
}
