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
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestGCIterator(t *testing.T) {
	type dataItem struct {
		engine.MVCCKeyValue
		txn *roachpb.Transaction
	}
	makeDataItem := func(k roachpb.Key, val []byte, ts int64, txn *roachpb.Transaction) dataItem {
		return dataItem{
			MVCCKeyValue: engine.MVCCKeyValue{
				Key: engine.MVCCKey{
					Key:       k,
					Timestamp: hlc.Timestamp{WallTime: ts * time.Nanosecond.Nanoseconds()},
				},
				Value: val,
			},
			txn: txn,
		}
	}
	makeLiteralDataDistribution := func(items ...dataItem) dataDistribution {
		return func() (engine.MVCCKeyValue, *roachpb.Transaction, bool) {
			if len(items) == 0 {
				return engine.MVCCKeyValue{}, nil, false
			}
			item := items[0]
			defer func() { items = items[1:] }()
			return item.MVCCKeyValue, item.txn, true
		}
	}
	type stateExpectations struct {
		cur, next, afterNext int
		isNewest             bool
		isIntent             bool
		isNotValue           bool
	}
	isNil := -1
	isMD := -2
	exp := func(cur, next, afterNext int, isNewest, isIntent, isNotValue bool) stateExpectations {
		return stateExpectations{
			cur, next, afterNext, isNewest, isIntent, isNotValue,
		}
	}
	di := makeDataItem
	vals := uniformValueDistribution(3, 5, 0, rand.New(rand.NewSource(1)))
	tablePrefix := keys.MakeTablePrefix(42)
	desc := roachpb.RangeDescriptor{StartKey: tablePrefix, EndKey: roachpb.RKey(roachpb.Key(tablePrefix).PrefixEnd())}
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

	checkExpectations := func(t *testing.T, data []dataItem, ex stateExpectations, s gcIteratorState) {
		check := func(ex int, kv *engine.MVCCKeyValue) {
			switch {
			case ex >= 0:
				require.EqualValues(t, &data[ex].MVCCKeyValue, kv)
			case ex == isNil:
				require.Nil(t, kv)
			case ex == isMD:
				require.False(t, kv.Key.IsValue())
			}
		}
		check(ex.cur, s.cur)
		check(ex.next, s.next)
		check(ex.afterNext, s.afterNext)
		require.Equal(t, ex.isNewest, s.curIsNewest())
		require.Equal(t, ex.isIntent, s.curIsIntent())
	}
	type testCase struct {
		name         string
		data         []dataItem
		expectations []stateExpectations
	}
	makeTest := func(tc testCase) func(t *testing.T) {
		return func(t *testing.T) {
			eng := engine.NewDefaultInMem()
			ds := makeLiteralDataDistribution(tc.data...)
			ds.setupTest(t, eng, desc)
			snap := eng.NewSnapshot()
			defer snap.Close()
			it := makeGCIterator(&desc, snap)
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
				exp(4, isMD, isNil, false, true, false),
				exp(isMD, isNil, isNil, false, false, true),
				exp(3, isNil, isNil, true, false, false),
				exp(0, 1, 2, false, false, false),
				exp(1, 2, isNil, false, false, false),
				exp(2, isNil, isNil, true, false, false),
			},
		},
	} {
		t.Run(tc.name, makeTest(tc))
	}
}
