// Copyright 2022 The Cockroach Authors.
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
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestCatchupScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var (
		testKey1 = roachpb.Key("/db1")
		testKey2 = roachpb.Key("/db2")

		testValue1 = []byte("val1")
		testValue2 = []byte("val2")
		testValue3 = []byte("val3")
		testValue4 = []byte("val4")

		ts1 = hlc.Timestamp{WallTime: 1, Logical: 0}
		ts2 = hlc.Timestamp{WallTime: 2, Logical: 0}
		ts3 = hlc.Timestamp{WallTime: 3, Logical: 0}
		ts4 = hlc.Timestamp{WallTime: 4, Logical: 0}
		ts5 = hlc.Timestamp{WallTime: 4, Logical: 0}
	)

	makeTxn := func(key roachpb.Key, val []byte, ts hlc.Timestamp,
	) (roachpb.Transaction, roachpb.Value) {
		txnID := uuid.MakeV4()
		txnMeta := enginepb.TxnMeta{
			Key:            key,
			ID:             txnID,
			Epoch:          1,
			WriteTimestamp: ts,
		}
		return roachpb.Transaction{
				TxnMeta:       txnMeta,
				ReadTimestamp: ts,
			}, roachpb.Value{
				RawBytes: val,
			}
	}

	makeKTV := func(key roachpb.Key, ts hlc.Timestamp, value []byte) storage.MVCCKeyValue {
		return storage.MVCCKeyValue{Key: storage.MVCCKey{Key: key, Timestamp: ts}, Value: value}
	}
	// testKey1 has an intent and provisional value that will be skipped. Both
	// testKey1 and testKey2 have a value that is older than what we need with
	// the catchup scan, but will be read if a diff is desired.
	kv1_1_1 := makeKTV(testKey1, ts1, testValue1)
	kv1_2_2 := makeKTV(testKey1, ts2, testValue2)
	kv1_3_3 := makeKTV(testKey1, ts3, testValue3)
	kv1_4_4 := makeKTV(testKey1, ts4, testValue4)
	txn, val := makeTxn(testKey1, testValue4, ts4)
	kv2_1_1 := makeKTV(testKey2, ts1, testValue1)
	kv2_2_2 := makeKTV(testKey2, ts2, testValue2)
	kv2_5_3 := makeKTV(testKey2, ts5, testValue3)

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	// Put with no intent.
	for _, kv := range []storage.MVCCKeyValue{kv1_1_1, kv1_2_2, kv1_3_3, kv2_1_1, kv2_2_2, kv2_5_3} {
		v := roachpb.Value{RawBytes: kv.Value}
		if err := storage.MVCCPut(ctx, eng, nil, kv.Key.Key, kv.Key.Timestamp, v, nil); err != nil {
			t.Fatal(err)
		}
	}
	// Put with an intent.
	if err := storage.MVCCPut(ctx, eng, nil, kv1_4_4.Key.Key, txn.ReadTimestamp, val, &txn); err != nil {
		t.Fatal(err)
	}
	testutils.RunTrueAndFalse(t, "useTBI", func(t *testing.T, useTBI bool) {
		testutils.RunTrueAndFalse(t, "withDiff", func(t *testing.T, withDiff bool) {
			iter := NewCatchUpIterator(eng, &roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					// Inclusive, so want everything >= ts2
					Timestamp: ts2,
				},
				Span: roachpb.Span{
					EndKey: roachpb.KeyMax,
				},
				WithDiff: withDiff,
			}, useTBI, nil)
			defer iter.Close()
			var events []roachpb.RangeFeedValue
			// ts1 here is exclusive, so we do not want the versions at ts1.
			require.NoError(t, iter.CatchUpScan(storage.MakeMVCCMetadataKey(testKey1),
				storage.MakeMVCCMetadataKey(roachpb.KeyMax), ts1, withDiff,
				func(e *roachpb.RangeFeedEvent) error {
					events = append(events, *e.Val)
					return nil
				}))
			require.Equal(t, 4, len(events))
			checkEquality := func(
				kv storage.MVCCKeyValue, prevKV storage.MVCCKeyValue, event roachpb.RangeFeedValue) {
				require.Equal(t, string(kv.Key.Key), string(event.Key))
				require.Equal(t, kv.Key.Timestamp, event.Value.Timestamp)
				require.Equal(t, string(kv.Value), string(event.Value.RawBytes))
				if withDiff {
					// TODO(sumeer): uncomment after clarifying CatchUpScan behavior.
					// require.Equal(t, prevKV.Key.Timestamp, event.PrevValue.Timestamp)
					require.Equal(t, string(prevKV.Value), string(event.PrevValue.RawBytes))
				} else {
					require.Equal(t, hlc.Timestamp{}, event.PrevValue.Timestamp)
					require.Equal(t, 0, len(event.PrevValue.RawBytes))
				}
			}
			checkEquality(kv1_2_2, kv1_1_1, events[0])
			checkEquality(kv1_3_3, kv1_2_2, events[1])
			checkEquality(kv2_2_2, kv2_1_1, events[2])
			checkEquality(kv2_5_3, kv2_2_2, events[3])
		})
	})
}

func TestCatchupScanSeesOldIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Regression test for [#85886]. When with-diff is specified, the iterator may
	// be positioned on an intent that is outside the time bounds. When we read
	// the intent and want to load the version, we must make sure to ignore time
	// bounds, or we'll see a wholly unrelated version.
	//
	// [#85886]: https://github.com/cockroachdb/cockroach/issues/85886

	ctx := context.Background()
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	// b -> version @ 1100 (visible)
	// d -> intent @ 990   (iterator will be positioned here because of with-diff option)
	// e -> version @ 1100
	tsCutoff := hlc.Timestamp{WallTime: 1000} // the lower bound of the catch-up scan
	tsIntent := tsCutoff.Add(-10, 0)          // the intent is below the lower bound
	tsVersionInWindow := tsCutoff.Add(10, 0)  // an unrelated version is above the lower bound

	require.NoError(t, storage.MVCCPut(ctx, eng, nil, roachpb.Key("b"),
		tsVersionInWindow, roachpb.MakeValueFromString("foo"), nil))

	txn := roachpb.MakeTransaction("foo", roachpb.Key("d"), roachpb.NormalUserPriority, tsIntent, 100, 0)
	require.NoError(t, storage.MVCCPut(ctx, eng, nil, roachpb.Key("d"),
		tsIntent, roachpb.MakeValueFromString("intent"), &txn))

	require.NoError(t, storage.MVCCPut(ctx, eng, nil, roachpb.Key("e"),
		tsVersionInWindow, roachpb.MakeValueFromString("bar"), nil))

	// Run a catchup scan across the span and watch it succeed.
	span := roachpb.Span{Key: keys.LocalMax, EndKey: keys.MaxKey}
	args := &roachpb.RangeFeedRequest{Span: span, WithDiff: true}
	args.Timestamp = tsCutoff
	iter := NewCatchUpIterator(eng, args, true /* useTBI */, nil)
	defer iter.Close()

	keys := map[string]struct{}{}
	require.NoError(t, iter.CatchUpScan(storage.MVCCKey{Key: span.Key}, storage.MVCCKey{Key: span.EndKey}, tsCutoff, true /* withDiff */, func(e *roachpb.RangeFeedEvent) error {
		keys[string(e.Val.Key)] = struct{}{}
		return nil
	}))
	require.Equal(t, map[string]struct{}{
		"b": {},
		"e": {},
	}, keys)
}
