// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

func iterateExpectErr(
	e Engine,
	iterFn func(*MVCCIncrementalIterator),
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	errString string,
) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()
		iter := NewMVCCIncrementalIterator(e, MVCCIncrementalIterOptions{
			IterOptions: IterOptions{
				UpperBound: endKey,
			},
			StartTime: startTime,
			EndTime:   endTime,
		})
		defer iter.Close()
		for iter.SeekGE(MakeMVCCMetadataKey(startKey)); ; iterFn(iter) {
			if ok, _ := iter.Valid(); !ok || iter.UnsafeKey().Key.Compare(endKey) >= 0 {
				break
			}
			// pass
		}
		if _, err := iter.Valid(); !testutils.IsError(err, errString) {
			t.Fatalf("expected error %q but got %v", errString, err)
		}
	}
}

func assertEqualKVs(
	e Engine,
	iterFn func(*MVCCIncrementalIterator),
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	expected []MVCCKeyValue,
) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()
		iter := NewMVCCIncrementalIterator(e, MVCCIncrementalIterOptions{
			IterOptions: IterOptions{
				UpperBound: endKey,
			},
			StartTime: startTime,
			EndTime:   endTime,
		})
		defer iter.Close()
		var kvs []MVCCKeyValue
		for iter.SeekGE(MakeMVCCMetadataKey(startKey)); ; iterFn(iter) {
			if ok, err := iter.Valid(); err != nil {
				t.Fatalf("unexpected error: %+v", err)
			} else if !ok || iter.UnsafeKey().Key.Compare(endKey) >= 0 {
				break
			}
			kvs = append(kvs, MVCCKeyValue{Key: iter.Key(), Value: iter.Value()})
		}

		if len(kvs) != len(expected) {
			t.Fatalf("got %d kvs but expected %d: %v", len(kvs), len(expected), kvs)
		}
		for i := range kvs {
			if !kvs[i].Key.Equal(expected[i].Key) {
				t.Fatalf("%d key: got %v but expected %v", i, kvs[i].Key, expected[i].Key)
			}
			if !bytes.Equal(kvs[i].Value, expected[i].Value) {
				t.Fatalf("%d value: got %x but expected %x", i, kvs[i].Value, expected[i].Value)
			}
		}
	}
}

func TestMVCCIncrementalIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	var (
		keyMin   = roachpb.KeyMin
		keyMax   = roachpb.KeyMax
		testKey1 = roachpb.Key("/db1")
		testKey2 = roachpb.Key("/db2")

		testValue1 = []byte("val1")
		testValue2 = []byte("val2")
		testValue3 = []byte("val3")
		testValue4 = []byte("val4")

		tsMin = hlc.Timestamp{WallTime: 0, Logical: 0}
		ts1   = hlc.Timestamp{WallTime: 1, Logical: 0}
		ts2   = hlc.Timestamp{WallTime: 2, Logical: 0}
		ts3   = hlc.Timestamp{WallTime: 3, Logical: 0}
		ts4   = hlc.Timestamp{WallTime: 4, Logical: 0}
		tsMax = hlc.Timestamp{WallTime: math.MaxInt64, Logical: 0}
	)

	makeKVT := func(key roachpb.Key, value []byte, ts hlc.Timestamp) MVCCKeyValue {
		return MVCCKeyValue{Key: MVCCKey{Key: key, Timestamp: ts}, Value: value}
	}

	kv1_1_1 := makeKVT(testKey1, testValue1, ts1)
	kv1_4_4 := makeKVT(testKey1, testValue4, ts4)
	kv1_2_2 := makeKVT(testKey1, testValue2, ts2)
	kv2_2_2 := makeKVT(testKey2, testValue3, ts2)
	kv1_3Deleted := makeKVT(testKey1, nil, ts3)
	kvs := func(kvs ...MVCCKeyValue) []MVCCKeyValue { return kvs }

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			fn := (*MVCCIncrementalIterator).NextKey

			t.Run("empty", assertEqualKVs(e, fn, keyMin, keyMax, tsMin, ts3, nil))

			for _, kv := range kvs(kv1_1_1, kv1_2_2, kv2_2_2) {
				v := roachpb.Value{RawBytes: kv.Value}
				if err := MVCCPut(ctx, e, nil, kv.Key.Key, kv.Key.Timestamp, v, nil); err != nil {
					t.Fatal(err)
				}
			}

			// Exercise time ranges.
			t.Run("ts (0-0]", assertEqualKVs(e, fn, keyMin, keyMax, tsMin, tsMin, nil))
			t.Run("ts (0-1]", assertEqualKVs(e, fn, keyMin, keyMax, tsMin, ts1, kvs(kv1_1_1)))
			t.Run("ts (0-∞]", assertEqualKVs(e, fn, keyMin, keyMax, tsMin, tsMax, kvs(kv1_2_2, kv2_2_2)))
			t.Run("ts (1-1]", assertEqualKVs(e, fn, keyMin, keyMax, ts1, ts1, nil))
			t.Run("ts (1-2]", assertEqualKVs(e, fn, keyMin, keyMax, ts1, ts2, kvs(kv1_2_2, kv2_2_2)))
			t.Run("ts (2-2]", assertEqualKVs(e, fn, keyMin, keyMax, ts2, ts2, nil))

			// Exercise key ranges.
			t.Run("kv [1-1)", assertEqualKVs(e, fn, testKey1, testKey1, tsMin, tsMax, nil))
			t.Run("kv [1-2)", assertEqualKVs(e, fn, testKey1, testKey2, tsMin, tsMax, kvs(kv1_2_2)))

			// Exercise deletion.
			if err := MVCCDelete(ctx, e, nil, testKey1, ts3, nil); err != nil {
				t.Fatal(err)
			}
			t.Run("del", assertEqualKVs(e, fn, keyMin, keyMax, ts1, tsMax, kvs(kv1_3Deleted, kv2_2_2)))

			// Exercise intent handling.
			txn1ID := uuid.MakeV4()
			txn1 := roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					Key:       testKey1,
					ID:        txn1ID,
					Epoch:     1,
					Timestamp: ts4,
				},
				OrigTimestamp: ts4,
			}
			txn1Val := roachpb.Value{RawBytes: testValue4}
			if err := MVCCPut(ctx, e, nil, txn1.TxnMeta.Key, txn1.OrigTimestamp, txn1Val, &txn1); err != nil {
				t.Fatal(err)
			}
			txn2ID := uuid.MakeV4()
			txn2 := roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					Key:       testKey2,
					ID:        txn2ID,
					Epoch:     1,
					Timestamp: ts4,
				},
				OrigTimestamp: ts4,
			}
			txn2Val := roachpb.Value{RawBytes: testValue4}
			if err := MVCCPut(ctx, e, nil, txn2.TxnMeta.Key, txn2.OrigTimestamp, txn2Val, &txn2); err != nil {
				t.Fatal(err)
			}
			t.Run("intents",
				iterateExpectErr(e, fn, testKey1, testKey1.PrefixEnd(), tsMin, tsMax, "conflicting intents"))
			t.Run("intents",
				iterateExpectErr(e, fn, testKey2, testKey2.PrefixEnd(), tsMin, tsMax, "conflicting intents"))
			t.Run("intents",
				iterateExpectErr(e, fn, keyMin, keyMax, tsMin, ts4, "conflicting intents"))
			// Intents above the upper time bound or beneath the lower time bound must
			// be ignored (#28358). Note that the lower time bound is exclusive while
			// the upper time bound is inclusive.
			t.Run("intents", assertEqualKVs(e, fn, keyMin, keyMax, tsMin, ts3, kvs(kv1_3Deleted, kv2_2_2)))
			t.Run("intents", assertEqualKVs(e, fn, keyMin, keyMax, ts4, tsMax, kvs()))
			t.Run("intents", assertEqualKVs(e, fn, keyMin, keyMax, ts4.Next(), tsMax, kvs()))

			intent1 := roachpb.Intent{Span: roachpb.Span{Key: testKey1}, Txn: txn1.TxnMeta, Status: roachpb.COMMITTED}
			if err := MVCCResolveWriteIntent(ctx, e, nil, intent1); err != nil {
				t.Fatal(err)
			}
			intent2 := roachpb.Intent{Span: roachpb.Span{Key: testKey2}, Txn: txn2.TxnMeta, Status: roachpb.ABORTED}
			if err := MVCCResolveWriteIntent(ctx, e, nil, intent2); err != nil {
				t.Fatal(err)
			}
			t.Run("intents", assertEqualKVs(e, fn, keyMin, keyMax, tsMin, tsMax, kvs(kv1_4_4, kv2_2_2)))
		})
	}

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			fn := (*MVCCIncrementalIterator).Next

			t.Run("empty", assertEqualKVs(e, fn, keyMin, keyMax, tsMin, ts3, nil))

			for _, kv := range kvs(kv1_1_1, kv1_2_2, kv2_2_2) {
				v := roachpb.Value{RawBytes: kv.Value}
				if err := MVCCPut(ctx, e, nil, kv.Key.Key, kv.Key.Timestamp, v, nil); err != nil {
					t.Fatal(err)
				}
			}

			// Exercise time ranges.
			t.Run("ts (0-0]", assertEqualKVs(e, fn, keyMin, keyMax, tsMin, tsMin, nil))
			t.Run("ts (0-1]", assertEqualKVs(e, fn, keyMin, keyMax, tsMin, ts1, kvs(kv1_1_1)))
			t.Run("ts (0-∞]", assertEqualKVs(e, fn, keyMin, keyMax, tsMin, tsMax, kvs(kv1_2_2, kv1_1_1, kv2_2_2)))
			t.Run("ts (1-1]", assertEqualKVs(e, fn, keyMin, keyMax, ts1, ts1, nil))
			t.Run("ts (1-2]", assertEqualKVs(e, fn, keyMin, keyMax, ts1, ts2, kvs(kv1_2_2, kv2_2_2)))
			t.Run("ts (2-2]", assertEqualKVs(e, fn, keyMin, keyMax, ts2, ts2, nil))

			// Exercise key ranges.
			t.Run("kv [1-1)", assertEqualKVs(e, fn, testKey1, testKey1, tsMin, tsMax, nil))
			t.Run("kv [1-2)", assertEqualKVs(e, fn, testKey1, testKey2, tsMin, tsMax, kvs(kv1_2_2, kv1_1_1)))

			// Exercise deletion.
			if err := MVCCDelete(ctx, e, nil, testKey1, ts3, nil); err != nil {
				t.Fatal(err)
			}
			t.Run("del", assertEqualKVs(e, fn, keyMin, keyMax, ts1, tsMax, kvs(kv1_3Deleted, kv1_2_2, kv2_2_2)))

			// Exercise intent handling.
			txn1ID := uuid.MakeV4()
			txn1 := roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					Key:       testKey1,
					ID:        txn1ID,
					Epoch:     1,
					Timestamp: ts4,
				},
				OrigTimestamp: ts4,
			}
			txn1Val := roachpb.Value{RawBytes: testValue4}
			if err := MVCCPut(ctx, e, nil, txn1.TxnMeta.Key, txn1.OrigTimestamp, txn1Val, &txn1); err != nil {
				t.Fatal(err)
			}
			txn2ID := uuid.MakeV4()
			txn2 := roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					Key:       testKey2,
					ID:        txn2ID,
					Epoch:     1,
					Timestamp: ts4,
				},
				OrigTimestamp: ts4,
			}
			txn2Val := roachpb.Value{RawBytes: testValue4}
			if err := MVCCPut(ctx, e, nil, txn2.TxnMeta.Key, txn2.OrigTimestamp, txn2Val, &txn2); err != nil {
				t.Fatal(err)
			}
			t.Run("intents",
				iterateExpectErr(e, fn, testKey1, testKey1.PrefixEnd(), tsMin, tsMax, "conflicting intents"))
			t.Run("intents",
				iterateExpectErr(e, fn, testKey2, testKey2.PrefixEnd(), tsMin, tsMax, "conflicting intents"))
			t.Run("intents",
				iterateExpectErr(e, fn, keyMin, keyMax, tsMin, ts4, "conflicting intents"))
			// Intents above the upper time bound or beneath the lower time bound must
			// be ignored (#28358). Note that the lower time bound is exclusive while
			// the upper time bound is inclusive.
			t.Run("intents", assertEqualKVs(e, fn, keyMin, keyMax, tsMin, ts3, kvs(kv1_3Deleted, kv1_2_2, kv1_1_1, kv2_2_2)))
			t.Run("intents", assertEqualKVs(e, fn, keyMin, keyMax, ts4, tsMax, kvs()))
			t.Run("intents", assertEqualKVs(e, fn, keyMin, keyMax, ts4.Next(), tsMax, kvs()))

			intent1 := roachpb.Intent{Span: roachpb.Span{Key: testKey1}, Txn: txn1.TxnMeta, Status: roachpb.COMMITTED}
			if err := MVCCResolveWriteIntent(ctx, e, nil, intent1); err != nil {
				t.Fatal(err)
			}
			intent2 := roachpb.Intent{Span: roachpb.Span{Key: testKey2}, Txn: txn2.TxnMeta, Status: roachpb.ABORTED}
			if err := MVCCResolveWriteIntent(ctx, e, nil, intent2); err != nil {
				t.Fatal(err)
			}
			t.Run("intents", assertEqualKVs(e, fn, keyMin, keyMax, tsMin, tsMax, kvs(kv1_4_4, kv1_3Deleted, kv1_2_2, kv1_1_1, kv2_2_2)))
		})
	}
}

func slurpKVsInTimeRange(
	e Reader, prefix roachpb.Key, startTime, endTime hlc.Timestamp,
) ([]MVCCKeyValue, error) {
	endKey := prefix.PrefixEnd()
	iter := NewMVCCIncrementalIterator(e, MVCCIncrementalIterOptions{
		IterOptions: IterOptions{
			UpperBound: endKey,
		},
		StartTime: startTime,
		EndTime:   endTime,
	})
	defer iter.Close()
	var kvs []MVCCKeyValue
	for iter.SeekGE(MakeMVCCMetadataKey(prefix)); ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return nil, err
		} else if !ok || iter.UnsafeKey().Key.Compare(endKey) >= 0 {
			break
		}
		kvs = append(kvs, MVCCKeyValue{Key: iter.Key(), Value: iter.Value()})
	}
	return kvs, nil
}

// TestMVCCIncrementalIteratorIntentRewrittenConcurrently verifies that the
// workaround in MVCCIncrementalIterator to double-check for deleted intents
// properly handles cases where an intent originally in a time-bound iterator's
// time range is rewritten at a timestamp outside of its time range.
func TestMVCCIncrementalIteratorIntentRewrittenConcurrently(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			// Create a DB containing a single intent.
			ctx := context.Background()

			kA := roachpb.Key("kA")
			vA1 := roachpb.MakeValueFromString("vA1")
			vA2 := roachpb.MakeValueFromString("vA2")
			ts0 := hlc.Timestamp{WallTime: 0}
			ts1 := hlc.Timestamp{WallTime: 1}
			ts2 := hlc.Timestamp{WallTime: 2}
			ts3 := hlc.Timestamp{WallTime: 3}
			txn := &roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					Key:       roachpb.Key("b"),
					ID:        uuid.MakeV4(),
					Epoch:     1,
					Timestamp: ts1,
					Sequence:  1,
				},
				OrigTimestamp: ts1,
			}
			if err := MVCCPut(ctx, e, nil, kA, ts1, vA1, txn); err != nil {
				t.Fatal(err)
			}

			// Concurrently iterate over the intent using a time-bound iterator and move
			// the intent out of the time-bound iterator's time range by writing to it
			// again at a higher timestamp.
			g, _ := errgroup.WithContext(ctx)
			g.Go(func() error {
				// Re-write the intent with a higher timestamp.
				txn.Timestamp = ts3
				txn.Sequence = 2
				return MVCCPut(ctx, e, nil, kA, ts1, vA2, txn)
			})
			g.Go(func() error {
				// Iterate with a time range that includes the initial intent but does
				// not include the new intent.
				kvs, err := slurpKVsInTimeRange(e, kA, ts0, ts2)

				// There are two permissible outcomes from the scan. If the iteration
				// wins the race with the put that moves the intent then it should
				// observe the intent and return a write intent error. If the iteration
				// loses the race with the put that moves the intent then it should
				// observe and return nothing because there will be no committed or
				// provisional keys in its time range.
				if err != nil {
					if !testutils.IsError(err, `conflicting intents on "kA"`) {
						return err
					}
				} else {
					if len(kvs) != 0 {
						return errors.Errorf(`unexpected kvs: %v`, kvs)
					}
				}
				return nil
			})
			if err := g.Wait(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
