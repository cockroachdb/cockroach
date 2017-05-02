// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package engineccl

import (
	"bytes"
	"math"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func iterateExpectErr(
	e engine.Engine, startKey, endKey roachpb.Key, startTime, endTime hlc.Timestamp, errString string,
) func(*testing.T) {
	return func(t *testing.T) {
		iter := NewMVCCIncrementalIterator(e)
		defer iter.Close()
		for iter.Reset(startKey, endKey, startTime, endTime); iter.Valid(); iter.Next() {
			// pass
		}
		if err := iter.Error(); !testutils.IsError(err, errString) {
			t.Fatalf("expected error %q but got %v", errString, err)
		}
	}
}

func assertEqualKVs(
	e engine.Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	expected []engine.MVCCKeyValue,
) func(*testing.T) {
	return func(t *testing.T) {
		iter := NewMVCCIncrementalIterator(e)
		defer iter.Close()
		var kvs []engine.MVCCKeyValue
		for iter.Reset(startKey, endKey, startTime, endTime); iter.Valid(); iter.Next() {
			kvs = append(kvs, engine.MVCCKeyValue{Key: iter.Key(), Value: iter.Value()})
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

func TestMVCCIterateIncremental(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	e := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	defer e.Close()

	var (
		keyMin   = roachpb.KeyMin
		keyMax   = roachpb.KeyMax
		testKey1 = roachpb.Key("/db1")
		testKey2 = roachpb.Key("/db2")

		testValue1 = []byte("val1")
		testValue2 = []byte("val2")
		testValue3 = []byte("val3")
		testValue4 = []byte("val4")

		ts0   = hlc.Timestamp{WallTime: 0, Logical: 0}
		ts1   = hlc.Timestamp{WallTime: 1, Logical: 0}
		ts2   = hlc.Timestamp{WallTime: 2, Logical: 0}
		ts3   = hlc.Timestamp{WallTime: 3, Logical: 0}
		ts4   = hlc.Timestamp{WallTime: 4, Logical: 0}
		tsMax = hlc.Timestamp{WallTime: math.MaxInt64, Logical: 0}
	)

	makeKVT := func(key roachpb.Key, value []byte, ts hlc.Timestamp) engine.MVCCKeyValue {
		return engine.MVCCKeyValue{Key: engine.MVCCKey{Key: key, Timestamp: ts}, Value: value}
	}

	kv1_1_1 := makeKVT(testKey1, testValue1, ts1)
	kv1_4_4 := makeKVT(testKey1, testValue4, ts4)
	kv1_2_2 := makeKVT(testKey1, testValue2, ts2)
	kv2_2_2 := makeKVT(testKey2, testValue3, ts2)
	kv1_3Deleted := makeKVT(testKey1, nil, ts3)
	kvs := func(kvs ...engine.MVCCKeyValue) []engine.MVCCKeyValue { return kvs }

	t.Run("empty", assertEqualKVs(e, keyMin, keyMax, ts0, ts3, nil))

	for _, kv := range kvs(kv1_1_1, kv1_2_2, kv2_2_2) {
		v := roachpb.Value{RawBytes: kv.Value}
		if err := engine.MVCCPut(ctx, e, nil, kv.Key.Key, kv.Key.Timestamp, v, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Exercise time ranges.
	t.Run("ts 1-1", assertEqualKVs(e, keyMin, keyMax, ts1, ts1, nil))
	t.Run("ts 1-2", assertEqualKVs(e, keyMin, keyMax, ts1, ts2, kvs(kv1_1_1)))
	t.Run("ts 1-∞", assertEqualKVs(e, keyMin, keyMax, ts1, tsMax, kvs(kv1_2_2, kv2_2_2)))
	t.Run("ts 2-2", assertEqualKVs(e, keyMin, keyMax, ts2, ts2, nil))
	t.Run("ts 2-3", assertEqualKVs(e, keyMin, keyMax, ts2, ts3, kvs(kv1_2_2, kv2_2_2)))
	t.Run("ts 3-3", assertEqualKVs(e, keyMin, keyMax, ts3, ts3, nil))

	// Exercise key ranges.
	t.Run("kv 1-1", assertEqualKVs(e, testKey1, testKey1, ts0, tsMax, nil))
	t.Run("kv 1-2", assertEqualKVs(e, testKey1, testKey2, ts0, tsMax, kvs(kv1_2_2)))

	// Exercise deletion.
	if err := engine.MVCCDelete(ctx, e, nil, testKey1, ts3, nil); err != nil {
		t.Fatal(err)
	}
	t.Run("del", assertEqualKVs(e, keyMin, keyMax, ts0, tsMax, kvs(kv1_3Deleted, kv2_2_2)))

	// Exercise intent handling.
	txn1ID := uuid.MakeV4()
	txn1 := roachpb.Transaction{TxnMeta: enginepb.TxnMeta{
		Key:       testKey1,
		ID:        &txn1ID,
		Epoch:     1,
		Timestamp: ts4,
	}}
	txn1Val := roachpb.Value{RawBytes: testValue4}
	if err := engine.MVCCPut(ctx, e, nil, txn1.TxnMeta.Key, txn1.TxnMeta.Timestamp, txn1Val, &txn1); err != nil {
		t.Fatal(err)
	}
	txn2ID := uuid.MakeV4()
	txn2 := roachpb.Transaction{TxnMeta: enginepb.TxnMeta{
		Key:       testKey2,
		ID:        &txn2ID,
		Epoch:     1,
		Timestamp: ts4,
	}}
	txn2Val := roachpb.Value{RawBytes: testValue4}
	if err := engine.MVCCPut(ctx, e, nil, txn2.TxnMeta.Key, txn2.TxnMeta.Timestamp, txn2Val, &txn2); err != nil {
		t.Fatal(err)
	}
	t.Run("intents1",
		iterateExpectErr(e, testKey1, testKey1.PrefixEnd(), ts0, tsMax, "conflicting intents"))
	t.Run("intents2",
		iterateExpectErr(e, testKey2, testKey2.PrefixEnd(), ts0, tsMax, "conflicting intents"))
	t.Run("intents3", assertEqualKVs(e, keyMin, keyMax, ts0, ts4, nil))

	intent1 := roachpb.Intent{Span: roachpb.Span{Key: testKey1}, Txn: txn1.TxnMeta, Status: roachpb.COMMITTED}
	if err := engine.MVCCResolveWriteIntent(ctx, e, nil, intent1); err != nil {
		t.Fatal(err)
	}
	intent2 := roachpb.Intent{Span: roachpb.Span{Key: testKey2}, Txn: txn2.TxnMeta, Status: roachpb.ABORTED}
	if err := engine.MVCCResolveWriteIntent(ctx, e, nil, intent2); err != nil {
		t.Fatal(err)
	}
	t.Run("intents4", assertEqualKVs(e, keyMin, keyMax, ts0, tsMax, kvs(kv1_4_4, kv2_2_2)))
}
