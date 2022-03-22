// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

const all, latest = true, false

func makeKVT(key roachpb.Key, value []byte, ts hlc.Timestamp) MVCCKeyValue {
	return MVCCKeyValue{Key: MVCCKey{Key: key, Timestamp: ts}, Value: value}
}

func makeKVTxn(
	key roachpb.Key, val []byte, ts hlc.Timestamp,
) (roachpb.Transaction, roachpb.Value, roachpb.Intent) {
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
		}, roachpb.MakeIntent(&txnMeta, key)
}

func intents(intents ...roachpb.Intent) []roachpb.Intent {
	return intents
}

func kvs(kvs ...MVCCKeyValue) []MVCCKeyValue {
	return kvs
}

func iterateExpectErr(
	e Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	revisions bool,
	intents []roachpb.Intent,
) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()

		t.Run("aggregate-intents", func(t *testing.T) {
			assertExpectErrs(t, e, startKey, endKey, startTime, endTime, revisions, intents)
		})
		t.Run("first-intent", func(t *testing.T) {
			assertExpectErr(t, e, startKey, endKey, startTime, endTime, revisions, intents[0])
		})
		t.Run("export-intents", func(t *testing.T) {
			assertExportedErrs(t, e, startKey, endKey, startTime, endTime, revisions, intents, false)
		})
		t.Run("export-intents-tbi", func(t *testing.T) {
			assertExportedErrs(t, e, startKey, endKey, startTime, endTime, revisions, intents, true)
		})
	}
}

func assertExpectErr(
	t *testing.T,
	e Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	revisions bool,
	expectedIntent roachpb.Intent,
) {
	iter := NewMVCCIncrementalIterator(e, MVCCIncrementalIterOptions{
		EndKey:    endKey,
		StartTime: startTime,
		EndTime:   endTime,
	})
	defer iter.Close()
	var iterFn func()
	if revisions {
		iterFn = iter.Next
	} else {
		iterFn = iter.NextKey
	}
	for iter.SeekGE(MakeMVCCMetadataKey(startKey)); ; iterFn() {
		if ok, _ := iter.Valid(); !ok || iter.UnsafeKey().Key.Compare(endKey) >= 0 {
			break
		}
		// pass
	}

	_, err := iter.Valid()
	if intentErr := (*roachpb.WriteIntentError)(nil); errors.As(err, &intentErr) {
		if !expectedIntent.Key.Equal(intentErr.Intents[0].Key) {
			t.Fatalf("Expected intent key %v, but got %v", expectedIntent.Key, intentErr.Intents[0].Key)
		}
	} else {
		t.Fatalf("expected error with intent %v but got %v", expectedIntent, err)
	}
}

func assertExpectErrs(
	t *testing.T,
	e Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	revisions bool,
	expectedIntents []roachpb.Intent,
) {
	iter := NewMVCCIncrementalIterator(e, MVCCIncrementalIterOptions{
		EndKey:       endKey,
		StartTime:    startTime,
		EndTime:      endTime,
		IntentPolicy: MVCCIncrementalIterIntentPolicyAggregate,
	})
	defer iter.Close()
	var iterFn func()
	if revisions {
		iterFn = iter.Next
	} else {
		iterFn = iter.NextKey
	}
	for iter.SeekGE(MakeMVCCMetadataKey(startKey)); ; iterFn() {
		if ok, _ := iter.Valid(); !ok || iter.UnsafeKey().Key.Compare(endKey) >= 0 {
			break
		}
		// pass
	}

	if iter.NumCollectedIntents() != len(expectedIntents) {
		t.Fatalf("Expected %d intents but found %d", len(expectedIntents), iter.NumCollectedIntents())
	}
	err := iter.TryGetIntentError()
	if intentErr := (*roachpb.WriteIntentError)(nil); errors.As(err, &intentErr) {
		for i := range expectedIntents {
			if !expectedIntents[i].Key.Equal(intentErr.Intents[i].Key) {
				t.Fatalf("%d intent key: got %v, expected %v", i, intentErr.Intents[i].Key, expectedIntents[i].Key)
			}
			if !expectedIntents[i].Txn.ID.Equal(intentErr.Intents[i].Txn.ID) {
				t.Fatalf("%d intent key: got %v, expected %v", i, intentErr.Intents[i].Txn.ID, expectedIntents[i].Txn.ID)
			}
		}
	} else {
		t.Fatalf("Expected roachpb.WriteIntentError, found %T", err)
	}
}

func assertExportedErrs(
	t *testing.T,
	e Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	revisions bool,
	expectedIntents []roachpb.Intent,
	useTBI bool,
) {
	const big = 1 << 30
	sstFile := &MemFile{}
	_, _, _, err := e.ExportMVCCToSst(context.Background(), ExportOptions{
		StartKey:           MVCCKey{Key: startKey},
		EndKey:             endKey,
		StartTS:            startTime,
		EndTS:              endTime,
		ExportAllRevisions: revisions,
		TargetSize:         big,
		MaxSize:            big,
		MaxIntents:         uint64(MaxIntentsPerWriteIntentError.Default()),
		StopMidKey:         false,
		UseTBI:             useTBI,
	}, sstFile)
	require.Error(t, err)

	if intentErr := (*roachpb.WriteIntentError)(nil); errors.As(err, &intentErr) {
		for i := range expectedIntents {
			if !expectedIntents[i].Key.Equal(intentErr.Intents[i].Key) {
				t.Fatalf("%d intent key: got %v, expected %v", i, intentErr.Intents[i].Key, expectedIntents[i].Key)
			}
			if !expectedIntents[i].Txn.ID.Equal(intentErr.Intents[i].Txn.ID) {
				t.Fatalf("%d intent key: got %v, expected %v", i, intentErr.Intents[i].Txn.ID, expectedIntents[i].Txn.ID)
			}
		}
	} else {
		t.Fatalf("Expected roachpb.WriteIntentError, found %T", err)
	}
}

func assertExportedKVs(
	t *testing.T,
	e Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	revisions bool,
	expected []MVCCKeyValue,
	useTBI bool,
) {
	const big = 1 << 30
	sstFile := &MemFile{}
	_, _, _, err := e.ExportMVCCToSst(context.Background(), ExportOptions{
		StartKey:           MVCCKey{Key: startKey},
		EndKey:             endKey,
		StartTS:            startTime,
		EndTS:              endTime,
		ExportAllRevisions: revisions,
		TargetSize:         big,
		MaxSize:            big,
		StopMidKey:         false,
		UseTBI:             useTBI,
	}, sstFile)
	require.NoError(t, err)
	data := sstFile.Data()
	if data == nil {
		require.Nil(t, expected)
		return
	}
	sst, err := NewMemSSTIterator(data, false)
	require.NoError(t, err)
	defer sst.Close()

	sst.SeekGE(MVCCKey{})
	for i := range expected {
		ok, err := sst.Valid()
		require.NoError(t, err)
		require.Truef(t, ok, "iteration produced %d keys, expected %d", i, len(expected))
		assert.Equalf(t, expected[i].Key, sst.UnsafeKey(), "key %d", i)
		if expected[i].Value == nil {
			assert.Equalf(t, []byte{}, sst.UnsafeValue(), "key %d %q", i, sst.UnsafeKey())
		} else {
			assert.Equalf(t, expected[i].Value, sst.UnsafeValue(), "key %d %q", i, sst.UnsafeKey())
		}
		sst.Next()
	}
	ok, err := sst.Valid()
	require.NoError(t, err)
	require.False(t, ok)
}

func nextIgnoreTimeExpectErr(
	t *testing.T,
	e Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	errString string,
) {
	// The semantics of the methods NextIgnoringTime() should not change whether
	// or not we enable the TBI optimization.
	for _, useTBI := range []bool{true, false} {
		t.Run(fmt.Sprintf("useTBI-%t", useTBI), func(t *testing.T) {
			iter := NewMVCCIncrementalIterator(e, MVCCIncrementalIterOptions{
				EndKey:                              endKey,
				EnableTimeBoundIteratorOptimization: useTBI,
				StartTime:                           startTime,
				EndTime:                             endTime,
			})
			defer iter.Close()
			for iter.SeekGE(MakeMVCCMetadataKey(startKey)); ; iter.NextIgnoringTime() {
				if ok, _ := iter.Valid(); !ok || iter.UnsafeKey().Key.Compare(endKey) >= 0 {
					break
				}
				// pass
			}
			if _, err := iter.Valid(); !testutils.IsError(err, errString) {
				t.Fatalf("expected error %q but got %v", errString, err)
			}
		})
	}
}

func nextKeyIgnoreTimeExpectErr(
	t *testing.T,
	e Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	errString string,
) {
	// The semantics of the methods NextIgnoringTime() should not change whether
	// or not we enable the TBI optimization.
	for _, useTBI := range []bool{true, false} {
		t.Run(fmt.Sprintf("useTBI-%t", useTBI), func(t *testing.T) {
			iter := NewMVCCIncrementalIterator(e, MVCCIncrementalIterOptions{
				EndKey:                              endKey,
				EnableTimeBoundIteratorOptimization: useTBI,
				StartTime:                           startTime,
				EndTime:                             endTime,
			})
			defer iter.Close()
			for iter.SeekGE(MakeMVCCMetadataKey(startKey)); ; iter.NextKeyIgnoringTime() {
				if ok, _ := iter.Valid(); !ok || iter.UnsafeKey().Key.Compare(endKey) >= 0 {
					break
				}
				// pass
			}
			if _, err := iter.Valid(); !testutils.IsError(err, errString) {
				t.Fatalf("expected error %q but got %v", errString, err)
			}
		})
	}
}

func assertNextIgnoreTimeIteratedKVs(
	t *testing.T,
	e Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	expected []MVCCKeyValue,
) {
	// The semantics of the methods NextIgnoringTime() should not change whether
	// or not we enable the TBI optimization.
	for _, useTBI := range []bool{true, false} {
		t.Run(fmt.Sprintf("useTBI-%t", useTBI), func(t *testing.T) {
			iter := NewMVCCIncrementalIterator(e, MVCCIncrementalIterOptions{
				EndKey:                              endKey,
				EnableTimeBoundIteratorOptimization: useTBI,
				StartTime:                           startTime,
				EndTime:                             endTime,
			})
			defer iter.Close()
			var kvs []MVCCKeyValue
			for iter.SeekGE(MakeMVCCMetadataKey(startKey)); ; iter.NextIgnoringTime() {
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
		})
	}
}

func assertNextKeyIgnoreTimeIteratedKVs(
	t *testing.T,
	e Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	expected []MVCCKeyValue,
) {
	// The semantics of the methods NextKeyIgnoringTime() should not change whether
	// or not we enable the TBI optimization.
	for _, useTBI := range []bool{true, false} {
		t.Run(fmt.Sprintf("useTBI-%t", useTBI), func(t *testing.T) {
			iter := NewMVCCIncrementalIterator(e, MVCCIncrementalIterOptions{
				EndKey:                              endKey,
				EnableTimeBoundIteratorOptimization: useTBI,
				StartTime:                           startTime,
				EndTime:                             endTime,
			})
			defer iter.Close()
			var kvs []MVCCKeyValue
			for iter.SeekGE(MakeMVCCMetadataKey(startKey)); ; iter.NextKeyIgnoringTime() {
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
		})
	}
}

func assertIteratedKVs(
	t *testing.T,
	e Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	revisions bool,
	expected []MVCCKeyValue,
	useTBI bool,
) {
	iter := NewMVCCIncrementalIterator(e, MVCCIncrementalIterOptions{
		EndKey:                              endKey,
		EnableTimeBoundIteratorOptimization: useTBI,
		StartTime:                           startTime,
		EndTime:                             endTime,
		IntentPolicy:                        MVCCIncrementalIterIntentPolicyAggregate,
	})
	defer iter.Close()
	var iterFn func()
	if revisions {
		iterFn = iter.Next
	} else {
		iterFn = iter.NextKey
	}
	var kvs []MVCCKeyValue
	for iter.SeekGE(MakeMVCCMetadataKey(startKey)); ; iterFn() {
		if ok, err := iter.Valid(); err != nil {
			t.Fatalf("unexpected error: %+v", err)
		} else if !ok || iter.UnsafeKey().Key.Compare(endKey) >= 0 {
			break
		}
		if iter.NumCollectedIntents() > 0 {
			t.Fatal("got unexpected intent error")
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

func assertEqualKVs(
	e Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	revisions bool,
	expected []MVCCKeyValue,
) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()
		t.Run("iterate", func(t *testing.T) {
			assertIteratedKVs(t, e, startKey, endKey, startTime, endTime, revisions, expected,
				false /* useTBI */)
		})
		t.Run("iterate-tbi", func(t *testing.T) {
			assertIteratedKVs(t, e, startKey, endKey, startTime, endTime, revisions, expected,
				true /* useTBI */)
		})

		t.Run("export", func(t *testing.T) {
			assertExportedKVs(t, e, startKey, endKey, startTime, endTime, revisions, expected,
				false /* useTBI */)
		})
		t.Run("export-tbi", func(t *testing.T) {
			assertExportedKVs(t, e, startKey, endKey, startTime, endTime, revisions, expected,
				true /* useTBI */)
		})
	}
}

// TestMVCCIncrementalIteratorNextIgnoringTime tests the iteration semantics of
// the method NextIgnoreTime(). This method is supposed to return all the KVs
// (versions and new keys) that would be encountered in a non-incremental
// iteration.
func TestMVCCIncrementalIteratorNextIgnoringTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var (
		keyMax   = roachpb.KeyMax
		testKey1 = roachpb.Key("/db1")
		testKey2 = roachpb.Key("/db2")

		testValue1 = []byte("val1")
		testValue2 = []byte("val2")
		testValue3 = []byte("val3")
		testValue4 = []byte("val4")

		// Use a non-zero min, since we use IsEmpty to decide if a ts should be used
		// as upper/lower-bound during iterator initialization.
		tsMin = hlc.Timestamp{WallTime: 0, Logical: 1}
		ts1   = hlc.Timestamp{WallTime: 1, Logical: 0}
		ts2   = hlc.Timestamp{WallTime: 2, Logical: 0}
		ts3   = hlc.Timestamp{WallTime: 3, Logical: 0}
		ts4   = hlc.Timestamp{WallTime: 4, Logical: 0}
		tsMax = hlc.Timestamp{WallTime: math.MaxInt64, Logical: 0}
	)

	kv1_1_1 := makeKVT(testKey1, testValue1, ts1)
	kv1_2_2 := makeKVT(testKey1, testValue2, ts2)
	kv2_2_2 := makeKVT(testKey2, testValue3, ts2)
	kv2_4_4 := makeKVT(testKey2, testValue4, ts4)
	kv1_3Deleted := makeKVT(testKey1, nil, ts3)

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			t.Run("empty", func(t *testing.T) {
				assertNextIgnoreTimeIteratedKVs(t, e, localMax, keyMax, tsMin, ts3, nil)
			})

			for _, kv := range kvs(kv1_1_1, kv1_2_2, kv2_2_2) {
				v := roachpb.Value{RawBytes: kv.Value}
				if err := MVCCPut(ctx, e, nil, kv.Key.Key, kv.Key.Timestamp, v, nil); err != nil {
					t.Fatal(err)
				}
			}

			// Exercise time ranges.
			t.Run("ts (0-0]", func(t *testing.T) {
				assertNextIgnoreTimeIteratedKVs(t, e, localMax, keyMax, tsMin, tsMin, nil)
			})
			// Returns the kv_2_2_2 even though it is outside (startTime, endTime].
			t.Run("ts (0-1]", func(t *testing.T) {
				assertNextIgnoreTimeIteratedKVs(t, e, localMax, keyMax, tsMin, ts1, kvs(kv1_1_1, kv2_2_2))
			})
			t.Run("ts (0-∞]", func(t *testing.T) {
				assertNextIgnoreTimeIteratedKVs(t, e, localMax, keyMax, tsMin, tsMax, kvs(kv1_2_2, kv1_1_1,
					kv2_2_2))
			})
			t.Run("ts (1-1]", func(t *testing.T) {
				assertNextIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts1, ts1, nil)
			})
			// Returns the kv_1_1_1 even though it is outside (startTime, endTime].
			t.Run("ts (1-2]", func(t *testing.T) {
				assertNextIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts1, ts2, kvs(kv1_2_2, kv1_1_1,
					kv2_2_2))
			})
			t.Run("ts (2-2]", func(t *testing.T) {
				assertNextIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts2, ts2, nil)
			})

			// Exercise key ranges.
			t.Run("kv [1-1)", func(t *testing.T) {
				assertNextIgnoreTimeIteratedKVs(t, e, testKey1, testKey1, tsMin, tsMax, nil)
			})
			t.Run("kv [1-2)", func(t *testing.T) {
				assertNextIgnoreTimeIteratedKVs(t, e, testKey1, testKey2, tsMin, tsMax, kvs(kv1_2_2, kv1_1_1))
			})

			// Exercise deletion.
			if err := MVCCDelete(ctx, e, nil, testKey1, ts3, nil); err != nil {
				t.Fatal(err)
			}
			// Returns the kv_1_1_1 even though it is outside (startTime, endTime].
			t.Run("del", func(t *testing.T) {
				assertNextIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts1, tsMax, kvs(kv1_3Deleted,
					kv1_2_2, kv1_1_1, kv2_2_2))
			})

			// Insert an intent of testKey2.
			txn1ID := uuid.MakeV4()
			txn1 := roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					Key:            testKey2,
					ID:             txn1ID,
					Epoch:          1,
					WriteTimestamp: ts4,
				},
				ReadTimestamp: ts4,
			}
			txn1Val := roachpb.Value{RawBytes: testValue4}
			if err := MVCCPut(ctx, e, nil, txn1.TxnMeta.Key, txn1.ReadTimestamp, txn1Val, &txn1); err != nil {
				t.Fatal(err)
			}

			// We have to be careful that we are testing the intent handling logic of
			// NextIgnoreTime() rather than the first SeekGE(). We do this by
			// ensuring that the SeekGE() doesn't encounter an intent.
			t.Run("intents", func(t *testing.T) {
				nextIgnoreTimeExpectErr(t, e, testKey1, testKey2.PrefixEnd(), tsMin, tsMax, "conflicting intents")
			})
			t.Run("intents", func(t *testing.T) {
				nextIgnoreTimeExpectErr(t, e, localMax, keyMax, tsMin, ts4, "conflicting intents")
			})
			// Intents above the upper time bound or beneath the lower time bound must
			// be ignored. Note that the lower time bound is exclusive while the upper
			// time bound is inclusive.
			//
			// The intent at ts=4 for kv2 lies outside the timespan
			// (startTime, endTime] so we do not raise an error and just move on to
			// its versioned KV.
			t.Run("intents", func(t *testing.T) {
				assertNextIgnoreTimeIteratedKVs(t, e, localMax, keyMax, tsMin, ts3, kvs(kv1_3Deleted,
					kv1_2_2, kv1_1_1, kv2_4_4, kv2_2_2))
			})
			t.Run("intents", func(t *testing.T) {
				assertNextIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts4, tsMax, kvs())
			})
			t.Run("intents", func(t *testing.T) {
				assertNextIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts4.Next(), tsMax, kvs())
			})
		})
	}
}

// TestMVCCIncrementalIteratorNextKeyIgnoringTime tests the iteration semantics
// of the method NextKeyIgnoreTime(). This method is supposed to return all new
// keys that would be encountered in a non-incremental iteration.
func TestMVCCIncrementalIteratorNextKeyIgnoringTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var (
		keyMax   = roachpb.KeyMax
		testKey1 = roachpb.Key("/db1")
		testKey2 = roachpb.Key("/db2")

		testValue1 = []byte("val1")
		testValue2 = []byte("val2")
		testValue3 = []byte("val3")
		testValue4 = []byte("val4")

		// Use a non-zero min, since we use IsEmpty to decide if a ts should be used
		// as upper/lower-bound during iterator initialization.
		tsMin = hlc.Timestamp{WallTime: 0, Logical: 1}
		ts1   = hlc.Timestamp{WallTime: 1, Logical: 0}
		ts2   = hlc.Timestamp{WallTime: 2, Logical: 0}
		ts3   = hlc.Timestamp{WallTime: 3, Logical: 0}
		ts4   = hlc.Timestamp{WallTime: 4, Logical: 0}
		tsMax = hlc.Timestamp{WallTime: math.MaxInt64, Logical: 0}
	)

	kv1_1_1 := makeKVT(testKey1, testValue1, ts1)
	kv1_2_2 := makeKVT(testKey1, testValue2, ts2)
	kv2_2_2 := makeKVT(testKey2, testValue3, ts2)
	kv1_3Deleted := makeKVT(testKey1, nil, ts3)

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			t.Run("empty", func(t *testing.T) {
				assertNextKeyIgnoreTimeIteratedKVs(t, e, localMax, keyMax, tsMin, ts3, nil)
			})

			for _, kv := range kvs(kv1_1_1, kv1_2_2, kv2_2_2) {
				v := roachpb.Value{RawBytes: kv.Value}
				if err := MVCCPut(ctx, e, nil, kv.Key.Key, kv.Key.Timestamp, v, nil); err != nil {
					t.Fatal(err)
				}
			}

			// Exercise time ranges.
			t.Run("ts (0-0]", func(t *testing.T) {
				assertNextKeyIgnoreTimeIteratedKVs(t, e, localMax, keyMax, tsMin, tsMin, nil)
			})
			// Returns the kv_2_2_2 even though it is outside (startTime, endTime].
			t.Run("ts (0-1]", func(t *testing.T) {
				assertNextKeyIgnoreTimeIteratedKVs(t, e, localMax, keyMax, tsMin, ts1, kvs(kv1_1_1, kv2_2_2))
			})
			t.Run("ts (0-∞]", func(t *testing.T) {
				assertNextKeyIgnoreTimeIteratedKVs(t, e, localMax, keyMax, tsMin, tsMax, kvs(kv1_2_2, kv2_2_2))
			})
			t.Run("ts (1-1]", func(t *testing.T) {
				assertNextKeyIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts1, ts1, nil)
			})
			t.Run("ts (1-2]", func(t *testing.T) {
				assertNextKeyIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts1, ts2, kvs(kv1_2_2, kv2_2_2))
			})
			t.Run("ts (2-2]", func(t *testing.T) {
				assertNextKeyIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts2, ts2, nil)
			})

			// Exercise key ranges.
			t.Run("kv [1-1)", func(t *testing.T) {
				assertNextKeyIgnoreTimeIteratedKVs(t, e, testKey1, testKey1, tsMin, tsMax, nil)
			})
			t.Run("kv [1-2)", func(t *testing.T) {
				assertNextKeyIgnoreTimeIteratedKVs(t, e, testKey1, testKey2, tsMin, tsMax, kvs(kv1_2_2))
			})

			// Exercise deletion.
			if err := MVCCDelete(ctx, e, nil, testKey1, ts3, nil); err != nil {
				t.Fatal(err)
			}
			// Returns the kv_1_1_1 even though it is outside (startTime, endTime].
			t.Run("del", func(t *testing.T) {
				assertNextKeyIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts1, tsMax, kvs(kv1_3Deleted,
					kv2_2_2))
			})

			// Insert an intent of testKey2.
			txn1ID := uuid.MakeV4()
			txn1 := roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					Key:            testKey2,
					ID:             txn1ID,
					Epoch:          1,
					WriteTimestamp: ts4,
				},
				ReadTimestamp: ts4,
			}
			txn1Val := roachpb.Value{RawBytes: testValue4}
			if err := MVCCPut(ctx, e, nil, txn1.TxnMeta.Key, txn1.ReadTimestamp, txn1Val, &txn1); err != nil {
				t.Fatal(err)
			}

			// We have to be careful that we are testing the intent handling logic of
			// NextIgnoreTime() rather than the first SeekGE(). We do this by
			// ensuring that the SeekGE() doesn't encounter an intent.
			t.Run("intents", func(t *testing.T) {
				nextKeyIgnoreTimeExpectErr(t, e, testKey1, testKey2.PrefixEnd(), tsMin, tsMax, "conflicting intents")
			})
			t.Run("intents", func(t *testing.T) {
				nextKeyIgnoreTimeExpectErr(t, e, localMax, keyMax, tsMin, ts4, "conflicting intents")
			})
			// Intents above the upper time bound or beneath the lower time bound must
			// be ignored. Note that the lower time bound is exclusive while the upper
			// time bound is inclusive.
			//
			// The intent at ts=4 for kv2 lies outside the timespan
			// (startTime, endTime] so we do not raise an error and just move on to
			// its versioned KV.
			t.Run("intents", func(t *testing.T) {
				assertNextKeyIgnoreTimeIteratedKVs(t, e, localMax, keyMax, tsMin, ts3, kvs(kv1_3Deleted,
					kv2_2_2))
			})
			t.Run("intents", func(t *testing.T) {
				assertNextKeyIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts4, tsMax, kvs())
			})
			t.Run("intents", func(t *testing.T) {
				assertNextKeyIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts4.Next(), tsMax, kvs())
			})
		})
	}
}

func TestMVCCIncrementalIteratorInlinePolicy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var (
		keyMax   = roachpb.KeyMax
		testKey1 = roachpb.Key("/db1")
		testKey2 = roachpb.Key("/db2")

		testValue1 = []byte("val1")
		testValue2 = []byte("val2")

		// Use a non-zero min, since we use IsEmpty to decide if a ts should be used
		// as upper/lower-bound during iterator initialization.
		tsMin = hlc.Timestamp{WallTime: 0, Logical: 1}
		ts1   = hlc.Timestamp{WallTime: 1, Logical: 0}
		ts2   = hlc.Timestamp{WallTime: 2, Logical: 0}
		tsMax = hlc.Timestamp{WallTime: math.MaxInt64, Logical: 0}
	)

	inline1_1_1 := makeKVT(testKey1, testValue1, hlc.Timestamp{})
	kv2_1_1 := makeKVT(testKey2, testValue1, ts1)
	kv2_2_2 := makeKVT(testKey2, testValue2, ts2)

	for _, engineImpl := range mvccEngineImpls {
		e := engineImpl.create()
		defer e.Close()
		for _, kv := range []MVCCKeyValue{inline1_1_1, kv2_1_1, kv2_2_2} {
			v := roachpb.Value{RawBytes: kv.Value}
			if err := MVCCPut(ctx, e, nil, kv.Key.Key, kv.Key.Timestamp, v, nil); err != nil {
				t.Fatal(err)
			}
		}
		t.Run(engineImpl.name, func(t *testing.T) {
			t.Run("PolicyError returns error if inline value is found", func(t *testing.T) {
				iter := NewMVCCIncrementalIterator(e, MVCCIncrementalIterOptions{
					EndKey:       keyMax,
					StartTime:    tsMin,
					EndTime:      tsMax,
					InlinePolicy: MVCCIncrementalIterInlinePolicyError,
				})
				defer iter.Close()
				iter.SeekGE(MakeMVCCMetadataKey(testKey1))
				_, err := iter.Valid()
				assert.EqualError(t, err, "unexpected inline value found: \"/db1\"")
			})
			t.Run("PolicyEmit returns inline values to caller", func(t *testing.T) {
				iter := NewMVCCIncrementalIterator(e, MVCCIncrementalIterOptions{
					EndKey:       keyMax,
					StartTime:    tsMin,
					EndTime:      tsMax,
					InlinePolicy: MVCCIncrementalIterInlinePolicyEmit,
				})
				defer iter.Close()
				iter.SeekGE(MakeMVCCMetadataKey(testKey1))
				expectInlineKeyValue(t, iter, inline1_1_1)
				iter.Next()
				expectKeyValue(t, iter, kv2_2_2)
				iter.Next()
				expectKeyValue(t, iter, kv2_1_1)

			})
		})
	}
}

func TestMVCCIncrementalIteratorIntentPolicy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var (
		keyMax   = roachpb.KeyMax
		testKey1 = roachpb.Key("/db1")
		testKey2 = roachpb.Key("/db2")

		testValue1 = []byte("val1")
		testValue2 = []byte("val2")
		testValue3 = []byte("val3")

		// Use a non-zero min, since we use IsEmpty to decide if a ts should be used
		// as upper/lower-bound during iterator initialization.
		tsMin = hlc.Timestamp{WallTime: 0, Logical: 1}
		ts1   = hlc.Timestamp{WallTime: 1, Logical: 0}
		ts2   = hlc.Timestamp{WallTime: 2, Logical: 0}
		ts3   = hlc.Timestamp{WallTime: 3, Logical: 0}
		tsMax = hlc.Timestamp{WallTime: math.MaxInt64, Logical: 0}
	)

	makeTxn := func(key roachpb.Key, val []byte, ts hlc.Timestamp) (roachpb.Transaction, roachpb.Value, roachpb.Intent) {
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
			}, roachpb.MakeIntent(&txnMeta, key)
	}

	kv1_1_1 := makeKVT(testKey1, testValue1, ts1)
	kv1_2_2 := makeKVT(testKey1, testValue2, ts2)
	kv1_3_3 := makeKVT(testKey1, testValue3, ts3)
	kv2_1_1 := makeKVT(testKey2, testValue1, ts1)
	kv2_2_2 := makeKVT(testKey2, testValue2, ts2)
	txn, val, intent2_2_2 := makeTxn(testKey2, testValue2, ts2)

	intentErr := &roachpb.WriteIntentError{Intents: []roachpb.Intent{intent2_2_2}}

	for _, engineImpl := range mvccEngineImpls {
		e := engineImpl.create()
		defer e.Close()
		for _, kv := range []MVCCKeyValue{kv1_1_1, kv1_2_2, kv1_3_3, kv2_1_1} {
			v := roachpb.Value{RawBytes: kv.Value}
			if err := MVCCPut(ctx, e, nil, kv.Key.Key, kv.Key.Timestamp, v, nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := MVCCPut(ctx, e, nil, txn.TxnMeta.Key, txn.ReadTimestamp, val, &txn); err != nil {
			t.Fatal(err)
		}
		t.Run(engineImpl.name, func(t *testing.T) {
			t.Run("PolicyError returns error if an intent is in the time range", func(t *testing.T) {
				iter := NewMVCCIncrementalIterator(e, MVCCIncrementalIterOptions{
					EndKey:       keyMax,
					StartTime:    tsMin,
					EndTime:      tsMax,
					IntentPolicy: MVCCIncrementalIterIntentPolicyError,
				})
				defer iter.Close()
				iter.SeekGE(MakeMVCCMetadataKey(testKey1))
				for ; ; iter.Next() {
					if ok, _ := iter.Valid(); !ok || iter.UnsafeKey().Key.Compare(keyMax) >= 0 {
						break
					}
				}
				_, err := iter.Valid()
				assert.EqualError(t, err, intentErr.Error())
			})
			t.Run("PolicyError ignores intents outside of time range", func(t *testing.T) {
				iter := NewMVCCIncrementalIterator(e, MVCCIncrementalIterOptions{
					EndKey:       keyMax,
					StartTime:    ts2,
					EndTime:      tsMax,
					IntentPolicy: MVCCIncrementalIterIntentPolicyError,
				})
				defer iter.Close()
				iter.SeekGE(MakeMVCCMetadataKey(testKey1))
				expectKeyValue(t, iter, kv1_3_3)
				iter.Next()
				valid, err := iter.Valid()
				assert.NoError(t, err)
				assert.False(t, valid)
			})
			t.Run("PolicyEmit returns inline values to caller", func(t *testing.T) {
				iter := NewMVCCIncrementalIterator(e, MVCCIncrementalIterOptions{
					EndKey:       keyMax,
					StartTime:    tsMin,
					EndTime:      tsMax,
					IntentPolicy: MVCCIncrementalIterIntentPolicyEmit,
				})
				defer iter.Close()
				iter.SeekGE(MakeMVCCMetadataKey(testKey1))
				for _, kv := range []MVCCKeyValue{kv1_3_3, kv1_2_2, kv1_1_1} {
					expectKeyValue(t, iter, kv)
					iter.Next()
				}
				expectIntent(t, iter, intent2_2_2)
				iter.Next()
				expectKeyValue(t, iter, kv2_2_2)
				iter.Next()
				expectKeyValue(t, iter, kv2_1_1)

			})
			t.Run("PolicyEmit ignores intents outside of time range", func(t *testing.T) {
				iter := NewMVCCIncrementalIterator(e, MVCCIncrementalIterOptions{
					EndKey:       keyMax,
					StartTime:    ts2,
					EndTime:      tsMax,
					IntentPolicy: MVCCIncrementalIterIntentPolicyEmit,
				})
				defer iter.Close()
				iter.SeekGE(MakeMVCCMetadataKey(testKey1))
				expectKeyValue(t, iter, kv1_3_3)
				iter.Next()
				valid, err := iter.Valid()
				assert.NoError(t, err)
				assert.False(t, valid)
			})
		})
	}
}

func expectKeyValue(t *testing.T, iter SimpleMVCCIterator, kv MVCCKeyValue) {
	valid, err := iter.Valid()
	assert.True(t, valid, "expected valid iterator")
	assert.NoError(t, err)

	unsafeKey := iter.UnsafeKey()
	unsafeVal := iter.UnsafeValue()

	assert.True(t, unsafeKey.Key.Equal(kv.Key.Key), "keys not equal")
	assert.Equal(t, kv.Key.Timestamp, unsafeKey.Timestamp)
	assert.Equal(t, kv.Value, unsafeVal)

}

func expectInlineKeyValue(t *testing.T, iter SimpleMVCCIterator, kv MVCCKeyValue) {
	valid, err := iter.Valid()
	assert.True(t, valid)
	assert.NoError(t, err)

	unsafeKey := iter.UnsafeKey()
	unsafeVal := iter.UnsafeValue()

	var meta enginepb.MVCCMetadata
	err = protoutil.Unmarshal(unsafeVal, &meta)
	require.NoError(t, err)

	assert.True(t, meta.IsInline())
	assert.False(t, unsafeKey.IsValue())
	assert.True(t, unsafeKey.Key.Equal(kv.Key.Key))
	assert.Equal(t, kv.Value, meta.RawBytes)
}

func expectIntent(t *testing.T, iter SimpleMVCCIterator, intent roachpb.Intent) {
	valid, err := iter.Valid()
	assert.True(t, valid)
	assert.NoError(t, err)

	unsafeKey := iter.UnsafeKey()
	unsafeVal := iter.UnsafeValue()

	var meta enginepb.MVCCMetadata
	err = protoutil.Unmarshal(unsafeVal, &meta)
	require.NoError(t, err)
	assert.NotNil(t, meta.Txn)

	assert.False(t, unsafeKey.IsValue())
	assert.True(t, unsafeKey.Key.Equal(intent.Key))
	assert.Equal(t, meta.Timestamp, intent.Txn.WriteTimestamp.ToLegacyTimestamp())

}

func TestMVCCIncrementalIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var (
		keyMax   = roachpb.KeyMax
		testKey1 = roachpb.Key("/db1")
		testKey2 = roachpb.Key("/db2")

		testValue1 = []byte("val1")
		testValue2 = []byte("val2")
		testValue3 = []byte("val3")
		testValue4 = []byte("val4")

		// Use a non-zero min, since we use IsEmpty to decide if a ts should be used
		// as upper/lower-bound during iterator initialization.
		tsMin = hlc.Timestamp{WallTime: 0, Logical: 1}
		ts1   = hlc.Timestamp{WallTime: 1, Logical: 0}
		ts2   = hlc.Timestamp{WallTime: 2, Logical: 0}
		ts3   = hlc.Timestamp{WallTime: 3, Logical: 0}
		ts4   = hlc.Timestamp{WallTime: 4, Logical: 0}
		tsMax = hlc.Timestamp{WallTime: math.MaxInt64, Logical: 0}
	)

	// Keys are named as kv<key>_<value>_<ts>.
	kv1_1_1 := makeKVT(testKey1, testValue1, ts1)
	kv1_4_4 := makeKVT(testKey1, testValue4, ts4)
	kv1_2_2 := makeKVT(testKey1, testValue2, ts2)
	kv2_2_2 := makeKVT(testKey2, testValue3, ts2)
	kv1Deleted3 := makeKVT(testKey1, nil, ts3)

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name+"-latest", func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			t.Run("empty", assertEqualKVs(e, localMax, keyMax, tsMin, ts3, latest, nil))

			for _, kv := range kvs(kv1_1_1, kv1_2_2, kv2_2_2) {
				v := roachpb.Value{RawBytes: kv.Value}
				if err := MVCCPut(ctx, e, nil, kv.Key.Key, kv.Key.Timestamp, v, nil); err != nil {
					t.Fatal(err)
				}
			}

			// Exercise time ranges.
			t.Run("ts (0-0]", assertEqualKVs(e, localMax, keyMax, tsMin, tsMin, latest, nil))
			t.Run("ts (0-1]", assertEqualKVs(e, localMax, keyMax, tsMin, ts1, latest, kvs(kv1_1_1)))
			t.Run("ts (0-∞]", assertEqualKVs(e, localMax, keyMax, tsMin, tsMax, latest, kvs(kv1_2_2, kv2_2_2)))
			t.Run("ts (1-1]", assertEqualKVs(e, localMax, keyMax, ts1, ts1, latest, nil))
			t.Run("ts (1-2]", assertEqualKVs(e, localMax, keyMax, ts1, ts2, latest, kvs(kv1_2_2, kv2_2_2)))
			t.Run("ts (2-2]", assertEqualKVs(e, localMax, keyMax, ts2, ts2, latest, nil))

			// Exercise key ranges.
			t.Run("kv [1-1)", assertEqualKVs(e, testKey1, testKey1, tsMin, tsMax, latest, nil))
			t.Run("kv [1-2)", assertEqualKVs(e, testKey1, testKey2, tsMin, tsMax, latest, kvs(kv1_2_2)))

			// Exercise deletion.
			if err := MVCCDelete(ctx, e, nil, testKey1, ts3, nil); err != nil {
				t.Fatal(err)
			}
			t.Run("del", assertEqualKVs(e, localMax, keyMax, ts1, tsMax, latest, kvs(kv1Deleted3, kv2_2_2)))

			// Exercise intent handling.
			txn1, txn1Val, intentErr1 := makeKVTxn(testKey1, testValue4, ts4)
			if err := MVCCPut(ctx, e, nil, txn1.TxnMeta.Key, txn1.ReadTimestamp, txn1Val, &txn1); err != nil {
				t.Fatal(err)
			}
			txn2, txn2Val, intentErr2 := makeKVTxn(testKey2, testValue4, ts4)
			if err := MVCCPut(ctx, e, nil, txn2.TxnMeta.Key, txn2.ReadTimestamp, txn2Val, &txn2); err != nil {
				t.Fatal(err)
			}
			t.Run("intents-1",
				iterateExpectErr(e, testKey1, testKey1.PrefixEnd(), tsMin, tsMax, latest, intents(intentErr1)))
			t.Run("intents-2",
				iterateExpectErr(e, testKey2, testKey2.PrefixEnd(), tsMin, tsMax, latest, intents(intentErr2)))
			t.Run("intents-multi",
				iterateExpectErr(e, localMax, keyMax, tsMin, ts4, latest, intents(intentErr1, intentErr2)))
			// Intents above the upper time bound or beneath the lower time bound must
			// be ignored (#28358). Note that the lower time bound is exclusive while
			// the upper time bound is inclusive.
			t.Run("intents-filtered-1", assertEqualKVs(e, localMax, keyMax, tsMin, ts3, latest, kvs(kv1Deleted3, kv2_2_2)))
			t.Run("intents-filtered-2", assertEqualKVs(e, localMax, keyMax, ts4, tsMax, latest, kvs()))
			t.Run("intents-filtered-3", assertEqualKVs(e, localMax, keyMax, ts4.Next(), tsMax, latest, kvs()))

			intent1 := roachpb.MakeLockUpdate(&txn1, roachpb.Span{Key: testKey1})
			intent1.Status = roachpb.COMMITTED
			if _, err := MVCCResolveWriteIntent(ctx, e, nil, intent1); err != nil {
				t.Fatal(err)
			}
			intent2 := roachpb.MakeLockUpdate(&txn2, roachpb.Span{Key: testKey2})
			intent2.Status = roachpb.ABORTED
			if _, err := MVCCResolveWriteIntent(ctx, e, nil, intent2); err != nil {
				t.Fatal(err)
			}
			t.Run("intents-resolved", assertEqualKVs(e, localMax, keyMax, tsMin, tsMax, latest, kvs(kv1_4_4, kv2_2_2)))
		})
	}

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name+"-all", func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			t.Run("empty", assertEqualKVs(e, localMax, keyMax, tsMin, ts3, all, nil))

			for _, kv := range kvs(kv1_1_1, kv1_2_2, kv2_2_2) {
				v := roachpb.Value{RawBytes: kv.Value}
				if err := MVCCPut(ctx, e, nil, kv.Key.Key, kv.Key.Timestamp, v, nil); err != nil {
					t.Fatal(err)
				}
			}

			// Exercise time ranges.
			t.Run("ts (0-0]", assertEqualKVs(e, localMax, keyMax, tsMin, tsMin, all, nil))
			t.Run("ts (0-1]", assertEqualKVs(e, localMax, keyMax, tsMin, ts1, all, kvs(kv1_1_1)))
			t.Run("ts (0-∞]", assertEqualKVs(e, localMax, keyMax, tsMin, tsMax, all, kvs(kv1_2_2, kv1_1_1, kv2_2_2)))
			t.Run("ts (1-1]", assertEqualKVs(e, localMax, keyMax, ts1, ts1, all, nil))
			t.Run("ts (1-2]", assertEqualKVs(e, localMax, keyMax, ts1, ts2, all, kvs(kv1_2_2, kv2_2_2)))
			t.Run("ts (2-2]", assertEqualKVs(e, localMax, keyMax, ts2, ts2, all, nil))

			// Exercise key ranges.
			t.Run("kv [1-1)", assertEqualKVs(e, testKey1, testKey1, tsMin, tsMax, all, nil))
			t.Run("kv [1-2)", assertEqualKVs(e, testKey1, testKey2, tsMin, tsMax, all, kvs(kv1_2_2, kv1_1_1)))

			// Exercise deletion.
			if err := MVCCDelete(ctx, e, nil, testKey1, ts3, nil); err != nil {
				t.Fatal(err)
			}
			t.Run("del", assertEqualKVs(e, localMax, keyMax, ts1, tsMax, all, kvs(kv1Deleted3, kv1_2_2, kv2_2_2)))

			// Exercise intent handling.
			txn1, txn1Val, intentErr1 := makeKVTxn(testKey1, testValue4, ts4)
			if err := MVCCPut(ctx, e, nil, txn1.TxnMeta.Key, txn1.ReadTimestamp, txn1Val, &txn1); err != nil {
				t.Fatal(err)
			}
			txn2, txn2Val, intentErr2 := makeKVTxn(testKey2, testValue4, ts4)
			if err := MVCCPut(ctx, e, nil, txn2.TxnMeta.Key, txn2.ReadTimestamp, txn2Val, &txn2); err != nil {
				t.Fatal(err)
			}
			// Single intent tests are verifying behavior when intent collection is not enabled.
			t.Run("intents-1",
				iterateExpectErr(e, testKey1, testKey1.PrefixEnd(), tsMin, tsMax, all, intents(intentErr1)))
			t.Run("intents-2",
				iterateExpectErr(e, testKey2, testKey2.PrefixEnd(), tsMin, tsMax, all, intents(intentErr2)))
			t.Run("intents-multi",
				iterateExpectErr(e, localMax, keyMax, tsMin, ts4, all, intents(intentErr1, intentErr2)))
			// Intents above the upper time bound or beneath the lower time bound must
			// be ignored (#28358). Note that the lower time bound is exclusive while
			// the upper time bound is inclusive.
			t.Run("intents-filtered-1", assertEqualKVs(e, localMax, keyMax, tsMin, ts3, all, kvs(kv1Deleted3, kv1_2_2, kv1_1_1, kv2_2_2)))
			t.Run("intents-filtered-2", assertEqualKVs(e, localMax, keyMax, ts4, tsMax, all, kvs()))
			t.Run("intents-filtered-3", assertEqualKVs(e, localMax, keyMax, ts4.Next(), tsMax, all, kvs()))

			intent1 := roachpb.MakeLockUpdate(&txn1, roachpb.Span{Key: testKey1})
			intent1.Status = roachpb.COMMITTED
			if _, err := MVCCResolveWriteIntent(ctx, e, nil, intent1); err != nil {
				t.Fatal(err)
			}
			intent2 := roachpb.MakeLockUpdate(&txn2, roachpb.Span{Key: testKey2})
			intent2.Status = roachpb.ABORTED
			if _, err := MVCCResolveWriteIntent(ctx, e, nil, intent2); err != nil {
				t.Fatal(err)
			}
			t.Run("intents-resolved", assertEqualKVs(e, localMax, keyMax, tsMin, tsMax, all, kvs(kv1_4_4, kv1Deleted3, kv1_2_2, kv1_1_1, kv2_2_2)))
		})
	}
}

func slurpKVsInTimeRange(
	reader Reader, prefix roachpb.Key, startTime, endTime hlc.Timestamp,
) ([]MVCCKeyValue, error) {
	endKey := prefix.PrefixEnd()
	iter := NewMVCCIncrementalIterator(reader, MVCCIncrementalIterOptions{
		EndKey:    endKey,
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
	defer log.Scope(t).Close(t)

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
					Key:            roachpb.Key("b"),
					ID:             uuid.MakeV4(),
					Epoch:          1,
					WriteTimestamp: ts1,
					Sequence:       1,
				},
				ReadTimestamp: ts1,
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
				txn.WriteTimestamp = ts3
				txn.Sequence = 2
				// Use a batch since MVCCPut is not atomic when using an Engine and we
				// are not using latches to prevent a concurrent read in the other
				// goroutine. A non-atomic Put can cause the strict invariant checking
				// in intentInterleavingIter to be violated.
				b := e.NewBatch()
				defer b.Close()
				if err := MVCCPut(ctx, b, nil, kA, ts1, vA2, txn); err != nil {
					return err
				}
				return b.Commit(false)
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

// TestMVCCIncrementalIteratorIntentDeletion checks a workaround in
// MVCCIncrementalIterator for a bug in time-bound iterators, where an intent
// has been deleted, but the time-bound iterator doesn't see the deletion.
func TestMVCCIncrementalIteratorIntentDeletion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	txn := func(key roachpb.Key, ts hlc.Timestamp) *roachpb.Transaction {
		return &roachpb.Transaction{
			TxnMeta: enginepb.TxnMeta{
				Key:            key,
				ID:             uuid.MakeV4(),
				Epoch:          1,
				WriteTimestamp: ts,
			},
			ReadTimestamp: ts,
		}
	}
	intent := func(txn *roachpb.Transaction) roachpb.LockUpdate {
		intent := roachpb.MakeLockUpdate(txn, roachpb.Span{Key: txn.Key})
		intent.Status = roachpb.COMMITTED
		return intent
	}

	ctx := context.Background()
	kA := roachpb.Key("kA")
	vA1 := roachpb.MakeValueFromString("vA1")
	vA2 := roachpb.MakeValueFromString("vA2")
	vA3 := roachpb.MakeValueFromString("vA3")
	kB := roachpb.Key("kB")
	vB1 := roachpb.MakeValueFromString("vB1")
	kC := roachpb.Key("kC")
	vC1 := roachpb.MakeValueFromString("vC1")
	ts0 := hlc.Timestamp{WallTime: 0}
	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	ts3 := hlc.Timestamp{WallTime: 3}
	txnA1 := txn(kA, ts1)
	txnA3 := txn(kA, ts3)
	txnB1 := txn(kB, ts1)
	txnC1 := txn(kC, ts1)

	db := createTestPebbleEngine()
	defer db.Close()

	// Set up two sstables very specifically:
	//
	// sst1 (time-bound metadata ts1->ts1)
	// kA -> (intent)
	// kA:1 -> vA1
	// kB -> (intent)
	// kB:1 -> vB1
	// kC -> (intent)
	// kC:1 -> vC1
	//
	// sst2 (time-bound metadata ts2->ts3) the intent deletions are for the
	// intents at ts1, but there's no way know that when constructing the
	// metadata (hence the time-bound iterator bug)
	// kA -> (intent) [NB this overwrites the intent deletion]
	// kA:3 -> vA3
	// kA:2 -> vA2
	// kB -> (intent deletion)
	require.NoError(t, MVCCPut(ctx, db, nil, kA, txnA1.ReadTimestamp, vA1, txnA1))
	require.NoError(t, MVCCPut(ctx, db, nil, kB, txnB1.ReadTimestamp, vB1, txnB1))
	require.NoError(t, MVCCPut(ctx, db, nil, kC, txnC1.ReadTimestamp, vC1, txnC1))
	require.NoError(t, db.Flush())
	require.NoError(t, db.Compact())
	_, err := MVCCResolveWriteIntent(ctx, db, nil, intent(txnA1))
	require.NoError(t, err)
	_, err = MVCCResolveWriteIntent(ctx, db, nil, intent(txnB1))
	require.NoError(t, err)
	require.NoError(t, MVCCPut(ctx, db, nil, kA, ts2, vA2, nil))
	require.NoError(t, MVCCPut(ctx, db, nil, kA, txnA3.WriteTimestamp, vA3, txnA3))
	require.NoError(t, db.Flush())

	// The kA ts1 intent has been resolved. There's now a new intent on kA, but
	// the timestamp (ts3) is too new so it should be ignored.
	kvs, err := slurpKVsInTimeRange(db, kA, ts0, ts1)
	require.NoError(t, err)
	require.Equal(t, []MVCCKeyValue{
		{Key: MVCCKey{Key: kA, Timestamp: ts1}, Value: vA1.RawBytes},
	}, kvs)
	// kA has a value at ts2. Again the intent is too new (ts3), so ignore.
	kvs, err = slurpKVsInTimeRange(db, kA, ts0, ts2)
	require.NoError(t, err)
	require.Equal(t, []MVCCKeyValue{
		{Key: MVCCKey{Key: kA, Timestamp: ts2}, Value: vA2.RawBytes},
		{Key: MVCCKey{Key: kA, Timestamp: ts1}, Value: vA1.RawBytes},
	}, kvs)
	// At ts3, we should see the new intent
	_, err = slurpKVsInTimeRange(db, kA, ts0, ts3)
	require.EqualError(t, err, `conflicting intents on "kA"`)

	// Similar to the kA ts1 check, but there is no newer intent. We expect to
	// pick up the intent deletion and it should cancel out the intent, leaving
	// only the value at ts1.
	kvs, err = slurpKVsInTimeRange(db, kB, ts0, ts1)
	require.NoError(t, err)
	require.Equal(t, []MVCCKeyValue{
		{Key: MVCCKey{Key: kB, Timestamp: ts1}, Value: vB1.RawBytes},
	}, kvs)

	// Sanity check that we see the still unresolved intent for kC ts1.
	_, err = slurpKVsInTimeRange(db, kC, ts0, ts1)
	require.EqualError(t, err, `conflicting intents on "kC"`)
}

func TestMVCCIncrementalIteratorIntentStraddlesSStables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a DB containing 2 keys, a and b, where b has an intent. We use the
	// regular MVCCPut operation to generate these keys, which we'll later be
	// copying into manually created sstables.
	ctx := context.Background()
	db1, err := Open(ctx, InMemory(), ForTesting)
	require.NoError(t, err)
	defer db1.Close()

	put := func(key, value string, ts int64, txn *roachpb.Transaction) {
		v := roachpb.MakeValueFromString(value)
		if err := MVCCPut(
			ctx, db1, nil, roachpb.Key(key), hlc.Timestamp{WallTime: ts}, v, txn,
		); err != nil {
			t.Fatal(err)
		}
	}

	put("a", "a value", 1, nil)
	put("b", "b value", 2, &roachpb.Transaction{
		TxnMeta: enginepb.TxnMeta{
			Key:            roachpb.Key("b"),
			ID:             uuid.MakeV4(),
			Epoch:          1,
			WriteTimestamp: hlc.Timestamp{WallTime: 2},
		},
		ReadTimestamp: hlc.Timestamp{WallTime: 2},
	})

	// Create a second DB in which we'll create a specific SSTable structure: the
	// first SSTable contains 2 KVs where the first is a regular versioned key
	// and the second is the MVCC metadata entry (i.e. an intent). The next
	// SSTable contains the provisional value for the intent. The effect is that
	// the metadata entry is separated from the entry it is metadata for.
	//
	//   SSTable 1:
	//     a@1
	//     b@<meta>
	//
	//   SSTable 2:
	//     b@2
	db2, err := Open(ctx, InMemory(), ForTesting)
	require.NoError(t, err)
	defer db2.Close()

	// NB: If the original intent was separated, iterating using an interleaving
	// iterator, as done below, and writing to an sst, transforms the separated
	// intent to an interleaved intent. This is ok for now since both kinds of
	// intents are supported.
	// TODO(sumeer): change this test before interleaved intents are disallowed.
	ingest := func(it MVCCIterator, count int) {
		memFile := &MemFile{}
		sst := MakeIngestionSSTWriter(ctx, db2.settings, memFile)
		defer sst.Close()

		for i := 0; i < count; i++ {
			ok, err := it.Valid()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("expected key")
			}
			if err := sst.Put(it.Key(), it.Value()); err != nil {
				t.Fatal(err)
			}
			it.Next()
		}
		if err := sst.Finish(); err != nil {
			t.Fatal(err)
		}
		if err := db2.WriteFile(`ingest`, memFile.Data()); err != nil {
			t.Fatal(err)
		}
		if err := db2.IngestExternalFiles(ctx, []string{`ingest`}); err != nil {
			t.Fatal(err)
		}
	}

	{
		// Iterate over the entries in the first DB, ingesting them into SSTables
		// in the second DB.
		it := db1.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
			UpperBound: keys.MaxKey,
		})
		defer it.Close()
		it.SeekGE(MVCCKey{Key: keys.LocalMax})
		ingest(it, 2)
		ingest(it, 1)
	}

	{
		// Use an incremental iterator to simulate an incremental backup from (1,
		// 2]. Note that incremental iterators are exclusive on the start time and
		// inclusive on the end time. The expectation is that we'll see a write
		// intent error.
		it := NewMVCCIncrementalIterator(db2, MVCCIncrementalIterOptions{
			EndKey:    keys.MaxKey,
			StartTime: hlc.Timestamp{WallTime: 1},
			EndTime:   hlc.Timestamp{WallTime: 2},
		})
		defer it.Close()
		for it.SeekGE(MVCCKey{Key: keys.LocalMax}); ; it.Next() {
			ok, err := it.Valid()
			if err != nil {
				if errors.HasType(err, (*roachpb.WriteIntentError)(nil)) {
					// This is the write intent error we were expecting.
					return
				}
				t.Fatalf("%T: %s", err, err)
			}
			if !ok {
				break
			}
		}
		t.Fatalf("expected write intent error, but found success")
	}
}

func TestMVCCIterateTimeBound(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	const numKeys = 1000
	const numBatches = 10
	const batchTimeSpan = 10
	const valueSize = 8

	eng, err := loadTestData(filepath.Join(dir, "mvcc_data"),
		numKeys, numBatches, batchTimeSpan, valueSize)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	for _, testCase := range []struct {
		start hlc.Timestamp
		end   hlc.Timestamp
	}{
		// entire time range
		{hlc.Timestamp{WallTime: 0, Logical: 0}, hlc.Timestamp{WallTime: 110, Logical: 0}},
		// one SST
		{hlc.Timestamp{WallTime: 10, Logical: 0}, hlc.Timestamp{WallTime: 19, Logical: 0}},
		// one SST, plus the min of the following SST
		{hlc.Timestamp{WallTime: 10, Logical: 0}, hlc.Timestamp{WallTime: 20, Logical: 0}},
		// one SST, plus the max of the preceding SST
		{hlc.Timestamp{WallTime: 9, Logical: 0}, hlc.Timestamp{WallTime: 19, Logical: 0}},
		// one SST, plus the min of the following and the max of the preceding SST
		{hlc.Timestamp{WallTime: 9, Logical: 0}, hlc.Timestamp{WallTime: 21, Logical: 0}},
		// one SST, not min or max
		{hlc.Timestamp{WallTime: 17, Logical: 0}, hlc.Timestamp{WallTime: 18, Logical: 0}},
		// one SST's max
		{hlc.Timestamp{WallTime: 18, Logical: 0}, hlc.Timestamp{WallTime: 19, Logical: 0}},
		// one SST's min
		{hlc.Timestamp{WallTime: 19, Logical: 0}, hlc.Timestamp{WallTime: 20, Logical: 0}},
		// random endpoints
		{hlc.Timestamp{WallTime: 32, Logical: 0}, hlc.Timestamp{WallTime: 78, Logical: 0}},
	} {
		t.Run(fmt.Sprintf("%s-%s", testCase.start, testCase.end), func(t *testing.T) {
			defer leaktest.AfterTest(t)()

			expectedKVs := collectMatchingWithMVCCIterator(t, eng, testCase.start, testCase.end)

			assertEqualKVs(eng, keys.LocalMax, keys.MaxKey, testCase.start, testCase.end, latest, expectedKVs)(t)
		})
	}
}

func collectMatchingWithMVCCIterator(
	t *testing.T, eng Engine, start, end hlc.Timestamp,
) []MVCCKeyValue {
	var expectedKVs []MVCCKeyValue
	iter := eng.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
	defer iter.Close()
	iter.SeekGE(MVCCKey{Key: localMax})
	for {
		ok, err := iter.Valid()
		if err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}
		ts := iter.Key().Timestamp
		if (ts.Less(end) || end == ts) && start.Less(ts) {
			expectedKVs = append(expectedKVs, MVCCKeyValue{Key: iter.Key(), Value: iter.Value()})
		}
		iter.Next()
	}
	if len(expectedKVs) < 1 {
		t.Fatalf("source of truth had no expected KVs; likely a bug in the test itself")
	}
	return expectedKVs
}

func runIncrementalBenchmark(
	b *testing.B, emk engineMaker, useTBI bool, ts hlc.Timestamp, opts benchDataOptions,
) {
	eng, _ := setupMVCCData(context.Background(), b, emk, opts)
	{
		// Pull all of the sstables into the cache.  This
		// probably defeats a lot of the benefits of the
		// time-based optimization.
		iter := eng.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
		_, _ = iter.ComputeStats(keys.LocalMax, roachpb.KeyMax, 0)
		iter.Close()
	}
	defer eng.Close()

	startKey := roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(0)))
	endKey := roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(opts.numKeys)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := NewMVCCIncrementalIterator(eng, MVCCIncrementalIterOptions{
			EnableTimeBoundIteratorOptimization: useTBI,
			EndKey:                              endKey,
			StartTime:                           ts,
			EndTime:                             hlc.MaxTimestamp,
		})
		defer it.Close()
		it.SeekGE(MVCCKey{Key: startKey})
		for {
			if ok, err := it.Valid(); err != nil {
				b.Fatalf("failed incremental iteration: %+v", err)
			} else if !ok {
				break
			}
			it.Next()
		}
	}
}

func BenchmarkMVCCIncrementalIterator(b *testing.B) {
	defer log.Scope(b).Close(b)
	numVersions := 100
	numKeys := 1000
	// Mean of 50 versions * 1000 bytes results in more than one block per
	// versioned key, so there is some chance of
	// EnableTimeBoundIteratorOptimization=true being useful.
	valueBytes := 1000

	setupMVCCPebbleWithBlockProperties := func(b testing.TB, dir string) Engine {
		peb, err := Open(
			context.Background(),
			Filesystem(dir),
			CacheSize(testCacheSize),
			func(cfg *engineConfig) error {
				cfg.Opts.FormatMajorVersion = pebble.FormatBlockPropertyCollector
				return nil
			})

		if err != nil {
			b.Fatalf("could not create new pebble instance at %s: %+v", dir, err)
		}
		return peb
	}
	for _, useTBI := range []bool{true, false} {
		b.Run(fmt.Sprintf("useTBI=%v", useTBI), func(b *testing.B) {
			for _, tsExcludePercent := range []float64{0, 0.95} {
				wallTime := int64((5 * (float64(numVersions)*tsExcludePercent + 1)))
				ts := hlc.Timestamp{WallTime: wallTime}
				b.Run(fmt.Sprintf("ts=%d", ts.WallTime), func(b *testing.B) {
					runIncrementalBenchmark(b, setupMVCCPebbleWithBlockProperties, useTBI, ts, benchDataOptions{
						numVersions: numVersions,
						numKeys:     numKeys,
						valueBytes:  valueBytes,
					})
				})
			}
		})
	}
}

// BenchmarkMVCCIncrementalIteratorForOldData is a benchmark for the case of
// finding old data when most data is in L6. This uses the MVCC timestamp to
// define age, for convenience, though it could be a different field in the
// key if one wrote a BlockPropertyCollector that could parse the key to find
// the field (for instance the crdb_internal_ttl_expiration used in
// https://github.com/cockroachdb/cockroach/pull/70241).
func BenchmarkMVCCIncrementalIteratorForOldData(b *testing.B) {
	defer log.Scope(b).Close(b)

	numKeys := 10000
	// 1 in 400 keys is being looked for. Roughly corresponds to a TTL of
	// slightly longer than 1 year, where each day, we run a pass to expire 1
	// day of keys. The old keys are uniformly distributed in the key space,
	// which is the worst case for block property filters.
	keyAgeInterval := 400
	setupMVCCPebbleWithBlockProperties := func(b *testing.B) Engine {
		eng, err := Open(
			context.Background(),
			InMemory(),
			// Use a small cache size. Scanning large tables with mostly cold data
			// will mostly miss the cache (especially since the block cache is meant
			// to be scan resistant).
			CacheSize(1<<10),
			func(cfg *engineConfig) error {
				cfg.Opts.FormatMajorVersion = pebble.FormatBlockPropertyCollector
				return nil
			})
		if err != nil {
			b.Fatal(err)
		}
		return eng
	}

	baseTimestamp := int64(1000)
	setupData := func(b *testing.B, eng Engine, valueSize int) {
		// Generate the same data every time.
		rng := rand.New(rand.NewSource(1449168817))
		batch := eng.NewBatch()
		for i := 0; i < numKeys; i++ {
			if (i+1)%100 == 0 {
				if err := batch.Commit(false /* sync */); err != nil {
					b.Fatal(err)
				}
				batch.Close()
				batch = eng.NewBatch()
			}
			key := encoding.EncodeUvarintAscending([]byte("key-"), uint64(i))
			value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
			value.InitChecksum(key)
			ts := hlc.Timestamp{WallTime: baseTimestamp + 100*int64(i%keyAgeInterval)}
			if err := MVCCPut(
				context.Background(), batch, nil /* ms */, key, ts, value, nil); err != nil {
				b.Fatal(err)
			}
		}
		if err := eng.Flush(); err != nil {
			b.Fatal(err)
		}
		if err := eng.Compact(); err != nil {
			b.Fatal(err)
		}
	}

	for _, valueSize := range []int{100, 500, 1000, 2000} {
		eng := setupMVCCPebbleWithBlockProperties(b)
		setupData(b, eng, valueSize)
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			for _, useTBI := range []bool{true, false} {
				b.Run(fmt.Sprintf("useTBI=%t", useTBI), func(b *testing.B) {
					startKey := roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(0)))
					endKey := roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(numKeys)))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						it := NewMVCCIncrementalIterator(eng, MVCCIncrementalIterOptions{
							EnableTimeBoundIteratorOptimization: useTBI,
							EndKey:                              endKey,
							StartTime:                           hlc.Timestamp{},
							EndTime:                             hlc.Timestamp{WallTime: baseTimestamp},
						})
						it.SeekGE(MVCCKey{Key: startKey})
						for {
							if ok, err := it.Valid(); err != nil {
								b.Fatalf("failed incremental iteration: %+v", err)
							} else if !ok {
								break
							}
							it.Next()
						}
						it.Close()
					}
				})
			}
		})
		eng.Close()
	}
}
