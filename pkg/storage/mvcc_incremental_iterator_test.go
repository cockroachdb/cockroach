// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// See also tests in testdata/mvcc_histories/range_key_iter_incremental which
// cover iteration with range keys.

const all, latest = true, false

func makeKVT(key roachpb.Key, value roachpb.Value, ts hlc.Timestamp) MVCCKeyValue {
	return MVCCKeyValue{Key: MVCCKey{Key: key, Timestamp: ts}, Value: value.RawBytes}
}

func makeKVTxn(key roachpb.Key, ts hlc.Timestamp) (roachpb.Transaction, roachpb.Intent) {
	txnID := uuid.MakeV4()
	txnMeta := enginepb.TxnMeta{
		Key:            key,
		ID:             txnID,
		Epoch:          1,
		WriteTimestamp: ts,
	}
	txn := roachpb.Transaction{
		TxnMeta:       txnMeta,
		ReadTimestamp: ts,
	}
	intent := roachpb.MakeIntent(&txnMeta, key)
	return txn, intent
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
			assertExpectErrs(t, e, startKey, endKey, startTime, endTime, revisions, intents, MaxConflictsPerLockConflictErrorDefault)
		})
		// Test case to check if iterator enforces maxLockConflict.
		t.Run("aggregate-intents-with-limit", func(t *testing.T) {
			if len(intents) > 0 {
				assertExpectErrs(t, e, startKey, endKey, startTime, endTime, revisions, intents[:1], 1)
			}
		})
		t.Run("first-intent", func(t *testing.T) {
			assertExpectErr(t, e, startKey, endKey, startTime, endTime, revisions, intents[0])
		})
		t.Run("export-intents", func(t *testing.T) {
			assertExportedErrs(t, e, startKey, endKey, startTime, endTime, revisions, intents)
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
	iter, err := NewMVCCIncrementalIterator(context.Background(), e, MVCCIncrementalIterOptions{
		EndKey:    endKey,
		StartTime: startTime,
		EndTime:   endTime,
	})
	if err != nil {
		t.Fatal(err)
	}
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

	_, err = iter.Valid()
	if lcErr := (*kvpb.LockConflictError)(nil); errors.As(err, &lcErr) {
		if !expectedIntent.Key.Equal(lcErr.Locks[0].Key) {
			t.Fatalf("Expected intent key %v, but got %v", expectedIntent.Key, lcErr.Locks[0].Key)
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
	maxLockConflicts uint64,
) {
	iter, err := NewMVCCIncrementalIterator(context.Background(), e, MVCCIncrementalIterOptions{
		EndKey:           endKey,
		StartTime:        startTime,
		EndTime:          endTime,
		IntentPolicy:     MVCCIncrementalIterIntentPolicyAggregate,
		MaxLockConflicts: maxLockConflicts,
	})
	if err != nil {
		t.Fatal(err)
	}
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

	if len(iter.intents) != len(expectedIntents) {
		t.Fatalf("Expected %d intents but found %d", len(expectedIntents), len(iter.intents))
	}
	err = iter.TryGetIntentError()
	if lcErr := (*kvpb.LockConflictError)(nil); errors.As(err, &lcErr) {
		for i := range expectedIntents {
			if !expectedIntents[i].Key.Equal(lcErr.Locks[i].Key) {
				t.Fatalf("%d intent key: got %v, expected %v", i, lcErr.Locks[i].Key, expectedIntents[i].Key)
			}
			if !expectedIntents[i].Txn.ID.Equal(lcErr.Locks[i].Txn.ID) {
				t.Fatalf("%d intent key: got %v, expected %v", i, lcErr.Locks[i].Txn.ID, expectedIntents[i].Txn.ID)
			}
		}
	} else {
		t.Fatalf("Expected kvpb.LockConflictError, found %T", err)
	}
}

func assertExportedErrs(
	t *testing.T,
	e Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	revisions bool,
	expectedIntents []roachpb.Intent,
) {
	const big = 1 << 30
	st := cluster.MakeTestingClusterSettings()
	_, _, err := MVCCExportToSST(context.Background(), st, e, MVCCExportOptions{
		StartKey:                MVCCKey{Key: startKey},
		EndKey:                  endKey,
		StartTS:                 startTime,
		EndTS:                   endTime,
		ExportAllRevisions:      revisions,
		TargetSize:              big,
		MaxSize:                 big,
		MaxLockConflicts:        uint64(MaxConflictsPerLockConflictError.Default()),
		TargetLockConflictBytes: uint64(TargetBytesPerLockConflictError.Default()),
		StopMidKey:              false,
	}, &bytes.Buffer{})
	require.Error(t, err)

	if lcErr := (*kvpb.LockConflictError)(nil); errors.As(err, &lcErr) {
		for i := range expectedIntents {
			if !expectedIntents[i].Key.Equal(lcErr.Locks[i].Key) {
				t.Fatalf("%d intent key: got %v, expected %v", i, lcErr.Locks[i].Key, expectedIntents[i].Key)
			}
			if !expectedIntents[i].Txn.ID.Equal(lcErr.Locks[i].Txn.ID) {
				t.Fatalf("%d intent key: got %v, expected %v", i, lcErr.Locks[i].Txn.ID, expectedIntents[i].Txn.ID)
			}
		}
	} else {
		t.Fatalf("Expected kvpb.LockConflictError, found %T", err)
	}
}

func assertExportedKVs(
	t *testing.T,
	e Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	revisions bool,
	expected []MVCCKeyValue,
) {
	const big = 1 << 30
	var sstFile bytes.Buffer
	st := cluster.MakeTestingClusterSettings()
	_, _, err := MVCCExportToSST(context.Background(), st, e, MVCCExportOptions{
		StartKey:           MVCCKey{Key: startKey},
		EndKey:             endKey,
		StartTS:            startTime,
		EndTS:              endTime,
		ExportAllRevisions: revisions,
		TargetSize:         big,
		MaxSize:            big,
		StopMidKey:         false,
	}, &sstFile)
	require.NoError(t, err)
	data := sstFile.Bytes()
	if data == nil {
		require.Nil(t, expected)
		return
	}
	sst, err := NewMemSSTIterator(data, false, IterOptions{
		LowerBound: keys.MinKey,
		UpperBound: keys.MaxKey,
	})
	require.NoError(t, err)
	defer sst.Close()

	sst.SeekGE(MVCCKey{})
	checkValErr := func(v []byte, err error) []byte {
		require.NoError(t, err)
		return v
	}
	for i := range expected {
		ok, err := sst.Valid()
		require.NoError(t, err)
		require.Truef(t, ok, "iteration produced %d keys, expected %d", i, len(expected))
		assert.Equalf(t, expected[i].Key, sst.UnsafeKey(), "key %d", i)
		if expected[i].Value == nil {
			assert.Equalf(t, []byte{}, checkValErr(sst.UnsafeValue()), "key %d %q", i, sst.UnsafeKey())
		} else {
			assert.Equalf(
				t, expected[i].Value, checkValErr(sst.UnsafeValue()), "key %d %q", i, sst.UnsafeKey())
		}
		sst.Next()
	}
	ok, err := sst.Valid()
	require.NoError(t, err)
	require.False(t, ok)
}

func ignoreTimeExpectErr(
	t *testing.T,
	e Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	errString string,
	nextKey bool,
) {
	iter, err := NewMVCCIncrementalIterator(context.Background(), e, MVCCIncrementalIterOptions{
		EndKey:    endKey,
		StartTime: startTime,
		EndTime:   endTime,
	})
	if err != nil {
		t.Fatal(err)
	}
	var next func()
	if nextKey {
		next = iter.NextKeyIgnoringTime
	} else {
		next = iter.NextIgnoringTime
	}
	defer iter.Close()
	for iter.SeekGE(MakeMVCCMetadataKey(startKey)); ; next() {
		if ok, _ := iter.Valid(); !ok || iter.UnsafeKey().Key.Compare(endKey) >= 0 {
			break
		}
		// pass
	}
	if _, err := iter.Valid(); !testutils.IsError(err, errString) {
		t.Fatalf("expected error %q but got %v", errString, err)
	}
}

func assertIgnoreTimeIteratedKVs(
	t *testing.T,
	e Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	expected []MVCCKeyValue,
	nextKey bool,
) {
	iter, err := NewMVCCIncrementalIterator(context.Background(), e, MVCCIncrementalIterOptions{
		EndKey:    endKey,
		StartTime: startTime,
		EndTime:   endTime,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	next := func() {
		if nextKey {
			iter.NextKeyIgnoringTime()
		} else {
			iter.NextIgnoringTime()
		}
	}
	var kvs []MVCCKeyValue
	for iter.SeekGE(MakeMVCCMetadataKey(startKey)); ; next() {
		if ok, err := iter.Valid(); err != nil {
			t.Fatalf("unexpected error: %+v", err)
		} else if !ok || iter.UnsafeKey().Key.Compare(endKey) >= 0 {
			break
		}
		v, err := iter.UnsafeValue()
		if err != nil {
			t.Fatalf("unexpected error: %+v", err)
		}
		kvs = append(kvs, MVCCKeyValue{
			Key: iter.UnsafeKey().Clone(), Value: append([]byte{}, v...)})
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

func assertIteratedKVs(
	t *testing.T,
	e Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	revisions bool,
	expected []MVCCKeyValue,
) {
	iter, err := NewMVCCIncrementalIterator(context.Background(), e, MVCCIncrementalIterOptions{
		EndKey:           endKey,
		StartTime:        startTime,
		EndTime:          endTime,
		IntentPolicy:     MVCCIncrementalIterIntentPolicyAggregate,
		MaxLockConflicts: MaxConflictsPerLockConflictErrorDefault,
	})
	if err != nil {
		t.Fatal(err)
	}
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
		if len(iter.intents) > 0 {
			t.Fatal("got unexpected intent error")
		}
		v, err := iter.UnsafeValue()
		if err != nil {
			t.Fatalf("unexpected error: %+v", err)
		}
		kvs = append(kvs, MVCCKeyValue{
			Key: iter.UnsafeKey().Clone(), Value: append([]byte{}, v...)})
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
			assertIteratedKVs(t, e, startKey, endKey, startTime, endTime, revisions, expected)
		})
		t.Run("export", func(t *testing.T) {
			assertExportedKVs(t, e, startKey, endKey, startTime, endTime, revisions, expected)
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
	DisableMetamorphicSimpleValueEncoding(t)
	ctx := context.Background()

	var (
		keyMax   = roachpb.KeyMax
		testKey1 = roachpb.Key("/db1")
		testKey2 = roachpb.Key("/db2")

		testValue1 = roachpb.MakeValueFromString("val1")
		testValue2 = roachpb.MakeValueFromString("val2")
		testValue3 = roachpb.MakeValueFromString("val3")
		testValue4 = roachpb.MakeValueFromString("val4")

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
	kv1_3Deleted := makeKVT(testKey1, roachpb.Value{}, ts3)

	e := NewDefaultInMemForTesting()
	defer e.Close()

	t.Run("empty", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, tsMin, ts3, nil, false)
	})

	for _, kv := range kvs(kv1_1_1, kv1_2_2, kv2_2_2) {
		v := roachpb.Value{RawBytes: kv.Value}
		if _, err := MVCCPut(ctx, e, kv.Key.Key, kv.Key.Timestamp, v, MVCCWriteOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	// Exercise time ranges.
	t.Run("ts (0-0]", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, tsMin, tsMin, nil, false)
	})
	// Returns the kv_2_2_2 even though it is outside (startTime, endTime].
	t.Run("ts (0-1]", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, tsMin, ts1, kvs(kv1_1_1, kv2_2_2), false)
	})
	t.Run("ts (0-∞]", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, tsMin, tsMax, kvs(kv1_2_2, kv1_1_1,
			kv2_2_2), false)
	})
	t.Run("ts (1-1]", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts1, ts1, nil, false)
	})
	// Returns the kv_1_1_1 even though it is outside (startTime, endTime].
	t.Run("ts (1-2]", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts1, ts2, kvs(kv1_2_2, kv1_1_1,
			kv2_2_2), false)
	})
	t.Run("ts (2-2]", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts2, ts2, nil, false)
	})

	// Exercise key ranges.
	t.Run("kv [1-1)", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, testKey1, testKey1, tsMin, tsMax, nil, false)
	})
	t.Run("kv [1-2)", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, testKey1, testKey2, tsMin, tsMax, kvs(kv1_2_2,
			kv1_1_1), false)
	})

	// Exercise deletion.
	if _, _, err := MVCCDelete(ctx, e, testKey1, ts3, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	// Returns the kv_1_1_1 even though it is outside (startTime, endTime].
	t.Run("del", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts1, tsMax, kvs(kv1_3Deleted,
			kv1_2_2, kv1_1_1, kv2_2_2), false)
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
	if _, err := MVCCPut(ctx, e, txn1.TxnMeta.Key, txn1.ReadTimestamp, testValue4, MVCCWriteOptions{Txn: &txn1}); err != nil {
		t.Fatal(err)
	}

	// We have to be careful that we are testing the intent handling logic of
	// NextIgnoreTime() rather than the first SeekGE(). We do this by
	// ensuring that the SeekGE() doesn't encounter an intent.
	t.Run("intents", func(t *testing.T) {
		ignoreTimeExpectErr(t, e, testKey1, testKey2.PrefixEnd(), tsMin, tsMax,
			"conflicting locks", false)
	})
	t.Run("intents", func(t *testing.T) {
		ignoreTimeExpectErr(t, e, localMax, keyMax, tsMin, ts4, "conflicting locks", false)
	})
	// Intents above the upper time bound or beneath the lower time bound must
	// be ignored. Note that the lower time bound is exclusive while the upper
	// time bound is inclusive.
	//
	// The intent at ts=4 for kv2 lies outside the timespan
	// (startTime, endTime] so we do not raise an error and just move on to
	// its versioned KV, a provisional value.
	t.Run("intents", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, tsMin, ts3, kvs(kv1_3Deleted,
			kv1_2_2, kv1_1_1, kv2_4_4, kv2_2_2), false)
	})
	t.Run("intents", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts4, tsMax, kvs(), false)
	})
	t.Run("intents", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts4.Next(), tsMax, kvs(), false)
	})
}

// TestMVCCIncrementalIteratorNextKeyIgnoringTime tests the iteration semantics
// of the method NextKeyIgnoreTime(). This method is supposed to return all new
// keys that would be encountered in a non-incremental iteration.
func TestMVCCIncrementalIteratorNextKeyIgnoringTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	DisableMetamorphicSimpleValueEncoding(t)
	ctx := context.Background()

	var (
		keyMax   = roachpb.KeyMax
		testKey1 = roachpb.Key("/db1")
		testKey2 = roachpb.Key("/db2")

		testValue1 = roachpb.MakeValueFromString("val1")
		testValue2 = roachpb.MakeValueFromString("val2")
		testValue3 = roachpb.MakeValueFromString("val3")
		testValue4 = roachpb.MakeValueFromString("val4")

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
	kv1_3Deleted := makeKVT(testKey1, roachpb.Value{}, ts3)

	e := NewDefaultInMemForTesting()
	defer e.Close()

	t.Run("empty", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, tsMin, ts3, nil, true)
	})

	for _, kv := range kvs(kv1_1_1, kv1_2_2, kv2_2_2) {
		v := roachpb.Value{RawBytes: kv.Value}
		if _, err := MVCCPut(ctx, e, kv.Key.Key, kv.Key.Timestamp, v, MVCCWriteOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	// Exercise time ranges.
	t.Run("ts (0-0]", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, tsMin, tsMin, nil, true)
	})
	// Returns the kv_2_2_2 even though it is outside (startTime, endTime].
	t.Run("ts (0-1]", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, tsMin, ts1, kvs(kv1_1_1, kv2_2_2), true)
	})
	t.Run("ts (0-∞]", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, tsMin, tsMax, kvs(kv1_2_2, kv2_2_2),
			true)
	})
	t.Run("ts (1-1]", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts1, ts1, nil, true)
	})
	t.Run("ts (1-2]", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts1, ts2, kvs(kv1_2_2, kv2_2_2), true)
	})
	t.Run("ts (2-2]", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts2, ts2, nil, true)
	})

	// Exercise key ranges.
	t.Run("kv [1-1)", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, testKey1, testKey1, tsMin, tsMax, nil, true)
	})
	t.Run("kv [1-2)", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, testKey1, testKey2, tsMin, tsMax, kvs(kv1_2_2), true)
	})

	// Exercise deletion.
	if _, _, err := MVCCDelete(ctx, e, testKey1, ts3, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	// Returns the kv_1_1_1 even though it is outside (startTime, endTime].
	t.Run("del", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts1, tsMax, kvs(kv1_3Deleted,
			kv2_2_2), true)
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
	if _, err := MVCCPut(ctx, e, txn1.TxnMeta.Key, txn1.ReadTimestamp, testValue4, MVCCWriteOptions{Txn: &txn1}); err != nil {
		t.Fatal(err)
	}

	// We have to be careful that we are testing the intent handling logic of
	// NextIgnoreTime() rather than the first SeekGE(). We do this by
	// ensuring that the SeekGE() doesn't encounter an intent.
	t.Run("intents", func(t *testing.T) {
		ignoreTimeExpectErr(t, e, testKey1, testKey2.PrefixEnd(), tsMin, tsMax,
			"conflicting locks", true)
	})
	t.Run("intents", func(t *testing.T) {
		ignoreTimeExpectErr(t, e, localMax, keyMax, tsMin, ts4, "conflicting locks", true)
	})
	// Intents above the upper time bound or beneath the lower time bound must
	// be ignored. Note that the lower time bound is exclusive while the upper
	// time bound is inclusive.
	//
	// The intent at ts=4 for kv2 lies outside the timespan
	// (startTime, endTime] so we do not raise an error and just move on to
	// its versioned KV, a provisional value.
	t.Run("intents", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, tsMin, ts3, kvs(kv1_3Deleted,
			kv2_4_4), true)
	})
	t.Run("intents", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts4, tsMax, kvs(), true)
	})
	t.Run("intents", func(t *testing.T) {
		assertIgnoreTimeIteratedKVs(t, e, localMax, keyMax, ts4.Next(), tsMax, kvs(), true)
	})
}

func TestMVCCIncrementalIteratorInlinePolicy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	DisableMetamorphicSimpleValueEncoding(t)
	ctx := context.Background()

	var (
		keyMax   = roachpb.KeyMax
		testKey1 = roachpb.Key("/db1")
		testKey2 = roachpb.Key("/db2")
		testKey3 = roachpb.Key("/db3")

		testValue1 = roachpb.MakeValueFromString("val1")
		testValue2 = roachpb.MakeValueFromString("val2")

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
	inline3_2_1 := makeKVT(testKey3, testValue2, hlc.Timestamp{})

	e := NewDefaultInMemForTesting()
	defer e.Close()
	for _, kv := range []MVCCKeyValue{inline1_1_1, kv2_1_1, kv2_2_2, inline3_2_1} {
		v := roachpb.Value{RawBytes: kv.Value}
		if _, err := MVCCPut(ctx, e, kv.Key.Key, kv.Key.Timestamp, v, MVCCWriteOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("returns error if inline value is found", func(t *testing.T) {
		iter, err := NewMVCCIncrementalIterator(context.Background(), e, MVCCIncrementalIterOptions{
			EndKey:    keyMax,
			StartTime: tsMin,
			EndTime:   tsMax,
		})
		assert.NoError(t, err)
		defer iter.Close()
		iter.SeekGE(MakeMVCCMetadataKey(testKey1))
		_, err = iter.Valid()
		assert.EqualError(t, err, "unexpected inline value found: \"/db1\"")
	})
	t.Run("returns error on NextIgnoringTime if inline value is found",
		func(t *testing.T) {
			iter, err := NewMVCCIncrementalIterator(context.Background(), e, MVCCIncrementalIterOptions{
				EndKey:    keyMax,
				StartTime: tsMin,
				EndTime:   tsMax,
			})
			assert.NoError(t, err)
			defer iter.Close()
			iter.SeekGE(MakeMVCCMetadataKey(testKey2))
			expectKeyValue(t, iter, kv2_2_2)
			iter.NextIgnoringTime()
			expectKeyValue(t, iter, kv2_1_1)
			iter.NextIgnoringTime()
			_, err = iter.Valid()
			assert.EqualError(t, err, "unexpected inline value found: \"/db3\"")
		})
}

func TestMVCCIncrementalIteratorIntentPolicy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	DisableMetamorphicSimpleValueEncoding(t)
	ctx := context.Background()

	var (
		keyMax   = roachpb.KeyMax
		testKey1 = roachpb.Key("/db1")
		testKey2 = roachpb.Key("/db2")

		testValue1 = roachpb.MakeValueFromString("val1")
		testValue2 = roachpb.MakeValueFromString("val2")
		testValue3 = roachpb.MakeValueFromString("val3")

		// Use a non-zero min, since we use IsEmpty to decide if a ts should be used
		// as upper/lower-bound during iterator initialization.
		tsMin = hlc.Timestamp{WallTime: 0, Logical: 1}
		ts1   = hlc.Timestamp{WallTime: 1, Logical: 0}
		ts2   = hlc.Timestamp{WallTime: 2, Logical: 0}
		ts3   = hlc.Timestamp{WallTime: 3, Logical: 0}
		tsMax = hlc.Timestamp{WallTime: math.MaxInt64, Logical: 0}
	)

	kv1_1_1 := makeKVT(testKey1, testValue1, ts1)
	kv1_2_2 := makeKVT(testKey1, testValue2, ts2)
	kv1_3_3 := makeKVT(testKey1, testValue3, ts3)
	kv2_1_1 := makeKVT(testKey2, testValue1, ts1)
	kv2_2_2 := makeKVT(testKey2, testValue2, ts2)
	txn, intent2_2_2 := makeKVTxn(testKey2, ts2)

	lcErr := &kvpb.LockConflictError{Locks: []roachpb.Lock{intent2_2_2.AsLock()}}

	e := NewDefaultInMemForTesting()
	defer e.Close()
	for _, kv := range []MVCCKeyValue{kv1_1_1, kv1_2_2, kv1_3_3, kv2_1_1} {
		v := roachpb.Value{RawBytes: kv.Value}
		if _, err := MVCCPut(ctx, e, kv.Key.Key, kv.Key.Timestamp, v, MVCCWriteOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := MVCCPut(ctx, e, txn.TxnMeta.Key, txn.ReadTimestamp, testValue2, MVCCWriteOptions{Txn: &txn}); err != nil {
		t.Fatal(err)
	}
	t.Run("PolicyError returns error if an intent is in the time range", func(t *testing.T) {
		iter, err := NewMVCCIncrementalIterator(context.Background(), e, MVCCIncrementalIterOptions{
			EndKey:       keyMax,
			StartTime:    tsMin,
			EndTime:      tsMax,
			IntentPolicy: MVCCIncrementalIterIntentPolicyError,
		})
		assert.NoError(t, err)
		defer iter.Close()
		iter.SeekGE(MakeMVCCMetadataKey(testKey1))
		for ; ; iter.Next() {
			if ok, _ := iter.Valid(); !ok || iter.UnsafeKey().Key.Compare(keyMax) >= 0 {
				break
			}
		}
		_, err = iter.Valid()
		assert.EqualError(t, err, lcErr.Error())

		iter.SeekGE(MakeMVCCMetadataKey(testKey1))
		_, err = iter.Valid()
		require.NoError(t, err)
		for ; ; iter.NextIgnoringTime() {
			if ok, err := iter.Valid(); !ok {
				assert.EqualError(t, err, lcErr.Error())
				break
			}
		}
	})
	t.Run("PolicyError ignores intents outside of time range", func(t *testing.T) {
		iter, err := NewMVCCIncrementalIterator(context.Background(), e, MVCCIncrementalIterOptions{
			EndKey:       keyMax,
			StartTime:    ts2,
			EndTime:      tsMax,
			IntentPolicy: MVCCIncrementalIterIntentPolicyError,
		})
		assert.NoError(t, err)
		defer iter.Close()
		iter.SeekGE(MakeMVCCMetadataKey(testKey1))
		expectKeyValue(t, iter, kv1_3_3)
		iter.Next()
		valid, err := iter.Valid()
		assert.NoError(t, err)
		assert.False(t, valid)
	})
	t.Run("PolicyEmit returns inline values to caller", func(t *testing.T) {
		iter, err := NewMVCCIncrementalIterator(context.Background(), e, MVCCIncrementalIterOptions{
			EndKey:       keyMax,
			StartTime:    tsMin,
			EndTime:      tsMax,
			IntentPolicy: MVCCIncrementalIterIntentPolicyEmit,
		})
		assert.NoError(t, err)
		defer iter.Close()
		testIterWithNextFunc := func(nextFunc func()) {
			iter.SeekGE(MakeMVCCMetadataKey(testKey1))
			for _, kv := range []MVCCKeyValue{kv1_3_3, kv1_2_2, kv1_1_1} {
				expectKeyValue(t, iter, kv)
				nextFunc()
			}
			expectIntent(t, iter, intent2_2_2)
			nextFunc()
			expectKeyValue(t, iter, kv2_2_2)
			nextFunc()
			expectKeyValue(t, iter, kv2_1_1)
		}
		testIterWithNextFunc(iter.Next)
		testIterWithNextFunc(iter.NextIgnoringTime)
	})
	t.Run("PolicyEmit ignores intents outside of time range", func(t *testing.T) {
		iter, err := NewMVCCIncrementalIterator(context.Background(), e, MVCCIncrementalIterOptions{
			EndKey:       keyMax,
			StartTime:    ts2,
			EndTime:      tsMax,
			IntentPolicy: MVCCIncrementalIterIntentPolicyEmit,
		})
		assert.NoError(t, err)
		defer iter.Close()
		iter.SeekGE(MakeMVCCMetadataKey(testKey1))
		expectKeyValue(t, iter, kv1_3_3)
		iter.Next()
		valid, err := iter.Valid()
		assert.NoError(t, err)
		assert.False(t, valid)
	})
}

func expectKeyValue(t *testing.T, iter SimpleMVCCIterator, kv MVCCKeyValue) {
	valid, err := iter.Valid()
	assert.True(t, valid, "expected valid iterator")
	assert.NoError(t, err)

	unsafeKey := iter.UnsafeKey()
	unsafeVal, err := iter.UnsafeValue()
	require.NoError(t, err)

	assert.True(t, unsafeKey.Key.Equal(kv.Key.Key), "keys not equal")
	assert.Equal(t, kv.Key.Timestamp, unsafeKey.Timestamp)
	assert.Equal(t, kv.Value, unsafeVal)

}

func expectIntent(t *testing.T, iter SimpleMVCCIterator, intent roachpb.Intent) {
	valid, err := iter.Valid()
	assert.True(t, valid)
	assert.NoError(t, err)

	unsafeKey := iter.UnsafeKey()
	unsafeVal, err := iter.UnsafeValue()
	require.NoError(t, err)

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
	DisableMetamorphicSimpleValueEncoding(t)
	ctx := context.Background()

	var (
		keyMax   = roachpb.KeyMax
		testKey1 = roachpb.Key("/db1")
		testKey2 = roachpb.Key("/db2")

		testValue1 = roachpb.MakeValueFromString("val1")
		testValue2 = roachpb.MakeValueFromString("val2")
		testValue3 = roachpb.MakeValueFromString("val3")
		testValue4 = roachpb.MakeValueFromString("val4")

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
	kv1Deleted3 := makeKVT(testKey1, roachpb.Value{}, ts3)

	t.Run("latest", func(t *testing.T) {
		e := NewDefaultInMemForTesting()
		defer e.Close()

		t.Run("empty", assertEqualKVs(e, localMax, keyMax, tsMin, ts3, latest, nil))

		for _, kv := range kvs(kv1_1_1, kv1_2_2, kv2_2_2) {
			v := roachpb.Value{RawBytes: kv.Value}
			if _, err := MVCCPut(ctx, e, kv.Key.Key, kv.Key.Timestamp, v, MVCCWriteOptions{}); err != nil {
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
		if _, _, err := MVCCDelete(ctx, e, testKey1, ts3, MVCCWriteOptions{}); err != nil {
			t.Fatal(err)
		}
		t.Run("del", assertEqualKVs(e, localMax, keyMax, ts1, tsMax, latest, kvs(kv1Deleted3, kv2_2_2)))

		// Exercise intent handling.
		txn1, lcErr1 := makeKVTxn(testKey1, ts4)
		if _, err := MVCCPut(ctx, e, txn1.TxnMeta.Key, txn1.ReadTimestamp, testValue4, MVCCWriteOptions{Txn: &txn1}); err != nil {
			t.Fatal(err)
		}
		txn2, lcErr2 := makeKVTxn(testKey2, ts4)
		if _, err := MVCCPut(ctx, e, txn2.TxnMeta.Key, txn2.ReadTimestamp, testValue4, MVCCWriteOptions{Txn: &txn2}); err != nil {
			t.Fatal(err)
		}
		t.Run("intents-1",
			iterateExpectErr(e, testKey1, testKey1.PrefixEnd(), tsMin, tsMax, latest, intents(lcErr1)))
		t.Run("intents-2",
			iterateExpectErr(e, testKey2, testKey2.PrefixEnd(), tsMin, tsMax, latest, intents(lcErr2)))
		t.Run("intents-multi",
			iterateExpectErr(e, localMax, keyMax, tsMin, ts4, latest, intents(lcErr1, lcErr2)))
		// Intents above the upper time bound or beneath the lower time bound must
		// be ignored (#28358). Note that the lower time bound is exclusive while
		// the upper time bound is inclusive.
		t.Run("intents-filtered-1", assertEqualKVs(e, localMax, keyMax, tsMin, ts3, latest, kvs(kv1Deleted3, kv2_2_2)))
		t.Run("intents-filtered-2", assertEqualKVs(e, localMax, keyMax, ts4, tsMax, latest, kvs()))
		t.Run("intents-filtered-3", assertEqualKVs(e, localMax, keyMax, ts4.Next(), tsMax, latest, kvs()))

		intent1 := roachpb.MakeLockUpdate(&txn1, roachpb.Span{Key: testKey1})
		intent1.Status = roachpb.COMMITTED
		if _, _, _, _, err := MVCCResolveWriteIntent(ctx, e, nil, intent1, MVCCResolveWriteIntentOptions{}); err != nil {
			t.Fatal(err)
		}
		intent2 := roachpb.MakeLockUpdate(&txn2, roachpb.Span{Key: testKey2})
		intent2.Status = roachpb.ABORTED
		if _, _, _, _, err := MVCCResolveWriteIntent(ctx, e, nil, intent2, MVCCResolveWriteIntentOptions{}); err != nil {
			t.Fatal(err)
		}
		t.Run("intents-resolved", assertEqualKVs(e, localMax, keyMax, tsMin, tsMax, latest, kvs(kv1_4_4, kv2_2_2)))
	})

	t.Run("all", func(t *testing.T) {
		e := NewDefaultInMemForTesting()
		defer e.Close()

		t.Run("empty", assertEqualKVs(e, localMax, keyMax, tsMin, ts3, all, nil))

		for _, kv := range kvs(kv1_1_1, kv1_2_2, kv2_2_2) {
			v := roachpb.Value{RawBytes: kv.Value}
			if _, err := MVCCPut(ctx, e, kv.Key.Key, kv.Key.Timestamp, v, MVCCWriteOptions{}); err != nil {
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
		if _, _, err := MVCCDelete(ctx, e, testKey1, ts3, MVCCWriteOptions{}); err != nil {
			t.Fatal(err)
		}
		t.Run("del", assertEqualKVs(e, localMax, keyMax, ts1, tsMax, all, kvs(kv1Deleted3, kv1_2_2, kv2_2_2)))

		// Exercise intent handling.
		txn1, lcErr1 := makeKVTxn(testKey1, ts4)
		if _, err := MVCCPut(ctx, e, txn1.TxnMeta.Key, txn1.ReadTimestamp, testValue4, MVCCWriteOptions{Txn: &txn1}); err != nil {
			t.Fatal(err)
		}
		txn2, lcErr2 := makeKVTxn(testKey2, ts4)
		if _, err := MVCCPut(ctx, e, txn2.TxnMeta.Key, txn2.ReadTimestamp, testValue4, MVCCWriteOptions{Txn: &txn2}); err != nil {
			t.Fatal(err)
		}
		// Single intent tests are verifying behavior when intent collection is not enabled.
		t.Run("intents-1",
			iterateExpectErr(e, testKey1, testKey1.PrefixEnd(), tsMin, tsMax, all, intents(lcErr1)))
		t.Run("intents-2",
			iterateExpectErr(e, testKey2, testKey2.PrefixEnd(), tsMin, tsMax, all, intents(lcErr2)))
		t.Run("intents-multi",
			iterateExpectErr(e, localMax, keyMax, tsMin, ts4, all, intents(lcErr1, lcErr2)))
		// Intents above the upper time bound or beneath the lower time bound must
		// be ignored (#28358). Note that the lower time bound is exclusive while
		// the upper time bound is inclusive.
		t.Run("intents-filtered-1", assertEqualKVs(e, localMax, keyMax, tsMin, ts3, all, kvs(kv1Deleted3, kv1_2_2, kv1_1_1, kv2_2_2)))
		t.Run("intents-filtered-2", assertEqualKVs(e, localMax, keyMax, ts4, tsMax, all, kvs()))
		t.Run("intents-filtered-3", assertEqualKVs(e, localMax, keyMax, ts4.Next(), tsMax, all, kvs()))

		intent1 := roachpb.MakeLockUpdate(&txn1, roachpb.Span{Key: testKey1})
		intent1.Status = roachpb.COMMITTED
		if _, _, _, _, err := MVCCResolveWriteIntent(ctx, e, nil, intent1, MVCCResolveWriteIntentOptions{}); err != nil {
			t.Fatal(err)
		}
		intent2 := roachpb.MakeLockUpdate(&txn2, roachpb.Span{Key: testKey2})
		intent2.Status = roachpb.ABORTED
		if _, _, _, _, err := MVCCResolveWriteIntent(ctx, e, nil, intent2, MVCCResolveWriteIntentOptions{}); err != nil {
			t.Fatal(err)
		}
		t.Run("intents-resolved", assertEqualKVs(e, localMax, keyMax, tsMin, tsMax, all, kvs(kv1_4_4, kv1Deleted3, kv1_2_2, kv1_1_1, kv2_2_2)))
	})
}

func slurpKVsInTimeRange(
	reader Reader, prefix roachpb.Key, startTime, endTime hlc.Timestamp,
) ([]MVCCKeyValue, error) {
	endKey := prefix.PrefixEnd()
	iter, err := NewMVCCIncrementalIterator(context.Background(), reader, MVCCIncrementalIterOptions{
		EndKey:    endKey,
		StartTime: startTime,
		EndTime:   endTime,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var kvs []MVCCKeyValue
	for iter.SeekGE(MakeMVCCMetadataKey(prefix)); ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return nil, err
		} else if !ok || iter.UnsafeKey().Key.Compare(endKey) >= 0 {
			break
		}
		v, err := iter.UnsafeValue()
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, MVCCKeyValue{
			Key: iter.UnsafeKey().Clone(), Value: append([]byte{}, v...)})
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

	e := NewDefaultInMemForTesting()
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
	if _, err := MVCCPut(ctx, e, kA, ts1, vA1, MVCCWriteOptions{Txn: txn}); err != nil {
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
		if _, err := MVCCPut(ctx, b, kA, ts1, vA2, MVCCWriteOptions{Txn: txn}); err != nil {
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
		// observe the intent and return a lock conflict error. If the iteration
		// loses the race with the put that moves the intent then it should
		// observe and return nothing because there will be no committed or
		// provisional keys in its time range.
		if err != nil {
			if !testutils.IsError(err, `conflicting locks on "kA"`) {
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
}

// TestMVCCIncrementalIteratorIntentDeletion checks a workaround in
// MVCCIncrementalIterator for a bug in time-bound iterators, where an intent
// has been deleted, but the time-bound iterator doesn't see the deletion.
func TestMVCCIncrementalIteratorIntentDeletion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	DisableMetamorphicSimpleValueEncoding(t)

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
	_, err := MVCCPut(ctx, db, kA, txnA1.ReadTimestamp, vA1, MVCCWriteOptions{Txn: txnA1})
	require.NoError(t, err)
	_, err = MVCCPut(ctx, db, kB, txnB1.ReadTimestamp, vB1, MVCCWriteOptions{Txn: txnB1})
	require.NoError(t, err)
	_, err = MVCCPut(ctx, db, kC, txnC1.ReadTimestamp, vC1, MVCCWriteOptions{Txn: txnC1})
	require.NoError(t, err)
	require.NoError(t, db.Flush())
	require.NoError(t, db.Compact())
	_, _, _, _, err = MVCCResolveWriteIntent(ctx, db, nil, intent(txnA1), MVCCResolveWriteIntentOptions{})
	require.NoError(t, err)
	_, _, _, _, err = MVCCResolveWriteIntent(ctx, db, nil, intent(txnB1), MVCCResolveWriteIntentOptions{})
	require.NoError(t, err)
	_, err = MVCCPut(ctx, db, kA, ts2, vA2, MVCCWriteOptions{})
	require.NoError(t, err)
	_, err = MVCCPut(ctx, db, kA, txnA3.WriteTimestamp, vA3, MVCCWriteOptions{Txn: txnA3})
	require.NoError(t, err)
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
	require.EqualError(t, err, `conflicting locks on "kA"`)

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
	require.EqualError(t, err, `conflicting locks on "kC"`)
}

func TestMVCCIncrementalIteratorIntentStraddlesSStables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a DB containing 2 keys, a and b, where a has an intent. We use the
	// regular MVCCPut operation to generate these keys, which we'll later be
	// copying into manually created sstables.
	ctx := context.Background()
	db1, err := Open(ctx, InMemory(), cluster.MakeClusterSettings(), ForTesting)
	require.NoError(t, err)
	defer db1.Close()

	put := func(key, value string, ts int64, txn *roachpb.Transaction) {
		v := roachpb.MakeValueFromString(value)
		if _, err := MVCCPut(ctx, db1, roachpb.Key(key), hlc.Timestamp{WallTime: ts}, v, MVCCWriteOptions{Txn: txn}); err != nil {
			t.Fatal(err)
		}
	}

	put("a", "a value", 2, &roachpb.Transaction{
		TxnMeta: enginepb.TxnMeta{
			Key:            roachpb.Key("b"),
			ID:             uuid.MakeV4(),
			Epoch:          1,
			WriteTimestamp: hlc.Timestamp{WallTime: 2},
		},
		ReadTimestamp: hlc.Timestamp{WallTime: 2},
	})
	put("b", "b value", 1, nil)

	// Create a second DB in which we'll create a specific SSTable structure:
	// the first SSTable contains the intent, and the second contains the two
	// versioned keys. The effect is that the intent is in a separate sstable
	// from the entry it is the intent for.
	//
	//   SSTable 1:
	//     a@<meta> (in the lock table)
	//
	//   SSTable 2:
	//     a@2
	//     b@1
	db2, err := Open(ctx, InMemory(), cluster.MakeTestingClusterSettings(), ForTesting)
	require.NoError(t, err)
	defer db2.Close()

	ingest := func(it EngineIterator, valid bool, err error, count int) {
		memFile := &MemObject{}
		sst := MakeIngestionSSTWriter(ctx, db2.cfg.settings, memFile)
		defer sst.Close()

		for i := 0; i < count; i++ {
			require.NoError(t, err)
			require.True(t, valid)
			ek, err := it.EngineKey()
			require.NoError(t, err)
			require.NoError(t, err)
			v, err := it.Value()
			require.NoError(t, err)
			if err := sst.PutEngineKey(ek, v); err != nil {
				t.Fatal(err)
			}
			valid, err = it.NextEngineKey()
			// Make linter happy.
			require.NoError(t, err)
		}
		if err := sst.Finish(); err != nil {
			t.Fatal(err)
		}
		if err := fs.WriteFile(db2.Env(), `ingest`, memFile.Data(), fs.UnspecifiedWriteCategory); err != nil {
			t.Fatal(err)
		}
		if err := db2.IngestLocalFiles(ctx, []string{`ingest`}); err != nil {
			t.Fatal(err)
		}
	}

	{
		// Iterate over the entries in the first DB, ingesting them into SSTables
		// in the second DB.
		it, err := db1.NewEngineIterator(context.Background(), IterOptions{UpperBound: keys.MaxKey})
		require.NoError(t, err)
		defer it.Close()
		valid, err := it.SeekEngineKeyGE(EngineKey{Key: keys.LocalRangeLockTablePrefix})
		ingest(it, valid, err, 1)
		valid, err = it.SeekEngineKeyGE(EngineKey{Key: keys.LocalMax})
		ingest(it, valid, err, 2)
	}

	{
		// Use an incremental iterator to simulate an incremental backup from (1,
		// 2]. Note that incremental iterators are exclusive on the start time and
		// inclusive on the end time. The expectation is that we'll see a write
		// intent error.
		it, err := NewMVCCIncrementalIterator(context.Background(), db2, MVCCIncrementalIterOptions{
			EndKey:    keys.MaxKey,
			StartTime: hlc.Timestamp{WallTime: 1},
			EndTime:   hlc.Timestamp{WallTime: 2},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()
		for it.SeekGE(MVCCKey{Key: keys.LocalMax}); ; it.Next() {
			ok, err := it.Valid()
			if err != nil {
				if errors.HasType(err, (*kvpb.LockConflictError)(nil)) {
					// This is the lock conflict error we were expecting.
					return
				}
				t.Fatalf("%T: %s", err, err)
			}
			if !ok {
				break
			}
		}
		t.Fatalf("expected lock conflict error, but found success")
	}
}

func TestMVCCIterateTimeBound(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	DisableMetamorphicSimpleValueEncoding(t)

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

	// TODO(jackson): This test is tightly coupled with the prng seed constant
	// in loadTestData. Some of the assertions below assume there exist keys
	// within narrow bounds (eg, [19,20)). This is the case today for the
	// current prng seed, but it's a bit too brittle.
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
	iter, err := eng.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
	require.NoError(t, err)
	defer iter.Close()
	iter.SeekGE(MVCCKey{Key: localMax})
	for {
		ok, err := iter.Valid()
		if err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}
		ts := iter.UnsafeKey().Timestamp
		if (ts.Less(end) || end == ts) && start.Less(ts) {
			v, err := iter.Value()
			if err != nil {
				t.Fatal(err)
			}
			expectedKVs = append(expectedKVs, MVCCKeyValue{Key: iter.UnsafeKey().Clone(), Value: v})
		}
		iter.Next()
	}
	if len(expectedKVs) < 1 {
		t.Fatalf("source of truth had no expected KVs; likely a bug in the test itself")
	}
	return expectedKVs
}

func runIncrementalBenchmark(b *testing.B, ts hlc.Timestamp, opts mvccBenchData) {
	eng := getInitialStateEngine(context.Background(), b, opts, false /* inMemory */)
	defer eng.Close()
	{
		// Pull all of the sstables into the cache.  This
		// probably defeats a lot of the benefits of the
		// time-based optimization.
		_, err := ComputeStats(context.Background(), eng, keys.LocalMax, roachpb.KeyMax, 0)
		if err != nil {
			b.Fatalf("stats failed: %s", err)
		}
	}

	startKey := roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(0)))
	endKey := roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(opts.numKeys)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, err := NewMVCCIncrementalIterator(context.Background(), eng, MVCCIncrementalIterOptions{
			EndKey:    endKey,
			StartTime: ts,
			EndTime:   hlc.MaxTimestamp,
		})
		if err != nil {
			b.Fatal(err)
		}
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
	valueBytes := 1000

	for _, tsExcludePercent := range []float64{0, 0.95} {
		wallTime := int64((5 * (float64(numVersions)*tsExcludePercent + 1)))
		ts := hlc.Timestamp{WallTime: wallTime}
		b.Run(fmt.Sprintf("ts=%d", ts.WallTime), func(b *testing.B) {
			runIncrementalBenchmark(b, ts, mvccBenchData{
				numVersions: numVersions,
				numKeys:     numKeys,
				valueBytes:  valueBytes,
			})
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
	setupMVCCPebble := func(b *testing.B) Engine {
		eng, err := Open(
			context.Background(),
			InMemory(),
			cluster.MakeClusterSettings(),
			// Use a small cache size. Scanning large tables with mostly cold data
			// will mostly miss the cache (especially since the block cache is meant
			// to be scan resistant).
			CacheSize(1<<10),
		)
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
			if _, err := MVCCPut(context.Background(), batch, key, ts, value, MVCCWriteOptions{}); err != nil {
				b.Fatal(err)
			}
		}
		batch.Close()
		if err := eng.Flush(); err != nil {
			b.Fatal(err)
		}
		if err := eng.Compact(); err != nil {
			b.Fatal(err)
		}
	}

	for _, valueSize := range []int{100, 500, 1000, 2000} {
		eng := setupMVCCPebble(b)
		setupData(b, eng, valueSize)
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			startKey := roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(0)))
			endKey := roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(numKeys)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				func() {
					it, err := NewMVCCIncrementalIterator(context.Background(), eng, MVCCIncrementalIterOptions{
						EndKey:    endKey,
						StartTime: hlc.Timestamp{},
						EndTime:   hlc.Timestamp{WallTime: baseTimestamp},
					})
					if err != nil {
						b.Fatal(err)
					}
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
				}()
			}
		})
		eng.Close()
	}
}
