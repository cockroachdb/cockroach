// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/zerofields"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

// Constants for system-reserved keys in the KV map.
var (
	localMax     = keys.LocalMax
	keyMax       = roachpb.KeyMax
	testKey1     = roachpb.Key("/db1")
	testKey2     = roachpb.Key("/db2")
	testKey3     = roachpb.Key("/db3")
	testKey4     = roachpb.Key("/db4")
	testKey5     = roachpb.Key("/db5")
	testKey6     = roachpb.Key("/db6")
	txn1ID       = uuid.MakeV4()
	txn2ID       = uuid.MakeV4()
	txn1TS       = hlc.Timestamp{Logical: 1}
	txn2TS       = hlc.Timestamp{Logical: 2}
	txn1         = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn1ID, Epoch: 1, WriteTimestamp: txn1TS, MinTimestamp: txn1TS, CoordinatorNodeID: 1}, ReadTimestamp: txn1TS}
	txn1Commit   = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn1ID, Epoch: 1, WriteTimestamp: txn1TS, MinTimestamp: txn1TS, CoordinatorNodeID: 1}, ReadTimestamp: txn1TS, Status: roachpb.COMMITTED}
	txn1Abort    = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn1ID, Epoch: 1, WriteTimestamp: txn1TS, MinTimestamp: txn1TS, CoordinatorNodeID: 1}, Status: roachpb.ABORTED}
	txn1e2       = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn1ID, Epoch: 2, WriteTimestamp: txn1TS, MinTimestamp: txn1TS, CoordinatorNodeID: 1}, ReadTimestamp: txn1TS}
	txn1e2Commit = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn1ID, Epoch: 2, WriteTimestamp: txn1TS, MinTimestamp: txn1TS, CoordinatorNodeID: 1}, ReadTimestamp: txn1TS, Status: roachpb.COMMITTED}
	txn2         = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn2ID, WriteTimestamp: txn2TS, MinTimestamp: txn2TS, CoordinatorNodeID: 2}, ReadTimestamp: txn2TS}
	txn2Commit   = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn2ID, WriteTimestamp: txn2TS, MinTimestamp: txn2TS, CoordinatorNodeID: 2}, ReadTimestamp: txn2TS, Status: roachpb.COMMITTED}
	value1       = roachpb.MakeValueFromString("testValue1")
	value2       = roachpb.MakeValueFromString("testValue2")
	value3       = roachpb.MakeValueFromString("testValue3")
	value4       = roachpb.MakeValueFromString("testValue4")
	value5       = roachpb.MakeValueFromString("testValue5")
	value6       = roachpb.MakeValueFromString("testValue6")
	tsvalue1     = timeSeriesRowAsValue(testtime, 1000, []tsSample{
		{1, 1, 5, 5, 5},
	}...)
	tsvalue2 = timeSeriesRowAsValue(testtime, 1000, []tsSample{
		{1, 1, 15, 15, 15},
	}...)
)

// createTestPebbleEngine returns a new in-memory Pebble storage engine.
func createTestPebbleEngine(opts ...ConfigOption) Engine {
	return NewDefaultInMemForTesting(opts...)
}

// makeTxn creates a new transaction using the specified base
// txn and timestamp.
func makeTxn(baseTxn roachpb.Transaction, ts hlc.Timestamp) *roachpb.Transaction {
	txn := baseTxn.Clone()
	txn.ReadTimestamp = ts
	txn.WriteTimestamp = ts
	return txn
}

func mvccVersionKey(key roachpb.Key, ts hlc.Timestamp) MVCCKey {
	return MVCCKey{Key: key, Timestamp: ts}
}

type mvccKeys []MVCCKey

func (n mvccKeys) Len() int           { return len(n) }
func (n mvccKeys) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n mvccKeys) Less(i, j int) bool { return n[i].Less(n[j]) }

func TestMVCCStatsAddSubForward(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	goldMS := enginepb.MVCCStats{
		ContainsEstimates: 1,
		KeyBytes:          1,
		KeyCount:          1,
		ValBytes:          1,
		ValCount:          1,
		IntentBytes:       1,
		IntentCount:       1,
		RangeKeyCount:     1,
		RangeKeyBytes:     1,
		RangeValCount:     1,
		RangeValBytes:     1,
		LockBytes:         1,
		LockCount:         1,
		LockAge:           1,
		GCBytesAge:        1,
		LiveBytes:         1,
		LiveCount:         1,
		SysBytes:          1,
		SysCount:          1,
		LastUpdateNanos:   1,
		AbortSpanBytes:    1,
	}
	require.NoError(t, zerofields.NoZeroField(&goldMS))

	ms := goldMS
	zeroWithLU := enginepb.MVCCStats{
		ContainsEstimates: 0,
		LastUpdateNanos:   ms.LastUpdateNanos,
	}

	ms.Subtract(goldMS)
	require.Equal(t, zeroWithLU, ms)

	ms.Add(goldMS)
	require.Equal(t, goldMS, ms)

	// Double-add double-sub guards against mistaking `+=` for `=`.
	ms = zeroWithLU
	ms.Add(goldMS)
	ms.Add(goldMS)
	ms.Subtract(goldMS)
	ms.Subtract(goldMS)
	require.Equal(t, zeroWithLU, ms)

	// Run some checks for Forward.
	goldDelta := enginepb.MVCCStats{
		KeyBytes:        42,
		LockCount:       11,
		LastUpdateNanos: 1e9 - 1000,
	}
	delta := goldDelta

	for _, ns := range []int64{1, 1e9 - 1001, 1e9 - 1000, 1e9 - 1, 1e9, 1e9 + 1, 2e9 - 1} {
		oldDelta := delta
		delta.AgeTo(ns)
		require.GreaterOrEqual(t, delta.LastUpdateNanos, ns, "LastUpdateNanos")
		shouldAge := ns/1e9-oldDelta.LastUpdateNanos/1e9 > 0
		didAge := delta.LockAge != oldDelta.LockAge &&
			delta.GCBytesAge != oldDelta.GCBytesAge
		require.Equal(t, shouldAge, didAge)
	}

	expDelta := goldDelta
	expDelta.LastUpdateNanos = 2e9 - 1
	expDelta.GCBytesAge = 42
	expDelta.LockAge = 11
	require.Equal(t, expDelta, delta)

	delta.AgeTo(2e9)
	expDelta.LastUpdateNanos = 2e9
	expDelta.GCBytesAge += 42
	expDelta.LockAge += 11
	require.Equal(t, expDelta, delta)

	{
		// Verify that AgeTo can go backwards in time.
		// Works on a copy.
		tmpDelta := delta
		expDelta := expDelta

		tmpDelta.AgeTo(2e9 - 1)
		expDelta.LastUpdateNanos = 2e9 - 1
		expDelta.GCBytesAge -= 42
		expDelta.LockAge -= 11
		require.Equal(t, expDelta, tmpDelta)
	}

	delta.AgeTo(3e9 - 1)
	delta.Forward(5) // should be noop
	expDelta.LastUpdateNanos = 3e9 - 1
	require.Equal(t, expDelta, delta)

	// Check that Add calls Forward appropriately.
	mss := []enginepb.MVCCStats{goldMS, goldMS}

	mss[0].LastUpdateNanos = 2e9 - 1
	mss[1].LastUpdateNanos = 10e9 + 1

	expMS := goldMS
	expMS.Add(goldMS)
	expMS.LastUpdateNanos = 10e9 + 1
	expMS.LockAge += 9        // from aging 9 ticks from 2E9-1 to 10E9+1
	expMS.GCBytesAge += 3 * 9 // ditto

	for i := range mss[:1] {
		ms := mss[(1+i)%2]
		ms.Add(mss[i])
		require.Equal(t, expMS, ms)
	}

	// Finally, check Forward with negative counts (can happen).
	neg := zeroWithLU
	neg.Subtract(goldMS)
	exp := neg

	neg.AgeTo(2e9)

	exp.LastUpdateNanos = 2e9
	exp.GCBytesAge = -7
	exp.LockAge = -3
	require.Equal(t, exp, neg)
}

func TestMVCCStatsHasUserDataCloseTo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ms1 := enginepb.MVCCStats{
		ContainsEstimates: 10,
		KeyBytes:          10,
		KeyCount:          10,
		ValBytes:          10,
		ValCount:          10,
		IntentBytes:       10,
		IntentCount:       10,
		RangeKeyCount:     10,
		RangeKeyBytes:     10,
		RangeValCount:     10,
		RangeValBytes:     10,
		LockBytes:         10,
		LockCount:         10,
		LockAge:           10,
		GCBytesAge:        10,
		LiveBytes:         10,
		LiveCount:         10,
		SysBytes:          10,
		SysCount:          10,
		LastUpdateNanos:   10,
		AbortSpanBytes:    10,
	}
	require.NoError(t, zerofields.NoZeroField(&ms1))

	ms2 := ms1
	require.True(t, ms1.HasUserDataCloseTo(ms2, 1, 2))
	require.True(t, ms1.HasUserDataCloseTo(ms2, 0, 0))

	ms2.KeyCount += 5
	require.True(t, ms1.HasUserDataCloseTo(ms2, 6, 0))
	require.True(t, ms1.HasUserDataCloseTo(ms2, 5, 0))
	require.False(t, ms1.HasUserDataCloseTo(ms2, 4, 0))

	ms2.ValBytes += 20
	require.True(t, ms1.HasUserDataCloseTo(ms2, 5, 21))
	require.True(t, ms1.HasUserDataCloseTo(ms2, 5, 20))
	require.False(t, ms1.HasUserDataCloseTo(ms2, 5, 19))
}

func TestMVCCGetNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	valueRes, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{Logical: 1},
		MVCCGetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if valueRes.Value != nil {
		t.Fatal("the value should be empty")
	}
}

func TestMVCCGetNoMoreOldVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()
	// Need to handle the case here where the scan takes us to the
	// next key, which may not match the key we're looking for. In
	// other words, if we're looking for a<T=2>, and we have the
	// following keys:
	//
	// a: MVCCMetadata(a)
	// a<T=3>
	// b: MVCCMetadata(b)
	// b<T=1>
	//
	// If we search for a<T=2>, the scan should not return "b".
	if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 3}, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey2, hlc.Timestamp{WallTime: 1}, value2, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	valueRes, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 2}, MVCCGetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if valueRes.Value != nil {
		t.Fatal("the value should be empty")
	}
}

func TestMVCCGetWithValueHeader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1, Logical: 1}, value1, MVCCWriteOptions{LocalTimestamp: hlc.ClockTimestamp{WallTime: 1}}); err != nil {
		t.Fatal(err)
	}
	valueRes, vh, err := MVCCGetWithValueHeader(ctx, engine, testKey1, hlc.Timestamp{WallTime: 2}, MVCCGetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if valueRes.Value == nil {
		t.Fatal("the value should not be empty")
	}
	require.Equal(t, hlc.ClockTimestamp{WallTime: 1}, vh.LocalTimestamp)

	_, _, err = MVCCDelete(ctx, engine, testKey1, hlc.Timestamp{WallTime: 3}, MVCCWriteOptions{LocalTimestamp: hlc.ClockTimestamp{WallTime: 2, Logical: 1}})
	if err != nil {
		t.Fatal(err)
	}

	// Read the latest version which should be deleted.
	valueRes, vh, err = MVCCGetWithValueHeader(ctx, engine, testKey1, hlc.Timestamp{WallTime: 4}, MVCCGetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if valueRes.Value != nil {
		t.Fatal("the value should be empty")
	}
	require.Zero(t, vh.LocalTimestamp)
	// Read the latest version with tombstone.
	valueRes, vh, err = MVCCGetWithValueHeader(ctx, engine, testKey1, hlc.Timestamp{WallTime: 4},
		MVCCGetOptions{Tombstones: true})
	if err != nil {
		t.Fatal(err)
	} else if valueRes.Value == nil || len(valueRes.Value.RawBytes) != 0 {
		t.Fatalf("the value should be non-nil with empty RawBytes; got %+v", valueRes.Value)
	}

	require.Equal(t, hlc.ClockTimestamp{WallTime: 2, Logical: 1}, vh.LocalTimestamp)

	// Read the old version which should still exist.
	for _, logical := range []int32{0, math.MaxInt32} {
		valueRes, vh, err := MVCCGetWithValueHeader(ctx, engine, testKey1, hlc.Timestamp{WallTime: 2, Logical: logical},
			MVCCGetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if valueRes.Value == nil {
			t.Fatal("the value should not be empty")
		}
		require.Equal(t, hlc.ClockTimestamp{WallTime: 1}, vh.LocalTimestamp)
	}
}

func TestMVCCValueHeaderOriginTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	// Put a value with a non-zero origin timestamp.
	_, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, value1, MVCCWriteOptions{OriginTimestamp: hlc.Timestamp{WallTime: 1}})
	require.NoError(t, err)

	valueRes, vh, err := MVCCGetWithValueHeader(ctx, engine, testKey1, hlc.Timestamp{WallTime: 3}, MVCCGetOptions{})
	require.NoError(t, err)
	require.NotNil(t, valueRes.Value)
	require.Equal(t, hlc.Timestamp{WallTime: 1}, vh.OriginTimestamp)

	// Ensure a regular put has no origin timestamp.
	_, err = MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 2}, value1, MVCCWriteOptions{})
	require.NoError(t, err)
	valueRes, vh, err = MVCCGetWithValueHeader(ctx, engine, testKey1, hlc.Timestamp{WallTime: 3}, MVCCGetOptions{})
	require.NoError(t, err)
	require.Zero(t, vh.OriginTimestamp)
}

// TestMVCCValueHeadersForRangefeeds tests that the value headers used by
// rangefeeds are set correctly.
func TestMVCCValueHeadersForRangefeeds(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, omitInRangefeeds := range []bool{false, true} {
		for _, originID := range []uint32{0, 1} {
			t.Run(fmt.Sprintf("omitInRangefeeds=%t/originID=%d", omitInRangefeeds, originID), func(t *testing.T) {
				ctx := context.Background()
				engine := NewDefaultInMemForTesting()
				defer engine.Close()

				// Transactional put
				txn := *txn1
				_, err := MVCCPut(ctx, engine, testKey1, txn.WriteTimestamp, value1,
					MVCCWriteOptions{Txn: &txn, OmitInRangefeeds: omitInRangefeeds, OriginID: originID})
				require.NoError(t, err)

				txnCommit := txn
				txnCommit.Status = roachpb.COMMITTED
				txnCommit.WriteTimestamp = txn.WriteTimestamp
				_, _, _, _, err = MVCCResolveWriteIntent(ctx, engine, nil,
					roachpb.MakeLockUpdate(&txnCommit, roachpb.Span{Key: testKey1}),
					MVCCResolveWriteIntentOptions{})
				require.NoError(t, err)

				valueRes, vh, err := MVCCGetWithValueHeader(ctx, engine, testKey1, hlc.Timestamp{WallTime: 4}, MVCCGetOptions{})
				require.NoError(t, err)
				require.NotNil(t, valueRes.Value)
				require.Equal(t, omitInRangefeeds, vh.OmitInRangefeeds)
				require.Equal(t, originID, vh.OriginID)

				txn = *txn2
				// Transactional delete
				_, _, err = MVCCDelete(ctx, engine, testKey1, txn.WriteTimestamp,
					MVCCWriteOptions{Txn: &txn, OmitInRangefeeds: omitInRangefeeds, OriginID: originID})
				require.NoError(t, err)

				txnCommit = txn
				txnCommit.Status = roachpb.COMMITTED
				txnCommit.WriteTimestamp = txn.WriteTimestamp
				_, _, _, _, err = MVCCResolveWriteIntent(ctx, engine, nil,
					roachpb.MakeLockUpdate(&txnCommit, roachpb.Span{Key: testKey1}),
					MVCCResolveWriteIntentOptions{})
				require.NoError(t, err)

				// Read the latest version with tombstone.
				valueRes, vh, err = MVCCGetWithValueHeader(ctx, engine, testKey1, hlc.Timestamp{WallTime: 4},
					MVCCGetOptions{Tombstones: true})
				require.NoError(t, err)
				require.NotNil(t, valueRes.Value)
				require.Zero(t, len(valueRes.Value.RawBytes))
				require.Equal(t, omitInRangefeeds, vh.OmitInRangefeeds)
				require.Equal(t, originID, vh.OriginID)

				// Non-transactional put (e.g. 1PC put)
				writeTs := hlc.Timestamp{Logical: 3}
				_, err = MVCCPut(ctx, engine, testKey1, writeTs, value2, MVCCWriteOptions{OmitInRangefeeds: omitInRangefeeds, OriginID: originID})
				require.NoError(t, err)

				valueRes, vh, err = MVCCGetWithValueHeader(ctx, engine, testKey1, hlc.Timestamp{WallTime: 4}, MVCCGetOptions{})
				require.NoError(t, err)
				require.NotNil(t, valueRes.Value)
				require.Equal(t, omitInRangefeeds, vh.OmitInRangefeeds)
				require.Equal(t, originID, vh.OriginID)

				// Non-transactional delete (e.g. 1PC delete)
				writeTs = hlc.Timestamp{Logical: 4}
				_, _, err = MVCCDelete(ctx, engine, testKey1, writeTs, MVCCWriteOptions{OmitInRangefeeds: omitInRangefeeds, OriginID: originID})
				require.NoError(t, err)

				// Read the latest version with tombstone.
				valueRes, vh, err = MVCCGetWithValueHeader(ctx, engine, testKey1, hlc.Timestamp{WallTime: 4},
					MVCCGetOptions{Tombstones: true})
				require.NoError(t, err)
				require.NotNil(t, valueRes.Value)
				require.Zero(t, len(valueRes.Value.RawBytes))
				require.Equal(t, omitInRangefeeds, vh.OmitInRangefeeds)
				require.Equal(t, originID, vh.OriginID)
			})
		}
	}
}

// TestMVCCWriteWithOlderTimestampAfterDeletionOfNonexistentKey tests a write
// that comes after a delete on a nonexistent key, with the write holding a
// timestamp earlier than the delete timestamp. The delete must write a
// tombstone with its timestamp in order to prevent the write from succeeding.
func TestMVCCWriteWithOlderTimestampAfterDeletionOfNonexistentKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	_, _, err := MVCCDelete(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 3}, MVCCWriteOptions{})
	require.NoError(t, err)

	_, err = MVCCPut(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 1}, value1, MVCCWriteOptions{})
	require.ErrorAs(t, err, new(*kvpb.WriteTooOldError))
	require.Regexp(t, err, "WriteTooOldError: write for key \"/db1\" at timestamp 0.000000001,0 too old; must write at or above 0.000000003,1")

	// The attempted write at ts(1,0) failed, so we should not be able to see it.
	valueRes, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 4}, MVCCGetOptions{})
	require.NoError(t, err)
	require.False(t, valueRes.Value.IsPresent())
}

func TestMVCCInlineWithTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	// Put an inline value.
	if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{}, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}

	// Now verify inline get.
	valueRes, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{}, MVCCGetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(value1, *valueRes.Value) {
		t.Errorf("the inline value should be %v; got %v", value1, *valueRes.Value)
	}

	// Verify inline get with txn does still work (this will happen on a
	// scan if the distributed sender is forced to wrap it in a txn).
	if _, err = MVCCGet(ctx, engine, testKey1, hlc.Timestamp{}, MVCCGetOptions{
		Txn: txn1,
	}); err != nil {
		t.Error(err)
	}

	// Verify inline put with txn is an error.
	_, err = MVCCPut(ctx, engine, testKey2, hlc.Timestamp{}, value2, MVCCWriteOptions{Txn: txn2})
	if !testutils.IsError(err, "writes not allowed within transactions") {
		t.Errorf("unexpected error: %+v", err)
	}
}

func TestMVCCDeleteMissingKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	DisableMetamorphicSimpleValueEncoding(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	if _, _, err := MVCCDelete(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	require.Empty(t, mvccGetRaw(t, engine, mvccKey(testKey1)))
}

func TestMVCCGetAndDeleteInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	txn := makeTxn(*txn1, hlc.Timestamp{WallTime: 1})
	txn.Sequence++
	if _, err := MVCCPut(ctx, engine, testKey1, txn.ReadTimestamp, value1, MVCCWriteOptions{Txn: txn}); err != nil {
		t.Fatal(err)
	}

	if valueRes, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 2}, MVCCGetOptions{
		Txn: txn,
	}); err != nil {
		t.Fatal(err)
	} else if valueRes.Value == nil {
		t.Fatal("the value should not be empty")
	}

	txn.Sequence++
	txn.WriteTimestamp = hlc.Timestamp{WallTime: 3}
	if _, _, err := MVCCDelete(ctx, engine, testKey1, txn.ReadTimestamp, MVCCWriteOptions{Txn: txn}); err != nil {
		t.Fatal(err)
	}

	// Read the latest version which should be deleted.
	if valueRes, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 4}, MVCCGetOptions{
		Txn: txn,
	}); err != nil {
		t.Fatal(err)
	} else if valueRes.Value != nil {
		t.Fatal("the value should be empty")
	}
	// Read the latest version with tombstone.
	if valueRes, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 4}, MVCCGetOptions{
		Tombstones: true,
		Txn:        txn,
	}); err != nil {
		t.Fatal(err)
	} else if valueRes.Value == nil || len(valueRes.Value.RawBytes) != 0 {
		t.Fatalf("the value should be non-nil with empty RawBytes; got %+v", valueRes.Value)
	}

	// Read the old version which shouldn't exist, as within a
	// transaction, we delete previous values.
	if valueRes, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 2}, MVCCGetOptions{}); err != nil {
		t.Fatal(err)
	} else if valueRes.Value != nil {
		t.Fatalf("expected value nil, got: %s", valueRes.Value)
	}
}

func TestMVCCGetLockConflictError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	if _, err := MVCCPut(ctx, engine, testKey1, txn1.ReadTimestamp, value1, MVCCWriteOptions{Txn: txn1}); err != nil {
		t.Fatal(err)
	}

	if _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{}); err == nil {
		t.Fatal("cannot read the value of a write intent without TxnID")
	}

	if _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{
		Txn: txn2,
	}); err == nil {
		t.Fatal("cannot read the value of a write intent from a different TxnID")
	}
}

func mkVal(s string, ts hlc.Timestamp) roachpb.Value {
	v := roachpb.MakeValueFromString(s)
	v.Timestamp = ts
	return v
}

func TestMVCCScanLockConflictError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	ts := []hlc.Timestamp{{Logical: 1}, {Logical: 2}, {Logical: 3}, {Logical: 4}, {Logical: 5}, {Logical: 6}, {Logical: 7}}

	txn1ts := makeTxn(*txn1, ts[2])
	txn2ts := makeTxn(*txn2, ts[5])
	txnMap := map[int]*roachpb.Transaction{
		2: txn1ts,
		5: txn2ts,
		6: txn2ts,
		7: txn2ts,
	}

	fixtureKVs := []roachpb.KeyValue{
		{Key: testKey1, Value: mkVal("testValue1 pre", ts[0])},
		{Key: testKey4, Value: mkVal("testValue4 pre", ts[1])},
		{Key: testKey1, Value: mkVal("testValue1", ts[2])},
		{Key: testKey2, Value: mkVal("testValue2", ts[3])},
		{Key: testKey3, Value: mkVal("testValue3", ts[4])},
		{Key: testKey4, Value: mkVal("testValue4", ts[5])},
		{Key: testKey5, Value: mkVal("testValue5", ts[5])},
		{Key: testKey6, Value: mkVal("testValue5", ts[5])},
	}
	for i, kv := range fixtureKVs {
		v := *protoutil.Clone(&kv.Value).(*roachpb.Value)
		v.Timestamp = hlc.Timestamp{}
		if _, err := MVCCPut(ctx, engine, kv.Key, kv.Value.Timestamp, v, MVCCWriteOptions{Txn: txnMap[i]}); err != nil {
			t.Fatal(err)
		}
	}

	scanCases := []struct {
		name       string
		consistent bool
		txn        *roachpb.Transaction
		expIntents []roachpb.Intent
		expValues  []roachpb.KeyValue
	}{
		{
			name:       "consistent-all-keys",
			consistent: true,
			txn:        nil,
			expIntents: []roachpb.Intent{
				roachpb.MakeIntent(&txn1ts.TxnMeta, testKey1),
				roachpb.MakeIntent(&txn2ts.TxnMeta, testKey4),
			},
			// would be []roachpb.KeyValue{fixtureKVs[3], fixtureKVs[4]} without LockConflictError
			expValues: nil,
		},
		{
			name:       "consistent-txn1",
			consistent: true,
			txn:        txn1ts,
			expIntents: []roachpb.Intent{
				roachpb.MakeIntent(&txn2ts.TxnMeta, testKey4),
				roachpb.MakeIntent(&txn2ts.TxnMeta, testKey5),
			},
			expValues: nil, // []roachpb.KeyValue{fixtureKVs[2], fixtureKVs[3], fixtureKVs[4]},
		},
		{
			name:       "consistent-txn2",
			consistent: true,
			txn:        txn2ts,
			expIntents: []roachpb.Intent{
				roachpb.MakeIntent(&txn1ts.TxnMeta, testKey1),
			},
			expValues: nil, // []roachpb.KeyValue{fixtureKVs[3], fixtureKVs[4], fixtureKVs[5]},
		},
		{
			name:       "inconsistent-all-keys",
			consistent: false,
			txn:        nil,
			expIntents: []roachpb.Intent{
				roachpb.MakeIntent(&txn1ts.TxnMeta, testKey1),
				roachpb.MakeIntent(&txn2ts.TxnMeta, testKey4),
				roachpb.MakeIntent(&txn2ts.TxnMeta, testKey5),
				roachpb.MakeIntent(&txn2ts.TxnMeta, testKey6),
			},
			expValues: []roachpb.KeyValue{fixtureKVs[0], fixtureKVs[3], fixtureKVs[4], fixtureKVs[1]},
		},
	}

	for _, scan := range scanCases {
		t.Run(scan.name, func(t *testing.T) {
			res, err := MVCCScan(ctx, engine, testKey1, testKey6.Next(),
				hlc.Timestamp{WallTime: 1}, MVCCScanOptions{Inconsistent: !scan.consistent, Txn: scan.txn, MaxLockConflicts: 2})
			var lcErr *kvpb.LockConflictError
			_ = errors.As(err, &lcErr)
			if (err == nil) != (lcErr == nil) {
				t.Errorf("unexpected error: %+v", err)
			}

			if lcErr == nil != !scan.consistent {
				t.Fatalf("expected lock conflict error; got %s", err)
			}

			locks := roachpb.AsLocks(res.Intents)
			kvs := res.KVs
			if len(locks) > 0 != !scan.consistent {
				t.Fatalf("expected different intents slice; got %+v", locks)
			}

			if scan.consistent {
				locks = lcErr.Locks
			}

			expLocks := roachpb.AsLocks(scan.expIntents)
			if !reflect.DeepEqual(locks, expLocks) {
				t.Fatalf("expected locks:\n%+v;\n got\n%+v", expLocks, locks)
			}

			if !reflect.DeepEqual(kvs, scan.expValues) {
				t.Fatalf("expected values %+v; got %+v", scan.expValues, kvs)
			}
		})
	}
}

// TestMVCCGetInconsistent verifies the behavior of get with
// consistent set to false.
func TestMVCCGetInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	// Put two values to key 1, the latest with a txn.
	if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	txn1ts := makeTxn(*txn1, hlc.Timestamp{WallTime: 2})
	if _, err := MVCCPut(ctx, engine, testKey1, txn1ts.ReadTimestamp, value2, MVCCWriteOptions{Txn: txn1ts}); err != nil {
		t.Fatal(err)
	}

	// A get with consistent=false should fail in a txn.
	if _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{
		Inconsistent: true,
		Txn:          txn1,
	}); err == nil {
		t.Error("expected an error getting with consistent=false in txn")
	}

	// Inconsistent get will fetch value1 for any timestamp.
	for _, ts := range []hlc.Timestamp{{WallTime: 1}, {WallTime: 2}} {
		res, err := MVCCGet(ctx, engine, testKey1, ts, MVCCGetOptions{Inconsistent: true})
		if ts.Less(hlc.Timestamp{WallTime: 2}) {
			if err != nil {
				t.Fatal(err)
			}
		} else {
			if res.Intent == nil || !res.Intent.Key.Equal(testKey1) {
				t.Fatalf("expected %v, but got %v", testKey1, res.Intent)
			}
		}
		if !bytes.Equal(res.Value.RawBytes, value1.RawBytes) {
			t.Errorf("@%s expected %q; got %q", ts, value1.RawBytes, res.Value.RawBytes)
		}
	}

	// Write a single intent for key 2 and verify get returns empty.
	if _, err := MVCCPut(ctx, engine, testKey2, txn2.ReadTimestamp, value1, MVCCWriteOptions{Txn: txn2}); err != nil {
		t.Fatal(err)
	}
	res, err := MVCCGet(ctx, engine, testKey2, hlc.Timestamp{WallTime: 2},
		MVCCGetOptions{Inconsistent: true})
	if res.Intent == nil || !res.Intent.Key.Equal(testKey2) {
		t.Fatal(err)
	}
	if res.Value != nil {
		t.Errorf("expected empty val; got %+v", res.Value)
	}
}

// TestMVCCGetProtoInconsistent verifies the behavior of MVCCGetProto with
// consistent set to false.
func TestMVCCGetProtoInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	bytes1, err := protoutil.Marshal(&value1)
	if err != nil {
		t.Fatal(err)
	}
	bytes2, err := protoutil.Marshal(&value2)
	if err != nil {
		t.Fatal(err)
	}

	v1 := roachpb.MakeValueFromBytes(bytes1)
	v2 := roachpb.MakeValueFromBytes(bytes2)

	// Put two values to key 1, the latest with a txn.
	if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, v1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	txn1ts := makeTxn(*txn1, hlc.Timestamp{WallTime: 2})
	if _, err := MVCCPut(ctx, engine, testKey1, txn1ts.ReadTimestamp, v2, MVCCWriteOptions{Txn: txn1ts}); err != nil {
		t.Fatal(err)
	}

	// An inconsistent get should fail in a txn.
	if _, err := MVCCGetProto(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, nil, MVCCGetOptions{
		Inconsistent: true,
		Txn:          txn1,
	}); err == nil {
		t.Error("expected an error getting inconsistently in txn")
	} else if errors.HasType(err, (*kvpb.LockConflictError)(nil)) {
		t.Error("expected non-LockConflictError with inconsistent read in txn")
	}

	// Inconsistent get will fetch value1 for any timestamp.

	for _, ts := range []hlc.Timestamp{{WallTime: 1}, {WallTime: 2}} {
		val := roachpb.Value{}
		found, err := MVCCGetProto(ctx, engine, testKey1, ts, &val, MVCCGetOptions{
			Inconsistent: true,
		})
		if ts.Less(hlc.Timestamp{WallTime: 2}) {
			if err != nil {
				t.Fatal(err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Errorf("expected to find result with inconsistent read")
		}
		valBytes, err := val.GetBytes()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(valBytes, []byte("testValue1")) {
			t.Errorf("@%s expected %q; got %q", ts, []byte("value1"), valBytes)
		}
	}

	{
		// Write a single intent for key 2 and verify get returns empty.
		if _, err := MVCCPut(ctx, engine, testKey2, txn2.ReadTimestamp, v1, MVCCWriteOptions{Txn: txn2}); err != nil {
			t.Fatal(err)
		}
		val := roachpb.Value{}
		found, err := MVCCGetProto(ctx, engine, testKey2, hlc.Timestamp{WallTime: 2}, &val, MVCCGetOptions{
			Inconsistent: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if found {
			t.Errorf("expected no result; got %+v", val)
		}
	}

	{
		// Write a malformed value (not an encoded MVCCKeyValue) and a
		// write intent to key 3; the parse error is returned instead of the
		// write intent.
		if _, err := MVCCPut(ctx, engine, testKey3, hlc.Timestamp{WallTime: 1}, value3, MVCCWriteOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, err := MVCCPut(ctx, engine, testKey3, txn1ts.ReadTimestamp, v2, MVCCWriteOptions{Txn: txn1ts}); err != nil {
			t.Fatal(err)
		}
		val := roachpb.Value{}
		found, err := MVCCGetProto(ctx, engine, testKey3, hlc.Timestamp{WallTime: 1}, &val, MVCCGetOptions{
			Inconsistent: true,
		})
		if err == nil {
			t.Errorf("expected error reading malformed data")
		} else if !strings.HasPrefix(err.Error(), "proto: ") {
			t.Errorf("expected proto error, got %s", err)
		}
		if !found {
			t.Errorf("expected to find result with malformed data")
		}
	}
}

// Regression test for #28205: MVCCGet and MVCCScan, FindSplitKey, and
// ComputeStatsForIter need to invalidate the cached iterator data.
func TestMVCCInvalidateIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, which := range []string{"get", "scan", "findSplitKey", "computeStats"} {
		t.Run(which, func(t *testing.T) {
			engine := NewDefaultInMemForTesting()
			defer engine.Close()

			ctx := context.Background()
			ts1 := hlc.Timestamp{WallTime: 1}
			ts2 := hlc.Timestamp{WallTime: 2}

			key := roachpb.Key("a")
			if _, err := MVCCPut(ctx, engine, key, ts1, value1, MVCCWriteOptions{}); err != nil {
				t.Fatal(err)
			}

			var iterOptions IterOptions
			switch which {
			case "get":
				iterOptions.Prefix = true
			case "computeStats":
				iterOptions.KeyTypes = IterKeyTypePointsAndRanges
				iterOptions.UpperBound = roachpb.KeyMax
			case "scan", "findSplitKey":
				iterOptions.UpperBound = roachpb.KeyMax
			}

			// Use a batch which internally caches the iterator.
			batch := engine.NewBatch()
			defer batch.Close()

			{
				// Seek the iter to a valid position.
				iter, err := batch.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, iterOptions)
				if err != nil {
					t.Fatal(err)
				}
				iter.SeekGE(MakeMVCCMetadataKey(key))
				iter.Close()
			}

			var err error
			switch which {
			case "get":
				_, err = MVCCGet(ctx, batch, key, ts2, MVCCGetOptions{})
			case "scan":
				_, err = MVCCScan(ctx, batch, key, roachpb.KeyMax, ts2, MVCCScanOptions{})
			case "findSplitKey":
				_, err = MVCCFindSplitKey(ctx, batch, roachpb.RKeyMin, roachpb.RKeyMax, 64<<20)
			case "computeStatsForIter":
				iter, err := batch.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, iterOptions)
				if err != nil {
					t.Fatal(err)
				}
				iter.SeekGE(MVCCKey{Key: iterOptions.LowerBound})
				_, err = ComputeStatsForIter(iter, 0)
				if err != nil {
					t.Fatal(err)
				}
				iter.Close()
			}
			if err != nil {
				t.Fatal(err)
			}

			// Verify that the iter is invalid.
			iter, err := batch.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, iterOptions)
			if err != nil {
				t.Fatal(err)
			}
			defer iter.Close()
			if ok, _ := iter.Valid(); ok {
				t.Fatalf("iterator should not be valid")
			}
		})
	}
}

func mvccScanTest(ctx context.Context, t *testing.T, engine Engine) {
	if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 2}, value4, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey2, hlc.Timestamp{WallTime: 1}, value2, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey2, hlc.Timestamp{WallTime: 3}, value3, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey3, hlc.Timestamp{WallTime: 1}, value3, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey3, hlc.Timestamp{WallTime: 4}, value2, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey4, hlc.Timestamp{WallTime: 1}, value4, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey4, hlc.Timestamp{WallTime: 5}, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}

	res, err := MVCCScan(ctx, engine, testKey2, testKey4,
		hlc.Timestamp{WallTime: 1}, MVCCScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	kvs := res.KVs
	resumeSpan := res.ResumeSpan
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, testKey2) ||
		!bytes.Equal(kvs[1].Key, testKey3) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value2.RawBytes) ||
		!bytes.Equal(kvs[1].Value.RawBytes, value3.RawBytes) {
		t.Fatal("the value should not be empty")
	}
	if resumeSpan != nil {
		t.Fatalf("resumeSpan = %+v", resumeSpan)
	}

	res, err = MVCCScan(ctx, engine, testKey2, testKey4,
		hlc.Timestamp{WallTime: 4}, MVCCScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	kvs = res.KVs
	resumeSpan = res.ResumeSpan
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, testKey2) ||
		!bytes.Equal(kvs[1].Key, testKey3) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value3.RawBytes) ||
		!bytes.Equal(kvs[1].Value.RawBytes, value2.RawBytes) {
		t.Fatal("the value should not be empty")
	}
	if resumeSpan != nil {
		t.Fatalf("resumeSpan = %+v", resumeSpan)
	}

	res, err = MVCCScan(
		ctx, engine, testKey4, keyMax, hlc.Timestamp{WallTime: 1}, MVCCScanOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	kvs = res.KVs
	resumeSpan = res.ResumeSpan
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey4) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value4.RawBytes) {
		t.Fatal("the value should not be empty")
	}
	if resumeSpan != nil {
		t.Fatalf("resumeSpan = %+v", resumeSpan)
	}

	if _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{
		Txn: txn2,
	}); err != nil {
		t.Fatal(err)
	}
	res, err = MVCCScan(ctx, engine, localMax, testKey2,
		hlc.Timestamp{WallTime: 1}, MVCCScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	kvs = res.KVs
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey1) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value1.RawBytes) {
		t.Fatal("the value should not be empty")
	}
}

func TestMVCCScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()
	mvccScanTest(ctx, t, engine)
}

func TestMVCCScanMaxNum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey2, hlc.Timestamp{WallTime: 1}, value2, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey3, hlc.Timestamp{WallTime: 1}, value3, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey4, hlc.Timestamp{WallTime: 1}, value4, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey6, hlc.Timestamp{WallTime: 1}, value4, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}

	res, err := MVCCScan(ctx, engine, testKey2, testKey4,
		hlc.Timestamp{WallTime: 1}, MVCCScanOptions{MaxKeys: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 1 ||
		!bytes.Equal(res.KVs[0].Key, testKey2) ||
		!bytes.Equal(res.KVs[0].Value.RawBytes, value2.RawBytes) {
		t.Fatal("the value should not be empty")
	}
	if expected := (roachpb.Span{Key: testKey3, EndKey: testKey4}); !res.ResumeSpan.EqualValue(expected) {
		t.Fatalf("expected = %+v, resumeSpan = %+v", expected, res.ResumeSpan)
	}

	res, err = MVCCScan(ctx, engine, testKey2, testKey4,
		hlc.Timestamp{WallTime: 1}, MVCCScanOptions{MaxKeys: -1})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 0 {
		t.Fatal("the value should be empty")
	}
	if expected := (roachpb.Span{Key: testKey2, EndKey: testKey4}); !res.ResumeSpan.EqualValue(expected) {
		t.Fatalf("expected = %+v, resumeSpan = %+v", expected, res.ResumeSpan)
	}

	// Note: testKey6, though not scanned directly, is important in testing that
	// the computed resume span does not extend beyond the upper bound of a scan.
	res, err = MVCCScan(ctx, engine, testKey4, testKey5,
		hlc.Timestamp{WallTime: 1}, MVCCScanOptions{MaxKeys: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 1 {
		t.Fatalf("expected 1 key but got %d", len(res.KVs))
	}
	if res.ResumeSpan != nil {
		t.Fatalf("resumeSpan = %+v", res.ResumeSpan)
	}

	res, err = MVCCScan(ctx, engine, testKey5, testKey6.Next(),
		hlc.Timestamp{WallTime: 1}, MVCCScanOptions{Reverse: true, MaxKeys: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 1 {
		t.Fatalf("expected 1 key but got %d", len(res.KVs))
	}
	if res.ResumeSpan != nil {
		t.Fatalf("resumeSpan = %+v", res.ResumeSpan)
	}
}

func TestMVCCScanWithKeyPrefix(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	// Let's say you have:
	// a
	// a<T=2>
	// a<T=1>
	// aa
	// aa<T=3>
	// aa<T=2>
	// b
	// b<T=5>
	// In this case, if we scan from "a"-"b", we wish to skip
	// a<T=2> and a<T=1> and find "aa'.
	if _, err := MVCCPut(ctx, engine, roachpb.Key("/a"), hlc.Timestamp{WallTime: 1}, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, roachpb.Key("/a"), hlc.Timestamp{WallTime: 2}, value2, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, roachpb.Key("/aa"), hlc.Timestamp{WallTime: 2}, value2, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, roachpb.Key("/aa"), hlc.Timestamp{WallTime: 3}, value3, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, roachpb.Key("/b"), hlc.Timestamp{WallTime: 1}, value3, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}

	res, err := MVCCScan(ctx, engine, roachpb.Key("/a"), roachpb.Key("/b"),
		hlc.Timestamp{WallTime: 2}, MVCCScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 2 ||
		!bytes.Equal(res.KVs[0].Key, roachpb.Key("/a")) ||
		!bytes.Equal(res.KVs[1].Key, roachpb.Key("/aa")) ||
		!bytes.Equal(res.KVs[0].Value.RawBytes, value2.RawBytes) ||
		!bytes.Equal(res.KVs[1].Value.RawBytes, value2.RawBytes) {
		t.Fatal("the value should not be empty")
	}
}

func TestMVCCScanInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey2, hlc.Timestamp{WallTime: 1}, value2, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	txn := makeTxn(*txn1, hlc.Timestamp{WallTime: 1})
	if _, err := MVCCPut(ctx, engine, testKey3, txn.ReadTimestamp, value3, MVCCWriteOptions{Txn: txn}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey4, hlc.Timestamp{WallTime: 1}, value4, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}

	res, err := MVCCScan(ctx, engine, testKey2, testKey4,
		hlc.Timestamp{WallTime: 1}, MVCCScanOptions{Txn: txn1})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 2 ||
		!bytes.Equal(res.KVs[0].Key, testKey2) ||
		!bytes.Equal(res.KVs[1].Key, testKey3) ||
		!bytes.Equal(res.KVs[0].Value.RawBytes, value2.RawBytes) ||
		!bytes.Equal(res.KVs[1].Value.RawBytes, value3.RawBytes) {
		t.Fatal("the value should not be empty")
	}

	if _, err := MVCCScan(
		ctx, engine, testKey2, testKey4, hlc.Timestamp{WallTime: 1}, MVCCScanOptions{},
	); err == nil {
		t.Fatal("expected error on uncommitted write intent")
	}
}

// TestMVCCScanInconsistent writes several values, some as intents and
// verifies that the scan sees only the committed versions.
func TestMVCCScanInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	// A scan with consistent=false should fail in a txn.
	if _, err := MVCCScan(
		ctx, engine, localMax, keyMax, hlc.Timestamp{WallTime: 1},
		MVCCScanOptions{Inconsistent: true, Txn: txn1},
	); err == nil {
		t.Error("expected an error scanning with consistent=false in txn")
	}

	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	ts3 := hlc.Timestamp{WallTime: 3}
	ts4 := hlc.Timestamp{WallTime: 4}
	ts5 := hlc.Timestamp{WallTime: 5}
	ts6 := hlc.Timestamp{WallTime: 6}
	if _, err := MVCCPut(ctx, engine, testKey1, ts1, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	txn1ts2 := makeTxn(*txn1, ts2)
	if _, err := MVCCPut(ctx, engine, testKey1, txn1ts2.ReadTimestamp, value2, MVCCWriteOptions{Txn: txn1ts2}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey2, ts3, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey2, ts4, value2, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	txn2ts5 := makeTxn(*txn2, ts5)
	if _, err := MVCCPut(ctx, engine, testKey3, txn2ts5.ReadTimestamp, value3, MVCCWriteOptions{Txn: txn2ts5}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey4, ts6, value4, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}

	expIntents := []roachpb.Intent{
		roachpb.MakeIntent(&txn1ts2.TxnMeta, testKey1),
		roachpb.MakeIntent(&txn2ts5.TxnMeta, testKey3),
	}
	res, err := MVCCScan(
		ctx, engine, testKey1, testKey4.Next(), hlc.Timestamp{WallTime: 7},
		MVCCScanOptions{Inconsistent: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res.Intents, expIntents) {
		t.Fatalf("expected %v, but found %v", expIntents, res.Intents)
	}

	makeTimestampedValue := func(v roachpb.Value, ts hlc.Timestamp) roachpb.Value {
		v.Timestamp = ts
		return v
	}

	expKVs := []roachpb.KeyValue{
		{Key: testKey1, Value: makeTimestampedValue(value1, ts1)},
		{Key: testKey2, Value: makeTimestampedValue(value2, ts4)},
		{Key: testKey4, Value: makeTimestampedValue(value4, ts6)},
	}
	if !reflect.DeepEqual(res.KVs, expKVs) {
		t.Errorf("expected key values equal %v != %v", res.KVs, expKVs)
	}

	// Now try a scan at a historical timestamp.
	expIntents = expIntents[:1]
	res, err = MVCCScan(ctx, engine, testKey1, testKey4.Next(),
		hlc.Timestamp{WallTime: 3}, MVCCScanOptions{Inconsistent: true})
	if !reflect.DeepEqual(res.Intents, expIntents) {
		t.Fatal(err)
	}
	expKVs = []roachpb.KeyValue{
		{Key: testKey1, Value: makeTimestampedValue(value1, ts1)},
		{Key: testKey2, Value: makeTimestampedValue(value1, ts3)},
	}
	if !reflect.DeepEqual(res.KVs, expKVs) {
		t.Errorf("expected key values equal %v != %v", res.Intents, expKVs)
	}
}

func TestMVCCDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey2, hlc.Timestamp{WallTime: 1}, value2, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey3, hlc.Timestamp{WallTime: 1}, value3, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey4, hlc.Timestamp{WallTime: 1}, value4, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey5, hlc.Timestamp{WallTime: 1}, value5, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey6, hlc.Timestamp{WallTime: 1}, value6, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}

	// Attempt to delete two keys.
	deleted, resumeSpan, num, _, err := MVCCDeleteRange(ctx, engine, testKey2, testKey6,
		2, hlc.Timestamp{WallTime: 2}, MVCCWriteOptions{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != nil {
		t.Fatal("the value should be empty")
	}
	if num != 2 {
		t.Fatalf("incorrect number of keys deleted: %d", num)
	}
	if expected := (roachpb.Span{Key: testKey4, EndKey: testKey6}); !resumeSpan.EqualValue(expected) {
		t.Fatalf("expected = %+v, resumeSpan = %+v", expected, resumeSpan)
	}
	res, _ := MVCCScan(ctx, engine, localMax, keyMax,
		hlc.Timestamp{WallTime: 2}, MVCCScanOptions{})
	if len(res.KVs) != 4 ||
		!bytes.Equal(res.KVs[0].Key, testKey1) ||
		!bytes.Equal(res.KVs[1].Key, testKey4) ||
		!bytes.Equal(res.KVs[2].Key, testKey5) ||
		!bytes.Equal(res.KVs[3].Key, testKey6) ||
		!bytes.Equal(res.KVs[0].Value.RawBytes, value1.RawBytes) ||
		!bytes.Equal(res.KVs[1].Value.RawBytes, value4.RawBytes) ||
		!bytes.Equal(res.KVs[2].Value.RawBytes, value5.RawBytes) ||
		!bytes.Equal(res.KVs[3].Value.RawBytes, value6.RawBytes) {
		t.Fatal("the value should not be empty")
	}

	// Try again, but with tombstones set to true to fetch the deleted keys as well.
	kvs := []roachpb.KeyValue{}
	if _, err = MVCCIterate(
		ctx, engine, localMax, keyMax, hlc.Timestamp{WallTime: 2}, MVCCScanOptions{Tombstones: true},
		func(kv roachpb.KeyValue) error {
			kvs = append(kvs, kv)
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 6 ||
		!bytes.Equal(kvs[0].Key, testKey1) ||
		!bytes.Equal(kvs[1].Key, testKey2) ||
		!bytes.Equal(kvs[2].Key, testKey3) ||
		!bytes.Equal(kvs[3].Key, testKey4) ||
		!bytes.Equal(kvs[4].Key, testKey5) ||
		!bytes.Equal(kvs[5].Key, testKey6) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value1.RawBytes) ||
		!bytes.Equal(kvs[1].Value.RawBytes, nil) ||
		!bytes.Equal(kvs[2].Value.RawBytes, nil) ||
		!bytes.Equal(kvs[3].Value.RawBytes, value4.RawBytes) ||
		!bytes.Equal(kvs[4].Value.RawBytes, value5.RawBytes) ||
		!bytes.Equal(kvs[5].Value.RawBytes, value6.RawBytes) {
		t.Fatal("the value should not be empty")
	}

	// Attempt to delete no keys.
	deleted, resumeSpan, num, _, err = MVCCDeleteRange(ctx, engine, testKey2, testKey6,
		-1, hlc.Timestamp{WallTime: 2}, MVCCWriteOptions{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != nil {
		t.Fatal("the value should be empty")
	}
	if num != 0 {
		t.Fatalf("incorrect number of keys deleted: %d", num)
	}
	if expected := (roachpb.Span{Key: testKey2, EndKey: testKey6}); !resumeSpan.EqualValue(expected) {
		t.Fatalf("expected = %+v, resumeSpan = %+v", expected, resumeSpan)
	}
	res, _ = MVCCScan(ctx, engine, localMax, keyMax, hlc.Timestamp{WallTime: 2},
		MVCCScanOptions{})
	if len(res.KVs) != 4 ||
		!bytes.Equal(res.KVs[0].Key, testKey1) ||
		!bytes.Equal(res.KVs[1].Key, testKey4) ||
		!bytes.Equal(res.KVs[2].Key, testKey5) ||
		!bytes.Equal(res.KVs[3].Key, testKey6) ||
		!bytes.Equal(res.KVs[0].Value.RawBytes, value1.RawBytes) ||
		!bytes.Equal(res.KVs[1].Value.RawBytes, value4.RawBytes) ||
		!bytes.Equal(res.KVs[2].Value.RawBytes, value5.RawBytes) ||
		!bytes.Equal(res.KVs[3].Value.RawBytes, value6.RawBytes) {
		t.Fatal("the value should not be empty")
	}

	deleted, resumeSpan, num, _, err = MVCCDeleteRange(ctx, engine, testKey4, keyMax,
		0, hlc.Timestamp{WallTime: 2}, MVCCWriteOptions{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != nil {
		t.Fatal("the value should be empty")
	}
	if num != 3 {
		t.Fatalf("incorrect number of keys deleted: %d", num)
	}
	if resumeSpan != nil {
		t.Fatalf("wrong resume key: expected nil, found %v", resumeSpan)
	}
	res, err = MVCCScan(ctx, engine, localMax, keyMax, hlc.Timestamp{WallTime: 2},
		MVCCScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 1 ||
		!bytes.Equal(res.KVs[0].Key, testKey1) ||
		!bytes.Equal(res.KVs[0].Value.RawBytes, value1.RawBytes) {
		t.Fatalf("the value should not be empty: %+v", res.KVs)
	}

	deleted, resumeSpan, num, _, err = MVCCDeleteRange(ctx, engine, localMax, testKey2,
		0, hlc.Timestamp{WallTime: 2}, MVCCWriteOptions{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != nil {
		t.Fatal("the value should not be empty")
	}
	if num != 1 {
		t.Fatalf("incorrect number of keys deleted: %d", num)
	}
	if resumeSpan != nil {
		t.Fatalf("wrong resume key: expected nil, found %v", resumeSpan)
	}
	res, _ = MVCCScan(ctx, engine, localMax, keyMax, hlc.Timestamp{WallTime: 2},
		MVCCScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 0 {
		t.Fatal("the value should be empty")
	}
}

func TestMVCCDeleteRangeReturnKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey2, hlc.Timestamp{WallTime: 1}, value2, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey3, hlc.Timestamp{WallTime: 1}, value3, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey4, hlc.Timestamp{WallTime: 1}, value4, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey5, hlc.Timestamp{WallTime: 1}, value5, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey6, hlc.Timestamp{WallTime: 1}, value6, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}

	// Attempt to delete two keys.
	deleted, resumeSpan, num, _, err := MVCCDeleteRange(ctx, engine, testKey2, testKey6,
		2, hlc.Timestamp{WallTime: 2}, MVCCWriteOptions{}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 2 {
		t.Fatal("the value should not be empty")
	}
	if num != 2 {
		t.Fatalf("incorrect number of keys deleted: %d", num)
	}
	if expected, actual := testKey2, deleted[0]; !expected.Equal(actual) {
		t.Fatalf("wrong key deleted: expected %v found %v", expected, actual)
	}
	if expected, actual := testKey3, deleted[1]; !expected.Equal(actual) {
		t.Fatalf("wrong key deleted: expected %v found %v", expected, actual)
	}
	if expected := (roachpb.Span{Key: testKey4, EndKey: testKey6}); !resumeSpan.EqualValue(expected) {
		t.Fatalf("expected = %+v, resumeSpan = %+v", expected, resumeSpan)
	}
	res, _ := MVCCScan(ctx, engine, localMax, keyMax, hlc.Timestamp{WallTime: 2},
		MVCCScanOptions{})
	if len(res.KVs) != 4 ||
		!bytes.Equal(res.KVs[0].Key, testKey1) ||
		!bytes.Equal(res.KVs[1].Key, testKey4) ||
		!bytes.Equal(res.KVs[2].Key, testKey5) ||
		!bytes.Equal(res.KVs[3].Key, testKey6) ||
		!bytes.Equal(res.KVs[0].Value.RawBytes, value1.RawBytes) ||
		!bytes.Equal(res.KVs[1].Value.RawBytes, value4.RawBytes) ||
		!bytes.Equal(res.KVs[2].Value.RawBytes, value5.RawBytes) ||
		!bytes.Equal(res.KVs[3].Value.RawBytes, value6.RawBytes) {
		t.Fatal("the value should not be empty")
	}

	// Attempt to delete no keys.
	deleted, resumeSpan, num, _, err = MVCCDeleteRange(ctx, engine, testKey2, testKey6,
		-1, hlc.Timestamp{WallTime: 2}, MVCCWriteOptions{}, true)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != nil {
		t.Fatalf("the value should be empty: %s", deleted)
	}
	if num != 0 {
		t.Fatalf("incorrect number of keys deleted: %d", num)
	}
	if expected := (roachpb.Span{Key: testKey2, EndKey: testKey6}); !resumeSpan.EqualValue(expected) {
		t.Fatalf("expected = %+v, resumeSpan = %+v", expected, resumeSpan)
	}
	res, _ = MVCCScan(ctx, engine, localMax, keyMax, hlc.Timestamp{WallTime: 2},
		MVCCScanOptions{})
	if len(res.KVs) != 4 ||
		!bytes.Equal(res.KVs[0].Key, testKey1) ||
		!bytes.Equal(res.KVs[1].Key, testKey4) ||
		!bytes.Equal(res.KVs[2].Key, testKey5) ||
		!bytes.Equal(res.KVs[3].Key, testKey6) ||
		!bytes.Equal(res.KVs[0].Value.RawBytes, value1.RawBytes) ||
		!bytes.Equal(res.KVs[1].Value.RawBytes, value4.RawBytes) ||
		!bytes.Equal(res.KVs[2].Value.RawBytes, value5.RawBytes) ||
		!bytes.Equal(res.KVs[3].Value.RawBytes, value6.RawBytes) {
		t.Fatal("the value should not be empty")
	}

	deleted, resumeSpan, num, _, err = MVCCDeleteRange(ctx, engine, testKey4, keyMax,
		math.MaxInt64, hlc.Timestamp{WallTime: 2}, MVCCWriteOptions{}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 3 {
		t.Fatal("the value should not be empty")
	}
	if num != 3 {
		t.Fatalf("incorrect number of keys deleted: %d", num)
	}
	if expected, actual := testKey4, deleted[0]; !expected.Equal(actual) {
		t.Fatalf("wrong key deleted: expected %v found %v", expected, actual)
	}
	if expected, actual := testKey5, deleted[1]; !expected.Equal(actual) {
		t.Fatalf("wrong key deleted: expected %v found %v", expected, actual)
	}
	if expected, actual := testKey6, deleted[2]; !expected.Equal(actual) {
		t.Fatalf("wrong key deleted: expected %v found %v", expected, actual)
	}
	if resumeSpan != nil {
		t.Fatalf("wrong resume key: expected nil, found %v", resumeSpan)
	}
	res, _ = MVCCScan(ctx, engine, localMax, keyMax, hlc.Timestamp{WallTime: 2},
		MVCCScanOptions{})
	if len(res.KVs) != 1 ||
		!bytes.Equal(res.KVs[0].Key, testKey1) ||
		!bytes.Equal(res.KVs[0].Value.RawBytes, value1.RawBytes) {
		t.Fatal("the value should not be empty")
	}

	deleted, resumeSpan, num, _, err = MVCCDeleteRange(ctx, engine, localMax, testKey2,
		math.MaxInt64, hlc.Timestamp{WallTime: 2}, MVCCWriteOptions{}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 1 {
		t.Fatal("the value should not be empty")
	}
	if num != 1 {
		t.Fatalf("incorrect number of keys deleted: %d", num)
	}
	if expected, actual := testKey1, deleted[0]; !expected.Equal(actual) {
		t.Fatalf("wrong key deleted: expected %v found %v", expected, actual)
	}
	if resumeSpan != nil {
		t.Fatalf("wrong resume key: %v", resumeSpan)
	}
	res, _ = MVCCScan(ctx, engine, localMax, keyMax, hlc.Timestamp{WallTime: 2},
		MVCCScanOptions{})
	if len(res.KVs) != 0 {
		t.Fatal("the value should be empty")
	}
}

func TestMVCCDeleteRangeFailed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	txn := makeTxn(*txn1, hlc.Timestamp{WallTime: 1})
	if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	txn.Sequence++
	if _, err := MVCCPut(ctx, engine, testKey2, txn.ReadTimestamp, value2, MVCCWriteOptions{Txn: txn}); err != nil {
		t.Fatal(err)
	}
	txn.Sequence++
	if _, err := MVCCPut(ctx, engine, testKey3, txn.ReadTimestamp, value3, MVCCWriteOptions{Txn: txn}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey4, hlc.Timestamp{WallTime: 1}, value4, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, _, _, _, err := MVCCDeleteRange(ctx, engine, testKey2, testKey4,
		math.MaxInt64, hlc.Timestamp{WallTime: 1}, MVCCWriteOptions{}, false); err == nil {
		t.Fatal("expected error on uncommitted write intent")
	}

	txn.Sequence++
	if _, _, _, _, err := MVCCDeleteRange(ctx, engine, testKey2, testKey4,
		math.MaxInt64, txn.ReadTimestamp, MVCCWriteOptions{Txn: txn}, false); err != nil {
		t.Fatal(err)
	}
}

func TestMVCCDeleteRangeConcurrentTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	txn1ts := makeTxn(*txn1, hlc.Timestamp{WallTime: 1})
	txn2ts := makeTxn(*txn2, hlc.Timestamp{WallTime: 2})

	if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey2, txn1ts.ReadTimestamp, value2, MVCCWriteOptions{Txn: txn1ts}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey3, txn2ts.ReadTimestamp, value3, MVCCWriteOptions{Txn: txn2ts}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey4, hlc.Timestamp{WallTime: 1}, value4, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, _, _, _, err := MVCCDeleteRange(ctx, engine, testKey2, testKey4,
		math.MaxInt64, txn1ts.ReadTimestamp, MVCCWriteOptions{Txn: txn1ts}, false,
	); err == nil {
		t.Fatal("expected error on uncommitted write intent")
	}
}

// TestMVCCUncommittedDeleteRangeVisible tests that the keys in an uncommitted
// DeleteRange are visible to the same transaction at a higher epoch.
func TestMVCCUncommittedDeleteRangeVisible(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey2, hlc.Timestamp{WallTime: 1}, value2, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey3, hlc.Timestamp{WallTime: 1}, value3, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, _, err := MVCCDelete(ctx, engine, testKey2, hlc.Timestamp{WallTime: 2, Logical: 1}, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}

	txn := makeTxn(*txn1, hlc.Timestamp{WallTime: 2, Logical: 2})
	if _, _, _, _, err := MVCCDeleteRange(ctx, engine, testKey1, testKey4,
		math.MaxInt64, txn.ReadTimestamp, MVCCWriteOptions{Txn: txn}, false,
	); err != nil {
		t.Fatal(err)
	}

	txn.Epoch++
	res, _ := MVCCScan(ctx, engine, testKey1, testKey4,
		hlc.Timestamp{WallTime: 3}, MVCCScanOptions{Txn: txn})
	if e := 2; len(res.KVs) != e {
		t.Fatalf("e = %d, got %d", e, len(res.KVs))
	}
}

// TestMVCCDeleteRangeOldTimestamp tests a case where a delete range with an
// older timestamp happens after a delete with a newer timestamp.
func TestMVCCDeleteRangeOldTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()
	_, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, value1, MVCCWriteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = MVCCPut(ctx, engine, testKey2, hlc.Timestamp{WallTime: 3}, value2, MVCCWriteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = MVCCDelete(ctx, engine, testKey2, hlc.Timestamp{WallTime: 5}, MVCCWriteOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Delete at a time before the tombstone. Should return a WriteTooOld error.
	b := engine.NewBatch()
	defer b.Close()
	keys, resume, keyCount, _, err := MVCCDeleteRange(ctx, b, testKey1, testKey4,
		math.MaxInt64, hlc.Timestamp{WallTime: 4}, MVCCWriteOptions{}, true)
	require.Nil(t, keys)
	require.Nil(t, resume)
	require.Equal(t, int64(0), keyCount)
	require.NotNil(t, err)
	require.IsType(t, (*kvpb.WriteTooOldError)(nil), err)

	// Delete at the same time as the tombstone. Should return a WriteTooOld error.
	b = engine.NewBatch()
	defer b.Close()
	keys, resume, keyCount, _, err = MVCCDeleteRange(ctx, b, testKey1, testKey4,
		math.MaxInt64, hlc.Timestamp{WallTime: 5}, MVCCWriteOptions{}, true)
	require.Nil(t, keys)
	require.Nil(t, resume)
	require.Equal(t, int64(0), keyCount)
	require.NotNil(t, err)
	require.IsType(t, (*kvpb.WriteTooOldError)(nil), err)

	// Delete at a time after the tombstone. Should succeed and should not
	// include the tombstone in the returned keys.
	b = engine.NewBatch()
	defer b.Close()
	keys, resume, keyCount, _, err = MVCCDeleteRange(ctx, b, testKey1, testKey4,
		math.MaxInt64, hlc.Timestamp{WallTime: 6}, MVCCWriteOptions{}, true)
	require.Equal(t, []roachpb.Key{testKey1}, keys)
	require.Nil(t, resume)
	require.Equal(t, int64(1), keyCount)
	require.NoError(t, err)
}

func TestMVCCDeleteRangeInline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	// Make five inline values (zero timestamp).
	for i, kv := range []struct {
		key   roachpb.Key
		value roachpb.Value
	}{
		{testKey1, value1},
		{testKey2, value2},
		{testKey3, value3},
		{testKey4, value4},
		{testKey5, value5},
	} {
		if _, err := MVCCPut(ctx, engine, kv.key, hlc.Timestamp{Logical: 0}, kv.value, MVCCWriteOptions{}); err != nil {
			t.Fatalf("%d: %+v", i, err)
		}
	}

	// Create one non-inline value (non-zero timestamp).
	if _, err := MVCCPut(ctx, engine, testKey6, hlc.Timestamp{WallTime: 1}, value6, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}

	// Attempt to delete two inline keys, should succeed.
	deleted, resumeSpan, num, _, err := MVCCDeleteRange(ctx, engine, testKey2, testKey6,
		2, hlc.Timestamp{Logical: 0}, MVCCWriteOptions{}, true)
	if err != nil {
		t.Fatal(err)
	}
	if expected := int64(2); num != expected {
		t.Fatalf("got %d deleted keys, expected %d", num, expected)
	}
	if expected := []roachpb.Key{testKey2, testKey3}; !reflect.DeepEqual(deleted, expected) {
		t.Fatalf("got deleted values = %v, expected = %v", deleted, expected)
	}
	if expected := (roachpb.Span{Key: testKey4, EndKey: testKey6}); !resumeSpan.EqualValue(expected) {
		t.Fatalf("got resume span = %s, expected = %s", resumeSpan, expected)
	}

	// Attempt to delete inline keys at a timestamp; should fail.
	const inlineMismatchErrString = "put is inline"
	if _, _, _, _, err := MVCCDeleteRange(ctx, engine, testKey1, testKey6,
		1, hlc.Timestamp{WallTime: 2}, MVCCWriteOptions{}, true,
	); !testutils.IsError(err, inlineMismatchErrString) {
		t.Fatalf("got error %v, expected error with text '%s'", err, inlineMismatchErrString)
	}

	// Attempt to delete non-inline key at zero timestamp; should fail.
	if _, _, _, _, err := MVCCDeleteRange(ctx, engine, testKey6, keyMax,
		1, hlc.Timestamp{Logical: 0}, MVCCWriteOptions{}, true,
	); !testutils.IsError(err, inlineMismatchErrString) {
		t.Fatalf("got error %v, expected error with text '%s'", err, inlineMismatchErrString)
	}

	// Attempt to delete inline keys in a transaction; should fail.
	if _, _, _, _, err := MVCCDeleteRange(ctx, engine, testKey2, testKey6,
		2, hlc.Timestamp{Logical: 0}, MVCCWriteOptions{Txn: txn1}, true,
	); !testutils.IsError(err, "writes not allowed within transactions") {
		t.Errorf("unexpected error: %+v", err)
	}

	// Verify final state of the engine.
	expectedKvs := []roachpb.KeyValue{
		{
			Key:   testKey1,
			Value: value1,
		},
		{
			Key:   testKey4,
			Value: value4,
		},
		{
			Key:   testKey5,
			Value: value5,
		},
		{
			Key:   testKey6,
			Value: value6,
		},
	}
	res, err := MVCCScan(ctx, engine, localMax, keyMax, hlc.Timestamp{WallTime: 2},
		MVCCScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if a, e := len(res.KVs), len(expectedKvs); a != e {
		t.Fatalf("engine scan found %d keys; expected %d", a, e)
	}
	res.KVs[3].Value.Timestamp = hlc.Timestamp{}
	if !reflect.DeepEqual(expectedKvs, res.KVs) {
		t.Fatalf(
			"engine scan found key/values: %v; expected %v. Diff: %s",
			res.KVs,
			expectedKvs,
			pretty.Diff(res.KVs, expectedKvs),
		)
	}
}

func TestMVCCClearTimeRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	ts0 := hlc.Timestamp{WallTime: 0}
	ts0Content := []roachpb.KeyValue{}
	ts1 := hlc.Timestamp{WallTime: 10}
	v1 := value1
	v1.Timestamp = ts1
	ts1Content := []roachpb.KeyValue{{Key: testKey2, Value: v1}}
	ts2 := hlc.Timestamp{WallTime: 20}
	v2 := value2
	v2.Timestamp = ts2
	ts2Content := []roachpb.KeyValue{{Key: testKey2, Value: v2}, {Key: testKey5, Value: v2}}
	ts3 := hlc.Timestamp{WallTime: 30}
	v3 := value3
	v3.Timestamp = ts3
	ts3Content := []roachpb.KeyValue{
		{Key: testKey1, Value: v3}, {Key: testKey2, Value: v2}, {Key: testKey5, Value: v2},
	}
	ts4 := hlc.Timestamp{WallTime: 40}
	v4 := value4
	v4.Timestamp = ts4
	ts4Content := []roachpb.KeyValue{
		{Key: testKey1, Value: v3}, {Key: testKey2, Value: v4}, {Key: testKey5, Value: v4},
	}
	ts5 := hlc.Timestamp{WallTime: 50}

	// Set up an engine with the key-time space as follows:
	//    50 -
	//       |
	//    40 -      v4          v4
	//       |
	//    30 -  v3
	// time  |
	//    20 -      v2          v2
	//       |
	//    10 -      v1
	//       |
	//     0 -----------------------
	//          k1  k2  k3  k4  k5
	//                 keys
	eng := NewDefaultInMemForTesting()
	defer eng.Close()
	_, err := MVCCPut(ctx, eng, testKey2, ts1, value1, MVCCWriteOptions{})
	require.NoError(t, err)
	_, err = MVCCPut(ctx, eng, testKey2, ts2, value2, MVCCWriteOptions{})
	require.NoError(t, err)
	_, err = MVCCPut(ctx, eng, testKey5, ts2, value2, MVCCWriteOptions{})
	require.NoError(t, err)
	_, err = MVCCPut(ctx, eng, testKey1, ts3, value3, MVCCWriteOptions{})
	require.NoError(t, err)
	_, err = MVCCPut(ctx, eng, testKey5, ts4, value4, MVCCWriteOptions{})
	require.NoError(t, err)
	_, err = MVCCPut(ctx, eng, testKey2, ts4, value4, MVCCWriteOptions{})
	require.NoError(t, err)

	assertKVs := func(t *testing.T, reader Reader, at hlc.Timestamp, expected []roachpb.KeyValue) {
		t.Helper()
		res, err := MVCCScan(ctx, reader, localMax, keyMax, at, MVCCScanOptions{})
		require.NoError(t, err)
		require.Equal(t, expected, res.KVs)
	}

	const kb = 1024

	resumingClear := func(
		t *testing.T,
		ctx context.Context,
		rw ReadWriter,
		ms *enginepb.MVCCStats,
		key, endKey roachpb.Key,
		ts, endTs hlc.Timestamp,
		sz int64,
		byteLimit int64,
	) int {
		resume, err := MVCCClearTimeRange(ctx, rw, ms, key, endKey, ts, endTs, nil, nil, 64, sz, byteLimit, 0)
		require.NoError(t, err)
		attempts := 1
		for resume != nil {
			resume, err = MVCCClearTimeRange(ctx, rw, ms, resume, endKey, ts, endTs, nil, nil, 64, sz, byteLimit, 0)
			require.NoError(t, err)
			attempts++
		}
		return attempts
	}
	t.Run("clear > ts0", func(t *testing.T) {
		b := eng.NewBatch()
		defer b.Close()
		_, err := MVCCClearTimeRange(ctx, b, nil, localMax, keyMax, ts0, ts5, nil, nil, 64, 10, 1<<10, 0)
		require.NoError(t, err)
		assertKVs(t, b, ts0, ts0Content)
		assertKVs(t, b, ts1, ts0Content)
		assertKVs(t, b, ts5, ts0Content)
	})

	t.Run("clear > ts1 ", func(t *testing.T) {
		b := eng.NewBatch()
		defer b.Close()
		attempts := resumingClear(t, ctx, b, nil, localMax, keyMax, ts1, ts5, 10, kb)
		require.Equal(t, 1, attempts)
		assertKVs(t, b, ts1, ts1Content)
		assertKVs(t, b, ts2, ts1Content)
		assertKVs(t, b, ts5, ts1Content)
	})
	t.Run("clear > ts1 count-size batch", func(t *testing.T) {
		b := eng.NewBatch()
		defer b.Close()
		attempts := resumingClear(t, ctx, b, nil, localMax, keyMax, ts1, ts5, 1, kb)
		require.Equal(t, 2, attempts)
		assertKVs(t, b, ts1, ts1Content)
		assertKVs(t, b, ts2, ts1Content)
		assertKVs(t, b, ts5, ts1Content)
	})

	t.Run("clear > ts1 byte-size batch", func(t *testing.T) {
		b := eng.NewBatch()
		defer b.Close()
		attempts := resumingClear(t, ctx, b, nil, localMax, keyMax, ts1, ts5, 10, 1)
		require.Equal(t, 2, attempts)
		assertKVs(t, b, ts1, ts1Content)
		assertKVs(t, b, ts2, ts1Content)
		assertKVs(t, b, ts5, ts1Content)
	})

	t.Run("clear > ts2", func(t *testing.T) {
		b := eng.NewBatch()
		defer b.Close()
		attempts := resumingClear(t, ctx, b, nil, localMax, keyMax, ts2, ts5, 10, kb)
		require.Equal(t, 1, attempts)
		assertKVs(t, b, ts2, ts2Content)
		assertKVs(t, b, ts5, ts2Content)
	})

	t.Run("clear > ts3", func(t *testing.T) {
		b := eng.NewBatch()
		defer b.Close()
		resumingClear(t, ctx, b, nil, localMax, keyMax, ts3, ts5, 10, kb)
		assertKVs(t, b, ts3, ts3Content)
		assertKVs(t, b, ts5, ts3Content)
	})

	t.Run("clear > ts4 (nothing) ", func(t *testing.T) {
		b := eng.NewBatch()
		defer b.Close()
		_, err := MVCCClearTimeRange(ctx, b, nil, localMax, keyMax, ts4, ts5, nil, nil, 64, 10, kb, 0)
		require.NoError(t, err)
		assertKVs(t, b, ts4, ts4Content)
		assertKVs(t, b, ts5, ts4Content)
	})

	t.Run("clear > ts5 (nothing)", func(t *testing.T) {
		b := eng.NewBatch()
		defer b.Close()
		_, err := MVCCClearTimeRange(ctx, b, nil, localMax, keyMax, ts5, ts5.Next(), nil, nil, 64, 10, kb, 0)
		require.NoError(t, err)
		assertKVs(t, b, ts4, ts4Content)
		assertKVs(t, b, ts5, ts4Content)
	})

	t.Run("clear up to k5 to ts0", func(t *testing.T) {
		b := eng.NewBatch()
		defer b.Close()
		resumingClear(t, ctx, b, nil, testKey1, testKey5, ts0, ts5, 10, kb)
		assertKVs(t, b, ts2, []roachpb.KeyValue{{Key: testKey5, Value: v2}})
		assertKVs(t, b, ts5, []roachpb.KeyValue{{Key: testKey5, Value: v4}})
	})

	t.Run("clear > ts0 in empty span (nothing)", func(t *testing.T) {
		b := eng.NewBatch()
		defer b.Close()
		_, err := MVCCClearTimeRange(ctx, b, nil, testKey3, testKey5, ts0, ts5, nil, nil, 64, 10, kb, 0)
		require.NoError(t, err)
		assertKVs(t, b, ts2, ts2Content)
		assertKVs(t, b, ts5, ts4Content)
	})

	t.Run("clear > ts0 in empty span [k3,k5) (nothing)", func(t *testing.T) {
		b := eng.NewBatch()
		defer b.Close()
		_, err := MVCCClearTimeRange(ctx, b, nil, testKey3, testKey5, ts0, ts5, nil, nil, 64, 10, 1<<10, 0)
		require.NoError(t, err)
		assertKVs(t, b, ts2, ts2Content)
		assertKVs(t, b, ts5, ts4Content)
	})

	t.Run("clear k3 and up in ts0 > x >= ts1 (nothing)", func(t *testing.T) {
		b := eng.NewBatch()
		defer b.Close()
		_, err := MVCCClearTimeRange(ctx, b, nil, testKey3, keyMax, ts0, ts1, nil, nil, 64, 10, 1<<10, 0)
		require.NoError(t, err)
		assertKVs(t, b, ts2, ts2Content)
		assertKVs(t, b, ts5, ts4Content)
	})

	// Add an intent at k3@ts3.
	txn := roachpb.MakeTransaction("test", nil, isolation.Serializable, roachpb.NormalUserPriority, ts3, 1, 1, 0, false /* omitInRangefeeds */)
	addIntent := func(t *testing.T, rw ReadWriter) {
		_, err := MVCCPut(ctx, rw, testKey3, ts3, value3, MVCCWriteOptions{Txn: &txn})
		require.NoError(t, err)
	}
	t.Run("clear everything hitting intent fails", func(t *testing.T) {
		b := eng.NewBatch()
		defer b.Close()
		addIntent(t, b)
		_, err := MVCCClearTimeRange(ctx, b, nil, localMax, keyMax, ts0, ts5, nil, nil, 64, 10, 1<<10, 0)
		require.EqualError(t, err, "conflicting locks on \"/db3\"")
	})

	t.Run("clear exactly hitting intent fails", func(t *testing.T) {
		b := eng.NewBatch()
		defer b.Close()
		addIntent(t, b)
		_, err := MVCCClearTimeRange(ctx, b, nil, testKey3, testKey4, ts2, ts3, nil, nil, 64, 10, 1<<10, 0)
		require.EqualError(t, err, "conflicting locks on \"/db3\"")
	})

	t.Run("clear everything above intent fails", func(t *testing.T) {
		b := eng.NewBatch()
		defer b.Close()
		addIntent(t, b)
		_, err := MVCCClearTimeRange(ctx, b, nil, localMax, keyMax, ts3, ts5, nil, nil, 64, 10, 1<<10, 0)
		require.EqualError(t, err, "conflicting locks on \"/db3\"")
	})

	t.Run("clear below intent fails", func(t *testing.T) {
		b := eng.NewBatch()
		defer b.Close()
		addIntent(t, b)
		_, err := MVCCClearTimeRange(ctx, b, nil, localMax, keyMax, ts1, ts2, nil, nil, 64, 10, 1<<10, 0)
		require.EqualError(t, err, "conflicting locks on \"/db3\"")
	})

	// Add a shared lock at k1 with a txn at ts3.
	addLock := func(t *testing.T, rw ReadWriter) {
		err := MVCCAcquireLock(ctx, rw, &txn.TxnMeta, txn.IgnoredSeqNums, lock.Shared, testKey1, nil, 0, 0)
		require.NoError(t, err)
	}
	t.Run("clear everything hitting lock fails", func(t *testing.T) {
		b := eng.NewBatch()
		defer b.Close()
		addLock(t, b)
		_, err := MVCCClearTimeRange(ctx, b, nil, localMax, keyMax, ts0, ts5, nil, nil, 64, 10, 1<<10, 0)
		require.EqualError(t, err, "conflicting locks on \"/db1\"")
	})

	t.Run("clear exactly hitting lock fails", func(t *testing.T) {
		b := eng.NewBatch()
		defer b.Close()
		addLock(t, b)
		_, err := MVCCClearTimeRange(ctx, b, nil, localMax, keyMax, ts2, ts3, nil, nil, 64, 10, 1<<10, 0)
		require.EqualError(t, err, "conflicting locks on \"/db1\"")
	})

	t.Run("clear everything above lock fails", func(t *testing.T) {
		b := eng.NewBatch()
		defer b.Close()
		addLock(t, b)
		_, err := MVCCClearTimeRange(ctx, b, nil, localMax, keyMax, ts3, ts5, nil, nil, 64, 10, 1<<10, 0)
		require.EqualError(t, err, "conflicting locks on \"/db1\"")
	})

	t.Run("clear below lock fails", func(t *testing.T) {
		b := eng.NewBatch()
		defer b.Close()
		addLock(t, b)
		_, err := MVCCClearTimeRange(ctx, b, nil, localMax, keyMax, ts1, ts2, nil, nil, 64, 10, 1<<10, 0)
		require.EqualError(t, err, "conflicting locks on \"/db1\"")
	})
}

// TestMVCCClearTimeRangeOnRandomData sets up mostly random KVs and then picks
// some random times to which to revert, ensuring that a MVCC-Scan at each of
// those times before reverting matches the result of an MVCC-Scan done at a
// later time post-revert.
func TestMVCCClearTimeRangeOnRandomData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewTestRand()
	ctx := context.Background()

	e := NewDefaultInMemForTesting()
	defer e.Close()

	now := hlc.Timestamp{WallTime: 100000000}

	var ms enginepb.MVCCStats

	// Setup numKVs random kv by writing to random keys [0, keyRange) except for
	// the span [swathStart, swathEnd). Then fill in that swath with kvs all
	// having the same ts, to ensure they all revert at the same time, thus
	// triggering the ClearRange optimization path.
	const numKVs = 10000
	const keyRange, swathStart, swathEnd = 5000, 3500, 4000
	const swathSize = swathEnd - swathStart
	const randTimeRange = 1000

	wrote := make(map[int]int64, keyRange)
	for i := 0; i < numKVs-swathSize; i++ {
		k := rng.Intn(keyRange - swathSize)
		if k >= swathStart {
			k += swathSize
		}

		ts := int64(rng.Intn(randTimeRange))
		// Ensure writes to a given key are increasing in time.
		if ts <= wrote[k] {
			ts = wrote[k] + 1
		}
		wrote[k] = ts

		key := roachpb.Key(fmt.Sprintf("%05d", k))
		if rand.Float64() > 0.8 {
			_, _, err := MVCCDelete(ctx, e, key, hlc.Timestamp{WallTime: ts}, MVCCWriteOptions{Stats: &ms})
			require.NoError(t, err)
		} else {
			v := roachpb.MakeValueFromString(fmt.Sprintf("v-%d", i))
			_, err := MVCCPut(ctx, e, key, hlc.Timestamp{WallTime: ts}, v, MVCCWriteOptions{Stats: &ms})
			require.NoError(t, err)
		}
	}
	swathTime := rand.Intn(randTimeRange-100) + 100
	for i := swathStart; i < swathEnd; i++ {
		key := roachpb.Key(fmt.Sprintf("%05d", i))
		v := roachpb.MakeValueFromString(fmt.Sprintf("v-%d", i))
		_, err := MVCCPut(ctx, e, key, hlc.Timestamp{WallTime: int64(swathTime)}, v, MVCCWriteOptions{Stats: &ms})
		require.NoError(t, err)
	}

	// Add another swath of keys above to exercise an after-iteration range flush.
	for i := keyRange; i < keyRange+200; i++ {
		key := roachpb.Key(fmt.Sprintf("%05d", i))
		v := roachpb.MakeValueFromString(fmt.Sprintf("v-%d", i))
		_, err := MVCCPut(ctx, e, key, hlc.Timestamp{WallTime: int64(randTimeRange + 1)}, v, MVCCWriteOptions{Stats: &ms})
		require.NoError(t, err)
	}

	ms.AgeTo(2000)

	// Sanity check starting stats.
	msComputed, err := ComputeStats(ctx, e, localMax, keyMax, 2000)
	require.NoError(t, err)
	require.Equal(t, msComputed, ms)

	// Pick timestamps to which we'll revert, and sort them so we can go back
	// though them in order. The largest will still be less than randTimeRange so
	// the initial revert will be assured to use ClearRange.
	reverts := make([]int, 5)
	for i := range reverts {
		reverts[i] = rand.Intn(randTimeRange)
	}
	reverts[0] = swathTime - 1
	sort.Ints(reverts)
	const byteLimit = 1000
	const keyLimit = 100
	const clearRangeThreshold = 64
	keyLen := int64(len(roachpb.Key(fmt.Sprintf("%05d", 1)))) + MVCCVersionTimestampSize
	maxAttempts := (numKVs * keyLen) / byteLimit
	var attempts int64
	for i := len(reverts) - 1; i >= 0; i-- {
		t.Run(fmt.Sprintf("revert-%d", i), func(t *testing.T) {
			revertTo := hlc.Timestamp{WallTime: int64(reverts[i])}
			// MVCC-Scan at the revert time.
			resBefore, err := MVCCScan(ctx, e, localMax, keyMax, revertTo, MVCCScanOptions{MaxKeys: numKVs})
			require.NoError(t, err)

			// Revert to the revert time.
			startKey := localMax
			for len(startKey) > 0 {
				attempts++
				batch := e.NewBatch()
				startKey, err = MVCCClearTimeRange(ctx, batch, &ms, startKey, keyMax, revertTo, now,
					nil, nil, clearRangeThreshold, keyLimit, byteLimit, 0)
				require.NoError(t, err)
				require.NoError(t, batch.Commit(false))
				batch.Close()
			}

			msComputed, err := ComputeStats(ctx, e, localMax, keyMax, 2000)
			require.NoError(t, err)
			require.Equal(t, msComputed, ms)
			// Scanning at "now" post-revert should yield the same result as scanning
			// at revert-time pre-revert.
			resAfter, err := MVCCScan(ctx, e, localMax, keyMax, now, MVCCScanOptions{MaxKeys: numKVs})
			require.NoError(t, err)
			require.Equal(t, resBefore.KVs, resAfter.KVs)
		})
	}
	require.LessOrEqual(t, attempts, maxAttempts)
}

func TestMVCCInitPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	_, err := MVCCInitPut(ctx, engine, testKey1, hlc.Timestamp{Logical: 1}, value1, false, MVCCWriteOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// A repeat of the command will still succeed
	_, err = MVCCInitPut(ctx, engine, testKey1, hlc.Timestamp{Logical: 2}, value1, false, MVCCWriteOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Delete.
	_, _, err = MVCCDelete(ctx, engine, testKey1, hlc.Timestamp{Logical: 3}, MVCCWriteOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Reinserting the value fails if we fail on tombstones.
	_, err = MVCCInitPut(ctx, engine, testKey1, hlc.Timestamp{Logical: 4}, value1, true, MVCCWriteOptions{})
	if e := (*kvpb.ConditionFailedError)(nil); errors.As(err, &e) {
		if !bytes.Equal(e.ActualValue.RawBytes, nil) {
			t.Fatalf("the value %s in get result is not a tombstone", e.ActualValue.RawBytes)
		}
	} else if err == nil {
		t.Fatal("MVCCInitPut with a different value did not fail")
	} else {
		t.Fatalf("unexpected error %T", e)
	}

	// But doesn't if we *don't* fail on tombstones.
	_, err = MVCCInitPut(ctx, engine, testKey1, hlc.Timestamp{Logical: 5}, value1, false, MVCCWriteOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// A repeat of the command with a different value will fail.
	_, err = MVCCInitPut(ctx, engine, testKey1, hlc.Timestamp{Logical: 6}, value2, false, MVCCWriteOptions{})
	if e := (*kvpb.ConditionFailedError)(nil); errors.As(err, &e) {
		if !bytes.Equal(e.ActualValue.RawBytes, value1.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				e.ActualValue.RawBytes, value1.RawBytes)
		}
	} else if err == nil {
		t.Fatal("MVCCInitPut with a different value did not fail")
	} else {
		t.Fatalf("unexpected error %T", e)
	}

	// Ensure that the timestamps were correctly updated.
	for _, check := range []struct {
		ts, expTS hlc.Timestamp
	}{
		{ts: hlc.Timestamp{Logical: 1}, expTS: hlc.Timestamp{Logical: 1}},
		{ts: hlc.Timestamp{Logical: 2}, expTS: hlc.Timestamp{Logical: 2}},
		// If we're checking the future wall time case, the rewrite after delete
		// will be present.
		{ts: hlc.Timestamp{WallTime: 1}, expTS: hlc.Timestamp{Logical: 5}},
	} {
		valueRes, err := MVCCGet(ctx, engine, testKey1, check.ts, MVCCGetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value1.RawBytes, valueRes.Value.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				value1.RawBytes, valueRes.Value.RawBytes)
		}
		if valueRes.Value.Timestamp != check.expTS {
			t.Errorf("value at timestamp %s seen, expected %s", valueRes.Value.Timestamp, check.expTS)
		}
	}

	valueRes, pErr := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{Logical: 0}, MVCCGetOptions{})
	if pErr != nil {
		t.Fatal(pErr)
	}
	if valueRes.Value != nil {
		t.Fatalf("%v present at old timestamp", valueRes.Value)
	}
}

func TestMVCCInitPutWithTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 123)))

	txn := *txn1
	txn.Sequence++
	_, err := MVCCInitPut(ctx, engine, testKey1, txn.ReadTimestamp, value1, false, MVCCWriteOptions{Txn: &txn})
	require.NoError(t, err)

	// A repeat of the command will still succeed.
	txn.Sequence++
	_, err = MVCCInitPut(ctx, engine, testKey1, txn.ReadTimestamp, value1, false, MVCCWriteOptions{Txn: &txn})
	require.NoError(t, err)

	// A repeat of the command with a different value at a different epoch
	// will still succeed.
	txn.Sequence++
	txn.Epoch = 2
	_, err = MVCCInitPut(ctx, engine, testKey1, txn.ReadTimestamp, value2, false, MVCCWriteOptions{Txn: &txn})
	require.NoError(t, err)

	// Commit value3.
	txnCommit := txn
	txnCommit.Status = roachpb.COMMITTED
	txnCommit.WriteTimestamp = clock.Now().Add(1, 0)
	_, _, _, _, err = MVCCResolveWriteIntent(ctx, engine, nil,
		roachpb.MakeLockUpdate(&txnCommit, roachpb.Span{Key: testKey1}),
		MVCCResolveWriteIntentOptions{})
	require.NoError(t, err)

	// Write value4 with an old timestamp without txn...should get an error.
	_, err = MVCCInitPut(ctx, engine, testKey1, clock.Now(), value4, false, MVCCWriteOptions{})
	require.ErrorAs(t, err, new(*kvpb.WriteTooOldError))
}

// TestMVCCReverseScan verifies that MVCCReverseScan scans [start,
// end) in descending order of keys.
func TestMVCCReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 2}, value2, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey2, hlc.Timestamp{WallTime: 1}, value3, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey2, hlc.Timestamp{WallTime: 3}, value4, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey3, hlc.Timestamp{WallTime: 1}, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey4, hlc.Timestamp{WallTime: 1}, value2, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey5, hlc.Timestamp{WallTime: 3}, value5, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey6, hlc.Timestamp{WallTime: 3}, value6, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}

	res, err := MVCCScan(ctx, engine, testKey2, testKey4,
		hlc.Timestamp{WallTime: 1}, MVCCScanOptions{Reverse: true})

	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 2 ||
		!bytes.Equal(res.KVs[0].Key, testKey3) ||
		!bytes.Equal(res.KVs[1].Key, testKey2) ||
		!bytes.Equal(res.KVs[0].Value.RawBytes, value1.RawBytes) ||
		!bytes.Equal(res.KVs[1].Value.RawBytes, value3.RawBytes) {
		t.Fatalf("unexpected value: %v", res.KVs)
	}
	if res.ResumeSpan != nil {
		t.Fatalf("resumeSpan = %+v", res.ResumeSpan)
	}

	res, err = MVCCScan(ctx, engine, testKey2, testKey4, hlc.Timestamp{WallTime: 1},
		MVCCScanOptions{Reverse: true, MaxKeys: 1})

	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 1 ||
		!bytes.Equal(res.KVs[0].Key, testKey3) ||
		!bytes.Equal(res.KVs[0].Value.RawBytes, value1.RawBytes) {
		t.Fatalf("unexpected value: %v", res.KVs)
	}
	if expected := (roachpb.Span{Key: testKey2, EndKey: testKey2.Next()}); !res.ResumeSpan.EqualValue(expected) {
		t.Fatalf("expected = %+v, resumeSpan = %+v", expected, res.ResumeSpan)
	}

	res, err = MVCCScan(ctx, engine, testKey2, testKey4, hlc.Timestamp{WallTime: 1},
		MVCCScanOptions{Reverse: true, MaxKeys: -1})

	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 0 {
		t.Fatalf("unexpected value: %v", res.KVs)
	}
	if expected := (roachpb.Span{Key: testKey2, EndKey: testKey4}); !res.ResumeSpan.EqualValue(expected) {
		t.Fatalf("expected = %+v, resumeSpan = %+v", expected, res.ResumeSpan)
	}

	// The first key we encounter has multiple versions and we need to read the
	// latest.
	res, err = MVCCScan(ctx, engine, testKey2, testKey3, hlc.Timestamp{WallTime: 4},
		MVCCScanOptions{Reverse: true, MaxKeys: 1})

	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 1 ||
		!bytes.Equal(res.KVs[0].Key, testKey2) ||
		!bytes.Equal(res.KVs[0].Value.RawBytes, value4.RawBytes) {
		t.Errorf("unexpected value: %v", res.KVs)
	}

	// The first key we encounter is newer than our read timestamp and we need to
	// back up to the previous key.
	res, err = MVCCScan(ctx, engine, testKey4, testKey6, hlc.Timestamp{WallTime: 1},
		MVCCScanOptions{Reverse: true, MaxKeys: 1})

	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 1 ||
		!bytes.Equal(res.KVs[0].Key, testKey4) ||
		!bytes.Equal(res.KVs[0].Value.RawBytes, value2.RawBytes) {
		t.Fatalf("unexpected value: %v", res.KVs)
	}

	// Scan only the first key in the key space.
	res, err = MVCCScan(ctx, engine, testKey1, testKey1.Next(), hlc.Timestamp{WallTime: 1},
		MVCCScanOptions{Reverse: true, MaxKeys: 1})

	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 1 ||
		!bytes.Equal(res.KVs[0].Key, testKey1) ||
		!bytes.Equal(res.KVs[0].Value.RawBytes, value1.RawBytes) {
		t.Fatalf("unexpected value: %v", res.KVs)
	}
}

// TestMVCCReverseScanFirstKeyInFuture verifies that when MVCCReverseScan scans
// encounter a key with only future timestamps first, that it skips the key and
// continues to scan in reverse. #17825 was caused by this not working correctly.
func TestMVCCReverseScanFirstKeyInFuture(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	// The value at key2 will be at a lower timestamp than the ReverseScan, but
	// the value at key3 will be at a larger timestamp. The ReverseScan should
	// see key3 and ignore it because none of it versions are at a low enough
	// timestamp to read. It should then continue scanning backwards and find a
	// value at key2.
	//
	// Before fixing #17825, the MVCC version scan on key3 would fall out of the
	// scan bounds and if it never found another valid key before reaching
	// KeyMax, would stop the ReverseScan from continuing.
	if _, err := MVCCPut(ctx, engine, testKey2, hlc.Timestamp{WallTime: 1}, value2, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey3, hlc.Timestamp{WallTime: 3}, value3, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}

	res, err := MVCCScan(ctx, engine, testKey1, testKey4,
		hlc.Timestamp{WallTime: 2}, MVCCScanOptions{Reverse: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 1 ||
		!bytes.Equal(res.KVs[0].Key, testKey2) ||
		!bytes.Equal(res.KVs[0].Value.RawBytes, value2.RawBytes) {
		t.Errorf("unexpected value: %v", res.KVs)
	}
}

// Exposes a bug where the reverse MVCC scan can get stuck in an infinite loop
// until we OOM. It happened in the code path optimized to use `SeekForPrev()`
// after N `Prev()`s do not reach another logical key. Further, a write intent
// needed to be present on the logical key to make it conflict with our chosen
// `SeekForPrev()` target (logical key + '\0').
func TestMVCCReverseScanSeeksOverRepeatedKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	// 10 is the value of `kMaxItersBeforeSeek` at the time this test case was
	// written. Repeat the key enough times to make sure the `SeekForPrev()`
	// optimization will be used.
	for i := 1; i <= 10; i++ {
		if _, err := MVCCPut(ctx, engine, testKey2, hlc.Timestamp{WallTime: int64(i)}, value2, MVCCWriteOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	txn1ts := makeTxn(*txn1, hlc.Timestamp{WallTime: 11})
	if _, err := MVCCPut(ctx, engine, testKey2, txn1ts.ReadTimestamp, value2, MVCCWriteOptions{Txn: txn1ts}); err != nil {
		t.Fatal(err)
	}

	res, err := MVCCScan(ctx, engine, testKey1, testKey3,
		hlc.Timestamp{WallTime: 1}, MVCCScanOptions{Reverse: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 1 ||
		!bytes.Equal(res.KVs[0].Key, testKey2) ||
		!bytes.Equal(res.KVs[0].Value.RawBytes, value2.RawBytes) {
		t.Fatal("unexpected scan results")
	}
}

// Exposes a bug where the reverse MVCC scan can get stuck in an infinite loop until we OOM.
//
// The bug happened in this scenario.
// (1) reverse scan is positioned at the range's smallest key and calls `prevKey()`
// (2) `prevKey()` peeks and sees newer versions of the same logical key
//
//	`iters_before_seek_-1` times, moving the iterator backwards each time
//
// (3) on the `iters_before_seek_`th peek, there are no previous keys found
//
// Then, the problem was `prevKey()` treated finding no previous key as if it had found a
// new logical key with the empty string. It would use `backwardLatestVersion()` to find
// the latest version of this empty string logical key. Due to condition (3),
// `backwardLatestVersion()` would go directly to its seeking optimization rather than
// trying to incrementally move backwards (if it had tried moving incrementally backwards,
// it would've noticed it's out of bounds). The seek optimization would then seek to "\0",
// which is the empty logical key with zero timestamp. Since we set RocksDB iterator lower
// bound to be the lower bound of the range scan, this seek actually lands back at the
// range's smallest key. It thinks it found a new key so it adds it to the result, and then
// this whole process repeats ad infinitum.
func TestMVCCReverseScanStopAtSmallestKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	run := func(numPuts int, ts int64) {
		ctx := context.Background()
		engine := NewDefaultInMemForTesting()
		defer engine.Close()

		for i := 1; i <= numPuts; i++ {
			if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: int64(i)}, value1, MVCCWriteOptions{}); err != nil {
				t.Fatal(err)
			}
		}

		res, err := MVCCScan(ctx, engine, testKey1, testKey3,
			hlc.Timestamp{WallTime: ts}, MVCCScanOptions{Reverse: true})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.KVs) != 1 ||
			!bytes.Equal(res.KVs[0].Key, testKey1) ||
			!bytes.Equal(res.KVs[0].Value.RawBytes, value1.RawBytes) {
			t.Fatal("unexpected scan results")
		}
	}
	// Satisfying (2) and (3) is incredibly intricate because of how `iters_before_seek_`
	// is incremented/decremented heuristically. For example, at the time of writing, the
	// infinitely looping cases are `numPuts == 6 && ts == 2`, `numPuts == 7 && ts == 3`,
	// `numPuts == 8 && ts == 4`, `numPuts == 9 && ts == 5`, and `numPuts == 10 && ts == 6`.
	// Tying our test case to the `iters_before_seek_` setting logic seems brittle so let's
	// just brute force test a wide range of cases.
	for numPuts := 1; numPuts <= 10; numPuts++ {
		for ts := 1; ts <= 10; ts++ {
			run(numPuts, int64(ts))
		}
	}
}

func TestMVCCResolveTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	if _, err := MVCCPut(ctx, engine, testKey1, txn1.ReadTimestamp, value1, MVCCWriteOptions{Txn: txn1}); err != nil {
		t.Fatal(err)
	}

	{
		valueRes, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{Logical: 1}, MVCCGetOptions{
			Txn: txn1,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value1.RawBytes, valueRes.Value.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				value1.RawBytes, valueRes.Value.RawBytes)
		}
	}

	// Resolve will write with txn1's timestamp which is 0,1.
	if _, _, _, _, err := MVCCResolveWriteIntent(ctx, engine, nil,
		roachpb.MakeLockUpdate(txn1Commit, roachpb.Span{Key: testKey1}),
		MVCCResolveWriteIntentOptions{}); err != nil {
		t.Fatal(err)
	}

	{
		valueRes, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{Logical: 1}, MVCCGetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value1.RawBytes, valueRes.Value.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				value1.RawBytes, valueRes.Value.RawBytes)
		}
	}
}

// TestMVCCResolveNewerIntent verifies that resolving a newer intent
// than the committing transaction aborts the intent.
func TestMVCCResolveNewerIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	// Write first value.
	if _, err := MVCCPut(ctx, engine, testKey1, txn1Commit.WriteTimestamp, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	// Now, put down an intent which should return a write too old error
	// (but will still write the intent at tx1Commit.Timestamp+1.
	_, err := MVCCPut(ctx, engine, testKey1, txn1.ReadTimestamp, value2, MVCCWriteOptions{Txn: txn1})
	if !errors.HasType(err, (*kvpb.WriteTooOldError)(nil)) {
		t.Fatalf("expected write too old error; got %s", err)
	}

	// Resolve will succeed but should remove the intent.
	if _, _, _, _, err := MVCCResolveWriteIntent(ctx, engine, nil,
		roachpb.MakeLockUpdate(txn1Commit, roachpb.Span{Key: testKey1}),
		MVCCResolveWriteIntentOptions{}); err != nil {
		t.Fatal(err)
	}

	valueRes, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{Logical: 2}, MVCCGetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value1.RawBytes, valueRes.Value.RawBytes) {
		t.Fatalf("expected value1 bytes; got %q", valueRes.Value.RawBytes)
	}
}

// TestMVCCPaginate tests that MVCCPaginate respects the MaxKeys and
// TargetBytes limits, and returns the correct numKeys, numBytes, and
// resumeReason.
func TestMVCCPaginate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		maxKeys              int64
		targetBytes          int64
		allowEmpty           bool
		numKeysPerIter       int64
		numBytesPerIter      int64
		numIters             int
		expectedNumKeys      int64
		expectedNumBytes     int64
		expectedResumeReason kvpb.ResumeReason
	}{
		// MaxKeys and TargetBytes limits not reached, so do all 10 iterations.
		{
			maxKeys:              31,
			targetBytes:          51,
			allowEmpty:           false,
			numKeysPerIter:       3,
			numBytesPerIter:      5,
			numIters:             10,
			expectedNumKeys:      30,
			expectedNumBytes:     50,
			expectedResumeReason: 0,
		},
		// MaxKeys limit reached after 7 iterations.
		{
			maxKeys:              21,
			targetBytes:          51,
			allowEmpty:           false,
			numKeysPerIter:       3,
			numBytesPerIter:      5,
			numIters:             10,
			expectedNumKeys:      21,
			expectedNumBytes:     35,
			expectedResumeReason: kvpb.RESUME_KEY_LIMIT,
		},
		// MaxKeys limit reached after 10 iterations. Despite the fact we
		// finished iterating, we still return a resume reason because we check
		// the MaxKeys and TargetBytes limits before we check if we stop
		// iteration.
		{
			maxKeys:              30,
			targetBytes:          50,
			allowEmpty:           false,
			numKeysPerIter:       3,
			numBytesPerIter:      5,
			numIters:             10,
			expectedNumKeys:      30,
			expectedNumBytes:     50,
			expectedResumeReason: kvpb.RESUME_KEY_LIMIT,
		},
		// TargetBytes limit reached after 7 iterations.
		{
			maxKeys:              31,
			targetBytes:          34,
			allowEmpty:           false,
			numKeysPerIter:       3,
			numBytesPerIter:      5,
			numIters:             10,
			expectedNumKeys:      21,
			expectedNumBytes:     35,
			expectedResumeReason: kvpb.RESUME_BYTE_LIMIT,
		},
		// TargetBytes limit reached after 7 iterations, but with TargetBytes
		// limit exactly the number of bytes.
		{
			maxKeys:              31,
			targetBytes:          35,
			allowEmpty:           false,
			numKeysPerIter:       3,
			numBytesPerIter:      5,
			numIters:             10,
			expectedNumKeys:      21,
			expectedNumBytes:     35,
			expectedResumeReason: kvpb.RESUME_BYTE_LIMIT,
		},
		// TargetBytes limit reached after 7 iterations, but with AllowEmpty
		// set to true, so only 6 iterations are completed.
		{
			maxKeys:              31,
			targetBytes:          34,
			allowEmpty:           true,
			numKeysPerIter:       3,
			numBytesPerIter:      5,
			numIters:             10,
			expectedNumKeys:      18,
			expectedNumBytes:     30,
			expectedResumeReason: kvpb.RESUME_BYTE_LIMIT,
		},
	}

	for _, tc := range testCases {
		var iter int
		numKeys, numBytes, resumeReason, err := MVCCPaginate(context.Background(), tc.maxKeys, tc.targetBytes, tc.allowEmpty,
			func(maxKeys, targetBytes int64) (numKeys int64, numBytes int64, resumeReason kvpb.ResumeReason, err error) {
				if iter == tc.numIters {
					return 0, 0, 0, iterutil.StopIteration()
				}
				iter++
				if tc.allowEmpty && tc.numBytesPerIter > targetBytes {
					return 0, 0, kvpb.RESUME_BYTE_LIMIT, nil
				}
				return tc.numKeysPerIter, tc.numBytesPerIter, 0, nil
			})
		require.NoError(t, err)
		require.Equal(t, tc.expectedNumKeys, numKeys)
		require.Equal(t, tc.expectedNumBytes, numBytes)
		require.Equal(t, tc.expectedResumeReason, resumeReason)
	}
}

func TestMVCCResolveIntentTxnTimestampMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	txn := txn1.Clone()
	tsEarly := txn.WriteTimestamp
	txn.TxnMeta.WriteTimestamp.Forward(tsEarly.Add(10, 0))

	// Write an intent which has txn.WriteTimestamp > meta.timestamp.
	if _, err := MVCCPut(ctx, engine, testKey1, tsEarly, value1, MVCCWriteOptions{Txn: txn}); err != nil {
		t.Fatal(err)
	}

	// The Timestamp within is equal to that of txn.Meta even though
	// the intent sits at tsEarly. The bug was looking at the former
	// instead of the latter (and so we could also tickle it with
	// smaller timestamps in Txn).
	intent := roachpb.MakeLockUpdate(txn, roachpb.Span{Key: testKey1})
	intent.Status = roachpb.PENDING

	// A bug (see #7654) caused intents to just stay where they were instead
	// of being moved forward in the situation set up above.
	if _, _, _, _, err := MVCCResolveWriteIntent(ctx, engine, nil, intent, MVCCResolveWriteIntentOptions{}); err != nil {
		t.Fatal(err)
	}

	for i, test := range []struct {
		hlc.Timestamp
		found bool
	}{
		// Check that the intent has indeed moved to where we pushed it.
		{tsEarly, false},
		{intent.Txn.WriteTimestamp.Prev(), false},
		{intent.Txn.WriteTimestamp, true},
		{hlc.MaxTimestamp, true},
	} {
		_, err := MVCCGet(ctx, engine, testKey1, test.Timestamp, MVCCGetOptions{})
		if errors.HasType(err, (*kvpb.LockConflictError)(nil)) != test.found {
			t.Fatalf("%d: expected lock conflict error: %t, got %v", i, test.found, err)
		}
	}
}

// TestMVCCConditionalPutOldTimestamp tests a case where a conditional
// put with an older timestamp happens after a put with a newer timestamp.
// The conditional put fails with WriteTooOld errors, regardless of whether
// the condition succeeds or not.
func TestMVCCConditionalPutOldTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()
	_, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, value1, MVCCWriteOptions{})
	require.NoError(t, err)
	_, err = MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 3}, value2, MVCCWriteOptions{})
	require.NoError(t, err)

	// Check that a write too old error is thrown, regardless of whether the value
	// matches.
	for _, expVal := range []roachpb.Value{
		// Condition does not match.
		value1,
		// Condition matches.
		value2,
	} {
		_, err = MVCCConditionalPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 2}, value3, expVal.TagAndDataBytes(),
			ConditionalPutWriteOptions{AllowIfDoesNotExist: CPutFailIfMissing})
		require.ErrorAs(t, err, new(*kvpb.WriteTooOldError))

		// Either way, no new value is written.
		ts := hlc.Timestamp{WallTime: 3}
		valueRes, err := MVCCGet(ctx, engine, testKey1, ts, MVCCGetOptions{})
		require.NoError(t, err)
		require.Equal(t, value2.RawBytes, valueRes.Value.RawBytes)
	}
}

// TestMVCCMultiplePutOldTimestamp tests a case where multiple transactional
// Puts occur to the same key, but with older timestamps than a pre-existing
// key. The first should generate a WriteTooOldError and fail to write. The
// second should avoid the WriteTooOldError and write at the higher timestamp.
func TestMVCCMultiplePutOldTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	_, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 3}, value1, MVCCWriteOptions{})
	require.NoError(t, err)

	// Verify the first txn Put returns a write too old error and does not
	// write a new value.
	txn := makeTxn(*txn1, hlc.Timestamp{WallTime: 1})
	txn.Sequence++
	_, err = MVCCPut(ctx, engine, testKey1, txn.ReadTimestamp, value2, MVCCWriteOptions{Txn: txn})
	var wtoErr *kvpb.WriteTooOldError
	require.ErrorAs(t, err, &wtoErr)
	expTS := hlc.Timestamp{WallTime: 3, Logical: 1}
	require.Equal(t, expTS, wtoErr.ActualTimestamp)
	// Verify no value was written.
	valueRes, err := MVCCGet(ctx, engine, testKey1, hlc.MaxTimestamp, MVCCGetOptions{Txn: txn})
	require.NoError(t, err)
	require.Equal(t, value1.RawBytes, valueRes.Value.RawBytes)

	// Put again after advancing the txn's timestamp to the WriteTooOld error's
	// timestamp and verify no WriteTooOldError.
	txn.BumpReadTimestamp(wtoErr.ActualTimestamp)
	txn.Sequence++
	_, err = MVCCPut(ctx, engine, testKey1, txn.ReadTimestamp, value3, MVCCWriteOptions{Txn: txn})
	require.NoError(t, err)
	// Verify new value was actually written at (3, 1).
	valueRes, err = MVCCGet(ctx, engine, testKey1, hlc.MaxTimestamp, MVCCGetOptions{Txn: txn})
	require.NoError(t, err)
	require.Equal(t, expTS, valueRes.Value.Timestamp)
	require.Equal(t, value3.RawBytes, valueRes.Value.RawBytes)
}

func TestMVCCPutNegativeTimestampError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	timestamp := hlc.Timestamp{WallTime: -1}
	expectedErrorString := fmt.Sprintf("cannot write to %q at timestamp %s", testKey1, timestamp)
	_, err := MVCCPut(ctx, engine, testKey1, timestamp, value1, MVCCWriteOptions{})
	require.EqualError(t, err, expectedErrorString)
}

// TestMVCCPutOldOrigTimestampNewCommitTimestamp tests a case where a
// transactional Put occurs to the same key, but with an older original
// timestamp than a pre-existing key. As always, this should result in a
// WriteTooOld error.
func TestMVCCPutOldOrigTimestampNewCommitTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	_, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 3}, value1, MVCCWriteOptions{})
	require.NoError(t, err)

	// Perform a transactional Put with a transaction whose read timestamp is
	// below the existing key's timestamp and whose write timestamp is above the
	// existing key's timestamp.
	txn := makeTxn(*txn1, hlc.Timestamp{WallTime: 1})
	txn.WriteTimestamp = hlc.Timestamp{WallTime: 5}
	txn.Sequence++
	_, err = MVCCPut(ctx, engine, testKey1, txn.ReadTimestamp, value2, MVCCWriteOptions{Txn: txn})

	// Verify that the Put returned a WriteTooOld with the ActualTime set to the
	// transactions provisional commit timestamp.
	expTS := txn.WriteTimestamp
	var wtoErr *kvpb.WriteTooOldError
	require.ErrorAs(t, err, &wtoErr)
	require.Equal(t, expTS, wtoErr.ActualTimestamp)

	// Verify no value was written.
	valueRes, err := MVCCGet(ctx, engine, testKey1, hlc.MaxTimestamp, MVCCGetOptions{Txn: txn})
	require.NoError(t, err)
	require.Equal(t, value1.RawBytes, valueRes.Value.RawBytes)
}

func TestMVCCAbortTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	if _, err := MVCCPut(
		ctx, engine, testKey1, txn1.ReadTimestamp, value1, MVCCWriteOptions{Txn: txn1},
	); err != nil {
		t.Fatal(err)
	}

	txn1AbortWithTS := txn1Abort.Clone()
	txn1AbortWithTS.WriteTimestamp = hlc.Timestamp{Logical: 1}

	if _, _, _, _, err := MVCCResolveWriteIntent(ctx, engine, nil,
		roachpb.MakeLockUpdate(txn1AbortWithTS, roachpb.Span{Key: testKey1}),
		MVCCResolveWriteIntentOptions{},
	); err != nil {
		t.Fatal(err)
	}

	if valueRes, err := MVCCGet(
		ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{},
	); err != nil {
		t.Fatal(err)
	} else if valueRes.Value != nil {
		t.Fatalf("expected the value to be empty: %s", valueRes.Value)
	}
	require.Empty(t, mvccGetRaw(t, engine, mvccKey(testKey1)))
}

func TestMVCCAbortTxnWithPreviousVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{Logical: 1}, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, value2, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	txn1ts := makeTxn(*txn1, hlc.Timestamp{WallTime: 2})
	if _, err := MVCCPut(ctx, engine, testKey1, txn1ts.ReadTimestamp, value3, MVCCWriteOptions{Txn: txn1ts}); err != nil {
		t.Fatal(err)
	}

	txn1AbortWithTS := txn1Abort.Clone()
	txn1AbortWithTS.WriteTimestamp = hlc.Timestamp{WallTime: 2}

	if _, _, _, _, err := MVCCResolveWriteIntent(ctx, engine, nil,
		roachpb.MakeLockUpdate(txn1AbortWithTS, roachpb.Span{Key: testKey1}),
		MVCCResolveWriteIntentOptions{},
	); err != nil {
		t.Fatal(err)
	}

	if intentRes, err := MVCCGet(ctx, engine, testKey1, hlc.MaxTimestamp, MVCCGetOptions{
		Inconsistent: true,
	}); err != nil {
		t.Fatal(err)
	} else if intentRes.Intent != nil {
		t.Fatalf("expected no intent, got: %s", intentRes.Intent)
	}

	if valueRes, err := MVCCGet(
		ctx, engine, testKey1, hlc.Timestamp{WallTime: 3}, MVCCGetOptions{},
	); err != nil {
		t.Fatal(err)
	} else if expTS := (hlc.Timestamp{WallTime: 1}); valueRes.Value.Timestamp != expTS {
		t.Fatalf("expected timestamp %+v == %+v", valueRes.Value.Timestamp, expTS)
	} else if !bytes.Equal(value2.RawBytes, valueRes.Value.RawBytes) {
		t.Fatalf("the value %q in get result does not match the value %q in request",
			valueRes.Value.RawBytes, value2.RawBytes)
	}
}

func TestMVCCWriteWithDiffTimestampsAndEpochs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	// Start with epoch 1.
	txn := *txn1
	txn.Sequence++
	_, err := MVCCPut(ctx, engine, testKey1, txn.ReadTimestamp, value1, MVCCWriteOptions{Txn: &txn})
	require.NoError(t, err)
	// Now write with greater timestamp and epoch 2.
	txne2 := txn
	txne2.Sequence++
	txne2.Epoch = 2
	txne2.WriteTimestamp = hlc.Timestamp{WallTime: 1}
	_, err = MVCCPut(ctx, engine, testKey1, txne2.ReadTimestamp, value2, MVCCWriteOptions{Txn: &txne2})
	require.NoError(t, err)
	// Try a write with an earlier timestamp; this is just ignored.
	txne2.Sequence++
	txne2.WriteTimestamp = hlc.Timestamp{WallTime: 1}
	_, err = MVCCPut(ctx, engine, testKey1, txne2.ReadTimestamp, value1, MVCCWriteOptions{Txn: &txne2})
	require.NoError(t, err)
	// Try a write with an earlier epoch; ignored with error.
	_, err = MVCCPut(ctx, engine, testKey1, txn.ReadTimestamp, value1, MVCCWriteOptions{Txn: &txn})
	require.Error(t, err)
	require.Regexp(t, "put with epoch 1 came after put with epoch 2 in txn", err)
	// Try a write with different value using both later timestamp and epoch.
	txne2.Sequence++
	_, err = MVCCPut(ctx, engine, testKey1, txne2.ReadTimestamp, value3, MVCCWriteOptions{Txn: &txne2})
	require.NoError(t, err)
	// Resolve the intent.
	txne2Commit := txne2
	txne2Commit.Status = roachpb.COMMITTED
	txne2Commit.WriteTimestamp = hlc.Timestamp{WallTime: 1}
	_, _, _, _, err = MVCCResolveWriteIntent(ctx, engine, nil,
		roachpb.MakeLockUpdate(&txne2Commit, roachpb.Span{Key: testKey1}),
		MVCCResolveWriteIntentOptions{})
	require.NoError(t, err)

	expTS := txne2Commit.WriteTimestamp.Next()

	// Now try writing an earlier value without a txn--should get WriteTooOldError.
	_, err = MVCCPut(ctx, engine, testKey1, hlc.Timestamp{Logical: 1}, value4, MVCCWriteOptions{})
	var wtoErr *kvpb.WriteTooOldError
	require.ErrorAs(t, err, &wtoErr)
	require.Equal(t, expTS, wtoErr.ActualTimestamp)
	// Verify no value was written.
	valueRes, err := MVCCGet(ctx, engine, testKey1, hlc.MaxTimestamp, MVCCGetOptions{})
	require.NoError(t, err)
	require.Equal(t, txne2Commit.WriteTimestamp, valueRes.Value.Timestamp)
	require.Equal(t, value3.RawBytes, valueRes.Value.RawBytes)

	// Attempt to read older timestamp; should fail.
	valueRes, err = MVCCGet(ctx, engine, testKey1, hlc.Timestamp{Logical: 0}, MVCCGetOptions{})
	require.NoError(t, err)
	require.False(t, valueRes.Value.IsPresent())
	// Read at correct timestamp.
	valueRes, err = MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{})
	require.NoError(t, err)
	require.Equal(t, txne2Commit.WriteTimestamp, valueRes.Value.Timestamp)
	require.Equal(t, value3.RawBytes, valueRes.Value.RawBytes)
}

// TestMVCCGetWithDiffEpochs writes a value first using epoch 1, then
// reads using epoch 2 to verify that values written during different
// transaction epochs are not visible.
func TestMVCCGetWithDiffEpochs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	// Write initial value without a txn.
	if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{Logical: 1}, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	// Now write using txn1, epoch 1.
	txn1ts := makeTxn(*txn1, hlc.Timestamp{WallTime: 1})
	if _, err := MVCCPut(ctx, engine, testKey1, txn1ts.ReadTimestamp, value2, MVCCWriteOptions{Txn: txn1ts}); err != nil {
		t.Fatal(err)
	}
	// Try reading using different txns & epochs.
	testCases := []struct {
		txn      *roachpb.Transaction
		expValue *roachpb.Value
		expErr   bool
	}{
		// No transaction; should see error.
		{nil, nil, true},
		// Txn1, epoch 1; should see new value2.
		{txn1, &value2, false},
		// Txn1, epoch 2; should see original value1.
		{txn1e2, &value1, false},
		// Txn2; should see error.
		{txn2, nil, true},
	}
	for i, test := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			valueRes, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 2}, MVCCGetOptions{
				Txn: test.txn,
			})
			if test.expErr {
				if err == nil {
					t.Errorf("test %d: unexpected success", i)
				} else if !errors.HasType(err, (*kvpb.LockConflictError)(nil)) {
					t.Errorf("test %d: expected lock conflict error; got %v", i, err)
				}
			} else if err != nil || valueRes.Value == nil || !bytes.Equal(test.expValue.RawBytes, valueRes.Value.RawBytes) {
				t.Errorf("test %d: expected value %q, err nil; got %+v, %v", i, test.expValue.RawBytes, valueRes.Value, err)
			}
		})
	}
}

// TestMVCCGetWithDiffEpochsAndTimestamps writes a value first using
// epoch 1, then reads using epoch 2 with different timestamps to verify
// that values written during different transaction epochs are not visible.
//
// The test includes the case where the read at epoch 2 is at a *lower*
// timestamp than the intent write at epoch 1. This is not expected to
// happen commonly, but caused issues in #36089.
func TestMVCCGetWithDiffEpochsAndTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	// Write initial value without a txn at timestamp 1.
	_, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, value1, MVCCWriteOptions{})
	require.NoError(t, err)
	// Write another value without a txn at timestamp 3.
	_, err = MVCCPut(ctx, engine, testKey1, hlc.Timestamp{WallTime: 3}, value2, MVCCWriteOptions{})
	require.NoError(t, err)
	// Now write using txn1, epoch 1.
	txn1ts := makeTxn(*txn1, hlc.Timestamp{WallTime: 4})
	_, err = MVCCPut(ctx, engine, testKey1, txn1ts.ReadTimestamp, value3, MVCCWriteOptions{Txn: txn1ts})
	require.NoError(t, err)
	// Try reading using different epochs & timestamps.
	testCases := []struct {
		txn      *roachpb.Transaction
		readTS   hlc.Timestamp
		expValue *roachpb.Value
	}{
		// Epoch 1, read 1; should see new value3.
		{txn1, hlc.Timestamp{WallTime: 1}, &value3},
		// Epoch 1, read 2; should see new value3.
		{txn1, hlc.Timestamp{WallTime: 2}, &value3},
		// Epoch 1, read 3; should see new value3.
		{txn1, hlc.Timestamp{WallTime: 3}, &value3},
		// Epoch 1, read 4; should see new value3.
		{txn1, hlc.Timestamp{WallTime: 4}, &value3},
		// Epoch 1, read 5; should see new value3.
		{txn1, hlc.Timestamp{WallTime: 5}, &value3},
		// Epoch 2, read 1; should see committed value1.
		{txn1e2, hlc.Timestamp{WallTime: 1}, &value1},
		// Epoch 2, read 2; should see committed value1.
		{txn1e2, hlc.Timestamp{WallTime: 2}, &value1},
		// Epoch 2, read 3; should see committed value2.
		{txn1e2, hlc.Timestamp{WallTime: 3}, &value2},
		// Epoch 2, read 4; should see committed value2.
		{txn1e2, hlc.Timestamp{WallTime: 4}, &value2},
		// Epoch 2, read 5; should see committed value2.
		{txn1e2, hlc.Timestamp{WallTime: 5}, &value2},
	}
	for i, test := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			valueRes, err := MVCCGet(ctx, engine, testKey1, test.readTS, MVCCGetOptions{Txn: test.txn})
			if err != nil || valueRes.Value == nil || !bytes.Equal(test.expValue.RawBytes, valueRes.Value.RawBytes) {
				t.Errorf("test %d: expected value %q, err nil; got %+v, %v", i, test.expValue.RawBytes, valueRes.Value, err)
			}
		})
	}
}

// TestMVCCGetWithOldEpoch writes a value first using epoch 2, then
// reads using epoch 1 to verify that the read will fail.
func TestMVCCGetWithOldEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	if _, err := MVCCPut(ctx, engine, testKey1, txn1e2.ReadTimestamp, value2, MVCCWriteOptions{Txn: txn1e2}); err != nil {
		t.Fatal(err)
	}
	_, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 2}, MVCCGetOptions{
		Txn: txn1,
	})
	if err == nil {
		t.Fatalf("unexpected success of get")
	}
}

// TestMVCCDeleteRangeWithSequence verifies that delete range operations at sequence
// numbers equal to or below the sequence of a previous delete range operation
// verify that they agree with the sequence history of each intent left by the
// delete range. If so, they become no-ops because writes are meant to be
// idempotent. If not, they throw errors.
func TestMVCCDeleteRangeWithSequence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	testCases := []struct {
		name     string
		sequence enginepb.TxnSeq
		expErr   string
	}{
		{"old seq", 5, "missing an intent"},
		{"same seq", 6, ""},
		{"new seq", 7, ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			prefix := roachpb.Key(fmt.Sprintf("key-%d", tc.sequence))
			txn := *txn1
			for i := enginepb.TxnSeq(0); i < 3; i++ {
				key := append(prefix, []byte(strconv.Itoa(int(i)))...)
				txn.Sequence = 2 + i
				if _, err := MVCCPut(ctx, engine, key, txn.WriteTimestamp, value1, MVCCWriteOptions{Txn: &txn}); err != nil {
					t.Fatal(err)
				}
			}

			// Perform the initial DeleteRange.
			const origSeq = 6
			txn.Sequence = origSeq
			origDeleted, _, origNum, _, err := MVCCDeleteRange(ctx, engine, prefix, prefix.PrefixEnd(),
				math.MaxInt64, txn.WriteTimestamp, MVCCWriteOptions{Txn: &txn}, true)
			if err != nil {
				t.Fatal(err)
			}

			txn.Sequence = tc.sequence
			deleted, _, num, _, err := MVCCDeleteRange(ctx, engine, prefix, prefix.PrefixEnd(),
				math.MaxInt64, txn.WriteTimestamp, MVCCWriteOptions{Txn: &txn}, true)
			if tc.expErr != "" && err != nil {
				if !testutils.IsError(err, tc.expErr) {
					t.Fatalf("unexpected error: %+v", err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %+v", err)
			}

			// If at the same sequence as the initial DeleteRange.
			if tc.sequence == origSeq {
				if !reflect.DeepEqual(origDeleted, deleted) {
					t.Fatalf("deleted keys did not match original execution: %+v vs. %+v",
						origDeleted, deleted)
				}
				if origNum != num {
					t.Fatalf("number of keys deleted did not match original execution: %d vs. %d",
						origNum, num)
				}
			}
		})
	}
}

// TestMVCCGetWithPushedTimestamp verifies that a read for a value
// written by the transaction, but then subsequently pushed, can still
// be read by the txn at the later timestamp, even if an earlier
// timestamp is specified. This happens when a txn's intents are
// resolved by other actors; the intents shouldn't become invisible
// to pushed txn.
func TestMVCCGetWithPushedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	// Start with epoch 1.
	if _, err := MVCCPut(ctx, engine, testKey1, txn1.ReadTimestamp, value1, MVCCWriteOptions{Txn: txn1}); err != nil {
		t.Fatal(err)
	}
	// Resolve the intent, pushing its timestamp forward.
	txn := makeTxn(*txn1, hlc.Timestamp{WallTime: 1})
	if _, _, _, _, err := MVCCResolveWriteIntent(ctx, engine, nil,
		roachpb.MakeLockUpdate(txn, roachpb.Span{Key: testKey1}),
		MVCCResolveWriteIntentOptions{}); err != nil {
		t.Fatal(err)
	}
	// Attempt to read using naive txn's previous timestamp.
	valueRes, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{Logical: 1}, MVCCGetOptions{
		Txn: txn1,
	})
	if err != nil || valueRes.Value == nil || !bytes.Equal(valueRes.Value.RawBytes, value1.RawBytes) {
		t.Errorf("expected value %q, err nil; got %+v, %v", value1.RawBytes, valueRes.Value, err)
	}
}

func TestMVCCResolveWithDiffEpochs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	if _, err := MVCCPut(ctx, engine, testKey1, txn1.ReadTimestamp, value1, MVCCWriteOptions{Txn: txn1}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey2, txn1e2.ReadTimestamp, value2, MVCCWriteOptions{Txn: txn1e2}); err != nil {
		t.Fatal(err)
	}
	numKeys, _, _, _, _, err := MVCCResolveWriteIntentRange(ctx, engine, nil,
		roachpb.MakeLockUpdate(txn1e2Commit, roachpb.Span{Key: testKey1, EndKey: testKey2.Next()}),
		MVCCResolveWriteIntentRangeOptions{MaxKeys: 2})
	if err != nil {
		t.Fatal(err)
	}
	if numKeys != 2 {
		t.Errorf("expected 2 rows resolved; got %d", numKeys)
	}

	// Verify key1 is empty, as resolution with epoch 2 would have
	// aborted the epoch 1 intent.
	valueRes, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{Logical: 1}, MVCCGetOptions{})
	if valueRes.Value != nil || err != nil {
		t.Errorf("expected value nil, err nil; got %+v, %v", valueRes.Value, err)
	}

	// Key2 should be committed.
	valueRes, err = MVCCGet(ctx, engine, testKey2, hlc.Timestamp{Logical: 1}, MVCCGetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value2.RawBytes, valueRes.Value.RawBytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value2.RawBytes, valueRes.Value.RawBytes)
	}
}

func TestMVCCResolveWithUpdatedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	if _, err := MVCCPut(ctx, engine, testKey1, txn1.ReadTimestamp, value1, MVCCWriteOptions{Txn: txn1}); err != nil {
		t.Fatal(err)
	}

	valueRes, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{
		Txn: txn1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value1.RawBytes, valueRes.Value.RawBytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.RawBytes, valueRes.Value.RawBytes)
	}

	// Resolve with a higher commit timestamp -- this should rewrite the
	// intent when making it permanent.
	txn := makeTxn(*txn1Commit, hlc.Timestamp{WallTime: 1})
	if _, _, _, _, err = MVCCResolveWriteIntent(ctx, engine, nil,
		roachpb.MakeLockUpdate(txn, roachpb.Span{Key: testKey1}),
		MVCCResolveWriteIntentOptions{}); err != nil {
		t.Fatal(err)
	}

	valueRes, err = MVCCGet(ctx, engine, testKey1, hlc.Timestamp{Logical: 1}, MVCCGetOptions{})
	if valueRes.Value != nil || err != nil {
		t.Fatalf("expected both value and err to be nil: %+v, %v", valueRes.Value, err)
	}

	valueRes, err = MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{})
	if err != nil {
		t.Error(err)
	}
	if expTS := (hlc.Timestamp{WallTime: 1}); valueRes.Value.Timestamp != expTS {
		t.Fatalf("expected timestamp %+v == %+v", valueRes.Value.Timestamp, expTS)
	}
	if !bytes.Equal(value1.RawBytes, valueRes.Value.RawBytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.RawBytes, valueRes.Value.RawBytes)
	}
}

func TestMVCCResolveWithPushedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	if _, err := MVCCPut(ctx, engine, testKey1, txn1.ReadTimestamp, value1, MVCCWriteOptions{Txn: txn1}); err != nil {
		t.Fatal(err)
	}
	valueRes, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{
		Txn: txn1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value1.RawBytes, valueRes.Value.RawBytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.RawBytes, valueRes.Value.RawBytes)
	}

	// Resolve with a higher commit timestamp, but with still-pending transaction.
	// This represents a straightforward push (i.e. from a read/write conflict).
	txn := makeTxn(*txn1, hlc.Timestamp{WallTime: 1})
	if _, _, _, _, err = MVCCResolveWriteIntent(ctx, engine, nil,
		roachpb.MakeLockUpdate(txn, roachpb.Span{Key: testKey1}),
		MVCCResolveWriteIntentOptions{}); err != nil {
		t.Fatal(err)
	}

	valueRes, err = MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{})
	if valueRes.Value != nil || err == nil {
		t.Fatalf("expected both value nil and err to be a LockConflictError: %+v", valueRes.Value)
	}

	// Can still fetch the value using txn1.
	valueRes, err = MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{
		Txn: txn1,
	})
	if err != nil {
		t.Error(err)
	}
	if expTS := (hlc.Timestamp{WallTime: 1}); valueRes.Value.Timestamp != expTS {
		t.Fatalf("expected timestamp %+v == %+v", valueRes.Value.Timestamp, expTS)
	}
	if !bytes.Equal(value1.RawBytes, valueRes.Value.RawBytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.RawBytes, valueRes.Value.RawBytes)
	}
}

func TestMVCCResolveTxnNoOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	// Resolve a non existent key; noop.
	if _, _, _, _, err := MVCCResolveWriteIntent(ctx, engine, nil,
		roachpb.MakeLockUpdate(txn1Commit, roachpb.Span{Key: testKey1}),
		MVCCResolveWriteIntentOptions{}); err != nil {
		t.Fatal(err)
	}

	// Add key and resolve despite there being no intent.
	if _, err := MVCCPut(ctx, engine, testKey1, hlc.Timestamp{Logical: 1}, value1, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := MVCCResolveWriteIntent(ctx, engine, nil,
		roachpb.MakeLockUpdate(txn2Commit, roachpb.Span{Key: testKey1}),
		MVCCResolveWriteIntentOptions{}); err != nil {
		t.Fatal(err)
	}

	// Write intent and resolve with different txn.
	if _, err := MVCCPut(ctx, engine, testKey2, txn1.ReadTimestamp, value2, MVCCWriteOptions{Txn: txn1}); err != nil {
		t.Fatal(err)
	}

	txn1CommitWithTS := txn2Commit.Clone()
	txn1CommitWithTS.WriteTimestamp = hlc.Timestamp{WallTime: 1}
	if _, _, _, _, err := MVCCResolveWriteIntent(ctx, engine, nil,
		roachpb.MakeLockUpdate(txn1CommitWithTS, roachpb.Span{Key: testKey2}),
		MVCCResolveWriteIntentOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestMVCCResolveTxnRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	if _, err := MVCCPut(ctx, engine, testKey1, txn1.ReadTimestamp, value1, MVCCWriteOptions{Txn: txn1}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey2, hlc.Timestamp{Logical: 1}, value2, MVCCWriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey3, txn2.ReadTimestamp, value3, MVCCWriteOptions{Txn: txn2}); err != nil {
		t.Fatal(err)
	}
	if _, err := MVCCPut(ctx, engine, testKey4, txn1.ReadTimestamp, value4, MVCCWriteOptions{Txn: txn1}); err != nil {
		t.Fatal(err)
	}

	numKeys, _, resumeSpan, _, _, err := MVCCResolveWriteIntentRange(ctx, engine, nil,
		roachpb.MakeLockUpdate(txn1Commit, roachpb.Span{Key: testKey1, EndKey: testKey4.Next()}),
		MVCCResolveWriteIntentRangeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if numKeys != 2 || resumeSpan != nil {
		t.Fatalf("expected all keys to process for resolution, even though 2 are noops; got %d, resume=%s",
			numKeys, resumeSpan)
	}

	{
		valueRes, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{Logical: 1}, MVCCGetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value1.RawBytes, valueRes.Value.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				value1.RawBytes, valueRes.Value.RawBytes)
		}
	}
	{
		valueRes, err := MVCCGet(ctx, engine, testKey2, hlc.Timestamp{Logical: 1}, MVCCGetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value2.RawBytes, valueRes.Value.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				value2.RawBytes, valueRes.Value.RawBytes)
		}
	}
	{
		valueRes, err := MVCCGet(ctx, engine, testKey3, hlc.Timestamp{Logical: 1}, MVCCGetOptions{
			Txn: txn2,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value3.RawBytes, valueRes.Value.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				value3.RawBytes, valueRes.Value.RawBytes)
		}
	}
	{
		valueRes, err := MVCCGet(ctx, engine, testKey4, hlc.Timestamp{Logical: 1}, MVCCGetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value4.RawBytes, valueRes.Value.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				value1.RawBytes, valueRes.Value.RawBytes)
		}
	}
}

func TestMVCCResolveTxnRangeResume(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	// Write 10 keys from txn1, 10 from txn2, and 10 with no txn,
	// interleaved. The length of these keys changes and is non-decreasing.
	// This exercises a subtle bug where separatedIntentAndVersionIter
	// forgot to update its intentKey, but in some cases the shared slice
	// for the unsafe key caused it to be inadvertently updated in a correct
	// way.
	for i := 0; i < 30; i += 3 {
		key0 := roachpb.Key(fmt.Sprintf("%02d%d", i+0, i+0))
		key1 := roachpb.Key(fmt.Sprintf("%02d%d", i+1, i+1))
		key2 := roachpb.Key(fmt.Sprintf("%02d%d", i+2, i+2))
		if _, err := MVCCPut(ctx, engine, key0, txn1.ReadTimestamp, value1, MVCCWriteOptions{Txn: txn1}); err != nil {
			t.Fatal(err)
		}
		txn2ts := makeTxn(*txn2, hlc.Timestamp{Logical: 2})
		if _, err := MVCCPut(ctx, engine, key1, txn2ts.ReadTimestamp, value2, MVCCWriteOptions{Txn: txn2ts}); err != nil {
			t.Fatal(err)
		}
		if _, err := MVCCPut(ctx, engine, key2, hlc.Timestamp{Logical: 3}, value3, MVCCWriteOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	rw := engine.NewBatch()
	defer rw.Close()

	// Resolve up to 6 intents: the keys are 000, 033, 066, 099, 1212, 1515.
	numKeys, _, resumeSpan, _, _, err := MVCCResolveWriteIntentRange(ctx, rw, nil,
		roachpb.MakeLockUpdate(txn1Commit, roachpb.Span{Key: roachpb.Key("00"), EndKey: roachpb.Key("33")}),
		MVCCResolveWriteIntentRangeOptions{MaxKeys: 6})
	if err != nil {
		t.Fatal(err)
	}
	if numKeys != 6 || resumeSpan == nil {
		t.Errorf("expected resolution for only 6 keys; got %d, resume=%s", numKeys, resumeSpan)
	}
	expResumeSpan := roachpb.Span{Key: roachpb.Key("1515").Next(), EndKey: roachpb.Key("33")}
	if !resumeSpan.Equal(expResumeSpan) {
		t.Errorf("expected resume span %s; got %s", expResumeSpan, resumeSpan)
	}
	require.NoError(t, rw.Commit(true))
	// Check that the intents are actually gone by trying to read above them
	// using txn2.
	for i := 0; i < 18; i += 3 {
		res, err := MVCCGet(ctx, engine, roachpb.Key(fmt.Sprintf("%02d%d", i, i)),
			txn2.ReadTimestamp, MVCCGetOptions{Txn: txn2})
		require.NotNil(t, res.Value)
		require.NoError(t, err)
		require.Nil(t, res.Intent)
	}
}

// This test is similar to TestMVCCResolveTxnRangeResume, and additionally has
// keys with many versions and resumes intent resolution until it completes.
func TestMVCCResolveTxnRangeResumeWithManyVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	// Write 1000 keys with intents of which 100 are by the txn for which we
	// will perform intent resolution.
	lockUpdate := setupKeysWithIntent(t, engine, 5, /* numVersions */
		1 /* numFlushedVersions */, false, 10,
		false /* resolveIntentForLatestVersionWhenNonLockUpdateTxn */)
	lockUpdate.Key = makeKey(nil, 0)
	lockUpdate.EndKey = makeKey(nil, numIntentKeys)
	i := 0
	for {
		// Resolve up to 20 intents.
		numKeys, _, resumeSpan, _, _, err := MVCCResolveWriteIntentRange(ctx, engine, nil, lockUpdate,
			MVCCResolveWriteIntentRangeOptions{MaxKeys: 20})
		require.NoError(t, err)
		require.Equal(t, int64(20), numKeys)
		i++
		if resumeSpan == nil {
			break
		}
		expResumeSpan := roachpb.Span{
			Key:    makeKey(nil, (i*20-1)*10).Next(),
			EndKey: lockUpdate.EndKey,
		}
		if !resumeSpan.Equal(expResumeSpan) {
			t.Errorf("expected resume span %s; got %s", expResumeSpan, resumeSpan)
		}
		lockUpdate.Span = expResumeSpan
	}
	require.Equal(t, 5, i)
}

func generateBytes(rng *rand.Rand, min int, max int) []byte {
	iterations := min + rng.Intn(max-min)
	result := make([]byte, 0, iterations)
	for i := 0; i < iterations; i++ {
		result = append(result, byte(rng.Float64()*float64('z'-'a')+'a'))
	}
	return result
}

func createEngWithSeparatedIntents(t *testing.T) Engine {
	eng, err := Open(context.Background(), InMemory(),
		cluster.MakeTestingClusterSettings(), MaxSizeBytes(1<<20))
	require.NoError(t, err)
	return eng
}

type putState struct {
	key     roachpb.Key
	values  []roachpb.Value
	seqs    []enginepb.TxnSeq
	writeTS []hlc.Timestamp
}

func writeToEngine(
	t *testing.T, eng Engine, puts []putState, txn *roachpb.Transaction, debug bool,
) {
	ctx := context.Background()
	if debug {
		log.Infof(ctx, "writeToEngine")
	}
	for _, p := range puts {
		for i := range p.writeTS {
			txn.Sequence = p.seqs[i]
			txn.WriteTimestamp = p.writeTS[i]
			if debug {
				log.Infof(ctx, "Put: %s, seq: %d, writets: %s",
					p.key.String(), txn.Sequence, txn.WriteTimestamp.String())
			}
			_, err := MVCCPut(ctx, eng, p.key, txn.ReadTimestamp, p.values[i], MVCCWriteOptions{Txn: txn})
			require.NoError(t, err)
		}
	}
}

func checkEngineEquality(
	t *testing.T, span roachpb.Span, eng1 Engine, eng2 Engine, expectEmpty bool, debug bool,
) {
	ctx := context.Background()
	if debug {
		log.Infof(ctx, "checkEngineEquality")
	}
	makeIter := func(eng Engine) MVCCIterator {
		iter, err := eng.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{LowerBound: span.Key, UpperBound: span.EndKey})
		if err != nil {
			t.Fatal(err)
		}
		iter.SeekGE(MVCCKey{Key: span.Key})
		return iter
	}
	iter1, iter2 := makeIter(eng1), makeIter(eng2)
	defer iter1.Close()
	defer iter2.Close()
	count := 0
	for {
		valid1, err1 := iter1.Valid()
		valid2, err2 := iter2.Valid()
		require.NoError(t, err1)
		require.NoError(t, err2)
		if valid1 && !valid2 {
			t.Fatalf("iter2 exhausted before iter1")
		} else if !valid1 && valid2 {
			t.Fatalf("iter1 exhausted before iter2")
		}
		if !valid1 && !valid2 {
			break
		}
		count++
		if !iter1.UnsafeKey().Equal(iter2.UnsafeKey()) {
			t.Fatalf("keys not equal %s, %s", iter1.UnsafeKey().String(), iter2.UnsafeKey().String())
		}
		v1, err := iter1.UnsafeValue()
		require.NoError(t, err)
		v2, err := iter2.UnsafeValue()
		require.NoError(t, err)
		if !bytes.Equal(v1, v2) {
			t.Fatalf("key %s has different values: %x, %x", iter1.UnsafeKey().String(), v1, v2)
		}
		if debug {
			log.Infof(ctx, "key: %s", iter1.UnsafeKey().String())
		}
		iter1.Next()
		iter2.Next()
	}
	if expectEmpty && count > 0 {
		t.Fatalf("expected no keys but found %d", count)
	}
}

// TestRandomizedMVCCResolveWriteIntentRange generates random keys and values
// of different lengths, and exercises ranged intent resolution with
// randomized transaction status and ignored seqnums. Currently it compares
// the result of using the slow and fast paths for equality. When the slow
// path is removed we will need to improve the correctness checking in this
// test.
//
// TODO(sumeer): add epoch changes.
func TestRandomizedMVCCResolveWriteIntentRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	seed := *seedFlag
	debug := true
	if seed < 0 {
		seed = rand.Int63()
		debug = false
	}
	// Else, seed is being specified to debug a failure.

	fmt.Printf("seed: %d\n", seed)
	rng := rand.New(rand.NewSource(seed))
	ctx := context.Background()
	var engs [2]struct {
		eng   Engine
		stats enginepb.MVCCStats
	}
	// Engines are created with separated intents enabled.
	for i := range engs {
		engs[i].eng = createEngWithSeparatedIntents(t)
		defer engs[i].eng.Close()
	}
	var seq enginepb.TxnSeq
	var puts []putState
	timestamps := []hlc.Timestamp{{WallTime: 100}, {WallTime: 200}, {WallTime: 300}}
	keys := make(map[string]struct{})
	// 100 keys.
	for i := 0; i < 100; i++ {
		var key []byte
		for {
			// Keys are of different lengths. We've had a bug in the past that was
			// not triggered by tests that were using same length keys.
			key = generateBytes(rng, 10, 20)
			if _, ok := keys[string(key)]; !ok {
				break
			}
		}
		put := putState{
			key: key,
		}
		tsIndex := 0
		// Multiple versions of the key. The timestamps will be monotonically
		// non-decreasing due to tsIndex.
		versions := rng.Intn(3) + 1
		for j := 0; j < versions; j++ {
			val := roachpb.MakeValueFromBytes(generateBytes(rng, 20, 30))
			put.values = append(put.values, val)
			put.seqs = append(put.seqs, seq)
			seq++
			index := rng.Intn(len(timestamps))
			if index > tsIndex {
				tsIndex = index
			}
			put.writeTS = append(put.writeTS, timestamps[tsIndex])
		}
		puts = append(puts, put)
	}
	sort.Slice(puts, func(i, j int) bool {
		return puts[i].key.Compare(puts[j].key) < 0
	})
	// Do the puts to the engines.
	for i := range engs {
		txn := *txn1
		txn.ReadTimestamp = timestamps[0]
		txn.MinTimestamp = txn.ReadTimestamp
		writeToEngine(t, engs[i].eng, puts, &txn, debug)
	}
	// Resolve intent range.
	txn := *txn1
	txnMeta := txn.TxnMeta
	txnMeta.WriteTimestamp = timestamps[len(timestamps)-1]
	txnMeta.MinTimestamp = timestamps[0]
	txnMeta.Sequence = seq
	status := []roachpb.TransactionStatus{
		roachpb.PENDING, roachpb.COMMITTED, roachpb.ABORTED}[rng.Intn(3)]
	var ignoredSeqNums []enginepb.IgnoredSeqNumRange
	// Since the number of versions per key are randomized, stepping here by the
	// constant 5 is sufficient to randomize which versions of a key get
	// ignored.
	for i := enginepb.TxnSeq(0); i < seq; i += 5 {
		ignoredSeqNums = append(ignoredSeqNums, enginepb.IgnoredSeqNumRange{Start: i, End: i})
	}
	lu := roachpb.LockUpdate{
		Span: roachpb.Span{
			Key:    puts[0].key,
			EndKey: encoding.BytesNext(puts[len(puts)-1].key),
		},
		Txn:            txnMeta,
		Status:         status,
		IgnoredSeqNums: ignoredSeqNums,
	}
	if debug {
		log.Infof(ctx, "LockUpdate: %s, %s", status.String(), lu.String())
	}
	for i := range engs {
		func() {
			batch := engs[i].eng.NewBatch()
			defer batch.Close()
			_, _, _, _, _, err := MVCCResolveWriteIntentRange(ctx, batch, &engs[i].stats, lu, MVCCResolveWriteIntentRangeOptions{})
			require.NoError(t, err)
			require.NoError(t, batch.Commit(false))
		}()
	}
	require.Equal(t, engs[0].stats, engs[1].stats)
	// TODO(sumeer): mvccResolveWriteIntent has a bug when the txn is being
	// ABORTED and there are IgnoredSeqNums that are causing a partial rollback.
	// It does the partial rollback and does not actually resolve the intent.
	// This does not affect correctness since the intent resolution will get
	// retried. So we pass expectEmpty=false here, and retry the intent
	// resolution if aborted, and then check again with expectEmpty=true.
	checkEngineEquality(t, lu.Span, engs[0].eng, engs[1].eng, false, debug)
	if status == roachpb.ABORTED {
		for i := range engs {
			func() {
				batch := engs[i].eng.NewBatch()
				defer batch.Close()
				_, _, _, _, _, err := MVCCResolveWriteIntentRange(ctx, batch, &engs[i].stats, lu, MVCCResolveWriteIntentRangeOptions{})
				require.NoError(t, err)
				require.NoError(t, batch.Commit(false))
			}()
		}
		checkEngineEquality(t, lu.Span, engs[0].eng, engs[1].eng, true, debug)
	}
}

// TestRandomizedSavepointRollbackAndIntentResolution is a randomized test
// that tries to confirm that rolling back savepoints and then putting again
// does not cause incorrectness when doing intent resolution. This would fail
// under the bug documented in #69891.
func TestRandomizedSavepointRollbackAndIntentResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	seed := *seedFlag
	debug := true
	if seed < 0 {
		seed = rand.Int63()
		debug = false
	}
	// Else, seed is being specified to debug a failure.

	fmt.Printf("seed: %d\n", seed)
	rng := rand.New(rand.NewSource(seed))
	ctx := context.Background()
	eng, err := Open(
		context.Background(), InMemory(), cluster.MakeTestingClusterSettings(),
		func(cfg *engineConfig) error {
			cfg.opts.LBaseMaxBytes = int64(100 + rng.Intn(16384))
			log.Infof(ctx, "lbase: %d", cfg.opts.LBaseMaxBytes)
			return nil
		})
	require.NoError(t, err)
	defer eng.Close()

	var seq enginepb.TxnSeq
	var puts []putState
	timestamps := []hlc.Timestamp{{WallTime: 100}, {WallTime: 200}, {WallTime: 300}}
	keys := make(map[string]struct{})
	// 100 keys, each written to twice by the txn.
	for i := 0; i < 100; i++ {
		var key []byte
		for {
			key = generateBytes(rng, 10, 20)
			if _, ok := keys[string(key)]; !ok {
				break
			}
		}
		put := putState{
			key: key,
		}
		for j := 0; j < 2; j++ {
			val := roachpb.MakeValueFromBytes(generateBytes(rng, 20, 30))
			put.values = append(put.values, val)
			put.seqs = append(put.seqs, seq)
			seq++
			put.writeTS = append(put.writeTS, timestamps[j])
		}
		puts = append(puts, put)
	}
	sort.Slice(puts, func(i, j int) bool {
		return puts[i].key.Compare(puts[j].key) < 0
	})
	txn := *txn1
	txn.ReadTimestamp = timestamps[0]
	txn.MinTimestamp = txn.ReadTimestamp
	writeToEngine(t, eng, puts, &txn, debug)
	// The two SET calls for writing the intent are collapsed down to L6.
	require.NoError(t, eng.Flush())
	require.NoError(t, eng.Compact())

	txn.WriteTimestamp = timestamps[1]
	txn.Sequence = seq
	ignoredSeqNums := []enginepb.IgnoredSeqNumRange{{Start: 0, End: seq - 1}}
	lu := roachpb.LockUpdate{
		Span: roachpb.Span{
			Key:    puts[0].key,
			EndKey: encoding.BytesNext(puts[len(puts)-1].key),
		},
		Txn:            txn.TxnMeta,
		Status:         roachpb.PENDING,
		IgnoredSeqNums: ignoredSeqNums,
	}
	if debug {
		log.Infof(ctx, "LockUpdate: %s", lu.String())
	}
	// All the writes are ignored, so DEL is written for the intent. These
	// should be buffered in the memtable.
	_, _, _, _, _, err = MVCCResolveWriteIntentRange(ctx, eng, nil, lu, MVCCResolveWriteIntentRangeOptions{})
	require.NoError(t, err)
	{
		iter, err := eng.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{LowerBound: lu.Span.Key, UpperBound: lu.Span.EndKey})
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()
		iter.SeekGE(MVCCKey{Key: lu.Span.Key})
		valid, err := iter.Valid()
		require.NoError(t, err)
		require.False(t, valid)
	}
	// Do another put for all these keys. These will also be in the memtable.
	for i := 0; i < 100; i++ {
		val := roachpb.MakeValueFromBytes(generateBytes(rng, 2, 3))
		puts[i].values = append(puts[i].values[:0], val)
		puts[i].seqs = append(puts[i].seqs[:0], seq)
		seq++
		puts[i].writeTS = append(puts[i].writeTS[:0], timestamps[2])
	}
	writeToEngine(t, eng, puts, &txn, debug)
	// Flush of the memtable will collapse DEL=>SET into SETWITHDEL.
	require.NoError(t, eng.Flush())

	// Commit or abort the txn, so that we eventually get
	// SET=>SETWITHDEL=>SINGLEDEL for the intents.
	txn.WriteTimestamp = timestamps[2]
	txn.Sequence = seq
	lu.Txn = txn.TxnMeta
	lu.Status = []roachpb.TransactionStatus{roachpb.COMMITTED, roachpb.ABORTED}[rng.Intn(2)]
	if debug {
		log.Infof(ctx, "LockUpdate: %s", lu.String())
	}
	_, _, _, _, _, err = MVCCResolveWriteIntentRange(ctx, eng, nil, lu, MVCCResolveWriteIntentRangeOptions{})
	require.NoError(t, err)
	// Compact the engine so that SINGLEDEL consumes the SETWITHDEL, becoming a
	// DEL.
	require.NoError(t, eng.Compact())
	iter, err := eng.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{LowerBound: lu.Span.Key, UpperBound: lu.Span.EndKey})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	iter.SeekGE(MVCCKey{Key: lu.Span.Key})
	if lu.Status == roachpb.COMMITTED {
		i := 0
		for {
			valid, err := iter.Valid()
			require.NoError(t, err)
			if !valid {
				break
			}
			i++
			// Expect only the committed values.
			require.Equal(t, timestamps[2], iter.UnsafeKey().Timestamp)
			iter.Next()
		}
		require.Equal(t, 100, i)
	} else {
		// ABORTED. Nothing to iterate over.
		valid, err := iter.Valid()
		require.NoError(t, err)
		// The correct behavior is !valid. But if there is a bug, the
		// intentInterleavingIter does not always expose its error immediately (in
		// this case the error would an intent without a provisional value), so we
		// step it forward once.
		if valid {
			iter.Next()
			_, err = iter.Valid()
			require.NoError(t, err)
			// Should fail on previous statement, but this whole path is incorrect,
			// so fail here.
			t.Fatal(t, "iter is valid")
		}
	}
}

func TestValidSplitKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		key   roachpb.Key
		valid bool
	}{
		{roachpb.Key("\x02"), false},
		{roachpb.Key("\x02\x00"), false},
		{roachpb.Key("\x02\xff"), false},
		{roachpb.Key("\x03"), true},
		{roachpb.Key("\x03\x00"), true},
		{roachpb.Key("\x03\xff"), true},
		{roachpb.Key("\x03\xff\xff"), false},
		{roachpb.Key("\x03\xff\xff\x88"), false},
		{roachpb.Key("\x04"), true},
		{roachpb.Key("\x05"), true},
		{roachpb.Key("a"), true},
		{roachpb.Key("\xff"), true},
		{roachpb.Key("\xff\x01"), true},
	}
	for i, test := range testCases {
		valid := IsValidSplitKey(test.key)
		if valid != test.valid {
			t.Errorf("%d: expected %q [%x] valid %t; got %t",
				i, test.key, []byte(test.key), test.valid, valid)
		}
	}
}

func TestFindSplitKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	ms := &enginepb.MVCCStats{}
	// Generate a series of KeyValues, each containing targetLength
	// bytes, writing key #i to (encoded) key #i through the MVCC
	// facility. Assuming that this translates roughly into same-length
	// values after MVCC encoding, the split key should hence be chosen
	// as the middle key of the interval.
	splitReservoirSize := 100
	for i := 0; i < splitReservoirSize; i++ {
		k := fmt.Sprintf("%09d", i)
		v := strings.Repeat("X", 10-len(k))
		val := roachpb.MakeValueFromString(v)
		// Write the key and value through MVCC
		if _, err := MVCCPut(
			ctx, engine, []byte(k), hlc.Timestamp{Logical: 1}, val, MVCCWriteOptions{Stats: ms},
		); err != nil {
			t.Fatal(err)
		}
	}

	testData := []struct {
		targetSize int64
		splitInd   int
	}{
		{(ms.KeyBytes + ms.ValBytes) / 2, splitReservoirSize / 2},
		{0, 0},
		{math.MaxInt64, splitReservoirSize},
	}

	for i, td := range testData {
		humanSplitKey, err := MVCCFindSplitKey(ctx, engine, roachpb.RKeyMin, roachpb.RKeyMax, td.targetSize)
		if err != nil {
			t.Fatal(err)
		}
		ind, err := strconv.Atoi(string(humanSplitKey))
		if err != nil {
			t.Fatalf("%d: could not parse key %s as int: %+v", i, humanSplitKey, err)
		}
		if ind == 0 {
			t.Fatalf("%d: should never select first key as split key", i)
		}
		if diff := td.splitInd - ind; diff > 1 || diff < -1 {
			t.Fatalf("%d: wanted key #%d+-1, but got %d (diff %d)", i, td.splitInd, ind, diff)
		}
	}
}

// Injected via `external_helpers_test.go`.
var TestingUserDescID func(offset uint32) uint32

// TestFindValidSplitKeys verifies split keys are located such that
// they avoid splits through invalid key ranges.
func TestFindValidSplitKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	userID := TestingUserDescID(0)
	type testCase struct {
		keys       []roachpb.Key
		rangeStart roachpb.Key // optional
		expSplit   roachpb.Key
		expError   bool
		skipTenant bool // if true, skip tenant subtests
	}
	prefixTestKeys := func(test testCase, prefix roachpb.Key) testCase {
		// testCase.keys
		oldKeys := test.keys
		test.keys = make([]roachpb.Key, len(oldKeys))
		for i, key := range oldKeys {
			test.keys[i] = append(prefix, key...)
		}
		// testCase.rangeStart
		if test.rangeStart != nil {
			test.rangeStart = append(prefix, test.rangeStart...)
		}
		// testCase.expSplit
		if test.expSplit != nil {
			test.expSplit = append(prefix, test.expSplit...)
		}
		return test
	}

	testCases := []testCase{
		// All m1 cannot be split.
		{
			keys: []roachpb.Key{
				roachpb.Key("\x02"),
				roachpb.Key("\x02\x00"),
				roachpb.Key("\x02\xff"),
			},
			expSplit:   nil,
			expError:   false,
			skipTenant: true,
		},
		// Between meta1 and meta2, splits at meta2.
		{
			keys: []roachpb.Key{
				roachpb.Key("\x02"),
				roachpb.Key("\x02\x00"),
				roachpb.Key("\x02\xff"),
				roachpb.Key("\x03"),
				roachpb.Key("\x03\x00"),
				roachpb.Key("\x03\xff"),
			},
			expSplit:   roachpb.Key("\x03"),
			expError:   false,
			skipTenant: true,
		},
		// Even lopsided, always split at meta2.
		{
			keys: []roachpb.Key{
				roachpb.Key("\x02"),
				roachpb.Key("\x02\x00"),
				roachpb.Key("\x02\xff"),
				roachpb.Key("\x03"),
			},
			expSplit:   roachpb.Key("\x03"),
			expError:   false,
			skipTenant: true,
		},
		// Between meta2Max and metaMax, splits at metaMax.
		{
			keys: []roachpb.Key{
				roachpb.Key("\x03\xff\xff"),
				roachpb.Key("\x03\xff\xff\x88"),
				roachpb.Key("\x04"),
				roachpb.Key("\x04\xff\xff\x88"),
			},
			expSplit:   roachpb.Key("\x04"),
			expError:   false,
			skipTenant: true,
		},
		// Even lopsided, always split at metaMax.
		{
			keys: []roachpb.Key{
				roachpb.Key("\x03\xff\xff"),
				roachpb.Key("\x03\xff\xff\x11"),
				roachpb.Key("\x03\xff\xff\x88"),
				roachpb.Key("\x03\xff\xff\xee"),
				roachpb.Key("\x04"),
			},
			expSplit:   roachpb.Key("\x04"),
			expError:   false,
			skipTenant: true,
		},
		// Lopsided, truncate non-zone prefix.
		{
			keys: []roachpb.Key{
				roachpb.Key("\x04zond"),
				roachpb.Key("\x04zone"),
				roachpb.Key("\x04zone\x00"),
				roachpb.Key("\x04zone\xff"),
			},
			expSplit:   roachpb.Key("\x04zone\x00"),
			expError:   false,
			skipTenant: true,
		},
		// Lopsided, truncate non-zone suffix.
		{
			keys: []roachpb.Key{
				roachpb.Key("\x04zone"),
				roachpb.Key("\x04zone\x00"),
				roachpb.Key("\x04zone\xff"),
				roachpb.Key("\x04zonf"),
			},
			expSplit:   roachpb.Key("\x04zone\xff"),
			expError:   false,
			skipTenant: true,
		},
		// A Range for which MVCCSplitKey would return the start key isn't fair
		// game, even if the StartKey actually exists. There was once a bug
		// here which compared timestamps as well as keys and thus didn't
		// realize it was splitting at the initial key (due to getting confused
		// by the actual value having a nonzero timestamp).
		{
			keys: []roachpb.Key{
				roachpb.Key("b"),
			},
			rangeStart: roachpb.Key("a"),
			expSplit:   nil,
			expError:   false,
		},
		// Similar test, but the range starts at first key.
		{
			keys: []roachpb.Key{
				roachpb.Key("b"),
			},
			rangeStart: roachpb.Key("b"),
			expSplit:   nil,
			expError:   false,
		},
		// Some example table data. Make sure we don't split in the middle of a row
		// or return the start key of the range.
		{
			keys: []roachpb.Key{
				testAddColFam(testTablePrefix(userID, "a"), 1),
				testAddColFam(testTablePrefix(userID, "a"), 2),
				testAddColFam(testTablePrefix(userID, "a"), 3),
				testAddColFam(testTablePrefix(userID, "a"), 4),
				testAddColFam(testTablePrefix(userID, "a"), 5),
				testAddColFam(testTablePrefix(userID, "b"), 1),
				testAddColFam(testTablePrefix(userID, "c"), 1),
			},
			rangeStart: testTablePrefix(userID, "a"),
			expSplit:   testTablePrefix(userID, "b"),
			expError:   false,
		},
		// More example table data. Make sure ranges at the start of a table can
		// be split properly - this checks that the minSplitKey logic doesn't
		// break for such ranges.
		{
			keys: []roachpb.Key{
				testAddColFam(testTablePrefix(userID, "a"), 1),
				testAddColFam(testTablePrefix(userID, "b"), 1),
				testAddColFam(testTablePrefix(userID, "c"), 1),
				testAddColFam(testTablePrefix(userID, "d"), 1),
			},
			rangeStart: keys.SystemSQLCodec.TablePrefix(userID),
			expSplit:   testTablePrefix(userID, "c"),
			expError:   false,
		},
		// More example table data. Make sure ranges at the start of a table can
		// be split properly even in the presence of a large first row.
		{
			keys: []roachpb.Key{
				testAddColFam(testTablePrefix(userID, "a"), 1),
				testAddColFam(testTablePrefix(userID, "a"), 2),
				testAddColFam(testTablePrefix(userID, "a"), 3),
				testAddColFam(testTablePrefix(userID, "a"), 4),
				testAddColFam(testTablePrefix(userID, "a"), 5),
				testAddColFam(testTablePrefix(userID, "b"), 1),
				testAddColFam(testTablePrefix(userID, "c"), 1),
			},
			rangeStart: keys.SystemSQLCodec.TablePrefix(TestingUserDescID(0)),
			expSplit:   testTablePrefix(userID, "b"),
			expError:   false,
		},
		// One partition where partition key is the first column. Checks that
		// split logic is not confused by the special partition start key.
		{
			keys: []roachpb.Key{
				testAddColFam(testTablePrefix(userID, "a", "a"), 1),
				testAddColFam(testTablePrefix(userID, "a", "b"), 1),
				testAddColFam(testTablePrefix(userID, "a", "c"), 1),
				testAddColFam(testTablePrefix(userID, "a", "d"), 1),
			},
			rangeStart: testTablePrefix(userID, "a"),
			expSplit:   testTablePrefix(userID, "a", "c"),
			expError:   false,
		},
		// One partition with a large first row. Checks that our logic to avoid
		// splitting in the middle of a row still applies.
		{
			keys: []roachpb.Key{
				testAddColFam(testTablePrefix(userID, "a", "a"), 1),
				testAddColFam(testTablePrefix(userID, "a", "a"), 2),
				testAddColFam(testTablePrefix(userID, "a", "a"), 3),
				testAddColFam(testTablePrefix(userID, "a", "a"), 4),
				testAddColFam(testTablePrefix(userID, "a", "a"), 5),
				testAddColFam(testTablePrefix(userID, "a", "b"), 1),
				testAddColFam(testTablePrefix(userID, "a", "c"), 1),
			},
			rangeStart: testTablePrefix(userID, "a"),
			expSplit:   testTablePrefix(userID, "a", "b"),
			expError:   false,
		},
	}

	testutils.RunTrueAndFalse(t, "tenant", func(t *testing.T, tenant bool) {
		for i, test := range testCases {
			t.Run("", func(t *testing.T) {
				if tenant {
					if test.skipTenant {
						skip.IgnoreLint(t, "")
					}
					// Update all keys to include a tenant prefix.
					tenPrefix := keys.MakeSQLCodec(roachpb.MinTenantID).TenantPrefix()
					test = prefixTestKeys(test, tenPrefix)
				}

				ctx := context.Background()
				engine := NewDefaultInMemForTesting()
				defer engine.Close()

				ms := &enginepb.MVCCStats{}
				testPopulateKeysWithVersions(ctx, t, engine, ms, test.keys)
				rangeStart := test.keys[0]
				if len(test.rangeStart) > 0 {
					rangeStart = test.rangeStart
				}
				rangeEnd := test.keys[len(test.keys)-1].Next()
				rangeStartAddr, err := keys.Addr(rangeStart)
				if err != nil {
					t.Fatal(err)
				}
				rangeEndAddr, err := keys.Addr(rangeEnd)
				if err != nil {
					t.Fatal(err)
				}
				targetSize := (ms.KeyBytes + ms.ValBytes) / 2
				splitKey, err := MVCCFindSplitKey(ctx, engine, rangeStartAddr, rangeEndAddr, targetSize)
				if test.expError {
					if !testutils.IsError(err, "has no valid splits") {
						t.Fatalf("%d: unexpected error: %+v", i, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("%d; unexpected error: %+v", i, err)
				}
				if !splitKey.Equal(test.expSplit) {
					t.Errorf("%d: expected split key %q; got %q", i, test.expSplit, splitKey)
				}
			})
		}
	})
}

// TestFindBalancedSplitKeys verifies split keys are located such that
// the left and right halves are equally balanced.
func TestFindBalancedSplitKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCases := []struct {
		keySizes []int
		valSizes []int
		expSplit int
	}{
		// Bigger keys on right side.
		{
			keySizes: []int{10, 100, 10, 10, 500},
			valSizes: []int{1, 1, 1, 1, 1},
			expSplit: 4,
		},
		// Bigger keys on left side.
		{
			keySizes: []int{1000, 500, 500, 10, 10},
			valSizes: []int{1, 1, 1, 1, 1},
			expSplit: 1,
		},
		// Bigger values on right side.
		{
			keySizes: []int{1, 1, 1, 1, 1},
			valSizes: []int{10, 100, 10, 10, 500},
			expSplit: 4,
		},
		// Bigger values on left side.
		{
			keySizes: []int{1, 1, 1, 1, 1},
			valSizes: []int{1000, 100, 500, 10, 10},
			expSplit: 1,
		},
		// Bigger key/values on right side.
		{
			keySizes: []int{10, 100, 10, 10, 250},
			valSizes: []int{10, 100, 10, 10, 250},
			expSplit: 4,
		},
		// Bigger key/values on left side.
		{
			keySizes: []int{500, 50, 250, 10, 10},
			valSizes: []int{500, 50, 250, 10, 10},
			expSplit: 1,
		},
	}

	for i, test := range testCases {
		t.Run("", func(t *testing.T) {
			ctx := context.Background()
			engine := NewDefaultInMemForTesting()
			defer engine.Close()

			ms := &enginepb.MVCCStats{}
			var expKey roachpb.Key
			for j, keySize := range test.keySizes {
				key := roachpb.Key(fmt.Sprintf("%d%s", j, strings.Repeat("X", keySize)))
				if test.expSplit == j {
					expKey = key
				}
				val := roachpb.MakeValueFromString(strings.Repeat("X", test.valSizes[j]))
				if _, err := MVCCPut(
					ctx, engine, key, hlc.Timestamp{Logical: 1}, val, MVCCWriteOptions{Stats: ms},
				); err != nil {
					t.Fatal(err)
				}
			}
			targetSize := (ms.KeyBytes + ms.ValBytes) / 2
			splitKey, err := MVCCFindSplitKey(ctx, engine, roachpb.RKey("\x02"), roachpb.RKeyMax, targetSize)
			if err != nil {
				t.Fatalf("unexpected error: %+v", err)
			}
			if !splitKey.Equal(expKey) {
				t.Errorf("%d: expected split key %q; got %q", i, expKey, splitKey)
			}
		})
	}
}

// testAddPrefix manually creates rows corresponding to the schema e.g.
// CREATE TABLE t (id1 STRING, id2 STRING, ... PRIMARY KEY (id1, id2, ...))
func testAddPrefix(prefix roachpb.Key, id uint32, rowVals ...string) roachpb.Key {
	tableKey := append(prefix, keys.SystemSQLCodec.TablePrefix(id)...)
	rowKey := roachpb.Key(encoding.EncodeVarintAscending(tableKey, 1))
	for _, rowVal := range rowVals {
		rowKey = encoding.EncodeStringAscending(rowKey, rowVal)
	}
	return rowKey
}

func testTablePrefix(id uint32, rowVals ...string) roachpb.Key {
	return testAddPrefix(nil, id, rowVals...)
}
func testAddColFam(rowKey roachpb.Key, colFam uint32) roachpb.Key {
	return keys.MakeFamilyKey(append([]byte(nil), rowKey...), colFam)
}

// testPopulateKeysWithVersions puts the keys into the engine provided. Each
// key is added with 3 MVCC versions with a XX.. value.
func testPopulateKeysWithVersions(
	ctx context.Context, t *testing.T, engine Engine, ms *enginepb.MVCCStats, keys []roachpb.Key,
) {
	val := roachpb.MakeValueFromString(strings.Repeat("X", 10))
	for _, k := range keys {
		// Add three MVCC versions of every key. Splits are not allowed
		// between MVCC versions, so this shouldn't have any effect.
		for j := 1; j <= 3; j++ {
			ts := hlc.Timestamp{Logical: int32(j)}
			_, err := MVCCPut(ctx, engine, []byte(k), ts, val, MVCCWriteOptions{Stats: ms})
			require.NoError(t, err)
		}
	}
}

// TestMVCCFirstSplitKey checks that the split key returned from
// MVCCFirstSplitKey is:
// (1) Within a range's bounds
// (2) No less than the desired split key.
// (3) Greater than the first key, or first row's keys in table ranges.
// (4) Not inbetween the start and end of a row for table ranges.
func TestMVCCFirstSplitKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	userID := TestingUserDescID(0)

	type splitExpect struct {
		desired  roachpb.Key
		expected roachpb.Key
	}

	testCases := []struct {
		keys             []roachpb.Key
		startKey, endKey roachpb.Key
		splits           []splitExpect
	}{
		{
			// No keys, no splits.
			keys:     []roachpb.Key{},
			startKey: roachpb.Key("a"),
			endKey:   roachpb.Key("z"),
			splits: []splitExpect{
				{desired: roachpb.Key("a"), expected: nil},
				{desired: roachpb.Key("m"), expected: nil},
				{desired: roachpb.Key("z"), expected: nil},
			},
		},
		{
			// meta1 cannot be split. Previously, this test would cause a panic in
			// mvccMinSplitKey, called by MVCCFirstSplitKey. The iterator is
			// initialized with a global key constraint from the endKey
			// ("\x02\xff\xff"), but we seekGE the start key (MinKey="") which is
			// local because it is before LocalMax (0x02).
			keys: []roachpb.Key{
				roachpb.Key("\x02"),
				roachpb.Key("\x02\x00"),
				roachpb.Key("\x02\xff"),
			},
			startKey: keys.MinKey,
			endKey:   keys.Meta1KeyMax,
			splits: []splitExpect{
				{desired: keys.MinKey, expected: nil},
				{desired: roachpb.Key("\x02"), expected: nil},
				{desired: roachpb.Key("\x02\x00"), expected: nil},
			},
		},
		{
			// All keys are outside the range, no keys to spit at so expect no
			// splits.
			keys: []roachpb.Key{
				roachpb.Key("0"),
				roachpb.Key("c"),
				roachpb.Key("d"),
			},
			startKey: roachpb.Key("a"),
			endKey:   roachpb.Key("c"),
			splits: []splitExpect{
				{desired: roachpb.Key("a"), expected: nil},
				{desired: roachpb.Key("b"), expected: nil},
				{desired: roachpb.Key("c"), expected: nil},
			},
		},
		{
			// Only one key within the range, require at least two keys to split.
			keys: []roachpb.Key{
				// (0) is outside the range [a,c)
				roachpb.Key("0"),
				roachpb.Key("a"),
				// (c) is outside of the range [a,c).
				roachpb.Key("c"),
			},
			startKey: roachpb.Key("a"),
			endKey:   roachpb.Key("c"),
			splits: []splitExpect{
				{desired: roachpb.Key("a"), expected: nil},
				{desired: roachpb.Key("b"), expected: nil},
				{desired: roachpb.Key("c"), expected: nil},
			},
		},
		{
			// Enough keys to realize a split on c. Only desiredSplitKeys <= c should
			// split at c.
			keys: []roachpb.Key{
				// (0) is outside the range [a,e)
				roachpb.Key("0"),
				roachpb.Key("b"),
				roachpb.Key("c"),
				// (e) is outside of the range [a,e).
				roachpb.Key("e"),
			},
			startKey: roachpb.Key("a"),
			endKey:   roachpb.Key("e"),
			splits: []splitExpect{
				// Should iterate to the first split key after minSpitKey which is (c).
				{desired: roachpb.Key("0"), expected: roachpb.Key("c")},
				{desired: roachpb.Key("b"), expected: roachpb.Key("c")},
				{desired: roachpb.Key("c"), expected: roachpb.Key("c")},
				{desired: keys.MinKey, expected: roachpb.Key("c")},
				// Desired split key is after the last key in the range (c), shouldn't
				// split.
				{desired: roachpb.Key("d"), expected: nil},
			},
		},
		{
			keys: []roachpb.Key{
				testAddColFam(testTablePrefix(userID, "a"), 1),
				testAddColFam(testTablePrefix(userID, "b"), 1),
				testAddColFam(testTablePrefix(userID, "b"), 2),
				testAddColFam(testTablePrefix(userID, "b"), 3),
				testAddColFam(testTablePrefix(userID, "d"), 1),
				// (e,1) is outside of the range [a,e)
				testAddColFam(testTablePrefix(userID, "e"), 1),
			},
			startKey: testTablePrefix(userID, "a"),
			endKey:   testTablePrefix(userID, "e"),
			splits: []splitExpect{
				{desired: testAddColFam(testTablePrefix(userID, "a"), 0), expected: testTablePrefix(userID, "b")},
				{desired: testAddColFam(testTablePrefix(userID, "b"), 3), expected: testTablePrefix(userID, "b")},
				// The first key after the desired split key is (d,1), expect a split
				// at the prefix (d).
				{desired: testAddColFam(testTablePrefix(userID, "b"), 4), expected: testTablePrefix(userID, "d")},
				// Desired split key is after the last key in the range (d,1),
				// shouldn't split.
				{desired: testAddColFam(testTablePrefix(userID, "d"), 2), expected: nil},
			},
		},
		{
			// One partiton key, where the partition key is the first column (a).
			keys: []roachpb.Key{
				testAddColFam(testTablePrefix(userID, "a", "a"), 1),
				testAddColFam(testTablePrefix(userID, "a", "a"), 3),
				testAddColFam(testTablePrefix(userID, "a", "b"), 1),
				testAddColFam(testTablePrefix(userID, "a", "c"), 1),
				// (a,d,0) is outside the range [a,(a,d)).
				testAddColFam(testTablePrefix(userID, "a", "d"), 0),
			},
			startKey: testTablePrefix(userID, "a"),
			endKey:   testTablePrefix(userID, "a", "d"),
			splits: []splitExpect{
				{desired: testTablePrefix(userID, "a"), expected: testTablePrefix(userID, "a", "b")},
				{desired: testAddColFam(testTablePrefix(userID, "a", "a"), 3), expected: testTablePrefix(userID, "a", "b")},
				{desired: testAddColFam(testTablePrefix(userID, "a", "b"), 2), expected: testTablePrefix(userID, "a", "c")},
				// Desired split key is after the last key in the range (a,c,1),
				// shouldn't split.
				{desired: testAddColFam(testTablePrefix(userID, "a", "c"), 2), expected: nil},
				{desired: testTablePrefix(userID, "a", "e"), expected: nil},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%v", tc.keys),
			func(t *testing.T) {
				ctx := context.Background()
				engine := NewDefaultInMemForTesting()
				defer engine.Close()

				testPopulateKeysWithVersions(ctx, t, engine, &enginepb.MVCCStats{}, tc.keys)
				rangeStartAddr := keys.MustAddr(tc.startKey)
				rangeEndAddr := keys.MustAddr(tc.endKey)
				for _, split := range tc.splits {
					t.Run(fmt.Sprintf("%v", split.desired), func(t *testing.T) {
						desiredSplitAddr := keys.MustAddr(split.desired)
						splitKey, err := MVCCFirstSplitKey(ctx, engine, desiredSplitAddr, rangeStartAddr, rangeEndAddr)
						// NB: We don't expect errors. If no split key can be found, we
						// expect a nil splitKey to be returned.
						require.NoError(t, err)
						require.Equal(t, split.expected, splitKey)
					})
				}
			})
	}
}

// TestMVCCGarbageCollect writes a series of gc'able bytes and then
// sends an MVCC GC request and verifies cleared values and updated
// stats.
func TestMVCCGarbageCollect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	ms := &enginepb.MVCCStats{}

	val := []byte("value")
	ts1 := hlc.Timestamp{WallTime: 1e9}
	ts2 := hlc.Timestamp{WallTime: 2e9}
	ts3 := hlc.Timestamp{WallTime: 3e9}
	ts4 := hlc.Timestamp{WallTime: 4e9}
	ts5 := hlc.Timestamp{WallTime: 4e9}
	val1 := roachpb.MakeValueFromBytesAndTimestamp(val, ts1)
	val2 := roachpb.MakeValueFromBytesAndTimestamp(val, ts2)
	val3 := roachpb.MakeValueFromBytesAndTimestamp(val, ts3)
	valInline := roachpb.MakeValueFromBytesAndTimestamp(val, hlc.Timestamp{})

	testData := []struct {
		key       roachpb.Key
		vals      []roachpb.Value
		isDeleted bool // is the most recent value a deletion tombstone?
	}{
		{roachpb.Key("a"), []roachpb.Value{val1, val2}, false},
		{roachpb.Key("a-del"), []roachpb.Value{val1, val2}, true},
		{roachpb.Key("b"), []roachpb.Value{val1, val2, val3}, false},
		{roachpb.Key("b-del"), []roachpb.Value{val1, val2, val3}, true},
		{roachpb.Key("inline"), []roachpb.Value{valInline}, false},
		{roachpb.Key("r-2"), []roachpb.Value{val1}, false},
		{roachpb.Key("r-3"), []roachpb.Value{val1}, false},
		{roachpb.Key("r-4"), []roachpb.Value{val1}, false},
		{roachpb.Key("r-6"), []roachpb.Value{val1}, true},
		{roachpb.Key("t"), []roachpb.Value{val1}, false},
	}

	for i := 0; i < 3; i++ {
		for _, test := range testData {
			if i >= len(test.vals) {
				continue
			}
			for _, val := range test.vals[i : i+1] {
				if i == len(test.vals)-1 && test.isDeleted {
					if _, _, err := MVCCDelete(ctx, engine, test.key, val.Timestamp, MVCCWriteOptions{Stats: ms}); err != nil {
						t.Fatal(err)
					}
					continue
				}
				valCpy := *protoutil.Clone(&val).(*roachpb.Value)
				valCpy.Timestamp = hlc.Timestamp{}
				if _, err := MVCCPut(ctx, engine, test.key, val.Timestamp,
					valCpy, MVCCWriteOptions{Stats: ms}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if err := MVCCDeleteRangeUsingTombstone(ctx, engine, ms, roachpb.Key("r"),
		roachpb.Key("r-del").Next(), ts3, hlc.ClockTimestamp{}, nil, nil, false, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCDeleteRangeUsingTombstone(ctx, engine, ms, roachpb.Key("t"),
		roachpb.Key("u").Next(), ts2, hlc.ClockTimestamp{}, nil, nil, false, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCDeleteRangeUsingTombstone(ctx, engine, ms, roachpb.Key("t"),
		roachpb.Key("u").Next(), ts3, hlc.ClockTimestamp{}, nil, nil, false, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if log.V(1) {
		log.Info(context.Background(), "Engine content before GC")
		kvsn, err := Scan(context.Background(), engine, localMax, keyMax, 0)
		if err != nil {
			t.Fatal(err)
		}
		for i, kv := range kvsn {
			log.Infof(context.Background(), "%d: %s", i, kv.Key)
		}
	}

	gcTime := ts5
	gcKeys := []kvpb.GCRequest_GCKey{
		{Key: roachpb.Key("a"), Timestamp: ts1},
		{Key: roachpb.Key("a-del"), Timestamp: ts2},
		{Key: roachpb.Key("b"), Timestamp: ts1},
		{Key: roachpb.Key("b-del"), Timestamp: ts2},
		{Key: roachpb.Key("inline"), Timestamp: hlc.Timestamp{}},
		// Keys that don't exist, which should result in a no-op.
		{Key: roachpb.Key("a-bad"), Timestamp: ts2},
		{Key: roachpb.Key("inline-bad"), Timestamp: hlc.Timestamp{}},
		// Keys that are hidden by range key.
		// Non-existing keys that needs to skip gracefully without
		// distorting stats. (Checking that following keys doesn't affect it)
		{Key: roachpb.Key("r-0"), Timestamp: ts1},
		{Key: roachpb.Key("r-1"), Timestamp: ts4},
		// Request has a timestamp below range key, it will be handled by
		// logic processing range tombstones specifically.
		{Key: roachpb.Key("r-2"), Timestamp: ts1},
		// Requests has a timestamp at or above range key, it will be handled by
		// logic processing synthesized metadata.
		{Key: roachpb.Key("r-3"), Timestamp: ts3},
		{Key: roachpb.Key("r-4"), Timestamp: ts4},
		// This is a non-existing key that needs to skip gracefully without
		// distorting stats. Checking that absence of next key is handled.
		{Key: roachpb.Key("r-5"), Timestamp: ts4},
		// Delete key covered by range delete key.
		{Key: roachpb.Key("r-6"), Timestamp: ts4},

		{Key: roachpb.Key("t"), Timestamp: ts4},
	}
	if err := MVCCGarbageCollect(
		context.Background(), engine, ms, gcKeys, gcTime,
	); err != nil {
		t.Fatal(err)
	}

	if log.V(1) {
		log.Info(context.Background(), "Engine content after GC")
		kvsn, err := Scan(context.Background(), engine, localMax, keyMax, 0)
		if err != nil {
			t.Fatal(err)
		}
		for i, kv := range kvsn {
			log.Infof(context.Background(), "%d: %s", i, kv.Key)
		}
	}

	expEncKeys := []MVCCKey{
		mvccVersionKey(roachpb.Key("a"), ts2),
		mvccVersionKey(roachpb.Key("b"), ts3),
		mvccVersionKey(roachpb.Key("b"), ts2),
		mvccVersionKey(roachpb.Key("b-del"), ts3),
	}
	kvs, err := Scan(context.Background(), engine, localMax, keyMax, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != len(expEncKeys) {
		t.Fatalf("number of kvs %d != expected %d", len(kvs), len(expEncKeys))
	}
	for i, kv := range kvs {
		if !kv.Key.Equal(expEncKeys[i]) {
			t.Errorf("%d: expected key %q; got %q", i, expEncKeys[i], kv.Key)
		}
	}

	// Verify aggregated stats match computed stats after GC.
	for _, mvccStatsTest := range mvccStatsTests {
		t.Run(mvccStatsTest.name, func(t *testing.T) {
			expMS, err := mvccStatsTest.fn(engine, localMax, roachpb.KeyMax, gcTime.WallTime)
			if err != nil {
				t.Fatal(err)
			}
			assertEq(t, engine, "verification", ms, &expMS)
		})
	}
	// Compact the engine; the ForTesting() config option will assert that all
	// DELSIZED tombstones were appropriately sized.
	require.NoError(t, engine.Compact())
}

// TestMVCCGarbageCollectNonDeleted verifies that the first value for
// a key cannot be GC'd if it's not deleted.
func TestMVCCGarbageCollectNonDeleted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	s := "string"
	ts1 := hlc.Timestamp{WallTime: 1e9}
	ts2 := hlc.Timestamp{WallTime: 2e9}
	val1 := mkVal(s, ts1)
	val2 := mkVal(s, ts2)
	valInline := mkVal(s, hlc.Timestamp{})

	testData := []struct {
		key      roachpb.Key
		vals     []roachpb.Value
		expError string
	}{
		{roachpb.Key("a"), []roachpb.Value{val1, val2}, `request to GC non-deleted, latest value of "a"`},
		{roachpb.Key("inline"), []roachpb.Value{valInline}, ""},
	}

	for _, test := range testData {
		for _, val := range test.vals {
			valCpy := *protoutil.Clone(&val).(*roachpb.Value)
			valCpy.Timestamp = hlc.Timestamp{}
			if _, err := MVCCPut(ctx, engine, test.key, val.Timestamp, valCpy, MVCCWriteOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		keys := []kvpb.GCRequest_GCKey{
			{Key: test.key, Timestamp: ts2},
		}
		err := MVCCGarbageCollect(ctx, engine, nil, keys, ts2)
		if !testutils.IsError(err, test.expError) {
			t.Fatalf("expected error %q when garbage collecting a non-deleted live value, found %v",
				test.expError, err)
		}
	}

	// Compact the engine; the ForTesting() config option will assert that all
	// DELSIZED tombstones were appropriately sized.
	require.NoError(t, engine.Compact())
}

// TestMVCCGarbageCollectIntent verifies that an intent cannot be GC'd.
func TestMVCCGarbageCollectIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	bytes := []byte("value")
	ts1 := hlc.Timestamp{WallTime: 1e9}
	ts2 := hlc.Timestamp{WallTime: 2e9}
	key := roachpb.Key("a")
	{
		val1 := roachpb.MakeValueFromBytes(bytes)
		if _, err := MVCCPut(ctx, engine, key, ts1, val1, MVCCWriteOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	txn := &roachpb.Transaction{
		TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), WriteTimestamp: ts2},
		ReadTimestamp: ts2,
	}
	if _, _, err := MVCCDelete(ctx, engine, key, txn.ReadTimestamp, MVCCWriteOptions{Txn: txn}); err != nil {
		t.Fatal(err)
	}
	keys := []kvpb.GCRequest_GCKey{
		{Key: key, Timestamp: ts2},
	}
	if err := MVCCGarbageCollect(ctx, engine, nil, keys, ts2); err == nil {
		t.Fatal("expected error garbage collecting an intent")
	}
	// Compact the engine; the ForTesting() config option will assert that all
	// DELSIZED tombstones were appropriately sized.
	require.NoError(t, engine.Compact())
}

// TestMVCCGarbageCollectPanicsWithMixOfLocalAndGlobalKeys verifies that
// MVCCGarbageCollect panics when presented with a mix of local and global
// keys.
func TestMVCCGarbageCollectPanicsWithMixOfLocalAndGlobalKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	require.Panics(t, func() {
		ts := hlc.Timestamp{WallTime: 1e9}
		k := roachpb.Key("a")
		keys := []kvpb.GCRequest_GCKey{
			{Key: k, Timestamp: ts},
			{Key: keys.RangeDescriptorKey(roachpb.RKey(k))},
		}
		if err := MVCCGarbageCollect(ctx, engine, nil, keys, ts); err != nil {
			panic(err)
		}
	})
}

// readWriterReturningSeekLTTrackingIterator is used in a test to inject errors
// and ensure that SeekLT is returned an appropriate number of times.
type readWriterReturningSeekLTTrackingIterator struct {
	it seekLTTrackingIterator
	ReadWriter
}

// NewMVCCIterator injects a seekLTTrackingIterator over the engine's real iterator.
func (rw *readWriterReturningSeekLTTrackingIterator) NewMVCCIterator(
	ctx context.Context, iterKind MVCCIterKind, opts IterOptions,
) (MVCCIterator, error) {
	var err error
	rw.it.MVCCIterator, err = rw.ReadWriter.NewMVCCIterator(ctx, iterKind, opts)
	return &rw.it, err
}

// seekLTTrackingIterator is used to determine the number of times seekLT is
// called.
type seekLTTrackingIterator struct {
	seekLTCalled int
	MVCCIterator
}

func (it *seekLTTrackingIterator) SeekLT(k MVCCKey) {
	it.seekLTCalled++
	it.MVCCIterator.SeekLT(k)
}

// TestMVCCGarbageCollectUsesSeekLTAppropriately ensures that the garbage
// collection only utilizes SeekLT if there are enough undeleted versions.
func TestMVCCGarbageCollectUsesSeekLTAppropriately(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCaseKey struct {
		key         string
		timestamps  []int
		gcTimestamp int
		expSeekLT   bool
	}
	type testCase struct {
		name string
		keys []testCaseKey
	}
	bytes := []byte("value")
	toHLC := func(seconds int) hlc.Timestamp {
		return hlc.Timestamp{WallTime: (time.Duration(seconds) * time.Second).Nanoseconds()}
	}
	runTestCase := func(t *testing.T, tc testCase, engine Engine) {
		ctx := context.Background()
		ms := &enginepb.MVCCStats{}
		for _, key := range tc.keys {
			for _, seconds := range key.timestamps {
				val := roachpb.MakeValueFromBytes(bytes)
				ts := toHLC(seconds)
				if _, err := MVCCPut(ctx, engine, roachpb.Key(key.key), ts, val, MVCCWriteOptions{Stats: ms}); err != nil {
					t.Fatal(err)
				}
			}
		}

		var keys []kvpb.GCRequest_GCKey
		var expectedSeekLTs int
		for _, key := range tc.keys {
			keys = append(keys, kvpb.GCRequest_GCKey{
				Key:       roachpb.Key(key.key),
				Timestamp: toHLC(key.gcTimestamp),
			})
			if key.expSeekLT {
				expectedSeekLTs++
			}
		}

		batch := engine.NewBatch()
		defer batch.Close()
		rw := readWriterReturningSeekLTTrackingIterator{ReadWriter: batch}

		require.NoError(t, MVCCGarbageCollect(ctx, &rw, ms, keys, toHLC(10)))
		require.Equal(t, expectedSeekLTs, rw.it.seekLTCalled)
	}
	cases := []testCase{
		{
			name: "basic",
			keys: []testCaseKey{
				{
					key:         "a",
					timestamps:  []int{1, 2},
					gcTimestamp: 1,
				},
				{
					key:         "b",
					timestamps:  []int{1, 2, 3},
					gcTimestamp: 1,
				},
				{
					key:         "c",
					timestamps:  []int{1, 2, 3, 4},
					gcTimestamp: 1,
				},
				{
					key:         "d",
					timestamps:  []int{1, 2, 3, 4, 5},
					gcTimestamp: 1,
					expSeekLT:   true,
				},
				{
					key:         "e",
					timestamps:  []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
					gcTimestamp: 1,
					expSeekLT:   true,
				},
				{
					key:         "f",
					timestamps:  []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
					gcTimestamp: 6,
					expSeekLT:   false,
				},
			},
		},
		{
			name: "SeekLT to the end",
			keys: []testCaseKey{
				{
					key:         "ee",
					timestamps:  []int{2, 3, 4, 5, 6, 7, 8, 9},
					gcTimestamp: 1,
					expSeekLT:   true,
				},
			},
		},
		{
			name: "Next to the end",
			keys: []testCaseKey{
				{
					key:         "eee",
					timestamps:  []int{8, 9},
					gcTimestamp: 1,
					expSeekLT:   false,
				},
			},
		},
		{
			name: "Next to the next key",
			keys: []testCaseKey{
				{
					key:         "eeee",
					timestamps:  []int{8, 9},
					gcTimestamp: 1,
					expSeekLT:   false,
				},
				{
					key:         "eeeee",
					timestamps:  []int{8, 9},
					gcTimestamp: 1,
					expSeekLT:   false,
				},
			},
		},
		{
			name: "Next to the end on the first version",
			keys: []testCaseKey{
				{
					key:         "h",
					timestamps:  []int{9},
					gcTimestamp: 1,
					expSeekLT:   false,
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			engine := NewDefaultInMemForTesting()
			defer engine.Close()
			runTestCase(t, tc, engine)
			// Compact the engine; the ForTesting() config option will assert
			// that all DELSIZED tombstones were appropriately sized.
			require.NoError(t, engine.Compact())
		})
	}
}

type rangeTestDataItem struct {
	point          MVCCKeyValue
	txn            *roachpb.Transaction
	rangeTombstone MVCCRangeKey
}

type rangeTestData []rangeTestDataItem

func (d rangeTestData) populateEngine(
	t *testing.T, engine ReadWriter, ms *enginepb.MVCCStats,
) hlc.Timestamp {
	ctx := context.Background()
	var ts hlc.Timestamp
	for _, v := range d {
		if v.rangeTombstone.Timestamp.IsEmpty() {
			if v.point.Value != nil {
				_, err := MVCCPut(ctx, engine, v.point.Key.Key, v.point.Key.Timestamp,
					roachpb.MakeValueFromBytes(v.point.Value), MVCCWriteOptions{Txn: v.txn, Stats: ms})
				require.NoError(t, err,
					"failed to insert test value into engine (%s)", v.point.Key)
			} else {
				_, _, err := MVCCDelete(ctx, engine, v.point.Key.Key, v.point.Key.Timestamp, MVCCWriteOptions{Txn: v.txn, Stats: ms})
				require.NoError(t, err, "failed to insert tombstone value into engine (%s)", v.point.Key.String())
			}
			ts = v.point.Key.Timestamp
		} else {
			rw := mvccRangeKeyEncodedTimestampReadWriter{engine}
			require.NoError(t, MVCCDeleteRangeUsingTombstone(ctx, rw, ms, v.rangeTombstone.StartKey,
				v.rangeTombstone.EndKey, v.rangeTombstone.Timestamp, hlc.ClockTimestamp{}, nil, nil, false, 0, 0, nil),
				"failed to insert range tombstone into engine (%s)", v.rangeTombstone.String())
			ts = v.rangeTombstone.Timestamp
		}
	}
	return ts
}

// mvccRangeKeyEncodedTimestampReadWriter wraps a ReadWriter, overriding
// PutMVCCRangeKey so that if MVCCRangeKey.EncodedTimestampSuffix is non-empty,
// the resulting range key uses the encoded suffix verbatim. This is a bit of a
// subtle and convoluted test construction, but it allows testing of range keys
// with the synthetic bit without needing to bring back knowledge of the
// synthetic bit to the various MVCC key encoding and decoding routines.
//
// Note that all production ReadWriter implementations only use
// EncodedTimestampSuffix in their ClearMVCCRangeKey implementations, not in
// their PutMVCCRangeKey implementations.
//
// TODO(jackson): Remove this when we've guaranteed that all range keys have
// timestamps without the synthetic bit.
type mvccRangeKeyEncodedTimestampReadWriter struct {
	ReadWriter
}

func (w mvccRangeKeyEncodedTimestampReadWriter) PutMVCCRangeKey(
	rk MVCCRangeKey, v MVCCValue,
) error {
	if len(rk.EncodedTimestampSuffix) == 0 {
		return w.ReadWriter.PutMVCCRangeKey(rk, v)
	}
	valueRaw, err := EncodeMVCCValue(v)
	if err != nil {
		return errors.Wrapf(err, "failed to encode MVCC value for range key %s", rk)
	} else if err := rk.Validate(); err != nil {
		return err
	}
	return w.ReadWriter.PutEngineRangeKey(
		rk.StartKey, rk.EndKey, rk.EncodedTimestampSuffix, valueRaw)
}

// pt creates a point update for key with default value.
func pt(key roachpb.Key, ts hlc.Timestamp) rangeTestDataItem {
	val := roachpb.MakeValueFromString("testval").RawBytes
	return rangeTestDataItem{point: MVCCKeyValue{Key: mvccVersionKey(key, ts), Value: val}}
}

// inlineValue constant is used with pt function for readability of created inline
// values.
var inlineValue hlc.Timestamp

// tb creates a point tombstone.
func tb(key roachpb.Key, ts hlc.Timestamp) rangeTestDataItem {
	return rangeTestDataItem{point: MVCCKeyValue{Key: mvccVersionKey(key, ts)}}
}

// txn wraps point update and adds transaction to it for intent creation.
func txn(d rangeTestDataItem) rangeTestDataItem {
	ts := d.point.Key.Timestamp
	d.txn = &roachpb.Transaction{
		Status:                 roachpb.PENDING,
		ReadTimestamp:          ts,
		GlobalUncertaintyLimit: ts.Next().Next(),
	}
	d.txn.ID = uuid.MakeV4()
	d.txn.WriteTimestamp = ts
	d.txn.Key = roachpb.Key([]byte{0, 1})
	return d
}

// rng creates range tombstone update.
func rng(start, end roachpb.Key, ts hlc.Timestamp) rangeTestDataItem {
	return rangeTestDataItem{rangeTombstone: MVCCRangeKey{StartKey: start, EndKey: end, Timestamp: ts}}
}

func TestMVCCGarbageCollectRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	mkKey := func(k string) roachpb.Key {
		return append(keys.SystemSQLCodec.TablePrefix(42), k...)
	}
	rangeStart := mkKey("")
	rangeEnd := rangeStart.PrefixEnd()

	// Note we use keys of different lengths so that stats accounting errors
	// would not obviously cancel out if right and left bounds are used
	// incorrectly.
	keyA := mkKey("a")
	keyB := mkKey("bb")
	keyC := mkKey("ccc")
	keyD := mkKey("dddd")
	keyE := mkKey("eeeee")
	keyF := mkKey("ffffff")

	mkTs := func(wallTimeSec int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: time.Second.Nanoseconds() * wallTimeSec}
	}

	ts1 := mkTs(1)
	ts2 := mkTs(2)
	ts3 := mkTs(3)
	ts4 := mkTs(4)
	tsMax := mkTs(9)

	testData := []struct {
		name string
		// Note that range test data should be in ascending order (valid writes).
		before  rangeTestData
		request []kvpb.GCRequest_GCRangeKey
		// Note that expectations should be in timestamp descending order
		// (forward iteration).
		after []MVCCRangeKey
		// Optional start and end range for tests that want to restrict default
		// key range.
		rangeStart roachpb.Key
		rangeEnd   roachpb.Key
	}{
		{
			name: "signle range",
			before: rangeTestData{
				rng(keyA, keyD, ts3),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyA, EndKey: keyD, Timestamp: ts3},
			},
			after: []MVCCRangeKey{},
		},
		{
			name: "multiple contiguous fragments",
			before: rangeTestData{
				rng(keyA, keyD, ts2),
				rng(keyB, keyC, ts4),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyA, EndKey: keyD, Timestamp: ts2},
			},
			after: []MVCCRangeKey{
				{StartKey: keyB, EndKey: keyC, Timestamp: ts4, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts4)},
			},
		},
		{
			name: "multiple non-contiguous fragments",
			before: rangeTestData{
				rng(keyA, keyB, ts2),
				rng(keyC, keyD, ts2),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyA, EndKey: keyD, Timestamp: ts2},
			},
			after: []MVCCRangeKey{},
		},
		{
			name: "multiple non-overlapping fragments",
			before: rangeTestData{
				rng(keyA, keyB, ts2),
				rng(keyC, keyD, ts2),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyB, EndKey: keyC, Timestamp: ts2},
			},
			after: []MVCCRangeKey{
				{StartKey: keyA, EndKey: keyB, Timestamp: ts2, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts2)},
				{StartKey: keyC, EndKey: keyD, Timestamp: ts2, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts2)},
			},
		},
		{
			name: "overlapping [A--[B--B]--A]",
			before: rangeTestData{
				rng(keyB, keyC, ts2),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyA, EndKey: keyD, Timestamp: ts2},
			},
			after: []MVCCRangeKey{},
		},
		{
			name: "overlapping [A--[B--A]--B]",
			before: rangeTestData{
				rng(keyB, keyD, ts2),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyA, EndKey: keyC, Timestamp: ts2},
			},
			after: []MVCCRangeKey{
				{StartKey: keyC, EndKey: keyD, Timestamp: ts2, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts2)},
			},
		},
		{
			name: "overlapping [B--[A--B]--A]",
			before: rangeTestData{
				rng(keyA, keyC, ts2),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyB, EndKey: keyD, Timestamp: ts2},
			},
			after: []MVCCRangeKey{
				{StartKey: keyA, EndKey: keyB, Timestamp: ts2, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts2)},
			},
		},
		{
			name: "overlapping [B--[A--A]--B]",
			before: rangeTestData{
				rng(keyA, keyD, ts2),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyB, EndKey: keyC, Timestamp: ts2},
			},
			after: []MVCCRangeKey{
				{StartKey: keyA, EndKey: keyB, Timestamp: ts2, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts2)},
				{StartKey: keyC, EndKey: keyD, Timestamp: ts2, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts2)},
			},
		},
		{
			name: "overlapping [[AB--A]--B]",
			before: rangeTestData{
				rng(keyA, keyD, ts2),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyA, EndKey: keyB, Timestamp: ts2},
			},
			after: []MVCCRangeKey{
				{StartKey: keyB, EndKey: keyD, Timestamp: ts2, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts2)},
			},
		},
		{
			name: "overlapping [[AB--B]--A]",
			before: rangeTestData{
				rng(keyA, keyB, ts2),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyA, EndKey: keyD, Timestamp: ts2},
			},
			after: []MVCCRangeKey{},
		},
		{
			name: "overlapping [B--[A--AB]]",
			before: rangeTestData{
				rng(keyA, keyD, ts2),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyB, EndKey: keyD, Timestamp: ts2},
			},
			after: []MVCCRangeKey{
				{StartKey: keyA, EndKey: keyB, Timestamp: ts2, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts2)},
			},
		},
		{
			name: "overlapping [A--[B--AB]]",
			before: rangeTestData{
				rng(keyB, keyD, ts2),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyA, EndKey: keyD, Timestamp: ts2},
			},
			after: []MVCCRangeKey{},
		},
		{
			name: "overlapping [B--[A--AB]] point before",
			before: rangeTestData{
				rng(keyB, keyD, ts2),
				pt(keyA, ts4),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyC, EndKey: keyD, Timestamp: ts2},
			},
			after: []MVCCRangeKey{
				{StartKey: keyB, EndKey: keyC, Timestamp: ts2, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts2)},
			},
		},
		{
			name: "overlapping [B--[A--AB]] point at range start",
			before: rangeTestData{
				rng(keyA, keyD, ts2),
				pt(keyA, ts4),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyB, EndKey: keyD, Timestamp: ts2},
			},
			after: []MVCCRangeKey{
				{StartKey: keyA, EndKey: keyB, Timestamp: ts2, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts2)},
			},
		},
		{
			name: "overlapping [B--[A--AB]] point between",
			before: rangeTestData{
				rng(keyA, keyD, ts2),
				pt(keyB, ts4),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyC, EndKey: keyD, Timestamp: ts2},
			},
			after: []MVCCRangeKey{
				{StartKey: keyA, EndKey: keyC, Timestamp: ts2, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts2)},
			},
		},
		{
			name: "overlapping [B--[A--AB]] point at gc start",
			before: rangeTestData{
				rng(keyA, keyD, ts2),
				pt(keyB, ts4),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyB, EndKey: keyD, Timestamp: ts2},
			},
			after: []MVCCRangeKey{
				{StartKey: keyA, EndKey: keyB, Timestamp: ts2, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts2)},
			},
		},
		{
			name: "overlapping [A--[B--AB]] point before",
			before: rangeTestData{
				rng(keyC, keyD, ts2),
				pt(keyA, ts4),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyB, EndKey: keyD, Timestamp: ts2},
			},
			after: []MVCCRangeKey{},
		},
		{
			name: "overlapping [A--[B--AB]] point at gc start",
			before: rangeTestData{
				rng(keyB, keyD, ts2),
				pt(keyA, ts4),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyA, EndKey: keyD, Timestamp: ts2},
			},
			after: []MVCCRangeKey{},
		},
		{
			name: "overlapping [A--[B--AB]] point between",
			before: rangeTestData{
				rng(keyC, keyD, ts2),
				pt(keyB, ts4),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyA, EndKey: keyD, Timestamp: ts2},
			},
			after: []MVCCRangeKey{},
		},
		{
			name: "overlapping [A--[B--AB]] point at range start",
			before: rangeTestData{
				rng(keyB, keyD, ts2),
				pt(keyB, ts4),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyA, EndKey: keyD, Timestamp: ts2},
			},
			after: []MVCCRangeKey{},
		},
		{
			name: "range under intent",
			before: rangeTestData{
				rng(keyA, keyD, ts2),
				txn(pt(keyA, ts4)),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyA, EndKey: keyD, Timestamp: ts2},
			},
			after: []MVCCRangeKey{},
		},
		{
			name: "stacked range fragments",
			before: rangeTestData{
				rng(keyB, keyC, ts2),
				rng(keyA, keyD, ts4),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyA, EndKey: keyD, Timestamp: ts4},
			},
			after: []MVCCRangeKey{},
		},
		{
			name: "old value before range",
			before: rangeTestData{
				pt(keyA, ts2),
				rng(keyB, keyC, ts3),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyB, EndKey: keyC, Timestamp: ts3},
			},
			after: []MVCCRangeKey{},
		},
		{
			name: "old value at range end",
			before: rangeTestData{
				pt(keyC, ts2),
				rng(keyB, keyC, ts3),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyB, EndKey: keyC, Timestamp: ts3},
			},
			after: []MVCCRangeKey{},
		},
		{
			name: "range partially overlap gc request",
			before: rangeTestData{
				rng(keyA, keyD, ts1),
				rng(keyA, keyD, ts3),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyB, EndKey: keyC, Timestamp: ts1},
			},
			after: []MVCCRangeKey{
				{StartKey: keyA, EndKey: keyB, Timestamp: ts3, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts3)},
				{StartKey: keyA, EndKey: keyB, Timestamp: ts1, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts1)},
				{StartKey: keyB, EndKey: keyC, Timestamp: ts3, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts3)},
				{StartKey: keyC, EndKey: keyD, Timestamp: ts3, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts3)},
				{StartKey: keyC, EndKey: keyD, Timestamp: ts1, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts1)},
			},
		},
		{
			name: "range merges sides",
			before: rangeTestData{
				rng(keyB, keyC, ts1),
				rng(keyA, keyD, ts3),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyB, EndKey: keyC, Timestamp: ts1},
			},
			after: []MVCCRangeKey{
				{StartKey: keyA, EndKey: keyD, Timestamp: ts3, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts3)},
			},
		},
		{
			name: "range merges next",
			before: rangeTestData{
				rng(keyB, keyC, ts1),
				rng(keyA, keyC, ts3),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyB, EndKey: keyC, Timestamp: ts1},
			},
			after: []MVCCRangeKey{
				{StartKey: keyA, EndKey: keyC, Timestamp: ts3, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts3)},
			},
		},
		{
			name: "range merges previous",
			before: rangeTestData{
				rng(keyA, keyB, ts1),
				rng(keyA, keyD, ts3),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyA, EndKey: keyB, Timestamp: ts1},
			},
			after: []MVCCRangeKey{
				{StartKey: keyA, EndKey: keyD, Timestamp: ts3, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts3)},
			},
		},
		{
			name: "range merges chain",
			before: rangeTestData{
				rng(keyB, keyC, ts1),
				rng(keyD, keyE, ts2),
				rng(keyA, keyF, ts3),
				rng(keyA, keyF, ts4),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyB, EndKey: keyC, Timestamp: ts1},
				{StartKey: keyD, EndKey: keyE, Timestamp: ts2},
			},
			after: []MVCCRangeKey{
				{StartKey: keyA, EndKey: keyF, Timestamp: ts4, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts4)},
				{StartKey: keyA, EndKey: keyF, Timestamp: ts3, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts3)},
			},
		},
		{
			name: "range merges sequential",
			before: rangeTestData{
				rng(keyC, keyD, ts1),
				rng(keyB, keyD, ts2),
				rng(keyA, keyE, ts3),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyB, EndKey: keyC, Timestamp: ts2},
				{StartKey: keyC, EndKey: keyD, Timestamp: ts2},
			},
			after: []MVCCRangeKey{
				{StartKey: keyA, EndKey: keyE, Timestamp: ts3, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts3)},
			},
		},
		{
			name: "don't merge outside range",
			before: rangeTestData{
				rng(keyB, keyC, ts1),
				// Tombstone spanning multiple ranges.
				rng(keyA, keyD, ts4),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyB, EndKey: keyC, Timestamp: ts1},
			},
			after: []MVCCRangeKey{
				// We only iterate data within range, so range keys would be
				// truncated.
				{StartKey: keyB, EndKey: keyC, Timestamp: ts4, EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(ts4)},
			},
			rangeStart: keyB,
			rangeEnd:   keyC,
		},
		{
			name: "synthetic bit range keys",
			before: rangeTestData{
				rangeTestDataItem{rangeTombstone: MVCCRangeKey{
					StartKey:               keyA,
					EndKey:                 keyC,
					Timestamp:              ts4,
					EncodedTimestampSuffix: EncodeMVCCTimestampSuffixWithSyntheticBitForTesting(ts4),
				}},
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyA, EndKey: keyC, Timestamp: ts4},
			},
			after: []MVCCRangeKey{},
		},
	}

	for _, d := range testData {
		t.Run(d.name, func(t *testing.T) {
			engine := NewDefaultInMemForTesting()
			defer engine.Close()

			// Populate range descriptor defaults.
			if len(d.rangeStart) == 0 {
				d.rangeStart = rangeStart
			}
			if len(d.rangeEnd) == 0 {
				d.rangeEnd = rangeEnd
			}

			var ms enginepb.MVCCStats
			d.before.populateEngine(t, engine, &ms)

			rangeKeys := rangesFromRequests(rangeStart, rangeEnd, d.request)
			require.NoError(t, MVCCGarbageCollectRangeKeys(ctx, engine, &ms, rangeKeys),
				"failed to run mvcc range tombstone garbage collect")

			it, err := engine.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{
				KeyTypes:   IterKeyTypeRangesOnly,
				LowerBound: d.rangeStart,
				UpperBound: d.rangeEnd,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer it.Close()
			it.SeekGE(MVCCKey{Key: d.rangeStart})
			expectIndex := 0
			for ; ; it.Next() {
				ok, err := it.Valid()
				require.NoError(t, err, "failed to iterate engine")
				if !ok {
					break
				}
				for _, rk := range it.RangeKeys().AsRangeKeys() {
					require.Less(t, expectIndex, len(d.after), "not enough expectations; at unexpected range: %s", rk)
					require.EqualValues(t, d.after[expectIndex], rk, "range key is not equal")
					expectIndex++
				}
			}
			require.Equal(t, len(d.after), expectIndex,
				"not all range tombstone expectations were consumed")

			ms.AgeTo(tsMax.WallTime)
			expMs, err := ComputeStats(ctx, engine, d.rangeStart, d.rangeEnd, tsMax.WallTime)
			require.NoError(t, err, "failed to compute stats for range")
			require.EqualValues(t, expMs, ms, "computed range stats vs gc'd")

			// Compact the engine; the ForTesting() config option will assert
			// that all DELSIZED tombstones were appropriately sized.
			require.NoError(t, engine.Compact())
		})
	}
}

func rangesFromRequests(
	rangeStart, rangeEnd roachpb.Key, rangeKeys []kvpb.GCRequest_GCRangeKey,
) []CollectableGCRangeKey {
	collectableKeys := make([]CollectableGCRangeKey, len(rangeKeys))
	for i, rk := range rangeKeys {
		leftPeekBound := rk.StartKey.Prevish(roachpb.PrevishKeyLength)
		if leftPeekBound.Compare(rangeStart) <= 0 {
			leftPeekBound = rangeStart
		}
		rightPeekBound := rk.EndKey.Next()
		if rightPeekBound.Compare(rangeEnd) >= 0 {
			rightPeekBound = rangeEnd
		}
		collectableKeys[i] = CollectableGCRangeKey{
			MVCCRangeKey: MVCCRangeKey{
				StartKey:  rk.StartKey,
				EndKey:    rk.EndKey,
				Timestamp: rk.Timestamp,
			},
			LatchSpan: roachpb.Span{Key: leftPeekBound, EndKey: rightPeekBound},
		}
	}
	return collectableKeys
}

func TestMVCCGarbageCollectRangesFailures(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	mkKey := func(k string) roachpb.Key {
		return append(keys.SystemSQLCodec.TablePrefix(42), k...)
	}
	rangeStart := mkKey("")
	rangeEnd := rangeStart.PrefixEnd()

	keyA := mkKey("a")
	keyB := mkKey("b")
	keyC := mkKey("c")
	keyD := mkKey("d")

	mkTs := func(wallTimeSec int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: time.Second.Nanoseconds() * wallTimeSec}
	}

	ts1 := mkTs(1)
	ts2 := mkTs(2)
	ts3 := mkTs(3)
	ts4 := mkTs(4)
	ts5 := mkTs(5)
	ts6 := mkTs(6)
	ts7 := mkTs(7)
	ts8 := mkTs(8)

	testData := []struct {
		name    string
		before  rangeTestData
		request []kvpb.GCRequest_GCRangeKey
		error   string
	}{
		{
			name: "request overlap",
			before: rangeTestData{
				rng(keyA, keyD, ts3),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyA, EndKey: keyC, Timestamp: ts3},
				{StartKey: keyB, EndKey: keyD, Timestamp: ts3},
			},
			error: "range keys in gc request should be non-overlapping",
		},
		{
			name: "delete range above value",
			before: rangeTestData{
				pt(keyB, ts2),
				rng(keyA, keyD, ts3),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyA, EndKey: keyD, Timestamp: ts3},
			},
			error: "attempt to delete range tombstone .* hiding key at .*",
		},
		{
			// Note that this test is a bit contrived as we can't put intent
			// under the range tombstone, but we test that if you try to delete
			// tombstone above intents even if it doesn't exist, we would reject
			// the attempt as it is an indication of inconsistency.
			// This might be relaxed to ignore any points which are not covered.
			name: "delete range above intent",
			before: rangeTestData{
				rng(keyA, keyD, ts2),
				txn(pt(keyB, ts3)),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyA, EndKey: keyD, Timestamp: ts4},
			},
			error: "attempt to delete range tombstone .* hiding key at .*",
		},
		{
			name: "delete range above tail of long history",
			before: rangeTestData{
				pt(keyB, ts1),
				rng(keyA, keyD, ts2),
				pt(keyB, ts3),
				pt(keyB, ts4),
				pt(keyB, ts5),
				pt(keyB, ts6),
				pt(keyB, ts7),
				pt(keyB, ts8),
			},
			request: []kvpb.GCRequest_GCRangeKey{
				{StartKey: keyA, EndKey: keyD, Timestamp: ts2},
			},
			error: "attempt to delete range tombstone .* hiding key at .*",
		},
	}

	ctx := context.Background()
	for _, d := range testData {
		t.Run(d.name, func(t *testing.T) {
			engine := NewDefaultInMemForTesting()
			defer engine.Close()
			d.before.populateEngine(t, engine, nil)
			rangeKeys := rangesFromRequests(rangeStart, rangeEnd, d.request)
			err := MVCCGarbageCollectRangeKeys(ctx, engine, nil, rangeKeys)
			require.Errorf(t, err, "expected error '%s' but found none", d.error)
			require.True(t, testutils.IsError(err, d.error),
				"expected error '%s' found '%s'", d.error, err)
		})
	}
}

// TestMVCCGarbageCollectClearRange checks that basic GCClearRange functionality
// works. Fine grained tests cases are tested in mvcc_histories_test
// 'gc_clear_range'. This test could be used when debugging any issues found.
func TestMVCCGarbageCollectClearRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	mkKey := func(k string) roachpb.Key {
		return append(keys.SystemSQLCodec.TablePrefix(42), k...)
	}
	rangeStart := mkKey("")
	rangeEnd := rangeStart.PrefixEnd()

	// Note we use keys of different lengths so that stats accounting errors
	// would not obviously cancel out if right and left bounds are used
	// incorrectly.
	keyA := mkKey("a")
	keyB := mkKey("bb")
	keyD := mkKey("dddd")

	mkTs := func(wallTimeSec int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: time.Second.Nanoseconds() * wallTimeSec}
	}

	ts2 := mkTs(2)
	ts4 := mkTs(4)
	tsGC := mkTs(5)
	tsMax := mkTs(9)

	mkGCReq := func(start roachpb.Key, end roachpb.Key) kvpb.GCRequest_GCClearRange {
		return kvpb.GCRequest_GCClearRange{
			StartKey: start,
			EndKey:   end,
		}
	}

	before := rangeTestData{
		pt(keyB, ts2),
		rng(keyA, keyD, ts4),
	}
	request := mkGCReq(keyA, keyD)

	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	var ms, diff enginepb.MVCCStats
	before.populateEngine(t, engine, &ms)

	require.NoError(t,
		MVCCGarbageCollectWholeRange(ctx, engine, &diff, request.StartKey, request.EndKey, tsGC, ms),
		"failed to run mvcc range tombstone garbage collect")
	ms.Add(diff)

	rks := scanRangeKeys(t, engine)
	require.Empty(t, rks)
	ks := scanPointKeys(t, engine)
	require.Empty(t, ks)

	ms.AgeTo(tsMax.WallTime)
	it, err := engine.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{
		KeyTypes:   IterKeyTypePointsAndRanges,
		LowerBound: rangeStart,
		UpperBound: rangeEnd,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	expMs, err := ComputeStatsForIter(it, tsMax.WallTime)
	require.NoError(t, err, "failed to compute stats for range")
	require.EqualValues(t, expMs, ms, "computed range stats vs gc'd")
}

func TestMVCCGarbageCollectClearRangeInlinedValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	mkKey := func(k string) roachpb.Key {
		return append(keys.SystemSQLCodec.TablePrefix(42), k...)
	}

	// Note we use keys of different lengths so that stats accounting errors
	// would not obviously cancel out if right and left bounds are used
	// incorrectly.
	keyA := mkKey("a")
	keyB := mkKey("b")
	keyD := mkKey("dddd")

	mkTs := func(wallTimeSec int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: time.Second.Nanoseconds() * wallTimeSec}
	}

	tsGC := mkTs(5)

	mkGCReq := func(start roachpb.Key, end roachpb.Key) kvpb.GCRequest_GCClearRange {
		return kvpb.GCRequest_GCClearRange{
			StartKey: start,
			EndKey:   end,
		}
	}

	before := rangeTestData{
		pt(keyB, inlineValue),
	}
	request := mkGCReq(keyA, keyD)
	expectedError := `found key not covered by range tombstone /Table/42/"b"/0,0`
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	var ms, diff enginepb.MVCCStats
	before.populateEngine(t, engine, &ms)
	// We are forcing stats to be estimates to bypass quick liveness check
	// that will prevent actual data checks if there's some live data.
	ms.ContainsEstimates = 1
	err := MVCCGarbageCollectWholeRange(ctx, engine, &diff, request.StartKey, request.EndKey,
		tsGC, ms)
	ms.Add(diff)
	require.Errorf(t, err, "expected error '%s' but found none", expectedError)
	require.True(t, testutils.IsError(err, expectedError),
		"expected error '%s' found '%s'", expectedError, err)
	// Compact the engine; the ForTesting() config option will assert that all
	// DELSIZED tombstones were appropriately sized.
	require.NoError(t, engine.Compact())
}

func TestMVCCGarbageCollectClearPointsInRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	mkKey := func(k string) roachpb.Key {
		return append(keys.SystemSQLCodec.TablePrefix(42), k...)
	}
	rangeStart := mkKey("")
	rangeEnd := rangeStart.PrefixEnd()

	// Note we use keys of different lengths so that stats accounting errors
	// would not obviously cancel out if right and left bounds are used
	// incorrectly.
	keyA := mkKey("a")
	keyB := mkKey("bb")
	keyC := mkKey("ccc")
	keyD := mkKey("dddd")

	mkTs := func(wallTimeSec int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: time.Second.Nanoseconds() * wallTimeSec}
	}

	ts1 := mkTs(1)
	ts2 := mkTs(2)
	ts3 := mkTs(3)
	ts4 := mkTs(4)
	tsMax := mkTs(9)

	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	var ms enginepb.MVCCStats
	rangeTestData{
		pt(keyB, ts1),
		pt(keyB, ts2),
		tb(keyB, ts3),
		tb(keyB, ts4),
		pt(keyC, ts1),
		tb(keyC, ts2),
		pt(keyC, ts3),
		tb(keyC, ts4),
	}.populateEngine(t, engine, &ms)

	require.NoError(t,
		MVCCGarbageCollectPointsWithClearRange(ctx, engine, &ms, keyA, keyD, hlc.Timestamp{}, tsMax),
		"failed to run mvcc range tombstone garbage collect")

	expected := NewDefaultInMemForTesting()
	defer expected.Close()
	var expMs enginepb.MVCCStats

	rks := scanRangeKeys(t, engine)
	expRks := scanRangeKeys(t, expected)
	require.EqualValues(t, expRks, rks)

	ks := scanPointKeys(t, engine)
	expKs := scanPointKeys(t, expected)
	require.EqualValues(t, expKs, ks)

	ms.AgeTo(tsMax.WallTime)
	expMs, err := ComputeStats(ctx, engine, rangeStart, rangeEnd, tsMax.WallTime)
	require.NoError(t, err, "failed to compute stats for range")
	require.EqualValues(t, expMs, ms, "computed range stats vs gc'd")

	// Compact the engine; the ForTesting() config option will assert that all
	// DELSIZED tombstones were appropriately sized.
	require.NoError(t, engine.Compact())
}

func TestMVCCGarbageCollectClearRangeFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	mkKey := func(k string) roachpb.Key {
		return append(keys.SystemSQLCodec.TablePrefix(42), k...)
	}

	// Note we use keys of different lengths so that stats accounting errors
	// would not obviously cancel out if right and left bounds are used
	// incorrectly.
	keyA := mkKey("a")
	keyD := mkKey("dddd")

	mkTs := func(wallTimeSec int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: time.Second.Nanoseconds() * wallTimeSec}
	}

	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	var ms enginepb.MVCCStats
	rangeTestData{
		pt(keyA, mkTs(1)),
	}.populateEngine(t, engine, &ms)

	err := MVCCGarbageCollectPointsWithClearRange(ctx, engine, &ms, keyA, keyD, mkTs(1), mkTs(5))
	errMsg := `attempt to GC data /Table/42/"a"/1.000000000,0 still visible at GC threshold 5.000000000,0 with clear range`
	require.Errorf(t, err, "expected error '%s' but found none", errMsg)
	require.True(t, testutils.IsError(err, errMsg),
		"expected error '%s' found '%s'", errMsg, err)
}

// TestResolveIntentWithLowerEpoch verifies that trying to resolve
// an intent at an epoch that is lower than the epoch of the intent
// leaves the intent untouched.
func TestResolveIntentWithLowerEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	// Lay down an intent with a high epoch.
	if _, err := MVCCPut(ctx, engine, testKey1, txn1e2.ReadTimestamp, value1, MVCCWriteOptions{Txn: txn1e2}); err != nil {
		t.Fatal(err)
	}
	// Resolve the intent with a low epoch.
	if _, _, _, _, err := MVCCResolveWriteIntent(ctx, engine, nil,
		roachpb.MakeLockUpdate(txn1, roachpb.Span{Key: testKey1}),
		MVCCResolveWriteIntentOptions{}); err != nil {
		t.Fatal(err)
	}

	// Check that the intent was not cleared.
	intentRes, err := MVCCGet(ctx, engine, testKey1, hlc.MaxTimestamp, MVCCGetOptions{
		Inconsistent: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if intentRes.Intent == nil {
		t.Fatal("intent should not be cleared by resolve intent request with lower epoch")
	}
}

// TestTimeSeriesMVCCStats ensures that merge operations
// result in an expected increase in timeseries data.
func TestTimeSeriesMVCCStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()
	var ms = enginepb.MVCCStats{}

	// Perform a sequence of merges on the same key
	// and record the MVCC stats for it.
	if err := MVCCMerge(ctx, engine, &ms, testKey1, hlc.Timestamp{Logical: 1}, tsvalue1); err != nil {
		t.Fatal(err)
	}
	firstMS := ms

	if err := MVCCMerge(ctx, engine, &ms, testKey1, hlc.Timestamp{Logical: 1}, tsvalue1); err != nil {
		t.Fatal(err)
	}
	secondMS := ms

	// Ensure timeseries metrics increase as expected.
	expectedMS := firstMS
	expectedMS.LiveBytes += int64(len(tsvalue1.RawBytes))
	expectedMS.ValBytes += int64(len(tsvalue1.RawBytes))

	if secondMS.LiveBytes != expectedMS.LiveBytes {
		t.Fatalf("second merged LiveBytes value %v differed from expected LiveBytes value %v", secondMS.LiveBytes, expectedMS.LiveBytes)
	}
	if secondMS.ValBytes != expectedMS.ValBytes {
		t.Fatalf("second merged ValBytes value %v differed from expected ValBytes value %v", secondMS.LiveBytes, expectedMS.LiveBytes)
	}
}

// TestMVCCTimeSeriesPartialMerge ensures that "partial merges" of merged time
// series data does not result in a different final result than a "full merge".
func TestMVCCTimeSeriesPartialMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	// Perform the same sequence of merges on two different keys. For
	// one of them, insert some compactions which cause partial merges
	// to be run and affect the results.
	vals := make([]*roachpb.Value, 2)

	for i, k := range []roachpb.Key{testKey1, testKey2} {
		if err := MVCCMerge(ctx, engine, nil, k, hlc.Timestamp{Logical: 1}, tsvalue1); err != nil {
			t.Fatal(err)
		}
		if err := MVCCMerge(ctx, engine, nil, k, hlc.Timestamp{Logical: 2}, tsvalue2); err != nil {
			t.Fatal(err)
		}

		if i == 1 {
			if err := engine.Compact(); err != nil {
				t.Fatal(err)
			}
		}

		if err := MVCCMerge(ctx, engine, nil, k, hlc.Timestamp{Logical: 2}, tsvalue2); err != nil {
			t.Fatal(err)
		}
		if err := MVCCMerge(ctx, engine, nil, k, hlc.Timestamp{Logical: 1}, tsvalue1); err != nil {
			t.Fatal(err)
		}

		if i == 1 {
			if err := engine.Compact(); err != nil {
				t.Fatal(err)
			}
		}

		if valueRes, err := MVCCGet(ctx, engine, k, hlc.Timestamp{}, MVCCGetOptions{}); err != nil {
			t.Fatal(err)
		} else {
			vals[i] = valueRes.Value
		}
	}

	if first, second := vals[0], vals[1]; !reflect.DeepEqual(first, second) {
		var firstTS, secondTS roachpb.InternalTimeSeriesData
		if err := first.GetProto(&firstTS); err != nil {
			t.Fatal(err)
		}
		if err := second.GetProto(&secondTS); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("partially merged value %v differed from expected merged value %v", secondTS, firstTS)
	}
}

func TestWillOverflow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		a, b     int64
		overflow bool // will a+b over- or underflow?
	}{
		{0, 0, false},
		{math.MaxInt64, 0, false},
		{math.MaxInt64, 1, true},
		{math.MaxInt64, math.MinInt64, false},
		{math.MinInt64, 0, false},
		{math.MinInt64, -1, true},
		{math.MinInt64, math.MinInt64, true},
	}

	for i, c := range testCases {
		if willOverflow(c.a, c.b) != c.overflow ||
			willOverflow(c.b, c.a) != c.overflow {
			t.Errorf("%d: overflow recognition error", i)
		}
	}
}

// TestMVCCExportToSSTExhaustedAtStart is a regression test for a bug
// in which mis-handling of resume spans would cause MVCCExportToSST
// to return an empty resume key in cases where the resource limiters
// caused an early return of a resume span.
//
// NB: That this test treats the result of MVCCExportToSST _without_
// CPU rate limiting as the truth. Bugs that affect all exports will
// not be caught by this test.
func TestMVCCExportToSSTExhaustedAtStart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	st := cluster.MakeTestingClusterSettings()

	var (
		minKey       = int64(0)
		maxKey       = int64(1000)
		minTimestamp = hlc.Timestamp{WallTime: 100000}
		maxTimestamp = hlc.Timestamp{WallTime: 200000}

		exportAllQuery = queryLimits{
			minKey:       minKey,
			maxKey:       maxKey,
			minTimestamp: minTimestamp,
			maxTimestamp: maxTimestamp,
			latest:       false,
		}
	)

	// When ExportRequest is interrupted by the CPU limiter, the currently
	// buffered range key stack will have its EndKey truncated to the resume
	// key. To account for this, we write all of the range keys back into a
	// store and then export them out again without interruption.
	canonicalizeRangeKeys := func(in []MVCCRangeKeyStack) []MVCCRangeKeyStack {
		if len(in) == 0 {
			return in
		}

		engine := createTestPebbleEngine()
		defer engine.Close()
		for _, keyStack := range in {
			for _, version := range keyStack.Versions {
				require.NoError(t, engine.PutRawMVCCRangeKey(keyStack.AsRangeKey(version), []byte{}))
			}
		}
		require.NoError(t, engine.Flush())
		keys, rKeys := exportAllData(t, engine, exportAllQuery)
		require.Equal(t, 0, len(keys))
		return rKeys
	}

	assertExportEqualWithOptions := func(t *testing.T, ctx context.Context, engine Engine,
		expectedKeys []MVCCKey,
		expectedRangeKeys []MVCCRangeKeyStack,
		initialOpts MVCCExportOptions) {

		keysIndex := 0
		rKeysBuf := []MVCCRangeKeyStack{}

		startKey := initialOpts.StartKey
		for len(startKey.Key) > 0 {
			var sstFile bytes.Buffer
			opts := initialOpts
			opts.StartKey = startKey
			_, resumeInfo, err := MVCCExportToSST(ctx, st, engine, opts, &sstFile)
			require.NoError(t, err)

			keys, rangeKeys := sstToKeys(t, sstFile.Bytes())

			require.LessOrEqual(t, len(keys), len(expectedKeys)-keysIndex, "remaining test key data")

			for _, key := range keys {
				require.True(t, key.Equal(expectedKeys[keysIndex]), "returned key is not equal")
				keysIndex++
			}
			rKeysBuf = append(rKeysBuf, rangeKeys...)
			startKey = resumeInfo.ResumeKey
		}
		require.Equal(t, len(expectedKeys), keysIndex, "not all expected keys were consumed")

		actualRangeKeys := canonicalizeRangeKeys(rKeysBuf)
		require.Equal(t, len(expectedRangeKeys), len(actualRangeKeys))
		for i, actual := range actualRangeKeys {
			expected := expectedRangeKeys[i]
			require.True(t, actual.Equal(expected), "range key mismatch %v != %v", actual, expected)
		}

	}
	t.Run("elastic CPU limit exhausted",
		func(t *testing.T) {
			engine := createTestPebbleEngine()
			defer engine.Close()

			limits := dataLimits{
				minKey:          minKey,
				maxKey:          maxKey,
				minTimestamp:    minTimestamp,
				maxTimestamp:    maxTimestamp,
				tombstoneChance: 0.01,
			}
			generateData(t, engine, limits, (limits.maxKey-limits.minKey)*10)
			keys, rKeys := exportAllData(t, engine, exportAllQuery)

			// Our ElasticCPUWorkHandle will fail on the very first call. As a result,
			// the very first return from MVCCExportToSST will actually contain no
			// data but _should_ return a resume key.
			firstCall := true
			ctx := admission.ContextWithElasticCPUWorkHandle(context.Background(), admission.TestingNewElasticCPUHandleWithCallback(func() (bool, time.Duration) {
				if firstCall {
					firstCall = false
					return true, 0
				}
				return false, 0
			}))
			assertExportEqualWithOptions(t, ctx, engine, keys, rKeys, MVCCExportOptions{
				StartKey:           MVCCKey{Key: testKey(limits.minKey), Timestamp: limits.minTimestamp},
				EndKey:             testKey(limits.maxKey),
				StartTS:            limits.minTimestamp,
				EndTS:              limits.maxTimestamp,
				ExportAllRevisions: true,
			})
			require.False(t, firstCall)
		})
	t.Run("elastic CPU limit always exhausted",
		func(t *testing.T) {
			engine := createTestPebbleEngine()
			defer engine.Close()

			limits := dataLimits{
				minKey:          minKey,
				maxKey:          maxKey,
				minTimestamp:    minTimestamp,
				maxTimestamp:    maxTimestamp,
				tombstoneChance: 0.01,
			}
			generateData(t, engine, limits, (limits.maxKey-limits.minKey)*10)
			keys, rKeys := exportAllData(t, engine, exportAllQuery)

			// Our ElasticCPUWorkHandle will always fail. But, we
			// should still make progress, one key at a time.
			ctx := admission.ContextWithElasticCPUWorkHandle(context.Background(), admission.TestingNewElasticCPUHandleWithCallback(func() (bool, time.Duration) {
				return true, 0
			}))
			assertExportEqualWithOptions(t, ctx, engine, keys, rKeys, MVCCExportOptions{
				StartKey:           MVCCKey{Key: testKey(limits.minKey), Timestamp: limits.minTimestamp},
				EndKey:             testKey(limits.maxKey),
				StartTS:            limits.minTimestamp,
				EndTS:              limits.maxTimestamp,
				ExportAllRevisions: true,
			})
		})
	t.Run("elastic CPU limit always exhausted with range keys",
		func(t *testing.T) {
			engine := createTestPebbleEngine()
			defer engine.Close()
			limits := dataLimits{
				minKey:             minKey,
				maxKey:             maxKey,
				minTimestamp:       minTimestamp,
				maxTimestamp:       maxTimestamp,
				tombstoneChance:    0.50,
				useRangeTombstones: true,
			}
			// Adding many range keys makes this test much slower,
			// so we use 2*keyRange rather than 10*keyRange here.
			generateData(t, engine, limits, (limits.maxKey-limits.minKey)*2)
			keys, rKeys := exportAllData(t, engine, exportAllQuery)

			// Our ElasticCPUWorkHandle will always fail. But, we
			// should still make progress, one key at a time.
			ctx := admission.ContextWithElasticCPUWorkHandle(context.Background(), admission.TestingNewElasticCPUHandleWithCallback(func() (bool, time.Duration) {
				return true, 0
			}))
			assertExportEqualWithOptions(t, ctx, engine, keys, rKeys, MVCCExportOptions{
				StartKey:           MVCCKey{Key: testKey(limits.minKey), Timestamp: limits.minTimestamp},
				EndKey:             testKey(limits.maxKey),
				StartTS:            limits.minTimestamp,
				EndTS:              limits.maxTimestamp,
				ExportAllRevisions: true,
				StopMidKey:         true,
			})
		})
	t.Run("elastic CPU limit exhausted respects StopMidKey",
		func(t *testing.T) {
			engine := createTestPebbleEngine()
			defer engine.Close()

			// Construct a data set that contains 6
			// revisions of the same key.
			//
			// We expect that MVCCExportToSST with
			// ExportAllRevisions set to true but with
			// StopMidKey set to false to always return
			// all or none of this key.
			revisionCount := 6
			rng := rand.New(rand.NewSource(timeutil.Now().Unix()))
			key := testKey(6)
			start := minTimestamp.Add(1, 0)
			nextKey := func(i int64) MVCCKey { return MVCCKey{Key: key, Timestamp: start.Add(i, 0)} }
			nextValue := func() MVCCValue { return MVCCValue{Value: roachpb.MakeValueFromBytes(randutil.RandBytes(rng, 256))} }
			for i := 0; i < revisionCount; i++ {
				require.NoError(t, engine.PutMVCC(nextKey(int64(i)), nextValue()), "write data to test storage")
			}
			require.NoError(t, engine.Flush(), "Flush engine data")

			var sstFile bytes.Buffer
			opts := MVCCExportOptions{
				StartKey:           MVCCKey{Key: testKey(minKey), Timestamp: minTimestamp},
				EndKey:             testKey(maxKey),
				StartTS:            minTimestamp,
				EndTS:              maxTimestamp,
				ExportAllRevisions: true,
				StopMidKey:         false,
			}

			// Create an ElasticCPUWorkHandler that will
			// simulate a resource constraint failure
			// after some number of loop iterations.
			ctx := context.Background()
			callsBeforeFailure := 2
			ctx = admission.ContextWithElasticCPUWorkHandle(ctx, admission.TestingNewElasticCPUHandleWithCallback(func() (bool, time.Duration) {
				if callsBeforeFailure > 0 {
					callsBeforeFailure--
					return false, 0
				}
				return true, 0
			}))

			// With StopMidKey=false, we expect 6
			// revisions or 0 revisions.
			_, _, err := MVCCExportToSST(ctx, st, engine, opts, &sstFile)
			require.NoError(t, err)
			chunk, _ := sstToKeys(t, sstFile.Bytes())
			require.Equal(t, 6, len(chunk))

			// With StopMidKey=true, we can stop in the
			// middle of iteration.
			callsBeforeFailure = 2
			sstFile.Reset()
			opts.StopMidKey = true
			_, _, err = MVCCExportToSST(ctx, st, engine, opts, &sstFile)
			require.NoError(t, err)
			chunk, _ = sstToKeys(t, sstFile.Bytes())
			// We expect 3 here rather than 2 because the
			// first iteration never calls the handler.
			require.Equal(t, 3, len(chunk))

		})
}

type queryLimits struct {
	minKey       int64
	maxKey       int64
	minTimestamp hlc.Timestamp
	maxTimestamp hlc.Timestamp
	latest       bool
}

func testKey(id int64) roachpb.Key {
	return []byte(fmt.Sprintf("key-%08d", id))
}

type dataLimits struct {
	minKey             int64
	maxKey             int64
	minTimestamp       hlc.Timestamp
	maxTimestamp       hlc.Timestamp
	tombstoneChance    float64
	useRangeTombstones bool
}

func exportAllData(
	t *testing.T, engine Engine, limits queryLimits,
) ([]MVCCKey, []MVCCRangeKeyStack) {
	st := cluster.MakeTestingClusterSettings()
	var sstFile bytes.Buffer
	_, _, err := MVCCExportToSST(context.Background(), st, engine, MVCCExportOptions{
		StartKey:           MVCCKey{Key: testKey(limits.minKey), Timestamp: limits.minTimestamp},
		EndKey:             testKey(limits.maxKey),
		StartTS:            limits.minTimestamp,
		EndTS:              limits.maxTimestamp,
		ExportAllRevisions: !limits.latest,
	}, &sstFile)
	require.NoError(t, err, "Failed to export expected data")
	return sstToKeys(t, sstFile.Bytes())
}

func sstToKeys(t *testing.T, data []byte) ([]MVCCKey, []MVCCRangeKeyStack) {
	var results []MVCCKey
	var rangeKeyRes []MVCCRangeKeyStack
	it, err := NewMemSSTIterator(data, false, IterOptions{
		KeyTypes:   pebble.IterKeyTypePointsAndRanges,
		LowerBound: keys.MinKey,
		UpperBound: keys.MaxKey,
	})
	require.NoError(t, err, "Failed to read exported data")
	defer it.Close()
	for it.SeekGE(MVCCKey{Key: []byte{}}); ; {
		ok, err := it.Valid()
		require.NoError(t, err, "Failed to advance iterator while preparing data")
		if !ok {
			break
		}

		if it.RangeKeyChanged() {
			hasPoint, hasRange := it.HasPointAndRange()
			if hasRange {
				rangeKeyRes = append(rangeKeyRes, it.RangeKeys().Clone())
			}
			if !hasPoint {
				it.Next()
				continue
			}
		}

		results = append(results, MVCCKey{
			Key:       append(roachpb.Key(nil), it.UnsafeKey().Key...),
			Timestamp: it.UnsafeKey().Timestamp,
		})
		it.Next()
	}
	return results, rangeKeyRes
}

func generateData(t *testing.T, engine Engine, limits dataLimits, totalEntries int64) {
	rng := rand.New(rand.NewSource(timeutil.Now().Unix()))
	for i := int64(0); i < totalEntries; i++ {
		keyID := limits.minKey + rand.Int63n(limits.maxKey-limits.minKey)
		key := testKey(keyID)
		timestamp := limits.minTimestamp.Add(rand.Int63n(limits.maxTimestamp.WallTime-limits.minTimestamp.WallTime), 0)
		size := 256
		if rng.Float64() < limits.tombstoneChance {
			size = 0
		}

		if limits.useRangeTombstones && size == 0 {
			require.NoError(t, engine.PutRawMVCCRangeKey(MVCCRangeKey{
				StartKey:  key,
				EndKey:    testKey(keyID + 2),
				Timestamp: timestamp}, []byte{}), "write data to test storage")
		} else {
			value := MVCCValue{Value: roachpb.MakeValueFromBytes(randutil.RandBytes(rng, size))}
			require.NoError(t, engine.PutMVCC(MVCCKey{Key: key, Timestamp: timestamp}, value), "Write data to test storage")
		}
	}
	require.NoError(t, engine.Flush(), "Flush engine data")
}

func TestMVCCExportToSSTFailureIntentBatching(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test function uses a fixed time and key range to produce SST.
	// Use varying inserted keys for values and intents to putting them in and out of ranges.
	checkReportedErrors := func(data []testValue, expectedIntentIndices []int, targetBytes uint64) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			st := cluster.MakeTestingClusterSettings()

			engine := createTestPebbleEngine()
			defer engine.Close()

			require.NoError(t, fillInData(ctx, engine, data))
			ss := kvpb.ScanStats{}
			_, _, err := MVCCExportToSST(ctx, st, engine, MVCCExportOptions{
				StartKey:                MVCCKey{Key: key(10)},
				EndKey:                  key(20000),
				StartTS:                 ts(999),
				EndTS:                   ts(2000),
				ExportAllRevisions:      true,
				TargetSize:              0,
				MaxSize:                 0,
				MaxLockConflicts:        uint64(MaxConflictsPerLockConflictError.Default()),
				TargetLockConflictBytes: targetBytes,
				StopMidKey:              false,
				ScanStats:               &ss,
			}, &bytes.Buffer{})
			if len(expectedIntentIndices) == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				e := (*kvpb.LockConflictError)(nil)
				if !errors.As(err, &e) {
					require.Fail(t, "Expected LockConflictError, got %T", err)
				}
				require.Equal(t, len(expectedIntentIndices), len(e.Locks))
				for i, dataIdx := range expectedIntentIndices {
					require.Equal(t, data[dataIdx].txn.ID, e.Locks[i].Txn.ID)
				}
			}
			// Check some stats to ensure they are being populated.
			require.Less(t, uint64(MaxConflictsPerLockConflictError.Default()), ss.NumInterfaceSteps)
			require.Equal(t, uint64(1), ss.NumScans)
		}
	}

	// Export range is fixed to k:["00010", "10000"), ts:(999, 2000] for all tests.
	testDataCount := int(MaxConflictsPerLockConflictError.Default() + 1)
	testData := make([]testValue, testDataCount*2)
	expectedErrors := make([]int, testDataCount)
	for i := 0; i < testDataCount; i++ {
		testData[i*2] = value(key(i*2+11), "value", ts(1000))
		testData[i*2+1] = intent(key(i*2+12), "intent", ts(1001))
		expectedErrors[i] = i*2 + 1
	}
	t.Run("Receive no more than limit intents", checkReportedErrors(testData, expectedErrors[:MaxConflictsPerLockConflictError.Default()], 0))
	t.Run("Byte target checking", checkReportedErrors(testData, expectedErrors[:5000], uint64(TargetBytesPerLockConflictError.Default())))
}

// TestMVCCExportToSSTSplitMidKey verifies that split mid key in exports will
// omit resume timestamps where they are unnecessary e.g. when we split at the
// new key. In this case we can safely use the SST as is without the need to
// merge with the remaining versions of the key.
func TestMVCCExportToSSTSplitMidKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	engine := createTestPebbleEngine()
	defer engine.Close()

	const keyValueSize = 11

	var testData = []testValue{
		value(key(1), "value1", ts(1000)),
		value(key(2), "value2", ts(1000)),
		value(key(2), "value3", ts(2000)),
		value(key(3), "value4", ts(2000)),
	}
	require.NoError(t, fillInData(ctx, engine, testData))

	for _, test := range []struct {
		exportAll    bool
		stopMidKey   bool
		useMaxSize   bool
		resumeCount  int
		resumeWithTs int
	}{
		{false, false, false, 3, 0},
		{true, false, false, 3, 0},
		{false, true, false, 3, 0},
		// No resume timestamps since we fall under max size criteria
		{true, true, false, 3, 0},
		{true, true, true, 4, 1},
	} {
		t.Run(
			fmt.Sprintf("exportAll=%t,stopMidKey=%t,useMaxSize=%t",
				test.exportAll, test.stopMidKey, test.useMaxSize),
			func(t *testing.T) {
				resumeKey := MVCCKey{Key: key(1)}
				resumeWithTs := 0
				resumeCount := 0
				var maxSize uint64 = 0
				if test.useMaxSize {
					maxSize = keyValueSize * 2
				}
				for !resumeKey.Equal(MVCCKey{}) {
					_, resumeInfo, err := MVCCExportToSST(
						ctx, st, engine, MVCCExportOptions{
							StartKey:           resumeKey,
							EndKey:             key(3).Next(),
							StartTS:            hlc.Timestamp{},
							EndTS:              hlc.Timestamp{WallTime: 9999},
							ExportAllRevisions: test.exportAll,
							TargetSize:         1,
							MaxSize:            maxSize,
							StopMidKey:         test.stopMidKey,
						}, &bytes.Buffer{})
					require.NoError(t, err)
					resumeKey = resumeInfo.ResumeKey
					if !resumeKey.Timestamp.IsEmpty() {
						resumeWithTs++
					}
					resumeCount++
				}
				require.Equal(t, test.resumeCount, resumeCount)
				require.Equal(t, test.resumeWithTs, resumeWithTs)
			})
	}
}

// TestMVCCExportToSSTSErrorsOnLargeKV verifies that MVCCExportToSST errors on a
// single kv that is larger than max size.
func TestMVCCExportToSSTSErrorsOnLargeKV(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	engine := createTestPebbleEngine()
	defer engine.Close()
	var testData = []testValue{value(key(1), "value1", ts(1000))}
	require.NoError(t, fillInData(ctx, engine, testData))
	summary, _, err := MVCCExportToSST(
		ctx, st, engine, MVCCExportOptions{
			StartKey:           MVCCKey{Key: key(1)},
			EndKey:             key(3).Next(),
			StartTS:            hlc.Timestamp{},
			EndTS:              hlc.Timestamp{WallTime: 9999},
			ExportAllRevisions: false,
			TargetSize:         1,
			MaxSize:            1,
			StopMidKey:         true,
		}, &bytes.Buffer{})
	require.Equal(t, int64(0), summary.DataSize)
	expectedErr := &ExceedMaxSizeError{}
	require.ErrorAs(t, err, &expectedErr)
}

func TestMVCCExportDeadlineExceeded(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancelFunc := context.WithCancel(context.Background())
	st := cluster.MakeTestingClusterSettings()

	engine := createTestPebbleEngine()
	defer engine.Close()
	var testData = []testValue{value(key(1), "value1", ts(1000))}
	require.NoError(t, fillInData(ctx, engine, testData))
	ss := kvpb.ScanStats{}
	cancelFunc()
	_, _, err := MVCCExportToSST(
		ctx, st, engine, MVCCExportOptions{
			StartKey:           MVCCKey{Key: key(1)},
			EndKey:             key(3).Next(),
			StartTS:            hlc.Timestamp{},
			EndTS:              hlc.Timestamp{WallTime: 9999},
			ExportAllRevisions: false,
			TargetSize:         1,
			ScanStats:          &ss,
		}, &bytes.Buffer{})
	// Stats are not completely deterministic, so we assert lower bounds.
	require.LessOrEqual(t, uint64(1), ss.NumInterfaceSeeks)
	require.LessOrEqual(t, uint64(1), ss.NumInterfaceSteps)
	require.LessOrEqual(t, uint64(1), ss.PointCount)
	require.LessOrEqual(t, uint64(10), ss.KeyBytes)
	require.LessOrEqual(t, uint64(10), ss.ValueBytes)
	require.Equal(t, uint64(1), ss.NumScans)
	require.NoError(t, err)
}

// TestMVCCExportFingerprint verifies that MVCCExportFingerprint correctly
// fingerprints point keys in a given key and time interval, and returns the
// range keys in a pebble SST.
//
// This test uses a `fingerprintOracle` to verify that the fingerprint generated
// by `MVCCExportFingerprint` is what we would get if we iterated over an SST
// with all keys and computed our own fingerprint.
func TestMVCCExportFingerprint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	fingerprint := func(opts MVCCExportOptions, engine Engine) (uint64, []byte, kvpb.BulkOpSummary, MVCCKey) {
		var dest bytes.Buffer
		var err error
		res, resumeInfo, fingerprint, hasRangeKeys, err := MVCCExportFingerprint(
			ctx, st, engine, opts, &dest)
		require.NoError(t, err)
		if !hasRangeKeys {
			dest.Reset()
		}
		return fingerprint, dest.Bytes(), res, resumeInfo.ResumeKey
	}

	// verifyFingerprintAgainstOracle uses the `fingerprintOracle` to compute a
	// fingerprint over the same key and time interval, and ensure our fingerprint
	// and range keys match up with that generated by the oracle.
	verifyFingerprintAgainstOracle := func(
		actualFingerprint uint64,
		actualRangekeys []MVCCRangeKeyStack,
		opts MVCCExportOptions,
		engine Engine) {
		oracle := makeFingerprintOracle(st, engine, opts)
		expectedFingerprint, expectedRangeKeys := oracle.getFingerprintAndRangeKeys(ctx, t)
		require.Equal(t, expectedFingerprint, actualFingerprint)
		require.Equal(t, expectedRangeKeys, actualRangekeys)
	}

	engine := createTestPebbleEngine()
	defer engine.Close()

	kvSize := int64(16)
	rangeKeySize := int64(10)

	// Insert some point keys.
	//
	// 2000						value3		value4
	//
	// 1000 value1		value2
	//
	//       1					2					3
	var testData = []testValue{
		value(key(1), "value1", ts(1000)),
		value(key(2), "value2", ts(1000)),
		value(key(2), "value3", ts(2000)),
		value(key(3), "value4", ts(2000)),
	}
	require.NoError(t, fillInData(ctx, engine, testData))

	// Insert range keys.
	//
	// 3000 [--- r2 ---)
	//
	// 2000						value3		value4     [--- r1 ---)
	//
	// 1000 value1		value2
	//
	//       1					2					3					4					5
	require.NoError(t, engine.PutRawMVCCRangeKey(MVCCRangeKey{
		StartKey:  key(4),
		EndKey:    key(5),
		Timestamp: ts(2000),
	}, []byte{}))
	require.NoError(t, engine.PutRawMVCCRangeKey(MVCCRangeKey{
		StartKey:  key(1),
		EndKey:    key(2),
		Timestamp: ts(3000),
	}, []byte{}))

	testutils.RunTrueAndFalse(t, "allRevisions", func(t *testing.T, allRevisions bool) {
		t.Run("no-key-or-ts-bounds", func(t *testing.T) {
			opts := MVCCExportOptions{
				StartKey:           MVCCKey{Key: key(1)},
				EndKey:             keys.MaxKey,
				StartTS:            hlc.Timestamp{},
				EndTS:              hlc.Timestamp{WallTime: 9999},
				ExportAllRevisions: allRevisions,
			}
			fingerprint, rangeKeySST, summary, resumeKey := fingerprint(opts, engine)
			require.Empty(t, resumeKey)
			rangeKeys := getRangeKeys(t, rangeKeySST)
			if allRevisions {
				require.Equal(t, kvSize*4+rangeKeySize*2, summary.DataSize)
				require.Equal(t, 2, len(rangeKeys))
			} else {
				require.Equal(t, kvSize*2, summary.DataSize)
				// StartTime is empty so we don't read rangekeys when not exporting all
				// revisions.
				require.Empty(t, rangeKeys)
			}
			verifyFingerprintAgainstOracle(fingerprint, rangeKeys, opts, engine)
		})

		t.Run("key-bounds", func(t *testing.T) {
			opts := MVCCExportOptions{
				StartKey:           MVCCKey{Key: key(1)},
				EndKey:             key(2).Next(),
				StartTS:            hlc.Timestamp{},
				EndTS:              hlc.Timestamp{WallTime: 9999},
				ExportAllRevisions: allRevisions,
			}
			fingerprint, rangeKeySST, summary, resumeKey := fingerprint(opts, engine)
			require.Empty(t, resumeKey)
			rangeKeys := getRangeKeys(t, rangeKeySST)
			if allRevisions {
				require.Equal(t, kvSize*3+rangeKeySize, summary.DataSize)
				require.Equal(t, 1, len(rangeKeys))
			} else {
				// Rangekey masks the point key 1@1000, so we only see 2@2000.
				require.Equal(t, kvSize*1, summary.DataSize)
				// StartTime is empty, so we don't read rangekeys when not exporting all
				// revisions.
				require.Empty(t, rangeKeys)
			}
			verifyFingerprintAgainstOracle(fingerprint, getRangeKeys(t, rangeKeySST), opts, engine)
		})

		t.Run("outside-point-key-bounds", func(t *testing.T) {
			opts := MVCCExportOptions{
				StartKey:           MVCCKey{Key: key(3).Next()},
				EndKey:             keys.MaxKey,
				StartTS:            hlc.Timestamp{},
				EndTS:              hlc.Timestamp{WallTime: 9999},
				ExportAllRevisions: allRevisions,
			}
			fingerprint, rangeKeySST, summary, resumeKey := fingerprint(opts, engine)
			require.Empty(t, resumeKey)
			rangeKeys := getRangeKeys(t, rangeKeySST)
			require.Equal(t, uint64(0), fingerprint)
			if allRevisions {
				require.Equal(t, rangeKeySize, summary.DataSize)
				require.Len(t, rangeKeys, 1)
			} else {
				require.Equal(t, int64(0), summary.DataSize)
				require.Empty(t, rangeKeys)
			}
			verifyFingerprintAgainstOracle(fingerprint, getRangeKeys(t, rangeKeySST), opts, engine)
		})

		t.Run("time-bounds", func(t *testing.T) {
			opts := MVCCExportOptions{
				StartKey:           MVCCKey{Key: key(1)},
				EndKey:             keys.MaxKey,
				StartTS:            ts(1000).Prev(),
				EndTS:              ts(1000),
				ExportAllRevisions: allRevisions,
			}
			fingerprint, rangeKeySST, summary, resumeKey := fingerprint(opts, engine)
			require.Empty(t, resumeKey)
			rangeKeys := getRangeKeys(t, rangeKeySST)
			require.Empty(t, rangeKeys)
			require.Equal(t, kvSize*2, summary.DataSize)
			verifyFingerprintAgainstOracle(fingerprint, getRangeKeys(t, rangeKeySST), opts, engine)
		})

		t.Run("outside-point-key-time-bounds", func(t *testing.T) {
			opts := MVCCExportOptions{
				StartKey:           MVCCKey{Key: key(1)},
				EndKey:             keys.MaxKey,
				StartTS:            ts(2000),
				EndTS:              ts(3000),
				ExportAllRevisions: allRevisions,
			}
			fingerprint, rangeKeySST, summary, resumeKey := fingerprint(opts, engine)
			require.Empty(t, resumeKey)
			rangeKeys := getRangeKeys(t, rangeKeySST)
			require.Equal(t, rangeKeySize, summary.DataSize)
			require.Len(t, rangeKeys, 1)
			require.Equal(t, uint64(0), fingerprint)
			verifyFingerprintAgainstOracle(fingerprint, getRangeKeys(t, rangeKeySST), opts, engine)
		})

		t.Run("assert-hash-is-per-kv", func(t *testing.T) {
			// Fingerprint point keys 1 and 2.
			opts := MVCCExportOptions{
				StartKey:           MVCCKey{Key: key(1)},
				EndKey:             key(2).Next(),
				StartTS:            hlc.Timestamp{},
				EndTS:              hlc.Timestamp{WallTime: 9999},
				ExportAllRevisions: allRevisions,
			}
			fingerprint1, _, summary, resumeKey := fingerprint(opts, engine)
			require.Empty(t, resumeKey)
			if allRevisions {
				require.Equal(t, 3*kvSize+rangeKeySize, summary.DataSize)
			} else {
				// Rangekey masking means we only see 2@2000.
				require.Equal(t, kvSize, summary.DataSize)
			}

			// Fingerprint point key 3.
			opts = MVCCExportOptions{
				StartKey:           MVCCKey{Key: key(3)},
				EndKey:             keys.MaxKey,
				StartTS:            hlc.Timestamp{},
				EndTS:              hlc.Timestamp{WallTime: 9999},
				ExportAllRevisions: allRevisions,
			}
			fingerprint2, _, summary2, resumeKey2 := fingerprint(opts, engine)
			require.Empty(t, resumeKey2)
			if allRevisions {
				require.Equal(t, kvSize+rangeKeySize, summary2.DataSize)
			} else {
				require.Equal(t, kvSize, summary2.DataSize)
			}

			// Fingerprint point keys 1 to 3.
			opts = MVCCExportOptions{
				StartKey:           MVCCKey{Key: key(1)},
				EndKey:             keys.MaxKey,
				StartTS:            hlc.Timestamp{},
				EndTS:              hlc.Timestamp{WallTime: 9999},
				ExportAllRevisions: allRevisions,
			}
			fingerprint3, _, summary3, resumeKey3 := fingerprint(opts, engine)
			require.Empty(t, resumeKey3)
			if allRevisions {
				require.Equal(t, 4*kvSize+2*rangeKeySize, summary3.DataSize)
			} else {
				require.Equal(t, 2*kvSize, summary3.DataSize)
			}

			// Verify that fp3 = fp1 ^ fp2
			require.Equal(t, fingerprint3, fingerprint1^fingerprint2)
		})
	})
}

type fingerprintOracle struct {
	st     *cluster.Settings
	engine Engine
	opts   *MVCCExportOptions
}

// makeFingerprintOracle returns a fingerprintOracle that can be used to check
// the correctness of a fingerprint for point keys.
func makeFingerprintOracle(
	st *cluster.Settings, engine Engine, opts MVCCExportOptions,
) *fingerprintOracle {
	return &fingerprintOracle{
		opts:   &opts,
		engine: engine,
		st:     st,
	}
}

// getFingerprintAndRangeKeys can be used to generate the fingerprint of point
// keys in an interval determined by the supplied `MVCCExportOptions`. This
// fingerprint is generated by exporting the point and range keys to a pebble
// SST using `MVCCExportToSST` and then maintaining a XOR aggregate of the hash
// of every point key in the SST. Range keys are not fingerprinted but instead
// returned as is to the caller.
func (f *fingerprintOracle) getFingerprintAndRangeKeys(
	ctx context.Context, t *testing.T,
) (uint64, []MVCCRangeKeyStack) {
	t.Helper()

	var dest bytes.Buffer
	_, _, err := MVCCExportToSST(ctx, f.st, f.engine, *f.opts, &dest)
	require.NoError(t, err)
	return f.fingerprintPointKeys(t, dest.Bytes()), getRangeKeys(t, dest.Bytes())
}

func (f *fingerprintOracle) fingerprintPointKeys(t *testing.T, dataSST []byte) uint64 {
	t.Helper()

	hasher := fnv.New64()
	var xorAgg uint64
	iterOpts := IterOptions{
		KeyTypes:   IterKeyTypePointsOnly,
		LowerBound: keys.LocalMax,
		UpperBound: keys.MaxKey,
	}
	iter, err := NewMemSSTIterator(dataSST, false, iterOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	for iter.SeekGE(MVCCKey{Key: keys.MinKey}); ; iter.Next() {
		if valid, err := iter.Valid(); !valid || err != nil {
			if err != nil {
				t.Fatal(err)
			}
			break
		}
		k := iter.UnsafeKey()
		if k.Timestamp.IsEmpty() {
			_, err := hasher.Write(k.Key)
			require.NoError(t, err)
			v, err := iter.UnsafeValue()
			require.NoError(t, err)
			_, err = hasher.Write(v)
			require.NoError(t, err)
		} else {
			_, err := hasher.Write(k.Key)
			require.NoError(t, err)
			tsLen := encodedMVCCTimestampLength(k.Timestamp)
			require.NotZero(t, tsLen)
			timestampBuf := make([]byte, tsLen)
			encodeMVCCTimestampToBuf(timestampBuf, k.Timestamp)
			_, err = hasher.Write(timestampBuf)
			require.NoError(t, err)
			v, err := iter.UnsafeValue()
			require.NoError(t, err)
			_, err = hasher.Write(v)
			require.NoError(t, err)
		}
		xorAgg = xorAgg ^ hasher.Sum64()
		hasher.Reset()
	}

	return xorAgg
}

func getRangeKeys(t *testing.T, dataSST []byte) []MVCCRangeKeyStack {
	t.Helper()

	if len(dataSST) == 0 {
		return []MVCCRangeKeyStack{}
	}

	iterOpts := IterOptions{
		KeyTypes:   IterKeyTypeRangesOnly,
		LowerBound: keys.LocalMax,
		UpperBound: keys.MaxKey,
	}
	iter, err := NewMemSSTIterator(dataSST, false, iterOpts)
	require.NoError(t, err)
	defer iter.Close()

	allRangeKeys := make([]MVCCRangeKeyStack, 0)
	for iter.SeekGE(MVCCKey{Key: keys.MinKey}); ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}
		rangeKeys := iter.RangeKeys()
		allRangeKeys = append(allRangeKeys, rangeKeys.Clone())
	}
	return allRangeKeys
}

// mvccGetRaw fetches a raw MVCC value, for use in tests.
func mvccGetRaw(t *testing.T, r Reader, key MVCCKey) []byte {
	value, err := mvccGetRawWithError(t, r, key)
	require.NoError(t, err)
	return value
}

func mvccGetRawWithError(t *testing.T, r Reader, key MVCCKey) ([]byte, error) {
	iter, err := r.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{Prefix: true})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	iter.SeekGE(key)
	if ok, err := iter.Valid(); err != nil || !ok {
		return nil, err
	}
	return iter.Value()
}

func TestMVCCLookupRangeKeyValue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eng := createTestPebbleEngine()
	defer eng.Close()

	const diagram = `
# 	      a     b     c     d
# t=4000  [-----v1----)
# t=2000  [-v2-)[-----v3----)
#
`

	v1 := MVCCValue{
		MVCCValueHeader: enginepb.MVCCValueHeader{
			LocalTimestamp: hlc.ClockTimestamp{WallTime: 1},
		},
	}
	v2 := MVCCValue{
		MVCCValueHeader: enginepb.MVCCValueHeader{
			LocalTimestamp: hlc.ClockTimestamp{WallTime: 2},
		},
	}
	v3 := MVCCValue{
		MVCCValueHeader: enginepb.MVCCValueHeader{
			LocalTimestamp: hlc.ClockTimestamp{WallTime: 3},
		},
	}

	t2000 := hlc.Timestamp{WallTime: 2000}
	t4000 := hlc.Timestamp{WallTime: 4000}

	a, b, c, d := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c"), roachpb.Key("d")

	require.NoError(t, eng.PutMVCCRangeKey(MVCCRangeKey{
		StartKey:  a,
		EndKey:    c,
		Timestamp: t4000,
	}, v1))

	require.NoError(t, eng.PutMVCCRangeKey(MVCCRangeKey{
		StartKey:  a,
		EndKey:    b,
		Timestamp: t2000,
	}, v2))

	require.NoError(t, eng.PutMVCCRangeKey(MVCCRangeKey{
		StartKey:  b,
		EndKey:    d,
		Timestamp: t2000,
	}, v3))

	var buf bytes.Buffer
	fmt.Fprintln(&buf, strings.TrimSpace(diagram))

	for _, tc := range []struct {
		name  string
		k, ek roachpb.Key
		ts    hlc.Timestamp
	}{
		{
			// Look up the exact rangedel.
			name: "ac-valid-full",
			k:    a,
			ek:   c,
			ts:   t4000,
		},
		{
			// Look up inside of the rangedel.
			name: "ac-valid-partial",
			k:    a.Next(),
			ek:   b,
			ts:   t4000,
		},
		{
			// Correct bounds, but incorrect timestamp,
			// will see part of ab and bd which are not compatible and error out.
			name: "ac-incompatible-fragments",
			k:    a,
			ek:   c,
			ts:   t2000,
		},
		{
			// Correct bounds, but timestamp too early.
			// Won't see anything and error out.
			name: "ac-ts-too-early",
			k:    a,
			ek:   b,
			ts:   t2000,
		},
		{
			// See ac but with a gap. Start key before rangedel starts. Errors out.
			name: "ac-invalid-pre",
			k:    roachpb.KeyMin,
			ek:   c,
			ts:   t4000,
		},
		{
			// Sees ac but with a gap. End key after rangedel end. Errors out.
			name: "ac-invalid-post",
			k:    a,
			ek:   d,
			ts:   t4000,
		},
		// Sees cd but wants it longer. Errors.
		{
			name: "cd-invalid-post",
			k:    c,
			ek:   roachpb.Key("f"),
			ts:   t2000,
		},
	} {
		fmt.Fprintf(&buf, "# %s\n", tc.name)
		fmt.Fprintf(&buf, "lookup([%s,%s) @ %d) = ", tc.k, tc.ek, tc.ts.WallTime)
		valBytes, err := MVCCLookupRangeKeyValue(context.Background(), eng, tc.k, tc.ek, tc.ts)
		if err != nil {
			fmt.Fprintln(&buf, err)
		} else {
			v, err := DecodeMVCCValue(valBytes)
			if err != nil {
				fmt.Fprintln(&buf, err)
			} else {
				fmt.Fprintf(&buf, "v%d\n", v.MVCCValueHeader.LocalTimestamp.WallTime)
			}
		}
	}
	path := datapathutils.TestDataPath(t, t.Name())
	echotest.Require(t, buf.String(), path)
}

func TestMVCCGetForKnownTimestampWithNoIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	for _, tc := range []struct {
		name         string
		key          roachpb.Key
		writeTs      hlc.Timestamp
		writeToBatch bool
		writeWithTxn bool
		val          string
		readTs       hlc.Timestamp
		errStr       string
	}{
		{
			name:         "found-in-batch",
			key:          roachpb.Key("a"),
			writeTs:      hlc.Timestamp{WallTime: 5},
			writeToBatch: true,
			val:          "a-val",
			readTs:       hlc.Timestamp{WallTime: 5},
		},
		{
			name:         "found-in-engine",
			key:          roachpb.Key("b"),
			writeTs:      hlc.Timestamp{WallTime: 5},
			writeToBatch: false,
			val:          "b-val",
			readTs:       hlc.Timestamp{WallTime: 5},
		},
		{
			name:         "higher-timestamp-in-batch",
			key:          roachpb.Key("c"),
			writeTs:      hlc.Timestamp{WallTime: 5},
			writeToBatch: true,
			val:          "c-val",
			readTs:       hlc.Timestamp{WallTime: 4},
			errStr:       "value missing for key",
		},
		{
			name:         "higher-timestamp-in-engine",
			key:          roachpb.Key("d"),
			writeTs:      hlc.Timestamp{WallTime: 5},
			writeToBatch: false,
			val:          "d-val",
			readTs:       hlc.Timestamp{WallTime: 4},
			errStr:       "value missing for key",
		},
		{
			name:         "intent-in-batch-not-seen",
			key:          roachpb.Key("e"),
			writeTs:      hlc.Timestamp{WallTime: 5},
			writeToBatch: true,
			writeWithTxn: true,
			val:          "e-val",
			readTs:       hlc.Timestamp{WallTime: 5},
		},
		{
			name:         "intent-in-engine-not-seen",
			key:          roachpb.Key("f"),
			writeTs:      hlc.Timestamp{WallTime: 5},
			writeToBatch: false,
			writeWithTxn: true,
			val:          "f-val",
			readTs:       hlc.Timestamp{WallTime: 5},
		},
		{
			name:         "lower-timestamp-in-batch",
			key:          roachpb.Key("g"),
			writeTs:      hlc.Timestamp{WallTime: 5},
			writeToBatch: true,
			val:          "g-val",
			readTs:       hlc.Timestamp{WallTime: 6},
			errStr:       "expected timestamp 0.000000006,0 and found 0.000000005,0",
		},
		{
			name:         "lower-timestamp-in-engine",
			key:          roachpb.Key("h"),
			writeTs:      hlc.Timestamp{WallTime: 5},
			writeToBatch: false,
			val:          "h-val",
			readTs:       hlc.Timestamp{WallTime: 6},
			errStr:       "expected timestamp 0.000000006,0 and found 0.000000005,0",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var rw ReadWriter
			rw = engine
			var batch Batch
			if tc.writeToBatch {
				batch = engine.NewBatch()
				defer batch.Close()
				rw = batch
			}
			val := roachpb.MakeValueFromString(tc.val)
			var txn *roachpb.Transaction
			if tc.writeWithTxn {
				txn = makeTxn(*txn1, tc.writeTs)
			}
			_, err := MVCCPut(ctx, rw, tc.key, tc.writeTs, val, MVCCWriteOptions{Txn: txn})
			require.NoError(t, err)
			if batch == nil {
				batch = engine.NewBatch()
				defer batch.Close()
			}
			v, _, err := MVCCGetForKnownTimestampWithNoIntent(
				ctx, batch, tc.key, tc.readTs, tc.writeToBatch)
			if len(tc.errStr) == 0 {
				b, err := v.GetBytes()
				require.NoError(t, err)
				require.Equal(t, tc.val, string(b))
				if tc.writeToBatch {
					// Also read without the batch-only optimization.
					v, _, err = MVCCGetForKnownTimestampWithNoIntent(
						ctx, batch, tc.key, tc.readTs, false)
					require.NoError(t, err)
					b, err = v.GetBytes()
					require.NoError(t, err)
					require.Equal(t, tc.val, string(b))
				}
			} else {
				require.ErrorContains(t, err, tc.errStr)
			}
		})
	}
}
