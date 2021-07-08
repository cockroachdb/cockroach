// Copyright 2014 The Cockroach Authors.
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
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/zerofields"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/shuffle"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
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
	txn1         = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn1ID, Epoch: 1, WriteTimestamp: txn1TS, MinTimestamp: txn1TS}, ReadTimestamp: txn1TS}
	txn1Commit   = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn1ID, Epoch: 1, WriteTimestamp: txn1TS, MinTimestamp: txn1TS}, ReadTimestamp: txn1TS, Status: roachpb.COMMITTED}
	txn1Abort    = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn1ID, Epoch: 1, WriteTimestamp: txn1TS, MinTimestamp: txn1TS}, Status: roachpb.ABORTED}
	txn1e2       = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn1ID, Epoch: 2, WriteTimestamp: txn1TS, MinTimestamp: txn1TS}, ReadTimestamp: txn1TS}
	txn1e2Commit = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn1ID, Epoch: 2, WriteTimestamp: txn1TS, MinTimestamp: txn1TS}, ReadTimestamp: txn1TS, Status: roachpb.COMMITTED}
	txn2         = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn2ID, WriteTimestamp: txn2TS, MinTimestamp: txn2TS}, ReadTimestamp: txn2TS}
	txn2Commit   = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn2ID, WriteTimestamp: txn2TS, MinTimestamp: txn2TS}, ReadTimestamp: txn2TS, Status: roachpb.COMMITTED}
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
func createTestPebbleEngine() Engine {
	return NewDefaultInMemForTesting()
}

func createTestPebbleEngineWithSettings(settings *cluster.Settings) Engine {
	return NewInMem(
		context.Background(),
		roachpb.Attributes{},
		1<<20,   /* cacheSize */
		512<<20, /* storeSize */
		settings,
	)
}

// TODO(sumeer): the following is legacy from when we had multiple engine
// implementations. Some tests are switched over to only create Pebble, since
// the create method does not provide control over cluster.Settings. Switch
// the rest and remove this.
var mvccEngineImpls = []struct {
	name   string
	create func() Engine
}{
	{"pebble", createTestPebbleEngine},
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
		ContainsEstimates:    1,
		KeyBytes:             1,
		KeyCount:             1,
		ValBytes:             1,
		ValCount:             1,
		IntentBytes:          1,
		IntentCount:          1,
		SeparatedIntentCount: 1,
		IntentAge:            1,
		GCBytesAge:           1,
		LiveBytes:            1,
		LiveCount:            1,
		SysBytes:             1,
		SysCount:             1,
		LastUpdateNanos:      1,
		AbortSpanBytes:       1,
	}
	if err := zerofields.NoZeroField(&goldMS); err != nil {
		t.Fatal(err) // prevent rot as fields are added
	}

	cmp := func(act, exp enginepb.MVCCStats) {
		t.Helper()
		f, l, _ := caller.Lookup(1)
		if !reflect.DeepEqual(act, exp) {
			t.Fatalf("%s:%d: wanted %+v back, got %+v", f, l, exp, act)
		}
	}

	ms := goldMS
	zeroWithLU := enginepb.MVCCStats{
		ContainsEstimates: 0,
		LastUpdateNanos:   ms.LastUpdateNanos,
	}

	ms.Subtract(goldMS)
	cmp(ms, zeroWithLU)

	ms.Add(goldMS)
	cmp(ms, goldMS)

	// Double-add double-sub guards against mistaking `+=` for `=`.
	ms = zeroWithLU
	ms.Add(goldMS)
	ms.Add(goldMS)
	ms.Subtract(goldMS)
	ms.Subtract(goldMS)
	cmp(ms, zeroWithLU)

	// Run some checks for Forward.
	goldDelta := enginepb.MVCCStats{
		KeyBytes:        42,
		IntentCount:     11,
		LastUpdateNanos: 1e9 - 1000,
	}
	delta := goldDelta

	for i, ns := range []int64{1, 1e9 - 1001, 1e9 - 1000, 1e9 - 1, 1e9, 1e9 + 1, 2e9 - 1} {
		oldDelta := delta
		delta.AgeTo(ns)
		if delta.LastUpdateNanos < ns {
			t.Fatalf("%d: expected LastUpdateNanos < %d, got %d", i, ns, delta.LastUpdateNanos)
		}
		shouldAge := ns/1e9-oldDelta.LastUpdateNanos/1e9 > 0
		didAge := delta.IntentAge != oldDelta.IntentAge &&
			delta.GCBytesAge != oldDelta.GCBytesAge
		if shouldAge != didAge {
			t.Fatalf("%d: should age: %t, but had\n%+v\nand now\n%+v", i, shouldAge, oldDelta, delta)
		}
	}

	expDelta := goldDelta
	expDelta.LastUpdateNanos = 2e9 - 1
	expDelta.GCBytesAge = 42
	expDelta.IntentAge = 11
	cmp(delta, expDelta)

	delta.AgeTo(2e9)
	expDelta.LastUpdateNanos = 2e9
	expDelta.GCBytesAge += 42
	expDelta.IntentAge += 11
	cmp(delta, expDelta)

	{
		// Verify that AgeTo can go backwards in time.
		// Works on a copy.
		tmpDelta := delta
		expDelta := expDelta

		tmpDelta.AgeTo(2e9 - 1)
		expDelta.LastUpdateNanos = 2e9 - 1
		expDelta.GCBytesAge -= 42
		expDelta.IntentAge -= 11
		cmp(tmpDelta, expDelta)
	}

	delta.AgeTo(3e9 - 1)
	delta.Forward(5) // should be noop
	expDelta.LastUpdateNanos = 3e9 - 1
	cmp(delta, expDelta)

	// Check that Add calls Forward appropriately.
	mss := []enginepb.MVCCStats{goldMS, goldMS}

	mss[0].LastUpdateNanos = 2e9 - 1
	mss[1].LastUpdateNanos = 10e9 + 1

	expMS := goldMS
	expMS.Add(goldMS)
	expMS.LastUpdateNanos = 10e9 + 1
	expMS.IntentAge += 9  // from aging 9 ticks from 2E9-1 to 10E9+1
	expMS.GCBytesAge += 9 // ditto

	for i := range mss[:1] {
		ms := mss[(1+i)%2]
		ms.Add(mss[i])
		cmp(ms, expMS)
	}

	// Finally, check Forward with negative counts (can happen).
	neg := zeroWithLU
	neg.Subtract(goldMS)
	exp := neg

	neg.AgeTo(2e9)

	exp.LastUpdateNanos = 2e9
	exp.GCBytesAge = -3
	exp.IntentAge = -3
	cmp(neg, exp)
}

// Verify the sort ordering of successive keys with metadata and
// versioned values. In particular, the following sequence of keys /
// versions:
//
// a
// a<t=max>
// a<t=1>
// a<t=0>
// a\x00
// a\x00<t=max>
// a\x00<t=1>
// a\x00<t=0>
func TestMVCCKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	aKey := roachpb.Key("a")
	a0Key := roachpb.Key("a\x00")
	keys := mvccKeys{
		mvccKey(aKey),
		mvccVersionKey(aKey, hlc.Timestamp{WallTime: math.MaxInt64}),
		mvccVersionKey(aKey, hlc.Timestamp{WallTime: 1}),
		mvccVersionKey(aKey, hlc.Timestamp{Logical: 1}),
		mvccKey(a0Key),
		mvccVersionKey(a0Key, hlc.Timestamp{WallTime: math.MaxInt64}),
		mvccVersionKey(a0Key, hlc.Timestamp{WallTime: 1}),
		mvccVersionKey(a0Key, hlc.Timestamp{Logical: 1}),
	}
	sortKeys := make(mvccKeys, len(keys))
	copy(sortKeys, keys)
	shuffle.Shuffle(sortKeys)
	sort.Sort(sortKeys)
	if !reflect.DeepEqual(sortKeys, keys) {
		t.Errorf("expected keys to sort in order %s, but got %s", keys, sortKeys)
	}
}

func TestMVCCGetNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			value, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{Logical: 1},
				MVCCGetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if value != nil {
				t.Fatal("the value should be empty")
			}
		})
	}
}

func TestMVCCGetNoMoreOldVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
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

			engine := engineImpl.create()
			defer engine.Close()

			if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 3}, value1, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey2, hlc.Timestamp{WallTime: 1}, value2, nil); err != nil {
				t.Fatal(err)
			}

			value, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 2}, MVCCGetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if value != nil {
				t.Fatal("the value should be empty")
			}
		})
	}
}

func TestMVCCGetAndDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
				t.Fatal(err)
			}
			value, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 2}, MVCCGetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if value == nil {
				t.Fatal("the value should not be empty")
			}

			err = MVCCDelete(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 3}, nil)
			if err != nil {
				t.Fatal(err)
			}

			// Read the latest version which should be deleted.
			value, _, err = MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 4}, MVCCGetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if value != nil {
				t.Fatal("the value should be empty")
			}
			// Read the latest version with tombstone.
			value, _, err = MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 4},
				MVCCGetOptions{Tombstones: true})
			if err != nil {
				t.Fatal(err)
			} else if value == nil || len(value.RawBytes) != 0 {
				t.Fatalf("the value should be non-nil with empty RawBytes; got %+v", value)
			}

			// Read the old version which should still exist.
			for _, logical := range []int32{0, math.MaxInt32} {
				value, _, err = MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 2, Logical: logical},
					MVCCGetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if value == nil {
					t.Fatal("the value should not be empty")
				}
			}
		})
	}
}

// TestMVCCWriteWithOlderTimestampAfterDeletionOfNonexistentKey tests a write
// that comes after a delete on a nonexistent key, with the write holding a
// timestamp earlier than the delete timestamp. The delete must write a
// tombstone with its timestamp in order to push the write's timestamp.
func TestMVCCWriteWithOlderTimestampAfterDeletionOfNonexistentKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			if err := MVCCDelete(
				context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 3}, nil,
			); err != nil {
				t.Fatal(err)
			}

			if err := MVCCPut(
				context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil,
			); !testutils.IsError(
				err, "write at timestamp 0.000000001,0 too old; wrote at 0.000000003,1",
			) {
				t.Fatal(err)
			}

			value, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 2},
				MVCCGetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			// The attempted write at ts(1,0) was performed at ts(3,1), so we should
			// not see it at ts(2,0).
			if value != nil {
				t.Fatalf("value present at TS = %s", value.Timestamp)
			}

			// Read the latest version which will be the value written with the timestamp pushed.
			value, _, err = MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 4},
				MVCCGetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if value == nil {
				t.Fatal("value doesn't exist")
			}
			if !bytes.Equal(value.RawBytes, value1.RawBytes) {
				t.Errorf("expected %q; got %q", value1.RawBytes, value.RawBytes)
			}
			if expTS := (hlc.Timestamp{WallTime: 3, Logical: 1}); value.Timestamp != expTS {
				t.Fatalf("timestamp was not pushed: %s, expected %s", value.Timestamp, expTS)
			}
		})
	}
}

func TestMVCCInlineWithTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			// Put an inline value.
			if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{}, value1, nil); err != nil {
				t.Fatal(err)
			}

			// Now verify inline get.
			value, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{}, MVCCGetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(value1, *value) {
				t.Errorf("the inline value should be %v; got %v", value1, *value)
			}

			// Verify inline get with txn does still work (this will happen on a
			// scan if the distributed sender is forced to wrap it in a txn).
			if _, _, err = MVCCGet(ctx, engine, testKey1, hlc.Timestamp{}, MVCCGetOptions{
				Txn: txn1,
			}); err != nil {
				t.Error(err)
			}

			// Verify inline put with txn is an error.
			err = MVCCPut(ctx, engine, nil, testKey2, hlc.Timestamp{}, value2, txn2)
			if !testutils.IsError(err, "writes not allowed within transactions") {
				t.Errorf("unexpected error: %+v", err)
			}
		})
	}
}

func TestMVCCDeleteMissingKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			if err := MVCCDelete(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, nil); err != nil {
				t.Fatal(err)
			}
			// Verify nothing is written to the engine.
			if val, err := engine.MVCCGet(mvccKey(testKey1)); err != nil || val != nil {
				t.Fatalf("expected no mvcc metadata after delete of a missing key; got %q: %+v", val, err)
			}
		})
	}
}

func TestMVCCGetAndDeleteInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			txn := makeTxn(*txn1, hlc.Timestamp{WallTime: 1})
			txn.Sequence++
			if err := MVCCPut(ctx, engine, nil, testKey1, txn.ReadTimestamp, value1, txn); err != nil {
				t.Fatal(err)
			}

			if value, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 2}, MVCCGetOptions{
				Txn: txn,
			}); err != nil {
				t.Fatal(err)
			} else if value == nil {
				t.Fatal("the value should not be empty")
			}

			txn.Sequence++
			txn.WriteTimestamp = hlc.Timestamp{WallTime: 3}
			if err := MVCCDelete(ctx, engine, nil, testKey1, txn.ReadTimestamp, txn); err != nil {
				t.Fatal(err)
			}

			// Read the latest version which should be deleted.
			if value, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 4}, MVCCGetOptions{
				Txn: txn,
			}); err != nil {
				t.Fatal(err)
			} else if value != nil {
				t.Fatal("the value should be empty")
			}
			// Read the latest version with tombstone.
			if value, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 4}, MVCCGetOptions{
				Tombstones: true,
				Txn:        txn,
			}); err != nil {
				t.Fatal(err)
			} else if value == nil || len(value.RawBytes) != 0 {
				t.Fatalf("the value should be non-nil with empty RawBytes; got %+v", value)
			}

			// Read the old version which shouldn't exist, as within a
			// transaction, we delete previous values.
			if value, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 2}, MVCCGetOptions{}); err != nil {
				t.Fatal(err)
			} else if value != nil {
				t.Fatalf("expected value nil, got: %s", value)
			}
		})
	}
}

func TestMVCCGetWriteIntentError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			if err := MVCCPut(ctx, engine, nil, testKey1, txn1.ReadTimestamp, value1, txn1); err != nil {
				t.Fatal(err)
			}

			if _, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{}); err == nil {
				t.Fatal("cannot read the value of a write intent without TxnID")
			}

			if _, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{
				Txn: txn2,
			}); err == nil {
				t.Fatal("cannot read the value of a write intent from a different TxnID")
			}
		})
	}
}

func mkVal(s string, ts hlc.Timestamp) roachpb.Value {
	v := roachpb.MakeValueFromString(s)
	v.Timestamp = ts
	return v
}

func TestMVCCScanWriteIntentError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
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
				if err := MVCCPut(ctx, engine, nil, kv.Key, kv.Value.Timestamp, v, txnMap[i]); err != nil {
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
					// would be []roachpb.KeyValue{fixtureKVs[3], fixtureKVs[4]} without WriteIntentError
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
						hlc.Timestamp{WallTime: 1}, MVCCScanOptions{Inconsistent: !scan.consistent, Txn: scan.txn, MaxIntents: 2})
					var wiErr *roachpb.WriteIntentError
					_ = errors.As(err, &wiErr)
					if (err == nil) != (wiErr == nil) {
						t.Errorf("unexpected error: %+v", err)
					}

					if wiErr == nil != !scan.consistent {
						t.Fatalf("expected write intent error; got %s", err)
					}

					intents := res.Intents
					kvs := res.KVs
					if len(intents) > 0 != !scan.consistent {
						t.Fatalf("expected different intents slice; got %+v", intents)
					}

					if scan.consistent {
						intents = wiErr.Intents
					}

					if !reflect.DeepEqual(intents, scan.expIntents) {
						t.Fatalf("expected intents:\n%+v;\n got\n%+v", scan.expIntents, intents)
					}

					if !reflect.DeepEqual(kvs, scan.expValues) {
						t.Fatalf("expected values %+v; got %+v", scan.expValues, kvs)
					}
				})
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

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			// Put two values to key 1, the latest with a txn.
			if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
				t.Fatal(err)
			}
			txn1ts := makeTxn(*txn1, hlc.Timestamp{WallTime: 2})
			if err := MVCCPut(ctx, engine, nil, testKey1, txn1ts.ReadTimestamp, value2, txn1ts); err != nil {
				t.Fatal(err)
			}

			// A get with consistent=false should fail in a txn.
			if _, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{
				Inconsistent: true,
				Txn:          txn1,
			}); err == nil {
				t.Error("expected an error getting with consistent=false in txn")
			}

			// Inconsistent get will fetch value1 for any timestamp.
			for _, ts := range []hlc.Timestamp{{WallTime: 1}, {WallTime: 2}} {
				val, intent, err := MVCCGet(ctx, engine, testKey1, ts, MVCCGetOptions{Inconsistent: true})
				if ts.Less(hlc.Timestamp{WallTime: 2}) {
					if err != nil {
						t.Fatal(err)
					}
				} else {
					if intent == nil || !intent.Key.Equal(testKey1) {
						t.Fatalf("expected %v, but got %v", testKey1, intent)
					}
				}
				if !bytes.Equal(val.RawBytes, value1.RawBytes) {
					t.Errorf("@%s expected %q; got %q", ts, value1.RawBytes, val.RawBytes)
				}
			}

			// Write a single intent for key 2 and verify get returns empty.
			if err := MVCCPut(ctx, engine, nil, testKey2, txn2.ReadTimestamp, value1, txn2); err != nil {
				t.Fatal(err)
			}
			val, intent, err := MVCCGet(ctx, engine, testKey2, hlc.Timestamp{WallTime: 2},
				MVCCGetOptions{Inconsistent: true})
			if intent == nil || !intent.Key.Equal(testKey2) {
				t.Fatal(err)
			}
			if val != nil {
				t.Errorf("expected empty val; got %+v", val)
			}
		})
	}
}

// TestMVCCGetProtoInconsistent verifies the behavior of MVCCGetProto with
// consistent set to false.
func TestMVCCGetProtoInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
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
			if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, v1, nil); err != nil {
				t.Fatal(err)
			}
			txn1ts := makeTxn(*txn1, hlc.Timestamp{WallTime: 2})
			if err := MVCCPut(ctx, engine, nil, testKey1, txn1ts.ReadTimestamp, v2, txn1ts); err != nil {
				t.Fatal(err)
			}

			// An inconsistent get should fail in a txn.
			if _, err := MVCCGetProto(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, nil, MVCCGetOptions{
				Inconsistent: true,
				Txn:          txn1,
			}); err == nil {
				t.Error("expected an error getting inconsistently in txn")
			} else if errors.HasType(err, (*roachpb.WriteIntentError)(nil)) {
				t.Error("expected non-WriteIntentError with inconsistent read in txn")
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
				if err := MVCCPut(ctx, engine, nil, testKey2, txn2.ReadTimestamp, v1, txn2); err != nil {
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
				if err := MVCCPut(ctx, engine, nil, testKey3, hlc.Timestamp{WallTime: 1}, value3, nil); err != nil {
					t.Fatal(err)
				}
				if err := MVCCPut(ctx, engine, nil, testKey3, txn1ts.ReadTimestamp, v2, txn1ts); err != nil {
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
		})
	}
}

// Regression test for #28205: MVCCGet and MVCCScan, FindSplitKey, and
// ComputeStats need to invalidate the cached iterator data.
func TestMVCCInvalidateIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, which := range []string{"get", "scan", "findSplitKey", "computeStats"} {
		t.Run(which, func(t *testing.T) {
			for _, engineImpl := range mvccEngineImpls {
				t.Run(engineImpl.name, func(t *testing.T) {
					engine := engineImpl.create()
					defer engine.Close()

					ctx := context.Background()
					ts1 := hlc.Timestamp{WallTime: 1}
					ts2 := hlc.Timestamp{WallTime: 2}

					key := roachpb.Key("a")
					if err := MVCCPut(ctx, engine, nil, key, ts1, value1, nil); err != nil {
						t.Fatal(err)
					}

					var iterOptions IterOptions
					switch which {
					case "get":
						iterOptions.Prefix = true
					case "scan", "findSplitKey", "computeStats":
						iterOptions.UpperBound = roachpb.KeyMax
					}

					// Use a batch which internally caches the iterator.
					batch := engine.NewBatch()
					defer batch.Close()

					{
						// Seek the iter to a valid position.
						iter := batch.NewMVCCIterator(MVCCKeyAndIntentsIterKind, iterOptions)
						iter.SeekGE(MakeMVCCMetadataKey(key))
						iter.Close()
					}

					var err error
					switch which {
					case "get":
						_, _, err = MVCCGet(ctx, batch, key, ts2, MVCCGetOptions{})
					case "scan":
						_, err = MVCCScan(ctx, batch, key, roachpb.KeyMax, ts2, MVCCScanOptions{})
					case "findSplitKey":
						_, err = MVCCFindSplitKey(ctx, batch, roachpb.RKeyMin, roachpb.RKeyMax, 64<<20)
					case "computeStats":
						iter := batch.NewMVCCIterator(MVCCKeyAndIntentsIterKind, iterOptions)
						_, err = iter.ComputeStats(keys.LocalMax, roachpb.KeyMax, 0)
						iter.Close()
					}
					if err != nil {
						t.Fatal(err)
					}

					// Verify that the iter is invalid.
					iter := batch.NewMVCCIterator(MVCCKeyAndIntentsIterKind, iterOptions)
					defer iter.Close()
					if ok, _ := iter.Valid(); ok {
						t.Fatalf("iterator should not be valid")
					}
				})
			}
		})
	}
}

func TestMVCCPutAfterBatchIterCreate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			err := engine.PutMVCC(MVCCKey{testKey1, hlc.Timestamp{WallTime: 5}}, []byte("foobar"))
			if err != nil {
				t.Fatal(err)
			}
			err = engine.PutMVCC(MVCCKey{testKey2, hlc.Timestamp{WallTime: 5}}, []byte("foobar"))
			if err != nil {
				t.Fatal(err)
			}
			err = engine.PutMVCC(MVCCKey{testKey2, hlc.Timestamp{WallTime: 3}}, []byte("foobar"))
			if err != nil {
				t.Fatal(err)
			}
			err = engine.PutMVCC(MVCCKey{testKey3, hlc.Timestamp{WallTime: 5}}, []byte("foobar"))
			if err != nil {
				t.Fatal(err)
			}
			err = engine.PutMVCC(MVCCKey{testKey4, hlc.Timestamp{WallTime: 5}}, []byte("foobar"))
			if err != nil {
				t.Fatal(err)
			}

			batch := engine.NewBatch()
			defer batch.Close()
			txn := &roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					WriteTimestamp: hlc.Timestamp{WallTime: 10},
				},
				Name:                   "test",
				Status:                 roachpb.PENDING,
				ReadTimestamp:          hlc.Timestamp{WallTime: 10},
				GlobalUncertaintyLimit: hlc.Timestamp{WallTime: 10},
			}
			iter := batch.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
				LowerBound: testKey1,
				UpperBound: testKey5,
			})
			defer iter.Close()
			iter.SeekGE(MVCCKey{testKey1, hlc.Timestamp{WallTime: 5}})
			iter.Next() // key2/5

			// Lay down an intent on key3, which will go at key3/0 and sort before key3/5.
			err = MVCCDelete(context.Background(), batch, nil, testKey3, txn.WriteTimestamp, txn)
			if err != nil {
				t.Fatal(err)
			}
			iter.SeekGE(MVCCKey{Key: testKey3})
			if ok, err := iter.Valid(); !ok || err != nil {
				t.Fatalf("expected valid iter: ok %t, err %s", ok, err.Error())
			}
			// Should see the intent.
			if iter.UnsafeKey().IsValue() {
				t.Fatalf("expected iterator to land on an intent, got a value: %v", iter.UnsafeKey())
			}
		})
	}
}

func mvccScanTest(ctx context.Context, t *testing.T, engine Engine) {
	if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 2}, value4, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(ctx, engine, nil, testKey2, hlc.Timestamp{WallTime: 1}, value2, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(ctx, engine, nil, testKey2, hlc.Timestamp{WallTime: 3}, value3, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(ctx, engine, nil, testKey3, hlc.Timestamp{WallTime: 1}, value3, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(ctx, engine, nil, testKey3, hlc.Timestamp{WallTime: 4}, value2, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(ctx, engine, nil, testKey4, hlc.Timestamp{WallTime: 1}, value4, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(ctx, engine, nil, testKey4, hlc.Timestamp{WallTime: 5}, value1, nil); err != nil {
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

	if _, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{
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
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			mvccScanTest(ctx, t, engine)
		})
	}
}

func TestMVCCScanMaxNum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey2, hlc.Timestamp{WallTime: 1}, value2, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey3, hlc.Timestamp{WallTime: 1}, value3, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey4, hlc.Timestamp{WallTime: 1}, value4, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey6, hlc.Timestamp{WallTime: 1}, value4, nil); err != nil {
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
		})
	}
}

func TestMVCCScanWithKeyPrefix(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
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
			if err := MVCCPut(ctx, engine, nil, roachpb.Key("/a"), hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, roachpb.Key("/a"), hlc.Timestamp{WallTime: 2}, value2, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, roachpb.Key("/aa"), hlc.Timestamp{WallTime: 2}, value2, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, roachpb.Key("/aa"), hlc.Timestamp{WallTime: 3}, value3, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, roachpb.Key("/b"), hlc.Timestamp{WallTime: 1}, value3, nil); err != nil {
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
		})
	}
}

func TestMVCCScanInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey2, hlc.Timestamp{WallTime: 1}, value2, nil); err != nil {
				t.Fatal(err)
			}
			txn := makeTxn(*txn1, hlc.Timestamp{WallTime: 1})
			if err := MVCCPut(ctx, engine, nil, testKey3, txn.ReadTimestamp, value3, txn); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey4, hlc.Timestamp{WallTime: 1}, value4, nil); err != nil {
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
		})
	}
}

// TestMVCCScanInconsistent writes several values, some as intents and
// verifies that the scan sees only the committed versions.
func TestMVCCScanInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
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
			if err := MVCCPut(ctx, engine, nil, testKey1, ts1, value1, nil); err != nil {
				t.Fatal(err)
			}
			txn1ts2 := makeTxn(*txn1, ts2)
			if err := MVCCPut(ctx, engine, nil, testKey1, txn1ts2.ReadTimestamp, value2, txn1ts2); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey2, ts3, value1, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey2, ts4, value2, nil); err != nil {
				t.Fatal(err)
			}
			txn2ts5 := makeTxn(*txn2, ts5)
			if err := MVCCPut(ctx, engine, nil, testKey3, txn2ts5.ReadTimestamp, value3, txn2ts5); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey4, ts6, value4, nil); err != nil {
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
		})
	}
}

func TestMVCCDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey2, hlc.Timestamp{WallTime: 1}, value2, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey3, hlc.Timestamp{WallTime: 1}, value3, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey4, hlc.Timestamp{WallTime: 1}, value4, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey5, hlc.Timestamp{WallTime: 1}, value5, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey6, hlc.Timestamp{WallTime: 1}, value6, nil); err != nil {
				t.Fatal(err)
			}

			// Attempt to delete two keys.
			deleted, resumeSpan, num, err := MVCCDeleteRange(
				ctx, engine, nil, testKey2, testKey6, 2, hlc.Timestamp{WallTime: 2}, nil, false,
			)
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
			deleted, resumeSpan, num, err = MVCCDeleteRange(
				ctx, engine, nil, testKey2, testKey6, -1, hlc.Timestamp{WallTime: 2}, nil, false)
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

			deleted, resumeSpan, num, err = MVCCDeleteRange(
				ctx, engine, nil, testKey4, keyMax, 0, hlc.Timestamp{WallTime: 2}, nil, false)
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

			deleted, resumeSpan, num, err = MVCCDeleteRange(
				ctx, engine, nil, localMax, testKey2, 0, hlc.Timestamp{WallTime: 2}, nil, false)
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
		})
	}
}

func TestMVCCDeleteRangeReturnKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey2, hlc.Timestamp{WallTime: 1}, value2, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey3, hlc.Timestamp{WallTime: 1}, value3, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey4, hlc.Timestamp{WallTime: 1}, value4, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey5, hlc.Timestamp{WallTime: 1}, value5, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey6, hlc.Timestamp{WallTime: 1}, value6, nil); err != nil {
				t.Fatal(err)
			}

			// Attempt to delete two keys.
			deleted, resumeSpan, num, err := MVCCDeleteRange(
				ctx, engine, nil, testKey2, testKey6, 2, hlc.Timestamp{WallTime: 2}, nil, true)
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
			deleted, resumeSpan, num, err = MVCCDeleteRange(
				ctx, engine, nil, testKey2, testKey6, -1, hlc.Timestamp{WallTime: 2}, nil, true)
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

			deleted, resumeSpan, num, err = MVCCDeleteRange(
				ctx, engine, nil, testKey4, keyMax, math.MaxInt64, hlc.Timestamp{WallTime: 2}, nil, true)
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

			deleted, resumeSpan, num, err = MVCCDeleteRange(
				ctx, engine, nil, localMax, testKey2, math.MaxInt64, hlc.Timestamp{WallTime: 2}, nil, true)
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
		})
	}
}

func TestMVCCDeleteRangeFailed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			txn := makeTxn(*txn1, hlc.Timestamp{WallTime: 1})
			if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
				t.Fatal(err)
			}
			txn.Sequence++
			if err := MVCCPut(ctx, engine, nil, testKey2, txn.ReadTimestamp, value2, txn); err != nil {
				t.Fatal(err)
			}
			txn.Sequence++
			if err := MVCCPut(ctx, engine, nil, testKey3, txn.ReadTimestamp, value3, txn); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey4, hlc.Timestamp{WallTime: 1}, value4, nil); err != nil {
				t.Fatal(err)
			}

			if _, _, _, err := MVCCDeleteRange(
				ctx, engine, nil, testKey2, testKey4, math.MaxInt64, hlc.Timestamp{WallTime: 1}, nil, false,
			); err == nil {
				t.Fatal("expected error on uncommitted write intent")
			}

			txn.Sequence++
			if _, _, _, err := MVCCDeleteRange(
				ctx, engine, nil, testKey2, testKey4, math.MaxInt64, txn.ReadTimestamp, txn, false,
			); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMVCCDeleteRangeConcurrentTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			txn1ts := makeTxn(*txn1, hlc.Timestamp{WallTime: 1})
			txn2ts := makeTxn(*txn2, hlc.Timestamp{WallTime: 2})

			if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey2, txn1ts.ReadTimestamp, value2, txn1ts); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey3, txn2ts.ReadTimestamp, value3, txn2ts); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey4, hlc.Timestamp{WallTime: 1}, value4, nil); err != nil {
				t.Fatal(err)
			}

			if _, _, _, err := MVCCDeleteRange(
				ctx, engine, nil, testKey2, testKey4, math.MaxInt64, txn1ts.ReadTimestamp, txn1ts, false,
			); err == nil {
				t.Fatal("expected error on uncommitted write intent")
			}
		})
	}
}

// TestMVCCUncommittedDeleteRangeVisible tests that the keys in an uncommitted
// DeleteRange are visible to the same transaction at a higher epoch.
func TestMVCCUncommittedDeleteRangeVisible(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			if err := MVCCPut(
				ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil,
			); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(
				ctx, engine, nil, testKey2, hlc.Timestamp{WallTime: 1}, value2, nil,
			); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(
				ctx, engine, nil, testKey3, hlc.Timestamp{WallTime: 1}, value3, nil,
			); err != nil {
				t.Fatal(err)
			}

			if err := MVCCDelete(
				ctx, engine, nil, testKey2, hlc.Timestamp{WallTime: 2, Logical: 1}, nil,
			); err != nil {
				t.Fatal(err)
			}

			txn := makeTxn(*txn1, hlc.Timestamp{WallTime: 2, Logical: 2})
			if _, _, _, err := MVCCDeleteRange(
				ctx, engine, nil, testKey1, testKey4, math.MaxInt64, txn.ReadTimestamp, txn, false,
			); err != nil {
				t.Fatal(err)
			}

			txn.Epoch++
			res, _ := MVCCScan(ctx, engine, testKey1, testKey4,
				hlc.Timestamp{WallTime: 3}, MVCCScanOptions{Txn: txn})
			if e := 2; len(res.KVs) != e {
				t.Fatalf("e = %d, got %d", e, len(res.KVs))
			}
		})
	}
}

// TestMVCCDeleteRangeOldTimestamp tests a case where a delete range with an
// older timestamp happens after a delete with a newer timestamp.
func TestMVCCDeleteRangeOldTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()
			err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil)
			if err != nil {
				t.Fatal(err)
			}
			err = MVCCPut(ctx, engine, nil, testKey2, hlc.Timestamp{WallTime: 3}, value2, nil)
			if err != nil {
				t.Fatal(err)
			}
			err = MVCCDelete(ctx, engine, nil, testKey2, hlc.Timestamp{WallTime: 5}, nil)
			if err != nil {
				t.Fatal(err)
			}

			// Delete at a time before the tombstone. Should return a WriteTooOld error.
			b := engine.NewBatch()
			defer b.Close()
			keys, resume, keyCount, err := MVCCDeleteRange(
				ctx, b, nil, testKey1, testKey4, math.MaxInt64, hlc.Timestamp{WallTime: 4}, nil, true,
			)
			require.Nil(t, keys)
			require.Nil(t, resume)
			require.Equal(t, int64(0), keyCount)
			require.NotNil(t, err)
			require.IsType(t, (*roachpb.WriteTooOldError)(nil), err)

			// Delete at the same time as the tombstone. Should return a WriteTooOld error.
			b = engine.NewBatch()
			defer b.Close()
			keys, resume, keyCount, err = MVCCDeleteRange(
				ctx, b, nil, testKey1, testKey4, math.MaxInt64, hlc.Timestamp{WallTime: 5}, nil, true,
			)
			require.Nil(t, keys)
			require.Nil(t, resume)
			require.Equal(t, int64(0), keyCount)
			require.NotNil(t, err)
			require.IsType(t, (*roachpb.WriteTooOldError)(nil), err)

			// Delete at a time after the tombstone. Should succeed and should not
			// include the tombstone in the returned keys.
			b = engine.NewBatch()
			defer b.Close()
			keys, resume, keyCount, err = MVCCDeleteRange(
				ctx, b, nil, testKey1, testKey4, math.MaxInt64, hlc.Timestamp{WallTime: 6}, nil, true,
			)
			require.Equal(t, []roachpb.Key{testKey1}, keys)
			require.Nil(t, resume)
			require.Equal(t, int64(1), keyCount)
			require.NoError(t, err)
		})
	}
}

func TestMVCCDeleteRangeInline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
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
				if err := MVCCPut(ctx, engine, nil, kv.key, hlc.Timestamp{Logical: 0}, kv.value, nil); err != nil {
					t.Fatalf("%d: %+v", i, err)
				}
			}

			// Create one non-inline value (non-zero timestamp).
			if err := MVCCPut(ctx, engine, nil, testKey6, hlc.Timestamp{WallTime: 1}, value6, nil); err != nil {
				t.Fatal(err)
			}

			// Attempt to delete two inline keys, should succeed.
			deleted, resumeSpan, num, err := MVCCDeleteRange(
				ctx, engine, nil, testKey2, testKey6, 2, hlc.Timestamp{Logical: 0}, nil, true,
			)
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
			if _, _, _, err := MVCCDeleteRange(
				ctx, engine, nil, testKey1, testKey6, 1, hlc.Timestamp{WallTime: 2}, nil, true,
			); !testutils.IsError(err, inlineMismatchErrString) {
				t.Fatalf("got error %v, expected error with text '%s'", err, inlineMismatchErrString)
			}

			// Attempt to delete non-inline key at zero timestamp; should fail.
			const writeTooOldErrString = "WriteTooOldError"
			if _, _, _, err := MVCCDeleteRange(
				ctx, engine, nil, testKey6, keyMax, 1, hlc.Timestamp{Logical: 0}, nil, true,
			); !testutils.IsError(err, writeTooOldErrString) {
				t.Fatalf("got error %v, expected error with text '%s'", err, writeTooOldErrString)
			}

			// Attempt to delete inline keys in a transaction; should fail.
			if _, _, _, err := MVCCDeleteRange(
				ctx, engine, nil, testKey2, testKey6, 2, hlc.Timestamp{Logical: 0}, txn1, true,
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
		})
	}
}

func TestMVCCClearTimeRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {

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

			// setupKVs will generate an engine with the key-time space as follows:
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
			// This returns a new, populated engine since we can't just setup one and use
			// a new batch in each subtest, since batches don't reflect ClearRange results
			// when read.
			setupKVs := func(t *testing.T) Engine {
				engine := engineImpl.create()
				require.NoError(t, MVCCPut(ctx, engine, nil, testKey2, ts1, value1, nil))
				require.NoError(t, MVCCPut(ctx, engine, nil, testKey2, ts2, value2, nil))
				require.NoError(t, MVCCPut(ctx, engine, nil, testKey5, ts2, value2, nil))
				require.NoError(t, MVCCPut(ctx, engine, nil, testKey1, ts3, value3, nil))
				require.NoError(t, MVCCPut(ctx, engine, nil, testKey5, ts4, value4, nil))
				require.NoError(t, MVCCPut(ctx, engine, nil, testKey2, ts4, value4, nil))
				return engine
			}

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
				useTBI bool,
			) int {
				resume, err := MVCCClearTimeRange(ctx, rw, ms, key, endKey, ts, endTs, sz, byteLimit, useTBI)
				require.NoError(t, err)
				attempts := 1
				for resume != nil {
					resume, err = MVCCClearTimeRange(ctx, rw, ms, resume.Key, resume.EndKey, ts, endTs, sz, byteLimit, useTBI)
					require.NoError(t, err)
					attempts++
				}
				return attempts
			}
			for _, useTBI := range []bool{true, false} {
				t.Run(fmt.Sprintf("useTBI-%t", useTBI), func(t *testing.T) {
					t.Run("clear > ts0", func(t *testing.T) {
						e := setupKVs(t)
						defer e.Close()
						_, err := MVCCClearTimeRange(ctx, e, &enginepb.MVCCStats{}, localMax, keyMax, ts0, ts5, 10, 1<<10,
							useTBI)
						require.NoError(t, err)
						assertKVs(t, e, ts0, ts0Content)
						assertKVs(t, e, ts1, ts0Content)
						assertKVs(t, e, ts5, ts0Content)
					})

					t.Run("clear > ts1 ", func(t *testing.T) {
						e := setupKVs(t)
						defer e.Close()
						attempts := resumingClear(t, ctx, e, &enginepb.MVCCStats{}, localMax, keyMax, ts1, ts5, 10, kb, useTBI)
						require.Equal(t, 1, attempts)
						assertKVs(t, e, ts1, ts1Content)
						assertKVs(t, e, ts2, ts1Content)
						assertKVs(t, e, ts5, ts1Content)
					})
					t.Run("clear > ts1 count-size batch", func(t *testing.T) {
						e := setupKVs(t)
						defer e.Close()
						attempts := resumingClear(t, ctx, e, &enginepb.MVCCStats{}, localMax, keyMax, ts1, ts5, 1, kb,
							useTBI)
						require.Equal(t, 2, attempts)
						assertKVs(t, e, ts1, ts1Content)
						assertKVs(t, e, ts2, ts1Content)
						assertKVs(t, e, ts5, ts1Content)
					})

					t.Run("clear > ts1 byte-size batch", func(t *testing.T) {
						e := setupKVs(t)
						defer e.Close()
						attempts := resumingClear(t, ctx, e, &enginepb.MVCCStats{}, localMax, keyMax, ts1, ts5, 10, 1,
							useTBI)
						require.Equal(t, 2, attempts)
						assertKVs(t, e, ts1, ts1Content)
						assertKVs(t, e, ts2, ts1Content)
						assertKVs(t, e, ts5, ts1Content)
					})

					t.Run("clear > ts2", func(t *testing.T) {
						e := setupKVs(t)
						defer e.Close()
						attempts := resumingClear(t, ctx, e, &enginepb.MVCCStats{}, localMax, keyMax, ts2, ts5, 10, kb,
							useTBI)
						require.Equal(t, 1, attempts)
						assertKVs(t, e, ts2, ts2Content)
						assertKVs(t, e, ts5, ts2Content)
					})

					t.Run("clear > ts3", func(t *testing.T) {
						e := setupKVs(t)
						defer e.Close()
						resumingClear(t, ctx, e, &enginepb.MVCCStats{}, localMax, keyMax, ts3, ts5, 10, kb,
							useTBI)
						assertKVs(t, e, ts3, ts3Content)
						assertKVs(t, e, ts5, ts3Content)
					})

					t.Run("clear > ts4 (nothing) ", func(t *testing.T) {
						e := setupKVs(t)
						defer e.Close()
						_, err := MVCCClearTimeRange(ctx, e, &enginepb.MVCCStats{}, localMax, keyMax, ts4, ts5, 10, kb,
							useTBI)
						require.NoError(t, err)
						assertKVs(t, e, ts4, ts4Content)
						assertKVs(t, e, ts5, ts4Content)
					})

					t.Run("clear > ts5 (nothing)", func(t *testing.T) {
						e := setupKVs(t)
						defer e.Close()
						_, err := MVCCClearTimeRange(ctx, e, &enginepb.MVCCStats{}, localMax, keyMax, ts5, ts5, 10, kb,
							useTBI)
						require.NoError(t, err)
						assertKVs(t, e, ts4, ts4Content)
						assertKVs(t, e, ts5, ts4Content)
					})

					t.Run("clear up to k5 to ts0", func(t *testing.T) {
						e := setupKVs(t)
						defer e.Close()
						resumingClear(t, ctx, e, &enginepb.MVCCStats{}, testKey1, testKey5, ts0, ts5, 10, kb,
							useTBI)
						assertKVs(t, e, ts2, []roachpb.KeyValue{{Key: testKey5, Value: v2}})
						assertKVs(t, e, ts5, []roachpb.KeyValue{{Key: testKey5, Value: v4}})
					})

					t.Run("clear > ts0 in empty span (nothing)", func(t *testing.T) {
						e := setupKVs(t)
						defer e.Close()
						_, err := MVCCClearTimeRange(ctx, e, &enginepb.MVCCStats{}, testKey3, testKey5, ts0, ts5, 10, kb,
							useTBI)
						require.NoError(t, err)
						assertKVs(t, e, ts2, ts2Content)
						assertKVs(t, e, ts5, ts4Content)
					})

					t.Run("clear > ts0 in empty span [k3,k5) (nothing)", func(t *testing.T) {
						e := setupKVs(t)
						defer e.Close()
						_, err := MVCCClearTimeRange(ctx, e, &enginepb.MVCCStats{}, testKey3, testKey5, ts0, ts5, 10, 1<<10,
							useTBI)
						require.NoError(t, err)
						assertKVs(t, e, ts2, ts2Content)
						assertKVs(t, e, ts5, ts4Content)
					})

					t.Run("clear k3 and up in ts0 > x >= ts1 (nothing)", func(t *testing.T) {
						e := setupKVs(t)
						defer e.Close()
						_, err := MVCCClearTimeRange(ctx, e, &enginepb.MVCCStats{}, testKey3, keyMax, ts0, ts1, 10, 1<<10,
							useTBI)
						require.NoError(t, err)
						assertKVs(t, e, ts2, ts2Content)
						assertKVs(t, e, ts5, ts4Content)
					})

					// Add an intent at k3@ts3.
					txn := roachpb.MakeTransaction("test", nil, roachpb.NormalUserPriority, ts3, 1)
					setupKVsWithIntent := func(t *testing.T) Engine {
						e := setupKVs(t)
						require.NoError(t, MVCCPut(ctx, e, &enginepb.MVCCStats{}, testKey3, ts3, value3, &txn))
						return e
					}
					t.Run("clear everything hitting intent fails", func(t *testing.T) {
						e := setupKVsWithIntent(t)
						defer e.Close()
						_, err := MVCCClearTimeRange(ctx, e, &enginepb.MVCCStats{}, localMax, keyMax, ts0, ts5, 10, 1<<10,
							useTBI)
						require.EqualError(t, err, "conflicting intents on \"/db3\"")
					})

					t.Run("clear exactly hitting intent fails", func(t *testing.T) {
						e := setupKVsWithIntent(t)
						defer e.Close()
						_, err := MVCCClearTimeRange(ctx, e, &enginepb.MVCCStats{}, testKey3, testKey4, ts2, ts3, 10, 1<<10,
							useTBI)
						require.EqualError(t, err, "conflicting intents on \"/db3\"")
					})

					t.Run("clear everything above intent", func(t *testing.T) {
						e := setupKVsWithIntent(t)
						defer e.Close()
						resumingClear(t, ctx, e, &enginepb.MVCCStats{}, localMax, keyMax, ts3, ts5, 10, kb,
							useTBI)
						assertKVs(t, e, ts2, ts2Content)

						// Scan (< k3 to avoid intent) to confirm that k2 was indeed reverted to
						// value as of ts3 (i.e. v4 was cleared to expose v2).
						res, err := MVCCScan(ctx, e, localMax, testKey3, ts5, MVCCScanOptions{})
						require.NoError(t, err)
						require.Equal(t, ts3Content[:2], res.KVs)

						// Verify the intent was left alone.
						_, err = MVCCScan(ctx, e, testKey3, testKey4, ts5, MVCCScanOptions{})
						require.Error(t, err)

						// Scan (> k3 to avoid intent) to confirm that k5 was indeed reverted to
						// value as of ts3 (i.e. v4 was cleared to expose v2).
						res, err = MVCCScan(ctx, e, testKey4, keyMax, ts5, MVCCScanOptions{})
						require.NoError(t, err)
						require.Equal(t, ts3Content[2:], res.KVs)
					})

					t.Run("clear below intent", func(t *testing.T) {
						e := setupKVsWithIntent(t)
						defer e.Close()
						assertKVs(t, e, ts2, ts2Content)
						resumingClear(t, ctx, e, &enginepb.MVCCStats{}, localMax, keyMax, ts1, ts2, 10, kb,
							useTBI)
						assertKVs(t, e, ts2, ts1Content)
					})
				})
			}
		})
	}
}

func computeStats(
	t *testing.T, reader Reader, from, to roachpb.Key, nowNanos int64,
) enginepb.MVCCStats {
	t.Helper()
	iter := reader.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: to})
	defer iter.Close()
	s, err := ComputeStatsForRange(iter, from, to, nowNanos)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	return s
}

// TestMVCCClearTimeRangeOnRandomData sets up mostly random KVs and then picks
// some random times to which to revert, ensuring that a MVCC-Scan at each of
// those times before reverting matches the result of an MVCC-Scan done at a
// later time post-revert.
func TestMVCCClearTimeRangeOnRandomData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewPseudoRand()

	ctx := context.Background()

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
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
					require.NoError(t, MVCCDelete(ctx, e, &ms, key, hlc.Timestamp{WallTime: ts}, nil))
				} else {
					v := roachpb.MakeValueFromString(fmt.Sprintf("v-%d", i))
					require.NoError(t, MVCCPut(ctx, e, &ms, key, hlc.Timestamp{WallTime: ts}, v, nil))
				}
			}
			swathTime := rand.Intn(randTimeRange-100) + 100
			for i := swathStart; i < swathEnd; i++ {
				key := roachpb.Key(fmt.Sprintf("%05d", i))
				v := roachpb.MakeValueFromString(fmt.Sprintf("v-%d", i))
				require.NoError(t, MVCCPut(ctx, e, &ms, key, hlc.Timestamp{WallTime: int64(swathTime)}, v, nil))
			}

			// Add another swath of keys above to exercise an after-iteration range flush.
			for i := keyRange; i < keyRange+200; i++ {
				key := roachpb.Key(fmt.Sprintf("%05d", i))
				v := roachpb.MakeValueFromString(fmt.Sprintf("v-%d", i))
				require.NoError(t, MVCCPut(ctx, e, &ms, key, hlc.Timestamp{WallTime: int64(randTimeRange + 1)}, v, nil))
			}

			ms.AgeTo(2000)

			// Sanity check starting stats.
			require.Equal(t, computeStats(t, e, localMax, keyMax, 2000), ms)

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
			keyLen := int64(len(roachpb.Key(fmt.Sprintf("%05d", 1)))) + MVCCVersionTimestampSize
			maxAttempts := (numKVs * keyLen) / byteLimit
			var attempts int64
			for i := len(reverts) - 1; i >= 0; i-- {
				for _, useTBI := range []bool{false, true} {
					t.Run(fmt.Sprintf("useTBI-%t revert-%d", useTBI, i), func(t *testing.T) {
						revertTo := hlc.Timestamp{WallTime: int64(reverts[i])}
						// MVCC-Scan at the revert time.
						resBefore, err := MVCCScan(ctx, e, localMax, keyMax, revertTo, MVCCScanOptions{MaxKeys: numKVs})
						require.NoError(t, err)

						// Revert to the revert time.
						startKey := localMax
						for {
							attempts++
							resume, err := MVCCClearTimeRange(ctx, e, &ms, startKey, keyMax, revertTo, now,
								keyLimit, byteLimit, useTBI)
							require.NoError(t, err)
							if resume == nil {
								break
							}
							startKey = resume.Key
						}

						require.Equal(t, computeStats(t, e, localMax, keyMax, 2000), ms)
						// Scanning at "now" post-revert should yield the same result as scanning
						// at revert-time pre-revert.
						resAfter, err := MVCCScan(ctx, e, localMax, keyMax, now, MVCCScanOptions{MaxKeys: numKVs})
						require.NoError(t, err)
						require.Equal(t, resBefore.KVs, resAfter.KVs)
					})
				}
			}
			require.LessOrEqual(t, attempts, maxAttempts)
		})
	}
}

func TestMVCCInitPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			err := MVCCInitPut(ctx, engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, false, nil)
			if err != nil {
				t.Fatal(err)
			}

			// A repeat of the command will still succeed
			err = MVCCInitPut(ctx, engine, nil, testKey1, hlc.Timestamp{Logical: 2}, value1, false, nil)
			if err != nil {
				t.Fatal(err)
			}

			// Delete.
			err = MVCCDelete(ctx, engine, nil, testKey1, hlc.Timestamp{Logical: 3}, nil)
			if err != nil {
				t.Fatal(err)
			}

			// Reinserting the value fails if we fail on tombstones.
			err = MVCCInitPut(ctx, engine, nil, testKey1, hlc.Timestamp{Logical: 4}, value1, true, nil)
			if e := (*roachpb.ConditionFailedError)(nil); errors.As(err, &e) {
				if !bytes.Equal(e.ActualValue.RawBytes, nil) {
					t.Fatalf("the value %s in get result is not a tombstone", e.ActualValue.RawBytes)
				}
			} else if err == nil {
				t.Fatal("MVCCInitPut with a different value did not fail")
			} else {
				t.Fatalf("unexpected error %T", e)
			}

			// But doesn't if we *don't* fail on tombstones.
			err = MVCCInitPut(ctx, engine, nil, testKey1, hlc.Timestamp{Logical: 5}, value1, false, nil)
			if err != nil {
				t.Fatal(err)
			}

			// A repeat of the command with a different value will fail.
			err = MVCCInitPut(ctx, engine, nil, testKey1, hlc.Timestamp{Logical: 6}, value2, false, nil)
			if e := (*roachpb.ConditionFailedError)(nil); errors.As(err, &e) {
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
				value, _, err := MVCCGet(ctx, engine, testKey1, check.ts, MVCCGetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(value1.RawBytes, value.RawBytes) {
					t.Fatalf("the value %s in get result does not match the value %s in request",
						value1.RawBytes, value.RawBytes)
				}
				if value.Timestamp != check.expTS {
					t.Errorf("value at timestamp %s seen, expected %s", value.Timestamp, check.expTS)
				}
			}

			value, _, pErr := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{Logical: 0}, MVCCGetOptions{})
			if pErr != nil {
				t.Fatal(pErr)
			}
			if value != nil {
				t.Fatalf("%v present at old timestamp", value)
			}
		})
	}
}

func TestMVCCInitPutWithTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			clock := hlc.NewClock(hlc.NewManualClock(123).UnixNano, time.Nanosecond)

			txn := *txn1
			txn.Sequence++
			err := MVCCInitPut(ctx, engine, nil, testKey1, txn.ReadTimestamp, value1, false, &txn)
			if err != nil {
				t.Fatal(err)
			}

			// A repeat of the command will still succeed.
			txn.Sequence++
			err = MVCCInitPut(ctx, engine, nil, testKey1, txn.ReadTimestamp, value1, false, &txn)
			if err != nil {
				t.Fatal(err)
			}

			// A repeat of the command with a different value at a different epoch
			// will still succeed.
			txn.Sequence++
			txn.Epoch = 2
			err = MVCCInitPut(ctx, engine, nil, testKey1, txn.ReadTimestamp, value2, false, &txn)
			if err != nil {
				t.Fatal(err)
			}

			// Commit value3.
			txnCommit := txn
			txnCommit.Status = roachpb.COMMITTED
			txnCommit.WriteTimestamp = clock.Now().Add(1, 0)
			if _, err := MVCCResolveWriteIntent(ctx, engine, nil,
				roachpb.MakeLockUpdate(&txnCommit, roachpb.Span{Key: testKey1})); err != nil {
				t.Fatal(err)
			}

			// Write value4 with an old timestamp without txn...should get an error.
			err = MVCCInitPut(ctx, engine, nil, testKey1, clock.Now(), value4, false, nil)
			if e := (*roachpb.ConditionFailedError)(nil); errors.As(err, &e) {
				if !bytes.Equal(e.ActualValue.RawBytes, value2.RawBytes) {
					t.Fatalf("the value %s in get result does not match the value %s in request",
						e.ActualValue.RawBytes, value2.RawBytes)
				}
			} else {
				t.Fatalf("unexpected error %T", e)
			}
		})
	}
}

// TestMVCCReverseScan verifies that MVCCReverseScan scans [start,
// end) in descending order of keys.
func TestMVCCReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 2}, value2, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey2, hlc.Timestamp{WallTime: 1}, value3, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey2, hlc.Timestamp{WallTime: 3}, value4, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey3, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey4, hlc.Timestamp{WallTime: 1}, value2, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey5, hlc.Timestamp{WallTime: 3}, value5, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey6, hlc.Timestamp{WallTime: 3}, value6, nil); err != nil {
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
		})
	}
}

// TestMVCCReverseScanFirstKeyInFuture verifies that when MVCCReverseScan scans
// encounter a key with only future timestamps first, that it skips the key and
// continues to scan in reverse. #17825 was caused by this not working correctly.
func TestMVCCReverseScanFirstKeyInFuture(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			// The value at key2 will be at a lower timestamp than the ReverseScan, but
			// the value at key3 will be at a larger timetamp. The ReverseScan should
			// see key3 and ignore it because none of it versions are at a low enough
			// timestamp to read. It should then continue scanning backwards and find a
			// value at key2.
			//
			// Before fixing #17825, the MVCC version scan on key3 would fall out of the
			// scan bounds and if it never found another valid key before reaching
			// KeyMax, would stop the ReverseScan from continuing.
			if err := MVCCPut(ctx, engine, nil, testKey2, hlc.Timestamp{WallTime: 1}, value2, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey3, hlc.Timestamp{WallTime: 3}, value3, nil); err != nil {
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
		})
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
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			// 10 is the value of `kMaxItersBeforeSeek` at the time this test case was
			// written. Repeat the key enough times to make sure the `SeekForPrev()`
			// optimization will be used.
			for i := 1; i <= 10; i++ {
				if err := MVCCPut(ctx, engine, nil, testKey2, hlc.Timestamp{WallTime: int64(i)}, value2, nil); err != nil {
					t.Fatal(err)
				}
			}
			txn1ts := makeTxn(*txn1, hlc.Timestamp{WallTime: 11})
			if err := MVCCPut(ctx, engine, nil, testKey2, txn1ts.ReadTimestamp, value2, txn1ts); err != nil {
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
		})
	}
}

// Exposes a bug where the reverse MVCC scan can get stuck in an infinite loop until we OOM.
//
// The bug happened in this scenario.
// (1) reverse scan is positioned at the range's smallest key and calls `prevKey()`
// (2) `prevKey()` peeks and sees newer versions of the same logical key
//     `iters_before_seek_-1` times, moving the iterator backwards each time
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
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			run := func(numPuts int, ts int64) {
				ctx := context.Background()
				engine := engineImpl.create()
				defer engine.Close()

				for i := 1; i <= numPuts; i++ {
					if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: int64(i)}, value1, nil); err != nil {
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
		})
	}
}

func TestMVCCResolveTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			if err := MVCCPut(ctx, engine, nil, testKey1, txn1.ReadTimestamp, value1, txn1); err != nil {
				t.Fatal(err)
			}

			{
				value, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{Logical: 1}, MVCCGetOptions{
					Txn: txn1,
				})
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(value1.RawBytes, value.RawBytes) {
					t.Fatalf("the value %s in get result does not match the value %s in request",
						value1.RawBytes, value.RawBytes)
				}
			}

			// Resolve will write with txn1's timestamp which is 0,1.
			if _, err := MVCCResolveWriteIntent(ctx, engine, nil,
				roachpb.MakeLockUpdate(txn1Commit, roachpb.Span{Key: testKey1})); err != nil {
				t.Fatal(err)
			}

			{
				value, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{Logical: 1}, MVCCGetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(value1.RawBytes, value.RawBytes) {
					t.Fatalf("the value %s in get result does not match the value %s in request",
						value1.RawBytes, value.RawBytes)
				}
			}
		})
	}
}

// TestMVCCResolveNewerIntent verifies that resolving a newer intent
// than the committing transaction aborts the intent.
func TestMVCCResolveNewerIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			// Write first value.
			if err := MVCCPut(ctx, engine, nil, testKey1, txn1Commit.WriteTimestamp, value1, nil); err != nil {
				t.Fatal(err)
			}
			// Now, put down an intent which should return a write too old error
			// (but will still write the intent at tx1Commit.Timestmap+1.
			err := MVCCPut(ctx, engine, nil, testKey1, txn1.ReadTimestamp, value2, txn1)
			if !errors.HasType(err, (*roachpb.WriteTooOldError)(nil)) {
				t.Fatalf("expected write too old error; got %s", err)
			}

			// Resolve will succeed but should remove the intent.
			if _, err := MVCCResolveWriteIntent(ctx, engine, nil,
				roachpb.MakeLockUpdate(txn1Commit, roachpb.Span{Key: testKey1})); err != nil {
				t.Fatal(err)
			}

			value, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{Logical: 2}, MVCCGetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value1.RawBytes, value.RawBytes) {
				t.Fatalf("expected value1 bytes; got %q", value.RawBytes)
			}
		})
	}
}

func TestMVCCResolveIntentTxnTimestampMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			txn := txn1.Clone()
			tsEarly := txn.WriteTimestamp
			txn.TxnMeta.WriteTimestamp.Forward(tsEarly.Add(10, 0))

			// Write an intent which has txn.WriteTimestamp > meta.timestamp.
			if err := MVCCPut(ctx, engine, nil, testKey1, tsEarly, value1, txn); err != nil {
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
			if _, err := MVCCResolveWriteIntent(ctx, engine, nil, intent); err != nil {
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
				_, _, err := MVCCGet(ctx, engine, testKey1, test.Timestamp, MVCCGetOptions{})
				if errors.HasType(err, (*roachpb.WriteIntentError)(nil)) != test.found {
					t.Fatalf("%d: expected write intent error: %t, got %v", i, test.found, err)
				}
			}
		})
	}
}

// TestMVCCConditionalPutOldTimestamp tests a case where a conditional
// put with an older timestamp happens after a put with a newer timestamp.
//
// The conditional put uses the actual value at the timestamp as the
// basis for comparison first, and then may fail later with a
// WriteTooOldError if that timestamp isn't recent.
func TestMVCCConditionalPutOldTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()
			err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil)
			if err != nil {
				t.Fatal(err)
			}
			err = MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 3}, value2, nil)
			if err != nil {
				t.Fatal(err)
			}

			// Check nothing is written if the value doesn't match.
			err = MVCCConditionalPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 2}, value3, value1.TagAndDataBytes(), CPutFailIfMissing, nil)
			if err == nil {
				t.Errorf("unexpected success on conditional put")
			}
			if !errors.HasType(err, (*roachpb.ConditionFailedError)(nil)) {
				t.Errorf("unexpected error on conditional put: %+v", err)
			}

			// But if value does match the most recently written version, we'll get
			// a write too old error but still write updated value.
			err = MVCCConditionalPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 2}, value3, value2.TagAndDataBytes(), CPutFailIfMissing, nil)
			if err == nil {
				t.Errorf("unexpected success on conditional put")
			}
			if !errors.HasType(err, (*roachpb.WriteTooOldError)(nil)) {
				t.Errorf("unexpected error on conditional put: %+v", err)
			}
			// Verify new value was actually written at (3, 1).
			ts := hlc.Timestamp{WallTime: 3, Logical: 1}
			value, _, err := MVCCGet(ctx, engine, testKey1, ts, MVCCGetOptions{})
			if err != nil || value.Timestamp != ts || !bytes.Equal(value3.RawBytes, value.RawBytes) {
				t.Fatalf("expected err=nil (got %s), timestamp=%s (got %s), value=%q (got %q)",
					err, value.Timestamp, ts, value3.RawBytes, value.RawBytes)
			}
		})
	}
}

// TestMVCCMultiplePutOldTimestamp tests a case where multiple
// transactional Puts occur to the same key, but with older timestamps
// than a pre-existing key. The first should generate a
// WriteTooOldError and write at a higher timestamp. The second should
// avoid the WriteTooOldError but also write at the higher timestamp.
func TestMVCCMultiplePutOldTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 3}, value1, nil)
			if err != nil {
				t.Fatal(err)
			}

			// Verify the first txn Put returns a write too old error, but the
			// intent is written at the advanced timestamp.
			txn := makeTxn(*txn1, hlc.Timestamp{WallTime: 1})
			txn.Sequence++
			err = MVCCPut(ctx, engine, nil, testKey1, txn.ReadTimestamp, value2, txn)
			if !errors.HasType(err, (*roachpb.WriteTooOldError)(nil)) {
				t.Errorf("expected WriteTooOldError on Put; got %v", err)
			}
			// Verify new value was actually written at (3, 1).
			value, _, err := MVCCGet(ctx, engine, testKey1, hlc.MaxTimestamp, MVCCGetOptions{Txn: txn})
			if err != nil {
				t.Fatal(err)
			}
			expTS := hlc.Timestamp{WallTime: 3, Logical: 1}
			if value.Timestamp != expTS || !bytes.Equal(value2.RawBytes, value.RawBytes) {
				t.Fatalf("expected timestamp=%s (got %s), value=%q (got %q)",
					value.Timestamp, expTS, value2.RawBytes, value.RawBytes)
			}

			// Put again and verify no WriteTooOldError, but timestamp should continue
			// to be set to (3,1).
			txn.Sequence++
			err = MVCCPut(ctx, engine, nil, testKey1, txn.ReadTimestamp, value3, txn)
			if err != nil {
				t.Error(err)
			}
			// Verify new value was actually written at (3, 1).
			value, _, err = MVCCGet(ctx, engine, testKey1, hlc.MaxTimestamp, MVCCGetOptions{Txn: txn})
			if err != nil {
				t.Fatal(err)
			}
			if value.Timestamp != expTS || !bytes.Equal(value3.RawBytes, value.RawBytes) {
				t.Fatalf("expected timestamp=%s (got %s), value=%q (got %q)",
					value.Timestamp, expTS, value3.RawBytes, value.RawBytes)
			}
		})
	}
}

func TestMVCCPutNegativeTimestampError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			timestamp := hlc.Timestamp{WallTime: -1}
			expectedErrorString := fmt.Sprintf("cannot write to %q at timestamp %s", testKey1, timestamp)

			err := MVCCPut(ctx, engine, nil, testKey1, timestamp, value1, nil)

			require.EqualError(t, err, expectedErrorString)
		})
	}
}

// TestMVCCPutOldOrigTimestampNewCommitTimestamp tests a case where a
// transactional Put occurs to the same key, but with an older original
// timestamp than a pre-existing key. As always, this should result in a
// WriteTooOld error. However, in this case the transaction has a larger
// provisional commit timestamp than the pre-existing key. It should write
// its intent at this timestamp instead of directly above the existing key.
func TestMVCCPutOldOrigTimestampNewCommitTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 3}, value1, nil)
			if err != nil {
				t.Fatal(err)
			}

			// Perform a transactional Put with a transaction whose original timestamp is
			// below the existing key's timestamp and whose provisional commit timestamp
			// is above the existing key's timestamp.
			txn := makeTxn(*txn1, hlc.Timestamp{WallTime: 1})
			txn.WriteTimestamp = hlc.Timestamp{WallTime: 5}
			txn.Sequence++
			err = MVCCPut(ctx, engine, nil, testKey1, txn.ReadTimestamp, value2, txn)

			// Verify that the Put returned a WriteTooOld with the ActualTime set to the
			// transactions provisional commit timestamp.
			expTS := txn.WriteTimestamp
			if wtoErr := (*roachpb.WriteTooOldError)(nil); !errors.As(err, &wtoErr) || wtoErr.ActualTimestamp != expTS {
				t.Fatalf("expected WriteTooOldError with actual time = %s; got %s", expTS, wtoErr)
			}

			// Verify new value was actually written at the transaction's provisional
			// commit timestamp.
			value, _, err := MVCCGet(ctx, engine, testKey1, hlc.MaxTimestamp, MVCCGetOptions{Txn: txn})
			if err != nil {
				t.Fatal(err)
			}
			if value.Timestamp != expTS || !bytes.Equal(value2.RawBytes, value.RawBytes) {
				t.Fatalf("expected timestamp=%s (got %s), value=%q (got %q)",
					value.Timestamp, expTS, value2.RawBytes, value.RawBytes)
			}
		})
	}
}

func TestMVCCAbortTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			if err := MVCCPut(ctx, engine, nil, testKey1, txn1.ReadTimestamp, value1, txn1); err != nil {
				t.Fatal(err)
			}

			txn1AbortWithTS := txn1Abort.Clone()
			txn1AbortWithTS.WriteTimestamp = hlc.Timestamp{Logical: 1}

			if _, err := MVCCResolveWriteIntent(ctx, engine, nil,
				roachpb.MakeLockUpdate(txn1AbortWithTS, roachpb.Span{Key: testKey1}),
			); err != nil {
				t.Fatal(err)
			}

			if value, _, err := MVCCGet(
				ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{},
			); err != nil {
				t.Fatal(err)
			} else if value != nil {
				t.Fatalf("expected the value to be empty: %s", value)
			}
			if meta, err := engine.MVCCGet(mvccKey(testKey1)); err != nil {
				t.Fatal(err)
			} else if len(meta) != 0 {
				t.Fatalf("expected no more MVCCMetadata, got: %s", meta)
			}
		})
	}
}

func TestMVCCAbortTxnWithPreviousVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value2, nil); err != nil {
				t.Fatal(err)
			}
			txn1ts := makeTxn(*txn1, hlc.Timestamp{WallTime: 2})
			if err := MVCCPut(ctx, engine, nil, testKey1, txn1ts.ReadTimestamp, value3, txn1ts); err != nil {
				t.Fatal(err)
			}

			txn1AbortWithTS := txn1Abort.Clone()
			txn1AbortWithTS.WriteTimestamp = hlc.Timestamp{WallTime: 2}

			if _, err := MVCCResolveWriteIntent(ctx, engine, nil,
				roachpb.MakeLockUpdate(txn1AbortWithTS, roachpb.Span{Key: testKey1}),
			); err != nil {
				t.Fatal(err)
			}

			if meta, err := engine.MVCCGet(mvccKey(testKey1)); err != nil {
				t.Fatal(err)
			} else if len(meta) != 0 {
				t.Fatalf("expected no more MVCCMetadata, got: %s", meta)
			}

			if value, _, err := MVCCGet(
				ctx, engine, testKey1, hlc.Timestamp{WallTime: 3}, MVCCGetOptions{},
			); err != nil {
				t.Fatal(err)
			} else if expTS := (hlc.Timestamp{WallTime: 1}); value.Timestamp != expTS {
				t.Fatalf("expected timestamp %+v == %+v", value.Timestamp, expTS)
			} else if !bytes.Equal(value2.RawBytes, value.RawBytes) {
				t.Fatalf("the value %q in get result does not match the value %q in request",
					value.RawBytes, value2.RawBytes)
			}
		})
	}
}

func TestMVCCWriteWithDiffTimestampsAndEpochs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			// Start with epoch 1.
			txn := *txn1
			txn.Sequence++
			if err := MVCCPut(ctx, engine, nil, testKey1, txn.ReadTimestamp, value1, &txn); err != nil {
				t.Fatal(err)
			}
			// Now write with greater timestamp and epoch 2.
			txne2 := txn
			txne2.Sequence++
			txne2.Epoch = 2
			txne2.WriteTimestamp = hlc.Timestamp{WallTime: 1}
			if err := MVCCPut(ctx, engine, nil, testKey1, txne2.ReadTimestamp, value2, &txne2); err != nil {
				t.Fatal(err)
			}
			// Try a write with an earlier timestamp; this is just ignored.
			txne2.Sequence++
			txne2.WriteTimestamp = hlc.Timestamp{WallTime: 1}
			if err := MVCCPut(ctx, engine, nil, testKey1, txne2.ReadTimestamp, value1, &txne2); err != nil {
				t.Fatal(err)
			}
			// Try a write with an earlier epoch; again ignored.
			if err := MVCCPut(ctx, engine, nil, testKey1, txn.ReadTimestamp, value1, &txn); err == nil {
				t.Fatal("unexpected success of a write with an earlier epoch")
			}
			// Try a write with different value using both later timestamp and epoch.
			txne2.Sequence++
			if err := MVCCPut(ctx, engine, nil, testKey1, txne2.ReadTimestamp, value3, &txne2); err != nil {
				t.Fatal(err)
			}
			// Resolve the intent.
			txne2Commit := txne2
			txne2Commit.Status = roachpb.COMMITTED
			txne2Commit.WriteTimestamp = hlc.Timestamp{WallTime: 1}
			if _, err := MVCCResolveWriteIntent(ctx, engine, nil,
				roachpb.MakeLockUpdate(&txne2Commit, roachpb.Span{Key: testKey1})); err != nil {
				t.Fatal(err)
			}

			expTS := txne2Commit.WriteTimestamp.Next()

			// Now try writing an earlier value without a txn--should get WriteTooOldError.
			err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value4, nil)
			if wtoErr := (*roachpb.WriteTooOldError)(nil); !errors.As(err, &wtoErr) {
				t.Fatal("unexpected success")
			} else if wtoErr.ActualTimestamp != expTS {
				t.Fatalf("expected write too old error with actual ts %s; got %s", expTS, wtoErr.ActualTimestamp)
			}
			// Verify value was actually written at (1, 1).
			value, _, err := MVCCGet(ctx, engine, testKey1, expTS, MVCCGetOptions{})
			if err != nil || value.Timestamp != expTS || !bytes.Equal(value4.RawBytes, value.RawBytes) {
				t.Fatalf("expected err=nil (got %s), timestamp=%s (got %s), value=%q (got %q)",
					err, value.Timestamp, expTS, value4.RawBytes, value.RawBytes)
			}
			// Now write an intent with exactly the same timestamp--ties also get WriteTooOldError.
			err = MVCCPut(ctx, engine, nil, testKey1, txn2.ReadTimestamp, value5, txn2)
			intentTS := expTS.Next()
			if wtoErr := (*roachpb.WriteTooOldError)(nil); !errors.As(err, &wtoErr) {
				t.Fatal("unexpected success")
			} else if wtoErr.ActualTimestamp != intentTS {
				t.Fatalf("expected write too old error with actual ts %s; got %s", intentTS, wtoErr.ActualTimestamp)
			}
			// Verify intent value was actually written at (1, 2).
			value, _, err = MVCCGet(ctx, engine, testKey1, intentTS, MVCCGetOptions{Txn: txn2})
			if err != nil || value.Timestamp != intentTS || !bytes.Equal(value5.RawBytes, value.RawBytes) {
				t.Fatalf("expected err=nil (got %s), timestamp=%s (got %s), value=%q (got %q)",
					err, value.Timestamp, intentTS, value5.RawBytes, value.RawBytes)
			}
			// Attempt to read older timestamp; should fail.
			value, _, err = MVCCGet(ctx, engine, testKey1, hlc.Timestamp{Logical: 0}, MVCCGetOptions{})
			if value != nil || err != nil {
				t.Fatalf("expected value nil, err nil; got %+v, %v", value, err)
			}
			// Read at correct timestamp.
			value, _, err = MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if expTS := (hlc.Timestamp{WallTime: 1}); value.Timestamp != expTS {
				t.Fatalf("expected timestamp %+v == %+v", value.Timestamp, expTS)
			}
			if !bytes.Equal(value3.RawBytes, value.RawBytes) {
				t.Fatalf("the value %s in get result does not match the value %s in request",
					value3.RawBytes, value.RawBytes)
			}
		})
	}
}

// TestMVCCGetWithDiffEpochs writes a value first using epoch 1, then
// reads using epoch 2 to verify that values written during different
// transaction epochs are not visible.
func TestMVCCGetWithDiffEpochs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			ctx := context.Background()
			engine := engineImpl.create()
			defer engine.Close()

			// Write initial value without a txn.
			if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, nil); err != nil {
				t.Fatal(err)
			}
			// Now write using txn1, epoch 1.
			txn1ts := makeTxn(*txn1, hlc.Timestamp{WallTime: 1})
			if err := MVCCPut(ctx, engine, nil, testKey1, txn1ts.ReadTimestamp, value2, txn1ts); err != nil {
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
					value, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 2}, MVCCGetOptions{
						Txn: test.txn,
					})
					if test.expErr {
						if err == nil {
							t.Errorf("test %d: unexpected success", i)
						} else if !errors.HasType(err, (*roachpb.WriteIntentError)(nil)) {
							t.Errorf("test %d: expected write intent error; got %v", i, err)
						}
					} else if err != nil || value == nil || !bytes.Equal(test.expValue.RawBytes, value.RawBytes) {
						t.Errorf("test %d: expected value %q, err nil; got %+v, %v", i, test.expValue.RawBytes, value, err)
					}
				})
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

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			ctx := context.Background()
			engine := engineImpl.create()
			defer engine.Close()

			// Write initial value without a txn at timestamp 1.
			if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
				t.Fatal(err)
			}
			// Write another value without a txn at timestamp 3.
			if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{WallTime: 3}, value2, nil); err != nil {
				t.Fatal(err)
			}
			// Now write using txn1, epoch 1.
			txn1ts := makeTxn(*txn1, hlc.Timestamp{WallTime: 1})
			// Bump epoch 1's write timestamp to timestamp 4.
			txn1ts.WriteTimestamp = hlc.Timestamp{WallTime: 4}
			// Expected to hit WriteTooOld error but to still lay down intent.
			err := MVCCPut(ctx, engine, nil, testKey1, txn1ts.ReadTimestamp, value3, txn1ts)
			if wtoErr := (*roachpb.WriteTooOldError)(nil); !errors.As(err, &wtoErr) {
				t.Fatalf("unexpectedly not WriteTooOld: %+v", err)
			} else if expTS, actTS := txn1ts.WriteTimestamp, wtoErr.ActualTimestamp; expTS != actTS {
				t.Fatalf("expected write too old error with actual ts %s; got %s", expTS, actTS)
			}
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
					value, _, err := MVCCGet(ctx, engine, testKey1, test.readTS, MVCCGetOptions{Txn: test.txn})
					if err != nil || value == nil || !bytes.Equal(test.expValue.RawBytes, value.RawBytes) {
						t.Errorf("test %d: expected value %q, err nil; got %+v, %v", i, test.expValue.RawBytes, value, err)
					}
				})
			}
		})
	}
}

// TestMVCCGetWithOldEpoch writes a value first using epoch 2, then
// reads using epoch 1 to verify that the read will fail.
func TestMVCCGetWithOldEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			ctx := context.Background()
			engine := engineImpl.create()
			defer engine.Close()

			if err := MVCCPut(ctx, engine, nil, testKey1, txn1e2.ReadTimestamp, value2, txn1e2); err != nil {
				t.Fatal(err)
			}
			_, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 2}, MVCCGetOptions{
				Txn: txn1,
			})
			if err == nil {
				t.Fatalf("unexpected success of get")
			}
		})
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
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
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
						if err := MVCCPut(ctx, engine, nil, key, txn.WriteTimestamp, value1, &txn); err != nil {
							t.Fatal(err)
						}
					}

					// Perform the initial DeleteRange.
					const origSeq = 6
					txn.Sequence = origSeq
					origDeleted, _, origNum, err := MVCCDeleteRange(
						ctx, engine, nil, prefix, prefix.PrefixEnd(), math.MaxInt64, txn.WriteTimestamp, &txn, true,
					)
					if err != nil {
						t.Fatal(err)
					}

					txn.Sequence = tc.sequence
					deleted, _, num, err := MVCCDeleteRange(
						ctx, engine, nil, prefix, prefix.PrefixEnd(), math.MaxInt64, txn.WriteTimestamp, &txn, true,
					)
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

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			ctx := context.Background()
			engine := engineImpl.create()
			defer engine.Close()

			// Start with epoch 1.
			if err := MVCCPut(ctx, engine, nil, testKey1, txn1.ReadTimestamp, value1, txn1); err != nil {
				t.Fatal(err)
			}
			// Resolve the intent, pushing its timestamp forward.
			txn := makeTxn(*txn1, hlc.Timestamp{WallTime: 1})
			if _, err := MVCCResolveWriteIntent(ctx, engine, nil,
				roachpb.MakeLockUpdate(txn, roachpb.Span{Key: testKey1})); err != nil {
				t.Fatal(err)
			}
			// Attempt to read using naive txn's previous timestamp.
			value, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{Logical: 1}, MVCCGetOptions{
				Txn: txn1,
			})
			if err != nil || value == nil || !bytes.Equal(value.RawBytes, value1.RawBytes) {
				t.Errorf("expected value %q, err nil; got %+v, %v", value1.RawBytes, value, err)
			}
		})
	}
}

func TestMVCCResolveWithDiffEpochs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			if err := MVCCPut(ctx, engine, nil, testKey1, txn1.ReadTimestamp, value1, txn1); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey2, txn1e2.ReadTimestamp, value2, txn1e2); err != nil {
				t.Fatal(err)
			}
			num, _, err := MVCCResolveWriteIntentRange(ctx, engine, nil,
				roachpb.MakeLockUpdate(txn1e2Commit, roachpb.Span{Key: testKey1, EndKey: testKey2.Next()}), 2)
			if err != nil {
				t.Fatal(err)
			}
			if num != 2 {
				t.Errorf("expected 2 rows resolved; got %d", num)
			}

			// Verify key1 is empty, as resolution with epoch 2 would have
			// aborted the epoch 1 intent.
			value, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{Logical: 1}, MVCCGetOptions{})
			if value != nil || err != nil {
				t.Errorf("expected value nil, err nil; got %+v, %v", value, err)
			}

			// Key2 should be committed.
			value, _, err = MVCCGet(ctx, engine, testKey2, hlc.Timestamp{Logical: 1}, MVCCGetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value2.RawBytes, value.RawBytes) {
				t.Fatalf("the value %s in get result does not match the value %s in request",
					value2.RawBytes, value.RawBytes)
			}
		})
	}
}

func TestMVCCResolveWithUpdatedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			if err := MVCCPut(ctx, engine, nil, testKey1, txn1.ReadTimestamp, value1, txn1); err != nil {
				t.Fatal(err)
			}

			value, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{
				Txn: txn1,
			})
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value1.RawBytes, value.RawBytes) {
				t.Fatalf("the value %s in get result does not match the value %s in request",
					value1.RawBytes, value.RawBytes)
			}

			// Resolve with a higher commit timestamp -- this should rewrite the
			// intent when making it permanent.
			txn := makeTxn(*txn1Commit, hlc.Timestamp{WallTime: 1})
			if _, err = MVCCResolveWriteIntent(ctx, engine, nil,
				roachpb.MakeLockUpdate(txn, roachpb.Span{Key: testKey1})); err != nil {
				t.Fatal(err)
			}

			value, _, err = MVCCGet(ctx, engine, testKey1, hlc.Timestamp{Logical: 1}, MVCCGetOptions{})
			if value != nil || err != nil {
				t.Fatalf("expected both value and err to be nil: %+v, %v", value, err)
			}

			value, _, err = MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{})
			if err != nil {
				t.Error(err)
			}
			if expTS := (hlc.Timestamp{WallTime: 1}); value.Timestamp != expTS {
				t.Fatalf("expected timestamp %+v == %+v", value.Timestamp, expTS)
			}
			if !bytes.Equal(value1.RawBytes, value.RawBytes) {
				t.Fatalf("the value %s in get result does not match the value %s in request",
					value1.RawBytes, value.RawBytes)
			}
		})
	}
}

func TestMVCCResolveWithPushedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			if err := MVCCPut(ctx, engine, nil, testKey1, txn1.ReadTimestamp, value1, txn1); err != nil {
				t.Fatal(err)
			}
			value, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{
				Txn: txn1,
			})
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value1.RawBytes, value.RawBytes) {
				t.Fatalf("the value %s in get result does not match the value %s in request",
					value1.RawBytes, value.RawBytes)
			}

			// Resolve with a higher commit timestamp, but with still-pending transaction.
			// This represents a straightforward push (i.e. from a read/write conflict).
			txn := makeTxn(*txn1, hlc.Timestamp{WallTime: 1})
			if _, err = MVCCResolveWriteIntent(ctx, engine, nil,
				roachpb.MakeLockUpdate(txn, roachpb.Span{Key: testKey1})); err != nil {
				t.Fatal(err)
			}

			value, _, err = MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{})
			if value != nil || err == nil {
				t.Fatalf("expected both value nil and err to be a writeIntentError: %+v", value)
			}

			// Can still fetch the value using txn1.
			value, _, err = MVCCGet(ctx, engine, testKey1, hlc.Timestamp{WallTime: 1}, MVCCGetOptions{
				Txn: txn1,
			})
			if err != nil {
				t.Error(err)
			}
			if expTS := (hlc.Timestamp{WallTime: 1}); value.Timestamp != expTS {
				t.Fatalf("expected timestamp %+v == %+v", value.Timestamp, expTS)
			}
			if !bytes.Equal(value1.RawBytes, value.RawBytes) {
				t.Fatalf("the value %s in get result does not match the value %s in request",
					value1.RawBytes, value.RawBytes)
			}
		})
	}
}

func TestMVCCResolveTxnNoOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			// Resolve a non existent key; noop.
			if _, err := MVCCResolveWriteIntent(ctx, engine, nil,
				roachpb.MakeLockUpdate(txn1Commit, roachpb.Span{Key: testKey1})); err != nil {
				t.Fatal(err)
			}

			// Add key and resolve despite there being no intent.
			if err := MVCCPut(ctx, engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := MVCCResolveWriteIntent(ctx, engine, nil,
				roachpb.MakeLockUpdate(txn2Commit, roachpb.Span{Key: testKey1})); err != nil {
				t.Fatal(err)
			}

			// Write intent and resolve with different txn.
			if err := MVCCPut(ctx, engine, nil, testKey2, txn1.ReadTimestamp, value2, txn1); err != nil {
				t.Fatal(err)
			}

			txn1CommitWithTS := txn2Commit.Clone()
			txn1CommitWithTS.WriteTimestamp = hlc.Timestamp{WallTime: 1}
			if _, err := MVCCResolveWriteIntent(ctx, engine, nil,
				roachpb.MakeLockUpdate(txn1CommitWithTS, roachpb.Span{Key: testKey2})); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMVCCResolveTxnRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			if err := MVCCPut(ctx, engine, nil, testKey1, txn1.ReadTimestamp, value1, txn1); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey2, hlc.Timestamp{Logical: 1}, value2, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey3, txn2.ReadTimestamp, value3, txn2); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(ctx, engine, nil, testKey4, txn1.ReadTimestamp, value4, txn1); err != nil {
				t.Fatal(err)
			}

			num, resumeSpan, err := MVCCResolveWriteIntentRange(ctx, engine, nil,
				roachpb.MakeLockUpdate(txn1Commit, roachpb.Span{Key: testKey1, EndKey: testKey4.Next()}),
				math.MaxInt64)
			if err != nil {
				t.Fatal(err)
			}
			if num != 2 || resumeSpan != nil {
				t.Fatalf("expected all keys to process for resolution, even though 2 are noops; got %d, resume=%s",
					num, resumeSpan)
			}

			{
				value, _, err := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{Logical: 1}, MVCCGetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(value1.RawBytes, value.RawBytes) {
					t.Fatalf("the value %s in get result does not match the value %s in request",
						value1.RawBytes, value.RawBytes)
				}
			}
			{
				value, _, err := MVCCGet(ctx, engine, testKey2, hlc.Timestamp{Logical: 1}, MVCCGetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(value2.RawBytes, value.RawBytes) {
					t.Fatalf("the value %s in get result does not match the value %s in request",
						value2.RawBytes, value.RawBytes)
				}
			}
			{
				value, _, err := MVCCGet(ctx, engine, testKey3, hlc.Timestamp{Logical: 1}, MVCCGetOptions{
					Txn: txn2,
				})
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(value3.RawBytes, value.RawBytes) {
					t.Fatalf("the value %s in get result does not match the value %s in request",
						value3.RawBytes, value.RawBytes)
				}
			}
			{
				value, _, err := MVCCGet(ctx, engine, testKey4, hlc.Timestamp{Logical: 1}, MVCCGetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(value4.RawBytes, value.RawBytes) {
					t.Fatalf("the value %s in get result does not match the value %s in request",
						value1.RawBytes, value.RawBytes)
				}
			}
		})
	}
}

func TestMVCCResolveTxnRangeResume(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			// Write 10 keys from txn1, 10 from txn2, and 10 with no txn, interleaved.
			for i := 0; i < 30; i += 3 {
				key0 := roachpb.Key(fmt.Sprintf("%02d", i+0))
				key1 := roachpb.Key(fmt.Sprintf("%02d", i+1))
				key2 := roachpb.Key(fmt.Sprintf("%02d", i+2))
				if err := MVCCPut(ctx, engine, nil, key0, txn1.ReadTimestamp, value1, txn1); err != nil {
					t.Fatal(err)
				}
				txn2ts := makeTxn(*txn2, hlc.Timestamp{Logical: 2})
				if err := MVCCPut(ctx, engine, nil, key1, txn2ts.ReadTimestamp, value2, txn2ts); err != nil {
					t.Fatal(err)
				}
				if err := MVCCPut(ctx, engine, nil, key2, hlc.Timestamp{Logical: 3}, value3, nil); err != nil {
					t.Fatal(err)
				}
			}

			// Resolve up to 5 intents.
			num, resumeSpan, err := MVCCResolveWriteIntentRange(ctx, engine, nil,
				roachpb.MakeLockUpdate(txn1Commit, roachpb.Span{Key: roachpb.Key("00"), EndKey: roachpb.Key("30")}),
				5)
			if err != nil {
				t.Fatal(err)
			}
			if num != 5 || resumeSpan == nil {
				t.Errorf("expected resolution for only 5 keys; got %d, resume=%s", num, resumeSpan)
			}
			expResumeSpan := roachpb.Span{Key: roachpb.Key("12").Next(), EndKey: roachpb.Key("30")}
			if !resumeSpan.Equal(expResumeSpan) {
				t.Errorf("expected resume span %s; got %s", expResumeSpan, resumeSpan)
			}
		})
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
		{keys.SystemSQLCodec.TablePrefix(keys.MaxSystemConfigDescID), false},
		{keys.SystemSQLCodec.TablePrefix(keys.MaxSystemConfigDescID + 1), true},
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
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
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
				if err := MVCCPut(ctx, engine, ms, []byte(k), hlc.Timestamp{Logical: 1}, val, nil); err != nil {
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
		})
	}
}

// TestFindValidSplitKeys verifies split keys are located such that
// they avoid splits through invalid key ranges.
func TestFindValidSplitKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const userID = keys.MinUserDescID
	const interleave1 = userID + 1
	const interleave2 = userID + 2
	const interleave3 = userID + 3
	// Manually creates rows corresponding to the schema:
	// CREATE TABLE t (id1 STRING, id2 STRING, ... PRIMARY KEY (id1, id2, ...))
	addTablePrefix := func(prefix roachpb.Key, id uint32, rowVals ...string) roachpb.Key {
		tableKey := append(prefix, keys.SystemSQLCodec.TablePrefix(id)...)
		rowKey := roachpb.Key(encoding.EncodeVarintAscending(tableKey, 1))
		for _, rowVal := range rowVals {
			rowKey = encoding.EncodeStringAscending(rowKey, rowVal)
		}
		return rowKey
	}
	tablePrefix := func(id uint32, rowVals ...string) roachpb.Key {
		return addTablePrefix(nil, id, rowVals...)
	}
	addColFam := func(rowKey roachpb.Key, colFam uint32) roachpb.Key {
		return keys.MakeFamilyKey(append([]byte(nil), rowKey...), colFam)
	}
	addInterleave := func(rowKey roachpb.Key) roachpb.Key {
		return encoding.EncodeInterleavedSentinel(rowKey)
	}

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
		// All system span cannot be split.
		{
			keys: []roachpb.Key{
				addColFam(tablePrefix(1, "some", "data"), 1),
				addColFam(tablePrefix(keys.MaxSystemConfigDescID, "blah"), 1),
			},
			rangeStart: keys.SystemSQLCodec.TablePrefix(1),
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
				addColFam(tablePrefix(userID, "a"), 1),
				addColFam(tablePrefix(userID, "a"), 2),
				addColFam(tablePrefix(userID, "a"), 3),
				addColFam(tablePrefix(userID, "a"), 4),
				addColFam(tablePrefix(userID, "a"), 5),
				addColFam(tablePrefix(userID, "b"), 1),
				addColFam(tablePrefix(userID, "c"), 1),
			},
			rangeStart: tablePrefix(userID, "a"),
			expSplit:   tablePrefix(userID, "b"),
			expError:   false,
		},
		// More example table data. Make sure ranges at the start of a table can
		// be split properly - this checks that the minSplitKey logic doesn't
		// break for such ranges.
		{
			keys: []roachpb.Key{
				addColFam(tablePrefix(userID, "a"), 1),
				addColFam(tablePrefix(userID, "b"), 1),
				addColFam(tablePrefix(userID, "c"), 1),
				addColFam(tablePrefix(userID, "d"), 1),
			},
			rangeStart: keys.SystemSQLCodec.TablePrefix(userID),
			expSplit:   tablePrefix(userID, "c"),
			expError:   false,
		},
		// More example table data. Make sure ranges at the start of a table can
		// be split properly even in the presence of a large first row.
		{
			keys: []roachpb.Key{
				addColFam(tablePrefix(userID, "a"), 1),
				addColFam(tablePrefix(userID, "a"), 2),
				addColFam(tablePrefix(userID, "a"), 3),
				addColFam(tablePrefix(userID, "a"), 4),
				addColFam(tablePrefix(userID, "a"), 5),
				addColFam(tablePrefix(userID, "b"), 1),
				addColFam(tablePrefix(userID, "c"), 1),
			},
			rangeStart: keys.SystemSQLCodec.TablePrefix(keys.MinUserDescID),
			expSplit:   tablePrefix(userID, "b"),
			expError:   false,
		},
		// One partition where partition key is the first column. Checks that
		// split logic is not confused by the special partition start key.
		{
			keys: []roachpb.Key{
				addColFam(tablePrefix(userID, "a", "a"), 1),
				addColFam(tablePrefix(userID, "a", "b"), 1),
				addColFam(tablePrefix(userID, "a", "c"), 1),
				addColFam(tablePrefix(userID, "a", "d"), 1),
			},
			rangeStart: tablePrefix(userID, "a"),
			expSplit:   tablePrefix(userID, "a", "c"),
			expError:   false,
		},
		// One partition with a large first row. Checks that our logic to avoid
		// splitting in the middle of a row still applies.
		{
			keys: []roachpb.Key{
				addColFam(tablePrefix(userID, "a", "a"), 1),
				addColFam(tablePrefix(userID, "a", "a"), 2),
				addColFam(tablePrefix(userID, "a", "a"), 3),
				addColFam(tablePrefix(userID, "a", "a"), 4),
				addColFam(tablePrefix(userID, "a", "a"), 5),
				addColFam(tablePrefix(userID, "a", "b"), 1),
				addColFam(tablePrefix(userID, "a", "c"), 1),
			},
			rangeStart: tablePrefix(userID, "a"),
			expSplit:   tablePrefix(userID, "a", "b"),
			expError:   false,
		},
		// One large first row with interleaved child rows. Check that we can
		// split before the first interleaved row.
		{
			keys: []roachpb.Key{
				addColFam(tablePrefix(userID, "a"), 0),
				addColFam(tablePrefix(userID, "a"), 1),
				addColFam(tablePrefix(userID, "a"), 2),
				addColFam(addTablePrefix(addInterleave(tablePrefix(userID, "a")), interleave1, "b"), 0),
				addColFam(addTablePrefix(addInterleave(tablePrefix(userID, "a")), interleave1, "b"), 1),
				addColFam(addTablePrefix(addInterleave(tablePrefix(userID, "a")), interleave2, "c"), 1),
			},
			rangeStart: tablePrefix(userID, "a"),
			expSplit:   addTablePrefix(addInterleave(tablePrefix(userID, "a")), interleave1, "b"),
			expError:   false,
		},
		// One large first row with a double interleaved child row. Check that
		// we can split before the double interleaved row.
		{
			keys: []roachpb.Key{
				addColFam(tablePrefix(userID, "a"), 0),
				addColFam(tablePrefix(userID, "a"), 1),
				addColFam(tablePrefix(userID, "a"), 2),
				addColFam(addTablePrefix(addInterleave(
					addTablePrefix(addInterleave(tablePrefix(userID, "a")), interleave1, "b"),
				), interleave2, "d"), 3),
			},
			rangeStart: tablePrefix(userID, "a"),
			expSplit: addTablePrefix(addInterleave(
				addTablePrefix(addInterleave(tablePrefix(userID, "a")), interleave1, "b"),
			), interleave2, "d"),
			expError: false,
		},
		// Two interleaved rows. Check that we can split between them.
		{
			keys: []roachpb.Key{
				addColFam(addTablePrefix(addInterleave(tablePrefix(userID, "a")), interleave1, "b"), 0),
				addColFam(addTablePrefix(addInterleave(tablePrefix(userID, "a")), interleave1, "b"), 1),
				addColFam(addTablePrefix(addInterleave(tablePrefix(userID, "a")), interleave2, "c"), 3),
				addColFam(addTablePrefix(addInterleave(tablePrefix(userID, "a")), interleave2, "c"), 4),
			},
			rangeStart: addTablePrefix(addInterleave(tablePrefix(userID, "a")), interleave1, "b"),
			expSplit:   addTablePrefix(addInterleave(tablePrefix(userID, "a")), interleave2, "c"),
			expError:   false,
		},
		// Two small rows with interleaved child rows after the second. Check
		// that we can split before the first interleaved row.
		{
			keys: []roachpb.Key{
				addColFam(tablePrefix(userID, "a"), 0),
				addColFam(tablePrefix(userID, "b"), 0),
				addColFam(addTablePrefix(addInterleave(tablePrefix(userID, "b")), interleave1, "b"), 0),
				addColFam(addTablePrefix(addInterleave(tablePrefix(userID, "b")), interleave1, "b"), 1),
				addColFam(addTablePrefix(addInterleave(tablePrefix(userID, "b")), interleave2, "c"), 1),
			},
			rangeStart: tablePrefix(userID, "a"),
			expSplit:   addTablePrefix(addInterleave(tablePrefix(userID, "b")), interleave1, "b"),
			expError:   false,
		},
		// A chain of interleaved rows. Check that we can split them.
		{
			keys: []roachpb.Key{
				addColFam(tablePrefix(userID, "a"), 0),
				addColFam(addTablePrefix(addInterleave(tablePrefix(userID, "a")), interleave1, "b"), 0),
				addColFam(addTablePrefix(addInterleave(
					addTablePrefix(addInterleave(tablePrefix(userID, "a")), interleave1, "b"),
				), interleave2, "c"), 0),
				addColFam(addTablePrefix(addInterleave(
					addTablePrefix(addInterleave(
						addTablePrefix(addInterleave(tablePrefix(userID, "a")), interleave1, "b"),
					), interleave2, "c"),
				), interleave3, "d"), 0),
			},
			rangeStart: tablePrefix(userID, "a"),
			expSplit: addTablePrefix(addInterleave(
				addTablePrefix(addInterleave(tablePrefix(userID, "a")), interleave1, "b"),
			), interleave2, "c"),
			expError: false,
		},
	}

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
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
						engine := engineImpl.create()
						defer engine.Close()

						ms := &enginepb.MVCCStats{}
						val := roachpb.MakeValueFromString(strings.Repeat("X", 10))
						for _, k := range test.keys {
							// Add three MVCC versions of every key. Splits are not allowed
							// between MVCC versions, so this shouldn't have any effect.
							for j := 1; j <= 3; j++ {
								ts := hlc.Timestamp{Logical: int32(j)}
								if err := MVCCPut(ctx, engine, ms, []byte(k), ts, val, nil); err != nil {
									t.Fatal(err)
								}
							}
						}
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
		})
	}
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

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			for i, test := range testCases {
				t.Run("", func(t *testing.T) {
					ctx := context.Background()
					engine := engineImpl.create()
					defer engine.Close()

					ms := &enginepb.MVCCStats{}
					var expKey roachpb.Key
					for j, keySize := range test.keySizes {
						key := roachpb.Key(fmt.Sprintf("%d%s", j, strings.Repeat("X", keySize)))
						if test.expSplit == j {
							expKey = key
						}
						val := roachpb.MakeValueFromString(strings.Repeat("X", test.valSizes[j]))
						if err := MVCCPut(ctx, engine, ms, key, hlc.Timestamp{Logical: 1}, val, nil); err != nil {
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
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			ms := &enginepb.MVCCStats{}

			bytes := []byte("value")
			ts1 := hlc.Timestamp{WallTime: 1e9}
			ts2 := hlc.Timestamp{WallTime: 2e9}
			ts3 := hlc.Timestamp{WallTime: 3e9}
			val1 := roachpb.MakeValueFromBytesAndTimestamp(bytes, ts1)
			val2 := roachpb.MakeValueFromBytesAndTimestamp(bytes, ts2)
			val3 := roachpb.MakeValueFromBytesAndTimestamp(bytes, ts3)
			valInline := roachpb.MakeValueFromBytesAndTimestamp(bytes, hlc.Timestamp{})

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
			}

			for i := 0; i < 3; i++ {
				for _, test := range testData {
					if i >= len(test.vals) {
						continue
					}
					for _, val := range test.vals[i : i+1] {
						if i == len(test.vals)-1 && test.isDeleted {
							if err := MVCCDelete(ctx, engine, ms, test.key, val.Timestamp, nil); err != nil {
								t.Fatal(err)
							}
							continue
						}
						valCpy := *protoutil.Clone(&val).(*roachpb.Value)
						valCpy.Timestamp = hlc.Timestamp{}
						if err := MVCCPut(ctx, engine, ms, test.key, val.Timestamp, valCpy, nil); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			if log.V(1) {
				kvsn, err := Scan(engine, localMax, keyMax, 0)
				if err != nil {
					t.Fatal(err)
				}
				for i, kv := range kvsn {
					log.Infof(context.Background(), "%d: %s", i, kv.Key)
				}
			}

			keys := []roachpb.GCRequest_GCKey{
				{Key: roachpb.Key("a"), Timestamp: ts1},
				{Key: roachpb.Key("a-del"), Timestamp: ts2},
				{Key: roachpb.Key("b"), Timestamp: ts1},
				{Key: roachpb.Key("b-del"), Timestamp: ts2},
				{Key: roachpb.Key("inline"), Timestamp: hlc.Timestamp{}},
				// Keys that don't exist, which should result in a no-op.
				{Key: roachpb.Key("a-bad"), Timestamp: ts2},
				{Key: roachpb.Key("inline-bad"), Timestamp: hlc.Timestamp{}},
			}
			if err := MVCCGarbageCollect(
				context.Background(), engine, ms, keys, ts3,
			); err != nil {
				t.Fatal(err)
			}

			expEncKeys := []MVCCKey{
				mvccVersionKey(roachpb.Key("a"), ts2),
				mvccVersionKey(roachpb.Key("b"), ts3),
				mvccVersionKey(roachpb.Key("b"), ts2),
				mvccVersionKey(roachpb.Key("b-del"), ts3),
			}
			kvs, err := Scan(engine, localMax, keyMax, 0)
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
			iter := engine.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
			defer iter.Close()
			for _, mvccStatsTest := range mvccStatsTests {
				t.Run(mvccStatsTest.name, func(t *testing.T) {
					expMS, err := mvccStatsTest.fn(iter, localMax, roachpb.KeyMax, ts3.WallTime)
					if err != nil {
						t.Fatal(err)
					}
					assertEq(t, engine, "verification", ms, &expMS)
				})
			}
		})
	}
}

// TestMVCCGarbageCollectNonDeleted verifies that the first value for
// a key cannot be GC'd if it's not deleted.
func TestMVCCGarbageCollectNonDeleted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
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
					if err := MVCCPut(ctx, engine, nil, test.key, val.Timestamp, valCpy, nil); err != nil {
						t.Fatal(err)
					}
				}
				keys := []roachpb.GCRequest_GCKey{
					{Key: test.key, Timestamp: ts2},
				}
				err := MVCCGarbageCollect(ctx, engine, nil, keys, ts2)
				if !testutils.IsError(err, test.expError) {
					t.Fatalf("expected error %q when garbage collecting a non-deleted live value, found %v",
						test.expError, err)
				}
			}
		})
	}
}

// TestMVCCGarbageCollectIntent verifies that an intent cannot be GC'd.
func TestMVCCGarbageCollectIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			bytes := []byte("value")
			ts1 := hlc.Timestamp{WallTime: 1e9}
			ts2 := hlc.Timestamp{WallTime: 2e9}
			key := roachpb.Key("a")
			{
				val1 := roachpb.MakeValueFromBytes(bytes)
				if err := MVCCPut(ctx, engine, nil, key, ts1, val1, nil); err != nil {
					t.Fatal(err)
				}
			}
			txn := &roachpb.Transaction{
				TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), WriteTimestamp: ts2},
				ReadTimestamp: ts2,
			}
			if err := MVCCDelete(ctx, engine, nil, key, txn.ReadTimestamp, txn); err != nil {
				t.Fatal(err)
			}
			keys := []roachpb.GCRequest_GCKey{
				{Key: key, Timestamp: ts2},
			}
			if err := MVCCGarbageCollect(ctx, engine, nil, keys, ts2); err == nil {
				t.Fatal("expected error garbage collecting an intent")
			}
		})
	}
}

// TestMVCCGarbageCollectPanicsWithMixOfLocalAndGlobalKeys verifies that
// MVCCGarbageCollect panics when presented with a mix of local and global
// keys.
func TestMVCCGarbageCollectPanicsWithMixOfLocalAndGlobalKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			require.Panics(t, func() {
				ts := hlc.Timestamp{WallTime: 1e9}
				k := roachpb.Key("a")
				keys := []roachpb.GCRequest_GCKey{
					{Key: k, Timestamp: ts},
					{Key: keys.RangeDescriptorKey(roachpb.RKey(k))},
				}
				if err := MVCCGarbageCollect(ctx, engine, nil, keys, ts); err != nil {
					panic(err)
				}
			})
		})
	}
}

// readWriterReturningSeekLTTrackingIterator is used in a test to inject errors
// and ensure that SeekLT is returned an appropriate number of times.
type readWriterReturningSeekLTTrackingIterator struct {
	it seekLTTrackingIterator
	ReadWriter
}

// NewMVCCIterator injects a seekLTTrackingIterator over the engine's real iterator.
func (rw *readWriterReturningSeekLTTrackingIterator) NewMVCCIterator(
	iterKind MVCCIterKind, opts IterOptions,
) MVCCIterator {
	rw.it.MVCCIterator = rw.ReadWriter.NewMVCCIterator(iterKind, opts)
	return &rw.it
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
	engineBatchIteratorSupportsPrev := func(engine Engine) bool {
		batch := engine.NewBatch()
		defer batch.Close()
		it := batch.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
			UpperBound: keys.UserTableDataMin,
			LowerBound: keys.MaxKey,
		})
		defer it.Close()
		return it.SupportsPrev()
	}
	runTestCase := func(t *testing.T, tc testCase, engine Engine) {
		ctx := context.Background()
		ms := &enginepb.MVCCStats{}
		for _, key := range tc.keys {
			for _, seconds := range key.timestamps {
				val := roachpb.MakeValueFromBytes(bytes)
				ts := toHLC(seconds)
				if err := MVCCPut(
					ctx, engine, ms, roachpb.Key(key.key), ts, val, nil,
				); err != nil {
					t.Fatal(err)
				}
			}
		}

		supportsPrev := engineBatchIteratorSupportsPrev(engine)

		var keys []roachpb.GCRequest_GCKey
		var expectedSeekLTs int
		for _, key := range tc.keys {
			keys = append(keys, roachpb.GCRequest_GCKey{
				Key:       roachpb.Key(key.key),
				Timestamp: toHLC(key.gcTimestamp),
			})
			if supportsPrev && key.expSeekLT {
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
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					engine := engineImpl.create()
					defer engine.Close()
					runTestCase(t, tc, engine)
				})
			}
		})
	}
}

// TestResolveIntentWithLowerEpoch verifies that trying to resolve
// an intent at an epoch that is lower than the epoch of the intent
// leaves the intent untouched.
func TestResolveIntentWithLowerEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			// Lay down an intent with a high epoch.
			if err := MVCCPut(ctx, engine, nil, testKey1, txn1e2.ReadTimestamp, value1, txn1e2); err != nil {
				t.Fatal(err)
			}
			// Resolve the intent with a low epoch.
			if _, err := MVCCResolveWriteIntent(ctx, engine, nil,
				roachpb.MakeLockUpdate(txn1, roachpb.Span{Key: testKey1})); err != nil {
				t.Fatal(err)
			}

			// Check that the intent was not cleared.
			metaKey := mvccKey(testKey1)
			meta := &enginepb.MVCCMetadata{}
			ok, _, _, err := engine.MVCCGetProto(metaKey, meta)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("intent should not be cleared by resolve intent request with lower epoch")
			}
		})
	}
}

// TestTimeSeriesMVCCStats ensures that merge operations
// result in an expected increase in timeseries data.
func TestTimeSeriesMVCCStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
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
		})
	}
}

// TestMVCCTimeSeriesPartialMerge ensures that "partial merges" of merged time
// series data does not result in a different final result than a "full merge".
func TestMVCCTimeSeriesPartialMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
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

				if v, _, err := MVCCGet(ctx, engine, k, hlc.Timestamp{}, MVCCGetOptions{}); err != nil {
					t.Fatal(err)
				} else {
					vals[i] = v
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
		})
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
