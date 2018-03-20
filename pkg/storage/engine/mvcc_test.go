// Copyright 2014 The Cockroach Authors.
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

package engine

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/kr/pretty"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/zerofields"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/shuffle"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// Constants for system-reserved keys in the KV map.
var (
	txn1ID = uuid.MakeV4()
	txn2ID = uuid.MakeV4()

	keyMin       = roachpb.KeyMin
	keyMax       = roachpb.KeyMax
	testKey1     = roachpb.Key("/db1")
	testKey2     = roachpb.Key("/db2")
	testKey3     = roachpb.Key("/db3")
	testKey4     = roachpb.Key("/db4")
	testKey5     = roachpb.Key("/db5")
	testKey6     = roachpb.Key("/db6")
	txn1         = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn1ID, Epoch: 1, Timestamp: hlc.Timestamp{Logical: 1}}}
	txn1Commit   = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn1ID, Epoch: 1, Timestamp: hlc.Timestamp{Logical: 1}}, Status: roachpb.COMMITTED}
	txn1Abort    = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn1ID, Epoch: 1}, Status: roachpb.ABORTED}
	txn1e2       = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn1ID, Epoch: 2, Timestamp: hlc.Timestamp{Logical: 1}}}
	txn1e2Commit = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn1ID, Epoch: 2, Timestamp: hlc.Timestamp{Logical: 1}}, Status: roachpb.COMMITTED}
	txn2         = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn2ID, Timestamp: hlc.Timestamp{Logical: 1}}}
	txn2Commit   = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("a"), ID: txn2ID, Timestamp: hlc.Timestamp{Logical: 1}}, Status: roachpb.COMMITTED}
	value1       = roachpb.MakeValueFromString("testValue1")
	value2       = roachpb.MakeValueFromString("testValue2")
	value3       = roachpb.MakeValueFromString("testValue3")
	value4       = roachpb.MakeValueFromString("testValue4")
	value5       = roachpb.MakeValueFromString("testValue5")
	value6       = roachpb.MakeValueFromString("testValue6")
	tsvalue1     = timeSeriesAsValue(testtime, 1000, []tsSample{
		{1, 1, 5, 5, 5},
	}...)
	tsvalue2 = timeSeriesAsValue(testtime, 1000, []tsSample{
		{1, 1, 15, 15, 15},
	}...)
	valueEmpty = roachpb.MakeValueFromString("")
)

// createTestEngine returns a new in-memory engine with 1MB of storage
// capacity.
func createTestEngine() Engine {
	return NewInMem(roachpb.Attributes{}, 1<<20)
}

// makeTxn creates a new transaction using the specified base
// txn and timestamp.
func makeTxn(baseTxn roachpb.Transaction, ts hlc.Timestamp) *roachpb.Transaction {
	txn := baseTxn.Clone()
	txn.Timestamp = ts
	return &txn
}

type mvccKeys []MVCCKey

func (n mvccKeys) Len() int           { return len(n) }
func (n mvccKeys) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n mvccKeys) Less(i, j int) bool { return n[i].Less(n[j]) }

// mvccGetGo is identical to MVCCGet except that it uses mvccGetInternal
// instead of the C++ Iterator.MVCCGet. It is used to test mvccGetInternal
// which is used by mvccPutInternal to avoid Cgo crossings. Simply using the
// C++ MVCCGet in mvccPutInternal causes a significant performance hit to
// conditional put operations.
func mvccGetGo(
	ctx context.Context,
	engine Reader,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	consistent bool,
	txn *roachpb.Transaction,
) (*roachpb.Value, []roachpb.Intent, error) {
	if len(key) == 0 {
		return nil, nil, emptyKeyError()
	}

	iter := engine.NewIterator(true)
	defer iter.Close()

	buf := newGetBuffer()
	defer buf.release()

	metaKey := MakeMVCCMetadataKey(key)
	ok, _, _, err := mvccGetMetadata(iter, metaKey, &buf.meta)
	if !ok || err != nil {
		return nil, nil, err
	}

	value, intents, _, err := mvccGetInternal(ctx, iter, metaKey,
		timestamp, consistent, safeValue, txn, buf)
	if !value.IsPresent() {
		value = nil
	}
	if value == &buf.value {
		value = &roachpb.Value{}
		*value = buf.value
		buf.value.Reset()
	}
	return value, intents, err
}

var mvccGetImpls = []struct {
	name string
	fn   func(
		ctx context.Context,
		engine Reader,
		key roachpb.Key,
		timestamp hlc.Timestamp,
		consistent bool,
		txn *roachpb.Transaction,
	) (*roachpb.Value, []roachpb.Intent, error)
}{
	{"cpp", MVCCGet},
	{"go", mvccGetGo},
}

func TestMVCCStatsAddSubForward(t *testing.T) {
	defer leaktest.AfterTest(t)()
	goldMS := enginepb.MVCCStats{
		ContainsEstimates: true,
		KeyBytes:          1,
		KeyCount:          1,
		ValBytes:          1,
		ValCount:          1,
		IntentBytes:       1,
		IntentCount:       1,
		IntentAge:         1,
		GCBytesAge:        1,
		LiveBytes:         1,
		LiveCount:         1,
		SysBytes:          1,
		SysCount:          1,
		LastUpdateNanos:   1,
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
		ContainsEstimates: true,
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

	for i, ns := range []int64{1, 1e9 - 1001, 1e9 - 1000, 1E9 - 1, 1E9, 1E9 + 1, 2E9 - 1} {
		oldDelta := delta
		delta.AgeTo(ns)
		if delta.LastUpdateNanos < ns {
			t.Fatalf("%d: expected LastUpdateNanos < %d, got %d", i, ns, delta.LastUpdateNanos)
		}
		shouldAge := ns/1E9-oldDelta.LastUpdateNanos/1E9 > 0
		didAge := delta.IntentAge != oldDelta.IntentAge &&
			delta.GCBytesAge != oldDelta.GCBytesAge
		if shouldAge != didAge {
			t.Fatalf("%d: should age: %t, but had\n%+v\nand now\n%+v", i, shouldAge, oldDelta, delta)
		}
	}

	expDelta := goldDelta
	expDelta.LastUpdateNanos = 2E9 - 1
	expDelta.GCBytesAge = 42
	expDelta.IntentAge = 11
	cmp(delta, expDelta)

	delta.AgeTo(2E9)
	expDelta.LastUpdateNanos = 2E9
	expDelta.GCBytesAge += 42
	expDelta.IntentAge += 11
	cmp(delta, expDelta)

	{
		// Verify that AgeTo can go backwards in time.
		// Works on a copy.
		tmpDelta := delta
		expDelta := expDelta

		tmpDelta.AgeTo(2E9 - 1)
		expDelta.LastUpdateNanos = 2E9 - 1
		expDelta.GCBytesAge -= 42
		expDelta.IntentAge -= 11
		cmp(tmpDelta, expDelta)
	}

	delta.AgeTo(3E9 - 1)
	delta.Forward(5) // should be noop
	expDelta.LastUpdateNanos = 3E9 - 1
	cmp(delta, expDelta)

	// Check that Add calls Forward appropriately.
	mss := []enginepb.MVCCStats{goldMS, goldMS}

	mss[0].LastUpdateNanos = 2E9 - 1
	mss[1].LastUpdateNanos = 10E9 + 1

	expMS := goldMS
	expMS.Add(goldMS)
	expMS.LastUpdateNanos = 10E9 + 1
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

	neg.AgeTo(2E9)

	exp.LastUpdateNanos = 2E9
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

func TestMVCCEmptyKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if _, _, err := MVCCGet(context.Background(), engine, roachpb.Key{}, hlc.Timestamp{Logical: 1}, true, nil); err == nil {
		t.Error("expected empty key error")
	}
	if err := MVCCPut(context.Background(), engine, nil, roachpb.Key{}, hlc.Timestamp{Logical: 1}, value1, nil); err == nil {
		t.Error("expected empty key error")
	}
	if _, _, _, err := MVCCScan(context.Background(), engine, roachpb.Key{}, testKey1, math.MaxInt64, hlc.Timestamp{Logical: 1}, true, nil); err != nil {
		t.Errorf("empty key allowed for start key in scan; got %s", err)
	}
	if _, _, _, err := MVCCScan(context.Background(), engine, testKey1, roachpb.Key{}, math.MaxInt64, hlc.Timestamp{Logical: 1}, true, nil); err == nil {
		t.Error("expected empty key error")
	}
	if err := MVCCResolveWriteIntent(context.Background(), engine, nil, roachpb.Intent{}); err == nil {
		t.Error("expected empty key error")
	}
}

func TestMVCCGetNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, impl := range mvccGetImpls {
		t.Run(impl.name, func(t *testing.T) {
			mvccGet := impl.fn

			engine := createTestEngine()
			defer engine.Close()

			value, _, err := mvccGet(context.Background(), engine, testKey1, hlc.Timestamp{Logical: 1}, true, nil)
			if err != nil {
				t.Fatal(err)
			}
			if value != nil {
				t.Fatal("the value should be empty")
			}
		})
	}
}

func TestMVCCPutWithTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, txn1); err != nil {
		t.Fatal(err)
	}

	for _, ts := range []hlc.Timestamp{{Logical: 1}, {Logical: 2}, {WallTime: 1}} {
		value, _, err := MVCCGet(context.Background(), engine, testKey1, ts, true, txn1)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value1.RawBytes, value.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				value1.RawBytes, value.RawBytes)
		}
	}
}

func TestMVCCPutWithoutTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, ts := range []hlc.Timestamp{{Logical: 1}, {Logical: 2}, {WallTime: 1}} {
		value, _, err := MVCCGet(context.Background(), engine, testKey1, ts, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value1.RawBytes, value.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				value1.RawBytes, value.RawBytes)
		}
	}
}

// TestMVCCPutOutOfOrder tests a scenario where a put operation of an
// older timestamp comes after a put operation of a newer timestamp.
func TestMVCCPutOutOfOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 2, Logical: 1}, value1, txn1); err != nil {
		t.Fatal(err)
	}

	// Put operation with earlier wall time. Will NOT be ignored.
	txn := *txn1
	txn.Sequence++
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value2, &txn); err != nil {
		t.Fatal(err)
	}

	value, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 3}, true, &txn)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value.RawBytes, value2.RawBytes) {
		t.Fatalf("the value should be %s, but got %s",
			value2.RawBytes, value.RawBytes)
	}

	// Another put operation with earlier logical time. Will NOT be ignored.
	txn.Sequence++
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 2}, value2, &txn); err != nil {
		t.Fatal(err)
	}

	value, _, err = MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 3}, true, &txn)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value.RawBytes, value2.RawBytes) {
		t.Fatalf("the value should be %s, but got %s",
			value2.RawBytes, value.RawBytes)
	}
}

// Test that a write with a higher epoch is permitted even when the sequence
// number has decreased compared to an existing intent. This is because, on
// transaction restart, the sequence number should not be compared with intents
// from the old epoch.
func TestMVCCPutNewEpochLowerSequence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	txn := *txn1
	txn.Sequence = 5
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, &txn); err != nil {
		t.Fatal(err)
	}
	value, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 3}, true, &txn)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value.RawBytes, value1.RawBytes) {
		t.Fatalf("the value should be %s, but got %s",
			value2.RawBytes, value.RawBytes)
	}

	txn.Sequence = 4
	txn.Epoch++
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value2, &txn); err != nil {
		t.Fatal(err)
	}
	value, _, err = MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 3}, true, &txn)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value.RawBytes, value2.RawBytes) {
		t.Fatalf("the value should be %s, but got %s",
			value2.RawBytes, value.RawBytes)
	}
}

// TestMVCCIncrement verifies increment behavior. In particular,
// incrementing a non-existent key by 0 will create the value.
func TestMVCCIncrement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	newVal, err := MVCCIncrement(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if newVal != 0 {
		t.Errorf("expected new value of 0; got %d", newVal)
	}
	val, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{Logical: 1}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if val == nil {
		t.Errorf("expected increment of 0 to create key/value")
	}

	newVal, err = MVCCIncrement(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 2}, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	if newVal != 2 {
		t.Errorf("expected new value of 2; got %d", newVal)
	}
}

// TestMVCCIncrementTxn verifies increment behavior within a txn.
func TestMVCCIncrementTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	txn := *txn1
	for i := 1; i <= 2; i++ {
		txn.Sequence++
		newVal, err := MVCCIncrement(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, &txn, 1)
		if err != nil {
			t.Fatal(err)
		}
		if newVal != int64(i) {
			t.Errorf("expected new value of %d; got %d", i, newVal)
		}
	}
}

// TestMVCCIncrementOldTimestamp tests a case where MVCCIncrement is
// called with an old timestamp. The test verifies that a value is
// read with the newer timestamp and a write too old error is returned.
func TestMVCCIncrementOldTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	// Write an integer value.
	val := roachpb.Value{}
	val.SetInt(1)
	err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, val, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Override value.
	val.SetInt(2)
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 3}, val, nil); err != nil {
		t.Fatal(err)
	}

	// Attempt to increment a value with an older timestamp than
	// the previous put. This will fail with type mismatch (not
	// with WriteTooOldError).
	incVal, err := MVCCIncrement(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 2}, nil, 1)
	if wtoErr, ok := err.(*roachpb.WriteTooOldError); !ok {
		t.Fatalf("unexpectedly not WriteTooOld: %s", err)
	} else if expTS := (hlc.Timestamp{WallTime: 3, Logical: 1}); wtoErr.ActualTimestamp != (expTS) {
		t.Fatalf("expected write too old error with actual ts %s; got %s", expTS, wtoErr.ActualTimestamp)
	}
	if incVal != 3 {
		t.Fatalf("expected value=%d; got %d", 3, incVal)
	}
}

func TestMVCCUpdateExistingKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, nil)
	if err != nil {
		t.Fatal(err)
	}

	value, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 1}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value1.RawBytes, value.RawBytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.RawBytes, value.RawBytes)
	}

	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 2}, value2, nil); err != nil {
		t.Fatal(err)
	}

	// Read the latest version.
	value, _, err = MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 3}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value2.RawBytes, value.RawBytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value2.RawBytes, value.RawBytes)
	}

	// Read the old version.
	value, _, err = MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 1}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value1.RawBytes, value.RawBytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.RawBytes, value.RawBytes)
	}
}

func TestMVCCUpdateExistingKeyOldVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1, Logical: 1}, value1, nil); err != nil {
		t.Fatal(err)
	}
	// Earlier wall time.
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value2, nil); err == nil {
		t.Fatal("expected error on old version")
	}
	// Earlier logical time.
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value2, nil); err == nil {
		t.Fatal("expected error on old version")
	}
}

func TestMVCCUpdateExistingKeyInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	txn := *txn1
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, &txn); err != nil {
		t.Fatal(err)
	}

	txn.Sequence++
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, &txn); err != nil {
		t.Fatal(err)
	}
}

func TestMVCCUpdateExistingKeyDiffTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, txn1); err != nil {
		t.Fatal(err)
	}

	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value2, txn2); err == nil {
		t.Fatal("expected error on uncommitted write intent")
	}
}

func TestMVCCGetNoMoreOldVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, impl := range mvccGetImpls {
		t.Run(impl.name, func(t *testing.T) {
			mvccGet := impl.fn

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

			engine := createTestEngine()
			defer engine.Close()

			if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 3}, value1, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(context.Background(), engine, nil, testKey2, hlc.Timestamp{WallTime: 1}, value2, nil); err != nil {
				t.Fatal(err)
			}

			value, _, err := mvccGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 2}, true, nil)
			if err != nil {
				t.Fatal(err)
			}
			if value != nil {
				t.Fatal("the value should be empty")
			}
		})
	}
}

// TestMVCCGetUncertainty verifies that the appropriate error results when
// a transaction reads a key at a timestamp that has versions newer than that
// timestamp, but older than the transaction's MaxTimestamp.
func TestMVCCGetUncertainty(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, impl := range mvccGetImpls {
		t.Run(impl.name, func(t *testing.T) {
			mvccGet := impl.fn

			engine := createTestEngine()
			defer engine.Close()

			txn := &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{ID: uuid.MakeV4(), Timestamp: hlc.Timestamp{WallTime: 5}}, MaxTimestamp: hlc.Timestamp{WallTime: 10}}
			// Put a value from the past.
			if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
				t.Fatal(err)
			}
			// Put a value that is ahead of MaxTimestamp, it should not interfere.
			if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 12}, value2, nil); err != nil {
				t.Fatal(err)
			}
			// Read with transaction, should get a value back.
			val, _, err := mvccGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 7}, true, txn)
			if err != nil {
				t.Fatal(err)
			}
			if val == nil || !bytes.Equal(val.RawBytes, value1.RawBytes) {
				t.Fatalf("wanted %q, got %v", value1.RawBytes, val)
			}

			// Now using testKey2.
			// Put a value that conflicts with MaxTimestamp.
			if err := MVCCPut(context.Background(), engine, nil, testKey2, hlc.Timestamp{WallTime: 9}, value2, nil); err != nil {
				t.Fatal(err)
			}
			// Read with transaction, should get error back.
			if _, _, err := mvccGet(context.Background(), engine, testKey2, hlc.Timestamp{WallTime: 7}, true, txn); err == nil {
				t.Fatal("wanted an error")
			} else if _, ok := err.(*roachpb.ReadWithinUncertaintyIntervalError); !ok {
				t.Fatalf("wanted a ReadWithinUncertaintyIntervalError, got %+v", err)
			}
			if _, _, _, err := MVCCScan(context.Background(), engine, testKey2, testKey2.PrefixEnd(), 10, hlc.Timestamp{WallTime: 7}, true, txn); err == nil {
				t.Fatal("wanted an error")
			} else if _, ok := err.(*roachpb.ReadWithinUncertaintyIntervalError); !ok {
				t.Fatalf("wanted a ReadWithinUncertaintyIntervalError, got %+v", err)
			}
			// Adjust MaxTimestamp and retry.
			txn.MaxTimestamp = hlc.Timestamp{WallTime: 7}
			if _, _, err := mvccGet(context.Background(), engine, testKey2, hlc.Timestamp{WallTime: 7}, true, txn); err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := MVCCScan(context.Background(), engine, testKey2, testKey2.PrefixEnd(), 10, hlc.Timestamp{WallTime: 7}, true, txn); err != nil {
				t.Fatal(err)
			}

			txn.MaxTimestamp = hlc.Timestamp{WallTime: 10}
			// Now using testKey3.
			// Put a value that conflicts with MaxTimestamp and another write further
			// ahead and not conflicting any longer. The first write should still ruin
			// it.
			if err := MVCCPut(context.Background(), engine, nil, testKey3, hlc.Timestamp{WallTime: 9}, value2, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(context.Background(), engine, nil, testKey3, hlc.Timestamp{WallTime: 99}, value2, nil); err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := MVCCScan(context.Background(), engine, testKey3, testKey3.PrefixEnd(), 10, hlc.Timestamp{WallTime: 7}, true, txn); err == nil {
				t.Fatal("wanted an error")
			} else if _, ok := err.(*roachpb.ReadWithinUncertaintyIntervalError); !ok {
				t.Fatalf("wanted a ReadWithinUncertaintyIntervalError, got %+v", err)
			}
			if _, _, err := mvccGet(context.Background(), engine, testKey3, hlc.Timestamp{WallTime: 7}, true, txn); err == nil {
				t.Fatalf("wanted an error")
			} else if _, ok := err.(*roachpb.ReadWithinUncertaintyIntervalError); !ok {
				t.Fatalf("wanted a ReadWithinUncertaintyIntervalError, got %+v", err)
			}
		})
	}
}

func TestMVCCGetAndDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, impl := range mvccGetImpls {
		t.Run(impl.name, func(t *testing.T) {
			mvccGet := impl.fn

			engine := createTestEngine()
			defer engine.Close()

			if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
				t.Fatal(err)
			}
			value, _, err := mvccGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 2}, true, nil)
			if err != nil {
				t.Fatal(err)
			}
			if value == nil {
				t.Fatal("the value should not be empty")
			}

			err = MVCCDelete(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 3}, nil)
			if err != nil {
				t.Fatal(err)
			}

			// Read the latest version which should be deleted.
			value, _, err = mvccGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 4}, true, nil)
			if err != nil {
				t.Fatal(err)
			}
			if value != nil {
				t.Fatal("the value should be empty")
			}
			// Read the latest version with tombstone.
			value, _, err = MVCCGetWithTombstone(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 4}, true, nil)
			if err != nil {
				t.Fatal(err)
			} else if value == nil || len(value.RawBytes) != 0 {
				t.Fatalf("the value should be non-nil with empty RawBytes; got %+v", value)
			}

			// Read the old version which should still exist.
			for _, logical := range []int32{0, math.MaxInt32} {
				value, _, err = mvccGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 2, Logical: logical}, true, nil)
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
	engine := createTestEngine()
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

	value, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 2}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	// The attempted write at ts(1,0) was performed at ts(3,1), so we should
	// not see it at ts(2,0).
	if value != nil {
		t.Fatalf("value present at TS = %s", value.Timestamp)
	}

	// Read the latest version which will be the value written with the timestamp pushed.
	value, _, err = MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 4}, true, nil)
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
}

func TestMVCCInlineWithTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	// Put an inline value.
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{}, value1, nil); err != nil {
		t.Fatal(err)
	}

	// Now verify inline get.
	value, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(value1, *value) {
		t.Errorf("the inline value should be %s; got %s", value1, *value)
	}

	// Verify inline get with txn does still work (this will happen on a
	// scan if the distributed sender is forced to wrap it in a txn).
	if _, _, err = MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{}, true, txn1); err != nil {
		t.Error(err)
	}

	// Verify inline put with txn is an error.
	if err = MVCCPut(context.Background(), engine, nil, testKey2, hlc.Timestamp{}, value2, txn2); !testutils.IsError(err, "writes not allowed within transactions") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestMVCCDeleteMissingKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCDelete(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, nil); err != nil {
		t.Fatal(err)
	}
	// Verify nothing is written to the engine.
	if val, err := engine.Get(mvccKey(testKey1)); err != nil || val != nil {
		t.Fatalf("expected no mvcc metadata after delete of a missing key; got %q: %s", val, err)
	}
}

func TestMVCCGetAndDeleteInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, impl := range mvccGetImpls {
		t.Run(impl.name, func(t *testing.T) {
			mvccGet := impl.fn

			engine := createTestEngine()
			defer engine.Close()

			txn := *txn1
			txn.Sequence++
			if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, &txn); err != nil {
				t.Fatal(err)
			}

			if value, _, err := mvccGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 2}, true, &txn); err != nil {
				t.Fatal(err)
			} else if value == nil {
				t.Fatal("the value should not be empty")
			}

			txn.Sequence++
			if err := MVCCDelete(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 3}, &txn); err != nil {
				t.Fatal(err)
			}

			// Read the latest version which should be deleted.
			if value, _, err := mvccGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 4}, true, &txn); err != nil {
				t.Fatal(err)
			} else if value != nil {
				t.Fatal("the value should be empty")
			}
			// Read the latest version with tombstone.
			if value, _, err := MVCCGetWithTombstone(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 4}, true, &txn); err != nil {
				t.Fatal(err)
			} else if value == nil || len(value.RawBytes) != 0 {
				t.Fatalf("the value should be non-nil with empty RawBytes; got %+v", value)
			}

			// Read the old version which shouldn't exist, as within a
			// transaction, we delete previous values.
			if value, _, err := mvccGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 2}, true, nil); err != nil {
				t.Fatal(err)
			} else if value != nil {
				t.Fatalf("expected value nil, got: %s", value)
			}
		})
	}
}

func TestMVCCGetWriteIntentError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, impl := range mvccGetImpls {
		t.Run(impl.name, func(t *testing.T) {
			mvccGet := impl.fn

			engine := createTestEngine()
			defer engine.Close()

			if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, txn1); err != nil {
				t.Fatal(err)
			}

			if _, _, err := mvccGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 1}, true, nil); err == nil {
				t.Fatal("cannot read the value of a write intent without TxnID")
			}

			if _, _, err := mvccGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 1}, true, txn2); err == nil {
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
	engine := createTestEngine()
	defer engine.Close()

	ts := []hlc.Timestamp{{Logical: 1}, {Logical: 2}, {Logical: 3}, {Logical: 4}, {Logical: 5}, {Logical: 6}}

	fixtureKVs := []roachpb.KeyValue{
		{Key: testKey1, Value: mkVal("testValue1 pre", ts[0])},
		{Key: testKey4, Value: mkVal("testValue4 pre", ts[1])},
		{Key: testKey1, Value: mkVal("testValue1", ts[2])},
		{Key: testKey2, Value: mkVal("testValue2", ts[3])},
		{Key: testKey3, Value: mkVal("testValue3", ts[4])},
		{Key: testKey4, Value: mkVal("testValue4", ts[5])},
	}
	for i, kv := range fixtureKVs {
		var txn *roachpb.Transaction
		if i == 2 {
			txn = txn1
		} else if i == 5 {
			txn = txn2
		}
		v := *protoutil.Clone(&kv.Value).(*roachpb.Value)
		v.Timestamp = hlc.Timestamp{}
		if err := MVCCPut(context.Background(), engine, nil, kv.Key, kv.Value.Timestamp, v, txn); err != nil {
			t.Fatal(err)
		}
	}

	scanCases := []struct {
		consistent bool
		txn        *roachpb.Transaction
		expIntents []roachpb.Intent
		expValues  []roachpb.KeyValue
	}{
		{
			consistent: true,
			txn:        nil,
			expIntents: []roachpb.Intent{
				{Span: roachpb.Span{Key: testKey1}, Txn: txn1.TxnMeta},
				{Span: roachpb.Span{Key: testKey4}, Txn: txn2.TxnMeta},
			},
			// would be []roachpb.KeyValue{fixtureKVs[3], fixtureKVs[4]} without WriteIntentError
			expValues: nil,
		},
		{
			consistent: true,
			txn:        txn1,
			expIntents: []roachpb.Intent{
				{Span: roachpb.Span{Key: testKey4}, Txn: txn2.TxnMeta},
			},
			expValues: nil, // []roachpb.KeyValue{fixtureKVs[2], fixtureKVs[3], fixtureKVs[4]},
		},
		{
			consistent: true,
			txn:        txn2,
			expIntents: []roachpb.Intent{
				{Span: roachpb.Span{Key: testKey1}, Txn: txn1.TxnMeta},
			},
			expValues: nil, // []roachpb.KeyValue{fixtureKVs[3], fixtureKVs[4], fixtureKVs[5]},
		},
		{
			consistent: false,
			txn:        nil,
			expIntents: []roachpb.Intent{
				{Span: roachpb.Span{Key: testKey1}, Txn: txn1.TxnMeta},
				{Span: roachpb.Span{Key: testKey4}, Txn: txn2.TxnMeta},
			},
			expValues: []roachpb.KeyValue{fixtureKVs[0], fixtureKVs[3], fixtureKVs[4], fixtureKVs[1]},
		},
	}

	for i, scan := range scanCases {
		cStr := "inconsistent"
		if scan.consistent {
			cStr = "consistent"
		}
		kvs, _, intents, err := MVCCScan(context.Background(), engine, testKey1, testKey4.Next(), math.MaxInt64, hlc.Timestamp{WallTime: 1}, scan.consistent, scan.txn)
		wiErr, _ := err.(*roachpb.WriteIntentError)
		if (err == nil) != (wiErr == nil) {
			t.Errorf("%s(%d): unexpected error: %s", cStr, i, err)
		}

		if wiErr == nil != !scan.consistent {
			t.Errorf("%s(%d): expected write intent error; got %s", cStr, i, err)
			continue
		}

		if len(intents) > 0 != !scan.consistent {
			t.Errorf("%s(%d): expected different intents slice; got %+v", cStr, i, intents)
			continue
		}

		if scan.consistent {
			intents = wiErr.Intents
		}

		if !reflect.DeepEqual(intents, scan.expIntents) {
			t.Fatalf("%s(%d): expected intents:\n%+v;\n got\n%+v", cStr, i, scan.expIntents, intents)
		}

		if !reflect.DeepEqual(kvs, scan.expValues) {
			t.Errorf("%s(%d): expected values %+v; got %+v", cStr, i, scan.expValues, kvs)
		}
	}
}

// TestMVCCGetInconsistent verifies the behavior of get with
// consistent set to false.
func TestMVCCGetInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, impl := range mvccGetImpls {
		t.Run(impl.name, func(t *testing.T) {
			mvccGet := impl.fn

			engine := createTestEngine()
			defer engine.Close()

			// Put two values to key 1, the latest with a txn.
			if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 2}, value2, txn1); err != nil {
				t.Fatal(err)
			}

			// A get with consistent=false should fail in a txn.
			if _, _, err := mvccGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 1}, false, txn1); err == nil {
				t.Error("expected an error getting with consistent=false in txn")
			}

			// Inconsistent get will fetch value1 for any timestamp.
			for _, ts := range []hlc.Timestamp{{WallTime: 1}, {WallTime: 2}} {
				val, intents, err := mvccGet(context.Background(), engine, testKey1, ts, false, nil)
				if ts.Less(hlc.Timestamp{WallTime: 2}) {
					if err != nil {
						t.Fatal(err)
					}
				} else {
					if len(intents) == 0 || !intents[0].Key.Equal(testKey1) {
						t.Fatalf("expected %v, but got %v", testKey1, intents)
					}
				}
				if !bytes.Equal(val.RawBytes, value1.RawBytes) {
					t.Errorf("@%s expected %q; got %q", ts, value1.RawBytes, val.RawBytes)
				}
			}

			// Write a single intent for key 2 and verify get returns empty.
			if err := MVCCPut(context.Background(), engine, nil, testKey2, hlc.Timestamp{WallTime: 2}, value1, txn2); err != nil {
				t.Fatal(err)
			}
			val, intents, err := mvccGet(context.Background(), engine, testKey2, hlc.Timestamp{WallTime: 2}, false, nil)
			if len(intents) == 0 || !intents[0].Key.Equal(testKey2) {
				t.Fatal(err)
			}
			if val != nil {
				t.Errorf("expected empty val; got %+v", val)
			}
		})
	}
}

// TestMVCCGetProtoInconsistent verifies the behavior of GetProto with
// consistent set to false.
func TestMVCCGetProtoInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
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
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, v1, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 2}, v2, txn1); err != nil {
		t.Fatal(err)
	}

	// A get with consistent=false should fail in a txn.
	if _, err := MVCCGetProto(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 1}, false, txn1, nil); err == nil {
		t.Error("expected an error getting with consistent=false in txn")
	} else if _, ok := err.(*roachpb.WriteIntentError); ok {
		t.Error("expected non-WriteIntentError with inconsistent read in txn")
	}

	// Inconsistent get will fetch value1 for any timestamp.

	for _, ts := range []hlc.Timestamp{{WallTime: 1}, {WallTime: 2}} {
		val := roachpb.Value{}
		found, err := MVCCGetProto(context.Background(), engine, testKey1, ts, false, nil, &val)
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
		if err := MVCCPut(context.Background(), engine, nil, testKey2, hlc.Timestamp{WallTime: 2}, v1, txn2); err != nil {
			t.Fatal(err)
		}
		val := roachpb.Value{}
		found, err := MVCCGetProto(context.Background(), engine, testKey2, hlc.Timestamp{WallTime: 2}, false, nil, &val)
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
		if err := MVCCPut(context.Background(), engine, nil, testKey3, hlc.Timestamp{WallTime: 1}, value3, nil); err != nil {
			t.Fatal(err)
		}
		if err := MVCCPut(context.Background(), engine, nil, testKey3, hlc.Timestamp{WallTime: 2}, v2, txn1); err != nil {
			t.Fatal(err)
		}
		val := roachpb.Value{}
		found, err := MVCCGetProto(context.Background(), engine, testKey3, hlc.Timestamp{WallTime: 1}, false, nil, &val)
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

func TestMVCCScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 2}, value4, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey2, hlc.Timestamp{WallTime: 1}, value2, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey2, hlc.Timestamp{WallTime: 3}, value3, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey3, hlc.Timestamp{WallTime: 1}, value3, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey3, hlc.Timestamp{WallTime: 4}, value2, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey4, hlc.Timestamp{WallTime: 1}, value4, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey4, hlc.Timestamp{WallTime: 5}, value1, nil); err != nil {
		t.Fatal(err)
	}

	kvs, resumeSpan, _, err := MVCCScan(context.Background(), engine, testKey2, testKey4, math.MaxInt64, hlc.Timestamp{WallTime: 1}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
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

	kvs, resumeSpan, _, err = MVCCScan(context.Background(), engine, testKey2, testKey4, math.MaxInt64, hlc.Timestamp{WallTime: 4}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
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

	kvs, resumeSpan, _, err = MVCCScan(context.Background(), engine, testKey4, keyMax, math.MaxInt64, hlc.Timestamp{WallTime: 1}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey4) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value4.RawBytes) {
		t.Fatal("the value should not be empty")
	}
	if resumeSpan != nil {
		t.Fatalf("resumeSpan = %+v", resumeSpan)
	}

	if _, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 1}, true, txn2); err != nil {
		t.Fatal(err)
	}
	kvs, _, _, err = MVCCScan(context.Background(), engine, keyMin, testKey2, math.MaxInt64, hlc.Timestamp{WallTime: 1}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey1) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value1.RawBytes) {
		t.Fatal("the value should not be empty")
	}
}

func TestMVCCScanMaxNum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey2, hlc.Timestamp{WallTime: 1}, value2, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey3, hlc.Timestamp{WallTime: 1}, value3, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey4, hlc.Timestamp{WallTime: 1}, value4, nil); err != nil {
		t.Fatal(err)
	}

	kvs, resumeSpan, _, err := MVCCScan(context.Background(), engine, testKey2, testKey4, 1, hlc.Timestamp{WallTime: 1}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey2) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value2.RawBytes) {
		t.Fatal("the value should not be empty")
	}
	if expected := (roachpb.Span{Key: testKey3, EndKey: testKey4}); !resumeSpan.EqualValue(expected) {
		t.Fatalf("expected = %+v, resumeSpan = %+v", expected, resumeSpan)
	}

	kvs, resumeSpan, _, err = MVCCScan(context.Background(), engine, testKey2, testKey4, 0, hlc.Timestamp{WallTime: 1}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 0 {
		t.Fatal("the value should be empty")
	}
	if expected := (roachpb.Span{Key: testKey2, EndKey: testKey4}); !resumeSpan.EqualValue(expected) {
		t.Fatalf("expected = %+v, resumeSpan = %+v", expected, resumeSpan)
	}
}

func TestMVCCScanWithKeyPrefix(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
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
	if err := MVCCPut(context.Background(), engine, nil, roachpb.Key("/a"), hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, roachpb.Key("/a"), hlc.Timestamp{WallTime: 2}, value2, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, roachpb.Key("/aa"), hlc.Timestamp{WallTime: 2}, value2, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, roachpb.Key("/aa"), hlc.Timestamp{WallTime: 3}, value3, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, roachpb.Key("/b"), hlc.Timestamp{WallTime: 1}, value3, nil); err != nil {
		t.Fatal(err)
	}

	kvs, _, _, err := MVCCScan(context.Background(), engine, roachpb.Key("/a"), roachpb.Key("/b"), math.MaxInt64, hlc.Timestamp{WallTime: 2}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, roachpb.Key("/a")) ||
		!bytes.Equal(kvs[1].Key, roachpb.Key("/aa")) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value2.RawBytes) ||
		!bytes.Equal(kvs[1].Value.RawBytes, value2.RawBytes) {
		t.Fatal("the value should not be empty")
	}
}

func TestMVCCScanInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey2, hlc.Timestamp{WallTime: 1}, value2, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey3, hlc.Timestamp{WallTime: 1}, value3, txn1); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey4, hlc.Timestamp{WallTime: 1}, value4, nil); err != nil {
		t.Fatal(err)
	}

	kvs, _, _, err := MVCCScan(context.Background(), engine, testKey2, testKey4, math.MaxInt64, hlc.Timestamp{WallTime: 1}, true, txn1)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, testKey2) ||
		!bytes.Equal(kvs[1].Key, testKey3) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value2.RawBytes) ||
		!bytes.Equal(kvs[1].Value.RawBytes, value3.RawBytes) {
		t.Fatal("the value should not be empty")
	}

	if _, _, _, err := MVCCScan(context.Background(), engine, testKey2, testKey4, math.MaxInt64, hlc.Timestamp{WallTime: 1}, true, nil); err == nil {
		t.Fatal("expected error on uncommitted write intent")
	}
}

// TestMVCCScanInconsistent writes several values, some as intents and
// verifies that the scan sees only the committed versions.
func TestMVCCScanInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	// A scan with consistent=false should fail in a txn.
	if _, _, _, err := MVCCScan(context.Background(), engine, keyMin, keyMax, math.MaxInt64, hlc.Timestamp{WallTime: 1}, false, txn1); err == nil {
		t.Error("expected an error scanning with consistent=false in txn")
	}

	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	ts3 := hlc.Timestamp{WallTime: 3}
	ts4 := hlc.Timestamp{WallTime: 4}
	ts5 := hlc.Timestamp{WallTime: 5}
	ts6 := hlc.Timestamp{WallTime: 6}
	if err := MVCCPut(context.Background(), engine, nil, testKey1, ts1, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey1, ts2, value2, txn1); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey2, ts3, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey2, ts4, value2, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey3, ts5, value3, txn2); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey4, ts6, value4, nil); err != nil {
		t.Fatal(err)
	}

	expIntents := []roachpb.Intent{
		{Span: roachpb.Span{Key: testKey1}, Txn: txn1.TxnMeta},
		{Span: roachpb.Span{Key: testKey3}, Txn: txn2.TxnMeta},
	}
	kvs, _, intents, err := MVCCScan(context.Background(), engine, testKey1, testKey4.Next(), math.MaxInt64, hlc.Timestamp{WallTime: 7}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(intents, expIntents) {
		t.Fatalf("expected %v, but found %v", expIntents, intents)
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
	if !reflect.DeepEqual(kvs, expKVs) {
		t.Errorf("expected key values equal %v != %v", kvs, expKVs)
	}

	// Now try a scan at a historical timestamp.
	expIntents = expIntents[:1]
	kvs, _, intents, err = MVCCScan(context.Background(), engine, testKey1, testKey4.Next(), math.MaxInt64, hlc.Timestamp{WallTime: 3}, false, nil)
	if !reflect.DeepEqual(intents, expIntents) {
		t.Fatal(err)
	}
	expKVs = []roachpb.KeyValue{
		{Key: testKey1, Value: makeTimestampedValue(value1, ts1)},
		{Key: testKey2, Value: makeTimestampedValue(value1, ts3)},
	}
	if !reflect.DeepEqual(kvs, expKVs) {
		t.Errorf("expected key values equal %v != %v", kvs, expKVs)
	}
}

func TestMVCCDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey2, hlc.Timestamp{WallTime: 1}, value2, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey3, hlc.Timestamp{WallTime: 1}, value3, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey4, hlc.Timestamp{WallTime: 1}, value4, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey5, hlc.Timestamp{WallTime: 1}, value5, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey6, hlc.Timestamp{WallTime: 1}, value6, nil); err != nil {
		t.Fatal(err)
	}

	// Attempt to delete two keys.
	deleted, resumeSpan, num, err := MVCCDeleteRange(
		context.Background(), engine, nil, testKey2, testKey6, 2, hlc.Timestamp{WallTime: 2}, nil, false,
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
	kvs, _, _, _ := MVCCScan(context.Background(), engine, keyMin, keyMax, math.MaxInt64, hlc.Timestamp{WallTime: 2}, true, nil)
	if len(kvs) != 4 ||
		!bytes.Equal(kvs[0].Key, testKey1) ||
		!bytes.Equal(kvs[1].Key, testKey4) ||
		!bytes.Equal(kvs[2].Key, testKey5) ||
		!bytes.Equal(kvs[3].Key, testKey6) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value1.RawBytes) ||
		!bytes.Equal(kvs[1].Value.RawBytes, value4.RawBytes) ||
		!bytes.Equal(kvs[2].Value.RawBytes, value5.RawBytes) ||
		!bytes.Equal(kvs[3].Value.RawBytes, value6.RawBytes) {
		t.Fatal("the value should not be empty")
	}

	// Try again, but with tombstones set to true to fetch the deleted keys as well.
	kvs = []roachpb.KeyValue{}
	if _, err = MVCCIterate(
		context.Background(), engine, keyMin, keyMax, hlc.Timestamp{WallTime: 2},
		true, true /* tombstones */, nil, false, func(kv roachpb.KeyValue) (bool, error) {
			kvs = append(kvs, kv)
			return false, nil
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
		context.Background(), engine, nil, testKey2, testKey6, 0, hlc.Timestamp{WallTime: 2}, nil, false,
	)
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
	kvs, _, _, _ = MVCCScan(context.Background(), engine, keyMin, keyMax, math.MaxInt64, hlc.Timestamp{WallTime: 2}, true, nil)
	if len(kvs) != 4 ||
		!bytes.Equal(kvs[0].Key, testKey1) ||
		!bytes.Equal(kvs[1].Key, testKey4) ||
		!bytes.Equal(kvs[2].Key, testKey5) ||
		!bytes.Equal(kvs[3].Key, testKey6) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value1.RawBytes) ||
		!bytes.Equal(kvs[1].Value.RawBytes, value4.RawBytes) ||
		!bytes.Equal(kvs[2].Value.RawBytes, value5.RawBytes) ||
		!bytes.Equal(kvs[3].Value.RawBytes, value6.RawBytes) {
		t.Fatal("the value should not be empty")
	}

	deleted, resumeSpan, num, err = MVCCDeleteRange(
		context.Background(), engine, nil, testKey4, keyMax, math.MaxInt64, hlc.Timestamp{WallTime: 2}, nil, false,
	)
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
	kvs, _, _, _ = MVCCScan(context.Background(), engine, keyMin, keyMax, math.MaxInt64, hlc.Timestamp{WallTime: 2}, true, nil)
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey1) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value1.RawBytes) {
		t.Fatal("the value should not be empty")
	}

	deleted, resumeSpan, num, err = MVCCDeleteRange(
		context.Background(), engine, nil, keyMin, testKey2, math.MaxInt64, hlc.Timestamp{WallTime: 2}, nil, false,
	)
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
	kvs, _, _, _ = MVCCScan(context.Background(), engine, keyMin, keyMax, math.MaxInt64, hlc.Timestamp{WallTime: 2}, true, nil)
	if len(kvs) != 0 {
		t.Fatal("the value should be empty")
	}
}

func TestMVCCDeleteRangeReturnKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey2, hlc.Timestamp{WallTime: 1}, value2, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey3, hlc.Timestamp{WallTime: 1}, value3, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey4, hlc.Timestamp{WallTime: 1}, value4, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey5, hlc.Timestamp{WallTime: 1}, value5, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey6, hlc.Timestamp{WallTime: 1}, value6, nil); err != nil {
		t.Fatal(err)
	}

	// Attempt to delete two keys.
	deleted, resumeSpan, num, err := MVCCDeleteRange(
		context.Background(), engine, nil, testKey2, testKey6, 2, hlc.Timestamp{WallTime: 2}, nil, true,
	)
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
	kvs, _, _, _ := MVCCScan(context.Background(), engine, keyMin, keyMax, math.MaxInt64, hlc.Timestamp{WallTime: 2}, true, nil)
	if len(kvs) != 4 ||
		!bytes.Equal(kvs[0].Key, testKey1) ||
		!bytes.Equal(kvs[1].Key, testKey4) ||
		!bytes.Equal(kvs[2].Key, testKey5) ||
		!bytes.Equal(kvs[3].Key, testKey6) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value1.RawBytes) ||
		!bytes.Equal(kvs[1].Value.RawBytes, value4.RawBytes) ||
		!bytes.Equal(kvs[2].Value.RawBytes, value5.RawBytes) ||
		!bytes.Equal(kvs[3].Value.RawBytes, value6.RawBytes) {
		t.Fatal("the value should not be empty")
	}

	// Attempt to delete no keys.
	deleted, resumeSpan, num, err = MVCCDeleteRange(
		context.Background(), engine, nil, testKey2, testKey6, 0, hlc.Timestamp{WallTime: 2}, nil, true,
	)
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
	kvs, _, _, _ = MVCCScan(context.Background(), engine, keyMin, keyMax, math.MaxInt64, hlc.Timestamp{WallTime: 2}, true, nil)
	if len(kvs) != 4 ||
		!bytes.Equal(kvs[0].Key, testKey1) ||
		!bytes.Equal(kvs[1].Key, testKey4) ||
		!bytes.Equal(kvs[2].Key, testKey5) ||
		!bytes.Equal(kvs[3].Key, testKey6) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value1.RawBytes) ||
		!bytes.Equal(kvs[1].Value.RawBytes, value4.RawBytes) ||
		!bytes.Equal(kvs[2].Value.RawBytes, value5.RawBytes) ||
		!bytes.Equal(kvs[3].Value.RawBytes, value6.RawBytes) {
		t.Fatal("the value should not be empty")
	}

	deleted, resumeSpan, num, err = MVCCDeleteRange(
		context.Background(), engine, nil, testKey4, keyMax, math.MaxInt64, hlc.Timestamp{WallTime: 2}, nil, true,
	)
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
	kvs, _, _, _ = MVCCScan(context.Background(), engine, keyMin, keyMax, math.MaxInt64, hlc.Timestamp{WallTime: 2}, true, nil)
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey1) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value1.RawBytes) {
		t.Fatal("the value should not be empty")
	}

	deleted, resumeSpan, num, err = MVCCDeleteRange(
		context.Background(), engine, nil, keyMin, testKey2, math.MaxInt64, hlc.Timestamp{WallTime: 2}, nil, true,
	)
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
	kvs, _, _, _ = MVCCScan(context.Background(), engine, keyMin, keyMax, math.MaxInt64, hlc.Timestamp{WallTime: 2}, true, nil)
	if len(kvs) != 0 {
		t.Fatal("the value should be empty")
	}
}

func TestMVCCDeleteRangeFailed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	txn := *txn1
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
		t.Fatal(err)
	}
	txn.Sequence++
	if err := MVCCPut(context.Background(), engine, nil, testKey2, hlc.Timestamp{WallTime: 1}, value2, &txn); err != nil {
		t.Fatal(err)
	}
	txn.Sequence++
	if err := MVCCPut(context.Background(), engine, nil, testKey3, hlc.Timestamp{WallTime: 1}, value3, &txn); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey4, hlc.Timestamp{WallTime: 1}, value4, nil); err != nil {
		t.Fatal(err)
	}

	if _, _, _, err := MVCCDeleteRange(context.Background(), engine, nil, testKey2, testKey4, math.MaxInt64, hlc.Timestamp{WallTime: 1}, nil, false); err == nil {
		t.Fatal("expected error on uncommitted write intent")
	}

	txn.Sequence++
	if _, _, _, err := MVCCDeleteRange(context.Background(), engine, nil, testKey2, testKey4, math.MaxInt64, hlc.Timestamp{WallTime: 1}, &txn, false); err != nil {
		t.Fatal(err)
	}
}

func TestMVCCDeleteRangeConcurrentTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey2, hlc.Timestamp{WallTime: 1}, value2, txn1); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey3, hlc.Timestamp{WallTime: 2}, value3, txn2); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey4, hlc.Timestamp{WallTime: 1}, value4, nil); err != nil {
		t.Fatal(err)
	}

	if _, _, _, err := MVCCDeleteRange(context.Background(), engine, nil, testKey2, testKey4, math.MaxInt64, hlc.Timestamp{WallTime: 1}, txn1, false); err == nil {
		t.Fatal("expected error on uncommitted write intent")
	}
}

// TestMVCCUncommittedDeleteRangeVisible tests that the keys in an uncommitted
// DeleteRange are visible to the same transaction at a higher epoch.
func TestMVCCUncommittedDeleteRangeVisible(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(
		context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil,
	); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(
		context.Background(), engine, nil, testKey2, hlc.Timestamp{WallTime: 1}, value2, nil,
	); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(
		context.Background(), engine, nil, testKey3, hlc.Timestamp{WallTime: 1}, value3, nil,
	); err != nil {
		t.Fatal(err)
	}

	if err := MVCCDelete(
		context.Background(), engine, nil, testKey2, hlc.Timestamp{WallTime: 2, Logical: 1}, nil,
	); err != nil {
		t.Fatal(err)
	}

	txn := txn1.Clone()
	if _, _, _, err := MVCCDeleteRange(
		context.Background(), engine, nil, testKey1, testKey4, math.MaxInt64, hlc.Timestamp{WallTime: 2}, &txn, false,
	); err != nil {
		t.Fatal(err)
	}

	txn.Epoch++
	kvs, _, _, _ := MVCCScan(context.Background(), engine, testKey1, testKey4, math.MaxInt64, hlc.Timestamp{WallTime: 3}, true, &txn)
	if e := 2; len(kvs) != e {
		t.Fatalf("e = %d, got %d", e, len(kvs))
	}
}

func TestMVCCDeleteRangeInline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
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
		if err := MVCCPut(context.Background(), engine, nil, kv.key, hlc.Timestamp{Logical: 0}, kv.value, nil); err != nil {
			t.Fatalf("%d: %s", i, err)
		}
	}

	// Create one non-inline value (non-zero timestamp).
	if err := MVCCPut(context.Background(), engine, nil, testKey6, hlc.Timestamp{WallTime: 1}, value6, nil); err != nil {
		t.Fatal(err)
	}

	// Attempt to delete two inline keys, should succeed.
	deleted, resumeSpan, num, err := MVCCDeleteRange(
		context.Background(), engine, nil, testKey2, testKey6, 2, hlc.Timestamp{Logical: 0}, nil, true,
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

	const inlineMismatchErrString = "put is inline"

	// Attempt to delete inline keys at a timestamp; should fail.
	if _, _, _, err := MVCCDeleteRange(
		context.Background(), engine, nil, testKey1, testKey6, 1, hlc.Timestamp{WallTime: 2}, nil, true,
	); !testutils.IsError(err, inlineMismatchErrString) {
		t.Fatalf("got error %v, expected error with text '%s'", err, inlineMismatchErrString)
	}

	// Attempt to delete non-inline key at zero timestamp; should fail.
	if _, _, _, err := MVCCDeleteRange(
		context.Background(), engine, nil, testKey6, keyMax, 1, hlc.Timestamp{Logical: 0}, nil, true,
	); !testutils.IsError(err, inlineMismatchErrString) {
		t.Fatalf("got error %v, expected error with text '%s'", err, inlineMismatchErrString)
	}

	// Attempt to delete inline keys in a transaction; should fail.
	if _, _, _, err := MVCCDeleteRange(
		context.Background(), engine, nil, testKey2, testKey6, 2, hlc.Timestamp{Logical: 0}, txn1, true,
	); !testutils.IsError(err, "writes not allowed within transactions") {
		t.Errorf("unexpected error: %v", err)
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
	kvs, _, _, err := MVCCScan(context.Background(), engine, keyMin, keyMax, math.MaxInt64, hlc.Timestamp{WallTime: 2}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if a, e := len(kvs), len(expectedKvs); a != e {
		t.Fatalf("engine scan found %d keys; expected %d", a, e)
	}
	kvs[3].Value.Timestamp = hlc.Timestamp{}
	if !reflect.DeepEqual(expectedKvs, kvs) {
		t.Fatalf(
			"engine scan found key/values: %v; expected %v. Diff: %s",
			kvs,
			expectedKvs,
			pretty.Diff(kvs, expectedKvs),
		)
	}
}

func TestMVCCConditionalPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	clock := hlc.NewClock(hlc.NewManualClock(123).UnixNano, time.Nanosecond)

	err := MVCCConditionalPut(context.Background(), engine, nil, testKey1, clock.Now(), value1, &value2, nil)
	if err == nil {
		t.Fatal("expected error on key not exists")
	}
	switch e := err.(type) {
	default:
		t.Fatalf("unexpected error %T", e)
	case *roachpb.ConditionFailedError:
		if e.ActualValue != nil {
			t.Fatalf("expected missing actual value: %v", e.ActualValue)
		}
	}

	// Verify the difference between missing value and empty value.
	err = MVCCConditionalPut(context.Background(), engine, nil, testKey1, clock.Now(), value1, &valueEmpty, nil)
	if err == nil {
		t.Fatal("expected error on key not exists")
	}
	switch e := err.(type) {
	default:
		t.Fatalf("unexpected error %T", e)
	case *roachpb.ConditionFailedError:
		if e.ActualValue != nil {
			t.Fatalf("expected missing actual value: %v", e.ActualValue)
		}
	}

	// Do a conditional put with expectation that the value is completely missing; will succeed.
	err = MVCCConditionalPut(context.Background(), engine, nil, testKey1, clock.Now(), value1, nil, nil)
	if err != nil {
		t.Fatalf("expected success with condition that key doesn't yet exist: %v", err)
	}

	// Another conditional put expecting value missing will fail, now that value1 is written.
	err = MVCCConditionalPut(context.Background(), engine, nil, testKey1, clock.Now(), value1, nil, nil)
	if err == nil {
		t.Fatal("expected error on key already exists")
	}
	var actualValue *roachpb.Value
	switch e := err.(type) {
	default:
		t.Fatalf("unexpected error %T", e)
	case *roachpb.ConditionFailedError:
		actualValue = e.ActualValue
		if !bytes.Equal(e.ActualValue.RawBytes, value1.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				e.ActualValue.RawBytes, value1.RawBytes)
		}
	}

	// Conditional put expecting wrong value2, will fail.
	err = MVCCConditionalPut(context.Background(), engine, nil, testKey1, clock.Now(), value1, &value2, nil)
	if err == nil {
		t.Fatal("expected error on key does not match")
	}
	switch e := err.(type) {
	default:
		t.Fatalf("unexpected error %T", e)
	case *roachpb.ConditionFailedError:
		if actualValue == e.ActualValue {
			t.Fatalf("unexpected sharing of *roachpb.Value")
		}
		if !bytes.Equal(e.ActualValue.RawBytes, value1.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				e.ActualValue.RawBytes, value1.RawBytes)
		}
	}

	// Move to an empty value. Will succeed.
	if err := MVCCConditionalPut(context.Background(), engine, nil, testKey1, clock.Now(), valueEmpty, &value1, nil); err != nil {
		t.Fatal(err)
	}
	// Now move to value2 from expected empty value.
	if err := MVCCConditionalPut(context.Background(), engine, nil, testKey1, clock.Now(), value2, &valueEmpty, nil); err != nil {
		t.Fatal(err)
	}
	// Verify we get value2 as expected.
	value, _, err := MVCCGet(context.Background(), engine, testKey1, clock.Now(), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value2.RawBytes, value.RawBytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.RawBytes, value.RawBytes)
	}
}

func TestMVCCConditionalPutWithTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	clock := hlc.NewClock(hlc.NewManualClock(123).UnixNano, time.Nanosecond)

	// Write value1.
	txn := *txn1
	txn.Sequence++
	if err := MVCCConditionalPut(context.Background(), engine, nil, testKey1, clock.Now(), value1, nil, &txn); err != nil {
		t.Fatal(err)
	}
	// Now, overwrite value1 with value2 from same txn; should see value1 as pre-existing value.
	txn.Sequence++
	if err := MVCCConditionalPut(context.Background(), engine, nil, testKey1, clock.Now(), value2, &value1, &txn); err != nil {
		t.Fatal(err)
	}
	// Writing value3 from a new epoch should see nil again.
	txn.Sequence++
	txn.Epoch = 2
	if err := MVCCConditionalPut(context.Background(), engine, nil, testKey1, clock.Now(), value3, nil, &txn); err != nil {
		t.Fatal(err)
	}
	// Commit value3.
	txnCommit := txn
	txnCommit.Status = roachpb.COMMITTED
	txnCommit.Timestamp = clock.Now().Add(1, 0)
	if err := MVCCResolveWriteIntent(context.Background(), engine, nil, roachpb.Intent{Span: roachpb.Span{Key: testKey1}, Status: txnCommit.Status, Txn: txnCommit.TxnMeta}); err != nil {
		t.Fatal(err)
	}
	// Write value4 with an old timestamp without txn...should get a write too old error.
	err := MVCCConditionalPut(context.Background(), engine, nil, testKey1, clock.Now(), value4, &value3, nil)
	if _, ok := err.(*roachpb.WriteTooOldError); !ok {
		t.Fatalf("expected write too old error; got %s", err)
	}
	expTS := txnCommit.Timestamp.Next()
	if wtoErr, ok := err.(*roachpb.WriteTooOldError); !ok || wtoErr.ActualTimestamp != expTS {
		t.Fatalf("expected wto error with actual timestamp = %s; got %s", expTS, wtoErr)
	}
}

func TestMVCCInitPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	ctx := context.Background()
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
	switch e := err.(type) {
	case *roachpb.ConditionFailedError:
		if !bytes.Equal(e.ActualValue.RawBytes, nil) {
			t.Fatalf("the value %s in get result is not a tombstone", e.ActualValue.RawBytes)
		}
	case nil:
		t.Fatal("MVCCInitPut with a different value did not fail")
	default:
		t.Fatalf("unexpected error %T", e)
	}

	// But doesn't if we *don't* fail on tombstones.
	err = MVCCInitPut(ctx, engine, nil, testKey1, hlc.Timestamp{Logical: 5}, value1, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	// A repeat of the command with a different value will fail.
	err = MVCCInitPut(ctx, engine, nil, testKey1, hlc.Timestamp{Logical: 6}, value2, false, nil)
	switch e := err.(type) {
	case *roachpb.ConditionFailedError:
		if !bytes.Equal(e.ActualValue.RawBytes, value1.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				e.ActualValue.RawBytes, value1.RawBytes)
		}
	case nil:
		t.Fatal("MVCCInitPut with a different value did not fail")
	default:
		t.Fatalf("unexpected error %T", e)
	}

	for _, ts := range []hlc.Timestamp{{Logical: 1}, {Logical: 2}, {WallTime: 1}} {
		value, _, err := MVCCGet(ctx, engine, testKey1, ts, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value1.RawBytes, value.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				value1.RawBytes, value.RawBytes)
		}
		// Ensure that the timestamp didn't get updated.
		expTS := (hlc.Timestamp{Logical: 1})
		if ts.WallTime != 0 {
			// If we're checking the future wall time case, the rewrite after delete
			// will be present.
			expTS.Logical = 5
		}
		if value.Timestamp != expTS {
			t.Errorf("value at timestamp %s seen, expected %s", value.Timestamp, expTS)
		}
	}

	value, _, pErr := MVCCGet(ctx, engine, testKey1, hlc.Timestamp{Logical: 0}, true, nil)
	if pErr != nil {
		t.Fatal(pErr)
	}
	if value != nil {
		t.Fatalf("%v present at old timestamp", value)
	}
}

func TestMVCCInitPutWithTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	clock := hlc.NewClock(hlc.NewManualClock(123).UnixNano, time.Nanosecond)

	txn := *txn1
	txn.Sequence++
	err := MVCCInitPut(context.Background(), engine, nil, testKey1, clock.Now(), value1, false, &txn)
	if err != nil {
		t.Fatal(err)
	}

	// A repeat of the command will still succeed.
	txn.Sequence++
	err = MVCCInitPut(context.Background(), engine, nil, testKey1, clock.Now(), value1, false, &txn)
	if err != nil {
		t.Fatal(err)
	}

	// A repeat of the command with a different value at a different epoch
	// will still succeed.
	txn.Sequence++
	txn.Epoch = 2
	err = MVCCInitPut(context.Background(), engine, nil, testKey1, clock.Now(), value2, false, &txn)
	if err != nil {
		t.Fatal(err)
	}

	// Commit value3.
	txnCommit := txn
	txnCommit.Status = roachpb.COMMITTED
	txnCommit.Timestamp = clock.Now().Add(1, 0)
	if err := MVCCResolveWriteIntent(context.Background(), engine, nil,
		roachpb.Intent{
			Span:   roachpb.Span{Key: testKey1},
			Status: txnCommit.Status,
			Txn:    txnCommit.TxnMeta,
		},
	); err != nil {
		t.Fatal(err)
	}

	// Write value4 with an old timestamp without txn...should get an error.
	err = MVCCInitPut(context.Background(), engine, nil, testKey1, clock.Now(), value4, false, nil)
	switch e := err.(type) {
	case *roachpb.ConditionFailedError:
		if !bytes.Equal(e.ActualValue.RawBytes, value2.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				e.ActualValue.RawBytes, value2.RawBytes)
		}

	default:
		t.Fatalf("unexpected error %T", e)
	}
}

// TestMVCCConditionalPutWriteTooOld verifies the differing behavior
// of conditional puts when writing with an older timestamp than the
// existing write. If there's no transaction, the conditional put
// should use the latest value. When there's a transaction, then it
// should use the value at the specified timestamp.
func TestMVCCConditionalPutWriteTooOld(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	// Write value1 @t=10ns.
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 10}, value1, nil); err != nil {
		t.Fatal(err)
	}
	// Try a non-transactional put @t=1ns with expectation of nil; should fail.
	if err := MVCCConditionalPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value2, nil, nil); err == nil {
		t.Fatal("expected error on conditional put")
	}
	// Now do a non-transactional put @t=1ns with expectation of value1; will succeed @t=10,1.
	err := MVCCConditionalPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value2, &value1, nil)
	expTS := hlc.Timestamp{WallTime: 10, Logical: 1}
	if wtoErr, ok := err.(*roachpb.WriteTooOldError); !ok || wtoErr.ActualTimestamp != expTS {
		t.Fatalf("expected WriteTooOldError with actual time = %s; got %s", expTS, err)
	}
	// Try a transactional put @t=1ns with expectation of value2; should fail.
	if err := MVCCConditionalPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value2, &value1, txn1); err == nil {
		t.Fatal("expected error on conditional put")
	}
	// Now do a transactional put @t=1ns with expectation of nil; will succeed @t=10,2.
	err = MVCCConditionalPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value3, nil, txn1)
	expTS = hlc.Timestamp{WallTime: 10, Logical: 2}
	if wtoErr, ok := err.(*roachpb.WriteTooOldError); !ok || wtoErr.ActualTimestamp != expTS {
		t.Fatalf("expected WriteTooOldError with actual time = %s; got %s", expTS, err)
	}
}

// TestMVCCIncrementWriteTooOld verifies the differing behavior of
// increment when writing with an older timestamp. See comment on
// TestMVCCConditionalPutWriteTooOld for more details.
func TestMVCCIncrementWriteTooOld(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	// Start with an increment.
	if val, err := MVCCIncrement(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 10}, nil, 1); val != 1 || err != nil {
		t.Fatalf("expected val=1 (got %d): %s", val, err)
	}
	// Try a non-transactional increment @t=1ns.
	val, err := MVCCIncrement(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, nil, 1)
	if val != 2 || err == nil {
		t.Fatalf("expected val=2 (got %d) and nil error: %s", val, err)
	}
	expTS := hlc.Timestamp{WallTime: 10, Logical: 1}
	if wtoErr, ok := err.(*roachpb.WriteTooOldError); !ok || wtoErr.ActualTimestamp != expTS {
		t.Fatalf("expected WriteTooOldError with actual time = %s; got %s", expTS, wtoErr)
	}
	// Try a transaction increment @t=1ns.
	val, err = MVCCIncrement(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, txn1, 1)
	if val != 1 || err == nil {
		t.Fatalf("expected val=1 (got %d) and nil error: %s", val, err)
	}
	expTS = hlc.Timestamp{WallTime: 10, Logical: 2}
	if wtoErr, ok := err.(*roachpb.WriteTooOldError); !ok || wtoErr.ActualTimestamp != expTS {
		t.Fatalf("expected WriteTooOldError with actual time = %s; got %s", expTS, wtoErr)
	}
}

// TestMVCCReverseScan verifies that MVCCReverseScan scans [start,
// end) in descending order of keys.
func TestMVCCReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(context.Background(), engine, nil, testKey1,
		hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey1,
		hlc.Timestamp{WallTime: 2}, value2, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey2,
		hlc.Timestamp{WallTime: 1}, value3, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey2,
		hlc.Timestamp{WallTime: 3}, value4, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey3,
		hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey4,
		hlc.Timestamp{WallTime: 1}, value2, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey5,
		hlc.Timestamp{WallTime: 3}, value5, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey6,
		hlc.Timestamp{WallTime: 3}, value6, nil); err != nil {
		t.Fatal(err)
	}

	kvs, resumeSpan, _, err := MVCCReverseScan(context.Background(), engine,
		testKey2, testKey4, math.MaxInt64, hlc.Timestamp{WallTime: 1}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, testKey3) ||
		!bytes.Equal(kvs[1].Key, testKey2) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value1.RawBytes) ||
		!bytes.Equal(kvs[1].Value.RawBytes, value3.RawBytes) {
		t.Fatalf("unexpected value: %v", kvs)
	}
	if resumeSpan != nil {
		t.Fatalf("resumeSpan = %+v", resumeSpan)
	}

	kvs, resumeSpan, _, err = MVCCReverseScan(context.Background(), engine,
		testKey2, testKey4, 1, hlc.Timestamp{WallTime: 1}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey3) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value1.RawBytes) {
		t.Fatalf("unexpected value: %v", kvs)
	}
	if expected := (roachpb.Span{Key: testKey2, EndKey: testKey2.Next()}); !resumeSpan.EqualValue(expected) {
		t.Fatalf("expected = %+v, resumeSpan = %+v", expected, resumeSpan)
	}

	kvs, resumeSpan, _, err = MVCCReverseScan(context.Background(), engine,
		testKey2, testKey4, 0, hlc.Timestamp{WallTime: 1}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 0 {
		t.Fatalf("unexpected value: %v", kvs)
	}
	if expected := (roachpb.Span{Key: testKey2, EndKey: testKey4}); !resumeSpan.EqualValue(expected) {
		t.Fatalf("expected = %+v, resumeSpan = %+v", expected, resumeSpan)
	}

	// The first key we encounter has multiple versions and we need to read the
	// latest.
	kvs, _, _, err = MVCCReverseScan(context.Background(), engine,
		testKey2, testKey3, 1, hlc.Timestamp{WallTime: 4}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey2) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value4.RawBytes) {
		t.Errorf("unexpected value: %v", kvs)
	}

	// The first key we encounter is newer than our read timestamp and we need to
	// back up to the previous key.
	kvs, _, _, err = MVCCReverseScan(context.Background(), engine,
		testKey4, testKey6, 1, hlc.Timestamp{WallTime: 1}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey4) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value2.RawBytes) {
		t.Fatalf("unexpected value: %v", kvs)
	}

	// Scan only the first key in the key space.
	kvs, _, _, err = MVCCReverseScan(context.Background(), engine,
		testKey1, testKey1.Next(), 1, hlc.Timestamp{WallTime: 1}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey1) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value1.RawBytes) {
		t.Fatalf("unexpected value: %v", kvs)
	}
}

// TestMVCCReverseScanFirstKeyInFuture verifies that when MVCCReverseScan scans
// encounter a key with only future timestamps first, that it skips the key and
// continues to scan in reverse. #17825 was caused by this not working correctly.
func TestMVCCReverseScanFirstKeyInFuture(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
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
	if err := MVCCPut(context.Background(), engine, nil, testKey2, hlc.Timestamp{WallTime: 1}, value2, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey3, hlc.Timestamp{WallTime: 3}, value3, nil); err != nil {
		t.Fatal(err)
	}

	kvs, _, _, err := MVCCReverseScan(context.Background(), engine, testKey1, testKey4, math.MaxInt64, hlc.Timestamp{WallTime: 2}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey2) ||
		!bytes.Equal(kvs[0].Value.RawBytes, value2.RawBytes) {
		t.Errorf("unexpected value: %v", kvs)
	}
}

func TestMVCCResolveTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, txn1); err != nil {
		t.Fatal(err)
	}

	{
		value, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{Logical: 1}, true, txn1)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value1.RawBytes, value.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				value1.RawBytes, value.RawBytes)
		}
	}

	// Resolve will write with txn1's timestamp which is 0,1.
	if err := MVCCResolveWriteIntent(context.Background(), engine, nil, roachpb.Intent{Span: roachpb.Span{Key: testKey1}, Txn: txn1Commit.TxnMeta, Status: txn1Commit.Status}); err != nil {
		t.Fatal(err)
	}

	{
		value, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{Logical: 1}, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value1.RawBytes, value.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				value1.RawBytes, value.RawBytes)
		}
	}
}

// TestMVCCResolveNewerIntent verifies that resolving a newer intent
// than the committing transaction aborts the intent.
func TestMVCCResolveNewerIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	// Write first value.
	if err := MVCCPut(context.Background(), engine, nil, testKey1, txn1Commit.Timestamp, value1, nil); err != nil {
		t.Fatal(err)
	}
	// Now, put down an intent which should return a write too old error
	// (but will still write the intent at tx1Commit.Timestmap+1.
	err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value2, txn1)
	if _, ok := err.(*roachpb.WriteTooOldError); !ok {
		t.Fatalf("expected write too old error; got %s", err)
	}

	// Resolve will succeed but should remove the intent.
	if err := MVCCResolveWriteIntent(context.Background(), engine, nil, roachpb.Intent{Span: roachpb.Span{Key: testKey1}, Txn: txn1Commit.TxnMeta, Status: txn1Commit.Status}); err != nil {
		t.Fatal(err)
	}

	value, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{Logical: 2}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value1.RawBytes, value.RawBytes) {
		t.Fatalf("expected value1 bytes; got %q", value.RawBytes)
	}
}

func TestMVCCResolveIntentTxnTimestampMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	txn := txn1.Clone()
	tsEarly := txn.Timestamp
	txn.TxnMeta.Timestamp.Forward(tsEarly.Add(10, 0))

	// Write an intent which has txn.Timestamp > meta.timestamp.
	if err := MVCCPut(
		context.Background(), engine, nil, testKey1, tsEarly, value1, &txn,
	); err != nil {
		t.Fatal(err)
	}

	intent := roachpb.Intent{
		Span:   roachpb.Span{Key: testKey1},
		Status: roachpb.PENDING,
		// The Timestamp within is equal to that of txn.Meta even though
		// the intent sits at tsEarly. The bug was looking at the former
		// instead of the latter (and so we could also tickle it with
		// smaller timestamps in Txn).
		Txn: txn.TxnMeta,
	}

	// A bug (see #7654) caused intents to just stay where they were instead
	// of being moved forward in the situation set up above.
	if err := MVCCResolveWriteIntent(context.Background(), engine, nil, intent); err != nil {
		t.Fatal(err)
	}

	for i, test := range []struct {
		hlc.Timestamp
		found bool
	}{
		// Check that the intent has indeed moved to where we pushed it.
		{tsEarly, false},
		{intent.Txn.Timestamp.Prev(), false},
		{intent.Txn.Timestamp, true},
		{hlc.MaxTimestamp, true},
	} {
		_, _, err := MVCCGet(
			context.Background(), engine, testKey1, test.Timestamp, true, nil,
		)

		if _, ok := err.(*roachpb.WriteIntentError); ok != test.found {
			t.Fatalf("%d: expected write intent error: %t, got %v", i, test.found, err)
		}
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
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 3}, value2, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Check nothing is written if the value doesn't match.
	err = MVCCConditionalPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 2}, value3, &value1, nil)
	if err == nil {
		t.Errorf("unexpected success on conditional put")
	}
	if _, ok := err.(*roachpb.ConditionFailedError); !ok {
		t.Errorf("unexpected error on conditional put: %s", err)
	}

	// But if value does match the most recently written version, we'll get
	// a write too old error but still write updated value.
	err = MVCCConditionalPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 2}, value3, &value2, nil)
	if err == nil {
		t.Errorf("unexpected success on conditional put")
	}
	if _, ok := err.(*roachpb.WriteTooOldError); !ok {
		t.Errorf("unexpected error on conditional put: %s", err)
	}
	// Verify new value was actually written at (3, 1).
	ts := hlc.Timestamp{WallTime: 3, Logical: 1}
	value, _, err := MVCCGet(context.Background(), engine, testKey1, ts, true, nil)
	if err != nil || value.Timestamp != ts || !bytes.Equal(value3.RawBytes, value.RawBytes) {
		t.Fatalf("expected err=nil (got %s), timestamp=%s (got %s), value=%q (got %q)",
			err, value.Timestamp, ts, value3.RawBytes, value.RawBytes)
	}
}

// TestMVCCMultiplePutOldTimestamp tests a case where multiple
// transactional Puts occur to the same key, but with older timestamps
// than a pre-existing key. The first should generate a
// WriteTooOldError and write at a higher timestamp. The second should
// avoid the WriteTooOldError but also write at the higher timestamp.
func TestMVCCMultiplePutOldTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 3}, value1, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the first txn Put returns a write too old error, but the
	// intent is written at the advanced timestamp.
	txn := *txn1
	txn.Sequence++
	err = MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value2, &txn)
	if _, ok := err.(*roachpb.WriteTooOldError); !ok {
		t.Errorf("expected WriteTooOldError on Put; got %v", err)
	}
	// Verify new value was actually written at (3, 1).
	value, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.MaxTimestamp, true, &txn)
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
	err = MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value3, &txn)
	if err != nil {
		t.Error(err)
	}
	// Verify new value was actually written at (3, 1).
	value, _, err = MVCCGet(context.Background(), engine, testKey1, hlc.MaxTimestamp, true, &txn)
	if err != nil {
		t.Fatal(err)
	}
	if value.Timestamp != expTS || !bytes.Equal(value3.RawBytes, value.RawBytes) {
		t.Fatalf("expected timestamp=%s (got %s), value=%q (got %q)",
			value.Timestamp, expTS, value3.RawBytes, value.RawBytes)
	}
}

func TestMVCCAbortTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, txn1); err != nil {
		t.Fatal(err)
	}

	txn1AbortWithTS := txn1Abort.Clone()
	txn1AbortWithTS.Timestamp = hlc.Timestamp{Logical: 1}

	if err := MVCCResolveWriteIntent(context.Background(), engine, nil, roachpb.Intent{Span: roachpb.Span{Key: testKey1}, Txn: txn1AbortWithTS.TxnMeta, Status: txn1AbortWithTS.Status}); err != nil {
		t.Fatal(err)
	}

	if value, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 1}, true, nil); err != nil {
		t.Fatal(err)
	} else if value != nil {
		t.Fatalf("expected the value to be empty: %s", value)
	}
	if meta, err := engine.Get(mvccKey(testKey1)); err != nil {
		t.Fatal(err)
	} else if len(meta) != 0 {
		t.Fatalf("expected no more MVCCMetadata, got: %s", meta)
	}
}

func TestMVCCAbortTxnWithPreviousVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value2, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 2}, value3, txn1); err != nil {
		t.Fatal(err)
	}

	txn1AbortWithTS := txn1Abort.Clone()
	txn1AbortWithTS.Timestamp = hlc.Timestamp{WallTime: 2}

	if err := MVCCResolveWriteIntent(context.Background(), engine, nil, roachpb.Intent{Span: roachpb.Span{Key: testKey1}, Status: txn1AbortWithTS.Status, Txn: txn1AbortWithTS.TxnMeta}); err != nil {
		t.Fatal(err)
	}

	if meta, err := engine.Get(mvccKey(testKey1)); err != nil {
		t.Fatal(err)
	} else if len(meta) != 0 {
		t.Fatalf("expected no more MVCCMetadata, got: %s", meta)
	}

	if value, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 3}, true, nil); err != nil {
		t.Fatal(err)
	} else if expTS := (hlc.Timestamp{WallTime: 1}); value.Timestamp != expTS {
		t.Fatalf("expected timestamp %+v == %+v", value.Timestamp, expTS)
	} else if !bytes.Equal(value2.RawBytes, value.RawBytes) {
		t.Fatalf("the value %q in get result does not match the value %q in request",
			value.RawBytes, value2.RawBytes)
	}
}

func TestMVCCWriteWithDiffTimestampsAndEpochs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	// Start with epoch 1.
	txn := *txn1
	txn.Sequence++
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, &txn); err != nil {
		t.Fatal(err)
	}
	// Now write with greater timestamp and epoch 2.
	txne2 := txn
	txne2.Sequence++
	txne2.Epoch = 2
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value2, &txne2); err != nil {
		t.Fatal(err)
	}
	// Try a write with an earlier timestamp; this is just ignored.
	txne2.Sequence++
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, &txne2); err != nil {
		t.Fatal(err)
	}
	// Try a write with an earlier epoch; again ignored.
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, &txn); err == nil {
		t.Fatal("unexpected success of a write with an earlier epoch")
	}
	// Try a write with different value using both later timestamp and epoch.
	txne2.Sequence++
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value3, &txne2); err != nil {
		t.Fatal(err)
	}
	// Resolve the intent.
	txne2Commit := txne2
	txne2Commit.Status = roachpb.COMMITTED
	txne2Commit.Timestamp = hlc.Timestamp{WallTime: 1}
	if err := MVCCResolveWriteIntent(context.Background(), engine, nil, roachpb.Intent{Span: roachpb.Span{Key: testKey1}, Status: txne2Commit.Status, Txn: txne2Commit.TxnMeta}); err != nil {
		t.Fatal(err)
	}

	expTS := txne2Commit.Timestamp.Add(0, 1)

	// Now try writing an earlier value without a txn--should get WriteTooOldError.
	err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value4, nil)
	if wtoErr, ok := err.(*roachpb.WriteTooOldError); !ok {
		t.Fatal("unexpected success")
	} else if wtoErr.ActualTimestamp != expTS {
		t.Fatalf("expected write too old error with actual ts %s; got %s", expTS, wtoErr.ActualTimestamp)
	}
	// Verify value was actually written at (1, 1).
	value, _, err := MVCCGet(context.Background(), engine, testKey1, expTS, true, nil)
	if err != nil || value.Timestamp != expTS || !bytes.Equal(value4.RawBytes, value.RawBytes) {
		t.Fatalf("expected err=nil (got %s), timestamp=%s (got %s), value=%q (got %q)",
			err, value.Timestamp, expTS, value4.RawBytes, value.RawBytes)
	}
	// Now write an intent with exactly the same timestamp--ties also get WriteTooOldError.
	err = MVCCPut(context.Background(), engine, nil, testKey1, expTS, value5, txn2)
	intentTS := expTS.Add(0, 1)
	if wtoErr, ok := err.(*roachpb.WriteTooOldError); !ok {
		t.Fatal("unexpected success")
	} else if wtoErr.ActualTimestamp != intentTS {
		t.Fatalf("expected write too old error with actual ts %s; got %s", intentTS, wtoErr.ActualTimestamp)
	}
	// Verify intent value was actually written at (1, 2).
	value, _, err = MVCCGet(context.Background(), engine, testKey1, intentTS, true, txn2)
	if err != nil || value.Timestamp != intentTS || !bytes.Equal(value5.RawBytes, value.RawBytes) {
		t.Fatalf("expected err=nil (got %s), timestamp=%s (got %s), value=%q (got %q)",
			err, value.Timestamp, intentTS, value5.RawBytes, value.RawBytes)
	}
	// Attempt to read older timestamp; should fail.
	value, _, err = MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{Logical: 0}, true, nil)
	if value != nil || err != nil {
		t.Fatalf("expected value nil, err nil; got %+v, %v", value, err)
	}
	// Read at correct timestamp.
	value, _, err = MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 1}, true, nil)
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
}

// TestMVCCReadWithDiffEpochs writes a value first using epoch 1, then
// reads using epoch 2 to verify that values written during different
// transaction epochs are not visible.
func TestMVCCReadWithDiffEpochs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	// Write initial value without a txn.
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, nil); err != nil {
		t.Fatal(err)
	}
	// Now write using txn1, epoch 1.
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value2, txn1); err != nil {
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
		value, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 2}, true, test.txn)
		if test.expErr {
			if err == nil {
				t.Errorf("test %d: unexpected success", i)
			} else if _, ok := err.(*roachpb.WriteIntentError); !ok {
				t.Errorf("test %d: expected write intent error; got %v", i, err)
			}
		} else if err != nil || value == nil || !bytes.Equal(test.expValue.RawBytes, value.RawBytes) {
			t.Errorf("test %d: expected value %q, err nil; got %+v, %v", i, test.expValue.RawBytes, value, err)
		}
	}
}

// TestMVCCReadWithOldEpoch writes a value first using epoch 2, then
// reads using epoch 1 to verify that the read will fail.
func TestMVCCReadWithOldEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value2, txn1e2); err != nil {
		t.Fatal(err)
	}
	_, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 2}, true, txn1)
	if err == nil {
		t.Fatalf("unexpected success of get")
	}
}

// TestMVCCWriteWithSequenceAndBatchIndex verifies that retry errors
// are thrown in the event that earlier sequence numbers are
// encountered or if the same sequence and an earlier or same batch
// index.
func TestMVCCWriteWithSequenceAndBatchIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	testCases := []struct {
		sequence   int32
		batchIndex int32
		expRetry   bool
	}{
		{1, 0, true},  // old sequence old batch index
		{1, 1, true},  // old sequence, same batch index
		{1, 2, true},  // old sequence, new batch index
		{2, 0, true},  // same sequence, old batch index
		{2, 1, true},  // same sequence, same batch index
		{2, 2, false}, // same sequence, new batch index
		{3, 0, false}, // new sequence, old batch index
		{3, 1, false}, // new sequence, same batch index
		{3, 2, false}, // new sequence, new batch index
	}

	ts := hlc.Timestamp{Logical: 1}
	for i, tc := range testCases {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		// Start with sequence 2, batch index 1.
		txn := *txn1
		txn.Sequence = 2
		txn.BatchIndex = 1
		if err := MVCCPut(context.Background(), engine, nil, key, ts, value1, &txn); err != nil {
			t.Fatal(err)
		}

		txn.Sequence, txn.BatchIndex = tc.sequence, tc.batchIndex
		err := MVCCPut(context.Background(), engine, nil, key, ts, value2, &txn)
		_, ok := err.(*roachpb.TransactionRetryError)
		if !tc.expRetry && ok {
			t.Fatalf("%d: unexpected error: %s", i, err)
		} else if ok != tc.expRetry {
			t.Fatalf("%d: expected retry %t but got %s", i, tc.expRetry, err)
		}
	}
}

// TestMVCCReadWithPushedTimestamp verifies that a read for a value
// written by the transaction, but then subsequently pushed, can still
// be read by the txn at the later timestamp, even if an earlier
// timestamp is specified. This happens when a txn's intents are
// resolved by other actors; the intents shouldn't become invisible
// to pushed txn.
func TestMVCCReadWithPushedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	// Start with epoch 1.
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, txn1); err != nil {
		t.Fatal(err)
	}
	// Resolve the intent, pushing its timestamp forward.
	txn := makeTxn(*txn1, hlc.Timestamp{WallTime: 1})
	if err := MVCCResolveWriteIntent(context.Background(), engine, nil, roachpb.Intent{Span: roachpb.Span{Key: testKey1}, Status: txn.Status, Txn: txn.TxnMeta}); err != nil {
		t.Fatal(err)
	}
	// Attempt to read using naive txn's previous timestamp.
	value, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{Logical: 1}, true, txn1)
	if err != nil || value == nil || !bytes.Equal(value.RawBytes, value1.RawBytes) {
		t.Errorf("expected value %q, err nil; got %+v, %v", value1.RawBytes, value, err)
	}
}

func TestMVCCResolveWithDiffEpochs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, txn1); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey2, hlc.Timestamp{Logical: 1}, value2, txn1e2); err != nil {
		t.Fatal(err)
	}
	num, _, err := MVCCResolveWriteIntentRange(context.Background(), engine, nil, roachpb.Intent{Span: roachpb.Span{Key: testKey1, EndKey: testKey2.Next()}, Txn: txn1e2Commit.TxnMeta, Status: txn1e2Commit.Status}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if num != 2 {
		t.Errorf("expected 2 rows resolved; got %d", num)
	}

	// Verify key1 is empty, as resolution with epoch 2 would have
	// aborted the epoch 1 intent.
	if value, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{Logical: 1}, true, nil); value != nil || err != nil {
		t.Errorf("expected value nil, err nil; got %+v, %v", value, err)
	}

	// Key2 should be committed.
	value, _, err := MVCCGet(context.Background(), engine, testKey2, hlc.Timestamp{Logical: 1}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value2.RawBytes, value.RawBytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value2.RawBytes, value.RawBytes)
	}
}

func TestMVCCResolveWithUpdatedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, txn1); err != nil {
		t.Fatal(err)
	}

	value, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 1}, true, txn1)
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
	if err = MVCCResolveWriteIntent(context.Background(), engine, nil, roachpb.Intent{Span: roachpb.Span{Key: testKey1}, Status: txn.Status, Txn: txn.TxnMeta}); err != nil {
		t.Fatal(err)
	}

	if value, _, err = MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{Logical: 1}, true, nil); value != nil || err != nil {
		t.Fatalf("expected both value and err to be nil: %+v, %v", value, err)
	}

	value, _, err = MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 1}, true, nil)
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
}

func TestMVCCResolveWithPushedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, txn1); err != nil {
		t.Fatal(err)
	}
	value, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 1}, true, txn1)
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
	if err = MVCCResolveWriteIntent(context.Background(), engine, nil, roachpb.Intent{Span: roachpb.Span{Key: testKey1}, Status: txn.Status, Txn: txn.TxnMeta}); err != nil {
		t.Fatal(err)
	}

	if value, _, err = MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 1}, true, nil); value != nil || err == nil {
		t.Fatalf("expected both value nil and err to be a writeIntentError: %+v", value)
	}

	// Can still fetch the value using txn1.
	value, _, err = MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{WallTime: 1}, true, txn1)
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
}

func TestMVCCResolveTxnNoOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	// Resolve a non existent key; noop.
	if err := MVCCResolveWriteIntent(context.Background(), engine, nil, roachpb.Intent{Span: roachpb.Span{Key: testKey1}, Status: txn1Commit.Status, Txn: txn1Commit.TxnMeta}); err != nil {
		t.Fatal(err)
	}

	// Add key and resolve despite there being no intent.
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCResolveWriteIntent(context.Background(), engine, nil, roachpb.Intent{Span: roachpb.Span{Key: testKey1}, Status: txn2Commit.Status, Txn: txn2Commit.TxnMeta}); err != nil {
		t.Fatal(err)
	}

	// Write intent and resolve with different txn.
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{WallTime: 1}, value2, txn1); err != nil {
		t.Fatal(err)
	}

	txn1CommitWithTS := txn2Commit.Clone()
	txn1CommitWithTS.Timestamp = hlc.Timestamp{WallTime: 1}
	if err := MVCCResolveWriteIntent(context.Background(), engine, nil, roachpb.Intent{Span: roachpb.Span{Key: testKey1}, Status: txn1CommitWithTS.Status, Txn: txn1CommitWithTS.TxnMeta}); err != nil {
		t.Fatal(err)
	}
}

func TestMVCCResolveTxnRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, txn1); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey2, hlc.Timestamp{Logical: 1}, value2, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey3, hlc.Timestamp{Logical: 1}, value3, txn2); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(context.Background(), engine, nil, testKey4, hlc.Timestamp{Logical: 1}, value4, txn1); err != nil {
		t.Fatal(err)
	}

	num, resumeSpan, err := MVCCResolveWriteIntentRange(context.Background(), engine, nil, roachpb.Intent{Span: roachpb.Span{Key: testKey1, EndKey: testKey4.Next()}, Txn: txn1Commit.TxnMeta, Status: txn1Commit.Status}, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if num != 2 || resumeSpan != nil {
		t.Fatalf("expected all keys to process for resolution, even though 2 are noops; got %d, resume=%s", num, resumeSpan)
	}

	{
		value, _, err := MVCCGet(context.Background(), engine, testKey1, hlc.Timestamp{Logical: 1}, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value1.RawBytes, value.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				value1.RawBytes, value.RawBytes)
		}
	}
	{
		value, _, err := MVCCGet(context.Background(), engine, testKey2, hlc.Timestamp{Logical: 1}, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value2.RawBytes, value.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				value2.RawBytes, value.RawBytes)
		}
	}
	{
		value, _, err := MVCCGet(context.Background(), engine, testKey3, hlc.Timestamp{Logical: 1}, true, txn2)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value3.RawBytes, value.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				value3.RawBytes, value.RawBytes)
		}
	}
	{
		value, _, err := MVCCGet(context.Background(), engine, testKey4, hlc.Timestamp{Logical: 1}, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value4.RawBytes, value.RawBytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				value1.RawBytes, value.RawBytes)
		}
	}
}

func TestMVCCResolveTxnRangeResume(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	// Write 10 keys from txn1, 10 from txn2, and 10 with no txn, interleaved.
	for i := 0; i < 30; i += 3 {
		if err := MVCCPut(context.Background(), engine, nil, roachpb.Key(fmt.Sprintf("%02d", i+0)), hlc.Timestamp{Logical: 1}, value1, txn1); err != nil {
			t.Fatal(err)
		}
		if err := MVCCPut(context.Background(), engine, nil, roachpb.Key(fmt.Sprintf("%02d", i+1)), hlc.Timestamp{Logical: 2}, value2, txn2); err != nil {
			t.Fatal(err)
		}
		if err := MVCCPut(context.Background(), engine, nil, roachpb.Key(fmt.Sprintf("%02d", i+2)), hlc.Timestamp{Logical: 3}, value3, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Resolve up to 5 intents.
	num, resumeSpan, err := MVCCResolveWriteIntentRange(context.Background(), engine, nil, roachpb.Intent{Span: roachpb.Span{Key: roachpb.Key("00"), EndKey: roachpb.Key("30")}, Txn: txn1Commit.TxnMeta, Status: txn1Commit.Status}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if num != 5 || resumeSpan == nil {
		t.Errorf("expected resolution for only 5 keys; got %d, resume=%s", num, resumeSpan)
	}
	expResumeSpan := &roachpb.Span{Key: roachpb.Key("12").Next(), EndKey: roachpb.Key("30")}
	if !resumeSpan.Equal(expResumeSpan) {
		t.Errorf("expected resume span %s; got %s", expResumeSpan, resumeSpan)
	}
}

func TestValidSplitKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()

	versionedTestCases := map[bool][]struct {
		key   roachpb.Key
		valid bool
	}{
		false /* allowMeta2Splits */ : {
			{roachpb.Key("\x02"), false},
			{roachpb.Key("\x02\x00"), false},
			{roachpb.Key("\x02\xff"), false},
			{roachpb.Key("\x03"), false},
			{roachpb.Key("\x03\x00"), false},
			{roachpb.Key("\x03\xff"), false},
			{roachpb.Key("\x03\xff\xff"), false},
			{roachpb.Key("\x03\xff\xff\x88"), false},
			{roachpb.Key("\x04"), true},
			{roachpb.Key("\x05"), true},
			{roachpb.Key("a"), true},
			{roachpb.Key("\xff"), true},
			{roachpb.Key("\xff\x01"), true},
			{roachpb.Key(keys.MakeTablePrefix(keys.MaxSystemConfigDescID)), false},
			{roachpb.Key(keys.MakeTablePrefix(keys.MaxSystemConfigDescID + 1)), true},
		},
		true /* allowMeta2Splits */ : {
			{roachpb.Key("\x02"), false},
			{roachpb.Key("\x02\x00"), false},
			{roachpb.Key("\x02\xff"), false},
			{roachpb.Key("\x03"), true},     // different
			{roachpb.Key("\x03\x00"), true}, // different
			{roachpb.Key("\x03\xff"), true}, // different
			{roachpb.Key("\x03\xff\xff"), false},
			{roachpb.Key("\x03\xff\xff\x88"), false},
			{roachpb.Key("\x04"), true},
			{roachpb.Key("\x05"), true},
			{roachpb.Key("a"), true},
			{roachpb.Key("\xff"), true},
			{roachpb.Key("\xff\x01"), true},
			{roachpb.Key(keys.MakeTablePrefix(keys.MaxSystemConfigDescID)), false},
			{roachpb.Key(keys.MakeTablePrefix(keys.MaxSystemConfigDescID + 1)), true},
		},
	}
	for allowMeta2Splits, testCases := range versionedTestCases {
		t.Run(fmt.Sprintf("allowMeta2Splits=%t", allowMeta2Splits), func(t *testing.T) {
			for i, test := range testCases {
				valid := IsValidSplitKey(test.key, allowMeta2Splits)
				if valid != test.valid {
					t.Errorf("%d: expected %q [%x] valid %t; got %t",
						i, test.key, []byte(test.key), test.valid, valid)
				}
			}
		})
	}
}

func TestFindSplitKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
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
		if err := MVCCPut(context.Background(), engine, ms, []byte(k), hlc.Timestamp{Logical: 1}, val, nil); err != nil {
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
		humanSplitKey, err := MVCCFindSplitKey(context.Background(), engine,
			roachpb.RKeyMin, roachpb.RKeyMax, td.targetSize, true /* allowMeta2Splits */)
		if err != nil {
			t.Fatal(err)
		}
		ind, err := strconv.Atoi(string(humanSplitKey))
		if err != nil {
			t.Fatalf("%d: could not parse key %s as int: %v", i, humanSplitKey, err)
		}
		if ind == 0 {
			t.Fatalf("%d: should never select first key as split key", i)
		}
		if diff := td.splitInd - ind; diff > 1 || diff < -1 {
			t.Fatalf("%d: wanted key #%d+-1, but got %d (diff %d)", i, td.splitInd, ind, diff)
		}
	}
}

// TestFindValidSplitKeys verifies split keys are located such that
// they avoid splits through invalid key ranges.
func TestFindValidSplitKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Manually creates rows corresponding to the schema:
	// CREATE TABLE t (id STRING PRIMARY KEY, col INT)
	encodeTableKey := func(rowVal string, colFam uint32) roachpb.Key {
		tableKey := keys.MakeTablePrefix(keys.MaxReservedDescID + 1)
		rowKey := roachpb.Key(encoding.EncodeVarintAscending(append([]byte(nil), tableKey...), 1))
		rowKey = encoding.EncodeStringAscending(encoding.EncodeVarintAscending(rowKey, 1), rowVal)
		colKey := keys.MakeFamilyKey(append([]byte(nil), rowKey...), colFam)
		return colKey
	}
	splitKeyFromTableKey := func(tableKey roachpb.Key) roachpb.Key {
		splitKey, err := keys.EnsureSafeSplitKey(tableKey)
		if err != nil {
			t.Fatal(err)
		}
		return splitKey
	}

	testCases := []struct {
		keys       []roachpb.Key
		rangeStart roachpb.Key // optional
		expSplit   roachpb.Key
		expError   bool
	}{
		// All m1 cannot be split.
		{
			keys: []roachpb.Key{
				roachpb.Key("\x02"),
				roachpb.Key("\x02\x00"),
				roachpb.Key("\x02\xff"),
			},
			expSplit: nil,
			expError: false,
		},
		// All system span cannot be split.
		{
			keys: []roachpb.Key{
				roachpb.Key(keys.MakeTablePrefix(1)),
				roachpb.Key(keys.MakeTablePrefix(keys.MaxSystemConfigDescID)),
			},
			expSplit: nil,
			expError: false,
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
			expSplit: roachpb.Key("\x03"),
			expError: false,
		},
		// Even lopsided, always split at meta2.
		{
			keys: []roachpb.Key{
				roachpb.Key("\x02"),
				roachpb.Key("\x02\x00"),
				roachpb.Key("\x02\xff"),
				roachpb.Key("\x03"),
			},
			expSplit: roachpb.Key("\x03"),
			expError: false,
		},
		// Between meta2Max and metaMax, splits at metaMax.
		{
			keys: []roachpb.Key{
				roachpb.Key("\x03\xff\xff"),
				roachpb.Key("\x03\xff\xff\x88"),
				roachpb.Key("\x04"),
				roachpb.Key("\x04\xff\xff\x88"),
			},
			expSplit: roachpb.Key("\x04"),
			expError: false,
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
			expSplit: roachpb.Key("\x04"),
			expError: false,
		},
		// Lopsided, truncate non-zone prefix.
		{
			keys: []roachpb.Key{
				roachpb.Key("\x04zond"),
				roachpb.Key("\x04zone"),
				roachpb.Key("\x04zone\x00"),
				roachpb.Key("\x04zone\xff"),
			},
			expSplit: roachpb.Key("\x04zone\x00"),
			expError: false,
		},
		// Lopsided, truncate non-zone suffix.
		{
			keys: []roachpb.Key{
				roachpb.Key("\x04zone"),
				roachpb.Key("\x04zone\x00"),
				roachpb.Key("\x04zone\xff"),
				roachpb.Key("\x04zonf"),
			},
			expSplit: roachpb.Key("\x04zone\xff"),
			expError: false,
		},
		// A Range for which MVCCSplitKey would return the start key isn't fair
		// game, even if the StartKey actually exists. There was once a bug
		// here which compared timestamps as well as keys and thus didn't
		// realize it was splitting at the initial key (due to getting confused
		// by the actual value having a nonzero timestamp).
		{
			keys: []roachpb.Key{
				roachpb.Key("a"),
			},
			expSplit: nil,
			expError: false,
		},
		// Some example table data. Make sure we don't split in the middle of a row
		// or return the start key of the range.
		{
			keys: []roachpb.Key{
				encodeTableKey("a", 1),
				encodeTableKey("a", 2),
				encodeTableKey("a", 3),
				encodeTableKey("a", 4),
				encodeTableKey("a", 5),
				encodeTableKey("b", 1),
				encodeTableKey("c", 1),
			},
			rangeStart: splitKeyFromTableKey(encodeTableKey("a", 1)),
			expSplit:   splitKeyFromTableKey(encodeTableKey("b", 1)),
			expError:   false,
		},
		// More example table data. Make sure ranges at the start of a table can
		// be split properly - this checks that the minSplitKey logic doesn't
		// break for such ranges.
		{
			keys: []roachpb.Key{
				encodeTableKey("a", 1),
				encodeTableKey("b", 1),
				encodeTableKey("c", 1),
				encodeTableKey("d", 1),
			},
			rangeStart: keys.MakeTablePrefix(keys.MaxReservedDescID + 1),
			expSplit:   splitKeyFromTableKey(encodeTableKey("c", 1)),
			expError:   false,
		},
		// More example table data. Make sure ranges at the start of a table can
		// be split properly (even if "properly" means creating an empty LHS,
		// splitting here will at least allow the resulting RHS to split again).
		{
			keys: []roachpb.Key{
				encodeTableKey("a", 1),
				encodeTableKey("a", 2),
				encodeTableKey("a", 3),
				encodeTableKey("a", 4),
				encodeTableKey("a", 5),
				encodeTableKey("b", 1),
				encodeTableKey("c", 1),
			},
			rangeStart: keys.MakeTablePrefix(keys.MaxReservedDescID + 1),
			expSplit:   splitKeyFromTableKey(encodeTableKey("a", 1)),
			expError:   false,
		},
	}

	for i, test := range testCases {
		t.Run("", func(t *testing.T) {
			engine := createTestEngine()
			defer engine.Close()

			ms := &enginepb.MVCCStats{}
			val := roachpb.MakeValueFromString(strings.Repeat("X", 10))
			for _, k := range test.keys {
				if err := MVCCPut(context.Background(), engine, ms, []byte(k), hlc.Timestamp{Logical: 1}, val, nil); err != nil {
					t.Fatal(err)
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
			splitKey, err := MVCCFindSplitKey(context.Background(), engine,
				rangeStartAddr, rangeEndAddr, targetSize, true /* allowMeta2Splits */)
			if test.expError {
				if !testutils.IsError(err, "has no valid splits") {
					t.Fatalf("%d: unexpected error: %v", i, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("%d; unexpected error: %s", i, err)
			}
			if !splitKey.Equal(test.expSplit) {
				t.Errorf("%d: expected split key %q; got %q", i, test.expSplit, splitKey)
			}
		})
	}
}

// TestFindBalancedSplitKeys verifies split keys are located such that
// the left and right halves are equally balanced.
func TestFindBalancedSplitKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
			engine := createTestEngine()
			defer engine.Close()

			ms := &enginepb.MVCCStats{}
			var expKey roachpb.Key
			for j, keySize := range test.keySizes {
				key := roachpb.Key(fmt.Sprintf("%d%s", j, strings.Repeat("X", keySize)))
				if test.expSplit == j {
					expKey = key
				}
				val := roachpb.MakeValueFromString(strings.Repeat("X", test.valSizes[j]))
				if err := MVCCPut(context.Background(), engine, ms, key, hlc.Timestamp{Logical: 1}, val, nil); err != nil {
					t.Fatal(err)
				}
			}
			targetSize := (ms.KeyBytes + ms.ValBytes) / 2
			splitKey, err := MVCCFindSplitKey(context.Background(), engine,
				roachpb.RKey("\x02"), roachpb.RKeyMax, targetSize, true /* allowMeta2Splits */)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if !splitKey.Equal(expKey) {
				t.Errorf("%d: expected split key %q; got %q", i, expKey, splitKey)
			}
		})
	}
}

// TestMVCCGarbageCollect writes a series of gc'able bytes and then
// sends an MVCC GC request and verifies cleared values and updated
// stats.
func TestMVCCGarbageCollect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	ms := &enginepb.MVCCStats{}

	bytes := []byte("value")
	ts1 := hlc.Timestamp{WallTime: 1E9}
	ts2 := hlc.Timestamp{WallTime: 2E9}
	ts3 := hlc.Timestamp{WallTime: 3E9}
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
					if err := MVCCDelete(context.Background(), engine, ms, test.key, val.Timestamp, nil); err != nil {
						t.Fatal(err)
					}
					continue
				}
				valCpy := *protoutil.Clone(&val).(*roachpb.Value)
				valCpy.Timestamp = hlc.Timestamp{}
				if err := MVCCPut(context.Background(), engine, ms, test.key, val.Timestamp, valCpy, nil); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if log.V(1) {
		kvsn, err := Scan(engine, mvccKey(keyMin), mvccKey(keyMax), 0)
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
	kvs, err := Scan(engine, mvccKey(keyMin), mvccKey(keyMax), 0)
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
	iter := engine.NewIterator(false)
	defer iter.Close()
	for _, mvccStatsTest := range mvccStatsTests {
		t.Run(mvccStatsTest.name, func(t *testing.T) {
			expMS, err := mvccStatsTest.fn(iter, mvccKey(roachpb.KeyMin),
				mvccKey(roachpb.KeyMax), ts3.WallTime)
			if err != nil {
				t.Fatal(err)
			}
			assertEq(t, engine, "verification", ms, &expMS)
		})
	}
}

// TestMVCCGarbageCollectNonDeleted verifies that the first value for
// a key cannot be GC'd if it's not deleted.
func TestMVCCGarbageCollectNonDeleted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	s := "string"
	ts1 := hlc.Timestamp{WallTime: 1E9}
	ts2 := hlc.Timestamp{WallTime: 2E9}
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
			if err := MVCCPut(context.Background(), engine, nil, test.key, val.Timestamp, valCpy, nil); err != nil {
				t.Fatal(err)
			}
		}
		keys := []roachpb.GCRequest_GCKey{
			{Key: test.key, Timestamp: ts2},
		}
		err := MVCCGarbageCollect(context.Background(), engine, nil, keys, ts2)
		if !testutils.IsError(err, test.expError) {
			t.Fatalf("expected error %q when garbage collecting a non-deleted live value, found %v", test.expError, err)
		}
	}

}

// TestMVCCGarbageCollectIntent verifies that an intent cannot be GC'd.
func TestMVCCGarbageCollectIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	bytes := []byte("value")
	ts1 := hlc.Timestamp{WallTime: 1E9}
	ts2 := hlc.Timestamp{WallTime: 2E9}
	key := roachpb.Key("a")
	{
		val1 := roachpb.MakeValueFromBytes(bytes)
		if err := MVCCPut(context.Background(), engine, nil, key, ts1, val1, nil); err != nil {
			t.Fatal(err)
		}
	}
	txn := &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{ID: uuid.MakeV4(), Timestamp: ts2}}
	if err := MVCCDelete(context.Background(), engine, nil, key, ts2, txn); err != nil {
		t.Fatal(err)
	}
	keys := []roachpb.GCRequest_GCKey{
		{Key: key, Timestamp: ts2},
	}
	if err := MVCCGarbageCollect(
		context.Background(), engine, nil, keys, ts2,
	); err == nil {
		t.Fatal("expected error garbage collecting an intent")
	}
}

// TestResolveIntentWithLowerEpoch verifies that trying to resolve
// an intent at an epoch that is lower than the epoch of the intent
// leaves the intent untouched.
func TestResolveIntentWithLowerEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	// Lay down an intent with a high epoch.
	if err := MVCCPut(context.Background(), engine, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, txn1e2); err != nil {
		t.Fatal(err)
	}
	// Resolve the intent with a low epoch.
	if err := MVCCResolveWriteIntent(context.Background(), engine, nil, roachpb.Intent{Span: roachpb.Span{Key: testKey1}, Status: txn1.Status, Txn: txn1.TxnMeta}); err != nil {
		t.Fatal(err)
	}

	// Check that the intent was not cleared.
	metaKey := mvccKey(testKey1)
	meta := &enginepb.MVCCMetadata{}
	ok, _, _, err := engine.GetProto(metaKey, meta)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("intent should not be cleared by resolve intent request with lower epoch")
	}
}

// TestMVCCTimeSeriesPartialMerge ensures that "partial merges" of merged time
// series data does not result in a different final result than a "full merge".
func TestMVCCTimeSeriesPartialMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	// Perform the same sequence of merges on two different keys. For
	// one of them, insert some compactions which cause partial merges
	// to be run and affect the results.
	vals := make([]*roachpb.Value, 2)

	for i, k := range []roachpb.Key{testKey1, testKey2} {
		if err := MVCCMerge(context.Background(), engine, nil, k, hlc.Timestamp{Logical: 1}, tsvalue1); err != nil {
			t.Fatal(err)
		}
		if err := MVCCMerge(context.Background(), engine, nil, k, hlc.Timestamp{Logical: 2}, tsvalue2); err != nil {
			t.Fatal(err)
		}

		if i == 1 {
			if err := engine.(InMem).Compact(); err != nil {
				t.Fatal(err)
			}
		}

		if err := MVCCMerge(context.Background(), engine, nil, k, hlc.Timestamp{Logical: 2}, tsvalue2); err != nil {
			t.Fatal(err)
		}
		if err := MVCCMerge(context.Background(), engine, nil, k, hlc.Timestamp{Logical: 1}, tsvalue1); err != nil {
			t.Fatal(err)
		}

		if i == 1 {
			if err := engine.(InMem).Compact(); err != nil {
				t.Fatal(err)
			}
		}

		if v, _, err := MVCCGet(context.Background(), engine, k, hlc.Timestamp{}, true, nil); err != nil {
			t.Fatal(err)
		} else {
			vals[i] = v
		}
	}

	if first, second := vals[0], vals[1]; !proto.Equal(first, second) {
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
