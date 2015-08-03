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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Jiang-Ming Yang (jiangming.yang@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package engine

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/randutil"
	gogoproto "github.com/gogo/protobuf/proto"
)

// Constants for system-reserved keys in the KV map.
var (
	testKey1     = proto.Key("/db1")
	testKey2     = proto.Key("/db2")
	testKey3     = proto.Key("/db3")
	testKey4     = proto.Key("/db4")
	txn1         = &proto.Transaction{Key: proto.Key("a"), ID: []byte("Txn1"), Epoch: 1}
	txn1Commit   = &proto.Transaction{Key: proto.Key("a"), ID: []byte("Txn1"), Epoch: 1, Status: proto.COMMITTED}
	txn1Abort    = &proto.Transaction{Key: proto.Key("a"), ID: []byte("Txn1"), Epoch: 1, Status: proto.ABORTED}
	txn1e2       = &proto.Transaction{Key: proto.Key("a"), ID: []byte("Txn1"), Epoch: 2}
	txn1e2Commit = &proto.Transaction{Key: proto.Key("a"), ID: []byte("Txn1"), Epoch: 2, Status: proto.COMMITTED}
	txn2         = &proto.Transaction{Key: proto.Key("a"), ID: []byte("Txn2")}
	txn2Commit   = &proto.Transaction{Key: proto.Key("a"), ID: []byte("Txn2"), Status: proto.COMMITTED}
	value1       = proto.Value{Bytes: []byte("testValue1")}
	value2       = proto.Value{Bytes: []byte("testValue2")}
	value3       = proto.Value{Bytes: []byte("testValue3")}
	value4       = proto.Value{Bytes: []byte("testValue4")}
	valueEmpty   = proto.Value{}
)

// createTestEngine returns a new in-memory engine with 1MB of storage
// capacity.
func createTestEngine() Engine {
	return NewInMem(proto.Attributes{}, 1<<20)
}

// makeTxn creates a new transaction using the specified base
// txn and timestamp.
func makeTxn(baseTxn *proto.Transaction, ts proto.Timestamp) *proto.Transaction {
	txn := gogoproto.Clone(baseTxn).(*proto.Transaction)
	txn.Timestamp = ts
	return txn
}

// makeTS creates a new hybrid logical timestamp.
func makeTS(nanos int64, logical int32) proto.Timestamp {
	return proto.Timestamp{
		WallTime: nanos,
		Logical:  logical,
	}
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
	defer leaktest.AfterTest(t)
	aKey := proto.Key("a")
	a0Key := proto.Key("a\x00")
	keys := []string{
		string(MVCCEncodeKey(aKey)),
		string(MVCCEncodeVersionKey(aKey, makeTS(math.MaxInt64, 0))),
		string(MVCCEncodeVersionKey(aKey, makeTS(1, 0))),
		string(MVCCEncodeVersionKey(aKey, makeTS(0, 0))),
		string(MVCCEncodeKey(a0Key)),
		string(MVCCEncodeVersionKey(a0Key, makeTS(math.MaxInt64, 0))),
		string(MVCCEncodeVersionKey(a0Key, makeTS(1, 0))),
		string(MVCCEncodeVersionKey(a0Key, makeTS(0, 0))),
	}
	sortKeys := make([]string, len(keys))
	copy(sortKeys, keys)
	sort.Strings(sortKeys)
	if !reflect.DeepEqual(sortKeys, keys) {
		t.Errorf("expected keys to sort in order %s, but got %s", keys, sortKeys)
	}
}

func TestMVCCEmptyKey(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	if _, _, err := MVCCGet(engine, proto.Key{}, makeTS(0, 1), true, nil); err == nil {
		t.Error("expected empty key error")
	}
	if err := MVCCPut(engine, nil, proto.Key{}, makeTS(0, 1), value1, nil); err == nil {
		t.Error("expected empty key error")
	}
	if _, _, err := MVCCScan(engine, proto.Key{}, testKey1, 0, makeTS(0, 1), true, nil); err != nil {
		t.Errorf("empty key allowed for start key in scan; got %s", err)
	}
	if _, _, err := MVCCScan(engine, testKey1, proto.Key{}, 0, makeTS(0, 1), true, nil); err == nil {
		t.Error("expected empty key error")
	}
	if err := MVCCResolveWriteIntent(engine, nil, proto.Key{}, makeTS(0, 1), txn1); err == nil {
		t.Error("expected empty key error")
	}
}

func TestMVCCGetNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	value, _, err := MVCCGet(engine, testKey1, makeTS(0, 1), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if value != nil {
		t.Fatal("the value should be empty")
	}
}

func TestMVCCPutWithTxn(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(0, 1), value1, txn1)
	if err != nil {
		t.Fatal(err)
	}

	for _, ts := range []proto.Timestamp{makeTS(0, 1), makeTS(0, 2), makeTS(1, 0)} {
		value, _, err := MVCCGet(engine, testKey1, ts, true, txn1)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value1.Bytes, value.Bytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				value1.Bytes, value.Bytes)
		}
	}
}

func TestMVCCPutWithoutTxn(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(0, 1), value1, nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, ts := range []proto.Timestamp{makeTS(0, 1), makeTS(0, 2), makeTS(1, 0)} {
		value, _, err := MVCCGet(engine, testKey1, ts, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value1.Bytes, value.Bytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				value1.Bytes, value.Bytes)
		}
	}
}

// TestMVCCPutOutOfOrder tests a scenario where a put operation of an
// older timestamp comes after a put operation of a newer timestamp.
func TestMVCCPutOutOfOrder(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(2, 1), value1, txn1)
	if err != nil {
		t.Fatal(err)
	}

	// Put operation with earlier walltime. Will NOT be ignored.
	err = MVCCPut(engine, nil, testKey1, makeTS(1, 0), value2, txn1)
	if err != nil {
		t.Fatal(err)
	}

	value, _, err := MVCCGet(engine, testKey1, makeTS(3, 0), true, txn1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value.Bytes, value2.Bytes) {
		t.Fatalf("the value should be %s, but got %s",
			value2.Bytes, value.Bytes)
	}

	// Another put operation with earlier logical time. Will NOT be ignored.
	err = MVCCPut(engine, nil, testKey1, makeTS(2, 0), value2, txn1)
	if err != nil {
		t.Fatal(err)
	}

	value, _, err = MVCCGet(engine, testKey1, makeTS(3, 0), true, txn1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value.Bytes, value2.Bytes) {
		t.Fatalf("the value should be %s, but got %s",
			value2.Bytes, value.Bytes)
	}
}

// TestMVCCIncrement verifies increment behavior. In particular,
// incrementing a non-existent key by 0 will create the value.
func TestMVCCIncrement(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	newVal, err := MVCCIncrement(engine, nil, testKey1, makeTS(0, 1), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if newVal != 0 {
		t.Errorf("expected new value of 0; got %d", newVal)
	}
	val, _, err := MVCCGet(engine, testKey1, makeTS(0, 1), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if val == nil {
		t.Errorf("expected increment of 0 to create key/value")
	}

	newVal, err = MVCCIncrement(engine, nil, testKey1, makeTS(0, 2), nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	if newVal != 2 {
		t.Errorf("expected new value of 2; got %d", newVal)
	}
}

// TestMVCCIncrementOldTimestamp tests a case where MVCCIncrement is
// called with an old timestamp. The test verifies that a value is
// read with the same timestamp as we use to write a value.
func TestMVCCIncrementOldTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	// Write a non-interger value.
	err := MVCCPut(engine, nil, testKey1, makeTS(1, 0), value1, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Override the non-integer value.
	val := proto.Value{}
	val.SetInteger(1)
	err = MVCCPut(engine, nil, testKey1, makeTS(3, 0), val, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Attempt to increment a value with an older timestamp than
	// the previous put. This will fail with type mismatch (not
	// with WriteTooOldError).
	_, err = MVCCIncrement(engine, nil, testKey1, makeTS(2, 0), nil, 1)
	if err == nil {
		t.Fatalf("unexpected success of increment")
	}
	if !strings.Contains(err.Error(), "does not contain an integer value") {
		t.Fatalf("unexpected error %s", err)
	}
}

func TestMVCCUpdateExistingKey(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(0, 1), value1, nil)
	if err != nil {
		t.Fatal(err)
	}

	value, _, err := MVCCGet(engine, testKey1, makeTS(1, 0), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value1.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.Bytes, value.Bytes)
	}

	err = MVCCPut(engine, nil, testKey1, makeTS(2, 0), value2, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Read the latest version.
	value, _, err = MVCCGet(engine, testKey1, makeTS(3, 0), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value2.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value2.Bytes, value.Bytes)
	}

	// Read the old version.
	value, _, err = MVCCGet(engine, testKey1, makeTS(1, 0), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value1.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.Bytes, value.Bytes)
	}
}

func TestMVCCUpdateExistingKeyOldVersion(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(1, 1), value1, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Earlier walltime.
	err = MVCCPut(engine, nil, testKey1, makeTS(0, 1), value2, nil)
	if err == nil {
		t.Fatal("expected error on old version")
	}
	// Earlier logical time.
	err = MVCCPut(engine, nil, testKey1, makeTS(1, 0), value2, nil)
	if err == nil {
		t.Fatal("expected error on old version")
	}
}

func TestMVCCUpdateExistingKeyInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(0, 1), value1, txn1)
	if err != nil {
		t.Fatal(err)
	}

	err = MVCCPut(engine, nil, testKey1, makeTS(1, 0), value1, txn1)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMVCCUpdateExistingKeyDiffTxn(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(0, 1), value1, txn1)
	if err != nil {
		t.Fatal(err)
	}

	err = MVCCPut(engine, nil, testKey1, makeTS(1, 0), value2, txn2)
	if err == nil {
		t.Fatal("expected error on uncommitted write intent")
	}
}

func TestMVCCGetNoMoreOldVersion(t *testing.T) {
	defer leaktest.AfterTest(t)
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

	err := MVCCPut(engine, nil, testKey1, makeTS(3, 0), value1, nil)
	err = MVCCPut(engine, nil, testKey2, makeTS(1, 0), value2, nil)

	value, _, err := MVCCGet(engine, testKey1, makeTS(2, 0), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if value != nil {
		t.Fatal("the value should be empty")
	}
}

// TestMVCCGetUncertainty verifies that the appropriate error results when
// a transaction reads a key at a timestamp that has versions newer than that
// timestamp, but older than the transaction's MaxTimestamp.
func TestMVCCGetUncertainty(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	txn := &proto.Transaction{ID: []byte("txn"), Timestamp: makeTS(5, 0), MaxTimestamp: makeTS(10, 0)}
	// Put a value from the past.
	if err := MVCCPut(engine, nil, testKey1, makeTS(1, 0), value1, nil); err != nil {
		t.Fatal(err)
	}
	// Put a value that is ahead of MaxTimestamp, it should not interfere.
	if err := MVCCPut(engine, nil, testKey1, makeTS(12, 0), value2, nil); err != nil {
		t.Fatal(err)
	}
	// Read with transaction, should get a value back.
	val, _, err := MVCCGet(engine, testKey1, makeTS(7, 0), true, txn)
	if err != nil {
		t.Fatal(err)
	}
	if val == nil || !bytes.Equal(val.Bytes, value1.Bytes) {
		t.Fatalf("wanted %q, got %v", value1.Bytes, val)
	}

	// Now using testKey2.
	// Put a value that conflicts with MaxTimestamp.
	if err := MVCCPut(engine, nil, testKey2, makeTS(9, 0), value2, nil); err != nil {
		t.Fatal(err)
	}
	// Read with transaction, should get error back.
	if _, _, err := MVCCGet(engine, testKey2, makeTS(7, 0), true, txn); err == nil {
		t.Fatal("wanted an error")
	} else if e, ok := err.(*proto.ReadWithinUncertaintyIntervalError); !ok {
		t.Fatalf("wanted a ReadWithinUncertaintyIntervalError, got %+v", e)
	}
	if _, _, err := MVCCScan(engine, testKey2, testKey2.PrefixEnd(), 10, makeTS(7, 0), true, txn); err == nil {
		t.Fatal("wanted an error")
	}
	// Adjust MaxTimestamp and retry.
	txn.MaxTimestamp = makeTS(7, 0)
	if _, _, err := MVCCGet(engine, testKey2, makeTS(7, 0), true, txn); err != nil {
		t.Fatal(err)
	}
	if _, _, err := MVCCScan(engine, testKey2, testKey2.PrefixEnd(), 10, makeTS(7, 0), true, txn); err != nil {
		t.Fatal(err)
	}

	txn.MaxTimestamp = makeTS(10, 0)
	// Now using testKey3.
	// Put a value that conflicts with MaxTimestamp and another write further
	// ahead and not conflicting any longer. The first write should still ruin
	// it.
	if err := MVCCPut(engine, nil, testKey3, makeTS(9, 0), value2, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(engine, nil, testKey3, makeTS(99, 0), value2, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := MVCCScan(engine, testKey3, testKey3.PrefixEnd(), 10, makeTS(7, 0), true, txn); err == nil {
		t.Fatal("wanted an error")
	}
	if _, _, err := MVCCGet(engine, testKey3, makeTS(7, 0), true, txn); err == nil {
		t.Fatalf("wanted an error")
	}
}

func TestMVCCGetAndDelete(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(1, 0), value1, nil)
	value, _, err := MVCCGet(engine, testKey1, makeTS(2, 0), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if value == nil {
		t.Fatal("the value should not be empty")
	}

	err = MVCCDelete(engine, nil, testKey1, makeTS(3, 0), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Read the latest version which should be deleted.
	value, _, err = MVCCGet(engine, testKey1, makeTS(4, 0), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if value != nil {
		t.Fatal("the value should be empty")
	}

	// Read the old version which should still exist.
	for _, logical := range []int32{0, math.MaxInt32} {
		value, _, err = MVCCGet(engine, testKey1, makeTS(2, logical), true, nil)
		if err != nil {
			t.Fatal(err)
		}
		if value == nil {
			t.Fatal("the value should not be empty")
		}
	}
}

func TestMVCCDeleteMissingKey(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := NewInMem(proto.Attributes{}, 1<<20)
	defer engine.Close()

	if err := MVCCDelete(engine, nil, testKey1, makeTS(1, 0), nil); err != nil {
		t.Fatal(err)
	}
	// Verify nothing is written to the engine.
	if val, err := engine.Get(MVCCEncodeKey(testKey1)); err != nil || val != nil {
		t.Fatalf("expected no mvcc metadata after delete of a missing key; got %q: %s", val, err)
	}
}

func TestMVCCGetAndDeleteInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(1, 0), value1, txn1)
	value, _, err := MVCCGet(engine, testKey1, makeTS(2, 0), true, txn1)
	if err != nil {
		t.Fatal(err)
	}
	if value == nil {
		t.Fatal("the value should not be empty")
	}

	err = MVCCDelete(engine, nil, testKey1, makeTS(3, 0), txn1)
	if err != nil {
		t.Fatal(err)
	}

	// Read the latest version which should be deleted.
	value, _, err = MVCCGet(engine, testKey1, makeTS(4, 0), true, txn1)
	if err != nil {
		t.Fatal(err)
	}
	if value != nil {
		t.Fatal("the value should be empty")
	}

	// Read the old version which shouldn't exist, as within a
	// transaction, we delete previous values.
	value, _, err = MVCCGet(engine, testKey1, makeTS(2, 0), true, nil)
	if value != nil || err != nil {
		t.Fatalf("expected value and err to be nil: %+v, %v", value, err)
	}
}

func TestMVCCGetWriteIntentError(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(0, 1), value1, txn1)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = MVCCGet(engine, testKey1, makeTS(1, 0), true, nil)
	if err == nil {
		t.Fatal("cannot read the value of a write intent without TxnID")
	}

	_, _, err = MVCCGet(engine, testKey1, makeTS(1, 0), true, txn2)
	if err == nil {
		t.Fatal("cannot read the value of a write intent from a different TxnID")
	}
}

func TestMVCCScanWriteIntentError(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	ts := []proto.Timestamp{makeTS(0, 1), makeTS(0, 2), makeTS(0, 3), makeTS(0, 4), makeTS(0, 5), makeTS(0, 6)}
	fixtureKVs := []proto.KeyValue{
		{Key: testKey1, Value: proto.Value{Bytes: []byte("testValue1 pre"), Timestamp: &ts[0]}},
		{Key: testKey4, Value: proto.Value{Bytes: []byte("testValue4 pre"), Timestamp: &ts[1]}},
		{Key: testKey1, Value: proto.Value{Bytes: []byte("testValue1"), Timestamp: &ts[2]}},
		{Key: testKey2, Value: proto.Value{Bytes: []byte("testValue2"), Timestamp: &ts[3]}},
		{Key: testKey3, Value: proto.Value{Bytes: []byte("testValue3"), Timestamp: &ts[4]}},
		{Key: testKey4, Value: proto.Value{Bytes: []byte("testValue4"), Timestamp: &ts[5]}},
	}
	for i, kv := range fixtureKVs {
		var txn *proto.Transaction
		if i == 2 {
			txn = txn1
		} else if i == 5 {
			txn = txn2
		}
		err := MVCCPut(engine, nil, kv.Key, *kv.Value.Timestamp, kv.Value, txn)
		if err != nil {
			t.Fatal(err)
		}
	}

	scanCases := []struct {
		consistent bool
		txn        *proto.Transaction
		expIntents []proto.Intent
		expValues  []proto.KeyValue
	}{
		{
			consistent: true,
			txn:        nil,
			expIntents: []proto.Intent{
				{Key: testKey1, Txn: *txn1},
				{Key: testKey4, Txn: *txn2},
			},
			// would be []proto.KeyValue{fixtureKVs[3], fixtureKVs[4]} without WriteIntentError
			expValues: nil,
		},
		{
			consistent: true,
			txn:        txn1,
			expIntents: []proto.Intent{
				{Key: testKey4, Txn: *txn2},
			},
			expValues: nil, // []proto.KeyValue{fixtureKVs[2], fixtureKVs[3], fixtureKVs[4]},
		},
		{
			consistent: true,
			txn:        txn2,
			expIntents: []proto.Intent{
				{Key: testKey1, Txn: *txn1},
			},
			expValues: nil, // []proto.KeyValue{fixtureKVs[3], fixtureKVs[4], fixtureKVs[5]},
		},
		{
			consistent: false,
			txn:        nil,
			expIntents: []proto.Intent{
				{Key: testKey1, Txn: *txn1},
				{Key: testKey4, Txn: *txn2},
			},
			expValues: []proto.KeyValue{fixtureKVs[0], fixtureKVs[3], fixtureKVs[4], fixtureKVs[1]},
		},
	}

	for i, scan := range scanCases {
		cStr := "inconsistent"
		if scan.consistent {
			cStr = "consistent"
		}
		kvs, intents, err := MVCCScan(engine, testKey1, testKey4.Next(), 0, makeTS(1, 0), scan.consistent, scan.txn)
		wiErr, _ := err.(*proto.WriteIntentError)
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
			t.Errorf("%s(%d): expected intents:\n%+v;\n got\n%+v", cStr, i, scan.expIntents, intents)
		}

		if !reflect.DeepEqual(kvs, scan.expValues) {
			t.Errorf("%s(%d): expected no values; got %+v", cStr, i, kvs)
		} else if !reflect.DeepEqual(kvs, scan.expValues) {
			t.Errorf("%s(%d): expected values %+v; got %+v", cStr, i, scan.expValues, kvs)
		}
	}
}

// TestMVCCGetInconsistent verifies the behavior of get with
// consistent set to false.
func TestMVCCGetInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	// Put two values to key 1, the latest with a txn.
	err := MVCCPut(engine, nil, testKey1, makeTS(1, 0), value1, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = MVCCPut(engine, nil, testKey1, makeTS(2, 0), value2, txn1)
	if err != nil {
		t.Fatal(err)
	}

	// A get with consistent=false should fail in a txn.
	if _, _, err := MVCCGet(engine, testKey1, makeTS(1, 0), false, txn1); err == nil {
		t.Error("expected an error getting with consistent=false in txn")
	}

	// Inconsistent get will fetch value1 for any timestamp.
	for _, ts := range []proto.Timestamp{makeTS(1, 0), makeTS(2, 0)} {
		val, intents, err := MVCCGet(engine, testKey1, ts, false, nil)
		if ts.Less(makeTS(2, 0)) {
			if err != nil {
				t.Fatal(err)
			}
		} else {
			if len(intents) == 0 || !intents[0].Key.Equal(testKey1) {
				t.Fatal(err)
			}
		}
		if !bytes.Equal(val.Bytes, value1.Bytes) {
			t.Errorf("@%s expected %q; got %q", ts, value1.Bytes, val.Bytes)
		}
	}

	// Write a single intent for key 2 and verify get returns empty.
	err = MVCCPut(engine, nil, testKey2, makeTS(2, 0), value1, txn2)
	if err != nil {
		t.Fatal(err)
	}
	val, intents, err := MVCCGet(engine, testKey2, makeTS(2, 0), false, nil)
	if len(intents) == 0 || !intents[0].Key.Equal(testKey2) {
		t.Fatal(err)
	}
	if val != nil {
		t.Errorf("expected empty val; got %+v", val)
	}
}

// TestMVCCGetProtoInconsistent verifies the behavior of GetProto with
// consistent set to false.
func TestMVCCGetProtoInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	value1, err := gogoproto.Marshal(&proto.RawKeyValue{Value: []byte("value1")})
	if err != nil {
		t.Fatal(err)
	}
	value2, err := gogoproto.Marshal(&proto.RawKeyValue{Value: []byte("value2")})
	if err != nil {
		t.Fatal(err)
	}

	// Put two values to key 1, the latest with a txn.
	if err := MVCCPut(engine, nil, testKey1, makeTS(1, 0), proto.Value{Bytes: value1}, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(engine, nil, testKey1, makeTS(2, 0), proto.Value{Bytes: value2}, txn1); err != nil {
		t.Fatal(err)
	}

	// A get with consistent=false should fail in a txn.
	val := &proto.RawKeyValue{}
	_, err = MVCCGetProto(engine, testKey1, makeTS(1, 0), false, txn1, val)
	if err == nil {
		t.Error("expected an error getting with consistent=false in txn")
	} else if _, ok := err.(*proto.WriteIntentError); ok {
		t.Error("expected non-WriteIntentError with inconsistent read in txn")
	}

	// Inconsistent get will fetch value1 for any timestamp.
	for _, ts := range []proto.Timestamp{makeTS(1, 0), makeTS(2, 0)} {
		val.Reset()
		found, err := MVCCGetProto(engine, testKey1, ts, false, nil, val)
		if ts.Less(makeTS(2, 0)) {
			if err != nil {
				t.Fatal(err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Errorf("expected to find result with inconsistent read")
		}
		if !bytes.Equal(val.Value, []byte("value1")) {
			t.Errorf("@%s expected %q; got %q", ts, []byte("value1"), val.Value)
		}
	}

	// Write a single intent for key 2 and verify get returns empty.
	if err := MVCCPut(engine, nil, testKey2, makeTS(2, 0), proto.Value{Bytes: value1}, txn2); err != nil {
		t.Fatal(err)
	}
	found, err := MVCCGetProto(engine, testKey2, makeTS(2, 0), false, nil, val)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Errorf("expected no result; got %+v", val)
	}

	// Write a malformed value (not an encoded proto.RawKeyValue) and a
	// write intent to key 3; the parse error is returned instead of the
	// write intent.
	if err := MVCCPut(engine, nil, testKey3, makeTS(1, 0), value3, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(engine, nil, testKey3, makeTS(2, 0), proto.Value{Bytes: value2}, txn1); err != nil {
		t.Fatal(err)
	}
	val.Reset()
	found, err = MVCCGetProto(engine, testKey3, makeTS(1, 0), false, nil, val)
	if err == nil {
		t.Errorf("expected error reading malformed data")
	} else if !strings.Contains(err.Error(), "unexpected EOF") {
		t.Errorf("expected EOF error, got %s", err)
	}
	if !found {
		t.Errorf("expected to find result with malformed data")
	}
}

func TestMVCCScan(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(1, 0), value1, nil)
	err = MVCCPut(engine, nil, testKey1, makeTS(2, 0), value4, nil)
	err = MVCCPut(engine, nil, testKey2, makeTS(1, 0), value2, nil)
	err = MVCCPut(engine, nil, testKey2, makeTS(3, 0), value3, nil)
	err = MVCCPut(engine, nil, testKey3, makeTS(1, 0), value3, nil)
	err = MVCCPut(engine, nil, testKey3, makeTS(4, 0), value2, nil)
	err = MVCCPut(engine, nil, testKey4, makeTS(1, 0), value4, nil)
	err = MVCCPut(engine, nil, testKey4, makeTS(5, 0), value1, nil)

	kvs, _, err := MVCCScan(engine, testKey2, testKey4, 0, makeTS(1, 0), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, testKey2) ||
		!bytes.Equal(kvs[1].Key, testKey3) ||
		!bytes.Equal(kvs[0].Value.Bytes, value2.Bytes) ||
		!bytes.Equal(kvs[1].Value.Bytes, value3.Bytes) {
		t.Fatal("the value should not be empty")
	}

	kvs, _, err = MVCCScan(engine, testKey2, testKey4, 0, makeTS(4, 0), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, testKey2) ||
		!bytes.Equal(kvs[1].Key, testKey3) ||
		!bytes.Equal(kvs[0].Value.Bytes, value3.Bytes) ||
		!bytes.Equal(kvs[1].Value.Bytes, value2.Bytes) {
		t.Fatal("the value should not be empty")
	}

	kvs, _, err = MVCCScan(engine, testKey4, proto.KeyMax, 0, makeTS(1, 0), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey4) ||
		!bytes.Equal(kvs[0].Value.Bytes, value4.Bytes) {
		t.Fatal("the value should not be empty")
	}

	_, _, err = MVCCGet(engine, testKey1, makeTS(1, 0), true, txn2)
	kvs, _, err = MVCCScan(engine, proto.KeyMin, testKey2, 0, makeTS(1, 0), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey1) ||
		!bytes.Equal(kvs[0].Value.Bytes, value1.Bytes) {
		t.Fatal("the value should not be empty")
	}
}

func TestMVCCScanMaxNum(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(1, 0), value1, nil)
	err = MVCCPut(engine, nil, testKey2, makeTS(1, 0), value2, nil)
	err = MVCCPut(engine, nil, testKey3, makeTS(1, 0), value3, nil)
	err = MVCCPut(engine, nil, testKey4, makeTS(1, 0), value4, nil)

	kvs, _, err := MVCCScan(engine, testKey2, testKey4, 1, makeTS(1, 0), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey2) ||
		!bytes.Equal(kvs[0].Value.Bytes, value2.Bytes) {
		t.Fatal("the value should not be empty")
	}
}

func TestMVCCScanWithKeyPrefix(t *testing.T) {
	defer leaktest.AfterTest(t)
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
	err := MVCCPut(engine, nil, proto.Key("/a"), makeTS(1, 0), value1, nil)
	err = MVCCPut(engine, nil, proto.Key("/a"), makeTS(2, 0), value2, nil)
	err = MVCCPut(engine, nil, proto.Key("/aa"), makeTS(2, 0), value2, nil)
	err = MVCCPut(engine, nil, proto.Key("/aa"), makeTS(3, 0), value3, nil)
	err = MVCCPut(engine, nil, proto.Key("/b"), makeTS(1, 0), value3, nil)

	kvs, _, err := MVCCScan(engine, proto.Key("/a"), proto.Key("/b"), 0, makeTS(2, 0), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, proto.Key("/a")) ||
		!bytes.Equal(kvs[1].Key, proto.Key("/aa")) ||
		!bytes.Equal(kvs[0].Value.Bytes, value2.Bytes) ||
		!bytes.Equal(kvs[1].Value.Bytes, value2.Bytes) {
		t.Fatal("the value should not be empty")
	}
}

func TestMVCCScanInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(1, 0), value1, nil)
	err = MVCCPut(engine, nil, testKey2, makeTS(1, 0), value2, nil)
	err = MVCCPut(engine, nil, testKey3, makeTS(1, 0), value3, txn1)
	err = MVCCPut(engine, nil, testKey4, makeTS(1, 0), value4, nil)

	kvs, _, err := MVCCScan(engine, testKey2, testKey4, 0, makeTS(1, 0), true, txn1)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, testKey2) ||
		!bytes.Equal(kvs[1].Key, testKey3) ||
		!bytes.Equal(kvs[0].Value.Bytes, value2.Bytes) ||
		!bytes.Equal(kvs[1].Value.Bytes, value3.Bytes) {
		t.Fatal("the value should not be empty")
	}

	kvs, _, err = MVCCScan(engine, testKey2, testKey4, 0, makeTS(1, 0), true, nil)
	if err == nil {
		t.Fatal("expected error on uncommitted write intent")
	}
}

// TestMVCCScanInconsistent writes several values, some as intents and
// verifies that the scan sees only the committed versions.
func TestMVCCScanInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	// A scan with consistent=false should fail in a txn.
	if _, _, err := MVCCScan(engine, proto.KeyMin, proto.KeyMax, 0, makeTS(1, 0), false, txn1); err == nil {
		t.Error("expected an error scanning with consistent=false in txn")
	}

	ts1 := makeTS(1, 0)
	ts2 := makeTS(2, 0)
	ts3 := makeTS(3, 0)
	ts4 := makeTS(4, 0)
	ts5 := makeTS(5, 0)
	ts6 := makeTS(6, 0)
	if err := MVCCPut(engine, nil, testKey1, ts1, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(engine, nil, testKey1, ts2, value2, txn1); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(engine, nil, testKey2, ts3, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(engine, nil, testKey2, ts4, value2, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(engine, nil, testKey3, ts5, value3, txn2); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(engine, nil, testKey4, ts6, value4, nil); err != nil {
		t.Fatal(err)
	}

	expIntents := []proto.Intent{
		{Key: testKey1, Txn: *txn1},
		{Key: testKey3, Txn: *txn2},
	}
	kvs, intents, err := MVCCScan(engine, testKey1, testKey4.Next(), 0, makeTS(7, 0), false, nil)
	if !reflect.DeepEqual(intents, expIntents) {
		t.Fatal(err)
	}

	expKVs := []proto.KeyValue{
		{Key: testKey1, Value: proto.Value{Bytes: value1.Bytes, Timestamp: &ts1}},
		{Key: testKey2, Value: proto.Value{Bytes: value2.Bytes, Timestamp: &ts4}},
		{Key: testKey4, Value: proto.Value{Bytes: value4.Bytes, Timestamp: &ts6}},
	}
	if !reflect.DeepEqual(kvs, expKVs) {
		t.Errorf("expected key values equal %v != %v", kvs, expKVs)
	}

	// Now try a scan at a historical timestamp.
	expIntents = expIntents[:1]
	kvs, intents, err = MVCCScan(engine, testKey1, testKey4.Next(), 0, makeTS(3, 0), false, nil)
	if !reflect.DeepEqual(intents, expIntents) {
		t.Fatal(err)
	}
	expKVs = []proto.KeyValue{
		{Key: testKey1, Value: proto.Value{Bytes: value1.Bytes, Timestamp: &ts1}},
		{Key: testKey2, Value: proto.Value{Bytes: value1.Bytes, Timestamp: &ts3}},
	}
	if !reflect.DeepEqual(kvs, expKVs) {
		t.Errorf("expected key values equal %v != %v", kvs, expKVs)
	}
}

func TestMVCCDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(1, 0), value1, nil)
	err = MVCCPut(engine, nil, testKey2, makeTS(1, 0), value2, nil)
	err = MVCCPut(engine, nil, testKey3, makeTS(1, 0), value3, nil)
	err = MVCCPut(engine, nil, testKey4, makeTS(1, 0), value4, nil)

	num, err := MVCCDeleteRange(engine, nil, testKey2, testKey4, 0, makeTS(2, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if num != 2 {
		t.Fatal("the value should not be empty")
	}
	kvs, _, _ := MVCCScan(engine, proto.KeyMin, proto.KeyMax, 0, makeTS(2, 0), true, nil)
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, testKey1) ||
		!bytes.Equal(kvs[1].Key, testKey4) ||
		!bytes.Equal(kvs[0].Value.Bytes, value1.Bytes) ||
		!bytes.Equal(kvs[1].Value.Bytes, value4.Bytes) {
		t.Fatal("the value should not be empty")
	}

	num, err = MVCCDeleteRange(engine, nil, testKey4, proto.KeyMax, 0, makeTS(2, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if num != 1 {
		t.Fatal("the value should not be empty")
	}
	kvs, _, _ = MVCCScan(engine, proto.KeyMin, proto.KeyMax, 0, makeTS(2, 0), true, nil)
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey1) ||
		!bytes.Equal(kvs[0].Value.Bytes, value1.Bytes) {
		t.Fatal("the value should not be empty")
	}

	num, err = MVCCDeleteRange(engine, nil, proto.KeyMin, testKey2, 0, makeTS(2, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if num != 1 {
		t.Fatal("the value should not be empty")
	}
	kvs, _, _ = MVCCScan(engine, proto.KeyMin, proto.KeyMax, 0, makeTS(2, 0), true, nil)
	if len(kvs) != 0 {
		t.Fatal("the value should be empty")
	}
}

func TestMVCCDeleteRangeFailed(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(1, 0), value1, nil)
	err = MVCCPut(engine, nil, testKey2, makeTS(1, 0), value2, txn1)
	err = MVCCPut(engine, nil, testKey3, makeTS(1, 0), value3, txn1)
	err = MVCCPut(engine, nil, testKey4, makeTS(1, 0), value4, nil)

	_, err = MVCCDeleteRange(engine, nil, testKey2, testKey4, 0, makeTS(1, 0), nil)
	if err == nil {
		t.Fatal("expected error on uncommitted write intent")
	}

	_, err = MVCCDeleteRange(engine, nil, testKey2, testKey4, 0, makeTS(1, 0), txn1)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMVCCDeleteRangeConcurrentTxn(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(1, 0), value1, nil)
	err = MVCCPut(engine, nil, testKey2, makeTS(1, 0), value2, txn1)
	err = MVCCPut(engine, nil, testKey3, makeTS(2, 0), value3, txn2)
	err = MVCCPut(engine, nil, testKey4, makeTS(1, 0), value4, nil)

	_, err = MVCCDeleteRange(engine, nil, testKey2, testKey4, 0, makeTS(1, 0), txn1)
	if err == nil {
		t.Fatal("expected error on uncommitted write intent")
	}
}

func TestMVCCConditionalPut(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	clock := hlc.NewClock(hlc.NewManualClock(0).UnixNano)

	err := MVCCConditionalPut(engine, nil, testKey1, clock.Now(), value1, &value2, nil)
	if err == nil {
		t.Fatal("expected error on key not exists")
	}
	switch e := err.(type) {
	default:
		t.Fatalf("unexpected error %T", e)
	case *proto.ConditionFailedError:
		if e.ActualValue != nil {
			t.Fatalf("expected missing actual value: %v", e.ActualValue)
		}
	}

	// Verify the difference between missing value and empty value.
	err = MVCCConditionalPut(engine, nil, testKey1, clock.Now(), value1, &valueEmpty, nil)
	if err == nil {
		t.Fatal("expected error on key not exists")
	}
	switch e := err.(type) {
	default:
		t.Fatalf("unexpected error %T", e)
	case *proto.ConditionFailedError:
		if e.ActualValue != nil {
			t.Fatalf("expected missing actual value: %v", e.ActualValue)
		}
	}

	// Do a conditional put with expectation that the value is completely missing; will succeed.
	err = MVCCConditionalPut(engine, nil, testKey1, clock.Now(), value1, nil, nil)
	if err != nil {
		t.Fatalf("expected success with condition that key doesn't yet exist: %v", err)
	}

	// Another conditional put expecting value missing will fail, now that value1 is written.
	err = MVCCConditionalPut(engine, nil, testKey1, clock.Now(), value1, nil, nil)
	if err == nil {
		t.Fatal("expected error on key already exists")
	}
	switch e := err.(type) {
	default:
		t.Fatalf("unexpected error %T", e)
	case *proto.ConditionFailedError:
		if !bytes.Equal(e.ActualValue.Bytes, value1.Bytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				e.ActualValue.Bytes, value1.Bytes)
		}
	}

	// Conditional put expecting wrong value2, will fail.
	err = MVCCConditionalPut(engine, nil, testKey1, clock.Now(), value1, &value2, nil)
	if err == nil {
		t.Fatal("expected error on key does not match")
	}
	switch e := err.(type) {
	default:
		t.Fatalf("unexpected error %T", e)
	case *proto.ConditionFailedError:
		if !bytes.Equal(e.ActualValue.Bytes, value1.Bytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				e.ActualValue.Bytes, value1.Bytes)
		}
	}

	// Move to an empty value. Will succeed.
	err = MVCCConditionalPut(engine, nil, testKey1, clock.Now(), valueEmpty, &value1, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Now move to value2 from expected empty value.
	err = MVCCConditionalPut(engine, nil, testKey1, clock.Now(), value2, &valueEmpty, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Verify we get value2 as expected.
	value, _, err := MVCCGet(engine, testKey1, clock.Now(), true, nil)
	if !bytes.Equal(value2.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.Bytes, value.Bytes)
	}
}

func TestMVCCResolveTxn(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(0, 1), value1, txn1)
	value, _, err := MVCCGet(engine, testKey1, makeTS(0, 1), true, txn1)
	if !bytes.Equal(value1.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.Bytes, value.Bytes)
	}

	// Resolve will write with txn1's timestamp which is 0,1.
	err = MVCCResolveWriteIntent(engine, nil, testKey1, makeTS(0, 1), txn1Commit)
	if err != nil {
		t.Fatal(err)
	}

	value, _, err = MVCCGet(engine, testKey1, makeTS(0, 1), true, nil)
	if !bytes.Equal(value1.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.Bytes, value.Bytes)
	}
}

// TestMVCCConditionalPutOldTimestamp tests a case where a conditional
// put with an older timestamp happens after a put with a newer timestamp.
//
// The conditional put uses the actual value at the timestamp as the
// basis for comparison first, and then may fail later with a
// WriteTooOldError if that timestamp isn't recent.
func TestMVCCConditionalPutOldTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(1, 0), value1, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = MVCCPut(engine, nil, testKey1, makeTS(3, 0), value2, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = MVCCConditionalPut(engine, nil, testKey1, makeTS(2, 0), value3, &value1, nil)
	if err == nil {
		t.Errorf("unexpected success on conditional put")
	}
	if _, ok := err.(*proto.WriteTooOldError); !ok {
		t.Errorf("unexpected error on conditional put: %s", err)
	}

	err = MVCCConditionalPut(engine, nil, testKey1, makeTS(2, 0), value3, &value2, nil)
	if err == nil {
		t.Errorf("unexpected success on conditional put")
	}
	if _, ok := err.(*proto.ConditionFailedError); !ok {
		t.Errorf("unexpected error on conditional put: %s", err)
	}
}

func TestMVCCAbortTxn(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(0, 1), value1, txn1)
	err = MVCCResolveWriteIntent(engine, nil, testKey1, makeTS(0, 1), txn1Abort)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Commit(); err != nil {
		t.Fatal(err)
	}

	value, _, err := MVCCGet(engine, testKey1, makeTS(1, 0), true, nil)
	if err != nil || value != nil {
		t.Fatalf("the value should be empty: %s", err)
	}
	meta, err := engine.Get(MVCCEncodeKey(testKey1))
	if err != nil {
		t.Fatal(err)
	}
	if len(meta) != 0 {
		t.Fatalf("expected no more MVCCMetadata")
	}
}

func TestMVCCAbortTxnWithPreviousVersion(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(0, 1), value1, nil)
	err = MVCCPut(engine, nil, testKey1, makeTS(1, 0), value2, nil)
	err = MVCCPut(engine, nil, testKey1, makeTS(2, 0), value3, txn1)
	err = MVCCResolveWriteIntent(engine, nil, testKey1, makeTS(2, 0), txn1Abort)
	if err := engine.Commit(); err != nil {
		t.Fatal(err)
	}

	meta, err := engine.Get(MVCCEncodeKey(testKey1))
	if err != nil {
		t.Fatal(err)
	}
	if len(meta) == 0 {
		t.Fatalf("expected the MVCCMetadata")
	}

	value, _, err := MVCCGet(engine, testKey1, makeTS(3, 0), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !value.Timestamp.Equal(makeTS(1, 0)) {
		t.Fatalf("expected timestamp %+v == %+v", value.Timestamp, makeTS(1, 0))
	}
	if !bytes.Equal(value2.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value.Bytes, value2.Bytes)
	}
}

func TestMVCCWriteWithDiffTimestampsAndEpochs(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	// Start with epoch 1.
	if err := MVCCPut(engine, nil, testKey1, makeTS(0, 1), value1, txn1); err != nil {
		t.Fatal(err)
	}
	// Now write with greater timestamp and epoch 2.
	if err := MVCCPut(engine, nil, testKey1, makeTS(1, 0), value2, txn1e2); err != nil {
		t.Fatal(err)
	}
	// Try a write with an earlier timestamp; this is just ignored.
	if err := MVCCPut(engine, nil, testKey1, makeTS(0, 1), value1, txn1e2); err != nil {
		t.Fatal(err)
	}
	// Try a write with an earlier epoch; again ignored.
	if err := MVCCPut(engine, nil, testKey1, makeTS(1, 0), value1, txn1); err == nil {
		t.Fatal("unexpected success of a write with an earlier epoch")
	}
	// Try a write with different value using both later timestamp and epoch.
	if err := MVCCPut(engine, nil, testKey1, makeTS(1, 0), value3, txn1e2); err != nil {
		t.Fatal(err)
	}
	// Resolve the intent.
	if err := MVCCResolveWriteIntent(engine, nil, testKey1, makeTS(1, 0), makeTxn(txn1e2Commit, makeTS(1, 0))); err != nil {
		t.Fatal(err)
	}
	// Now try writing an earlier intent--should get WriteTooOldError.
	if err := MVCCPut(engine, nil, testKey1, makeTS(0, 1), value2, txn2); err == nil {
		t.Fatal("expected write too old error")
	}
	// Ties also get WriteTooOldError.
	if err := MVCCPut(engine, nil, testKey1, makeTS(1, 0), value2, txn2); err == nil {
		t.Fatal("expected write too old error")
	}
	// Attempt to read older timestamp; should fail.
	value, _, err := MVCCGet(engine, testKey1, makeTS(0, 0), true, nil)
	if value != nil || err != nil {
		t.Errorf("expected value nil, err nil; got %+v, %v", value, err)
	}
	// Read at correct timestamp.
	value, _, err = MVCCGet(engine, testKey1, makeTS(1, 0), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !value.Timestamp.Equal(makeTS(1, 0)) {
		t.Fatalf("expected timestamp %+v == %+v", value.Timestamp, makeTS(1, 0))
	}
	if !bytes.Equal(value3.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value3.Bytes, value.Bytes)
	}
}

// TestMVCCReadWithDiffEpochs writes a value first using epoch 1, then
// reads using epoch 2 to verify that values written during different
// transaction epochs are not visible.
func TestMVCCReadWithDiffEpochs(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	// Write initial value wihtout a txn.
	if err := MVCCPut(engine, nil, testKey1, makeTS(0, 1), value1, nil); err != nil {
		t.Fatal(err)
	}
	// Now write using txn1, epoch 1.
	if err := MVCCPut(engine, nil, testKey1, makeTS(1, 0), value2, txn1); err != nil {
		t.Fatal(err)
	}
	// Try reading using different txns & epochs.
	testCases := []struct {
		txn      *proto.Transaction
		expValue *proto.Value
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
		value, _, err := MVCCGet(engine, testKey1, makeTS(2, 0), true, test.txn)
		if test.expErr {
			if err == nil {
				t.Errorf("test %d: unexpected success", i)
			} else if _, ok := err.(*proto.WriteIntentError); !ok {
				t.Errorf("test %d: expected write intent error; got %v", i, err)
			}
		} else if err != nil || value == nil || !bytes.Equal(test.expValue.Bytes, value.Bytes) {
			t.Errorf("test %d: expected value %q, err nil; got %+v, %v", i, test.expValue.Bytes, value, err)
		}
	}
}

// TestMVCCReadWithOldEpoch writes a value first using epoch 2, then
// reads using epoch 1 to verify that the read will fail.
func TestMVCCReadWithOldEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(engine, nil, testKey1, makeTS(1, 0), value2, txn1e2); err != nil {
		t.Fatal(err)
	}
	_, _, err := MVCCGet(engine, testKey1, makeTS(2, 0), true, txn1)
	if err == nil {
		t.Fatalf("unexpected success of get")
	}
}

// TestMVCCReadWithPushedTimestamp verifies that a read for a value
// written by the transaction, but then subsequently pushed, can still
// be read by the txn at the later timestamp, even if an earlier
// timestamp is specified. This happens when a txn's intents are
// resolved by other actors; the intents shouldn't become invisible
// to pushed txn.
func TestMVCCReadWithPushedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	// Start with epoch 1.
	if err := MVCCPut(engine, nil, testKey1, makeTS(0, 1), value1, txn1); err != nil {
		t.Fatal(err)
	}
	// Resolve the intent, pushing its timestamp forward.
	if err := MVCCResolveWriteIntent(engine, nil, testKey1, makeTS(0, 1), makeTxn(txn1, makeTS(1, 0))); err != nil {
		t.Fatal(err)
	}
	// Attempt to read using naive txn's previous timestamp.
	value, _, err := MVCCGet(engine, testKey1, makeTS(0, 1), true, txn1)
	if err != nil || value == nil || !bytes.Equal(value.Bytes, value1.Bytes) {
		t.Errorf("expected value %q, err nil; got %+v, %v", value1.Bytes, value, err)
	}
}

func TestMVCCResolveWithDiffEpochs(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(0, 1), value1, txn1)
	err = MVCCPut(engine, nil, testKey2, makeTS(0, 1), value2, txn1e2)
	num, err := MVCCResolveWriteIntentRange(engine, nil, testKey1, testKey2.Next(), 2, makeTS(0, 1), txn1e2Commit)
	if num != 2 {
		t.Errorf("expected 2 rows resolved; got %d", num)
	}

	// Verify key1 is empty, as resolution with epoch 2 would have
	// aborted the epoch 1 intent.
	value, _, err := MVCCGet(engine, testKey1, makeTS(0, 1), true, nil)
	if value != nil || err != nil {
		t.Errorf("expected value nil, err nil; got %+v, %v", value, err)
	}

	// Key2 should be committed.
	value, _, err = MVCCGet(engine, testKey2, makeTS(0, 1), true, nil)
	if !bytes.Equal(value2.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value2.Bytes, value.Bytes)
	}
}

func TestMVCCResolveWithUpdatedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(engine, nil, testKey1, makeTS(0, 1), value1, txn1); err != nil {
		t.Fatal(err)
	}

	value, _, err := MVCCGet(engine, testKey1, makeTS(1, 0), true, txn1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value1.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.Bytes, value.Bytes)
	}

	// Resolve with a higher commit timestamp -- this should rewrite the
	// intent when making it permanent.
	if err = MVCCResolveWriteIntent(engine, nil, testKey1, makeTS(1, 0), makeTxn(txn1Commit, makeTS(1, 0))); err != nil {
		t.Fatal(err)
	}

	if value, _, err = MVCCGet(engine, testKey1, makeTS(0, 1), true, nil); value != nil || err != nil {
		t.Fatalf("expected both value and err to be nil: %+v, %v", value, err)
	}

	value, _, err = MVCCGet(engine, testKey1, makeTS(1, 0), true, nil)
	if err != nil {
		t.Error(err)
	}
	if !value.Timestamp.Equal(makeTS(1, 0)) {
		t.Fatalf("expected timestamp %+v == %+v", value.Timestamp, makeTS(1, 0))
	}
	if !bytes.Equal(value1.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.Bytes, value.Bytes)
	}
}

func TestMVCCResolveWithPushedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	if err := MVCCPut(engine, nil, testKey1, makeTS(0, 1), value1, txn1); err != nil {
		t.Fatal(err)
	}
	value, _, err := MVCCGet(engine, testKey1, makeTS(1, 0), true, txn1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value1.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.Bytes, value.Bytes)
	}

	// Resolve with a higher commit timestamp, but with still-pending transaction.
	// This represents a straightforward push (i.e. from a read/write conflict).
	if err = MVCCResolveWriteIntent(engine, nil, testKey1, makeTS(1, 0), makeTxn(txn1, makeTS(1, 0))); err != nil {
		t.Fatal(err)
	}

	if value, _, err = MVCCGet(engine, testKey1, makeTS(1, 0), true, nil); value != nil || err == nil {
		t.Fatalf("expected both value nil and err to be a writeIntentError: %+v", value)
	}

	// Can still fetch the value using txn1.
	value, _, err = MVCCGet(engine, testKey1, makeTS(1, 0), true, txn1)
	if err != nil {
		t.Error(err)
	}
	if !value.Timestamp.Equal(makeTS(1, 0)) {
		t.Fatalf("expected timestamp %+v == %+v", value.Timestamp, makeTS(1, 0))
	}
	if !bytes.Equal(value1.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.Bytes, value.Bytes)
	}
}

func TestMVCCResolveTxnNoOps(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	// Resolve a non existent key; noop.
	err := MVCCResolveWriteIntent(engine, nil, testKey1, makeTS(0, 1), txn1Commit)
	if err != nil {
		t.Fatal(err)
	}

	// Add key and resolve despite there being no intent.
	err = MVCCPut(engine, nil, testKey1, makeTS(0, 1), value1, nil)
	err = MVCCResolveWriteIntent(engine, nil, testKey1, makeTS(0, 1), txn2Commit)
	if err != nil {
		t.Fatal(err)
	}

	// Write intent and resolve with different txn.
	err = MVCCPut(engine, nil, testKey1, makeTS(1, 0), value2, txn1)
	err = MVCCResolveWriteIntent(engine, nil, testKey1, makeTS(1, 0), txn2Commit)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMVCCResolveTxnRange(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	err := MVCCPut(engine, nil, testKey1, makeTS(0, 1), value1, txn1)
	err = MVCCPut(engine, nil, testKey2, makeTS(0, 1), value2, nil)
	err = MVCCPut(engine, nil, testKey3, makeTS(0, 1), value3, txn2)
	err = MVCCPut(engine, nil, testKey4, makeTS(0, 1), value4, txn1)

	num, err := MVCCResolveWriteIntentRange(engine, nil, testKey1, testKey4.Next(), 0, makeTS(0, 1), txn1Commit)
	if err != nil {
		t.Fatal(err)
	}
	if num != 4 {
		t.Fatalf("expected all keys to process for resolution, even though 2 are noops; got %d", num)
	}

	value, _, err := MVCCGet(engine, testKey1, makeTS(0, 1), true, nil)
	if !bytes.Equal(value1.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.Bytes, value.Bytes)
	}

	value, _, err = MVCCGet(engine, testKey2, makeTS(0, 1), true, nil)
	if !bytes.Equal(value2.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value2.Bytes, value.Bytes)
	}

	value, _, err = MVCCGet(engine, testKey3, makeTS(0, 1), true, txn2)
	if !bytes.Equal(value3.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value3.Bytes, value.Bytes)
	}

	value, _, err = MVCCGet(engine, testKey4, makeTS(0, 1), true, nil)
	if !bytes.Equal(value4.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value4.Bytes, value.Bytes)
	}
}

func TestValidSplitKeys(t *testing.T) {
	defer leaktest.AfterTest(t)
	testCases := []struct {
		key   proto.Key
		valid bool
	}{
		{proto.Key("\x00"), false},
		{proto.Key("\x00\x00"), false},
		{proto.Key("\x00\x00meta1"), false},
		{proto.Key("\x00\x00meta1\x00"), false},
		{proto.Key("\x00\x00meta1\xff"), false},
		{proto.Key("\x00\x00meta2"), true},
		{proto.Key("\x00\x00meta2\x00"), true},
		{proto.Key("\x00\x00meta2\xff"), true},
		{proto.Key("\x00\x00meta2\xff\xff"), false},
		{proto.Key("\x00\x00meta3"), true},
		{proto.Key("\x00\x01"), true},
		{proto.Key("\x00accs\xff"), true},
		{proto.Key("\x00acct"), true},
		{proto.Key("\x00acct\x00"), false},
		{proto.Key("\x00acct\xff"), false},
		{proto.Key("\x00accu"), true},
		{proto.Key("\x00perl"), true},
		{proto.Key("\x00perm"), true},
		{proto.Key("\x00perm\x00"), false},
		{proto.Key("\x00perm\xff"), false},
		{proto.Key("\x00pern"), true},
		{proto.Key("\x00zond"), true},
		{proto.Key("\x00zone"), true},
		{proto.Key("\x00zone\x00"), false},
		{proto.Key("\x00zone\xff"), false},
		{proto.Key("\x00zonf"), true},
		{proto.Key("\x01"), true},
		{proto.Key("a"), true},
		{proto.Key("\xff"), true},
	}

	for i, test := range testCases {
		if valid := IsValidSplitKey(test.key); valid != test.valid {
			t.Errorf("%d: expected %q valid %t; got %t", i, test.key, test.valid, valid)
		}
	}
}

func TestFindSplitKey(t *testing.T) {
	defer leaktest.AfterTest(t)
	raftID := proto.RangeID(1)
	engine := NewInMem(proto.Attributes{}, 1<<20)
	defer engine.Close()

	ms := &MVCCStats{}
	// Generate a series of KeyValues, each containing targetLength
	// bytes, writing key #i to (encoded) key #i through the MVCC
	// facility. Assuming that this translates roughly into same-length
	// values after MVCC encoding, the split key should hence be chosen
	// as the middle key of the interval.
	splitReservoirSize := 100
	for i := 0; i < splitReservoirSize; i++ {
		k := fmt.Sprintf("%09d", i)
		v := strings.Repeat("X", 10-len(k))
		val := proto.Value{Bytes: []byte(v)}
		// Write the key and value through MVCC
		if err := MVCCPut(engine, ms, []byte(k), makeTS(0, 1), val, nil); err != nil {
			t.Fatal(err)
		}
	}
	// write stats
	if err := MVCCSetRangeStats(engine, raftID, ms); err != nil {
		t.Fatal(err)
	}
	snap := engine.NewSnapshot()
	defer snap.Close()
	humanSplitKey, err := MVCCFindSplitKey(snap, raftID, proto.KeyMin, proto.KeyMax)
	if err != nil {
		t.Fatal(err)
	}
	ind, _ := strconv.Atoi(string(humanSplitKey))
	if diff := splitReservoirSize/2 - ind; diff > 1 || diff < -1 {
		t.Fatalf("wanted key #%d+-1, but got %d (diff %d)", ind+diff, ind, diff)
	}
}

// TestFindValidSplitKeys verifies split keys are located such that
// they avoid splits through invalid key ranges.
func TestFindValidSplitKeys(t *testing.T) {
	defer leaktest.AfterTest(t)
	raftID := proto.RangeID(1)
	testCases := []struct {
		keys     []proto.Key
		expSplit proto.Key
		expError bool
	}{
		// All meta1 cannot be split.
		{
			keys: []proto.Key{
				proto.Key("\x00\x00meta1"),
				proto.Key("\x00\x00meta1\x00"),
				proto.Key("\x00\x00meta1\xff"),
			},
			expSplit: nil,
			expError: true,
		},
		// All zone cannot be split.
		{
			keys: []proto.Key{
				proto.Key("\x00zone"),
				proto.Key("\x00zone\x00"),
				proto.Key("\x00zone\xff"),
			},
			expSplit: nil,
			expError: true,
		},
		// Between meta1 and meta2, splits at meta2.
		{
			keys: []proto.Key{
				proto.Key("\x00\x00meta1"),
				proto.Key("\x00\x00meta1\x00"),
				proto.Key("\x00\x00meta1\xff"),
				proto.Key("\x00\x00meta2"),
				proto.Key("\x00\x00meta2\x00"),
				proto.Key("\x00\x00meta2\xff"),
			},
			expSplit: proto.Key("\x00\x00meta2"),
			expError: false,
		},
		// Even lopsided, always split at meta2.
		{
			keys: []proto.Key{
				proto.Key("\x00\x00meta1"),
				proto.Key("\x00\x00meta1\x00"),
				proto.Key("\x00\x00meta1\xff"),
				proto.Key("\x00\x00meta2"),
			},
			expSplit: proto.Key("\x00\x00meta2"),
			expError: false,
		},
		// Lopsided, truncate non-zone prefix.
		{
			keys: []proto.Key{
				proto.Key("\x00zond"),
				proto.Key("\x00zone"),
				proto.Key("\x00zone\x00"),
				proto.Key("\x00zone\xff"),
			},
			expSplit: proto.Key("\x00zone"),
			expError: false,
		},
		// Lopsided, truncate non-zone suffix.
		{
			keys: []proto.Key{
				proto.Key("\x00zone"),
				proto.Key("\x00zone\x00"),
				proto.Key("\x00zone\xff"),
				proto.Key("\x00zonf"),
			},
			expSplit: proto.Key("\x00zonf"),
			expError: false,
		},
	}

	for i, test := range testCases {
		engine := NewInMem(proto.Attributes{}, 1<<20)
		defer engine.Close()

		ms := &MVCCStats{}
		val := proto.Value{Bytes: []byte(strings.Repeat("X", 10))}
		for _, k := range test.keys {
			if err := MVCCPut(engine, ms, []byte(k), makeTS(0, 1), val, nil); err != nil {
				t.Fatal(err)
			}
		}
		// write stats
		if err := MVCCSetRangeStats(engine, raftID, ms); err != nil {
			t.Fatal(err)
		}
		snap := engine.NewSnapshot()
		defer snap.Close()
		rangeStart := test.keys[0]
		rangeEnd := test.keys[len(test.keys)-1].Next()
		splitKey, err := MVCCFindSplitKey(snap, raftID, rangeStart, rangeEnd)
		if test.expError {
			if err == nil {
				t.Errorf("%d: expected error", i)
			}
			continue
		}
		if err != nil {
			t.Errorf("%d; unexpected error: %s", i, err)
			continue
		}
		if !splitKey.Equal(test.expSplit) {
			t.Errorf("%d: expected split key %q; got %q", i, test.expSplit, splitKey)
		}
	}
}

// TestFindBalancedSplitKeys verifies split keys are located such that
// the left and right halves are equally balanced.
func TestFindBalancedSplitKeys(t *testing.T) {
	defer leaktest.AfterTest(t)
	raftID := proto.RangeID(1)
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
		engine := NewInMem(proto.Attributes{}, 1<<20)
		defer engine.Close()

		ms := &MVCCStats{}
		var expKey proto.Key
		for j, keySize := range test.keySizes {
			key := proto.Key(fmt.Sprintf("%d%s", j, strings.Repeat("X", keySize)))
			if test.expSplit == j {
				expKey = key
			}
			val := proto.Value{Bytes: []byte(strings.Repeat("X", test.valSizes[j]))}
			if err := MVCCPut(engine, ms, key, makeTS(0, 1), val, nil); err != nil {
				t.Fatal(err)
			}
		}
		// write stats
		if err := MVCCSetRangeStats(engine, raftID, ms); err != nil {
			t.Fatal(err)
		}
		snap := engine.NewSnapshot()
		defer snap.Close()
		splitKey, err := MVCCFindSplitKey(snap, raftID, proto.Key("\x01"), proto.KeyMax)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
			continue
		}
		if !splitKey.Equal(expKey) {
			t.Errorf("%d: expected split key %q; got %q", i, expKey, splitKey)
		}
	}
}

// encodedSize returns the encoded size of the protobuf message.
func encodedSize(msg gogoproto.Message, t *testing.T) int64 {
	data, err := gogoproto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	return int64(len(data))
}

func verifyStats(debug string, ms *MVCCStats, expMS *MVCCStats, t *testing.T) {
	// ...And verify stats.
	if ms.LiveBytes != expMS.LiveBytes {
		t.Errorf("%s: mvcc live bytes %d; expected %d", debug, ms.LiveBytes, expMS.LiveBytes)
	}
	if ms.KeyBytes != expMS.KeyBytes {
		t.Errorf("%s: mvcc keyBytes %d; expected %d", debug, ms.KeyBytes, expMS.KeyBytes)
	}
	if ms.ValBytes != expMS.ValBytes {
		t.Errorf("%s: mvcc valBytes %d; expected %d", debug, ms.ValBytes, expMS.ValBytes)
	}
	if ms.IntentBytes != expMS.IntentBytes {
		t.Errorf("%s: mvcc intentBytes %d; expected %d", debug, ms.IntentBytes, expMS.IntentBytes)
	}
	if ms.LiveCount != expMS.LiveCount {
		t.Errorf("%s: mvcc liveCount %d; expected %d", debug, ms.LiveCount, expMS.LiveCount)
	}
	if ms.KeyCount != expMS.KeyCount {
		t.Errorf("%s: mvcc keyCount %d; expected %d", debug, ms.KeyCount, expMS.KeyCount)
	}
	if ms.ValCount != expMS.ValCount {
		t.Errorf("%s: mvcc valCount %d; expected %d", debug, ms.ValCount, expMS.ValCount)
	}
	if ms.IntentCount != expMS.IntentCount {
		t.Errorf("%s: mvcc intentCount %d; expected %d", debug, ms.IntentCount, expMS.IntentCount)
	}
	if ms.IntentAge != expMS.IntentAge {
		t.Errorf("%s: mvcc intentAge %d; expected %d", debug, ms.IntentAge, expMS.IntentAge)
	}
	if ms.GCBytesAge != expMS.GCBytesAge {
		t.Errorf("%s: mvcc gcBytesAge %d; expected %d", debug, ms.GCBytesAge, expMS.GCBytesAge)
	}
	if ms.SysBytes != expMS.SysBytes {
		t.Errorf("%s: mvcc sysBytes %d; expected %d", debug, ms.SysBytes, expMS.SysBytes)
	}
	if ms.SysCount != expMS.SysCount {
		t.Errorf("%s: mvcc sysCount %d; expected %d", debug, ms.SysCount, expMS.SysCount)
	}
}

// TestMVCCStatsBasic writes a value, then deletes it as an intent via
// a transaction, then resolves the intent, manually verifying the
// mvcc stats at each step.
func TestMVCCStatsBasic(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	ms := &MVCCStats{}

	// Verify size of mvccVersionTimestampSize.
	ts := makeTS(1*1E9, 0)
	key := proto.Key("a")
	keySize := int64(len(MVCCEncodeVersionKey(key, ts)) - len(MVCCEncodeKey(key)))
	if keySize != mvccVersionTimestampSize {
		t.Errorf("expected version timestamp size %d; got %d", mvccVersionTimestampSize, keySize)
	}

	// Put a value.
	value := proto.Value{Bytes: []byte("value")}
	if err := MVCCPut(engine, ms, key, ts, value, nil); err != nil {
		t.Fatal(err)
	}
	mKeySize := int64(len(MVCCEncodeKey(key)))
	mValSize := encodedSize(&MVCCMetadata{Timestamp: ts}, t)
	vKeySize := mvccVersionTimestampSize
	vValSize := encodedSize(&MVCCValue{Value: &value}, t)

	expMS := MVCCStats{
		LiveBytes: mKeySize + mValSize + vKeySize + vValSize,
		LiveCount: 1,
		KeyBytes:  mKeySize + vKeySize,
		KeyCount:  1,
		ValBytes:  mValSize + vValSize,
		ValCount:  1,
	}
	verifyStats("after put", ms, &expMS, t)

	// Delete the value using a transaction.
	txn := &proto.Transaction{ID: []byte("txn1"), Timestamp: makeTS(1*1E9, 0)}
	ts2 := makeTS(2*1E9, 0)
	if err := MVCCDelete(engine, ms, key, ts2, txn); err != nil {
		t.Fatal(err)
	}
	m2ValSize := encodedSize(&MVCCMetadata{Timestamp: ts2, Deleted: true, Txn: txn}, t)
	v2KeySize := mvccVersionTimestampSize
	v2ValSize := encodedSize(&MVCCValue{Deleted: true}, t)
	expMS2 := MVCCStats{
		KeyBytes:    mKeySize + vKeySize + v2KeySize,
		KeyCount:    1,
		ValBytes:    m2ValSize + vValSize + v2ValSize,
		ValCount:    2,
		IntentBytes: v2KeySize + v2ValSize,
		IntentCount: 1,
		IntentAge:   0,
		GCBytesAge:  vValSize + vKeySize, // immediately recognizes GC'able bytes from old value
	}
	verifyStats("after delete", ms, &expMS2, t)

	// Resolve the deletion by aborting it.
	txn.Status = proto.ABORTED
	if err := MVCCResolveWriteIntent(engine, ms, key, ts2, txn); err != nil {
		t.Fatal(err)
	}
	// Stats should equal same as before the deletion after aborting the intent.
	verifyStats("after abort", ms, &expMS, t)

	// Re-delete, but this time, we're going to commit it.
	txn.Status = proto.PENDING
	ts3 := makeTS(3*1E9, 0)
	if err := MVCCDelete(engine, ms, key, ts3, txn); err != nil {
		t.Fatal(err)
	}
	// GCBytesAge will be x2 seconds now.
	expMS2.GCBytesAge = (vValSize + vKeySize) * 2
	verifyStats("after 2nd delete", ms, &expMS2, t) // should be same as before.

	// Put another intent, which should age deleted intent.
	ts4 := makeTS(4*1E9, 0)
	txn.Timestamp = ts4
	key2 := proto.Key("b")
	value2 := proto.Value{Bytes: []byte("value")}
	if err := MVCCPut(engine, ms, key2, ts4, value2, txn); err != nil {
		t.Fatal(err)
	}
	mKey2Size := int64(len(MVCCEncodeKey(key2)))
	mVal2Size := encodedSize(&MVCCMetadata{Timestamp: ts4, Txn: txn}, t)
	vKey2Size := mvccVersionTimestampSize
	vVal2Size := encodedSize(&MVCCValue{Value: &value2}, t)
	expMS3 := MVCCStats{
		KeyBytes:    mKeySize + vKeySize + v2KeySize + mKey2Size + vKey2Size,
		KeyCount:    2,
		ValBytes:    m2ValSize + vValSize + v2ValSize + mVal2Size + vVal2Size,
		ValCount:    3,
		LiveBytes:   mKey2Size + vKey2Size + mVal2Size + vVal2Size,
		LiveCount:   1,
		IntentBytes: v2KeySize + v2ValSize + vKey2Size + vVal2Size,
		IntentCount: 2,
		GCBytesAge:  (vValSize + vKeySize) * 2,
	}
	verifyStats("after 2nd put", ms, &expMS3, t)

	// Now commit both values.
	txn.Status = proto.COMMITTED
	if err := MVCCResolveWriteIntent(engine, ms, key, ts4, txn); err != nil {
		t.Fatal(err)
	}
	if err := MVCCResolveWriteIntent(engine, ms, key2, ts4, txn); err != nil {
		t.Fatal(err)
	}
	m3ValSize := encodedSize(&MVCCMetadata{Timestamp: ts4, Deleted: true}, t)
	m2Val2Size := encodedSize(&MVCCMetadata{Timestamp: ts4}, t)
	expMS4 := MVCCStats{
		KeyBytes:   mKeySize + vKeySize + v2KeySize + mKey2Size + vKey2Size,
		KeyCount:   2,
		ValBytes:   m3ValSize + vValSize + v2ValSize + m2Val2Size + vVal2Size,
		ValCount:   3,
		LiveBytes:  mKey2Size + vKey2Size + m2Val2Size + vVal2Size,
		LiveCount:  1,
		IntentAge:  -1,
		GCBytesAge: (vValSize+vKeySize)*2 + (m3ValSize - m2ValSize), // subtract off difference in metas
	}
	verifyStats("after commit", ms, &expMS4, t)

	// Write over existing value to create GC'able bytes.
	ts5 := makeTS(10*1E9, 0) // skip ahead 6s
	if err := MVCCPut(engine, ms, key2, ts5, value2, nil); err != nil {
		t.Fatal(err)
	}
	expMS5 := expMS4
	expMS5.KeyBytes += vKey2Size
	expMS5.ValBytes += vVal2Size
	expMS5.ValCount = 4
	expMS5.GCBytesAge += (vKey2Size + vVal2Size) * 6 // since we skipped ahead 6s
	verifyStats("after overwrite", ms, &expMS5, t)

	// Write a transaction record which is a system-local key.
	txnKey := keys.TransactionKey(txn.Key, txn.ID)
	txnVal := proto.Value{Bytes: []byte("txn-data")}
	if err := MVCCPut(engine, ms, txnKey, proto.ZeroTimestamp, txnVal, nil); err != nil {
		t.Fatal(err)
	}
	txnKeySize := int64(len(MVCCEncodeKey(txnKey)))
	txnValSize := encodedSize(&MVCCMetadata{Value: &txnVal}, t)
	expMS6 := expMS5
	expMS6.SysBytes += txnKeySize + txnValSize
	expMS6.SysCount++
	verifyStats("after sys-local key", ms, &expMS6, t)
}

// TestMVCCStatsWithRandomRuns creates a random sequence of puts,
// deletes and delete ranges and at each step verifies that the mvcc
// stats match a manual computation of range stats via a scan of the
// underlying engine.
func TestMVCCStatsWithRandomRuns(t *testing.T) {
	defer leaktest.AfterTest(t)
	rng, seed := randutil.NewPseudoRand()
	log.Infof("using pseudo random number generator with seed %d", seed)
	engine := createTestEngine()
	defer engine.Close()

	ms := &MVCCStats{}

	// Now, generate a random sequence of puts, deletes and resolves.
	// Each put and delete may or may not involve a txn. Resolves may
	// either commit or abort.
	keys := map[int32][]byte{}
	for i := int32(0); i < int32(1000); i++ {
		// Manually advance aggregate intent age based on one extra second of simulation.
		ms.IntentAge += ms.IntentCount
		// Same for aggregate gc'able bytes age.
		ms.GCBytesAge += ms.KeyBytes + ms.ValBytes - ms.LiveBytes

		key := []byte(fmt.Sprintf("%s-%d", randutil.RandBytes(rng, int(rng.Int31n(32))), i))
		keys[i] = key
		var txn *proto.Transaction
		if rng.Int31n(2) == 0 { // create a txn with 50% prob
			txn = &proto.Transaction{ID: []byte(fmt.Sprintf("txn-%d", i)), Timestamp: makeTS(int64(i+1)*1E9, 0)}
		}
		// With 25% probability, put a new value; otherwise, delete an earlier
		// key. Because an earlier step in this process may have itself been
		// a delete, we could end up deleting a non-existent key, which is good;
		// we don't mind testing that case as well.
		isDelete := rng.Int31n(4) == 0
		if i > 0 && isDelete {
			idx := rng.Int31n(i)
			if log.V(1) {
				log.Infof("*** DELETE index %d", idx)
			}
			if err := MVCCDelete(engine, ms, keys[idx], makeTS(int64(i+1)*1E9, 0), txn); err != nil {
				// Abort any write intent on an earlier, unresolved txn.
				if wiErr, ok := err.(*proto.WriteIntentError); ok {
					wiErr.Intents[0].Txn.Status = proto.ABORTED
					if log.V(1) {
						log.Infof("*** ABORT index %d", idx)
					}
					if err := MVCCResolveWriteIntent(engine, ms, keys[idx], makeTS(int64(i+1)*1E9, 0), &wiErr.Intents[0].Txn); err != nil {
						t.Fatal(err)
					}
					// Now, re-delete.
					if log.V(1) {
						log.Infof("*** RE-DELETE index %d", idx)
					}
					if err := MVCCDelete(engine, ms, keys[idx], makeTS(int64(i+1)*1E9, 0), txn); err != nil {
						t.Fatal(err)
					}
				} else {
					t.Fatal(err)
				}
			}
		} else {
			rngVal := proto.Value{Bytes: randutil.RandBytes(rng, int(rng.Int31n(128)))}
			if log.V(1) {
				log.Infof("*** PUT index %d; TXN=%t", i, txn != nil)
			}
			if err := MVCCPut(engine, ms, key, makeTS(int64(i+1)*1E9, 0), rngVal, txn); err != nil {
				t.Fatal(err)
			}
		}
		if !isDelete && txn != nil && rng.Int31n(2) == 0 { // resolve txn with 50% prob
			txn.Status = proto.COMMITTED
			if rng.Int31n(10) == 0 { // abort txn with 10% prob
				txn.Status = proto.ABORTED
			}
			if log.V(1) {
				log.Infof("*** RESOLVE index %d; COMMIT=%t", i, txn.Status == proto.COMMITTED)
			}
			if err := MVCCResolveWriteIntent(engine, ms, key, makeTS(int64(i+1)*1E9, 0), txn); err != nil {
				t.Fatal(err)
			}
		}

		// Every 10th step, verify the stats via manual engine scan.
		if i%10 == 0 {
			// Compute the stats manually.
			iter := engine.NewIterator()
			iter.Seek(proto.KeyMin)
			expMS, err := MVCCComputeStats(iter, int64(i+1)*1E9)
			iter.Close()
			if err != nil {
				t.Fatal(err)
			}
			verifyStats(fmt.Sprintf("cycle %d", i), ms, &expMS, t)
		}
	}
}

// TestMVCCGarbageCollect writes a series of gc'able bytes and then
// sends an MVCC GC request and verifies cleared values and updated
// stats.
func TestMVCCGarbageCollect(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	ms := &MVCCStats{}

	bytes := []byte("value")
	ts1 := makeTS(1E9, 0)
	ts2 := makeTS(2E9, 0)
	ts3 := makeTS(3E9, 0)
	val1 := proto.Value{Bytes: bytes, Timestamp: &ts1}
	val2 := proto.Value{Bytes: bytes, Timestamp: &ts2}
	val3 := proto.Value{Bytes: bytes, Timestamp: &ts3}

	testData := []struct {
		key       proto.Key
		vals      []proto.Value
		isDeleted bool // is the most recent value a deletion tombstone?
	}{
		{proto.Key("a"), []proto.Value{val1, val2}, false},
		{proto.Key("a-del"), []proto.Value{val1, val2}, true},
		{proto.Key("b"), []proto.Value{val1, val2, val3}, false},
		{proto.Key("b-del"), []proto.Value{val1, val2, val3}, true},
	}

	for i := 0; i < 3; i++ {
		// Manually advance aggregate gc'able bytes age based on one extra second of simulation.
		ms.GCBytesAge += ms.KeyBytes + ms.ValBytes - ms.LiveBytes

		for _, test := range testData {
			if i >= len(test.vals) {
				continue
			}
			for _, val := range test.vals[i : i+1] {
				if i == len(test.vals)-1 && test.isDeleted {
					if err := MVCCDelete(engine, ms, test.key, *val.Timestamp, nil); err != nil {
						t.Fatal(err)
					}
					continue
				}
				if err := MVCCPut(engine, ms, test.key, *val.Timestamp, val, nil); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	kvsn, err := Scan(engine, MVCCEncodeKey(proto.KeyMin), MVCCEncodeKey(proto.KeyMax), 0)
	if err != nil {
		t.Fatal(err)
	}
	for i, kv := range kvsn {
		key, ts, _ := MVCCDecodeKey(kv.Key)
		if log.V(1) {
			log.Infof("%d: %q, ts=%s", i, key, ts)
		}
	}

	keys := []proto.InternalGCRequest_GCKey{
		{Key: proto.Key("a"), Timestamp: ts1},
		{Key: proto.Key("a-del"), Timestamp: ts2},
		{Key: proto.Key("b"), Timestamp: ts1},
		{Key: proto.Key("b-del"), Timestamp: ts2},
	}
	if err := MVCCGarbageCollect(engine, ms, keys, ts3); err != nil {
		t.Fatal(err)
	}

	expEncKeys := []proto.EncodedKey{
		MVCCEncodeKey(proto.Key("a")),
		MVCCEncodeVersionKey(proto.Key("a"), ts2),
		MVCCEncodeKey(proto.Key("b")),
		MVCCEncodeVersionKey(proto.Key("b"), ts3),
		MVCCEncodeVersionKey(proto.Key("b"), ts2),
		MVCCEncodeKey(proto.Key("b-del")),
		MVCCEncodeVersionKey(proto.Key("b-del"), ts3),
	}
	kvs, err := Scan(engine, MVCCEncodeKey(proto.KeyMin), MVCCEncodeKey(proto.KeyMax), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != len(expEncKeys) {
		t.Fatalf("number of kvs %d != expected %d", len(kvs), len(expEncKeys))
	}
	for i, kv := range kvs {
		key, ts, _ := MVCCDecodeKey(kv.Key)
		log.Infof("%d: %q, ts=%s", i, key, ts)
		if !kv.Key.Equal(expEncKeys[i]) {
			t.Errorf("%d: expected key %q; got %q", i, expEncKeys[i], kv.Key)
		}
	}

	// Verify aggregated stats match computed stats after GC.
	iter := engine.NewIterator()
	iter.Seek(proto.KeyMin)
	expMS, err := MVCCComputeStats(iter, ts3.WallTime)
	iter.Close()
	if err != nil {
		t.Fatal(err)
	}
	verifyStats("verification", ms, &expMS, t)
}

// TestMVCCGarbageCollectNonDeleted verifies that the first value for
// a key cannot be GC'd if it's not deleted.
func TestMVCCGarbageCollectNonDeleted(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	bytes := []byte("value")
	ts1 := makeTS(1E9, 0)
	ts2 := makeTS(2E9, 0)
	val1 := proto.Value{Bytes: bytes, Timestamp: &ts1}
	val2 := proto.Value{Bytes: bytes, Timestamp: &ts2}
	key := proto.Key("a")
	vals := []proto.Value{val1, val2}
	for _, val := range vals {
		if err := MVCCPut(engine, nil, key, *val.Timestamp, val, nil); err != nil {
			t.Fatal(err)
		}
	}
	keys := []proto.InternalGCRequest_GCKey{
		{Key: proto.Key("a"), Timestamp: ts2},
	}
	if err := MVCCGarbageCollect(engine, nil, keys, ts2); err == nil {
		t.Fatal("expected error garbage collecting a non-deleted live value")
	}
}

// TestMVCCGarbageCollectIntent verifies that an intent cannot be GC'd.
func TestMVCCGarbageCollectIntent(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	bytes := []byte("value")
	ts1 := makeTS(1E9, 0)
	ts2 := makeTS(2E9, 0)
	val1 := proto.Value{Bytes: bytes, Timestamp: &ts1}
	key := proto.Key("a")
	if err := MVCCPut(engine, nil, key, ts1, val1, nil); err != nil {
		t.Fatal(err)
	}
	txn := &proto.Transaction{ID: []byte("txn"), Timestamp: ts2}
	if err := MVCCDelete(engine, nil, key, ts2, txn); err != nil {
		t.Fatal(err)
	}
	keys := []proto.InternalGCRequest_GCKey{
		{Key: proto.Key("a"), Timestamp: ts2},
	}
	if err := MVCCGarbageCollect(engine, nil, keys, ts2); err == nil {
		t.Fatal("expected error garbage collecting an intent")
	}
}

// TestResovleIntentWithLowerEpoch verifies that trying to resolve
// an intent at an epoch that is lower than the epoch of the intent
// leaves the intent untouched.
func TestResovleIntentWithLowerEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine := createTestEngine()
	defer engine.Close()

	// Lay down an intent with a high epoch.
	if err := MVCCPut(engine, nil, testKey1, makeTS(0, 1), value1, txn1e2); err != nil {
		t.Fatal(err)
	}
	// Resolve the intent with a low epoch.
	if err := MVCCResolveWriteIntent(engine, nil, testKey1, makeTS(0, 1), txn1); err != nil {
		t.Fatal(err)
	}

	// Check that the intent was not cleared.
	metaKey := MVCCEncodeKey(testKey1)
	meta := &MVCCMetadata{}
	ok, _, _, err := engine.GetProto(metaKey, meta)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("intent should not be cleared by resolve intent request with lower epoch")
	}
}

// BenchmarkMVCCStats set MVCCStats values.
func BenchmarkMVCCStats(b *testing.B) {
	rocksdb := NewInMem(proto.Attributes{Attrs: []string{"ssd"}}, testCacheSize)
	defer rocksdb.Close()

	ms := MVCCStats{
		LiveBytes:       1,
		KeyBytes:        1,
		ValBytes:        1,
		IntentBytes:     1,
		LiveCount:       1,
		KeyCount:        1,
		ValCount:        1,
		IntentCount:     1,
		IntentAge:       1,
		GCBytesAge:      1,
		SysBytes:        1,
		SysCount:        1,
		LastUpdateNanos: 1,
	}
	b.SetBytes(int64(unsafe.Sizeof(ms)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := MVCCSetRangeStats(rocksdb, 1, &ms); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
}
