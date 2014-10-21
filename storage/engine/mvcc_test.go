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

package engine

import (
	"bytes"
	crypto_rand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// Constants for system-reserved keys in the KV map.
var (
	testKey1     = proto.Key("/db1")
	testKey2     = proto.Key("/db2")
	testKey3     = proto.Key("/db3")
	testKey4     = proto.Key("/db4")
	txn1         = &proto.Transaction{ID: []byte("Txn1"), Epoch: 1}
	txn1Commit   = &proto.Transaction{ID: []byte("Txn1"), Epoch: 1, Status: proto.COMMITTED}
	txn1Abort    = &proto.Transaction{ID: []byte("Txn1"), Epoch: 1, Status: proto.ABORTED}
	txn1e2       = &proto.Transaction{ID: []byte("Txn1"), Epoch: 2}
	txn1e2Commit = &proto.Transaction{ID: []byte("Txn1"), Epoch: 2, Status: proto.COMMITTED}
	txn2         = &proto.Transaction{ID: []byte("Txn2")}
	txn2Commit   = &proto.Transaction{ID: []byte("Txn2"), Status: proto.COMMITTED}
	value1       = proto.Value{Bytes: []byte("testValue1")}
	value2       = proto.Value{Bytes: []byte("testValue2")}
	value3       = proto.Value{Bytes: []byte("testValue3")}
	value4       = proto.Value{Bytes: []byte("testValue4")}
	valueEmpty   = proto.Value{}
)

// createTestMVCC returns a new MVCC object wrapping a Batch, wrapping
// an in-memory engine with 1MB of storage capacity. Also returns the
// underlying engine for direct access.
func createTestMVCC() (*MVCC, Engine) {
	engine := NewInMem(proto.Attributes{}, 1<<20)
	return NewMVCC(engine.NewBatch()), engine
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

func TestMVCCGetNotExist(t *testing.T) {
	mvcc, _ := createTestMVCC()
	value, err := mvcc.Get(testKey1, makeTS(0, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if value != nil {
		t.Fatal("the value should be empty")
	}
}

func TestMVCCPutWithBadValue(t *testing.T) {
	mvcc, _ := createTestMVCC()
	badValue := proto.Value{Bytes: []byte("a"), Integer: gogoproto.Int64(1)}
	err := mvcc.Put(testKey1, makeTS(0, 0), badValue, nil)
	if err == nil {
		t.Fatal("expected an error putting a value with both byte slice and integer components")
	}
}

func TestMVCCPutWithTxn(t *testing.T) {
	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(0, 0), value1, txn1)
	if err != nil {
		t.Fatal(err)
	}

	for _, ts := range []proto.Timestamp{makeTS(0, 0), makeTS(0, 1), makeTS(1, 0)} {
		value, err := mvcc.Get(testKey1, ts, txn1)
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
	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(0, 0), value1, nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, ts := range []proto.Timestamp{makeTS(0, 0), makeTS(0, 1), makeTS(1, 0)} {
		value, err := mvcc.Get(testKey1, ts, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value1.Bytes, value.Bytes) {
			t.Fatalf("the value %s in get result does not match the value %s in request",
				value1.Bytes, value.Bytes)
		}
	}
}

func TestMVCCUpdateExistingKey(t *testing.T) {
	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(0, 0), value1, nil)
	if err != nil {
		t.Fatal(err)
	}

	value, err := mvcc.Get(testKey1, makeTS(1, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value1.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.Bytes, value.Bytes)
	}

	err = mvcc.Put(testKey1, makeTS(2, 0), value2, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Read the latest version.
	value, err = mvcc.Get(testKey1, makeTS(3, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value2.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value2.Bytes, value.Bytes)
	}

	// Read the old version.
	value, err = mvcc.Get(testKey1, makeTS(1, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value1.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.Bytes, value.Bytes)
	}
}

func TestMVCCUpdateExistingKeyOldVersion(t *testing.T) {
	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(1, 1), value1, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Earlier walltime.
	err = mvcc.Put(testKey1, makeTS(0, 0), value2, nil)
	if err == nil {
		t.Fatal("expected error on old version")
	}
	// Earlier logical time.
	err = mvcc.Put(testKey1, makeTS(1, 0), value2, nil)
	if err == nil {
		t.Fatal("expected error on old version")
	}
}

func TestMVCCUpdateExistingKeyInTxn(t *testing.T) {
	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(0, 0), value1, txn1)
	if err != nil {
		t.Fatal(err)
	}

	err = mvcc.Put(testKey1, makeTS(1, 0), value1, txn1)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMVCCUpdateExistingKeyDiffTxn(t *testing.T) {
	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(0, 0), value1, txn1)
	if err != nil {
		t.Fatal(err)
	}

	err = mvcc.Put(testKey1, makeTS(1, 0), value2, txn2)
	if err == nil {
		t.Fatal("expected error on uncommitted write intent")
	}
}

func TestMVCCGetNoMoreOldVersion(t *testing.T) {
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

	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(3, 0), value1, nil)
	err = mvcc.Put(testKey2, makeTS(1, 0), value2, nil)

	value, err := mvcc.Get(testKey1, makeTS(2, 0), nil)
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
// TODO(Tobias): Test this in a live transactions as well, verifying the
// necessary transaction restarts happen correctly etc.
// Spencer's suggestion:
// Create 3 clocks, each with max drift (offset) set to 100ms. Set clock one to
// time=t, clock two to time=t+50ms, clock three to time=t+100ms. Write three
// values at current time according to each of the three clocks. Start three
// txns, each using one of the three clocks. In each txn, read the three values
// and ensure that all txns read the correct values.
func TestMVCCGetUncertainty(t *testing.T) {
	mvcc, _ := createTestMVCC()
	txn := &proto.Transaction{ID: []byte("txn"), Timestamp: makeTS(5, 0), MaxTimestamp: makeTS(10, 0)}
	// Put a value from the past.
	if err := mvcc.Put(testKey1, makeTS(1, 0), value1, nil); err != nil {
		t.Fatal(err)
	}
	// Put a value that is ahead of MaxTimestamp, it should not interfere.
	if err := mvcc.Put(testKey1, makeTS(12, 0), value2, nil); err != nil {
		t.Fatal(err)
	}
	// Read with transaction, should get a value back.
	val, err := mvcc.Get(testKey1, makeTS(7, 0), txn)
	if err != nil {
		t.Fatal(err)
	}
	if val == nil || !bytes.Equal(val.Bytes, value1.Bytes) {
		t.Fatalf("wanted %q, got %v", value1.Bytes, val)
	}

	// Now using testKey2.
	// Put a value that conflicts with MaxTimestamp.
	if err := mvcc.Put(testKey2, makeTS(9, 0), value2, nil); err != nil {
		t.Fatal(err)
	}
	// Read with transaction, should get error back.
	if _, err := mvcc.Get(testKey2, makeTS(7, 0), txn); err == nil {
		t.Fatal("wanted an error")
	} else if e, ok := err.(*proto.ReadWithinUncertaintyIntervalError); !ok {
		t.Fatalf("wanted a ReadWithinUncertaintyIntervalError, got %+v", e)
	}
	if _, err := mvcc.Scan(testKey2, testKey2.PrefixEnd(), 10, makeTS(7, 0), txn); err == nil {
		t.Fatal("wanted an error")
	}
	// Adjust MaxTimestamp and retry.
	txn.MaxTimestamp = makeTS(7, 0)
	if _, err := mvcc.Get(testKey2, makeTS(7, 0), txn); err != nil {
		t.Fatal(err)
	}
	if _, err := mvcc.Scan(testKey2, testKey2.PrefixEnd(), 10, makeTS(7, 0), txn); err != nil {
		t.Fatal(err)
	}

	txn.MaxTimestamp = makeTS(10, 0)
	// Now using testKey3.
	// Put a value that conflicts with MaxTimestamp and another write further
	// ahead and not conflicting any longer. The first write should still ruin
	// it.
	if err := mvcc.Put(testKey3, makeTS(9, 0), value2, nil); err != nil {
		t.Fatal(err)
	}
	if err := mvcc.Put(testKey3, makeTS(99, 0), value2, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := mvcc.Scan(testKey3, testKey3.PrefixEnd(), 10, makeTS(7, 0), txn); err == nil {
		t.Fatal("wanted an error")
	}
	if _, err := mvcc.Get(testKey3, makeTS(7, 0), txn); err == nil {
		t.Fatalf("wanted an error")
	}
}

func TestMVCCGetAndDelete(t *testing.T) {
	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(1, 0), value1, nil)
	value, err := mvcc.Get(testKey1, makeTS(2, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if value == nil {
		t.Fatal("the value should not be empty")
	}

	err = mvcc.Delete(testKey1, makeTS(3, 0), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Read the latest version which should be deleted.
	value, err = mvcc.Get(testKey1, makeTS(4, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if value != nil {
		t.Fatal("the value should be empty")
	}

	// Read the old version which should still exist.
	for _, logical := range []int32{0, math.MaxInt32} {
		value, err = mvcc.Get(testKey1, makeTS(2, logical), nil)
		if err != nil {
			t.Fatal(err)
		}
		if value == nil {
			t.Fatal("the value should not be empty")
		}
	}
}

func TestMVCCDeleteMissingKey(t *testing.T) {
	engine := NewInMem(proto.Attributes{}, 1<<20)
	mvcc := NewMVCC(engine)
	if err := mvcc.Delete(testKey1, makeTS(1, 0), nil); err != nil {
		t.Fatal(err)
	}
	// Verify nothing is written to the engine.
	if val, err := engine.Get(MVCCEncodeKey(testKey1)); err != nil || val != nil {
		t.Fatalf("expected no mvcc metadata after delete of a missing key; got %q: %s", val, err)
	}
}

func TestMVCCGetAndDeleteInTxn(t *testing.T) {
	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(1, 0), value1, txn1)
	value, err := mvcc.Get(testKey1, makeTS(2, 0), txn1)
	if err != nil {
		t.Fatal(err)
	}
	if value == nil {
		t.Fatal("the value should not be empty")
	}

	err = mvcc.Delete(testKey1, makeTS(3, 0), txn1)
	if err != nil {
		t.Fatal(err)
	}

	// Read the latest version which should be deleted.
	value, err = mvcc.Get(testKey1, makeTS(4, 0), txn1)
	if err != nil {
		t.Fatal(err)
	}
	if value != nil {
		t.Fatal("the value should be empty")
	}

	// Read the old version which shouldn't exist, as within a
	// transaction, we delete previous values.
	value, err = mvcc.Get(testKey1, makeTS(2, 0), nil)
	if value != nil || err != nil {
		t.Fatalf("expected value and err to be nil: %+v, %v", value, err)
	}
}

func TestMVCCGetWriteIntentError(t *testing.T) {
	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(0, 0), value1, txn1)
	if err != nil {
		t.Fatal(err)
	}

	_, err = mvcc.Get(testKey1, makeTS(1, 0), nil)
	if err == nil {
		t.Fatal("cannot read the value of a write intent without TxnID")
	}

	_, err = mvcc.Get(testKey1, makeTS(1, 0), txn2)
	if err == nil {
		t.Fatal("cannot read the value of a write intent from a different TxnID")
	}
}

func TestMVCCScan(t *testing.T) {
	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(1, 0), value1, nil)
	err = mvcc.Put(testKey1, makeTS(2, 0), value4, nil)
	err = mvcc.Put(testKey2, makeTS(1, 0), value2, nil)
	err = mvcc.Put(testKey2, makeTS(3, 0), value3, nil)
	err = mvcc.Put(testKey3, makeTS(1, 0), value3, nil)
	err = mvcc.Put(testKey3, makeTS(4, 0), value2, nil)
	err = mvcc.Put(testKey4, makeTS(1, 0), value4, nil)
	err = mvcc.Put(testKey4, makeTS(5, 0), value1, nil)

	kvs, err := mvcc.Scan(testKey2, testKey4, 0, makeTS(1, 0), nil)
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

	kvs, err = mvcc.Scan(testKey2, testKey4, 0, makeTS(4, 0), nil)
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

	kvs, err = mvcc.Scan(testKey4, KeyMax, 0, makeTS(1, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey4) ||
		!bytes.Equal(kvs[0].Value.Bytes, value4.Bytes) {
		t.Fatal("the value should not be empty")
	}

	_, err = mvcc.Get(testKey1, makeTS(1, 0), txn2)
	kvs, err = mvcc.Scan(KeyMin, testKey2, 0, makeTS(1, 0), nil)
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
	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(1, 0), value1, nil)
	err = mvcc.Put(testKey2, makeTS(1, 0), value2, nil)
	err = mvcc.Put(testKey3, makeTS(1, 0), value3, nil)
	err = mvcc.Put(testKey4, makeTS(1, 0), value4, nil)

	kvs, err := mvcc.Scan(testKey2, testKey4, 1, makeTS(1, 0), nil)
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
	mvcc, _ := createTestMVCC()
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
	err := mvcc.Put(proto.Key("/a"), makeTS(1, 0), value1, nil)
	err = mvcc.Put(proto.Key("/a"), makeTS(2, 0), value2, nil)
	err = mvcc.Put(proto.Key("/aa"), makeTS(2, 0), value2, nil)
	err = mvcc.Put(proto.Key("/aa"), makeTS(3, 0), value3, nil)
	err = mvcc.Put(proto.Key("/b"), makeTS(1, 0), value3, nil)

	kvs, err := mvcc.Scan(proto.Key("/a"), proto.Key("/b"), 0, makeTS(2, 0), nil)
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
	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(1, 0), value1, nil)
	err = mvcc.Put(testKey2, makeTS(1, 0), value2, nil)
	err = mvcc.Put(testKey3, makeTS(1, 0), value3, txn1)
	err = mvcc.Put(testKey4, makeTS(1, 0), value4, nil)

	kvs, err := mvcc.Scan(testKey2, testKey4, 0, makeTS(1, 0), txn1)
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

	kvs, err = mvcc.Scan(testKey2, testKey4, 0, makeTS(1, 0), nil)
	if err == nil {
		t.Fatal("expected error on uncommitted write intent")
	}
}

// TestMVCCIterateCommitted writes several values, some as intents
// and verifies that IterateCommitted sees only the committed versions.
func TestMVCCIterateCommitted(t *testing.T) {
	mvcc, _ := createTestMVCC()
	ts1 := makeTS(1, 0)
	ts2 := makeTS(2, 0)
	ts3 := makeTS(3, 0)
	ts4 := makeTS(4, 0)
	ts5 := makeTS(5, 0)
	ts6 := makeTS(6, 0)
	if err := mvcc.Put(testKey1, ts1, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := mvcc.Put(testKey1, ts2, value2, txn1); err != nil {
		t.Fatal(err)
	}
	if err := mvcc.Put(testKey2, ts3, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := mvcc.Put(testKey2, ts4, value2, nil); err != nil {
		t.Fatal(err)
	}
	if err := mvcc.Put(testKey3, ts5, value3, txn2); err != nil {
		t.Fatal(err)
	}
	if err := mvcc.Put(testKey4, ts6, value4, nil); err != nil {
		t.Fatal(err)
	}

	kvs := []proto.KeyValue(nil)
	if err := mvcc.IterateCommitted(testKey1, testKey4.Next(), func(kv proto.KeyValue) (bool, error) {
		kvs = append(kvs, kv)
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}

	expKVs := []proto.KeyValue{
		proto.KeyValue{Key: testKey1, Value: proto.Value{Bytes: value1.Bytes, Timestamp: &ts1}},
		proto.KeyValue{Key: testKey2, Value: proto.Value{Bytes: value2.Bytes, Timestamp: &ts4}},
		proto.KeyValue{Key: testKey4, Value: proto.Value{Bytes: value4.Bytes, Timestamp: &ts6}},
	}
	if !reflect.DeepEqual(kvs, expKVs) {
		t.Errorf("expected key values equal %v != %v", kvs, expKVs)
	}
}

func TestMVCCDeleteRange(t *testing.T) {
	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(1, 0), value1, nil)
	err = mvcc.Put(testKey2, makeTS(1, 0), value2, nil)
	err = mvcc.Put(testKey3, makeTS(1, 0), value3, nil)
	err = mvcc.Put(testKey4, makeTS(1, 0), value4, nil)

	num, err := mvcc.DeleteRange(testKey2, testKey4, 0, makeTS(2, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if num != 2 {
		t.Fatal("the value should not be empty")
	}
	kvs, _ := mvcc.Scan(KeyMin, KeyMax, 0, makeTS(2, 0), nil)
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, testKey1) ||
		!bytes.Equal(kvs[1].Key, testKey4) ||
		!bytes.Equal(kvs[0].Value.Bytes, value1.Bytes) ||
		!bytes.Equal(kvs[1].Value.Bytes, value4.Bytes) {
		t.Fatal("the value should not be empty")
	}

	num, err = mvcc.DeleteRange(testKey4, KeyMax, 0, makeTS(2, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if num != 1 {
		t.Fatal("the value should not be empty")
	}
	kvs, _ = mvcc.Scan(KeyMin, KeyMax, 0, makeTS(2, 0), nil)
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey1) ||
		!bytes.Equal(kvs[0].Value.Bytes, value1.Bytes) {
		t.Fatal("the value should not be empty")
	}

	num, err = mvcc.DeleteRange(KeyMin, testKey2, 0, makeTS(2, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if num != 1 {
		t.Fatal("the value should not be empty")
	}
	kvs, _ = mvcc.Scan(KeyMin, KeyMax, 0, makeTS(2, 0), nil)
	if len(kvs) != 0 {
		t.Fatal("the value should be empty")
	}
}

func TestMVCCDeleteRangeFailed(t *testing.T) {
	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(1, 0), value1, nil)
	err = mvcc.Put(testKey2, makeTS(1, 0), value2, txn1)
	err = mvcc.Put(testKey3, makeTS(1, 0), value3, txn1)
	err = mvcc.Put(testKey4, makeTS(1, 0), value4, nil)

	_, err = mvcc.DeleteRange(testKey2, testKey4, 0, makeTS(1, 0), nil)
	if err == nil {
		t.Fatal("expected error on uncommitted write intent")
	}

	_, err = mvcc.DeleteRange(testKey2, testKey4, 0, makeTS(1, 0), txn1)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMVCCDeleteRangeConcurrentTxn(t *testing.T) {
	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(1, 0), value1, nil)
	err = mvcc.Put(testKey2, makeTS(1, 0), value2, txn1)
	err = mvcc.Put(testKey3, makeTS(2, 0), value3, txn2)
	err = mvcc.Put(testKey4, makeTS(1, 0), value4, nil)

	_, err = mvcc.DeleteRange(testKey2, testKey4, 0, makeTS(1, 0), txn1)
	if err == nil {
		t.Fatal("expected error on uncommitted write intent")
	}
}

func TestMVCCConditionalPut(t *testing.T) {
	mvcc, _ := createTestMVCC()
	actualVal, err := mvcc.ConditionalPut(testKey1, makeTS(0, 0), value1, &value2, nil)
	if err == nil {
		t.Fatal("expected error on key not exists")
	}
	if actualVal != nil {
		t.Fatalf("expected missing actual value: %v", actualVal)
	}

	// Verify the difference between missing value and empty value.
	actualVal, err = mvcc.ConditionalPut(testKey1, makeTS(0, 0), value1, &valueEmpty, nil)
	if err == nil {
		t.Fatal("expected error on key not exists")
	}
	if actualVal != nil {
		t.Fatalf("expected missing actual value: %v", actualVal)
	}

	// Do a conditional put with expectation that the value is completely missing; will succeed.
	_, err = mvcc.ConditionalPut(testKey1, makeTS(0, 0), value1, nil, nil)
	if err != nil {
		t.Fatalf("expected success with condition that key doesn't yet exist: %v", err)
	}

	// Another conditional put expecting value missing will fail, now that value1 is written.
	actualVal, err = mvcc.ConditionalPut(testKey1, makeTS(0, 0), value1, nil, nil)
	if err == nil {
		t.Fatal("expected error on key already exists")
	}
	if !bytes.Equal(actualVal.Bytes, value1.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			actualVal.Bytes, value1.Bytes)
	}

	// Conditional put expecting wrong value2, will fail.
	actualVal, err = mvcc.ConditionalPut(testKey1, makeTS(0, 0), value1, &value2, nil)
	if err == nil {
		t.Fatal("expected error on key does not match")
	}
	if !bytes.Equal(actualVal.Bytes, value1.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			actualVal.Bytes, value1.Bytes)
	}

	// Move to a empty value. Will succeed.
	_, err = mvcc.ConditionalPut(testKey1, makeTS(0, 0), valueEmpty, &value1, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Now move to value2 from expected empty value.
	_, err = mvcc.ConditionalPut(testKey1, makeTS(0, 0), value2, &valueEmpty, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Verify we get value2 as expected.
	value, err := mvcc.Get(testKey1, makeTS(0, 0), nil)
	if !bytes.Equal(value2.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.Bytes, value.Bytes)
	}
}

func TestMVCCResolveTxn(t *testing.T) {
	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(0, 0), value1, txn1)
	value, err := mvcc.Get(testKey1, makeTS(0, 0), txn1)
	if !bytes.Equal(value1.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.Bytes, value.Bytes)
	}

	// Resolve will write with txn1's timestamp which is 0,0.
	err = mvcc.ResolveWriteIntent(testKey1, txn1Commit)
	if err != nil {
		t.Fatal(err)
	}

	value, err = mvcc.Get(testKey1, makeTS(0, 0), nil)
	if !bytes.Equal(value1.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.Bytes, value.Bytes)
	}
}

func TestMVCCAbortTxn(t *testing.T) {
	mvcc, engine := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(0, 0), value1, txn1)
	err = mvcc.ResolveWriteIntent(testKey1, txn1Abort)
	if err != nil {
		t.Fatal(err)
	}
	if err := mvcc.engine.Commit(); err != nil {
		t.Fatal(err)
	}

	value, err := mvcc.Get(testKey1, makeTS(1, 0), nil)
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
	mvcc, engine := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(0, 0), value1, nil)
	err = mvcc.Put(testKey1, makeTS(1, 0), value2, nil)
	err = mvcc.Put(testKey1, makeTS(2, 0), value3, txn1)
	err = mvcc.ResolveWriteIntent(testKey1, txn1Abort)
	if err := mvcc.engine.Commit(); err != nil {
		t.Fatal(err)
	}

	meta, err := engine.Get(MVCCEncodeKey(testKey1))
	if err != nil {
		t.Fatal(err)
	}
	if len(meta) == 0 {
		t.Fatalf("expected the MVCCMetadata")
	}

	value, err := mvcc.Get(testKey1, makeTS(3, 0), nil)
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
	mvcc, _ := createTestMVCC()
	// Start with epoch 1.
	if err := mvcc.Put(testKey1, makeTS(0, 0), value1, txn1); err != nil {
		t.Fatal(err)
	}
	// Now write with greater timestamp and epoch 2.
	if err := mvcc.Put(testKey1, makeTS(1, 0), value2, txn1e2); err != nil {
		t.Fatal(err)
	}
	// Try a write with an earlier timestamp; this is just ignored.
	if err := mvcc.Put(testKey1, makeTS(0, 0), value1, txn1e2); err != nil {
		t.Fatal(err)
	}
	// Try a write with an earlier epoch; again ignored.
	if err := mvcc.Put(testKey1, makeTS(1, 0), value1, txn1); err != nil {
		t.Fatal(err)
	}
	// Try a write with different value using both later timestamp and epoch.
	if err := mvcc.Put(testKey1, makeTS(1, 0), value3, txn1e2); err != nil {
		t.Fatal(err)
	}
	// Resolve the intent.
	if err := mvcc.ResolveWriteIntent(testKey1, makeTxn(txn1e2Commit, makeTS(1, 0))); err != nil {
		t.Fatal(err)
	}
	// Now try writing an earlier intent--should get write too old error.
	if err := mvcc.Put(testKey1, makeTS(0, 0), value2, txn2); err == nil {
		t.Fatal("expected write too old error")
	}
	// Attempt to read older timestamp; should fail.
	value, err := mvcc.Get(testKey1, makeTS(0, 0), nil)
	if value != nil || err != nil {
		t.Errorf("expected value nil, err nil; got %+v, %v", value, err)
	}
	// Read at correct timestamp.
	value, err = mvcc.Get(testKey1, makeTS(1, 0), nil)
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
	mvcc, _ := createTestMVCC()
	// Write initial value wihtout a txn.
	if err := mvcc.Put(testKey1, makeTS(0, 0), value1, nil); err != nil {
		t.Fatal(err)
	}
	// Now write using txn1, epoch 1.
	if err := mvcc.Put(testKey1, makeTS(1, 0), value2, txn1); err != nil {
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
		value, err := mvcc.Get(testKey1, makeTS(2, 0), test.txn)
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

// TestMVCCReadWithPushedTimestamp verifies that a read for a value
// written by the transaction, but then subsequently pushed, can still
// be read by the txn at the later timestamp, even if an earlier
// timestamp is specified. This happens when a txn's intents are
// resolved by other actors; the intents shouldn't become invisible
// to pushed txn.
func TestMVCCReadWithPushedTimestamp(t *testing.T) {
	mvcc, _ := createTestMVCC()
	// Start with epoch 1.
	if err := mvcc.Put(testKey1, makeTS(0, 0), value1, txn1); err != nil {
		t.Fatal(err)
	}
	// Resolve the intent, pushing its timestamp forward.
	if err := mvcc.ResolveWriteIntent(testKey1, makeTxn(txn1, makeTS(1, 0))); err != nil {
		t.Fatal(err)
	}
	// Attempt to read using naive txn's previous timestamp.
	value, err := mvcc.Get(testKey1, makeTS(0, 0), txn1)
	if err != nil || value == nil || !bytes.Equal(value.Bytes, value1.Bytes) {
		t.Errorf("expected value %q, err nil; got %+v, %v", value1.Bytes, value, err)
	}
}

func TestMVCCResolveWithDiffEpochs(t *testing.T) {
	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(0, 0), value1, txn1)
	err = mvcc.Put(testKey2, makeTS(0, 0), value2, txn1e2)
	num, err := mvcc.ResolveWriteIntentRange(testKey1, testKey2.Next(), 2, txn1e2Commit)
	if num != 2 {
		t.Errorf("expected 2 rows resolved; got %d", num)
	}

	// Verify key1 is empty, as resolution with epoch 2 would have
	// aborted the epoch 1 intent.
	value, err := mvcc.Get(testKey1, makeTS(0, 0), nil)
	if value != nil || err != nil {
		t.Errorf("expected value nil, err nil; got %+v, %v", value, err)
	}

	// Key2 should be committed.
	value, err = mvcc.Get(testKey2, makeTS(0, 0), nil)
	if !bytes.Equal(value2.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value2.Bytes, value.Bytes)
	}
}

func TestMVCCResolveWithUpdatedTimestamp(t *testing.T) {
	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(0, 0), value1, txn1)
	value, err := mvcc.Get(testKey1, makeTS(1, 0), txn1)
	if !bytes.Equal(value1.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.Bytes, value.Bytes)
	}

	// Resolve with a higher commit timestamp -- this should rewrite the
	// intent when making it permanent.
	err = mvcc.ResolveWriteIntent(testKey1, makeTxn(txn1Commit, makeTS(1, 0)))
	if err != nil {
		t.Fatal(err)
	}

	if value, err := mvcc.Get(testKey1, makeTS(0, 0), nil); value != nil || err != nil {
		t.Fatalf("expected both value and err to be nil: %+v, %v", value, err)
	}

	value, err = mvcc.Get(testKey1, makeTS(1, 0), nil)
	if !value.Timestamp.Equal(makeTS(1, 0)) {
		t.Fatalf("expected timestamp %+v == %+v", value.Timestamp, makeTS(1, 0))
	}
	if !bytes.Equal(value1.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.Bytes, value.Bytes)
	}
}

func TestMVCCResolveWithPushedTimestamp(t *testing.T) {
	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(0, 0), value1, txn1)
	value, err := mvcc.Get(testKey1, makeTS(1, 0), txn1)
	if !bytes.Equal(value1.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.Bytes, value.Bytes)
	}

	// Resolve with a higher commit timestamp, but with still-pending transaction.
	// This represents a straightforward push (i.e. from a read/write conflict).
	err = mvcc.ResolveWriteIntent(testKey1, makeTxn(txn1, makeTS(1, 0)))
	if err != nil {
		t.Fatal(err)
	}

	if value, err := mvcc.Get(testKey1, makeTS(1, 0), nil); value != nil || err == nil {
		t.Fatalf("expected both value nil and err to be a writeIntentError: %+v", value)
	}

	// Can still fetch the value using txn1.
	value, err = mvcc.Get(testKey1, makeTS(1, 0), txn1)
	if !value.Timestamp.Equal(makeTS(1, 0)) {
		t.Fatalf("expected timestamp %+v == %+v", value.Timestamp, makeTS(1, 0))
	}
	if !bytes.Equal(value1.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.Bytes, value.Bytes)
	}
}

func TestMVCCResolveTxnNoOps(t *testing.T) {
	mvcc, _ := createTestMVCC()

	// Resolve a non existent key; noop.
	err := mvcc.ResolveWriteIntent(testKey1, txn1Commit)
	if err != nil {
		t.Fatal(err)
	}

	// Add key and resolve despite there being no intent.
	err = mvcc.Put(testKey1, makeTS(0, 0), value1, nil)
	err = mvcc.ResolveWriteIntent(testKey1, txn2Commit)
	if err != nil {
		t.Fatal(err)
	}

	// Write intent and resolve with different txn.
	err = mvcc.Put(testKey1, makeTS(1, 0), value2, txn1)
	err = mvcc.ResolveWriteIntent(testKey1, txn2Commit)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMVCCResolveTxnRange(t *testing.T) {
	mvcc, _ := createTestMVCC()
	err := mvcc.Put(testKey1, makeTS(0, 0), value1, txn1)
	err = mvcc.Put(testKey2, makeTS(0, 0), value2, nil)
	err = mvcc.Put(testKey3, makeTS(0, 0), value3, txn2)
	err = mvcc.Put(testKey4, makeTS(0, 0), value4, txn1)

	num, err := mvcc.ResolveWriteIntentRange(testKey1, testKey4.Next(), 0, txn1Commit)
	if err != nil {
		t.Fatal(err)
	}
	if num != 4 {
		t.Fatalf("expected all keys to process for resolution, even though 2 are noops; got %d", num)
	}

	value, err := mvcc.Get(testKey1, makeTS(0, 0), nil)
	if !bytes.Equal(value1.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value1.Bytes, value.Bytes)
	}

	value, err = mvcc.Get(testKey2, makeTS(0, 0), nil)
	if !bytes.Equal(value2.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value2.Bytes, value.Bytes)
	}

	value, err = mvcc.Get(testKey3, makeTS(0, 0), txn2)
	if !bytes.Equal(value3.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value3.Bytes, value.Bytes)
	}

	value, err = mvcc.Get(testKey4, makeTS(0, 0), nil)
	if !bytes.Equal(value4.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value4.Bytes, value.Bytes)
	}
}

func TestFindSplitKey(t *testing.T) {
	mvcc, engine := createTestMVCC()
	// Generate a reservoir worth of KeyValues, each containing targetLength
	// bytes, writing key #i to (encoded) key #i through the MVCC facility.
	// Assuming that this translates roughly into same-length values after MVCC
	// encoding, the split key should hence be chosen as the middle key of the
	// interval.
	for i := 0; i < splitReservoirSize; i++ {
		k := fmt.Sprintf("%09d", i)
		v := strings.Repeat("X", 10-len(k))
		val := proto.Value{Bytes: []byte(v)}
		// Write the key and value through MVCC
		if err := mvcc.Put([]byte(k), makeTS(0, 0), val, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := mvcc.engine.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := engine.CreateSnapshot("snap1"); err != nil {
		t.Fatal(err)
	}
	humanSplitKey, err := MVCCFindSplitKey(engine, KeyMin, KeyMax, "snap1")
	if err != nil {
		t.Fatal(err)
	}
	ind, _ := strconv.Atoi(string(humanSplitKey))
	if diff := splitReservoirSize/2 - ind; diff > 1 || diff < -1 {
		t.Fatalf("wanted key #%d+-1, but got %d (diff %d)", ind+diff, ind, diff)
	}
	if err := engine.ReleaseSnapshot("snap1"); err != nil {
		t.Fatal(err)
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

func verifyStats(debug string, mvcc *MVCC, ms MVCCStats, t *testing.T) {
	// ...And verify stats.
	if ms.LiveBytes != mvcc.LiveBytes {
		t.Errorf("%s: mvcc live bytes %d; measured %d", debug, mvcc.LiveBytes, ms.LiveBytes)
	}
	if ms.KeyBytes != mvcc.KeyBytes {
		t.Errorf("%s: mvcc keyBytes %d; measured %d", debug, mvcc.KeyBytes, ms.KeyBytes)
	}
	if ms.ValBytes != mvcc.ValBytes {
		t.Errorf("%s: mvcc valBytes %d; measured %d", debug, mvcc.ValBytes, ms.ValBytes)
	}
	if ms.IntentBytes != mvcc.IntentBytes {
		t.Errorf("%s: mvcc intentBytes %d; measured %d", debug, mvcc.IntentBytes, ms.IntentBytes)
	}
	if ms.LiveCount != mvcc.LiveCount {
		t.Errorf("%s: mvcc liveCount %d; measured %d", debug, mvcc.LiveCount, ms.LiveCount)
	}
	if ms.KeyCount != mvcc.KeyCount {
		t.Errorf("%s: mvcc keyCount %d; measured %d", debug, mvcc.KeyCount, ms.KeyCount)
	}
	if ms.ValCount != mvcc.ValCount {
		t.Errorf("%s: mvcc valCount %d; measured %d", debug, mvcc.ValCount, ms.ValCount)
	}
	if ms.IntentCount != mvcc.IntentCount {
		t.Errorf("%s: mvcc intentCount %d; measured %d", debug, mvcc.IntentCount, ms.IntentCount)
	}
}

// TestMVCCStatsBasic writes a value, then deletes it as an intent via
// a transaction, then resolves the intent, manually verifying the
// mvcc stats at each step.
func TestMVCCStatsBasic(t *testing.T) {
	mvcc, _ := createTestMVCC()

	// Put a value.
	ts := makeTS(0, 1)
	key := proto.Key("a")
	value := proto.Value{Bytes: []byte("value")}
	if err := mvcc.Put(key, ts, value, nil); err != nil {
		t.Fatal(err)
	}
	mKeySize := int64(len(MVCCEncodeKey(key)))
	mValSize := encodedSize(&proto.MVCCMetadata{Timestamp: ts}, t)
	vKeySize := int64(len(MVCCEncodeVersionKey(key, ts)))
	vValSize := encodedSize(&proto.MVCCValue{Value: &value}, t)

	ms := MVCCStats{
		LiveBytes: mKeySize + mValSize + vKeySize + vValSize,
		LiveCount: 1,
		KeyBytes:  mKeySize + vKeySize,
		KeyCount:  1,
		ValBytes:  mValSize + vValSize,
		ValCount:  1,
	}
	verifyStats("after put", mvcc, ms, t)

	// Delete the value using a transaction.
	txn := &proto.Transaction{ID: []byte("txn1"), Timestamp: makeTS(0, 1)}
	ts2 := makeTS(0, 2)
	if err := mvcc.Delete(key, ts2, txn); err != nil {
		t.Fatal(err)
	}
	m2ValSize := encodedSize(&proto.MVCCMetadata{Timestamp: ts2, Deleted: true, Txn: txn}, t)
	v2KeySize := int64(len(MVCCEncodeVersionKey(key, ts2)))
	v2ValSize := encodedSize(&proto.MVCCValue{Deleted: true}, t)
	ms2 := MVCCStats{
		KeyBytes:    mKeySize + vKeySize + v2KeySize,
		KeyCount:    1,
		ValBytes:    m2ValSize + vValSize + v2ValSize,
		ValCount:    2,
		IntentBytes: v2KeySize + v2ValSize,
		IntentCount: 1,
	}
	verifyStats("after delete", mvcc, ms2, t)

	// Resolve the deletion by aborting it.
	txn.Status = proto.ABORTED
	if err := mvcc.ResolveWriteIntent(key, txn); err != nil {
		t.Fatal(err)
	}
	// Stats should equal same as before the deletion after aborting the intent.
	verifyStats("after abort", mvcc, ms, t)

	// Re-delete, but this time commit it.
	txn.Status = proto.PENDING
	ts3 := makeTS(0, 3)
	if err := mvcc.Delete(key, ts3, txn); err != nil {
		t.Fatal(err)
	}
	verifyStats("after 2nd delete", mvcc, ms2, t) // should be same as before.

	// Now commit.
	txn.Status = proto.COMMITTED
	if err := mvcc.ResolveWriteIntent(key, txn); err != nil {
		t.Fatal(err)
	}
	m3ValSize := encodedSize(&proto.MVCCMetadata{Timestamp: ts3, Deleted: true}, t)
	ms3 := MVCCStats{
		KeyBytes: mKeySize + vKeySize + v2KeySize,
		KeyCount: 1,
		ValBytes: m3ValSize + vValSize + v2ValSize,
		ValCount: 2,
	}
	verifyStats("after abort", mvcc, ms3, t)
}

// TestMVCCStatsWithRandomRuns creates a random sequence of puts,
// deletes and delete ranges and at each step verifies that the mvcc
// stats match a manual computation of range stats via a scan of the
// underlying engine.
func TestMVCCStatsWithRandomRuns(t *testing.T) {
	var seed int64
	err := binary.Read(crypto_rand.Reader, binary.LittleEndian, &seed)
	if err != nil {
		t.Fatalf("could not read from crypto/rand: %s", err)
	}
	log.Infof("using pseudo random number generator with seed %d", seed)
	rng := rand.New(rand.NewSource(seed))
	mvcc, _ := createTestMVCC()

	// Test with empty mvcc.
	verifyStats("empty test", mvcc, MVCCStats{}, t)

	// Now, generate a rngom sequence of puts, deletes and resolves.
	// Each put and delete may or may not involve a txn. Resolves may
	// either commit or abort.
	keys := map[int32][]byte{}
	for i := int32(0); i < int32(1000); i++ {
		key := []byte(fmt.Sprintf("%s-%d", util.RandString(rng, int(rng.Int31n(32))), i))
		keys[i] = key
		var txn *proto.Transaction
		if rng.Int31n(2) == 0 { // create a txn with 50% prob
			txn = &proto.Transaction{ID: []byte(fmt.Sprintf("txn-%d", i)), Timestamp: makeTS(0, i)}
		}
		// With 25% probability, put a new value; otherwise, delete an earlier
		// key. Because an earlier step in this process may have itself been
		// a delete, we could end up deleting a non-existent key, which is good;
		// we don't mind testing that case as well.
		isDelete := rng.Int31n(4) == 0
		if i > 0 && isDelete {
			idx := rng.Int31n(i)
			log.V(1).Infof("*** DELETE index %d", idx)
			if err := mvcc.Delete(keys[idx], makeTS(0, i), txn); err != nil {
				// Abort any write intent on an earlier, unresolved txn.
				if wiErr, ok := err.(*proto.WriteIntentError); ok {
					wiErr.Txn.Status = proto.ABORTED
					log.V(1).Infof("*** ABORT index %d", idx)
					if err := mvcc.ResolveWriteIntent(keys[idx], &wiErr.Txn); err != nil {
						t.Fatal(err)
					}
					// Now, re-delete.
					log.V(1).Infof("*** RE-DELETE index %d", idx)
					if err := mvcc.Delete(keys[idx], makeTS(0, i), txn); err != nil {
						t.Fatal(err)
					}
				} else {
					t.Fatal(err)
				}
			}
		} else {
			rngVal := proto.Value{Bytes: []byte(util.RandString(rng, int(rng.Int31n(128))))}
			log.V(1).Infof("*** PUT index %d; TXN=%t", i, txn != nil)
			if err := mvcc.Put(key, makeTS(0, i), rngVal, txn); err != nil {
				t.Fatal(err)
			}
		}
		if !isDelete && txn != nil && rng.Int31n(2) == 0 { // resolve txn with 50% prob
			txn.Status = proto.COMMITTED
			if rng.Int31n(10) == 0 { // abort txn with 10% prob
				txn.Status = proto.ABORTED
			}
			log.V(1).Infof("*** RESOLVE index %d; COMMIT=%t", i, txn.Status == proto.COMMITTED)
			if err := mvcc.ResolveWriteIntent(key, txn); err != nil {
				t.Fatal(err)
			}
		}

		// Every 10th step, verify the stats via manual engine scan.
		if i%10 == 0 {
			// Compute the stats manually.
			ms, err := MVCCComputeStats(mvcc.engine, KeyMin, KeyMax)
			if err != nil {
				t.Fatal(err)
			}
			verifyStats(fmt.Sprintf("cycle %d", i), mvcc, ms, t)
		}
	}
}
