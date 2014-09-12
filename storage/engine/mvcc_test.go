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
	"math"
	"reflect"
	"sort"
	"testing"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// Constants for system-reserved keys in the KV map.
var (
	testKey01  = Key("/db1")
	testKey02  = Key("/db2")
	testKey03  = Key("/db3")
	testKey04  = Key("/db4")
	txn01      = []byte("Txn01")
	txn02      = []byte("Txn02")
	value01    = proto.Value{Bytes: []byte("testValue01")}
	value02    = proto.Value{Bytes: []byte("testValue02")}
	value03    = proto.Value{Bytes: []byte("testValue03")}
	value04    = proto.Value{Bytes: []byte("testValue04")}
	valueEmpty = proto.Value{}
)

// createTestMVCC creates a new MVCC instance with the given engine.
func createTestMVCC(t *testing.T) *MVCC {
	return &MVCC{
		engine: NewInMem(proto.Attributes{}, 1<<20),
	}
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
	aBinKey := encoding.EncodeBinary(nil, []byte("a"))
	a0BinKey := encoding.EncodeBinary(nil, []byte("a\x00"))
	keys := []string{
		string(aBinKey),
		string(mvccEncodeKey(aBinKey, makeTS(math.MaxInt64, 0))),
		string(mvccEncodeKey(aBinKey, makeTS(1, 0))),
		string(mvccEncodeKey(aBinKey, makeTS(0, 0))),
		string(a0BinKey),
		string(mvccEncodeKey(a0BinKey, makeTS(math.MaxInt64, 0))),
		string(mvccEncodeKey(a0BinKey, makeTS(1, 0))),
		string(mvccEncodeKey(a0BinKey, makeTS(0, 0))),
	}
	sortKeys := make([]string, len(keys))
	copy(sortKeys, keys)
	sort.Strings(sortKeys)
	if !reflect.DeepEqual(sortKeys, keys) {
		t.Error("expected keys to sort in order %s, but got %s", keys, sortKeys)
	}
}

func TestMVCCGetNotExist(t *testing.T) {
	mvcc := createTestMVCC(t)
	value, err := mvcc.Get(testKey01, makeTS(0, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if value != nil {
		t.Fatal("the value should be empty")
	}
}

func TestMVCCPutWithBadValue(t *testing.T) {
	mvcc := createTestMVCC(t)
	badValue := proto.Value{Bytes: []byte("a"), Integer: gogoproto.Int64(1)}
	err := mvcc.Put(testKey01, makeTS(0, 0), badValue, nil)
	if err == nil {
		t.Fatal("expected an error putting a value with both byte slice and integer components")
	}
}

func TestMVCCPutWithTxn(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(0, 0), value01, txn01)
	if err != nil {
		t.Fatal(err)
	}

	value, err := mvcc.Get(testKey01, makeTS(1, 0), txn01)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value01.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value01.Bytes, value.Bytes)
	}
}

func TestMVCCPutWithoutTxn(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(0, 0), value01, nil)
	if err != nil {
		t.Fatal(err)
	}

	value, err := mvcc.Get(testKey01, makeTS(1, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value01.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value01.Bytes, value.Bytes)
	}
}

func TestMVCCUpdateExistingKey(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(0, 0), value01, nil)
	if err != nil {
		t.Fatal(err)
	}

	value, err := mvcc.Get(testKey01, makeTS(1, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value01.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value01.Bytes, value.Bytes)
	}

	err = mvcc.Put(testKey01, makeTS(2, 0), value02, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Read the latest version.
	value, err = mvcc.Get(testKey01, makeTS(3, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value02.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value02.Bytes, value.Bytes)
	}

	// Read the old version.
	value, err = mvcc.Get(testKey01, makeTS(1, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value01.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value01.Bytes, value.Bytes)
	}
}

func TestMVCCUpdateExistingKeyOldVersion(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(1, 1), value01, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Earlier walltime.
	err = mvcc.Put(testKey01, makeTS(0, 0), value02, nil)
	if err == nil {
		t.Fatal("expected error on old version")
	}
	// Earlier logical time.
	err = mvcc.Put(testKey01, makeTS(1, 0), value02, nil)
	if err == nil {
		t.Fatal("expected error on old version")
	}
}

func TestMVCCUpdateExistingKeyInTxn(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(0, 0), value01, txn01)
	if err != nil {
		t.Fatal(err)
	}

	err = mvcc.Put(testKey01, makeTS(1, 0), value01, txn01)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMVCCUpdateExistingKeyDiffTxn(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(0, 0), value01, txn01)
	if err != nil {
		t.Fatal(err)
	}

	err = mvcc.Put(testKey01, makeTS(1, 0), value02, txn02)
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

	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(3, 0), value01, nil)
	err = mvcc.Put(testKey02, makeTS(1, 0), value02, nil)

	value, err := mvcc.Get(testKey01, makeTS(2, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if value != nil {
		t.Fatal("the value should be empty")
	}
}

func TestMVCCGetAndDelete(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(1, 0), value01, nil)
	value, err := mvcc.Get(testKey01, makeTS(2, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if value == nil {
		t.Fatal("the value should not be empty")
	}

	err = mvcc.Delete(testKey01, makeTS(3, 0), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Read the latest version which should be deleted.
	value, err = mvcc.Get(testKey01, makeTS(4, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if value != nil {
		t.Fatal("the value should be empty")
	}

	// Read the old version which should still exist.
	for _, logical := range []int32{0, math.MaxInt32} {
		value, err = mvcc.Get(testKey01, makeTS(2, logical), nil)
		if err != nil {
			t.Fatal(err)
		}
		if value == nil {
			t.Fatal("the value should not be empty")
		}
	}
}

func TestMVCCGetAndDeleteInTxn(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(1, 0), value01, txn01)
	value, err := mvcc.Get(testKey01, makeTS(2, 0), txn01)
	if err != nil {
		t.Fatal(err)
	}
	if value == nil {
		t.Fatal("the value should not be empty")
	}

	err = mvcc.Delete(testKey01, makeTS(3, 0), txn01)
	if err != nil {
		t.Fatal(err)
	}

	// Read the latest version which should be deleted.
	value, err = mvcc.Get(testKey01, makeTS(4, 0), txn01)
	if err != nil {
		t.Fatal(err)
	}
	if value != nil {
		t.Fatal("the value should be empty")
	}

	// Read the old version which should still exist.
	value, err = mvcc.Get(testKey01, makeTS(2, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if value == nil {
		t.Fatal("the value should not be empty")
	}
}

func TestMVCCGetWriteIntentError(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(0, 0), value01, txn01)
	if err != nil {
		t.Fatal(err)
	}

	_, err = mvcc.Get(testKey01, makeTS(1, 0), nil)
	if err == nil {
		t.Fatal("cannot read the value of a write intent without TxnID")
	}

	_, err = mvcc.Get(testKey01, makeTS(1, 0), txn02)
	if err == nil {
		t.Fatal("cannot read the value of a write intent from a different TxnID")
	}
}

func TestMVCCScan(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(1, 0), value01, nil)
	err = mvcc.Put(testKey01, makeTS(2, 0), value04, nil)
	err = mvcc.Put(testKey02, makeTS(1, 0), value02, nil)
	err = mvcc.Put(testKey02, makeTS(3, 0), value03, nil)
	err = mvcc.Put(testKey03, makeTS(1, 0), value03, nil)
	err = mvcc.Put(testKey03, makeTS(4, 0), value02, nil)
	err = mvcc.Put(testKey04, makeTS(1, 0), value04, nil)
	err = mvcc.Put(testKey04, makeTS(5, 0), value01, nil)

	kvs, _, err := mvcc.Scan(testKey02, testKey04, 0, makeTS(1, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, testKey02) ||
		!bytes.Equal(kvs[1].Key, testKey03) ||
		!bytes.Equal(kvs[0].Value.Bytes, value02.Bytes) ||
		!bytes.Equal(kvs[1].Value.Bytes, value03.Bytes) {
		t.Fatal("the value should not be empty")
	}

	kvs, _, err = mvcc.Scan(testKey02, testKey04, 0, makeTS(4, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, testKey02) ||
		!bytes.Equal(kvs[1].Key, testKey03) ||
		!bytes.Equal(kvs[0].Value.Bytes, value03.Bytes) ||
		!bytes.Equal(kvs[1].Value.Bytes, value02.Bytes) {
		t.Fatal("the value should not be empty")
	}

	kvs, _, err = mvcc.Scan(testKey04, KeyMax, 0, makeTS(1, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey04) ||
		!bytes.Equal(kvs[0].Value.Bytes, value04.Bytes) {
		t.Fatal("the value should not be empty")
	}

	_, err = mvcc.Get(testKey01, makeTS(1, 0), txn02)
	kvs, _, err = mvcc.Scan(KeyMin, testKey02, 0, makeTS(1, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey01) ||
		!bytes.Equal(kvs[0].Value.Bytes, value01.Bytes) {
		t.Fatal("the value should not be empty")
	}
}

func TestMVCCScanMaxNum(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(1, 0), value01, nil)
	err = mvcc.Put(testKey02, makeTS(1, 0), value02, nil)
	err = mvcc.Put(testKey03, makeTS(1, 0), value03, nil)
	err = mvcc.Put(testKey04, makeTS(1, 0), value04, nil)

	kvs, _, err := mvcc.Scan(testKey02, testKey04, 1, makeTS(1, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey02) ||
		!bytes.Equal(kvs[0].Value.Bytes, value02.Bytes) {
		t.Fatal("the value should not be empty")
	}
}

func TestMVCCScanWithKeyPrefix(t *testing.T) {
	mvcc := createTestMVCC(t)
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
	err := mvcc.Put(Key(encoding.EncodeString([]byte{}, "/a")), makeTS(1, 0), value01, nil)
	err = mvcc.Put(Key(encoding.EncodeString([]byte{}, "/a")), makeTS(2, 0), value02, nil)
	err = mvcc.Put(Key(encoding.EncodeString([]byte{}, "/aa")), makeTS(2, 0), value02, nil)
	err = mvcc.Put(Key(encoding.EncodeString([]byte{}, "/aa")), makeTS(3, 0), value03, nil)
	err = mvcc.Put(Key(encoding.EncodeString([]byte{}, "/b")), makeTS(1, 0), value03, nil)

	kvs, _, err := mvcc.Scan(Key(encoding.EncodeString([]byte{}, "/a")),
		Key(encoding.EncodeString([]byte{}, "/b")), 0, makeTS(2, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, Key(encoding.EncodeString([]byte{}, "/a"))) ||
		!bytes.Equal(kvs[1].Key, Key(encoding.EncodeString([]byte{}, "/aa"))) ||
		!bytes.Equal(kvs[0].Value.Bytes, value02.Bytes) ||
		!bytes.Equal(kvs[1].Value.Bytes, value02.Bytes) {
		t.Fatal("the value should not be empty")
	}
}

func TestMVCCScanInTxn(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(1, 0), value01, nil)
	err = mvcc.Put(testKey02, makeTS(1, 0), value02, nil)
	err = mvcc.Put(testKey03, makeTS(1, 0), value03, txn01)
	err = mvcc.Put(testKey04, makeTS(1, 0), value04, nil)

	kvs, _, err := mvcc.Scan(testKey02, testKey04, 0, makeTS(1, 0), txn01)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, testKey02) ||
		!bytes.Equal(kvs[1].Key, testKey03) ||
		!bytes.Equal(kvs[0].Value.Bytes, value02.Bytes) ||
		!bytes.Equal(kvs[1].Value.Bytes, value03.Bytes) {
		t.Fatal("the value should not be empty")
	}

	kvs, _, err = mvcc.Scan(testKey02, testKey04, 0, makeTS(1, 0), nil)
	if err == nil {
		t.Fatal("expected error on uncommitted write intent")
	}
}

func TestMVCCDeleteRange(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(1, 0), value01, nil)
	err = mvcc.Put(testKey02, makeTS(1, 0), value02, nil)
	err = mvcc.Put(testKey03, makeTS(1, 0), value03, nil)
	err = mvcc.Put(testKey04, makeTS(1, 0), value04, nil)

	num, err := mvcc.DeleteRange(testKey02, testKey04, 0, makeTS(2, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if num != 2 {
		t.Fatal("the value should not be empty")
	}
	kvs, _, _ := mvcc.Scan(KeyMin, KeyMax, 0, makeTS(2, 0), nil)
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, testKey01) ||
		!bytes.Equal(kvs[1].Key, testKey04) ||
		!bytes.Equal(kvs[0].Value.Bytes, value01.Bytes) ||
		!bytes.Equal(kvs[1].Value.Bytes, value04.Bytes) {
		t.Fatal("the value should not be empty")
	}

	num, err = mvcc.DeleteRange(testKey04, KeyMax, 0, makeTS(2, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if num != 1 {
		t.Fatal("the value should not be empty")
	}
	kvs, _, _ = mvcc.Scan(KeyMin, KeyMax, 0, makeTS(2, 0), nil)
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey01) ||
		!bytes.Equal(kvs[0].Value.Bytes, value01.Bytes) {
		t.Fatal("the value should not be empty")
	}

	num, err = mvcc.DeleteRange(KeyMin, testKey02, 0, makeTS(2, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if num != 1 {
		t.Fatal("the value should not be empty")
	}
	kvs, _, _ = mvcc.Scan(KeyMin, KeyMax, 0, makeTS(2, 0), nil)
	if len(kvs) != 0 {
		t.Fatal("the value should be empty")
	}
}

func TestMVCCDeleteRangeFailed(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(1, 0), value01, nil)
	err = mvcc.Put(testKey02, makeTS(1, 0), value02, txn01)
	err = mvcc.Put(testKey03, makeTS(1, 0), value03, txn01)
	err = mvcc.Put(testKey04, makeTS(1, 0), value04, nil)

	_, err = mvcc.DeleteRange(testKey02, testKey04, 0, makeTS(1, 0), nil)
	if err == nil {
		t.Fatal("expected error on uncommitted write intent")
	}

	_, err = mvcc.DeleteRange(testKey02, testKey04, 0, makeTS(1, 0), txn01)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMVCCDeleteRangeConcurrentTxn(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(1, 0), value01, nil)
	err = mvcc.Put(testKey02, makeTS(1, 0), value02, txn01)
	err = mvcc.Put(testKey03, makeTS(2, 0), value03, txn02)
	err = mvcc.Put(testKey04, makeTS(1, 0), value04, nil)

	_, err = mvcc.DeleteRange(testKey02, testKey04, 0, makeTS(1, 0), txn01)
	if err == nil {
		t.Fatal("expected error on uncommitted write intent")
	}
}

func TestMVCCConditionalPut(t *testing.T) {
	mvcc := createTestMVCC(t)
	actualVal, err := mvcc.ConditionalPut(testKey01, makeTS(0, 0), value01, &value02, nil)
	if err == nil {
		t.Fatal("expected error on key not exists")
	}
	if actualVal != nil {
		t.Fatalf("expected missing actual value: %v", actualVal)
	}

	// Verify the difference between missing value and empty value.
	actualVal, err = mvcc.ConditionalPut(testKey01, makeTS(0, 0), value01, &valueEmpty, nil)
	if err == nil {
		t.Fatal("expected error on key not exists")
	}
	if actualVal != nil {
		t.Fatalf("expected missing actual value: %v", actualVal)
	}

	// Do a conditional put with expectation that the value is completely missing; will succeed.
	_, err = mvcc.ConditionalPut(testKey01, makeTS(0, 0), value01, nil, nil)
	if err != nil {
		t.Fatalf("expected success with condition that key doesn't yet exist: %v", err)
	}

	// Another conditional put expecting value missing will fail, now that value01 is written.
	actualVal, err = mvcc.ConditionalPut(testKey01, makeTS(0, 0), value01, nil, nil)
	if err == nil {
		t.Fatal("expected error on key already exists")
	}
	if !bytes.Equal(actualVal.Bytes, value01.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			actualVal.Bytes, value01.Bytes)
	}

	// Conditional put expecting wrong value02, will fail.
	actualVal, err = mvcc.ConditionalPut(testKey01, makeTS(0, 0), value01, &value02, nil)
	if err == nil {
		t.Fatal("expected error on key does not match")
	}
	if !bytes.Equal(actualVal.Bytes, value01.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			actualVal.Bytes, value01.Bytes)
	}

	// Move to a empty value. Will succeed.
	_, err = mvcc.ConditionalPut(testKey01, makeTS(0, 0), valueEmpty, &value01, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Now move to value02 from expected empty value.
	_, err = mvcc.ConditionalPut(testKey01, makeTS(0, 0), value02, &valueEmpty, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Verify we get value02 as expected.
	value, err := mvcc.Get(testKey01, makeTS(0, 0), nil)
	if !bytes.Equal(value02.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value01.Bytes, value.Bytes)
	}
}

func TestMVCCResolveTxn(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(0, 0), value01, txn01)
	value, err := mvcc.Get(testKey01, makeTS(1, 0), txn01)
	if !bytes.Equal(value01.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value01.Bytes, value.Bytes)
	}

	err = mvcc.ResolveWriteIntent(testKey01, txn01, true)
	if err != nil {
		t.Fatal(err)
	}

	value, err = mvcc.Get(testKey01, makeTS(1, 0), nil)
	if !bytes.Equal(value01.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value01.Bytes, value.Bytes)
	}
}

func TestMVCCAbortTxn(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(0, 0), value01, txn01)
	err = mvcc.ResolveWriteIntent(testKey01, txn01, false)
	if err != nil {
		t.Fatal(err)
	}

	value, err := mvcc.Get(testKey01, makeTS(1, 0), nil)
	if value != nil {
		t.Fatalf("the value should be empty")
	}
	meta, err := mvcc.engine.Get(encoding.EncodeBinary(nil, testKey01))
	if err != nil {
		t.Fatal(err)
	}
	if len(meta) != 0 {
		t.Fatalf("expected no more MVCCMetadata")
	}
}

func TestMVCCAbortTxnWithPreviousVersion(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(0, 0), value01, nil)
	err = mvcc.Put(testKey01, makeTS(1, 0), value02, nil)
	err = mvcc.Put(testKey01, makeTS(2, 0), value03, txn01)
	err = mvcc.ResolveWriteIntent(testKey01, txn01, false)

	meta, err := mvcc.engine.Get(encoding.EncodeBinary(nil, testKey01))
	if err != nil {
		t.Fatal(err)
	}
	if len(meta) == 0 {
		t.Fatalf("expected the MVCCMetadata")
	}

	value, err := mvcc.Get(testKey01, makeTS(3, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value02.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value.Bytes, value02.Bytes)
	}
}

func TestMVCCResolveTxnFailure(t *testing.T) {
	mvcc := createTestMVCC(t)

	err := mvcc.ResolveWriteIntent(testKey01, txn01, true)
	if err == nil {
		t.Fatal("expected error on key not exist")
	}

	err = mvcc.Put(testKey01, makeTS(0, 0), value01, nil)
	err = mvcc.ResolveWriteIntent(testKey01, txn02, true)
	if err == nil {
		t.Fatal("expected error on write intent not exist")
	}

	err = mvcc.Put(testKey01, makeTS(1, 0), value02, txn01)
	err = mvcc.ResolveWriteIntent(testKey01, txn02, true)
	if err == nil {
		t.Fatal("expected error due to other txn")
	}
}

func TestMVCCResolveTxnRange(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(0, 0), value01, txn01)
	err = mvcc.Put(testKey02, makeTS(0, 0), value02, nil)
	err = mvcc.Put(testKey03, makeTS(0, 0), value03, txn02)
	err = mvcc.Put(testKey04, makeTS(0, 0), value04, txn01)

	num, err := mvcc.ResolveWriteIntentRange(testKey01, testKey04, 0, txn01, true)
	if err != nil {
		t.Fatal(err)
	}
	if num != 1 {
		t.Fatal("expected only one key to be committed")
	}

	value, err := mvcc.Get(testKey01, makeTS(1, 0), nil)
	if !bytes.Equal(value01.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value01.Bytes, value.Bytes)
	}

	value, err = mvcc.Get(testKey02, makeTS(1, 0), nil)
	if !bytes.Equal(value02.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value02.Bytes, value.Bytes)
	}

	value, err = mvcc.Get(testKey03, makeTS(1, 0), txn02)
	if !bytes.Equal(value03.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value03.Bytes, value.Bytes)
	}

	value, err = mvcc.Get(testKey04, makeTS(1, 0), txn01)
	if !bytes.Equal(value04.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value04.Bytes, value.Bytes)
	}
}
