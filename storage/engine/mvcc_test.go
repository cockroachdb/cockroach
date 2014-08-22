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
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// Constants for system-reserved keys in the KV map.
var (
	testKey01 = Key("/db1")
	testKey02 = Key("/db2")
	testKey03 = Key("/db3")
	testKey04 = Key("/db4")
	txn01     = "Txn01"
	txn02     = "Txn02"
	value01   = Value{Bytes: []byte("testValue01")}
	value02   = Value{Bytes: []byte("testValue02")}
	value03   = Value{Bytes: []byte("testValue03")}
	value04   = Value{Bytes: []byte("testValue04")}
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

func TestMVCCGetNotExist(t *testing.T) {
	mvcc := createTestMVCC(t)
	value, err := mvcc.Get(testKey01, makeTS(0, 0), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(value.Bytes) != 0 {
		t.Fatal("the value should be empty")
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
	err := mvcc.Put(testKey01, makeTS(0, 0), value01, "")
	if err != nil {
		t.Fatal(err)
	}

	value, err := mvcc.Get(testKey01, makeTS(1, 0), "")
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
	err := mvcc.Put(testKey01, makeTS(0, 0), value01, "")
	if err != nil {
		t.Fatal(err)
	}

	value, err := mvcc.Get(testKey01, makeTS(1, 0), "")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value01.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value01.Bytes, value.Bytes)
	}

	err = mvcc.Put(testKey01, makeTS(2, 0), value02, "")
	if err != nil {
		t.Fatal(err)
	}

	// Read the latest version.
	value, err = mvcc.Get(testKey01, makeTS(3, 0), "")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value02.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value02.Bytes, value.Bytes)
	}

	// Read the old version.
	value, err = mvcc.Get(testKey01, makeTS(1, 0), "")
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
	err := mvcc.Put(testKey01, makeTS(1, 1), value01, "")
	if err != nil {
		t.Fatal(err)
	}
	// Earlier walltime.
	err = mvcc.Put(testKey01, makeTS(0, 0), value02, "")
	if err == nil {
		t.Fatal("expected error on old version")
	}
	// Earlier logical time.
	err = mvcc.Put(testKey01, makeTS(1, 0), value02, "")
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
	err := mvcc.Put(testKey01, makeTS(3, 0), value01, "")
	err = mvcc.Put(testKey02, makeTS(1, 0), value02, "")

	value, err := mvcc.Get(testKey01, makeTS(2, 0), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(value.Bytes) != 0 {
		t.Fatal("the value should be empty")
	}
}

func TestMVCCGetAndDelete(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(1, 0), value01, "")
	value, err := mvcc.Get(testKey01, makeTS(2, 0), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(value.Bytes) == 0 {
		t.Fatal("the value should not be empty")
	}

	err = mvcc.Delete(testKey01, makeTS(3, 0), "")
	if err != nil {
		t.Fatal(err)
	}

	// Read the latest version which should be deleted.
	value, err = mvcc.Get(testKey01, makeTS(4, 0), "")
	if err != nil {
		t.Fatal(err)
	}
	if value.Bytes != nil {
		t.Fatal("the value should be empty")
	}

	// Read the old version which should still exist.
	for _, logical := range []int32{0, math.MaxInt32} {
		value, err = mvcc.Get(testKey01, makeTS(2, logical), "")
		if err != nil {
			t.Fatal(err)
		}
		if len(value.Bytes) == 0 {
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
	if len(value.Bytes) == 0 {
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
	if value.Bytes != nil {
		t.Fatal("the value should be empty")
	}

	// Read the old version which should still exist.
	value, err = mvcc.Get(testKey01, makeTS(2, 0), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(value.Bytes) == 0 {
		t.Fatal("the value should not be empty")
	}
}

func TestMVCCGetWriteIntentError(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(0, 0), value01, txn01)
	if err != nil {
		t.Fatal(err)
	}

	_, err = mvcc.Get(testKey01, makeTS(1, 0), "")
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
	err := mvcc.Put(testKey01, makeTS(1, 0), value01, "")
	err = mvcc.Put(testKey01, makeTS(2, 0), value04, "")
	err = mvcc.Put(testKey02, makeTS(1, 0), value02, "")
	err = mvcc.Put(testKey02, makeTS(3, 0), value03, "")
	err = mvcc.Put(testKey03, makeTS(1, 0), value03, "")
	err = mvcc.Put(testKey03, makeTS(4, 0), value02, "")
	err = mvcc.Put(testKey04, makeTS(1, 0), value04, "")
	err = mvcc.Put(testKey04, makeTS(5, 0), value01, "")

	kvs, _, err := mvcc.Scan(testKey02, testKey04, 0, makeTS(1, 0), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, testKey02) ||
		!bytes.Equal(kvs[1].Key, testKey03) ||
		!bytes.Equal(kvs[0].Bytes, value02.Bytes) ||
		!bytes.Equal(kvs[1].Bytes, value03.Bytes) {
		t.Fatal("the value should not be empty")
	}

	kvs, _, err = mvcc.Scan(testKey02, testKey04, 0, makeTS(4, 0), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, testKey02) ||
		!bytes.Equal(kvs[1].Key, testKey03) ||
		!bytes.Equal(kvs[0].Bytes, value03.Bytes) ||
		!bytes.Equal(kvs[1].Bytes, value02.Bytes) {
		t.Fatal("the value should not be empty")
	}

	kvs, _, err = mvcc.Scan(testKey04, KeyMax, 0, makeTS(1, 0), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey04) ||
		!bytes.Equal(kvs[0].Bytes, value04.Bytes) {
		t.Fatal("the value should not be empty")
	}

	_, err = mvcc.Get(testKey01, makeTS(1, 0), txn02)
	kvs, _, err = mvcc.Scan(KeyMin, testKey02, 0, makeTS(1, 0), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey01) ||
		!bytes.Equal(kvs[0].Bytes, value01.Bytes) {
		t.Fatal("the value should not be empty")
	}
}

func TestMVCCScanMaxNum(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(1, 0), value01, "")
	err = mvcc.Put(testKey02, makeTS(1, 0), value02, "")
	err = mvcc.Put(testKey03, makeTS(1, 0), value03, "")
	err = mvcc.Put(testKey04, makeTS(1, 0), value04, "")

	kvs, _, err := mvcc.Scan(testKey02, testKey04, 1, makeTS(1, 0), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey02) ||
		!bytes.Equal(kvs[0].Bytes, value02.Bytes) {
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
	err := mvcc.Put(Key(encoding.EncodeString([]byte{}, "/a")), makeTS(1, 0), value01, "")
	err = mvcc.Put(Key(encoding.EncodeString([]byte{}, "/a")), makeTS(2, 0), value02, "")
	err = mvcc.Put(Key(encoding.EncodeString([]byte{}, "/aa")), makeTS(2, 0), value02, "")
	err = mvcc.Put(Key(encoding.EncodeString([]byte{}, "/aa")), makeTS(3, 0), value03, "")
	err = mvcc.Put(Key(encoding.EncodeString([]byte{}, "/b")), makeTS(1, 0), value03, "")

	kvs, _, err := mvcc.Scan(Key(encoding.EncodeString([]byte{}, "/a")),
		Key(encoding.EncodeString([]byte{}, "/b")), 0, makeTS(2, 0), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, Key(encoding.EncodeString([]byte{}, "/a"))) ||
		!bytes.Equal(kvs[1].Key, Key(encoding.EncodeString([]byte{}, "/aa"))) ||
		!bytes.Equal(kvs[0].Bytes, value02.Bytes) ||
		!bytes.Equal(kvs[1].Bytes, value02.Bytes) {
		t.Fatal("the value should not be empty")
	}
}

func TestMVCCScanInTxn(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(1, 0), value01, "")
	err = mvcc.Put(testKey02, makeTS(1, 0), value02, "")
	err = mvcc.Put(testKey03, makeTS(1, 0), value03, txn01)
	err = mvcc.Put(testKey04, makeTS(1, 0), value04, "")

	kvs, _, err := mvcc.Scan(testKey02, testKey04, 0, makeTS(1, 0), txn01)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, testKey02) ||
		!bytes.Equal(kvs[1].Key, testKey03) ||
		!bytes.Equal(kvs[0].Bytes, value02.Bytes) ||
		!bytes.Equal(kvs[1].Bytes, value03.Bytes) {
		t.Fatal("the value should not be empty")
	}

	kvs, _, err = mvcc.Scan(testKey02, testKey04, 0, makeTS(1, 0), "")
	if err == nil {
		t.Fatal("expected error on uncommitted write intent")
	}
}

func TestMVCCDeleteRange(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(1, 0), value01, "")
	err = mvcc.Put(testKey02, makeTS(1, 0), value02, "")
	err = mvcc.Put(testKey03, makeTS(1, 0), value03, "")
	err = mvcc.Put(testKey04, makeTS(1, 0), value04, "")

	num, err := mvcc.DeleteRange(testKey02, testKey04, 0, makeTS(2, 0), "")
	if err != nil {
		t.Fatal(err)
	}
	if num != 2 {
		t.Fatal("the value should not be empty")
	}
	kvs, _, _ := mvcc.Scan(KeyMin, KeyMax, 0, makeTS(2, 0), "")
	if len(kvs) != 2 ||
		!bytes.Equal(kvs[0].Key, testKey01) ||
		!bytes.Equal(kvs[1].Key, testKey04) ||
		!bytes.Equal(kvs[0].Bytes, value01.Bytes) ||
		!bytes.Equal(kvs[1].Bytes, value04.Bytes) {
		t.Fatal("the value should not be empty")
	}

	num, err = mvcc.DeleteRange(testKey04, KeyMax, 0, makeTS(2, 0), "")
	if err != nil {
		t.Fatal(err)
	}
	if num != 1 {
		t.Fatal("the value should not be empty")
	}
	kvs, _, _ = mvcc.Scan(KeyMin, KeyMax, 0, makeTS(2, 0), "")
	if len(kvs) != 1 ||
		!bytes.Equal(kvs[0].Key, testKey01) ||
		!bytes.Equal(kvs[0].Bytes, value01.Bytes) {
		t.Fatal("the value should not be empty")
	}

	num, err = mvcc.DeleteRange(KeyMin, testKey02, 0, makeTS(2, 0), "")
	if err != nil {
		t.Fatal(err)
	}
	if num != 1 {
		t.Fatal("the value should not be empty")
	}
	kvs, _, _ = mvcc.Scan(KeyMin, KeyMax, 0, makeTS(2, 0), "")
	if len(kvs) != 0 {
		t.Fatal("the value should be empty")
	}
}

func TestMVCCDeleteRangeFailed(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.Put(testKey01, makeTS(1, 0), value01, "")
	err = mvcc.Put(testKey02, makeTS(1, 0), value02, txn01)
	err = mvcc.Put(testKey03, makeTS(1, 0), value03, txn01)
	err = mvcc.Put(testKey04, makeTS(1, 0), value04, "")

	_, err = mvcc.DeleteRange(testKey02, testKey04, 0, makeTS(1, 0), "")
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
	err := mvcc.Put(testKey01, makeTS(1, 0), value01, "")
	err = mvcc.Put(testKey02, makeTS(1, 0), value02, txn01)
	err = mvcc.Put(testKey03, makeTS(2, 0), value03, txn02)
	err = mvcc.Put(testKey04, makeTS(1, 0), value04, "")

	_, err = mvcc.DeleteRange(testKey02, testKey04, 0, makeTS(1, 0), txn01)
	if err == nil {
		t.Fatal("expected error on uncommitted write intent")
	}
}

func TestMVCCConditionalPut(t *testing.T) {
	mvcc := createTestMVCC(t)
	actualVal, err := mvcc.ConditionalPut(testKey01, makeTS(0, 0), value01, value02, "")
	if err == nil {
		t.Fatal("expected error on key not exists")
	}

	err = mvcc.Put(testKey01, makeTS(0, 0), value01, "")

	actualVal, err = mvcc.ConditionalPut(testKey01, makeTS(0, 0), value01, Value{}, "")
	if err == nil {
		t.Fatal("expected error on key already exists")
	}

	actualVal, err = mvcc.ConditionalPut(testKey01, makeTS(0, 0), value01, value02, "")
	if err == nil {
		t.Fatal("expected error on key does not match")
	}
	if !bytes.Equal(actualVal.Bytes, value01.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			actualVal.Bytes, value01.Bytes)
	}

	actualVal, err = mvcc.ConditionalPut(testKey01, makeTS(0, 0), value02, value01, "")
	if err != nil {
		t.Fatal(err)
	}

	value, err := mvcc.Get(testKey01, makeTS(0, 0), "")
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

	value, err = mvcc.Get(testKey01, makeTS(1, 0), "")
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

	value, err := mvcc.Get(testKey01, makeTS(1, 0), "")
	if len(value.Bytes) != 0 {
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
	err := mvcc.Put(testKey01, makeTS(0, 0), value01, "")
	err = mvcc.Put(testKey01, makeTS(1, 0), value02, "")
	err = mvcc.Put(testKey01, makeTS(2, 0), value03, txn01)
	err = mvcc.ResolveWriteIntent(testKey01, txn01, false)

	meta, err := mvcc.engine.Get(encoding.EncodeBinary(nil, testKey01))
	if err != nil {
		t.Fatal(err)
	}
	if len(meta) == 0 {
		t.Fatalf("expected the MVCCMetadata")
	}

	value, err := mvcc.Get(testKey01, makeTS(3, 0), "")
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

	err = mvcc.Put(testKey01, makeTS(0, 0), value01, "")
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
	err = mvcc.Put(testKey02, makeTS(0, 0), value02, "")
	err = mvcc.Put(testKey03, makeTS(0, 0), value03, txn02)
	err = mvcc.Put(testKey04, makeTS(0, 0), value04, txn01)

	num, err := mvcc.ResolveWriteIntentRange(testKey01, testKey04, 0, txn01, true)
	if err != nil {
		t.Fatal(err)
	}
	if num != 1 {
		t.Fatal("expected only one key to be committed")
	}

	value, err := mvcc.Get(testKey01, makeTS(1, 0), "")
	if !bytes.Equal(value01.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value01.Bytes, value.Bytes)
	}

	value, err = mvcc.Get(testKey02, makeTS(1, 0), "")
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
