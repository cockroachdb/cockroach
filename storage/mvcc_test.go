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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Jiang-Ming Yang (jiangming.yang@gmail.com)

package storage

import (
	"bytes"
	"testing"
)

// Constants for system-reserved keys in the KV map.
var (
	testKey   = Key("/db1")
	testKey02 = Key("/db2")
	txn01     = "Txn01"
	txn02     = "Txn02"
	value01   = Value{Bytes: []byte("testValue01")}
	value02   = Value{Bytes: []byte("testValue02")}
)

// createTestMVCC creates a new MVCC instance with the given engine.
func createTestMVCC(t *testing.T) *MVCC {
	return &MVCC{
		engine: createTestEngine(t),
	}
}

func TestMVCCGetNotExist(t *testing.T) {
	mvcc := createTestMVCC(t)
	value, txnID, err := mvcc.get(testKey, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(value.Bytes) != 0 {
		t.Fatal("the value should be empty")
	}
	if len(txnID) != 0 {
		t.Fatal("the txnID should be empty")
	}
}

func TestMVCCPutWithTxn(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.put(testKey, 0, value01, txn01)
	if err != nil {
		t.Fatal(err)
	}

	value, txnID, err := mvcc.get(testKey, 1, txn01)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value01.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value01.Bytes, value.Bytes)
	}
	if len(txnID) == 0 {
		t.Fatal("the txnID should not be empty")
	}
}

func TestMVCCPutWithoutTxn(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.put(testKey, 0, value01, "")
	if err != nil {
		t.Fatal(err)
	}

	value, txnID, err := mvcc.get(testKey, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value01.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value01.Bytes, value.Bytes)
	}
	if len(txnID) != 0 {
		t.Fatal("the txnID should be empty")
	}
}

func TestMVCCUpdateExistingKey(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.put(testKey, 0, value01, "")
	if err != nil {
		t.Fatal(err)
	}

	value, _, err := mvcc.get(testKey, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value01.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value01.Bytes, value.Bytes)
	}

	err = mvcc.put(testKey, 2, value02, "")
	if err != nil {
		t.Fatal(err)
	}

	// Read the latest version.
	value, _, err = mvcc.get(testKey, 3, "")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value02.Bytes, value.Bytes) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			value02.Bytes, value.Bytes)
	}

	// Read the old version.
	value, _, err = mvcc.get(testKey, 1, "")
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
	err := mvcc.put(testKey, 1, value01, "")
	if err != nil {
		t.Fatal(err)
	}

	err = mvcc.put(testKey, 0, value02, "")
	if err == nil {
		t.Fatal("expected error on old version")
	}
}

func TestMVCCUpdateExistingKeyInTxn(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.put(testKey, 0, value01, txn01)
	if err != nil {
		t.Fatal(err)
	}

	err = mvcc.put(testKey, 1, value01, txn01)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMVCCUpdateExistingKeyDiffTxn(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.put(testKey, 0, value01, txn01)
	if err != nil {
		t.Fatal(err)
	}

	err = mvcc.put(testKey, 1, value02, txn02)
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
	// a: keyMetadata(a)
	// a<T=3>
	// b: keyMetadata(b)
	// b<T=1>
	//
	// If we search for a<T=2>, the scan should not return "b".

	mvcc := createTestMVCC(t)
	err := mvcc.put(testKey, 3, value01, "")
	err = mvcc.put(testKey02, 1, value02, "")

	value, txnID, err := mvcc.get(testKey, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(value.Bytes) != 0 {
		t.Fatal("the value should be empty")
	}
	if len(txnID) != 0 {
		t.Fatal("the txnID should be empty")
	}
}

func TestMVCCGetAndDelete(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.put(testKey, 1, value01, "")
	value, txnID, err := mvcc.get(testKey, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(value.Bytes) == 0 {
		t.Fatal("the value should not be empty")
	}
	if len(txnID) != 0 {
		t.Fatal("the txnID should be empty")
	}

	err = mvcc.delete(testKey, 3, "")
	if err != nil {
		t.Fatal(err)
	}

	// Read the latest version which should be deleted.
	value, txnID, err = mvcc.get(testKey, 4, "")
	if err != nil {
		t.Fatal(err)
	}
	if value.Bytes != nil {
		t.Fatal("the value should be empty")
	}
	if len(txnID) != 0 {
		t.Fatal("the txnID should be empty")
	}

	// Read the old version which should still exist.
	value, txnID, err = mvcc.get(testKey, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(value.Bytes) == 0 {
		t.Fatal("the value should not be empty")
	}
}

func TestMVCCGetAndDeleteInTxn(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.put(testKey, 1, value01, txn01)
	value, txnID, err := mvcc.get(testKey, 2, txn01)
	if err != nil {
		t.Fatal(err)
	}
	if len(value.Bytes) == 0 {
		t.Fatal("the value should not be empty")
	}
	if txnID != txn01 {
		t.Fatalf("the received TxnID %s does not match the expected TxnID %s",
			txnID, txn01)
	}

	err = mvcc.delete(testKey, 3, txn01)
	if err != nil {
		t.Fatal(err)
	}

	// Read the latest version which should be deleted.
	value, txnID, err = mvcc.get(testKey, 4, txn01)
	if err != nil {
		t.Fatal(err)
	}
	if value.Bytes != nil {
		t.Fatal("The value should be empty")
	}
	if txnID != txn01 {
		t.Fatalf("the received TxnID %s does not match the expected TxnID %s",
			txnID, txn01)
	}

	// Read the old version which should still exist.
	value, txnID, err = mvcc.get(testKey, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(value.Bytes) == 0 {
		t.Fatal("The value should not be empty")
	}
	if len(txnID) != 0 {
		t.Fatal("the txnID should be empty")
	}
}

func TestMVCCGetWriteIntentError(t *testing.T) {
	mvcc := createTestMVCC(t)
	err := mvcc.put(testKey, 0, value01, txn01)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = mvcc.get(testKey, 1, "")
	if err == nil {
		t.Fatal("Cannot read the value of a write intent without TxnID")
	}

	_, _, err = mvcc.get(testKey, 1, txn02)
	if err == nil {
		t.Fatal("Cannot read the value of a write intent from a different TxnID")
	}
}
