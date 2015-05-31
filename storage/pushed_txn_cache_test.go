// Copyright 2015 The Cockroach Authors.
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
// Author: yananzhi (zac.zhiyanan@gmail.com)

package storage

import (
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/leaktest"
	gogoproto "github.com/gogo/protobuf/proto"
)

var (
	txn1       = &proto.Transaction{Key: proto.Key("a"), ID: []byte("Txn1"), Epoch: 1}
	txn1Pushed = &proto.Transaction{Key: proto.Key("a"), ID: []byte("Txn1"), Epoch: 1, Timestamp: makeTS(1, 0)}
	txn1Commit = &proto.Transaction{Key: proto.Key("a"), ID: []byte("Txn1"), Epoch: 1, Status: proto.COMMITTED}
	txn1Abort  = &proto.Transaction{Key: proto.Key("a"), ID: []byte("Txn1"), Epoch: 1, Status: proto.ABORTED}
	txn2       = &proto.Transaction{Key: proto.Key("b"), ID: []byte("Txn2"), Epoch: 1}
)

func txnEqual(txn1 *proto.Transaction, txn2 *proto.Transaction) bool {
	if txn1.Status == txn2.Status && txn1.Timestamp.Equal(txn2.Timestamp) &&
		txn1.Epoch == txn2.Epoch && txn1.Priority == txn2.Priority {
		return true
	}
	return false
}

// TestTxnCacheProcessPushTxnReq verifies processPushTxnReq fucntion work correctly
func TestTxnCacheProcessPushTxnReq(t *testing.T) {
	defer leaktest.AfterTest(t)
	testCases := []struct {
		cachedTxn      *proto.Transaction
		pusheeTxn      *proto.Transaction
		pushType       proto.PushTxnType
		expCached      bool
		replyPusheeTxn *proto.Transaction
	}{
		{txn1, txn1Pushed, proto.PUSH_TIMESTAMP, false, nil},
		{txn1, txn1Pushed, proto.ABORT_TXN, false, nil},
		{txn1, txn1Pushed, proto.CLEANUP_TXN, false, nil},
		{txn1Commit, txn1, proto.PUSH_TIMESTAMP, true, txn1Commit},
		{txn1Abort, txn1, proto.ABORT_TXN, true, txn1Abort},
		{txn1Commit, txn1, proto.CLEANUP_TXN, true, txn1Commit},
	}

	txnCache := newPushedTxnCache(10)
	for i, test := range testCases {
		req, resp := pushTxnArgs(txn2, test.pusheeTxn, test.pushType, 1, 1)
		txnCache.txnCache.Add(string(test.cachedTxn.ID), gogoproto.Clone(test.cachedTxn).(*proto.Transaction))

		cached := txnCache.processPushedTxn(req, resp)

		if cached != test.expCached {
			t.Fatalf("test %d expect cached=%v,  cached=%v,   cached txn=%v, pushee txn=%v, push txn type=%v",
				i, test.expCached, cached, *test.cachedTxn, *test.pusheeTxn, test.pushType)
		}
		// If the request is cached, the cache txn's epoch and priority may be updated
		if cached && !txnEqual(resp.PusheeTxn, test.replyPusheeTxn) {
			t.Fatalf("test %d expect reply pushee txn=%v,  reply pushee txn=%v", i, resp.PusheeTxn, test.replyPusheeTxn)
		}
		txnCache.txnCache.Clear()

	}
}

// TestTxnCacheUpdate verifies updateCache function work correctly
func TestTxnCacheUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)
	testCases := []struct {
		cachedTxn        *proto.Transaction
		repleyPusheeTxn  *proto.Transaction
		replaced         bool
		updatedCachedTxn *proto.Transaction
	}{
		{txn1, txn1, false, txn1},
		{txn1, txn1Pushed, true, txn1Pushed},
		{txn1Pushed, txn1, false, txn1Pushed},
		{txn1, txn1Abort, true, txn1Abort},
		{txn1Abort, txn1, false, txn1Abort},
		{txn1Commit, txn1, false, txn1Commit},
	}

	txnCache := newPushedTxnCache(10)
	for i, test := range testCases {

		cachedTxn := gogoproto.Clone(test.cachedTxn).(*proto.Transaction)
		replyPusheeTxn := gogoproto.Clone(test.repleyPusheeTxn).(*proto.Transaction)
		txnCache.txnCache.Add(string(cachedTxn.ID), cachedTxn)

		replaced := txnCache.updateCache(cachedTxn, replyPusheeTxn)
		if replaced != test.replaced {
			t.Fatalf("test %d expect replaced=%v,  replaced=%v,   cached txn=%v, reply pushee txn=%v",
				i, test.replaced, replaced, *test.cachedTxn, *test.repleyPusheeTxn)
		}
		value, _ := txnCache.txnCache.Get(string(cachedTxn.ID))

		finalCachedTxn := value.(*proto.Transaction)
		if !txnEqual(finalCachedTxn, test.updatedCachedTxn) {
			t.Fatalf("test %d expect cached txn=%v,  cached txn=%v", i, test.updatedCachedTxn, finalCachedTxn)
		}

		txnCache.txnCache.Clear()
	}
}
