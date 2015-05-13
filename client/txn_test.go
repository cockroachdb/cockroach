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
// Author: Peter Mattis (peter.mattis@gmail.com)

package client

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	gogoproto "github.com/gogo/protobuf/proto"
)

var (
	txnKey = proto.Key("test-txn")
	txnID  = []byte(util.NewUUID4())
)

func makeTS(walltime int64, logical int32) proto.Timestamp {
	return proto.Timestamp{
		WallTime: walltime,
		Logical:  logical,
	}
}

func newTestSender(handler func(Call)) KVSenderFunc {
	return func(_ context.Context, call Call) {
		header := call.Args.Header()
		header.UserPriority = gogoproto.Int32(-1)
		if header.Txn != nil && len(header.Txn.ID) == 0 {
			header.Txn.Key = txnKey
			header.Txn.ID = txnID
		}
		call.Reply.Reset()
		switch call.Args.(type) {
		case *proto.PutRequest:
			gogoproto.Merge(call.Reply, testPutResp)
		default:
			// Do nothing.
		}
		call.Reply.Header().Txn = gogoproto.Clone(call.Args.Header().Txn).(*proto.Transaction)

		if handler != nil {
			handler(call)
		}
	}
}

// TestTxnRequestTxnTimestamp verifies response txn timestamp is
// always upgraded on successive requests.
func TestTxnRequestTxnTimestamp(t *testing.T) {
	testCases := []struct {
		expRequestTS, responseTS proto.Timestamp
	}{
		{makeTS(0, 0), makeTS(10, 0)},
		{makeTS(10, 0), makeTS(10, 1)},
		{makeTS(10, 1), makeTS(10, 0)},
		{makeTS(10, 1), makeTS(20, 1)},
		{makeTS(20, 1), makeTS(20, 1)},
		{makeTS(20, 1), makeTS(0, 0)},
		{makeTS(20, 1), makeTS(20, 1)},
	}

	var testIdx int
	kv := NewKV(nil, newTestSender(func(call Call) {
		test := testCases[testIdx]
		if !test.expRequestTS.Equal(call.Args.Header().Txn.Timestamp) {
			t.Errorf("%d: expected ts %s got %s", testIdx, test.expRequestTS, call.Args.Header().Txn.Timestamp)
		}
		call.Reply.Header().Txn.Timestamp = test.responseTS
	}))
	txn := newTxn(kv, nil)

	for testIdx = range testCases {
		txn.kv.Sender.Send(context.Background(), Call{Args: testPutReq, Reply: &proto.PutResponse{}})
	}
}

// TestTxnResetTxnOnAbort verifies transaction is reset on abort.
func TestTxnResetTxnOnAbort(t *testing.T) {
	kv := NewKV(nil, newTestSender(func(call Call) {
		call.Reply.Header().Txn = gogoproto.Clone(call.Args.Header().Txn).(*proto.Transaction)
		call.Reply.Header().SetGoError(&proto.TransactionAbortedError{})
	}))
	txn := newTxn(kv, nil)

	txn.kv.Sender.Send(context.Background(), Call{Args: testPutReq, Reply: &proto.PutResponse{}})

	if len(txn.txn.ID) != 0 {
		t.Errorf("expected txn to be cleared")
	}
}
