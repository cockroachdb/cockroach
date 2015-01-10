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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package client

import (
	"testing"

	"code.google.com/p/go-uuid/uuid"
	"github.com/cockroachdb/cockroach/proto"
	gogoproto "github.com/gogo/protobuf/proto"
)

var (
	txnKey = proto.Key("test-txn")
	txnID  = []byte(uuid.New())
)

func makeTS(walltime int64, logical int32) proto.Timestamp {
	return proto.Timestamp{
		WallTime: walltime,
		Logical:  logical,
	}
}

type testSender struct {
	handler func(call *Call) // called after standard servicing
}

func newTestSender(handler func(*Call)) *testSender {
	return &testSender{
		handler: handler,
	}
}

func (ts *testSender) Send(call *Call) {
	header := call.Args.Header()
	header.UserPriority = gogoproto.Int32(-1)
	if header.Txn != nil && len(header.Txn.ID) == 0 {
		header.Txn.Key = txnKey
		header.Txn.ID = txnID
	}
	call.Reply.Reset()
	switch call.Method {
	case proto.Put:
		gogoproto.Merge(call.Reply, testPutResp)
	default:
		// Do nothing.
	}
	call.Reply.Header().Txn = gogoproto.Clone(call.Args.Header().Txn).(*proto.Transaction)

	if ts.handler != nil {
		ts.handler(call)
	}
}

func (ts *testSender) Close() {
}

// TestTxnSenderRequestTxnTimestamp verifies response txn timestamp is
// always upgraded on successive requests.
func TestTxnSenderRequestTxnTimestamp(t *testing.T) {
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

	testIdx := 0
	ts := newTxnSender(newTestSender(func(call *Call) {
		test := testCases[testIdx]
		if !test.expRequestTS.Equal(call.Args.Header().Txn.Timestamp) {
			t.Errorf("%d: expected ts %s got %s", testIdx, test.expRequestTS, call.Args.Header().Txn.Timestamp)
		}
		call.Reply.Header().Txn.Timestamp = test.responseTS
	}), &TransactionOptions{})

	for testIdx = range testCases {
		ts.Send(&Call{Method: proto.Put, Args: testPutReq, Reply: &proto.PutResponse{}})
	}
}

// TestTxnSenderResetTxnOnAbort verifies transaction is reset on abort.
func TestTxnSenderResetTxnOnAbort(t *testing.T) {
	ts := newTxnSender(newTestSender(func(call *Call) {
		call.Reply.Header().Txn = gogoproto.Clone(call.Args.Header().Txn).(*proto.Transaction)
		call.Reply.Header().SetGoError(&proto.TransactionAbortedError{})
	}), &TransactionOptions{})

	reply := &proto.PutResponse{}
	ts.Send(&Call{Method: proto.Put, Args: testPutReq, Reply: reply})

	if len(ts.txn.ID) != 0 {
		t.Errorf("expected txn to be cleared")
	}
}
