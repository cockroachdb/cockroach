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
	"bytes"
	"testing"
	"time"

	"code.google.com/p/go-uuid/uuid"
	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/proto"
)

var (
	txnID = append([]byte("test-txn"), []byte(uuid.New())...)
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
	call.Args.Header().UserPriority = gogoproto.Int32(-1)
	call.Reply.Reset()
	switch call.Method {
	case proto.Put:
		gogoproto.Merge(call.Reply, testPutResp)
	case proto.BeginTransaction:
		gogoproto.Merge(call.Reply, &proto.BeginTransactionResponse{
			Txn: &proto.Transaction{
				ID:           txnID,
				Timestamp:    makeTS(0, 0),
				MaxTimestamp: makeTS(1000, 0),
			},
		})
		return
	default:
		// Do nothing.
	}

	if ts.handler != nil {
		ts.handler(call)
	}
}

func (ts *testSender) Close() {
}

// TestTxnSenderBeginTxn verifies a BeginTransaction is issued as soon
// as a request is received using the request key as base key.
func TestTxnSenderBeginTxn(t *testing.T) {
	ts := newTxnSender(newTestSender(nil), nil, &TransactionOptions{})
	ts.Send(&Call{Method: proto.Put, Args: testPutReq, Reply: &proto.PutResponse{}})
	if ts.txn == nil || !bytes.Equal(ts.txn.ID, txnID) {
		t.Errorf("expected sender to have transaction initialized with ID %s: %s", txnID, ts.txn)
	}
}

// TestTxnSenderNewClientCmdID verifies new client command IDs are
// created on each retry due to write intent errors.
func TestTxnSenderNewClientCmdID(t *testing.T) {
	TxnRetryOptions.Backoff = 1 * time.Millisecond

	// Allow three retries and verify new client command ID on each.
	count := 0
	cmdIDs := map[string]struct{}{}
	ts := newTxnSender(newTestSender(func(call *Call) {
		count++
		if _, ok := cmdIDs[call.Args.Header().CmdID.String()]; ok {
			t.Errorf("expected different client command IDs on each invocation")
		} else {
			cmdIDs[call.Args.Header().CmdID.String()] = struct{}{}
		}
		if count == 3 {
			return
		}
		call.Reply.Header().SetGoError(&proto.WriteIntentError{})
	}), nil, &TransactionOptions{})
	ts.Send(&Call{Method: proto.Put, Args: testPutReq, Reply: &proto.PutResponse{}})
	if count != 3 {
		t.Errorf("expected three retries; got %d", count)
	}
	if len(cmdIDs) != 3 {
		t.Errorf("expected three distinct client command IDs; got %v", cmdIDs)
	}
}

// TestTxnSenderEndTxn verifies that request key is set to the txn ID
// on a call to EndTransaction.
func TestTxnSenderEndTxn(t *testing.T) {
	ts := newTxnSender(newTestSender(func(call *Call) {
		if !call.Args.Header().Key.Equal(txnID) {
			t.Errorf("expected request key to be %q; got %q", txnID, call.Args.Header().Key)
		}
	}), nil, &TransactionOptions{})
	ts.Send(&Call{
		Method: proto.EndTransaction,
		Args:   &proto.EndTransactionRequest{Commit: true},
		Reply:  &proto.EndTransactionResponse{},
	})
	if !ts.txnEnd {
		t.Errorf("expected txnEnd to be true")
	}
}

// TestTxnSenderTransactionalVsNon verifies that non-transactional
// requests (in particular InternalResolveIntent) are passed directly
// through to the wrapped sender, and transactional requests get a
// txn set as expected.
func TestTxnSenderNonTransactional(t *testing.T) {
	for method := range proto.AllMethods {
		isTransactional := proto.IsTransactional(method) || method == proto.EndTransaction
		ts := newTxnSender(newTestSender(func(call *Call) {
			if !isTransactional {
				t.Errorf("%s: should not have received this method type in txn", method)
			}
		}), nil, &TransactionOptions{})
		args, reply, err := proto.CreateArgsAndReply(method)
		if err != nil {
			t.Errorf("%s: unexpected error creating args and reply: %s", method, err)
		}
		ts.Send(&Call{Method: method, Args: args, Reply: reply})
		if isTransactional && reply.Header().GoError() != nil {
			t.Errorf("%s: failed to create args and reply: %s", method, reply.Header().GoError())
		} else if !isTransactional && reply.Header().GoError() == nil {
			t.Errorf("%s: expected an error trying to send non-transactional request through txn", method)
		}
	}
}

// TestTxnSenderRequestTimestamp verifies timestamp is always
// upgraded on successive requests.
func TestTxnSenderRequestTimestamp(t *testing.T) {
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
		if !test.expRequestTS.Equal(call.Args.Header().Timestamp) {
			t.Errorf("%d: expected ts %s got %s", testIdx, test.expRequestTS, call.Args.Header().Timestamp)
		}
		call.Reply.Header().Timestamp = test.responseTS
	}), nil, &TransactionOptions{})

	for testIdx = range testCases {
		ts.Send(&Call{Method: proto.Put, Args: testPutReq, Reply: &proto.PutResponse{}})
	}
}

// TestTxnSenderReadWithinUncertaintyIntervalError verifies no txn
// abort, and timestamp is upgraded to 1 + the existing timestamp.
func TestTxnSenderReadWithinUncertaintyIntervalError(t *testing.T) {
	TxnRetryOptions.Backoff = 1 * time.Millisecond

	existingTS := makeTS(10, 10)
	count := 0
	ts := newTxnSender(newTestSender(func(call *Call) {
		count++
		call.Reply.Header().SetGoError(&proto.ReadWithinUncertaintyIntervalError{ExistingTimestamp: existingTS})
	}), nil, &TransactionOptions{})
	reply := &proto.PutResponse{}
	ts.Send(&Call{Method: proto.Put, Args: testPutReq, Reply: reply})
	if count != 1 {
		t.Errorf("expected no retries; got %d", count)
	}
	if _, ok := reply.GoError().(*proto.ReadWithinUncertaintyIntervalError); !ok {
		t.Fatalf("expected read within uncertainty interval error; got %s", reply.GoError())
	}
	if !bytes.Equal(ts.txn.ID, txnID) {
		t.Errorf("expected txn restart, but got abort/retry: %s", ts.txn)
	}
	if !ts.timestamp.Equal(makeTS(10, 11)) {
		t.Errorf("expected timestamp to be %s + 1; got %s", existingTS, ts.timestamp)
	}
}

// TestTxnSenderTransactionAbortedError verifies the transaction is
// aborted and the new txn given a new ID. Timestamp should be set
// to the aborted timestamp and priority upgraded.
func TestTxnSenderTransactionAbortedError(t *testing.T) {
	TxnRetryOptions.Backoff = 1 * time.Millisecond

	count := 0
	abortTS := makeTS(20, 10)
	abortPri := int32(10)
	ts := newTxnSender(newTestSender(func(call *Call) {
		count++
		call.Reply.Header().SetGoError(&proto.TransactionAbortedError{
			Txn: proto.Transaction{Timestamp: abortTS, Priority: abortPri},
		})
	}), nil, &TransactionOptions{})
	reply := &proto.PutResponse{}
	ts.Send(&Call{Method: proto.Put, Args: testPutReq, Reply: reply})
	if count != 1 {
		t.Errorf("expected no retries; got %d", count)
	}
	if _, ok := reply.GoError().(*proto.TransactionAbortedError); !ok {
		t.Fatalf("expected txn aborted error; got %s", reply.GoError())
	}
	if ts.txn != nil {
		t.Errorf("expected txn abort/retry to clear transaction")
	}
	if ts.minPriority != abortPri {
		t.Errorf("expected txn sender minPriority to be upgraded to abort priority %d; got %d", abortPri, ts.minPriority)
	}
	if !ts.timestamp.Equal(abortTS) {
		t.Errorf("expected timestamp to be %s; got %s", abortTS, ts.timestamp)
	}
}

// TestTxnSenderTransactionPushError verifies the transaction is
// restarted (not aborted), with timestamp increased to 1 + pushee's
// timestamp and priority upgraded to pushee's priority - 1.
func TestTxnSenderTransactionPushError(t *testing.T) {
	TxnRetryOptions.Backoff = 1 * time.Millisecond

	pusheeTS := makeTS(10, 10)
	pusheePri := int32(10)
	count := 0
	ts := newTxnSender(newTestSender(func(call *Call) {
		count++
		call.Reply.Header().SetGoError(&proto.TransactionPushError{
			PusheeTxn: proto.Transaction{Timestamp: pusheeTS, Priority: pusheePri},
		})
	}), nil, &TransactionOptions{})
	reply := &proto.PutResponse{}
	ts.Send(&Call{Method: proto.Put, Args: testPutReq, Reply: reply})
	if count != 1 {
		t.Errorf("expected no retries; got %d", count)
	}
	if _, ok := reply.GoError().(*proto.TransactionPushError); !ok {
		t.Fatalf("expected txn push error; got %s", reply.GoError())
	}
	if !bytes.Equal(ts.txn.ID, txnID) {
		t.Errorf("expected txn restart, but got abort/retry: %s", ts.txn)
	}
	if ts.txn.Priority != pusheePri-1 {
		t.Errorf("expected priority to be upgraded to pushee priority %d - 1; got %d", pusheePri, ts.txn.Priority)
	}
	if !ts.timestamp.Equal(makeTS(10, 11)) {
		t.Errorf("expected timestamp to be %s + 1; got %s", pusheeTS, ts.timestamp)
	}
}

// TestTxnSenderTransactionRetryError verifies transaction is
// restarted (not aborted), with timestamp increased, and priority
// upgraded.
func TestTxnSenderTransactionRetryError(t *testing.T) {
	TxnRetryOptions.Backoff = 1 * time.Millisecond

	newTS := makeTS(10, 10)
	newPri := int32(10)
	count := 0
	ts := newTxnSender(newTestSender(func(call *Call) {
		count++
		call.Reply.Header().SetGoError(&proto.TransactionRetryError{
			Txn: proto.Transaction{Timestamp: newTS, Priority: newPri},
		})
	}), nil, &TransactionOptions{})
	reply := &proto.PutResponse{}
	ts.Send(&Call{Method: proto.Put, Args: testPutReq, Reply: reply})
	if count != 1 {
		t.Errorf("expected no retries; got %d", count)
	}
	if _, ok := reply.GoError().(*proto.TransactionRetryError); !ok {
		t.Fatalf("expected txn retry error; got %s", reply.GoError())
	}
	if !bytes.Equal(ts.txn.ID, txnID) {
		t.Errorf("expected txn restart, but got abort/retry: %s", ts.txn)
	}
	if ts.txn.Priority != newPri {
		t.Errorf("expected priority to be upgraded to new priority %d; got %d", newPri, ts.txn.Priority)
	}
	if !ts.timestamp.Equal(newTS) {
		t.Errorf("expected timestamp to be %s; got %s", newTS, ts.timestamp)
	}
}

// TestTxnSenderWriteTooOldError verifies immediate retry of the
// operation using a timestamp one greater than existing timestamp.
func TestTxnSenderWriteTooOldError(t *testing.T) {
	TxnRetryOptions.Backoff = 1 * time.Millisecond

	existingTS := makeTS(10, 10)
	count := 0
	ts := newTxnSender(newTestSender(func(call *Call) {
		count++
		// Only return write too old on first request.
		if count == 1 {
			call.Reply.Header().SetGoError(&proto.WriteTooOldError{ExistingTimestamp: existingTS})
			return
		}
		if !call.Args.Header().Timestamp.Equal(makeTS(10, 11)) {
			t.Errorf("expected args timestamp to be %s + 1; got %s", existingTS, call.Args.Header().Timestamp)
		}
	}), nil, &TransactionOptions{})
	reply := &proto.PutResponse{}
	ts.Send(&Call{Method: proto.Put, Args: testPutReq, Reply: reply})
	if count != 2 {
		t.Errorf("expected one retry; got %d", count)
	}
	if reply.GoError() != nil {
		t.Fatalf("expected no error after retry; got %s", reply.GoError())
	}
	if !bytes.Equal(ts.txn.ID, txnID) {
		t.Errorf("expected no txn abort/retry: %s", ts.txn)
	}
	if !ts.timestamp.Equal(makeTS(10, 11)) {
		t.Errorf("expected timestamp to be %s + 1; got %s", existingTS, ts.timestamp)
	}
}

// TestTxnSenderWriteIntentError verifies that the send is retried
// on write intent errors.
func TestTxnSenderWriteIntentError(t *testing.T) {
	TxnRetryOptions.Backoff = 1 * time.Millisecond

	for _, resolved := range []bool{true, false} {
		existingTS := makeTS(10, 10)
		existingPri := int32(10)
		count := 0
		ts := newTxnSender(newTestSender(func(call *Call) {
			count++
			// Only return write intent on first request.
			if count == 1 {
				call.Reply.Header().SetGoError(&proto.WriteIntentError{
					Txn:      proto.Transaction{Timestamp: existingTS, Priority: existingPri},
					Resolved: resolved,
				})
				return
			}
			expTS := makeTS(0, 0)
			if !resolved {
				expTS = existingTS
				expTS.Logical++
			}
			if !call.Args.Header().Timestamp.Equal(expTS) {
				t.Errorf("resolved? %t: expected args timestamp to be %s; got %s", resolved, expTS, call.Args.Header().Timestamp)
			}
		}), nil, &TransactionOptions{})
		reply := &proto.PutResponse{}
		ts.Send(&Call{Method: proto.Put, Args: testPutReq, Reply: reply})
		if count != 2 {
			t.Errorf("expected one retry; got %d", count)
		}
		if reply.GoError() != nil {
			t.Fatalf("expected no error after retry; got %s", reply.GoError())
		}
		if !bytes.Equal(ts.txn.ID, txnID) {
			t.Errorf("expected no txn abort/retry: %s", ts.txn)
		}
		expTS := testTS
		expPri := int32(0)
		if !resolved {
			expTS = existingTS
			expTS.Logical++
			expPri = existingPri - 1
		}
		if ts.txn.Priority != expPri {
			t.Errorf("resolved? %t: expected priority to be %d; got %d", resolved, expPri, ts.txn.Priority)
		}
		if !ts.timestamp.Equal(expTS) {
			t.Errorf("resolved? %t: expected timestamp to be %s; got %s", resolved, expTS, ts.timestamp)
		}
	}
}
