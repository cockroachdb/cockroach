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

	"github.com/cockroachdb/cockroach/proto"
)

func TestSingleCallSenderSend(t *testing.T) {
	ts := newSingleCallSender(newTestSender(nil), nil)
	reply := &proto.PutResponse{}
	ts.Send(&Call{Method: proto.Put, Args: testPutReq, Reply: reply})
	if reply.GoError() != nil {
		t.Errorf("unexpected error: %s", reply.GoError())
	}
}

//////////////////////////////////////////
// TODO(spencer): fix these three tests; they're just placeholders!!!
//////////////////////////////////////////

// TestSingleCallSenderTransactionPushError verifies .
func TestSingleCallSenderTransactionPushError(t *testing.T) {
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

// TestSingleCallSenderWriteTooOldError verifies .
func TestSingleCallSenderWriteTooOldError(t *testing.T) {
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

// TestSingleCallSenderWriteIntentError verifies .
func TestSingleCallSenderWriteIntentError(t *testing.T) {
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
