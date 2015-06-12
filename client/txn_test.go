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
// Author: Peter Mattis (peter@cockroachlabs.com)

package client

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	gogoproto "github.com/gogo/protobuf/proto"
)

func newDB(sender Sender) *DB {
	return &DB{
		Sender:          sender,
		txnRetryOptions: DefaultTxnRetryOptions,
	}
}

func newTestSender(handler func(Call)) SenderFunc {
	txnKey := proto.Key("test-txn")
	txnID := []byte(util.NewUUID4())

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
	makeTS := func(walltime int64, logical int32) proto.Timestamp {
		return proto.Timestamp{
			WallTime: walltime,
			Logical:  logical,
		}
	}

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
	db := newDB(newTestSender(func(call Call) {
		test := testCases[testIdx]
		if !test.expRequestTS.Equal(call.Args.Header().Txn.Timestamp) {
			t.Errorf("%d: expected ts %s got %s", testIdx, test.expRequestTS, call.Args.Header().Txn.Timestamp)
		}
		call.Reply.Header().Txn.Timestamp = test.responseTS
	}))

	txn := newTxn(*db, 0)

	for testIdx = range testCases {
		txn.db.Sender.Send(context.Background(), Call{Args: testPutReq, Reply: &proto.PutResponse{}})
	}
}

// TestTxnResetTxnOnAbort verifies transaction is reset on abort.
func TestTxnResetTxnOnAbort(t *testing.T) {
	db := newDB(newTestSender(func(call Call) {
		call.Reply.Header().Txn = gogoproto.Clone(call.Args.Header().Txn).(*proto.Transaction)
		call.Reply.Header().SetGoError(&proto.TransactionAbortedError{})
	}))

	txn := newTxn(*db, 0)
	txn.db.Sender.Send(context.Background(), Call{Args: testPutReq, Reply: &proto.PutResponse{}})

	if len(txn.txn.ID) != 0 {
		t.Errorf("expected txn to be cleared")
	}
}

// TestTransactionConfig verifies the proper unwrapping and
// re-wrapping of the client's sender when starting a transaction.
// Also verifies that User and UserPriority are propagated to the
// transactional client.
func TestTransactionConfig(t *testing.T) {
	db := newDB(newTestSender(func(call Call) {}))
	db.user = "foo"
	db.userPriority = 101
	if err := db.Txn(func(txn *Txn) error {
		if txn.db.user != db.user {
			t.Errorf("expected txn user %s; got %s", db.user, txn.db.user)
		}
		if txn.db.userPriority != db.userPriority {
			t.Errorf("expected txn user priority %d; got %d", db.userPriority, txn.db.userPriority)
		}
		return nil
	}); err != nil {
		t.Errorf("unexpected error on commit: %s", err)
	}
}

// TestCommitReadOnlyTransaction verifies that transaction is
// committed but EndTransaction is not sent if only read-only
// operations were performed.
func TestCommitReadOnlyTransaction(t *testing.T) {
	var calls []proto.Method
	db := newDB(newTestSender(func(call Call) {
		calls = append(calls, call.Method())
	}))
	if err := db.Txn(func(txn *Txn) error {
		_, err := txn.Get("a")
		return err
	}); err != nil {
		t.Errorf("unexpected error on commit: %s", err)
	}
	expectedCalls := []proto.Method{proto.Get}
	if !reflect.DeepEqual(expectedCalls, calls) {
		t.Errorf("expected %s, got %s", expectedCalls, calls)
	}
}

// TestCommitMutatingTransaction verifies that transaction is committed
// upon successful invocation of the retryable func.
func TestCommitMutatingTransaction(t *testing.T) {
	var calls []proto.Method
	db := newDB(newTestSender(func(call Call) {
		calls = append(calls, call.Method())
		if et, ok := call.Args.(*proto.EndTransactionRequest); ok && !et.Commit {
			t.Errorf("expected commit to be true; got %t", et.Commit)
		}
	}))
	if err := db.Txn(func(txn *Txn) error {
		return txn.Put("a", "b")
	}); err != nil {
		t.Errorf("unexpected error on commit: %s", err)
	}
	expectedCalls := []proto.Method{proto.Put, proto.EndTransaction}
	if !reflect.DeepEqual(expectedCalls, calls) {
		t.Errorf("expected %s, got %s", expectedCalls, calls)
	}
}

// TestCommitTransactionOnce verifies that if the transaction is
// ended explicitly in the retryable func, it is not automatically
// ended a second time at completion of retryable func.
func TestCommitTransactionOnce(t *testing.T) {
	count := 0
	db := newDB(newTestSender(func(call Call) {
		count++
	}))
	if err := db.Txn(func(txn *Txn) error {
		return txn.Commit(&Batch{})
	}); err != nil {
		t.Errorf("unexpected error on commit: %s", err)
	}
	if count != 1 {
		t.Errorf("expected single invocation of EndTransaction; got %d", count)
	}
}

// TestAbortReadOnlyTransaction verifies that transaction is aborted
// upon failed invocation of the retryable func.
func TestAbortReadOnlyTransaction(t *testing.T) {
	db := newDB(newTestSender(func(call Call) {
		if _, ok := call.Args.(*proto.EndTransactionRequest); ok {
			t.Errorf("did not expect EndTransaction")
		}
	}))
	if err := db.Txn(func(txn *Txn) error {
		return errors.New("foo")
	}); err == nil {
		t.Error("expected error on abort")
	}
}

// TestAbortMutatingTransaction verifies that transaction is aborted
// upon failed invocation of the retryable func.
func TestAbortMutatingTransaction(t *testing.T) {
	var calls []proto.Method
	db := newDB(newTestSender(func(call Call) {
		calls = append(calls, call.Method())
		if et, ok := call.Args.(*proto.EndTransactionRequest); ok && et.Commit {
			t.Errorf("expected commit to be false; got %t", et.Commit)
		}
	}))

	if err := db.Txn(func(txn *Txn) error {
		if err := txn.Put("a", "b"); err != nil {
			return err
		}
		return errors.New("foo")
	}); err == nil {
		t.Error("expected error on abort")
	}
	expectedCalls := []proto.Method{proto.Put, proto.EndTransaction}
	if !reflect.DeepEqual(expectedCalls, calls) {
		t.Errorf("expected %s, got %s", expectedCalls, calls)
	}
}

// TestRunTransactionRetryOnErrors verifies that the transaction
// is retried on the correct errors.
func TestRunTransactionRetryOnErrors(t *testing.T) {
	testCases := []struct {
		err   error
		retry bool // Expect retry?
	}{
		{&proto.ReadWithinUncertaintyIntervalError{}, true},
		{&proto.TransactionAbortedError{}, true},
		{&proto.TransactionPushError{}, true},
		{&proto.TransactionRetryError{}, true},
		{&proto.Error{}, false},
		{&proto.RangeNotFoundError{}, false},
		{&proto.RangeKeyMismatchError{}, false},
		{&proto.TransactionStatusError{}, false},
	}

	for i, test := range testCases {
		count := 0
		db := newDB(newTestSender(func(call Call) {
			if _, ok := call.Args.(*proto.PutRequest); ok {
				count++
				if count == 1 {
					call.Reply.Header().SetGoError(test.err)
				}
			}
		}))
		db.txnRetryOptions.BackOff.InitialInterval = 1 * time.Millisecond
		err := db.Txn(func(txn *Txn) error {
			return txn.Put("a", "b")
		})
		if test.retry {
			if count != 2 {
				t.Errorf("%d: expected one retry; got %d", i, count)
			}
			if err != nil {
				t.Errorf("%d: expected success on retry; got %s", i, err)
			}
		} else {
			if count != 1 {
				t.Errorf("%d: expected no retries; got %d", i, count)
			}
			if reflect.TypeOf(err) != reflect.TypeOf(test.err) {
				t.Errorf("%d: expected error of type %T; got %T", i, test.err, err)
			}
		}
	}
}
