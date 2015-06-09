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
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
)

func newDB(sender Sender) *DB {
	return &DB{
		kv: kv{
			Sender:          sender,
			txnRetryOptions: DefaultTxnRetryOptions,
		},
	}
}

func TestCallError(t *testing.T) {
	count := 0
	db := newDB(newTestSender(func(call Call) {
		count++
	}))

	testError := "test error"
	if err := db.send(Call{Err: errors.New(testError)}); err == nil {
		t.Fatalf("expected error, but found success")
	} else if err.Error() != testError {
		t.Fatalf("expected %s, but got %s", testError, err)
	}
}

// TestClientCommandID verifies that client command ID is set
// on call.
func TestClientCommandID(t *testing.T) {
	count := 0
	db := newDB(newTestSender(func(call Call) {
		count++
		if call.Args.Header().CmdID.WallTime == 0 {
			t.Errorf("expected client command ID to be initialized")
		}
	}))
	if err := db.Put("a", "b"); err != nil {
		t.Error(err)
	}
	if count != 1 {
		t.Errorf("expected test sender to be invoked once; got %d", count)
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
	if err := db.Tx(func(tx *Tx) error {
		if tx.txn.kv.user != db.user {
			t.Errorf("expected txn user %s; got %s", db.user, tx.txn.kv.user)
		}
		if tx.txn.kv.userPriority != db.userPriority {
			t.Errorf("expected txn user priority %d; got %d", db.userPriority, tx.txn.kv.userPriority)
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
	if err := db.Tx(func(tx *Tx) error {
		_, err := tx.Get("a")
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
	if err := db.Tx(func(tx *Tx) error {
		return tx.Put("a", "b")
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
	if err := db.Tx(func(tx *Tx) error {
		return tx.Commit(&Batch{})
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
	if err := db.Tx(func(tx *Tx) error {
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

	if err := db.Tx(func(tx *Tx) error {
		if err := tx.Put("a", "b"); err != nil {
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
		db.txnRetryOptions.Backoff = 1 * time.Millisecond
		err := db.Tx(func(tx *Tx) error {
			return tx.Put("a", "b")
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
