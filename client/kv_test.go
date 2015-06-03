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

func TestKVCallError(t *testing.T) {
	count := 0
	client := newKV(newTestSender(func(call Call) {
		count++
	}))

	testError := "test error"
	if err := client.Run(Call{Err: errors.New(testError)}); err == nil {
		t.Fatalf("expected error, but found success")
	} else if err.Error() != testError {
		t.Fatalf("expected %s, but got %s", testError, err)
	}
}

// TestKVTransactionEmptyFlush verifies that flushing without preparing any
// calls is a noop.
func TestKVTransactionEmptyFlush(t *testing.T) {
	count := 0
	client := newKV(newTestSender(func(call Call) {
		count++
	}))
	if err := client.RunTransaction(nil, func(txn *Txn) error {
		if count != 0 {
			t.Errorf("expected 0 count; got %d", count)
		}
		return txn.Flush()
	}); err != nil {
		t.Error(err)
	}
}

// TestKVClientCommandID verifies that client command ID is set
// on call.
func TestKVClientCommandID(t *testing.T) {
	count := 0
	client := newKV(newTestSender(func(call Call) {
		count++
		if call.Args.Header().CmdID.WallTime == 0 {
			t.Errorf("expected client command ID to be initialized")
		}
	}))
	if err := client.Run(Call{Args: testPutReq, Reply: &proto.PutResponse{}}); err != nil {
		t.Error(err)
	}
	if count != 1 {
		t.Errorf("expected test sender to be invoked once; got %d", count)
	}
}

// TestKVTransactionPrepareAndFlush verifies that Flush sends single prepared
// call without a batch and more than one prepared calls with a batch.
func TestKVTransactionPrepareAndFlush(t *testing.T) {
	for i := 1; i < 3; i++ {
		var calls []proto.Method
		client := newKV(newTestSender(func(call Call) {
			calls = append(calls, call.Method())
			if call.Args.Header().CmdID.WallTime == 0 {
				t.Errorf("expected batch client command ID to be initialized: %v", call.Args.Header().CmdID)
			}
		}))

		if err := client.RunTransaction(nil, func(txn *Txn) error {
			for j := 0; j < i; j++ {
				txn.Prepare(Call{Args: testPutReq, Reply: &proto.PutResponse{}})
			}
			return txn.Flush()
		}); err != nil {
			t.Error(err)
		}
		expectedCalls := []proto.Method{proto.Batch, proto.EndTransaction}
		if i == 1 {
			expectedCalls[0] = proto.Put
		}
		if !reflect.DeepEqual(expectedCalls, calls) {
			t.Errorf("expected %s, got %s", expectedCalls, calls)
		}
	}
}

// TestKVTransactionConfig verifies the proper unwrapping and
// re-wrapping of the client's sender when starting a transaction.
// Also verifies that User and UserPriority are propagated to the
// transactional client.
func TestKVTransactionConfig(t *testing.T) {
	client := newKV(newTestSender(func(call Call) {}))
	client.User = "foo"
	client.UserPriority = 101
	if err := client.RunTransaction(nil, func(txn *Txn) error {
		if txn.kv.User != client.User {
			t.Errorf("expected txn user %s; got %s", client.User, txn.kv.User)
		}
		if txn.kv.UserPriority != client.UserPriority {
			t.Errorf("expected txn user priority %d; got %d", client.UserPriority, txn.kv.UserPriority)
		}
		return nil
	}); err != nil {
		t.Errorf("unexpected error on commit: %s", err)
	}
}

// TestKVCommitReadOnlyTransaction verifies that transaction is
// committed but EndTransaction is not sent if only read-only
// operations were performed.
func TestKVCommitReadOnlyTransaction(t *testing.T) {
	var calls []proto.Method
	client := newKV(newTestSender(func(call Call) {
		calls = append(calls, call.Method())
	}))
	if err := client.RunTransaction(nil, func(txn *Txn) error {
		return txn.Run(Get(proto.Key("a")))
	}); err != nil {
		t.Errorf("unexpected error on commit: %s", err)
	}
	expectedCalls := []proto.Method{proto.Get}
	if !reflect.DeepEqual(expectedCalls, calls) {
		t.Errorf("expected %s, got %s", expectedCalls, calls)
	}
}

// TestKVCommitMutatingTransaction verifies that transaction is committed
// upon successful invocation of the retryable func.
func TestKVCommitMutatingTransaction(t *testing.T) {
	var calls []proto.Method
	client := newKV(newTestSender(func(call Call) {
		calls = append(calls, call.Method())
		if et, ok := call.Args.(*proto.EndTransactionRequest); ok && !et.Commit {
			t.Errorf("expected commit to be true; got %t", et.Commit)
		}
	}))
	if err := client.RunTransaction(nil, func(txn *Txn) error {
		return txn.Run(Put(proto.Key("a"), nil))
	}); err != nil {
		t.Errorf("unexpected error on commit: %s", err)
	}
	expectedCalls := []proto.Method{proto.Put, proto.EndTransaction}
	if !reflect.DeepEqual(expectedCalls, calls) {
		t.Errorf("expected %s, got %s", expectedCalls, calls)
	}
}

// TestKVCommitTransactionOnce verifies that if the transaction is
// ended explicitly in the retryable func, it is not automatically
// ended a second time at completion of retryable func.
func TestKVCommitTransactionOnce(t *testing.T) {
	count := 0
	client := newKV(newTestSender(func(call Call) {
		count++
	}))
	if err := client.RunTransaction(nil, func(txn *Txn) error {
		if err := txn.Run(Call{
			Args:  &proto.EndTransactionRequest{Commit: true},
			Reply: &proto.EndTransactionResponse{},
		}); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Errorf("unexpected error on commit: %s", err)
	}
	if count != 1 {
		t.Errorf("expected single invocation of EndTransaction; got %d", count)
	}
}

// TestKVAbortReadOnlyTransaction verifies that transaction is aborted
// upon failed invocation of the retryable func.
func TestKVAbortReadOnlyTransaction(t *testing.T) {
	client := newKV(newTestSender(func(call Call) {
		if _, ok := call.Args.(*proto.EndTransactionRequest); ok {
			t.Errorf("did not expect EndTransaction")
		}
	}))
	if err := client.RunTransaction(nil, func(txn *Txn) error {
		return errors.New("foo")
	}); err == nil {
		t.Error("expected error on abort")
	}
}

// TestKVAbortMutatingTransaction verifies that transaction is aborted
// upon failed invocation of the retryable func.
func TestKVAbortMutatingTransaction(t *testing.T) {
	var calls []proto.Method
	client := newKV(newTestSender(func(call Call) {
		calls = append(calls, call.Method())
		if et, ok := call.Args.(*proto.EndTransactionRequest); ok && et.Commit {
			t.Errorf("expected commit to be false; got %t", et.Commit)
		}
	}))

	if err := client.RunTransaction(nil, func(txn *Txn) error {
		if err := txn.Run(Put(proto.Key("a"), nil)); err != nil {
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

// TestKVRunTransactionRetryOnErrors verifies that the transaction
// is retried on the correct errors.
func TestKVRunTransactionRetryOnErrors(t *testing.T) {
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
		client := newKV(newTestSender(func(call Call) {
			if _, ok := call.Args.(*proto.PutRequest); ok {
				count++
				if count == 1 {
					call.Reply.Header().SetGoError(test.err)
				}
			}
		}))
		client.TxnRetryOptions.Backoff = 1 * time.Millisecond
		err := client.RunTransaction(nil, func(txn *Txn) error {
			reply := &proto.PutResponse{}
			return client.Run(Call{Args: testPutReq, Reply: reply})
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
