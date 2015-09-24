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
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/uuid"
	gogoproto "github.com/gogo/protobuf/proto"
)

var (
	testKey     = proto.Key("a")
	testTS      = proto.Timestamp{WallTime: 1, Logical: 1}
	testPutResp = &proto.PutResponse{ResponseHeader: proto.ResponseHeader{Timestamp: testTS}}
)

func newDB(sender Sender) *DB {
	return &DB{
		sender:          sender,
		txnRetryOptions: DefaultTxnRetryOptions,
	}
}

func newTestSender(pre, post func(proto.BatchRequest) (*proto.BatchResponse, *proto.Error)) SenderFunc {
	txnKey := proto.Key("test-txn")
	txnID := []byte(uuid.NewUUID4())

	return func(_ context.Context, ba proto.BatchRequest) (*proto.BatchResponse, *proto.Error) {
		ba.UserPriority = gogoproto.Int32(-1)
		if ba.Txn != nil && len(ba.Txn.ID) == 0 {
			ba.Txn.Key = txnKey
			ba.Txn.ID = txnID
		}

		var br *proto.BatchResponse
		var pErr *proto.Error
		if pre != nil {
			br, pErr = pre(ba)
		} else {
			br = ba.CreateReply().(*proto.BatchResponse)
		}
		if pErr != nil {
			return nil, pErr
		}
		var writing bool
		status := proto.PENDING
		if _, ok := ba.GetArg(proto.Put); ok {
			br.Add(gogoproto.Clone(testPutResp).(proto.Response))
			writing = true
		}
		if args, ok := ba.GetArg(proto.EndTransaction); ok {
			et := args.(*proto.EndTransactionRequest)
			writing = true
			if et.Commit {
				status = proto.COMMITTED
			} else {
				status = proto.ABORTED
			}
		}
		br.Txn = gogoproto.Clone(ba.Txn).(*proto.Transaction)
		if br.Txn != nil && pErr == nil {
			br.Txn.Writing = writing
			br.Txn.Status = status
		}

		if post != nil {
			br, pErr = post(ba)
		}
		return br, pErr
	}
}

func testPut() proto.BatchRequest {
	var ba proto.BatchRequest
	ba.Timestamp = testTS
	put := &proto.PutRequest{}
	put.Key = testKey
	ba.Add(put)
	return ba
}

// TestTxnRequestTxnTimestamp verifies response txn timestamp is
// always upgraded on successive requests.
func TestTxnRequestTxnTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)
	makeTS := func(walltime int64, logical int32) proto.Timestamp {
		return proto.ZeroTimestamp.Add(walltime, logical)
	}
	ba := testPut()

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
	db := NewDB(newTestSender(nil, func(ba proto.BatchRequest) (*proto.BatchResponse, *proto.Error) {
		test := testCases[testIdx]
		if !test.expRequestTS.Equal(ba.Txn.Timestamp) {
			return nil, proto.NewError(util.Errorf("%d: expected ts %s got %s", testIdx, test.expRequestTS, ba.Txn.Timestamp))
		}
		br := &proto.BatchResponse{}
		br.Txn = &proto.Transaction{}
		br.Txn.Update(ba.Txn) // copy
		br.Txn.Timestamp = test.responseTS
		return br, nil
	}))

	txn := NewTxn(*db)

	for testIdx = range testCases {
		if _, pErr := txn.db.sender.Send(context.Background(), ba); pErr != nil {
			t.Fatal(pErr)
		}
	}
}

// TestTxnResetTxnOnAbort verifies transaction is reset on abort.
func TestTxnResetTxnOnAbort(t *testing.T) {
	defer leaktest.AfterTest(t)
	db := newDB(newTestSender(func(ba proto.BatchRequest) (*proto.BatchResponse, *proto.Error) {
		return nil, proto.NewError(&proto.TransactionAbortedError{
			Txn: *gogoproto.Clone(ba.Txn).(*proto.Transaction),
		})
	}, nil))

	txn := NewTxn(*db)
	_, pErr := txn.db.sender.Send(context.Background(), testPut())
	if _, ok := pErr.GoError().(*proto.TransactionAbortedError); !ok {
		t.Fatalf("expected TransactionAbortedError, got %v", pErr)
	}

	if len(txn.Proto.ID) != 0 {
		t.Errorf("expected txn to be cleared")
	}
}

// TestTransactionConfig verifies the proper unwrapping and
// re-wrapping of the client's sender when starting a transaction.
// Also verifies that the UserPriority is propagated to the
// transactional client.
func TestTransactionConfig(t *testing.T) {
	defer leaktest.AfterTest(t)
	db := NewDB(newTestSender(nil, nil))
	db.userPriority = 101
	if err := db.Txn(func(txn *Txn) error {
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
	defer leaktest.AfterTest(t)
	var calls []proto.Method
	db := newDB(newTestSender(func(ba proto.BatchRequest) (*proto.BatchResponse, *proto.Error) {
		calls = append(calls, ba.Methods()...)
		return ba.CreateReply().(*proto.BatchResponse), nil
	}, nil))
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

// TestCommitReadOnlyTransactionExplicit verifies that a read-only
// transaction with an explicit EndTransaction call does not send
// that call.
func TestCommitReadOnlyTransactionExplicit(t *testing.T) {
	defer leaktest.AfterTest(t)
	for _, withGet := range []bool{true, false} {
		var calls []proto.Method
		db := newDB(newTestSender(func(ba proto.BatchRequest) (*proto.BatchResponse, *proto.Error) {
			calls = append(calls, ba.Methods()...)
			return ba.CreateReply().(*proto.BatchResponse), nil
		}, nil))
		if err := db.Txn(func(txn *Txn) error {
			b := &Batch{}
			if withGet {
				b.Get("foo")
			}
			return txn.CommitInBatch(b)
		}); err != nil {
			t.Errorf("unexpected error on commit: %s", err)
		}
		expectedCalls := []proto.Method(nil)
		if withGet {
			expectedCalls = append(expectedCalls, proto.Get)
		}
		if !reflect.DeepEqual(expectedCalls, calls) {
			t.Errorf("expected %s, got %s", expectedCalls, calls)
		}
	}
}

// TestCommitMutatingTransaction verifies that transaction is committed
// upon successful invocation of the retryable func.
func TestCommitMutatingTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)
	var calls []proto.Method
	db := newDB(newTestSender(func(ba proto.BatchRequest) (*proto.BatchResponse, *proto.Error) {
		calls = append(calls, ba.Methods()...)
		if et, ok := ba.GetArg(proto.EndTransaction); ok && !et.(*proto.EndTransactionRequest).Commit {
			t.Errorf("expected commit to be true")
		}
		return ba.CreateReply().(*proto.BatchResponse), nil
	}, nil))
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
	defer leaktest.AfterTest(t)
	count := 0
	db := NewDB(newTestSender(func(ba proto.BatchRequest) (*proto.BatchResponse, *proto.Error) {
		count++
		return ba.CreateReply().(*proto.BatchResponse), nil
	}, nil))
	if err := db.Txn(func(txn *Txn) error {
		b := &Batch{}
		b.Put("z", "adding a write exposed a bug in #1882")
		return txn.CommitInBatch(b)
	}); err != nil {
		t.Errorf("unexpected error on commit: %s", err)
	}
	if count != 1 {
		t.Errorf("expected single Batch, got %d sent calls", count)
	}
}

// TestAbortReadOnlyTransaction verifies that aborting a read-only
// transaction does not prompt an EndTransaction call.
func TestAbortReadOnlyTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)
	db := newDB(newTestSender(func(ba proto.BatchRequest) (*proto.BatchResponse, *proto.Error) {
		if _, ok := ba.GetArg(proto.EndTransaction); ok {
			t.Errorf("did not expect EndTransaction")
		}
		return ba.CreateReply().(*proto.BatchResponse), nil
	}, nil))
	if err := db.Txn(func(txn *Txn) error {
		return errors.New("foo")
	}); err == nil {
		t.Error("expected error on abort")
	}
}

// TestEndWriteRestartReadOnlyTransaction verifies that if
// a transaction writes, then restarts and turns read-only,
// an explicit EndTransaction call is still sent if retry-
// able didn't, regardless of whether there is an error
// or not.
func TestEndWriteRestartReadOnlyTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)
	for _, success := range []bool{true, false} {
		expCalls := []proto.Method{proto.Put, proto.EndTransaction}
		var calls []proto.Method
		db := newDB(newTestSender(func(ba proto.BatchRequest) (*proto.BatchResponse, *proto.Error) {
			calls = append(calls, ba.Methods()...)
			return ba.CreateReply().(*proto.BatchResponse), nil
		}, nil))
		ok := false
		if err := db.Txn(func(txn *Txn) error {
			if !ok {
				if err := txn.Put("consider", "phlebas"); err != nil {
					t.Fatal(err)
				}
				ok = true
				return &proto.TransactionRetryError{} // immediate txn retry
			}
			if !success {
				return errors.New("aborting on purpose")
			}
			return nil
		}); err == nil != success {
			t.Errorf("expected error: %t, got error: %v", !success, err)
		}
		if !reflect.DeepEqual(expCalls, calls) {
			t.Fatalf("expected %v, got %v", expCalls, calls)
		}
	}
}

// TestAbortMutatingTransaction verifies that transaction is aborted
// upon failed invocation of the retryable func.
func TestAbortMutatingTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)
	var calls []proto.Method
	db := newDB(newTestSender(func(ba proto.BatchRequest) (*proto.BatchResponse, *proto.Error) {
		calls = append(calls, ba.Methods()...)
		if et, ok := ba.GetArg(proto.EndTransaction); ok && et.(*proto.EndTransactionRequest).Commit {
			t.Errorf("expected commit to be false")
		}
		return ba.CreateReply().(*proto.BatchResponse), nil
	}, nil))

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
	defer leaktest.AfterTest(t)
	testCases := []struct {
		err   error
		retry bool // Expect retry?
	}{
		{&proto.ReadWithinUncertaintyIntervalError{}, true},
		{&proto.TransactionAbortedError{}, true},
		{&proto.TransactionPushError{}, true},
		{&proto.TransactionRetryError{}, true},
		{&proto.RangeNotFoundError{}, false},
		{&proto.RangeKeyMismatchError{}, false},
		{&proto.TransactionStatusError{}, false},
	}

	for i, test := range testCases {
		count := 0
		db := newDB(newTestSender(func(ba proto.BatchRequest) (*proto.BatchResponse, *proto.Error) {

			if _, ok := ba.GetArg(proto.Put); ok {
				count++
				if count == 1 {
					return nil, proto.NewError(test.err)
				}
			}
			return ba.CreateReply().(*proto.BatchResponse), nil
		}, nil))
		db.txnRetryOptions.InitialBackoff = 1 * time.Millisecond
		err := db.Txn(func(txn *Txn) error {
			return txn.Put("a", "b")
		})
		if test.retry {
			if count != 2 {
				t.Errorf("%d: expected one retry; got %d", i, count-1)
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

// TestAbortTransactionOnCommitErrors verifies that non-exec transactions are
// aborted on the correct errors.
func TestAbortTransactionOnCommitErrors(t *testing.T) {
	defer leaktest.AfterTest(t)

	testCases := []struct {
		err   error
		abort bool
	}{
		{&proto.ReadWithinUncertaintyIntervalError{}, true},
		{&proto.TransactionAbortedError{}, false},
		{&proto.TransactionPushError{}, true},
		{&proto.TransactionRetryError{}, true},
		{&proto.RangeNotFoundError{}, true},
		{&proto.RangeKeyMismatchError{}, true},
		{&proto.TransactionStatusError{}, true},
	}

	for _, test := range testCases {
		var commit, abort bool
		db := NewDB(newTestSender(func(ba proto.BatchRequest) (*proto.BatchResponse, *proto.Error) {

			switch t := ba.Requests[0].GetInner().(type) {
			case *proto.EndTransactionRequest:
				if t.Commit {
					commit = true
					return nil, proto.NewError(test.err)
				}
				abort = true
			}
			return ba.CreateReply().(*proto.BatchResponse), nil
		}, nil))

		txn := NewTxn(*db)
		_ = txn.Put("a", "b")
		_ = txn.Commit()

		if !commit {
			t.Errorf("%T: failed to find commit", test.err)
		}
		if test.abort && !abort {
			t.Errorf("%T: failed to find abort", test.err)
		} else if !test.abort && abort {
			t.Errorf("%T: found unexpected abort", test.err)
		}
	}
}

// TestTransactionStatus verifies that transactions always have their
// status updated correctly.
func TestTransactionStatus(t *testing.T) {
	defer leaktest.AfterTest(t)

	db := NewDB(newTestSender(nil, nil))
	for _, write := range []bool{true, false} {
		for _, commit := range []bool{true, false} {
			txn := NewTxn(*db)

			if _, err := txn.Get("a"); err != nil {
				t.Fatal(err)
			}
			if write {
				if err := txn.Put("a", "b"); err != nil {
					t.Fatal(err)
				}
			}
			if commit {
				if err := txn.Commit(); err != nil {
					t.Fatal(err)
				}
				if a, e := txn.Proto.Status, proto.COMMITTED; a != e {
					t.Errorf("write: %t, commit: %t transaction expected to have status %q but had %q", write, commit, e, a)
				}
			} else {
				if err := txn.Rollback(); err != nil {
					t.Fatal(err)
				}
				if a, e := txn.Proto.Status, proto.ABORTED; a != e {
					t.Errorf("write: %t, commit: %t transaction expected to have status %q but had %q", write, commit, e, a)
				}
			}
		}
	}
}
