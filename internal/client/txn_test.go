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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package client

import (
	"reflect"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/uuid"
)

var (
	testKey     = roachpb.Key("a")
	testTS      = hlc.Timestamp{WallTime: 1, Logical: 1}
	testPutResp = roachpb.PutResponse{}
	txnKey      = roachpb.Key("test-txn")
)

// TestSender mocks out some of the txn coordinator sender's
// functionality. It responds to PutRequests using testPutResp.
func newTestSender(pre, post func(roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)) SenderFunc {
	txnID := uuid.NewV4()

	return func(_ context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		if ba.UserPriority == 0 {
			ba.UserPriority = 1
		}
		if ba.Txn != nil && ba.Txn.ID == nil {
			ba.Txn.Key = txnKey
			ba.Txn.ID = txnID
		}

		var br *roachpb.BatchResponse
		var pErr *roachpb.Error
		if pre != nil {
			br, pErr = pre(ba)
		} else {
			br = ba.CreateReply()
		}
		if pErr != nil {
			return nil, pErr
		}
		var writing bool
		status := roachpb.PENDING
		for i, req := range ba.Requests {
			args := req.GetInner()
			if _, ok := args.(*roachpb.PutRequest); ok {
				testPutRespCopy := testPutResp
				union := &br.Responses[i] // avoid operating on copy
				union.MustSetInner(&testPutRespCopy)
			}
			if roachpb.IsTransactionWrite(args) {
				writing = true
			}
		}
		if args, ok := ba.GetArg(roachpb.EndTransaction); ok {
			et := args.(*roachpb.EndTransactionRequest)
			writing = true
			if et.Commit {
				status = roachpb.COMMITTED
			} else {
				status = roachpb.ABORTED
			}
		}
		if ba.Txn != nil {
			txnClone := ba.Txn.Clone()
			br.Txn = &txnClone
			if pErr == nil {
				br.Txn.Writing = writing
				br.Txn.Status = status
			}
		}

		if post != nil {
			br, pErr = post(ba)
		}
		return br, pErr
	}
}

func testPut() roachpb.BatchRequest {
	var ba roachpb.BatchRequest
	ba.Timestamp = testTS
	put := &roachpb.PutRequest{}
	put.Key = testKey
	ba.Add(put)
	return ba
}

func TestInitPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// This test is mostly an excuse to exercise otherwise unused code.
	// TODO(vivekmenezes): update test or remove when InitPut is being
	// considered sufficiently tested and this path exercised.
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		br := ba.CreateReply()
		return br, nil
	}, nil))

	txn := NewTxn(context.Background(), *db)
	if pErr := txn.InitPut("a", "b"); pErr != nil {
		t.Fatal(pErr)
	}
}

// TestTxnRequestTxnTimestamp verifies response txn timestamp is
// always upgraded on successive requests.
func TestTxnRequestTxnTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	makeTS := func(walltime int64, logical int32) hlc.Timestamp {
		return hlc.ZeroTimestamp.Add(walltime, logical)
	}
	ba := testPut()

	testCases := []struct {
		expRequestTS, responseTS hlc.Timestamp
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
	db := NewDB(newTestSender(nil, func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		test := testCases[testIdx]
		if !test.expRequestTS.Equal(ba.Txn.Timestamp) {
			return nil, roachpb.NewErrorf("%d: expected ts %s got %s", testIdx, test.expRequestTS, ba.Txn.Timestamp)
		}
		br := &roachpb.BatchResponse{}
		br.Txn = &roachpb.Transaction{}
		br.Txn.Update(ba.Txn) // copy
		br.Txn.Timestamp = test.responseTS
		return br, nil
	}))

	txn := NewTxn(context.Background(), *db)

	for testIdx = range testCases {
		if _, pErr := txn.db.sender.Send(context.Background(), ba); pErr != nil {
			t.Fatal(pErr)
		}
	}
}

// TestTxnResetTxnOnAbort verifies transaction is reset on abort.
func TestTxnResetTxnOnAbort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		return nil, roachpb.NewErrorWithTxn(&roachpb.TransactionAbortedError{}, ba.Txn)
	}, nil))

	txn := NewTxn(context.Background(), *db)
	_, pErr := txn.db.sender.Send(context.Background(), testPut())
	if _, ok := pErr.GetDetail().(*roachpb.TransactionAbortedError); !ok {
		t.Fatalf("expected TransactionAbortedError, got %v", pErr)
	}

	if txn.Proto.ID != nil {
		t.Errorf("expected txn to be cleared")
	}
}

// TestTransactionConfig verifies the proper unwrapping and
// re-wrapping of the client's sender when starting a transaction.
// Also verifies that the UserPriority is propagated to the
// transactional client.
func TestTransactionConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dbCtx := DefaultDBContext()
	dbCtx.UserPriority = 101
	db := NewDBWithContext(newTestSender(nil, nil), dbCtx)
	if err := db.Txn(func(txn *Txn) error {
		if txn.db.ctx.UserPriority != db.ctx.UserPriority {
			t.Errorf("expected txn user priority %f; got %f",
				db.ctx.UserPriority, txn.db.ctx.UserPriority)
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
	defer leaktest.AfterTest(t)()
	var calls []roachpb.Method
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		calls = append(calls, ba.Methods()...)
		return ba.CreateReply(), nil
	}, nil))
	if err := db.Txn(func(txn *Txn) error {
		_, err := txn.Get("a")
		return err
	}); err != nil {
		t.Errorf("unexpected error on commit: %s", err)
	}
	expectedCalls := []roachpb.Method{roachpb.Get}
	if !reflect.DeepEqual(expectedCalls, calls) {
		t.Errorf("expected %s, got %s", expectedCalls, calls)
	}
}

// TestCommitReadOnlyTransactionExplicit verifies that a read-only
// transaction with an explicit EndTransaction call does not send
// that call.
func TestCommitReadOnlyTransactionExplicit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, withGet := range []bool{true, false} {
		var calls []roachpb.Method
		db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			calls = append(calls, ba.Methods()...)
			return ba.CreateReply(), nil
		}, nil))
		if err := db.Txn(func(txn *Txn) error {
			b := txn.NewBatch()
			if withGet {
				b.Get("foo")
			}
			return txn.CommitInBatch(b)
		}); err != nil {
			t.Errorf("unexpected error on commit: %s", err)
		}
		expectedCalls := []roachpb.Method(nil)
		if withGet {
			expectedCalls = append(expectedCalls, roachpb.Get)
		}
		if !reflect.DeepEqual(expectedCalls, calls) {
			t.Errorf("expected %s, got %s", expectedCalls, calls)
		}
	}
}

// TestCommitMutatingTransaction verifies that transaction is committed
// upon successful invocation of the retryable func.
func TestCommitMutatingTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var calls []roachpb.Method
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		calls = append(calls, ba.Methods()...)
		if bt, ok := ba.GetArg(roachpb.BeginTransaction); ok && !bt.Header().Key.Equal(roachpb.Key("a")) {
			t.Errorf("expected begin transaction key to be \"a\"; got %s", bt.Header().Key)
		}
		if et, ok := ba.GetArg(roachpb.EndTransaction); ok && !et.(*roachpb.EndTransactionRequest).Commit {
			t.Errorf("expected commit to be true")
		}
		return ba.CreateReply(), nil
	}, nil))

	// Test all transactional write methods.
	testArgs := []struct {
		f         func(txn *Txn) error
		expMethod roachpb.Method
	}{
		{func(txn *Txn) error { return txn.Put("a", "b") }, roachpb.Put},
		{func(txn *Txn) error { return txn.CPut("a", "b", nil) }, roachpb.ConditionalPut},
		{func(txn *Txn) error {
			_, err := txn.Inc("a", 1)
			return err
		}, roachpb.Increment},
		{func(txn *Txn) error { return txn.Del("a") }, roachpb.Delete},
		{func(txn *Txn) error { return txn.DelRange("a", "b") }, roachpb.DeleteRange},
	}
	for i, test := range testArgs {
		calls = []roachpb.Method{}
		if err := db.Txn(func(txn *Txn) error {
			return test.f(txn)
		}); err != nil {
			t.Errorf("%d: unexpected error on commit: %s", i, err)
		}
		expectedCalls := []roachpb.Method{roachpb.BeginTransaction, test.expMethod, roachpb.EndTransaction}
		if !reflect.DeepEqual(expectedCalls, calls) {
			t.Errorf("%d: expected %s, got %s", i, expectedCalls, calls)
		}
	}
}

// TestTxnInsertBeginTransaction verifies that a begin transaction
// request is inserted just before the first mutating command.
func TestTxnInsertBeginTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var calls []roachpb.Method
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		calls = append(calls, ba.Methods()...)
		return ba.CreateReply(), nil
	}, nil))
	if err := db.Txn(func(txn *Txn) error {
		if _, err := txn.Get("foo"); err != nil {
			return err
		}
		return txn.Put("a", "b")
	}); err != nil {
		t.Errorf("unexpected error on commit: %s", err)
	}
	expectedCalls := []roachpb.Method{roachpb.Get, roachpb.BeginTransaction, roachpb.Put, roachpb.EndTransaction}
	if !reflect.DeepEqual(expectedCalls, calls) {
		t.Errorf("expected %s, got %s", expectedCalls, calls)
	}
}

// TestBeginTransactionErrorIndex verifies that the error index is cleared
// when a BeginTransaction command causes an error.
func TestBeginTransactionErrorIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		pErr := roachpb.NewError(&roachpb.WriteIntentError{})
		pErr.SetErrorIndex(0)
		return nil, pErr
	}, nil))
	_ = db.Txn(func(txn *Txn) error {
		b := txn.NewBatch()
		b.Put("a", "b")
		_, err := runOneResult(txn, b)
		pErr := b.MustPErr()
		// Verify that the original error type is preserved, but the error index is unset.
		if _, ok := pErr.GetDetail().(*roachpb.WriteIntentError); !ok {
			t.Fatalf("unexpected error %s", pErr)
		}
		if pErr.Index != nil {
			t.Errorf("error index must not be set, but got %s", pErr.Index)
		}
		return err
	})
}

// TestCommitTransactionOnce verifies that if the transaction is
// ended explicitly in the retryable func, it is not automatically
// ended a second time at completion of retryable func.
func TestCommitTransactionOnce(t *testing.T) {
	defer leaktest.AfterTest(t)()
	count := 0
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		count++
		return ba.CreateReply(), nil
	}, nil))
	if err := db.Txn(func(txn *Txn) error {
		b := txn.NewBatch()
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
	defer leaktest.AfterTest(t)()
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		if _, ok := ba.GetArg(roachpb.EndTransaction); ok {
			t.Errorf("did not expect EndTransaction")
		}
		return ba.CreateReply(), nil
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
	defer leaktest.AfterTest(t)()
	for _, success := range []bool{true, false} {
		expCalls := []roachpb.Method{roachpb.BeginTransaction, roachpb.Put, roachpb.EndTransaction}
		var calls []roachpb.Method
		db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			calls = append(calls, ba.Methods()...)
			return ba.CreateReply(), nil
		}, nil))
		ok := false
		if err := db.Txn(func(txn *Txn) error {
			if !ok {
				if err := txn.Put("consider", "phlebas"); err != nil {
					t.Fatal(err)
				}
				ok = true
				// Return an immediate txn retry error. We need to go through the pErr
				// and back to get a RetryableTxnError.
				return roachpb.NewErrorWithTxn(roachpb.NewTransactionRetryError(), &txn.Proto).GoError()
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

// TestTransactionKeyNotChangedInRestart verifies that if the transaction already has a key (we're
// in a restart), the key in the begin transaction request is not changed.
func TestTransactionKeyNotChangedInRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tries := 0
	db := NewDB(newTestSender(nil, func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		var bt *roachpb.BeginTransactionRequest
		if args, ok := ba.GetArg(roachpb.BeginTransaction); ok {
			bt = args.(*roachpb.BeginTransactionRequest)
		} else {
			t.Fatal("failed to find a begin transaction request")
		}

		// In the first try, the transaction key is the key of the first write command. Before the
		// second try, the transaction key is set to txnKey by the test sender. In the second try, the
		// transaction key is txnKey.
		var expectedKey roachpb.Key
		if tries == 1 {
			expectedKey = testKey
		} else {
			expectedKey = txnKey
		}
		if !bt.Key.Equal(expectedKey) {
			t.Fatalf("expected transaction key %v, got %v", expectedKey, bt.Key)
		}

		return ba.CreateReply(), nil
	}))

	if err := db.Txn(func(txn *Txn) error {
		tries++
		b := txn.NewBatch()
		b.Put("a", "b")
		if err := txn.Run(b); err != nil {
			t.Fatal(err)
		}
		if tries == 1 {
			return roachpb.NewErrorWithTxn(roachpb.NewTransactionRetryError(), &txn.Proto).GoError()
		}
		return nil
	}); err != nil {
		t.Errorf("unexpected error on commit: %s", err)
	}
	minimumTries := 2
	if tries < minimumTries {
		t.Errorf("expected try count >= %d, got %d", minimumTries, tries)
	}
}

// TestAbortMutatingTransaction verifies that transaction is aborted
// upon failed invocation of the retryable func.
func TestAbortMutatingTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var calls []roachpb.Method
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		calls = append(calls, ba.Methods()...)
		if et, ok := ba.GetArg(roachpb.EndTransaction); ok && et.(*roachpb.EndTransactionRequest).Commit {
			t.Errorf("expected commit to be false")
		}
		return ba.CreateReply(), nil
	}, nil))

	if err := db.Txn(func(txn *Txn) error {
		if err := txn.Put("a", "b"); err != nil {
			return err
		}
		return errors.Errorf("foo")
	}); err == nil {
		t.Error("expected error on abort")
	}
	expectedCalls := []roachpb.Method{roachpb.BeginTransaction, roachpb.Put, roachpb.EndTransaction}
	if !reflect.DeepEqual(expectedCalls, calls) {
		t.Errorf("expected %s, got %s", expectedCalls, calls)
	}
}

// TestRunTransactionRetryOnErrors verifies that the transaction
// is retried on the correct errors.
func TestRunTransactionRetryOnErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		err   error
		retry bool // Expect retry?
	}{
		{roachpb.NewReadWithinUncertaintyIntervalError(hlc.ZeroTimestamp, hlc.ZeroTimestamp), true},
		{&roachpb.TransactionAbortedError{}, true},
		{&roachpb.TransactionPushError{}, true},
		{&roachpb.TransactionRetryError{}, true},
		{&roachpb.WriteTooOldError{}, true},
		{&roachpb.RangeNotFoundError{}, false},
		{&roachpb.RangeKeyMismatchError{}, false},
		{&roachpb.TransactionStatusError{}, false},
	}

	for i, test := range testCases {
		count := 0
		dbCtx := DefaultDBContext()
		dbCtx.TxnRetryOptions.InitialBackoff = 1 * time.Millisecond
		db := NewDBWithContext(newTestSender(
			func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {

				if _, ok := ba.GetArg(roachpb.Put); ok {
					count++
					if count == 1 {
						return nil, roachpb.NewErrorWithTxn(test.err, ba.Txn)
					}
				}
				return ba.CreateReply(), nil
			}, nil), dbCtx)
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

// TestAbortedRetryPreservesTimestamp verifies that the minimum timestamp
// set by the client is preserved across retries of aborted transactions.
func TestAbortedRetryPreservesTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a TestSender that aborts a transaction 2 times before succeeding.
	count := 0
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		if _, ok := ba.GetArg(roachpb.Put); ok {
			count++
			if count < 3 {
				return nil, roachpb.NewError(&roachpb.TransactionAbortedError{})
			}
		}
		return ba.CreateReply(), nil
	}, nil))

	txnClosure := func(txn *Txn, opt *TxnExecOptions) error {
		// Ensure the KV transaction is created.
		return txn.Put("a", "b")
	}

	txn := NewTxn(context.Background(), *db)

	// Request a client-defined timestamp.
	refTimestamp := hlc.Timestamp{WallTime: 42, Logical: 69}
	execOpt := TxnExecOptions{AutoRetry: true, AutoCommit: true}
	execOpt.MinInitialTimestamp = refTimestamp

	// Perform the transaction.
	if err := txn.Exec(execOpt, txnClosure); err != nil {
		t.Fatal(err)
	}

	// Check the timestamp was preserved.
	if txn.Proto.OrigTimestamp != refTimestamp {
		t.Errorf("expected txn orig ts to be %s; got %s", refTimestamp, txn.Proto.OrigTimestamp)
	}
}

// TestAbortTransactionOnCommitErrors verifies that transactions are
// aborted on the correct errors.
func TestAbortTransactionOnCommitErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		err   error
		abort bool
	}{
		{roachpb.NewReadWithinUncertaintyIntervalError(hlc.ZeroTimestamp, hlc.ZeroTimestamp), true},
		{&roachpb.TransactionAbortedError{}, false},
		{&roachpb.TransactionPushError{}, true},
		{&roachpb.TransactionRetryError{}, true},
		{&roachpb.RangeNotFoundError{}, true},
		{&roachpb.RangeKeyMismatchError{}, true},
		{&roachpb.TransactionStatusError{}, true},
	}

	for _, test := range testCases {
		var commit, abort bool
		db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {

			switch t := ba.Requests[0].GetInner().(type) {
			case *roachpb.EndTransactionRequest:
				if t.Commit {
					commit = true
					return nil, roachpb.NewError(test.err)
				}
				abort = true
			}
			return ba.CreateReply(), nil
		}, nil))

		txn := NewTxn(context.Background(), *db)
		if pErr := txn.Put("a", "b"); pErr != nil {
			t.Fatalf("put failed: %s", pErr)
		}
		if pErr := txn.CommitOrCleanup(); pErr == nil {
			t.Fatalf("unexpected commit success")
		}

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
	defer leaktest.AfterTest(t)()

	db := NewDB(newTestSender(nil, nil))
	for _, write := range []bool{true, false} {
		for _, commit := range []bool{true, false} {
			txn := NewTxn(context.Background(), *db)

			if _, pErr := txn.Get("a"); pErr != nil {
				t.Fatal(pErr)
			}
			if write {
				if pErr := txn.Put("a", "b"); pErr != nil {
					t.Fatal(pErr)
				}
			}
			if commit {
				if pErr := txn.CommitOrCleanup(); pErr != nil {
					t.Fatal(pErr)
				}
				if a, e := txn.Proto.Status, roachpb.COMMITTED; a != e {
					t.Errorf("write: %t, commit: %t transaction expected to have status %q but had %q", write, commit, e, a)
				}
			} else {
				if pErr := txn.Rollback(); pErr != nil {
					t.Fatal(pErr)
				}
				if a, e := txn.Proto.Status, roachpb.ABORTED; a != e {
					t.Errorf("write: %t, commit: %t transaction expected to have status %q but had %q", write, commit, e, a)
				}
			}
		}
	}
}

func TestCommitInBatchWrongTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := NewDB(newTestSender(nil, nil))
	txn := NewTxn(context.Background(), *db)

	b1 := &Batch{}
	txn2 := NewTxn(context.Background(), *db)
	b2 := txn2.NewBatch()

	for _, b := range []*Batch{b1, b2} {
		if err := txn.CommitInBatch(b); !testutils.IsError(err, "can only be committed by") {
			t.Error(err)
		}
	}
}

// TestTimestampSelectionInOptions verifies that a client can set the
// Txn timestamp using client.TxnExecOptions.
func TestTimestampSelectionInOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := NewDB(newTestSender(nil, nil))
	txn := NewTxn(context.Background(), *db)

	var execOpt TxnExecOptions
	refTimestamp := hlc.Timestamp{WallTime: 42, Logical: 69}
	execOpt.MinInitialTimestamp = refTimestamp

	txnClosure := func(txn *Txn, opt *TxnExecOptions) error {
		// Ensure the KV transaction is created.
		return txn.Put("a", "b")
	}

	if err := txn.Exec(execOpt, txnClosure); err != nil {
		t.Fatal(err)
	}

	// Check the timestamp was preserved.
	if txn.Proto.OrigTimestamp != refTimestamp {
		t.Errorf("expected txn orig ts to be %s; got %s", refTimestamp, txn.Proto.OrigTimestamp)
	}
}

// TestSetPriority verifies that the batch UserPriority is correctly set
// depending on the transaction priority.
func TestSetPriority(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var expected roachpb.UserPriority

	db := NewDB(newTestSender(
		func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			if ba.UserPriority != expected {
				pErr := roachpb.NewErrorf("Priority not set correctly in the batch! "+
					"(expected: %s, value: %s)", expected, ba.UserPriority)
				return nil, pErr
			}

			br := &roachpb.BatchResponse{}
			br.Txn = &roachpb.Transaction{}
			br.Txn.Update(ba.Txn) // copy
			return br, nil
		}, nil))

	// Verify the normal priority setting path.
	expected = roachpb.HighUserPriority
	txn := NewTxn(context.Background(), *db)
	if err := txn.SetUserPriority(expected); err != nil {
		t.Fatal(err)
	}
	if _, pErr := txn.db.sender.Send(context.Background(), roachpb.BatchRequest{}); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify the internal (fixed value) priority setting path.
	expected = roachpb.UserPriority(-13)
	txn = NewTxn(context.Background(), *db)
	txn.InternalSetPriority(13)
	if _, pErr := txn.db.sender.Send(context.Background(), roachpb.BatchRequest{}); pErr != nil {
		t.Fatal(pErr)
	}
}

// Tests that a retryable error for an inner txn doesn't cause the outer txn to
// be retried.
func TestWrongTxnRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := NewDB(newTestSender(nil, nil))

	var retries int
	txnClosure := func(outerTxn *Txn) error {
		log.Infof(context.Background(), "outer retry")
		retries++
		// Ensure the KV transaction is created.
		if err := outerTxn.Put("a", "b"); err != nil {
			t.Fatal(err)
		}
		var execOpt TxnExecOptions
		execOpt.AutoRetry = false
		err := outerTxn.Exec(
			execOpt,
			func(innerTxn *Txn, opt *TxnExecOptions) error {
				// Ensure the KV transaction is created.
				if err := innerTxn.Put("x", "y"); err != nil {
					t.Fatal(err)
				}
				return roachpb.NewErrorWithTxn(&roachpb.TransactionPushError{
					PusheeTxn: outerTxn.Proto}, &innerTxn.Proto).GoError()
			})
		return err
	}

	if err := db.Txn(txnClosure); !testutils.IsError(err, "failed to push") {
		t.Fatal(err)
	}
	if retries != 1 {
		t.Fatalf("unexpected retries: %d", retries)
	}
}

func TestBatchMixRawRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := NewDB(newTestSender(nil, nil))

	b := db.NewBatch()
	b.AddRawRequest(&roachpb.EndTransactionRequest{})
	b.Put("x", "y")
	if err := db.Run(b); !testutils.IsError(err, "non-raw operations") {
		t.Fatal(err)
	}
}

func TestUpdateDeadlineMaybe(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := NewDB(newTestSender(nil, nil))
	txn := NewTxn(context.Background(), *db)

	if txn.deadline != nil {
		t.Errorf("unexpected initial deadline: %s", txn.deadline)
	}

	deadline := hlc.Timestamp{WallTime: 10, Logical: 1}
	if !txn.UpdateDeadlineMaybe(deadline) {
		t.Errorf("expected update, but it didn't happen")
	}
	if !txn.deadline.Equal(deadline) {
		t.Errorf("unexpected deadline: %s", txn.deadline)
	}

	futureDeadline := hlc.Timestamp{WallTime: 11, Logical: 1}
	if txn.UpdateDeadlineMaybe(futureDeadline) {
		t.Errorf("expected no update, but update happened")
	}
	if !txn.deadline.Equal(deadline) {
		t.Errorf("unexpected deadline: %s", txn.deadline)
	}

	pastDeadline := hlc.Timestamp{WallTime: 9, Logical: 1}
	if !txn.UpdateDeadlineMaybe(pastDeadline) {
		t.Errorf("expected update, but it didn't happen")
	}
	if !txn.deadline.Equal(pastDeadline) {
		t.Errorf("unexpected deadline: %s", txn.deadline)
	}
}
