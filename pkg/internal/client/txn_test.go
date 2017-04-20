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
	"bytes"
	"reflect"
	"regexp"
	"testing"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

var (
	testKey     = roachpb.Key("a")
	testTS      = hlc.Timestamp{WallTime: 1, Logical: 1}
	testPutResp = roachpb.PutResponse{}
)

// An example of snowball tracing being used to dump a trace around a
// transaction. Use something similar whenever you cannot use
// sql.trace.txn.threshold.
func TestTxnSnowballTrace(t *testing.T) {
	defer leaktest.AfterTest(t)()

	clock := hlc.NewClock(hlc.UnixNano, 0)
	db := NewDB(newTestSender(nil), clock)
	ctx, trace, err := tracing.StartSnowballTrace(context.Background(), "test-txn")
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Txn(ctx, func(ctx context.Context, txn *Txn) error {
		log.Event(ctx, "inside txn")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	log.Event(ctx, "txn complete")
	sp := opentracing.SpanFromContext(ctx)
	sp.Finish()
	trace.Done()
	collectedSpans := trace.GetSpans()
	dump := tracing.FormatRawSpans(collectedSpans)
	// dump:
	//    0.105ms      0.000ms    event:inside txn
	//    0.275ms      0.171ms    event:client.Txn did AutoCommit. err: <nil>
	//txn: "internal/client/txn_test.go:67 TestTxnSnowballTrace" id=<nil> key=/Min rw=false pri=0.00000000 iso=SERIALIZABLE stat=COMMITTED epo=0 ts=0.000000000,0 orig=0.000000000,0 max=0.000000000,0 wto=false rop=false
	//    0.278ms      0.173ms    event:txn complete
	found, err := regexp.MatchString(".*event:inside txn\n.*event:client.Txn did AutoCommit. err: <nil>\n.*\n.*event:txn complete.*", dump)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("didn't match: %s", dump)
	}
}

// TestSender mocks out some of the txn coordinator sender's
// functionality. It responds to PutRequests using testPutResp.
func newTestSender(
	createReply func(roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error),
) SenderFunc {
	return func(_ context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		if ba.UserPriority == 0 {
			ba.UserPriority = 1
		}

		var br *roachpb.BatchResponse
		var pErr *roachpb.Error
		if createReply != nil {
			br, pErr = createReply(ba)
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
		if ba.Txn != nil && br.Txn == nil {
			txnClone := ba.Txn.Clone()
			br.Txn = &txnClone
			if pErr == nil {
				br.Txn.Writing = writing
				br.Txn.Status = status
			}
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
	clock := hlc.NewClock(hlc.UnixNano, 0)
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		br := ba.CreateReply()
		return br, nil
	}), clock)

	txn := NewTxn(db)
	if pErr := txn.InitPut(context.Background(), "a", "b"); pErr != nil {
		t.Fatal(pErr)
	}
}

// TestTxnRequestTxnTimestamp verifies response txn timestamp is
// always upgraded on successive requests.
func TestTxnRequestTxnTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ba := testPut()

	testCases := []struct {
		expRequestTS, responseTS hlc.Timestamp
	}{
		{hlc.Timestamp{WallTime: 5, Logical: 0}, hlc.Timestamp{WallTime: 10, Logical: 0}},
		{hlc.Timestamp{WallTime: 10, Logical: 0}, hlc.Timestamp{WallTime: 10, Logical: 1}},
		{hlc.Timestamp{WallTime: 10, Logical: 1}, hlc.Timestamp{WallTime: 10, Logical: 0}},
		{hlc.Timestamp{WallTime: 10, Logical: 1}, hlc.Timestamp{WallTime: 20, Logical: 1}},
		{hlc.Timestamp{WallTime: 20, Logical: 1}, hlc.Timestamp{WallTime: 20, Logical: 1}},
		{hlc.Timestamp{WallTime: 20, Logical: 1}, hlc.Timestamp{WallTime: 0, Logical: 0}},
		{hlc.Timestamp{WallTime: 20, Logical: 1}, hlc.Timestamp{WallTime: 20, Logical: 1}},
	}

	manual := hlc.NewManualClock(testCases[0].expRequestTS.WallTime)
	clock := hlc.NewClock(manual.UnixNano, 0)
	var testIdx int
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		test := testCases[testIdx]
		if test.expRequestTS != ba.Txn.Timestamp {
			return nil, roachpb.NewErrorf("%d: expected ts %s got %s", testIdx, test.expRequestTS, ba.Txn.Timestamp)
		}
		br := ba.CreateReply()
		txnClone := ba.Txn.Clone()
		br.Txn = &txnClone
		br.Txn.Timestamp = test.responseTS
		return br, nil
	}), clock)

	txn := NewTxn(db)

	for testIdx = range testCases {
		if _, pErr := txn.send(context.Background(), ba); pErr != nil {
			t.Fatal(pErr)
		}
	}
}

// TestTxnResetTxnOnAbort verifies transaction is reset on abort.
func TestTxnResetTxnOnAbort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	clock := hlc.NewClock(hlc.UnixNano, 0)
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		return nil, roachpb.NewErrorWithTxn(&roachpb.TransactionAbortedError{}, ba.Txn)
	}), clock)

	txn := NewTxn(db)
	_, pErr := txn.send(context.Background(), testPut())
	if _, ok := pErr.GetDetail().(*roachpb.TransactionAbortedError); !ok {
		t.Fatalf("expected TransactionAbortedError, got %v", pErr)
	}

	if txn.Proto().ID != nil {
		t.Error("expected txn to be cleared")
	}
}

// TestTransactionConfig verifies the proper unwrapping and
// re-wrapping of the client's sender when starting a transaction.
// Also verifies that the UserPriority is propagated to the
// transactional client.
func TestTransactionConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	clock := hlc.NewClock(hlc.UnixNano, 0)
	dbCtx := DefaultDBContext()
	dbCtx.UserPriority = 101
	db := NewDBWithContext(newTestSender(nil), clock, dbCtx)
	if err := db.Txn(context.TODO(), func(ctx context.Context, txn *Txn) error {
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
	clock := hlc.NewClock(hlc.UnixNano, 0)
	var calls []roachpb.Method
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		calls = append(calls, ba.Methods()...)
		return ba.CreateReply(), nil
	}), clock)
	if err := db.Txn(context.TODO(), func(ctx context.Context, txn *Txn) error {
		_, err := txn.Get(ctx, "a")
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
	clock := hlc.NewClock(hlc.UnixNano, 0)
	for _, withGet := range []bool{true, false} {
		var calls []roachpb.Method
		db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			calls = append(calls, ba.Methods()...)
			return ba.CreateReply(), nil
		}), clock)
		if err := db.Txn(context.TODO(), func(ctx context.Context, txn *Txn) error {
			b := txn.NewBatch()
			if withGet {
				b.Get("foo")
			}
			return txn.CommitInBatch(ctx, b)
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
	clock := hlc.NewClock(hlc.UnixNano, 0)
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
	}), clock)

	// Test all transactional write methods.
	testArgs := []struct {
		f         func(ctx context.Context, txn *Txn) error
		expMethod roachpb.Method
	}{
		{func(ctx context.Context, txn *Txn) error { return txn.Put(ctx, "a", "b") }, roachpb.Put},
		{func(ctx context.Context, txn *Txn) error { return txn.CPut(ctx, "a", "b", nil) }, roachpb.ConditionalPut},
		{func(ctx context.Context, txn *Txn) error {
			_, err := txn.Inc(ctx, "a", 1)
			return err
		}, roachpb.Increment},
		{func(ctx context.Context, txn *Txn) error { return txn.Del(ctx, "a") }, roachpb.Delete},
		{func(ctx context.Context, txn *Txn) error { return txn.DelRange(ctx, "a", "b") }, roachpb.DeleteRange},
	}
	for i, test := range testArgs {
		calls = []roachpb.Method{}
		if err := db.Txn(context.TODO(), func(ctx context.Context, txn *Txn) error {
			return test.f(ctx, txn)
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
	clock := hlc.NewClock(hlc.UnixNano, 0)
	var calls []roachpb.Method
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		calls = append(calls, ba.Methods()...)
		return ba.CreateReply(), nil
	}), clock)
	if err := db.Txn(context.TODO(), func(ctx context.Context, txn *Txn) error {
		if _, err := txn.Get(ctx, "foo"); err != nil {
			return err
		}
		return txn.Put(ctx, "a", "b")
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
	clock := hlc.NewClock(hlc.UnixNano, 0)
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		pErr := roachpb.NewError(&roachpb.WriteIntentError{})
		pErr.SetErrorIndex(0)
		return nil, pErr
	}), clock)
	_ = db.Txn(context.TODO(), func(ctx context.Context, txn *Txn) error {
		b := txn.NewBatch()
		b.Put("a", "b")
		err := getOneErr(txn.Run(ctx, b), b)
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
	clock := hlc.NewClock(hlc.UnixNano, 0)
	count := 0
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		count++
		return ba.CreateReply(), nil
	}), clock)
	if err := db.Txn(context.TODO(), func(ctx context.Context, txn *Txn) error {
		b := txn.NewBatch()
		b.Put("z", "adding a write exposed a bug in #1882")
		return txn.CommitInBatch(ctx, b)
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
	clock := hlc.NewClock(hlc.UnixNano, 0)
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		if _, ok := ba.GetArg(roachpb.EndTransaction); ok {
			t.Errorf("did not expect EndTransaction")
		}
		return ba.CreateReply(), nil
	}), clock)
	if err := db.Txn(context.TODO(), func(ctx context.Context, txn *Txn) error {
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
	clock := hlc.NewClock(hlc.UnixNano, 0)
	for _, success := range []bool{true, false} {
		expCalls := []roachpb.Method{roachpb.BeginTransaction, roachpb.Put, roachpb.EndTransaction}
		var calls []roachpb.Method
		db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			calls = append(calls, ba.Methods()...)
			return ba.CreateReply(), nil
		}), clock)
		ok := false
		if err := db.Txn(context.TODO(), func(ctx context.Context, txn *Txn) error {
			if !ok {
				if err := txn.Put(ctx, "consider", "phlebas"); err != nil {
					t.Fatal(err)
				}
				ok = true
				// Return an immediate txn retry error. We need to go through the pErr
				// and back to get a RetryableTxnError.
				return roachpb.NewErrorWithTxn(
					roachpb.NewTransactionRetryError(), txn.Proto()).GoError()
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

	clock := hlc.NewClock(hlc.UnixNano, 0)
	attempt := 0
	keys := []string{"first", "second"}
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		// Ignore the final EndTxnRequest.
		if _, ok := ba.GetArg(roachpb.EndTransaction); ok {
			return ba.CreateReply(), nil
		}

		// Attempt 0 should have a BeginTxnRequest, and a PutRequest.
		// Attempt 1 should have a PutRequest.
		if attempt == 0 {
			if _, ok := ba.GetArg(roachpb.BeginTransaction); !ok {
				t.Fatalf("failed to find a begin transaction request: %v", ba)
			}
		}
		if _, ok := ba.GetArg(roachpb.Put); !ok {
			t.Fatalf("failed to find a put request: %v", ba)
		}

		// In the first attempt, the transaction key is the key of the first write command.
		// This key is retained between restarts, so we see the same key in the second attempt.
		if expectedKey := []byte(keys[0]); !bytes.Equal(expectedKey, ba.Txn.Key) {
			t.Fatalf("expected transaction key %v, got %v", expectedKey, ba.Txn.Key)
		}

		if attempt == 0 {
			// Abort the first attempt so that we need to retry with
			// a new transaction proto.
			return nil, roachpb.NewErrorWithTxn(roachpb.NewTransactionRetryError(), ba.Txn)
		}
		return ba.CreateReply(), nil
	}), clock)

	if err := db.Txn(context.TODO(), func(ctx context.Context, txn *Txn) error {
		defer func() { attempt++ }()
		b := txn.NewBatch()
		b.Put(keys[attempt], "b")
		return txn.Run(ctx, b)
	}); err != nil {
		t.Errorf("unexpected error on commit: %s", err)
	}
	minimumAttempts := 2
	if attempt < minimumAttempts {
		t.Errorf("expected attempt count >= %d, got %d", minimumAttempts, attempt)
	}
}

// TestAbortMutatingTransaction verifies that transaction is aborted
// upon failed invocation of the retryable func.
func TestAbortMutatingTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	clock := hlc.NewClock(hlc.UnixNano, 0)
	var calls []roachpb.Method
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		calls = append(calls, ba.Methods()...)
		if et, ok := ba.GetArg(roachpb.EndTransaction); ok && et.(*roachpb.EndTransactionRequest).Commit {
			t.Errorf("expected commit to be false")
		}
		return ba.CreateReply(), nil
	}), clock)

	if err := db.Txn(context.TODO(), func(ctx context.Context, txn *Txn) error {
		if err := txn.Put(ctx, "a", "b"); err != nil {
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
	clock := hlc.NewClock(hlc.UnixNano, 0)
	testCases := []struct {
		err   error
		retry bool // Expect retry?
	}{
		{roachpb.NewReadWithinUncertaintyIntervalError(hlc.Timestamp{}, hlc.Timestamp{}), true},
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
		db := NewDB(newTestSender(
			func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {

				if _, ok := ba.GetArg(roachpb.Put); ok {
					count++
					if count == 1 {
						return nil, roachpb.NewErrorWithTxn(test.err, ba.Txn)
					}
				}
				return ba.CreateReply(), nil
			}), clock)
		err := db.Txn(context.TODO(), func(ctx context.Context, txn *Txn) error {
			return txn.Put(ctx, "a", "b")
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

// Test that the a txn gets a fresh OrigTimestamp with every retry.
func TestAbortedRetryRenewsTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a TestSender that aborts a transaction 2 times before succeeding.
	mc := hlc.NewManualClock(123)
	clock := hlc.NewClock(mc.UnixNano, time.Nanosecond)
	count := 0
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		if _, ok := ba.GetArg(roachpb.Put); ok {
			mc.Increment(1)
			count++
			if count < 3 {
				return nil, roachpb.NewErrorWithTxn(&roachpb.TransactionAbortedError{}, ba.Txn)
			}
		}
		return ba.CreateReply(), nil
	}), clock)

	txnClosure := func(ctx context.Context, txn *Txn, opt *TxnExecOptions) error {
		// Ensure the KV transaction is created.
		return txn.Put(ctx, "a", "b")
	}

	txn := NewTxn(db)

	// Request a client-defined timestamp.
	refTimestamp := clock.Now()
	execOpt := TxnExecOptions{
		AutoRetry:                  true,
		AutoCommit:                 true,
		AssignTimestampImmediately: true,
	}

	// Perform the transaction.
	if err := txn.Exec(context.Background(), execOpt, txnClosure); err != nil {
		t.Fatal(err)
	}

	// Check the timestamp was preserved.
	if txn.Proto().OrigTimestamp.WallTime == refTimestamp.WallTime {
		t.Errorf("expected txn orig ts to be different than %s", refTimestamp)
	}
}

// TestAbortTransactionOnCommitErrors verifies that transactions are
// aborted on the correct errors.
func TestAbortTransactionOnCommitErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	clock := hlc.NewClock(hlc.UnixNano, 0)

	testCases := []struct {
		err   error
		abort bool
	}{
		{roachpb.NewReadWithinUncertaintyIntervalError(hlc.Timestamp{}, hlc.Timestamp{}), true},
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
					return nil, roachpb.NewErrorWithTxn(test.err, ba.Txn)
				}
				abort = true
			}
			return ba.CreateReply(), nil
		}), clock)

		txn := NewTxn(db)
		if pErr := txn.Put(context.Background(), "a", "b"); pErr != nil {
			t.Fatalf("put failed: %s", pErr)
		}
		if pErr := txn.CommitOrCleanup(context.Background()); pErr == nil {
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

	clock := hlc.NewClock(hlc.UnixNano, 0)
	db := NewDB(newTestSender(nil), clock)
	for _, write := range []bool{true, false} {
		for _, commit := range []bool{true, false} {
			txn := NewTxn(db)

			if _, pErr := txn.Get(context.Background(), "a"); pErr != nil {
				t.Fatal(pErr)
			}
			if write {
				if pErr := txn.Put(context.Background(), "a", "b"); pErr != nil {
					t.Fatal(pErr)
				}
			}
			if commit {
				if pErr := txn.CommitOrCleanup(context.Background()); pErr != nil {
					t.Fatal(pErr)
				}
				if a, e := txn.Proto().Status, roachpb.COMMITTED; a != e {
					t.Errorf("write: %t, commit: %t transaction expected to have status %q but had %q", write, commit, e, a)
				}
			} else {
				if pErr := txn.Rollback(context.Background()); pErr != nil {
					t.Fatal(pErr)
				}
				if a, e := txn.Proto().Status, roachpb.ABORTED; a != e {
					t.Errorf("write: %t, commit: %t transaction expected to have status %q but had %q", write, commit, e, a)
				}
			}
		}
	}
}

func TestCommitInBatchWrongTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	clock := hlc.NewClock(hlc.UnixNano, 0)
	db := NewDB(newTestSender(nil), clock)
	txn := NewTxn(db)

	b1 := &Batch{}
	txn2 := NewTxn(db)
	b2 := txn2.NewBatch()

	for _, b := range []*Batch{b1, b2} {
		if err := txn.CommitInBatch(context.Background(), b); !testutils.IsError(err, "can only be committed by") {
			t.Error(err)
		}
	}
}

// TestTimestampSelectionInOptions verifies that a client can set the
// Txn timestamp using client.TxnExecOptions.
func TestTimestampSelectionInOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mc := hlc.NewManualClock(100)
	clock := hlc.NewClock(mc.UnixNano, time.Nanosecond)
	db := NewDB(newTestSender(nil), clock)
	txn := NewTxn(db)

	execOpt := TxnExecOptions{
		AssignTimestampImmediately: true,
	}
	refTimestamp := clock.Now()

	txnClosure := func(ctx context.Context, txn *Txn, opt *TxnExecOptions) error {
		// Ensure the KV transaction is created.
		return txn.Put(ctx, "a", "b")
	}

	if err := txn.Exec(context.Background(), execOpt, txnClosure); err != nil {
		t.Fatal(err)
	}

	// Check the timestamp was initialized.
	if txn.Proto().OrigTimestamp.WallTime != refTimestamp.WallTime {
		t.Errorf("expected txn orig ts to be %s; got %s", refTimestamp, txn.Proto().OrigTimestamp)
	}
}

// TestSetPriority verifies that the batch UserPriority is correctly set
// depending on the transaction priority.
func TestSetPriority(t *testing.T) {
	defer leaktest.AfterTest(t)()

	clock := hlc.NewClock(hlc.UnixNano, 0)
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
		}), clock)

	// Verify the normal priority setting path.
	expected = roachpb.NormalUserPriority
	txn := NewTxn(db)
	if err := txn.SetUserPriority(expected); err != nil {
		t.Fatal(err)
	}
	if _, pErr := txn.send(context.Background(), roachpb.BatchRequest{}); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify the internal (fixed value) priority setting path.
	expected = roachpb.UserPriority(-13)
	txn = NewTxn(db)
	txn.InternalSetPriority(13)
	if _, pErr := txn.send(context.Background(), roachpb.BatchRequest{}); pErr != nil {
		t.Fatal(pErr)
	}
}

// Tests that a retryable error for an inner txn doesn't cause the outer txn to
// be retried.
func TestWrongTxnRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	clock := hlc.NewClock(hlc.UnixNano, 0)
	db := NewDB(newTestSender(nil), clock)

	var retries int
	txnClosure := func(ctx context.Context, outerTxn *Txn) error {
		log.Infof(ctx, "outer retry")
		retries++
		// Ensure the KV transaction is created.
		if err := outerTxn.Put(ctx, "a", "b"); err != nil {
			t.Fatal(err)
		}
		var execOpt TxnExecOptions
		execOpt.AutoRetry = false
		innerClosure := func(ctx context.Context, innerTxn *Txn, opt *TxnExecOptions) error {
			log.Infof(ctx, "starting inner: %s", innerTxn.Proto())
			// Ensure the KV transaction is created.
			if err := innerTxn.Put(ctx, "x", "y"); err != nil {
				t.Fatal(err)
			}
			return roachpb.NewErrorWithTxn(&roachpb.TransactionPushError{
				PusheeTxn: *outerTxn.Proto()}, innerTxn.Proto()).GoError()
		}
		innerTxn := NewTxn(db)
		err := innerTxn.Exec(ctx, execOpt, innerClosure)
		if !testutils.IsError(err, "failed to push") {
			t.Fatalf("unexpected inner failure: %v", err)
		}
		return err
	}

	if err := db.Txn(context.TODO(), txnClosure); !testutils.IsError(err, "failed to push") {
		t.Fatal(err)
	}
	if retries != 1 {
		t.Fatalf("unexpected retries: %d", retries)
	}
}

func TestBatchMixRawRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	clock := hlc.NewClock(hlc.UnixNano, 0)
	db := NewDB(newTestSender(nil), clock)

	b := &Batch{}
	b.AddRawRequest(&roachpb.EndTransactionRequest{})
	b.Put("x", "y")
	if err := db.Run(context.TODO(), b); !testutils.IsError(err, "non-raw operations") {
		t.Fatal(err)
	}
}

func TestUpdateDeadlineMaybe(t *testing.T) {
	defer leaktest.AfterTest(t)()
	clock := hlc.NewClock(hlc.UnixNano, 0)
	db := NewDB(newTestSender(nil), clock)
	txn := NewTxn(db)

	if txn.deadline != nil {
		t.Errorf("unexpected initial deadline: %s", txn.deadline)
	}

	deadline := hlc.Timestamp{WallTime: 10, Logical: 1}
	if !txn.UpdateDeadlineMaybe(deadline) {
		t.Errorf("expected update, but it didn't happen")
	}
	if d := *txn.deadline; d != deadline {
		t.Errorf("unexpected deadline: %s", d)
	}

	futureDeadline := hlc.Timestamp{WallTime: 11, Logical: 1}
	if txn.UpdateDeadlineMaybe(futureDeadline) {
		t.Errorf("expected no update, but update happened")
	}
	if d := *txn.deadline; d != deadline {
		t.Errorf("unexpected deadline: %s", d)
	}

	pastDeadline := hlc.Timestamp{WallTime: 9, Logical: 1}
	if !txn.UpdateDeadlineMaybe(pastDeadline) {
		t.Errorf("expected update, but it didn't happen")
	}
	if d := *txn.deadline; d != pastDeadline {
		t.Errorf("unexpected deadline: %s", d)
	}
}

// TestConcurrentTxnRequests verifies that multiple requests can be executed on
// a transaction at the same time from multiple goroutines. It makes sure that
// exactly one BeginTxnRequest and one EndTxnRequest are sent.
func TestConcurrentTxnRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()

	clock := hlc.NewClock(hlc.UnixNano, 0)
	var callCountsMu syncutil.Mutex
	callCounts := make(map[roachpb.Method]int)
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		callCountsMu.Lock()
		for _, m := range ba.Methods() {
			callCounts[m]++
		}
		callCountsMu.Unlock()
		return ba.CreateReply(), nil
	}), clock)

	const keys = "abcdefghijklmnopqrstuvwxyz"
	const value = "value"
	if err := db.Txn(context.TODO(), func(ctx context.Context, txn *Txn) error {
		g, gCtx := errgroup.WithContext(ctx)
		for _, keyChar := range keys {
			key := string(keyChar)
			g.Go(func() error {
				return txn.Put(gCtx, key, value)
			})
		}
		return g.Wait()
	}); err != nil {
		t.Fatal(err)
	}

	expectedCallCounts := map[roachpb.Method]int{
		roachpb.BeginTransaction: 1,
		roachpb.Put:              26,
		roachpb.EndTransaction:   1,
	}
	if !reflect.DeepEqual(expectedCallCounts, callCounts) {
		t.Errorf("expected %v, got %v", expectedCallCounts, callCounts)
	}
}
