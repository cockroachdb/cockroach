// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kv

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var (
	testPutResp = roachpb.PutResponse{}
)

// An example of verbose tracing being used to dump a trace around a
// transaction. Use something similar whenever you cannot use
// sql.trace.txn.threshold.
func TestTxnVerboseTrace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tracer := tracing.NewTracer()
	ctx, sp := tracer.StartSpanCtx(context.Background(), "test-txn", tracing.WithRecording(tracing.RecordingVerbose))
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), newTestTxnFactory(nil), clock, stopper)

	if err := db.Txn(ctx, func(ctx context.Context, txn *Txn) error {
		log.Event(ctx, "inside txn")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	log.Event(ctx, "txn complete")
	collectedSpans := sp.FinishAndGetRecording(tracing.RecordingVerbose)
	dump := collectedSpans.String()
	// dump:
	//    0.105ms      0.000ms    event:inside txn
	//    0.275ms      0.171ms    event:client.Txn did AutoCommit. err: <nil>
	//txn: "internal/client/txn_test.go:67 TestTxnVerboseTrace" id=<nil> key=/Min lock=false pri=0.00000000 iso=SERIALIZABLE stat=COMMITTED epo=0 ts=0.000000000,0 orig=0.000000000,0 max=0.000000000,0 wto=false rop=false
	//    0.278ms      0.173ms    event:txn complete
	found, err := regexp.MatchString(
		// The (?s) makes "." match \n. This makes the test resilient to other log
		// lines being interspersed.
		`(?s)`+
			`.*event:[^:]*:\d+ inside txn\n`+
			`.*event:[^:]*:\d+ client\.Txn did AutoCommit\. err: <nil>\n`+
			`.*event:[^:]*:\d+ txn complete.*`,
		dump)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("didn't match: %s", dump)
	}
}

func newTestTxnFactory(
	createReply func(roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error),
) TxnSenderFactory {
	return MakeMockTxnSenderFactory(
		func(
			ctx context.Context, txn *roachpb.Transaction, ba roachpb.BatchRequest,
		) (*roachpb.BatchResponse, *roachpb.Error) {
			if ba.UserPriority == 0 {
				ba.UserPriority = 1
			}

			var br *roachpb.BatchResponse
			var pErr *roachpb.Error

			ba.Txn = txn

			if createReply != nil {
				br, pErr = createReply(ba)
			} else {
				br = ba.CreateReply()
			}
			if pErr != nil {
				return nil, pErr
			}
			status := roachpb.PENDING
			for i, req := range ba.Requests {
				args := req.GetInner()
				if _, ok := args.(*roachpb.PutRequest); ok {
					testPutRespCopy := testPutResp
					union := &br.Responses[i] // avoid operating on copy
					union.MustSetInner(&testPutRespCopy)
				}
			}
			if args, ok := ba.GetArg(roachpb.EndTxn); ok {
				et := args.(*roachpb.EndTxnRequest)
				if et.Commit {
					status = roachpb.COMMITTED
				} else {
					status = roachpb.ABORTED
				}
			}
			if ba.Txn != nil && br.Txn == nil {
				br.Txn = ba.Txn.Clone()
				if pErr == nil {
					br.Txn.Status = status
				}
				// Update the MockTxnSender's proto.
				*txn = *br.Txn
			}
			return br, pErr
		})
}

func TestInitPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	// This test is mostly an excuse to exercise otherwise unused code.
	// TODO(vivekmenezes): update test or remove when InitPut is being
	// considered sufficiently tested and this path exercised.
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), newTestTxnFactory(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		br := ba.CreateReply()
		return br, nil
	}), clock, stopper)

	txn := NewTxn(ctx, db, 0 /* gatewayNodeID */)
	if pErr := txn.InitPut(ctx, "a", "b", false); pErr != nil {
		t.Fatal(pErr)
	}
}

// TestTransactionConfig verifies the proper unwrapping and re-wrapping of the
// client's sender when starting a transaction. Also verifies that the
// UserPriority is propagated to the transactional client and that the admission
// header is set correctly depending on how the transaction is instantiated.
func TestTransactionConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	dbCtx := DefaultDBContext(stopper)
	dbCtx.UserPriority = 101
	db := NewDBWithContext(
		log.MakeTestingAmbientCtxWithNewTracer(),
		newTestTxnFactory(nil), clock, dbCtx)
	for _, tc := range []struct {
		label               string
		txnCreator          func(context.Context, func(context.Context, *Txn) error) error
		wantAdmissionHeader roachpb.AdmissionHeader_Source
	}{
		{
			label:               "source is other",
			txnCreator:          db.Txn,
			wantAdmissionHeader: roachpb.AdmissionHeader_OTHER,
		},
		{
			label:               "source is root kv",
			txnCreator:          db.TxnRootKV,
			wantAdmissionHeader: roachpb.AdmissionHeader_ROOT_KV,
		},
	} {
		if err := tc.txnCreator(context.Background(), func(ctx context.Context, txn *Txn) error {
			if txn.db.ctx.UserPriority != db.ctx.UserPriority {
				t.Errorf("expected txn user priority %f; got %f",
					db.ctx.UserPriority, txn.db.ctx.UserPriority)
			}
			if txn.admissionHeader.Source != tc.wantAdmissionHeader {
				t.Errorf("expected txn source %d; got %d", tc.wantAdmissionHeader, txn.admissionHeader.Source)
			}
			return nil
		}); err != nil {
			t.Errorf("unexpected error on commit: %s", err)
		}
	}
}

// TestCommitTransactionOnce verifies that if the transaction is
// ended explicitly in the retryable func, it is not automatically
// ended a second time at completion of retryable func.
func TestCommitTransactionOnce(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	count := 0
	db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), newTestTxnFactory(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		count++
		return ba.CreateReply(), nil
	}), clock, stopper)
	if err := db.Txn(context.Background(), func(ctx context.Context, txn *Txn) error {
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

// TestAbortMutatingTransaction verifies that transaction is aborted
// upon failed invocation of the retryable func.
func TestAbortMutatingTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	var calls []roachpb.Method
	db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), newTestTxnFactory(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		calls = append(calls, ba.Methods()...)
		if et, ok := ba.GetArg(roachpb.EndTxn); ok && et.(*roachpb.EndTxnRequest).Commit {
			t.Errorf("expected commit to be false")
		}
		return ba.CreateReply(), nil
	}), clock, stopper)

	if err := db.Txn(context.Background(), func(ctx context.Context, txn *Txn) error {
		if err := txn.Put(ctx, "a", "b"); err != nil {
			return err
		}
		return errors.Errorf("foo")
	}); err == nil {
		t.Error("expected error on abort")
	}
	expectedCalls := []roachpb.Method{roachpb.Put, roachpb.EndTxn}
	if !reflect.DeepEqual(expectedCalls, calls) {
		t.Errorf("expected %s, got %s", expectedCalls, calls)
	}
}

// TestRunTransactionRetryOnErrors verifies that the transaction
// is retried on the correct errors.
//
// TODO(andrei): This test is probably not actually testing much, the mock
// sender implementation recognizes the retryable errors. We should give this
// test a TxnCoordSender instead.
func TestRunTransactionRetryOnErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	testCases := []struct {
		err   error
		retry bool // Expect retry?
	}{
		{roachpb.NewReadWithinUncertaintyIntervalError(hlc.Timestamp{}, hlc.Timestamp{}, hlc.Timestamp{}, nil), true},
		{&roachpb.TransactionAbortedError{}, true},
		{&roachpb.TransactionPushError{}, true},
		{&roachpb.TransactionRetryError{}, true},
		{&roachpb.WriteTooOldError{}, true},
		{&roachpb.RangeNotFoundError{}, false},
		{&roachpb.RangeKeyMismatchError{}, false},
		{&roachpb.TransactionStatusError{}, false},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("%T", test.err), func(t *testing.T) {
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			count := 0
			db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), newTestTxnFactory(
				func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {

					if _, ok := ba.GetArg(roachpb.Put); ok {
						count++
						if count == 1 {
							var pErr *roachpb.Error
							if errors.HasType(test.err, (*roachpb.ReadWithinUncertaintyIntervalError)(nil)) {
								// This error requires an observed timestamp to have been
								// recorded on the origin node.
								ba.Txn.UpdateObservedTimestamp(1, hlc.ClockTimestamp{WallTime: 1, Logical: 1})
								pErr = roachpb.NewErrorWithTxn(test.err, ba.Txn)
								pErr.OriginNode = 1
							} else {
								pErr = roachpb.NewErrorWithTxn(test.err, ba.Txn)
							}

							if pErr.TransactionRestart() != roachpb.TransactionRestart_NONE {
								// HACK ALERT: to do without a TxnCoordSender, we jump through
								// hoops to get the retryable error expected by db.Txn().
								return nil, roachpb.NewError(roachpb.NewTransactionRetryWithProtoRefreshError(
									"foo", ba.Txn.ID, *ba.Txn))
							}
							return nil, pErr
						}
					}
					return ba.CreateReply(), nil
				}), clock, stopper)
			err := db.Txn(context.Background(), func(ctx context.Context, txn *Txn) error {
				return txn.Put(ctx, "a", "b")
			})
			if test.retry {
				if err != nil {
					t.Fatalf("expected success on retry; got %s", err)
				}
				if count != 2 {
					t.Fatalf("expected one retry; got %d", count-1)
				}
			} else {
				if count != 1 {
					t.Errorf("expected no retries; got %d", count)
				}
				if reflect.TypeOf(err) != reflect.TypeOf(test.err) {
					t.Errorf("expected error of type %T; got %T", test.err, err)
				}
			}
		})
	}
}

// TestTransactionStatus verifies that transactions always have their
// status updated correctly.
func TestTransactionStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), newTestTxnFactory(nil), clock, stopper)
	for _, write := range []bool{true, false} {
		for _, commit := range []bool{true, false} {
			txn := NewTxn(ctx, db, 0 /* gatewayNodeID */)

			if _, pErr := txn.Get(ctx, "a"); pErr != nil {
				t.Fatal(pErr)
			}
			if write {
				if pErr := txn.Put(ctx, "a", "b"); pErr != nil {
					t.Fatal(pErr)
				}
			}
			if commit {
				if pErr := txn.CommitOrCleanup(ctx); pErr != nil {
					t.Fatal(pErr)
				}
				if a, e := txn.TestingCloneTxn().Status, roachpb.COMMITTED; a != e {
					t.Errorf("write: %t, commit: %t transaction expected to have status %q but had %q", write, commit, e, a)
				}
			} else {
				if pErr := txn.Rollback(ctx); pErr != nil {
					t.Fatal(pErr)
				}
				if a, e := txn.TestingCloneTxn().Status, roachpb.ABORTED; a != e {
					t.Errorf("write: %t, commit: %t transaction expected to have status %q but had %q", write, commit, e, a)
				}
			}
		}
	}
}

func TestCommitInBatchWrongTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), newTestTxnFactory(nil), clock, stopper)
	txn := NewTxn(ctx, db, 0 /* gatewayNodeID */)

	b1 := &Batch{}
	txn2 := NewTxn(ctx, db, 0 /* gatewayNodeID */)
	b2 := txn2.NewBatch()

	for _, b := range []*Batch{b1, b2} {
		if err := txn.CommitInBatch(ctx, b); !testutils.IsError(err, "can only be committed by") {
			t.Error(err)
		}
	}
}

// TestSetPriority verifies that the batch UserPriority is correctly set
// depending on the transaction priority.
func TestSetPriority(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	var expected roachpb.UserPriority
	db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), newTestTxnFactory(
		func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			if ba.UserPriority != expected {
				pErr := roachpb.NewErrorf("Priority not set correctly in the batch! "+
					"(expected: %s, value: %s)", expected, ba.UserPriority)
				return nil, pErr
			}

			br := &roachpb.BatchResponse{}
			br.Txn.Update(ba.Txn) // copy
			return br, nil
		}), clock, stopper)

	// Verify the normal priority setting path.
	expected = roachpb.NormalUserPriority
	txn := NewTxn(ctx, db, 0 /* gatewayNodeID */)
	if err := txn.SetUserPriority(expected); err != nil {
		t.Fatal(err)
	}
	if _, pErr := txn.Send(ctx, roachpb.BatchRequest{}); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify the internal (fixed value) priority setting path.
	expected = roachpb.UserPriority(-13)
	txn = NewTxn(ctx, db, 0 /* gatewayNodeID */)
	txn.TestingSetPriority(13)
	if _, pErr := txn.Send(ctx, roachpb.BatchRequest{}); pErr != nil {
		t.Fatal(pErr)
	}
}

// Tests that a retryable error for an inner txn doesn't cause the outer txn to
// be retried.
func TestWrongTxnRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), newTestTxnFactory(nil), clock, stopper)

	var retries int
	txnClosure := func(ctx context.Context, outerTxn *Txn) error {
		log.Infof(ctx, "outer retry")
		retries++
		// Ensure the KV transaction is created.
		if err := outerTxn.Put(ctx, "a", "b"); err != nil {
			t.Fatal(err)
		}
		// Simulate an inner txn by generating an error with a bogus txn id.
		return roachpb.NewTransactionRetryWithProtoRefreshError("test error", uuid.MakeV4(), roachpb.Transaction{})
	}

	if err := db.Txn(context.Background(), txnClosure); !testutils.IsError(err, "test error") {
		t.Fatal(err)
	}
	if retries != 1 {
		t.Fatalf("unexpected retries: %d", retries)
	}
}

func TestBatchMixRawRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), newTestTxnFactory(nil), clock, stopper)

	b := &Batch{}
	b.AddRawRequest(&roachpb.EndTxnRequest{})
	b.Put("x", "y")
	if err := db.Run(context.Background(), b); !testutils.IsError(err, "non-raw operations") {
		t.Fatal(err)
	}
}

func TestUpdateDeadlineMaybe(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	mc := hlc.NewManualClock(1)
	clock := hlc.NewClock(mc.UnixNano, time.Nanosecond)
	db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), MakeMockTxnSenderFactory(
		func(context.Context, *roachpb.Transaction, roachpb.BatchRequest,
		) (*roachpb.BatchResponse, *roachpb.Error) {
			return nil, nil
		}), clock, stopper)
	txn := NewTxn(ctx, db, 0 /* gatewayNodeID */)

	if txn.deadline() != nil {
		t.Errorf("unexpected initial deadline: %s", txn.deadline())
	}

	deadline := hlc.Timestamp{WallTime: 10, Logical: 1}
	err := txn.UpdateDeadline(ctx, deadline)
	require.NoError(t, err, "Deadline update failed")
	if d := *txn.deadline(); d != deadline {
		t.Errorf("unexpected deadline: %s", d)
	}

	// Deadline is always updated now, there is no
	// maybe.
	futureDeadline := hlc.Timestamp{WallTime: 11, Logical: 1}
	err = txn.UpdateDeadline(ctx, futureDeadline)
	require.NoError(t, err, "Future deadline update failed")
	if d := *txn.deadline(); d != futureDeadline {
		t.Errorf("unexpected deadline: %s", d)
	}

	pastDeadline := hlc.Timestamp{WallTime: 9, Logical: 1}
	err = txn.UpdateDeadline(ctx, pastDeadline)
	require.NoError(t, err, "Past deadline update failed")
	if d := *txn.deadline(); d != pastDeadline {
		t.Errorf("unexpected deadline: %s", d)
	}
}

// Test that, if DeprecatedSetSystemConfigTrigger() fails, the systemConfigTrigger has not
// been set.
func TestAnchoringErrorNoTrigger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	mc := hlc.NewManualClock(1)
	clock := hlc.NewClock(mc.UnixNano, time.Nanosecond)
	db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), MakeMockTxnSenderFactory(
		func(context.Context, *roachpb.Transaction, roachpb.BatchRequest,
		) (*roachpb.BatchResponse, *roachpb.Error) {
			return nil, nil
		}), clock, stopper)
	txn := NewTxn(ctx, db, 0 /* gatewayNodeID */)
	require.EqualError(t, txn.DeprecatedSetSystemConfigTrigger(true /* forSystemTenant */), "unimplemented")
	require.False(t, txn.systemConfigTrigger)
}

// TestTxnNegotiateAndSend tests the behavior of NegotiateAndSend, both when the
// server-side fast path is possible (for single-range reads) and when it is not
// (for cross-range reads).
func TestTxnNegotiateAndSend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	testutils.RunTrueAndFalse(t, "fast-path", func(t *testing.T, fastPath bool) {
		ts10 := hlc.Timestamp{WallTime: 10}
		ts20 := hlc.Timestamp{WallTime: 20}
		mc := hlc.NewManualClock(1)
		clock := hlc.NewClock(mc.UnixNano, time.Nanosecond)
		txnSender := MakeMockTxnSenderFactoryWithNonTxnSender(nil /* senderFunc */, func(
			_ context.Context, ba roachpb.BatchRequest,
		) (*roachpb.BatchResponse, *roachpb.Error) {
			require.NotNil(t, ba.BoundedStaleness)
			require.Equal(t, ts10, ba.BoundedStaleness.MinTimestampBound)
			require.False(t, ba.BoundedStaleness.MinTimestampBoundStrict)
			require.Zero(t, ba.BoundedStaleness.MaxTimestampBound)

			if !fastPath {
				return nil, roachpb.NewError(&roachpb.OpRequiresTxnError{})
			}
			br := ba.CreateReply()
			br.Timestamp = ts20
			return br, nil
		})
		db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), txnSender, clock, stopper)
		txn := NewTxn(ctx, db, 0 /* gatewayNodeID */)

		var ba roachpb.BatchRequest
		ba.BoundedStaleness = &roachpb.BoundedStalenessHeader{
			MinTimestampBound: ts10,
		}
		ba.RoutingPolicy = roachpb.RoutingPolicy_NEAREST
		ba.Add(roachpb.NewGet(roachpb.Key("a"), false))
		br, pErr := txn.NegotiateAndSend(ctx, ba)

		if fastPath {
			require.Nil(t, pErr)
			require.NotNil(t, br)
			require.Equal(t, ts20, br.Timestamp)
			require.True(t, txn.CommitTimestampFixed())
			require.Equal(t, ts20, txn.CommitTimestamp())
		} else {
			require.Nil(t, br)
			require.NotNil(t, pErr)
			require.Regexp(t, "unimplemented: cross-range bounded staleness reads not yet implemented", pErr)
			require.False(t, txn.CommitTimestampFixed())
		}
	})
}

// TestTxnNegotiateAndSendWithDeadline tests the behavior of NegotiateAndSend
// when the transaction has a deadline.
func TestTxnNegotiateAndSendWithDeadline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	ts10 := hlc.Timestamp{WallTime: 10}
	ts20 := hlc.Timestamp{WallTime: 20}
	ts30 := hlc.Timestamp{WallTime: 30}
	ts40 := hlc.Timestamp{WallTime: 40}
	minTSBound := ts20

	for _, test := range []struct {
		name        string
		txnDeadline hlc.Timestamp
		maxTSBound  hlc.Timestamp

		expMaxTS hlc.Timestamp
		expErr   string
	}{
		{
			name:        "no max timestamp bound",
			txnDeadline: ts30,
			expMaxTS:    ts30,
		},
		{
			name:        "earlier max timestamp bound",
			txnDeadline: ts40,
			maxTSBound:  ts30,
			expMaxTS:    ts30,
		},
		{
			name:        "equal max timestamp bound",
			txnDeadline: ts40,
			maxTSBound:  ts40,
			expMaxTS:    ts40,
		},
		{
			name:        "later max timestamp bound",
			txnDeadline: ts30,
			maxTSBound:  ts40,
			expMaxTS:    ts30,
		},
		{
			name:        "txn deadline equal to min timestamp bound",
			txnDeadline: ts20,
			expErr:      "transaction deadline .* equal to or below min_timestamp_bound .*",
		},
		{
			name:        "txn deadline less than min timestamp bound",
			txnDeadline: ts10,
			expErr:      "transaction deadline .* equal to or below min_timestamp_bound .*",
		},
		{
			name:        "max timestamp bound equal to min timestamp bound",
			txnDeadline: ts30,
			maxTSBound:  ts20,
			expErr:      "max_timestamp_bound, if set, must be greater than min_timestamp_bound",
		},
		{
			name:        "max timestamp bound less than min timestamp bound",
			txnDeadline: ts30,
			maxTSBound:  ts10,
			expErr:      "max_timestamp_bound, if set, must be greater than min_timestamp_bound",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mc := hlc.NewManualClock(1)
			clock := hlc.NewClock(mc.UnixNano, time.Nanosecond)
			txnSender := MakeMockTxnSenderFactoryWithNonTxnSender(nil /* senderFunc */, func(
				_ context.Context, ba roachpb.BatchRequest,
			) (*roachpb.BatchResponse, *roachpb.Error) {
				require.NotNil(t, ba.BoundedStaleness)
				require.Equal(t, minTSBound, ba.BoundedStaleness.MinTimestampBound)
				require.False(t, ba.BoundedStaleness.MinTimestampBoundStrict)
				require.Equal(t, test.expMaxTS, ba.BoundedStaleness.MaxTimestampBound)

				br := ba.CreateReply()
				br.Timestamp = minTSBound
				return br, nil
			})
			db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), txnSender, clock, stopper)
			txn := NewTxn(ctx, db, 0 /* gatewayNodeID */)
			require.NoError(t, txn.UpdateDeadline(ctx, test.txnDeadline))

			var ba roachpb.BatchRequest
			ba.BoundedStaleness = &roachpb.BoundedStalenessHeader{
				MinTimestampBound: minTSBound,
				MaxTimestampBound: test.maxTSBound,
			}
			ba.RoutingPolicy = roachpb.RoutingPolicy_NEAREST
			ba.Add(roachpb.NewGet(roachpb.Key("a"), false))
			br, pErr := txn.NegotiateAndSend(ctx, ba)

			if test.expErr == "" {
				require.Nil(t, pErr)
				require.NotNil(t, br)
				require.Equal(t, minTSBound, br.Timestamp)
				require.True(t, txn.CommitTimestampFixed())
				require.Equal(t, minTSBound, txn.CommitTimestamp())
			} else {
				require.Nil(t, br)
				require.NotNil(t, pErr)
				require.Regexp(t, test.expErr, pErr)
				require.False(t, txn.CommitTimestampFixed())
			}
		})
	}
}

// TestTxnNegotiateAndSendWithResumeSpan tests that a bounded staleness read
// request performed using NegotiateAndSend negotiates a timestamp over the
// provided batch's entire set of read spans even if it only performs reads and
// returns results from part of them due to a key/byte limit. It then uses this
// negotiated timestamp to fix its transaction's commit timestamp.
func TestTxnNegotiateAndSendWithResumeSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	testutils.RunTrueAndFalse(t, "fast-path", func(t *testing.T, fastPath bool) {
		ts10 := hlc.Timestamp{WallTime: 10}
		ts20 := hlc.Timestamp{WallTime: 20}
		mc := hlc.NewManualClock(1)
		clock := hlc.NewClock(mc.UnixNano, time.Nanosecond)
		txnSender := MakeMockTxnSenderFactoryWithNonTxnSender(nil /* senderFunc */, func(
			_ context.Context, ba roachpb.BatchRequest,
		) (*roachpb.BatchResponse, *roachpb.Error) {
			require.NotNil(t, ba.BoundedStaleness)
			require.Equal(t, ts10, ba.BoundedStaleness.MinTimestampBound)
			require.False(t, ba.BoundedStaleness.MinTimestampBoundStrict)
			require.Zero(t, ba.BoundedStaleness.MaxTimestampBound)
			require.Equal(t, int64(2), ba.MaxSpanRequestKeys)

			if !fastPath {
				return nil, roachpb.NewError(&roachpb.OpRequiresTxnError{})
			}
			br := ba.CreateReply()
			br.Timestamp = ts20
			scanResp := br.Responses[0].GetScan()
			scanResp.Rows = []roachpb.KeyValue{
				{Key: roachpb.Key("a")},
				{Key: roachpb.Key("b")},
			}
			scanResp.ResumeSpan = &roachpb.Span{
				Key:    roachpb.Key("c"),
				EndKey: roachpb.Key("d"),
			}
			scanResp.ResumeReason = roachpb.RESUME_KEY_LIMIT
			return br, nil
		})
		db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), txnSender, clock, stopper)
		txn := NewTxn(ctx, db, 0 /* gatewayNodeID */)

		var ba roachpb.BatchRequest
		ba.BoundedStaleness = &roachpb.BoundedStalenessHeader{
			MinTimestampBound: ts10,
		}
		ba.RoutingPolicy = roachpb.RoutingPolicy_NEAREST
		ba.MaxSpanRequestKeys = 2
		ba.Add(roachpb.NewScan(roachpb.Key("a"), roachpb.Key("d"), false /* forUpdate */))
		br, pErr := txn.NegotiateAndSend(ctx, ba)

		if fastPath {
			require.Nil(t, pErr)
			require.NotNil(t, br)
			// The negotiated timestamp should be returned and fixed.
			require.Equal(t, ts20, br.Timestamp)
			require.True(t, txn.CommitTimestampFixed())
			require.Equal(t, ts20, txn.CommitTimestamp())
			// Even though the response is paginated and carries a resume span.
			require.Len(t, br.Responses, 1)
			scanResp := br.Responses[0].GetScan()
			require.Len(t, scanResp.Rows, 2)
			require.NotNil(t, scanResp.ResumeSpan)
			require.Equal(t, roachpb.Key("c"), scanResp.ResumeSpan.Key)
			require.Equal(t, roachpb.Key("d"), scanResp.ResumeSpan.EndKey)
			require.Equal(t, roachpb.RESUME_KEY_LIMIT, scanResp.ResumeReason)
		} else {
			require.Nil(t, br)
			require.NotNil(t, pErr)
			require.Regexp(t, "unimplemented: cross-range bounded staleness reads not yet implemented", pErr)
			require.False(t, txn.CommitTimestampFixed())
		}
	})
}
