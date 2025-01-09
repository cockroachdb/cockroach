// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kv

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var (
	testPutResp = kvpb.PutResponse{}
)

// An example of verbose tracing being used to dump a trace around a
// transaction. Use something similar whenever you cannot use
// sql.trace.txn.threshold.
func TestTxnVerboseTrace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tracer := tracing.NewTracer()
	ctx, sp := tracer.StartSpanCtx(context.Background(), "test-txn", tracing.WithRecording(tracingpb.RecordingVerbose))
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClockForTesting(nil)
	db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), newTestTxnFactory(nil), clock, stopper)

	if err := db.Txn(ctx, func(ctx context.Context, txn *Txn) error {
		log.Event(ctx, "inside txn")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	log.Event(ctx, "txn complete")
	collectedSpans := sp.FinishAndGetRecording(tracingpb.RecordingVerbose)
	dump := collectedSpans.String()
	// dump:
	//    0.105ms      0.000ms    event:inside txn
	//    0.275ms      0.171ms    event:kv.Txn did AutoCommit. err: <nil>
	//    0.278ms      0.173ms    event:txn complete
	found, err := regexp.MatchString(
		// The (?s) makes "." match \n. This makes the test resilient to other log
		// lines being interspersed.
		`(?s)`+
			`.*event:[^:]*:\d+ inside txn\n`+
			`.*event:[^:]*:\d+ kv\.Txn did AutoCommit\. err: <nil>\n`+
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
	createReply func(*kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error),
) TxnSenderFactory {
	return MakeMockTxnSenderFactory(
		func(
			ctx context.Context, txn *roachpb.Transaction, ba *kvpb.BatchRequest,
		) (*kvpb.BatchResponse, *kvpb.Error) {
			if ba.UserPriority == 0 {
				ba.UserPriority = 1
			}

			var br *kvpb.BatchResponse
			var pErr *kvpb.Error

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
				if _, ok := args.(*kvpb.PutRequest); ok {
					testPutRespCopy := testPutResp
					union := &br.Responses[i] // avoid operating on copy
					union.MustSetInner(&testPutRespCopy)
				}
			}
			if args, ok := ba.GetArg(kvpb.EndTxn); ok {
				et := args.(*kvpb.EndTxnRequest)
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
	st := cluster.MakeTestingClusterSettings()
	clock := hlc.NewClockForTesting(nil)
	dbCtx := DefaultDBContext(st, stopper)
	dbCtx.UserPriority = 101
	db := NewDBWithContext(
		log.MakeTestingAmbientCtxWithNewTracer(),
		newTestTxnFactory(nil), clock, dbCtx)
	for _, tc := range []struct {
		label               string
		txnCreator          func(context.Context, func(context.Context, *Txn) error) error
		wantAdmissionHeader kvpb.AdmissionHeader_Source
	}{
		{
			label:               "source is other",
			txnCreator:          db.Txn,
			wantAdmissionHeader: kvpb.AdmissionHeader_OTHER,
		},
		{
			label:               "source is root kv",
			txnCreator:          db.TxnRootKV,
			wantAdmissionHeader: kvpb.AdmissionHeader_ROOT_KV,
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
	clock := hlc.NewClockForTesting(nil)
	count := 0
	db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), newTestTxnFactory(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
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
	clock := hlc.NewClockForTesting(nil)
	var calls []kvpb.Method
	db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), newTestTxnFactory(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		calls = append(calls, ba.Methods()...)
		if et, ok := ba.GetArg(kvpb.EndTxn); ok && et.(*kvpb.EndTxnRequest).Commit {
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
	expectedCalls := []kvpb.Method{kvpb.Put, kvpb.EndTxn}
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
	clock := hlc.NewClockForTesting(nil)
	testCases := []struct {
		err   error
		retry bool // Expect retry?
	}{
		{kvpb.NewReadWithinUncertaintyIntervalError(hlc.Timestamp{}, hlc.ClockTimestamp{}, nil, hlc.Timestamp{}, hlc.ClockTimestamp{}), true},
		{&kvpb.TransactionAbortedError{}, true},
		{&kvpb.TransactionPushError{}, true},
		{&kvpb.TransactionRetryError{}, true},
		{&kvpb.WriteTooOldError{}, true},
		{&kvpb.RangeNotFoundError{}, false},
		{&kvpb.RangeKeyMismatchError{}, false},
		{&kvpb.TransactionStatusError{}, false},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("%T", test.err), func(t *testing.T) {
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			count := 0
			db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), newTestTxnFactory(
				func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {

					if _, ok := ba.GetArg(kvpb.Put); ok {
						count++
						if count == 1 {
							var pErr *kvpb.Error
							if errors.HasType(test.err, (*kvpb.ReadWithinUncertaintyIntervalError)(nil)) {
								// This error requires an observed timestamp to have been
								// recorded on the origin node.
								ba.Txn.UpdateObservedTimestamp(1, hlc.ClockTimestamp{WallTime: 1, Logical: 1})
								pErr = kvpb.NewErrorWithTxn(test.err, ba.Txn)
								pErr.OriginNode = 1
							} else {
								pErr = kvpb.NewErrorWithTxn(test.err, ba.Txn)
							}

							if pErr.TransactionRestart() != kvpb.TransactionRestart_NONE {
								// HACK ALERT: to do without a TxnCoordSender, we jump through
								// hoops to get the retryable error expected by db.Txn().
								return nil, kvpb.NewError(kvpb.NewTransactionRetryWithProtoRefreshError(
									"foo", ba.Txn.ID, ba.Txn.Epoch, *ba.Txn))
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

	clock := hlc.NewClockForTesting(nil)
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
				require.NoError(t, txn.Commit(ctx))
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

	clock := hlc.NewClockForTesting(nil)
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

	clock := hlc.NewClockForTesting(nil)
	var expected roachpb.UserPriority
	db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), newTestTxnFactory(
		func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			if ba.UserPriority != expected {
				pErr := kvpb.NewErrorf("Priority not set correctly in the batch! "+
					"(expected: %s, value: %s)", expected, ba.UserPriority)
				return nil, pErr
			}

			br := &kvpb.BatchResponse{}
			br.Txn.Update(ba.Txn) // copy
			return br, nil
		}), clock, stopper)

	// Verify the normal priority setting path.
	expected = roachpb.NormalUserPriority
	txn := NewTxn(ctx, db, 0 /* gatewayNodeID */)
	if err := txn.SetUserPriority(expected); err != nil {
		t.Fatal(err)
	}
	if _, pErr := txn.Send(ctx, &kvpb.BatchRequest{}); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify the internal (fixed value) priority setting path.
	expected = roachpb.UserPriority(-13)
	txn = NewTxn(ctx, db, 0 /* gatewayNodeID */)
	txn.TestingSetPriority(13)
	if _, pErr := txn.Send(ctx, &kvpb.BatchRequest{}); pErr != nil {
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
	clock := hlc.NewClockForTesting(nil)
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
		return kvpb.NewTransactionRetryWithProtoRefreshError("test error", uuid.MakeV4(), 0, roachpb.Transaction{})
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

	clock := hlc.NewClockForTesting(nil)
	db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), newTestTxnFactory(nil), clock, stopper)

	b := &Batch{}
	b.AddRawRequest(&kvpb.EndTxnRequest{})
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

	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 1)))
	db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), MakeMockTxnSenderFactory(
		func(context.Context, *roachpb.Transaction, *kvpb.BatchRequest,
		) (*kvpb.BatchResponse, *kvpb.Error) {
			return nil, nil
		}), clock, stopper)
	txn := NewTxn(ctx, db, 0 /* gatewayNodeID */)

	if !txn.deadline().IsEmpty() {
		t.Errorf("unexpected initial deadline: %s", txn.deadline())
	}

	deadline := hlc.Timestamp{WallTime: 10, Logical: 1}
	err := txn.UpdateDeadline(ctx, deadline)
	require.NoError(t, err, "Deadline update failed")
	if d := txn.deadline(); d != deadline {
		t.Errorf("unexpected deadline: %s", d)
	}

	// Deadline is always updated now, there is no
	// maybe.
	futureDeadline := hlc.Timestamp{WallTime: 11, Logical: 1}
	err = txn.UpdateDeadline(ctx, futureDeadline)
	require.NoError(t, err, "Future deadline update failed")
	if d := txn.deadline(); d != futureDeadline {
		t.Errorf("unexpected deadline: %s", d)
	}

	pastDeadline := hlc.Timestamp{WallTime: 9, Logical: 1}
	err = txn.UpdateDeadline(ctx, pastDeadline)
	require.NoError(t, err, "Past deadline update failed")
	if d := txn.deadline(); d != pastDeadline {
		t.Errorf("unexpected deadline: %s", d)
	}
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
		clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 1)))
		txnSender := MakeMockTxnSenderFactoryWithNonTxnSender(nil /* senderFunc */, func(
			_ context.Context, ba *kvpb.BatchRequest,
		) (*kvpb.BatchResponse, *kvpb.Error) {
			require.NotNil(t, ba.BoundedStaleness)
			require.Equal(t, ts10, ba.BoundedStaleness.MinTimestampBound)
			require.False(t, ba.BoundedStaleness.MinTimestampBoundStrict)
			require.Zero(t, ba.BoundedStaleness.MaxTimestampBound)

			if !fastPath {
				return nil, kvpb.NewError(&kvpb.OpRequiresTxnError{})
			}
			br := ba.CreateReply()
			br.Timestamp = ts20
			return br, nil
		})
		db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), txnSender, clock, stopper)
		txn := NewTxn(ctx, db, 0 /* gatewayNodeID */)

		ba := &kvpb.BatchRequest{}
		ba.BoundedStaleness = &kvpb.BoundedStalenessHeader{
			MinTimestampBound: ts10,
		}
		ba.RoutingPolicy = kvpb.RoutingPolicy_NEAREST
		ba.Add(kvpb.NewGet(roachpb.Key("a")))
		br, pErr := txn.NegotiateAndSend(ctx, ba)

		if fastPath {
			require.Nil(t, pErr)
			require.NotNil(t, br)
			require.Equal(t, ts20, br.Timestamp)
			require.True(t, txn.ReadTimestampFixed())
			require.Equal(t, ts20, txn.ReadTimestamp())
		} else {
			require.Nil(t, br)
			require.NotNil(t, pErr)
			require.Regexp(t, "unimplemented: cross-range bounded staleness reads not yet implemented", pErr)
			require.False(t, txn.ReadTimestampFixed())
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
			clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 1)))
			txnSender := MakeMockTxnSenderFactoryWithNonTxnSender(nil /* senderFunc */, func(
				_ context.Context, ba *kvpb.BatchRequest,
			) (*kvpb.BatchResponse, *kvpb.Error) {
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

			ba := &kvpb.BatchRequest{}
			ba.BoundedStaleness = &kvpb.BoundedStalenessHeader{
				MinTimestampBound: minTSBound,
				MaxTimestampBound: test.maxTSBound,
			}
			ba.RoutingPolicy = kvpb.RoutingPolicy_NEAREST
			ba.Add(kvpb.NewGet(roachpb.Key("a")))
			br, pErr := txn.NegotiateAndSend(ctx, ba)

			if test.expErr == "" {
				require.Nil(t, pErr)
				require.NotNil(t, br)
				require.Equal(t, minTSBound, br.Timestamp)
				require.True(t, txn.ReadTimestampFixed())
				require.Equal(t, minTSBound, txn.ReadTimestamp())
			} else {
				require.Nil(t, br)
				require.NotNil(t, pErr)
				require.Regexp(t, test.expErr, pErr)
				require.False(t, txn.ReadTimestampFixed())
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
		clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 1)))
		txnSender := MakeMockTxnSenderFactoryWithNonTxnSender(nil /* senderFunc */, func(
			_ context.Context, ba *kvpb.BatchRequest,
		) (*kvpb.BatchResponse, *kvpb.Error) {
			require.NotNil(t, ba.BoundedStaleness)
			require.Equal(t, ts10, ba.BoundedStaleness.MinTimestampBound)
			require.False(t, ba.BoundedStaleness.MinTimestampBoundStrict)
			require.Zero(t, ba.BoundedStaleness.MaxTimestampBound)
			require.Equal(t, int64(2), ba.MaxSpanRequestKeys)

			if !fastPath {
				return nil, kvpb.NewError(&kvpb.OpRequiresTxnError{})
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
			scanResp.ResumeReason = kvpb.RESUME_KEY_LIMIT
			return br, nil
		})
		db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), txnSender, clock, stopper)
		txn := NewTxn(ctx, db, 0 /* gatewayNodeID */)

		ba := &kvpb.BatchRequest{}
		ba.BoundedStaleness = &kvpb.BoundedStalenessHeader{
			MinTimestampBound: ts10,
		}
		ba.RoutingPolicy = kvpb.RoutingPolicy_NEAREST
		ba.MaxSpanRequestKeys = 2
		ba.Add(kvpb.NewScan(roachpb.Key("a"), roachpb.Key("d")))
		br, pErr := txn.NegotiateAndSend(ctx, ba)

		if fastPath {
			require.Nil(t, pErr)
			require.NotNil(t, br)
			// The negotiated timestamp should be returned and fixed.
			require.Equal(t, ts20, br.Timestamp)
			require.True(t, txn.ReadTimestampFixed())
			require.Equal(t, ts20, txn.ReadTimestamp())
			// Even though the response is paginated and carries a resume span.
			require.Len(t, br.Responses, 1)
			scanResp := br.Responses[0].GetScan()
			require.Len(t, scanResp.Rows, 2)
			require.NotNil(t, scanResp.ResumeSpan)
			require.Equal(t, roachpb.Key("c"), scanResp.ResumeSpan.Key)
			require.Equal(t, roachpb.Key("d"), scanResp.ResumeSpan.EndKey)
			require.Equal(t, kvpb.RESUME_KEY_LIMIT, scanResp.ResumeReason)
		} else {
			require.Nil(t, br)
			require.NotNil(t, pErr)
			require.Regexp(t, "unimplemented: cross-range bounded staleness reads not yet implemented", pErr)
			require.False(t, txn.ReadTimestampFixed())
		}
	})
}

// TestTxnCommitTriggers tests the behavior of invoking commit triggers, as part
// of a Commit or a manual EndTxnRequest that includes a commit.
func TestTxnCommitTriggers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	for _, test := range []struct {
		name string
		// A function that specifies how a transaction ends.
		endTxnFn func(txn *Txn) error
		// Assuming a trigger bool value starts off as false, expTrigger is the
		// expected value of the trigger after the transaction ends.
		expTrigger bool
	}{
		{
			name:       "explicit commit",
			endTxnFn:   func(txn *Txn) error { return txn.Commit(ctx) },
			expTrigger: true,
		},
		{
			name: "manual commit",
			endTxnFn: func(txn *Txn) error {
				b := txn.NewBatch()
				b.AddRawRequest(&kvpb.EndTxnRequest{Commit: true})
				return txn.Run(ctx, b)
			},
			expTrigger: true,
		},
		{
			name: "manual abort",
			endTxnFn: func(txn *Txn) error {
				b := txn.NewBatch()
				b.AddRawRequest(&kvpb.EndTxnRequest{Commit: false})
				return txn.Run(ctx, b)
			},
			expTrigger: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			clock := hlc.NewClockForTesting(nil)
			db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(), newTestTxnFactory(nil), clock, stopper)
			txn := NewTxn(ctx, db, 0 /* gatewayNodeID */)
			triggerVal := false
			triggerFn := func(ctx context.Context) { triggerVal = true }
			txn.AddCommitTrigger(triggerFn)
			err := test.endTxnFn(txn)
			require.NoError(t, err)
			require.Equal(t, test.expTrigger, triggerVal)
		})
	}
}

type txnSenderLockingOverrideWrapper struct {
	TxnSender
}

func (t txnSenderLockingOverrideWrapper) IsLocking() bool {
	return true
}

func TestTransactionAdmissionHeader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClockForTesting(nil)
	db := NewDB(log.MakeTestingAmbientCtxWithNewTracer(),
		newTestTxnFactory(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			br := ba.CreateReply()
			return br, nil
		}), clock, stopper)

	txn := NewTxnWithAdmissionControl(
		ctx, db, 0 /* gatewayNodeID */, kvpb.AdmissionHeader_FROM_SQL, admissionpb.NormalPri)
	header := txn.AdmissionHeader()
	expectedHeader := kvpb.AdmissionHeader{
		Priority:   int32(admissionpb.NormalPri),
		CreateTime: header.CreateTime,
		Source:     kvpb.AdmissionHeader_FROM_SQL,
	}
	require.Equal(t, expectedHeader, header)
	// MockTransactionalSender always return false from IsLocking, so wrap it to
	// return true.
	txn.mu.sender = txnSenderLockingOverrideWrapper{txn.mu.sender}
	header = txn.AdmissionHeader()
	expectedHeader.Priority = int32(admissionpb.LockingNormalPri)
	require.Equal(t, expectedHeader, header)
}
