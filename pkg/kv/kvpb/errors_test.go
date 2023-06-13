// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvpb

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testError struct{}

func (t *testError) Error() string { return "test" }

// TestNewError verifies that a test error that
// implements retryable or indexed is converted properly into a generic error.
func TestNewError(t *testing.T) {
	pErr := NewError(&testError{})
	if pErr.GoError().Error() != "test" {
		t.Errorf("unexpected error: %s", pErr)
	}
}

// TestNewErrorNil verifies that a nil error can be set
// and retrieved from a response header.
func TestNewErrorNil(t *testing.T) {
	pErr := NewError(nil)
	if pErr != nil {
		t.Errorf("expected nil error; got %s", pErr)
	}
}

// TestSetTxn verifies that SetTxn updates the error message.
func TestSetTxn(t *testing.T) {
	e := NewError(NewTransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND))
	txn := roachpb.MakeTransaction("test", roachpb.Key("a"), isolation.Serializable, 1, hlc.Timestamp{}, 0, 99)
	e.SetTxn(&txn)
	if !strings.HasPrefix(
		e.String(), "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND): \"test\"") {
		t.Errorf("unexpected message: %s", e.String())
	}
}

func TestErrPriority(t *testing.T) {
	unhandledAbort := &UnhandledRetryableError{
		PErr: *NewError(&TransactionAbortedError{}),
	}
	unhandledRetry := &UnhandledRetryableError{
		PErr: *NewError(&ReadWithinUncertaintyIntervalError{}),
	}
	require.Equal(t, ErrorPriority(0), ErrPriority(nil))
	require.Equal(t, ErrorScoreTxnAbort, ErrPriority(unhandledAbort))
	require.Equal(t, ErrorScoreTxnRestart, ErrPriority(unhandledRetry))
	{
		id1 := uuid.Must(uuid.NewV4())
		require.Equal(t, ErrorScoreTxnRestart, ErrPriority(&TransactionRetryWithProtoRefreshError{
			TxnID:       id1,
			Transaction: roachpb.Transaction{TxnMeta: enginepb.TxnMeta{ID: id1}},
		}))
		id2 := uuid.Nil
		require.Equal(t, ErrorScoreTxnAbort, ErrPriority(&TransactionRetryWithProtoRefreshError{
			TxnID:       id1,
			Transaction: roachpb.Transaction{TxnMeta: enginepb.TxnMeta{ID: id2}},
		}))
	}
	require.Equal(t, ErrorScoreUnambiguousError, ErrPriority(&ConditionFailedError{}))
	require.Equal(t, ErrorScoreUnambiguousError, ErrPriority(NewError(&ConditionFailedError{}).GoError()))
	require.Equal(t, ErrorScoreNonRetriable, ErrPriority(errors.New("foo")))
}

func TestErrorTxn(t *testing.T) {
	var pErr *Error
	if txn := pErr.GetTxn(); txn != nil {
		t.Fatalf("wanted nil, unexpected: %+v", txn)
	}
	pErr = NewErrorf("foo")
	const name = "X"
	pErr.SetTxn(&roachpb.Transaction{Name: name})
	if txn := pErr.GetTxn(); txn == nil || txn.Name != name {
		t.Fatalf("wanted name %s, unexpected: %+v", name, txn)
	}
}

func TestReadWithinUncertaintyIntervalError(t *testing.T) {
	{
		rwueNew := NewReadWithinUncertaintyIntervalError(
			hlc.Timestamp{WallTime: 1},
			hlc.ClockTimestamp{WallTime: 2, Logical: 2},
			&roachpb.Transaction{
				GlobalUncertaintyLimit: hlc.Timestamp{WallTime: 3},
				ObservedTimestamps:     []roachpb.ObservedTimestamp{{NodeID: 12, Timestamp: hlc.ClockTimestamp{WallTime: 4}}},
			},
			hlc.Timestamp{WallTime: 2},
			hlc.ClockTimestamp{WallTime: 1, Logical: 2})
		expNew := "ReadWithinUncertaintyIntervalError: read at time 0.000000001,0 encountered " +
			"previous write with future timestamp 0.000000002,0 (local=0.000000001,2) within uncertainty interval " +
			"`t <= (local=0.000000002,2, global=0.000000003,0)`; observed timestamps: [{12 0.000000004,0}]"
		require.Equal(t, expNew, rwueNew.Error())
	}

	{
		rwueOld := NewReadWithinUncertaintyIntervalError(
			hlc.Timestamp{WallTime: 1},
			hlc.ClockTimestamp{},
			nil,
			hlc.Timestamp{WallTime: 2},
			hlc.ClockTimestamp{WallTime: 1, Logical: 2})

		expOld := "ReadWithinUncertaintyIntervalError: read at time 0.000000001,0 encountered " +
			"previous write with future timestamp 0.000000002,0 (local=0.000000001,2) within uncertainty interval " +
			"`t <= (local=0,0, global=0,0)`; observed timestamps: []"
		require.Equal(t, expOld, rwueOld.Error())
	}
}

type TestPrinter struct {
	buf io.Writer
}

func (t TestPrinter) Print(args ...interface{}) {
	redact.Fprint(t.buf, args...)
}

func (t TestPrinter) Printf(format string, args ...interface{}) {
	redact.Fprintf(t.buf, format, args...)
}

func (t TestPrinter) Detail() bool {
	return false
}

var _ errors.Printer = &TestPrinter{}

func TestErrorRedaction(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		var pErr *Error
		var s redact.StringBuilder
		s.Print(pErr)
		const exp = "<nil>"
		act := s.RedactableString()
		require.EqualValues(t, exp, act)
	})
	t.Run("uncertainty-restart", func(t *testing.T) {
		// NB: most other errors don't redact properly. More elbow grease is needed.
		wrappedPErr := NewError(NewReadWithinUncertaintyIntervalError(
			hlc.Timestamp{WallTime: 1},
			hlc.ClockTimestamp{WallTime: 2, Logical: 2},
			&roachpb.Transaction{
				GlobalUncertaintyLimit: hlc.Timestamp{WallTime: 3},
				ObservedTimestamps:     []roachpb.ObservedTimestamp{{NodeID: 12, Timestamp: hlc.ClockTimestamp{WallTime: 4}}},
			},
			hlc.Timestamp{WallTime: 2},
			hlc.ClockTimestamp{WallTime: 1, Logical: 2},
		))
		txn := roachpb.MakeTransaction("foo", roachpb.Key("bar"), isolation.Serializable, 1, hlc.Timestamp{WallTime: 1}, 1, 99)
		txn.ID = uuid.Nil
		txn.Priority = 1234
		wrappedPErr.UnexposedTxn = &txn
		r := &UnhandledRetryableError{
			PErr: *wrappedPErr,
		}
		var s redact.StringBuilder
		s.Print(r)
		act := s.RedactableString().Redact()
		const exp = "ReadWithinUncertaintyIntervalError: read at time 0.000000001,0 encountered previous write with future timestamp 0.000000002,0 (local=0.000000001,2) within uncertainty interval `t <= (local=0.000000002,2, global=0.000000003,0)`; observed timestamps: [{12 0.000000004,0}]: \"foo\" meta={id=00000000 key=‹×› iso=Serializable pri=0.00005746 epo=0 ts=0.000000001,0 min=0.000000001,0 seq=0} lock=true stat=PENDING rts=0.000000001,0 wto=false gul=0.000000002,0"
		require.Equal(t, exp, string(act))
	})

	// The purpose of these tests is to ensure that most of the error is printed
	// without redaction markers. The contents of the error aren't really being
	// tested here, although the test could be extended to care more about that.
	for _, tc := range []struct {
		err    errors.SafeFormatter
		expect string
	}{
		{
			err:    &NotLeaseHolderError{},
			expect: "[NotLeaseHolderError] r0: replica not lease holder; lease holder unknown",
		},
		{
			err:    &RangeNotFoundError{},
			expect: "r0 was not found",
		},
		{
			err:    &RangeKeyMismatchError{},
			expect: "RangeKeyMismatchError (key range /Min-/Min) with empty RangeInfo slicekey range /Min-/Min outside of bounds of range /Min-/Min; suggested ranges: []",
		},
		{
			err:    &ReadWithinUncertaintyIntervalError{},
			expect: "ReadWithinUncertaintyIntervalError: read at time 0,0 encountered previous write with future timestamp 0,0 within uncertainty interval `t <= (local=0,0, global=0,0)`; observed timestamps: []",
		},
		{
			err:    &TransactionAbortedError{},
			expect: "TransactionAbortedError(ABORT_REASON_UNKNOWN)",
		},
		{
			err:    &TransactionPushError{},
			expect: "failed to push meta={id=00000000 key=/Min iso=Serializable pri=0.00000000 epo=0 ts=0,0 min=0,0 seq=0} lock=false stat=PENDING rts=0,0 wto=false gul=0,0",
		},
		{
			err:    &TransactionRetryError{},
			expect: "TransactionRetryError: retry txn (RETRY_REASON_UNKNOWN)",
		},
		{
			err:    &TransactionStatusError{},
			expect: "TransactionStatusError:  (REASON_UNKNOWN)",
		},
		{
			err:    &WriteIntentError{},
			expect: "conflicting intents on ",
		},
		{
			err:    &WriteTooOldError{},
			expect: "WriteTooOldError: write at timestamp 0,0 too old; must write at or above 0,0",
		},
		{
			err:    &OpRequiresTxnError{},
			expect: "the operation requires transactional context",
		},
		{
			err:    &ConditionFailedError{},
			expect: "unexpected value: ‹<nil>›",
		},
		{
			err:    &LeaseRejectedError{},
			expect: "cannot replace lease <empty> with <empty>: ",
		},
		{err: &NodeUnavailableError{}, expect: "node unavailable; try another peer"},
		{
			err:    &RaftGroupDeletedError{},
			expect: "raft group deleted",
		},
		{
			err:    &ReplicaCorruptionError{},
			expect: "replica corruption (processed=false)",
		},
		{
			err:    &ReplicaTooOldError{},
			expect: "sender replica too old, discarding message",
		},
		{
			err:    &AmbiguousResultError{},
			expect: "result is ambiguous: unknown cause",
		},
		{
			err:    &StoreNotFoundError{},
			expect: "store 0 was not found",
		},
		{
			err:    &TransactionRetryWithProtoRefreshError{MsgRedactable: redact.RedactableString("this is redactable")},
			expect: "TransactionRetryWithProtoRefreshError: this is redactable",
		},
		{
			err:    &IntegerOverflowError{},
			expect: "key /Min with value 0 incremented by 0 results in overflow",
		},
		{
			err:    &UnsupportedRequestError{},
			expect: "unsupported request",
		},
		{
			err:    &BatchTimestampBeforeGCError{},
			expect: "batch timestamp 0,0 must be after replica GC threshold 0,0",
		},
		{
			err:    &TxnAlreadyEncounteredErrorError{},
			expect: "txn already encountered an error; cannot be used anymore (previous err: )",
		},
		{
			err:    &IntentMissingError{},
			expect: "intent missing",
		},
		{
			err:    &MergeInProgressError{},
			expect: "merge in progress",
		},
		{
			err:    &RangeFeedRetryError{},
			expect: "retry rangefeed (REASON_REPLICA_REMOVED)",
		},
		{
			err:    &IndeterminateCommitError{},
			expect: "found txn in indeterminate STAGING state meta={id=00000000 key=/Min iso=Serializable pri=0.00000000 epo=0 ts=0,0 min=0,0 seq=0} lock=false stat=PENDING rts=0,0 wto=false gul=0,0",
		},
		{
			err:    &InvalidLeaseError{},
			expect: "invalid lease",
		},
		{
			err:    &OptimisticEvalConflictsError{},
			expect: "optimistic eval encountered conflict",
		},
		{
			err:    &MinTimestampBoundUnsatisfiableError{},
			expect: "bounded staleness read with minimum timestamp bound of 0,0 could not be satisfied by a local resolved timestamp of 0,0",
		},
		{
			err:    &RefreshFailedError{},
			expect: "encountered recently written committed value /Min @0,0",
		},
		{
			err:    &MVCCHistoryMutationError{},
			expect: "unexpected MVCC history mutation in span ‹/Min›",
		},
		{
			err:    &UnhandledRetryableError{},
			expect: "{<nil> 0 {<nil>} ‹<nil>› 0,0}",
		},
	} {
		t.Run(fmt.Sprintf("%T", tc.err), func(t *testing.T) {
			var b []byte
			buf := bytes.NewBuffer(b)
			printer := &TestPrinter{buf: buf}
			err := tc.err.SafeFormatError(printer)
			require.NoError(t, err)
			require.Equal(t, tc.expect, buf.String())
		})
	}
}

func TestErrorGRPCStatus(t *testing.T) {
	// Verify that gRPC status error en/decoding via
	// github.com/cockroachdb/errors/extgrpc is set up correctly.

	s := status.New(codes.PermissionDenied, "foo")
	sErr := s.Err()
	pbErr := NewError(sErr)
	goErr := pbErr.GoError()

	decoded, ok := status.FromError(goErr)
	require.True(t, ok, "expected gRPC status error, got %T: %v", goErr, goErr)
	require.Equal(t, s.Code(), decoded.Code())
	require.Equal(t, s.Message(), decoded.Message())
}

func TestRefreshSpanError(t *testing.T) {
	e1 := NewRefreshFailedError(RefreshFailedError_REASON_COMMITTED_VALUE, roachpb.Key("foo"), hlc.Timestamp{WallTime: 3})
	require.Equal(t, "encountered recently written committed value \"foo\" @0.000000003,0", e1.Error())

	e2 := NewRefreshFailedError(RefreshFailedError_REASON_INTENT, roachpb.Key("bar"), hlc.Timestamp{WallTime: 4})
	require.Equal(t, "encountered recently written intent \"bar\" @0.000000004,0", e2.Error())
}

func TestNotLeaseholderError(t *testing.T) {
	rd := &roachpb.ReplicaDescriptor{
		ReplicaID: 1, StoreID: 1, NodeID: 1,
	}
	for _, tc := range []struct {
		exp string
		err *NotLeaseHolderError
	}{
		{
			exp: `[NotLeaseHolderError] r1: replica not lease holder; replica (n1,s1):1 is`,
			err: &NotLeaseHolderError{
				RangeID:               1,
				DeprecatedLeaseHolder: rd,
			},
		},
		{
			exp: `[NotLeaseHolderError] r1: replica not lease holder; current lease is repl=(n1,s1):1 seq=2 start=0.000000001,0 epo=1`,
			err: &NotLeaseHolderError{
				RangeID: 1,
				Lease: &roachpb.Lease{
					Start:           hlc.ClockTimestamp{WallTime: 1},
					Replica:         *rd,
					Epoch:           1,
					Sequence:        2,
					AcquisitionType: roachpb.LeaseAcquisitionType_Transfer,
				},
			},
		},
		{
			exp: `[NotLeaseHolderError] r1: replica not lease holder; lease holder unknown`,
			err: &NotLeaseHolderError{
				RangeID: 1,
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tc.exp, tc.err.Error())
		})
	}
}
