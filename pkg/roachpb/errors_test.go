// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"strings"
	"testing"

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
	txn := MakeTransaction("test", Key("a"), 1, hlc.Timestamp{}, 0, 99)
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
			Transaction: Transaction{TxnMeta: enginepb.TxnMeta{ID: id1}},
		}))
		id2 := uuid.Nil
		require.Equal(t, ErrorScoreTxnAbort, ErrPriority(&TransactionRetryWithProtoRefreshError{
			TxnID:       id1,
			Transaction: Transaction{TxnMeta: enginepb.TxnMeta{ID: id2}},
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
	pErr = &Error{}
	const name = "X"
	pErr.SetTxn(&Transaction{Name: name})
	if txn := pErr.GetTxn(); txn == nil || txn.Name != name {
		t.Fatalf("wanted name %s, unexpected: %+v", name, txn)
	}
}

func TestReadWithinUncertaintyIntervalError(t *testing.T) {
	{
		rwueNew := NewReadWithinUncertaintyIntervalError(
			hlc.Timestamp{WallTime: 1}, hlc.Timestamp{WallTime: 2}, hlc.Timestamp{WallTime: 2, Logical: 2},
			&Transaction{
				GlobalUncertaintyLimit: hlc.Timestamp{WallTime: 3},
				ObservedTimestamps:     []ObservedTimestamp{{NodeID: 12, Timestamp: hlc.ClockTimestamp{WallTime: 4}}},
			})
		expNew := "ReadWithinUncertaintyIntervalError: read at time 0.000000001,0 encountered " +
			"previous write with future timestamp 0.000000002,0 within uncertainty interval " +
			"`t <= (local=0.000000002,2, global=0.000000003,0)`; observed timestamps: [{12 0.000000004,0}]"
		if a := rwueNew.Error(); a != expNew {
			t.Fatalf("expected: %s\ngot: %s", a, expNew)
		}
	}

	{
		rwueOld := NewReadWithinUncertaintyIntervalError(
			hlc.Timestamp{WallTime: 1}, hlc.Timestamp{WallTime: 2}, hlc.Timestamp{}, nil)

		expOld := "ReadWithinUncertaintyIntervalError: read at time 0.000000001,0 encountered " +
			"previous write with future timestamp 0.000000002,0 within uncertainty interval " +
			"`t <= (local=0,0, global=0,0)`; observed timestamps: []"
		if a := rwueOld.Error(); a != expOld {
			t.Fatalf("expected: %s\ngot: %s", a, expOld)
		}
	}
}

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
			hlc.Timestamp{WallTime: 1}, hlc.Timestamp{WallTime: 2}, hlc.Timestamp{WallTime: 2, Logical: 2},
			&Transaction{
				GlobalUncertaintyLimit: hlc.Timestamp{WallTime: 3},
				ObservedTimestamps:     []ObservedTimestamp{{NodeID: 12, Timestamp: hlc.ClockTimestamp{WallTime: 4}}},
			}))
		txn := MakeTransaction("foo", Key("bar"), 1, hlc.Timestamp{WallTime: 1}, 1, 99)
		txn.ID = uuid.Nil
		txn.Priority = 1234
		wrappedPErr.UnexposedTxn = &txn
		r := &UnhandledRetryableError{
			PErr: *wrappedPErr,
		}
		var s redact.StringBuilder
		s.Print(r)
		act := s.RedactableString().Redact()
		const exp = "ReadWithinUncertaintyIntervalError: read at time 0.000000001,0 encountered previous write with future timestamp 0.000000002,0 within uncertainty interval `t <= (local=0.000000002,2, global=0.000000003,0)`; observed timestamps: [{12 0.000000004,0}]: \"foo\" meta={id=00000000 key=‹×› pri=0.00005746 epo=0 ts=0.000000001,0 min=0.000000001,0 seq=0} lock=true stat=PENDING rts=0.000000001,0 wto=false gul=0.000000002,0"
		require.Equal(t, exp, string(act))
	})
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
	e1 := NewRefreshFailedError(RefreshFailedError_REASON_COMMITTED_VALUE, Key("foo"), hlc.Timestamp{WallTime: 3})
	require.Equal(t, "encountered recently written committed value \"foo\" @0.000000003,0", e1.Error())

	e2 := NewRefreshFailedError(RefreshFailedError_REASON_INTENT, Key("bar"), hlc.Timestamp{WallTime: 4})
	require.Equal(t, "encountered recently written intent \"bar\" @0.000000004,0", e2.Error())
}
