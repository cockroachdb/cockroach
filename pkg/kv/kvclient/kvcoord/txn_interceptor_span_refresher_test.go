// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

func makeMockTxnSpanRefresher() (txnSpanRefresher, *mockLockedSender) {
	mockSender := &mockLockedSender{}
	metrics := MakeTxnMetrics(metric.TestSampleInterval)
	return txnSpanRefresher{
		st:           cluster.MakeTestingClusterSettings(),
		knobs:        new(ClientTestingKnobs),
		wrapped:      mockSender,
		metrics:      &metrics,
		canAutoRetry: true,
	}, mockSender
}

// TestTxnSpanRefresherCollectsSpans tests that the txnSpanRefresher collects
// spans for requests that succeeded and would need to be refreshed if the
// transaction's provisional commit timestamp moved forward.
func TestTxnSpanRefresherCollectsSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tsr, mockSender := makeMockTxnSpanRefresher()

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	keyC, keyD := roachpb.Key("c"), roachpb.Key("d")

	// Basic case.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	getArgs := kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}}
	putArgs := kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}}
	delRangeArgs := kvpb.DeleteRangeRequest{RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyB}}
	ba.Add(&getArgs, &putArgs, &delRangeArgs)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.True(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &kvpb.GetRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.DeleteRangeRequest{}, ba.Requests[2].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	expSpans := []roachpb.Span{getArgs.Span(), delRangeArgs.Span()}
	require.Equal(t, expSpans, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, 3+int64(len(expSpans))*roachpb.SpanOverhead, tsr.refreshFootprint.bytes)
	require.Zero(t, tsr.refreshedTimestamp)

	// Scan with limit. Only the scanned keys are added to the refresh spans.
	ba.Requests = nil
	scanArgs := kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyB, EndKey: keyD}}
	ba.Add(&scanArgs)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.False(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &kvpb.ScanRequest{}, ba.Requests[0].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetScan().ResumeSpan = &roachpb.Span{Key: keyC, EndKey: keyD}
		return br, nil
	})

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	expSpans = []roachpb.Span{getArgs.Span(), delRangeArgs.Span(), {Key: scanArgs.Key, EndKey: keyC}}
	require.Equal(t, expSpans, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, 5+int64(len(expSpans))*roachpb.SpanOverhead, tsr.refreshFootprint.bytes)
	require.Zero(t, tsr.refreshedTimestamp)
}

// TestTxnSpanRefresherRefreshesTransactions tests that the txnSpanRefresher
// refreshes the transaction's read and write spans if it observes an error
// that indicates that the transaction's timestamp is being pushed.
func TestTxnSpanRefresherRefreshesTransactions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")

	cases := []struct {
		// If name is not set, the test will use pErr.String().
		name string
		// OnFirstSend, if set, is invoked to evaluate the batch. If not set, pErr()
		// will be used to provide an error.
		onFirstSend  func(*kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error)
		pErr         func() *kvpb.Error
		expRefresh   bool
		expRefreshTS hlc.Timestamp
		expErr       bool
	}{
		{
			pErr: func() *kvpb.Error {
				return kvpb.NewError(
					&kvpb.TransactionRetryError{Reason: kvpb.RETRY_SERIALIZABLE})
			},
			expRefresh:   true,
			expRefreshTS: txn.WriteTimestamp,
		},
		{
			pErr: func() *kvpb.Error {
				return kvpb.NewError(
					&kvpb.TransactionRetryError{Reason: kvpb.RETRY_WRITE_TOO_OLD})
			},
			expRefresh:   true,
			expRefreshTS: txn.WriteTimestamp,
		},
		{
			pErr: func() *kvpb.Error {
				return kvpb.NewError(
					&kvpb.WriteTooOldError{ActualTimestamp: txn.WriteTimestamp.Add(15, 0)})
			},
			expRefresh:   true,
			expRefreshTS: txn.WriteTimestamp.Add(15, 0),
		},
		{
			pErr: func() *kvpb.Error {
				return kvpb.NewError(
					&kvpb.ReadWithinUncertaintyIntervalError{
						ValueTimestamp: txn.WriteTimestamp.Add(25, 0),
					})
			},
			expRefresh:   true,
			expRefreshTS: txn.WriteTimestamp.Add(25, 1), // see ExistingTimestamp
		},
		{
			pErr: func() *kvpb.Error {
				return kvpb.NewError(
					&kvpb.ReadWithinUncertaintyIntervalError{
						ValueTimestamp:        txn.WriteTimestamp.Add(25, 0),
						LocalUncertaintyLimit: hlc.ClockTimestamp(txn.WriteTimestamp.Add(30, 0)),
					})
			},
			expRefresh:   true,
			expRefreshTS: txn.WriteTimestamp.Add(30, 0), // see LocalUncertaintyLimit
		},
		{
			pErr: func() *kvpb.Error {
				return kvpb.NewErrorf("no refresh")
			},
			expRefresh: false,
			expErr:     true,
		},
		{
			name: "write_too_old flag (pending)",
			onFirstSend: func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				br := ba.CreateReply()
				br.Txn = ba.Txn.Clone()
				br.Txn.Status = roachpb.PENDING
				br.Txn.WriteTooOld = true
				br.Txn.WriteTimestamp = txn.WriteTimestamp.Add(20, 1)
				return br, nil
			},
			expRefresh:   true,
			expRefreshTS: txn.WriteTimestamp.Add(20, 1), // Same as br.Txn.WriteTimestamp.
		},
		{
			name: "write_too_old flag (staging)",
			onFirstSend: func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				br := ba.CreateReply()
				br.Txn = ba.Txn.Clone()
				br.Txn.Status = roachpb.STAGING
				br.Txn.WriteTooOld = true
				br.Txn.WriteTimestamp = txn.WriteTimestamp.Add(20, 1)
				return br, nil
			},
			expRefresh: false,
			expErr:     false,
		},
		{
			name: "write_too_old flag (committed)",
			onFirstSend: func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				br := ba.CreateReply()
				br.Txn = ba.Txn.Clone()
				br.Txn.Status = roachpb.COMMITTED
				br.Txn.WriteTooOld = true
				br.Txn.WriteTimestamp = txn.WriteTimestamp.Add(20, 1)
				return br, nil
			},
			expRefresh: false,
			expErr:     false,
		},
		{
			name: "write_too_old flag (aborted)",
			onFirstSend: func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				br := ba.CreateReply()
				br.Txn = ba.Txn.Clone()
				br.Txn.Status = roachpb.ABORTED
				br.Txn.WriteTooOld = true
				br.Txn.WriteTimestamp = txn.WriteTimestamp.Add(20, 1)
				return br, nil
			},
			expRefresh: false,
			expErr:     false,
		},
	}
	for _, tc := range cases {
		name := tc.name
		if name == "" {
			name = tc.pErr().String()
		}
		if (tc.onFirstSend != nil) == (tc.pErr != nil) {
			panic("exactly one tc.onFirstSend and tc.pErr must be set")
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			tsr, mockSender := makeMockTxnSpanRefresher()

			// Collect some refresh spans.
			ba := &kvpb.BatchRequest{}
			ba.Header = kvpb.Header{Txn: txn.Clone()} // clone txn since it's shared between subtests
			getArgs := kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}}
			delRangeArgs := kvpb.DeleteRangeRequest{RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyB}}
			ba.Add(&getArgs, &delRangeArgs)

			br, pErr := tsr.SendLocked(ctx, ba)
			require.Nil(t, pErr)
			require.NotNil(t, br)

			require.Equal(t, []roachpb.Span{getArgs.Span(), delRangeArgs.Span()}, tsr.refreshFootprint.asSlice())
			require.False(t, tsr.refreshInvalid)
			require.Zero(t, tsr.refreshedTimestamp)

			// Hook up a chain of mocking functions.
			onFirstSend := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())

				// Return a transaction retry error.
				if tc.onFirstSend != nil {
					return tc.onFirstSend(ba)
				}
				pErr = tc.pErr()
				pErr.SetTxn(ba.Txn)
				return nil, pErr
			}
			onSecondSend := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				// Should not be called if !expRefresh.
				require.True(t, tc.expRefresh)

				require.Len(t, ba.Requests, 1)
				require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())

				// Don't return an error.
				br = ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			}
			onRefresh := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				// Should not be called if !expRefresh.
				require.True(t, tc.expRefresh)

				require.Len(t, ba.Requests, 2)
				require.Equal(t, tc.expRefreshTS, ba.Txn.ReadTimestamp)
				require.IsType(t, &kvpb.RefreshRequest{}, ba.Requests[0].GetInner())
				require.IsType(t, &kvpb.RefreshRangeRequest{}, ba.Requests[1].GetInner())

				refReq := ba.Requests[0].GetRefresh()
				require.Equal(t, getArgs.Span(), refReq.Span())
				require.Equal(t, txn.ReadTimestamp, refReq.RefreshFrom)

				refRngReq := ba.Requests[1].GetRefreshRange()
				require.Equal(t, delRangeArgs.Span(), refRngReq.Span())
				require.Equal(t, txn.ReadTimestamp, refRngReq.RefreshFrom)

				br = ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			}
			mockSender.ChainMockSend(onFirstSend, onRefresh, onSecondSend)

			// Send a request that will hit a retry error. Depending on the
			// error type, we may or may not perform a refresh.
			ba.Requests = nil
			putArgs := kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}}
			ba.Add(&putArgs)

			br, pErr = tsr.SendLocked(ctx, ba)
			if tc.expRefresh {
				require.Nil(t, pErr)
				require.NotNil(t, br)
				require.NotNil(t, br.Txn)
				require.False(t, br.Txn.WriteTooOld)
				require.Equal(t, tc.expRefreshTS, br.Txn.WriteTimestamp)
				require.Equal(t, tc.expRefreshTS, br.Txn.ReadTimestamp)
				require.Equal(t, tc.expRefreshTS, tsr.refreshedTimestamp)
				require.Equal(t, int64(1), tsr.metrics.ClientRefreshSuccess.Count())
				require.Equal(t, int64(0), tsr.metrics.ClientRefreshFail.Count())
				require.Equal(t, int64(1), tsr.metrics.ClientRefreshAutoRetries.Count())
				require.Equal(t, int64(0), tsr.metrics.ServerRefreshSuccess.Count())
			} else {
				if tc.expErr {
					require.Nil(t, br)
					require.NotNil(t, pErr)
				} else {
					require.Nil(t, pErr)
					require.NotNil(t, br)
					require.NotNil(t, br.Txn)
					require.False(t, br.Txn.WriteTooOld)
				}
				require.Zero(t, tsr.refreshedTimestamp)
				require.Equal(t, int64(0), tsr.metrics.ClientRefreshSuccess.Count())
				require.Equal(t, int64(0), tsr.metrics.ClientRefreshFail.Count())
				require.Equal(t, int64(0), tsr.metrics.ClientRefreshAutoRetries.Count())
				require.Equal(t, int64(0), tsr.metrics.ServerRefreshSuccess.Count())
			}
		})
	}
}

// TestTxnSpanRefresherDowngradesStagingTxnStatus tests that the txnSpanRefresher
// tolerates retry errors with a STAGING transaction status. In such cases, it
// will downgrade the status to PENDING before refreshing and retrying, because
// the presence of an error proves that the transaction failed to implicitly
// commit.
func TestTxnSpanRefresherDowngradesStagingTxnStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tsr, mockSender := makeMockTxnSpanRefresher()

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	conflictTs := txn.WriteTimestamp.Add(15, 0)

	// Collect some refresh spans.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	scanArgs := kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyB}}
	ba.Add(&scanArgs)

	br, pErr := tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
	require.Zero(t, tsr.refreshedTimestamp)

	// Hook up a chain of mocking functions.
	onPutAndEndTxn := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[1].GetInner())
		require.True(t, ba.Requests[1].GetEndTxn().IsParallelCommit())

		// Return a write-too-old error with a STAGING status, emulating a
		// successful EndTxn request and a failed Put request. This mixed success
		// state is possible if the requests were split across ranges.
		pErrTxn := ba.Txn.Clone()
		pErrTxn.Status = roachpb.STAGING
		pErr := &kvpb.WriteTooOldError{ActualTimestamp: conflictTs}
		return nil, kvpb.NewErrorWithTxn(pErr, pErrTxn)
	}
	onRefresh := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Equal(t, roachpb.PENDING, ba.Txn.Status) // downgraded
		require.Len(t, ba.Requests, 1)
		require.Equal(t, conflictTs, ba.Txn.ReadTimestamp)
		require.IsType(t, &kvpb.RefreshRangeRequest{}, ba.Requests[0].GetInner())

		refReq := ba.Requests[0].GetRefreshRange()
		require.Equal(t, scanArgs.Span(), refReq.Span())
		require.Equal(t, txn.ReadTimestamp, refReq.RefreshFrom)

		br = ba.CreateReply()
		br.Txn = ba.Txn.Clone()
		return br, nil
	}
	onPut := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Equal(t, roachpb.PENDING, ba.Txn.Status) // downgraded
		require.Len(t, ba.Requests, 1)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn.Clone()
		return br, nil
	}
	onEndTxn := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Equal(t, roachpb.PENDING, ba.Txn.Status) // downgraded
		require.Len(t, ba.Requests, 1)
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())
		require.False(t, ba.Requests[0].GetEndTxn().IsParallelCommit())

		br = ba.CreateReply()
		br.Txn = ba.Txn.Clone()
		br.Txn.Status = roachpb.COMMITTED
		return br, nil
	}
	mockSender.ChainMockSend(onPutAndEndTxn, onRefresh, onPut, onEndTxn)

	// Send a request that will hit a write-too-old error while also returning a
	// STAGING transaction status.
	ba.Requests = nil
	putArgs := kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyB}}
	etArgs := kvpb.EndTxnRequest{Commit: true}
	putArgs.Sequence = 1
	etArgs.Sequence = 2
	etArgs.InFlightWrites = []roachpb.SequencedWrite{{Key: keyB, Sequence: 1}}
	ba.Add(&putArgs, &etArgs)

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.NotNil(t, br.Txn)
	require.Equal(t, roachpb.COMMITTED, br.Txn.Status)
}

// TestTxnSpanRefresherMaxRefreshAttempts tests that the txnSpanRefresher
// attempts some number of retries before giving up and passing retryable
// errors back up the stack.
func TestTxnSpanRefresherMaxRefreshAttempts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tsr, mockSender := makeMockTxnSpanRefresher()

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")

	// Set MaxTxnRefreshAttempts to 2.
	tsr.knobs.MaxTxnRefreshAttempts = 2

	// Collect some refresh spans.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	scanArgs := kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyB}}
	ba.Add(&scanArgs)

	br, pErr := tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
	require.Zero(t, tsr.refreshedTimestamp)

	// Hook up a chain of mocking functions.
	onPut := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())

		// Return a transaction retry error.
		return nil, kvpb.NewErrorWithTxn(
			kvpb.NewTransactionRetryError(kvpb.RETRY_SERIALIZABLE, ""), ba.Txn)
	}
	refreshes := 0
	onRefresh := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		refreshes++
		require.Len(t, ba.Requests, 1)
		require.Equal(t, txn.WriteTimestamp, ba.Txn.ReadTimestamp)
		require.IsType(t, &kvpb.RefreshRangeRequest{}, ba.Requests[0].GetInner())

		refReq := ba.Requests[0].GetRefreshRange()
		require.Equal(t, scanArgs.Span(), refReq.Span())
		require.Equal(t, txn.ReadTimestamp, refReq.RefreshFrom)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	}
	unexpected := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Fail(t, "unexpected")
		return nil, nil
	}
	mockSender.ChainMockSend(onPut, onRefresh, onPut, onRefresh, onPut, unexpected)

	// Send a request that will hit a retry error. It will successfully retry
	// but continue to hit a retry error each time it is attempted. Eventually,
	// the txnSpanRefresher should give up and propagate the error.
	ba.Requests = nil
	putArgs := kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyB}}
	ba.Add(&putArgs)

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, br)
	require.NotNil(t, pErr)
	exp := kvpb.NewTransactionRetryError(kvpb.RETRY_SERIALIZABLE, "")
	require.Equal(t, exp, pErr.GetDetail())
	require.Equal(t, tsr.knobs.MaxTxnRefreshAttempts, refreshes)
}

// TestTxnSpanRefresherPreemptiveRefresh tests that the txnSpanRefresher
// performs a preemptive client-side refresh when doing so would be free or when
// it observes a batch containing an EndTxn request that will necessarily throw
// a serializable error.
func TestTxnSpanRefresherPreemptiveRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tsr, mockSender := makeMockTxnSpanRefresher()

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")

	// Push the txn so that it needs a refresh.
	txn.WriteTimestamp = txn.WriteTimestamp.Add(1, 0)
	origReadTs := txn.ReadTimestamp
	pushedWriteTs := txn.WriteTimestamp

	// Send an EndTxn request that will need a refresh to succeed. Because
	// no refresh spans have been recorded, the preemptive refresh should be
	// free, so the txnSpanRefresher should do so.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	etArgs := kvpb.EndTxnRequest{Commit: true}
	ba.Add(&etArgs)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.True(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())

		// The transaction should be refreshed.
		require.NotEqual(t, origReadTs, ba.Txn.ReadTimestamp)
		require.Equal(t, pushedWriteTs, ba.Txn.ReadTimestamp)
		require.Equal(t, pushedWriteTs, ba.Txn.WriteTimestamp)

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, int64(1), tsr.metrics.ClientRefreshSuccess.Count())
	require.Equal(t, int64(0), tsr.metrics.ClientRefreshFail.Count())
	require.Equal(t, int64(0), tsr.metrics.ClientRefreshAutoRetries.Count())
	require.Equal(t, int64(0), tsr.metrics.ServerRefreshSuccess.Count())
	require.True(t, tsr.refreshFootprint.empty())
	require.False(t, tsr.refreshInvalid)

	// Reset refreshedTimestamp to avoid confusing ourselves.
	tsr.refreshedTimestamp = origReadTs

	// Send a Scan request. Again, because a preemptive refresh would be free,
	// the txnSpanRefresher should do so. NOTE: This inhibits a server-side
	// refreshes when we issue EndTxn requests through the rest of this test.
	ba.Requests = nil
	scanArgs := kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyB}}
	ba.Add(&scanArgs)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.True(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &kvpb.ScanRequest{}, ba.Requests[0].GetInner())

		// The transaction should be refreshed.
		require.NotEqual(t, origReadTs, ba.Txn.ReadTimestamp)
		require.Equal(t, pushedWriteTs, ba.Txn.ReadTimestamp)
		require.Equal(t, pushedWriteTs, ba.Txn.WriteTimestamp)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, int64(2), tsr.metrics.ClientRefreshSuccess.Count())
	require.Equal(t, int64(0), tsr.metrics.ClientRefreshFail.Count())
	require.Equal(t, int64(0), tsr.metrics.ClientRefreshAutoRetries.Count())
	require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)

	// Reset refreshedTimestamp to avoid confusing ourselves.
	tsr.refreshedTimestamp = origReadTs

	// Now that we have accumulated refresh spans and refreshing is no longer a
	// no-op, send an EndTxn request that will need a refresh to succeed. This
	// should trigger a preemptive refresh. Try this twice. First, have the
	// refresh fail, which should prevent the rest of the request from being
	// issued. Second, have the refresh succeed, which should result in the
	// batch being issued with the refreshed transaction.
	ba.Requests = nil
	ba.Add(&etArgs)

	onRefresh := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.Equal(t, pushedWriteTs, ba.Txn.ReadTimestamp)
		require.IsType(t, &kvpb.RefreshRangeRequest{}, ba.Requests[0].GetInner())

		refReq := ba.Requests[0].GetRefreshRange()
		require.Equal(t, scanArgs.Span(), refReq.Span())
		require.Equal(t, origReadTs, refReq.RefreshFrom)

		return nil, kvpb.NewError(kvpb.NewRefreshFailedError(ctx, kvpb.RefreshFailedError_REASON_COMMITTED_VALUE, roachpb.Key("a"), hlc.Timestamp{WallTime: 1}))
	}
	unexpected := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Fail(t, "unexpected")
		return nil, nil
	}
	mockSender.ChainMockSend(onRefresh, unexpected)

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, br)
	require.NotNil(t, pErr)
	require.Regexp(t,
		"TransactionRetryError: retry txn \\(RETRY_SERIALIZABLE - failed preemptive refresh "+
			"due to encountered recently written committed value \"a\" @0.000000001,0\\)", pErr)
	require.Equal(t, int64(2), tsr.metrics.ClientRefreshSuccess.Count())
	require.Equal(t, int64(1), tsr.metrics.ClientRefreshFail.Count())
	require.Equal(t, int64(0), tsr.metrics.ClientRefreshAutoRetries.Count())
	require.Equal(t, int64(0), tsr.metrics.ServerRefreshSuccess.Count())
	require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)

	// Try again, but this time let the refresh succeed.
	onRefresh = func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.Equal(t, pushedWriteTs, ba.Txn.ReadTimestamp)
		require.IsType(t, &kvpb.RefreshRangeRequest{}, ba.Requests[0].GetInner())

		refReq := ba.Requests[0].GetRefreshRange()
		require.Equal(t, scanArgs.Span(), refReq.Span())
		require.Equal(t, origReadTs, refReq.RefreshFrom)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	}
	onEndTxn := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.False(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())

		// The transaction should be refreshed.
		require.NotEqual(t, origReadTs, ba.Txn.ReadTimestamp)
		require.Equal(t, pushedWriteTs, ba.Txn.ReadTimestamp)
		require.Equal(t, pushedWriteTs, ba.Txn.WriteTimestamp)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	}
	mockSender.ChainMockSend(onRefresh, onEndTxn, unexpected)

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, int64(3), tsr.metrics.ClientRefreshSuccess.Count())
	require.Equal(t, int64(1), tsr.metrics.ClientRefreshFail.Count())
	require.Equal(t, int64(0), tsr.metrics.ClientRefreshAutoRetries.Count())
	require.Equal(t, int64(0), tsr.metrics.ServerRefreshSuccess.Count())
	require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
}

// TestTxnSpanRefresherPreemptiveRefreshIsoLevel tests that the txnSpanRefresher
// only performed preemptive client-side refreshes of Serializable transactions,
// except when the preemptive refresh is free.
func TestTxnSpanRefresherPreemptiveRefreshIsoLevel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		isoLevel       isolation.Level
		noRefreshSpans bool
		expRefresh     bool
	}{
		{isolation.Serializable, false, true},
		{isolation.Serializable, true, true},
		{isolation.Snapshot, false, false},
		{isolation.Snapshot, true, true},
		{isolation.ReadCommitted, false, false},
		{isolation.ReadCommitted, true, true},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("iso=%s,noRefreshSpans=%t", tt.isoLevel, tt.noRefreshSpans)
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			tsr, mockSender := makeMockTxnSpanRefresher()

			txn := makeTxnProto()
			txn.IsoLevel = tt.isoLevel

			// Add refresh spans, if necessary.
			if !tt.noRefreshSpans {
				ba := &kvpb.BatchRequest{}
				ba.Header = kvpb.Header{Txn: &txn}
				ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: txn.Key}})

				br, pErr := tsr.SendLocked(ctx, ba)
				require.Nil(t, pErr)
				require.NotNil(t, br)
			}
			require.Equal(t, tt.noRefreshSpans, tsr.refreshFootprint.empty())

			// Push the txn.
			txn.WriteTimestamp = txn.WriteTimestamp.Add(1, 0)
			origReadTs := txn.ReadTimestamp
			pushedWriteTs := txn.WriteTimestamp

			// Send an EndTxn request.
			ba := &kvpb.BatchRequest{}
			ba.Header = kvpb.Header{Txn: &txn}
			etArgs := kvpb.EndTxnRequest{Commit: true}
			ba.Add(&etArgs)

			var sawRefreshReq bool
			expRefreshReq := tt.expRefresh && !tt.noRefreshSpans
			mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.Equal(t, tt.noRefreshSpans, ba.CanForwardReadTimestamp)
				req := ba.Requests[0].GetInner()
				switch req.(type) {
				case *kvpb.RefreshRequest:
					require.True(t, expRefreshReq)
					sawRefreshReq = true
				case *kvpb.EndTxnRequest:
					if tt.expRefresh {
						// The transaction should be refreshed.
						require.NotEqual(t, origReadTs, ba.Txn.ReadTimestamp)
						require.Equal(t, pushedWriteTs, ba.Txn.ReadTimestamp)
						require.Equal(t, pushedWriteTs, ba.Txn.WriteTimestamp)
					} else {
						// The transaction should not be refreshed.
						require.Equal(t, origReadTs, ba.Txn.ReadTimestamp)
						require.NotEqual(t, pushedWriteTs, ba.Txn.ReadTimestamp)
						require.Equal(t, pushedWriteTs, ba.Txn.WriteTimestamp)
					}
				default:
					t.Fatalf("unexpected request: %T", req)
				}

				br := ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			})

			br, pErr := tsr.SendLocked(ctx, ba)
			require.Nil(t, pErr)
			require.NotNil(t, br)
			require.Equal(t, expRefreshReq, sawRefreshReq)

			expRefreshSuccess := int64(0)
			if tt.expRefresh {
				expRefreshSuccess = 1
			}
			require.Equal(t, expRefreshSuccess, tsr.metrics.ClientRefreshSuccess.Count())
			require.Equal(t, int64(0), tsr.metrics.ClientRefreshFail.Count())
			require.Equal(t, int64(0), tsr.metrics.ClientRefreshAutoRetries.Count())
			require.Equal(t, int64(0), tsr.metrics.ServerRefreshSuccess.Count())
			require.Equal(t, tt.noRefreshSpans, tsr.refreshFootprint.empty())
			require.False(t, tsr.refreshInvalid)
		})
	}
}

// TestTxnSpanRefresherSplitEndTxnOnAutoRetry tests that EndTxn requests are
// split into their own sub-batch on auto-retries after a successful refresh.
// This is done to avoid starvation.
func TestTxnSpanRefresherSplitEndTxnOnAutoRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	txn := makeTxnProto()
	origTs := txn.ReadTimestamp
	pushedTs1 := txn.ReadTimestamp.Add(1, 0)
	pushedTs2 := txn.ReadTimestamp.Add(2, 0)
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")

	scanArgs := kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyB}}
	putArgs := kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyB}}
	etArgs := kvpb.EndTxnRequest{Commit: true}
	putArgs.Sequence = 1
	etArgs.Sequence = 2
	etArgs.InFlightWrites = []roachpb.SequencedWrite{{Key: keyB, Sequence: 1}}

	// Run the test with two slightly different configurations. When priorReads
	// is true, issue a {Put, EndTxn} batch after having previously accumulated
	// refresh spans due to a Scan. When priorReads is false, issue a {Scan,
	// Put, EndTxn} batch with no previously accumulated refresh spans.
	testutils.RunTrueAndFalse(t, "prior_reads", func(t *testing.T, priorReads bool) {
		var mockFns []func(*kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error)
		if priorReads {
			// Hook up a chain of mocking functions. Expected order of requests:
			// 1. {Put, EndTxn} -> retry error with pushed timestamp
			// 2. {Refresh}     -> successful
			// 3. {Put}         -> successful with pushed timestamp
			// 4. {Refresh}     -> successful
			// 5. {EndTxn}      -> successful
			onPutAndEndTxn := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 2)
				require.False(t, ba.CanForwardReadTimestamp)
				require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
				require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[1].GetInner())
				require.True(t, ba.Requests[1].GetEndTxn().IsParallelCommit())

				pushedTxn := ba.Txn.Clone()
				pushedTxn.WriteTimestamp = pushedTs1
				return nil, kvpb.NewErrorWithTxn(
					kvpb.NewTransactionRetryError(kvpb.RETRY_SERIALIZABLE, ""), pushedTxn)
			}
			onRefresh1 := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.Equal(t, pushedTs1, ba.Txn.ReadTimestamp)
				require.IsType(t, &kvpb.RefreshRangeRequest{}, ba.Requests[0].GetInner())

				refReq := ba.Requests[0].GetRefreshRange()
				require.Equal(t, scanArgs.Span(), refReq.Span())
				require.Equal(t, origTs, refReq.RefreshFrom)

				br := ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			}
			onPut := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.False(t, ba.CanForwardReadTimestamp)
				require.Equal(t, pushedTs1, ba.Txn.ReadTimestamp)
				require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())

				br := ba.CreateReply()
				br.Txn = ba.Txn.Clone()
				br.Txn.WriteTimestamp = pushedTs2
				return br, nil
			}
			onRefresh2 := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.Equal(t, pushedTs2, ba.Txn.ReadTimestamp)
				require.IsType(t, &kvpb.RefreshRangeRequest{}, ba.Requests[0].GetInner())

				refReq := ba.Requests[0].GetRefreshRange()
				require.Equal(t, scanArgs.Span(), refReq.Span())
				require.Equal(t, pushedTs1, refReq.RefreshFrom)

				br := ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			}
			onEndTxn := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.False(t, ba.CanForwardReadTimestamp)
				require.Equal(t, pushedTs2, ba.Txn.ReadTimestamp)
				require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())
				require.False(t, ba.Requests[0].GetEndTxn().IsParallelCommit())

				br := ba.CreateReply()
				br.Txn = ba.Txn.Clone()
				br.Txn.Status = roachpb.COMMITTED
				return br, nil
			}
			mockFns = append(mockFns, onPutAndEndTxn, onRefresh1, onPut, onRefresh2, onEndTxn)
		} else {
			// Hook up a chain of mocking functions. Expected order of requests:
			// 1. {Scan, Put, EndTxn} -> retry error with pushed timestamp
			// 3. {Scan, Put}         -> successful with pushed timestamp
			// 4. {Refresh}           -> successful
			// 5. {EndTxn}            -> successful
			onScanPutAndEndTxn := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 3)
				require.True(t, ba.CanForwardReadTimestamp)
				require.IsType(t, &kvpb.ScanRequest{}, ba.Requests[0].GetInner())
				require.IsType(t, &kvpb.PutRequest{}, ba.Requests[1].GetInner())
				require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[2].GetInner())
				require.True(t, ba.Requests[2].GetEndTxn().IsParallelCommit())

				pushedTxn := ba.Txn.Clone()
				pushedTxn.WriteTimestamp = pushedTs1
				return nil, kvpb.NewErrorWithTxn(
					kvpb.NewTransactionRetryError(kvpb.RETRY_SERIALIZABLE, ""), pushedTxn)
			}
			onScanAndPut := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 2)
				require.True(t, ba.CanForwardReadTimestamp)
				require.Equal(t, pushedTs1, ba.Txn.ReadTimestamp)
				require.IsType(t, &kvpb.ScanRequest{}, ba.Requests[0].GetInner())
				require.IsType(t, &kvpb.PutRequest{}, ba.Requests[1].GetInner())

				br := ba.CreateReply()
				br.Txn = ba.Txn.Clone()
				br.Txn.WriteTimestamp = pushedTs2
				return br, nil
			}
			onRefresh := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.Equal(t, pushedTs2, ba.Txn.ReadTimestamp)
				require.IsType(t, &kvpb.RefreshRangeRequest{}, ba.Requests[0].GetInner())

				refReq := ba.Requests[0].GetRefreshRange()
				require.Equal(t, scanArgs.Span(), refReq.Span())
				require.Equal(t, pushedTs1, refReq.RefreshFrom)

				br := ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			}
			onEndTxn := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 1)
				// IMPORTANT! CanForwardReadTimestamp should no longer be set
				// for EndTxn batch, because the Scan in the earlier batch needs
				// to be refreshed if the read timestamp changes.
				require.False(t, ba.CanForwardReadTimestamp)
				require.Equal(t, pushedTs2, ba.Txn.ReadTimestamp)
				require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())
				require.False(t, ba.Requests[0].GetEndTxn().IsParallelCommit())

				br := ba.CreateReply()
				br.Txn = ba.Txn.Clone()
				br.Txn.Status = roachpb.COMMITTED
				return br, nil
			}
			mockFns = append(mockFns, onScanPutAndEndTxn, onScanAndPut, onRefresh, onEndTxn)
		}

		// Iterate over each RPC to inject an error to test error propagation.
		// Include a test case where no error is returned and the entire chain
		// of requests succeeds.
		for errIdx := 0; errIdx <= len(mockFns); errIdx++ {
			errIdxStr := strconv.Itoa(errIdx)
			if errIdx == len(mockFns) {
				errIdxStr = "none"
			}
			t.Run(fmt.Sprintf("error_index=%s", errIdxStr), func(t *testing.T) {
				ctx := context.Background()
				tsr, mockSender := makeMockTxnSpanRefresher()

				ba := &kvpb.BatchRequest{}
				if priorReads {
					// Collect some refresh spans first.
					ba.Header = kvpb.Header{Txn: &txn}
					ba.Add(&scanArgs)

					br, pErr := tsr.SendLocked(ctx, ba)
					require.Nil(t, pErr)
					require.NotNil(t, br)
					require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshFootprint.asSlice())
					require.False(t, tsr.refreshInvalid)
					require.Zero(t, tsr.refreshedTimestamp)

					ba.Requests = nil
					ba.Add(&putArgs, &etArgs)
				} else {
					// No refresh spans to begin with.
					require.Equal(t, []roachpb.Span(nil), tsr.refreshFootprint.asSlice())

					ba.Header = kvpb.Header{Txn: &txn}
					ba.Add(&scanArgs, &putArgs, &etArgs)
				}

				// Construct the mock sender chain, injecting an error where
				// appropriate. Make a copy of mockFns to avoid sharing state
				// between subtests.
				mockFnsCpy := append([]func(*kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error)(nil), mockFns...)
				if errIdx < len(mockFnsCpy) {
					errFn := mockFnsCpy[errIdx]
					newErrFn := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
						_, _ = errFn(ba)
						return nil, kvpb.NewErrorf("error")
					}
					mockFnsCpy[errIdx] = newErrFn
				}
				unexpected := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
					require.Fail(t, "unexpected")
					return nil, nil
				}
				mockSender.ChainMockSend(append(mockFnsCpy, unexpected)...)

				br, pErr := tsr.SendLocked(ctx, ba)
				if priorReads {
					if errIdx < 5 {
						require.Nil(t, br)
						require.NotNil(t, pErr)
					} else {
						require.Nil(t, pErr)
						require.NotNil(t, br)
						require.Len(t, br.Responses, 2)
						require.IsType(t, &kvpb.PutResponse{}, br.Responses[0].GetInner())
						require.IsType(t, &kvpb.EndTxnResponse{}, br.Responses[1].GetInner())
						require.Equal(t, roachpb.COMMITTED, br.Txn.Status)
						require.Equal(t, pushedTs2, br.Txn.ReadTimestamp)
						require.Equal(t, pushedTs2, br.Txn.WriteTimestamp)
					}

					var expSuccess, expFail, expAutoRetries int64
					switch errIdx {
					case 0:
						expSuccess, expFail, expAutoRetries = 0, 0, 0
					case 1:
						expSuccess, expFail, expAutoRetries = 0, 1, 0
					case 2:
						expSuccess, expFail, expAutoRetries = 1, 0, 1
					case 3:
						expSuccess, expFail, expAutoRetries = 1, 1, 1
					case 4, 5:
						expSuccess, expFail, expAutoRetries = 2, 0, 1
					default:
						require.Fail(t, "unexpected")
					}
					require.Equal(t, expSuccess, tsr.metrics.ClientRefreshSuccess.Count())
					require.Equal(t, expFail, tsr.metrics.ClientRefreshFail.Count())
					require.Equal(t, expAutoRetries, tsr.metrics.ClientRefreshAutoRetries.Count())

					require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshFootprint.asSlice())
					require.False(t, tsr.refreshInvalid)
				} else {
					if errIdx < 4 {
						require.Nil(t, br)
						require.NotNil(t, pErr)
					} else {
						require.Nil(t, pErr)
						require.NotNil(t, br)
						require.Len(t, br.Responses, 3)
						require.IsType(t, &kvpb.ScanResponse{}, br.Responses[0].GetInner())
						require.IsType(t, &kvpb.PutResponse{}, br.Responses[1].GetInner())
						require.IsType(t, &kvpb.EndTxnResponse{}, br.Responses[2].GetInner())
						require.Equal(t, roachpb.COMMITTED, br.Txn.Status)
						require.Equal(t, pushedTs2, br.Txn.ReadTimestamp)
						require.Equal(t, pushedTs2, br.Txn.WriteTimestamp)
					}

					var expSuccess, expFail, expAutoRetries int64
					switch errIdx {
					case 0:
						expSuccess, expFail, expAutoRetries = 0, 0, 0
					case 1:
						expSuccess, expFail, expAutoRetries = 1, 0, 1
					case 2:
						expSuccess, expFail, expAutoRetries = 1, 1, 1
					case 3, 4:
						expSuccess, expFail, expAutoRetries = 2, 0, 1
					default:
						require.Fail(t, "unexpected")
					}
					require.Equal(t, expSuccess, tsr.metrics.ClientRefreshSuccess.Count())
					require.Equal(t, expFail, tsr.metrics.ClientRefreshFail.Count())
					require.Equal(t, expAutoRetries, tsr.metrics.ClientRefreshAutoRetries.Count())

					if errIdx < 2 {
						require.Equal(t, []roachpb.Span(nil), tsr.refreshFootprint.asSlice())
					} else {
						require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshFootprint.asSlice())
					}
					require.False(t, tsr.refreshInvalid)
				}
			})
		}
	})
}

type singleRangeIterator struct{}

var _ condensableSpanSetRangeIterator = singleRangeIterator{}

func (s singleRangeIterator) Valid() bool {
	return true
}

func (s singleRangeIterator) Seek(context.Context, roachpb.RKey, ScanDirection) {}

func (s singleRangeIterator) Error() error {
	return nil
}

func (s singleRangeIterator) Desc() *roachpb.RangeDescriptor {
	return &roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKeyMax,
	}
}

// TestTxnSpanRefresherMaxTxnRefreshSpansBytes tests that the txnSpanRefresher
// collapses spans after they exceed kv.transaction.max_refresh_spans_bytes
// refresh bytes.
func TestTxnSpanRefresherMaxTxnRefreshSpansBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tsr, mockSender := makeMockTxnSpanRefresher()
	tsr.riGen = rangeIteratorFactory{factory: func() condensableSpanSetRangeIterator {
		return singleRangeIterator{}
	}}

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	keyC := roachpb.Key("c")
	keyD, keyE := roachpb.Key("d"), roachpb.Key("e")

	// Set MaxTxnRefreshSpansBytes limit to 3 bytes plus the span overhead.
	MaxTxnRefreshSpansBytes.Override(ctx, &tsr.st.SV, 3+roachpb.SpanOverhead)

	// Send a batch below the limit.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	scanArgs := kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyB}}
	ba.Add(&scanArgs)

	br, pErr := tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
	require.Zero(t, tsr.refreshedTimestamp)
	require.Equal(t, 2+roachpb.SpanOverhead, tsr.refreshFootprint.bytes)

	// Send another batch that pushes us above the limit. The tracked spans are
	// adjacent so the spans will be merged, but not condensed.
	ba.Requests = nil
	scanArgs2 := kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyB, EndKey: keyC}}
	ba.Add(&scanArgs2)

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	require.Equal(t, []roachpb.Span{{Key: keyA, EndKey: keyC}}, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, 2+roachpb.SpanOverhead, tsr.refreshFootprint.bytes)
	require.False(t, tsr.refreshFootprint.condensed)
	require.Equal(t, int64(0), tsr.metrics.ClientRefreshMemoryLimitExceeded.Count())
	require.Zero(t, tsr.refreshedTimestamp)

	// Exceed the limit again, this time with a non-adjacent span such that
	// condensing needs to occur.
	ba.Requests = nil
	scanArgs3 := kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyD, EndKey: keyE}}
	ba.Add(&scanArgs3)

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	require.Equal(t, []roachpb.Span{{Key: keyA, EndKey: keyE}}, tsr.refreshFootprint.asSlice())
	require.True(t, tsr.refreshFootprint.condensed)
	require.False(t, tsr.refreshInvalid)
	require.Zero(t, tsr.refreshedTimestamp)
	require.Equal(t, int64(1), tsr.metrics.ClientRefreshMemoryLimitExceeded.Count())
	require.Equal(t, int64(0), tsr.metrics.ClientRefreshFailWithCondensedSpans.Count())

	// Return a transaction retry error and make sure the metric indicating that
	// we did not retry due to the refresh span bytes is incremented.
	mockSender.MockSend(func(ba *kvpb.BatchRequest) (batchResponse *kvpb.BatchResponse, r *kvpb.Error) {
		return nil, kvpb.NewErrorWithTxn(
			kvpb.NewTransactionRetryError(kvpb.RETRY_SERIALIZABLE, ""), ba.Txn)
	})

	br, pErr = tsr.SendLocked(ctx, ba)
	exp := kvpb.NewTransactionRetryError(kvpb.RETRY_SERIALIZABLE, "")
	require.Equal(t, exp, pErr.GetDetail())
	require.Nil(t, br)
	require.Equal(t, int64(1), tsr.metrics.ClientRefreshFailWithCondensedSpans.Count())
}

// TestTxnSpanRefresherAssignsCanForwardReadTimestamp tests that the
// txnSpanRefresher assigns the CanForwardReadTimestamp flag on Batch
// headers.
func TestTxnSpanRefresherAssignsCanForwardReadTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tsr, mockSender := makeMockTxnSpanRefresher()

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	keyC, keyD := roachpb.Key("c"), roachpb.Key("d")
	refreshTS1 := txn.WriteTimestamp.Add(1, 0)
	refreshTS2 := txn.WriteTimestamp.Add(2, 0)
	refreshTS3 := txn.WriteTimestamp.Add(3, 0)

	// Send a Put request. Should set CanForwardReadTimestamp flag. Should not
	// collect refresh spans.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.True(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn.Clone()
		br.Txn.BumpReadTimestamp(refreshTS1) // server-side refresh
		return br, nil
	})

	br, pErr := tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.NotNil(t, br.Txn)
	require.Equal(t, refreshTS1, br.Txn.WriteTimestamp)
	require.Equal(t, refreshTS1, br.Txn.ReadTimestamp)
	txn.Update(br.Txn)

	require.Equal(t, refreshTS1, tsr.refreshedTimestamp)
	require.Nil(t, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, int64(0), tsr.metrics.ClientRefreshSuccess.Count())
	require.Equal(t, int64(0), tsr.metrics.ClientRefreshFail.Count())
	require.Equal(t, int64(0), tsr.metrics.ClientRefreshAutoRetries.Count())
	require.Equal(t, int64(1), tsr.metrics.ServerRefreshSuccess.Count())

	// Send a Put request for a transaction with a fixed read timestamp.
	// Should NOT set CanForwardReadTimestamp flag.
	txnFixed := txn.Clone()
	txnFixed.ReadTimestampFixed = true
	baFixed := &kvpb.BatchRequest{}
	baFixed.Header = kvpb.Header{Txn: txnFixed}
	baFixed.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.False(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tsr.SendLocked(ctx, baFixed)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, refreshTS1, tsr.refreshedTimestamp)
	require.Nil(t, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)

	// Send a Scan request. Should set CanForwardReadTimestamp flag. Should
	// collect refresh spans.
	ba.Requests = nil
	scanArgs := kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyB}}
	ba.Add(&scanArgs)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.True(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &kvpb.ScanRequest{}, ba.Requests[0].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn.Clone()
		br.Txn.BumpReadTimestamp(refreshTS2) // server-side refresh
		return br, nil
	})

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.NotNil(t, br.Txn)
	require.Equal(t, refreshTS2, br.Txn.WriteTimestamp)
	require.Equal(t, refreshTS2, br.Txn.ReadTimestamp)
	txn.Update(br.Txn)

	require.Equal(t, refreshTS2, tsr.refreshedTimestamp)
	require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, int64(0), tsr.metrics.ClientRefreshSuccess.Count())
	require.Equal(t, int64(0), tsr.metrics.ClientRefreshFail.Count())
	require.Equal(t, int64(0), tsr.metrics.ClientRefreshAutoRetries.Count())
	require.Equal(t, int64(2), tsr.metrics.ServerRefreshSuccess.Count())

	// Send another Scan request. Should NOT set CanForwardReadTimestamp flag.
	ba.Requests = nil
	scanArgs2 := kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyC, EndKey: keyD}}
	ba.Add(&scanArgs2)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.False(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &kvpb.ScanRequest{}, ba.Requests[0].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, refreshTS2, tsr.refreshedTimestamp)
	require.Equal(t, []roachpb.Span{{Key: keyA, EndKey: keyB}, {Key: keyC, EndKey: keyD}}, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)

	// Send another Put request. Still should NOT set CanForwardReadTimestamp flag.
	ba.Requests = nil
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyB}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.False(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, refreshTS2, tsr.refreshedTimestamp)
	require.Equal(t, []roachpb.Span{{Key: keyA, EndKey: keyB}, {Key: keyC, EndKey: keyD}}, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)

	// Increment the transaction's epoch and send a ConditionalPut request. Should
	// set CanForwardReadTimestamp flag.
	ba.Requests = nil
	ba.Add(&kvpb.ConditionalPutRequest{RequestHeader: kvpb.RequestHeader{Key: keyB}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.True(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &kvpb.ConditionalPutRequest{}, ba.Requests[0].GetInner())

		// Return an error that contains a server-side refreshed transaction.
		txn := ba.Txn.Clone()
		txn.BumpReadTimestamp(refreshTS3) // server-side refresh
		pErr := kvpb.NewErrorWithTxn(&kvpb.ConditionFailedError{}, txn)
		return nil, pErr
	})

	tsr.epochBumpedLocked()
	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, br)
	require.NotNil(t, pErr)
	require.NotNil(t, pErr.GetTxn())
	require.Equal(t, refreshTS3, pErr.GetTxn().WriteTimestamp)
	require.Equal(t, refreshTS3, pErr.GetTxn().ReadTimestamp)
	txn.Update(pErr.GetTxn())

	require.Equal(t, refreshTS3, tsr.refreshedTimestamp)
	require.Equal(t, []roachpb.Span(nil), tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, int64(0), tsr.metrics.ClientRefreshSuccess.Count())
	require.Equal(t, int64(0), tsr.metrics.ClientRefreshFail.Count())
	require.Equal(t, int64(0), tsr.metrics.ClientRefreshAutoRetries.Count())
	require.Equal(t, int64(3), tsr.metrics.ServerRefreshSuccess.Count())
}

// TestTxnSpanRefresherEpochIncrement tests that a txnSpanRefresher's refresh
// spans and span validity status are reset on an epoch increment.
func TestTxnSpanRefresherEpochIncrement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tsr, _ := makeMockTxnSpanRefresher()
	// Disable span condensing.
	tsr.knobs.CondenseRefreshSpansFilter = func() bool { return false }

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	keyC, keyD := roachpb.Key("c"), roachpb.Key("d")

	// Set MaxTxnRefreshSpansBytes limit to 3 bytes plus the span overhead.
	MaxTxnRefreshSpansBytes.Override(ctx, &tsr.st.SV, 3+roachpb.SpanOverhead)

	// Send a batch below the limit.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	scanArgs := kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyB}}
	ba.Add(&scanArgs)

	br, pErr := tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, 2+roachpb.SpanOverhead, tsr.refreshFootprint.bytes)
	require.Zero(t, tsr.refreshedTimestamp)

	// Incrementing the transaction epoch clears the spans.
	tsr.epochBumpedLocked()

	require.Equal(t, []roachpb.Span(nil), tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, hlc.Timestamp{}, tsr.refreshedTimestamp)

	// Send a batch above the limit.
	ba.Requests = nil
	scanArgs2 := kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyC, EndKey: keyD}}
	ba.Add(&scanArgs, &scanArgs2)

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	require.Equal(t, []roachpb.Span(nil), tsr.refreshFootprint.asSlice())
	require.True(t, tsr.refreshInvalid)
	require.Equal(t, int64(0), tsr.refreshFootprint.bytes)
	require.Zero(t, tsr.refreshedTimestamp)

	// Incrementing the transaction epoch clears the invalid status.
	tsr.epochBumpedLocked()

	require.Equal(t, []roachpb.Span(nil), tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
	require.Zero(t, tsr.refreshedTimestamp)
}

// TestTxnSpanRefresherSavepoint checks that the span refresher can savepoint
// its state and restore it.
func TestTxnSpanRefresherSavepoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "keep-refresh-spans", func(t *testing.T, keepRefreshSpans bool) {
		ctx := context.Background()
		tsr, mockSender := makeMockTxnSpanRefresher()

		if keepRefreshSpans {
			KeepRefreshSpansOnSavepointRollback.Override(ctx, &tsr.st.SV, true)
		} else {
			KeepRefreshSpansOnSavepointRollback.Override(ctx, &tsr.st.SV, false)
		}

		keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
		txn := makeTxnProto()

		read := func(key roachpb.Key) {
			ba := &kvpb.BatchRequest{}
			ba.Header = kvpb.Header{Txn: &txn}
			getArgs := kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: key}}
			ba.Add(&getArgs)
			mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.IsType(t, &kvpb.GetRequest{}, ba.Requests[0].GetInner())

				br := ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			})
			br, pErr := tsr.SendLocked(ctx, ba)
			require.Nil(t, pErr)
			require.NotNil(t, br)
		}
		read(keyA)
		require.Equal(t, []roachpb.Span{{Key: keyA}}, tsr.refreshFootprint.asSlice())

		s := savepoint{}
		tsr.createSavepointLocked(ctx, &s)

		// Another read after the savepoint was created.
		read(keyB)
		require.Equal(t, []roachpb.Span{{Key: keyA}, {Key: keyB}}, tsr.refreshFootprint.asSlice())

		require.Equal(t, []roachpb.Span{{Key: keyA}}, s.refreshSpans)
		require.False(t, s.refreshInvalid)

		// Rollback the savepoint.
		tsr.rollbackToSavepointLocked(ctx, s)
		if keepRefreshSpans {
			// Check that refresh spans were kept as such.
			require.Equal(t, []roachpb.Span{{Key: keyA}, {Key: keyB}}, tsr.refreshFootprint.asSlice())
		} else {
			// Check that refresh spans were overwritten.
			require.Equal(t, []roachpb.Span{{Key: keyA}}, tsr.refreshFootprint.asSlice())
		}

		tsr.refreshInvalid = true
		tsr.rollbackToSavepointLocked(ctx, s)
		if keepRefreshSpans {
			// Check that rolling back to the savepoint keeps refreshInvalid as such.
			require.True(t, tsr.refreshInvalid)
		} else {
			// Check that rolling back to the savepoint resets refreshInvalid.
			require.False(t, tsr.refreshInvalid)
		}

		// Set refreshInvalid and then create a savepoint.
		tsr.refreshInvalid = true
		s = savepoint{}
		tsr.createSavepointLocked(ctx, &s)
		require.True(t, s.refreshInvalid)
		// Rollback to the savepoint check that refreshes are still invalid.
		tsr.rollbackToSavepointLocked(ctx, s)
		require.True(t, tsr.refreshInvalid)
	})
}
