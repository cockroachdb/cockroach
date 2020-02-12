// Copyright 2018 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

func makeMockTxnSpanRefresher() (txnSpanRefresher, *mockLockedSender) {
	mockSender := &mockLockedSender{}
	return txnSpanRefresher{
		st:               cluster.MakeTestingClusterSettings(),
		knobs:            new(ClientTestingKnobs),
		wrapped:          mockSender,
		canAutoRetry:     true,
		autoRetryCounter: metric.NewCounter(metaAutoRetriesRates),
	}, mockSender
}

// TestTxnSpanRefresherCollectsSpans tests that the txnSpanRefresher collects
// spans for requests that succeeded and would need to be refreshed if the
// transaction's provisional commit timestamp moved forward.
func TestTxnSpanRefresherCollectsSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tsr, mockSender := makeMockTxnSpanRefresher()

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	keyC, keyD := roachpb.Key("c"), roachpb.Key("d")

	// Basic case.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	getArgs := roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	putArgs := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	delRangeArgs := roachpb.DeleteRangeRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}}
	ba.Add(&getArgs, &putArgs, &delRangeArgs)

	br, pErr := tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	require.Equal(t, []roachpb.Span{getArgs.Span(), delRangeArgs.Span()}, tsr.refreshSpans)
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, int64(3), tsr.refreshSpansBytes)
	require.Equal(t, txn.ReadTimestamp, tsr.refreshedTimestamp)

	// Scan with limit. Only the scanned keys are added to the refresh spans.
	ba.Requests = nil
	scanArgs := roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyB, EndKey: keyD}}
	ba.Add(&scanArgs)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.IsType(t, &roachpb.ScanRequest{}, ba.Requests[0].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetScan().ResumeSpan = &roachpb.Span{Key: keyC, EndKey: keyD}
		return br, nil
	})

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	require.Equal(t,
		[]roachpb.Span{getArgs.Span(), delRangeArgs.Span(), {Key: scanArgs.Key, EndKey: keyC}},
		tsr.refreshSpans)
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, int64(5), tsr.refreshSpansBytes)
	require.Equal(t, txn.ReadTimestamp, tsr.refreshedTimestamp)
}

// TestTxnSpanRefresherRefreshesTransactions tests that the txnSpanRefresher
// refreshes the transaction's read and write spans if it observes an error
// that indicates that the transaction's timestamp is being pushed.
func TestTxnSpanRefresherRefreshesTransactions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	txn := makeTxnProto()
	txn.UpdateObservedTimestamp(1, txn.WriteTimestamp.Add(20, 0))
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")

	cases := []struct {
		pErr         func() *roachpb.Error
		expRefresh   bool
		expRefreshTS hlc.Timestamp
	}{
		{
			pErr: func() *roachpb.Error {
				return roachpb.NewError(
					&roachpb.TransactionRetryError{Reason: roachpb.RETRY_SERIALIZABLE})
			},
			expRefresh:   true,
			expRefreshTS: txn.WriteTimestamp,
		},
		{
			pErr: func() *roachpb.Error {
				return roachpb.NewError(
					&roachpb.TransactionRetryError{Reason: roachpb.RETRY_WRITE_TOO_OLD})
			},
			expRefresh:   true,
			expRefreshTS: txn.WriteTimestamp,
		},
		{
			pErr: func() *roachpb.Error {
				return roachpb.NewError(
					&roachpb.WriteTooOldError{ActualTimestamp: txn.WriteTimestamp.Add(15, 0)})
			},
			expRefresh:   true,
			expRefreshTS: txn.WriteTimestamp.Add(15, 0),
		},
		{
			pErr: func() *roachpb.Error {
				pErr := roachpb.NewError(&roachpb.ReadWithinUncertaintyIntervalError{})
				pErr.OriginNode = 1
				return pErr
			},
			expRefresh:   true,
			expRefreshTS: txn.WriteTimestamp.Add(20, 0), // see UpdateObservedTimestamp
		},
		{
			pErr: func() *roachpb.Error {
				pErr := roachpb.NewError(
					&roachpb.ReadWithinUncertaintyIntervalError{
						ExistingTimestamp: txn.WriteTimestamp.Add(25, 0),
					})
				pErr.OriginNode = 1
				return pErr
			},
			expRefresh:   true,
			expRefreshTS: txn.WriteTimestamp.Add(25, 1), // see ExistingTimestamp
		},
		{
			pErr: func() *roachpb.Error {
				return roachpb.NewErrorf("no refresh")
			},
			expRefresh: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.pErr().String(), func(t *testing.T) {
			ctx := context.Background()
			tsr, mockSender := makeMockTxnSpanRefresher()

			// Collect some refresh spans.
			var ba roachpb.BatchRequest
			ba.Header = roachpb.Header{Txn: txn.Clone()} // clone txn since it's shared between subtests
			getArgs := roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
			delRangeArgs := roachpb.DeleteRangeRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}}
			ba.Add(&getArgs, &delRangeArgs)

			br, pErr := tsr.SendLocked(ctx, ba)
			require.Nil(t, pErr)
			require.NotNil(t, br)

			require.Equal(t, []roachpb.Span{getArgs.Span(), delRangeArgs.Span()}, tsr.refreshSpans)
			require.False(t, tsr.refreshInvalid)
			require.Equal(t, int64(3), tsr.refreshSpansBytes)
			require.Equal(t, br.Txn.ReadTimestamp, tsr.refreshedTimestamp)

			// Hook up a chain of mocking functions.
			onFirstSend := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())

				// Return a transaction retry error.
				pErr = tc.pErr()
				pErr.SetTxn(ba.Txn)
				return nil, pErr
			}
			onSecondSend := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				// Should not be called if !expRefresh.
				require.True(t, tc.expRefresh)

				require.Len(t, ba.Requests, 1)
				require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())

				// Don't return an error.
				br = ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			}
			onRefresh := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				// Should not be called if !expRefresh.
				require.True(t, tc.expRefresh)

				require.Len(t, ba.Requests, 2)
				require.IsType(t, &roachpb.RefreshRequest{}, ba.Requests[0].GetInner())
				require.IsType(t, &roachpb.RefreshRangeRequest{}, ba.Requests[1].GetInner())

				refReq := ba.Requests[0].GetRefresh()
				require.Equal(t, getArgs.Span(), refReq.Span())

				refRngReq := ba.Requests[1].GetRefreshRange()
				require.Equal(t, delRangeArgs.Span(), refRngReq.Span())

				br = ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			}
			mockSender.ChainMockSend(onFirstSend, onRefresh, onSecondSend)

			// Send a request that will hit a retry error. Depending on the
			// error type, we may or may not perform a refresh.
			ba.Requests = nil
			putArgs := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
			ba.Add(&putArgs)

			br, pErr = tsr.SendLocked(ctx, ba)
			if tc.expRefresh {
				require.Nil(t, pErr)
				require.NotNil(t, br)
				require.Equal(t, tc.expRefreshTS, br.Txn.WriteTimestamp)
				require.Equal(t, tc.expRefreshTS, br.Txn.ReadTimestamp)
				require.Equal(t, tc.expRefreshTS, tsr.refreshedTimestamp)
			} else {
				require.Nil(t, br)
				require.NotNil(t, pErr)
				require.Equal(t, ba.Txn.ReadTimestamp, tsr.refreshedTimestamp)
			}
		})
	}
}

// TestTxnSpanRefresherMaxRefreshAttempts tests that the txnSpanRefresher
// attempts some number of retries before giving up and passing retryable
// errors back up the stack.
func TestTxnSpanRefresherMaxRefreshAttempts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tsr, mockSender := makeMockTxnSpanRefresher()

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")

	// Set MaxTxnRefreshAttempts to 2.
	tsr.knobs.MaxTxnRefreshAttempts = 2

	// Collect some refresh spans.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	scanArgs := roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}}
	ba.Add(&scanArgs)

	br, pErr := tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshSpans)
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, int64(2), tsr.refreshSpansBytes)
	require.Equal(t, br.Txn.ReadTimestamp, tsr.refreshedTimestamp)

	// Hook up a chain of mocking functions.
	onPut := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())

		// Return a transaction retry error.
		return nil, roachpb.NewErrorWithTxn(
			roachpb.NewTransactionRetryError(roachpb.RETRY_SERIALIZABLE, ""), ba.Txn)
	}
	refreshes := 0
	onRefresh := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		refreshes++
		require.Len(t, ba.Requests, 1)
		require.IsType(t, &roachpb.RefreshRangeRequest{}, ba.Requests[0].GetInner())

		refReq := ba.Requests[0].GetRefreshRange()
		require.Equal(t, scanArgs.Span(), refReq.Span())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	}
	unexpected := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Fail(t, "unexpected")
		return nil, nil
	}
	mockSender.ChainMockSend(onPut, onRefresh, onPut, onRefresh, onPut, unexpected)

	// Send a request that will hit a retry error. It will successfully retry
	// but continue to hit a retry error each time it is attempted. Eventually,
	// the txnSpanRefresher should give up and propagate the error.
	ba.Requests = nil
	putArgs := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyB}}
	ba.Add(&putArgs)

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, br)
	require.NotNil(t, pErr)
	exp := roachpb.NewTransactionRetryError(roachpb.RETRY_SERIALIZABLE, "")
	require.Equal(t, exp, pErr.GetDetail())
	require.Equal(t, tsr.knobs.MaxTxnRefreshAttempts, refreshes)
}

// TestTxnSpanRefresherMaxTxnRefreshSpansBytes tests that the txnSpanRefresher
// only collects up to kv.transaction.max_refresh_spans_bytes refresh bytes
// before throwing away refresh spans and refusing to attempt to refresh
// transactions.
func TestTxnSpanRefresherMaxTxnRefreshSpansBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tsr, _ := makeMockTxnSpanRefresher()

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	keyC, keyD := roachpb.Key("c"), roachpb.Key("d")

	// Set MaxTxnRefreshSpansBytes limit to 3 bytes.
	MaxTxnRefreshSpansBytes.Override(&tsr.st.SV, 3)

	// Send a batch below the limit.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	scanArgs := roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}}
	ba.Add(&scanArgs)

	br, pErr := tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshSpans)
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, int64(2), tsr.refreshSpansBytes)
	require.Equal(t, txn.ReadTimestamp, tsr.refreshedTimestamp)

	// Send another batch that pushes us above the limit. The refresh spans
	// should become invalid.
	ba.Requests = nil
	scanArgs2 := roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyB, EndKey: keyC}}
	ba.Add(&scanArgs2)

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	require.Equal(t, []roachpb.Span(nil), tsr.refreshSpans)
	require.True(t, tsr.refreshInvalid)
	require.Equal(t, int64(0), tsr.refreshSpansBytes)
	require.Equal(t, txn.ReadTimestamp, tsr.refreshedTimestamp)

	// Once invalid, the refresh spans should stay invalid.
	ba.Requests = nil
	scanArgs3 := roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyC, EndKey: keyD}}
	ba.Add(&scanArgs3)

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	require.Equal(t, []roachpb.Span(nil), tsr.refreshSpans)
	require.True(t, tsr.refreshInvalid)
	require.Equal(t, int64(0), tsr.refreshSpansBytes)
	require.Equal(t, txn.ReadTimestamp, tsr.refreshedTimestamp)
}

// TestTxnSpanRefresherAssignsCanCommitAtHigherTimestamp tests that the
// txnSpanRefresher assigns the CanCommitAtHigherTimestamp flag on EndTxn
// requests.
func TestTxnSpanRefresherAssignsCanCommitAtHigherTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tsr, mockSender := makeMockTxnSpanRefresher()

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	keyC, keyD := roachpb.Key("c"), roachpb.Key("d")

	// Set MaxTxnRefreshSpansBytes limit to 3 bytes.
	MaxTxnRefreshSpansBytes.Override(&tsr.st.SV, 3)

	// Send an EndTxn request. Should set CanCommitAtHigherTimestamp flag.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.EndTxnRequest{})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.IsType(t, &roachpb.EndTxnRequest{}, ba.Requests[0].GetInner())
		require.True(t, ba.Requests[0].GetEndTxn().CanCommitAtHigherTimestamp)

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Send a batch below the limit to collect refresh spans.
	ba.Requests = nil
	scanArgs := roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}}
	ba.Add(&scanArgs)

	mockSender.Reset()
	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshSpans)
	require.False(t, tsr.refreshInvalid)

	// Send another EndTxn request. Should NOT set CanCommitAtHigherTimestamp flag.
	ba.Requests = nil
	ba.Add(&roachpb.EndTxnRequest{})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.IsType(t, &roachpb.EndTxnRequest{}, ba.Requests[0].GetInner())
		require.False(t, ba.Requests[0].GetEndTxn().CanCommitAtHigherTimestamp)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Send another batch to push the spans above the limit.
	ba.Requests = nil
	scanArgs2 := roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyC, EndKey: keyD}}
	ba.Add(&scanArgs2)

	mockSender.Reset()
	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, []roachpb.Span(nil), tsr.refreshSpans)
	require.True(t, tsr.refreshInvalid)

	// Send another EndTxn request. Still should NOT set CanCommitAtHigherTimestamp flag.
	ba.Requests = nil
	ba.Add(&roachpb.EndTxnRequest{})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.IsType(t, &roachpb.EndTxnRequest{}, ba.Requests[0].GetInner())
		require.False(t, ba.Requests[0].GetEndTxn().CanCommitAtHigherTimestamp)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
}

// TestTxnSpanRefresherEpochIncrement tests that a txnSpanRefresher's refresh
// spans and span validity status are reset on an epoch increment.
func TestTxnSpanRefresherEpochIncrement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tsr, _ := makeMockTxnSpanRefresher()

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	keyC, keyD := roachpb.Key("c"), roachpb.Key("d")

	// Set MaxTxnRefreshSpansBytes limit to 3 bytes.
	MaxTxnRefreshSpansBytes.Override(&tsr.st.SV, 3)

	// Send a batch below the limit.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	scanArgs := roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}}
	ba.Add(&scanArgs)

	br, pErr := tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshSpans)
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, int64(2), tsr.refreshSpansBytes)
	require.Equal(t, txn.ReadTimestamp, tsr.refreshedTimestamp)

	// Incrementing the transaction epoch clears the spans.
	tsr.epochBumpedLocked()

	require.Equal(t, []roachpb.Span(nil), tsr.refreshSpans)
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, int64(0), tsr.refreshSpansBytes)
	require.Equal(t, hlc.Timestamp{}, tsr.refreshedTimestamp)

	// Send a batch above the limit.
	ba.Requests = nil
	scanArgs2 := roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyC, EndKey: keyD}}
	ba.Add(&scanArgs, &scanArgs2)

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	require.Equal(t, []roachpb.Span(nil), tsr.refreshSpans)
	require.True(t, tsr.refreshInvalid)
	require.Equal(t, int64(0), tsr.refreshSpansBytes)
	require.Equal(t, txn.ReadTimestamp, tsr.refreshedTimestamp)

	// Incrementing the transaction epoch clears the invalid status.
	tsr.epochBumpedLocked()

	require.Equal(t, []roachpb.Span(nil), tsr.refreshSpans)
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, int64(0), tsr.refreshSpansBytes)
	require.Equal(t, hlc.Timestamp{}, tsr.refreshedTimestamp)
}
