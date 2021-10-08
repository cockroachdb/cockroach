// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"
	"fmt"
	"strconv"
	"testing"

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
	return txnSpanRefresher{
		st:                            cluster.MakeTestingClusterSettings(),
		knobs:                         new(ClientTestingKnobs),
		wrapped:                       mockSender,
		canAutoRetry:                  true,
		refreshSuccess:                metric.NewCounter(metaRefreshSuccess),
		refreshFail:                   metric.NewCounter(metaRefreshFail),
		refreshFailWithCondensedSpans: metric.NewCounter(metaRefreshFailWithCondensedSpans),
		refreshMemoryLimitExceeded:    metric.NewCounter(metaRefreshMemoryLimitExceeded),
		refreshAutoRetries:            metric.NewCounter(metaRefreshAutoRetries),
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
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	getArgs := roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	putArgs := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	delRangeArgs := roachpb.DeleteRangeRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}}
	ba.Add(&getArgs, &putArgs, &delRangeArgs)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.True(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &roachpb.GetRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.DeleteRangeRequest{}, ba.Requests[2].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	require.Equal(t, []roachpb.Span{getArgs.Span(), delRangeArgs.Span()},
		tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, int64(3), tsr.refreshFootprint.bytes)
	require.Equal(t, txn.ReadTimestamp, tsr.refreshedTimestamp)

	// Scan with limit. Only the scanned keys are added to the refresh spans.
	ba.Requests = nil
	scanArgs := roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyB, EndKey: keyD}}
	ba.Add(&scanArgs)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.False(t, ba.CanForwardReadTimestamp)
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
		tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, int64(5), tsr.refreshFootprint.bytes)
	require.Equal(t, txn.ReadTimestamp, tsr.refreshedTimestamp)
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
		onFirstSend  func(request roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)
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
				return roachpb.NewError(
					&roachpb.ReadWithinUncertaintyIntervalError{
						ExistingTimestamp: txn.WriteTimestamp.Add(25, 0),
					})
			},
			expRefresh:   true,
			expRefreshTS: txn.WriteTimestamp.Add(25, 1), // see ExistingTimestamp
		},
		{
			pErr: func() *roachpb.Error {
				return roachpb.NewError(
					&roachpb.ReadWithinUncertaintyIntervalError{
						ExistingTimestamp:     txn.WriteTimestamp.Add(25, 0),
						LocalUncertaintyLimit: txn.WriteTimestamp.Add(30, 0),
					})
			},
			expRefresh:   true,
			expRefreshTS: txn.WriteTimestamp.Add(30, 0), // see LocalUncertaintyLimit
		},
		{
			pErr: func() *roachpb.Error {
				return roachpb.NewErrorf("no refresh")
			},
			expRefresh: false,
		},
		{
			name: "write_too_old flag",
			onFirstSend: func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				br := ba.CreateReply()
				br.Txn = ba.Txn.Clone()
				br.Txn.WriteTooOld = true
				br.Txn.WriteTimestamp = txn.WriteTimestamp.Add(20, 1)
				return br, nil
			},
			expRefresh:   true,
			expRefreshTS: txn.WriteTimestamp.Add(20, 1), // Same as br.Txn.WriteTimestamp.
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
			var ba roachpb.BatchRequest
			ba.Header = roachpb.Header{Txn: txn.Clone()} // clone txn since it's shared between subtests
			getArgs := roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
			delRangeArgs := roachpb.DeleteRangeRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}}
			ba.Add(&getArgs, &delRangeArgs)

			br, pErr := tsr.SendLocked(ctx, ba)
			require.Nil(t, pErr)
			require.NotNil(t, br)

			require.Equal(t, []roachpb.Span{getArgs.Span(), delRangeArgs.Span()}, tsr.refreshFootprint.asSlice())
			require.False(t, tsr.refreshInvalid)
			require.Equal(t, br.Txn.ReadTimestamp, tsr.refreshedTimestamp)

			// Hook up a chain of mocking functions.
			onFirstSend := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())

				// Return a transaction retry error.
				if tc.onFirstSend != nil {
					return tc.onFirstSend(ba)
				}
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
				require.Equal(t, tc.expRefreshTS, ba.Txn.ReadTimestamp)
				require.IsType(t, &roachpb.RefreshRequest{}, ba.Requests[0].GetInner())
				require.IsType(t, &roachpb.RefreshRangeRequest{}, ba.Requests[1].GetInner())

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
			putArgs := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
			ba.Add(&putArgs)

			br, pErr = tsr.SendLocked(ctx, ba)
			if tc.expRefresh {
				require.Nil(t, pErr)
				require.NotNil(t, br)
				require.Equal(t, tc.expRefreshTS, br.Txn.WriteTimestamp)
				require.Equal(t, tc.expRefreshTS, br.Txn.ReadTimestamp)
				require.Equal(t, tc.expRefreshTS, tsr.refreshedTimestamp)
				require.Equal(t, int64(1), tsr.refreshSuccess.Count())
				require.Equal(t, int64(0), tsr.refreshFail.Count())
				require.Equal(t, int64(1), tsr.refreshAutoRetries.Count())
			} else {
				require.Nil(t, br)
				require.NotNil(t, pErr)
				require.Equal(t, ba.Txn.ReadTimestamp, tsr.refreshedTimestamp)
				require.Equal(t, int64(0), tsr.refreshSuccess.Count())
				require.Equal(t, int64(0), tsr.refreshAutoRetries.Count())
				// Note that we don't check the tsr.refreshFail metric here as tests
				// here expect the refresh to not be attempted, not to fail.
			}
		})
	}
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
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	scanArgs := roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}}
	ba.Add(&scanArgs)

	br, pErr := tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
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
		require.Equal(t, txn.WriteTimestamp, ba.Txn.ReadTimestamp)
		require.IsType(t, &roachpb.RefreshRangeRequest{}, ba.Requests[0].GetInner())

		refReq := ba.Requests[0].GetRefreshRange()
		require.Equal(t, scanArgs.Span(), refReq.Span())
		require.Equal(t, txn.ReadTimestamp, refReq.RefreshFrom)

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
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	etArgs := roachpb.EndTxnRequest{Commit: true}
	ba.Add(&etArgs)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.True(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &roachpb.EndTxnRequest{}, ba.Requests[0].GetInner())

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
	require.Equal(t, int64(1), tsr.refreshSuccess.Count())
	require.Equal(t, int64(0), tsr.refreshFail.Count())
	require.Equal(t, int64(0), tsr.refreshAutoRetries.Count())
	require.True(t, tsr.refreshFootprint.empty())
	require.False(t, tsr.refreshInvalid)

	// Reset refreshedTimestamp to avoid confusing ourselves.
	tsr.refreshedTimestamp = origReadTs

	// Send a Scan request. Again, because a preemptive refresh would be free,
	// the txnSpanRefresher should do so. NOTE: This inhibits a server-side
	// refreshes when we issue EndTxn requests through the rest of this test.
	ba.Requests = nil
	scanArgs := roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}}
	ba.Add(&scanArgs)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.True(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &roachpb.ScanRequest{}, ba.Requests[0].GetInner())

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
	require.Equal(t, int64(2), tsr.refreshSuccess.Count())
	require.Equal(t, int64(0), tsr.refreshFail.Count())
	require.Equal(t, int64(0), tsr.refreshAutoRetries.Count())
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

	onRefresh := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.Equal(t, pushedWriteTs, ba.Txn.ReadTimestamp)
		require.IsType(t, &roachpb.RefreshRangeRequest{}, ba.Requests[0].GetInner())

		refReq := ba.Requests[0].GetRefreshRange()
		require.Equal(t, scanArgs.Span(), refReq.Span())
		require.Equal(t, origReadTs, refReq.RefreshFrom)

		return nil, roachpb.NewErrorf("encountered recently written key")
	}
	unexpected := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Fail(t, "unexpected")
		return nil, nil
	}
	mockSender.ChainMockSend(onRefresh, unexpected)

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, br)
	require.NotNil(t, pErr)
	require.Regexp(t, `TransactionRetryError: retry txn \(RETRY_SERIALIZABLE - failed preemptive refresh\)`, pErr)
	require.Equal(t, int64(2), tsr.refreshSuccess.Count())
	require.Equal(t, int64(1), tsr.refreshFail.Count())
	require.Equal(t, int64(0), tsr.refreshAutoRetries.Count())
	require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)

	// Try again, but this time let the refresh succeed.
	onRefresh = func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.Equal(t, pushedWriteTs, ba.Txn.ReadTimestamp)
		require.IsType(t, &roachpb.RefreshRangeRequest{}, ba.Requests[0].GetInner())

		refReq := ba.Requests[0].GetRefreshRange()
		require.Equal(t, scanArgs.Span(), refReq.Span())
		require.Equal(t, origReadTs, refReq.RefreshFrom)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	}
	onEndTxn := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.False(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &roachpb.EndTxnRequest{}, ba.Requests[0].GetInner())

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
	require.Equal(t, int64(3), tsr.refreshSuccess.Count())
	require.Equal(t, int64(1), tsr.refreshFail.Count())
	require.Equal(t, int64(0), tsr.refreshAutoRetries.Count())
	require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
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

	scanArgs := roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}}
	putArgs := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyB}}
	etArgs := roachpb.EndTxnRequest{Commit: true}

	// Run the test with two slightly different configurations. When priorReads
	// is true, issue a {Put, EndTxn} batch after having previously accumulated
	// refresh spans due to a Scan. When priorReads is false, issue a {Scan,
	// Put, EndTxn} batch with no previously accumulated refresh spans.
	testutils.RunTrueAndFalse(t, "prior_reads", func(t *testing.T, priorReads bool) {
		var mockFns []func(roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)
		if priorReads {
			// Hook up a chain of mocking functions. Expected order of requests:
			// 1. {Put, EndTxn} -> retry error with pushed timestamp
			// 2. {Refresh}     -> successful
			// 3. {Put}         -> successful with pushed timestamp
			// 4. {Refresh}     -> successful
			// 5. {EndTxn}      -> successful
			onPutAndEndTxn := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				require.Len(t, ba.Requests, 2)
				require.False(t, ba.CanForwardReadTimestamp)
				require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
				require.IsType(t, &roachpb.EndTxnRequest{}, ba.Requests[1].GetInner())

				pushedTxn := ba.Txn.Clone()
				pushedTxn.WriteTimestamp = pushedTs1
				return nil, roachpb.NewErrorWithTxn(
					roachpb.NewTransactionRetryError(roachpb.RETRY_SERIALIZABLE, ""), pushedTxn)
			}
			onRefresh1 := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.Equal(t, pushedTs1, ba.Txn.ReadTimestamp)
				require.IsType(t, &roachpb.RefreshRangeRequest{}, ba.Requests[0].GetInner())

				refReq := ba.Requests[0].GetRefreshRange()
				require.Equal(t, scanArgs.Span(), refReq.Span())
				require.Equal(t, origTs, refReq.RefreshFrom)

				br := ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			}
			onPut := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.False(t, ba.CanForwardReadTimestamp)
				require.Equal(t, pushedTs1, ba.Txn.ReadTimestamp)
				require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())

				br := ba.CreateReply()
				br.Txn = ba.Txn.Clone()
				br.Txn.WriteTimestamp = pushedTs2
				return br, nil
			}
			onRefresh2 := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.Equal(t, pushedTs2, ba.Txn.ReadTimestamp)
				require.IsType(t, &roachpb.RefreshRangeRequest{}, ba.Requests[0].GetInner())

				refReq := ba.Requests[0].GetRefreshRange()
				require.Equal(t, scanArgs.Span(), refReq.Span())
				require.Equal(t, pushedTs1, refReq.RefreshFrom)

				br := ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			}
			onEndTxn := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.False(t, ba.CanForwardReadTimestamp)
				require.Equal(t, pushedTs2, ba.Txn.ReadTimestamp)
				require.IsType(t, &roachpb.EndTxnRequest{}, ba.Requests[0].GetInner())

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
			onScanPutAndEndTxn := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				require.Len(t, ba.Requests, 3)
				require.True(t, ba.CanForwardReadTimestamp)
				require.IsType(t, &roachpb.ScanRequest{}, ba.Requests[0].GetInner())
				require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())
				require.IsType(t, &roachpb.EndTxnRequest{}, ba.Requests[2].GetInner())

				pushedTxn := ba.Txn.Clone()
				pushedTxn.WriteTimestamp = pushedTs1
				return nil, roachpb.NewErrorWithTxn(
					roachpb.NewTransactionRetryError(roachpb.RETRY_SERIALIZABLE, ""), pushedTxn)
			}
			onScanAndPut := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				require.Len(t, ba.Requests, 2)
				require.True(t, ba.CanForwardReadTimestamp)
				require.Equal(t, pushedTs1, ba.Txn.ReadTimestamp)
				require.IsType(t, &roachpb.ScanRequest{}, ba.Requests[0].GetInner())
				require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())

				br := ba.CreateReply()
				br.Txn = ba.Txn.Clone()
				br.Txn.WriteTimestamp = pushedTs2
				return br, nil
			}
			onRefresh := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.Equal(t, pushedTs2, ba.Txn.ReadTimestamp)
				require.IsType(t, &roachpb.RefreshRangeRequest{}, ba.Requests[0].GetInner())

				refReq := ba.Requests[0].GetRefreshRange()
				require.Equal(t, scanArgs.Span(), refReq.Span())
				require.Equal(t, pushedTs1, refReq.RefreshFrom)

				br := ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			}
			onEndTxn := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				require.Len(t, ba.Requests, 1)
				// IMPORTANT! CanForwardReadTimestamp should no longer be set
				// for EndTxn batch, because the Scan in the earlier batch needs
				// to be refreshed if the read timestamp changes.
				require.False(t, ba.CanForwardReadTimestamp)
				require.Equal(t, pushedTs2, ba.Txn.ReadTimestamp)
				require.IsType(t, &roachpb.EndTxnRequest{}, ba.Requests[0].GetInner())

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

				var ba roachpb.BatchRequest
				if priorReads {
					// Collect some refresh spans first.
					ba.Header = roachpb.Header{Txn: &txn}
					ba.Add(&scanArgs)

					br, pErr := tsr.SendLocked(ctx, ba)
					require.Nil(t, pErr)
					require.NotNil(t, br)
					require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshFootprint.asSlice())
					require.False(t, tsr.refreshInvalid)
					require.Equal(t, br.Txn.ReadTimestamp, tsr.refreshedTimestamp)

					ba.Requests = nil
					ba.Add(&putArgs, &etArgs)
				} else {
					// No refresh spans to begin with.
					require.Equal(t, []roachpb.Span(nil), tsr.refreshFootprint.asSlice())

					ba.Header = roachpb.Header{Txn: &txn}
					ba.Add(&scanArgs, &putArgs, &etArgs)
				}

				// Construct the mock sender chain, injecting an error where
				// appropriate. Make a copy of mockFns to avoid sharing state
				// between subtests.
				mockFnsCpy := append([]func(roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)(nil), mockFns...)
				if errIdx < len(mockFnsCpy) {
					errFn := mockFnsCpy[errIdx]
					newErrFn := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
						_, _ = errFn(ba)
						return nil, roachpb.NewErrorf("error")
					}
					mockFnsCpy[errIdx] = newErrFn
				}
				unexpected := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
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
						require.IsType(t, &roachpb.PutResponse{}, br.Responses[0].GetInner())
						require.IsType(t, &roachpb.EndTxnResponse{}, br.Responses[1].GetInner())
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
					require.Equal(t, expSuccess, tsr.refreshSuccess.Count())
					require.Equal(t, expFail, tsr.refreshFail.Count())
					require.Equal(t, expAutoRetries, tsr.refreshAutoRetries.Count())

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
						require.IsType(t, &roachpb.ScanResponse{}, br.Responses[0].GetInner())
						require.IsType(t, &roachpb.PutResponse{}, br.Responses[1].GetInner())
						require.IsType(t, &roachpb.EndTxnResponse{}, br.Responses[2].GetInner())
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
					require.Equal(t, expSuccess, tsr.refreshSuccess.Count())
					require.Equal(t, expFail, tsr.refreshFail.Count())
					require.Equal(t, expAutoRetries, tsr.refreshAutoRetries.Count())

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

	// Set MaxTxnRefreshSpansBytes limit to 3 bytes.
	MaxTxnRefreshSpansBytes.Override(ctx, &tsr.st.SV, 3)

	// Send a batch below the limit.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	scanArgs := roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}}
	ba.Add(&scanArgs)

	br, pErr := tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, txn.ReadTimestamp, tsr.refreshedTimestamp)
	require.Equal(t, int64(2), tsr.refreshFootprint.bytes)

	// Send another batch that pushes us above the limit. The tracked spans are
	// adjacent so the spans will be merged, but not condensed.
	ba.Requests = nil
	scanArgs2 := roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyB, EndKey: keyC}}
	ba.Add(&scanArgs2)

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	require.Equal(t, []roachpb.Span{{Key: keyA, EndKey: keyC}}, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, int64(2), tsr.refreshFootprint.bytes)
	require.False(t, tsr.refreshFootprint.condensed)
	require.Equal(t, int64(0), tsr.refreshMemoryLimitExceeded.Count())
	require.Equal(t, txn.ReadTimestamp, tsr.refreshedTimestamp)

	// Exceed the limit again, this time with a non-adjacent span such that
	// condensing needs to occur.
	ba.Requests = nil
	scanArgs3 := roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyD, EndKey: keyE}}
	ba.Add(&scanArgs3)

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	require.Equal(t, []roachpb.Span{{Key: keyA, EndKey: keyE}}, tsr.refreshFootprint.asSlice())
	require.True(t, tsr.refreshFootprint.condensed)
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, txn.ReadTimestamp, tsr.refreshedTimestamp)
	require.Equal(t, int64(1), tsr.refreshMemoryLimitExceeded.Count())
	require.Equal(t, int64(0), tsr.refreshFailWithCondensedSpans.Count())

	// Return a transaction retry error and make sure the metric indicating that
	// we did not retry due to the refresh span bytes in incremented.
	mockSender.MockSend(func(request roachpb.BatchRequest) (batchResponse *roachpb.BatchResponse, r *roachpb.Error) {
		return nil, roachpb.NewErrorWithTxn(
			roachpb.NewTransactionRetryError(roachpb.RETRY_SERIALIZABLE, ""), ba.Txn)
	})

	br, pErr = tsr.SendLocked(ctx, ba)
	exp := roachpb.NewTransactionRetryError(roachpb.RETRY_SERIALIZABLE, "")
	require.Equal(t, exp, pErr.GetDetail())
	require.Nil(t, br)
	require.Equal(t, int64(1), tsr.refreshFailWithCondensedSpans.Count())
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

	// Send a Put request. Should set CanForwardReadTimestamp flag. Should not
	// collect refresh spans.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.True(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Nil(t, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)

	// Send a Put request for a transaction with a fixed commit timestamp.
	// Should NOT set CanForwardReadTimestamp flag.
	txnFixed := txn.Clone()
	txnFixed.CommitTimestampFixed = true
	var baFixed roachpb.BatchRequest
	baFixed.Header = roachpb.Header{Txn: txnFixed}
	baFixed.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.False(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tsr.SendLocked(ctx, baFixed)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Nil(t, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)

	// Send a Scan request. Should set CanForwardReadTimestamp flag. Should
	// collect refresh spans.
	ba.Requests = nil
	scanArgs := roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}}
	ba.Add(&scanArgs)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.True(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &roachpb.ScanRequest{}, ba.Requests[0].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)

	// Send another Scan request. Should NOT set CanForwardReadTimestamp flag.
	ba.Requests = nil
	scanArgs2 := roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyC, EndKey: keyD}}
	ba.Add(&scanArgs2)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.False(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &roachpb.ScanRequest{}, ba.Requests[0].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, []roachpb.Span{{Key: keyA, EndKey: keyB}, {Key: keyC, EndKey: keyD}}, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)

	// Send another Put request. Still should NOT set CanForwardReadTimestamp flag.
	ba.Requests = nil
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyB}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.False(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, []roachpb.Span{{Key: keyA, EndKey: keyB}, {Key: keyC, EndKey: keyD}}, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)

	// Increment the transaction's epoch and send another Put request. Should
	// set CanForwardReadTimestamp flag.
	ba.Requests = nil
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyB}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.True(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	tsr.epochBumpedLocked()
	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, []roachpb.Span(nil), tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
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

	// Set MaxTxnRefreshSpansBytes limit to 3 bytes.
	MaxTxnRefreshSpansBytes.Override(ctx, &tsr.st.SV, 3)

	// Send a batch below the limit.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	scanArgs := roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}}
	ba.Add(&scanArgs)

	br, pErr := tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	require.Equal(t, []roachpb.Span{scanArgs.Span()}, tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, int64(2), tsr.refreshFootprint.bytes)
	require.Equal(t, txn.ReadTimestamp, tsr.refreshedTimestamp)

	// Incrementing the transaction epoch clears the spans.
	tsr.epochBumpedLocked()

	require.Equal(t, []roachpb.Span(nil), tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, hlc.Timestamp{}, tsr.refreshedTimestamp)

	// Send a batch above the limit.
	ba.Requests = nil
	scanArgs2 := roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyC, EndKey: keyD}}
	ba.Add(&scanArgs, &scanArgs2)

	br, pErr = tsr.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	require.Equal(t, []roachpb.Span(nil), tsr.refreshFootprint.asSlice())
	require.True(t, tsr.refreshInvalid)
	require.Equal(t, int64(0), tsr.refreshFootprint.bytes)
	require.Equal(t, txn.ReadTimestamp, tsr.refreshedTimestamp)

	// Incrementing the transaction epoch clears the invalid status.
	tsr.epochBumpedLocked()

	require.Equal(t, []roachpb.Span(nil), tsr.refreshFootprint.asSlice())
	require.False(t, tsr.refreshInvalid)
	require.Equal(t, hlc.Timestamp{}, tsr.refreshedTimestamp)
}

// TestTxnSpanRefresherSavepoint checks that the span refresher can savepoint
// its state and restore it.
func TestTxnSpanRefresherSavepoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tsr, mockSender := makeMockTxnSpanRefresher()

	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	txn := makeTxnProto()

	read := func(key roachpb.Key) {
		var ba roachpb.BatchRequest
		ba.Header = roachpb.Header{Txn: &txn}
		getArgs := roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: key}}
		ba.Add(&getArgs)
		mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &roachpb.GetRequest{}, ba.Requests[0].GetInner())

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

	// Rollback the savepoint and check that refresh spans were overwritten.
	tsr.rollbackToSavepointLocked(ctx, s)
	require.Equal(t, []roachpb.Span{{Key: keyA}}, tsr.refreshFootprint.asSlice())

	// Check that rolling back to the savepoint resets refreshInvalid.
	tsr.refreshInvalid = true
	tsr.rollbackToSavepointLocked(ctx, s)
	require.False(t, tsr.refreshInvalid)

	// Set refreshInvalid and then create a savepoint.
	tsr.refreshInvalid = true
	s = savepoint{}
	tsr.createSavepointLocked(ctx, &s)
	require.True(t, s.refreshInvalid)
	// Rollback to the savepoint check that refreshes are still invalid.
	tsr.rollbackToSavepointLocked(ctx, s)
	require.True(t, tsr.refreshInvalid)
}
