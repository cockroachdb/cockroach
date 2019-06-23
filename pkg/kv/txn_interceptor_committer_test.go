// Copyright 2019 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

func makeMockTxnCommitter() (txnCommitter, *mockLockedSender) {
	mockSender := &mockLockedSender{}
	return txnCommitter{
		st:      cluster.MakeTestingClusterSettings(),
		stopper: stop.NewStopper(),
		wrapped: mockSender,
		mu:      new(syncutil.Mutex),
	}, mockSender
}

// TestTxnCommitterElideEndTransaction tests that EndTransaction requests for
// read-only transactions are removed from their batches because they are not
// necessary. The test verifies the case where the EndTransaction request is
// part of a batch with other requests and the case where it is alone in its
// batch.
func TestTxnCommitterElideEndTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc, mockSender := makeMockTxnCommitter()
	defer tc.stopper.Stop(ctx)

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	// Test with both commits and rollbacks.
	testutils.RunTrueAndFalse(t, "commit", func(t *testing.T, commit bool) {
		expStatus := roachpb.COMMITTED
		if !commit {
			expStatus = roachpb.ABORTED
		}

		// Test the case where the EndTransaction request is part of a larger
		// batch of requests.
		var ba roachpb.BatchRequest
		ba.Header = roachpb.Header{Txn: &txn}
		ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
		ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
		ba.Add(&roachpb.EndTransactionRequest{Commit: commit, IntentSpans: nil})

		mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			require.Len(t, ba.Requests, 2)
			require.IsType(t, &roachpb.GetRequest{}, ba.Requests[0].GetInner())
			require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())

			br := ba.CreateReply()
			br.Txn = ba.Txn
			// The Sender did not receive an EndTransaction request, so it keeps
			// the Txn status as PENDING.
			br.Txn.Status = roachpb.PENDING
			return br, nil
		})

		br, pErr := tc.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
		require.NotNil(t, br.Txn)
		require.Equal(t, expStatus, br.Txn.Status)

		// Test the case where the EndTransaction request is alone.
		ba.Requests = nil
		ba.Add(&roachpb.EndTransactionRequest{Commit: commit, IntentSpans: nil})

		mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			require.Fail(t, "should not have issued batch request", ba)
			return nil, nil
		})

		br, pErr = tc.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
		require.NotNil(t, br.Txn)
		require.Equal(t, expStatus, br.Txn.Status)
	})
}

// TestTxnCommitterAttachesTxnKey tests that the txnCommitter attaches the
// transaction key to committing and aborting EndTransaction requests.
func TestTxnCommitterAttachesTxnKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc, mockSender := makeMockTxnCommitter()
	defer tc.stopper.Stop(ctx)

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	// Attach IntentSpans to each EndTransaction request to avoid the elided
	// EndTransaction optimization.
	intents := []roachpb.Span{{Key: keyA}}

	// Verify that the txn key is attached to committing EndTransaction requests.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.EndTransactionRequest{Commit: true, IntentSpans: intents})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.Equal(t, keyA, ba.Requests[0].GetInner().Header().Key)
		require.Equal(t, roachpb.Key(txn.Key), ba.Requests[1].GetInner().Header().Key)

		br := ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.COMMITTED
		return br, nil
	})

	br, pErr := tc.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Verify that the txn key is attached to aborting EndTransaction requests.
	ba.Requests = nil
	ba.Add(&roachpb.EndTransactionRequest{Commit: false, IntentSpans: intents})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.Equal(t, roachpb.Key(txn.Key), ba.Requests[0].GetInner().Header().Key)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.ABORTED
		return br, nil
	})

	br, pErr = tc.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
}

// TestTxnCommitterStripsInFlightWrites tests that the txnCommitter strips the
// pipelined writes that have yet to be proven and the new writes that are part
// of the same batch as an EndTransaction request from the in-flight write set
// when a parallel commit is not desired. It also tests that it keeps the
// in-flight writes attached to the EndTransaction request when a parallel
// commit is desired.
func TestTxnCommitterStripsInFlightWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc, mockSender := makeMockTxnCommitter()
	defer tc.stopper.Stop(ctx)

	// Start with parallel commits disabled. Should NOT attach in-flight writes.
	parallelCommitsEnabled.Override(&tc.st.SV, false)

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")

	// Verify that the QueryIntent and the Put are both attached as intent spans
	// to the committing EndTransaction request when expected.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	qiArgs := roachpb.QueryIntentRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	putArgs := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyB}}
	etArgs := roachpb.EndTransactionRequest{Commit: true}
	qiArgs.Txn.Sequence = 1
	putArgs.Sequence = 2
	etArgs.Sequence = 3
	etArgs.InFlightWrites = []roachpb.SequencedWrite{
		{Key: keyA, Sequence: 1}, {Key: keyB, Sequence: 2},
	}
	etArgsCopy := etArgs
	ba.Add(&putArgs, &qiArgs, &etArgsCopy)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.IsType(t, &roachpb.EndTransactionRequest{}, ba.Requests[2].GetInner())

		et := ba.Requests[2].GetInner().(*roachpb.EndTransactionRequest)
		require.True(t, et.Commit)
		require.Len(t, et.IntentSpans, 2)
		require.Equal(t, []roachpb.Span{{Key: keyA}, {Key: keyB}}, et.IntentSpans)
		require.Len(t, et.InFlightWrites, 0)

		br := ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.COMMITTED
		return br, nil
	})

	br, pErr := tc.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Enable parallel commits and send the same batch. Should attach in-flight writes.
	parallelCommitsEnabled.Override(&tc.st.SV, true)

	ba.Requests = nil
	etArgsCopy = etArgs
	ba.Add(&putArgs, &qiArgs, &etArgsCopy)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.IsType(t, &roachpb.EndTransactionRequest{}, ba.Requests[2].GetInner())

		et := ba.Requests[2].GetInner().(*roachpb.EndTransactionRequest)
		require.True(t, et.Commit)
		require.Len(t, et.InFlightWrites, 2)
		require.Equal(t, roachpb.SequencedWrite{Key: keyA, Sequence: 1}, et.InFlightWrites[0])
		require.Equal(t, roachpb.SequencedWrite{Key: keyB, Sequence: 2}, et.InFlightWrites[1])

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.COMMITTED
		return br, nil
	})

	br, pErr = tc.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Send the same batch but with an EndTransaction containing a commit trigger.
	// In-flight writes should not be attached because commit triggers disable
	// parallel commits.
	ba.Requests = nil
	etArgsWithTrigger := etArgs
	etArgsWithTrigger.InternalCommitTrigger = &roachpb.InternalCommitTrigger{
		ModifiedSpanTrigger: &roachpb.ModifiedSpanTrigger{
			SystemConfigSpan: true,
		},
	}
	ba.Add(&putArgs, &qiArgs, &etArgsWithTrigger)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.IsType(t, &roachpb.EndTransactionRequest{}, ba.Requests[2].GetInner())

		et := ba.Requests[2].GetInner().(*roachpb.EndTransactionRequest)
		require.True(t, et.Commit)
		require.Len(t, et.IntentSpans, 2)
		require.Equal(t, []roachpb.Span{{Key: keyA}, {Key: keyB}}, et.IntentSpans)
		require.Len(t, et.InFlightWrites, 0)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.COMMITTED
		return br, nil
	})

	br, pErr = tc.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Send the same batch but with a ranged write instead of a point write.
	// In-flight writes should not be attached because ranged writes cannot
	// be parallelized with a commit.
	ba.Requests = nil
	delRngArgs := roachpb.DeleteRangeRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}}
	delRngArgs.Sequence = 2
	etArgsWithRangedIntentSpan := etArgs
	etArgsWithRangedIntentSpan.IntentSpans = []roachpb.Span{{Key: keyA, EndKey: keyB}}
	etArgsWithRangedIntentSpan.InFlightWrites = []roachpb.SequencedWrite{{Key: keyA, Sequence: 1}}
	ba.Add(&delRngArgs, &qiArgs, &etArgsWithRangedIntentSpan)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.IsType(t, &roachpb.EndTransactionRequest{}, ba.Requests[2].GetInner())

		et := ba.Requests[2].GetInner().(*roachpb.EndTransactionRequest)
		require.True(t, et.Commit)
		require.Len(t, et.IntentSpans, 1)
		require.Equal(t, []roachpb.Span{{Key: keyA, EndKey: keyB}}, et.IntentSpans)
		require.Len(t, et.InFlightWrites, 0)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.COMMITTED
		return br, nil
	})

	br, pErr = tc.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
}

// TestTxnCommitterAsyncExplicitCommitTask verifies that when txnCommitter
// performs a parallel commit and receives a STAGING transaction status,
// it launches an async task to make the transaction commit explicit.
func TestTxnCommitterAsyncExplicitCommitTask(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc, mockSender := makeMockTxnCommitter()
	defer tc.stopper.Stop(ctx)

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	// Verify that the Put is attached as in-flight write to the committing
	// EndTransaction request.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	putArgs := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	etArgs := roachpb.EndTransactionRequest{Commit: true}
	putArgs.Sequence = 1
	etArgs.Sequence = 2
	etArgs.InFlightWrites = []roachpb.SequencedWrite{{Key: keyA, Sequence: 1}}
	ba.Add(&putArgs, &etArgs)

	explicitCommitCh := make(chan struct{})
	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.EndTransactionRequest{}, ba.Requests[1].GetInner())

		et := ba.Requests[1].GetInner().(*roachpb.EndTransactionRequest)
		require.True(t, et.Commit)
		require.Len(t, et.InFlightWrites, 1)
		require.Equal(t, roachpb.SequencedWrite{Key: keyA, Sequence: 1}, et.InFlightWrites[0])

		br := ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.STAGING
		br.Responses[1].GetInner().(*roachpb.EndTransactionResponse).StagingTimestamp = br.Txn.Timestamp

		// Before returning, mock out the sender again to test against the async
		// task that should be sent to make the implicit txn commit explicit.
		mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			defer close(explicitCommitCh)
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &roachpb.EndTransactionRequest{}, ba.Requests[0].GetInner())

			et := ba.Requests[0].GetInner().(*roachpb.EndTransactionRequest)
			require.True(t, et.Commit)
			require.Len(t, et.InFlightWrites, 0)

			br = ba.CreateReply()
			br.Txn = ba.Txn
			br.Txn.Status = roachpb.COMMITTED
			return br, nil
		})
		return br, nil
	})

	br, pErr := tc.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Wait until the explicit commit succeeds.
	<-explicitCommitCh
}

// TestTxnCommitterRetryAfterStaging verifies that txnCommitter returns a retry
// error when a write performed in parallel with staging a transaction is pushed
// to a timestamp above the staging timestamp.
func TestTxnCommitterRetryAfterStaging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc, mockSender := makeMockTxnCommitter()
	defer tc.stopper.Stop(ctx)

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	putArgs := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	etArgs := roachpb.EndTransactionRequest{Commit: true}
	putArgs.Sequence = 1
	etArgs.Sequence = 2
	etArgs.InFlightWrites = []roachpb.SequencedWrite{{Key: keyA, Sequence: 1}}
	ba.Add(&putArgs, &etArgs)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.EndTransactionRequest{}, ba.Requests[1].GetInner())

		et := ba.Requests[1].GetInner().(*roachpb.EndTransactionRequest)
		require.True(t, et.Commit)
		require.Len(t, et.InFlightWrites, 1)
		require.Equal(t, roachpb.SequencedWrite{Key: keyA, Sequence: 1}, et.InFlightWrites[0])

		br := ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.STAGING
		br.Responses[1].GetInner().(*roachpb.EndTransactionResponse).StagingTimestamp = br.Txn.Timestamp

		// Pretend the PutRequest was split and sent to a different Range. It
		// could hit a WriteTooOld error (which marks the WriteTooOld flag) and
		// have its timestamp pushed if it attempts to write under a committed
		// value. The intent will be written but the response transaction's
		// timestamp will be larger than the staging timestamp.
		br.Txn.WriteTooOld = true
		br.Txn.Timestamp = br.Txn.Timestamp.Add(1, 0)
		return br, nil
	})

	br, pErr := tc.SendLocked(ctx, ba)
	require.Nil(t, br)
	require.NotNil(t, pErr)
	require.IsType(t, &roachpb.TransactionRetryError{}, pErr.GetDetail())
	require.Equal(t, roachpb.RETRY_SERIALIZABLE, pErr.GetDetail().(*roachpb.TransactionRetryError).Reason)
}
