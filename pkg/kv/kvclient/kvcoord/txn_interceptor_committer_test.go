// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

func makeMockTxnCommitter() (txnCommitter, *mockLockedSender) {
	mockSender := &mockLockedSender{}
	metrics := MakeTxnMetrics(metric.TestSampleInterval)
	return txnCommitter{
		st:      cluster.MakeTestingClusterSettings(),
		stopper: stop.NewStopper(),
		wrapped: mockSender,
		metrics: &metrics,
		mu:      new(syncutil.Mutex),
	}, mockSender
}

// TestTxnCommitterElideEndTxn tests that EndTxn requests for read-only
// transactions are removed from their batches because they are not necessary.
// The test verifies the case where the EndTxn request is part of a batch with
// other requests and the case where it is alone in its batch.
func TestTxnCommitterElideEndTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc, mockSender := makeMockTxnCommitter()
	defer tc.stopper.Stop(ctx)

	txn := makeTxnProto()
	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")

	// Test with both commits and rollbacks.
	testutils.RunTrueAndFalse(t, "commit", func(t *testing.T, commit bool) {
		expStatus := roachpb.COMMITTED
		if !commit {
			expStatus = roachpb.ABORTED
		}

		// Test the case where the EndTxn request is part of a larger batch of
		// requests.
		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: &txn}
		ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})
		ba.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyB}})
		ba.Add(&kvpb.EndTxnRequest{Commit: commit, LockSpans: nil})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			require.Len(t, ba.Requests, 2)
			require.IsType(t, &kvpb.GetRequest{}, ba.Requests[0].GetInner())
			require.IsType(t, &kvpb.ScanRequest{}, ba.Requests[1].GetInner())

			br := ba.CreateReply()
			br.Txn = ba.Txn
			// The Sender did not receive an EndTxn request, so it keeps the Txn
			// status as PENDING.
			br.Txn.Status = roachpb.PENDING
			return br, nil
		})

		br, pErr := tc.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
		require.NotNil(t, br.Txn)
		require.Equal(t, expStatus, br.Txn.Status)

		// Test the case where the EndTxn request is alone.
		ba.Requests = nil
		ba.Add(&kvpb.EndTxnRequest{Commit: commit, LockSpans: nil})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
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
// transaction key to committing and aborting EndTxn requests.
func TestTxnCommitterAttachesTxnKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc, mockSender := makeMockTxnCommitter()
	defer tc.stopper.Stop(ctx)

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	// Attach LockSpans to each EndTxn request to avoid the elided EndTxn
	// optimization.
	intents := []roachpb.Span{{Key: keyA}}

	// Verify that the txn key is attached to committing EndTxn requests.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})
	ba.Add(&kvpb.EndTxnRequest{Commit: true, LockSpans: intents})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
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

	// Verify that the txn key is attached to aborting EndTxn requests.
	ba.Requests = nil
	ba.Add(&kvpb.EndTxnRequest{Commit: false, LockSpans: intents})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
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
// of the same batch as an EndTxn request from the in-flight write set when a
// parallel commit is not desired. It also tests that it keeps the in-flight
// writes attached to the EndTxn request when a parallel commit is desired.
func TestTxnCommitterStripsInFlightWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, mockSender := makeMockTxnCommitter()
	defer tc.stopper.Stop(ctx)

	// Start with parallel commits disabled. Should NOT attach in-flight writes.
	parallelCommitsEnabled.Override(ctx, &tc.st.SV, false)

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")

	// Verify that the QueryIntent and the Put are both attached as lock spans
	// to the committing EndTxn request when expected.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	qiArgs := kvpb.QueryIntentRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}}
	putArgs := kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyB}}
	etArgs := kvpb.EndTxnRequest{Commit: true}
	qiArgs.Txn.Sequence = 1
	putArgs.Sequence = 2
	etArgs.Sequence = 3
	etArgs.InFlightWrites = []roachpb.SequencedWrite{
		{Key: keyA, Sequence: 1}, {Key: keyB, Sequence: 2},
	}
	etArgsCopy := etArgs
	ba.Add(&putArgs, &qiArgs, &etArgsCopy)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[2].GetInner())

		et := ba.Requests[2].GetInner().(*kvpb.EndTxnRequest)
		require.True(t, et.Commit)
		require.Len(t, et.LockSpans, 2)
		require.Equal(t, []roachpb.Span{{Key: keyA}, {Key: keyB}}, et.LockSpans)
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
	parallelCommitsEnabled.Override(ctx, &tc.st.SV, true)

	ba.Requests = nil
	etArgsCopy = etArgs
	ba.Add(&putArgs, &qiArgs, &etArgsCopy)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[2].GetInner())

		et := ba.Requests[2].GetInner().(*kvpb.EndTxnRequest)
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

	// Send the same batch but with an EndTxn with the Prepare flag set. In-flight
	// writes should not be attached because the XA two-phase commit protocol
	// disables parallel commits.
	ba.Requests = nil
	etArgsPrepare := etArgs
	etArgsPrepare.Prepare = true
	ba.Add(&putArgs, &qiArgs, &etArgsPrepare)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[2].GetInner())

		et := ba.Requests[2].GetInner().(*kvpb.EndTxnRequest)
		require.True(t, et.Commit)
		require.True(t, et.Prepare)
		require.Len(t, et.LockSpans, 2)
		require.Equal(t, []roachpb.Span{{Key: keyA}, {Key: keyB}}, et.LockSpans)
		require.Len(t, et.InFlightWrites, 0)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.PREPARED
		return br, nil
	})

	br, pErr = tc.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Send the same batch but with an EndTxn containing a commit trigger.
	// In-flight writes should not be attached because commit triggers disable
	// parallel commits.
	ba.Requests = nil
	etArgsWithTrigger := etArgs
	etArgsWithTrigger.InternalCommitTrigger = &roachpb.InternalCommitTrigger{
		ModifiedSpanTrigger: &roachpb.ModifiedSpanTrigger{NodeLivenessSpan: &roachpb.Span{}},
	}
	ba.Add(&putArgs, &qiArgs, &etArgsWithTrigger)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[2].GetInner())

		et := ba.Requests[2].GetInner().(*kvpb.EndTxnRequest)
		require.True(t, et.Commit)
		require.Len(t, et.LockSpans, 2)
		require.Equal(t, []roachpb.Span{{Key: keyA}, {Key: keyB}}, et.LockSpans)
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
	delRngArgs := kvpb.DeleteRangeRequest{RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyB}}
	delRngArgs.Sequence = 2
	etArgsWithRangedIntentSpan := etArgs
	etArgsWithRangedIntentSpan.LockSpans = []roachpb.Span{{Key: keyA, EndKey: keyB}}
	etArgsWithRangedIntentSpan.InFlightWrites = []roachpb.SequencedWrite{{Key: keyA, Sequence: 1}}
	ba.Add(&delRngArgs, &qiArgs, &etArgsWithRangedIntentSpan)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[2].GetInner())

		et := ba.Requests[2].GetInner().(*kvpb.EndTxnRequest)
		require.True(t, et.Commit)
		require.Len(t, et.LockSpans, 1)
		require.Equal(t, []roachpb.Span{{Key: keyA, EndKey: keyB}}, et.LockSpans)
		require.Len(t, et.InFlightWrites, 0)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.COMMITTED
		return br, nil
	})

	br, pErr = tc.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Send the same batch but with a point read instead of a point write.
	// In-flight writes should not be attached because read-only requests
	// cannot be parallelized with a commit.
	ba.Requests = nil
	getArgs := kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}}
	getArgs.Sequence = 2
	etArgsCopy = etArgs
	ba.Add(&getArgs, &qiArgs, &etArgsCopy)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[2].GetInner())

		et := ba.Requests[2].GetInner().(*kvpb.EndTxnRequest)
		require.True(t, et.Commit)
		require.Len(t, et.LockSpans, 2)
		require.Equal(t, []roachpb.Span{{Key: keyA}, {Key: keyB}}, et.LockSpans)
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc, mockSender := makeMockTxnCommitter()
	defer tc.stopper.Stop(ctx)

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	// Verify that the Put is attached as in-flight write to the committing
	// EndTxn request.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putArgs := kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}}
	etArgs := kvpb.EndTxnRequest{Commit: true}
	putArgs.Sequence = 1
	etArgs.Sequence = 2
	etArgs.InFlightWrites = []roachpb.SequencedWrite{{Key: keyA, Sequence: 1}}
	ba.Add(&putArgs, &etArgs)

	// Set the CanForwardReadTimestamp flag so we can make sure that these are
	// propagated to the async explicit commit task.
	ba.Header.CanForwardReadTimestamp = true

	explicitCommitCh := make(chan struct{})
	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.True(t, ba.CanForwardReadTimestamp)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[1].GetInner())

		et := ba.Requests[1].GetInner().(*kvpb.EndTxnRequest)
		require.True(t, et.Commit)
		require.Len(t, et.InFlightWrites, 1)
		require.Equal(t, roachpb.SequencedWrite{Key: keyA, Sequence: 1}, et.InFlightWrites[0])

		br := ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.STAGING
		br.Responses[1].GetInner().(*kvpb.EndTxnResponse).StagingTimestamp = br.Txn.WriteTimestamp

		// Before returning, mock out the sender again to test against the async
		// task that should be sent to make the implicit txn commit explicit.
		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			defer close(explicitCommitCh)
			require.Len(t, ba.Requests, 1)
			require.False(t, ba.CanForwardReadTimestamp)
			require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())

			et := ba.Requests[0].GetInner().(*kvpb.EndTxnRequest)
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

// TestTxnCommitterRetryAfterStaging verifies that txnCommitter performs a
// parallel commit auto-retry when a write performed in parallel with staging a
// transaction is pushed to a timestamp above the staging timestamp.
func TestTxnCommitterRetryAfterStaging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testutils.RunTrueAndFalse(t, "errorOnRetry", testTxnCommitterRetryAfterStaging)
}

func testTxnCommitterRetryAfterStaging(t *testing.T, errorOnRetry bool) {
	ctx := context.Background()
	tc, mockSender := makeMockTxnCommitter()
	defer tc.stopper.Stop(ctx)

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putArgs := kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}}
	etArgs := kvpb.EndTxnRequest{Commit: true}
	putArgs.Sequence = 1
	etArgs.Sequence = 2
	etArgs.InFlightWrites = []roachpb.SequencedWrite{{Key: keyA, Sequence: 1}}
	ba.Add(&putArgs, &etArgs)

	onFirstSend := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.Equal(t, roachpb.PENDING, ba.Txn.Status)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[1].GetInner())

		et := ba.Requests[1].GetEndTxn()
		require.True(t, et.Commit)
		require.Nil(t, et.LockSpans)
		require.Len(t, et.InFlightWrites, 1)
		require.Equal(t, roachpb.SequencedWrite{Key: keyA, Sequence: 1}, et.InFlightWrites[0])

		br := ba.CreateReply()
		br.Txn = ba.Txn.Clone()
		br.Txn.Status = roachpb.STAGING
		br.Responses[1].GetEndTxn().StagingTimestamp = br.Txn.WriteTimestamp

		// Pretend the PutRequest was split and sent to a different Range. It
		// could hit the timestamp cache or a WriteTooOld error (which sets the
		// WriteTooOld flag which is stripped by the txnSpanRefresher). The intent
		// will be written but the response transaction's timestamp will be larger
		// than the staging timestamp.
		br.Txn.WriteTimestamp = br.Txn.WriteTimestamp.Add(1, 0)
		return br, nil
	}
	sawRetry := false
	onRetry := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		sawRetry = true
		require.Len(t, ba.Requests, 1)
		require.Equal(t, roachpb.PENDING, ba.Txn.Status)
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())

		et := ba.Requests[0].GetEndTxn()
		require.True(t, et.Commit)
		require.Nil(t, et.InFlightWrites)
		require.Len(t, et.LockSpans, 1)
		require.Equal(t, roachpb.Span{Key: keyA}, et.LockSpans[0])

		if errorOnRetry {
			err := kvpb.NewTransactionRetryError(kvpb.RETRY_COMMIT_DEADLINE_EXCEEDED, "")
			errTxn := ba.Txn.Clone()
			// Return a STAGING transaction status to verify that the txnCommitter
			// downgrades this status to PENDING before propagating the error.
			errTxn.Status = roachpb.STAGING
			return nil, kvpb.NewErrorWithTxn(err, errTxn)
		}
		br := ba.CreateReply()
		br.Txn = ba.Txn.Clone()
		br.Txn.Status = roachpb.COMMITTED
		return br, nil
	}
	mockSender.ChainMockSend(onFirstSend, onRetry)

	br, pErr := tc.SendLocked(ctx, ba)
	if errorOnRetry {
		require.Nil(t, br)
		require.NotNil(t, pErr)
		require.NotNil(t, pErr.GetTxn())
		require.Equal(t, roachpb.PENDING, pErr.GetTxn().Status)
		require.True(t, sawRetry)
		require.Equal(t, int64(1), tc.metrics.ParallelCommitAutoRetries.Count())
	} else {
		require.Nil(t, pErr)
		require.NotNil(t, br)
		require.NotNil(t, br.Txn)
		require.Equal(t, roachpb.COMMITTED, br.Txn.Status)
		require.True(t, sawRetry)
		require.Equal(t, int64(1), tc.metrics.ParallelCommitAutoRetries.Count())
	}
}

// TestTxnCommitterDisables1PC_DifferentBatch ensures the txn committer disables
// one phase commit for transactions that have acquired a replicated lock in a
// different batch than the one with an EndTxn request.
func TestTxnCommitterDisables1PC_DifferentBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "replicated", func(t *testing.T, replicated bool) {
		ctx := context.Background()
		tc, mockSender := makeMockTxnCommitter()
		defer tc.stopper.Stop(ctx)

		txn := makeTxnProto()
		keyA := roachpb.Key("a")

		// Verify that the txn key is attached to committing EndTxn requests.
		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: &txn}
		dur := lock.Unreplicated
		if replicated {
			dur = lock.Replicated
		}
		ba.Add(&kvpb.GetRequest{
			RequestHeader:        kvpb.RequestHeader{Key: keyA},
			KeyLockingDurability: dur,
			KeyLockingStrength:   lock.Exclusive,
		})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			require.Len(t, ba.Requests, 1)
			require.Equal(t, keyA, ba.Requests[0].GetInner().Header().Key)

			br := ba.CreateReply()
			br.Txn = ba.Txn
			return br, nil
		})

		br, pErr := tc.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)

		ba.Requests = nil
		ba.Add(&kvpb.EndTxnRequest{Commit: true})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			require.Len(t, ba.Requests, 1)
			require.Equal(t, roachpb.Key(txn.Key), ba.Requests[0].GetInner().Header().Key)
			require.Equal(t, replicated, ba.Requests[0].GetInner().(*kvpb.EndTxnRequest).Disable1PC)

			br = ba.CreateReply()
			br.Txn = ba.Txn
			br.Txn.Status = roachpb.COMMITTED
			return br, nil
		})

		br, pErr = tc.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
	})
}

// TestTxnCommitterDisables1PC_SameBatch ensures the txn committer disables
// one phase commit for transactions that have acquired a replicated lock in the
// same batch as the one with an EndTxn request.
func TestTxnCommitterDisables1PC_SameBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "replicated", func(t *testing.T, replicated bool) {
		ctx := context.Background()
		tc, mockSender := makeMockTxnCommitter()
		defer tc.stopper.Stop(ctx)

		txn := makeTxnProto()
		keyA := roachpb.Key("a")

		// Verify that the txn key is attached to committing EndTxn requests.
		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: &txn}
		dur := lock.Unreplicated
		if replicated {
			dur = lock.Replicated
		}
		ba.Add(&kvpb.GetRequest{
			RequestHeader:        kvpb.RequestHeader{Key: keyA},
			KeyLockingDurability: dur,
			KeyLockingStrength:   lock.Exclusive,
		})
		// Attach LockSpans to EndTxn request to avoid the elided EndTxn
		// optimization.
		intents := []roachpb.Span{{Key: keyA}}
		ba.Add(&kvpb.EndTxnRequest{Commit: true, LockSpans: intents})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			require.Len(t, ba.Requests, 2)
			require.Equal(t, keyA, ba.Requests[0].GetInner().Header().Key)
			require.Equal(t, roachpb.Key(txn.Key), ba.Requests[1].GetInner().Header().Key)
			require.Equal(t, replicated, ba.Requests[1].GetInner().(*kvpb.EndTxnRequest).Disable1PC)

			br := ba.CreateReply()
			br.Txn = ba.Txn
			br.Txn.Status = roachpb.COMMITTED
			return br, nil
		})

		br, pErr := tc.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
	})
}
