// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeMockTxnMetricRecorder(
	txn *roachpb.Transaction,
) (txnMetricRecorder, *mockLockedSender, *timeutil.ManualTime) {
	mockSender := &mockLockedSender{}
	metrics := MakeTxnMetrics(metric.TestSampleInterval)
	timeSource := timeutil.NewManualTime(timeutil.Unix(0, 123))
	return txnMetricRecorder{
		wrapped:    mockSender,
		metrics:    &metrics,
		timeSource: timeSource,
		txn:        txn,
	}, mockSender, timeSource
}

func TestTxnMetricRecorder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	type metrics struct {
		aborts, commits, commits1PC, commitsReadOnly, parallelCommits, rollbacksFailed, duration, restarts int
	}
	check := func(t *testing.T, tm *txnMetricRecorder, m metrics) {
		t.Helper()
		assert.Equal(t, int64(m.aborts), tm.metrics.Aborts.Count(), "TxnMetrics.Aborts")
		assert.Equal(t, int64(m.commits), tm.metrics.Commits.Count(), "TxnMetrics.Commits")
		assert.Equal(t, int64(m.commits1PC), tm.metrics.Commits1PC.Count(), "TxnMetrics.Commits1PC")
		assert.Equal(t, int64(m.commitsReadOnly), tm.metrics.CommitsReadOnly.Count(), "TxnMetrics.CommitsReadOnly")
		assert.Equal(t, int64(m.parallelCommits), tm.metrics.ParallelCommits.Count(), "TxnMetrics.ParallelCommits")
		assert.Equal(t, int64(m.rollbacksFailed), tm.metrics.RollbacksFailed.Count(), "TxnMetrics.RollbacksFailed")
		// NOTE: histograms don't retain full precision, so we don't check the exact
		// value. We just check whether the value is non-zero.
		_, sum := tm.metrics.Durations.CumulativeSnapshot().Total()
		assert.Equal(t, m.duration != 0, sum != 0, "TxnMetrics.Durations")
		_, sum = tm.metrics.Restarts.CumulativeSnapshot().Total()
		assert.Equal(t, m.restarts != 0, sum != 0, "TxnMetrics.Restarts")
	}

	t.Run("no-op", func(t *testing.T) {
		txn := makeTxnProto()
		tm, _, _ := makeMockTxnMetricRecorder(&txn)
		tm.closeLocked()

		check(t, &tm, metrics{aborts: 1, rollbacksFailed: 1})
	})

	t.Run("commit (1pc)", func(t *testing.T) {
		txn := makeTxnProto()
		tm, mockSender, timeSource := makeMockTxnMetricRecorder(&txn)

		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn.Clone()}
		ba.Add(&kvpb.EndTxnRequest{Commit: true})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())

			// Simulate delay.
			timeSource.Advance(234)

			br := ba.CreateReply()
			br.Txn = ba.Txn
			br.Txn.Status = roachpb.COMMITTED
			br.Responses[0].GetEndTxn().OnePhaseCommit = true
			return br, nil
		})
		br, pErr := tm.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)

		// Acting as TxnCoordSender.
		txn.Update(br.Txn)
		tm.closeLocked()

		check(t, &tm, metrics{commits: 1, commits1PC: 1, duration: 234})
	})

	t.Run("commit (parallel)", func(t *testing.T) {
		txn := makeTxnProto()
		tm, mockSender, timeSource := makeMockTxnMetricRecorder(&txn)

		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn.Clone()}
		ba.Add(&kvpb.EndTxnRequest{Commit: true})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())

			// Simulate delay.
			timeSource.Advance(234)

			br := ba.CreateReply()
			br.Txn = ba.Txn
			br.Txn.Status = roachpb.COMMITTED
			br.Responses[0].GetEndTxn().StagingTimestamp = br.Txn.WriteTimestamp
			return br, nil
		})
		br, pErr := tm.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)

		// Acting as TxnCoordSender.
		txn.Update(br.Txn)
		tm.closeLocked()

		check(t, &tm, metrics{commits: 1, parallelCommits: 1, duration: 234})
	})

	t.Run("commit (read-only)", func(t *testing.T) {
		txn := makeTxnProto()
		tm, _, _ := makeMockTxnMetricRecorder(&txn)

		// Acting as the TxnCoordSender, entering finalizeNonLockingTxnLocked
		// and short-circuiting the commit before issuing the EndTxn request.
		txn.Status = roachpb.COMMITTED
		tm.setReadOnlyCommit()
		tm.closeLocked()
		check(t, &tm, metrics{commits: 1, commitsReadOnly: 1})
	})

	t.Run("abort", func(t *testing.T) {
		txn := makeTxnProto()
		tm, mockSender, timeSource := makeMockTxnMetricRecorder(&txn)

		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn.Clone()}
		ba.Add(&kvpb.EndTxnRequest{Commit: true})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())

			// Simulate delay.
			timeSource.Advance(234)

			br := ba.CreateReply()
			br.Txn = ba.Txn
			br.Txn.Status = roachpb.ABORTED
			return br, nil
		})
		br, pErr := tm.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)

		// Acting as TxnCoordSender.
		txn.Update(br.Txn)
		tm.closeLocked()

		check(t, &tm, metrics{aborts: 1, duration: 234})
	})

	t.Run("restart", func(t *testing.T) {
		txn := makeTxnProto()
		tm, mockSender, timeSource := makeMockTxnMetricRecorder(&txn)

		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn.Clone()}
		ba.Add(&kvpb.EndTxnRequest{Commit: true})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			require.Equal(t, enginepb.TxnEpoch(0), ba.Txn.Epoch)
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())

			// Simulate delay.
			timeSource.Advance(234)

			wtoErr := &kvpb.WriteTooOldError{ActualTimestamp: txn.WriteTimestamp.Add(0, 10)}
			return nil, kvpb.NewErrorWithTxn(wtoErr, ba.Txn)
		})
		br, pErr := tm.SendLocked(ctx, ba)
		require.Nil(t, br)
		require.NotNil(t, pErr)
		require.NotNil(t, pErr.GetTxn())

		// Acting as TxnCoordSender.
		txn.Update(pErr.GetTxn())
		txn.Restart(0, 0, hlc.Timestamp{})

		// Resend the batch at the new epoch.
		ba.Header = kvpb.Header{Txn: txn.Clone()}

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			require.Equal(t, enginepb.TxnEpoch(1), ba.Txn.Epoch)
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())

			// Simulate delay.
			timeSource.Advance(234)

			br = ba.CreateReply()
			br.Txn = ba.Txn
			br.Txn.Status = roachpb.COMMITTED
			return br, nil
		})
		br, pErr = tm.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)

		// Acting as TxnCoordSender.
		txn.Update(br.Txn)
		tm.closeLocked()

		check(t, &tm, metrics{commits: 1, duration: 468, restarts: 1})
	})
}
