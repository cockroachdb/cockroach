// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// txnMetricRecorder is a txnInterceptor in charge of updating metrics about
// the behavior and outcome of a transaction. It records information about the
// requests that a transaction sends and updates counters and histograms when
// the transaction completes.
type txnMetricRecorder struct {
	wrapped    lockedSender
	metrics    *TxnMetrics
	timeSource timeutil.TimeSource

	txn            *roachpb.Transaction
	txnStart       time.Time
	onePCCommit    bool
	parallelCommit bool
	readOnlyCommit bool
}

// SendLocked is part of the txnInterceptor interface.
func (m *txnMetricRecorder) SendLocked(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	if m.txnStart.IsZero() {
		m.txnStart = m.timeSource.Now()
	}

	br, pErr := m.wrapped.SendLocked(ctx, ba)
	if pErr != nil {
		return br, pErr
	}

	if length := len(br.Responses); length > 0 {
		if et := br.Responses[length-1].GetEndTxn(); et != nil {
			// Check for 1-phase commit.
			m.onePCCommit = et.OnePhaseCommit

			// Check for parallel commit.
			m.parallelCommit = !et.StagingTimestamp.IsEmpty()
		}
	}
	return br, nil
}

// setWrapped is part of the txnInterceptor interface.
func (m *txnMetricRecorder) setWrapped(wrapped lockedSender) { m.wrapped = wrapped }

// populateLeafInputState is part of the txnInterceptor interface.
func (*txnMetricRecorder) populateLeafInputState(*roachpb.LeafTxnInputState) {}

// populateLeafFinalState is part of the txnInterceptor interface.
func (*txnMetricRecorder) populateLeafFinalState(*roachpb.LeafTxnFinalState) {}

// importLeafFinalState is part of the txnInterceptor interface.
func (*txnMetricRecorder) importLeafFinalState(context.Context, *roachpb.LeafTxnFinalState) error {
	return nil
}

// epochBumpedLocked is part of the txnInterceptor interface.
func (*txnMetricRecorder) epochBumpedLocked() {}

// createSavepointLocked is part of the txnInterceptor interface.
func (*txnMetricRecorder) createSavepointLocked(context.Context, *savepoint) {}

// rollbackToSavepointLocked is part of the txnInterceptor interface.
func (*txnMetricRecorder) rollbackToSavepointLocked(context.Context, savepoint) {}

// setReadOnlyCommit records the transaction commit as a read-only commit. Such
// commits do not send EndTxn requests because they have no writes to commit or
// locks to release.
func (m *txnMetricRecorder) setReadOnlyCommit() { m.readOnlyCommit = true }

// closeLocked is part of the txnInterceptor interface.
func (m *txnMetricRecorder) closeLocked() {
	if m.onePCCommit {
		m.metrics.Commits1PC.Inc(1)
	}
	if m.parallelCommit {
		m.metrics.ParallelCommits.Inc(1)
	}
	if m.readOnlyCommit {
		m.metrics.CommitsReadOnly.Inc(1)
	}

	if !m.txnStart.IsZero() {
		dur := m.timeSource.Since(m.txnStart)
		if dur >= 0 {
			m.metrics.Durations.RecordValue(dur.Nanoseconds())
		}
	}
	restarts := int64(m.txn.Epoch)
	status := m.txn.Status

	// TODO(andrei): We only record txn that had any restarts, otherwise the
	// serialization induced by the histogram shows on profiles. Figure something
	// out to make it cheaper - maybe augment the histogram library with an
	// "expected value" that is a cheap counter for the common case. See #30644.
	// Also, the epoch is not currently an accurate count since we sometimes bump
	// it artificially (in the parallel execution queue).
	if restarts > 0 {
		m.metrics.Restarts.RecordValue(restarts)
	}
	switch status {
	case roachpb.ABORTED:
		m.metrics.Aborts.Inc(1)
	case roachpb.PENDING:
		// NOTE(andrei): Getting a PENDING status here is possible when this
		// interceptor is closed without a rollback ever succeeding.
		// We increment the Aborts metric nevertheless; not sure how these
		// transactions should be accounted.
		m.metrics.Aborts.Inc(1)
		// Record failed aborts separately as in this case EndTxn never succeeded
		// which means intents are left for subsequent cleanup by reader.
		m.metrics.RollbacksFailed.Inc(1)
	case roachpb.COMMITTED:
		// Note that successful read-only txn are also counted as committed, even
		// though they never had a txn record.
		m.metrics.Commits.Inc(1)
	case roachpb.PREPARED:
		m.metrics.Prepares.Inc(1)
	}
}
