// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package kv

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// txnMetrics is a txnInterceptor in charge of updating some metrics in response
// of transactions going through it.
type txnMetrics struct {
	wrapped lockedSender
	metrics *TxnMetrics
	clock   *hlc.Clock

	mu struct {
		sync.Locker // the TxnCoordSender's lock

		txn           *roachpb.Transaction
		txnStartNanos int64
		onePCCommit   bool

		closed bool
	}
}

// init initializes the txnMetrics. This method exists instead of a constructor
// because txnMetrics lives in a pool in the TxnCoordSender.
func (m *txnMetrics) init(
	mu sync.Locker, txn *roachpb.Transaction, clock *hlc.Clock, metrics *TxnMetrics,
) {
	m.clock = clock
	m.metrics = metrics
	m.mu.Locker = mu
	m.mu.txn = txn
}

// SendLocked is part of the txnInterceptor interface.
func (m *txnMetrics) SendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	_, hasBegin := ba.GetArg(roachpb.BeginTransaction)
	et, hasEnd := ba.GetArg(roachpb.EndTransaction)
	m.mu.onePCCommit = hasBegin && hasEnd && et.(*roachpb.EndTransactionRequest).Commit

	if hasBegin {
		m.mu.txnStartNanos = m.clock.PhysicalNow()
	}

	return m.wrapped.SendLocked(ctx, ba)
}

// setWrapped is part of the txnInterceptor interface.
func (m *txnMetrics) setWrapped(wrapped lockedSender) { m.wrapped = wrapped }

// populateMetaLocked is part of the txnInterceptor interface.
func (*txnMetrics) populateMetaLocked(*roachpb.TxnCoordMeta) {}

// augmentMetaLocked is part of the txnInterceptor interface.
func (*txnMetrics) augmentMetaLocked(roachpb.TxnCoordMeta) {}

// epochBumpedLocked is part of the txnInterceptor interface.
func (*txnMetrics) epochBumpedLocked() {}

// closeLocked is part of the txnInterceptor interface.
func (m *txnMetrics) closeLocked() {
	if m.mu.closed {
		return
	}
	m.mu.closed = true

	if m.mu.onePCCommit {
		m.metrics.Commits1PC.Inc(1)
	}

	duration := m.clock.PhysicalNow() - m.mu.txnStartNanos
	restarts := int64(m.mu.txn.Epoch)
	status := m.mu.txn.Status

	m.metrics.Durations.RecordValue(duration)
	m.metrics.Restarts.RecordValue(restarts)
	switch status {
	case roachpb.ABORTED:
		m.metrics.Aborts.Inc(1)
	case roachpb.PENDING:
		// NOTE(andrei): Getting a PENDING status here is possible when this
		// interceptor is closed without a rollback ever succeeding.
		// We increment the Aborts metric nevertheless; not sure how these
		// transactions should be accounted.
		m.metrics.Aborts.Inc(1)
	case roachpb.COMMITTED:
		m.metrics.Commits.Inc(1)
	}
}
