// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// asSortedSlice returns the set data in sorted order.
//
// Too inefficient for production.
func (s *condensableSpanSet) asSortedSlice() []roachpb.Span {
	set := s.asSlice()
	cpy := make(roachpb.Spans, len(set))
	copy(cpy, set)
	sort.Sort(cpy)
	return cpy
}

// TestingSenderConcurrencyLimit exports the cluster setting for testing
// purposes.
var TestingSenderConcurrencyLimit = senderConcurrencyLimit

// TestingGetLockFootprint returns the internal lock footprint for testing
// purposes.
func (tc *TxnCoordSender) TestingGetLockFootprint(mergeAndSort bool) []roachpb.Span {
	if mergeAndSort {
		tc.interceptorAlloc.txnPipeliner.lockFootprint.mergeAndSort()
	}
	return tc.interceptorAlloc.txnPipeliner.lockFootprint.asSlice()
}

// TestingGetRefreshFootprint returns the internal refresh footprint for testing
// purposes.
func (tc *TxnCoordSender) TestingGetRefreshFootprint() []roachpb.Span {
	return tc.interceptorAlloc.txnSpanRefresher.refreshFootprint.asSlice()
}

// TestingSetLinearizable allows tests to enable linearizable behavior.
func (tcf *TxnCoordSenderFactory) TestingSetLinearizable(linearizable bool) {
	tcf.linearizable = linearizable
}

// TestingSetMetrics allows tests to override the factory's metrics struct.
func (tcf *TxnCoordSenderFactory) TestingSetMetrics(metrics TxnMetrics) {
	tcf.metrics = metrics
}

// TestingSetCommitWaitFilter allows tests to instrument the beginning of a
// transaction commit wait sleep.
func (tcf *TxnCoordSenderFactory) TestingSetCommitWaitFilter(filter func()) {
	tcf.testingKnobs.CommitWaitFilter = filter
}
