// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

// shedAction drives a single shed result recording on the
// rebalancingPassMetricsAndLogger. numRanges controls how many times
// startEvaluatingRange is called.
type shedAction struct {
	kind   shedKind
	result shedResult
}

// storeActions describes the overloaded store state and the shedding actions to
// replay for one store during a pass.
//
// skippedByPending models the production case where the store is overloaded
// but rebalanceStores does not shed from it this pass because the store
// already has too much pending decrease (or pending increase >= epsilon).
// The store is attributed to the corresponding `<bucket>.skipped` gauge.
type storeActions struct {
	storeID                  roachpb.StoreID
	ignoreLevel              ignoreLevel
	actions                  []shedAction
	skipped                  bool
	withinLeaseSheddingGrace bool
	skippedByPending         bool
}

// performAction executes the test actions defined in storeActions on a given
// rebalancingPassMetricsAndLogger struct.
func (s *storeActions) performActions(g *rebalancingPassMetricsAndLogger) {
	if s.skipped {
		g.skippedStore(s.storeID)
		return
	}
	g.storeOverloaded(s.storeID, s.withinLeaseSheddingGrace, s.ignoreLevel)
	if s.skippedByPending {
		g.skippedByPending()
		g.finishStore()
		return
	}
	for _, action := range s.actions {
		switch action.kind {
		case shedLease:
			g.leaseShed(action.result)
		case shedReplica:
			g.replicaShed(action.result)
		}
	}
	g.finishStore()
}

func TestRebalancingPassMetricsAndLogger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := log.ScopeWithoutShowLogs(t)
	defer s.Close(t)

	g := makeRebalancingPassMetricsAndLogger(1)

	// Define the test scenarios.
	for _, testData := range []struct {
		name  string
		setup []storeActions
	}{
		{
			// An empty test where nothing happened.
			name:  "empty",
			setup: []storeActions{},
		},
		{
			// A number of successful transfers.
			name: "success",
			setup: []storeActions{
				{
					storeID:     10,
					ignoreLevel: ignoreLoadNoChangeAndHigher,
					actions: []shedAction{
						{kind: shedLease, result: shedSuccess},
					},
				},
				{
					storeID:     3,
					ignoreLevel: ignoreLoadThresholdAndHigher,
					actions: []shedAction{
						{kind: shedReplica, result: shedSuccess},
					},
				},
			},
		},
		{
			// More stores than the logging limit.
			name: "limit",
			setup: []storeActions{
				{storeID: 1, skipped: true},
				{storeID: 2, skipped: true},
				{storeID: 3, skipped: true},
				{storeID: 4, skipped: true},
				{storeID: 5, skipped: true},
				{storeID: 6, skipped: true},
				{storeID: 7, skipped: true},
				{storeID: 8, skipped: true},
				{storeID: 9, skipped: true},
				{storeID: 10, skipped: true},
				{storeID: 11, skipped: true},
				{storeID: 12, skipped: true},
				{storeID: 13, skipped: true},
				{storeID: 14, skipped: true},
				{storeID: 15, skipped: true},
				{storeID: 16, skipped: true},
				{storeID: 17, skipped: true},
				{storeID: 18, skipped: true},
				{storeID: 19, skipped: true},
				{storeID: 20, skipped: true},
				{storeID: 21, skipped: true},
			},
		},
		{
			// One store sheds successfully; another store is overloaded but
			// rebalanceStores deferred shedding because of pending
			// decrease/increase. Both stores must appear in the per-bucket
			// summary; the deferred one in the "skipped" outcome.
			name: "skipped_by_pending",
			setup: []storeActions{
				{
					storeID:     3,
					ignoreLevel: ignoreLoadNoChangeAndHigher,
					actions: []shedAction{
						{kind: shedLease, result: shedSuccess},
					},
				},
				{
					storeID:          7,
					ignoreLevel:      ignoreLoadNoChangeAndHigher,
					skippedByPending: true,
				},
			},
		},
		{
			// Mixed success and failures.
			name: "mixed",
			setup: []storeActions{
				{
					storeID:     1,
					ignoreLevel: ignoreLoadNoChangeAndHigher,
					actions: []shedAction{
						{kind: shedLease, result: shedSuccess},
					},
				},
				{
					storeID:     8,
					ignoreLevel: ignoreLoadThresholdAndHigher,
					actions: []shedAction{
						{kind: shedReplica, result: noCandidate},
						{kind: shedReplica, result: noCandidateToAcceptLoad},
						{kind: shedReplica, result: shedSuccess},
					},
				},
				{
					storeID:     6,
					ignoreLevel: ignoreHigherThanLoadThreshold,
					actions: []shedAction{
						{kind: shedLease, result: noHealthyCandidate},
						{kind: shedLease, result: noHealthyCandidate},
						{kind: shedLease, result: noHealthyCandidate},
						{kind: shedLease, result: noHealthyCandidate},
					},
				},
				{
					storeID:                  10,
					withinLeaseSheddingGrace: true,
				},
				{storeID: 12, skipped: true},
				{storeID: 5, skipped: true},
			},
		},
	} {
		t.Run(testData.name, func(inner *testing.T) {
			// Perform the actions for the setup.
			g.resetForRebalancingPass()
			for _, setup := range testData.setup {
				setup.performActions(g)
			}

			// Compare the output of the logging pass.
			buf := redact.StringBuilder{}
			g.computePassSummary(&buf)
			echotest.Require(inner, string(buf.RedactableString()),
				datapathutils.TestDataPath(inner, t.Name(), testData.name))
		})
	}
}

// TestRebalancingPassMetricsSkippedGauges verifies that stores deferred via
// the pending-work skip path land in the per-bucket "skipped" gauge, that
// per-bucket success/failure/skipped sum to the count of overloaded stores
// observed in the bucket, and that gauges reset across passes.
func TestRebalancingPassMetricsSkippedGauges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := log.ScopeWithoutShowLogs(t)
	defer s.Close(t)

	g := makeRebalancingPassMetricsAndLogger(1)

	// Pass 1: s2 sheds successfully (short bucket), s3 fails (medium),
	// s4 is skipped (medium), s5 is skipped (long).
	g.resetForRebalancingPass()
	(&storeActions{
		storeID:     2,
		ignoreLevel: ignoreLoadNoChangeAndHigher,
		actions:     []shedAction{{kind: shedLease, result: shedSuccess}},
	}).performActions(g)
	(&storeActions{
		storeID:     3,
		ignoreLevel: ignoreLoadThresholdAndHigher,
		actions:     []shedAction{{kind: shedReplica, result: noCandidate}},
	}).performActions(g)
	(&storeActions{
		storeID:          4,
		ignoreLevel:      ignoreLoadThresholdAndHigher,
		skippedByPending: true,
	}).performActions(g)
	(&storeActions{
		storeID:          5,
		ignoreLevel:      ignoreHigherThanLoadThreshold,
		skippedByPending: true,
	}).performActions(g)

	g.computePassSummary(&redact.StringBuilder{})

	// Per-bucket totals must add up to the number of overloaded stores
	// observed in that bucket.
	require.Equal(t, int64(1), g.m.OverloadedStoreShortDurSuccess.Value())
	require.Equal(t, int64(0), g.m.OverloadedStoreShortDurFailure.Value())
	require.Equal(t, int64(0), g.m.OverloadedStoreShortDurSkipped.Value())

	require.Equal(t, int64(0), g.m.OverloadedStoreMediumDurSuccess.Value())
	require.Equal(t, int64(1), g.m.OverloadedStoreMediumDurFailure.Value())
	require.Equal(t, int64(1), g.m.OverloadedStoreMediumDurSkipped.Value())

	require.Equal(t, int64(0), g.m.OverloadedStoreLongDurSuccess.Value())
	require.Equal(t, int64(0), g.m.OverloadedStoreLongDurFailure.Value())
	require.Equal(t, int64(1), g.m.OverloadedStoreLongDurSkipped.Value())

	// Pass 2: s4 is no longer skipped and now sheds successfully (medium).
	// All other stores drop out. Gauges must reflect only this pass.
	g.resetForRebalancingPass()
	(&storeActions{
		storeID:     4,
		ignoreLevel: ignoreLoadThresholdAndHigher,
		actions:     []shedAction{{kind: shedReplica, result: shedSuccess}},
	}).performActions(g)
	g.computePassSummary(&redact.StringBuilder{})

	require.Equal(t, int64(0), g.m.OverloadedStoreShortDurSuccess.Value())
	require.Equal(t, int64(0), g.m.OverloadedStoreShortDurFailure.Value())
	require.Equal(t, int64(0), g.m.OverloadedStoreShortDurSkipped.Value())

	require.Equal(t, int64(1), g.m.OverloadedStoreMediumDurSuccess.Value())
	require.Equal(t, int64(0), g.m.OverloadedStoreMediumDurFailure.Value())
	require.Equal(t, int64(0), g.m.OverloadedStoreMediumDurSkipped.Value())

	require.Equal(t, int64(0), g.m.OverloadedStoreLongDurSuccess.Value())
	require.Equal(t, int64(0), g.m.OverloadedStoreLongDurFailure.Value())
	require.Equal(t, int64(0), g.m.OverloadedStoreLongDurSkipped.Value())
}
