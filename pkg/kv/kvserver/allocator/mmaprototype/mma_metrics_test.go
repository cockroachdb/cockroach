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
type storeActions struct {
	storeID                  roachpb.StoreID
	ignoreLevel              ignoreLevel
	actions                  []shedAction
	skipped                  bool
	withinLeaseSheddingGrace bool
}

// performAction executes the test actions defined in storeActions on a given
// rebalancingPassMetricsAndLogger struct.
func (s *storeActions) performActions(g *rebalancingPassMetricsAndLogger) {
	if s.skipped {
		g.skippedStore(s.storeID)
		return
	}
	g.storeOverloaded(s.storeID, s.withinLeaseSheddingGrace, s.ignoreLevel)
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
