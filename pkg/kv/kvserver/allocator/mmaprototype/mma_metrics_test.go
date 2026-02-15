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
	kind      shedKind
	result    shedResult
	numRanges int
}

// storeSetup describes the overloaded store state and the shedding actions to
// replay for one store during a pass.
type storeSetup struct {
	storeID                  roachpb.StoreID
	ignoreLevel              ignoreLevel
	actions                  []shedAction
	skipped                  bool
	withinLeaseSheddingGrace bool
}

// performAction executes the test actions defined in storeSetup on a given
// rebalancingPassMetricsAndLogger struct.
func (s *storeSetup) performActions(g *rebalancingPassMetricsAndLogger) {
	g.storeOverloaded(s.storeID, s.withinLeaseSheddingGrace, s.ignoreLevel)
	if s.skipped {
		g.skippedStore(s.storeID)
	} else {
		for _, action := range s.actions {
			for i := 0; i < action.numRanges; i++ {
				g.startEvaluatingRange()
			}
			switch action.kind {
			case shedLease:
				g.leaseShed(action.result)
			case shedReplica:
				g.replicaShed(action.result)
			}
		}
	}
	g.finishStore()
}

func TestRebalancingPassMetricsAndLogger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := log.ScopeWithoutShowLogs(t)
	defer s.Close(t)

	// Define the test scenario.
	testData := []storeSetup{
		{
			// Success: lease shed.
			storeID:     1,
			ignoreLevel: ignoreLoadNoChangeAndHigher,
			actions: []shedAction{
				{kind: shedLease, result: shedSuccess, numRanges: 1},
			},
		},
		{
			// Failure: multiple reasons.
			storeID:     8,
			ignoreLevel: ignoreLoadThresholdAndHigher,
			actions: []shedAction{
				{kind: shedReplica, result: noCandidate, numRanges: 1},
				{kind: shedReplica, result: noCandidateToAcceptLoad, numRanges: 1},
			},
		},
		{
			// Failure: single reason, more failures than s8.
			storeID:     6,
			ignoreLevel: ignoreHigherThanLoadThreshold,
			actions: []shedAction{
				{kind: shedLease, result: noHealthyCandidate, numRanges: 1},
				{kind: shedLease, result: noHealthyCandidate, numRanges: 1},
				{kind: shedLease, result: noHealthyCandidate, numRanges: 1},
				{kind: shedLease, result: noHealthyCandidate, numRanges: 1},
			},
		},
		{storeID: 12, skipped: true},
		{storeID: 5, skipped: true},
	}

	// Initialize the test and perform the actions.
	g := makeRebalancingPassMetricsAndLogger(1)
	for _, setup := range testData {
		setup.performActions(g)
	}

	// Compare the output of the logging pass.
	buf := redact.StringBuilder{}
	g.computePassSummary(&buf)
	echotest.Require(t, string(buf.RedactableString()), datapathutils.TestDataPath(t, t.Name()))
}
