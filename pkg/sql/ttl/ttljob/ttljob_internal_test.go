// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttljob

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// NOTE: This test is for functions in ttljob.go. We already have
// ttljob_test.go, but that is part of the ttljob_test package. This test is
// specifically part of the ttljob package to access non-exported functions and
// structs. Hence, the name '_internal_' in the file to signify that it accesses
// internal functions.

func makeFakeSpans(n int) []roachpb.Span {
	spans := make([]roachpb.Span, n)
	for i := 0; i < n; i++ {
		start := roachpb.Key(fmt.Sprintf("k%03d", i))
		end := roachpb.Key(fmt.Sprintf("k%03d", i+1))
		spans[i] = roachpb.Span{Key: start, EndKey: end}
	}
	return spans
}

func TestTTLProgressLifecycle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	infra := &physicalplan.PhysicalInfrastructure{
		Processors: []physicalplan.Processor{
			{
				SQLInstanceID: base.SQLInstanceID(11),
				Spec: execinfrapb.ProcessorSpec{
					ProcessorID: 1,
					Core: execinfrapb.ProcessorCoreUnion{
						Ttl: &execinfrapb.TTLSpec{
							Spans: makeFakeSpans(100),
						},
					},
				},
			},
			{
				SQLInstanceID: base.SQLInstanceID(12),
				Spec: execinfrapb.ProcessorSpec{
					ProcessorID: 2,
					Core: execinfrapb.ProcessorCoreUnion{
						Ttl: &execinfrapb.TTLSpec{
							Spans: makeFakeSpans(100),
						},
					},
				},
			},
		},
	}
	physPlan := physicalplan.PhysicalPlan{
		PhysicalInfrastructure: infra,
	}
	sqlPlan := sql.PhysicalPlan{
		PhysicalPlan: physPlan,
	}
	resumer := rowLevelTTLResumer{
		physicalPlan: &sqlPlan,
	}

	// Create two processors to match the IDs used in the physical plan
	proc1 := mockProcessor(1, roachpb.NodeID(11), 100)
	proc2 := mockProcessor(2, roachpb.NodeID(12), 100)
	mockRowReceiver := metadataCache{}

	// Step 1: initProgress
	progress, err := resumer.initProgress(200)
	require.NoError(t, err)
	require.NotNil(t, progress)
	require.Equal(t, float32(0), progress.GetFractionCompleted())
	ttlProgress := progress.GetRowLevelTTL()
	require.Equal(t, int64(200), ttlProgress.JobTotalSpanCount)
	require.Zero(t, ttlProgress.JobProcessedSpanCount)
	require.Zero(t, ttlProgress.JobDeletedRowCount)
	require.Len(t, ttlProgress.ProcessorProgresses, 0)
	md := jobs.JobMetadata{
		Progress: progress,
	}

	// First refresh (processor 1 partial)
	proc1.progressUpdater.OnSpanProcessed(50, 100)
	err = proc1.progressUpdater.UpdateProgress(ctx, &mockRowReceiver)
	require.NoError(t, err)
	progress, err = resumer.refreshProgress(ctx, &md, mockRowReceiver.GetLatest())
	require.NoError(t, err)
	require.NotNil(t, progress)
	md.Progress = progress

	require.NoError(t, err)
	require.InEpsilon(t, 0.25, progress.GetFractionCompleted(), 0.001)
	ttlProgress = progress.GetRowLevelTTL()
	require.Equal(t, int64(200), ttlProgress.JobTotalSpanCount)
	require.Equal(t, int64(50), ttlProgress.JobProcessedSpanCount)
	require.Equal(t, int64(100), ttlProgress.JobDeletedRowCount)
	require.Len(t, ttlProgress.ProcessorProgresses, 1)

	// Second refresh (processor 2 full)
	proc2.progressUpdater.OnSpanProcessed(100, 400)
	err = proc2.progressUpdater.UpdateProgress(ctx, &mockRowReceiver)
	require.NoError(t, err)
	progress, err = resumer.refreshProgress(ctx, &md, mockRowReceiver.GetLatest())
	require.NoError(t, err)
	require.NotNil(t, progress)
	md.Progress = progress

	require.NoError(t, err)
	require.InEpsilon(t, 0.75, progress.GetFractionCompleted(), 0.001)
	ttlProgress = progress.GetRowLevelTTL()
	require.Equal(t, int64(200), ttlProgress.JobTotalSpanCount)
	require.Equal(t, int64(150), ttlProgress.JobProcessedSpanCount)
	require.Equal(t, int64(500), ttlProgress.JobDeletedRowCount)
	require.Len(t, ttlProgress.ProcessorProgresses, 2)

	// No update refresh (processor 1 empty)
	err = proc1.progressUpdater.UpdateProgress(ctx, &mockRowReceiver) // No call to OnSpanProcessed
	require.NoError(t, err)
	progress, err = resumer.refreshProgress(ctx, &md, mockRowReceiver.GetLatest())
	require.NoError(t, err)
	require.Nil(t, progress)

	// Final refresh (processor 1 remaining)
	proc1.progressUpdater.OnSpanProcessed(50, 500)
	err = proc1.progressUpdater.UpdateProgress(ctx, &mockRowReceiver)
	require.NoError(t, err)
	progress, err = resumer.refreshProgress(ctx, &md, mockRowReceiver.GetLatest())
	require.NoError(t, err)
	require.NotNil(t, progress)

	require.NoError(t, err)
	require.InEpsilon(t, 1, progress.GetFractionCompleted(), 0.001)
	ttlProgress = progress.GetRowLevelTTL()
	require.Equal(t, int64(200), ttlProgress.JobTotalSpanCount)
	require.Equal(t, int64(200), ttlProgress.JobProcessedSpanCount)
	require.Equal(t, int64(1000), ttlProgress.JobDeletedRowCount)
	require.Len(t, ttlProgress.ProcessorProgresses, 2)
}

func TestReplanDecider(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		desc         string
		beforeNodes  []base.SQLInstanceID
		afterNodes   []base.SQLInstanceID
		threshold    float64
		expectReplan bool
	}{
		{
			desc:         "nodes don't change",
			beforeNodes:  []base.SQLInstanceID{1, 2, 3},
			afterNodes:   []base.SQLInstanceID{1, 2, 3},
			threshold:    0.1,
			expectReplan: false,
		},
		{
			desc:         "one node is shutdown",
			beforeNodes:  []base.SQLInstanceID{1, 2, 3},
			afterNodes:   []base.SQLInstanceID{1, 3},
			threshold:    0.1,
			expectReplan: true,
		},
		{
			desc:         "one node is brought online",
			beforeNodes:  []base.SQLInstanceID{1, 2, 3},
			afterNodes:   []base.SQLInstanceID{1, 2, 3, 4},
			threshold:    0.1,
			expectReplan: false,
		},
		{
			desc:         "one node is replaced",
			beforeNodes:  []base.SQLInstanceID{1, 2, 3},
			afterNodes:   []base.SQLInstanceID{1, 2, 4},
			threshold:    0.1,
			expectReplan: true,
		},
		{
			desc:         "multiple nodes shutdown",
			beforeNodes:  []base.SQLInstanceID{1, 2, 3, 4, 5},
			afterNodes:   []base.SQLInstanceID{1, 3},
			threshold:    0.1,
			expectReplan: true,
		},
		{
			desc:         "all nodes replaced",
			beforeNodes:  []base.SQLInstanceID{1, 2, 3},
			afterNodes:   []base.SQLInstanceID{4, 5, 6},
			threshold:    0.1,
			expectReplan: true,
		},
		{
			desc:         "threshold boundary: exactly at threshold",
			beforeNodes:  []base.SQLInstanceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			afterNodes:   []base.SQLInstanceID{1, 2, 3, 4, 5, 6, 7, 8, 9},
			threshold:    0.1,
			expectReplan: false,
		},
		{
			desc:         "threshold boundary: just above threshold",
			beforeNodes:  []base.SQLInstanceID{1, 2, 3, 4, 5, 6, 7, 8, 9},
			afterNodes:   []base.SQLInstanceID{1, 2, 3, 4, 5, 6, 7, 8},
			threshold:    0.1,
			expectReplan: true,
		},
		{
			desc:         "threshold disabled",
			beforeNodes:  []base.SQLInstanceID{1, 2, 3},
			afterNodes:   []base.SQLInstanceID{1, 2},
			threshold:    0.0,
			expectReplan: false,
		},
		{
			desc:         "large scale: many nodes lost",
			beforeNodes:  []base.SQLInstanceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			afterNodes:   []base.SQLInstanceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			threshold:    0.1,
			expectReplan: true,
		},
		{
			desc:         "mixed scenario: nodes added and removed",
			beforeNodes:  []base.SQLInstanceID{1, 2, 3, 4, 5},
			afterNodes:   []base.SQLInstanceID{1, 3, 5, 6, 7, 8},
			threshold:    0.1,
			expectReplan: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			// Create atomic counter and set stability window to 1 for immediate replan (current behavior)
			consecutiveReplanDecisions := &atomic.Int64{}
			decider := replanDecider(consecutiveReplanDecisions, func() int64 { return 1 }, func() float64 { return testCase.threshold })
			ctx := context.Background()
			oldPlan := &sql.PhysicalPlan{}
			oldPlan.PhysicalInfrastructure = &physicalplan.PhysicalInfrastructure{Processors: nil}
			for _, nodeID := range testCase.beforeNodes {
				oldPlan.Processors = append(oldPlan.Processors, physicalplan.Processor{SQLInstanceID: nodeID})
			}
			newPlan := &sql.PhysicalPlan{}
			newPlan.PhysicalInfrastructure = &physicalplan.PhysicalInfrastructure{Processors: nil}
			for _, nodeID := range testCase.afterNodes {
				newPlan.Processors = append(newPlan.Processors, physicalplan.Processor{SQLInstanceID: nodeID})
			}
			replan := decider(ctx, oldPlan, newPlan)
			require.Equal(t, testCase.expectReplan, replan)
		})
	}
}

func TestReplanDeciderStabilityWindow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		desc            string
		stabilityWindow int64
		threshold       float64
		planChanges     [][]base.SQLInstanceID // sequence of plan changes
		expectedReplans []bool                 // expected replan decision for each change
	}{
		{
			desc:            "stability window 1 - immediate replan",
			stabilityWindow: 1,
			threshold:       0.1,
			planChanges:     [][]base.SQLInstanceID{{2, 3}, {2, 4}, {3, 4}},
			expectedReplans: []bool{true, true, true},
		},
		{
			desc:            "stability window 2 - requires consecutive decisions",
			stabilityWindow: 2,
			threshold:       0.1,
			planChanges:     [][]base.SQLInstanceID{{2, 3}, {2, 4}, {1, 2, 3}},
			expectedReplans: []bool{false, true, false}, // first false, second true (meets window), third false (reset)
		},
		{
			desc:            "stability window 2 - interrupted sequence",
			stabilityWindow: 2,
			threshold:       0.1,
			planChanges:     [][]base.SQLInstanceID{{2, 3}, {1, 2, 3}, {2, 4}, {3, 4}},
			expectedReplans: []bool{false, false, false, true}, // interrupted, then consecutive
		},
		{
			desc:            "stability window 3 - three consecutive needed",
			stabilityWindow: 3,
			threshold:       0.1,
			planChanges:     [][]base.SQLInstanceID{{2, 3}, {2, 4}, {3, 4}, {1, 2, 3}},
			expectedReplans: []bool{false, false, true, false}, // third one triggers replan
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			consecutiveReplanDecisions := &atomic.Int64{}
			decider := replanDecider(
				consecutiveReplanDecisions,
				func() int64 { return testCase.stabilityWindow },
				func() float64 { return testCase.threshold },
			)
			ctx := context.Background()

			// Use initial plan with nodes 1,2,3
			initialPlan := &sql.PhysicalPlan{}
			initialPlan.PhysicalInfrastructure = &physicalplan.PhysicalInfrastructure{Processors: nil}
			for _, nodeID := range []base.SQLInstanceID{1, 2, 3} {
				initialPlan.Processors = append(initialPlan.Processors, physicalplan.Processor{SQLInstanceID: nodeID})
			}

			for i, nodes := range testCase.planChanges {
				newPlan := &sql.PhysicalPlan{}
				newPlan.PhysicalInfrastructure = &physicalplan.PhysicalInfrastructure{Processors: nil}
				for _, nodeID := range nodes {
					newPlan.Processors = append(newPlan.Processors, physicalplan.Processor{SQLInstanceID: nodeID})
				}

				replan := decider(ctx, initialPlan, newPlan)
				if replan != testCase.expectedReplans[i] {
					t.Errorf("step %d: expected replan=%v, got %v (consecutive count: %d)", i, testCase.expectedReplans[i], replan, consecutiveReplanDecisions.Load())
				}

				// Update initial plan for next iteration to maintain state
				initialPlan = newPlan
			}
		})
	}
}
