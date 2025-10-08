// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobfrontier"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

// setupProgressTestInfra creates common test infrastructure for progress tracking tests.
func setupProgressTestInfra(
	t *testing.T,
) (context.Context, serverutils.TestServerInterface, *jobs.Registry, *jobs.Job, func()) {
	t.Helper()

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})

	registry := s.JobRegistry().(*jobs.Registry)

	// Create a job using INSPECT details
	record := jobs.Record{
		Details:  jobspb.InspectDetails{},
		Progress: jobspb.InspectProgress{},
		Username: username.TestUserName(),
	}

	job, err := registry.CreateJobWithTxn(ctx, record, registry.MakeJobID(), nil /* txn */)
	if err != nil {
		s.Stopper().Stop(ctx)   // Clean up server if job creation fails
		require.NoError(t, err) // This will fail the test
	}

	cleanup := func() {
		s.Stopper().Stop(ctx)
	}

	return ctx, s, registry, job, cleanup
}

func TestInspectProgressTracker_CheckCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, s, _, job, cleanup := setupProgressTestInfra(t)
	defer cleanup()

	testCases := []struct {
		name                      string
		totalCheckCount           int64
		progressUpdates           []int64 // checks completed in each update
		expectedFractionCompleted float32
		expectedCompletedChecks   int64
	}{
		{
			name:                      "initialize with zero checks",
			totalCheckCount:           0,
			progressUpdates:           []int64{},
			expectedFractionCompleted: 0.0,
			expectedCompletedChecks:   0,
		},
		{
			name:                      "initialize with positive check count, no updates",
			totalCheckCount:           100,
			progressUpdates:           []int64{},
			expectedFractionCompleted: 0.0,
			expectedCompletedChecks:   0,
		},
		{
			name:                      "partial progress updates",
			totalCheckCount:           100,
			progressUpdates:           []int64{10, 15, 25},
			expectedFractionCompleted: 0.5,
			expectedCompletedChecks:   50,
		},
		{
			name:                      "complete progress",
			totalCheckCount:           50,
			progressUpdates:           []int64{25, 25},
			expectedFractionCompleted: 1.0,
			expectedCompletedChecks:   50,
		},
		{
			name:                      "single large update",
			totalCheckCount:           200,
			progressUpdates:           []int64{75},
			expectedFractionCompleted: 0.375,
			expectedCompletedChecks:   75,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create progress tracker
			tracker := newInspectProgressTracker(job, &s.ClusterSettings().SV, s.InternalDB().(isql.DB))
			defer tracker.terminateTracker()

			// Initialize job progress
			err := tracker.initJobProgress(ctx, tc.totalCheckCount, 0 /* completedCheckCount */)
			require.NoError(t, err)

			// Verify initial internal state
			totalChecks, completedChecks := tracker.getCachedCheckCounts()
			require.Equal(t, tc.totalCheckCount, totalChecks)
			require.Equal(t, int64(0), completedChecks)

			// Simulate processor progress updates
			for i, checksCompleted := range tc.progressUpdates {
				meta, err := createProcessorProgressUpdate(
					checksCompleted,
					i == len(tc.progressUpdates)-1, // Last update is finished
					nil,                            // No completed spans for this test
				)
				require.NoError(t, err)

				// Handle the processor progress update
				err = tracker.handleProgressUpdate(ctx, meta)
				require.NoError(t, err)
			}

			// Verify final internal state
			totalChecks, completedChecks = tracker.getCachedCheckCounts()
			require.Equal(t, tc.totalCheckCount, totalChecks)
			require.Equal(t, tc.expectedCompletedChecks, completedChecks)

			// Verify job progress was updated correctly
			progress := job.Progress()
			fractionCompleted, ok := progress.Progress.(*jobspb.Progress_FractionCompleted)
			require.True(t, ok, "progress should be FractionCompleted type")
			if tc.expectedFractionCompleted == 0.0 {
				require.Equal(t, tc.expectedFractionCompleted, fractionCompleted.FractionCompleted)
			} else {
				require.InEpsilon(t, tc.expectedFractionCompleted, fractionCompleted.FractionCompleted, 0.001)
			}

			// Check INSPECT progress details
			inspectProgress, ok := progress.Details.(*jobspb.Progress_Inspect)
			require.True(t, ok, "progress details should be Inspect type")
			require.Equal(t, tc.totalCheckCount, inspectProgress.Inspect.JobTotalCheckCount)
			require.Equal(t, tc.expectedCompletedChecks, inspectProgress.Inspect.JobCompletedCheckCount)
		})
	}
}

// progressUpdate represents a processor progress update for testing.
type progressUpdate struct {
	checksCompleted int64
	finished        bool
	completedSpans  []roachpb.Span
}

func TestInspectProgressTracker_SpanCheckpointing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, s, registry, _, cleanup := setupProgressTestInfra(t)
	defer cleanup()

	testCases := []struct {
		name            string
		progressUpdates []progressUpdate
		expectStored    bool
		expectFullCover bool // whether spans should cover the full table span
		expectSpanCount int  // exact number of spans we expect stored
	}{
		{
			name: "no completed spans",
			progressUpdates: []progressUpdate{
				{0, false, nil},
			},
			expectStored:    false,
			expectFullCover: false,
			expectSpanCount: 0,
		},
		{
			name: "single span completion",
			progressUpdates: []progressUpdate{
				{5, true, []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")}}},
			},
			expectStored:    true,
			expectFullCover: false,
			expectSpanCount: 1,
		},
		{
			name: "adjacent spans that merge",
			progressUpdates: []progressUpdate{
				{5, false, []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")}}},
				{5, true, []roachpb.Span{{Key: roachpb.Key("m"), EndKey: roachpb.Key("z")}}},
			},
			expectStored:    true,
			expectFullCover: true,
			expectSpanCount: 1, // Should merge into 1 span
		},
		{
			name: "non-adjacent spans",
			progressUpdates: []progressUpdate{
				{3, false, []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}}},
				{3, true, []roachpb.Span{{Key: roachpb.Key("x"), EndKey: roachpb.Key("z")}}},
			},
			expectStored:    true,
			expectFullCover: false,
			expectSpanCount: 2, // Should remain separate
		},
		{
			name: "overlapping spans",
			progressUpdates: []progressUpdate{
				{4, false, []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("n")}}},
				{4, true, []roachpb.Span{{Key: roachpb.Key("j"), EndKey: roachpb.Key("z")}}},
			},
			expectStored:    true,
			expectFullCover: true,
			expectSpanCount: 1, // Should merge due to overlap
		},
		{
			name: "duplicate spans",
			progressUpdates: []progressUpdate{
				{3, false, []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")}}},
				{3, true, []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")}}},
			},
			expectStored:    true,
			expectFullCover: false,
			expectSpanCount: 1, // Should deduplicate
		},
		{
			name: "many small adjacent spans",
			progressUpdates: []progressUpdate{
				{1, false, []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}}},
				{1, false, []roachpb.Span{{Key: roachpb.Key("c"), EndKey: roachpb.Key("e")}}},
				{1, false, []roachpb.Span{{Key: roachpb.Key("e"), EndKey: roachpb.Key("g")}}},
				{1, true, []roachpb.Span{{Key: roachpb.Key("g"), EndKey: roachpb.Key("z")}}},
			},
			expectStored:    true,
			expectFullCover: true,
			expectSpanCount: 1, // Should all merge into one span
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			record := jobs.Record{
				Details:  jobspb.InspectDetails{},
				Progress: jobspb.InspectProgress{},
				Username: username.TestUserName(),
			}

			job, err := registry.CreateJobWithTxn(ctx, record, registry.MakeJobID(), nil /* txn */)
			require.NoError(t, err)

			// Phase 1: Store spans and verify storage.
			tracker1 := newInspectProgressTracker(job, &s.ClusterSettings().SV, s.InternalDB().(isql.DB))
			defer tracker1.terminateTracker()

			// Initialize job progress.
			err = tracker1.initJobProgress(ctx, 100 /* totalCheckCount */, 0 /* completedCheckCount */)
			require.NoError(t, err)

			// Send progress updates.
			for _, update := range tc.progressUpdates {
				meta, err := createProcessorProgressUpdate(
					update.checksCompleted,
					update.finished,
					update.completedSpans,
				)
				require.NoError(t, err)

				err = tracker1.handleProgressUpdate(ctx, meta)
				require.NoError(t, err)
			}

			// Verify completed spans are stored (or not) as expected.
			storedCompletedSpans := verifyStoredSpans(t, ctx, s.InternalDB().(isql.DB), job.ID(), tc.expectStored, tc.expectSpanCount)

			if tc.expectStored && tc.expectFullCover {
				// Check if completed spans cover the entire table span [a, z).
				tableSpan := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}
				var spanGroup roachpb.SpanGroup
				spanGroup.Add(storedCompletedSpans...)
				mergedSpans := spanGroup.Slice()
				require.Greater(t, len(mergedSpans), 0)

				// Find the overall span covered by completed spans.
				overallStart := mergedSpans[0].Key
				overallEnd := mergedSpans[len(mergedSpans)-1].EndKey
				actualCoverage := roachpb.Span{Key: overallStart, EndKey: overallEnd}

				require.True(t, actualCoverage.Contains(tableSpan),
					"completed spans should cover entire table span. Expected: %s, Actual coverage: %s", tableSpan, actualCoverage)
			}

			// Phase 2: Test restart behavior.
			// Terminate the first tracker to simulate job interruption.
			tracker1.terminateTracker()

			// Create new tracker and verify it loads completed spans.
			tracker2 := newInspectProgressTracker(job, &s.ClusterSettings().SV, s.InternalDB().(isql.DB))
			defer tracker2.terminateTracker()

			// This simulates what would happen during job restart.
			loadedSpans, err := tracker2.initTracker(ctx)
			require.NoError(t, err)

			if !tc.expectStored {
				// For cases with no stored spans, restart should load empty span set.
				require.Equal(t, 0, len(loadedSpans), "should have loaded no spans when none were stored")
				return
			}

			require.Greater(t, len(loadedSpans), 0, "should have loaded completed spans on restart")

			// Verify the loaded spans match what we stored.
			var loadedSpanGroup roachpb.SpanGroup
			loadedSpanGroup.Add(loadedSpans...)
			loadedSpanSlice := loadedSpanGroup.Slice()

			var storedSpanGroup roachpb.SpanGroup
			storedSpanGroup.Add(storedCompletedSpans...)
			storedSpanSlice := storedSpanGroup.Slice()

			require.Equal(t, len(storedSpanSlice), len(loadedSpanSlice),
				"loaded spans should have same coverage as stored spans")

			for i, storedSpan := range storedSpanSlice {
				require.True(t, loadedSpanSlice[i].Equal(storedSpan),
					"loaded span %d should match stored span: loaded=%s, stored=%s",
					i, loadedSpanSlice[i], storedSpan)
			}

			// Test that these loaded spans would actually filter work.
			testSpans := []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("d")}, // May overlap with completed spans
				{Key: roachpb.Key("x"), EndKey: roachpb.Key("z")}, // May not overlap
			}

			for _, testSpan := range testSpans {
				isCompleted := false
				for _, completedSpan := range loadedSpans {
					if completedSpan.Contains(testSpan) {
						isCompleted = true
						break
					}
				}

				if tc.expectFullCover {
					require.True(t, isCompleted, "span %s should be contained in completed spans for full coverage case", testSpan)
				}
			}
		})
	}
}

// verifyStoredSpans verifies that completed spans are stored correctly in jobfrontier
// and returns the stored spans for further verification.
func verifyStoredSpans(
	t *testing.T,
	ctx context.Context,
	db isql.DB,
	jobID jobspb.JobID,
	expectStored bool,
	expectSpanCount int,
) []roachpb.Span {
	var storedCompletedSpans []roachpb.Span
	err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		resolvedSpans, found, err := jobfrontier.GetResolvedSpans(ctx, txn, jobID, inspectCompletedSpansKey)
		if err != nil {
			return err
		}
		if !found {
			if expectStored {
				return errors.New("expected to find inspect spans key")
			}
			return nil // No spans expected
		}

		// Extract just the spans (we don't care about timestamps for INSPECT).
		storedCompletedSpans = make([]roachpb.Span, len(resolvedSpans))
		for i, rs := range resolvedSpans {
			storedCompletedSpans[i] = rs.Span
		}
		return nil
	})
	require.NoError(t, err)

	if expectStored {
		require.Equal(t, expectSpanCount, len(storedCompletedSpans),
			"expected exactly %d stored spans, got %d", expectSpanCount, len(storedCompletedSpans))
	} else {
		require.Equal(t, 0, len(storedCompletedSpans), "expected no stored spans")
	}

	return storedCompletedSpans
}

// createProcessorProgressUpdate creates a processor progress update message.
func createProcessorProgressUpdate(
	checksCompleted int64, finished bool, completedSpans []roachpb.Span,
) (*execinfrapb.ProducerMetadata, error) {
	progressMsg := &jobspb.InspectProcessorProgress{
		ChecksCompleted: checksCompleted,
		Finished:        finished,
	}

	progressAny, err := pbtypes.MarshalAny(progressMsg)
	if err != nil {
		return nil, err
	}

	const testNodeID = 1
	const testProcessorID = 1
	meta := &execinfrapb.ProducerMetadata{
		BulkProcessorProgress: &execinfrapb.RemoteProducerMetadata_BulkProcessorProgress{
			CompletedSpans:  completedSpans,
			ProgressDetails: *progressAny,
			NodeID:          testNodeID,
			FlowID:          execinfrapb.FlowID{},
			ProcessorID:     testProcessorID,
			Drained:         finished,
		},
	}

	return meta, nil
}

func TestInspectProgressTracker_ProgressFlushConditions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, s, _, _, cleanup := setupProgressTestInfra(t)
	defer cleanup()

	const totalChecks = 1000

	testCases := []struct {
		name                      string
		setupFunc                 func(t *testing.T, tracker *inspectProgressTracker, job *jobs.Job)
		expectedFraction          float32
		expectUncheckpointedSpans bool
	}{
		{
			name: "initial state - no progress updates",
			setupFunc: func(t *testing.T, tracker *inspectProgressTracker, job *jobs.Job) {
				require.NoError(t, tracker.initJobProgress(ctx, totalChecks, 0))
			},
			expectedFraction:          0.0,
			expectUncheckpointedSpans: false,
		},
		{
			name: "check count updates without spans",
			setupFunc: func(t *testing.T, tracker *inspectProgressTracker, job *jobs.Job) {
				require.NoError(t, tracker.initJobProgress(ctx, totalChecks, 0))

				// Send progress updates with check counts but no spans.
				meta, err := createProcessorProgressUpdate(100, false, nil)
				require.NoError(t, err)
				_, err = tracker.updateProgressCache(meta)
				require.NoError(t, err)
			},
			expectedFraction:          0.1,
			expectUncheckpointedSpans: false,
		},
		{
			name: "span updates trigger checkpoint need",
			setupFunc: func(t *testing.T, tracker *inspectProgressTracker, job *jobs.Job) {
				require.NoError(t, tracker.initJobProgress(ctx, totalChecks, 0))

				// Send progress with completed spans.
				spans := []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")}}
				meta, err := createProcessorProgressUpdate(50, false, spans)
				require.NoError(t, err)
				_, err = tracker.updateProgressCache(meta)
				require.NoError(t, err)
			},
			expectedFraction:          0.05,
			expectUncheckpointedSpans: true,
		},
		{
			name: "automatic flush clears uncheckpointed state",
			setupFunc: func(t *testing.T, tracker *inspectProgressTracker, job *jobs.Job) {
				require.NoError(t, tracker.initJobProgress(ctx, totalChecks, 0))

				// Send progress with completed spans.
				spans := []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")}}
				meta, err := createProcessorProgressUpdate(100, false, spans)
				require.NoError(t, err)
				_, err = tracker.updateProgressCache(meta)
				require.NoError(t, err)

				// Wait for automatic checkpoint flush.
				testutils.SucceedsSoon(t, func() error {
					if tracker.hasUncheckpointedSpans() {
						return errors.New("still has uncheckpointed spans")
					}
					return nil
				})
			},
			expectedFraction:          0.1,
			expectUncheckpointedSpans: false,
		},
		{
			name: "multiple span updates merge and checkpoint",
			setupFunc: func(t *testing.T, tracker *inspectProgressTracker, job *jobs.Job) {
				require.NoError(t, tracker.initJobProgress(ctx, totalChecks, 0))

				// Send multiple progress updates with different spans.
				spans1 := []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("d")}}
				meta1, err := createProcessorProgressUpdate(100, false, spans1)
				require.NoError(t, err)
				_, err = tracker.updateProgressCache(meta1)
				require.NoError(t, err)

				spans2 := []roachpb.Span{{Key: roachpb.Key("d"), EndKey: roachpb.Key("g")}}
				meta2, err := createProcessorProgressUpdate(100, false, spans2)
				require.NoError(t, err)
				_, err = tracker.updateProgressCache(meta2)
				require.NoError(t, err)

				// Wait for checkpoint to complete.
				testutils.SucceedsSoon(t, func() error {
					if tracker.hasUncheckpointedSpans() {
						return errors.New("still has uncheckpointed spans")
					}
					return nil
				})
			},
			expectedFraction:          0.2,
			expectUncheckpointedSpans: false,
		},
		{
			name: "immediate flush on drained processor",
			setupFunc: func(t *testing.T, tracker *inspectProgressTracker, job *jobs.Job) {
				require.NoError(t, tracker.initJobProgress(ctx, totalChecks, 0))

				// Send progress with drained=true to trigger immediate flush.
				spans := []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}}
				meta, err := createProcessorProgressUpdate(500, true, spans)
				require.NoError(t, err)
				require.NoError(t, tracker.handleProgressUpdate(ctx, meta))

				// Immediate flush should have happened, so no uncheckpointed spans.
				require.False(t, tracker.hasUncheckpointedSpans())
			},
			expectedFraction:          0.5,
			expectUncheckpointedSpans: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a fresh job for each test case.
			record := jobs.Record{
				Details:  jobspb.InspectDetails{},
				Progress: jobspb.InspectProgress{},
				Username: username.TestUserName(),
			}

			freshJob, err := s.JobRegistry().(*jobs.Registry).CreateJobWithTxn(ctx, record, s.JobRegistry().(*jobs.Registry).MakeJobID(), nil)
			require.NoError(t, err)

			freshTracker := newInspectProgressTracker(freshJob, &s.ClusterSettings().SV, s.InternalDB().(isql.DB))
			defer freshTracker.terminateTracker()

			// Override intervals for faster testing.
			const fastCheckpointInterval = 10 * time.Millisecond
			const fastFractionInterval = 5 * time.Millisecond
			freshTracker.checkpointInterval = func() time.Duration { return fastCheckpointInterval }
			freshTracker.fractionInterval = func() time.Duration { return fastFractionInterval }

			// Run the test setup.
			tc.setupFunc(t, freshTracker, freshJob)

			// Verify uncheckpointed spans state.
			require.Equal(t, tc.expectUncheckpointedSpans, freshTracker.hasUncheckpointedSpans(),
				"unexpected uncheckpointed spans state")

			// Verify fraction complete.
			progress := freshJob.Progress()
			fractionCompleted, ok := progress.Progress.(*jobspb.Progress_FractionCompleted)
			require.True(t, ok, "progress should be FractionCompleted type")
			if tc.expectedFraction == 0.0 {
				// For zero expected fraction, check immediately.
				require.Equal(t, tc.expectedFraction, fractionCompleted.FractionCompleted)
			} else {
				// For non-zero expected fraction, wait for async flush to complete.
				testutils.SucceedsSoon(t, func() error {
					progress = freshJob.Progress()
					fractionCompleted, ok = progress.Progress.(*jobspb.Progress_FractionCompleted)
					if !ok {
						return errors.New("progress should be FractionCompleted type")
					}
					// Check if fraction is within epsilon (1% tolerance).
					const epsilon = 0.01
					if math.Abs(float64(fractionCompleted.FractionCompleted-tc.expectedFraction)) > epsilon {
						return errors.Newf("fraction complete not within epsilon: expected %f ± %f, got %f",
							tc.expectedFraction, epsilon, fractionCompleted.FractionCompleted)
					}
					return nil
				})
			}
		})
	}
}
