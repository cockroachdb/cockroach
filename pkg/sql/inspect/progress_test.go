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
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

	registry := s.ApplicationLayer().JobRegistry().(*jobs.Registry)

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
			execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
			tracker := newInspectProgressTracker(job, &s.ClusterSettings().SV, s.InternalDB().(descs.DB), execCfg.Codec, execCfg.ProtectedTimestampManager)
			defer tracker.terminateTracker(ctx)

			// Initialize job progress
			err := tracker.initJobProgress(ctx, tc.totalCheckCount, 0 /* completedCheckCount */)
			require.NoError(t, err)

			// Verify initial internal state
			totalChecks, completedChecks := tracker.getCachedCheckCounts()
			require.Equal(t, tc.totalCheckCount, totalChecks)
			require.Equal(t, int64(0), completedChecks)

			// Simulate processor progress updates
			for i, checksCompleted := range tc.progressUpdates {
				meta, _, err := createProcessorProgressUpdate(
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
			execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
			tracker1 := newInspectProgressTracker(job, &s.ClusterSettings().SV, s.InternalDB().(descs.DB), execCfg.Codec, execCfg.ProtectedTimestampManager)
			defer tracker1.terminateTracker(ctx)

			// Initialize job progress.
			err = tracker1.initJobProgress(ctx, 100 /* totalCheckCount */, 0 /* completedCheckCount */)
			require.NoError(t, err)

			// Send progress updates.
			for _, update := range tc.progressUpdates {
				meta, _, err := createProcessorProgressUpdate(
					update.checksCompleted,
					update.finished,
					update.completedSpans,
				)
				require.NoError(t, err)

				err = tracker1.handleProgressUpdate(ctx, meta)
				require.NoError(t, err)
			}

			// Verify completed spans are stored (or not) as expected.
			storedCompletedSpans := verifyStoredSpans(t, ctx, s.InternalDB().(descs.DB), job.ID(), tc.expectStored, tc.expectSpanCount)

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
			tracker1.terminateTracker(ctx)

			// Create new tracker and verify it loads completed spans.
			tracker2 := newInspectProgressTracker(job, &s.ClusterSettings().SV, s.InternalDB().(descs.DB), execCfg.Codec, execCfg.ProtectedTimestampManager)
			defer tracker2.terminateTracker(ctx)

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
// Returns both the metadata and the parsed progress struct for use with updateProgressCache.
func createProcessorProgressUpdate(
	checksCompleted int64, finished bool, completedSpans []roachpb.Span,
) (*execinfrapb.ProducerMetadata, jobspb.InspectProcessorProgress, error) {
	progressMsg := jobspb.InspectProcessorProgress{
		ChecksCompleted: checksCompleted,
		Finished:        finished,
	}

	progressAny, err := pbtypes.MarshalAny(&progressMsg)
	if err != nil {
		return nil, jobspb.InspectProcessorProgress{}, err
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

	return meta, progressMsg, nil
}

func TestInspectProgressTracker_ProgressFlushConditions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, s, registry, _, cleanup := setupProgressTestInfra(t)
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
				meta, progress, err := createProcessorProgressUpdate(100, false, nil)
				require.NoError(t, err)
				_, err = tracker.updateProgressCache(meta, progress)
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
				meta, progress, err := createProcessorProgressUpdate(50, false, spans)
				require.NoError(t, err)
				_, err = tracker.updateProgressCache(meta, progress)
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
				meta, progress, err := createProcessorProgressUpdate(100, false, spans)
				require.NoError(t, err)
				_, err = tracker.updateProgressCache(meta, progress)
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
				meta1, progress1, err := createProcessorProgressUpdate(100, false, spans1)
				require.NoError(t, err)
				_, err = tracker.updateProgressCache(meta1, progress1)
				require.NoError(t, err)

				spans2 := []roachpb.Span{{Key: roachpb.Key("d"), EndKey: roachpb.Key("g")}}
				meta2, progress2, err := createProcessorProgressUpdate(100, false, spans2)
				require.NoError(t, err)
				_, err = tracker.updateProgressCache(meta2, progress2)
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
				meta, _, err := createProcessorProgressUpdate(500, true, spans)
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

			freshJob, err := registry.CreateJobWithTxn(ctx, record, registry.MakeJobID(), nil)
			require.NoError(t, err)

			execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
			freshTracker := newInspectProgressTracker(freshJob, &s.ClusterSettings().SV, s.InternalDB().(descs.DB), execCfg.Codec, execCfg.ProtectedTimestampManager)
			defer freshTracker.terminateTracker(ctx)

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

// createSpanStartedProgressUpdate creates a "span started" progress update message.
func createSpanStartedProgressUpdate(
	span roachpb.Span, ts hlc.Timestamp,
) (*execinfrapb.ProducerMetadata, error) {
	progressMsg := jobspb.InspectProcessorProgress{
		SpanStarted: span,
		StartedAt:   ts,
	}

	progressAny, err := pbtypes.MarshalAny(&progressMsg)
	if err != nil {
		return nil, err
	}

	meta := &execinfrapb.ProducerMetadata{
		BulkProcessorProgress: &execinfrapb.RemoteProducerMetadata_BulkProcessorProgress{
			ProgressDetails: *progressAny,
		},
	}

	return meta, nil
}

func TestInspectProgressTracker_PTSLifecycle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, s, _, job, cleanup := setupProgressTestInfra(t)
	defer cleanup()

	t.Run("span started triggers PTS setup and span completed triggers cleanup", func(t *testing.T) {
		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
		tracker := newInspectProgressTracker(job, &s.ClusterSettings().SV, s.InternalDB().(descs.DB), execCfg.Codec, execCfg.ProtectedTimestampManager)
		defer tracker.terminateTracker(ctx)

		// Track PTS setup and cleanup calls.
		var setupCalls []roachpb.Span
		var setupTimestamps []hlc.Timestamp
		var cleanupCalls []string

		tracker.testingPTSProtector = func(ctx context.Context, span roachpb.Span, ts hlc.Timestamp) jobsprotectedts.Cleaner {
			setupCalls = append(setupCalls, span)
			setupTimestamps = append(setupTimestamps, ts)
			// Return a cleaner that tracks when it's called.
			return func(ctx context.Context) error {
				cleanupCalls = append(cleanupCalls, span.String())
				return nil
			}
		}

		// Initialize progress to allow updates.
		err := tracker.initJobProgress(ctx, 10, 0)
		require.NoError(t, err)

		// Simulate span started message.
		testSpan := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}
		testTimestamp := hlc.Timestamp{WallTime: 12345}
		spanStartedMeta, err := createSpanStartedProgressUpdate(testSpan, testTimestamp)
		require.NoError(t, err)

		err = tracker.handleProgressUpdate(ctx, spanStartedMeta)
		require.NoError(t, err)

		// Verify PTS setup was called.
		require.Len(t, setupCalls, 1)
		require.Equal(t, testSpan, setupCalls[0])
		require.Equal(t, testTimestamp, setupTimestamps[0])
		require.Empty(t, cleanupCalls, "cleaner should not be called yet")

		// Simulate span completed message.
		spanCompletedMeta, _, err := createProcessorProgressUpdate(1, false, []roachpb.Span{testSpan})
		require.NoError(t, err)

		err = tracker.handleProgressUpdate(ctx, spanCompletedMeta)
		require.NoError(t, err)

		// Verify cleaner was called.
		require.Len(t, cleanupCalls, 1)
		require.Equal(t, testSpan.String(), cleanupCalls[0])
	})

	t.Run("terminateTracker cleans up PTS record", func(t *testing.T) {
		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
		tracker := newInspectProgressTracker(job, &s.ClusterSettings().SV, s.InternalDB().(descs.DB), execCfg.Codec, execCfg.ProtectedTimestampManager)

		var cleanupCalls int

		tracker.testingPTSProtector = func(ctx context.Context, span roachpb.Span, ts hlc.Timestamp) jobsprotectedts.Cleaner {
			return func(ctx context.Context) error {
				cleanupCalls++
				return nil
			}
		}

		// Initialize progress.
		err := tracker.initJobProgress(ctx, 10, 0)
		require.NoError(t, err)

		// Start two spans but don't complete them.
		// With the minimum timestamp approach, only the first span (with smaller timestamp)
		// will create a PTS record. The second span's timestamp is newer so it's already protected.
		span1 := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}
		span2 := roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}

		meta1, err := createSpanStartedProgressUpdate(span1, hlc.Timestamp{WallTime: 100})
		require.NoError(t, err)
		err = tracker.handleProgressUpdate(ctx, meta1)
		require.NoError(t, err)

		meta2, err := createSpanStartedProgressUpdate(span2, hlc.Timestamp{WallTime: 200})
		require.NoError(t, err)
		err = tracker.handleProgressUpdate(ctx, meta2)
		require.NoError(t, err)

		// No cleanups yet - we have one active PTS record protecting the minimum timestamp.
		require.Equal(t, 0, cleanupCalls)

		// Terminate tracker - should clean up the single PTS record.
		tracker.terminateTracker(ctx)

		// Verify the single cleaner was called (we maintain only one PTS at the minimum timestamp).
		require.Equal(t, 1, cleanupCalls)
	})

	t.Run("minimum timestamp PTS is updated when oldest span completes", func(t *testing.T) {
		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
		tracker := newInspectProgressTracker(job, &s.ClusterSettings().SV, s.InternalDB().(descs.DB), execCfg.Codec, execCfg.ProtectedTimestampManager)
		defer tracker.terminateTracker(ctx)

		// Track PTS setup calls with their timestamps.
		var setupTimestamps []hlc.Timestamp
		var cleanupCalls int

		tracker.testingPTSProtector = func(ctx context.Context, span roachpb.Span, ts hlc.Timestamp) jobsprotectedts.Cleaner {
			setupTimestamps = append(setupTimestamps, ts)
			return func(ctx context.Context) error {
				cleanupCalls++
				return nil
			}
		}

		err := tracker.initJobProgress(ctx, 10, 0)
		require.NoError(t, err)

		// Start three spans with different timestamps.
		// The minimum timestamp (100) should be protected.
		span1 := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}
		span2 := roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}
		span3 := roachpb.Span{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")}

		meta1, err := createSpanStartedProgressUpdate(span1, hlc.Timestamp{WallTime: 100})
		require.NoError(t, err)
		err = tracker.handleProgressUpdate(ctx, meta1)
		require.NoError(t, err)

		meta2, err := createSpanStartedProgressUpdate(span2, hlc.Timestamp{WallTime: 300})
		require.NoError(t, err)
		err = tracker.handleProgressUpdate(ctx, meta2)
		require.NoError(t, err)

		meta3, err := createSpanStartedProgressUpdate(span3, hlc.Timestamp{WallTime: 200})
		require.NoError(t, err)
		err = tracker.handleProgressUpdate(ctx, meta3)
		require.NoError(t, err)

		// Only one PTS should be set up (at timestamp 100, the minimum).
		require.Len(t, setupTimestamps, 1)
		require.Equal(t, hlc.Timestamp{WallTime: 100}, setupTimestamps[0])
		require.Equal(t, 0, cleanupCalls)

		// Complete span1 (the one with minimum timestamp 100).
		// This should trigger PTS update to the new minimum (200 from span3).
		spanCompletedMeta1, _, err := createProcessorProgressUpdate(1, false, []roachpb.Span{span1})
		require.NoError(t, err)
		err = tracker.handleProgressUpdate(ctx, spanCompletedMeta1)
		require.NoError(t, err)

		// Old PTS cleaned up, new one created at timestamp 200.
		require.Len(t, setupTimestamps, 2)
		require.Equal(t, hlc.Timestamp{WallTime: 200}, setupTimestamps[1])
		require.Equal(t, 1, cleanupCalls) // Old PTS was cleaned up.

		// Complete span3 (timestamp 200, the current minimum).
		// This should trigger PTS update to the new minimum (300 from span2).
		spanCompletedMeta3, _, err := createProcessorProgressUpdate(1, false, []roachpb.Span{span3})
		require.NoError(t, err)
		err = tracker.handleProgressUpdate(ctx, spanCompletedMeta3)
		require.NoError(t, err)

		// Another update: old PTS cleaned up, new one at timestamp 300.
		require.Len(t, setupTimestamps, 3)
		require.Equal(t, hlc.Timestamp{WallTime: 300}, setupTimestamps[2])
		require.Equal(t, 2, cleanupCalls)

		// Complete the last span (span2).
		// This should clean up PTS entirely (no more active spans).
		spanCompletedMeta2, _, err := createProcessorProgressUpdate(1, false, []roachpb.Span{span2})
		require.NoError(t, err)
		err = tracker.handleProgressUpdate(ctx, spanCompletedMeta2)
		require.NoError(t, err)

		// Final cleanup - no new PTS created.
		require.Len(t, setupTimestamps, 3) // No new PTS.
		require.Equal(t, 3, cleanupCalls)  // Final cleanup.
	})

	t.Run("no cleaner called if PTS protector returns nil", func(t *testing.T) {
		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
		tracker := newInspectProgressTracker(job, &s.ClusterSettings().SV, s.InternalDB().(descs.DB), execCfg.Codec, execCfg.ProtectedTimestampManager)
		defer tracker.terminateTracker(ctx)

		var setupCalls int

		tracker.testingPTSProtector = func(ctx context.Context, span roachpb.Span, ts hlc.Timestamp) jobsprotectedts.Cleaner {
			setupCalls++
			return nil // Return nil cleaner (e.g., PTS setup declined).
		}

		err := tracker.initJobProgress(ctx, 10, 0)
		require.NoError(t, err)

		testSpan := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}
		spanStartedMeta, err := createSpanStartedProgressUpdate(testSpan, hlc.Timestamp{WallTime: 12345})
		require.NoError(t, err)

		err = tracker.handleProgressUpdate(ctx, spanStartedMeta)
		require.NoError(t, err)

		require.Equal(t, 1, setupCalls)

		// Verify no cleaner is stored (shouldn't panic on span completion).
		spanCompletedMeta, _, err := createProcessorProgressUpdate(1, false, []roachpb.Span{testSpan})
		require.NoError(t, err)

		err = tracker.handleProgressUpdate(ctx, spanCompletedMeta)
		require.NoError(t, err)
	})
}
