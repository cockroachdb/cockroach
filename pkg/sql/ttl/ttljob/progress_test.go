// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttljob

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

func makeFakeSpans(n int) []roachpb.Span {
	spans := make([]roachpb.Span, n)
	for i := 0; i < n; i++ {
		start := roachpb.Key(fmt.Sprintf("k%03d", i))
		end := roachpb.Key(fmt.Sprintf("k%03d", i+1))
		spans[i] = roachpb.Span{Key: start, EndKey: end}
	}
	return spans
}

func createProgressMetadata(
	t *testing.T,
	processorID int32,
	processedSpanCount int64,
	deletedRowCount int64,
	totalSpanCount int64,
	completedSpans []roachpb.Span,
) *execinfrapb.ProducerMetadata {
	progressMsg := &jobspb.RowLevelTTLProcessorProgress{
		ProcessorID:        processorID,
		ProcessedSpanCount: processedSpanCount,
		DeletedRowCount:    deletedRowCount,
		TotalSpanCount:     totalSpanCount,
	}
	progressAny, err := types.MarshalAny(progressMsg)
	if err != nil {
		t.Fatalf("failed to marshal progress: %v", err)
	}
	return &execinfrapb.ProducerMetadata{
		BulkProcessorProgress: &execinfrapb.RemoteProducerMetadata_BulkProcessorProgress{
			CompletedSpans:  completedSpans,
			ProgressDetails: *progressAny,
		},
	}
}

func TestLegacyProgressTrackerRefreshJobProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	testCases := []struct {
		name              string
		jobTotalSpanCount int64
		processorUpdates  []struct {
			processorID        int32
			processedSpanCount int64
			deletedRowCount    int64
			totalSpanCount     int64
		}
		expectedFraction       float32
		expectedProcessedSpans int64
		expectedDeletedRows    int64
		expectedProcessorCount int
		shouldError            bool
	}{
		{
			name:              "empty metadata",
			jobTotalSpanCount: 10,
			processorUpdates:  nil,
			expectedFraction:  0.0,
		},
		{
			name:              "single processor progress",
			jobTotalSpanCount: 10,
			processorUpdates: []struct {
				processorID        int32
				processedSpanCount int64
				deletedRowCount    int64
				totalSpanCount     int64
			}{
				{processorID: 1, processedSpanCount: 3, deletedRowCount: 150, totalSpanCount: 10},
			},
			expectedFraction:       0.3, // 3/10
			expectedProcessedSpans: 3,
			expectedDeletedRows:    150,
			expectedProcessorCount: 1,
		},
		{
			name:              "multiple processors",
			jobTotalSpanCount: 20,
			processorUpdates: []struct {
				processorID        int32
				processedSpanCount int64
				deletedRowCount    int64
				totalSpanCount     int64
			}{
				{processorID: 1, processedSpanCount: 5, deletedRowCount: 100, totalSpanCount: 10},
				{processorID: 2, processedSpanCount: 3, deletedRowCount: 75, totalSpanCount: 10},
			},
			expectedFraction:       0.4, // (5+3)/20
			expectedProcessedSpans: 8,
			expectedDeletedRows:    175,
			expectedProcessorCount: 2,
		},
		{
			name:              "processor update overwrites previous",
			jobTotalSpanCount: 10,
			processorUpdates: []struct {
				processorID        int32
				processedSpanCount int64
				deletedRowCount    int64
				totalSpanCount     int64
			}{
				{processorID: 1, processedSpanCount: 2, deletedRowCount: 50, totalSpanCount: 10},
				{processorID: 1, processedSpanCount: 4, deletedRowCount: 100, totalSpanCount: 10}, // Same processor ID
			},
			expectedFraction:       0.4, // 4/10 (latest update)
			expectedProcessedSpans: 4,
			expectedDeletedRows:    100,
			expectedProcessorCount: 1,
		},
		{
			name:              "total span count exceeded",
			jobTotalSpanCount: 5,
			processorUpdates: []struct {
				processorID        int32
				processedSpanCount int64
				deletedRowCount    int64
				totalSpanCount     int64
			}{
				{processorID: 1, processedSpanCount: 3, deletedRowCount: 50, totalSpanCount: 10}, // totalSpanCount > jobTotalSpanCount
			},
			shouldError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			progressUpdater := &legacyProgressTracker{}
			progressUpdater.setupUpdateFrequency(tc.jobTotalSpanCount)

			// Create initial progress
			initialProgress := &jobspb.Progress{
				Details: &jobspb.Progress_RowLevelTTL{
					RowLevelTTL: &jobspb.RowLevelTTLProgress{
						JobTotalSpanCount: tc.jobTotalSpanCount,
					},
				},
				Progress: &jobspb.Progress_FractionCompleted{
					FractionCompleted: 0,
				},
			}
			md := jobs.JobMetadata{Progress: initialProgress}

			var finalProgress *jobspb.Progress
			var err error

			if len(tc.processorUpdates) == 0 {
				// Test with empty metadata.
				emptyMetadata := &execinfrapb.ProducerMetadata{}
				finalProgress, err = progressUpdater.refreshJobProgress(ctx, &md, emptyMetadata)
				require.NoError(t, err)
				require.Nil(t, finalProgress)
				return
			}

			// Apply each processor update sequentially.
			for _, update := range tc.processorUpdates {
				metadata := createProgressMetadata(t, update.processorID, update.processedSpanCount,
					update.deletedRowCount, update.totalSpanCount, makeFakeSpans(int(update.processedSpanCount)))

				finalProgress, err = progressUpdater.refreshJobProgress(ctx, &md, metadata)
				if tc.shouldError {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)

				// Update md.Progress for the next iteration.
				if finalProgress != nil {
					md.Progress = finalProgress
				}
			}

			require.NotNil(t, finalProgress)

			// Verify final results
			require.InEpsilon(t, tc.expectedFraction, finalProgress.GetFractionCompleted(), 0.001)

			ttlProgress := finalProgress.GetRowLevelTTL()
			require.Equal(t, tc.jobTotalSpanCount, ttlProgress.JobTotalSpanCount)
			require.Equal(t, tc.expectedProcessedSpans, ttlProgress.JobProcessedSpanCount)
			require.Equal(t, tc.expectedDeletedRows, ttlProgress.JobDeletedRowCount)
			require.Len(t, ttlProgress.ProcessorProgresses, tc.expectedProcessorCount)
		})
	}
}

// setupCheckpointProgressTrackerForTest creates a checkpointProgressTracker with test-friendly settings
func setupCheckpointProgressTrackerForTest(
	ctx context.Context,
) (*checkpointProgressTracker, *cluster.Settings) {
	settings := cluster.MakeTestingClusterSettings()
	fractionUpdateInterval.Override(ctx, &settings.SV, 50*time.Millisecond)
	checkpointInterval.Override(ctx, &settings.SV, 150*time.Millisecond)

	updater := &checkpointProgressTracker{
		job:                nil,
		settings:           settings,
		clock:              timeutil.DefaultTimeSource{},
		fractionInterval:   func() time.Duration { return fractionUpdateInterval.Get(&settings.SV) },
		checkpointInterval: func() time.Duration { return checkpointInterval.Get(&settings.SV) },
	}
	return updater, settings
}

func TestCheckpointProgressUpdater(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	updater, settings := setupCheckpointProgressTrackerForTest(ctx)

	defer func() {
		if updater.stopFunc != nil {
			updater.termTracker()
		}
	}()

	t.Run("init progress creates goroutines", func(t *testing.T) {
		err := updater.initJobProgress(ctx, 100)
		require.NoError(t, err)

		require.NotNil(t, updater.stopFunc)
	})

	t.Run("cleanup stops goroutines", func(t *testing.T) {
		require.NotNil(t, updater.stopFunc)

		updater.termTracker()

		require.Nil(t, updater.stopFunc)
	})

	t.Run("verify intervals are different", func(t *testing.T) {
		fractionInterval := fractionUpdateInterval.Get(&settings.SV)
		checkpointIntervalVal := checkpointInterval.Get(&settings.SV)

		require.NotEqual(t, fractionInterval, checkpointIntervalVal)
		require.Less(t, fractionInterval, checkpointIntervalVal)
		require.Equal(t, 50*time.Millisecond, fractionInterval)
		require.Equal(t, 150*time.Millisecond, checkpointIntervalVal)
	})
}

func TestCheckpointProgressUpdaterContents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	updater, _ := setupCheckpointProgressTrackerForTest(ctx)

	defer func() {
		if updater.stopFunc != nil {
			updater.termTracker()
		}
	}()

	totalSpans := 10
	// Create non-adjacent spans to avoid automatic merging.
	testSpans := make([]roachpb.Span, totalSpans)
	for i := 0; i < totalSpans; i++ {
		start := roachpb.Key(fmt.Sprintf("t%03d", i*10)) // t000, t010, t020, etc.
		end := roachpb.Key(fmt.Sprintf("t%03d", i*10+1)) // t001, t011, t021, etc.
		testSpans[i] = roachpb.Span{Key: start, EndKey: end}
	}

	err := updater.initJobProgress(ctx, int64(totalSpans))
	require.NoError(t, err)

	// Verify no spans have been completed yet.
	currentSpans := updater.addCompleted() // Empty call to get current state.
	require.Len(t, currentSpans, 0)

	// Add first batch of spans.
	result1 := updater.addCompleted(testSpans[0:2]...)
	require.Len(t, result1, 2)
	require.Contains(t, result1, testSpans[0])
	require.Contains(t, result1, testSpans[1])

	// Add second batch of spans.
	result2 := updater.addCompleted(testSpans[2:7]...)
	require.Len(t, result2, 7) // Should have 2 + 5 = 7 total
	for i := 0; i < 7; i++ {
		require.Contains(t, result2, testSpans[i])
	}

	// Add final batch of spans.
	result3 := updater.addCompleted(testSpans[7:10]...)
	require.Len(t, result3, totalSpans) // Should have all 10 spans
	for i := 0; i < totalSpans; i++ {
		require.Contains(t, result3, testSpans[i])
	}

	// Test deduplication - adding the same spans again should not increase count.
	result4 := updater.addCompleted(testSpans[0:5]...)
	require.Len(t, result4, totalSpans) // Should still be 10, not 15
}

func TestCheckpointProgressUpdaterSpanDeduplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	updater, _ := setupCheckpointProgressTrackerForTest(ctx)

	// Test 1: Duplicate spans are deduplicated using addCompleted directly.
	span1 := roachpb.Span{Key: roachpb.Key("k01"), EndKey: roachpb.Key("k02")}
	span2 := roachpb.Span{Key: roachpb.Key("k05"), EndKey: roachpb.Key("k06")}
	span3 := roachpb.Span{Key: roachpb.Key("k09"), EndKey: roachpb.Key("k10")}

	// Add initial spans.
	result1 := updater.addCompleted(span1, span2, span3)
	require.Len(t, result1, 3)
	require.Contains(t, result1, span1)
	require.Contains(t, result1, span2)
	require.Contains(t, result1, span3)

	// Add the same spans again (simulating processor retry) - should not increase count.
	result2 := updater.addCompleted(span1, span2, span3)
	require.Len(t, result2, 3)
	require.Contains(t, result2, span1)
	require.Contains(t, result2, span2)
	require.Contains(t, result2, span3)

	// Test 2: Adjacent spans are merged.
	adjacentSpan1 := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}
	adjacentSpan2 := roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}

	result3 := updater.addCompleted(adjacentSpan1, adjacentSpan2)
	// Should have merged the adjacent spans plus the original 3 = 4 total.
	require.Len(t, result3, 4)
	mergedSpan := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}
	require.Contains(t, result3, mergedSpan)
}

func TestCheckpointProgressUpdaterAddCompleted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	updater, _ := setupCheckpointProgressTrackerForTest(ctx)

	// Create non-adjacent spans to avoid automatic merging.
	span1 := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}
	span2 := roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("e")}
	span3 := roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}

	// Add initial spans.
	result1 := updater.addCompleted(span1, span2)
	require.Len(t, result1, 2)
	require.Contains(t, result1, span1)
	require.Contains(t, result1, span2)

	// Add duplicate span - should not increase count.
	result2 := updater.addCompleted(span1)
	require.Len(t, result2, 2)

	// Add new span - should increase count.
	result3 := updater.addCompleted(span3)
	require.Len(t, result3, 3)
	require.Contains(t, result3, span3)
}

func TestCheckpointProgressUpdaterRestartScenario(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	updater, settings := setupCheckpointProgressTrackerForTest(ctx)

	// Test restart scenario by first simulating existing spans in the progress tracker.
	// Use non-adjacent spans to avoid merging.
	existingSpan1 := roachpb.Span{Key: roachpb.Key("e01"), EndKey: roachpb.Key("e02")}
	existingSpan2 := roachpb.Span{Key: roachpb.Key("e05"), EndKey: roachpb.Key("e06")}
	existingSpan3 := roachpb.Span{Key: roachpb.Key("e09"), EndKey: roachpb.Key("e10")}

	// First, simulate existing progress by calling initTracker with existing progress.
	existingProgress := &jobspb.RowLevelTTLProgress{
		CompletedSpans: []roachpb.Span{existingSpan1, existingSpan2, existingSpan3},
	}
	completedSpans, err := updater.initTracker(ctx, existingProgress, &settings.SV)
	require.NoError(t, err)
	require.Len(t, completedSpans, 3)
	require.Contains(t, completedSpans, existingSpan1)
	require.Contains(t, completedSpans, existingSpan2)
	require.Contains(t, completedSpans, existingSpan3)

	// Then initialize job progress.
	err = updater.initJobProgress(ctx, 10)
	require.NoError(t, err)

	defer func() {
		if updater.stopFunc != nil {
			updater.termTracker()
		}
	}()

	// Verify existing spans are available.
	currentSpans := updater.addCompleted() // Get current state through public method.
	require.Len(t, currentSpans, 3)
	require.Contains(t, currentSpans, existingSpan1)
	require.Contains(t, currentSpans, existingSpan2)
	require.Contains(t, currentSpans, existingSpan3)

	// Test adding new spans on top of existing ones.
	newSpan1 := roachpb.Span{Key: roachpb.Key("n01"), EndKey: roachpb.Key("n02")}
	newSpan2 := roachpb.Span{Key: roachpb.Key("n05"), EndKey: roachpb.Key("n06")}

	result := updater.addCompleted(newSpan1, newSpan2)
	// Should have 3 existing + 2 new = 5 total.
	require.Len(t, result, 5)
	require.Contains(t, result, existingSpan1)
	require.Contains(t, result, existingSpan2)
	require.Contains(t, result, existingSpan3)
	require.Contains(t, result, newSpan1)
	require.Contains(t, result, newSpan2)
}

func TestCheckpointProgressUpdaterOverlapDetection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	updater, _ := setupCheckpointProgressTrackerForTest(ctx)

	// Test: SpanGroup should prevent overlaps, so overlap detection should never trigger.
	// In normal operation, SpanGroup merges overlapping spans automatically.
	span1 := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}
	span2 := roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("d")} // Overlaps with span1

	// Should succeed because SpanGroup automatically merges overlapping spans.
	result := updater.addCompleted(span1, span2)

	// SpanGroup should have merged the overlapping spans into one.
	require.Len(t, result, 1)
	mergedSpan := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("d")}
	require.Contains(t, result, mergedSpan)

	// Test with multiple overlapping spans.
	span3 := roachpb.Span{Key: roachpb.Key("e"), EndKey: roachpb.Key("g")}
	span4 := roachpb.Span{Key: roachpb.Key("f"), EndKey: roachpb.Key("h")} // Overlaps with span3
	span5 := roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("i")} // Adjacent to span4

	result2 := updater.addCompleted(span3, span4, span5)
	// Should have original merged span + the new merged span = 2 total
	require.Len(t, result2, 2)
	require.Contains(t, result2, mergedSpan) // Original merged span
	mergedSpan2 := roachpb.Span{Key: roachpb.Key("e"), EndKey: roachpb.Key("i")}
	require.Contains(t, result2, mergedSpan2) // New merged span
}

func TestCheckpointProgressUpdaterSpanCountConsistency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	updater, _ := setupCheckpointProgressTrackerForTest(ctx)

	// Test span count consistency validation using non-adjacent spans.
	span1 := roachpb.Span{Key: roachpb.Key("c01"), EndKey: roachpb.Key("c02")}
	span2 := roachpb.Span{Key: roachpb.Key("c05"), EndKey: roachpb.Key("c06")}
	span3 := roachpb.Span{Key: roachpb.Key("c09"), EndKey: roachpb.Key("c10")}

	// Add spans and verify count consistency.
	result := updater.addCompleted(span1, span2, span3)

	// Verify that the number of spans returned matches what we added.
	require.Len(t, result, 3)
	require.Contains(t, result, span1)
	require.Contains(t, result, span2)
	require.Contains(t, result, span3)

	// Test incremental additions maintain consistency.
	span4 := roachpb.Span{Key: roachpb.Key("c13"), EndKey: roachpb.Key("c14")}
	span5 := roachpb.Span{Key: roachpb.Key("c17"), EndKey: roachpb.Key("c18")}

	result2 := updater.addCompleted(span4, span5)
	// Should have original 3 + new 2 = 5 total.
	require.Len(t, result2, 5)
	require.Contains(t, result2, span1)
	require.Contains(t, result2, span2)
	require.Contains(t, result2, span3)
	require.Contains(t, result2, span4)
	require.Contains(t, result2, span5)

	// Test that duplicate additions don't change count.
	result3 := updater.addCompleted(span1, span3) // Add duplicates
	require.Len(t, result3, 5)                    // Should still be 5, not 7
}
