// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttljob

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
