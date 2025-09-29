// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

func TestInspectProgressTracker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	registry := s.JobRegistry().(*jobs.Registry)

	// Create a job using INSPECT details
	record := jobs.Record{
		Details:  jobspb.InspectDetails{},
		Progress: jobspb.InspectProgress{},
		Username: username.TestUserName(),
	}

	job, err := registry.CreateJobWithTxn(ctx, record, registry.MakeJobID(), nil /* txn */)
	require.NoError(t, err)

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
			tracker := newInspectProgressTracker(job, time.Minute)
			defer tracker.terminateTracker(ctx)

			// Initialize job progress
			err := tracker.initJobProgress(ctx, tc.totalCheckCount)
			require.NoError(t, err)

			// Verify initial internal state
			require.Equal(t, tc.totalCheckCount, tracker.totalChecks)
			require.Equal(t, int64(0), tracker.completedChecks.Load())

			// Simulate processor progress updates
			for i, checksCompleted := range tc.progressUpdates {
				// Create processor progress metadata like the real processor does
				progressMsg := &jobspb.InspectProcessorProgress{
					ChecksCompleted: checksCompleted,
					Finished:        i == len(tc.progressUpdates)-1, // Last update is finished
				}

				progressAny, err := pbtypes.MarshalAny(progressMsg)
				require.NoError(t, err)

				meta := &execinfrapb.ProducerMetadata{
					BulkProcessorProgress: &execinfrapb.RemoteProducerMetadata_BulkProcessorProgress{
						ProgressDetails: *progressAny,
						NodeID:          1, // Test node ID
						FlowID:          execinfrapb.FlowID{},
						ProcessorID:     int32(i),
						Drained:         i == len(tc.progressUpdates)-1,
					},
				}

				// Handle the processor progress update
				err = tracker.handleProcessorProgress(ctx, meta)
				require.NoError(t, err)
			}

			// Verify final internal state
			require.Equal(t, tc.totalCheckCount, tracker.totalChecks)
			require.Equal(t, tc.expectedCompletedChecks, tracker.completedChecks.Load())

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
