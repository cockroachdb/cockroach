// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backfiller

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestPhaseTransitionCleaner verifies that phase transitions trigger cleanup
// of the correct subdirectories based on the completed phase.
func TestPhaseTransitionCleaner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const testJobID = jobspb.JobID(12345)
	testStoragePrefixes := []string{"nodelocal://1/", "nodelocal://2/"}

	tests := []struct {
		name              string
		transition        phaseTransition
		expectedSubdir    string
		shouldCallCleanup bool
	}{
		{
			name: "phase 0→1 transition cleans map/",
			transition: phaseTransition{
				TableID:            descpb.ID(1),
				OldPhase:           0,
				NewPhase:           1,
				SSTStoragePrefixes: testStoragePrefixes,
			},
			expectedSubdir:    "map/",
			shouldCallCleanup: true,
		},
		{
			name: "phase 1→2 transition cleans merge/iter-1/",
			transition: phaseTransition{
				TableID:            descpb.ID(1),
				OldPhase:           1,
				NewPhase:           2,
				SSTStoragePrefixes: testStoragePrefixes,
			},
			expectedSubdir:    "merge/iter-1/",
			shouldCallCleanup: true,
		},
		{
			name: "empty storage prefixes skips cleanup",
			transition: phaseTransition{
				TableID:            descpb.ID(1),
				OldPhase:           0,
				NewPhase:           1,
				SSTStoragePrefixes: nil,
			},
			expectedSubdir:    "",
			shouldCallCleanup: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			mockCleaner := &mockBulkJobCleaner{}
			cleaner := &phaseTransitionCleaner{
				jobID:   testJobID,
				cleaner: mockCleaner,
			}

			err := cleaner.cleanupTransition(ctx, tc.transition)
			require.NoError(t, err)

			if tc.shouldCallCleanup {
				require.Equal(t, 1, mockCleaner.cleanupCallCount)
				require.Equal(t, testJobID, mockCleaner.lastJobID)
				require.Equal(t, testStoragePrefixes, mockCleaner.lastStoragePrefixes)
				require.Equal(t, tc.expectedSubdir, mockCleaner.lastSubdirectory)
			} else {
				require.Equal(t, 0, mockCleaner.cleanupCallCount)
			}
		})
	}
}

// mockBulkJobCleaner is a test double that tracks calls to CleanupJobSubdirectory.
type mockBulkJobCleaner struct {
	cleanupCallCount    int
	lastJobID           jobspb.JobID
	lastStoragePrefixes []string
	lastSubdirectory    string
}

// CleanupJobSubdirectory records the call parameters for verification.
func (m *mockBulkJobCleaner) CleanupJobSubdirectory(
	_ context.Context, jobID jobspb.JobID, storagePrefixes []string, subdirectory string,
) error {
	m.cleanupCallCount++
	m.lastJobID = jobID
	m.lastStoragePrefixes = storagePrefixes
	m.lastSubdirectory = subdirectory
	return nil
}
