// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobfrontier"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestMigrateChangefeedSpanLevelCheckpoints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t,
		clusterversion.V26_2_ChangefeedsStopUsingSpanLevelCheckpoint)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         clusterversion.MinSupported.Version(),
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	registry := s.JobRegistry().(*jobs.Registry)
	db := s.InternalDB().(isql.DB)

	sp1 := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}
	sp2 := roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}
	sp3 := roachpb.Span{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")}
	ts1 := hlc.Timestamp{WallTime: 100}
	ts2 := hlc.Timestamp{WallTime: 200}
	ts3 := hlc.Timestamp{WallTime: 300}

	slc := jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
		ts1: {sp1},
		ts2: {sp2},
	})

	type testCase struct {
		slc              *jobspb.TimestampSpansMap
		existingFrontier []jobspb.ResolvedSpan
		wantFrontier     []jobspb.ResolvedSpan
	}

	tests := map[string]testCase{
		"with SLC": {
			slc: slc,
			wantFrontier: []jobspb.ResolvedSpan{
				{Span: sp1, Timestamp: ts1},
				{Span: sp2, Timestamp: ts2},
			},
		},
		"nil SLC": {
			slc:          nil,
			wantFrontier: nil,
		},
		"existing frontier and SLC": {
			slc: slc,
			existingFrontier: []jobspb.ResolvedSpan{
				{Span: sp3, Timestamp: ts3},
			},
			wantFrontier: []jobspb.ResolvedSpan{
				{Span: sp1, Timestamp: ts1},
				{Span: sp2, Timestamp: ts2},
				{Span: sp3, Timestamp: ts3},
			},
		},
	}

	// Create a changefeed job for each test case.
	jobIDs := make(map[string]jobspb.JobID, len(tests))
	for name, tc := range tests {
		jobID := registry.MakeJobID()
		jobIDs[name] = jobID
		record := jobs.Record{
			Description: name,
			Username:    username.RootUserName(),
			Details:     jobspb.ChangefeedDetails{},
			Progress: jobspb.ChangefeedProgress{
				SpanLevelCheckpoint: tc.slc,
			},
		}
		_, err := registry.CreateAdoptableJobWithTxn(ctx, record, jobID, nil /* txn */)
		require.NoError(t, err)

		// Write pre-existing frontier data if specified.
		if tc.existingFrontier != nil {
			require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				return jobfrontier.StoreResolvedSpans(ctx, txn, jobID, "coordinator", tc.existingFrontier)
			}))
		}
	}

	readProgress := func(t *testing.T, jobID jobspb.JobID) *jobspb.ChangefeedProgress {
		var cfProgress *jobspb.ChangefeedProgress
		err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			infoStorage := jobs.InfoStorageForJob(txn, jobID)
			progressBytes, exists, err := infoStorage.GetLegacyProgress(ctx, "test")
			if err != nil {
				return err
			}
			if !exists {
				return nil
			}
			var progress jobspb.Progress
			if err := protoutil.Unmarshal(progressBytes, &progress); err != nil {
				return err
			}
			cfProgress = progress.GetChangefeed()
			return nil
		})
		require.NoError(t, err)
		return cfProgress
	}

	readFrontier := func(t *testing.T, jobID jobspb.JobID) []jobspb.ResolvedSpan {
		var spans []jobspb.ResolvedSpan
		err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			var found bool
			var err error
			spans, found, err = jobfrontier.GetResolvedSpans(ctx, txn, jobID, "coordinator")
			if err != nil {
				return err
			}
			if !found {
				spans = nil
			}
			return nil
		})
		require.NoError(t, err)
		return spans
	}

	// Verify SLC is set before migration.
	for name, tc := range tests {
		cfProgress := readProgress(t, jobIDs[name])
		require.NotNil(t, cfProgress)
		if tc.slc != nil {
			require.False(t, cfProgress.SpanLevelCheckpoint.IsEmpty(),
				"pre-migration: %s should have SLC", name)
		}
	}

	// Run the upgrade.
	upgrades.Upgrade(t, s.SQLConn(t),
		clusterversion.V26_2_ChangefeedsStopUsingSpanLevelCheckpoint, nil, false)

	// Verify results.
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jobID := jobIDs[name]

			// SLC should always be nil after migration.
			cfProgress := readProgress(t, jobID)
			require.NotNil(t, cfProgress)
			require.True(t, cfProgress.SpanLevelCheckpoint.IsEmpty(),
				"post-migration: SLC should be nil")

			// Check frontier data.
			frontier := readFrontier(t, jobID)
			if tc.wantFrontier == nil {
				require.Nil(t, frontier)
			} else {
				require.ElementsMatch(t, tc.wantFrontier, frontier)
			}
		})
	}
}
