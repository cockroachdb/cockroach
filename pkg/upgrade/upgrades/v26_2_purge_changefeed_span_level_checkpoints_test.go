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

func TestPurgeChangefeedSpanLevelCheckpoints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t,
		clusterversion.V26_2_ChangefeedsNoLongerHaveSpanLevelCheckpoints)

	spanA := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}
	ts10 := hlc.Timestamp{WallTime: 10}

	testCases := map[string]struct {
		// spanLevelCheckpoint is the span-level checkpoint to set on the
		// job progress. nil means no span-level checkpoint.
		spanLevelCheckpoint *jobspb.TimestampSpansMap
	}{
		"with span-level checkpoint": {
			spanLevelCheckpoint: jobspb.NewTimestampSpansMap(
				map[hlc.Timestamp]roachpb.Spans{
					ts10: {spanA},
				}),
		},
		"without span-level checkpoint": {
			spanLevelCheckpoint: nil,
		},
	}

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         (clusterversion.V26_2_ChangefeedsNoLongerHaveSpanLevelCheckpoints - 1).Version(),
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	db := srv.InternalDB().(isql.DB)
	jr := srv.JobRegistry().(*jobs.Registry)

	// Create all test jobs before running the migration.
	jobIDs := make(map[string]jobspb.JobID)
	for name, tc := range testCases {
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			rec := jobs.Record{
				Details:  jobspb.ChangefeedDetails{},
				Progress: jobspb.ChangefeedProgress{SpanLevelCheckpoint: tc.spanLevelCheckpoint},
				Username: username.RootUserName(),
			}
			j, err := jr.CreateAdoptableJobWithTxn(ctx, rec, jr.MakeJobID(), txn)
			if err != nil {
				return err
			}
			jobIDs[name] = j.ID()
			return nil
		}))
	}

	// Run the purge migration.
	upgrades.Upgrade(t, srv.SQLConn(t),
		clusterversion.V26_2_ChangefeedsNoLongerHaveSpanLevelCheckpoints,
		nil, false)

	// Verify span-level checkpoints have been purged.
	for name := range testCases {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				progressBytes, exists, err := jobs.InfoStorageForJob(txn, jobIDs[name]).
					GetLegacyProgress(ctx, "verify-purge")
				if err != nil {
					return err
				}
				require.True(t, exists)
				var progress jobspb.Progress
				require.NoError(t, protoutil.Unmarshal(progressBytes, &progress))
				cfProgress := progress.GetChangefeed()
				require.NotNil(t, cfProgress)
				require.Nil(t, cfProgress.SpanLevelCheckpoint,
					"expected span-level checkpoint to be purged")
				return nil
			}))
		})
	}
}
