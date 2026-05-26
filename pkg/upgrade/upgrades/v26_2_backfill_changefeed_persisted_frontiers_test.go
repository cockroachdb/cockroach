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
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/stretchr/testify/require"
)

func TestBackfillChangefeedPersistedFrontiers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t,
		clusterversion.V26_2_ChangefeedsStopReadingSpanLevelCheckpoints)

	spanA := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}
	spanB := roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}
	ts10 := hlc.Timestamp{WallTime: 10}
	ts20 := hlc.Timestamp{WallTime: 20}
	ts30 := hlc.Timestamp{WallTime: 30}

	type testCase struct {
		// spanLevelCheckpoint is the span-level checkpoint to set on the
		// job progress. nil means no span-level checkpoint.
		spanLevelCheckpoint *jobspb.TimestampSpansMap
		// existingFrontier, if non-nil, is seeded as a pre-existing
		// persisted frontier before the migration runs.
		existingFrontier []jobspb.ResolvedSpan
		// expectedSpans are the expected resolved spans in the persisted
		// frontier after the migration. nil means no frontier expected.
		expectedSpans []jobspb.ResolvedSpan
	}
	testCases := map[string]testCase{
		"no span-level checkpoint": {
			spanLevelCheckpoint: nil,
			expectedSpans:       nil,
		},
		"span-level checkpoint without existing frontier": {
			spanLevelCheckpoint: jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
				ts10: {spanA},
				ts20: {spanB},
			}),
			expectedSpans: []jobspb.ResolvedSpan{
				{Span: spanA, Timestamp: ts10},
				{Span: spanB, Timestamp: ts20},
			},
		},
		"span-level checkpoint ahead of existing frontier": {
			spanLevelCheckpoint: jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
				ts30: {spanA},
			}),
			existingFrontier: []jobspb.ResolvedSpan{
				{Span: spanA, Timestamp: ts10},
			},
			expectedSpans: []jobspb.ResolvedSpan{
				{Span: spanA, Timestamp: ts30}, // forwarded
			},
		},
		"span-level checkpoint at existing frontier": {
			spanLevelCheckpoint: jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
				ts20: {spanA},
			}),
			existingFrontier: []jobspb.ResolvedSpan{
				{Span: spanA, Timestamp: ts20},
			},
			expectedSpans: []jobspb.ResolvedSpan{
				{Span: spanA, Timestamp: ts20}, // unchanged
			},
		},
		"span-level checkpoint behind existing frontier": {
			spanLevelCheckpoint: jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
				ts10: {spanA},
			}),
			existingFrontier: []jobspb.ResolvedSpan{
				{Span: spanA, Timestamp: ts30},
			},
			expectedSpans: []jobspb.ResolvedSpan{
				{Span: spanA, Timestamp: ts30}, // not regressed
			},
		},
	}

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         (clusterversion.V26_2_ChangefeedsStopReadingSpanLevelCheckpoints - 1).Version(),
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
			// Seed an existing frontier if specified.
			if tc.existingFrontier != nil {
				frontier, err := span.MakeFrontier()
				if err != nil {
					return err
				}
				defer frontier.Release()
				for _, rs := range tc.existingFrontier {
					if err := frontier.AddSpansAt(rs.Timestamp, rs.Span); err != nil {
						return err
					}
				}
				return jobfrontier.Store(ctx, txn, jobIDs[name], "coordinator", frontier)
			}
			return nil
		}))
	}

	// Run the migration.
	upgrades.Upgrade(t, srv.SQLConn(t),
		clusterversion.V26_2_ChangefeedsStopReadingSpanLevelCheckpoints,
		nil, false)

	// Verify results.
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				spans, found, err := jobfrontier.GetAllResolvedSpans(ctx, txn, jobIDs[name])
				if err != nil {
					return err
				}
				if tc.expectedSpans == nil {
					require.False(t, found, "expected no persisted frontier")
					return nil
				}
				require.True(t, found, "expected persisted frontier to exist")

				got := make(map[string]hlc.Timestamp)
				for _, rs := range spans {
					got[rs.Span.String()] = rs.Timestamp
				}
				expected := make(map[string]hlc.Timestamp)
				for _, rs := range tc.expectedSpans {
					expected[rs.Span.String()] = rs.Timestamp
				}
				require.Equal(t, expected, got)
				return nil
			}))
		})
	}
}
