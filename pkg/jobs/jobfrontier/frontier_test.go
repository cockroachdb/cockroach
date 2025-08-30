// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobfrontier_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobfrontier"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/stretchr/testify/require"
)

// countEntries counts the number of entries in a frontier
func countEntries(frontier span.Frontier) int {
	count := 0
	for _ = range frontier.Entries() {
		count++
	}
	return count
}

// checkFrontierContainsSpan verifies that a frontier contains a specific span with the expected timestamp
func checkFrontierContainsSpan(t *testing.T, frontier span.Frontier, expectedSpan roachpb.Span, expectedTimestamp hlc.Timestamp) {
	found := false
	for sp, ts := range frontier.Entries() {
		if sp.Equal(expectedSpan) && ts.Equal(expectedTimestamp) {
			found = true
			break
		}
	}
	require.True(t, found, "frontier should contain span %s with timestamp %s", expectedSpan, expectedTimestamp)
}

func TestFrontier(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()
	const startID = 101
	sp := func(i int) roachpb.Span {
		return roachpb.Span{Key: s.Codec().TablePrefix(100 + uint32(i)), EndKey: s.Codec().TablePrefix(101 + uint32(i))}
	}
	sp1, sp2, sp3 := sp(0), sp(1), sp(2)

	ts1, ts2 := hlc.Timestamp{WallTime: 100, Logical: 1}, hlc.Timestamp{WallTime: 200, Logical: 2}

	tests := []struct {
		name  string
		setup func(t *testing.T, txn isql.Txn, jobID jobspb.JobID, f span.Frontier)
		check func(t *testing.T, txn isql.Txn, jobID jobspb.JobID)
	}{
		{
			name: "basic store and get",
			setup: func(t *testing.T, txn isql.Txn, jobID jobspb.JobID, f span.Frontier) {
				require.NoError(t, jobfrontier.Store(ctx, txn, jobID, "basic", nil, f))
			},
			check: func(t *testing.T, txn isql.Txn, jobID jobspb.JobID) {
				loadedFrontier, found, err := jobfrontier.Get(ctx, txn, jobID, "basic")
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, 3, countEntries(loadedFrontier))
				checkFrontierContainsSpan(t, loadedFrontier, sp1, ts1)
				checkFrontierContainsSpan(t, loadedFrontier, sp3, ts1)
			},
		},
		{
			name:  "non-existent frontier",
			setup: func(t *testing.T, txn isql.Txn, jobID jobspb.JobID, f span.Frontier) {},
			check: func(t *testing.T, txn isql.Txn, jobID jobspb.JobID) {
				_, found, err := jobfrontier.Get(ctx, txn, jobID, "nonexistent")
				require.NoError(t, err)
				require.False(t, found)
			},
		},
		{
			name: "sharded-updates",
			setup: func(t *testing.T, txn isql.Txn, jobID jobspb.JobID, f span.Frontier) {
				shard := func(sp roachpb.Span) string { return fmt.Sprintf("%v", sp.Key) }
				// Store initial frontier.
				require.NoError(t, jobfrontier.Store(ctx, txn, jobID, "multi", shard, f))
				// Advance and store again.
				_, err := f.Forward(sp2, ts2)
				require.NoError(t, err)
				require.NoError(t, jobfrontier.Store(ctx, txn, jobID, "multi", shard, f))
			},
			check: func(t *testing.T, txn isql.Txn, jobID jobspb.JobID) {
				loadedFrontier, found, err := jobfrontier.Get(ctx, txn, jobID, "multi")
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, 3, countEntries(loadedFrontier))
				checkFrontierContainsSpan(t, loadedFrontier, sp1, ts1)
				checkFrontierContainsSpan(t, loadedFrontier, sp2, ts2)
				checkFrontierContainsSpan(t, loadedFrontier, sp3, ts1)
			},
		},
		{
			name: "delete",
			setup: func(t *testing.T, txn isql.Txn, jobID jobspb.JobID, f span.Frontier) {
				require.NoError(t, jobfrontier.Store(ctx, txn, jobID, "deleteme", nil, f))
				require.NoError(t, jobfrontier.Delete(ctx, txn, jobID, "deleteme"))
			},
			check: func(t *testing.T, txn isql.Txn, jobID jobspb.JobID) {
				_, found, err := jobfrontier.Get(ctx, txn, jobID, "deleteme")
				require.NoError(t, err)
				require.False(t, found)
			},
		},
		{
			name: "overwrite-resharded",
			setup: func(t *testing.T, txn isql.Txn, jobID jobspb.JobID, f span.Frontier) {
				reshard := func(_ roachpb.Span) string { return "x" }
				require.NoError(t, jobfrontier.Store(ctx, txn, jobID, "overwrite", reshard, f))
				// Create frontier with just sp1@ts2 and overwrite
				sp2ts2, err := span.MakeFrontierAt(ts2, sp2)
				require.NoError(t, err)
				require.NoError(t, jobfrontier.Store(ctx, txn, jobID, "overwrite", reshard, sp2ts2))
			},
			check: func(t *testing.T, txn isql.Txn, jobID jobspb.JobID) {
				loadedFrontier, found, err := jobfrontier.Get(ctx, txn, jobID, "overwrite")
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, 1, countEntries(loadedFrontier))
				checkFrontierContainsSpan(t, loadedFrontier, sp2, ts2)
			},
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			jobID := jobspb.JobID(1000 + i)
			var f span.Frontier
			f, err := span.MakeFrontier(sp1, sp2, sp3)
			require.NoError(t, err)
			_, err = f.Forward(sp1, ts1)
			require.NoError(t, err)
			_, err = f.Forward(sp3, ts1)
			require.NoError(t, err)

			require.NoError(t, s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				tc.setup(t, txn, jobID, f)
				return nil
			}))
			require.NoError(t, s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				tc.check(t, txn, jobID)
				return nil
			}))
		})
	}
}
