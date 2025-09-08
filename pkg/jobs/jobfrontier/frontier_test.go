// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobfrontier

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
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
	for range frontier.Entries() {
		count++
	}
	return count
}

// checkFrontierContainsSpan verifies that a frontier contains a specific span with the expected timestamp
func checkFrontierContainsSpan(
	t *testing.T, frontier span.Frontier, expectedSpan roachpb.Span, expectedTimestamp hlc.Timestamp,
) {
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
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()
	sp := func(i int) roachpb.Span {
		return roachpb.Span{
			Key:    s.Codec().TablePrefix(100 + uint32(i)),
			EndKey: s.Codec().TablePrefix(101 + uint32(i)),
		}
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
				require.NoError(t, Store(ctx, txn, jobID, "basic", f))
			},
			check: func(t *testing.T, txn isql.Txn, jobID jobspb.JobID) {
				loadedFrontier, found, err := Get(ctx, txn, jobID, "basic")
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, 3, countEntries(loadedFrontier))
				checkFrontierContainsSpan(t, loadedFrontier, sp1, ts1)
				checkFrontierContainsSpan(t, loadedFrontier, sp3, ts1)

				spans, found, err := GetResolvedSpans(ctx, txn, jobID, "basic")
				require.NoError(t, err)
				require.True(t, found)
				require.ElementsMatch(t,
					[]jobspb.ResolvedSpan{
						{Span: sp1, Timestamp: ts1},
						{Span: sp2},
						{Span: sp3, Timestamp: ts1},
					},
					spans)
			},
		},
		{
			name:  "non-existent frontier",
			setup: func(t *testing.T, txn isql.Txn, jobID jobspb.JobID, f span.Frontier) {},
			check: func(t *testing.T, txn isql.Txn, jobID jobspb.JobID) {
				_, found, err := Get(ctx, txn, jobID, "nonexistent")
				require.NoError(t, err)
				require.False(t, found)

				_, found, err = GetResolvedSpans(ctx, txn, jobID, "nonexistent")
				require.NoError(t, err)
				require.False(t, found)
			},
		},
		{
			name: "sharded-updates",
			setup: func(t *testing.T, txn isql.Txn, jobID jobspb.JobID, f span.Frontier) {
				// Store initial frontier.
				require.NoError(t, Store(ctx, txn, jobID, "multi", f))
				// Advance and store again.
				_, err := f.Forward(sp2, ts2)
				require.NoError(t, err)
				require.NoError(t, Store(ctx, txn, jobID, "multi", f))
			},
			check: func(t *testing.T, txn isql.Txn, jobID jobspb.JobID) {
				loadedFrontier, found, err := Get(ctx, txn, jobID, "multi")
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, 3, countEntries(loadedFrontier))
				checkFrontierContainsSpan(t, loadedFrontier, sp1, ts1)
				checkFrontierContainsSpan(t, loadedFrontier, sp2, ts2)
				checkFrontierContainsSpan(t, loadedFrontier, sp3, ts1)

				spans, found, err := GetResolvedSpans(ctx, txn, jobID, "multi")
				require.NoError(t, err)
				require.True(t, found)
				require.ElementsMatch(t,
					[]jobspb.ResolvedSpan{
						{Span: sp1, Timestamp: ts1},
						{Span: sp2, Timestamp: ts2},
						{Span: sp3, Timestamp: ts1},
					},
					spans)
			},
		},
		{
			name: "delete",
			setup: func(t *testing.T, txn isql.Txn, jobID jobspb.JobID, f span.Frontier) {
				require.NoError(t, Store(ctx, txn, jobID, "deleteme", f))
				require.NoError(t, Delete(ctx, txn, jobID, "deleteme"))
			},
			check: func(t *testing.T, txn isql.Txn, jobID jobspb.JobID) {
				_, found, err := Get(ctx, txn, jobID, "deleteme")
				require.NoError(t, err)
				require.False(t, found)

				_, found, err = GetResolvedSpans(ctx, txn, jobID, "deleteme")
				require.NoError(t, err)
				require.False(t, found)
			},
		},
		{
			name: "overwrite-resharded",
			setup: func(t *testing.T, txn isql.Txn, jobID jobspb.JobID, f span.Frontier) {
				require.NoError(t, Store(ctx, txn, jobID, "overwrite", f))
				// Create frontier with just sp1@ts2 and overwrite
				sp2ts2, err := span.MakeFrontierAt(ts2, sp2)
				require.NoError(t, err)
				require.NoError(t, Store(ctx, txn, jobID, "overwrite", sp2ts2))
			},
			check: func(t *testing.T, txn isql.Txn, jobID jobspb.JobID) {
				loadedFrontier, found, err := Get(ctx, txn, jobID, "overwrite")
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, 1, countEntries(loadedFrontier))
				checkFrontierContainsSpan(t, loadedFrontier, sp2, ts2)

				spans, found, err := GetResolvedSpans(ctx, txn, jobID, "overwrite")
				require.NoError(t, err)
				require.True(t, found)
				require.ElementsMatch(t,
					[]jobspb.ResolvedSpan{
						{Span: sp2, Timestamp: ts2},
					},
					spans)
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

func TestStoreChunked(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()

	makeSpan := func(i int) roachpb.Span {
		// Create spans with keys of predictable sizes
		// Each span will have key and endkey of ~10 bytes each
		key := []byte{byte(i)}
		endKey := append(key, 0xFF)
		return roachpb.Span{Key: key, EndKey: endKey}
	}

	// Calculate the size contribution of a single span entry
	// From the code: len(sp.Span.Key) + len(sp.Span.EndKey) + 16
	spanSize := 3 + 16 // 2 bytes for our test spans + 16 for timestamp/overhead

	tests := []struct {
		name      string
		numSpans  int
		chunkSize int
	}{
		{
			name:      "exact-multiple",
			numSpans:  6,
			chunkSize: spanSize * 2, // Exactly 2 spans per chunk, tests exact boundary
		},
		{
			name:      "one-less-boundary",
			numSpans:  5,
			chunkSize: spanSize*2 - 1, // Just under 2 spans, forces 1 per chunk
		},
		{
			name:      "one-more-boundary",
			numSpans:  7,
			chunkSize: spanSize*3 + 1, // Just over 3 spans, tests spillover
		},
		{
			name:      "single-span-chunks",
			numSpans:  3,
			chunkSize: spanSize, // Minimum useful chunk size
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			jobID := jobspb.JobID(2000 + i)

			// Create frontier with specified number of spans
			spans := make([]roachpb.Span, tc.numSpans)
			for j := 0; j < tc.numSpans; j++ {
				spans[j] = makeSpan(j)
			}

			frontier, err := span.MakeFrontier(spans...)
			require.NoError(t, err)

			// Advance each span to a unique timestamp
			for j, sp := range spans {
				ts := hlc.Timestamp{WallTime: int64(100 + j), Logical: int32(j)}
				_, err := frontier.Forward(sp, ts)
				require.NoError(t, err)
			}

			// Store using storeChunked with specified chunk size
			require.NoError(t, s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				return storeChunked(ctx, txn, jobID, tc.name, frontier, tc.chunkSize)
			}))

			// Verify the frontier can be loaded correctly
			require.NoError(t, s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				loadedFrontier, found, err := Get(ctx, txn, jobID, tc.name)
				require.NoError(t, err)
				require.True(t, found, "frontier should be found")
				require.Equal(t, tc.numSpans, loadedFrontier.Len(), "loaded frontier should have correct number of spans")

				// Verify each span and its timestamp
				for j, sp := range spans {
					expectedTs := hlc.Timestamp{WallTime: int64(100 + j), Logical: int32(j)}
					found := false
					for frontierSpan, ts := range loadedFrontier.Entries() {
						if frontierSpan.Equal(sp) {
							require.Equal(t, expectedTs, ts, "span %d should have correct timestamp", j)
							found = true
							break
						}
					}
					require.True(t, found, "span %d should be found in loaded frontier", j)
				}
				return nil
			}))
		})
	}
}
