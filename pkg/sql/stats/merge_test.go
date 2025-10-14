// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"context"
	"math"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// TestMergePartialStatistics calls mergePartialStatistic with various observed
// stats, that include full statistics and partial statistics.
func TestMergePartialStatistics(t *testing.T) {
	testCases := []struct {
		initial  *testStat
		partial  *testStat
		expected *testStat
		err      bool
	}{
		{
			// Partial stat with a single bucket at both extremes of the full stat.
			initial: &testStat{
				at: 1, row: 15, dist: 10, null: 4, size: 1,
				hist: testHistogram{{1, 0, 0, 30}, {1, 9, 7, 40}},
			},
			partial: &testStat{
				at: 1, row: 19, dist: 10, null: 0, size: 1, atExtremes: true,
				hist: testHistogram{
					{3, 2, 0, 20}, {3, 11, 8, 50}},
			},
			expected: &testStat{
				at: 1, row: 30, dist: 19, null: 0, size: 1,
				hist: testHistogram{
					{3, 2, 0, 20},
					{1, 0, 0, 30},
					{1, 9, 7, 40},
					{3, 11, 8, 50},
				},
			},
		},
		// Partial stats with multiple buckets at both extremes of the full stat.
		{
			initial: &testStat{
				at: 1, row: 5, dist: 5, null: 0, size: 2,
				hist: testHistogram{
					{1, 0, 0, 2},
					{1, 1, 1, 4},
					{1, 1, 1, 6},
				},
			},
			partial: &testStat{
				at: 2, row: 8, dist: 4, null: 0, size: 2, atExtremes: true,
				hist: testHistogram{
					{2, 0, 0, 0},
					{2, 0, 0, 1},
					{2, 0, 0, 7},
					{2, 0, 0, 8},
				},
			},
			expected: &testStat{
				at: 2, row: 13, dist: 9, null: 0, size: 2,
				hist: testHistogram{
					{2, 0, 0, 0},
					{2, 0, 0, 1},
					{1, 0, 0, 2},
					{1, 1, 1, 4},
					{1, 1, 1, 6},
					{2, 0, 0, 7},
					{2, 0, 0, 8},
				},
			},
		},
		{
			initial: &testStat{
				at: 2, row: 37, dist: 22, null: 0, size: 6,
				hist: testHistogram{
					{1, 0, 0, 9},
					{5, 5, 5, 11},
					{6, 6, 6, 15},
					{7, 7, 7, 18},
				},
			},
			partial: &testStat{
				at: 5, row: 48, dist: 26, null: 0, size: 2, atExtremes: true,
				hist: testHistogram{
					{3, 3, 2, 0},
					{3, 3, 2, 5},
					{3, 3, 2, 8},
					{7, 7, 7, 21},
					{8, 8, 8, 42},
				},
			},
			expected: &testStat{
				at: 5, row: 85, dist: 48, null: 0, size: 3,
				hist: testHistogram{
					{3, 3, 2, 0},
					{3, 3, 2, 5},
					{3, 3, 2, 8},
					{1, 0, 0, 9},
					{5, 5, 5, 11},
					{6, 6, 6, 15},
					{7, 7, 7, 18},
					{7, 7, 7, 21},
					{8, 8, 8, 42},
				},
			},
		},
		// Partial stats strictly before the full stat.
		{
			initial: &testStat{
				at: 2, row: 25, dist: 13, null: 2, size: 3,
				hist: testHistogram{
					{3, 3, 2, 5},
					{3, 3, 2, 8},
					{3, 3, 2, 12},
					{3, 2, 2, 15},
				},
			},
			partial: &testStat{
				at: 3, row: 6, dist: 4, null: 3, size: 5,
				hist: testHistogram{
					{1, 0, 0, 2},
					{1, 0, 0, 3},
					{1, 0, 0, 4},
				},
			},
			expected: &testStat{
				at: 3, row: 29, dist: 16, null: 3, size: 3,
				hist: testHistogram{
					{1, 0, 0, 2},
					{1, 0, 0, 3},
					{1, 0, 0, 4},
					{3, 3, 2, 5},
					{3, 3, 2, 8},
					{3, 3, 2, 12},
					{3, 2, 2, 15},
				},
			},
		},
		{
			initial: &testStat{
				at: 1, row: 25, dist: 19, null: 0, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{2, 9, 7, 20},
					{3, 4, 4, 30},
					{2, 4, 4, 40},
				},
			},
			partial: &testStat{
				at: 1, row: 11, dist: 6, null: 0, size: 1,
				hist: testHistogram{
					{1, 0, 0, 2},
					{2, 3, 1, 5},
					{3, 2, 2, 8},
				},
			},
			expected: &testStat{
				at: 1, row: 36, dist: 25, null: 0, size: 1,
				hist: testHistogram{
					{1, 0, 0, 2},
					{2, 3, 1, 5},
					{3, 2, 2, 8},
					{1, 0, 0, 10},
					{2, 9, 7, 20},
					{3, 4, 4, 30},
					{2, 4, 4, 40},
				},
			},
		},
		// Partial stats strictly after the full stat.
		{
			initial: &testStat{
				at: 2, row: 19, dist: 10, null: 0, size: 3,
				hist: testHistogram{
					{1, 0, 0, 0},
					{3, 3, 2, 5},
					{3, 3, 2, 8},
					{3, 3, 2, 15},
				},
			},
			partial: &testStat{
				at: 2, row: 18, dist: 11, null: 0, size: 8,
				hist: testHistogram{
					{4, 4, 4, 19},
					{5, 5, 5, 21},
				},
			},
			expected: &testStat{
				at: 2, row: 37, dist: 21, null: 0, size: 5,
				hist: testHistogram{
					{1, 0, 0, 0},
					{3, 3, 2, 5},
					{3, 3, 2, 8},
					{3, 3, 2, 15},
					{4, 4, 4, 19},
					{5, 5, 5, 21},
				},
			},
		},
		{
			initial: &testStat{
				at: 1, row: 25, dist: 19, null: 0, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{2, 9, 7, 20},
					{3, 4, 4, 30},
					{2, 4, 4, 40},
				},
			},
			partial: &testStat{
				at: 1, row: 11, dist: 6, null: 0, size: 1,
				hist: testHistogram{
					{1, 0, 0, 42},
					{2, 3, 1, 45},
					{3, 2, 2, 48},
				},
			},
			expected: &testStat{
				at: 1, row: 36, dist: 25, null: 0, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{2, 9, 7, 20},
					{3, 4, 4, 30},
					{2, 4, 4, 40},
					{1, 0, 0, 42},
					{2, 3, 1, 45},
					{3, 2, 2, 48},
				},
			},
		},
		// Partial stat fully contained within full stat, with aligned buckets.
		{
			initial: &testStat{
				at: 1, row: 25, dist: 19, null: 0, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{2, 9, 7, 20},
					{3, 4, 4, 30},
					{2, 4, 4, 40},
				},
			},
			partial: &testStat{
				at: 1, row: 29, dist: 14, null: 0, size: 1,
				hist: testHistogram{
					{3, 0, 0, 20},
					{2, 4, 3, 25},
					{2, 5, 3, 30},
					{3, 10, 4, 35},
				},
			},
			expected: &testStat{
				at: 1, row: 43, dist: 25, null: 0, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{3, 9, 7, 20},
					{2, 11, 7, 30},
					{2, 15, 7, 40},
				},
			},
		},
		// Partial stat starts before full stat and ends within full stat, with
		// aligned buckets.
		{
			initial: &testStat{
				at: 1, row: 25, dist: 19, null: 0, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{2, 9, 7, 20},
					{3, 4, 4, 30},
					{2, 4, 4, 40},
				},
			},
			partial: &testStat{
				at: 1, row: 40, dist: 17, null: 0, size: 1,
				hist: testHistogram{
					{4, 0, 0, 5},
					{2, 3, 2, 10},
					{2, 10, 4, 15},
					{5, 6, 4, 20},
					{3, 5, 2, 25},
				},
			},
			expected: &testStat{
				at: 1, row: 51, dist: 25, null: 0, size: 1,
				hist: testHistogram{
					{4, 0, 0, 5},
					{2, 3, 2, 10},
					{5, 18, 9, 20},
					{3, 10, 5, 30},
					{2, 4, 4, 40},
				},
			},
		},
		// Partial stat starts before full stat and ends within full stat, with
		// misaligned buckets.
		{
			initial: &testStat{
				at: 1, row: 25, dist: 19, null: 0, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{2, 9, 7, 20},
					{3, 4, 4, 30},
					{2, 4, 4, 40},
				},
			},
			partial: &testStat{
				at: 1, row: 36, dist: 13, null: 0, size: 1,
				hist: testHistogram{
					{2, 0, 0, 5},
					{2, 6, 2, 15},
					{8, 4, 4, 25},
					{6, 8, 3, 30},
				},
			},
			expected: &testStat{
				at: 1, row: 45, dist: 20, null: 0, size: 1,
				hist: testHistogram{
					{2, 0, 0, 5},
					{1, 3, 1, 10},
					{2, 7, 4, 20},
					{6, 18, 6, 30},
					{2, 4, 4, 40},
				},
			},
		},
		// Partial stat starts within full stat and ends after full stat, with
		// misaligned buckets.
		{
			initial: &testStat{
				at: 1, row: 25, dist: 19, null: 0, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{2, 9, 7, 20},
					{3, 4, 4, 30},
					{2, 4, 4, 40},
				},
			},
			partial: &testStat{
				at: 1, row: 36, dist: 13, null: 0, size: 1,
				hist: testHistogram{
					{3, 0, 0, 25},
					{8, 6, 2, 35},
					{6, 8, 6, 45},
					{2, 3, 1, 55},
				},
			},
			expected: &testStat{
				at: 1, row: 55, dist: 26, null: 0, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{2, 9, 7, 20},
					{3, 8, 4, 30},
					{2, 15, 5, 40},
					{6, 4, 3, 45},
					{2, 3, 1, 55},
				},
			},
		},
		// Partial stat bucket overlapping multiple full stat buckets.
		{
			initial: &testStat{
				at: 1, row: 25, dist: 19, null: 0, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{2, 9, 7, 20},
					{3, 4, 4, 30},
					{2, 4, 4, 40},
				},
			},
			partial: &testStat{
				at: 1, row: 33, dist: 19, null: 0, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{4, 4, 3, 15},
					{2, 16, 8, 35},
					{2, 4, 4, 40},
				},
			},
			expected: &testStat{
				at: 1, row: 38, dist: 21, null: 0, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{2, 12, 6, 20},
					{3, 8, 4, 30},
					{2, 10, 7, 40},
				},
			},
		},
		// Edge cases
		{
			initial: &testStat{
				at: 1, row: 25, dist: 19, null: 0, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{2, 9, 7, 20},
					{3, 4, 4, 30},
					{2, 4, 4, 40},
				},
			},
			partial: &testStat{
				at: 1, row: 2, dist: 1, null: 0, size: 1,
				hist: testHistogram{
					{2, 0, 0, 10},
				},
			},
			expected: &testStat{
				at: 1, row: 26, dist: 19, null: 0, size: 1,
				hist: testHistogram{
					{2, 0, 0, 10},
					{2, 9, 7, 20},
					{3, 4, 4, 30},
					{2, 4, 4, 40},
				},
			},
		},
		{
			initial: &testStat{
				at: 1, row: 25, dist: 19, null: 0, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{2, 9, 7, 20},
					{3, 4, 4, 30},
					{2, 4, 4, 40},
				},
			},
			partial: &testStat{
				at: 1, row: 1, dist: 1, null: 0, size: 1,
				hist: testHistogram{
					{1, 0, 0, 40},
				},
			},
			expected: &testStat{
				at: 1, row: 24, dist: 19, null: 0, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{2, 9, 7, 20},
					{3, 4, 4, 30},
					{1, 4, 4, 40},
				},
			},
		},
		// Full stat nulls are retained when partial stat has no nulls.
		{
			initial: &testStat{
				at: 1, row: 7, dist: 4, null: 4, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{1, 1, 1, 20},
				},
			},
			partial: &testStat{
				at: 1, row: 6, dist: 4, null: 0, size: 1,
				hist: testHistogram{
					{2, 0, 0, 5},
					{2, 2, 2, 10},
				},
			},
			expected: &testStat{
				at: 1, row: 12, dist: 7, null: 4, size: 1,
				hist: testHistogram{
					{2, 0, 0, 5},
					{2, 2, 2, 10},
					{1, 1, 1, 20},
				},
			},
		},
		// Full stat nulls are replaced when partial stat has nulls.
		{
			initial: &testStat{
				at: 1, row: 7, dist: 4, null: 4, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{1, 1, 1, 20},
				},
			},
			partial: &testStat{
				at: 1, row: 14, dist: 5, null: 8, size: 1,
				hist: testHistogram{
					{2, 0, 0, 5},
					{2, 2, 2, 10},
				},
			},
			expected: &testStat{
				at: 1, row: 16, dist: 7, null: 8, size: 1,
				hist: testHistogram{
					{2, 0, 0, 5},
					{2, 2, 2, 10},
					{1, 1, 1, 20},
				},
			},
		},
		// Partial stats with only nulls.
		{
			initial: &testStat{
				at: 1, row: 7, dist: 4, null: 4, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{1, 1, 1, 20},
				},
			},
			partial: &testStat{
				at: 1, row: 8, dist: 1, null: 8, size: 1, atExtremes: true,
				hist: testHistogram{},
			},
			expected: &testStat{
				at: 1, row: 11, dist: 4, null: 8, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{1, 1, 1, 20},
				},
			},
		},
		{
			initial: &testStat{
				at: 1, row: 7, dist: 4, null: 4, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{1, 1, 1, 20},
				},
			},
			partial: &testStat{
				at: 1, row: 8, dist: 1, null: 8, size: 1,
				hist: testHistogram{},
			},
			expected: &testStat{
				at: 1, row: 11, dist: 4, null: 8, size: 1,
				hist: testHistogram{
					{1, 0, 0, 10},
					{1, 1, 1, 20},
				},
			},
		},
		// Error cases:
		// Full stat with no histogram
		{
			initial: &testStat{
				at: 5, row: 0, dist: 0, null: 0, size: 0, colID: 1,
				hist: testHistogram{},
			},
			partial: &testStat{
				at: 6, row: 8, dist: 6, null: 0, size: 1, colID: 1,
				hist: testHistogram{{1, 0, 0, 10}, {1, 6, 4, 20}},
			},
			err: true,
		},
		// Full stat with only NULLs
		{
			initial: &testStat{
				at: 5, row: 10, dist: 1, null: 10, size: 0, colID: 1,
				hist: testHistogram{},
			},
			partial: &testStat{
				at: 6, row: 8, dist: 6, null: 0, size: 1, colID: 1,
				hist: testHistogram{{1, 0, 0, 10}, {1, 6, 4, 20}},
			},
			err: true,
		},
	}
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.NewTestingEvalContext(st)
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			initial := tc.initial.toTableStatistic(ctx, "stat", i, descpb.ColumnIDs{1}, 1 /* statID */, 0 /* fullStatID */, st)
			partial := tc.partial.toTableStatistic(ctx, "stat", i, descpb.ColumnIDs{1}, 0 /* statID */, 1 /* fullStatID */, st)
			expected := tc.expected.toTableStatistic(ctx, "__merged__", i, descpb.ColumnIDs{1}, 0 /* statID */, 0 /* fullStatID */, st)
			var extremesBound tree.Datum
			if tc.partial.atExtremes {
				extremesBound = initial.nonNullHistogram().buckets[0].UpperBound
			}
			merged, err := mergePartialStatistic(ctx, evalCtx, initial, partial, st,
				tc.partial.atExtremes, extremesBound)
			if err != nil {
				if !tc.err {
					t.Errorf("test case %d unexpected mergePartialStatistic err: %v", i, err)
				}
				return
			}
			if tc.err {
				t.Errorf("test case %d expected mergePartialStatistic err, was:\n%s", i, merged)
				return
			}
			// Round distinct ranges for easier comparison.
			merged.RoundDistinctRanges()
			if !reflect.DeepEqual(merged, expected) {
				t.Errorf("test case %d incorrect merge\n%s\nexpected\n%s", i, merged, expected)
			}
		})
	}
}

// TestMergedStatistics tests MergedStatistics which merges an array of full
// statistics with all newer partial statistics for the same column.
func TestMergedStatistics(t *testing.T) {
	// Array of one or more full statistics and one or more partial
	// statistics for different column sets.
	testCases := []struct {
		full     []*testStat
		partial  []*testStat
		expected []*testStat
	}{
		{
			// Simplest case, one newer partial stat for each full stat.
			full: []*testStat{
				{
					at: 5, row: 18, dist: 13, null: 4, size: 1, colID: 1,
					hist: testHistogram{{1, 3, 3, 30}, {1, 9, 7, 40}},
				},
				{
					at: 5, row: 8, dist: 6, null: 0, size: 1, colID: 2,
					hist: testHistogram{{1, 0, 0, 30}, {1, 6, 4, 40}},
				},
				{
					at: 5, row: 5, dist: 3, null: 0, size: 1, colID: 3,
					hist: testHistogram{{1, 0, 0, 30}, {1, 3, 1, 40}},
				},
				{
					at: 5, row: 5, dist: 3, null: 0, size: 1, colID: 4,
					hist: testHistogram{{1, 0, 0, 30}, {1, 3, 1, 40}},
				},
			},
			partial: []*testStat{
				{
					at: 6, row: 14, dist: 12, null: 0, size: 1, colID: 1, atExtremes: true,
					hist: testHistogram{{1, 3, 3, 50}, {1, 9, 7, 60}},
				},
				{
					at: 6, row: 8, dist: 6, null: 0, size: 1, colID: 2, atExtremes: true,
					hist: testHistogram{{1, 0, 0, 10}, {1, 6, 4, 20}},
				},
				{
					at: 6, row: 5, dist: 3, null: 0, size: 1, colID: 3, atExtremes: true,
					hist: testHistogram{{1, 0, 0, 10}, {1, 3, 1, 50}},
				},
				{
					at: 6, row: 7, dist: 5, null: 0, size: 1, colID: 4, atExtremes: true,
					hist: testHistogram{
						{1, 0, 0, 0},
						{1, 0, 0, 10},
						{1, 3, 1, 50},
						{1, 0, 0, 60},
					},
				},
			},
			expected: []*testStat{
				{
					at: 6, row: 28, dist: 24, null: 0, size: 1, colID: 1,
					hist: testHistogram{
						{1, 3, 3, 30.0},
						{1, 9, 7, 40.0},
						{1, 3, 3, 50},
						{1, 9, 7, 60},
					},
				},
				{
					at: 6, row: 16, dist: 12, null: 0, size: 1, colID: 2,
					hist: testHistogram{
						{1, 0, 0, 10},
						{1, 6, 4, 20},
						{1, 0, 0, 30},
						{1, 6, 4, 40},
					},
				},
				{
					at: 6, row: 10, dist: 6, null: 0, size: 1, colID: 3,
					hist: testHistogram{
						{1, 0, 0, 10.0},
						{1, 0, 0, 30},
						{1, 3, 1, 40},
						{1, 3, 1, 50.0},
					},
				},
				{
					at: 6, row: 12, dist: 8, null: 0, size: 1, colID: 4,
					hist: testHistogram{
						{1, 0, 0, 0.0},
						{1, 0, 0, 10.0},
						{1, 0, 0, 30},
						{1, 3, 1, 40},
						{1, 3, 1, 50},
						{1, 0, 0, 60},
					},
				},
			},
		},
		{
			// Multiple newer partial stats at extremes for each full stat.
			full: []*testStat{
				{
					at: 5, row: 15, dist: 10, null: 4, size: 1, colID: 1,
					hist: testHistogram{
						{1, 0, 0, 30},
						{1, 9, 7, 40},
					},
				},
				{
					at: 5, row: 15, dist: 10, null: 4, size: 1, colID: 2,
					hist: testHistogram{
						{1, 0, 0, 20},
						{1, 9, 7, 30},
					},
				},
			},
			partial: []*testStat{
				{
					at: 7, row: 15, dist: 10, null: 4, size: 1, colID: 1,
					hist: testHistogram{
						{1, 0, 0, 50},
						{1, 9, 7, 60},
					},
				},
				{
					at: 8, row: 26, dist: 19, null: 4, size: 1, colID: 1,
					hist: testHistogram{
						{2, 0, 0, 50},
						{1, 9, 8, 60},
						{1, 9, 7, 70},
					},
				},
				{
					at: 6, row: 5, dist: 2, null: 4, size: 1, colID: 2,
					hist: testHistogram{{1, 0, 0, 50}},
				},
				{
					at: 7, row: 6, dist: 3, null: 4, size: 1, colID: 2,
					hist: testHistogram{
						{1, 0, 0, 40},
						{1, 0, 0, 50},
					},
				},
			},
			expected: []*testStat{
				{
					at: 8, row: 37, dist: 28, null: 4, size: 1, colID: 1,
					hist: testHistogram{
						{1, 0, 0, 30.0},
						{1, 9, 7, 40.0},
						{2, 0, 0, 50.0},
						{1, 9, 8, 60.0},
						{1, 9, 7, 70.0},
					},
				},
				{
					at: 7, row: 17, dist: 12, null: 4, size: 1, colID: 2,
					hist: testHistogram{{1, 0, 0, 20.0},
						{1, 9, 7, 30.0},
						{1, 1, 1, 50.0},
					},
				},
			},
		},
		{
			// Multiple newer arbitrary partial stats after a full stat.
			full: []*testStat{
				{
					at: 5, row: 17, dist: 10, null: 4, size: 1, colID: 1,
					hist: testHistogram{
						{1, 0, 0, 30},
						{1, 6, 4, 40},
						{1, 4, 2, 50},
					},
				},
			},
			partial: []*testStat{
				{
					at: 6, row: 15, dist: 10, null: 4, size: 1, colID: 1, atExtremes: true,
					hist: testHistogram{
						{1, 0, 0, 20},
						{1, 9, 7, 60},
					},
				},
				{
					at: 7, row: 23, dist: 9, null: 6, size: 1, colID: 1,
					hist: testHistogram{
						{2, 0, 0, 35},
						{2, 8, 3, 40},
						{1, 4, 2, 45},
					},
				},
			},
			expected: []*testStat{
				{
					at: 7, row: 41, dist: 23, null: 6, size: 1, colID: 1,
					hist: testHistogram{
						{1, 0, 0, 20},
						{1, 0, 0, 30},
						{2, 13, 6, 40},
						{1, 7, 4, 50},
						{1, 9, 7, 60},
					},
				},
			},
		},
		{
			// Ensure partial stats at extremes are split at the corresponding full
			// stat lower bound.
			full: []*testStat{
				{
					at: 5, row: 17, dist: 10, null: 4, size: 1, colID: 1,
					hist: testHistogram{
						{1, 0, 0, 30},
						{1, 6, 4, 40},
						{1, 4, 2, 50},
					},
				},
			},
			partial: []*testStat{
				{
					at: 6, row: 16, dist: 6, null: 4, size: 1, colID: 1,
					hist: testHistogram{
						{2, 0, 0, 20},
						{2, 8, 3, 30},
					},
				},
				{
					at: 7, row: 17, dist: 10, null: 6, size: 1, colID: 1, atExtremes: true,
					hist: testHistogram{
						{1, 0, 0, 25},
						{1, 0, 0, 55},
					},
				},
			},
			expected: []*testStat{
				{
					at: 7, row: 32, dist: 16, null: 6, size: 1, colID: 1,
					hist: testHistogram{
						{2, 0, 0, 20},
						{2, 9, 4, 30},
						{1, 6, 4, 40},
						{1, 4, 2, 50},
						{1, 0, 0, 55},
					},
				},
			},
		},
		{
			// Ensure it returns nothing when the latest partial statistic
			// is earlier than the latest full or doesn't exist for that column,
			// as the merged stats are prepended to combined list of full statistics.
			full: []*testStat{
				{
					at: 5, row: 11, dist: 10, null: 4, size: 1, colID: 1,
					hist: testHistogram{{1, 0, 0, 30}, {1, 9, 7, 40}},
				},
				{
					at: 5, row: 11, dist: 10, null: 4, size: 1, colID: 2,
					hist: testHistogram{{1, 0, 0, 30}, {1, 9, 7, 40}},
				},
			},
			partial: []*testStat{
				{
					at: 4, row: 11, dist: 10, null: 4, size: 1, colID: 1,
					hist: testHistogram{{1, 0, 0, 50}, {1, 9, 7, 60}},
				},
			},
			expected: []*testStat{},
		},
	}
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			statsList := make([]*TableStatistic, 0, len(tc.full)+len(tc.partial))
			for _, s := range tc.full {
				stats := s.toTableStatistic(
					ctx, "full", i, descpb.ColumnIDs{descpb.ColumnID(s.colID)},
					uint64(s.colID) /* statID */, 0 /* fullStatID */, st,
				)
				statsList = append(statsList, stats)
			}
			for _, s := range tc.partial {
				var fullStatID uint64
				if s.atExtremes {
					fullStatID = uint64(s.colID)
				}
				stats := s.toTableStatistic(
					ctx, "partial", i, descpb.ColumnIDs{descpb.ColumnID(s.colID)},
					0 /* statID */, fullStatID, st,
				)
				// Set a dummy predicate to ensure that these are treated as partial stats.
				stats.PartialPredicate = "test predicate"
				statsList = append(statsList, stats)
			}
			sort.Slice(statsList, func(i, j int) bool {
				return statsList[i].CreatedAt.After(statsList[j].CreatedAt)
			})
			expected := make([]*TableStatistic, 0, len(tc.expected))
			for _, s := range tc.expected {
				stats := s.toTableStatistic(
					ctx, "__merged__", i, descpb.ColumnIDs{descpb.ColumnID(s.colID)},
					0 /* statID */, 0 /* fullStatID */, st,
				)
				expected = append(expected, stats)
			}
			merged := MergedStatistics(ctx, statsList, st)
			// Round distinct ranges for easier comparison.
			for _, m := range merged {
				m.RoundDistinctRanges()
			}
			if !reflect.DeepEqual(merged, expected) {
				t.Errorf("test case %d incorrect, merged:\n%s\nexpected:\n%s", i, merged, expected)
			}
		})
	}
}

// TestStripOuterBuckets tests stripOuterBuckets which removes outer buckets
// added by addOuterBuckets before merging partial statistics.
func TestStripOuterBuckets(t *testing.T) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.NewTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	t.Run("no outer buckets returns input", func(t *testing.T) {
		buckets := []cat.HistogramBucket{
			{NumEq: 1, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDInt(10)},
			{NumEq: 2, NumRange: 1, DistinctRange: 1, UpperBound: tree.NewDInt(20)},
		}
		strippedBuckets := stripOuterBuckets(ctx, evalCtx, buckets)
		if !reflect.DeepEqual(strippedBuckets, buckets) {
			t.Fatalf("expected buckets unchanged: %v", strippedBuckets)
		}
		if len(strippedBuckets) > 0 && &strippedBuckets[0] != &buckets[0] {
			t.Fatalf("unexpected copy of backing array when no outer buckets")
		}
	})

	testCases := []struct {
		name     string
		buckets  []cat.HistogramBucket
		expected []cat.HistogramBucket
	}{
		{
			buckets: []cat.HistogramBucket{
				{NumEq: 0, UpperBound: tree.NewDInt(math.MinInt64)},
				{NumEq: 1, NumRange: 10, DistinctRange: 5, UpperBound: tree.NewDInt(30)},
				{NumEq: 0, UpperBound: tree.NewDInt(math.MaxInt64)},
			},
			expected: []cat.HistogramBucket{
				{NumEq: 1, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDInt(30)},
			},
		},
		{
			buckets: []cat.HistogramBucket{
				{NumEq: 0, UpperBound: tree.NewDInt(math.MinInt64)},
				{NumEq: 1, NumRange: 10, DistinctRange: 5, UpperBound: tree.NewDInt(30)},
				{NumEq: 2, NumRange: 4, DistinctRange: 3, UpperBound: tree.NewDInt(40)},
			},
			expected: []cat.HistogramBucket{
				{NumEq: 1, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDInt(30)},
				{NumEq: 2, NumRange: 4, DistinctRange: 3, UpperBound: tree.NewDInt(40)},
			},
		},
		{
			buckets: []cat.HistogramBucket{
				{NumEq: 3, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDInt(30)},
				{NumEq: 2, NumRange: 4, DistinctRange: 3, UpperBound: tree.NewDInt(40)},
				{NumEq: 0, UpperBound: tree.NewDInt(math.MaxInt64)},
			},
			expected: []cat.HistogramBucket{
				{NumEq: 3, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDInt(30)},
				{NumEq: 2, NumRange: 4, DistinctRange: 3, UpperBound: tree.NewDInt(40)},
			},
		},
	}
	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buckets := append([]cat.HistogramBucket(nil), tc.buckets...)
			bucketsCopy := append([]cat.HistogramBucket(nil), buckets...)
			strippedBuckets := stripOuterBuckets(ctx, evalCtx, buckets)

			if !reflect.DeepEqual(strippedBuckets, tc.expected) {
				t.Fatalf("test case %d incorrect, stripped buckets:\n%v\nexpected:\n%v",
					i, strippedBuckets, tc.expected)
			}
			if !reflect.DeepEqual(buckets, bucketsCopy) {
				t.Fatalf("test case %d unexpected mutation of input buckets:\n%v\nexpected:\n%v",
					i, buckets, bucketsCopy)
			}
			if len(strippedBuckets) > 0 {
				for bi := range buckets {
					if &strippedBuckets[0] == &buckets[bi] {
						t.Fatalf("test case %d expected stripped buckets result to copy"+
							" input slice, but shares backing array", i)
					}
				}
			}
		})
	}
}

func (tabStat *TableStatistic) RoundDistinctRanges() {
	for i := range tabStat.Histogram {
		tabStat.Histogram[i].DistinctRange = math.Round(tabStat.Histogram[i].DistinctRange)
	}
	for i := range tabStat.HistogramData.Buckets {
		tabStat.HistogramData.Buckets[i].DistinctRange =
			math.Round(tabStat.HistogramData.Buckets[i].DistinctRange)
	}
}
