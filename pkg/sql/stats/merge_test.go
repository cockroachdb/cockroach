// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// TestMergeStatistics calls mergeStatistics with various observed
// stats, that include full statistics and partial statistics.
func TestMergeStatistics(t *testing.T) {
	testCases := []struct {
		initial  *testStat
		partial  *testStat
		expected *testStat
		err      bool
	}{
		{
			// Single partial at the extremes of full.
			initial: &testStat{
				at: 1, row: 15, dist: 10, null: 4, size: 1,
				hist: testHistogram{{1, 0, 0, 30}, {1, 9, 7, 40}},
			},
			partial: &testStat{
				at: 1, row: 19, dist: 10, null: 0, size: 1,
				hist: testHistogram{{3, 2, 0, 20}, {3, 11, 8, 50}},
			},
			expected: &testStat{
				at: 1, row: 30, dist: 19, null: 0, size: 1,
				hist: testHistogram{{3, 2, 0, 20}, {1, 0, 0, 30}, {1, 9, 7, 40}, {3, 11, 8, 50}},
			},
		},
		{
			// Multiple buckets at extremes.
			initial: &testStat{
				at: 1, row: 5, dist: 5, null: 0, size: 2,
				hist: testHistogram{
					{1, 0, 0, 2},
					{1, 1, 1, 4},
					{1, 1, 1, 6},
				},
			},
			partial: &testStat{
				at: 2, row: 8, dist: 4, null: 0, size: 2,
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
			// Multiple buckets at lower bound.
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
			// Multiple buckets at upper bound.
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
			// Multiple buckets at both upper and lower bounds.
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
				at: 5, row: 48, dist: 26, null: 0, size: 2,
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
	}
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			initial := tc.initial.toTableStatistic(ctx, "stat", i, descpb.ColumnIDs{1}, 1 /* statID */, 0 /* fullStatID */, st)
			partial := tc.partial.toTableStatistic(ctx, "stat", i, descpb.ColumnIDs{1}, 0 /* statID */, 1 /* fullStatID */, st)
			expected := tc.expected.toTableStatistic(ctx, "__merged__", i, descpb.ColumnIDs{1}, 0 /* statID */, 0 /* fullStatID */, st)
			merged, err := mergeExtremesStatistic(ctx, initial, partial, st)
			if err != nil {
				if !tc.err {
					t.Errorf("test case %d unexpected mergeStatistics err: %v", i, err)
				}
				return
			}
			if tc.err {
				t.Errorf("test case %d expected mergeStatistics err, was:\n%s", i, merged)
				return
			}
			if !reflect.DeepEqual(merged, expected) {
				t.Errorf("test case %d incorrect merge\n%s\nexpected\n%s", i, merged, expected)
			}
		})

	}
	t.Run("mismatched full and partial stat error", func(t *testing.T) {
		initial := testCases[0].initial.toTableStatistic(ctx, "stat", 1, descpb.ColumnIDs{1}, 2 /* statID */, 0 /* fullStatID */, st)
		partial := testCases[0].partial.toTableStatistic(ctx, "stat", 1, descpb.ColumnIDs{1}, 0 /* statID */, 1 /* fullStatID */, st)
		_, err := mergeExtremesStatistic(ctx, initial, partial, st)
		if err == nil {
			t.Errorf("error test case failed -- expected partial stat to have a different fullStatID than the statID of the full statistics")
		}
	})
}

// TestMergedStatistics tests MergedStatistics which
// merges an array of full statistics with partial statistics.
func TestMergedStatistics(t *testing.T) {

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	// Array of one or more full statistics and one or more partial
	// statistics for different column sets.
	testCases := []struct {
		full     []*testStat
		partial  []*testStat
		expected []*testStat
	}{
		{
			// Simplest case, one newer partial stat for each new full stat.
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
					at: 6, row: 14, dist: 12, null: 0, size: 1, colID: 1,
					hist: testHistogram{{1, 3, 3, 50}, {1, 9, 7, 60}},
				},
				{
					at: 6, row: 8, dist: 6, null: 0, size: 1, colID: 2,
					hist: testHistogram{{1, 0, 0, 10}, {1, 6, 4, 20}},
				},
				{
					at: 6, row: 5, dist: 3, null: 0, size: 1, colID: 3,
					hist: testHistogram{{1, 0, 0, 10}, {1, 3, 1, 50}},
				},
				{
					at: 6, row: 7, dist: 5, null: 0, size: 1, colID: 4,
					hist: testHistogram{{1, 0, 0, 0}, {1, 0, 0, 10}, {1, 3, 1, 50}, {1, 0, 0, 60}},
				},
			},
			expected: []*testStat{
				{
					at: 6, row: 28, dist: 24, null: 0, size: 1, colID: 1,
					hist: testHistogram{{1, 3, 3, 30.0}, {1, 9, 7, 40.0}, {1, 3, 3, 50}, {1, 9, 7, 60}},
				},
				{
					at: 6, row: 16, dist: 12, null: 0, size: 1, colID: 2,
					hist: testHistogram{{1, 0, 0, 10}, {1, 6, 4, 20}, {1, 0, 0, 30}, {1, 6, 4, 40}},
				},
				{
					at: 6, row: 10, dist: 6, null: 0, size: 1, colID: 3,
					hist: testHistogram{{1, 0, 0, 10.0}, {1, 0, 0, 30}, {1, 3, 1, 40}, {1, 3, 1, 50.0}},
				},
				{
					at: 6, row: 12, dist: 8, null: 0, size: 1, colID: 4,
					hist: testHistogram{{1, 0, 0, 0.0}, {1, 0, 0, 10.0}, {1, 0, 0, 30}, {1, 3, 1, 40}, {1, 3, 1, 50}, {1, 0, 0, 60}},
				},
			},
		},
		{
			// Multiple partial stats for a full statistic.
			// Ensure it picks the latest for each respective set of column keys.
			full: []*testStat{
				{
					at: 5, row: 15, dist: 10, null: 4, size: 1, colID: 1,
					hist: testHistogram{{1, 0, 0, 30}, {1, 9, 7, 40}},
				},
				{
					at: 5, row: 15, dist: 10, null: 4, size: 1, colID: 2,
					hist: testHistogram{{1, 0, 0, 20}, {1, 9, 7, 30}},
				},
			},
			partial: []*testStat{
				{
					at: 8, row: 26, dist: 19, null: 4, size: 1, colID: 1,
					hist: testHistogram{{2, 0, 0, 50}, {1, 9, 8, 60}, {1, 9, 7, 70}},
				},
				{
					at: 7, row: 6, dist: 3, null: 4, size: 1, colID: 2,
					hist: testHistogram{{1, 0, 0, 40}, {1, 0, 0, 50}},
				},
				{
					at: 7, row: 15, dist: 10, null: 4, size: 1, colID: 1,
					hist: testHistogram{{1, 0, 0, 50}, {1, 9, 7, 60}},
				},
				{
					at: 6, row: 5, dist: 2, null: 4, size: 1, colID: 2,
					hist: testHistogram{{1, 0, 0, 50}},
				},
			},
			expected: []*testStat{
				{
					at: 8, row: 37, dist: 28, null: 4, size: 1, colID: 1,
					hist: testHistogram{{1, 0, 0, 30.0}, {1, 9, 7, 40.0}, {2, 0, 0, 50.0}, {1, 9, 8, 60.0}, {1, 9, 7, 70.0}},
				},
				{
					at: 7, row: 17, dist: 12, null: 4, size: 1, colID: 2,
					hist: testHistogram{{1, 0, 0, 20.0}, {1, 9, 7, 30.0}, {1, 0, 0, 40.0}, {1, 0, 0, 50.0}},
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
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			statsList := make([]*TableStatistic, 0, len(tc.full)+len(tc.partial))
			predicates := make(map[uint64]string, len(tc.full))
			for _, s := range tc.full {
				stats := s.toTableStatistic(
					ctx, "full", i, descpb.ColumnIDs{descpb.ColumnID(s.colID)},
					uint64(s.colID) /* statID */, 0 /* fullStatID */, st,
				)
				statsList = append(statsList, stats)
				predicates[uint64(s.colID)] = fmt.Sprintf(
					"(c IS NULL) OR ((c < %v:::FLOAT8) OR (c > %v:::FLOAT8))",
					s.hist[0].UpperBound, s.hist[len(s.hist)-1].UpperBound,
				)
			}
			for _, s := range tc.partial {
				stats := s.toTableStatistic(
					ctx, "partial", i, descpb.ColumnIDs{descpb.ColumnID(s.colID)},
					0 /* statID */, uint64(s.colID) /* fullStatID */, st,
				)
				stats.PartialPredicate = predicates[uint64(s.colID)]
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
			if !reflect.DeepEqual(merged, expected) {
				t.Errorf("test case %d incorrect, merged:\n%s\nexpected:\n%s", i, merged, expected)
			}
		})
	}
}
