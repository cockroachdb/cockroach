// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"context"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// TestForecastColumnStatistics calls forecastColumnStatistics with various
// observed stats.
func TestForecastColumnStatistics(t *testing.T) {
	testCases := []struct {
		observed []*testStat
		at       uint64
		err      bool
		forecast *testStat
	}{
		// Error: Too few observations
		{
			at:  1,
			err: true,
		},
		// Error: Too few observations
		{
			observed: []*testStat{
				{at: 1, row: 1, dist: 1, null: 0, size: 1},
			},
			at:  2,
			err: true,
		},
		// Error: Too few observations
		{
			observed: []*testStat{
				{at: 1, row: 1, dist: 1, null: 0, size: 1},
				{at: 2, row: 2, dist: 2, null: 0, size: 1},
			},
			at:  3,
			err: true,
		},
		// Error: Multiple observations with the same collection time
		{
			observed: []*testStat{
				{at: 1, row: 1, dist: 1, null: 0, size: 1},
				{at: 2, row: 2, dist: 2, null: 0, size: 1},
				{at: 2, row: 2, dist: 2, null: 0, size: 1},
			},
			at:  3,
			err: true,
		},
		// Constant empty table
		{
			observed: []*testStat{
				{at: 1, row: 0, dist: 0, null: 0, size: 0},
				{at: 2, row: 0, dist: 0, null: 0, size: 0},
				{at: 3, row: 0, dist: 0, null: 0, size: 0},
			},
			at:       4,
			forecast: &testStat{at: 4, row: 0, dist: 0, null: 0, size: 0},
		},
		// Constant all null rows
		{
			observed: []*testStat{
				{at: 1, row: 1, dist: 1, null: 1, size: 0},
				{at: 2, row: 1, dist: 1, null: 1, size: 0},
				{at: 3, row: 1, dist: 1, null: 1, size: 0},
			},
			at:       4,
			forecast: &testStat{at: 4, row: 1, dist: 1, null: 1, size: 0},
		},
		// Constant all non-null rows
		{
			observed: []*testStat{
				{at: 1, row: 2, dist: 1, null: 0, size: 1},
				{at: 2, row: 2, dist: 1, null: 0, size: 1},
				{at: 3, row: 2, dist: 1, null: 0, size: 1},
			},
			at:       4,
			forecast: &testStat{at: 4, row: 2, dist: 1, null: 0, size: 1},
		},
		// Constant null and non-null rows
		{
			observed: []*testStat{
				{at: 1, row: 9, dist: 4, null: 5, size: 1},
				{at: 2, row: 9, dist: 4, null: 5, size: 1},
				{at: 3, row: 9, dist: 4, null: 5, size: 1},
			},
			at:       4,
			forecast: &testStat{at: 4, row: 9, dist: 4, null: 5, size: 1},
		},
		// Growing number of null rows
		{
			observed: []*testStat{
				{at: 1, row: 7, dist: 2, null: 2, size: 1},
				{at: 2, row: 7, dist: 2, null: 4, size: 1},
				{at: 3, row: 7, dist: 2, null: 6, size: 1},
			},
			at:       4,
			forecast: &testStat{at: 4, row: 7, dist: 1, null: 7, size: 1},
		},
		// Shrinking number of null rows
		{
			observed: []*testStat{
				{at: 1, row: 6, dist: 2, null: 5, size: 0},
				{at: 2, row: 6, dist: 2, null: 3, size: 0},
				{at: 3, row: 6, dist: 2, null: 1, size: 0},
			},
			at:       4,
			forecast: &testStat{at: 4, row: 6, dist: 2, null: 0, size: 0},
		},
		// Growing number of non-null rows
		{
			observed: []*testStat{
				{at: 1, row: 10, dist: 8, null: 3, size: 2},
				{at: 2, row: 20, dist: 18, null: 3, size: 2},
				{at: 3, row: 30, dist: 28, null: 3, size: 2},
				{at: 5, row: 50, dist: 48, null: 3, size: 2},
			},
			at:       6,
			forecast: &testStat{at: 6, row: 60, dist: 58, null: 3, size: 2},
		},
		// Shrinking number of non-null rows
		{
			observed: []*testStat{
				{at: 1, row: 13, dist: 10, null: 4, size: 11},
				{at: 2, row: 12, dist: 9, null: 4, size: 11},
				{at: 5, row: 9, dist: 6, null: 4, size: 11},
			},
			at:       9,
			forecast: &testStat{at: 9, row: 5, dist: 2, null: 4, size: 11},
		},
		// Growing number of null and non-null rows
		{
			observed: []*testStat{
				{at: 1, row: 4, dist: 3, null: 2, size: 5},
				{at: 3, row: 5, dist: 2, null: 4, size: 5},
				{at: 5, row: 6, dist: 1, null: 6, size: 5},
			},
			at:       7,
			forecast: &testStat{at: 7, row: 7, dist: 1, null: 7, size: 5},
		},
		// Shrinking number of null and non-null rows
		{
			observed: []*testStat{
				{at: 5, row: 14, dist: 9, null: 4, size: 1},
				{at: 6, row: 10, dist: 6, null: 3, size: 1},
				{at: 7, row: 6, dist: 3, null: 2, size: 1},
			},
			at:       8,
			forecast: &testStat{at: 8, row: 2, dist: 2, null: 1, size: 1},
		},
		// Growing distinct count
		{
			observed: []*testStat{
				{at: 1, row: 21, dist: 3, null: 2, size: 10},
				{at: 2, row: 22, dist: 6, null: 2, size: 10},
				{at: 5, row: 25, dist: 15, null: 2, size: 10},
				{at: 6, row: 26, dist: 18, null: 2, size: 10},
				{at: 7, row: 27, dist: 21, null: 2, size: 10},
			},
			at:       10,
			forecast: &testStat{at: 10, row: 30, dist: 29, null: 2, size: 10},
		},
		// Shrinking distinct count
		{
			observed: []*testStat{
				{at: 5, row: 25, dist: 15, null: 0, size: 1},
				{at: 6, row: 25, dist: 10, null: 0, size: 1},
				{at: 7, row: 25, dist: 5, null: 0, size: 1},
			},
			at:       11,
			forecast: &testStat{at: 11, row: 25, dist: 2, null: 0, size: 1},
		},
		// Growing AvgSize
		{
			observed: []*testStat{
				{at: 2, row: 9, dist: 3, null: 0, size: 1},
				{at: 4, row: 9, dist: 5, null: 0, size: 11},
				{at: 6, row: 9, dist: 7, null: 0, size: 21},
			},
			at:       7,
			forecast: &testStat{at: 7, row: 9, dist: 8, null: 0, size: 26},
		},
		// Shrinking AvgSize
		{
			observed: []*testStat{
				{at: 2, row: 10, dist: 8, null: 0, size: 30},
				{at: 4, row: 10, dist: 8, null: 0, size: 20},
				{at: 6, row: 10, dist: 8, null: 0, size: 10},
			},
			at:       9,
			forecast: &testStat{at: 9, row: 10, dist: 8, null: 0, size: 3},
		},
		// Growing from empty table
		{
			observed: []*testStat{
				{at: 1, row: 0, dist: 0, null: 0, size: 0},
				{at: 2, row: 1, dist: 1, null: 0, size: 2},
				{at: 3, row: 2, dist: 1, null: 0, size: 2},
			},
			at:       4,
			forecast: &testStat{at: 4, row: 3, dist: 1, null: 0, size: 2},
		},
		// Shrinking to empty table
		{
			observed: []*testStat{
				{at: 1, row: 3, dist: 1, null: 0, size: 2},
				{at: 2, row: 2, dist: 1, null: 0, size: 2},
				{at: 3, row: 1, dist: 1, null: 0, size: 2},
			},
			at:       4,
			forecast: &testStat{at: 4, row: 0, dist: 0, null: 0, size: 2},
		},
		// Error: RowCount bad fit
		{
			observed: []*testStat{
				{at: 1, row: 7, dist: 1, null: 1, size: 1},
				{at: 2, row: 9, dist: 2, null: 2, size: 2},
				{at: 3, row: 5, dist: 3, null: 3, size: 3},
			},
			at:  4,
			err: true,
		},
		// Error: NullCount bad fit
		{
			observed: []*testStat{
				{at: 1, row: 2, dist: 1, null: 2, size: 1},
				{at: 2, row: 4, dist: 2, null: 0, size: 2},
				{at: 3, row: 6, dist: 3, null: 3, size: 3},
			},
			at:  4,
			err: true,
		},
		// Error: DistinctCount bad fit
		{
			observed: []*testStat{
				{at: 1, row: 2, dist: 1, null: 0, size: 1},
				{at: 2, row: 4, dist: 2, null: 0, size: 2},
				{at: 3, row: 6, dist: 5, null: 0, size: 3},
			},
			at:  4,
			err: true,
		},
		// Error: AvgSize bad fit
		{
			observed: []*testStat{
				{at: 1, row: 2, dist: 1, null: 0, size: 1},
				{at: 2, row: 4, dist: 2, null: 0, size: 2},
				{at: 3, row: 6, dist: 5, null: 0, size: 3},
			},
			at:  4,
			err: true,
		},
		// Skip only-null stats for DistinctCount
		{
			observed: []*testStat{
				{at: 1, row: 2, dist: 1, null: 2, size: 1},
				{at: 2, row: 4, dist: 2, null: 2, size: 2},
				{at: 3, row: 6, dist: 5, null: 2, size: 3},
			},
			at:       4,
			forecast: &testStat{at: 4, row: 8, dist: 7, null: 2, size: 4},
		},
		// Skip only-null stats for AvgSize
		{
			observed: []*testStat{
				{at: 1, row: 2, dist: 1, null: 2, size: 0},
				{at: 2, row: 4, dist: 2, null: 2, size: 7},
				{at: 3, row: 6, dist: 3, null: 2, size: 6},
			},
			at:       4,
			forecast: &testStat{at: 4, row: 8, dist: 4, null: 2, size: 5},
		},
		// Histogram, constant empty table
		{
			observed: []*testStat{
				{at: 1, row: 0, dist: 0, null: 0, size: 0, hist: testHistogram{}},
				{at: 2, row: 0, dist: 0, null: 0, size: 0, hist: testHistogram{}},
				{at: 3, row: 0, dist: 0, null: 0, size: 0, hist: testHistogram{}},
			},
			at:       4,
			forecast: &testStat{at: 4, row: 0, dist: 0, null: 0, size: 0, hist: testHistogram{}},
		},
		// Histogram, constant all null rows
		{
			observed: []*testStat{
				{at: 1, row: 1, dist: 1, null: 1, size: 0, hist: testHistogram{}},
				{at: 2, row: 1, dist: 1, null: 1, size: 0, hist: testHistogram{}},
				{at: 3, row: 1, dist: 1, null: 1, size: 0, hist: testHistogram{}},
			},
			at:       4,
			forecast: &testStat{at: 4, row: 1, dist: 1, null: 1, size: 0, hist: testHistogram{}},
		},
		// Histogram, constant all non-null rows
		{
			observed: []*testStat{
				{at: 1, row: 2, dist: 1, null: 0, size: 1, hist: testHistogram{{2, 0, 0, 99}}},
				{at: 2, row: 2, dist: 1, null: 0, size: 1, hist: testHistogram{{2, 0, 0, 99}}},
				{at: 3, row: 2, dist: 1, null: 0, size: 1, hist: testHistogram{{2, 0, 0, 99}}},
			},
			at: 4,
			forecast: &testStat{
				at: 4, row: 2, dist: 1, null: 0, size: 1, hist: testHistogram{{2, 0, 0, 99}},
			},
		},
		// Histogram, constant null and non-null rows
		{
			observed: []*testStat{
				{
					at: 1, row: 9, dist: 4, null: 5, size: 1,
					hist: testHistogram{{2, 0, 0, 99}, {1, 1, 1, 119}},
				},
				{
					at: 2, row: 9, dist: 4, null: 5, size: 1,
					hist: testHistogram{{2, 0, 0, 99}, {1, 1, 1, 119}},
				},
				{
					at: 3, row: 9, dist: 4, null: 5, size: 1,
					hist: testHistogram{{2, 0, 0, 99}, {1, 1, 1, 119}},
				},
			},
			at: 4,
			forecast: &testStat{
				at: 4, row: 9, dist: 4, null: 5, size: 1,
				hist: testHistogram{{2, 0, 0, 99}, {1, 1, 1, 119}},
			},
		},
		// Histogram, growing number of null and non-null rows
		{
			observed: []*testStat{
				{
					at: 1, row: 4, dist: 3, null: 2, size: 5,
					hist: testHistogram{{1, 0, 0, 9000}, {1, 0, 0, 10000}},
				},
				{
					at: 3, row: 5, dist: 2, null: 4, size: 5,
					hist: testHistogram{{1, 0, 0, 10000}},
				},
				{
					at: 5, row: 6, dist: 1, null: 6, size: 5,
					hist: testHistogram{},
				},
			},
			at: 7,
			forecast: &testStat{
				at: 7, row: 7, dist: 1, null: 7, size: 5,
				hist: testHistogram{},
			},
		},
		// Histogram, shrinking number of null and non-null rows
		{
			observed: []*testStat{
				{
					at: 5, row: 14, dist: 9, null: 4, size: 1,
					hist: testHistogram{{0, 0, 0, 30}, {1, 9, 7, 40}},
				},
				{
					at: 6, row: 10, dist: 6, null: 3, size: 1,
					hist: testHistogram{{0, 0, 0, 30}, {1, 6, 4, 40}},
				},
				{
					at: 7, row: 6, dist: 3, null: 2, size: 1,
					hist: testHistogram{{0, 0, 0, 30}, {1, 3, 1, 40}},
				},
			},
			at: 8,
			forecast: &testStat{
				at: 8, row: 2, dist: 2, null: 1, size: 1,
				hist: testHistogram{{1, 0, 0, 40}},
			},
		},
		// Histogram, growing distinct count
		{
			observed: []*testStat{
				{
					at: 1, row: 21, dist: 3, null: 2, size: 10,
					hist: testHistogram{{0, 0, 0, 85}, {19, 0, 0, 100}},
				},
				{
					at: 2, row: 22, dist: 6, null: 2, size: 10,
					hist: testHistogram{{0, 0, 0, 85}, {17, 3, 3, 100}},
				},
				{
					at: 5, row: 25, dist: 15, null: 2, size: 10,
					hist: testHistogram{{0, 0, 0, 85}, {11, 12, 12, 100}},
				},
				{
					at: 6, row: 26, dist: 18, null: 2, size: 10,
					hist: testHistogram{{0, 0, 0, 85}, {9, 15, 15, 100}},
				},
				{
					at: 7, row: 27, dist: 21, null: 2, size: 10,
					hist: testHistogram{{0, 0, 0, 85}, {7, 18, 18, 100}},
				},
			},
			at: 10,
			forecast: &testStat{
				at: 10, row: 30, dist: 29, null: 2, size: 10,
				hist: testHistogram{{0, 0, 0, 85}, {1, 27, 27, 100}},
			},
		},
		// Histogram, shrinking distinct count
		{
			observed: []*testStat{
				{
					at: 5, row: 25, dist: 15, null: 0, size: 1,
					hist: testHistogram{{7, 0, 0, 404}, {0, 18, 14, 500}},
				},
				{
					at: 6, row: 25, dist: 10, null: 0, size: 1,
					hist: testHistogram{{10, 0, 0, 404}, {0, 15, 9, 500}},
				},
				{
					at: 7, row: 25, dist: 5, null: 0, size: 1,
					hist: testHistogram{{13, 0, 0, 404}, {0, 12, 4, 500}},
				},
			},
			at: 11,
			forecast: &testStat{
				at: 11, row: 25, dist: 2, null: 0, size: 1,
				hist: testHistogram{{13, 0, 0, 404}, {0, 12, 1, 500}},
			},
		},
		// Histogram, growing from empty table
		{
			observed: []*testStat{
				{
					at: 1, row: 0, dist: 0, null: 0, size: 0,
					hist: testHistogram{},
				},
				{
					at: 2, row: 1, dist: 1, null: 0, size: 2,
					hist: testHistogram{{1, 0, 0, -2345}},
				},
				{
					at: 3, row: 2, dist: 1, null: 0, size: 2,
					hist: testHistogram{{2, 0, 0, -2345}},
				},
			},
			at: 4,
			forecast: &testStat{
				at: 4, row: 3, dist: 1, null: 0, size: 2,
				hist: testHistogram{{3, 0, 0, -2345}},
			},
		},
		// Histogram, shrinking to empty table
		{
			observed: []*testStat{
				{
					at: 1, row: 3, dist: 1, null: 0, size: 2,
					hist: testHistogram{{3, 0, 0, 1700}},
				},
				{
					at: 2, row: 2, dist: 1, null: 0, size: 2,
					hist: testHistogram{{2, 0, 0, 1700}},
				},
				{
					at: 3, row: 1, dist: 1, null: 0, size: 2,
					hist: testHistogram{{1, 0, 0, 1700}},
				},
			},
			at: 4,
			forecast: &testStat{
				at: 4, row: 0, dist: 0, null: 0, size: 2,
				hist: testHistogram{},
			},
		},
		// Histogram, skip only-null stats
		{
			observed: []*testStat{
				{
					at: 1, row: 3, dist: 1, null: 3, size: 2,
					hist: testHistogram{},
				},
				{
					at: 2, row: 5, dist: 3, null: 3, size: 2,
					hist: testHistogram{{1, 0, 0, 200}, {0, 1, 1, 800}},
				},
				{
					at: 3, row: 7, dist: 4, null: 3, size: 2,
					hist: testHistogram{{2, 0, 0, 200}, {0, 2, 2, 800}},
				},
				{
					at: 4, row: 9, dist: 5, null: 3, size: 2,
					hist: testHistogram{{3, 0, 0, 200}, {0, 3, 3, 800}},
				},
			},
			at: 5,
			forecast: &testStat{
				at: 5, row: 11, dist: 6, null: 3, size: 2,
				hist: testHistogram{{4, 0, 0, 200}, {0, 4, 4, 800}},
			},
		},
		// Histogram, constant numbers but changing shape
		{
			observed: []*testStat{
				{
					at: 1, row: 16, dist: 7, null: 0, size: 1,
					hist: testHistogram{{1, 0, 0, 14}, {2, 6, 2, 15}, {1, 6, 2, 16}},
				},
				{
					at: 2, row: 16, dist: 7, null: 0, size: 1,
					hist: testHistogram{{1, 0, 0, 14}, {2, 6, 2, 14.75}, {1, 6, 2, 18}},
				},
				{
					at: 3, row: 16, dist: 7, null: 0, size: 1,
					hist: testHistogram{{1, 0, 0, 14}, {2, 6, 2, 14.5}, {1, 6, 2, 20}},
				},
				{
					at: 4, row: 16, dist: 7, null: 0, size: 1,
					hist: testHistogram{{1, 0, 0, 14}, {2, 6, 2, 14.25}, {1, 6, 2, 22}},
				},
			},
			at: 5,
			forecast: &testStat{
				at: 5, row: 16, dist: 7, null: 0, size: 1,
				hist: testHistogram{{9, 0, 0, 14}, {1, 6, 5, 24}},
			},
		},
		// Histogram, too few observations
		{
			observed: []*testStat{
				{
					at: 1, row: 10, dist: 2, null: 0, size: 1,
				},
				{
					at: 2, row: 20, dist: 2, null: 0, size: 1,
					hist: testHistogram{{10, 0, 0, 100}, {10, 0, 0, 200}},
				},
				{
					at: 3, row: 30, dist: 2, null: 0, size: 1,
					hist: testHistogram{{15, 0, 0, 100}, {15, 0, 0, 300}},
				},
			},
			at: 4,
			forecast: &testStat{
				at: 4, row: 40, dist: 2, null: 0, size: 1,
				hist: testHistogram{{20, 0, 0, 100}, {20, 0, 0, 300}},
			},
		},
		// Histogram, bad fit
		{
			observed: []*testStat{
				{
					at: 1, row: 10, dist: 2, null: 0, size: 1,
					hist: testHistogram{{5, 0, 0, 50}, {5, 0, 0, 100}},
				},
				{
					at: 2, row: 20, dist: 2, null: 0, size: 1,
					hist: testHistogram{{10, 0, 0, 50}, {10, 0, 0, 200}},
				},
				{
					at: 3, row: 30, dist: 2, null: 0, size: 1,
					hist: testHistogram{{15, 0, 0, 50}, {15, 0, 0, 301}},
				},
			},
			at: 4,
			forecast: &testStat{
				at: 4, row: 40, dist: 2, null: 0, size: 1,
				hist: testHistogram{{20, 0, 0, 50}, {20, 0, 0, 301}},
			},
		},
		// Histogram, rounded counts.
		{
			observed: []*testStat{
				{
					at: 2, row: 13, dist: 7, null: 3, size: 1,
					hist: testHistogram{{5, 0, 0, 100}, {0, 5, 5, 200}},
				},
				{
					at: 4, row: 26, dist: 12, null: 6, size: 4,
					hist: testHistogram{{10, 0, 0, 200}, {0, 10, 10, 300}},
				},
				{
					at: 6, row: 39, dist: 17, null: 9, size: 7,
					hist: testHistogram{{15, 0, 0, 300}, {0, 15, 15, 400}},
				},
				{
					at: 8, row: 52, dist: 22, null: 12, size: 10,
					hist: testHistogram{{20, 0, 0, 400}, {0, 20, 20, 500}},
				},
			},
			at: 9,
			forecast: &testStat{
				at: 9, row: 59, dist: 25, null: 14, size: 12,
				hist: testHistogram{{22, 0, 0, 450}, {0, 23, 23, 550}},
			},
		},
	}
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	var fullStatID, partialStatID uint64
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {

			// Set up observed TableStatistics in CreatedAt desc order.
			observed := make([]*TableStatistic, len(tc.observed))
			for j := range tc.observed {
				observed[len(observed)-j-1] = tc.observed[j].toTableStatistic(
					ctx, "testStat", i, descpb.ColumnIDs{1}, fullStatID, partialStatID, st,
				)
			}
			expected := tc.forecast.toTableStatistic(
				ctx, jobspb.ForecastStatsName, i, descpb.ColumnIDs{1}, fullStatID, partialStatID, st,
			)
			at := testStatTime(tc.at)

			forecast, err := forecastColumnStatistics(ctx, st, observed, at, 1)
			if err != nil {
				if !tc.err {
					t.Errorf("test case %d unexpected forecastColumnStatistics err: %v", i, err)
				}
				return
			}
			if tc.err {
				t.Errorf("test case %d expected forecastColumnStatistics err, was:\n%s", i, forecast)
				return
			}
			if !reflect.DeepEqual(forecast, expected) {
				t.Errorf("test case %d incorrect forecast\n%s\nexpected\n%s", i, forecast, expected)
			}
		})
	}
}

type testStat struct {
	at, row, dist, null, size uint64
	hist                      testHistogram
	colID                     uint32
}

func (ts *testStat) toTableStatistic(
	ctx context.Context,
	name string,
	tableID int,
	columnIDs descpb.ColumnIDs,
	statID uint64,
	fullStatID uint64,
	st *cluster.Settings,
) *TableStatistic {
	if ts == nil {
		return nil
	}
	stat := &TableStatistic{
		TableStatisticProto: TableStatisticProto{
			TableID:         catid.DescID(tableID),
			StatisticID:     statID,
			Name:            name,
			ColumnIDs:       columnIDs,
			CreatedAt:       testStatTime(ts.at),
			RowCount:        ts.row,
			DistinctCount:   ts.dist,
			NullCount:       ts.null,
			AvgSize:         ts.size,
			FullStatisticID: fullStatID,
		},
	}
	if ts.hist != nil {
		hist := ts.hist.toHistogram()
		histData, err := hist.toHistogramData(ctx, types.Float, st)
		if err != nil {
			panic(err)
		}
		stat.HistogramData = &histData
		stat.setHistogramBuckets(hist)
	}
	return stat
}

func testStatTime(at uint64) time.Time {
	return timeutil.Unix(int64(at), 0)
}
