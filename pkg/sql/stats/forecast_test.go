// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

func TestForecastTableStatistics(t *testing.T) {
	testCases := []struct {
	}{}
	for i := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
		})
	}
}

func TestForecastColumnStatistics(t *testing.T) {
	// We use all floats here for histograms. TestQuantileValue and
	// TestHistogramValue test conversions to other datatypes.
	columnIDs := []descpb.ColumnID{1}
	colType := types.Float
	testCases := []struct {
		// observed statistics must be in descending CreatedAt order
		observed       []*TableStatistic
		at             time.Time
		minRequiredFit float64
		forecast       *TableStatistic
		score          float64
	}{
		{
			observed: []*TableStatistic{
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(1999, 12, 31, 23, 59, 59, 0, time.UTC),
						RowCount:      1,
						DistinctCount: 1,
						NullCount:     0,
						AvgSize:       4,
					},
				},
			},
			at:             time.Date(1999, 12, 31, 23, 59, 59, 0, time.UTC),
			minRequiredFit: 1,
			forecast: &TableStatistic{
				TableStatisticProto: TableStatisticProto{
					Name:          jobspb.ForecastStatsName,
					ColumnIDs:     columnIDs,
					CreatedAt:     time.Date(1999, 12, 31, 23, 59, 59, 0, time.UTC),
					RowCount:      1,
					DistinctCount: 1,
					NullCount:     0,
					AvgSize:       4,
				},
			},
			score: 0,
		},
		{
			observed: []*TableStatistic{
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
						RowCount:      4,
						DistinctCount: 2,
						NullCount:     2,
						AvgSize:       8,
					},
				},
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
						RowCount:      2,
						DistinctCount: 1,
						NullCount:     1,
						AvgSize:       4,
					},
				},
			},
			at:             time.Date(2020, 1, 3, 0, 0, 0, 0, time.UTC),
			minRequiredFit: 1,
			forecast: &TableStatistic{
				TableStatisticProto: TableStatisticProto{
					Name:          jobspb.ForecastStatsName,
					ColumnIDs:     columnIDs,
					CreatedAt:     time.Date(2020, 1, 3, 0, 0, 0, 0, time.UTC),
					RowCount:      6,
					DistinctCount: 3,
					NullCount:     3,
					AvgSize:       12,
				},
			},
			score: 0,
		},
		// Test that minRequiredFit excludes predictions.
		{
			observed: []*TableStatistic{
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2020, 1, 3, 0, 0, 0, 0, time.UTC),
						RowCount:      100,
						DistinctCount: 10,
						NullCount:     10,
						AvgSize:       10,
					},
				},
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
						RowCount:      300,
						DistinctCount: 30,
						NullCount:     30,
						AvgSize:       30,
					},
				},
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
						RowCount:      200,
						DistinctCount: 20,
						NullCount:     20,
						AvgSize:       20,
					},
				},
			},
			at:             time.Date(2020, 1, 4, 0, 0, 0, 0, time.UTC),
			minRequiredFit: 1,
			forecast: &TableStatistic{
				TableStatisticProto: TableStatisticProto{
					Name:          jobspb.ForecastStatsName,
					ColumnIDs:     columnIDs,
					CreatedAt:     time.Date(2020, 1, 4, 0, 0, 0, 0, time.UTC),
					RowCount:      100,
					DistinctCount: 10,
					NullCount:     10,
					AvgSize:       10,
				},
			},
			score: 0,
		},
		// Test predicting empty table.
		{
			observed: []*TableStatistic{
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2020, 1, 3, 0, 0, 0, 0, time.UTC),
						RowCount:      10,
						DistinctCount: 4,
						NullCount:     4,
						AvgSize:       4,
					},
				},
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
						RowCount:      20,
						DistinctCount: 6,
						NullCount:     6,
						AvgSize:       6,
					},
				},
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
						RowCount:      30,
						DistinctCount: 8,
						NullCount:     8,
						AvgSize:       8,
					},
				},
			},
			at:             time.Date(2020, 1, 4, 0, 0, 0, 0, time.UTC),
			minRequiredFit: 1,
			forecast: &TableStatistic{
				TableStatisticProto: TableStatisticProto{
					Name:          jobspb.ForecastStatsName,
					ColumnIDs:     columnIDs,
					CreatedAt:     time.Date(2020, 1, 4, 0, 0, 0, 0, time.UTC),
					RowCount:      0,
					DistinctCount: 0,
					NullCount:     0,
					AvgSize:       0,
				},
			},
			score: 0,
		},
		// Test predicting all nulls.
		{
			observed: []*TableStatistic{
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2020, 1, 3, 0, 0, 0, 0, time.UTC),
						RowCount:      70,
						DistinctCount: 5,
						NullCount:     66,
						AvgSize:       8,
					},
				},
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
						RowCount:      60,
						DistinctCount: 15,
						NullCount:     33,
						AvgSize:       8,
					},
				},
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
						RowCount:      50,
						DistinctCount: 25,
						NullCount:     0,
						AvgSize:       8,
					},
				},
			},
			at:             time.Date(2020, 1, 4, 0, 0, 0, 0, time.UTC),
			minRequiredFit: 1,
			forecast: &TableStatistic{
				TableStatisticProto: TableStatisticProto{
					Name:          jobspb.ForecastStatsName,
					ColumnIDs:     columnIDs,
					CreatedAt:     time.Date(2020, 1, 4, 0, 0, 0, 0, time.UTC),
					RowCount:      80,
					DistinctCount: 1,
					NullCount:     80,
					AvgSize:       0,
				},
			},
			score: 0,
		},
		// Test predicting no nulls.
		{
			observed: []*TableStatistic{
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2020, 1, 3, 0, 0, 0, 0, time.UTC),
						RowCount:      11,
						DistinctCount: 2,
						NullCount:     2,
						AvgSize:       2,
					},
				},
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
						RowCount:      21,
						DistinctCount: 6,
						NullCount:     6,
						AvgSize:       6,
					},
				},
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
						RowCount:      31,
						DistinctCount: 10,
						NullCount:     10,
						AvgSize:       10,
					},
				},
			},
			at:             time.Date(2020, 1, 4, 0, 0, 0, 0, time.UTC),
			minRequiredFit: 1,
			forecast: &TableStatistic{
				TableStatisticProto: TableStatisticProto{
					Name:          jobspb.ForecastStatsName,
					ColumnIDs:     columnIDs,
					CreatedAt:     time.Date(2020, 1, 4, 0, 0, 0, 0, time.UTC),
					RowCount:      1,
					DistinctCount: 1,
					NullCount:     0,
					AvgSize:       1,
				},
			},
			score: 0,
		},
		// Test with histograms.
		{
			observed: []*TableStatistic{
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(1969, 7, 20, 20, 17, 40, 0, time.UTC),
						RowCount:      11,
						DistinctCount: 3,
						NullCount:     7,
						AvgSize:       8,
					},
					Histogram: []cat.HistogramBucket{
						{NumEq: 2, NumRange: 0, UpperBound: tree.NewDFloat(tree.DFloat(1201))},
						{NumEq: 2, NumRange: 0, UpperBound: tree.NewDFloat(tree.DFloat(1202))},
					},
				},
			},
			at:             time.Date(1969, 7, 20, 20, 17, 40, 0, time.UTC),
			minRequiredFit: 1,
			forecast: &TableStatistic{
				TableStatisticProto: TableStatisticProto{
					Name:          jobspb.ForecastStatsName,
					ColumnIDs:     columnIDs,
					CreatedAt:     time.Date(1969, 7, 20, 20, 17, 40, 0, time.UTC),
					RowCount:      11,
					DistinctCount: 3,
					NullCount:     7,
					AvgSize:       8,
				},
				Histogram: []cat.HistogramBucket{
					{NumEq: 2, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDFloat(tree.DFloat(1201))},
					{NumEq: 2, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDFloat(tree.DFloat(1202))},
				},
			},
			score: 1,
		},
		{
			observed: []*TableStatistic{
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
						RowCount:      10,
						DistinctCount: 5,
						NullCount:     2,
						AvgSize:       4,
					},
					Histogram: []cat.HistogramBucket{
						{NumEq: 2, NumRange: 0, UpperBound: tree.NewDFloat(tree.DFloat(2))},
						{NumEq: 2, NumRange: 4, UpperBound: tree.NewDFloat(tree.DFloat(4))},
					},
				},
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
						RowCount:      5,
						DistinctCount: 5,
						NullCount:     1,
						AvgSize:       2,
					},
					Histogram: []cat.HistogramBucket{
						{NumEq: 1, NumRange: 0, UpperBound: tree.NewDFloat(tree.DFloat(0))},
						{NumEq: 1, NumRange: 2, UpperBound: tree.NewDFloat(tree.DFloat(2))},
					},
				},
			},
			at:             time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC),
			minRequiredFit: 1,
			forecast: &TableStatistic{
				TableStatisticProto: TableStatisticProto{
					Name:          jobspb.ForecastStatsName,
					ColumnIDs:     columnIDs,
					CreatedAt:     time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC),
					RowCount:      15,
					DistinctCount: 5,
					NullCount:     3,
					AvgSize:       6,
				},
				Histogram: []cat.HistogramBucket{
					{NumEq: 3, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDFloat(tree.DFloat(4))},
					{NumEq: 3, NumRange: 6, DistinctRange: 2, UpperBound: tree.NewDFloat(tree.DFloat(6))},
				},
			},
			score: 1,
		},
		// Test histogram on empty tables.
		// First, nil buckets.
		{
			observed: []*TableStatistic{
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs: columnIDs,
						CreatedAt: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
						HistogramData: &HistogramData{
							ColumnType: colType,
							Version:    1,
						},
					},
				},
			},
			at:             time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
			minRequiredFit: 1,
			forecast: &TableStatistic{
				TableStatisticProto: TableStatisticProto{
					Name:      jobspb.ForecastStatsName,
					ColumnIDs: columnIDs,
					CreatedAt: time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
					HistogramData: &HistogramData{
						ColumnType: colType,
						Buckets:    make([]HistogramData_Bucket, 0),
						Version:    1,
					},
				},
				Histogram: make([]cat.HistogramBucket, 0),
			},
			score: 1,
		},
		// Zero-length buckets slice.
		{
			observed: []*TableStatistic{
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs: columnIDs,
						CreatedAt: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
						HistogramData: &HistogramData{
							ColumnType: colType,
							Buckets:    make([]HistogramData_Bucket, 0),
							Version:    1,
						},
					},
					Histogram: make([]cat.HistogramBucket, 0),
				},
			},
			at:             time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
			minRequiredFit: 1,
			forecast: &TableStatistic{
				TableStatisticProto: TableStatisticProto{
					Name:      jobspb.ForecastStatsName,
					ColumnIDs: columnIDs,
					CreatedAt: time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
					HistogramData: &HistogramData{
						ColumnType: colType,
						Buckets:    make([]HistogramData_Bucket, 0),
						Version:    1,
					},
				},
				Histogram: make([]cat.HistogramBucket, 0),
			},
			score: 1,
		},
		// One empty bucket.
		{
			observed: []*TableStatistic{
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs: columnIDs,
						CreatedAt: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
						HistogramData: &HistogramData{
							ColumnType: colType,
							Buckets: []HistogramData_Bucket{
								{UpperBound: encoding.EncodeFloatAscending(nil, 0)},
							},
							Version: 1,
						},
					},
					Histogram: []cat.HistogramBucket{
						{UpperBound: tree.NewDFloat(tree.DFloat(0))},
					},
				},
			},
			at:             time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
			minRequiredFit: 1,
			forecast: &TableStatistic{
				TableStatisticProto: TableStatisticProto{
					Name:      jobspb.ForecastStatsName,
					ColumnIDs: columnIDs,
					CreatedAt: time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
					HistogramData: &HistogramData{
						ColumnType: colType,
						Buckets:    make([]HistogramData_Bucket, 0),
						Version:    1,
					},
				},
				Histogram: make([]cat.HistogramBucket, 0),
			},
			score: 1,
		},
		// Test histogram on table that starts empty and grows.
		{
			observed: []*TableStatistic{
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC),
						RowCount:      2,
						DistinctCount: 1,
						NullCount:     0,
						AvgSize:       1,
					},
					Histogram: []cat.HistogramBucket{
						{NumEq: 2, NumRange: 0, UpperBound: tree.NewDFloat(tree.DFloat(99))},
					},
				},
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
						RowCount:      1,
						DistinctCount: 1,
						NullCount:     0,
						AvgSize:       1,
					},
					Histogram: []cat.HistogramBucket{
						{NumEq: 1, NumRange: 0, UpperBound: tree.NewDFloat(tree.DFloat(99))},
					},
				},
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
						RowCount:      0,
						DistinctCount: 0,
						NullCount:     0,
						AvgSize:       0,
						HistogramData: &HistogramData{
							ColumnType: colType,
							Buckets:    make([]HistogramData_Bucket, 0),
							Version:    1,
						},
					},
					Histogram: make([]cat.HistogramBucket, 0),
				},
			},
			at:             time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
			minRequiredFit: 1,
			forecast: &TableStatistic{
				TableStatisticProto: TableStatisticProto{
					Name:          jobspb.ForecastStatsName,
					ColumnIDs:     columnIDs,
					CreatedAt:     time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
					RowCount:      3,
					DistinctCount: 1,
					NullCount:     0,
					AvgSize:       1,
				},
				Histogram: []cat.HistogramBucket{
					{NumEq: 3, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDFloat(tree.DFloat(99))},
				},
			},
			score: 0,
		},
		// Test histogram on predicted empty table.
		{
			observed: []*TableStatistic{
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC),
						RowCount:      10,
						DistinctCount: 5,
						NullCount:     2,
						AvgSize:       4,
					},
					Histogram: []cat.HistogramBucket{
						{NumEq: 4, NumRange: 0, UpperBound: tree.NewDFloat(tree.DFloat(300))},
						{NumEq: 2, NumRange: 2, UpperBound: tree.NewDFloat(tree.DFloat(400))},
					},
				},
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
						RowCount:      30,
						DistinctCount: 9,
						NullCount:     6,
						AvgSize:       6,
					},
					Histogram: []cat.HistogramBucket{
						{NumEq: 12, NumRange: 0, UpperBound: tree.NewDFloat(tree.DFloat(200))},
						{NumEq: 6, NumRange: 6, UpperBound: tree.NewDFloat(tree.DFloat(300))},
					},
				},
				{
					TableStatisticProto: TableStatisticProto{
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
						RowCount:      50,
						DistinctCount: 13,
						NullCount:     10,
						AvgSize:       8,
					},
					Histogram: []cat.HistogramBucket{
						{NumEq: 20, NumRange: 0, UpperBound: tree.NewDFloat(tree.DFloat(100))},
						{NumEq: 10, NumRange: 10, UpperBound: tree.NewDFloat(tree.DFloat(200))},
					},
				},
			},
			at:             time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
			minRequiredFit: 1,
			forecast: &TableStatistic{
				TableStatisticProto: TableStatisticProto{
					Name:          jobspb.ForecastStatsName,
					ColumnIDs:     columnIDs,
					CreatedAt:     time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
					RowCount:      0,
					DistinctCount: 0,
					NullCount:     0,
					AvgSize:       0,
					HistogramData: &HistogramData{
						ColumnType: colType,
						Buckets:    make([]HistogramData_Bucket, 0),
						Version:    1,
					},
				},
				Histogram: make([]cat.HistogramBucket, 0),
			},
			score: 1,
		},
		// Test missing histograms.
		/*
			{
				observed: []*TableStatistic{
					{
						TableStatisticProto: TableStatisticProto{
							ColumnIDs:     columnIDs,
							CreatedAt:     time.Date(2022, 1, 3, 0, 0, 0, 0, time.UTC),
							RowCount:      30,
							DistinctCount: 22,
							NullCount:     3,
							AvgSize:       3,
						},
						Histogram: []cat.HistogramBucket{
							{NumEq: 3, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDFloat(tree.DFloat(200))},
							{NumEq: 3, NumRange: 9, DistinctRange: 9, UpperBound: tree.NewDFloat(tree.DFloat(300))},
							{NumEq: 3, NumRange: 9, DistinctRange: 9, UpperBound: tree.NewDFloat(tree.DFloat(400))},
						},
					},
					{
						TableStatisticProto: TableStatisticProto{
							ColumnIDs:     columnIDs,
							CreatedAt:     time.Date(2022, 1, 2, 0, 0, 0, 0, time.UTC),
							RowCount:      20,
							DistinctCount: 16,
							NullCount:     2,
							AvgSize:       2,
						},
						Histogram: []cat.HistogramBucket{
							{NumEq: 2, NumRange: 0, UpperBound: tree.NewDFloat(tree.DFloat(100))},
							{NumEq: 2, NumRange: 6, UpperBound: tree.NewDFloat(tree.DFloat(200))},
							{NumEq: 2, NumRange: 6, UpperBound: tree.NewDFloat(tree.DFloat(300))},
						},
					},
					{
						TableStatisticProto: TableStatisticProto{
							ColumnIDs:     columnIDs,
							CreatedAt:     time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
							RowCount:      10,
							DistinctCount: 10,
							NullCount:     1,
							AvgSize:       1,
						},
					},
				},
				at:             time.Date(2022, 1, 4, 0, 0, 0, 0, time.UTC),
				minRequiredFit: 1,
				forecast: &TableStatistic{
					TableStatisticProto: TableStatisticProto{
						Name:          jobspb.ForecastStatsName,
						ColumnIDs:     columnIDs,
						CreatedAt:     time.Date(2022, 1, 4, 0, 0, 0, 0, time.UTC),
						RowCount:      40,
						DistinctCount: 28,
						NullCount:     4,
						AvgSize:       4,
					},
					// In this case we adjust the latest histogram rather than predicting.
					Histogram: []cat.HistogramBucket{
						{NumEq: 4, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDFloat(tree.DFloat(200))},
						{NumEq: 4, NumRange: 12, DistinctRange: 12, UpperBound: tree.NewDFloat(tree.DFloat(300))},
						{NumEq: 4, NumRange: 12, DistinctRange: 12, UpperBound: tree.NewDFloat(tree.DFloat(400))},
					},
				},
				score: 0,
			},
		*/
		// Test adjustment of histogram copied from latest observed statistics.
		/*
			{
						observed: []*TableStatistic{
							{
								TableStatisticProto: TableStatisticProto{
									ColumnIDs:     columnIDs,
									CreatedAt:     time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC),
									RowCount:      ,
									DistinctCount: ,
									NullCount:     ,
									AvgSize:       ,
								},
								Histogram: []cat.HistogramBucket{
									{NumEq: , NumRange: , UpperBound: tree.NewDFloat(tree.DFloat(0))},
									{NumEq: , NumRange: , UpperBound: tree.NewDFloat(tree.DFloat(0))},
								},
							},
							{
								TableStatisticProto: TableStatisticProto{
									ColumnIDs:     columnIDs,
									CreatedAt:     time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
									RowCount:      ,
									DistinctCount: ,
									NullCount:     ,
									AvgSize:       ,
								},
								Histogram: []cat.HistogramBucket{
									{NumEq: , NumRange: , UpperBound: tree.NewDFloat(tree.DFloat(0))},
									{NumEq: , NumRange: , UpperBound: tree.NewDFloat(tree.DFloat(0))},
								},
							},
							{
								TableStatisticProto: TableStatisticProto{
									ColumnIDs:     columnIDs,
									CreatedAt:     time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
									RowCount:      ,
									DistinctCount: ,
									NullCount:     ,
									AvgSize:       ,
								},
								Histogram: []cat.HistogramBucket{
									{NumEq: , NumRange: , UpperBound: tree.NewDFloat(tree.DFloat(0))},
									{NumEq: , NumRange: , UpperBound: tree.NewDFloat(tree.DFloat(0))},
								},
							},
						},
						at:             time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
						minRequiredFit: 1,
						forecast: &TableStatistic{
							TableStatisticProto: TableStatisticProto{
								Name:          jobspb.ForecastStatsName,
								ColumnIDs:     columnIDs,
								CreatedAt:     time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
								RowCount:      ,
								DistinctCount: ,
								NullCount:     ,
								AvgSize:       ,
							},
								Histogram: []cat.HistogramBucket{
									{NumEq: , NumRange: , DistinctRange: , UpperBound: tree.NewDFloat(tree.DFloat(0))},
									{NumEq: , NumRange: , DistinctRange: , UpperBound: tree.NewDFloat(tree.DFloat(0))},
								},
						},
						score: 0,
					},
		*/
	}
	ctx := context.Background()
	evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			// Fill in HistogramData if we have a histogram.
			for _, stat := range tc.observed {
				if stat.HistogramData == nil && stat.Histogram != nil {
					hist := stat.nonNullHistogram()
					histData, err := hist.toHistogramData(colType)
					if err != nil {
						t.Errorf("test case %d unexpected toHistogramData err: %v", i, err)
						return
					}
					stat.HistogramData = &histData
					stat.setHistogramBuckets(hist)
				}
			}
			if tc.forecast.HistogramData == nil && tc.forecast.Histogram != nil {
				hist := tc.forecast.nonNullHistogram()
				histData, err := hist.toHistogramData(colType)
				if err != nil {
					t.Errorf("test case %d unexpected toHistogramData err: %v", i, err)
					return
				}
				tc.forecast.HistogramData = &histData
				tc.forecast.setHistogramBuckets(hist)
			}
			forecast, score := forecastColumnStatistics(
				ctx, evalCtx, tc.observed, tc.at, len(tc.observed), tc.minRequiredFit,
			)
			if !reflect.DeepEqual(forecast, tc.forecast) {
				t.Errorf("test case %d incorrect forecast\n%#v\nexpected\n%#v", i, forecast, tc.forecast)
			}
			if score != tc.score {
				t.Errorf("test case %d incorrect score %v expected %v", i, score, tc.score)
			}
		})
	}
}
