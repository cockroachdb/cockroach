// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testmodel

import (
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
)

// TestModelDBQuery is a high-level verification that the test model query
// system is assembled correctly. The query system is implemented primarily
// through the various immutable functions on the DataSeries type, which are
// tested extensively individually; therefore, this test seeks primarily to
// establish that those well-tested components are assembled correctly.
func TestModelDBQuery(t *testing.T) {
	db := NewModelDB()
	db.Record("testmetric", "source1", DataSeries{
		dp(0, 0.0),
		dp(100, 100.0),
		dp(200, 200.0),
		dp(400, 400.0),
	})
	db.Record("testmetric", "source2", DataSeries{
		dp(0, 0.0),
		dp(103, 50.0),
		dp(199, 150.0),
		dp(205, 400.0),
		dp(301, 600.0),
		dp(425, 800.0),
	})
	db.Record("testmetric", "source3", DataSeries{
		dp(0, 0.0),
		dp(105, 50.0),
		dp(164, 250.0),
		dp(230, 600.0),
		dp(253, 700.0),
		dp(425, 800.0),
	})

	db.Record("descendingmetric", "source1", DataSeries{
		dp(0, 400.0),
		dp(100, 300.0),
		dp(200, 200.0),
		dp(300, 400.0),
	})
	// otherMetric is recorded just to make sure that there is data in the
	// database other than the data being queried - querying this data would be a
	// false positive.
	db.Record("othermetric", "source1", DataSeries{
		dp(150, 10000),
		dp(250, 10000),
		dp(600, 5000),
	})

	for _, tc := range []struct {
		seriesName         string
		sources            []string
		downsampler        tspb.TimeSeriesQueryAggregator
		aggregator         tspb.TimeSeriesQueryAggregator
		derivative         tspb.TimeSeriesQueryDerivative
		slabDuration       int64
		sampleDuration     int64
		start              int64
		end                int64
		interpolationLimit int64
		nowNanos           int64
		expected           DataSeries
	}{
		// Basic Query, no sources specified.  Should pull information from all
		// sources for the supplied metric and apply the correct operations.
		{
			seriesName:         "testmetric",
			sources:            nil,
			downsampler:        tspb.TimeSeriesQueryAggregator_AVG,
			aggregator:         tspb.TimeSeriesQueryAggregator_SUM,
			derivative:         tspb.TimeSeriesQueryDerivative_NONE,
			slabDuration:       1000,
			sampleDuration:     100,
			start:              0,
			end:                10000,
			interpolationLimit: 10000,
			nowNanos:           math.MaxInt64,
			expected: DataSeries{
				dp(0, 0.0),      // 0 + 0 + 0
				dp(100, 350.0),  // 100 + 100 + 150
				dp(200, 1250.0), // 200 + 400 + 650
				dp(300, 1625.0), // 300 + 600 + 725
				dp(400, 2000.0), // 400 + 800 + 800
			},
		},
		// Same as first query, but with a different downsampler operation - ensures
		// that downsamplers are being selected correctly.
		{
			seriesName:         "testmetric",
			sources:            nil,
			downsampler:        tspb.TimeSeriesQueryAggregator_MAX,
			aggregator:         tspb.TimeSeriesQueryAggregator_SUM,
			derivative:         tspb.TimeSeriesQueryDerivative_NONE,
			slabDuration:       1000,
			sampleDuration:     100,
			start:              0,
			end:                10000,
			interpolationLimit: 10000,
			nowNanos:           math.MaxInt64,
			expected: DataSeries{
				dp(0, 0.0),      // 0 + 0 + 0
				dp(100, 500.0),  // 100 + 150 + 250
				dp(200, 1300.0), // 200 + 400 + 700
				dp(300, 1650.0), // 300 + 600 + 750
				dp(400, 2000.0), // 400 + 800 + 800
			},
		},
		// Same as first query, but with a different source aggregator option.
		{
			seriesName:         "testmetric",
			sources:            nil,
			downsampler:        tspb.TimeSeriesQueryAggregator_AVG,
			aggregator:         tspb.TimeSeriesQueryAggregator_MAX,
			derivative:         tspb.TimeSeriesQueryDerivative_NONE,
			slabDuration:       1000,
			sampleDuration:     100,
			start:              0,
			end:                10000,
			interpolationLimit: 10000,
			nowNanos:           math.MaxInt64,
			expected: DataSeries{
				dp(0, 0.0),     // max(0, 0, 0)
				dp(100, 150.0), // max(100, 100, 150)
				dp(200, 650.0), // max(200, 400, 650)
				dp(300, 725.0), // max(300, 600, 725)
				dp(400, 800.0), // max(400, 800, 800)
			},
		},
		// Same as first query, but with a different derivative option.
		{
			seriesName:         "testmetric",
			sources:            nil,
			downsampler:        tspb.TimeSeriesQueryAggregator_AVG,
			aggregator:         tspb.TimeSeriesQueryAggregator_SUM,
			derivative:         tspb.TimeSeriesQueryDerivative_DERIVATIVE,
			slabDuration:       1000,
			sampleDuration:     100,
			start:              0,
			end:                10000,
			interpolationLimit: 10000,
			nowNanos:           math.MaxInt64,
			expected: DataSeries{
				dp(100, 3.5e+09),
				dp(200, 9e+09),
				dp(300, 3.75e+09),
				dp(400, 3.75e+09),
			},
		},
		// Same as first query, but with interpolation disabled AND a derivative
		// option. Exercises the interplay between derivative and interpolation.
		{
			seriesName:         "testmetric",
			sources:            nil,
			downsampler:        tspb.TimeSeriesQueryAggregator_AVG,
			aggregator:         tspb.TimeSeriesQueryAggregator_SUM,
			derivative:         tspb.TimeSeriesQueryDerivative_DERIVATIVE,
			slabDuration:       1000,
			sampleDuration:     100,
			start:              0,
			end:                10000,
			interpolationLimit: 1,
			nowNanos:           math.MaxInt64,
			expected: DataSeries{
				dp(100, 3.5e+09),
				dp(200, 9e+09),
				dp(300, 2e+09),
				dp(400, 3.75e+09),
			},
		},
		// Same as first query, but with a single source specified.
		{
			seriesName:         "testmetric",
			sources:            []string{"source2"},
			downsampler:        tspb.TimeSeriesQueryAggregator_AVG,
			aggregator:         tspb.TimeSeriesQueryAggregator_SUM,
			derivative:         tspb.TimeSeriesQueryDerivative_NONE,
			slabDuration:       1000,
			sampleDuration:     100,
			start:              0,
			end:                10000,
			interpolationLimit: 10000,
			nowNanos:           math.MaxInt64,
			expected: DataSeries{
				dp(0, 0.0),
				dp(100, 100.0),
				dp(200, 400.0),
				dp(300, 600.0),
				dp(400, 800.0),
			},
		},
		// Same as first query, but with a limited time specified.
		{
			seriesName:         "testmetric",
			sources:            nil,
			downsampler:        tspb.TimeSeriesQueryAggregator_AVG,
			aggregator:         tspb.TimeSeriesQueryAggregator_SUM,
			derivative:         tspb.TimeSeriesQueryDerivative_NONE,
			slabDuration:       1000,
			sampleDuration:     100,
			start:              200,
			end:                300,
			interpolationLimit: 10000,
			nowNanos:           math.MaxInt64,
			expected: DataSeries{
				dp(200, 1250.0), // 200 + 400 + 650
				dp(300, 1625.0), // 300 + 600 + 725
			},
		},
		// Same as first query, but with a different sample duration.
		{
			seriesName:         "testmetric",
			sources:            nil,
			downsampler:        tspb.TimeSeriesQueryAggregator_AVG,
			aggregator:         tspb.TimeSeriesQueryAggregator_SUM,
			derivative:         tspb.TimeSeriesQueryDerivative_NONE,
			slabDuration:       1000,
			sampleDuration:     150,
			start:              0,
			end:                10000,
			interpolationLimit: 10000,
			nowNanos:           math.MaxInt64,
			expected: DataSeries{
				dp(0, 100.0),               // 50 + 25 + 25
				dp(150, 991.6666666666666), // 200 + 275 + 516.666667
				dp(300, 1900.0),            // 400 + 700 + 800
			},
		},
		// Same as first query, but with interpolation disabled.
		{
			seriesName:         "testmetric",
			sources:            nil,
			downsampler:        tspb.TimeSeriesQueryAggregator_AVG,
			aggregator:         tspb.TimeSeriesQueryAggregator_SUM,
			derivative:         tspb.TimeSeriesQueryDerivative_NONE,
			slabDuration:       1000,
			sampleDuration:     100,
			start:              0,
			end:                10000,
			interpolationLimit: 1,
			nowNanos:           math.MaxInt64,
			expected: DataSeries{
				dp(0, 0.0),      // 0 + 0 + 0
				dp(100, 350.0),  // 100 + 100 + 150
				dp(200, 1250.0), // 200 + 400 + 650
				dp(300, 600.0),  // 0 + 600 + 0
				dp(400, 2000.0), // 400 + 800 + 800
			},
		},
		// Same as first query, but with a different metric that has no data.
		{
			seriesName:         "wrongmetric",
			sources:            nil,
			downsampler:        tspb.TimeSeriesQueryAggregator_AVG,
			aggregator:         tspb.TimeSeriesQueryAggregator_SUM,
			derivative:         tspb.TimeSeriesQueryDerivative_NONE,
			slabDuration:       1000,
			sampleDuration:     100,
			start:              0,
			end:                10000,
			interpolationLimit: 10000,
			nowNanos:           math.MaxInt64,
			expected:           nil,
		},
		// Testing Non-negative derivative.
		{
			seriesName:         "descendingmetric",
			sources:            nil,
			downsampler:        tspb.TimeSeriesQueryAggregator_AVG,
			aggregator:         tspb.TimeSeriesQueryAggregator_SUM,
			derivative:         tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE,
			slabDuration:       1000,
			sampleDuration:     100,
			start:              0,
			end:                10000,
			interpolationLimit: 10000,
			nowNanos:           math.MaxInt64,
			expected: DataSeries{
				dp(100, 0.0),
				dp(200, 0.0),
				dp(300, 2e+09),
			},
		},
		// Specialty Feature: Testing the NowNanos cutoff, which disallows queries
		// in the future.
		{
			seriesName:         "testmetric",
			sources:            nil,
			downsampler:        tspb.TimeSeriesQueryAggregator_AVG,
			aggregator:         tspb.TimeSeriesQueryAggregator_SUM,
			derivative:         tspb.TimeSeriesQueryDerivative_NONE,
			slabDuration:       1000,
			sampleDuration:     100,
			start:              0,
			end:                10000,
			interpolationLimit: 10000,
			nowNanos:           300,
			expected: DataSeries{
				dp(0, 0.0),
				dp(100, 350.0),
				dp(200, 1250.0),
			},
		},
		// Specialty Feature: Testing the NowNanos cutoff in, which disallows queries
		// in the future, in combination with downsampling.
		{
			seriesName:         "testmetric",
			sources:            nil,
			downsampler:        tspb.TimeSeriesQueryAggregator_AVG,
			aggregator:         tspb.TimeSeriesQueryAggregator_SUM,
			derivative:         tspb.TimeSeriesQueryDerivative_NONE,
			slabDuration:       1000,
			sampleDuration:     250,
			start:              0,
			end:                10000,
			interpolationLimit: 10000,
			nowNanos:           300,
			expected: DataSeries{
				dp(0, 475.0),
			},
		},
		// Final test: Same metric as first query, but an entirely different set
		// of parameters.
		{
			seriesName:         "testmetric",
			sources:            []string{"source1", "source3"},
			downsampler:        tspb.TimeSeriesQueryAggregator_LAST,
			aggregator:         tspb.TimeSeriesQueryAggregator_AVG,
			derivative:         tspb.TimeSeriesQueryDerivative_DERIVATIVE,
			slabDuration:       1000,
			sampleDuration:     150,
			start:              100,
			end:                500,
			interpolationLimit: 10000,
			nowNanos:           math.MaxInt64,
			expected: DataSeries{
				dp(150, 2.5000000000000005e+9), // avg(dx(200 - 100), dx(700 - 50)) = avg(6.6666e+8, 4.3333 e+9)
				dp(300, 1.0000000000000001e+9), // avg(dx(400 - 200), dx(800-700)) = avg(1.3333e+9, 0.66666e+8)
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			result := db.Query(
				tc.seriesName,
				tc.sources,
				tc.downsampler,
				tc.aggregator,
				tc.derivative,
				tc.slabDuration,
				tc.sampleDuration,
				tc.start,
				tc.end,
				tc.interpolationLimit,
				tc.nowNanos,
			)
			if a, e := result, tc.expected; !reflect.DeepEqual(a, e) {
				t.Errorf("query got result %v, wanted %v", a, e)
			}
		})
	}
}

// TestModelDBDownsamplers ensures that all downsamplers are hooked up correctly
// in the test model.
func TestModelDBDownsamplers(t *testing.T) {
	db := NewModelDB()
	data := DataSeries{
		dp(0, 0.0),
		dp(50, 100.0),
		dp(100, 200.0),
		dp(150, 600.0),
	}
	db.Record("testmetric", "source1", data)
	for _, test := range []struct {
		fn          aggFunc
		downsampler tspb.TimeSeriesQueryAggregator
	}{
		{
			fn:          AggregateAverage,
			downsampler: tspb.TimeSeriesQueryAggregator_AVG,
		},
		{
			fn:          AggregateSum,
			downsampler: tspb.TimeSeriesQueryAggregator_SUM,
		},
		{
			fn:          AggregateMin,
			downsampler: tspb.TimeSeriesQueryAggregator_MIN,
		},
		{
			fn:          AggregateMax,
			downsampler: tspb.TimeSeriesQueryAggregator_MAX,
		},
		{
			fn:          AggregateFirst,
			downsampler: tspb.TimeSeriesQueryAggregator_FIRST,
		},
		{
			fn:          AggregateLast,
			downsampler: tspb.TimeSeriesQueryAggregator_LAST,
		},
		{
			fn:          AggregateVariance,
			downsampler: tspb.TimeSeriesQueryAggregator_VARIANCE,
		},
	} {
		t.Run(test.downsampler.String(), func(t *testing.T) {
			expected := data.GroupByResolution(100, test.fn)
			actual := db.Query(
				"testmetric",
				nil,
				test.downsampler,
				tspb.TimeSeriesQueryAggregator_SUM,
				tspb.TimeSeriesQueryDerivative_NONE,
				1000,
				100,
				0,
				200,
				10000,
				math.MaxInt64,
			)
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Query() got %v, wanted %v", actual, expected)
			}
		})
	}
}

// TestModelDBAggregators ensures that all source aggregators are hooked up
// correctly in the test model.
func TestModelDBAggregators(t *testing.T) {
	db := NewModelDB()
	data := DataSeries{
		dp(0, 0.0),
		dp(50, 100.0),
		dp(100, 200.0),
		dp(150, 600.0),
	}
	data2 := DataSeries{
		dp(0, 100.0),
		dp(50, 200.0),
		dp(100, 400.0),
		dp(150, 800.0),
	}
	db.Record("testmetric", "source1", data)
	db.Record("testmetric", "source2", data2)
	for _, test := range []struct {
		fn         aggFunc
		aggregator tspb.TimeSeriesQueryAggregator
	}{
		{
			fn:         AggregateAverage,
			aggregator: tspb.TimeSeriesQueryAggregator_AVG,
		},
		{
			fn:         AggregateSum,
			aggregator: tspb.TimeSeriesQueryAggregator_SUM,
		},
		{
			fn:         AggregateMin,
			aggregator: tspb.TimeSeriesQueryAggregator_MIN,
		},
		{
			fn:         AggregateMax,
			aggregator: tspb.TimeSeriesQueryAggregator_MAX,
		},
		{
			fn:         AggregateVariance,
			aggregator: tspb.TimeSeriesQueryAggregator_VARIANCE,
		},
		// First and Last are not valid for source aggregation, as sources are not
		// ordered.
	} {
		t.Run(test.aggregator.String(), func(t *testing.T) {
			expected := groupSeriesByTimestamp([]DataSeries{data, data2}, test.fn)
			actual := db.Query(
				"testmetric",
				nil,
				tspb.TimeSeriesQueryAggregator_LAST,
				test.aggregator,
				tspb.TimeSeriesQueryDerivative_NONE,
				1000,          /* slabduration */
				1,             /* sampleDuration */
				0,             /* start */
				200,           /* end */
				10000,         /* interpolationLimitNanos */
				math.MaxInt64, /* nowNanos */
			)
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Query() got %v, wanted %v", actual, expected)
			}
		})
	}
}
