// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ts

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// The data for the model test is generated for two metrics with four sources
// each. The values are randomly generated for each timestamp, but we do input
// a bitmap of "gaps" which determine where each data series is missing data.
// This allows us to exercise the various gap-filling strategies of the time
// series system.
//
// In the bitmap below, each column represents one of the eight series. Data
// points will be generated for each sample period in order, with each row
// representing one sample period. A 1 in a column means that a series gets a
// point for that sample period; a zero represents a gap.
var modelTestGapBitmap = [][8]int{
	{1, 1, 1, 1, 1, 1, 0, 1},
	{1, 1, 1, 1, 1, 1, 1, 1},
	{1, 1, 1, 1, 0, 1, 1, 1},
	{1, 1, 1, 1, 1, 1, 1, 1},
	{1, 0, 1, 1, 1, 1, 1, 1},
	{1, 1, 1, 1, 1, 1, 1, 1},
	{1, 1, 1, 1, 1, 1, 1, 1},
	{1, 1, 1, 1, 1, 0, 1, 1},
	{1, 1, 1, 1, 1, 1, 1, 1},
	{1, 1, 0, 1, 1, 1, 1, 1},
	{1, 0, 1, 1, 1, 1, 1, 1},
	{1, 0, 1, 1, 1, 1, 1, 1},
	{1, 0, 0, 1, 0, 1, 1, 1},
	{1, 0, 1, 1, 1, 1, 1, 1},
	{1, 0, 1, 1, 1, 1, 1, 1},
	{1, 1, 1, 1, 1, 1, 1, 1},
	{1, 1, 1, 1, 1, 1, 1, 1},
	{1, 1, 1, 1, 1, 1, 1, 1},
	{1, 1, 1, 0, 1, 1, 1, 1},
	{1, 1, 1, 1, 1, 1, 0, 1},
	{1, 1, 1, 1, 1, 1, 0, 1},
	{1, 1, 1, 1, 1, 1, 0, 1},
	{1, 1, 1, 1, 1, 1, 0, 1},
	{0, 0, 0, 0, 0, 0, 0, 0},
	{1, 1, 1, 1, 1, 1, 0, 1},
	{1, 1, 1, 1, 1, 1, 0, 1},
	{1, 1, 1, 1, 1, 1, 0, 1},
	{1, 1, 1, 1, 1, 0, 1, 1},
	{1, 1, 1, 1, 1, 1, 1, 1},
	{0, 0, 0, 1, 1, 1, 1, 1},
	{1, 1, 1, 1, 1, 1, 1, 1},
	{1, 1, 1, 1, 1, 1, 1, 1},
	{1, 1, 1, 1, 1, 0, 1, 1},
	{1, 1, 1, 1, 1, 1, 1, 1},
	{1, 0, 1, 1, 1, 1, 1, 1},
	{1, 0, 1, 1, 1, 1, 1, 1},
	{1, 0, 0, 1, 0, 1, 1, 1},
	{1, 0, 1, 1, 1, 1, 1, 1},
	{1, 0, 1, 1, 1, 1, 1, 1},
	{1, 1, 1, 1, 1, 1, 1, 1},
	{1, 1, 1, 1, 1, 0, 0, 0},
	{1, 1, 1, 1, 1, 1, 1, 1},
	{1, 1, 1, 0, 1, 1, 1, 1},
	{1, 1, 1, 1, 1, 1, 0, 1},
	{1, 1, 1, 1, 1, 1, 0, 1},
	{1, 1, 1, 1, 1, 1, 0, 1},
	{0, 0, 0, 0, 0, 0, 0, 0},
	{0, 0, 0, 0, 0, 0, 0, 0},
	{1, 1, 1, 1, 1, 1, 0, 1},
	{1, 1, 1, 1, 1, 1, 0, 1},
	{1, 1, 1, 1, 1, 1, 1, 1},
	{1, 1, 1, 1, 1, 1, 1, 1},
	{0, 0, 0, 1, 1, 1, 1, 1},
	{1, 0, 1, 1, 1, 1, 1, 1},
	{1, 1, 0, 1, 1, 0, 1, 1},
	{1, 1, 1, 1, 0, 0, 1, 1},
	{1, 1, 1, 1, 1, 0, 1, 1},
	{1, 1, 1, 1, 1, 0, 1, 1},
	{1, 1, 1, 1, 1, 0, 1, 1},
	{1, 1, 0, 1, 1, 0, 1, 1},
}

var modelTestSourceNames = []string{
	"source1",
	"source2",
	"source3",
	"source4",
}

var modelTestMetricNames = []string{
	"metric1",
	"metric2",
}

// modelTestDownsamplers are the downsamplers that will be exercised during
// the model test.
var modelTestDownsamplers = []tspb.TimeSeriesQueryAggregator{
	tspb.TimeSeriesQueryAggregator_AVG,
	tspb.TimeSeriesQueryAggregator_SUM,
	tspb.TimeSeriesQueryAggregator_MAX,
	tspb.TimeSeriesQueryAggregator_MIN,
}

// modelTestAggregators are the source aggregators that will be exercised during
// the model test.
var modelTestAggregators = []tspb.TimeSeriesQueryAggregator{
	tspb.TimeSeriesQueryAggregator_AVG,
	tspb.TimeSeriesQueryAggregator_SUM,
	tspb.TimeSeriesQueryAggregator_MAX,
	tspb.TimeSeriesQueryAggregator_MIN,
}

// modelTestDerivatives are the derivative options that will be exercised during
// the model test.
var modelTestDerivatives = []tspb.TimeSeriesQueryDerivative{
	tspb.TimeSeriesQueryDerivative_NONE,
	tspb.TimeSeriesQueryDerivative_DERIVATIVE,
	tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE,
}

// modelTestInterpolationLimits are the various interpolation limits that will
// be exercised.
var modelTestInterpolationLimits = []int64{
	time.Second.Nanoseconds() * 1,
	time.Minute.Nanoseconds(),     // Only two gaps in the bitmap exceed this limit.
	5 * time.Minute.Nanoseconds(), // No gaps exceed this length.
}

// modelTestSampleDurations are the various sample durations that will be
// exercised.
var modelTestSampleDurations = []int64{
	Resolution10s.SampleDuration(),
	Resolution10s.SampleDuration() * 3,
	Resolution10s.SlabDuration(),
}

// modelTestRowCount is the number of sample periods that will be filled for
// the test.
var modelTestRowCount = len(modelTestGapBitmap)

// modelTestStartTime, the first time at which data is recorded in the model, is
// set at a reasonably modern time and configured to overlap a slab boundary.
// This is accomplished by computing an anchor time, and then putting the start
// time earlier than the anchor time such that half of recorded samples occur
// before the anchor time.
var modelTestAnchorTime = time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
var modelTestStartTime = modelTestAnchorTime - int64(modelTestRowCount/2)*Resolution10s.SampleDuration()

// modelTestQueryTimes are the bounds that will be queried for the tests.
var modelTestQueryTimes = []struct {
	start int64
	end   int64
}{
	{
		start: modelTestStartTime,
		end:   modelTestStartTime + time.Hour.Nanoseconds(),
	},
	{
		start: modelTestStartTime + 20*Resolution10s.SampleDuration(),
		end:   modelTestStartTime + 35*Resolution10s.SampleDuration(),
	},
}

func TestTimeSeriesModelTest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tm := newTestModelRunner(t)
	tm.Start()
	defer tm.Stop()

	s1 := rand.NewSource(timeutil.Now().UnixNano())
	r1 := rand.New(s1)

	// populate model with random values according to gap bitmap.
	for rowNum := 0; rowNum <= modelTestRowCount; rowNum++ {
		for metricNum, metric := range modelTestMetricNames {
			for sourceNum, source := range modelTestSourceNames {
				// Check the gap bitmap to see if this sample period and source get a
				// data point.
				if modelTestGapBitmap[rowNum%len(modelTestGapBitmap)][4*metricNum+sourceNum] > 0 {
					tm.storeTimeSeriesData(Resolution10s, []tspb.TimeSeriesData{
						tsd(metric, source,
							tsdp(getSampleTime(rowNum), math.Floor(r1.Float64()*10000)),
						),
					})
				}
			}
		}
	}

	tm.assertModelCorrect()
	{
		// Sanity check: model should contain a datapoint for all but two sample
		// periods (one period is fully missing from the gap bitmap).
		query := tm.makeQuery(
			modelTestMetricNames[0], Resolution10s, modelTestStartTime, modelTestStartTime+time.Hour.Nanoseconds(),
		)
		query.assertSuccess(len(modelTestGapBitmap)-2, 4)
	}

	executeQueryMatrix(t, tm)
}

func TestTimeSeriesRollupModelTest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tm := newTestModelRunner(t)
	tm.Start()
	defer tm.Stop()

	s1 := rand.NewSource(timeutil.Now().UnixNano())
	r1 := rand.New(s1)

	// populate model with random values according to gap bitmap.
	for rowNum := 0; rowNum <= modelTestRowCount; rowNum++ {
		for metricNum, metric := range modelTestMetricNames {
			for sourceNum, source := range modelTestSourceNames {
				// Check the gap bitmap to see if this sample period and source get a
				// data point.
				if modelTestGapBitmap[rowNum%len(modelTestGapBitmap)][4*metricNum+sourceNum] > 0 {
					tm.storeTimeSeriesData(Resolution10s, []tspb.TimeSeriesData{
						tsd(metric, source,
							tsdp(getSampleTime(rowNum), math.Floor(r1.Float64()*10000)),
						),
					})
				}
			}
		}
	}

	tm.maintain(modelTestAnchorTime + resolution10sDefaultRollupThreshold.Nanoseconds())

	tm.assertModelCorrect()
	{
		// Sanity check: after the rollup, the 10s resolution should only have half
		// of its data points.
		query := tm.makeQuery(
			modelTestMetricNames[0], Resolution10s, modelTestStartTime, modelTestStartTime+time.Hour.Nanoseconds(),
		)
		query.assertSuccess(modelTestRowCount/2-1, 4)
	}
	{
		// Sanity check: after the rollup, the 30m resolution should contain a
		// single data point.
		query := tm.makeQuery(
			modelTestMetricNames[0], Resolution30m, modelTestStartTime, modelTestStartTime+time.Hour.Nanoseconds(),
		)
		query.assertSuccess(1, 4)
	}

	executeQueryMatrix(t, tm)
}

// getSampleTime returns the timestamp for the numbered sample period in the
// model test.
func getSampleTime(n int) time.Duration {
	return time.Duration(modelTestStartTime + int64(n)*Resolution10s.SampleDuration())
}

func executeQueryMatrix(t *testing.T, tm testModelRunner) {
	for _, metric := range modelTestMetricNames {
		for _, downsampler := range modelTestDownsamplers {
			for _, aggregator := range modelTestAggregators {
				for _, derivative := range modelTestDerivatives {
					for _, interpolationLimit := range modelTestInterpolationLimits {
						for _, sampleDuration := range modelTestSampleDurations {
							for _, queryRange := range modelTestQueryTimes {
								for srcCount := 0; srcCount <= len(modelTestSourceNames); srcCount++ {
									query := tm.makeQuery(
										metric, Resolution10s, queryRange.start, queryRange.end,
									)
									query.setDownsampler(downsampler)
									query.setSourceAggregator(aggregator)
									query.setDerivative(derivative)
									query.Sources = modelTestSourceNames[:srcCount]
									query.InterpolationLimitNanos = interpolationLimit
									query.SampleDurationNanos = sampleDuration
									query.assertMatchesModel()
								}
							}
						}
					}
				}
			}
		}
	}
}
