// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ts

import (
	"context"
	"math"
	"runtime"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// runTestCaseMultipleFormats runs the provided test case body against a
// testModelRunner configured to run in row format, and then again against a
// cluster configured to run in column format.
func runTestCaseMultipleFormats(t *testing.T, testCase func(*testing.T, testModelRunner)) {
	t.Run("Row Format", func(t *testing.T) {
		tm := newTestModelRunner(t)
		tm.Start()
		tm.DB.forceRowFormat = true
		defer tm.Stop()
		testCase(t, tm)
	})

	t.Run("Column Format", func(t *testing.T) {
		tm := newTestModelRunner(t)
		tm.Start()
		defer tm.Stop()
		testCase(t, tm)
	})
}

// TestQueryBasic validates that query results match the expectation of the test
// model in basic situations.
func TestQueryBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runTestCaseMultipleFormats(t, func(t *testing.T, tm testModelRunner) {
		tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{
			{
				Name: "test.metric",
				Datapoints: []tspb.TimeSeriesDatapoint{
					datapoint(1, 100),
					datapoint(5, 200),
					datapoint(15, 300),
					datapoint(16, 400),
					datapoint(17, 500),
					datapoint(22, 600),
					datapoint(52, 900),
				},
			},
		})
		tm.assertKeyCount(4)
		tm.assertModelCorrect()

		query := tm.makeQuery("test.metric", resolution1ns, 0, 60)
		query.assertSuccess(7, 1)

		// Verify across multiple sources
		tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{
			{
				Name:   "test.multimetric",
				Source: "source1",
				Datapoints: []tspb.TimeSeriesDatapoint{
					datapoint(1, 100),
					datapoint(15, 300),
					datapoint(17, 500),
					datapoint(52, 900),
				},
			},
			{
				Name:   "test.multimetric",
				Source: "source2",
				Datapoints: []tspb.TimeSeriesDatapoint{
					datapoint(5, 100),
					datapoint(16, 300),
					datapoint(22, 500),
					datapoint(82, 900),
				},
			},
		})

		tm.assertKeyCount(11)
		tm.assertModelCorrect()

		// Test default query: avg downsampler, sum aggregator, no derivative.
		query = tm.makeQuery("test.multimetric", resolution1ns, 0, 90)
		query.assertSuccess(8, 2)
		// Test with aggregator specified.
		query.setSourceAggregator(tspb.TimeSeriesQueryAggregator_MAX)
		query.assertSuccess(8, 2)
		// Test with aggregator and downsampler.
		query.setDownsampler(tspb.TimeSeriesQueryAggregator_AVG)
		query.assertSuccess(8, 2)
		// Test with derivative specified.
		query.Downsampler = nil
		query.setDerivative(tspb.TimeSeriesQueryDerivative_DERIVATIVE)
		query.assertSuccess(7, 2)
		// Test with everything specified.
		query = tm.makeQuery("test.multimetric", resolution1ns, 0, 90)
		query.setSourceAggregator(tspb.TimeSeriesQueryAggregator_MIN)
		query.setDownsampler(tspb.TimeSeriesQueryAggregator_MAX)
		query.setDerivative(tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE)
		query.assertSuccess(7, 2)

		// Test queries that return no data. Check with every
		// aggregator/downsampler/derivative combination. This situation is
		// particularly prone to nil panics (parts of the query system will not have
		// data).
		aggs := []tspb.TimeSeriesQueryAggregator{
			tspb.TimeSeriesQueryAggregator_MIN, tspb.TimeSeriesQueryAggregator_MAX,
			tspb.TimeSeriesQueryAggregator_AVG, tspb.TimeSeriesQueryAggregator_SUM,
		}
		derivs := []tspb.TimeSeriesQueryDerivative{
			tspb.TimeSeriesQueryDerivative_NONE, tspb.TimeSeriesQueryDerivative_DERIVATIVE,
			tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE,
		}
		query = tm.makeQuery("nodata", resolution1ns, 0, 90)
		for _, downsampler := range aggs {
			for _, agg := range aggs {
				for _, deriv := range derivs {
					query.setDownsampler(downsampler)
					query.setSourceAggregator(agg)
					query.setDerivative(deriv)
					query.assertSuccess(0, 0)
				}
			}
		}

		// Verify querying specific sources, thus excluding other available sources
		// in the same time period.
		tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{
			{
				Name:   "test.specificmetric",
				Source: "source1",
				Datapoints: []tspb.TimeSeriesDatapoint{
					datapoint(1, 9999),
					datapoint(11, 9999),
					datapoint(21, 9999),
					datapoint(31, 9999),
				},
			},
			{
				Name:   "test.specificmetric",
				Source: "source2",
				Datapoints: []tspb.TimeSeriesDatapoint{
					datapoint(2, 10),
					datapoint(12, 15),
					datapoint(22, 25),
					datapoint(32, 60),
				},
			},
			{
				Name:   "test.specificmetric",
				Source: "source3",
				Datapoints: []tspb.TimeSeriesDatapoint{
					datapoint(3, 9999),
					datapoint(13, 9999),
					datapoint(23, 9999),
					datapoint(33, 9999),
				},
			},
			{
				Name:   "test.specificmetric",
				Source: "source4",
				Datapoints: []tspb.TimeSeriesDatapoint{
					datapoint(4, 15),
					datapoint(14, 45),
					datapoint(24, 60),
					datapoint(32, 100),
				},
			},
			{
				Name:   "test.specificmetric",
				Source: "source5",
				Datapoints: []tspb.TimeSeriesDatapoint{
					datapoint(5, 9999),
					datapoint(15, 9999),
					datapoint(25, 9999),
					datapoint(35, 9999),
				},
			},
		})

		tm.assertKeyCount(31)
		tm.assertModelCorrect()

		// Assert querying data from subset of sources. Includes source with no
		// data.
		query = tm.makeQuery("test.specificmetric", resolution1ns, 0, 90)
		query.Sources = []string{"source2", "source4", "source6"}
		query.assertSuccess(7, 2)

		// Assert querying data over limited range for single source. Regression
		// test for #4987.
		query.Sources = []string{"source4", "source5"}
		query.QueryTimespan = QueryTimespan{
			StartNanos:          5,
			EndNanos:            24,
			SampleDurationNanos: 1,
			NowNanos:            math.MaxInt64,
		}
		query.assertSuccess(4, 2)
	})
}

// TestQueryDownsampling validates that query results match the expectation of
// the test model.
func TestQueryDownsampling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runTestCaseMultipleFormats(t, func(t *testing.T, tm testModelRunner) {
		// Query with sampleDuration that is too small, expect error.
		query := tm.makeQuery("", Resolution10s, 0, 10000)
		query.SampleDurationNanos = 1
		query.assertError("was not less")

		// Query with sampleDuration which is not an even multiple of the resolution.
		query.SampleDurationNanos = Resolution10s.SampleDuration() + 1
		query.assertError("not a multiple")

		tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{
			{
				Name:   "test.metric",
				Source: "source1",
				Datapoints: []tspb.TimeSeriesDatapoint{
					datapoint(1, 100),
					datapoint(5, 500),
					datapoint(15, 500),
					datapoint(16, 600),
					datapoint(17, 700),
					datapoint(22, 200),
					datapoint(45, 500),
					datapoint(46, 600),
					datapoint(52, 200),
				},
			},
			{
				Name:   "test.metric",
				Source: "source2",
				Datapoints: []tspb.TimeSeriesDatapoint{
					datapoint(7, 0),
					datapoint(7, 700),
					datapoint(9, 900),
					datapoint(14, 400),
					datapoint(18, 800),
					datapoint(33, 300),
					datapoint(34, 400),
					datapoint(56, 600),
					datapoint(59, 900),
				},
			},
		})
		tm.assertKeyCount(9)
		tm.assertModelCorrect()

		query = tm.makeQuery("test.metric", resolution1ns, 0, 60)
		query.SampleDurationNanos = 10
		query.assertSuccess(6, 2)

		query.Sources = []string{"source1"}
		query.assertSuccess(5, 1)

		query.Sources = []string{"source2"}
		query.assertSuccess(4, 1)
	})
}

// TestInterpolationLimit validates that query results match the expectation of
// the test model.
func TestInterpolationLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runTestCaseMultipleFormats(t, func(t *testing.T, tm testModelRunner) {
		// Metric with gaps at the edge of a queryable range.
		// The first source has missing data points at 14 and 19, which can
		// be interpolated from data points located in nearby slabs.
		// 5 - [15, 16, 17, 18] - 25
		tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{
			{
				Name:   "metric.edgegaps",
				Source: "source1",
				Datapoints: []tspb.TimeSeriesDatapoint{
					datapoint(5, 500),
					datapoint(15, 1500),
					datapoint(16, 1600),
					datapoint(17, 1700),
					datapoint(18, 1800),
					datapoint(25, 2500),
				},
			},
			{
				Name:   "metric.edgegaps",
				Source: "source2",
				Datapoints: []tspb.TimeSeriesDatapoint{
					datapoint(14, 1000),
					datapoint(15, 1000),
					datapoint(16, 1000),
					datapoint(17, 1000),
					datapoint(18, 1000),
					datapoint(19, 1000),
				},
			},
		})
		tm.assertKeyCount(4)
		tm.assertModelCorrect()

		{
			query := tm.makeQuery("metric.edgegaps", resolution1ns, 14, 19)
			query.assertSuccess(6, 2)
			query.InterpolationLimitNanos = 10
			query.assertSuccess(6, 2)
			query.Sources = []string{"source1", "source2"}
			query.assertSuccess(6, 2)
		}

		// Metric with inner gaps which may be effected by the interpolation limit.
		tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{
			{
				Name:   "metric.innergaps",
				Source: "source1",
				Datapoints: []tspb.TimeSeriesDatapoint{
					datapoint(1, 100),
					datapoint(2, 200),
					datapoint(4, 400),
					datapoint(7, 700),
					datapoint(10, 1000),
				},
			},
			{
				Name:   "metric.innergaps",
				Source: "source2",
				Datapoints: []tspb.TimeSeriesDatapoint{
					datapoint(1, 100),
					datapoint(2, 100),
					datapoint(3, 100),
					datapoint(4, 100),
					datapoint(5, 100),
					datapoint(6, 100),
					datapoint(7, 100),
					datapoint(8, 100),
					datapoint(9, 100),
				},
			},
		})
		tm.assertKeyCount(7)
		tm.assertModelCorrect()

		// Interpolation limit 0, 2, 3, and 10.
		{
			query := tm.makeQuery("metric.innergaps", resolution1ns, 0, 9)
			query.assertSuccess(9, 2)
			query.InterpolationLimitNanos = 2
			query.assertSuccess(9, 2)
			query.InterpolationLimitNanos = 3
			query.assertSuccess(9, 2)
			query.InterpolationLimitNanos = 10
			query.assertSuccess(9, 2)
		}

		// With explicit source list.
		{
			query := tm.makeQuery("metric.innergaps", resolution1ns, 0, 9)
			query.Sources = []string{"source1", "source2"}
			query.assertSuccess(9, 2)
			query.InterpolationLimitNanos = 2
			query.assertSuccess(9, 2)
			query.InterpolationLimitNanos = 3
			query.assertSuccess(9, 2)
			query.InterpolationLimitNanos = 10
			query.assertSuccess(9, 2)
		}
	})
}

func TestQueryWorkerMemoryConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runTestCaseMultipleFormats(t, func(t *testing.T, tm testModelRunner) {
		generateData := func(dps int64) []tspb.TimeSeriesDatapoint {
			result := make([]tspb.TimeSeriesDatapoint, 0, dps)
			var i int64
			for i = 0; i < dps; i++ {
				result = append(result, datapoint(i, float64(100*i)))
			}
			return result
		}

		// Store data for a large metric across many keys, so we can test across
		// many different memory maximums.
		tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{
			{
				Name:       "test.metric",
				Source:     "source1",
				Datapoints: generateData(120),
			},
			{
				Name:       "test.metric",
				Source:     "source2",
				Datapoints: generateData(120),
			},
			{
				Name:       "test.metric",
				Source:     "source3",
				Datapoints: generateData(120),
			},
		})
		tm.assertKeyCount(36)
		tm.assertModelCorrect()

		// Track the total maximum memory used for a query with no budget.
		{
			// Swap model's memory monitor in order to adjust allocation size.
			adjustedMon := mon.MakeMonitor(
				"timeseries-test-worker-adjusted",
				mon.MemoryResource,
				nil,
				nil,
				1,
				math.MaxInt64,
				cluster.MakeTestingClusterSettings(),
			)
			adjustedMon.Start(context.TODO(), tm.workerMemMonitor, mon.BoundAccount{})
			defer adjustedMon.Stop(context.TODO())

			query := tm.makeQuery("test.metric", resolution1ns, 11, 109)
			query.workerMemMonitor = &adjustedMon
			query.InterpolationLimitNanos = 10
			query.assertSuccess(99, 3)
			memoryUsed := adjustedMon.MaximumBytes()

			for _, limit := range []int64{
				memoryUsed,
				memoryUsed / 2,
				memoryUsed / 3,
			} {
				// Limit memory in use by model. Reset memory monitor to get new maximum.
				adjustedMon.Stop(context.TODO())
				adjustedMon.Start(context.TODO(), tm.workerMemMonitor, mon.BoundAccount{})
				if adjustedMon.MaximumBytes() != 0 {
					t.Fatalf("maximum bytes was %d, wanted zero", adjustedMon.MaximumBytes())
				}

				query.BudgetBytes = limit
				query.assertSuccess(99, 3)

				// Expected maximum usage may slightly exceed the budget due to the size of
				// dataSpan structures which are not accounted for in getMaxTimespan.
				if a, e := adjustedMon.MaximumBytes(), limit; a > e {
					t.Fatalf("memory usage for query was %d, exceeded set maximum limit %d", a, e)
				}

				// As an additional check, ensure that maximum bytes used was within 5% of memory budget;
				// we want to use as much memory as we can to ensure the fastest possible queries.
				if a, e := float64(adjustedMon.MaximumBytes()), float64(limit)*0.95; a < e {
					t.Fatalf("memory usage for query was %f, wanted at least %f", a, e)
				}
			}
		}

		// Verify insufficient memory error bubbles up.
		{
			query := tm.makeQuery("test.metric", resolution1ns, 0, 10000)
			query.BudgetBytes = 1000
			query.EstimatedSources = 3
			query.InterpolationLimitNanos = 5
			query.assertError("insufficient")
		}
	})
}

func TestQueryWorkerMemoryMonitor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runTestCaseMultipleFormats(t, func(t *testing.T, tm testModelRunner) {
		tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{
			{
				Name: "test.metric",
				Datapoints: []tspb.TimeSeriesDatapoint{
					datapoint(1, 100),
					datapoint(5, 200),
					datapoint(15, 300),
					datapoint(16, 400),
					datapoint(17, 500),
					datapoint(22, 600),
					datapoint(52, 900),
				},
			},
		})
		tm.assertKeyCount(4)
		tm.assertModelCorrect()

		// Create a limited bytes monitor.
		memoryBudget := int64(100 * 1024)
		limitedMon := mon.MakeMonitorWithLimit(
			"timeseries-test-limited",
			mon.MemoryResource,
			memoryBudget,
			nil,
			nil,
			100,
			100,
			cluster.MakeTestingClusterSettings(),
		)
		limitedMon.Start(context.TODO(), tm.workerMemMonitor, mon.BoundAccount{})
		defer limitedMon.Stop(context.TODO())

		// Assert correctness with no memory pressure.
		query := tm.makeQuery("test.metric", resolution1ns, 0, 60)
		query.workerMemMonitor = &limitedMon
		query.assertSuccess(7, 1)

		// Assert failure with memory pressure.
		acc := limitedMon.MakeBoundAccount()
		if err := acc.Grow(context.TODO(), memoryBudget-1); err != nil {
			t.Fatal(err)
		}

		query.assertError("memory budget exceeded")

		// Assert success again with memory pressure released.
		acc.Close(context.TODO())
		query.assertSuccess(7, 1)

		// Start/Stop limited monitor to reset maximum allocation.
		limitedMon.Stop(context.TODO())
		limitedMon.Start(context.TODO(), tm.workerMemMonitor, mon.BoundAccount{})

		var (
			memStatsBefore runtime.MemStats
			memStatsAfter  runtime.MemStats
		)
		runtime.ReadMemStats(&memStatsBefore)

		query.EndNanos = 10000
		query.assertSuccess(7, 1)

		runtime.ReadMemStats(&memStatsAfter)
		t.Logf("total allocations for query: %d\n", memStatsAfter.TotalAlloc-memStatsBefore.TotalAlloc)
		t.Logf("maximum allocations for query monitor: %d\n", limitedMon.MaximumBytes())
	})
}

// TestQueryBadRequests confirms that the query method returns gentle errors for
// obviously bad incoming data.
func TestQueryBadRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runTestCaseMultipleFormats(t, func(t *testing.T, tm testModelRunner) {
		// Query with a downsampler that is invalid, expect error.
		{
			query := tm.makeQuery("metric.test", resolution1ns, 0, 10000)
			query.SampleDurationNanos = 10
			query.setDownsampler((tspb.TimeSeriesQueryAggregator)(999))
			query.assertError("unknown time series downsampler")
		}

		// Query with a aggregator that is invalid, expect error.
		{
			query := tm.makeQuery("metric.test", resolution1ns, 0, 10000)
			query.SampleDurationNanos = 10
			query.setSourceAggregator((tspb.TimeSeriesQueryAggregator)(999))
			query.assertError("unknown time series aggregator")
		}

		// Query with a downsampler that is invalid, expect no error (default behavior is none).
		{
			query := tm.makeQuery("metric.test", resolution1ns, 0, 10000)
			query.SampleDurationNanos = 10
			query.setDerivative((tspb.TimeSeriesQueryDerivative)(999))
			query.assertSuccess(0, 0)
		}
	})
}

func TestQueryNearCurrentTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runTestCaseMultipleFormats(t, func(t *testing.T, tm testModelRunner) {
		tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{
			{
				Name:   "metric.test",
				Source: "source1",
				Datapoints: []tspb.TimeSeriesDatapoint{
					datapoint(1, 100),
					datapoint(5, 500),
					datapoint(15, 500),
					datapoint(16, 600),
					datapoint(17, 700),
					datapoint(22, 200),
					datapoint(45, 500),
					datapoint(46, 600),
					datapoint(52, 200),
				},
			},
			{
				Name:   "metric.test",
				Source: "source2",
				Datapoints: []tspb.TimeSeriesDatapoint{
					datapoint(7, 0),
					datapoint(7, 700),
					datapoint(9, 900),
					datapoint(14, 400),
					datapoint(18, 800),
					datapoint(33, 300),
					datapoint(34, 400),
					datapoint(56, 600),
					datapoint(59, 900),
				},
			},
		})
		tm.assertKeyCount(9)
		tm.assertModelCorrect()

		// All points returned for query with nowNanos in the future.
		{
			query := tm.makeQuery("metric.test", resolution1ns, 0, 500)
			query.NowNanos = 60
			query.assertSuccess(17, 2)
		}

		// Test query is disallowed in the future.
		{
			query := tm.makeQuery("metric.test", resolution1ns, 20, 500)
			query.NowNanos = 10
			query.assertError("cannot query time series in the future")
		}

		// Test query is truncated so that future datapoints are not queried.
		{
			query := tm.makeQuery("metric.test", resolution1ns, 0, 500)
			query.NowNanos = 30
			query.assertSuccess(10, 2)
		}

		// Data points from incomplete periods are not included.
		{
			query := tm.makeQuery("metric.test", resolution1ns, 0, 500)
			query.NowNanos = 59
			query.assertSuccess(16, 2)
		}

		// Data points for incomplete periods are not included (with downsampling).
		{
			query := tm.makeQuery("metric.test", resolution1ns, 0, 500)
			query.NowNanos = 60
			query.SampleDurationNanos = 10
			query.assertSuccess(6, 2)

			query = tm.makeQuery("metric.test", resolution1ns, 0, 500)
			query.NowNanos = 59
			query.SampleDurationNanos = 10
			query.assertSuccess(5, 2)
		}
	})
}

func TestQueryRollup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Rollups are always columnar, no need to run this test using row format.
	tm := newTestModelRunner(t)
	tm.Start()
	defer tm.Stop()

	tm.storeTimeSeriesData(resolution50ns, []tspb.TimeSeriesData{
		{
			Name:   "metric.test",
			Source: "source1",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(1, 100),
				datapoint(45, 500),
				datapoint(150, 500),
				datapoint(165, 600),
				datapoint(172, 700),
				datapoint(220, 200),
				datapoint(230, 200),
				datapoint(240, 242),
				datapoint(350, 500),
				datapoint(520, 199),
				datapoint(610, 200),
				datapoint(620, 999),
				datapoint(750, 200),
				datapoint(751, 2123),
				datapoint(921, 500),
				datapoint(991, 500),
				datapoint(1001, 1234),
				datapoint(1002, 234),
			},
		},
		{
			Name:   "metric.test",
			Source: "source2",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(7, 234),
				datapoint(63, 342),
				datapoint(74, 342),
				datapoint(124, 500),
				datapoint(186, 2345),
				datapoint(193, 1234),
				datapoint(220, 200),
				datapoint(221, 200),
				datapoint(240, 22342),
				datapoint(420, 975),
				datapoint(422, 396),
				datapoint(498, 6884.74),
				datapoint(610, 200),
				datapoint(620, 999),
				datapoint(750, 200),
				datapoint(751, 2123),
				datapoint(854, 9403),
				datapoint(921, 500),
				datapoint(991, 500),
				datapoint(1001, 1234),
				datapoint(1002, 234),
			},
		},
	})
	tm.assertKeyCount(4)
	tm.assertModelCorrect()

	{
		query := tm.makeQuery("metric.test", resolution50ns, 100, 1500)
		query.assertSuccess(13, 2)
	}

	{
		query := tm.makeQuery("metric.test", resolution50ns, 450, 850)
		query.setDownsampler(tspb.TimeSeriesQueryAggregator_MAX)
		query.setDownsampler(tspb.TimeSeriesQueryAggregator_MIN)
		query.assertSuccess(5, 2)
	}

	{
		query := tm.makeQuery("metric.test", resolution50ns, 100, 1500)
		query.setDerivative(tspb.TimeSeriesQueryDerivative_DERIVATIVE)
		query.assertSuccess(13, 2)
	}
}
