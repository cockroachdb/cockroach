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
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/kr/pretty"
)

type itsdByTimestamp []roachpb.InternalTimeSeriesData

func (bt itsdByTimestamp) Len() int {
	return len(bt)
}

func (bt itsdByTimestamp) Less(i int, j int) bool {
	return bt[i].StartTimestampNanos < bt[j].StartTimestampNanos
}

func (bt itsdByTimestamp) Swap(i int, j int) {
	bt[i], bt[j] = bt[j], bt[i]
}

func TestComputeRollupFromData(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		input    tspb.TimeSeriesData
		expected []roachpb.InternalTimeSeriesData
	}{
		{
			input: tsd("test.metric", "",
				tsdp(10, 200),
				tsdp(20, 300),
				tsdp(40, 400),
				tsdp(80, 400),
				tsdp(97, 400),
				tsdp(201, 41234),
				tsdp(249, 423),
				tsdp(424, 123),
				tsdp(425, 342),
				tsdp(426, 643),
				tsdp(427, 835),
				tsdp(1023, 999),
				tsdp(1048, 888),
				tsdp(1123, 999),
				tsdp(1248, 888),
				tsdp(1323, 999),
				tsdp(1348, 888),
			),
			expected: []roachpb.InternalTimeSeriesData{
				makeInternalColumnData(0, 50, []tspb.TimeSeriesDatapoint{
					tsdp(10, 200),
					tsdp(20, 300),
					tsdp(40, 400),
					tsdp(80, 400),
					tsdp(97, 400),
					tsdp(201, 41234),
					tsdp(249, 423),
					tsdp(424, 123),
					tsdp(425, 342),
					tsdp(426, 643),
					tsdp(427, 835),
				}),
				makeInternalColumnData(1000, 50, []tspb.TimeSeriesDatapoint{
					tsdp(1023, 999),
					tsdp(1048, 888),
					tsdp(1123, 999),
					tsdp(1248, 888),
					tsdp(1323, 999),
					tsdp(1348, 888),
				}),
			},
		},
		{
			input: tsd("test.metric", "",
				tsdp(1023, 999),
				tsdp(1048, 888),
				tsdp(1123, 999),
				tsdp(1248, 888),
			),
			expected: []roachpb.InternalTimeSeriesData{
				makeInternalColumnData(1000, 50, []tspb.TimeSeriesDatapoint{
					tsdp(1023, 999),
					tsdp(1048, 888),
					tsdp(1123, 999),
					tsdp(1248, 888),
				}),
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			rollups := computeRollupsFromData(tc.input, 50)
			internal, err := rollups.toInternal(1000, 50)
			if err != nil {
				t.Fatal(err)
			}
			if a, e := internal, tc.expected; !reflect.DeepEqual(a, e) {
				for _, diff := range pretty.Diff(a, e) {
					t.Error(diff)
				}
			}

			// Compare expected to test model output; the notion of rollups is
			// implemented on top of the testmodel, and though it is simple we need to
			// exercise it here.
			tm := newTestModelRunner(t)
			tm.Start()
			defer tm.Stop()

			tm.storeInModel(resolution1ns, tc.input)
			tm.rollup(math.MaxInt64, timeSeriesResolutionInfo{
				Name:       "test.metric",
				Resolution: resolution1ns,
			})
			tm.prune(math.MaxInt64, timeSeriesResolutionInfo{
				Name:       "test.metric",
				Resolution: resolution1ns,
			})

			var modelActual []roachpb.InternalTimeSeriesData
			layout := tm.getModelDiskLayout()
			for _, data := range layout {
				var val roachpb.InternalTimeSeriesData
				if err := data.GetProto(&val); err != nil {
					t.Fatal(err)
				}
				modelActual = append(modelActual, val)
			}
			sort.Sort(itsdByTimestamp(modelActual))

			if a, e := modelActual, tc.expected; !reflect.DeepEqual(a, e) {
				for _, diff := range pretty.Diff(a, e) {
					t.Error(diff)
				}
			}
		})
	}
}

func TestRollupBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tm := newTestModelRunner(t)
	tm.Start()
	defer tm.Stop()

	series1a := tsd("test.metric", "a")
	series1b := tsd("test.metric", "b")
	series2 := tsd("test.othermetric", "a")
	for i := 0; i < 500; i++ {
		series1a.Datapoints = append(series1a.Datapoints, tsdp(time.Duration(i), float64(i)))
		series1b.Datapoints = append(series1b.Datapoints, tsdp(time.Duration(i), float64(i)))
		series2.Datapoints = append(series2.Datapoints, tsdp(time.Duration(i), float64(i)))
	}

	tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{series1a, series1b, series2})
	tm.assertKeyCount(150)
	tm.assertModelCorrect()

	now := 250 + resolution1nsDefaultRollupThreshold.Nanoseconds()
	tm.rollup(now, timeSeriesResolutionInfo{
		Name:       "test.metric",
		Resolution: resolution1ns,
	})
	tm.assertKeyCount(152)
	tm.assertModelCorrect()

	tm.prune(now, timeSeriesResolutionInfo{
		Name:       "test.metric",
		Resolution: resolution1ns,
	})
	tm.assertKeyCount(102)
	tm.assertModelCorrect()

	// Specialty test - rollup only the real series, not the model, and ensure
	// that the query remains the same.  This ensures that the same result is
	// returned from rolled-up data as is returned from data downsampled during
	// a query.
	memOpts := QueryMemoryOptions{
		// Large budget, but not maximum to avoid overflows.
		BudgetBytes:             math.MaxInt64,
		EstimatedSources:        1, // Not needed for rollups
		InterpolationLimitNanos: 0,
		Columnar:                tm.DB.WriteColumnar(),
	}
	if err := tm.DB.rollupTimeSeries(
		context.Background(),
		[]timeSeriesResolutionInfo{
			{
				Name:       "test.othermetric",
				Resolution: resolution1ns,
			},
		},
		hlc.Timestamp{
			WallTime: 500 + resolution1nsDefaultRollupThreshold.Nanoseconds(),
			Logical:  0,
		},
		MakeQueryMemoryContext(tm.workerMemMonitor, tm.resultMemMonitor, memOpts),
	); err != nil {
		t.Fatal(err)
	}

	if err := tm.DB.pruneTimeSeries(
		context.Background(),
		tm.DB.db,
		[]timeSeriesResolutionInfo{
			{
				Name:       "test.othermetric",
				Resolution: resolution1ns,
			},
		},
		hlc.Timestamp{
			WallTime: 500 + resolution1nsDefaultRollupThreshold.Nanoseconds(),
			Logical:  0,
		},
	); err != nil {
		t.Fatal(err)
	}

	{
		query := tm.makeQuery("test.othermetric", resolution1ns, 0, 500)
		query.SampleDurationNanos = 50
		query.assertSuccess(10, 1)
	}
}

func TestRollupMemoryConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tm := newTestModelRunner(t)
	tm.Start()
	defer tm.Stop()

	series1 := tsd("test.metric", "a")
	series2 := tsd("test.othermetric", "a")
	for i := 0; i < 500; i++ {
		series1.Datapoints = append(series1.Datapoints, tsdp(time.Duration(i), float64(i)))
		series2.Datapoints = append(series2.Datapoints, tsdp(time.Duration(i), float64(i)))
	}

	tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{series1, series2})
	tm.assertKeyCount(100)
	tm.assertModelCorrect()

	// Construct a memory monitor that will be used to measure the high-water
	// mark of memory usage for the rollup process.
	adjustedMon := mon.NewMonitor(
		"timeseries-test-worker-adjusted",
		mon.MemoryResource,
		nil,
		nil,
		1,
		math.MaxInt64,
		cluster.MakeTestingClusterSettings(),
	)
	adjustedMon.Start(context.Background(), tm.workerMemMonitor, mon.BoundAccount{})
	defer adjustedMon.Stop(context.Background())

	// Roll up time series with the new monitor to measure high-water mark
	// of
	qmc := MakeQueryMemoryContext(adjustedMon, adjustedMon, QueryMemoryOptions{
		// Large budget, but not maximum to avoid overflows.
		BudgetBytes:      math.MaxInt64,
		EstimatedSources: 1, // Not needed for rollups
		Columnar:         tm.DB.WriteColumnar(),
	})
	tm.rollupWithMemoryContext(qmc, 500+resolution1nsDefaultRollupThreshold.Nanoseconds(), timeSeriesResolutionInfo{
		Name:       "test.othermetric",
		Resolution: resolution1ns,
	})
	tm.prune(500+resolution1nsDefaultRollupThreshold.Nanoseconds(), timeSeriesResolutionInfo{
		Name:       "test.othermetric",
		Resolution: resolution1ns,
	})

	tm.assertKeyCount(51)
	tm.assertModelCorrect()

	// Ensure that we used at least 50 slabs worth of memory at one time.
	if a, e := adjustedMon.MaximumBytes(), 50*qmc.computeSizeOfSlab(resolution1ns); a < e {
		t.Fatalf("memory usage for query was %d, wanted at least %d", a, e)
	}

	// Limit testing: set multiple constraints on memory and ensure that they
	// are being respected through chunking.
	for i, limit := range []int64{
		25 * qmc.computeSizeOfSlab(resolution1ns),
		10 * qmc.computeSizeOfSlab(resolution1ns),
	} {
		// Generate a new series.
		seriesName := fmt.Sprintf("metric.series%d", i)
		seriesData := tsd(seriesName, "a")
		for j := 0; j < 500; j++ {
			seriesData.Datapoints = append(seriesData.Datapoints, tsdp(time.Duration(j), float64(j)))
		}
		tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{seriesData})
		tm.assertModelCorrect()
		tm.assertKeyCount(51 + i /* rollups from previous iterations */ + 50)

		// Restart monitor to clear query memory options.
		adjustedMon.Stop(context.Background())
		adjustedMon.Start(context.Background(), tm.workerMemMonitor, mon.BoundAccount{})

		qmc := MakeQueryMemoryContext(adjustedMon, adjustedMon, QueryMemoryOptions{
			// Large budget, but not maximum to avoid overflows.
			BudgetBytes:      limit,
			EstimatedSources: 1, // Not needed for rollups
			Columnar:         tm.DB.WriteColumnar(),
		})
		tm.rollupWithMemoryContext(qmc, 500+resolution1nsDefaultRollupThreshold.Nanoseconds(), timeSeriesResolutionInfo{
			Name:       seriesName,
			Resolution: resolution1ns,
		})
		tm.prune(500+resolution1nsDefaultRollupThreshold.Nanoseconds(), timeSeriesResolutionInfo{
			Name:       seriesName,
			Resolution: resolution1ns,
		})

		tm.assertKeyCount(51 + i + 1)
		tm.assertModelCorrect()

		// Check budget was not exceeded.  Computation of budget usage is not exact
		// in the case of rollups, due to the fact that results are tracked with
		// the same monitor but may vary in size based on the specific input
		// rows. Because of this, allow up to 20% over limit.
		if a, e := float64(adjustedMon.MaximumBytes()), float64(limit)*1.2; a > e {
			t.Fatalf("memory usage for query was %f, wanted a limit of %f", a, e)
		}

		// Check that budget was used.
		if a, e := float64(adjustedMon.MaximumBytes()), float64(limit)*0.95; a < e {
			t.Fatalf("memory usage for query was %f, wanted at least %f", a, e)
		}
	}
}
