// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clusterstats

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/golang/mock/gomock"
	"github.com/montanaflynn/stats"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

// testingComplexAggFn returns the coefficient of variation of tagged values at
// the same index.
func testingComplexAggFn(query string, matSeries [][]float64) (string, []float64) {
	agg := make([]float64, 0, 1)
	for _, series := range matSeries {
		stdev, _ := stats.StandardDeviationSample(series)
		mean, _ := stats.Mean(series)
		cv := 0.0
		if mean != 0 {
			cv = stdev / mean
		}
		agg = append(agg, cv)
	}
	return "cv(foo)", agg
}

// testingAggFn returns the sum of tagged values at the same index.
func testingAggFn(query string, matSeries [][]float64) (string, []float64) {
	agg := make([]float64, 0, 1)
	for _, series := range matSeries {
		curSum := 0.0
		for _, val := range series {
			curSum += val
		}
		agg = append(agg, curSum)
	}
	return "sum(foo)", agg
}

var (
	statsTestingStartTime = time.Date(2020, 12, 25, 0, 0, 0, 0, time.UTC)
	statsTestingDuration  = prometheus.DefaultScrapeInterval

	fooStat = ClusterStat{Query: "foo_count", LabelName: "instance"}
	barStat = ClusterStat{Query: "bar_count", LabelName: "instance"}

	fooAggQuery = AggQuery{
		Stat:  fooStat,
		AggFn: testingAggFn,
	}
	barAggQuery = AggQuery{
		Stat:  barStat,
		AggFn: testingAggFn,
	}
	fooComplexAggQuery = AggQuery{
		Stat:  fooStat,
		AggFn: testingComplexAggFn,
	}

	fooTS = promTimeSeries{
		metric:    "foo_count",
		startTime: statsTestingStartTime,
		interval:  statsTestingDuration,
		values: map[string][]float64{
			"1": {1, 2, 3, 4, 5, 6, 7, 8, 9},
			"2": {11, 22, 33, 44, 55, 66, 77, 88, 99},
			"3": {111, 222, 333, 444, 555, 666, 777, 888, 999},
		},
	}
	emptyFooTS = promTimeSeries{
		metric:    "foo_count",
		startTime: statsTestingStartTime,
		interval:  statsTestingDuration,
		values: map[string][]float64{
			"1": {},
			"2": {},
			"3": {},
		},
	}

	zeroFooTS = promTimeSeries{
		metric:    "foo_count",
		startTime: statsTestingStartTime,
		interval:  statsTestingDuration,
		values: map[string][]float64{
			"1": {0, 0, 0, 0, 0, 0, 0, 0, 0},
			"2": {0, 0, 0, 0, 0, 0, 0, 0, 0},
			"3": {0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
	}

	barTS = promTimeSeries{
		metric:    "bar_count",
		startTime: statsTestingStartTime,
		interval:  statsTestingDuration,
		values: map[string][]float64{
			"1": {111, 222, 333, 444, 555, 666, 777, 888, 999},
			"2": {1, 2, 3, 4, 5, 6, 7, 8, 9},
			"3": {11, 22, 33, 44, 55, 66, 77, 88, 99},
		},
	}
)

func TestClusterStatsCollectorSummaryCollector(t *testing.T) {
	ctx := context.Background()

	type expectPromRangeQuery struct {
		q        string
		fromTime time.Time
		toTime   time.Time
		ret      model.Matrix
	}

	makePromQueryRange := func(q string, tags []string, ticks int, ts promTimeSeries, empty bool) expectPromRangeQuery {
		ret := make([]*model.SampleStream, len(tags))
		for i, tg := range tags {
			var values []model.SamplePair
			if !empty {
				values = make([]model.SamplePair, ticks)
				rawValues := ts.values[tg][:ticks]
				for j := range rawValues {
					values[j] = model.SamplePair{
						Timestamp: model.TimeFromUnix(ts.startTime.Add(ts.interval * time.Duration(j)).Unix()),
						Value:     model.SampleValue(rawValues[j]),
					}
				}
			}
			ss := &model.SampleStream{
				Metric: model.Metric(map[model.LabelName]model.LabelValue{
					"instance": model.LabelValue(tg),
				}),
				Values: values,
			}
			ret[i] = ss
		}

		return expectPromRangeQuery{
			q:        q,
			fromTime: ts.startTime,
			toTime:   ts.startTime.Add(ts.interval * time.Duration(ticks)),
			ret:      ret,
		}
	}

	makeTickList := func(startTime time.Time, interval time.Duration, ticks int) []int64 {
		ret := make([]int64, ticks)
		for i := 0; i < ticks; i++ {
			ret[i] = startTime.Add(interval * time.Duration(i)).UnixNano()
		}
		return ret
	}

	filterTSMap := func(tsMap map[string][]float64, ticks int, tags ...string) map[string][]float64 {
		retTSMap := make(map[string][]float64)
		for _, t := range tags {
			retTSMap[t] = tsMap[t][:ticks-1]
		}
		return retTSMap
	}

	makeExpectedTs := func(ts promTimeSeries, aggQuery AggQuery, ticks int, tags ...string) StatSummary {
		filteredMap := filterTSMap(ts.values, ticks, tags...)
		tsMat := ConvertEqualLengthMapToMat(filteredMap)
		tag, val := aggQuery.AggFn("", tsMat)
		return StatSummary{
			Time:   makeTickList(statsTestingStartTime, fooTS.interval, ticks-1),
			Value:  val,
			Tagged: filteredMap,
			AggTag: tag,
			Tag:    aggQuery.Stat.Query,
		}
	}

	testCases := []struct {
		desc              string
		ticks             int
		summaryQueries    []AggQuery
		mockedPromResults []expectPromRangeQuery
		expected          map[string]StatSummary
	}{
		{
			desc:           "single tag, multi stat, 8 ticks",
			ticks:          8,
			summaryQueries: []AggQuery{fooAggQuery, barAggQuery},
			mockedPromResults: []expectPromRangeQuery{
				makePromQueryRange(fooStat.Query, []string{"1"}, 8, fooTS, false),
				makePromQueryRange(barStat.Query, []string{"1"}, 8, barTS, false),
			},
			expected: map[string]StatSummary{
				fooStat.Query: makeExpectedTs(fooTS, fooAggQuery, 8, "1"),
				barStat.Query: makeExpectedTs(barTS, barAggQuery, 8, "1"),
			},
		},
		{
			desc:           "mutli tag, single stat, 8 ticks, no return values",
			ticks:          8,
			summaryQueries: []AggQuery{fooComplexAggQuery},
			mockedPromResults: []expectPromRangeQuery{
				makePromQueryRange(fooStat.Query, []string{"1", "2", "3"}, 8, emptyFooTS, true),
			},
			expected: map[string]StatSummary{},
		},
		{
			desc:           "mutli tag, single stat, 8 ticks, return values all zero",
			ticks:          8,
			summaryQueries: []AggQuery{fooComplexAggQuery},
			mockedPromResults: []expectPromRangeQuery{
				makePromQueryRange(fooStat.Query, []string{"1", "2", "3"}, 8, zeroFooTS, false),
			},
			expected: map[string]StatSummary{
				fooStat.Query: makeExpectedTs(zeroFooTS, fooComplexAggQuery, 8, "1", "2", "3"),
			},
		},
		{
			desc:              "multi tag, single stat, 7 ticks",
			ticks:             7,
			summaryQueries:    []AggQuery{fooAggQuery},
			mockedPromResults: []expectPromRangeQuery{makePromQueryRange(fooStat.Query, []string{"1", "2"}, 7, fooTS, false)},
			expected: map[string]StatSummary{
				fooStat.Query: makeExpectedTs(fooTS, fooAggQuery, 7, "1", "2"),
			},
		},
		{
			desc:           "multi tag, multi stat, 5 ticks",
			ticks:          5,
			summaryQueries: []AggQuery{fooAggQuery, barAggQuery},
			mockedPromResults: []expectPromRangeQuery{
				makePromQueryRange(fooStat.Query, []string{"1", "2", "3"}, 5, fooTS, false),
				makePromQueryRange(barStat.Query, []string{"1", "2", "3"}, 5, barTS, false),
			},
			expected: map[string]StatSummary{
				fooStat.Query: makeExpectedTs(fooTS, fooAggQuery, 5, "1", "2", "3"),
				barStat.Query: makeExpectedTs(barTS, barAggQuery, 5, "1", "2", "3"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			c := clusterStatCollector{
				interval: statsTestingDuration,
				promClient: func(ctrl *gomock.Controller) prometheus.Client {
					c := NewMockClient(ctrl)
					e := c.EXPECT()
					for _, m := range tc.mockedPromResults {
						e.QueryRange(ctx, m.q, promv1.Range{Start: m.fromTime, End: m.toTime, Step: statsTestingDuration}).Return(
							model.Value(m.ret),
							nil,
							nil,
						)
					}
					return c
				}(ctrl),
			}

			logger, err := logger.RootLogger("", logger.TeeToStdout)
			if err != nil {
				t.Fatal(err)
			}

			summaries, err := c.CollectSummaries(
				ctx,
				logger,
				Interval{statsTestingStartTime, statsTestingStartTime.Add(statsTestingDuration * time.Duration(tc.ticks))},
				tc.summaryQueries,
			)
			if err != nil {
				t.Fatal(err)
			}

			require.Equal(t, tc.expected, summaries)
		})
	}
}
