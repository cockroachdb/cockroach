// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/prometheus"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/golang/mock/gomock"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

type promTimeSeries struct {
	metric    string
	startTime time.Time
	interval  time.Duration
	values    map[tag][]float64
}

// testingAggFn returns the sum of tagged values at the same index.
func testingAggFn(vals map[tag][]float64) []float64 {
	agg := make([]float64, 0, 1)
	for _, tagged := range vals {
		for i, v := range tagged {
			if i >= len(agg) {
				agg = append(agg, v)
			} else {
				agg[i] += v
			}
		}
	}
	return agg
}

var (
	statsTestingStartTime = time.Date(2020, 12, 25, 0, 0, 0, 0, time.UTC)
	statsTestingDuration  = prometheus.DefaultScrapeInterval

	fooStat = clusterStat{query: "foo_count", tag: "foo"}
	barStat = clusterStat{query: "bar_count", tag: "bar"}

	fooAggQuery = statSummaryQuery{
		stat:   fooStat,
		aggTag: "sum(foo)",
		aggFn:  testingAggFn,
	}
	barAggQuery = statSummaryQuery{
		stat:   barStat,
		aggTag: "sum(foo)",
		aggFn:  testingAggFn,
	}

	fooTS = promTimeSeries{
		metric:    "foo_count",
		startTime: statsTestingStartTime,
		interval:  statsTestingDuration,
		values: map[tag][]float64{
			"1": {1, 2, 3, 4, 5, 6, 7, 8, 9},
			"2": {11, 22, 33, 44, 55, 66, 77, 88, 99},
			"3": {111, 222, 333, 444, 555, 666, 777, 888, 999},
		},
	}

	barTS = promTimeSeries{
		metric:    "bar_count",
		startTime: statsTestingStartTime,
		interval:  statsTestingDuration,
		values: map[tag][]float64{
			"1": {111, 222, 333, 444, 555, 666, 777, 888, 999},
			"2": {1, 2, 3, 4, 5, 6, 7, 8, 9},
			"3": {11, 22, 33, 44, 55, 66, 77, 88, 99},
		},
	}
)

func TestClusterStatsCollectorSummaryCollector(t *testing.T) {
	ctx := context.Background()

	type expectPromRangeQuery struct {
		q        query
		fromTime time.Time
		toTime   time.Time
		ret      model.Matrix
	}

	makePromQueryRange := func(q query, tags []string, ticks int, ts promTimeSeries) expectPromRangeQuery {
		ret := make([]*model.SampleStream, len(tags))
		for i, tg := range tags {
			rawValues := ts.values[tag(tg)][:ticks]
			values := make([]model.SamplePair, ticks)
			for j := range rawValues {
				values[j] = model.SamplePair{
					Timestamp: model.TimeFromUnix(ts.startTime.Add(ts.interval * time.Duration(j)).Unix()),
					Value:     model.SampleValue(rawValues[j]),
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
			fromTime: ts.startTime.Add(ts.interval),
			toTime:   ts.startTime.Add(ts.interval * time.Duration(ticks-1)),
			ret:      ret,
		}
	}

	makeTickList := func(startTime time.Time, interval time.Duration, ticks int) []int64 {
		ret := make([]int64, ticks)
		for i := 0; i < ticks; i++ {
			ret[i] = startTime.Add(interval * time.Duration(i)).Unix()
		}
		return ret
	}

	filterTSMap := func(tsMap map[tag][]float64, ticks int, tags ...string) map[tag][]float64 {
		retTSMap := make(map[tag][]float64)
		for _, t := range tags {
			retTSMap[tag(t)] = tsMap[tag(t)][:ticks]
		}
		return retTSMap
	}

	makeExpectedTs := func(ts promTimeSeries, aggQuery statSummaryQuery, ticks int, tags ...string) StatSummary {
		filteredMap := filterTSMap(ts.values, ticks, tags...)
		return StatSummary{
			Time:   makeTickList(statsTestingStartTime, fooTS.interval, ticks),
			Value:  aggQuery.aggFn(filteredMap),
			Tagged: filteredMap,
			AggTag: aggQuery.aggTag,
			Tag:    aggQuery.stat.tag,
		}
	}

	testCases := []struct {
		desc              string
		ticks             int
		summaryQueries    []statSummaryQuery
		mockedPromResults []expectPromRangeQuery
		expected          map[tag]StatSummary
	}{
		{
			desc:           "single tag, multi stat, 8 ticks",
			ticks:          8,
			summaryQueries: []statSummaryQuery{fooAggQuery, barAggQuery},
			mockedPromResults: []expectPromRangeQuery{
				makePromQueryRange(fooStat.query, []string{"1"}, 8, fooTS),
				makePromQueryRange(barStat.query, []string{"1"}, 8, barTS),
			},
			expected: map[tag]StatSummary{
				fooStat.tag: makeExpectedTs(fooTS, fooAggQuery, 8, "1"),
				barStat.tag: makeExpectedTs(barTS, barAggQuery, 8, "1"),
			},
		},
		{
			desc:              "multi tag, single stat, 7 ticks",
			ticks:             7,
			summaryQueries:    []statSummaryQuery{fooAggQuery},
			mockedPromResults: []expectPromRangeQuery{makePromQueryRange(fooStat.query, []string{"1", "2"}, 7, fooTS)},
			expected: map[tag]StatSummary{
				fooStat.tag: makeExpectedTs(fooTS, fooAggQuery, 7, "1", "2"),
			},
		},
		{
			desc:           "multi tag, multi stat, 5 ticks",
			ticks:          5,
			summaryQueries: []statSummaryQuery{fooAggQuery, barAggQuery},
			mockedPromResults: []expectPromRangeQuery{
				makePromQueryRange(fooStat.query, []string{"1", "2", "3"}, 5, fooTS),
				makePromQueryRange(barStat.query, []string{"1", "2", "3"}, 5, barTS),
			},
			expected: map[tag]StatSummary{
				fooStat.tag: makeExpectedTs(fooTS, fooAggQuery, 5, "1", "2", "3"),
				barStat.tag: makeExpectedTs(barTS, barAggQuery, 5, "1", "2", "3"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			c := clusterStatCollector{
				interval: statsTestingDuration,
				cleanup: func() {
				},
				startTime: statsTestingStartTime,
				promClient: func(ctrl *gomock.Controller) prometheus.Client {
					c := NewMockClient(ctrl)
					e := c.EXPECT()
					for _, m := range tc.mockedPromResults {
						e.QueryRange(ctx, string(m.q), promv1.Range{Start: m.fromTime, End: m.toTime, Step: statsTestingDuration}).Return(
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

			summaries, err := c.collectSummaries(
				ctx,
				logger,
				statsTestingStartTime,
				// Add one tick, to account for truncation that occurs to
				// prevent mismatched series lengths in prometheus stat
				// collection.
				statsTestingStartTime.Add(statsTestingDuration*time.Duration(tc.ticks)),
				tc.summaryQueries,
			)
			if err != nil {
				t.Fatal(err)
			}

			require.Equal(t, tc.expected, summaries)
			c.cleanup()
		})
	}
}

// TestClusterStatsStreamer asserts that the stats streamer:
// (1) run returns on processTickFn closure returning true.
// (2) scrapes prometheus at a correct intervals and parses into stat events.
func TestClusterStatsStreamer(t *testing.T) {
	ctx := context.Background()

	type expectPromQuery struct {
		q   query
		t   time.Time
		ret model.Vector
	}

	makeExpectedEvents := func(ts []promTimeSeries, ticks int, tags []string, stats ...clusterStat) [][]statEvent {
		ret := make([][]statEvent, ticks)
		for i := 0; i < ticks; i++ {
			next := make([]statEvent, len(stats))
			for j, stat := range stats {
				nextStat := statEvent{tag: stat.tag, values: make(map[tag]float64)}
				for _, tg := range tags {
					nextStat.values[tag(tg)] = ts[j].values[tag(tg)][i]
				}
				next[j] = nextStat
			}
			ret[i] = next
		}
		return ret
	}

	makePromResults := func(ts []promTimeSeries, ticks int, tags []string, stats ...clusterStat) []expectPromQuery {
		ret := make([]expectPromQuery, 0, 1)
		// We expect cluster_stats to truncate the first and last tick, as it
		// is possible to be empty for some nodes.
		for i := 0; i < ticks; i++ {
			for j, stat := range stats {
				queryTime := ts[j].startTime.Add(ts[j].interval * time.Duration(i))
				statVector := make([]*model.Sample, len(tags))
				for k, tg := range tags {
					statVector[k] = &model.Sample{
						Metric: model.Metric(map[model.LabelName]model.LabelValue{
							"instance": model.LabelValue(tg),
						}),
						Timestamp: model.Time(queryTime.Unix()),
						Value:     model.SampleValue(ts[j].values[tag(tg)][i]),
					}
				}
				ret = append(ret, expectPromQuery{stat.query, queryTime, model.Vector(statVector)})
			}
		}
		return ret
	}

	testCases := []struct {
		desc              string
		ticks             int
		trackedStats      []clusterStat
		mockedPromResults []expectPromQuery
		expected          [][]statEvent
	}{
		{
			desc:              "single tag, multi stat, 3 ticks",
			ticks:             3,
			trackedStats:      []clusterStat{fooStat, barStat},
			mockedPromResults: makePromResults([]promTimeSeries{fooTS, barTS}, 3, []string{"1"}, fooStat, barStat),
			expected:          makeExpectedEvents([]promTimeSeries{fooTS, barTS}, 3, []string{"1"}, fooStat, barStat),
		},
		{
			desc:              "multi tag, single stat, 5 ticks",
			ticks:             5,
			trackedStats:      []clusterStat{fooStat},
			mockedPromResults: makePromResults([]promTimeSeries{fooTS}, 5, []string{"1", "2"}, fooStat),
			expected:          makeExpectedEvents([]promTimeSeries{fooTS}, 5, []string{"1", "2"}, fooStat),
		},
		{
			desc:              "multi tag, multi stat, 9 ticks",
			ticks:             9,
			trackedStats:      []clusterStat{fooStat, barStat},
			mockedPromResults: makePromResults([]promTimeSeries{fooTS, barTS}, 9, []string{"1", "2", "3"}, fooStat, barStat),
			expected:          makeExpectedEvents([]promTimeSeries{fooTS, barTS}, 9, []string{"1", "2", "3"}, fooStat, barStat),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			c := clusterStatCollector{
				interval: statsTestingDuration,
				cleanup: func() {
				},
				startTime: statsTestingStartTime,
				promClient: func(ctrl *gomock.Controller) prometheus.Client {
					c := NewMockClient(ctrl)
					e := c.EXPECT()
					for _, m := range tc.mockedPromResults {
						e.Query(ctx, string(m.q), m.t).Return(
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

			iter := 0
			results := make([][]statEvent, 0, 1)
			processTickFn := func(stats []statEvent) bool {
				results = append(results, stats)
				iter++
				return iter >= tc.ticks
			}

			// To avoid a long test run, we set the interval to scrape to 0.
			// The query time for each scrape run will still be incremented by
			// the interval defined in the stats collector.
			statStreamer := newClusterStatStreamer(&c, processTickFn, 0)
			statStreamer.registerStat(tc.trackedStats...)

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				statStreamer.run(ctx, logger, statsTestingStartTime)
				wg.Done()
			}()
			wg.Wait()

			for i := range tc.expected {
				require.ElementsMatch(t, tc.expected[i], results[i])
			}
			require.NoError(t, statStreamer.err())
		})
	}
}
