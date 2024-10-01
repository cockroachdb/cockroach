// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusterstats

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/golang/mock/gomock"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

type promTimeSeries struct {
	metric    string
	startTime time.Time
	interval  time.Duration
	values    map[string][]float64
}

// TestClusterStatsStreamer asserts that the stats streamer:
// (1) run returns on processTickFn closure returning true.
// (2) scrapes prometheus at a correct intervals and parses into stat events.
func TestClusterStatsStreamer(t *testing.T) {
	ctx := context.Background()

	type expectPromQuery struct {
		q   string
		t   time.Time
		ret model.Value
	}

	makeExpectedEvents := func(ts []promTimeSeries, ticks int, tags []string, stats ...ClusterStat) [][]StatEvent {
		ret := make([][]StatEvent, ticks)
		for i := 0; i < ticks; i++ {
			next := make([]StatEvent, len(stats))
			for j, stat := range stats {
				queryTime := ts[j].startTime.Add(ts[j].interval * time.Duration(i))
				nextStat := StatEvent{Stat: stat, Value: make(map[string]StatPoint)}
				for _, tg := range tags {
					// NB: We expect the result to be in unix nano seconds.
					nextStat.Value[tg] = StatPoint{Value: ts[j].values[tg][i], Time: queryTime.UnixNano()}
				}
				next[j] = nextStat
			}
			ret[i] = next
		}
		return ret
	}

	makePromResults := func(ts []promTimeSeries, ticks int, tags []string, stats ...ClusterStat) []expectPromQuery {
		ret := make([]expectPromQuery, 0, 1)
		for i := 0; i < ticks; i++ {
			for j, stat := range stats {
				queryTime := ts[j].startTime.Add(ts[j].interval * time.Duration(i))
				statVector := make([]*model.Sample, len(tags))
				for k, tg := range tags {
					statVector[k] = &model.Sample{
						Metric: model.Metric(map[model.LabelName]model.LabelValue{
							"instance": model.LabelValue(tg),
						}),
						// NB: Prometheus operates on milliseconds so we return
						// the millsecond value here when mocking the
						// prometheus interface.
						Timestamp: model.Time(queryTime.UnixMilli()),
						Value:     model.SampleValue(ts[j].values[tg][i]),
					}
				}
				ret = append(ret, expectPromQuery{stat.Query, queryTime, model.Vector(statVector)})
			}
		}
		return ret
	}

	testCases := []struct {
		desc              string
		ticks             int
		trackedStats      []ClusterStat
		mockedPromResults []expectPromQuery
		expected          [][]StatEvent
	}{
		{
			desc:              "single tag, multi stat, 3 ticks",
			ticks:             3,
			trackedStats:      []ClusterStat{fooStat, barStat},
			mockedPromResults: makePromResults([]promTimeSeries{fooTS, barTS}, 3, []string{"1"}, fooStat, barStat),
			expected:          makeExpectedEvents([]promTimeSeries{fooTS, barTS}, 3, []string{"1"}, fooStat, barStat),
		},
		{
			desc:              "multi tag, single stat, 5 ticks",
			ticks:             5,
			trackedStats:      []ClusterStat{fooStat},
			mockedPromResults: makePromResults([]promTimeSeries{fooTS}, 5, []string{"1", "2"}, fooStat),
			expected:          makeExpectedEvents([]promTimeSeries{fooTS}, 5, []string{"1", "2"}, fooStat),
		},
		{
			desc:              "multi tag, multi stat, 9 ticks",
			ticks:             9,
			trackedStats:      []ClusterStat{fooStat, barStat},
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
				promClient: func(ctrl *gomock.Controller) prometheus.Client {
					c := NewMockClient(ctrl)
					e := c.EXPECT()
					for _, m := range tc.mockedPromResults {
						e.Query(ctx, m.q, m.t).Return(
							m.ret,
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
			results := make([][]StatEvent, 0, 1)
			processTickFn := func(stats []StatEvent) bool {
				results = append(results, stats)
				iter++
				return iter >= tc.ticks
			}

			// To avoid a long test run, we set the testingZeroTime to true so
			// that the delay between stat events is 0. The query time for each
			// scrape run will still be incremented by the default scrape
			// interval (10s).
			statStreamer := newClusterStatStreamer(&c, processTickFn)
			statStreamer.testingZeroTime = true
			statStreamer.SetInterval(defaultScrapeInterval)
			statStreamer.Register(tc.trackedStats...)

			var wg sync.WaitGroup
			wg.Add(1)
			var streamerErrors error
			go func() {
				streamerErrors = statStreamer.Run(ctx, logger, statsTestingStartTime)
				wg.Done()
			}()
			wg.Wait()

			for i := range tc.expected {
				require.ElementsMatch(t, tc.expected[i], results[i])
			}
			require.NoError(t, streamerErrors)
		})
	}
}
