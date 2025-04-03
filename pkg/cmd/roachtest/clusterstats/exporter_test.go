// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusterstats

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/golang/mock/gomock"
	"github.com/montanaflynn/stats"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

//go:embed openmetrics_expected.txt
var expectedOutput string

// matches the metrics line and extracts the line without the timestamp
// This ensures that the line exactly matches as timestamp is the only part that will not match
var metricsRegExNoTimestamp = regexp.MustCompile(`(.*) \d+`)

// matches the line after removing the timestamp to extract the tag. This is used to append the tag
// to the key as teh data is sorted per tag
var metricsRegExTag = regexp.MustCompile(`^.*,tag="(\d+)",agg_tag=.*$`)

// testingComplexAggFn returns the coefficient of variation of tagged values at
// the same index.
func testingComplexAggFn(_ string, matSeries [][]float64) (string, []float64) {
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
func testingAggFn(_ string, matSeries [][]float64) (string, []float64) {
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

type expectPromRangeQuery struct {
	q        string
	fromTime time.Time
	toTime   time.Time
	ret      model.Matrix
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
	makePromQueryRange = func(q string, tags []string, ticks int, ts promTimeSeries, empty bool) expectPromRangeQuery {
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

	makeTickList = func(startTime time.Time, interval time.Duration, ticks int) []int64 {
		ret := make([]int64, ticks)
		for i := 0; i < ticks; i++ {
			ret[i] = startTime.Add(interval * time.Duration(i)).UnixNano()
		}
		return ret
	}

	filterTSMap = func(tsMap map[string][]float64, ticks int, tags ...string) map[string][]float64 {
		retTSMap := make(map[string][]float64)
		for _, t := range tags {
			retTSMap[t] = tsMap[t][:ticks-1]
		}
		return retTSMap
	}

	makeExpectedTs = func(ts promTimeSeries, aggQuery AggQuery, ticks int, tags ...string) StatSummary {
		filteredMap := filterTSMap(ts.values, ticks, tags...)
		tsMat := convertEqualLengthMapToMat(filteredMap)
		tag, val := aggQuery.AggFn("", tsMat)
		return StatSummary{
			Time:   makeTickList(statsTestingStartTime, fooTS.interval, ticks-1),
			Value:  val,
			Tagged: filteredMap,
			AggTag: tag,
			Tag:    aggQuery.Stat.Query,
		}
	}

	benchMarkFn = func(totalKey string, totalValue float64) func(
		summaries map[string]StatSummary) (string, float64) {
		return func(summaries map[string]StatSummary) (string, float64) {
			return totalKey, totalValue
		}
	}
)

func TestClusterStatsCollectorSummaryCollector(t *testing.T) {
	ctx := context.Background()

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

			l, err := logger.RootLogger("", logger.TeeToStdout)
			if err != nil {
				t.Fatal(err)
			}

			require.Equal(t, tc.expected, c.collectSummaries(
				ctx,
				l,
				Interval{statsTestingStartTime, statsTestingStartTime.Add(statsTestingDuration * time.Duration(tc.ticks))},
				tc.summaryQueries,
			))
		})
	}
}

func TestExport(t *testing.T) {
	ctx := context.Background()
	statsWriter = nil
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	statsFileDest := "/location/of/file"
	t.Run("multi tag, multi stat, 5 ticks true", func(t *testing.T) {
		c := getClusterStatCollector(ctx, ctrl, []expectPromRangeQuery{
			makePromQueryRange(fooStat.Query, []string{"1", "2", "3"}, 5, fooTS, false),
			makePromQueryRange(barStat.Query, []string{"1", "2", "3"}, 5, barTS, false),
		})
		mockTest := getMockTest(t, ctrl, true, statsFileDest)
		mockCluster := NewMockCluster(ctrl)
		mockTest.EXPECT().Name().Return("mock_name")
		mockTest.EXPECT().GetRunId().Return("mock_id").AnyTimes()
		mockCluster.EXPECT().Cloud().Times(1).Return(spec.GCE)
		mockTest.EXPECT().Spec().Return(&registry.TestSpec{
			Owner: "roachtest_mock", Suites: registry.Suites(registry.Nightly),
		}).AnyTimes()
		statsWriter = func(ctx context.Context, tt test.Test, c cluster.Cluster, buffer *bytes.Buffer, dest string) error {
			require.Equal(t, mockTest, tt)
			require.Equal(t, mockCluster, c)
			require.Equal(t, "/location/of/file/stats.om", dest)

			require.Equal(t, parseData(expectedOutput), parseData(buffer.String()))
			return nil
		}
		defer func() {
			statsWriter = nil
		}()
		testRun, err := c.Export(
			ctx, mockCluster, mockTest, false, statsTestingStartTime,
			statsTestingStartTime.Add(statsTestingDuration*time.Duration(5)),
			[]AggQuery{fooAggQuery, barAggQuery},
			benchMarkFn("t1", 203),
			benchMarkFn("t3", 404),
		)
		require.Nil(t, err)
		require.Equal(t, ClusterStatRun{
			Stats: map[string]StatSummary{
				fooStat.Query: makeExpectedTs(fooTS, fooAggQuery, 5, "1", "2", "3"),
				barStat.Query: makeExpectedTs(barTS, barAggQuery, 5, "1", "2", "3"),
			},
			Total: map[string]float64{"t1": 203, "t3": 404},
		}, *testRun)
	})
	t.Run("multi tag, multi stat, 5 ticks with openmetrics false", func(t *testing.T) {
		c := getClusterStatCollector(ctx, ctrl, []expectPromRangeQuery{
			makePromQueryRange(fooStat.Query, []string{"1", "2", "3"}, 5, fooTS, false),
			makePromQueryRange(barStat.Query, []string{"1", "2", "3"}, 5, barTS, false),
		})
		mockTest := getMockTest(t, ctrl, false, statsFileDest)
		mockCluster := NewMockCluster(ctrl)
		statsWriter = func(ctx context.Context, tt test.Test, c cluster.Cluster, buffer *bytes.Buffer, dest string) error {
			require.Equal(t, mockTest, tt)
			require.Equal(t, mockCluster, c)
			require.Equal(t, "/location/of/file/stats.json", dest)
			var expectedJson map[string]interface{}
			require.Nil(t, json.Unmarshal([]byte(expectedJSONContent), &expectedJson))
			var actualJson map[string]interface{}
			require.Nil(t, json.Unmarshal(buffer.Bytes(), &actualJson))
			require.Equal(t, expectedJson, actualJson)
			return nil
		}
		defer func() {
			statsWriter = nil
		}()
		testRun, err := c.Export(
			ctx, mockCluster, mockTest, false, statsTestingStartTime,
			statsTestingStartTime.Add(statsTestingDuration*time.Duration(5)),
			[]AggQuery{fooAggQuery, barAggQuery},
			benchMarkFn("t1", 203),
			benchMarkFn("t3", 404),
		)
		require.Nil(t, err)
		require.Equal(t, ClusterStatRun{
			Stats: map[string]StatSummary{
				fooStat.Query: makeExpectedTs(fooTS, fooAggQuery, 5, "1", "2", "3"),
				barStat.Query: makeExpectedTs(barTS, barAggQuery, 5, "1", "2", "3"),
			},
			Total: map[string]float64{"t1": 203, "t3": 404},
		}, *testRun)
	})
}

func getClusterStatCollector(
	ctx context.Context, ctrl *gomock.Controller, mockedPromResults []expectPromRangeQuery,
) clusterStatCollector {
	return clusterStatCollector{
		interval: statsTestingDuration,
		promClient: func(ctrl *gomock.Controller) prometheus.Client {
			c := NewMockClient(ctrl)
			e := c.EXPECT()
			for _, m := range mockedPromResults {
				e.QueryRange(ctx, m.q, promv1.Range{Start: m.fromTime, End: m.toTime, Step: statsTestingDuration}).Return(
					model.Value(m.ret),
					nil,
					nil,
				)
			}
			return c
		}(ctrl),
	}
}

func getMockTest(
	t *testing.T, ctrl *gomock.Controller, exportOpenmetrics bool, dest string,
) *MockTest {
	tst := NewMockTest(ctrl)
	l, err := logger.RootLogger("", logger.TeeToStdout)
	if err != nil {
		t.Fatal(err)
	}
	tst.EXPECT().L().Return(l)
	tst.EXPECT().ExportOpenmetrics().Times(1).Return(exportOpenmetrics)
	tst.EXPECT().PerfArtifactsDir().Times(1).Return(dest)
	tst.EXPECT().Owner().Return("roachtest_mock").AnyTimes()
	return tst
}

// parseData is responsible for parsing the data and convert the same to a map pf metrics type to a list of metrics.
// The key is the line mentioning the metric type + the tag if present.
// The value is a list of metrics entries in the expected sequence with timestamp removed from each line.
func parseData(content string) map[string][]string {
	lines := strings.Split(content, "\n")
	// output is a map of the type of metrics appended with the tag as key and the list of metrics as values.
	// the values are must be sorted by timestamp. So, sequence of the data must be exactly matching after removing the timestamp
	output := make(map[string][]string)
	currentGauge := ""
	for _, line := range lines {
		if strings.HasPrefix(line, "# TYPE") || line == "# EOF" {
			currentGauge = line
			continue
		}
		match := metricsRegExNoTimestamp.FindStringSubmatch(line)
		// tag is extracted as data for each tag must be sorted by timestamp
		// the tag may not be present as well. In that case
		tag := ""
		if len(match) > 1 {
			// compare and remove the timestamp which keeps changing
			line = match[1]
			tagMatch := metricsRegExTag.FindStringSubmatch(line)
			if len(tagMatch) > 1 {
				tag = fmt.Sprintf("_%s", tagMatch[1])
			}
		}
		key := fmt.Sprintf("%s%s", currentGauge, tag)
		output[key] = append(output[key], line)
	}
	return output
}

const expectedJSONContent = `{
  "total": {
    "t1": 203,
    "t3": 404
  },
  "stats": {
    "bar_count": {
      "Time": [
        1608854400000000000,
        1608854410000000000,
        1608854420000000000,
        1608854430000000000
      ],
      "Value": [
        123,
        246,
        369,
        492
      ],
      "Tagged": {
        "1": [
          111,
          222,
          333,
          444
        ],
        "2": [
          1,
          2,
          3,
          4
        ],
        "3": [
          11,
          22,
          33,
          44
        ]
      },
      "AggTag": "sum(foo)",
      "Tag": "bar_count"
    },
    "foo_count": {
      "Time": [
        1608854400000000000,
        1608854410000000000,
        1608854420000000000,
        1608854430000000000
      ],
      "Value": [
        123,
        246,
        369,
        492
      ],
      "Tagged": {
        "1": [
          1,
          2,
          3,
          4
        ],
        "2": [
          11,
          22,
          33,
          44
        ],
        "3": [
          111,
          222,
          333,
          444
        ]
      },
      "AggTag": "sum(foo)",
      "Tag": "foo_count"
    }
  }
}`
