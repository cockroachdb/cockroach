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
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

// ClusterStat represents a filtered query by the given LabelName. For example,
//
//	ClusterStat{Query: "rebalancing_queriespersecond", LabelName: "store"}
//
// would collect a QPS stat per store in the cluster.
type ClusterStat struct {
	Query     string
	LabelName string
}

// AggregateFn processes a map of labeled series', aggregating into a single
// series. It must also return an appropriate label for the resulting series.
type AggregateFn func(query string, series [][]float64) (string, []float64)

// AggQuery represents a two tier query, that (1) provides a query generating
// multiple labeled timeseries results (AggQuery.Stat) and (2) a method to
// aggregate the multiple time series in (1). (2) May either be a PromQL query,
// defined in AggQuery.Query; or an AggregateFn defined in AggQuer.AggQuery,
// which proceses the result of (1). AggQuery.Interval defines the [from,to]
// time to query.
type AggQuery struct {
	Stat     ClusterStat
	Query    string
	AggFn    AggregateFn
	Interval Interval
}

// StatExporter defines an interface to export statistics to roachperf.
type StatExporter interface {
	// Export collects, serializes and saves a roachperf file, with statistics
	// collect from - to time, for the queries given. benchmarkFns define the
	// group of functions that summarize a run into a single pair, tag: result.
	// These pairs are plotted over multiple test runs and may be used to spot
	// regressions or track improvements. For example, in the case of
	// decomissioning we may export the time taken to decomission a node as
	// well as the cost in terms of snapshot bytes sent. benchmarkFns has no
	// requirement to make use of the StatSummary values given, rather they are
	// provided for conveiencen to derive a benchmark pair, if suited.
	Export(
		ctx context.Context,
		c cluster.Cluster,
		t test.Test,
		from time.Time,
		to time.Time,
		queries []AggQuery,
		benchmarkFns ...func(map[string]StatSummary) (string, float64),
	) error
	// CollectSummaries collects the summary for statistics, used in export.
	CollectSummaries(
		ctx context.Context,
		l *logger.Logger,
		interval Interval,
		statQueries []AggQuery,
	) (map[string]StatSummary, error)
}

// StatSummary holds the timeseries of some cluster aggregate statistic. The
// attributable, e.g. per instance statistics that contribute to this aggregate
// are also held. The aggregate tag describes the top aggregation that occurred
// over the multiple series of data to combine into one e.g. sum(qps), cv(qps),
// max(qps). The tag describes what stat is collected from each instance.
type StatSummary struct {
	Time   []int64
	Value  []float64
	Tagged map[string][]float64
	AggTag string
	Tag    string
}

// Export collects, serializes and saves a roachperf file, with statistics
// collect from - to time, for the AggQuery(s) given. The format is described
// in the doc.go and the AggQuery definition. In addition to the AggQuery(s),
// the benchmarkFn(s), return the top level scalar value(s) that summarize the
// run. For example, in a roachtest regarding decomissioning time, we may
// return the duration elapsed from the start of decomissioning till the end.
// This may be unrelated to the AggQueries.
func (cs *clusterStatCollector) Export(
	ctx context.Context,
	c cluster.Cluster,
	t test.Test,
	from time.Time,
	to time.Time,
	queries []AggQuery,
	benchmarkFns ...func(summaries map[string]StatSummary) (string, float64),
) error {
	l := t.L()
	summaries, err := cs.CollectSummaries(ctx, l, Interval{From: from, To: to}, queries)
	if err != nil {
		l.ErrorfCtx(ctx, "unable to collect cluster stat summaries: %+v", err)
		return err
	}

	summaryValues := make(map[string]float64)
	for _, scalarFn := range benchmarkFns {
		t, result := scalarFn(summaries)
		summaryValues[t] = result
	}

	perfBuf, err := serializeReport(summaries, summaryValues)
	if err != nil {
		return errors.Wrap(err, "failed to serialize perf artifacts")
	}

	return writeOutRoachPerf(ctx, t, c, perfBuf)
}

// writeOutRoachPerf is a utility function that writes out a buffer to the
// performance artifacts directory on the first node in a cluster.
func writeOutRoachPerf(
	ctx context.Context, t test.Test, c cluster.Cluster, buffer *bytes.Buffer,
) error {
	l := t.L()
	dest := filepath.Join(t.PerfArtifactsDir(), "stats.json")
	if err := c.RunE(ctx, c.Node(1), "mkdir -p "+filepath.Dir(dest)); err != nil {
		l.ErrorfCtx(ctx, "failed to create perf dir: %+v", err)
		return err
	}
	if err := c.PutString(ctx, buffer.String(), dest, 0755, c.Node(1)); err != nil {
		l.ErrorfCtx(ctx, "failed to upload perf artifacts to node: %s", err.Error())
		return err
	}
	return nil
}

// serializeReport serializes the passed in statistics into a roachperf
// parseable performance artifact format.
func serializeReport(
	summaries map[string]StatSummary, summaryStats map[string]float64,
) (*bytes.Buffer, error) {
	// ClusterStatsRun holds the summary value for a test run as well as per
	// stat information collected during the run. This struct is mirrored in
	// cockroachdb/roachperf for deserialization.
	type ClusterStatRun struct {
		Total map[string]float64     `json:"total"`
		Stats map[string]StatSummary `json:"stats"`
	}

	testRun := ClusterStatRun{Stats: make(map[string]StatSummary)}

	for tag, summary := range summaries {
		testRun.Stats[tag] = summary
	}
	testRun.Total = summaryStats

	bytesBuf := bytes.NewBuffer([]byte{})
	jsonEnc := json.NewEncoder(bytesBuf)
	err := jsonEnc.Encode(testRun)
	if err != nil {
		return nil, err
	}

	return bytesBuf, nil
}

// CollectSummaries iterates through the passed in aggregate queries and
// combines the results.
func (cs *clusterStatCollector) CollectSummaries(
	ctx context.Context, l *logger.Logger, interval Interval, statQueries []AggQuery,
) (map[string]StatSummary, error) {
	summaries := make(map[string]StatSummary)
	for _, clusterStat := range statQueries {
		clusterStat.Interval = interval
		summary, err := cs.getStatSummary(ctx, l, clusterStat)
		if err != nil {
			l.PrintfCtx(ctx, "Unable to collect summary (%v): %s. Skipping.", clusterStat, err.Error())
		}
		if summary.Tag != "" {
			summaries[summary.Tag] = summary
		}
	}
	return summaries, nil
}

// getStatSummary collects the individual results and an aggregate for an
// AggQuery. The AggQuery is executed in two components:
//
// (1) AggQuery.Stat declares a PromQL query to be used over the given
// interval. The corresponding AggQuery.Stat.LabelName declares the tag to
// filter the resulting time series results on. For example, AggQuery.Stat =
// {Query: "rebalancing_queriespersecond", LabelName: "store"} would return the
// per-store qps (e.g. 3 stores, 3 points, 100*storeID QPS): StatSummary.Tagged
// = {"1": {100,100,100}, "2": {200,200,200], "3": {300,300,300}}.
//
// (2) The second component is the aggregating query, which combines multiple
// time series into a single one, for the same metric. This can either be a
// query (2a) or aggregating function (2b), depending on whether the function
// is supported by prometheus.
//
//	(2a) AggQuery.Query declares a prometheus query to be used over the given
//	interval. For example, AggQuery.Query = "sum(rebalancing_queriespersecond)"
//	would return StatSummary.Value = {600, 600, 600}.
//	(2b) AggQuery.AggFn is a substitute for 2a, it aggregates over a collection
//	of labeled time series, returning a single time series. For example,
//	AggQuery.AggFn = func(...) {return max(...)} would return
//	StatSummary.Value{300, 300, 300}. It must also return an AggregateTag to
//	identify the resulting timeseries.
func (cs *clusterStatCollector) getStatSummary(
	ctx context.Context, l *logger.Logger, summaryQuery AggQuery,
) (StatSummary, error) {
	ret := StatSummary{}

	taggedSeries, err := cs.CollectInterval(ctx, l, summaryQuery.Interval, summaryQuery.Stat.Query)
	if err != nil {
		return ret, err
	}

	trimmedTaggedSeries, n, trimmedInterval := TrimTaggedSeries(ctx, l, taggedSeries)

	// When there are no values returned in the trimmed interval, return an
	// error, to log and skip this summary.
	if n == 0 {
		return ret, errors.Errorf("No timeseries values found")
	}

	// We are unable to find the label name requested in the returned time
	// series.
	if _, ok := trimmedTaggedSeries[summaryQuery.Stat.LabelName]; !ok {
		return ret, errors.Newf(
			"Unable to collect timeseries for query %s, on label %s",
			summaryQuery.Stat.Query,
			summaryQuery.Stat.LabelName,
		)
	}

	labelNameSeries := trimmedTaggedSeries[summaryQuery.Stat.LabelName]

	ret.Time = make([]int64, n)
	ret.Value = make([]float64, n)
	ret.Tag = summaryQuery.Stat.Query

	ret.Tagged = make(map[string][]float64)
	for labelName, series := range labelNameSeries {
		streamSize := n
		ret.Tagged[labelName] = make([]float64, streamSize)
		if streamSize > len(series) {
			return ret, errors.Newf(
				"Differing lengths on stream size on query %s, expected %d, actual %d",
				summaryQuery.Stat.Query,
				streamSize,
				len(series),
			)
		} else if streamSize < len(series) {
			// When the new series is longer than the expected, we are able to
			// trim it to the expected length by discarding values at the end.
			series = series[:streamSize]
		}

		for i, val := range series {
			ret.Time[i] = val.Time
			ret.Tagged[labelName][i] = val.Value
		}
	}

	// When an aggregating function is given (AggQuery.AggFn), use this.
	// Otherwise, parse the prometheus result in a similar manner to above.
	if summaryQuery.AggFn != nil {
		tag, val := summaryQuery.AggFn(summaryQuery.Stat.Query, ConvertEqualLengthMapToMat(ret.Tagged))
		ret.Value = val
		ret.AggTag = tag
	} else {
		taggedSummarySeries, err := cs.CollectInterval(ctx, l, trimmedInterval, summaryQuery.Query)
		if err != nil {
			return ret, err
		}

		ret.AggTag = summaryQuery.Query
		// If there is more than one label name associated with the summary, we
		// cannot be sure which is the correct label.
		if len(taggedSummarySeries) != 1 {
			return ret, errors.Newf(
				"Unable to find correct summary result for query %s [%s,%s], "+
					"there exists %d results when there should be 1",
				summaryQuery.Query,
				trimmedInterval.From,
				trimmedInterval.To,
				len(taggedSummarySeries),
			)
		}
		for _, labeledSeries := range taggedSummarySeries {
			// If there is more than one label value associated with the
			// summary, we cannot be sure which is the correct label.
			if len(labeledSeries) != 1 {
				return ret, nil
			}
			for _, series := range labeledSeries {
				for i := 0; i < len(series) && i < len(ret.Value); i++ {
					ret.Value[i] = series[i].Value
				}
			}
		}
	}
	return ret, nil
}
