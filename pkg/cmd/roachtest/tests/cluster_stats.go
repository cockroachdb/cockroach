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

// cluster_stats provides utilities to:
// (1) collect registered cluster statistics at an interval, tagged by node.
// (2) collect timeseries data over a roachtest run, which may exported into a
//     roachperf pareseable format. This format is used by roachperf to
//     display benchmark results over multiple runs, in addition to time
//     series statistics per-run.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/prometheus"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// query represents a query for cluster statistics.
type query string

// tag represents a tag associated with a cluster stat.
type tag string

// clusterStat represents a tagged query.
type clusterStat struct {
	query query
	tag   tag
}

// clusterStatCollector maintains a wrapper around a prometheus client.
type clusterStatCollector struct {
	promClient prometheus.Client
	interval   time.Duration
	cleanup    func()
	startTime  time.Time
}

func newClusterStatsCollector(
	ctx context.Context, t test.Test, c cluster.Cluster, interval time.Duration,
) (*clusterStatCollector, error) {
	// setupPrometheus puts the prometheus binary on the last node in the
	// cluster. It will then start the prometheus process, adding the remaining
	// nodes as scrape targets.
	promCfg, cleanupFunc := setupPrometheus(ctx, t, c, nil, false, []workloadInstance{})
	prometheusNodeIP, err := c.ExternalIP(ctx, t.L(), promCfg.PrometheusNode)
	if err != nil {
		return nil, err
	}

	client, err := promapi.NewClient(promapi.Config{
		Address: fmt.Sprintf("http://%s:9090", prometheusNodeIP[0]),
	})
	if err != nil {
		return nil, err
	}

	return &clusterStatCollector{
		promClient: promv1.NewAPI(client),
		interval:   interval,
		cleanup:    cleanupFunc,
	}, nil
}

// scrapeStatInterval collects the timeseries data over a period for a query.
func (cs *clusterStatCollector) scrapeStatInterval(
	ctx context.Context, q query, l *logger.Logger, fromTime time.Time, toTime time.Time,
) (model.Value, error) {
	// Add an extra interval to fromTime to account for the first data point
	// which may include some node not being fully started. Or missing a datapoint.
	fromTime = fromTime.Add(cs.interval)
	// Scale back the toTime to account for the data point potentially already
	// having data of a node which may have already shutdown or not reported
	// the last value.
	toTime = toTime.Add(-cs.interval)
	if !toTime.After(fromTime) {
		l.PrintfCtx(
			ctx,
			"to %s < from %s, skipping",
			toTime.Format(time.RFC3339),
			fromTime.Format(time.RFC3339),
		)
		return nil, nil
	}

	r := promv1.Range{Start: fromTime, End: toTime, Step: cs.interval}

	fromVal, warnings, err := cs.promClient.QueryRange(ctx, string(q), r)
	if err != nil {
		return nil, err
	}
	if len(warnings) > 0 {
		return nil, errors.Newf("found warnings querying prometheus: %s", warnings)
	}

	return fromVal, nil
}

// scrapeStatPoint collects a point in time result for a query.
func (cs *clusterStatCollector) scrapeStatPoint(
	ctx context.Context, l *logger.Logger, q query, at time.Time,
) (model.Value, error) {
	fromVal, warnings, err := cs.promClient.Query(ctx, string(q), at)
	if err != nil {
		return nil, err
	}
	if len(warnings) > 0 {
		return nil, errors.Newf("found warnings querying prometheus: %s", warnings)
	}

	return fromVal, nil
}

// statSummaryQuery holds the query and necessary information to aggregate over
// the query result, mapping it from two dimensions to one. This may make use
// of prometheus' aggregation functions in aggQuery or pass a function to
// process it instead.
type statSummaryQuery struct {
	stat     clusterStat
	aggQuery query
	aggFn    func(vals map[tag][]float64) []float64
	aggTag   tag
}

// StatSummary holds the timeseries of some cluster aggregate statistic. The
// attributable, e.g. per instance statistics that contribute to this aggregate
// are also held. The aggregate tag describes the top aggregation that occurred
// over the multiple series of data to combine into one e.g. sum(qps), cv(qps),
// max(qps). The tag describes what stat is collected from each instance.
type StatSummary struct {
	Time   []int64
	Value  []float64
	Tagged map[tag][]float64
	AggTag tag
	Tag    tag
}

// finalize collects, serializes and saves a roachperf file for fromTime to
// toTime, for the statQueries given. It requires an aggregating function to be
// given, in order to report the "total" value for the run, which is plotted
// over multiple runs in roachperf (i.e. per night).
func (cs *clusterStatCollector) finalize(
	ctx context.Context,
	c cluster.Cluster,
	l *logger.Logger,
	artifcatsDir string,
	fromTime time.Time,
	toTime time.Time,
	statQueries []statSummaryQuery,
	scalars ...func(summaries map[tag]StatSummary) (string, float64),
) error {
	summaries, err := cs.collectSummaries(ctx, l, fromTime, toTime, statQueries)
	if err != nil {
		l.ErrorfCtx(ctx, "unable to collect cluster stat summaries: %+v", err)
		return err
	}

	summaryValues := make(map[string]float64)
	for _, scalarFn := range scalars {
		t, result := scalarFn(summaries)
		summaryValues[t] = result
	}

	l.PrintfCtx(ctx, "summaries %+v", summaries)
	perfBuf, err := serializeReport(summaries, summaryValues)
	if err != nil {
		l.ErrorfCtx(ctx, "failed to serialize perf artifacts: %+v", err)
		return err
	}

	return writeOutRoachPerf(ctx, l, c, artifcatsDir, perfBuf)
}

// writeOutRoachPerf is a utility function that writes out a buffer to the
// performance artifacts directory on the first node in a cluster.
func writeOutRoachPerf(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, perfDir string, buffer *bytes.Buffer,
) error {
	dest := filepath.Join(perfDir, "stats.json")
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
	summaries map[tag]StatSummary, summaryStats map[string]float64,
) (*bytes.Buffer, error) {
	// clusterStatsRun holds the summary value for a test run as well as per
	// stat information collected during the run. This struct is mirrored in
	// cockroachdb/roachperf for deserialization.
	type ClusterStatRun struct {
		Total map[string]float64  `json:"total"`
		Stats map[tag]StatSummary `json:"stats"`
	}

	testRun := ClusterStatRun{Stats: make(map[tag]StatSummary)}

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

// collectSummaries iterates through the passed in queries and collects the
// results.
func (cs *clusterStatCollector) collectSummaries(
	ctx context.Context,
	l *logger.Logger,
	fromTime time.Time,
	toTime time.Time,
	statQueries []statSummaryQuery,
) (map[tag]StatSummary, error) {
	summaries := make(map[tag]StatSummary)
	for _, clusterStat := range statQueries {
		summary, err := cs.getStatSummary(ctx, l, clusterStat, fromTime, toTime)
		if err != nil {
			return nil, err
		}
		summaries[summary.Tag] = summary
	}
	return summaries, nil
}

// getStatSummary returns the aggregate and individual attributable stats over
// fromTime to toTime for a cluster statistic.
func (cs *clusterStatCollector) getStatSummary(
	ctx context.Context,
	l *logger.Logger,
	summaryQuery statSummaryQuery,
	fromTime time.Time,
	toTime time.Time,
) (StatSummary, error) {
	ret := StatSummary{}

	// Deserialize the prometheus values for the individual summary query stats.
	fromValTagged, err := cs.scrapeStatInterval(ctx, summaryQuery.stat.query, l, fromTime, toTime)
	if err != nil {
		return ret, err
	}
	fromMatrixTagged := fromValTagged.(model.Matrix)
	if len(fromMatrixTagged) == 0 {
		return ret, errors.Newf("unexpected empty fromMatrix (%v) for %s @ %s", fromMatrixTagged, summaryQuery.stat.query, fromTime.Format(time.RFC3339))
	}

	// fromMatrix is of the form []*SampleStream, where a sample stream is a
	// tagged metric over time. Here we assume that stats are collected per
	// instance.
	ret.Tagged = make(map[tag][]float64)
	for i, stream := range fromMatrixTagged {
		streamSize := len(stream.Values)
		statTag := tag(stream.Metric["instance"])
		ret.Tagged[statTag] = make([]float64, streamSize)

		// On the first metric, initialize the tick array to the correct size of samples.
		if i == 0 {
			ret.Time = make([]int64, streamSize)
			ret.Value = make([]float64, streamSize)
		}
		for j, val := range stream.Values {
			if i == 0 {
				ret.Time[j] = val.Timestamp.Unix()
			}
			ret.Tagged[statTag][j] = float64(val.Value)
		}
	}

	// When an aggregaton function is given, prefer that over a prometheus
	// query for aggregation. Otherwise, parse the prometheus result in a
	// similar manner to above.
	if summaryQuery.aggFn != nil {
		ret.Value = summaryQuery.aggFn(ret.Tagged)
	} else {
		fromValSummary, err := cs.scrapeStatInterval(ctx, summaryQuery.aggQuery, l, fromTime, toTime)
		if err != nil {
			return ret, err
		}

		// The summary must have only one timeseries result, otherwise the
		// intended timeseries cannot be inferred. In the case where no stats
		// could be collected (no metric found), we return an empty array and
		// log the issue.
		fromMatrixSummary := fromValSummary.(model.Matrix)
		if len(fromMatrixSummary) > 1 {
			return ret, errors.Newf("unexpected additional entries in fromMatrix (%+v) for %s @ %s", fromMatrixSummary, summaryQuery.aggQuery, fromTime.Format(time.RFC3339))
		} else if len(fromMatrixSummary) == 0 {
			ret.Value = make([]float64, len(ret.Time))
			l.PrintfCtx(ctx, "unable to find any results for aggregate query %s", summaryQuery.aggQuery)
		} else {
			toMatrixSummary := fromMatrixSummary[0]
			ret.Value = make([]float64, len(toMatrixSummary.Values))
			for i, val := range toMatrixSummary.Values {
				ret.Value[i] = float64(val.Value)
			}
		}
	}

	ret.Tag = summaryQuery.stat.tag
	ret.AggTag = summaryQuery.aggTag
	return ret, nil
}

// clusterStatStreamer collects registered point-in-time stats at an interval
// and reports them to processTickFn.
type clusterStatStreamer struct {
	statCollector *clusterStatCollector
	processTickFn func(stats []statEvent) bool
	registered    map[tag]query
	interval      time.Duration
	errs          []error
}

// statEvent holds a point-in-time stat for a query. The stat is reported
// against each instance, which is held in values.
type statEvent struct {
	tag    tag
	values map[tag]float64
}

func newClusterStatStreamer(
	statCollector *clusterStatCollector,
	processTickFn func(stats []statEvent) bool,
	interval time.Duration,
) *clusterStatStreamer {
	return &clusterStatStreamer{
		statCollector: statCollector,
		registered:    make(map[tag]query),
		processTickFn: processTickFn,
		interval:      interval,
		errs:          make([]error, 0, 1),
	}
}

// registerStat adds a cluster stat to be collected.
func (css *clusterStatStreamer) registerStat(stats ...clusterStat) {
	for _, stat := range stats {
		css.registered[stat.tag] = stat.query
	}
}

// parseStatEvent parses a prometheus point-in-time query result into a
// statEvent. It associates the instance, e.g. 127.0.0.1:8080 with individual
// results, as every promtheus metric will have unique instance tags.
func parseStatEvent(t tag, fromVal model.Value) (statEvent, error) {
	ret := statEvent{}

	fromVec := fromVal.(model.Vector)
	if len(fromVec) == 0 {
		return ret, errors.Newf("unexpected empty fromVector %v", fromVal)
	}

	ret.tag = t
	ret.values = make(map[tag]float64)
	for _, sample := range fromVec {
		statTag := tag(sample.Metric["instance"])
		ret.values[statTag] = float64(sample.Value)
	}
	return ret, nil
}

// run collects cluster statistics, attributable to by nodes in the cluster.
// This collection is done at an interval defined in the stat streamer, whilst
// the query time is increased bt the collecter interval. The results are
// passed the provided processTickFn. This function will run indefinitely,
// until either:
//   (1) the context cancellation
//   (2) processTickFn returns true, indicating that it has finished.
func (css *clusterStatStreamer) run(ctx context.Context, l *logger.Logger, startTime time.Time) {
	var statsTimer timeutil.Timer
	defer statsTimer.Stop()
	statsTimer.Reset(css.interval)
	queryTime := startTime
	for {
		select {
		case <-ctx.Done():
			return
		case <-statsTimer.C:
			eventBuffer := make([]statEvent, 0, 1)
			statsTimer.Read = true
			for tag, q := range css.registered {
				fromVal, err := css.statCollector.scrapeStatPoint(ctx, l, q, queryTime)
				if err != nil {

					css.writeErr(ctx, l, err)
				}
				event, err := parseStatEvent(tag, fromVal)
				if err != nil {
					css.writeErr(ctx, l, err)
				}
				eventBuffer = append(eventBuffer, event)
			}
			statsTimer.Reset(css.interval)
			queryTime = queryTime.Add(css.statCollector.interval)
			if finish := css.processTickFn(eventBuffer); finish {
				return
			}
		}
	}
}

func (css *clusterStatStreamer) writeErr(ctx context.Context, l *logger.Logger, err error) {
	l.PrintfCtx(ctx, "error during stat streaming: %v", err)
	css.errs = append(css.errs, err)
}

func (css *clusterStatStreamer) err() error {
	var err error
	for i := range css.errs {
		if i == 0 {
			err = css.errs[i]
		} else {
			err = errors.CombineErrors(err, css.errs[i])
		}
	}
	return err
}
