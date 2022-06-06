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

//go:generate mockgen -package=clusterstats -destination mocks_generated_test.go github.com/cockroachdb/cockroach/pkg/cmd/roachtest/prometheus Client

// cluster_stats provides utilities to:
// (1) collect registered cluster statistics at an interval, tagged by node.
// (2) collect timeseries data over a roachtest run, which may exported into a
//     roachperf pareseable format. This format is used by roachperf to
//     display benchmark results over multiple runs, in addition to time
//     series statistics per-run.

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/prometheus"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

const (
	defaultScrapeInterval = 10 * time.Second
)

// Interval represents a start and end time.
type Interval struct {
	From time.Time
	To   time.Time
}

// StatPoint represents a cluster metric value at a point in time.
type StatPoint struct {
	Time  int64
	Value float64
}

// StatSeries is a collection of StatPoints, sorted in ascending order by
// StatPoint.Time.
type StatSeries []StatPoint

// StatCollector provides a wrapper over PromQL, to query and return interval
// and point in time statistics.
type StatCollector interface {
	// CollectPoint collects a point in time result for a query.
	CollectPoint(context.Context, *logger.Logger, time.Time, string) (map[string]map[string]StatPoint, error)
	// CollectInterval collects the timeseries data over a period for a query.
	CollectInterval(context.Context, *logger.Logger, Interval, string) (map[string]map[string]StatSeries, error)
	// Exporter returns a StatExporter, using the StatCollector.
	Exporter() StatExporter
	// Cleanup terminates the prometheus client. It should be called once at
	// the end of a run.
	CleanUp()
}

// ClusterStat represents a tagged query.
type ClusterStat struct {
	Query     string
	LabelName string
}

type clusterStatCollector struct {
	promClient prometheus.Client
	interval   time.Duration
	cleanup    func()
}

// CleanUp terminates the prometheus client. It should be called once at
// the end of a run.
func (cs *clusterStatCollector) CleanUp() {
	cs.cleanup()
}

// NewStatsCollector returns an implementation of the StatCollector interface.
func NewStatsCollector(ctx context.Context, t test.Test, c cluster.Cluster) (StatCollector, error) {
	return newClusterStatsCollector(ctx, t, c)
}

func newClusterStatsCollector(
	ctx context.Context, t test.Test, c cluster.Cluster,
) (*clusterStatCollector, error) {
	// setupPrometheus puts the prometheus binary on the last node in the
	// cluster. It will then start the prometheus process, adding the remaining
	// nodes as scrape targets.
	promCfg, cleanupFunc := SetupPrometheus(ctx, t, c, nil)
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
		interval:   defaultScrapeInterval,
		cleanup:    cleanupFunc,
	}, nil
}

func (cs *clusterStatCollector) Exporter() StatExporter {
	return cs
}

// CollectPoint collects a point in time result for a query.
func (cs *clusterStatCollector) CollectPoint(
	ctx context.Context, l *logger.Logger, at time.Time, q string,
) (map[string]map[string]StatPoint, error) {
	fromVal, warnings, err := cs.promClient.Query(ctx, q, at)
	if err != nil {
		return nil, err
	}
	if len(warnings) > 0 {
		return nil, errors.Newf("found warnings querying prometheus: %s", warnings)
	}

	fromVec := fromVal.(model.Vector)
	if len(fromVec) == 0 {
		return nil, errors.Newf("Empty vector result for query %s @ %s (%v)", q, at.Format(time.RFC3339), fromVal)
	}

	// Here we populate a map with values, for every label that is returned,
	// with the tagged values. For example, if the query were
	// rebalancing_queriespersecond at time 110 and there were two stores (1,2)
	// with ip addresses 10.0.0.1 and 127.0.0.1, for store 2 and store 1
	// respectively.
	// {
	//    "store": {
	//        "1": {Time: 100, Value: 777},
	//        "2": {Time: 100, Value: 42},
	//    },
	//    "instance": {
	//         "10.0.0.1":  {Time: 100, Value: 42},
	//         "127.0.0.1": {Time: 100, Value: 777},
	//     },
	// }
	result := make(map[string]map[string]StatPoint)
	for _, sample := range fromVec {
		statPoint := StatPoint{Time: sample.Timestamp.Time().UnixNano(), Value: float64(sample.Value)}
		for labelName, labelValue := range sample.Metric {
			if _, ok := result[string(labelName)]; !ok {
				result[string(labelName)] = make(map[string]StatPoint)
			}
			result[string(labelName)][string(labelValue)] = statPoint
		}
	}

	return result, nil
}

// CollectInterval collects the timeseries data over a period for a query.
func (cs *clusterStatCollector) CollectInterval(
	ctx context.Context, l *logger.Logger, interval Interval, q string,
) (map[string]map[string]StatSeries, error) {
	if !interval.valid() {
		l.PrintfCtx(
			ctx,
			"to %s < from %s, skipping",
			interval.From.Format(time.RFC3339),
			interval.To.Format(time.RFC3339),
		)
		return nil, nil
	}

	r := promv1.Range{Start: interval.From, End: interval.To, Step: cs.interval}

	fromVal, warnings, err := cs.promClient.QueryRange(ctx, q, r)
	if err != nil {
		return nil, err
	}
	if len(warnings) > 0 {
		return nil, errors.Newf("found warnings querying prometheus: %s", warnings)
	}

	// The interval must have only one timeseries result, otherwise the
	// intended timeseries cannot be inferred. In the case where no stats could
	// be collected (no metric found), we return and log.
	fromMatrixTagged := fromVal.(model.Matrix)
	if len(fromMatrixTagged) == 0 {
		return nil, errors.Newf("Empty matrix result for query %s @ [%s,%s] (%v)", q, interval.From.Format(time.RFC3339), interval.To.Format(time.RFC3339), fromVal)
	}

	// Here we populate a map with values, for every label that is returned,
	// with the tagged values. For example, if the query were
	// rebalancing_queriespersecond in the interval [100,110] and there were
	// two stores (1,2) with ip addresses 10.0.0.1 and 127.0.0.1, for store 2
	// and store 1 respectively.
	// {
	//    "store": {
	//        "1": {
	//            {Time: 100, Value: 777},
	//            {Time: 110, Value: 888}
	//        },
	//        "2": {
	//            {Time: 100, Value: 42},
	//            {Time: 110, Value 42},
	//        },
	//    },
	//    "instance": {
	//         "10.0.0.1":  {
	//            {Time: 100, Value: 42},
	//            {Time: 110, Value 42},
	//        },
	//         "127.0.0.1": {
	//            {Time: 100, Value: 777},
	//            {Time: 110, Value: 888}
	//        },
	//     },
	// }
	result := make(map[string]map[string]StatSeries)
	for _, stream := range fromMatrixTagged {
		statSeries := make(StatSeries, len(stream.Values))
		for i := range stream.Values {
			statSeries[i] = StatPoint{Time: stream.Values[i].Timestamp.Time().UnixNano(), Value: float64(stream.Values[i].Value)}
		}

		for labelName, labelValue := range stream.Metric {
			if _, ok := result[string(labelName)]; !ok {
				result[string(labelName)] = make(map[string]StatSeries)
			}
			result[string(labelName)][string(labelValue)] = statSeries
		}
	}

	return result, nil
}
