// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusterstats

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/errors"
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
	// CollectPoint collects a stat for a query, at the time provided.
	CollectPoint(context.Context, *logger.Logger, time.Time, string) (map[string]map[string]StatPoint, error)
	// CollectInterval collects a timeseries for the given query, that is
	// contained within the interval provided.
	CollectInterval(context.Context, *logger.Logger, Interval, string) (map[string]map[string]StatSeries, error)
	// Exporter returns a StatExporter, using this StatCollector.
	Exporter() StatExporter
}

type clusterStatCollector struct {
	promClient prometheus.Client
	interval   time.Duration
}

// NewStatsCollector returns an implementation of the StatCollector interface.
func NewStatsCollector(ctx context.Context, promClient prometheus.Client) StatCollector {
	return newClusterStatsCollector(ctx, promClient)
}

func newClusterStatsCollector(
	ctx context.Context, promClient prometheus.Client,
) *clusterStatCollector {
	return &clusterStatCollector{
		promClient: promClient,
		interval:   defaultScrapeInterval,
	}
}

// Exporter returns a StatExporter, using this StatCollector.
func (cs *clusterStatCollector) Exporter() StatExporter {
	return cs
}

// CollectPoint collects a stat for a query, at the time provided. It will
// populate a map with values, for every label that is returned, with the
// tagged values. For example, if the query were rebalancing_queriespersecond
// at time 110 and there were two stores (1,2) with ip addresses 10.0.0.1 and
// 127.0.0.1, for store 2 and store 1 respectively.
//
//	{
//	   "store": {
//	       "1": {Time: 100, Value: 777},
//	       "2": {Time: 100, Value: 42},
//	   },
//	   "instance": {
//	        "10.0.0.1":  {Time: 100, Value: 42},
//	        "127.0.0.1": {Time: 100, Value: 777},
//	    },
//	}
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

	result := make(map[string]map[string]StatPoint)
	for _, sample := range fromVec {
		statPoint := StatPoint{
			Time:  sample.Timestamp.Time().UnixNano(),
			Value: float64(sample.Value),
		}
		for labelName, labelValue := range sample.Metric {
			if _, ok := result[string(labelName)]; !ok {
				result[string(labelName)] = make(map[string]StatPoint)
			}
			result[string(labelName)][string(labelValue)] = statPoint
		}
	}

	return result, nil
}

// CollectInterval collects a timeseries for the given query, that is contained
// within the interval provided. It will populate a map with values, for every
// label that is returned, with the tagged values. For example, if the query
// were rebalancing_queriespersecond in the interval [100,110] and there were
// two stores (1,2) with ip addresses 10.0.0.1 and 127.0.0.1, for store 2 and
// store 1 respectively.
//
//	{
//	   "store": {
//	       "1": {
//	           {Time: 100, Value: 777},
//	           {Time: 110, Value: 888}
//	       },
//	       "2": {
//	           {Time: 100, Value: 42},
//	           {Time: 110, Value 42},
//	       },
//	   },
//	   "instance": {
//	        "10.0.0.1":  {
//	           {Time: 100, Value: 42},
//	           {Time: 110, Value 42},
//	       },
//	        "127.0.0.1": {
//	           {Time: 100, Value: 777},
//	           {Time: 110, Value: 888}
//	       },
//	    },
//	}
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

	fromMatrixTagged := fromVal.(model.Matrix)
	if len(fromMatrixTagged) == 0 {
		return nil, errors.Newf(
			"Empty matrix result for query %s @ [%s,%s] (%v)",
			q,
			interval.From.Format(time.RFC3339),
			interval.To.Format(time.RFC3339),
			fromVal,
		)
	}

	result := make(map[string]map[string]StatSeries)
	for _, stream := range fromMatrixTagged {
		statSeries := make(StatSeries, len(stream.Values))
		for i := range stream.Values {
			statSeries[i] = StatPoint{
				Time:  stream.Values[i].Timestamp.Time().UnixNano(),
				Value: float64(stream.Values[i].Value),
			}
		}

		for labelName, labelValue := range stream.Metric {
			if _, ok := result[string(labelName)]; !ok {
				result[string(labelName)] = make(map[string]StatSeries)
			}
			result[string(labelName)][string(labelValue)] = statSeries
		}

		// When there is no tag associated with the result, put in a default
		// tag for the result.
		if len(stream.Metric) == 0 {
			result["default"] = map[string]StatSeries{"default": statSeries}
		}
	}

	return result, nil
}
