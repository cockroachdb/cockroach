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
)

// Interval represents a start and end time.
type Interval struct {
	From time.Time
	To   time.Time
}

// StatCollector provides a wrapper over PromQL, to query and return interval
// and point in time statistics.
type StatCollector interface {
	// CollectPoint collects a stat for a query, at the time provided.
	CollectPoint(context.Context, *logger.Logger, time.Time, string) (map[string]map[string]prometheus.StatPoint, error)
	// CollectInterval collects a timeseries for the given query, that is
	// contained within the interval provided.
	CollectInterval(context.Context, *logger.Logger, Interval, string) (map[string]map[string]prometheus.StatSeries, error)
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
		interval:   prometheus.DefaultScrapeInterval,
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
) (map[string]map[string]prometheus.StatPoint, error) {
	return prometheus.Query(ctx, cs.promClient, q, at)
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
) (map[string]map[string]prometheus.StatSeries, error) {
	if !interval.valid() {
		l.PrintfCtx(
			ctx,
			"to %s < from %s, skipping",
			interval.From.Format(time.RFC3339),
			interval.To.Format(time.RFC3339),
		)
		return nil, nil
	}
	return prometheus.QueryRange(ctx, cs.promClient, q, interval.From, interval.To, cs.interval)
}
