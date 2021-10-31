// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// maxSLIScopes is a static limit on the number of SLI scopes -- that is the number
// of SLI metrics we will keep track of.
// The limit is static due to metric.Registry limitations.
const maxSLIScopes = 8

// SLIMetrics is the list of SLI related metrics for changefeeds.
type SLIMetrics struct {
	*SinkMetrics
	ErrorRetries      *metric.Counter
	AdmitLatency      *metric.Histogram
	BackfillTimestamp *metric.Gauge
}

// MetricStruct implements metric.Struct interface
func (*SLIMetrics) MetricStruct() {}

func makeSLIMetrics(prefix string, histogramWindow time.Duration) *SLIMetrics {
	return &SLIMetrics{
		SinkMetrics: newSinkMetricsWithPrefix(prefix, histogramWindow),
		ErrorRetries: metric.NewCounter(metaWithPrefix(prefix, metric.Metadata{
			Name:        "error_retries",
			Help:        "Total retryable errors encountered this SLI",
			Measurement: "Errors",
			Unit:        metric.Unit_COUNT,
		})),
		AdmitLatency: metric.NewHistogram(metaWithPrefix(prefix, metric.Metadata{
			Name: "admit_latency",
			Help: "Event admission latency: a difference between event MVCC timestamp " +
				"and the time it was admitted into changefeed pipeline; Excludes latency during backfill",
			Measurement: "Nanoseconds",
			Unit:        metric.Unit_NANOSECONDS,
		}), base.DefaultHistogramWindowInterval(), admitLatencyMaxValue.Nanoseconds(), 1),

		BackfillTimestamp: metric.NewGauge(metaWithPrefix(prefix, metric.Metadata{
			Name:        "backfill_timestamp",
			Help:        "Backfill timestamp in seconds; Set when backfill is running, 0 otherwise.",
			Measurement: "Time (s)",
			Unit:        metric.Unit_NANOSECONDS,
		})),
	}
}

// SLIScopes represents a set of SLI related metrics for a particular "scope".
type SLIScopes struct {
	Scopes [maxSLIScopes]*SLIMetrics // Exported so that we can register w/ metrics registry.
	names  map[string]*SLIMetrics
}

// MetricStruct implements metric.Struct interface
func (*SLIScopes) MetricStruct() {}

// CreateSLIScopes creates changefeed specific SLI scope: a metric.Struct containing
// SLI specific metrics for each scope.
// The scopes are statically named "tier<number>", and each metric name
// contained in SLIMetrics will be prefixed by "changefeed.tier<number" prefix.
func CreateSLIScopes(histogramWindow time.Duration) *SLIScopes {
	scope := &SLIScopes{
		names: make(map[string]*SLIMetrics, maxSLIScopes),
	}

	for i := 0; i < maxSLIScopes; i++ {
		scopeName := fmt.Sprintf("tier%d", i)
		scope.Scopes[i] = makeSLIMetrics(fmt.Sprintf("changefeed.%s", scopeName), histogramWindow)
		scope.names[scopeName] = scope.Scopes[i]
	}
	return scope
}

// GetSLIMetrics returns a metric.Struct associated with the specified scope, or nil
// of no such scope exists.
func (s *SLIScopes) GetSLIMetrics(scopeName string) *SLIMetrics {
	return s.names[scopeName]
}
