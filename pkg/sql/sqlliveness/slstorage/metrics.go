// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slstorage

import (
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

var (
	metaIsAliveCacheHits = metric.Metadata{
		Name:        "sqlliveness.is_alive.cache_hits",
		Help:        "Number of calls to IsAlive that return from the cache",
		Measurement: "Calls",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	metaIsAliveCacheMisses = metric.Metadata{
		Name:        "sqlliveness.is_alive.cache_misses",
		Help:        "Number of calls to IsAlive that do not return from the cache",
		Measurement: "Calls",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	metaSessionsDeleted = metric.Metadata{
		Name:        "sqlliveness.sessions_deleted",
		Help:        "Number of expired sessions which have been deleted",
		Measurement: "Sessions",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	metaSessionDeletionRuns = metric.Metadata{
		Name:        "sqlliveness.sessions_deletion_runs",
		Help:        "Number of calls to delete sessions which have been performed",
		Measurement: "Sessions",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	metaWriteSuccesses = metric.Metadata{
		Name:        "sqlliveness.write_successes",
		Help:        "Number of update or insert calls successfully performed",
		Measurement: "Writes",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	metaWriteFailures = metric.Metadata{
		Name:        "sqlliveness.write_failures",
		Help:        "Number of update or insert calls which have failed",
		Measurement: "Writes",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
)

// Metrics is a metric.Struct which holds metrics for slstorage.
type Metrics struct {
	IsAliveCacheHits     *metric.Counter
	IsAliveCacheMisses   *metric.Counter
	SessionsDeleted      *metric.Counter
	SessionDeletionsRuns *metric.Counter
	WriteSuccesses       *metric.Counter
	WriteFailures        *metric.Counter
}

// MetricStruct make Metrics a metric.Struct.
func (m Metrics) MetricStruct() {}

var _ metric.Struct = (*Metrics)(nil)

func makeMetrics() Metrics {
	return Metrics{
		IsAliveCacheHits:     metric.NewCounter(metaIsAliveCacheHits),
		IsAliveCacheMisses:   metric.NewCounter(metaIsAliveCacheMisses),
		SessionsDeleted:      metric.NewCounter(metaSessionsDeleted),
		SessionDeletionsRuns: metric.NewCounter(metaSessionDeletionRuns),
		WriteSuccesses:       metric.NewCounter(metaWriteSuccesses),
		WriteFailures:        metric.NewCounter(metaWriteFailures),
	}
}
