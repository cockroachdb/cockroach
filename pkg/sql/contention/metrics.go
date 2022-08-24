// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package contention

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// Metrics is a struct that include all metrics related to contention event
// store.
type Metrics struct {
	ResolverQueueSize *metric.Gauge
	ResolverRetries   *metric.Counter
	ResolverFailed    *metric.Counter
}

var _ metric.Struct = Metrics{}

// MetricStruct returns a new instance of Metrics.
func (Metrics) MetricStruct() {}

// NewMetrics returns a new instance of Metrics.
func NewMetrics() Metrics {
	return Metrics{
		ResolverQueueSize: metric.NewGauge(metric.Metadata{
			Name:        "sql.contention.resolver.queue_size",
			Help:        "Length of queued unresolved contention events",
			Measurement: "Queue length",
			Unit:        metric.Unit_COUNT,
		}),
		ResolverRetries: metric.NewCounter(metric.Metadata{
			Name:        "sql.contention.resolver.retries",
			Help:        "Number of times transaction id resolution has been retried",
			Measurement: "Retry count",
			Unit:        metric.Unit_COUNT,
		}),
		ResolverFailed: metric.NewCounter(metric.Metadata{
			Name:        "sql.contention.resolver.failed_resolutions",
			Help:        "Number of failed transaction ID resolution attempts",
			Measurement: "Failed transaction ID resolution count",
			Unit:        metric.Unit_COUNT,
		}),
	}
}
