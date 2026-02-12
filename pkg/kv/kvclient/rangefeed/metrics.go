// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import "github.com/cockroachdb/cockroach/pkg/util/metric"

var (
	metaRangefeedCheckpoints = metric.Metadata{
		Name:        "kv.rangefeed.client.checkpoints",
		Help:        "Number of checkpoint events received by rangefeed clients",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics are for production monitoring of rangefeed clients.
type Metrics struct {
	Checkpoints *metric.Counter
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

// NewMetrics creates a new Metrics instance for rangefeed clients.
func NewMetrics() *Metrics {
	return &Metrics{
		Checkpoints: metric.NewCounter(metaRangefeedCheckpoints),
	}
}
