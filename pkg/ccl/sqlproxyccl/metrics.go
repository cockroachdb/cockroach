// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// Metrics contains pointers to the metrics for monitoring proxy
// operations.
type Metrics struct {
	CurConnCount *metric.Gauge
}

// MetricStruct implements the metrics.Struct interface.
func (Metrics) MetricStruct() {}

var _ metric.Struct = Metrics{}

var (
	metaCurConnCount = metric.Metadata{
		Name:        "proxy.sql.conns",
		Help:        "Number of active connectings being proxied",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
)

// MakeProxyMetrics instantiates the metrics holder for proxy monitoring.
func MakeProxyMetrics() Metrics {
	return Metrics{
		CurConnCount: metric.NewGauge(metaCurConnCount),
	}
}
