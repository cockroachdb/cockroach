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
	BackendDisconnectCount *metric.Counter
	BackendDownCount       *metric.Counter
	ClientDisconnectCount  *metric.Counter
	CurConnCount           *metric.Gauge
	RoutingErrCount        *metric.Counter
}

// MetricStruct implements the metrics.Struct interface.
func (Metrics) MetricStruct() {}

var _ metric.Struct = Metrics{}

var (
	metaCurConnCount = metric.Metadata{
		Name:        "proxy.sql.conns",
		Help:        "Number of connections being proxied",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	metaRoutingErrCount = metric.Metadata{
		Name:        "proxy.err.routing",
		Help:        "Number of errors encountered when attempting to route clients",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}
	metaBackendDownCount = metric.Metadata{
		Name:        "proxy.err.backend_down",
		Help:        "Number of errors encountered when connecting to backend servers",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}
	metaBackendDisconnectCount = metric.Metadata{
		Name:        "proxy.err.backend_disconnect",
		Help:        "Number of disconnects initiated by proxied backends",
		Measurement: "Disconnects",
		Unit:        metric.Unit_COUNT,
	}
	metaClientDisconnectCount = metric.Metadata{
		Name:        "proxy.err.client_disconnect",
		Help:        "Number of disconnects initiated by clients",
		Measurement: "Disconnects",
		Unit:        metric.Unit_COUNT,
	}
)

// MakeProxyMetrics instantiates the metrics holder for proxy monitoring.
func MakeProxyMetrics() Metrics {
	return Metrics{
		BackendDisconnectCount: metric.NewCounter(metaBackendDisconnectCount),
		BackendDownCount:       metric.NewCounter(metaBackendDownCount),
		ClientDisconnectCount:  metric.NewCounter(metaClientDisconnectCount),
		CurConnCount:           metric.NewGauge(metaCurConnCount),
		RoutingErrCount:        metric.NewCounter(metaRoutingErrCount),
	}
}
