// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import "github.com/cockroachdb/cockroach/pkg/util/metric"

var (
	metaMuxWaiters = metric.Metadata{
		Name:        "server.controller.mux_virtual_cluster_wait.waiters",
		Help:        "Current number of SQL connections waiting for the default virtual cluster to become routable",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	metaMuxWaitAdmitted = metric.Metadata{
		Name:        "server.controller.mux_virtual_cluster_wait.admitted",
		Help:        "Number of SQL connections admitted to wait for the default virtual cluster to become routable",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	metaMuxWaitRejected = metric.Metadata{
		Name:        "server.controller.mux_virtual_cluster_wait.rejected",
		Help:        "Number of SQL connections rejected because too many connections were already waiting for the default virtual cluster",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	metaMuxWaitSuccess = metric.Metadata{
		Name:        "server.controller.mux_virtual_cluster_wait.success",
		Help:        "Number of SQL connections that successfully waited for the default virtual cluster to become routable",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	metaMuxWaitTimeout = metric.Metadata{
		Name:        "server.controller.mux_virtual_cluster_wait.timeout",
		Help:        "Number of SQL connections that timed out while waiting for the default virtual cluster to become routable",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	metaMuxWaitCanceled = metric.Metadata{
		Name:        "server.controller.mux_virtual_cluster_wait.canceled",
		Help:        "Number of SQL connections whose wait for the default virtual cluster was canceled",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
)

type serverControllerMetrics struct {
	Waiters  *metric.Gauge
	Admitted *metric.Counter
	Rejected *metric.Counter
	Success  *metric.Counter
	Timeout  *metric.Counter
	Canceled *metric.Counter
}

var _ metric.Struct = (*serverControllerMetrics)(nil)

func (*serverControllerMetrics) MetricStruct() {}

func makeServerControllerMetrics(registry *metric.Registry) *serverControllerMetrics {
	m := &serverControllerMetrics{
		Waiters:  metric.NewGauge(metaMuxWaiters),
		Admitted: metric.NewCounter(metaMuxWaitAdmitted),
		Rejected: metric.NewCounter(metaMuxWaitRejected),
		Success:  metric.NewCounter(metaMuxWaitSuccess),
		Timeout:  metric.NewCounter(metaMuxWaitTimeout),
		Canceled: metric.NewCounter(metaMuxWaitCanceled),
	}
	if registry != nil {
		registry.AddMetricStruct(m)
	}
	return m
}
