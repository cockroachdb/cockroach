// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostserver

import (
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Metrics is a metric.Struct for reporting tenant resource consumption.
//
// All metrics are aggregate metrics, containing child metrics for all tenants
// that have communicated with this node. The metrics report cumulative usage
// for the tenant; the current value for a given tenant is the most recent (or,
// equivalently the largest) value reported across all nodes.  The top-level
// aggregated value for a metric is not useful (it sums up the consumption for
// each tenant, as last reported to this node).
type Metrics struct {
	TotalRU                *aggmetric.AggGaugeFloat64
	TotalReadRequests      *aggmetric.AggGauge
	TotalReadBytes         *aggmetric.AggGauge
	TotalWriteRequests     *aggmetric.AggGauge
	TotalWriteBytes        *aggmetric.AggGauge
	TotalSQLPodsCPUSeconds *aggmetric.AggGaugeFloat64

	mu struct {
		syncutil.Mutex
		// tenantMetrics stores the tenantMetrics for all tenants that have
		// sent TokenBucketRequests to this node.
		// TODO(radu): add garbage collection to remove inactive tenants.
		tenantMetrics map[roachpb.TenantID]tenantMetrics
	}
}

var _ metric.Struct = (*Metrics)(nil)

// MetricStruct indicates that Metrics is a metric.Struct
func (m *Metrics) MetricStruct() {}

var (
	metaTotalRU = metric.Metadata{
		Name:        "tenant.consumption.request_units",
		Help:        "Total RU consumption",
		Measurement: "Request Units",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalReadRequests = metric.Metadata{
		Name:        "tenant.consumption.read_requests",
		Help:        "Total number of KV read requests",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalReadBytes = metric.Metadata{
		Name:        "tenant.consumption.read_bytes",
		Help:        "Total number of bytes read from KV",
		Measurement: "Bytes",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalWriteRequests = metric.Metadata{
		Name:        "tenant.consumption.write_requests",
		Help:        "Total number of KV write requests",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalWriteBytes = metric.Metadata{
		Name:        "tenant.consumption.write_bytes",
		Help:        "Total number of bytes written to KV",
		Measurement: "Bytes",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalSQLPodsCPUSeconds = metric.Metadata{
		Name:        "tenant.consumption.sql_pods_cpu_seconds",
		Help:        "Total number of bytes written to KV",
		Measurement: "CPU Seconds",
		Unit:        metric.Unit_SECONDS,
	}
)

func (m *Metrics) init() {
	b := aggmetric.MakeBuilder(multitenant.TenantIDLabel)
	*m = Metrics{
		TotalRU:                b.GaugeFloat64(metaTotalRU),
		TotalReadRequests:      b.Gauge(metaTotalReadRequests),
		TotalReadBytes:         b.Gauge(metaTotalReadBytes),
		TotalWriteRequests:     b.Gauge(metaTotalWriteRequests),
		TotalWriteBytes:        b.Gauge(metaTotalWriteBytes),
		TotalSQLPodsCPUSeconds: b.GaugeFloat64(metaTotalSQLPodsCPUSeconds),
	}
	m.mu.tenantMetrics = make(map[roachpb.TenantID]tenantMetrics)
}

// tenantMetrics represent metrics for an individual tenant.
type tenantMetrics struct {
	totalRU                *aggmetric.GaugeFloat64
	totalReadRequests      *aggmetric.Gauge
	totalReadBytes         *aggmetric.Gauge
	totalWriteRequests     *aggmetric.Gauge
	totalWriteBytes        *aggmetric.Gauge
	totalSQLPodsCPUSeconds *aggmetric.GaugeFloat64

	// Mutex is used to atomically update metrics together with a corresponding
	// change to the system table.
	mutex *syncutil.Mutex
}

// getTenantMetrics returns the metrics for a tenant.
func (m *Metrics) getTenantMetrics(tenantID roachpb.TenantID) tenantMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	tm, ok := m.mu.tenantMetrics[tenantID]
	if !ok {
		tid := tenantID.String()
		tm = tenantMetrics{
			totalRU:                m.TotalRU.AddChild(tid),
			totalReadRequests:      m.TotalReadRequests.AddChild(tid),
			totalReadBytes:         m.TotalReadBytes.AddChild(tid),
			totalWriteRequests:     m.TotalWriteRequests.AddChild(tid),
			totalWriteBytes:        m.TotalWriteBytes.AddChild(tid),
			totalSQLPodsCPUSeconds: m.TotalSQLPodsCPUSeconds.AddChild(tid),
			mutex:                  &syncutil.Mutex{},
		}
		m.mu.tenantMetrics[tenantID] = tm
	}
	return tm
}
