// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantrate

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
)

// Metrics is a metric.Struct for the LimiterFactory.
type Metrics struct {
	Tenants            *metric.Gauge
	CurrentBlocked     *aggmetric.AggGauge
	RequestsAdmitted   *aggmetric.AggCounter
	WriteBytesAdmitted *aggmetric.AggCounter
}

var _ metric.Struct = (*Metrics)(nil)

var (
	metaTenants = metric.Metadata{
		Name:        "kv.tenant_rate_limit.num_tenants",
		Help:        "Number of tenants currently being tracked",
		Measurement: "Tenants",
		Unit:        metric.Unit_COUNT,
	}
	metaCurrentBlocked = metric.Metadata{
		Name:        "kv.tenant_rate_limit.current_blocked",
		Help:        "Number of requests currently blocked by the rate limiter",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaRequestsAdmitted = metric.Metadata{
		Name:        "kv.tenant_rate_limit.requests_admitted",
		Help:        "Number of requests admitted by the rate limiter",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaWriteBytesAdmitted = metric.Metadata{
		Name:        "kv.tenant_rate_limit.write_bytes_admitted",
		Help:        "Number of write bytes admitted by the rate limiter",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	// TODO(ajwerner): Write bytes admitted, read bytes admitted, clock spent
	// blocked, etc.
)

// TenantIDLabel is the label used with metrics associated with a tenant.
// The value will be the integer tenant ID.
const TenantIDLabel = "tenant_id"

func makeMetrics() Metrics {
	return Metrics{
		Tenants:            metric.NewGauge(metaTenants),
		CurrentBlocked:     aggmetric.NewGauge(metaCurrentBlocked, TenantIDLabel),
		RequestsAdmitted:   aggmetric.NewCounter(metaRequestsAdmitted, TenantIDLabel),
		WriteBytesAdmitted: aggmetric.NewCounter(metaWriteBytesAdmitted, TenantIDLabel),
	}
}

// MetricStruct indicates that Metrics is a metric.Struct
func (m *Metrics) MetricStruct() {}

// tenantMetrics represent metrics for an individual tenant.
type tenantMetrics struct {
	currentBlocked     *aggmetric.Gauge
	requestsAdmitted   *aggmetric.Counter
	writeBytesAdmitted *aggmetric.Counter
}

func (m *Metrics) tenantMetrics(tenantID roachpb.TenantID) tenantMetrics {
	tid := tenantID.String()
	return tenantMetrics{
		currentBlocked:     m.CurrentBlocked.AddChild(tid),
		requestsAdmitted:   m.RequestsAdmitted.AddChild(tid),
		writeBytesAdmitted: m.WriteBytesAdmitted.AddChild(tid),
	}
}

func (tm *tenantMetrics) destroy() {
	tm.currentBlocked.Destroy()
	tm.requestsAdmitted.Destroy()
	tm.writeBytesAdmitted.Destroy()
}
