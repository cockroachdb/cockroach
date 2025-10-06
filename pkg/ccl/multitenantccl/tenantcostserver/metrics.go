// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostserver

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
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
	TotalRU                     *aggmetric.AggCounterFloat64
	TotalKVRU                   *aggmetric.AggCounterFloat64
	TotalReadBatches            *aggmetric.AggGauge
	TotalReadRequests           *aggmetric.AggGauge
	TotalReadBytes              *aggmetric.AggGauge
	TotalWriteBatches           *aggmetric.AggGauge
	TotalWriteRequests          *aggmetric.AggGauge
	TotalWriteBytes             *aggmetric.AggGauge
	TotalSQLPodsCPUSeconds      *aggmetric.AggGaugeFloat64
	TotalPGWireEgressBytes      *aggmetric.AggGauge
	TotalExternalIOEgressBytes  *aggmetric.AggGauge
	TotalExternalIOIngressBytes *aggmetric.AggGauge
	TotalCrossRegionNetworkRU   *aggmetric.AggCounterFloat64

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
	metaTotalKVRU = metric.Metadata{
		Name:        "tenant.consumption.kv_request_units",
		Help:        "RU consumption attributable to KV",
		Measurement: "Request Units",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalReadBatches = metric.Metadata{
		Name:        "tenant.consumption.read_batches",
		Help:        "Total number of KV read batches",
		Measurement: "Requests",
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
	metaTotalWriteBatches = metric.Metadata{
		Name:        "tenant.consumption.write_batches",
		Help:        "Total number of KV write batches",
		Measurement: "Requests",
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
		Help:        "Total amount of CPU used by SQL pods",
		Measurement: "CPU Seconds",
		Unit:        metric.Unit_SECONDS,
	}
	metaTotalPGWireEgressBytes = metric.Metadata{
		Name:        "tenant.consumption.pgwire_egress_bytes",
		Help:        "Total number of bytes transferred from a SQL pod to the client",
		Measurement: "Bytes",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalExternalIOIngressBytes = metric.Metadata{
		Name:        "tenant.consumption.external_io_ingress_bytes",
		Help:        "Total number of bytes read from external services such as cloud storage providers",
		Measurement: "Bytes",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalExternalIOEgressBytes = metric.Metadata{
		Name:        "tenant.consumption.external_io_egress_bytes",
		Help:        "Total number of bytes written to external services such as cloud storage providers",
		Measurement: "Bytes",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalCrossRegionNetworkRU = metric.Metadata{
		Name:        "tenant.consumption.cross_region_network_ru",
		Help:        "Total number of RUs charged for cross-region network traffic",
		Measurement: "Request Units",
		Unit:        metric.Unit_COUNT,
	}
)

func (m *Metrics) init() {
	b := aggmetric.MakeBuilder(multitenant.TenantIDLabel)
	*m = Metrics{
		TotalRU:                     b.CounterFloat64(metaTotalRU),
		TotalKVRU:                   b.CounterFloat64(metaTotalKVRU),
		TotalReadBatches:            b.Gauge(metaTotalReadBatches),
		TotalReadRequests:           b.Gauge(metaTotalReadRequests),
		TotalReadBytes:              b.Gauge(metaTotalReadBytes),
		TotalWriteBatches:           b.Gauge(metaTotalWriteBatches),
		TotalWriteRequests:          b.Gauge(metaTotalWriteRequests),
		TotalWriteBytes:             b.Gauge(metaTotalWriteBytes),
		TotalSQLPodsCPUSeconds:      b.GaugeFloat64(metaTotalSQLPodsCPUSeconds),
		TotalPGWireEgressBytes:      b.Gauge(metaTotalPGWireEgressBytes),
		TotalExternalIOEgressBytes:  b.Gauge(metaTotalExternalIOEgressBytes),
		TotalExternalIOIngressBytes: b.Gauge(metaTotalExternalIOIngressBytes),
		TotalCrossRegionNetworkRU:   b.CounterFloat64(metaTotalCrossRegionNetworkRU),
	}
	m.mu.tenantMetrics = make(map[roachpb.TenantID]tenantMetrics)
}

// tenantMetrics represent metrics for an individual tenant.
type tenantMetrics struct {
	totalRU                     *aggmetric.CounterFloat64
	totalKVRU                   *aggmetric.CounterFloat64
	totalReadBatches            *aggmetric.Gauge
	totalReadRequests           *aggmetric.Gauge
	totalReadBytes              *aggmetric.Gauge
	totalWriteBatches           *aggmetric.Gauge
	totalWriteRequests          *aggmetric.Gauge
	totalWriteBytes             *aggmetric.Gauge
	totalSQLPodsCPUSeconds      *aggmetric.GaugeFloat64
	totalPGWireEgressBytes      *aggmetric.Gauge
	totalExternalIOEgressBytes  *aggmetric.Gauge
	totalExternalIOIngressBytes *aggmetric.Gauge
	totalCrossRegionNetworkRU   *aggmetric.CounterFloat64

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
			totalRU:                     m.TotalRU.AddChild(tid),
			totalKVRU:                   m.TotalKVRU.AddChild(tid),
			totalReadBatches:            m.TotalReadBatches.AddChild(tid),
			totalReadRequests:           m.TotalReadRequests.AddChild(tid),
			totalReadBytes:              m.TotalReadBytes.AddChild(tid),
			totalWriteBatches:           m.TotalWriteBatches.AddChild(tid),
			totalWriteRequests:          m.TotalWriteRequests.AddChild(tid),
			totalWriteBytes:             m.TotalWriteBytes.AddChild(tid),
			totalSQLPodsCPUSeconds:      m.TotalSQLPodsCPUSeconds.AddChild(tid),
			totalPGWireEgressBytes:      m.TotalPGWireEgressBytes.AddChild(tid),
			totalExternalIOEgressBytes:  m.TotalExternalIOEgressBytes.AddChild(tid),
			totalExternalIOIngressBytes: m.TotalExternalIOIngressBytes.AddChild(tid),
			totalCrossRegionNetworkRU:   m.TotalCrossRegionNetworkRU.AddChild(tid),
			mutex:                       &syncutil.Mutex{},
		}
		m.mu.tenantMetrics[tenantID] = tm
	}
	return tm
}

const metricsInterval = 10 * time.Second

// metricRates calculates rate of consumption for tenant metrics that are used
// by the cost model. Rates need to be calculated across all SQL pods for a
// given tenant. Because consumption can be reported at different intervals by
// different SQL pods, rates are determined on even 10-second boundaries and
// added together across pods. For example, if at 12:00:03, pod #1 reports 900
// write batches over the last 9 seconds, then the 12:00:00 rate is increased by
// 100. If pod #2 reports the last 9 seconds of consumption at 12:00:13, then it
// would instead be added to the 12:00:10 rate.
type metricRates struct {
	// next contains calculations for consumption rates at the most recent
	// 10-second boundary. Some of the SQL pods may not have yet reported their
	// rates, so this should be considered "in-progress".
	next kvpb.TenantConsumptionRates

	// current contains calculations for consumption rates at the previous
	// 10-second boundary. Most or all of the SQL pods should already have
	// reported their rates, so this is usually the final rate. However, it's
	// possible that a SQL pod is reporting on a delayed schedule due to low
	// consumption, so this rate can still change (though generally not by much).
	current kvpb.TenantConsumptionRates
}

// Current returns the set of consumption rates that's expected to include the
// rates reported by all SQL pods.
func (mw *metricRates) Current() *kvpb.TenantConsumptionRates { return &mw.current }

// Update calculates updated consumption rates given a new consumption report
// at "now" time. The "prev" argument gives the time of the last call to Update.
func (mw *metricRates) Update(
	now, last time.Time, consumption *kvpb.TenantConsumption, consumptionPeriod time.Duration,
) {
	// In all but strange edge cases, time should tick upwards. If that's not
	// the case, then just ignore the reported consumption.
	if now.Before(last) {
		return
	}

	// Calculate the start of the next and current periods, always on even
	// 10-second boundaries.
	nextStart := now.Truncate(metricsInterval)
	currentStart := nextStart.Add(-metricsInterval)

	// Check if the last Update call happened in a different metrics interval.
	if last.Before(currentStart) {
		// The last call happened at least 2 intervals ago.
		mw.current = kvpb.TenantConsumptionRates{}
		mw.next = kvpb.TenantConsumptionRates{}
	} else if last.Before(nextStart) {
		// The last call happened 1 interval ago, so the in-progress rates
		// should be shifted to become current rates.
		mw.current = mw.next
		mw.next = kvpb.TenantConsumptionRates{}
	}

	// Calculate rates per second.
	writeBatchRate := float64(consumption.WriteBatches) * float64(time.Second) / float64(consumptionPeriod)
	estimatedCPURate := consumption.EstimatedCPUSeconds * float64(time.Second) / float64(consumptionPeriod)

	// Add rates to the next and current reports, based on the length of the
	// consumption period.
	start := now.Add(-consumptionPeriod)
	if start.Before(nextStart) {
		mw.next.WriteBatchRate += writeBatchRate
		mw.next.EstimatedCPURate += estimatedCPURate
	}
	if start.Before(currentStart) {
		mw.current.WriteBatchRate += writeBatchRate
		mw.current.EstimatedCPURate += estimatedCPURate
	}
}
