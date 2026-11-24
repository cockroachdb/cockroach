// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostclient

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

var (
	metaCurrentBlocked = metric.Metadata{
		Name:        "tenant.cost_client.blocked_requests",
		Help:        "Number of requests currently blocked by the rate limiter",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}

	// SQL usage related metrics.
	metaTotalRU = metric.Metadata{
		Name:        "tenant.sql_usage.request_units",
		Help:        "RU consumption",
		Measurement: "Request Units",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalKVRU = metric.Metadata{
		Name:        "tenant.sql_usage.kv_request_units",
		Help:        "RU consumption attributable to KV",
		Measurement: "Request Units",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalReadBatches = metric.Metadata{
		Name:        "tenant.sql_usage.read_batches",
		Help:        "Total number of KV read batches",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalReadRequests = metric.Metadata{
		Name:        "tenant.sql_usage.read_requests",
		Help:        "Total number of KV read requests",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalReadBytes = metric.Metadata{
		Name:        "tenant.sql_usage.read_bytes",
		Help:        "Total number of bytes read from KV",
		Measurement: "Bytes",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalWriteBatches = metric.Metadata{
		Name:        "tenant.sql_usage.write_batches",
		Help:        "Total number of KV write batches",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalWriteRequests = metric.Metadata{
		Name:        "tenant.sql_usage.write_requests",
		Help:        "Total number of KV write requests",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalWriteBytes = metric.Metadata{
		Name:        "tenant.sql_usage.write_bytes",
		Help:        "Total number of bytes written to KV",
		Measurement: "Bytes",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalSQLPodsCPUSeconds = metric.Metadata{
		Name:        "tenant.sql_usage.sql_pods_cpu_seconds",
		Help:        "Total amount of CPU used by SQL pods",
		Measurement: "CPU Seconds",
		Unit:        metric.Unit_SECONDS,
	}
	metaTotalPGWireEgressBytes = metric.Metadata{
		Name:        "tenant.sql_usage.pgwire_egress_bytes",
		Help:        "Total number of bytes transferred from a SQL pod to the client",
		Measurement: "Bytes",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalExternalIOIngressBytes = metric.Metadata{
		Name:        "tenant.sql_usage.external_io_ingress_bytes",
		Help:        "Total number of bytes read from external services such as cloud storage providers",
		Measurement: "Bytes",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalExternalIOEgressBytes = metric.Metadata{
		Name:        "tenant.sql_usage.external_io_egress_bytes",
		Help:        "Total number of bytes written to external services such as cloud storage providers",
		Measurement: "Bytes",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalCrossRegionNetworkRU = metric.Metadata{
		Name:        "tenant.sql_usage.cross_region_network_ru",
		Help:        "Total number of RUs charged for cross-region network traffic",
		Measurement: "Request Units",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalEstimatedKVCPUSeconds = metric.Metadata{
		Name:        "tenant.sql_usage.estimated_kv_cpu_seconds",
		Help:        "Estimated amount of CPU consumed by a virtual cluster, in the KV layer",
		Measurement: "CPU Seconds",
		Unit:        metric.Unit_SECONDS,
	}
	metaTotalEstimatedCPUSeconds = metric.Metadata{
		Name:        "tenant.sql_usage.estimated_cpu_seconds",
		Help:        "Estimated amount of CPU consumed by a virtual cluster",
		Measurement: "CPU Seconds",
		Unit:        metric.Unit_SECONDS,
	}
	metaTotalEstimatedReplicationBytes = metric.Metadata{
		Name:        "tenant.sql_usage.estimated_replication_bytes",
		Help:        "Total number of estimated bytes for KV replication traffic",
		Measurement: "Bytes",
		Unit:        metric.Unit_COUNT,
	}
	metaProvisionedVcpus = metric.Metadata{
		Name:        "tenant.sql_usage.provisioned_vcpus",
		Help:        "Number of vcpus available to the virtual cluster",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}
)

// metrics manage the metrics used by the tenant cost client.
type metrics struct {
	CurrentBlocked              *metric.Gauge
	TotalRU                     *metric.CounterFloat64
	TotalKVRU                   *metric.CounterFloat64
	TotalReadBatches            *metric.Counter
	TotalReadRequests           *metric.Counter
	TotalReadBytes              *metric.Counter
	TotalWriteBatches           *metric.Counter
	TotalWriteRequests          *metric.Counter
	TotalWriteBytes             *metric.Counter
	TotalSQLPodsCPUSeconds      *metric.CounterFloat64
	TotalPGWireEgressBytes      *metric.Counter
	TotalExternalIOEgressBytes  *metric.Counter
	TotalExternalIOIngressBytes *metric.Counter
	TotalCrossRegionNetworkRU   *metric.CounterFloat64
	TotalEstimatedKVCPUSeconds  *metric.CounterFloat64
	TotalEstimatedCPUSeconds    *metric.CounterFloat64
	EstimatedReplicationBytes   *aggmetric.AggCounter
	ProvisionedVcpus            *metric.Gauge

	// cachedPathMetrics stores a cache of network paths to the metrics which
	// have been initialized. Having this layer of caching prevents us from
	// needing to compute the normalized locality on every request.
	cachedPathMetrics syncutil.Map[networkPath, networkPathMetrics]

	mu struct {
		syncutil.Mutex
		// pathMetrics stores a mapping of the locality values to network path
		// metrics. These metrics should only be initialized once for every set
		// of locality values. We will apply normalized locality values to
		// reduce the total number of metrics that we'll be tracking. For
		// example, for a cross-region request for nodes with the "region" and
		// "zone" locality keys, the metric labels will only include the region
		// (and not the zone).
		pathMetrics map[string]*networkPathMetrics
	}
	baseLocality roachpb.Locality
}

type networkPath struct {
	fromNodeID roachpb.NodeID
	toNodeID   roachpb.NodeID
}

type networkPathMetrics struct {
	EstimatedReplicationBytes *aggmetric.Counter
}

var _ metric.Struct = (*metrics)(nil)

// MetricStruct indicates that Metrics is a metric.Struct.
func (m *metrics) MetricStruct() {}

// Init initializes the tenant cost client metrics.
func (m *metrics) Init(locality roachpb.Locality) {
	m.CurrentBlocked = metric.NewGauge(metaCurrentBlocked)
	m.TotalRU = metric.NewCounterFloat64(metaTotalRU)
	m.TotalKVRU = metric.NewCounterFloat64(metaTotalKVRU)
	m.TotalReadBatches = metric.NewCounter(metaTotalReadBatches)
	m.TotalReadRequests = metric.NewCounter(metaTotalReadRequests)
	m.TotalReadBytes = metric.NewCounter(metaTotalReadBytes)
	m.TotalWriteBatches = metric.NewCounter(metaTotalWriteBatches)
	m.TotalWriteRequests = metric.NewCounter(metaTotalWriteRequests)
	m.TotalWriteBytes = metric.NewCounter(metaTotalWriteBytes)
	m.TotalSQLPodsCPUSeconds = metric.NewCounterFloat64(metaTotalSQLPodsCPUSeconds)
	m.TotalPGWireEgressBytes = metric.NewCounter(metaTotalPGWireEgressBytes)
	m.TotalExternalIOEgressBytes = metric.NewCounter(metaTotalExternalIOEgressBytes)
	m.TotalExternalIOIngressBytes = metric.NewCounter(metaTotalExternalIOIngressBytes)
	m.TotalCrossRegionNetworkRU = metric.NewCounterFloat64(metaTotalCrossRegionNetworkRU)
	m.TotalEstimatedKVCPUSeconds = metric.NewCounterFloat64(metaTotalEstimatedKVCPUSeconds)
	m.TotalEstimatedCPUSeconds = metric.NewCounterFloat64(metaTotalEstimatedCPUSeconds)
	m.ProvisionedVcpus = metric.NewGauge(metaProvisionedVcpus)

	// Metric labels for KV replication traffic will be derived from the SQL
	// server's locality. e.g. {"source_region", "source_az", "destination_region", "destination_az"}.
	var labels []string
	for _, t := range locality.Tiers {
		labels = append(labels, fmt.Sprintf("source_%s", t.Key))
	}
	for _, t := range locality.Tiers {
		labels = append(labels, fmt.Sprintf("destination_%s", t.Key))
	}
	m.EstimatedReplicationBytes = aggmetric.NewCounter(metaTotalEstimatedReplicationBytes, labels...)
	m.mu.pathMetrics = make(map[string]*networkPathMetrics)
	m.baseLocality = locality
}

func (m *metrics) getConsumption(consumption *kvpb.TenantConsumption) {
	consumption.RU = m.TotalRU.Count()
	consumption.KVRU = m.TotalKVRU.Count()
	consumption.ReadBatches = uint64(m.TotalReadBatches.Count())
	consumption.ReadRequests = uint64(m.TotalReadRequests.Count())
	consumption.ReadBytes = uint64(m.TotalReadBytes.Count())
	consumption.WriteBatches = uint64(m.TotalWriteBatches.Count())
	consumption.WriteRequests = uint64(m.TotalWriteRequests.Count())
	consumption.WriteBytes = uint64(m.TotalWriteBytes.Count())
	consumption.SQLPodsCPUSeconds = m.TotalSQLPodsCPUSeconds.Count()
	consumption.PGWireEgressBytes = uint64(m.TotalPGWireEgressBytes.Count())
	consumption.ExternalIOEgressBytes = uint64(m.TotalExternalIOEgressBytes.Count())
	consumption.ExternalIOIngressBytes = uint64(m.TotalExternalIOIngressBytes.Count())
	consumption.CrossRegionNetworkRU = m.TotalCrossRegionNetworkRU.Count()
	consumption.EstimatedCPUSeconds = m.TotalEstimatedCPUSeconds.Count()
}

// EstimatedReplicationBytesForPath returns the metric that represents the
// estimated replication bytes for the given network path.
func (m *metrics) EstimatedReplicationBytesForPath(
	fromNodeID roachpb.NodeID,
	fromLocality roachpb.Locality,
	toNodeID roachpb.NodeID,
	toLocality roachpb.Locality,
) *aggmetric.Counter {
	// Check if we have cached values.
	np := networkPath{fromNodeID: fromNodeID, toNodeID: toNodeID}
	if cached, ok := m.cachedPathMetrics.Load(np); ok {
		return cached.EstimatedReplicationBytes
	}

	pm := m.ensureNetworkPathMetrics(fromLocality, toLocality)

	// pm will never be nil, so it's fine if multiple Store calls end up racing.
	m.cachedPathMetrics.Store(np, pm)

	return pm.EstimatedReplicationBytes
}

// ensureNetworkPathMetrics ensures that the networkPathMetrics struct for the
// given locality (from, to) pair has been initialized.
func (m *metrics) ensureNetworkPathMetrics(
	fromLocality, toLocality roachpb.Locality,
) *networkPathMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if we've already initialized the metric. This is the case if a
	// different network path results in the same normalized locality values.
	labelValues := makeLocalityLabelValues(m.baseLocality, fromLocality, toLocality)
	storeKey := strings.Join(labelValues, ",")
	pm, ok := m.mu.pathMetrics[storeKey]
	if !ok {
		pm = &networkPathMetrics{
			EstimatedReplicationBytes: m.EstimatedReplicationBytes.AddChild(labelValues...),
		}
		m.mu.pathMetrics[storeKey] = pm
	}
	return pm
}

// makeLocalityLabelValues returns a list of label values which can be used for
// network path metrics. This applies a mapping approach where all the values in
// "from" and "to" localities are taken up until the first value that differs.
// Remaining values will be padded as empty strings. Note that the keys must
// exist in the given base locality. See test cases for more information.
func makeLocalityLabelValues(baseLocality, fromLocality, toLocality roachpb.Locality) []string {
	labelValues := make([]string, 2*len(baseLocality.Tiers))
	length := min(len(baseLocality.Tiers), len(fromLocality.Tiers), len(toLocality.Tiers))
	for i := 0; i < length; i++ {
		base := baseLocality.Tiers[i].Key
		if fromLocality.Tiers[i].Key != base || toLocality.Tiers[i].Key != base {
			break
		}
		labelValues[i] = fromLocality.Tiers[i].Value
		labelValues[i+len(baseLocality.Tiers)] = toLocality.Tiers[i].Value

		// Stop at the first non-matching value.
		if fromLocality.Tiers[i].Value != toLocality.Tiers[i].Value {
			break
		}
	}
	return labelValues
}
