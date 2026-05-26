// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

var (
	metaActiveWorkStates = metric.Metadata{
		Name:        "ash.work_states.active",
		Help:        "Number of goroutines with an active ASH work state",
		Measurement: "Goroutines",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
	metaTakeSampleLatency = metric.Metadata{
		Name:        "ash.sampler.take_sample.latency",
		Help:        "Latency of ASH sample collection ticks",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
		MetricType:  io_prometheus_client.MetricType_HISTOGRAM,
	}
	metaSamplesCollected = metric.Metadata{
		Name:        "ash.samples.collected",
		Help:        "Total number of ASH samples collected",
		Measurement: "Samples",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	metaEnrichmentHits = metric.Metadata{
		Name:        "ash.enrichment.cache.hits",
		Help:        "Number of enrichment cache lookups that found the entry",
		Measurement: "Lookups",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	metaEnrichmentMisses = metric.Metadata{
		Name:        "ash.enrichment.cache.misses",
		Help:        "Number of enrichment cache lookups that missed",
		Measurement: "Lookups",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	metaEnrichmentRemoteResolutions = metric.Metadata{
		Name:        "ash.enrichment.remote_resolutions",
		Help:        "Number of enrichment IDs resolved via remote RPC",
		Measurement: "Resolutions",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	metaEnrichmentPending = metric.Metadata{
		Name:        "ash.enrichment.pending",
		Help:        "Number of samples awaiting enrichment retry",
		Measurement: "Samples",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
	metaEnrichmentCacheEntries = metric.Metadata{
		Name:        "ash.enrichment.cache.entries",
		Help:        "Number of entries in the enrichment cache",
		Measurement: "Entries",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
)

// Metrics holds the metrics for the ASH sampler subsystem.
type Metrics struct {
	ActiveWorkStates            *metric.FunctionalGauge
	TakeSampleLatency           metric.IHistogram
	SamplesCollected            *metric.Counter
	EnrichmentHits              *metric.Counter
	EnrichmentMisses            *metric.Counter
	EnrichmentRemoteResolutions *metric.Counter
	EnrichmentPending           *metric.Gauge
	EnrichmentCacheEntries      *metric.Gauge
}

// MetricStruct implements the metric.Struct interface.
func (Metrics) MetricStruct() {}

var _ metric.Struct = (*Metrics)(nil)

func makeMetrics() Metrics {
	return Metrics{
		ActiveWorkStates: metric.NewFunctionalGauge(
			metaActiveWorkStates,
			func() int64 { return activeWorkStatesCount.Load() },
		),
		TakeSampleLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePreferHdrLatency,
			Metadata:     metaTakeSampleLatency,
			Duration:     base.DefaultHistogramWindowInterval(),
			BucketConfig: metric.IOLatencyBuckets,
		}),
		SamplesCollected:            metric.NewCounter(metaSamplesCollected),
		EnrichmentHits:              metric.NewCounter(metaEnrichmentHits),
		EnrichmentMisses:            metric.NewCounter(metaEnrichmentMisses),
		EnrichmentRemoteResolutions: metric.NewCounter(metaEnrichmentRemoteResolutions),
		EnrichmentPending:           metric.NewGauge(metaEnrichmentPending),
		EnrichmentCacheEntries:      metric.NewGauge(metaEnrichmentCacheEntries),
	}
}
