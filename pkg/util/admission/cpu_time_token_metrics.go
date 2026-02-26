// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import "github.com/cockroachdb/cockroach/pkg/util/metric"

var (
	cpuTimeTokenMultiplierMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.multiplier",
		Help: "The token-to-CPU-time multiplier used by the CPU time token " +
			"linear model to adjust refill rates; computed as the ratio of " +
			"total CPU time to tracked CPU time",
		Measurement: "Multiplier",
		Unit:        metric.Unit_COUNT,
	}

	cpuTimeTokenRefillRateMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.refill_rate",
		Help: "The rate at which CPU time tokens are added per second, " +
			"broken down by resource tier and burst qualification",
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}

	cpuTimeTokenBucketTokensMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.bucket_tokens",
		Help: "The current number of tokens in each CPU time token bucket, " +
			"broken down by resource tier and burst qualification",
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}
)

const (
	tierLabel      = "tier"
	burstQualLabel = "burst_qual"
)

// cpuTimeTokenMetrics tracks metrics for the CPU time token admission
// control. Fields are exported because metric.Registry.AddMetricStruct
// uses reflection to discover metrics.
//
// RefillRate and BucketTokens use GaugeVec rather than AggGauge because
// aggregating across tier/burst-qualification labels is not meaningful.
// GaugeVec exports per-label values to Prometheus/Datadog without writing
// to the internal time-series database or computing an aggregate.
type cpuTimeTokenMetrics struct {
	Multiplier   *metric.GaugeFloat64
	RefillRate   *metric.GaugeVec
	BucketTokens *metric.GaugeVec

	// Avoids allocations.
	labelMaps [numResourceTiers][numBurstQualifications]map[string]string
}

func makeCPUTimeTokenMetrics() *cpuTimeTokenMetrics {
	m := &cpuTimeTokenMetrics{
		Multiplier:   metric.NewGaugeFloat64(cpuTimeTokenMultiplierMeta),
		RefillRate:   metric.NewExportedGaugeVec(cpuTimeTokenRefillRateMeta, []string{tierLabel, burstQualLabel}),
		BucketTokens: metric.NewExportedGaugeVec(cpuTimeTokenBucketTokensMeta, []string{tierLabel, burstQualLabel}),
	}
	for tier := resourceTier(0); tier < numResourceTiers; tier++ {
		for qual := burstQualification(0); qual < numBurstQualifications; qual++ {
			m.labelMaps[tier][qual] = map[string]string{
				tierLabel:      tier.String(),
				burstQualLabel: qual.String(),
			}
		}
	}
	return m
}

// MetricStruct implements the metric.Struct interface.
func (cpuTimeTokenMetrics) MetricStruct() {}
