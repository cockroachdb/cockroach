// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/crlib/crstrings"
)

var (
	cpuTimeTokenMultiplierMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.multiplier",
		Help: crstrings.UnwrapText(`
			The token-to-CPU-time multiplier used by the CPU time token
			linear model to adjust refill rates; computed as the ratio of
			total CPU time to tracked CPU time`),
		Measurement: "Multiplier",
		Unit:        metric.Unit_COUNT,
	}

	cpuTimeTokenRefillRateMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.refill_rate",
		Help: crstrings.UnwrapText(`
			The rate at which CPU time tokens are added per second,
			broken down by resource tier and burst qualification; note that
			the refill rate of a bucket is also the capacity of the bucket`),
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}

	cpuTimeTokenBucketTokensMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.bucket_tokens",
		Help: crstrings.UnwrapText(`
			The current number of tokens in each CPU time token bucket,
			broken down by resource tier and burst qualification`),
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}

	cpuTimeTokensConsumedMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.consumed",
		Help: crstrings.UnwrapText(`
			Cumulative number of CPU time tokens consumed (deducted from
			buckets) by admitted work`),
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}

	cpuTimeTokensReturnedMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.returned",
		Help: crstrings.UnwrapText(`
			Cumulative number of CPU time tokens returned (credited back to
			buckets), for example when actual CPU usage was lower than the
			initial estimate`),
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
	Multiplier     *metric.GaugeFloat64
	RefillRate     *metric.GaugeVec
	BucketTokens   *metric.GaugeVec
	TokensConsumed *metric.Counter
	TokensReturned *metric.Counter

	// ExhaustedDurationNanos tracks cumulative nanoseconds each bucket has
	// spent exhausted. Each (tier, qual) bucket gets its own counter rather
	// than using a CounterVec, because tookWithoutPermissionLocked is on the
	// hot admission path: Counter.Inc is a single atomic add, whereas
	// CounterVec.Inc involves allocations, mutex grabs, and hash lookups.
	//
	// The counter accumulates nanoseconds of exhaustion. Applying
	// rate(exhausted_duration_nanos) in DD/Prometheus yields the fraction of
	// wall-clock time the bucket was exhausted, queryable over any window
	// (1m, 5m, 30m, etc.).
	//
	// This is a flat array indexed by exhaustedDurationIdx(tier, qual)
	// rather than a nested [tier][qual] array, because AddMetricStruct
	// cannot register metrics inside nested arrays.
	ExhaustedDurationNanos [int(numResourceTiers) * int(numBurstQualifications)]*metric.Counter

	// labelMaps avoids allocations when updating GaugeVec metrics.
	labelMaps [numResourceTiers][numBurstQualifications]map[string]string
}

func makeCPUTimeTokenMetrics() *cpuTimeTokenMetrics {
	m := &cpuTimeTokenMetrics{
		Multiplier:     metric.NewGaugeFloat64(cpuTimeTokenMultiplierMeta),
		RefillRate:     metric.NewExportedGaugeVec(cpuTimeTokenRefillRateMeta, []string{tierLabel, burstQualLabel}),
		BucketTokens:   metric.NewExportedGaugeVec(cpuTimeTokenBucketTokensMeta, []string{tierLabel, burstQualLabel}),
		TokensConsumed: metric.NewCounter(cpuTimeTokensConsumedMeta),
		TokensReturned: metric.NewCounter(cpuTimeTokensReturnedMeta),
	}
	for tier := resourceTier(0); tier < numResourceTiers; tier++ {
		for qual := burstQualification(0); qual < numBurstQualifications; qual++ {
			m.ExhaustedDurationNanos[exhaustedDurationIdx(tier, qual)] = metric.NewCounter(metric.Metadata{
				Name: fmt.Sprintf(
					"admission.cpu_time_tokens.exhausted_duration_nanos.%s.%s",
					tier, qual),
				Help: fmt.Sprintf(
					"Cumulative nanoseconds the %s/%s CPU time token bucket has spent "+
						"exhausted (tokens <= 0); rate() gives the fraction of wall-clock "+
						"time the bucket was exhausted",
					tier, qual),
				Measurement: "Nanoseconds",
				Unit:        metric.Unit_NANOSECONDS,
			})
		}
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

// exhaustedDurationIdx returns the flat index into
// ExhaustedDurationNanos for the given (tier, qual) pair.
func exhaustedDurationIdx(tier resourceTier, qual burstQualification) int {
	return int(tier)*int(numBurstQualifications) + int(qual)
}
