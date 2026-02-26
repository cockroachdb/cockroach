// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
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

	cpuTimeTokensConsumedMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.usage.consumed",
		Help: crstrings.UnwrapText(`
			Cumulative number of CPU time tokens consumed (deducted from
			buckets) by admitted work`),
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}

	cpuTimeTokensReturnedMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.usage.returned",
		Help: crstrings.UnwrapText(`
			Cumulative number of CPU time tokens returned (credited back to
			buckets), for example when actual CPU usage was lower than the
			initial estimate`),
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}

	cpuTimeTokenAdmittedCountPerTenantMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.per_tenant.admitted_count",
		Help: crstrings.UnwrapText(`
			Cumulative number of requests admitted per tenant by CPU time
			token admission control; use with wait_time_nanos to compute
			mean wait time via rate(wait_time) / rate(admitted_count)`),
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}

	cpuTimeTokenWaitTimeNanosPerTenantMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.per_tenant.wait_time_nanos",
		Help: crstrings.UnwrapText(`
			Cumulative nanoseconds of admission queue wait time per tenant
			in CPU time token admission control; use with admitted_count to
			compute mean wait time via rate(wait_time) / rate(admitted_count)`),
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

// cpuTimeTokenMetrics tracks metrics for the CPU time token admission
// control. Fields are exported because metric.Registry.AddMetricStruct
// uses reflection to discover metrics.
type cpuTimeTokenMetrics struct {
	Multiplier     *metric.GaugeFloat64
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
	// Per (tier, qual) counters use flat arrays indexed by
	// perBucketIdx(tier, qual) rather than nested [tier][qual] arrays,
	// because AddMetricStruct cannot register metrics inside nested arrays.
	ExhaustedDurationNanos [numPerBucketCounters]*metric.Counter

	// RefillAdded tracks cumulative tokens added to each bucket via
	// the refill process. rate(refill.added) gives the effective refill
	// rate per bucket.
	RefillAdded [numPerBucketCounters]*metric.Counter

	// RefillRemoved tracks cumulative tokens removed from each bucket
	// when refill rates decrease between intervals (negative delta).
	RefillRemoved [numPerBucketCounters]*metric.Counter

	// AdmittedCountPerTenant and WaitTimeNanosPerTenant track per-tenant
	// admission stats. Together they enable computing mean wait time per
	// tenant via rate(wait_time_nanos) / rate(admitted_count) in
	// DD/Prometheus. We start with these for just CPU time token AC for
	// cost reasons (there are many work queues); over time we may
	// integrate into WorkQueueMetrics. We use two counters to derive the
	// mean rather than a histogram, also for cost reasons.
	AdmittedCountPerTenant *aggmetric.AggCounter
	WaitTimeNanosPerTenant *aggmetric.AggCounter
}

func makeCPUTimeTokenMetrics() *cpuTimeTokenMetrics {
	// NB: Matches multitenant.TenantIDLabel for consistency with other
	// per-tenant metrics. Inlined to avoid a dependency cycle.
	b := aggmetric.MakeBuilder("tenant_id")
	m := &cpuTimeTokenMetrics{
		Multiplier:             metric.NewGaugeFloat64(cpuTimeTokenMultiplierMeta),
		TokensConsumed:         metric.NewCounter(cpuTimeTokensConsumedMeta),
		TokensReturned:         metric.NewCounter(cpuTimeTokensReturnedMeta),
		AdmittedCountPerTenant: b.Counter(cpuTimeTokenAdmittedCountPerTenantMeta),
		WaitTimeNanosPerTenant: b.Counter(cpuTimeTokenWaitTimeNanosPerTenantMeta),
	}
	for tier := resourceTier(0); tier < numResourceTiers; tier++ {
		for qual := burstQualification(0); qual < numBurstQualifications; qual++ {
			idx := perBucketIdx(tier, qual)
			m.ExhaustedDurationNanos[idx] = metric.NewCounter(metric.Metadata{
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
			m.RefillAdded[idx] = metric.NewCounter(metric.Metadata{
				Name: fmt.Sprintf(
					"admission.cpu_time_tokens.refill.added.%s.%s",
					tier, qual),
				Help: fmt.Sprintf(
					"Cumulative tokens added to the %s/%s CPU time token bucket "+
						"via the refill process; rate() gives the effective refill rate",
					tier, qual),
				Measurement: "Tokens",
				Unit:        metric.Unit_COUNT,
			})
			m.RefillRemoved[idx] = metric.NewCounter(metric.Metadata{
				Name: fmt.Sprintf(
					"admission.cpu_time_tokens.refill.removed.%s.%s",
					tier, qual),
				Help: fmt.Sprintf(
					"Cumulative tokens removed from the %s/%s CPU time token bucket "+
						"when refill rates decrease between intervals",
					tier, qual),
				Measurement: "Tokens",
				Unit:        metric.Unit_COUNT,
			})
		}
	}
	return m
}

// MetricStruct implements the metric.Struct interface.
func (cpuTimeTokenMetrics) MetricStruct() {}

const numPerBucketCounters = int(numResourceTiers) * int(numBurstQualifications)

// perBucketIdx returns the flat index into per-(tier, qual) counter
// arrays for the given (tier, qual) pair.
func perBucketIdx(tier resourceTier, qual burstQualification) int {
	return int(tier)*int(numBurstQualifications) + int(qual)
}
