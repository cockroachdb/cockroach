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
	cpuTimeTokenDampeningDeficitMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.dampening_deficit_nanos",
		Help: crstrings.UnwrapText(`
			Cumulative time-weighted deficit from dampening CPU time token
			allocations during scheduler overload. Each tick increments by
			(1 - dampening_factor) * elapsed_nanos, where dampening_factor
			is in [floor, 1.0] (floor is 0.5). rate() / 1e9 yields the
			average fraction of token issuance suppressed; a rate of 0 means
			no dampening was active in the window, 5e8 means the floor was
			held for the full window`),
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

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

	cpuTimeTokensUsedPerTenantMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.per_tenant.tokens_used",
		Help: crstrings.UnwrapText(`
			Cumulative CPU time tokens consumed per tenant by admitted
			work; rate() gives the per-tenant token consumption rate`),
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}

	cpuTimeTokensReturnedPerTenantMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.per_tenant.tokens_returned",
		Help: crstrings.UnwrapText(`
			Cumulative CPU time tokens returned per tenant, for example
			when actual CPU usage was lower than the initial estimate;
			rate() gives the per-tenant token return rate`),
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}
)

// cpuTimeTokenMetrics tracks metrics for the CPU time token admission
// control. Fields are exported because metric.Registry.AddMetricStruct
// uses reflection to discover metrics.
type cpuTimeTokenMetrics struct {
	// DampeningDeficitNanos is the cumulative dampening deficit
	// (1 - dampening_factor), weighted by elapsed nanoseconds. See
	// cpuTimeTokenDampeningDeficitMeta and
	// cpuTimeTokenAllocator.dampeningFactor for details.
	DampeningDeficitNanos *metric.Counter
	Multiplier            *metric.GaugeFloat64
	TokensConsumed        *metric.Counter
	TokensReturned        *metric.Counter

	// ExhaustedDurationNanos tracks cumulative nanoseconds each bucket has
	// spent exhausted. Each burst qualification gets its own counter rather
	// than using a CounterVec, because tookWithoutPermissionLocked is on the
	// hot admission path: Counter.Inc is a single atomic add, whereas
	// CounterVec.Inc involves allocations, mutex grabs, and hash lookups.
	//
	// The counter accumulates nanoseconds of exhaustion. Applying
	// rate(exhausted_duration_nanos) in DD/Prometheus yields the fraction of
	// wall-clock time the bucket was exhausted, queryable over any window
	// (1m, 5m, 30m, etc.).
	ExhaustedDurationNanos [numBurstQualifications]*metric.Counter

	// RefillAdded tracks cumulative tokens added to each bucket via
	// the refill process. rate(refill.added) gives the effective refill
	// rate per bucket.
	RefillAdded [numBurstQualifications]*metric.Counter

	// RefillRemoved tracks cumulative tokens removed from each bucket
	// when refill rates decrease between intervals (negative delta).
	RefillRemoved [numBurstQualifications]*metric.Counter

	// AdmittedCountPerTenant and WaitTimeNanosPerTenant track per-tenant
	// admission stats. Together they enable computing mean wait time per
	// tenant via rate(wait_time_nanos) / rate(admitted_count) in
	// DD/Prometheus. We start with these for just CPU time token AC for
	// cost reasons (there are many work queues); over time we may
	// integrate into WorkQueueMetrics. We use two counters to derive the
	// mean rather than a histogram, also for cost reasons.
	AdmittedCountPerTenant *aggmetric.AggCounter
	WaitTimeNanosPerTenant *aggmetric.AggCounter

	// TokensUsedPerTenant and TokensReturnedPerTenant track per-tenant
	// token consumption and returns via adjustGroupUsedLocked. Together
	// they give per-tenant visibility into token flow.
	TokensUsedPerTenant     *aggmetric.AggCounter
	TokensReturnedPerTenant *aggmetric.AggCounter
}

func makeCPUTimeTokenMetrics() *cpuTimeTokenMetrics {
	// Two-label scheme: kind discriminates "tenant" vs "rg" so the same
	// numeric ID in different namespaces (system tenant 1 vs rg 1) maps
	// to distinct child time series; tenant_id stays a bare numeric
	// string for parity with other per-tenant AggCounters
	// (multitenant.TenantIDLabel) so dashboards filtering on tenant_id
	// keep working.
	b := aggmetric.MakeBuilder("kind", "tenant_id")
	m := &cpuTimeTokenMetrics{
		DampeningDeficitNanos:   metric.NewCounter(cpuTimeTokenDampeningDeficitMeta),
		Multiplier:              metric.NewGaugeFloat64(cpuTimeTokenMultiplierMeta),
		TokensConsumed:          metric.NewCounter(cpuTimeTokensConsumedMeta),
		TokensReturned:          metric.NewCounter(cpuTimeTokensReturnedMeta),
		AdmittedCountPerTenant:  b.Counter(cpuTimeTokenAdmittedCountPerTenantMeta),
		WaitTimeNanosPerTenant:  b.Counter(cpuTimeTokenWaitTimeNanosPerTenantMeta),
		TokensUsedPerTenant:     b.Counter(cpuTimeTokensUsedPerTenantMeta),
		TokensReturnedPerTenant: b.Counter(cpuTimeTokensReturnedPerTenantMeta),
	}
	for qual := burstQualification(0); qual < numBurstQualifications; qual++ {
		exhMeta := metric.Metadata{
			Name: fmt.Sprintf(
				"admission.cpu_time_tokens.exhausted_duration_nanos.%s", qual),
			Help: fmt.Sprintf(
				"Cumulative nanoseconds the %s CPU time token bucket has spent "+
					"exhausted (tokens <= 0); rate() gives the fraction of wall-clock "+
					"time the bucket was exhausted",
				qual),
			Measurement: "Nanoseconds",
			Unit:        metric.Unit_NANOSECONDS,
		}
		m.ExhaustedDurationNanos[qual] = metric.NewCounter(exhMeta)

		addedMeta := metric.Metadata{
			Name: fmt.Sprintf(
				"admission.cpu_time_tokens.refill.added.%s", qual),
			Help: fmt.Sprintf(
				"Cumulative tokens added to the %s CPU time token bucket "+
					"via the refill process; rate() gives the effective refill rate",
				qual),
			Measurement: "Tokens",
			Unit:        metric.Unit_COUNT,
		}
		m.RefillAdded[qual] = metric.NewCounter(addedMeta)

		removedMeta := metric.Metadata{
			Name: fmt.Sprintf(
				"admission.cpu_time_tokens.refill.removed.%s", qual),
			Help: fmt.Sprintf(
				"Cumulative tokens removed from the %s CPU time token bucket "+
					"when refill rates decrease between intervals",
				qual),
			Measurement: "Tokens",
			Unit:        metric.Unit_COUNT,
		}
		m.RefillRemoved[qual] = metric.NewCounter(removedMeta)
	}
	return m
}

// MetricStruct implements the metric.Struct interface.
func (cpuTimeTokenMetrics) MetricStruct() {}
