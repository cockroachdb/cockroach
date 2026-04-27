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

	// NB: The per-tenant metric metadata templates below are used to create
	// one AggCounter per resource tier (system_tenant / app_tenant). The tier
	// suffix is appended in makeCPUTimeTokenMetrics. See the comment on
	// cpuTimeTokenMetrics.AdmittedCountPerTenant for the rationale.
	cpuTimeTokenAdmittedCountPerTenantMetaBase = metric.Metadata{
		Name: "admission.cpu_time_tokens.per_tenant.admitted_count.%s",
		Help: crstrings.UnwrapText(`
			Cumulative number of requests admitted per tenant by CPU time
			token admission control; use with wait_time_nanos to compute
			mean wait time via rate(wait_time) / rate(admitted_count)`),
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}

	cpuTimeTokenWaitTimeNanosPerTenantMetaBase = metric.Metadata{
		Name: "admission.cpu_time_tokens.per_tenant.wait_time_nanos.%s",
		Help: crstrings.UnwrapText(`
			Cumulative nanoseconds of admission queue wait time per tenant
			in CPU time token admission control; use with admitted_count to
			compute mean wait time via rate(wait_time) / rate(admitted_count)`),
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	cpuTimeTokensUsedPerTenantMetaBase = metric.Metadata{
		Name: "admission.cpu_time_tokens.per_tenant.tokens_used.%s",
		Help: crstrings.UnwrapText(`
			Cumulative CPU time tokens consumed per tenant by admitted
			work; rate() gives the per-tenant token consumption rate`),
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}

	cpuTimeTokensReturnedPerTenantMetaBase = metric.Metadata{
		Name: "admission.cpu_time_tokens.per_tenant.tokens_returned.%s",
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
	//
	// Each tier gets its own AggCounter because AggCounter.AddChild
	// panics on duplicate label values. Today GetKVWorkQueue routes each
	// tenant to exactly one tier, so duplicates can't happen, but it's
	// better not to rely on that routing invariant for panic safety.
	AdmittedCountPerTenant [numResourceTiers]*aggmetric.AggCounter
	WaitTimeNanosPerTenant [numResourceTiers]*aggmetric.AggCounter

	// TokensUsedPerTenant and TokensReturnedPerTenant track per-tenant
	// token consumption and returns via adjustGroupUsedLocked. Together
	// they give per-tenant visibility into token flow.
	TokensUsedPerTenant     [numResourceTiers]*aggmetric.AggCounter
	TokensReturnedPerTenant [numResourceTiers]*aggmetric.AggCounter
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
		Multiplier:     metric.NewGaugeFloat64(cpuTimeTokenMultiplierMeta),
		TokensConsumed: metric.NewCounter(cpuTimeTokensConsumedMeta),
		TokensReturned: metric.NewCounter(cpuTimeTokensReturnedMeta),
	}
	// Create one AggCounter per tier for each per-tenant metric.
	for tier := resourceTier(0); tier < numResourceTiers; tier++ {
		tierStr := tier.String()
		admittedMeta := cpuTimeTokenAdmittedCountPerTenantMetaBase
		admittedMeta.Name = fmt.Sprintf(
			"admission.cpu_time_tokens.per_tenant.admitted_count.%s", tierStr)
		m.AdmittedCountPerTenant[tier] = b.Counter(admittedMeta)

		waitMeta := cpuTimeTokenWaitTimeNanosPerTenantMetaBase
		waitMeta.Name = fmt.Sprintf(
			"admission.cpu_time_tokens.per_tenant.wait_time_nanos.%s", tierStr)
		m.WaitTimeNanosPerTenant[tier] = b.Counter(waitMeta)

		usedMeta := cpuTimeTokensUsedPerTenantMetaBase
		usedMeta.Name = fmt.Sprintf(
			"admission.cpu_time_tokens.per_tenant.tokens_used.%s", tierStr)
		m.TokensUsedPerTenant[tier] = b.Counter(usedMeta)

		returnedMeta := cpuTimeTokensReturnedPerTenantMetaBase
		returnedMeta.Name = fmt.Sprintf(
			"admission.cpu_time_tokens.per_tenant.tokens_returned.%s", tierStr)
		m.TokensReturnedPerTenant[tier] = b.Counter(returnedMeta)
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
