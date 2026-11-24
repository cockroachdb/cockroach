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

	cpuTimeTokensUsageConsumedMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.usage.consumed",
		Help: crstrings.UnwrapText(`
			Cumulative number of CPU time tokens consumed (deducted from
			buckets) by admitted work`),
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}

	cpuTimeTokensUsageReturnedMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.usage.returned",
		Help: crstrings.UnwrapText(`
			Cumulative number of CPU time tokens returned (credited back to
			buckets), for example when actual CPU usage was lower than the
			initial estimate`),
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}

	// Primary per-group metric metadata. The counters built from these
	// metadata are labeled (tenant_id, group_id) and cover every group
	// uniformly: serverless tenant groups have group_id="0", RM-mode
	// resource groups have tenant_id="0", and future user-defined
	// groups may set both. The same numeric id in different namespaces
	// (system tenant 1 vs rg 1) maps to distinct time series because
	// the (tenant_id, group_id) tuples differ.
	cpuTimeTokenAdmittedCountMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.admitted_count",
		Help: crstrings.UnwrapText(`
			Cumulative number of requests admitted per group by CPU time
			token admission control; use with wait_time_nanos to compute
			mean wait time via rate(wait_time) / rate(admitted_count)`),
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}

	cpuTimeTokenWaitTimeNanosMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.wait_time_nanos",
		Help: crstrings.UnwrapText(`
			Cumulative nanoseconds of admission queue wait time per group
			in CPU time token admission control; use with admitted_count to
			compute mean wait time via rate(wait_time) / rate(admitted_count)`),
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	cpuTimeTokensUsedMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.tokens_used",
		Help: crstrings.UnwrapText(`
			Cumulative CPU time tokens consumed per group by admitted
			work; rate() gives the per-group token consumption rate`),
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}

	cpuTimeTokensReturnedMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.tokens_returned",
		Help: crstrings.UnwrapText(`
			Cumulative CPU time tokens returned per group, for example
			when actual CPU usage was lower than the initial estimate;
			rate() gives the per-group token return rate`),
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}

	// Legacy per-tenant metric metadata. These are populated only for
	// serverless tenant groups (groupID==0 && tenantID>0) so that
	// dashboards predating the primary per-group family above continue
	// to work. New consumers should target the primary family.
	cpuTimeTokenLegacyAdmittedCountPerTenantMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.per_tenant.admitted_count",
		Help: crstrings.UnwrapText(`
			Cumulative number of requests admitted per tenant by CPU time
			token admission control; use with wait_time_nanos to compute
			mean wait time via rate(wait_time) / rate(admitted_count).
			Retained for compatibility with dashboards predating the
			admission.cpu_time_tokens.admitted_count family`),
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}

	cpuTimeTokenLegacyWaitTimeNanosPerTenantMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.per_tenant.wait_time_nanos",
		Help: crstrings.UnwrapText(`
			Cumulative nanoseconds of admission queue wait time per tenant
			in CPU time token admission control; use with admitted_count to
			compute mean wait time via rate(wait_time) / rate(admitted_count).
			Retained for compatibility with dashboards predating the
			admission.cpu_time_tokens.wait_time_nanos family`),
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	cpuTimeTokensLegacyUsedPerTenantMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.per_tenant.tokens_used",
		Help: crstrings.UnwrapText(`
			Cumulative CPU time tokens consumed per tenant by admitted
			work; rate() gives the per-tenant token consumption rate.
			Retained for compatibility with dashboards predating the
			admission.cpu_time_tokens.tokens_used family`),
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}

	cpuTimeTokensLegacyReturnedPerTenantMeta = metric.Metadata{
		Name: "admission.cpu_time_tokens.per_tenant.tokens_returned",
		Help: crstrings.UnwrapText(`
			Cumulative CPU time tokens returned per tenant, for example
			when actual CPU usage was lower than the initial estimate;
			rate() gives the per-tenant token return rate. Retained for
			compatibility with dashboards predating the
			admission.cpu_time_tokens.tokens_returned family`),
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
	UsageConsumed         *metric.Counter
	UsageReturned         *metric.Counter

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

	// Primary per-group AggCounters with (tenant_id, group_id) labels.
	// Every group — serverless tenant or RM resource group — feeds
	// these. AdmittedCount and WaitTimeNanos together enable mean
	// wait time per group via rate(wait_time_nanos) / rate(admitted_count).
	// TokensUsed and TokensReturned record per-group token flow
	// through WorkQueue.adjustGroupUsedLocked. We use two counters to
	// derive the mean rather than a histogram for cost reasons (there
	// are many work queues).
	AdmittedCount  *aggmetric.AggCounter
	WaitTimeNanos  *aggmetric.AggCounter
	TokensUsed     *aggmetric.AggCounter
	TokensReturned *aggmetric.AggCounter

	// Legacy per-tenant AggCounters, populated only for serverless
	// tenant groups (groupID==0 && tenantID>0). RM-mode resource
	// groups and any future user-defined groups do not feed these.
	// Retained so that dashboards predating the primary family above
	// continue to function; new consumers should target the primary
	// family.
	LegacyAdmittedCountPerTenant  *aggmetric.AggCounter
	LegacyWaitTimeNanosPerTenant  *aggmetric.AggCounter
	LegacyTokensUsedPerTenant     *aggmetric.AggCounter
	LegacyTokensReturnedPerTenant *aggmetric.AggCounter
}

func makeCPUTimeTokenMetrics() *cpuTimeTokenMetrics {
	// The primary family carries both labels so a single time series
	// shape covers every group in every mode. Tenant 1 and rg 1 are
	// distinct series — (tenant_id="1", group_id="0") vs
	// (tenant_id="0", group_id="1") — even when both share an id
	// value.
	primaryBuilder := aggmetric.MakeBuilder("tenant_id", "group_id")
	// The legacy per-tenant family uses a single tenant_id label
	// (matching multitenant.TenantIDLabel and the convention used by
	// other per-tenant AggCounters in CRDB) so dashboards filtering on
	// tenant_id keep working.
	legacyTenantBuilder := aggmetric.MakeBuilder("tenant_id")
	m := &cpuTimeTokenMetrics{
		DampeningDeficitNanos: metric.NewCounter(cpuTimeTokenDampeningDeficitMeta),
		Multiplier:            metric.NewGaugeFloat64(cpuTimeTokenMultiplierMeta),
		UsageConsumed:         metric.NewCounter(cpuTimeTokensUsageConsumedMeta),
		UsageReturned:         metric.NewCounter(cpuTimeTokensUsageReturnedMeta),

		AdmittedCount:  primaryBuilder.Counter(cpuTimeTokenAdmittedCountMeta),
		WaitTimeNanos:  primaryBuilder.Counter(cpuTimeTokenWaitTimeNanosMeta),
		TokensUsed:     primaryBuilder.Counter(cpuTimeTokensUsedMeta),
		TokensReturned: primaryBuilder.Counter(cpuTimeTokensReturnedMeta),

		LegacyAdmittedCountPerTenant:  legacyTenantBuilder.Counter(cpuTimeTokenLegacyAdmittedCountPerTenantMeta),
		LegacyWaitTimeNanosPerTenant:  legacyTenantBuilder.Counter(cpuTimeTokenLegacyWaitTimeNanosPerTenantMeta),
		LegacyTokensUsedPerTenant:     legacyTenantBuilder.Counter(cpuTimeTokensLegacyUsedPerTenantMeta),
		LegacyTokensReturnedPerTenant: legacyTenantBuilder.Counter(cpuTimeTokensLegacyReturnedPerTenantMeta),
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
