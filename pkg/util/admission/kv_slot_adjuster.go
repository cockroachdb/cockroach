// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
)

// KVSlotAdjusterOverloadThreshold sets a goroutine runnable threshold at
// which the CPU will be considered overloaded, when running in a node that
// executes KV operations.
var KVSlotAdjusterOverloadThreshold = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"admission.kv_slot_adjuster.overload_threshold",
	"when the number of runnable goroutines per CPU is greater than this threshold, the "+
		"slot adjuster considers the cpu to be overloaded",
	// Effectively disable slot based AC since the interaction between slots and
	// tokens is not ideal. If tokens are available but not slots, then tokens
	// can pile up and then when slots become available there can be a burst of
	// admission. Bursts of admission are bad for goroutine scheduling latency.
	8192, settings.PositiveInt)

var KVCPUTimeTokensEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"admission.kv_cpu_time_tokens.enabled", "", true)

var KVCPUTimeUtilGoal = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"admission.kv_cpu_time_util_goal",
	"the target CPU utilization for the KV CPU time token system", 0.8)

// kvSlotAdjuster is an implementer of CPULoadListener and
// cpuOverloadIndicator.
type kvSlotAdjuster struct {
	settings *cluster.Settings
	// This is the slotGranter used for KVWork.
	granter     *slotGranter
	minCPUSlots int
	maxCPUSlots int

	totalSlotsMetric                 *metric.Gauge
	cpuLoadShortPeriodDurationMetric *metric.Counter
	cpuLoadLongPeriodDurationMetric  *metric.Counter
	slotAdjusterIncrementsMetric     *metric.Counter
	slotAdjusterDecrementsMetric     *metric.Counter
}

var _ cpuOverloadIndicator = &kvSlotAdjuster{}
var _ CPULoadListener = &kvSlotAdjuster{}

func (kvsa *kvSlotAdjuster) CPULoad(runnable int, procs int, samplePeriod time.Duration) {
	threshold := int(KVSlotAdjusterOverloadThreshold.Get(&kvsa.settings.SV))

	periodDurationMicros := samplePeriod.Microseconds()
	if samplePeriod > time.Millisecond {
		kvsa.cpuLoadLongPeriodDurationMetric.Inc(periodDurationMicros)
	} else {
		kvsa.cpuLoadShortPeriodDurationMetric.Inc(periodDurationMicros)
	}

	// Simple heuristic, which worked ok in experiments. More sophisticated ones
	// could be devised.
	usedSlots := kvsa.granter.usedSlots
	tryDecreaseSlots := func(total int, adjustMetric bool) int {
		// Overload.
		// If using some slots, and the used slots is less than the total slots,
		// and total slots hasn't bottomed out at the min, decrease the total
		// slots. If currently using more than the total slots, it suggests that
		// the previous slot reduction has not taken effect yet, so we hold off on
		// further decreasing.
		// TODO(sumeer): despite the additive decrease and high multiplier value,
		// the metric showed some drops from 40 slots to 1 slot on a kv50 overload
		// workload. It was not accompanied by a drop in runnable count per proc,
		// so it is suggests that the drop in slots should not be causing cpu
		// under-utilization, but one cannot be sure. Experiment with a smoothed
		// signal or other ways to prevent a fast drop.
		if usedSlots > 0 && total > kvsa.minCPUSlots && usedSlots <= total {
			total--
			if adjustMetric {
				kvsa.slotAdjusterDecrementsMetric.Inc(1)
			}
		}
		return total
	}
	tryIncreaseSlots := func(total int, adjustMetric bool) int {
		// Underload.
		// Used all its slots and can increase further, so additive increase. We
		// also handle the case where the used slots are a bit less than total
		// slots, since callers for soft slots don't block.
		if usedSlots >= total && total < kvsa.maxCPUSlots {
			// NB: If the workload is IO bound, the slot count here will keep
			// incrementing until these slots are no longer the bottleneck for
			// admission. So it is not unreasonable to see this slot count go into
			// the 1000s. If the workload switches to being CPU bound, we can
			// decrease by 1000 slots every second (because the CPULoad ticks are at
			// 1ms intervals, and we do additive decrease).
			total++
			if adjustMetric {
				kvsa.slotAdjusterIncrementsMetric.Inc(1)
			}
		}
		return total
	}

	if runnable >= threshold*procs {
		// Overloaded.
		kvsa.granter.setTotalSlotsLocked(
			tryDecreaseSlots(kvsa.granter.totalSlots, true))
	} else if float64(runnable) <= float64((threshold*procs)/2) {
		// Underloaded -- can afford to increase regular slots.
		kvsa.granter.setTotalSlotsLocked(
			tryIncreaseSlots(kvsa.granter.totalSlots, true))
	}

	kvsa.totalSlotsMetric.Update(int64(kvsa.granter.totalSlots))
}

func (kvsa *kvSlotAdjuster) isOverloaded() bool {
	return kvsa.granter.usedSlots >= kvsa.granter.totalSlots && !kvsa.granter.skipSlotEnforcement
}

type cpuTimeTokenAdjuster struct {
	settings              *cluster.Settings
	granter               *slotAndCPUTimeTokenGranter
	tenantTokensRequester tenantTokensRequester

	kvCPUTimeTokens           *metric.Gauge
	kvCPUTimeTokensAdded      *metric.Counter
	kvCPUTimeTokensRemoved    *metric.Counter
	kvCPUTimeTokensRate       *metric.Gauge
	kvTenantCPUTimeTokensRate *metric.Gauge
	kvTokensToCPUMultiplier   *metric.Gauge

	lastSampleTime           time.Time
	totalCPUTimeMillis       int64
	lastTokensInBucket       int64
	lastCPUTimeTokensEnabled bool

	ticks int64

	tokenToCPUTimeMultiplier float64

	// tokenBucketRate is also the burst budget. And since adjust is called
	// every 1s, this is also the total tokens to give out until the next call
	// to adjust.
	tokenBucketRate       int64
	tokensAllocated       int64
	tenantTokenBucketRate int64
	tenantTokensAllocated int64
	init                  bool
}

func (ctta *cpuTimeTokenAdjuster) setGaugeMetrics() {
	if ctta.kvCPUTimeTokens != nil {
		ctta.kvCPUTimeTokens.Update(ctta.granter.cpuTimeTokens)
		ctta.kvCPUTimeTokensRate.Update(ctta.tokenBucketRate)
		ctta.kvTenantCPUTimeTokensRate.Update(ctta.tenantTokenBucketRate)
		ctta.kvTokensToCPUMultiplier.Update(int64(ctta.tokenToCPUTimeMultiplier * 100))
	}
}

// adjust is called every 1s.
func (ctta *cpuTimeTokenAdjuster) adjust(
	now time.Time, totalCPUTimeMillis int64, cpuCapacity float64,
) {
	goalUtil := KVCPUTimeUtilGoal.Get(&ctta.settings.SV)
	cpuTimeTokensEnabled := KVCPUTimeTokensEnabled.Get(&ctta.settings.SV)
	if !ctta.init {
		ctta.init = true
		ctta.lastSampleTime = now
		ctta.totalCPUTimeMillis = totalCPUTimeMillis
		ctta.tokenToCPUTimeMultiplier = 1.0
		ctta.tokenBucketRate = int64(cpuCapacity * float64(time.Second) * goalUtil)
		ctta.lastTokensInBucket = ctta.tokenBucketRate
		ctta.lastCPUTimeTokensEnabled = cpuTimeTokensEnabled
		ctta.granter.cpuTimeTokens = ctta.tokenBucketRate
		ctta.granter.tokensEnabled = cpuTimeTokensEnabled
		ctta.tenantTokenBucketRate = ctta.tokenBucketRate
		ctta.tenantTokensRequester.setTenantCPUTokensBurstLimit(
			ctta.tokenBucketRate, cpuTimeTokensEnabled)
		ctta.setGaugeMetrics()
		if ctta.kvCPUTimeTokensAdded != nil {
			ctta.kvCPUTimeTokensAdded.Inc(ctta.tokenBucketRate)
		}
		return
	}
	dur := now.Sub(ctta.lastSampleTime)
	ctta.lastSampleTime = now
	intCPUTimeMillis := totalCPUTimeMillis - ctta.totalCPUTimeMillis
	if intCPUTimeMillis < 0 {
		intCPUTimeMillis = 0
	}
	ctta.totalCPUTimeMillis = totalCPUTimeMillis
	intCPUTimeNanos := intCPUTimeMillis * 1e6
	const lowCPUUtilFrac = 0.25
	isLowCPUUtil := intCPUTimeNanos < int64(float64(dur)*cpuCapacity*lowCPUUtilFrac)
	intRegularTokensUsed, intUncontrolledTokensUsed := ctta.granter.getAndResetIntervalTokensUsed()
	if intRegularTokensUsed <= 0 {
		intRegularTokensUsed = 1
	}
	intElasticTokensUsed := int64(0) /* TODO(sumeer): get elastic tokens used and reset */
	if isLowCPUUtil {
		// Ensure that low CPU utilization is not due to a flawed tokenToCPUTimeMultiplier
		// by multiplicatively lowering it until we are below the upperBound.
		const upperBound = (1 / lowCPUUtilFrac) * 0.9
		if ctta.tokenToCPUTimeMultiplier > upperBound {
			ctta.tokenToCPUTimeMultiplier /= 1.5
			if ctta.tokenToCPUTimeMultiplier < upperBound {
				ctta.tokenToCPUTimeMultiplier = upperBound
			}
		}
	} else {
		tokenToCPUTimeMultiplier :=
			float64(intCPUTimeNanos) / float64(intRegularTokensUsed+intElasticTokensUsed)
		if tokenToCPUTimeMultiplier > 20 {
			// Cap the multiplier.
			tokenToCPUTimeMultiplier = 20
		} else if tokenToCPUTimeMultiplier < 1 {
			// Likely because work is queued up in the goroutine scheduler.
			tokenToCPUTimeMultiplier = 1
		}
		// Decrease faster than increase.
		alpha := 0.5
		if tokenToCPUTimeMultiplier < ctta.tokenToCPUTimeMultiplier {
			alpha = 0.8
		}
		ctta.tokenToCPUTimeMultiplier =
			alpha*tokenToCPUTimeMultiplier + (1-alpha)*ctta.tokenToCPUTimeMultiplier
	}
	tokenBucketRate :=
		int64((cpuCapacity * float64(time.Second) * goalUtil) / ctta.tokenToCPUTimeMultiplier)
	intUnderUtilizationOfTokens := ctta.granter.cpuTimeTokens > ctta.tokenBucketRate/8
	// Ignoring the fact that tokens are capped to the burst.
	intAddedTokens := int64(float64(ctta.tokenBucketRate) * (float64(dur) / float64(time.Second)))
	tokenBucketRateDelta := tokenBucketRate - ctta.tokenBucketRate
	prevTenantToTokenBucketRatio := float64(ctta.tenantTokenBucketRate) / float64(ctta.tokenBucketRate)
	ctta.tokenBucketRate = tokenBucketRate
	ctta.tenantTokenBucketRate = int64(prevTenantToTokenBucketRatio * float64(ctta.tokenBucketRate))

	cpuTimeTokens := ctta.granter.cpuTimeTokens + tokenBucketRateDelta
	if cpuTimeTokens > ctta.tokenBucketRate ||
		(cpuTimeTokensEnabled && !ctta.lastCPUTimeTokensEnabled) {
		cpuTimeTokens = ctta.tokenBucketRate
	} else if cpuTimeTokens < 0 {
		cpuTimeTokens = 0
	}
	change := cpuTimeTokens - ctta.granter.cpuTimeTokens
	ctta.granter.cpuTimeTokens = cpuTimeTokens
	ctta.granter.tokensEnabled = cpuTimeTokensEnabled
	if ctta.kvCPUTimeTokensAdded != nil {
		if change > 0 {
			ctta.kvCPUTimeTokensAdded.Inc(change)
		} else if change < 0 {
			ctta.kvCPUTimeTokensRemoved.Inc(-change)
		}
	}

	intMaxControlledTokensAvailable := ctta.lastTokensInBucket + intAddedTokens
	intMinControlledTokensAvailable := min(intMaxControlledTokensAvailable, intAddedTokens)
	excessTokens := intRegularTokensUsed - intMaxControlledTokensAvailable
	if excessTokens < 0 {
		excessTokens = 0
	}
	underUsage := intMinControlledTokensAvailable - intRegularTokensUsed
	ctta.lastTokensInBucket = ctta.granter.cpuTimeTokens
	if true {
		if intUnderUtilizationOfTokens || (cpuTimeTokensEnabled && !ctta.lastCPUTimeTokensEnabled) {
			ctta.tenantTokenBucketRate = ctta.tokenBucketRate
		} else {
			toDeductFromTenantTokens := excessTokens
			// Some tenants are taking more than we want. We need to restrict per-tenant
			// cpu-time tokens.
			ctta.tenantTokenBucketRate -= toDeductFromTenantTokens
			// Don't let the tenant token rate fall below 10%. This means each tenant
			// can consume 2.5% of goal without being controlled. So if overload due to
			// many small tenants, performance isolation will suffer. We deem this
			// acceptable since we are trying to avoid queueing for small tenants in
			// general.
			if ctta.tenantTokenBucketRate < ctta.tokenBucketRate/10 {
				ctta.tenantTokenBucketRate = ctta.tokenBucketRate / 10
			}
		}
	} else {
		toDeduct := max(excessTokens, intUncontrolledTokensUsed)
		ctta.tenantTokenBucketRate -= toDeduct
		if ctta.tenantTokenBucketRate < ctta.tokenBucketRate/10 {
			ctta.tenantTokenBucketRate = ctta.tokenBucketRate / 10
		}
		if underUsage > 0 {
			// We are under-utilizing the aggregate tokens. We need to
			// increase the tenant burst tokens.
			ctta.tenantTokenBucketRate += underUsage
			if ctta.tenantTokenBucketRate > ctta.tokenBucketRate {
				ctta.tenantTokenBucketRate = ctta.tokenBucketRate
			}
		}
	}
	log.Infof(context.Background(),
		"%s: rates=%s,%s, allocated=%s,%s, last-tokens=%s(underutil=%t,%s) excess=%s uncontrolled=%s mutiplier=%.1f ticks=%d",
		now.String(),
		time.Duration(ctta.tokenBucketRate), time.Duration(ctta.tenantTokenBucketRate),
		time.Duration(ctta.tokensAllocated), time.Duration(ctta.tenantTokensAllocated),
		time.Duration(ctta.lastTokensInBucket), intUnderUtilizationOfTokens, time.Duration(underUsage),
		time.Duration(excessTokens), time.Duration(intUncontrolledTokensUsed),
		ctta.tokenToCPUTimeMultiplier, ctta.ticks)
	ctta.ticks = 0
	ctta.tenantTokensRequester.setTenantCPUTokensBurstLimit(
		ctta.tenantTokenBucketRate, cpuTimeTokensEnabled)
	ctta.tokensAllocated = 0
	ctta.tenantTokensAllocated = 0
	ctta.lastCPUTimeTokensEnabled = cpuTimeTokensEnabled
}

func (ctta *cpuTimeTokenAdjuster) allocateTokensTick(remainingTicks int64) {
	ctta.ticks++
	allocateFunc := func(total int64, allocated int64, remainingTicks int64) (toAllocate int64) {
		remainingTokens := total - allocated
		// Round up so that we don't accumulate tokens to give in a burst on the
		// last tick.
		toAllocate = (remainingTokens + remainingTicks - 1) / remainingTicks
		if toAllocate < 0 {
			panic(errors.AssertionFailedf("toAllocate is negative %d", toAllocate))
		}
		if toAllocate+allocated > total {
			toAllocate = total - allocated
		}
		return toAllocate
	}
	toAllocateTokens := allocateFunc(ctta.tokenBucketRate, ctta.tokensAllocated, remainingTicks)
	ctta.tokensAllocated += toAllocateTokens
	cpuTimeTokens := ctta.granter.cpuTimeTokens + toAllocateTokens
	if cpuTimeTokens > ctta.tokenBucketRate {
		cpuTimeTokens = ctta.tokenBucketRate
	}
	delta := cpuTimeTokens - ctta.granter.cpuTimeTokens
	if ctta.kvCPUTimeTokensAdded != nil {
		if delta > 0 {
			ctta.kvCPUTimeTokensAdded.Inc(delta)
		} else if delta < 0 {
			ctta.kvCPUTimeTokensRemoved.Inc(-delta)
		}
	}
	ctta.granter.cpuTimeTokens = cpuTimeTokens

	toAllocateTenantTokens := allocateFunc(
		ctta.tenantTokenBucketRate, ctta.tenantTokensAllocated, remainingTicks)
	ctta.tenantTokensAllocated += toAllocateTenantTokens
	withoutPermissionTokens := ctta.tenantTokensRequester.tenantCPUTokensTick(toAllocateTenantTokens)
	for _, tokens := range withoutPermissionTokens {
		if tokens < 0 {
			panic("negative withoutPermissionTokens")
		}
		ctta.granter.tookWithoutPermissionLocked(tokens, 0)
	}
	ctta.setGaugeMetrics()
}

type tenantTokensRequester interface {
	tenantCPUTokensTick(tokensToAdd int64) (withoutPermissionTokens []int64)
	setTenantCPUTokensBurstLimit(tokens int64, enabled bool)
}
