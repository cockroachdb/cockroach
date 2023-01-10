// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package admission

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// KVSlotAdjusterOverloadThreshold sets a goroutine runnable threshold at
// which the CPU will be considered overloaded, when running in a node that
// executes KV operations.
var KVSlotAdjusterOverloadThreshold = settings.RegisterIntSetting(
	settings.TenantWritable,
	"admission.kv_slot_adjuster.overload_threshold",
	"when the number of runnable goroutines per CPU is greater than this threshold, the "+
		"slot adjuster considers the cpu to be overloaded",
	32, settings.PositiveInt)

// kvSlotAdjuster is an implementer of CPULoadListener and
// cpuOverloadIndicator.
type kvSlotAdjuster struct {
	settings *cluster.Settings
	// This is the slotGranter used for KVWork. In single-tenant settings, it
	// is the only one we adjust using the periodic cpu overload signal. We
	// don't adjust slots for SQLStatementLeafStartWork and
	// SQLStatementRootStartWork using the periodic cpu overload signal since:
	// - these are potentially long-lived work items and not CPU bound
	// - we don't know how to coordinate adjustment of those slots and the KV
	//   slots.
	granter     *slotGranter
	minCPUSlots int
	maxCPUSlots int
	// moderateSlotsClamp is the most recent value which may have been used to
	// clamp down on slotGranter.totalModerateLoadSlots. Justification for
	// clamping down on totalModerateLoadSlots is given where the moderateSlotsClamp
	// value is written to.
	moderateSlotsClamp int
	// moderateSlotsClampOverride is used during testing to override the value of the
	// moderateSlotsClamp. Its purpose is to make it easier to write tests. A default
	// value of 0 implies no override.
	moderateSlotsClampOverride int
	// runnableEWMA is a weighted average of the most recent runnable goroutine counts.
	// runnableEWMA is used to tune the slotGranter.totalModerateLoadSlots.
	runnableEWMA float64
	// runnableAlphaOverride is used to override the value of runnable alpha during testing.
	// A 0 value indicates that there is no override.
	runnableAlphaOverride float64

	totalSlotsMetric                 *metric.Gauge
	totalModerateSlotsMetric         *metric.Gauge
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
	// 0.009 gives weight to at least a few hundred samples at a 1ms sampling rate.
	alpha := 0.009 * float64(samplePeriod/time.Millisecond)
	if alpha > 0.5 {
		alpha = 0.5
	} else if alpha < 0.001 {
		alpha = 0.001
	}
	if kvsa.runnableAlphaOverride > 0 {
		alpha = kvsa.runnableAlphaOverride
	}
	kvsa.runnableEWMA = kvsa.runnableEWMA*(1-alpha) + float64(runnable)*alpha

	// Simple heuristic, which worked ok in experiments. More sophisticated ones
	// could be devised.
	usedSlots := kvsa.granter.usedSlots + kvsa.granter.usedSoftSlots
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
		// Very overloaded.
		kvsa.granter.setTotalHighLoadSlotsLocked(
			tryDecreaseSlots(kvsa.granter.totalHighLoadSlots, true))
		kvsa.granter.totalModerateLoadSlots = tryDecreaseSlots(
			kvsa.granter.totalModerateLoadSlots, false)
	} else if float64(runnable) <= float64((threshold*procs)/4) {
		// Very underloaded.
		kvsa.granter.setTotalHighLoadSlotsLocked(
			tryIncreaseSlots(kvsa.granter.totalHighLoadSlots, true))
		kvsa.granter.totalModerateLoadSlots = tryIncreaseSlots(
			kvsa.granter.totalModerateLoadSlots, false)
	} else if float64(runnable) <= float64((threshold*procs)/2) {
		// Moderately underloaded -- can afford to increase regular slots.
		kvsa.granter.setTotalHighLoadSlotsLocked(
			tryIncreaseSlots(kvsa.granter.totalHighLoadSlots, true))
	} else if runnable >= 3*threshold*procs/4 {
		// Moderately overloaded -- should decrease moderate load slots.
		//
		// NB: decreasing moderate load slots may not halt the runnable growth
		// since the regular traffic may be high and can use up to the high load
		// slots. When usedSlots>totalModerateLoadSlots, we won't actually
		// decrease totalModerateLoadSlots (see the logic in tryDecreaseSlots).
		// However, that doesn't mean that totalModerateLoadSlots is accurate.
		// This inaccuracy is fine since we have chosen to be in a high load
		// regime, since all the work we are doing is non-optional regular work
		// (not background work).
		//
		// Where this will help is when what is pushing us over moderate load is
		// optional background work, so by decreasing totalModerateLoadSlots we will
		// contain the load due to that work.
		kvsa.granter.totalModerateLoadSlots = tryDecreaseSlots(
			kvsa.granter.totalModerateLoadSlots, false)
	}
	// Consider the following cases, when we started this method with
	// totalHighLoadSlots==totalModerateLoadSlots.
	// - underload such that we are able to increase totalModerateLoadSlots: in
	//   this case we will also be able to increase totalHighLoadSlots (since
	//   the used and total comparisons gating the increase in tryIncreaseSlots
	//   will also be true for totalHighLoadSlots).
	// - overload such that we are able to decrease totalHighLoadSlots: in this
	//   case the logic in tryDecreaseSlots will also be able to decrease
	//   totalModerateLoadSlots.
	// So the natural behavior of the slot adjustment itself guarantees
	// totalHighLoadSlots >= totalModerateLoadSlots. But as a defensive measure
	// we clamp totalModerateLoadSlots to not exceed totalHighLoadSlots.
	if kvsa.granter.totalHighLoadSlots < kvsa.granter.totalModerateLoadSlots {
		kvsa.granter.totalModerateLoadSlots = kvsa.granter.totalHighLoadSlots
	}

	// During a kv50 workload, we noticed soft slots grants succeeding despite
	// high cpu utilization, and high runnable goroutine counts.
	//
	// Consider the following log lines from the kv50 experiment:
	// [runnable count 372 threshold*procs 256]
	// [totalHighLoadSlots 254 totalModerateLoadSlots 164 usedSlots 0 usedSoftSlots 1]
	//
	// Note that even though the runnable count is high, of the (254, 164),
	// (totalHighLoad, totalModerateLoad) slots respectively, only 1 slot is
	// being used. The slot mechanism behaves in a bi-modal manner in nodes that
	// do both KV and SQL processing. While there is backlogged KV work, the slot
	// usage is high, and blocks all SQL work, but eventually all callers have done
	// their KV processing and are queued up for SQL work. The latter causes bursts
	// of grants (because it uses tokens), gated only by the grant-chain mechanism,
	// during which runnable count is high but used (KV) slots are low. This is exactly
	// the case where we have low slot usage, but high CPU utilization.
	//
	// We can afford to be more conservative in calculating totalModerateLoadSlots
	// since we don't care about saturating CPU for the less important work that is
	// controlled by these slots. So we could use a slow reacting and conservative
	// signal to decide on the value of totalModerateLoadSlots.
	//
	// To account for the increased CPU utilization and runnable counts when the used
	// slots are low, we clamp down on the totalModerateSlots value by keeping track
	// of a historical runnable goroutine average.
	kvsa.moderateSlotsClamp = int(float64(threshold*procs)/2 - kvsa.runnableEWMA)
	if kvsa.moderateSlotsClampOverride != 0 {
		kvsa.moderateSlotsClamp = kvsa.moderateSlotsClampOverride
	}
	if kvsa.granter.totalModerateLoadSlots > kvsa.moderateSlotsClamp {
		kvsa.granter.totalModerateLoadSlots = kvsa.moderateSlotsClamp
	}
	if kvsa.granter.totalModerateLoadSlots < 0 {
		kvsa.granter.totalModerateLoadSlots = 0
	}

	kvsa.totalSlotsMetric.Update(int64(kvsa.granter.totalHighLoadSlots))
	kvsa.totalModerateSlotsMetric.Update(int64(kvsa.granter.totalModerateLoadSlots))
}

func (kvsa *kvSlotAdjuster) isOverloaded() bool {
	return kvsa.granter.usedSlots >= kvsa.granter.totalHighLoadSlots && !kvsa.granter.skipSlotEnforcement
}
