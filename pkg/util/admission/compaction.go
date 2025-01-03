// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/pebble"
)

// CompactionSlotAdjusterOverloadThreshold sets a goroutine runnable threshold at
// which the CPU will be considered overloaded, when running in a node that
// executes store background operations. This default should be lower than
// the equivalent setting for kv_slot_adjuster, as store background operations
// are lower priority than kv operations that could affect user-observed latency.
var CompactionSlotAdjusterCPUOverloadThreshold = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"admission.compaction_slot_adjuster.overload_threshold",
	"when the number of runnable goroutines per CPU is greater than this threshold, the compaction "+
		"slot adjuster considers the cpu to be overloaded",
	8, settings.PositiveInt)

// CompactionSlotAdjusterIOOverloadThreshold sets an IO score threshold at which
// the system will be considered overloaded, and compaction slots will be incrased to
// address IO overload.
var CompactionSlotAdjusterIOOverloadThreshold = settings.RegisterFloatSetting(
	settings.ApplicationLevel,
	"admission.compaction_slot_adjuster.io_overload_threshold",
	"when the IO load listener's score is above this number, the compaction "+
		"slot adjuster considers compaction debt to be high and increases "+
		"compaction concurrency",
	0.5, settings.PositiveFloat)

type compactionSlotRequester struct{}

var _ requester = compactionSlotRequester{}

func (c compactionSlotRequester) hasWaitingRequests() bool {
	return false
}

func (c compactionSlotRequester) granted(grantChainID grantChainID) int64 {
	return 1
}

func (c compactionSlotRequester) close() {
	// No-op.
}

// compactionSlotAdjuster is an implementer of CPULoadListener and
// cpuOverloadIndicator.
type compactionSlotAdjuster struct {
	settings *cluster.Settings
	// This is the slotGranter used for StoreBackgroundWork.
	granter     *compactionSlotGranter
	minCPUSlots int
	maxCPUSlots int

	kvGranter *kvStoreTokenGranter

	totalSlotsMetric *metric.Gauge
	// cpuLoadShortPeriodDurationMetric *metric.Counter
	// cpuLoadLongPeriodDurationMetric  *metric.Counter
	// slotAdjusterIncrementsMetric     *metric.Counter
	// slotAdjusterDecrementsMetric     *metric.Counter

	state struct {
		updating  atomic.Int32
		runnables atomic.Int32
		procs     atomic.Int32
		ioScore   atomic.Uint64

		lastRun time.Time
	}
}

var _ cpuOverloadIndicator = &compactionSlotAdjuster{}
var _ CPULoadListener = &compactionSlotAdjuster{}

func (ssa *compactionSlotAdjuster) adjustSlotsInnerLocked() {
	now := timeutil.Now()
	if now.Sub(ssa.state.lastRun) < 1*time.Second {
		return
	}
	ssa.state.lastRun = now

	var runnable, procs int32
	var ioScore float64
	runnable = ssa.state.runnables.Load()
	procs = ssa.state.procs.Load()
	ioScore = math.Float64frombits(ssa.state.ioScore.Load())

	threshold := int(CompactionSlotAdjusterCPUOverloadThreshold.Get(&ssa.settings.SV))

	// periodDurationMicros := samplePeriod.Microseconds()
	// if samplePeriod > time.Millisecond {
	// 	ssa.cpuLoadLongPeriodDurationMetric.Inc(periodDurationMicros)
	// } else {
	// 	ssa.cpuLoadShortPeriodDurationMetric.Inc(periodDurationMicros)
	// }

	// Simple heuristic, which worked ok in experiments. More sophisticated ones
	// could be devised.
	// elasticTokensPerCompaction := ssa.kvGranter.l0WriteLM.applyLinearModel(int64(ssa.granter.getCompactionInputSizeAvg()))
	usedSlots := ssa.granter.g.usedSlots
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
		if usedSlots > 0 && total > ssa.minCPUSlots && usedSlots <= total {
			total--
			// if adjustMetric {
			// 	ssa.slotAdjusterDecrementsMetric.Inc(1)
			// }
			// ssa.kvGranter.addIOTokens(admissionpb.ElasticStoreWorkType, elasticTokensPerCompaction)
		}
		return total
	}
	tryIncreaseSlots := func(total int, adjustMetric bool) int {
		// Underload.
		// Used all its slots and can increase further, so additive increase. We
		// also handle the case where the used slots are a bit less than total
		// slots, since callers for soft slots don't block.
		if usedSlots >= total && total < ssa.maxCPUSlots {
			// trySubtractTokens := ssa.kvGranter.trySubtractIOTokens(admissionpb.ElasticStoreWorkType, elasticTokensPerCompaction)
			// if trySubtractTokens {
			// NB: If the workload is IO bound, the slot count here will keep
			// incrementing until these slots are no longer the bottleneck for
			// admission. So it is not unreasonable to see this slot count go into
			// the 1000s. If the workload switches to being CPU bound, we can
			// decrease by 1000 slots every second (because the CPULoad ticks are at
			// 1ms intervals, and we do additive decrease).
			total++
			// if adjustMetric {
			// 	ssa.slotAdjusterIncrementsMetric.Inc(1)
			// }
			// }

		}
		return total
	}

	cpuThresholdOverloaded := int(runnable) >= threshold*int(procs)
	cpuThresholdUnderloaded := float64(runnable) <= float64((threshold*int(procs))/2)
	ioThreshold := CompactionSlotAdjusterIOOverloadThreshold.Get(&ssa.settings.SV)
	ssa.granter.mu.Lock()
	defer ssa.granter.mu.Unlock()

	if cpuThresholdOverloaded || ioScore <= (ioThreshold/2) {
		// CPU overloaded or IO score too low. We should reduce compaction concurrency.
		ssa.granter.g.setTotalSlotsLocked(
			tryDecreaseSlots(ssa.granter.g.totalSlots, true))
	} else if cpuThresholdUnderloaded || ioScore >= ioThreshold {
		// CPU underloaded or IO score very high; we should increase compaction concurrency.
		ssa.granter.g.setTotalSlotsLocked(
			tryIncreaseSlots(ssa.granter.g.totalSlots, true))
	}

	ssa.totalSlotsMetric.Update(int64(ssa.granter.g.totalSlots))
}

func (ssa *compactionSlotAdjuster) CPULoad(runnable int, procs int, samplePeriod time.Duration) {
	ssa.state.runnables.Store(int32(runnable))
	ssa.state.procs.Store(int32(procs))

	if ssa.state.updating.CompareAndSwap(0, 1) {
		ssa.adjustSlotsInnerLocked()
		ssa.state.updating.Store(0)
	}
}

func (ssa *compactionSlotAdjuster) updateIOScore(ioScore float64) {
	ssa.state.ioScore.Store(math.Float64bits(ioScore))

	if ssa.state.updating.CompareAndSwap(0, 1) {
		ssa.adjustSlotsInnerLocked()
		ssa.state.updating.Store(0)
	}
}

func (ssa *compactionSlotAdjuster) isOverloaded() bool {
	return ssa.granter.g.usedSlots >= ssa.granter.g.totalSlots && !ssa.granter.g.skipSlotEnforcement
}

type compactionSlotGranter struct {
	g slotGranter

	deniedSlotsMetric *metric.Counter

	mu struct {
		syncutil.Mutex

		// compactionInputSizeAvg is a running average of the input sizes of
		// compactions observed so far.
		compactionInputSizeAvg float64

		// compactionWriteLM is a linear model that fits the function between compaction
		// reads (input sizes) and writes. It is used to estimate the number of
		// bytes that will be written by an average compaction.
		compactionWriteLM tokensLinearModel
	}
}

var _ pebble.CompactionLimiter = &compactionSlotGranter{}

// TookWithoutPermission implements the pebble.CompactionLimiter interface.
func (c *compactionSlotGranter) TookWithoutPermission(ctx context.Context) pebble.CompactionSlot {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.g.tookWithoutPermission(1 /* count */)
	return &compactionSlot{ctx: ctx, g: c}
}

// RequestSlot implements the pebble.CompactionLimiter interface.
func (c *compactionSlotGranter) RequestSlot(ctx context.Context) (pebble.CompactionSlot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.g.tryGet(1) {
		return &compactionSlot{ctx: ctx, g: c}, nil
	}
	c.deniedSlotsMetric.Inc(1)
	return nil, nil
}

func (c *compactionSlotGranter) getCompactionInputSizeAvg() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.compactionInputSizeAvg
}

type compactionSlot struct {
	ctx context.Context
	g   *compactionSlotGranter

	inputSize             uint64
	estimatedBytesToWrite uint64
}

var _ pebble.CompactionSlot = &compactionSlot{}

// CompactionSelected implements the pebble.CompactionSlot interface.
func (c *compactionSlot) CompactionSelected(firstInputLevel, outputLevel int, inputSize uint64) {
	// TODO: implement
	const alpha = 0.2
	c.g.mu.Lock()
	defer c.g.mu.Unlock()

	// Update the running average of compaction input sizes.
	c.g.mu.compactionInputSizeAvg = alpha*float64(inputSize) + (1-alpha)*c.g.mu.compactionInputSizeAvg
	c.inputSize = inputSize
	// TODO(bilal): implement a linear model.
	c.estimatedBytesToWrite = 2 * inputSize
}

// UpdateMetrics implements the pebble.CompactionSlot interface.
func (c *compactionSlot) UpdateMetrics(bytesRead, bytesWritten uint64) {
	// No-op.
}

// Release implements the pebble.CompactionSlot interface.
func (c *compactionSlot) Release(totalBytesWritten uint64) {
	// TODO(bilal): fit a linear model on c.inputSize and totalBytesWritten.
	c.g.g.returnGrant(1)
}
