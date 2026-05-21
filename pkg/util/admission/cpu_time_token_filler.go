// Copyright 2025 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// KVCPUTimeUtilGoal is the non-burstable CPU utilization target. The
// InternalKey retains the historical .app_tenant suffix (predating the
// collapse) for storage continuity; the public name is the unsuffixed
// admission.cpu_time_tokens.target_util.
var KVCPUTimeUtilGoal = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"admission.cpu_time_tokens.target_util.app_tenant",
	"the target CPU utilization if using the KV CPU time "+
		"token system, value is in the interval [0,1] where 1 means all cores",
	0.8,
	settings.FloatWithMinimum(minTargetUtilFrac),
	settings.WithName("admission.cpu_time_tokens.target_util"))

// Keep the pre-collapse admission.cpu_time_tokens.target_util.system_tenant
// setting registered as a no-op so external automation that still references
// it does not see a missing-setting error. Its value is ignored;
// KVCPUTimeUtilGoal is the sole target.
var _ = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"admission.cpu_time_tokens.target_util.system_tenant",
	"retired: use admission.cpu_time_tokens.target_util",
	0.95,
	settings.Retired)

// KVCPUTimeUtilBurstDelta is the delta added to the non-burstable CPU
// utilization target to compute the burstable utilization ceiling.
//
// If target + delta exceeds 1.0, the burstable token bucket refill rate
// will exceed the machine's CPU capacity. In practice this means burstable
// work is never throttled by the token bucket (tokens accumulate faster
// than they can be consumed). No other invariant breaks because the token
// bucket simply acts as if it were infinite.
// TODO(wenyihu): Confirm this ^.
var KVCPUTimeUtilBurstDelta = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"admission.cpu_time_tokens.target_util.burst_delta",
	"the delta between non-burstable & burstable CPU utilization target if "+
		"using the KV CPU time token system, expressed in the same units as "+
		"admission.cpu_time_tokens.target_util (value is in the "+
		"interval [0,1] where 1 means all cores)",
	// Why is the default value 0.05 (5%)? It tends to work out because in the
	// worst case there is  5% remaining burst budget and then over time the 85%
	// bucket fills itself to full. For example, say the rates were 80 tokens/s
	// and 85 tokens/s respectively and the usage due to well-behaved tenants was
	// 30 tokens/s. Both buckets would be full, containing 80 and 85 tokens
	// respectively. If the noisy neighbor came and consumed all 80 tokens, the
	// burst bucket still has 5 tokens. From now on, if the noisy neighbor continues
	// to be present, it will fair share with the others and the actual steady
	// state consumption will continue to be 80 tokens/s (across all tenants, since
	// the others are still consuming 30 tokens/s and the noisy neighbor can only
	// consume the remaining 50 tokens/s). Which will keep the smaller bucket at 0
	// tokens, and the other bucket will slowly use the excess tokens to fill up
	// to its full size of 85 tokens.
	0.05,
	settings.PositiveFloat)

const (
	// See the extensive comments near isLowCPUUtil declaration for info
	// regarding this constant.
	lowCPUUtilFrac = 0.25
	// minTargetUtilFrac is the lowest that the admission.cpu_time_tokens.target_util
	// settings can be set to. < 50% CPU utilization is not a cost-effective choice,
	// as it leads lots of hardware resources unused, even in case of short spikes.
	minTargetUtilFrac = lowCPUUtilFrac + 0.25
)

// timePerTick is how frequently cpuTimeTokenFiller ticks its time.Ticker & adds
// tokens to the buckets. Must be < 1s. Must divide 1s evenly.
const timePerTick = 1 * time.Millisecond

// cpuTimeTokenFiller starts a goroutine which periodically calls
// cpuTimeTokenAllocator to add tokens to a cpuTimeTokenGranter. For example, on
// an 8 vCPU machine, we may want to allow burstable work to use 6 seconds
// of CPU time per second. Then the refill rates for burstable work would
// equal 6 seconds per second, and cpuTimeTokenFiller would add 6 seconds of token
// every second, but smoothly -- 1ms at a time. See cpuTimeTokenGranter for details
// on the token buckets owned by cpuTimeTokenGranter; the TLDR is there is one
// bucket per burst qualification.
//
// cpuTimeTokenFiller owns the time.Ticker logic. The details of the token allocation
// are left to the cpuTimeTokenAllocator, in order to improve clarity & testability.
//
// Note that the combination of cpuTimeTokenFiller & cpuTimeTokenAllocator are written
// to be robust against delayed and dropped time.Timer ticks. That
// is, in the presence of delayed and dropped ticks, the correct number of tokens will
// be added to the buckets; they just may be added in a less smooth fashion than
// normal. If ticks are delayed more than roughly 1s, not enough tokens will be
// added to the bucket, but we do not expect this significant of a delay in practice
// (admission control will be running).
//
// See ticker docs, where it is mentioned ticks can be dropped, if receivers are
// slow: https://pkg.go.dev/time#NewTicker
//
// The mechanism by which the goroutine adds the correct number of tokens, in the
// presence of delayed or dropped ticks, is:
//   - time is split into intervals of 1s
//   - intervals are split into 1s / timePerTick(=1ms) time.Ticker ticks
//   - cpuTimeTokenAllocator attempts to allocate remaining tokens for interval evenly
//     across remaining ticks in the interval
//   - once interval is complete, all remaining tokens needed for that interval
//     are added (e.g. see t.allocateTokens(1) below), then a new interval starts
type cpuTimeTokenFiller struct {
	allocator  cpuTimeTokenAllocatorI
	timeSource timeutil.TimeSource
	closeCh    chan struct{}
	// Used only in unit tests.
	tickCh *chan struct{}
}

func (f *cpuTimeTokenFiller) start(ctx context.Context) {
	// The token buckets should start full. The first call to resetInterval will
	// fill the buckets.
	f.allocator.resetInterval(ctx)

	ticker := f.timeSource.NewTicker(timePerTick)
	intervalStart := f.timeSource.Now()
	// Every 1s a new interval starts. every timePerTick time token allocation
	// is done. The expected number of ticks left in the interval is passed to
	// the allocator. The expected number of ticks left can jump around, if
	// time.Timer ticks are delayed or dropped.
	go func() {
		// We start with the assumption that a full interval worth of ticks are
		// remaining. Thus, in the unlikely case where a full 1s passes before
		// the first tick, the below allocateTokens(1) invariant is still
		// respected.
		lastRemainingTicks := int64(time.Second / timePerTick)
		for {
			select {
			case t := <-ticker.Ch():
				var remainingTicks int64
				// Note that time-measuring operations such as t.Sub use monotonic
				// time. Thus, elapsedSinceIntervalStart should always be >= 0.
				// https://pkg.go.dev/time#hdr-Monotonic_Clocks
				elapsedSinceIntervalStart := t.Sub(intervalStart)
				if elapsedSinceIntervalStart >= time.Second {
					// INVARIANT: During each interval, allocateTokens(1) must be
					// called, before resetInterval() can be called. Without this
					// invariant, cpuTimeTokenAllocator.refillRates tokens would not
					// be allocated every 1s.
					//
					// The below conditional ensures the rate provisioned since the
					// last tick was fully emitted, which may not be the case if ticks
					// arrived late. Ideally, this already happened in the else branch
					// during the previous tick which typically would have occurred at
					// millisecond 999 and then would compute remainingTicks <= 1 and
					// would have called allocateTokens(1) (i.e. "emit everything that's
					// left for this second"). But if the previous tick was not the
					// designated "last" tick yet (say it occurred at 900ms), and a delay
					// had occurred before our tick arrived, we need to call
					// allocateTokens(1) here to release the quota held back by the delay.
					if lastRemainingTicks > 1 {
						f.allocator.allocateTokens(1)
					}
					intervalStart = t
					f.allocator.resetInterval(ctx)
					remainingTicks = int64(time.Second / timePerTick)
				} else {
					remainingSinceIntervalStart := time.Second - elapsedSinceIntervalStart
					if remainingSinceIntervalStart <= 0 {
						panic(errors.AssertionFailedf("remainingSinceIntervalStart %d is <= 0", remainingSinceIntervalStart))
					}
					// ceil(a / b) == (a + b - 1) / b, when using integer division.
					// Round up so that we don't accumulate tokens to give in a burst on
					// the last tick.
					remainingTicks =
						int64((remainingSinceIntervalStart + timePerTick - 1) / timePerTick)
				}
				f.allocator.allocateTokens(max(1, remainingTicks))
				lastRemainingTicks = remainingTicks
				// Only non-nil in unit tests.
				if f.tickCh != nil {
					*f.tickCh <- struct{}{}
				}
			case <-f.closeCh:
				return
			}
		}
	}()
}

func (f *cpuTimeTokenFiller) close() {
	close(f.closeCh)
}

// cpuTimeTokenAllocatorI abstracts cpuTimeTokenAllocator for testing.
type cpuTimeTokenAllocatorI interface {
	allocateTokens(expectedRemainingTicksInInterval int64)
	resetInterval(context.Context)
}

var _ cpuTimeTokenAllocatorI = &cpuTimeTokenAllocator{}

// cpuTimeTokenAllocator allocates tokens to a cpuTimeTokenGranter. See the
// comment above cpuTimeTokenFiller for a high level picture. The
// responsibility of cpuTimeTokenAllocator is to gradually allocate tokens
// every interval, while respecting the bucket capacities. The computation
// of the rate of tokens to add every interval is left to cpuTimeModel.
type cpuTimeTokenAllocator struct {
	granter *cpuTimeTokenGranter
	// queue holds a reference to the WorkQueue. Used to refill per-group
	// burst buckets that determine queue priority ordering. See
	// cpu_time_token_burst.go for more.
	queue        workQueueIForAllocator
	settings     *cluster.Settings
	configHolder *ResourceGroupConfigHolder
	model        cpuTimeModel
	metrics      *cpuTimeTokenMetrics

	// refillRates stores the number of CPU time tokens to add to each bucket
	// per interval (1s).
	refillRates rates
	// snap is the ConfigSnapshot captured at the most recent
	// resetInterval. Every operation in the interval reads from this
	// snap rather than re-querying the holder, so per-tick work stays
	// coherent with the configuration that produced refillRates.
	snap ConfigSnapshot
	// allocated stores the number of tokens added to each bucket in the current
	// cpuTimeTokenAllocator. No mutex, since only a single goroutine will call
	// the allocator.
	allocated tokenCounts

	// nowMono returns a monotonic timestamp. Defaults to crtime.NowMono;
	// tests substitute a manual clock to make DampeningDeficitNanos
	// deterministic. Only called by the filler goroutine.
	nowMono func() crtime.Mono
	// dampeningFactor scales per-tick token allocations to leave CPU
	// headroom when the goroutine scheduler is overloaded. 1.0 means no
	// dampening; dampeningFloor means fully dampened. Written only by the
	// filler goroutine via allocateTokens.
	dampeningFactor float64
	// lastDampeningUpdate is the monotonic timestamp of the previous
	// allocateTokens call. Used to weight DampeningDeficitNanos by elapsed
	// time, so the counter reflects the true cumulative deficit even when
	// the filler ticker delays or drops ticks (see cpuTimeTokenFiller).
	// Zero until the first allocateTokens call. Only read/written by the
	// filler goroutine.
	lastDampeningUpdate crtime.Mono

	// Written by CPULoad callback (arbitrary goroutine), read by
	// filler goroutine on the next tick.
	lastRunnable atomic.Int64
	lastProcs    atomic.Int64
}

// computeTargets derives target utilizations from a ConfigSnapshot.
func computeTargets(snap ConfigSnapshot) targetUtilizations {
	return targetUtilizations{
		canBurst: snap.MaxFraction(),
		noBurst:  snap.MaxNonBurstableFraction(),
	}
}

// groupBurstRates returns (rate100, cap100) from which per-group
// burst amounts are derived. The allocator calls
// refillGroupBurstBuckets with these values.
//
// The 100%-CPU rate is recovered by dividing the canBurst allocation
// (or refill rate) by canBurstTarget, which is valid because
// cpuTimeTokenLinearModel is linear in target. snap must be the same
// snapshot used to compute allocations/refillRates this tick;
// otherwise the recovered rate100 can drift if cluster settings
// change between calls.
func (a *cpuTimeTokenAllocator) groupBurstRates(
	snap ConfigSnapshot, allocations tokenCounts, refillRates rates,
) (rate100, cap100 float64) {
	canBurstTarget := snap.MaxFraction()
	if canBurstTarget == 0 {
		return 0, 0
	}
	return float64(allocations[canBurst]) / canBurstTarget,
		float64(refillRates[canBurst]) / canBurstTarget
}

// rates stores a token count per second, for example, the refill
// rates at which we add tokens per second, one per bucket in
// cpuTimeTokenGranter.
type rates [numBurstQualifications]int64

func (r rates) String() string {
	return redact.StringWithoutMarkers(r)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (r rates) SafeFormat(s redact.SafePrinter, _ rune) {
	s.SafeRune('[')
	first := true
	for qual := burstQualification(0); qual < numBurstQualifications; qual++ {
		if !first {
			s.SafeRune(' ')
		}
		first = false
		s.Printf("%s=%s",
			qual, redact.Safe(time.Duration(r[qual])))
	}
	s.SafeRune(']')
}

// capacities stores the maximum number of tokens that can be in the
// buckets, one per bucket in cpuTimeTokenGranter.
type capacities [numBurstQualifications]int64

// minimums stores the minimum number of tokens that can be in the
// buckets, one per bucket in cpuTimeTokenGranter.
type minimums [numBurstQualifications]int64

// tokenCounts stores unit-less token counts, one per bucket in
// cpuTimeTokenGranter.
type tokenCounts [numBurstQualifications]int64

// targetUtilizations stores a target CPU utilization, as a float64 (so
// 0.8 for 80% CPU utilization), one per bucket in CPUTimeTokenGranter. This
// is aggregate CPU usage, so 0.8 means 80% of CPU time across all cores.
type targetUtilizations [numBurstQualifications]float64

// computeMinimums computes per-bucket minimums from refill rates. These
// minimums prevent higher priority work from putting lower priority buckets
// into unbounded token debt.
//
// The top priority bucket (canBurst) has a floor of 0. The noBurst
// bucket's floor is its rate minus the canBurst rate, which is always
// negative. For example, with refill rates of 85 and 80, the minimums
// are 0 and -5.
//
// Any choice of minimums must respect the invariant in cpuTimeTokenGranter
// that the canBurst bucket always has more tokens than the noBurst bucket
// (see cpuTimeTokenGranter.mu.buckets). The approach here satisfies this
// because the minimums are derived from the refill rates, which are
// themselves ordered by priority.
//
// One concrete benefit: during overload, when all buckets are at their
// minimums, the canBurst bucket recovers to positive first, which
// means burstable work is naturally prioritized over non-burstable work.
func computeMinimums(r rates) minimums {
	var m minimums
	topRate := r[canBurst]
	for qual := range r {
		m[qual] = r[qual] - topRate
	}
	return m
}

// CPULoad stores the latest runnable goroutine count and GOMAXPROCS
// for use by allocateTokens on the next tick. Called from an arbitrary
// goroutine by the goschedstats callback.
func (a *cpuTimeTokenAllocator) CPULoad(runnable, procs int, _ time.Duration) {
	a.lastRunnable.Store(int64(runnable))
	a.lastProcs.Store(int64(procs))
}

// Dampening descends slowly under overload and recovers faster once
// load eases. At 1ms ticks, descent from 1.0 to dampeningFloor takes
// ~50ms; recovery takes ~25ms.
const (
	dampeningDescentDelta  = 0.01
	dampeningRecoveryDelta = 0.02
	// dampeningFloor caps how far the factor can drop. Even under
	// sustained overload we still dispense at least this fraction so
	// the system can keep making progress and self-correct.
	dampeningFloor = 0.50
	// dampeningSnapBackFrac is the fraction of the overload threshold
	// below which the factor jumps directly to 1.0. With the default
	// threshold of 32 runnable goroutines per CPU this fires when
	// runnable falls below 4 per CPU.
	dampeningSnapBackFrac = 0.125
	// dampeningHoldFrac sets the lower edge of a hold zone immediately
	// below the overload threshold. While runnable is in [hold, overload)
	// the factor stays where it is, which prevents ping-pong recovery
	// the moment load dips just below the overload threshold. Recovery
	// only resumes once runnable drops further. With the default
	// threshold of 32 runnable goroutines per CPU this means we hold
	// in [24, 32) per CPU.
	dampeningHoldFrac = 0.75
)

// updateDampening advances the dampening factor for one tick based on the most
// recent CPULoad sample and records the resulting deficit on
// DampeningDeficitNanos. Called by allocateTokens.
//
//   - Runnable goroutines at or above the overload threshold: increase dampening
//     by 1% per tick, down to a floor of 50% (dampeningFactor = 0.5).
//
//   - Runnable goroutines in the hold zone just below the overload threshold
//     (>= 75% of threshold by default): leave the factor unchanged so we don't
//     bounce in and out of recovery while load hovers near the overload point.
//
//   - Runnable goroutines clearly below the overload threshold: decrease
//     dampening by 2% per tick.
//
//   - Runnable goroutines below 12.5% of the overload threshold: snap dampening
//     all the way back to 0 (dampeningFactor = 1) without ramping.
//
// The deficit (1 - factor) is added to the counter scaled by the wall-clock gap
// since the previous call. The gap is nominally timePerTick (1ms) but can be
// larger when the filler ticker is delayed or drops ticks. The first call is
// skipped so we don't attribute the process-start gap to dampening.
func (a *cpuTimeTokenAllocator) updateDampening() {
	procs := int(a.lastProcs.Load())
	if procs <= 0 {
		// CPULoad has not been called yet. Leave the factor
		// at its initial value.
		return
	}
	runnable := int(a.lastRunnable.Load())
	threshold := int(KVSlotAdjusterOverloadThreshold.Get(&a.settings.SV))
	snapPerCPU := max(1, int(float64(threshold)*dampeningSnapBackFrac))
	holdPerCPU := max(1, int(float64(threshold)*dampeningHoldFrac))

	switch {
	case runnable >= threshold*procs:
		a.dampeningFactor = max(dampeningFloor, a.dampeningFactor-dampeningDescentDelta)
	case runnable >= holdPerCPU*procs:
		// Hold zone: leave the factor where it is.
	case runnable < snapPerCPU*procs:
		a.dampeningFactor = 1.0
	default:
		a.dampeningFactor = min(1.0, a.dampeningFactor+dampeningRecoveryDelta)
	}

	now := a.nowMono()
	if a.lastDampeningUpdate != 0 {
		if elapsed := now.Sub(a.lastDampeningUpdate); elapsed > 0 {
			a.metrics.DampeningDeficitNanos.Inc(
				int64((1.0 - a.dampeningFactor) * float64(elapsed.Nanoseconds())))
		}
	}
	a.lastDampeningUpdate = now
}

// allocateTokensFn distributes refillRates across remaining ticks in
// the interval, returning the per-tick allocations. Extracted as a
// standalone function so it can be reused by future strategy
// implementations (e.g. resource manager mode).
func allocateTokensFn(refillRates rates, allocated *tokenCounts, remainingTicks int64) tokenCounts {
	allocateFunc := func(total int64, allocated int64, remainingTicks int64) (toAllocate int64) {
		remainingTokens := total - allocated
		// ceil(a / b) == (a + b - 1) / b, when using integer division.
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

	// refillRates must be added every 1s, but allocateTokens is called more
	// than once every 1s (typically). The amount we need to allocate this call
	// to allocateTokens is stored in allocations.
	var allocations tokenCounts
	for qual := range refillRates {
		toAllocate := allocateFunc(
			refillRates[qual], allocated[qual], remainingTicks)
		allocated[qual] += toAllocate
		allocations[qual] = toAllocate
	}
	return allocations
}

// allocateTokens allocates tokens to a cpuTimeTokenGranter. allocateTokens
// adds the desired number of tokens every interval, while respecting the bucket
// capacities. allocateTokens adds tokens evenly among the expected remaining
// ticks in the interval.
// INVARIANT: remainingTicks >= 1.
// TODO(josh): Expand to cover group-specific token buckets too.
func (a *cpuTimeTokenAllocator) allocateTokens(expectedRemainingTicksInInterval int64) {
	a.updateDampening()

	allocations := allocateTokensFn(a.refillRates, &a.allocated, expectedRemainingTicksInInterval)

	// Save undampened allocations for per-group burst bucket refill.
	// Per-group burst buckets control priority ordering (whether a group
	// qualifies for canBurst), not total throughput, so dampening — a
	// total-throughput control — does not apply.
	undampenedAllocations := allocations
	for qual := range allocations {
		allocations[qual] = int64(float64(allocations[qual]) * a.dampeningFactor)
	}

	// Each bucket has a max capacity. The max capacity for each bucket is
	// one second worth of tokens at the current refill rate. This is a fairly
	// arbitrary decision.
	bucketCapacities := capacities(a.refillRates)
	// Each bucket has a minimum allowed token count. This prevents
	// higher priority work from putting lower priority buckets into
	// unbounded token debt.
	bucketMinimums := computeMinimums(a.refillRates)
	// Metrics don't need higher fidelity than one update per second. For example,
	// in CC, we scrape metrics once every 10s.
	refillGranter(a.granter, a.metrics, allocations,
		bucketCapacities, bucketMinimums, false /* updateMetrics */)

	// Refill the per-group burst buckets in the WorkQueue. groupBurstRates
	// returns the 100%-CPU rate; refillGroupBurstBuckets scales it by each
	// group's burstFrac. If a group's bucket is mostly full, we allow it
	// to get priority in the queue (see cpu_time_token_burst.go). With
	// defaultTenantGroupConfig.BurstFrac = 0.20, an application tenant can
	// burst if it is using roughly less than 20% of the CPU on a CRDB node.
	//
	// Per-group burst buckets control priority ordering (whether a group
	// qualifies for canBurst), not total throughput, so dampening — a
	// total-throughput control — does not apply. Pass the undampened
	// allocations through.
	rate, capacity := a.groupBurstRates(a.snap, undampenedAllocations, a.refillRates)
	a.queue.refillGroupBurstBuckets(rate, capacity)
}

// resetInterval recomputes refill rates and applies the delta to the
// granter and burst buckets.
func (a *cpuTimeTokenAllocator) resetInterval(ctx context.Context) {
	// Refresh the cached snapshot for this interval. Everything that
	// runs until the next resetInterval — model.fit, the refillRates
	// it produces, and the per-tick groupBurstRates calls in
	// allocateTokens — reads from this snap so the interval is
	// coherent with one view of the holder.
	a.snap = a.configHolder.Snapshot()
	targets := computeTargets(a.snap)
	newRefillRates := a.model.fit(ctx, targets)

	// deltaRefillRates is the difference in tokens to add per interval (1s)
	// from the previous call to fit to this one. We add it immediately to the
	// bucket, which could mean adding tokens, or removing them, depending on
	// what change has been made to refillRates. The idea here is this: The model
	// itself handles smoothing; once a decision has been made by the model, the
	// allocator should immediately execute on the decision.
	// TODO(josh): This is missing logic to prevent token counts from becoming
	// negative. Also, the above comment needs to be beefed up.
	// https://github.com/cockroachdb/cockroach/issues/158539
	// TODO(wenyihu6): we should do something here for per group burst bucket as
	// well
	var deltaRefillRates tokenCounts
	for qual := range newRefillRates {
		deltaRefillRates[qual] = newRefillRates[qual] - a.refillRates[qual]
	}
	// See comment above the call to refill in allocateTokens for a discussion of
	// bucketCapacities and bucketMinimums.
	bucketCapacities := capacities(newRefillRates)
	bucketMinimums := computeMinimums(newRefillRates)
	// Metrics don't need higher fidelity than one update per second. For example,
	// in CC, we scrape metrics once every 10s.
	refillGranter(a.granter, a.metrics, deltaRefillRates,
		bucketCapacities, bucketMinimums, true /* updateMetrics */)
	a.refillRates = newRefillRates
	// Apply the delta to the per-group burst buckets also.
	rate, capacity := a.groupBurstRates(a.snap, deltaRefillRates, a.refillRates)
	a.queue.refillGroupBurstBuckets(rate, capacity)

	// Reset allocated.
	for qual := range a.allocated {
		a.allocated[qual] = 0
	}
}

// refillGranter increments per-bucket refill metrics, then delegates
// to granter.refill. Positive toAdd values are tracked as tokens
// added; negative values (which occur when refill rates decrease
// between intervals) are tracked as tokens removed.
func refillGranter(
	granter *cpuTimeTokenGranter,
	metrics *cpuTimeTokenMetrics,
	toAdd tokenCounts,
	bucketCapacities capacities,
	bucketMinimums minimums,
	updateMetrics bool,
) {
	for qual := range toAdd {
		if v := toAdd[qual]; v > 0 {
			metrics.RefillAdded[qual].Inc(v)
		} else if v < 0 {
			metrics.RefillRemoved[qual].Inc(-v)
		}
	}
	granter.refill(toAdd, bucketCapacities, bucketMinimums, updateMetrics)
}

// workQueueIForAllocator abstracts the WorkQueue methods called from
// the allocator/strategy layer, to enable unit testing.
type workQueueIForAllocator interface {
	// refillGroupBurstBuckets refills every group's burst bucket,
	// scaling rate and capacity by each group's burstFrac.
	refillGroupBurstBuckets(rate, capacity float64)
}

// cpuTimeModel abstracts cpuTimeLinearModel for testing.
type cpuTimeModel interface {
	fit(context.Context, targetUtilizations) rates
}

var _ cpuTimeModel = &cpuTimeTokenLinearModel{}

// cpuTimeTokenLinearModel computes the number of CPU time tokens to add
// to each bucket in the cpuTimeTokenGranter, per interval (per 1s).
//
// The refill rate is chosen such that the rate at which tokens are added
// results in an (actual measured) CPU utilization matching the target
// utilization. Tokens represent CPU work carried out by requests which acquired
// from the bucket, and the actual CPU time used by the requests is consumed
// from the bucket. However, requests can use additional CPU time that isn't
// reflected in what's consumed - for example, the CPU work incurred by heap
// allocations, which need to be garbage collected by the runtime at a
// near-future point in time, or more generally any other asynchronous work
// triggered by the request which may outlive it. Additionally, not all work
// in the system is visible to the bucket: work by the Go runtime is a basic
// example, but even "userspace work" is likely not tracked in its entirety.
//
// We address both of these issues by assuming an approximately constant ratio
// between the rate of total and tracked CPU time (at least over short periods
// of time) and then "punishing" tracked work by that factor, in effect assuming
// that any "untracked" CPU work is incurred by the tracked work. This motivates
// the tokenToCPUTimeMultiplier below, which is computed via
//
//	tokenToCPUTimeMultiplier = totalCPUTime / trackedCPUTime (over a short interval)
//
// Observing, for example, 20s of CPU time consumed in the process but only 10s
// in tracked requests, we would set tokenToCPUTimeMultiplier to 2 (dimensionless),
// and the refill rate would be halved (which corresponds to saying that a request
// that consumes, say, 100ms of CPU time should really be billed for twice that
// amount).
//
// Since 1 token represents 1 nanosecond of CPU time, we express CPU capacity in
// tokens/s (i.e., CPU-nanoseconds per wall-clock second). For example, an 8 vCPU
// machine has a capacity of 8E9 tokens/s. The refill rate is then simply:
//
//	refillRate [tokens/s] = targetUtilization * capacity [tokens/s] / tokenToCPUTimeMultiplier
//
// For an 8 vCPU machine (capacity = 8E9 tokens/s), with a target utilization of
// 80% and a tokenToCPUTimeMultiplier of 1:
//
//	refillRate = 0.8 * 8E9 tokens/s / 1 = 6.4E9 tokens/s
//
// which corresponds to 6.4 CPU-seconds of work admitted per wall-clock second.
//
// We clamp tokenToCPUTimeMultiplier to be in the interval [1, 20]. The lower
// bound 1 reflects our knowledge that whatever is measured by tracked requests
// was actually consumed (i.e. consumed tokens represent at least the
// corresponding amount of CPU time). As the multiplier increases, it is less and
// less likely that the tracked requests are actually to blame for the high
// utilization, but we continue to pretend that we are, to shift queuing into
// admission control rather than the Go scheduler (where we have little
// control). In highly degraded situations (multiplier >= 20), we cap the
// multiplier at 20 to avoid penalizing tracked requests further. See fit() for
// more details.
//
// As is discussed in the cpuTimeTokenGranter docs, the buckets are arranged in
// a priority hierarchy. Higher priority buckets have higher target utilizations
// than lower priority buckets, and incoming requests generally require that the
// bucket for their priority has enough tokens to accommodate the request, but then
// withdraw from all buckets (which may put lower-priority buckets in a deficit).
// Due to this, higher priority buckets have more tokens added per second than
// lower priority buckets.
type cpuTimeTokenLinearModel struct {
	granter            tokenUsageTracker
	cpuMetricsProvider CPUMetricsProvider
	timeSource         timeutil.TimeSource
	metrics            *cpuTimeTokenMetrics

	// True after first call to fit.
	init bool
	// The time that fit was called last.
	lastFitTime time.Time
	// The cumulative user/sys CPU time used since process start.
	totalCPUTime time.Duration
	// The linear correction term, see the docs above cpuTimeTokenLinearModel.
	tokenToCPUTimeMultiplier float64

	logger fitLogger
}

// tokenUsageTracker is implemented by cpuTimeTokenGranter. It provides
// information regarding the net token deduction since the last call to
// resetTokensUsedInInterval. This information is needed to model the
// relationship between token usage and actual CPU usage.
type tokenUsageTracker interface {
	// resetTokensUsedInInterval resets the tracked used tokens to zero.
	// The previous value is returned.
	resetTokensUsedInInterval() int64
}

var _ tokenUsageTracker = &cpuTimeTokenGranter{}

type CPUMetricsProvider interface {
	// GetCPUUsage returns the cumulative user/sys CPU time used since process
	// start.
	GetCPUUsage() (totalCPUTime time.Duration, err error)
	// GetCPUCapacity returns the cpuCapacity measured in vCPUs.
	GetCPUCapacity() (cpuCapacity float64)
}

// fit adjusts tokenToCPUTimeMultiplier based on CPU usage & token usage.
// fit computes refill rates from tokenToCPUTimeMultiplier and the targets
// parameter. targets tracks a target CPU utilization for all buckets in
// the multi-dimensional token buckets owned by cpuTimeTokenGranter. fit
// returns the refill rates.
func (m *cpuTimeTokenLinearModel) fit(ctx context.Context, targets targetUtilizations) rates {
	if !m.init {
		m.init = true
		m.lastFitTime = m.timeSource.Now()
		totalCPUTime, err := m.cpuMetricsProvider.GetCPUUsage()
		if err != nil {
			// We do not expect the syscall that fetches CPU usage to ever fail.
			log.Dev.Fatalf(ctx, "GetCPUUsage returned %q in cpuTimeTokenLinearModel.fit init", err)
		}
		m.totalCPUTime = totalCPUTime
		m.tokenToCPUTimeMultiplier = 1
		return m.computeRefillRates(targets, m.tokenToCPUTimeMultiplier, m.cpuMetricsProvider.GetCPUCapacity())
	}

	cpuCapacity := m.cpuMetricsProvider.GetCPUCapacity()
	totalCPUTime, err := m.cpuMetricsProvider.GetCPUUsage()
	if err != nil {
		// We do not expect the syscall that fetches CPU usage to ever fail.
		log.Dev.Fatalf(ctx, "GetCPUUsage returned %q in cpuTimeTokenLinearModel.fit", err)
	}

	intCPUTime := totalCPUTime - m.totalCPUTime
	// totalCPUTime is not necessarily monotonic in all environments,
	// e.g. in case of VM live migration on a public cloud provider. In this
	// case, we set intCPUTime to 0, so that the computation of
	// tokenToCPUTimeMultiplier is well-behaved.
	if intCPUTime < 0 {
		intCPUTime = 0
	}
	m.totalCPUTime = totalCPUTime

	now := m.timeSource.Now()
	elapsedSinceLastFit := now.Sub(m.lastFitTime)
	m.lastFitTime = now

	// Get used CPU tokens.
	// TODO(josh): Get tokens used by the elastic CPU AC in addition to
	// the normal CPU AC.
	tokensUsed := m.granter.resetTokensUsedInInterval()
	// At admission time, an estimate of CPU time is deducted. After
	// the request is done processing, a correction based on a measurement
	// from grunning is deducted. Thus it is theoretically possible for net
	// tokens used to be <=0. In this case, we set tokensUsed to 1, so that
	// the computation of tokenToCPUTimeMultiplier is well-behaved.
	if tokensUsed <= 0 {
		tokensUsed = 1
	}

	// Update multiplier.
	isLowCPUUtil := int64(intCPUTime) < int64(float64(elapsedSinceLastFit.Nanoseconds())*cpuCapacity*lowCPUUtilFrac)
	if isLowCPUUtil {
		// With good integration with admission control, most foreground
		// work will be tracked by AC and reflected in the model, and the
		// unaccounted CPU utilization in the system is likely to a large degree
		// directly induced by foreground work (heap GC, etc).
		//
		// As a result, in that situation, we expect a tokenToCPUTimeMultiplier
		// < 2 (in experiments, we have seen values as high as 3).
		//
		// Re-fitting the model at low utilization is generally problematic
		// because smaller sample sizes and inaccuracies can dominate and
		// result in noisy measurements. So we want to leave the multiplier
		// (which was computed at a higher utilization, and is hopefully
		// still meaningful) unchanged until CPU utilization picks up again.
		//
		// The exception to this is when the multiplier is actually the cause
		// of the low utilization. Consider the following scenario:
		//
		// - t=0: we re-fit and compute a multiplier of 10. Unbeknownst to the
		//   model, there was a one-off untracked request that consumed a large
		//   amount of CPU time during this interval; regular workload is unchanged
		//   in this example and would have resulted in a true multiplier of 1.
		// - t=[0,1s]: the large multiplier throttles tracked work (which is all
		//   work in this example) and CPU utilization drops to 10%.
		// - t=1s: the model re-fits and enters this branch. If it skips adjusting
		//   the multiplier (for the reasons outlined above), the workload remains
		//   throttled indefinitely.
		//
		// We address this scenario as follows:
		// - for simplicity, assume a 1 vCPU machine, so targetUtil represents the
		//   fraction of 1 second of CPU time we're willing to spend per wall-second.
		// - assume our high multiplier is M (10 for example)
		// - assume there is a "true" multiplier T (2 for example), i.e. a reported
		//   token causes T nanoseconds of CPU time to be consumed.
		// - M emits tokens at rate R := targetUtil/M tokens/second.
		// - if this rate were consumed by the workload in entirety, this would
		//   result in a CPU utilization of T*R = T*targetUtil/M.
		// - solving for T*R < lowCPUUtilFrac, we get
		//     M >  T*targetUtil/lowCPUUtilFrac
		//       >= targetUtil/lowCPUUtilFrac       (because T>=1)
		// - so whenever M > targetUtil/lowCPUUtilFrac, it could possibly be true
		//   that the multiplier is the cause of the low utilization. So we adjust
		//   the multiplier down by 50%, so that, possibly over multiple fitting
		//   intervals, it will drop below the threshold at which it could be
		//   responsible for the low utilization. (We smear this process over
		//   multiple intervals because the multiplier may also have been correct
		//   and load might pick up again soon.)
		//
		// Note that there are multiple target utilizations, for different buckets
		// in cpuTimeTokenGranter. We use the smallest one. This is in some sense
		// the most conservative choice, since it leads to the lowest value for the
		// right side of:
		//  M > targetUtil/lowCPUUtilFrac
		// Again, in the case of low CPU, we would rather give out too many tokens
		// than not enough.
		smallestTargetUtil := math.MaxFloat64
		for qual := range targets {
			if targets[qual] < smallestTargetUtil {
				smallestTargetUtil = targets[qual]
			}
		}
		upperBound := smallestTargetUtil / lowCPUUtilFrac
		if mult := m.tokenToCPUTimeMultiplier; mult > upperBound {
			m.tokenToCPUTimeMultiplier = max(mult/1.5, upperBound)
		}
	} else {
		tokenToCPUTimeMultiplier :=
			float64(intCPUTime) / float64(tokensUsed)
		// We clamp tokenToCPUTimeMultiplier to be in the interval [1, 20]. The
		// lower bound 1 reflects our knowledge that whatever is measured by
		// tracked requests was actually consumed (i.e. consumed tokens represent
		// at least the corresponding amount of CPU time). As the multiplier
		// increases, it is less and less likely that the tracked requests are
		// actually to blame for the high utilization, but we continue to pretend
		// that we are, to shift queuing into admission control rather than the Go
		// scheduler (where we have little control). In highly degraded situations
		// (multiplier >= 20), we cap the multiplier at 20 to avoid penalizing
		// tracked requests further. See fit() for more details.
		if tokenToCPUTimeMultiplier > 20 {
			tokenToCPUTimeMultiplier = 20
		} else if tokenToCPUTimeMultiplier < 1 {
			tokenToCPUTimeMultiplier = 1
		}
		// The model responds quicker to downward changes in tokenToCPUTimeMultiplier
		// than upward changes to tokenToCPUTimeMultiplier, since alpha is large
		// in the case of the former. Downward changes to tokenToCPUTimeMultiplier
		// imply increasing the number of tokens that are given out per second.
		// Upward changes to tokenToCPUTimeMultiplier imply decreasing the number
		// of tokens. This implies the model responds faster to under-admission
		// than over-admission, which should, slightly, decrease the risk of
		// under-admission and increase the risk of over-admission. This is
		// sensible, since in case of over-admission, there is a limit to how
		// performant CRDB can be (in a latency sense); CPU is constrained after
		// all. OTOH, we do not want under-admission to happen in case of
		// temporary model error, as that is avoidable.
		//
		// We can make this more concrete with an example. Say for some reason the
		// model estimates the multiplier to be 20 and it should have been 2. With
		// a 0.5 alpha, the model would get 11, then 6.5, 4.25, 3.125, so we have
		// multiple seconds of under-admission. With 0.8 alpha, it's 5.6, 2.72,
		// 2.144. So much faster. Note that the specific alphas we have here were
		// chosen somewhat arbitrarily.
		alpha := 0.5
		if tokenToCPUTimeMultiplier < m.tokenToCPUTimeMultiplier {
			alpha = 0.8
		}

		// Exponentially smooth changes to the multiplier. 1s of data is noisy,
		// so smoothing is necessary.
		m.tokenToCPUTimeMultiplier =
			alpha*tokenToCPUTimeMultiplier + (1-alpha)*m.tokenToCPUTimeMultiplier
	}

	refillRates := m.computeRefillRates(targets, m.tokenToCPUTimeMultiplier, cpuCapacity)

	m.metrics.Multiplier.Update(m.tokenToCPUTimeMultiplier)
	if msg, shouldLog := m.logger.accumulate(
		m.tokenToCPUTimeMultiplier, isLowCPUUtil,
		intCPUTime, tokensUsed,
		elapsedSinceLastFit, cpuCapacity,
	); shouldLog {
		log.Dev.Infof(ctx, "%s", msg)
	}

	return refillRates
}

// computeRefillRates is a pure helper function that computes refill rates.
// The CPU capacity is measured in vCPUs. This takes into account the cgroup, so
// can be fractional.
func (*cpuTimeTokenLinearModel) computeRefillRates(
	targets targetUtilizations, tokenToCPUTimeMultiplier float64, cpuCapacity float64,
) rates {
	var refillRates rates
	for qual := range targets {
		refillRates[qual] = int64(cpuCapacity * float64(time.Second) * targets[qual] / tokenToCPUTimeMultiplier)
	}
	return refillRates
}
