// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// KVCPUTimeUtilGoal is the non-burstable utilization goal.
var KVCPUTimeUtilGoal = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"admission.cpu_time_tokens.target_util",
	"the target CPU utilization for work if using the KV CPU time "+
		"token system, value is in the interval [0,1] where 1 means all cores",
	0.75,
	settings.FloatWithMinimum(minTargetUtilFrac))

// Burstable work is given this much CPU headroom above non-burstable. See
// resetInterval for more.
var KVCPUTimeUtilBurstDelta = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"admission.cpu_time_tokens.target_util.burst_delta",
	"the delta between non-burstable & burstable CPU utilization target if "+
		"using the KV CPU time token system (value is in the interval [0,1] "+
		"where 1 means all cores)",
	0.25,
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
// an 8 vCPU machine, we may want to allow burstable work to use 8 seconds
// of CPU time per second. Then the refill rates for burstable work would
// equal 8 seconds per second, and cpuTimeTokenFiller would add 8 seconds of token
// every second, but smoothly -- 1ms at a time. See cpuTimeTokenGranter for details
// on the token buckets owned by cpuTimeTokenGranter; the TLDR is there is one
// bucket per burstQualification.
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
	// queue holds a reference to the WorkQueue. Used to refill per-tenant
	// burst buckets that determine queue priority ordering. See
	// cpu_time_token_burst.go for more.
	queue    workQueueIForAllocator
	settings *cluster.Settings
	model    cpuTimeModel
	metrics  *cpuTimeTokenMetrics

	// refillRates stores the number of CPU time tokens to add to each bucket
	// per interval (1s).
	refillRates rates
	// allocated stores the number of tokens added to each bucket in the current
	// cpuTimeTokenAllocator. No mutex, since only a single goroutine will call
	// the allocator.
	allocated tokenCounts
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
		s.Printf("%s=%s", qual, redact.Safe(time.Duration(r[qual])))
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
// 0.75 for 75% CPU utilization), one per bucket in cpuTimeTokenGranter.
// This is aggregate CPU usage, so 0.75 means 75% of CPU time across all
// cores.
type targetUtilizations [numBurstQualifications]float64

// computeMinimums computes per-bucket minimums from refill rates. These
// minimums prevent higher priority work from putting lower priority buckets
// into unbounded token debt.
//
// The top priority bucket (canBurst) has a floor of 0. Each subsequent
// bucket's floor is its rate minus the top priority rate, which is always
// negative. For example, with refill rates of 100, 75, the minimums
// are 0, -25.
func computeMinimums(r rates) minimums {
	var m minimums
	topRate := r[0]
	for qual := range r {
		m[qual] = r[qual] - topRate
	}
	return m
}

// allocateTokens allocates tokens to a cpuTimeTokenGranter. allocateTokens
// adds the desired number of tokens every interval, while respecting the bucket
// capacities. allocateTokens adds tokens evenly among the expected remaining
// ticks in the interval.
// INVARIANT: remainingTicks >= 1.
func (a *cpuTimeTokenAllocator) allocateTokens(expectedRemainingTicksInInterval int64) {
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

	// a.refillRates must be added every 1s, but allocateTokens is called more than once
	// every 1s (typically). The amount we need to allocate this call to allocateTokens
	// is stored in allocations.
	var allocations tokenCounts
	for qual := range a.refillRates {
		toAllocate := allocateFunc(
			a.refillRates[qual], a.allocated[qual], expectedRemainingTicksInInterval)
		a.allocated[qual] += toAllocate
		allocations[qual] = toAllocate
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
	a.refill(allocations, bucketCapacities, bucketMinimums, false /* updateMetrics */)

	// Refill per-tenant burst buckets in the WorkQueue. We pass the
	// canBurst (= 100% CPU) allocation and rate as the base.
	// refillBurstBuckets scales these per-tenant by burstLimitFrac
	// (= CPU_MIN fraction), so a tenant with CPU_MIN=10% gets a refill
	// rate of 10% of cpuCapacity, breaking even at exactly 10% CPU usage.
	a.queue.refillBurstBuckets(allocations[canBurst], a.refillRates[canBurst])
}

// resetInterval is called to signal the beginning of a new interval.
// allocateTokens adds the desired number of tokens every interval.
func (a *cpuTimeTokenAllocator) resetInterval(ctx context.Context) {
	// Compute target utilizations from cluster settings. The noBurst target is
	// configurable by a cluster setting. The canBurst target adds a delta to the
	// noBurst target, and the delta is also configurable by a cluster setting.
	var targets targetUtilizations
	burstDelta := KVCPUTimeUtilBurstDelta.Get(&a.settings.SV)
	target := KVCPUTimeUtilGoal.Get(&a.settings.SV)
	targets[noBurst] = target
	targets[canBurst] = target + burstDelta

	newRefillRates := a.model.fit(ctx, targets)

	// deltaRefillRates is the difference in tokens to add per interval (1s)
	// from the previous call to fit to this one. We add it immediately to the
	// bucket, which could mean adding tokens, or removing them, depending on
	// what change has been made to refillRates. The idea here is this: The model
	// itself handles smoothing; once a decision has been made by the model, the
	// allocator should immediately execute on the decision.
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
	a.refill(deltaRefillRates, bucketCapacities, bucketMinimums, true /* updateMetrics */)
	a.refillRates = newRefillRates

	// Apply the delta to the per-tenant burst buckets also.
	a.queue.refillBurstBuckets(deltaRefillRates[canBurst], bucketCapacities[canBurst])

	// Reset allocated.
	for qual := range a.allocated {
		a.allocated[qual] = 0
	}
}

// refill increments per-bucket refill metrics, then delegates to
// granter.refill. Positive toAdd values are tracked as tokens added;
// negative values (which occur when refill rates decrease between
// intervals) are tracked as tokens removed.
func (a *cpuTimeTokenAllocator) refill(
	toAdd tokenCounts, bucketCapacities capacities, bucketMinimums minimums, updateMetrics bool,
) {
	for qual := range toAdd {
		idx := int(qual)
		if v := toAdd[qual]; v > 0 {
			a.metrics.RefillAdded[idx].Inc(v)
		} else if v < 0 {
			a.metrics.RefillRemoved[idx].Inc(-v)
		}
	}
	a.granter.refill(toAdd, bucketCapacities, bucketMinimums, updateMetrics)
}

// workQueueIForAllocator abstracts the burst bucket refill method in WorkQueue,
// to enable unit testing.
type workQueueIForAllocator interface {
	refillBurstBuckets(toAdd int64, capacity int64)
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
// 75% and a tokenToCPUTimeMultiplier of 1:
//
//	refillRate = 0.75 * 8E9 tokens/s / 1 = 6E9 tokens/s
//
// which corresponds to 6 CPU-seconds of work admitted per wall-clock second.
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
// the token buckets owned by cpuTimeTokenGranter. fit returns the refill
// rates.
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
		if tokenToCPUTimeMultiplier > 20 {
			tokenToCPUTimeMultiplier = 20
		} else if tokenToCPUTimeMultiplier < 1 {
			tokenToCPUTimeMultiplier = 1
		}
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
