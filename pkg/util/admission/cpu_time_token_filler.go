// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var KVCPUTimeUtilGoal = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"admission.cpu_time_tokens.target_util",
	"the target CPU utilization for the KV CPU time token system", 0.8)

// timePerTick is how frequently cpuTimeTokenFiller ticks its time.Ticker & adds
// tokens to the buckets. Must be < 1s. Must divide 1s evenly.
const timePerTick = 1 * time.Millisecond

// cpuTimeTokenFiller starts a goroutine which periodically calls
// cpuTimeTokenAllocator to add tokens to a cpuTimeTokenGranter. For example, on
// an 8 vCPU machine, we may want to allow burstable tier-0 work to use 6 seconds
// of CPU time per second. Then the refill rates for tier0 burstable work would
// equal 6 seconds per second, and cpuTimeTokenFiller would add 6 seconds of token
// every second, but smoothly -- 1ms at a time. See cpuTimeTokenGranter for details
// on the multi-dimensional token buckets owned by cpuTimeTokenGranter; the TLDR is
// there is one bucket per <resource tier, burst qualification> pair.
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
	allocator  tokenAllocator
	timeSource timeutil.TimeSource
	closeCh    chan struct{}
	// Used only in unit tests.
	tickCh *chan struct{}
}

func (f *cpuTimeTokenFiller) start() {
	// The token buckets starts full.
	f.allocator.init()
	f.allocator.allocateTokens(1)
	f.allocator.resetInterval(true /* skipFittingLinearModel */)

	ticker := f.timeSource.NewTicker(timePerTick)
	intervalStart := f.timeSource.Now()
	// Every 1s a new interval starts. every timePerTick time token allocation
	// is done. The expected number of ticks left in the interval is passed to
	// the allocator. The expected number of ticks left can jump around, if
	// time.Timer ticks are delayed or dropped.
	go func() {
		lastRemainingTicks := int64(0)
		for {
			select {
			case t := <-ticker.Ch():
				var remainingTicks int64
				elapsedSinceIntervalStart := t.Sub(intervalStart)
				if elapsedSinceIntervalStart >= time.Second {
					// INVARIANT: During each interval, allocateTokens(1) must be called, before
					// resetInterval() can be called.
					//
					// Without this invariant, cpuTimeTokenAllocator.rates tokens would not be
					// allocated every 1s.
					if lastRemainingTicks > 1 {
						f.allocator.allocateTokens(1)
					}
					intervalStart = t
					f.allocator.resetInterval(false /* skipFittingLinearModel */)
					remainingTicks = int64(time.Second / timePerTick)
				} else {
					remainingSinceIntervalStart := time.Second - elapsedSinceIntervalStart
					if remainingSinceIntervalStart < 0 {
						panic(errors.AssertionFailedf("remainingSinceIntervalStart is negative %d", remainingSinceIntervalStart))
					}
					// ceil(a / b) == (a + b - 1) / b, when using integer division.
					// Round up so that we don't accumulate tokens to give in a burst on the
					// last tick.
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

// tokenAllocator abstracts cpuTimeTokenAllocator for testing.
type tokenAllocator interface {
	init()
	allocateTokens(remainingTicksInInInterval int64)
	resetInterval(skipFittingLinearModel bool)
}

// cpuTimeTokenAllocator allocates tokens to a cpuTimeTokenGranter. See the comment
// above cpuTimeTokenFiller for a high level picture. The responsibility of
// cpuTimeTokenAllocator is to gradually allocate tokens every interval,
// while respecting bucketCapacity. The computation of the rate of tokens to add
// every interval is left to cpuTimeTokenLinearModel.
type cpuTimeTokenAllocator struct {
	granter *cpuTimeTokenGranter
	model   cpuTimeModel

	// allocated stores the number of tokens added to each bucket in the current
	// cpuTimeTokenAllocator. No mutex, since only a single goroutine will call
	// the interval.
	allocated [numResourceTiers][numBurstQualifications]int64
}

var _ tokenAllocator = &cpuTimeTokenAllocator{}

func (a *cpuTimeTokenAllocator) init() {
	a.model.init()
}

// allocateTokens allocates tokens to a cpuTimeTokenGranter. allocateTokens
// adds the desired number of tokens every interval, while respecting bucketCapacity.
// allocateTokens adds tokens evenly among the expected remaining ticks in
// the interval.
// INVARIANT: remainingTicks >= 1.
// TODO(josh): Expand to cover tenant-specific token buckets too.
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

	rates := a.model.getRefillRates()
	var delta [numResourceTiers][numBurstQualifications]int64
	for wc := range rates {
		for kind := range rates[wc] {
			toAllocateTokens := allocateFunc(
				rates[wc][kind], a.allocated[wc][kind], expectedRemainingTicksInInterval)
			a.allocated[wc][kind] += toAllocateTokens
			delta[wc][kind] = toAllocateTokens
		}
	}
	a.granter.refill(delta, rates)
}

// resetInterval is called to signal the beginning of a new interval. allocateTokens
// adds the desired number of tokens every interval.
func (a *cpuTimeTokenAllocator) resetInterval(skipFittingLinearModel bool) {
	if !skipFittingLinearModel {
		// delta is the difference in tokens to add per interval (1s) from previous
		// call to fit to this one. We add it immediately to the bucket. The model itself
		// handles filtering.
		delta := a.model.fit()
		a.granter.refill(delta, a.model.getRefillRates())
	}
	for wc := range a.allocated {
		for kind := range a.allocated[wc] {
			a.allocated[wc][kind] = 0
		}
	}
}

// cpuTimeModel abstracts cpuTimeLinearModel for testing.
type cpuTimeModel interface {
	init()
	fit() [numResourceTiers][numBurstQualifications]int64
	getRefillRates() [numResourceTiers][numBurstQualifications]int64
}

// cpuTimeTokenLinearModel computes the number of CPU time tokens to add
// to each bucket in the cpuTimeTokenGranter, per interval (per 1s). As is
// discussed in the cpuTimeTokenGranter docs, the buckets are arranged in a
// priority hierarchy; some buckets always have more tokens added per second
// than others.
//
// Consider the refill rate of the lowest priority bucket first. In that case:
// refillRate = 1s * vCPU count * targetUtilization * linearCorrectionTerm
//
// This formula is intuitive if linearCorrectionTerm equals 1. If CPU time
// tokens corresponded exactly to actual CPU time, e.g. one token corresponds
// to one nanosecond of CPU time, it would imply the token bucket will limit
// the CPU used by requests subject to it to targetUtilization. Note that
// targetUtilization is controllable via the admission.cpu_time_tokens.target_util
// cluster setting.
//
// Example:
// 8 vCPU machine. admission.cpu_time_tokens.target_util = 0.8 (80%)
// RefillRate = 8 * 1s * .8 = 6.4 seconds of CPU time per second
//
// linearCorrectionTerm is a correction term derived from a linear model (hence
// the name of the struct). There is work happening outside the kvserver BatchRequest
// evaluation path, such as compaction. AC continuously fits a linear model:
// total-cpu-time = linearCorrectionTerm * reported-cpu-time, where a is forced to be
// in the interval [1, 20].
//
// The higher priority buckets have higher target utlizations than the lowest
// priority one. The delta between the target utlizations is fixed, e.g.
// burstable tenant work has a 5% higher target utilization than non-burstable.
type cpuTimeTokenLinearModel struct {
	granter           tokenUsageTracker
	settings          *cluster.Settings
	cpuMetricProvider CPUMetricProvider
	timeSource        timeutil.TimeSource

	// The time that fit was called last.
	lastFitTime time.Time
	// The comulative user/sys CPU time used since process start in
	// milliseconds.
	totalCPUTimeMillis int64
	// The CPU capacity measured in vCPUs.
	cpuCapacity float64
	// The lineabr correction term, see the docs above cpuTimeTokenLinearModel.
	tokenToCPUTimeMultiplier float64

	// The number of CPU time tokens to add to each bucket per interval (1s).
	rates [numResourceTiers][numBurstQualifications]int64
}

type tokenUsageTracker interface {
	getTokensUsedInInterval() int64
}

type CPUMetricProvider interface {
	// GetCPUInfo returns the comulative user/sys CPU time used since process
	// start in milliseconds, and the cpuCapacity measured in vCPUs.
	GetCPUInfo() (totalCPUTimeMillis int64, cpuCapacity float64)
}

// init sets tokenToCPUTImeMultipler to 1 & computes refill rates.
func (m *cpuTimeTokenLinearModel) init() {
	m.lastFitTime = m.timeSource.Now()
	_, cpuCapacity := m.cpuMetricProvider.GetCPUInfo()
	m.cpuCapacity = cpuCapacity
	m.tokenToCPUTimeMultiplier = 1
	_ = m.updateRefillRates()
}

// fit adjusts tokenToCPUTimeMultiplier based on CPU usage & token usage. fit
// computes refill rates from tokenToCPUTimeMultiplier & the admission.cpu_time_tokens.target_util
// cluster setting. fit returns the delta refill rates. That is fit returns the difference in tokens
// to add per interval (1s) from previous call to fit to this one.
func (m *cpuTimeTokenLinearModel) fit() [numResourceTiers][numBurstQualifications]int64 {
	now := m.timeSource.Now()
	elapsedSinceLastFit := now.Sub(m.lastFitTime)
	m.lastFitTime = now

	// Get used CPU tokens.
	tokensUsed := m.granter.getTokensUsedInInterval()
	// At admission time, an estimate of CPU time is deducted. After
	// the request is done processing, a correction based on a measurement
	// from grunning is deducted. Thus it is theoretically possible for net
	// tokens used to be <=0. In this case, we set tokensUsed to 1, so that
	// the computation of tokenToCPUTimeMultiplier is well-behaved.
	if tokensUsed <= 0 {
		tokensUsed = 1
	}

	// Get used CPU time.
	totalCPUTimeMillis, _ := m.cpuMetricProvider.GetCPUInfo()
	intCPUTimeMillis := totalCPUTimeMillis - m.totalCPUTimeMillis
	// totalCPUTimeMillis is not necessarily monontonic in all envionrments,
	// e.g. in case of VM live migration on a public cloud provider.
	if intCPUTimeMillis < 0 {
		intCPUTimeMillis = 0
	}
	m.totalCPUTimeMillis = totalCPUTimeMillis
	intCPUTimeNanos := intCPUTimeMillis * 1e6

	// Update multiplier.
	const lowCPUUtilFrac = 0.25
	isLowCPUUtil := intCPUTimeNanos < int64(float64(elapsedSinceLastFit)*m.cpuCapacity*lowCPUUtilFrac)
	if isLowCPUUtil {
		// Ensure that low CPU utilization is not due to a flawed tokenToCPUTimeMultiplier
		// by multiplicatively lowering it until we are below the upperBound. If we are already
		// below uppperBound, we make no adjustment.
		const upperBound = (1 / lowCPUUtilFrac) * 0.9 // 3.6
		if m.tokenToCPUTimeMultiplier > upperBound {
			m.tokenToCPUTimeMultiplier /= 1.5
			if m.tokenToCPUTimeMultiplier < upperBound {
				m.tokenToCPUTimeMultiplier = upperBound
			}
		}
	} else {
		tokenToCPUTimeMultiplier :=
			float64(intCPUTimeNanos) / float64(tokensUsed)
		// Mulitplier is forced into the interval [1, 20].
		if tokenToCPUTimeMultiplier > 20 {
			// Cap the multiplier.
			tokenToCPUTimeMultiplier = 20
		} else if tokenToCPUTimeMultiplier < 1 {
			// Likely because work is queued up in the goroutine scheduler.
			tokenToCPUTimeMultiplier = 1
		}
		// Decrease faster than increase. Giving out too many tokens can
		// lead to goroutine scheduling latency.
		alpha := 0.5
		if tokenToCPUTimeMultiplier < m.tokenToCPUTimeMultiplier {
			alpha = 0.8
		}

		// Exponentially filter changes to the multiplier. 1s of data is noisy,
		// so filtering is necessary.
		m.tokenToCPUTimeMultiplier =
			alpha*tokenToCPUTimeMultiplier + (1-alpha)*m.tokenToCPUTimeMultiplier
	}

	return m.updateRefillRates()
}

// updateRefillRates computes refill rates from tokenToCPUTimeMultiplier &
// the admission.cpu_time_tokens.target_util cluster setting. updateRefillRates
// returns the delta refill rates. That is updateRefillRates returns the difference
// in tokens to add per interval (1s) from previous call to fit to this one.
func (m *cpuTimeTokenLinearModel) updateRefillRates() [numResourceTiers][numBurstQualifications]int64 {
	// Compute goals from cluster setting. Algorithmically, it is okay if some of
	// the below goalUtils are greater than 1. This would mean greater risk of
	// goroutine scheduling latency, but there is no immediate problem -- the
	// greater some goalUtil is, the more CPU time tokens will be in the corresponding
	// bucket.
	var goalUtils [numResourceTiers][numBurstQualifications]float64
	util := KVCPUTimeUtilGoal.Get(&m.settings.SV)
	var iter float64
	for tier := int(numResourceTiers - 1); tier >= 0; tier-- {
		for qual := int(numBurstQualifications - 1); qual >= 0; qual-- {
			goalUtils[tier][qual] = util + 0.05*iter
			iter++
		}
	}

	// Update refill rates. Return change in rates via delta.
	var delta [numResourceTiers][numBurstQualifications]int64
	for tier := range goalUtils {
		for qual := range goalUtils[tier] {
			newRate :=
				int64(m.cpuCapacity * float64(time.Second) * goalUtils[tier][qual] / m.tokenToCPUTimeMultiplier)
			delta[tier][qual] = newRate - m.rates[tier][qual]
			m.rates[tier][qual] = newRate
		}
	}
	return delta
}

// getRefillRates returns the number of CPU time tokens to add to each bucket per interval (1s).
func (m *cpuTimeTokenLinearModel) getRefillRates() [numResourceTiers][numBurstQualifications]int64 {
	return m.rates
}
