// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// timePerTick is how frequently cpuTimeTokenFiller ticks its time.Ticker & adds
// tokens to the buckets. Must be < 1s. Must divide 1s evenly.
const timePerTick = 1 * time.Millisecond

// cpuTimeTokenFiller starts a goroutine which periodically calls
// cpuTimeTokenAllocator to add tokens to a cpuTimeTokenGranter. For example, on
// an 8 vCPU machine, we may want to allow burstable tier-0 work to use 6 seconds
// of CPU time per second. Then cpuTimeTokenAllocator.rates[tier0][canBurst] would
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

// tokenAllocator abstracts cpuTimeTokenAllocator for testing.
type tokenAllocator interface {
	allocateTokens(remainingTicksInInInterval int64)
	resetInterval()
}

func (f *cpuTimeTokenFiller) start() {
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
					f.allocator.resetInterval()
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

// cpuTimeTokenAllocator allocates tokens to a cpuTimeTokenGranter. See the comment
// above cpuTimeTokenFiller for a high level picture. The responsibility of
// cpuTimeTokenAllocator is to gradually allocate rates tokens every interval,
// while respecting bucketCapacity. We have split up the ticking & token allocation
// logic, in order to improve clarity & testability.
type cpuTimeTokenAllocator struct {
	granter *cpuTimeTokenGranter

	// Mutable fields. No mutex, since only a single goroutine will call the
	// cpuTimeTokenAllocator.

	// rates stores the number of token added to each bucket every interval.
	rates [numResourceTiers][numBurstQualifications]int64
	// bucketCapacity stores the maximum number of tokens that can be in each bucket.
	// That is, if a bucket is already at capacity, no more tokens will be added.
	bucketCapacity [numResourceTiers][numBurstQualifications]int64
	// allocated stores the number of tokens added to each bucket in the current
	// interval.
	allocated [numResourceTiers][numBurstQualifications]int64
}

var _ tokenAllocator = &cpuTimeTokenAllocator{}

// allocateTokens allocates tokens to a cpuTimeTokenGranter. allocateTokens
// adds rates tokens every interval, while respecting bucketCapacity.
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

	var delta [numResourceTiers][numBurstQualifications]int64
	for wc := range a.rates {
		for kind := range a.rates[wc] {
			toAllocateTokens := allocateFunc(
				a.rates[wc][kind], a.allocated[wc][kind], expectedRemainingTicksInInterval)
			a.allocated[wc][kind] += toAllocateTokens
			delta[wc][kind] = toAllocateTokens
		}
	}
	a.granter.refill(delta, a.bucketCapacity)
}

// resetInterval is called to signal the beginning of a new interval. allocateTokens
// adds rates tokens every interval.
func (a *cpuTimeTokenAllocator) resetInterval() {
	for wc := range a.allocated {
		for kind := range a.allocated[wc] {
			a.allocated[wc][kind] = 0
		}
	}
}
