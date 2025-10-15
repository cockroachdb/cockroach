package admission

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// cpuTimeTokenFiller starts a goroutine which periodically adds tokens to a
// cpuTimeTokenGranter. For example, on an 8 vCPU machine, we may want to allow
// burstable tier-0 work to use 6 seconds of CPU time per second. Then
// rates[tier0][canBurst] would equal 6 seconds per second, and cpuTimeTokenFiller
// would add 6 seconds of token every second. See cpuTimeTokenGranter for details
// on the multi-dimensional token buckets owned by cpuTimeTokenGranter; the TLDR is
// there is one bucket per <resource tier, burst qualification> pair.
type cpuTimeTokenFiller struct {
	granter *cpuTimeTokenGranter

	// Immutable fields.
	// rates stores the number of token added to each bucket every second.
	rates [numResourceTiers][numBurstQualifications]int64
	// burstBudget stores the maximum number of tokens that can be in each bucket.
	// That is, if a bucket is already at its burst budget, no more tokens will
	// be added.
	burstBudget [numResourceTiers][numBurstQualifications]int64
	// timePerTick is how frequently tokens are added to the buckets.
	// Must be <= 1s.
	timePerTick time.Duration

	// Mutable fields.
	// No mutex, since there is only a single goroutine with
	// access to allocated.
	allocated  [numResourceTiers][numBurstQualifications]int64
	timeSource timeutil.TimeSource
	closeCh    chan struct{}
	// Used only in unit tests.
	tickCh *chan struct{}
}

func (f *cpuTimeTokenFiller) start() {
	ticker := f.timeSource.NewTicker(f.timePerTick)
	intervalStart := f.timeSource.Now()
	// The spawned goroutine will add the configured number of tokens to each
	// bucket in granter, every second. It is written to be robust against delayed
	// and dropped ticks. That is, in the presence of delayed and dropped ticks,
	// the correct number of tokens will be added to the buckets; they just may be
	// added in a less smooth fashion than normal. If ticks are delayed more than roughly
	// 1s, not enough tokens will be added to the bucket, but we do not expect this
	// significant of a delay in practice (admission control will be running).
	//
	// See ticker docs, where it is mentioned ticks can be dropped, if receivers are
	// slow: https://pkg.go.dev/time#NewTicker
	//
	// The mechanism by which the goroutine adds the correct number of tokens, in the
	// presence of delayed or dropped ticks, is:
	// - time is split into intervals of 1s
	// - intervals are split into 1s / timePerTick(=1ms) time.Ticker ticks
	// - the goroutine attempts to allocate remaining tokens for interval evenly across
	//   remaining ticks in the interval
	// - once interval is complete, all remaining tokens needed for that interval
	//   are added (e.g. see t.refill(1) below), then a new interval starts
	go func() {
		lastRemainingTicks := int64(0)
		for {
			select {
			case t := <-ticker.Ch():
				var remainingTicks int64
				elapsedSinceIntervalStart := t.Sub(intervalStart)
				if elapsedSinceIntervalStart >= time.Second {
					if lastRemainingTicks > 1 {
						f.refill(1)
					}
					intervalStart = t
					for wc := range f.allocated {
						for kind := range f.allocated[wc] {
							f.allocated[wc][kind] = 0
						}
					}
					remainingTicks = int64(time.Second / f.timePerTick)
				} else {
					remainingSinceIntervalStart := time.Second - elapsedSinceIntervalStart
					if remainingSinceIntervalStart < 0 {
						panic(errors.AssertionFailedf("remainingSinceIntervalStart is negative %d", remainingSinceIntervalStart))
					}
					// ceil(a / b) == (a + b - 1) / b, when using integer division.
					// Round up so that we don't accumulate tokens to give in a burst on the
					// last tick.
					remainingTicks =
						int64((remainingSinceIntervalStart + f.timePerTick - 1) / f.timePerTick)
				}
				f.refill(max(1, remainingTicks))
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

// TODO(josh): Expand to cover tenant-specific token buckets too.
func (f *cpuTimeTokenFiller) refill(remainingTicks int64) {
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
	for wc := range f.rates {
		for kind := range f.rates[wc] {
			toAllocateTokens := allocateFunc(
				f.rates[wc][kind], f.allocated[wc][kind], remainingTicks)
			f.allocated[wc][kind] += toAllocateTokens
			delta[wc][kind] = toAllocateTokens
		}
	}
	f.granter.refill(delta, f.burstBudget)
}
