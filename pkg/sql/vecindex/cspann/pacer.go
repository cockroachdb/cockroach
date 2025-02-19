// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"math"
	"time"

	"github.com/cockroachdb/crlib/crtime"
)

// targetQueuedFixups is the number of fixups that are allowed in the queue
// before throttling may begin. Note that even if the current queue size is
// below this threshold, throttling will still occur if the queue size is
// increasing at too high of a rate. Also, this is a "soft" target; as long as
// the size is reasonably close, the pacer won't do much.
const targetQueuedFixups = 5

// maxQueueSizeRate clamps the measured change in queue size over the course of
// one second, either positive or negative. This avoids pacer overreaction to
// brief bursts of change in small intervals.
const maxQueueSizeRate = 5

// gradualQueueSizeMax specifies the max rate of change when the fixup queue
// size needs to be reduced. For example, if the current fixup queue size is 50,
// this is much bigger than the allowed size of 5. However, rather than attempt
// to reduce the size from 50 to 5 in one step, this setting tries to reduce it
// in increments of 2 fixups per second.
const gradualQueueSizeMax = 2

// deltaFactor governs how quickly the pacer makes changes to the allowed
// ops/sec. A higher factor value makes the pacer more responsive to changes,
// but increases how much it will overshoot the point of equilibrium.
const deltaFactor = 2

// pacer limits the rate of foreground insert and delete operations in the
// vector index such that background split and merge operations can keep up. It
// does this by setting the allowed ops/sec and then delaying operations that
// would otherwise exceed this limit.
//
// During normal operation, the pacer sets ops/sec at a level that tries to
// maintain the fixup queue at its current size (i.e. change rate of zero).
// However, there are two cases in which it will target a non-zero change rate:
//
//  1. If the fixup queue is empty (or nearly so) and operations are being
//     delayed, then the pacer will raise allowed ops/sec, since it might be set
//     too low (as evidenced by a small queue).
//  2. If the fixup queue size is > targetQueuedFixups, then the pacer will
//     reduce allowed ops/sec in an attempt to reduce queue size. It does this
//     in increments, with the goal of reducing queue size over time rather than
//     all at once.
//
// NOTE: The pacer is *not* thread-safe. It is the responsibility of the caller
// to do any needed locking.
type pacer struct {
	// monoNow measures elapsed time and can be mocked for testing.
	monoNow func() crtime.Mono

	// lastOpAt records the time of the last insert or delete operation that
	// updated the token bucket.
	lastOpAt crtime.Mono

	// lastUpdateAt records the time of the last update to allowed ops/sec.
	lastUpdateAt crtime.Mono

	// lastQueuedFixups remembers the size of the fixup queue when the last
	// insert or delete operation was executed. It's used to observe the delta
	// in queue size since that time.
	lastQueuedFixups int

	// queueSizeRate estimates how much the size of the fixup queue has changed
	// over the last second. It is computed as an exponential moving average
	// (EMA) and clamped to +-maxQueueSizeRate.
	queueSizeRate float64

	// allowedOpsPerSec is the maximum rate of insert or delete operations
	// that is allowed by the pacer.
	allowedOpsPerSec float64

	// currentTokens tracks how many tokens are currently in the bucket. Each
	// token represents one insert or delete operation. When currentTokens
	// drops below zero, operations will be delayed.
	currentTokens float64

	// delayed is true if the pacer has delayed an insert or delete operation
	// since the last update to allowed ops/sec.
	delayed bool
}

// Init sets up the pacer. "initialOpsPerSec" initializes the token bucket
// refill rate that governs how many insert or delete operations run per second.
// This value will automatically change over time, but a more accurate initial
// value can decrease "ramp up" time for the pacer as it learns the optimal
// pace. "initialFixups" specifies the initial number of fixups in the queue
// (used for testing).
func (p *pacer) Init(initialOpsPerSec int, initialFixups int, monoNow func() crtime.Mono) {
	p.monoNow = monoNow
	p.lastUpdateAt = monoNow()
	p.allowedOpsPerSec = float64(initialOpsPerSec)
	p.lastQueuedFixups = initialFixups
}

// OnFixup is called when the size of the fixup queue has changed because a
// fixup has been added or removed to/from the queue by the vector index.
func (p *pacer) OnFixup(queuedFixups int) {
	// Compute elapsed time since the last update to allowed ops/sec.
	now := p.monoNow()
	sinceUpdate := now.Sub(p.lastUpdateAt)
	if sinceUpdate == 0 {
		// Avoid division by zero.
		sinceUpdate = 1
	}
	p.lastUpdateAt = now

	p.updateOpsPerSec(sinceUpdate, queuedFixups)
}

// OnInsertOrDelete is called when an insert or delete operation is about to be
// run by the vector index. It takes the current size of the fixup queue and
// based on that, returns how much time to delay before running the operation.
// This ensures that background index maintenance operations do not fall too far
// behind foreground operations.
func (p *pacer) OnInsertOrDelete(queuedFixups int) time.Duration {
	// Fast path: if there are enough tokens in the bucket, no need for delay.
	p.currentTokens--
	if p.currentTokens >= 0 {
		return 0
	}

	// If it's been at least a second since allowed ops/sec was updated, do so
	// now. This handles an edge case where ops/sec is being throttled so heavily
	// (e.g. 1 op/sec) that fixups are rare, and it takes too long to increase
	// allowed ops/sec.
	now := p.monoNow()
	sinceUpdate := now.Sub(p.lastUpdateAt)
	if sinceUpdate >= time.Second {
		p.lastUpdateAt = now
		p.updateOpsPerSec(sinceUpdate, queuedFixups)
	}

	// Compute elapsed time since the last insert or delete operation that
	// updated the token bucket.
	sinceOp := now.Sub(p.lastOpAt)
	p.lastOpAt = now

	// Add tokens to the bucket based on elapsed time. Allow bucket to contain
	// up to one second of excess tokens.
	p.currentTokens += p.allowedOpsPerSec * sinceOp.Seconds()
	if p.currentTokens > p.allowedOpsPerSec {
		p.currentTokens = p.allowedOpsPerSec
	}

	if p.currentTokens >= 0 {
		// Enough tokens, no delay.
		return 0
	}

	// The token bucket has gone into "debt", so return the pacing delay that
	// enforces the allowed ops/sec. This is the inverse of allowed ops/sec, or
	// the "time between operations". It is multiplied by the number of tokens
	// of other waiting operations (i.e. the token debt). For example, if the
	// allowed ops/sec is 1000, then operations should have an average 1 ms
	// interval between them. If three operations arrive in immediate succession,
	// then the first should have 0 ms delay, the second should have 1 ms delay,
	// and the third should have 2 ms delay, and so on.
	p.delayed = true
	return time.Duration(float64(time.Second) * -p.currentTokens / p.allowedOpsPerSec)
}

// OnInsertOrDeleteCanceled should be called when an insert or delete operation
// has its context canceled while waiting out its pacing delay. Because the
// operation was never completed, its token should not be consumed. Without
// this, a repeated sequence of cancellations could cause the token bucket to go
// increasingly negative, causing ever-increasing delays.
func (p *pacer) OnInsertOrDeleteCanceled() {
	p.currentTokens++
}

// updateOpsPerSec updates the allowed ops/sec based on the number of fixups in
// the queue. Updates are scaled by the amount of time that's elapsed since the
// last call to updateOpsPerSec. Allowing sub-second elapsed increments allows
// the pacer to be significantly more responsive.
func (p *pacer) updateOpsPerSec(elapsed time.Duration, queuedFixups int) {
	// Remember if any operation was throttled since the last call to update.
	delayed := p.delayed
	p.delayed = false

	// Calculate the desired rate of change in the fixup queue size over the next
	// second.
	var desiredQueueSizeRate float64
	if queuedFixups > targetQueuedFixups {
		// If the fixup queue is too large, reduce it at a gradual rate that's
		// proportional to its distance from the target. Never reduce it more
		// than gradualQueueSizeMax.
		const gradualRateFactor = 10
		desiredQueueSizeRate = float64(targetQueuedFixups-queuedFixups) / gradualRateFactor
		desiredQueueSizeRate = max(desiredQueueSizeRate, -gradualQueueSizeMax)
	} else if queuedFixups <= 1 {
		// If the fixup queue is empty or has just one fixup, then it could be
		// that background fixups are happening fast enough. However, it's also
		// possible that the fixup queue is small because the pacer is heavily
		// throttling operations. Sharply increase allowed ops/sec, up to the
		// target, in case that's true.
		desiredQueueSizeRate = float64(targetQueuedFixups - queuedFixups)
	}

	// Calculate the actual rate of change in the fixup queue size over the last
	// second.
	actualQueueSizeRate := p.calculateQueueSizeRate(elapsed, queuedFixups)

	// Calculate the net rate that's needed to match the desired rate. For
	// example, if we desire to decrease the queue size by 2 fixups/sec, but the
	// queue is actually growing at 2 fixups/sec, then we need a net decrease of
	// 4 fixups/sec.
	netQueueSizeRate := desiredQueueSizeRate - actualQueueSizeRate
	netQueueSizeRate = max(min(netQueueSizeRate, maxQueueSizeRate), -maxQueueSizeRate)

	// Do not increase allowed ops/sec if operations are not being throttled.
	// Otherwise, if there's little or no activity, the pacer would never stop
	// increasing allowed ops/sec.
	if netQueueSizeRate > 0 && !delayed {
		return
	}

	// Determine how much to change allowed ops/sec to achieve the net change in
	// fixup queue size over the next second. When allowed ops/sec is small,
	// allow it to ramp quickly by starting with a minimum delta of 10 ops/sec.
	const minDeltaOpsPerSec = 10
	deltaOpsPerSec := max(p.allowedOpsPerSec, minDeltaOpsPerSec)
	if netQueueSizeRate < 0 {
		// Decrease ops/sec up to some % of its current value. For example, if
		// deltaFactor is 2, then it can decrease by up to 50% of its current
		// value.
		deltaOpsPerSec = deltaOpsPerSec/deltaFactor - deltaOpsPerSec
	} else {
		// Increase ops/sec by some % of its current value. For example, if
		// deltaFactor is 2, then it can increase by up to 100% of its current
		// value.
		deltaOpsPerSec = deltaOpsPerSec*deltaFactor - deltaOpsPerSec
	}

	// Scale the change in ops/sec by the magnitude of desired change with respect
	// to the max allowed change.
	deltaOpsPerSec *= math.Abs(netQueueSizeRate) / maxQueueSizeRate

	// Scale the delta based on the elapsed time. For example, if we want to
	// decrease ops/sec by 200, but it's been only 0.5 seconds since the last
	// fixup, then we need to change ops/sec by -200 * 0.5 = -100. This allows
	// for multiple micro-adjustments over the course of a second that add up to
	// the full adjustment (if the trend doesn't change).
	deltaOpsPerSec = deltaOpsPerSec * min(elapsed.Seconds(), 1)

	// Update allowed ops/sec, but don't let it fall below 1, even in case where,
	// for example, fixups are somehow blocked.
	p.allowedOpsPerSec = max(p.allowedOpsPerSec+deltaOpsPerSec, 1)
}

// calculateQueueSizeRate calculates the exponential moving average (EMA) of the
// rate of change in the fixup queue size, over the last second.
func (p *pacer) calculateQueueSizeRate(elapsed time.Duration, queuedFixups int) float64 {
	// Calculate the rate of change in the fixup queue size over the elapsed time
	// period.
	queueSizeRate := float64(queuedFixups-p.lastQueuedFixups) / elapsed.Seconds()
	p.lastQueuedFixups = queuedFixups

	// Factor that sample into the EMA by weighting it according to the elapsed
	// time (clamped to 1 second max).
	alpha := min(elapsed.Seconds(), 1)
	p.queueSizeRate = alpha*queueSizeRate + (1-alpha)*p.queueSizeRate

	// Clamp the overall rate of change in order to prevent anomalies when a large
	// number of fixups are generated in a short amount of time (e.g. because of
	// statistical clustering or a backlog of fixups that is suddenly added to
	// the queue).
	p.queueSizeRate = max(min(p.queueSizeRate, maxQueueSizeRate), -maxQueueSizeRate)

	return p.queueSizeRate
}
