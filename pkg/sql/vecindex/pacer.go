// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/crlib/crtime"
)

// allowedQueuedFixups is the number of fixups that are allowed in the queue
// before throttling is guaranteed to begin. Note that even if the current queue
// size is below this threshold, throttling will still occur if the queue size
// is increasing.
const allowedQueuedFixups = 5

// maxQueueSizeRate clamps the measured change in queue size over the course of
// one second, either positive or negative. This avoids pacer overreaction to
// brief bursts of change in small intervals.
const maxQueueSizeRate = 5

// desiredQueueSizeRate specifies the target rate of change when the fixup queue
// size needs to be reduced or when it can be increased. For example, if the
// current fixup queue size is 50, this is much bigger than the allowed size of
// 5. However, rather than attempt to reduce the size from 50 to 5 in one step,
// this setting tries to reduce it in increments of 2 fixups per second.
const desiredQueueSizeRate = 2

// pacer limits the rate of foreground insert and delete operations in the
// vector index such that background split and merge operations can keep up. It
// does this by setting the allowed ops/sec and then delaying operations that
// would otherwise exceed this limit.
//
// During normal operation, the pacer sets ops/sec at a level that tries to
// maintain the fixup queue at its current size (i.e. change rate of zero).
// However, there are two cases in which it will target a non-zero change rate:
//
//  1. If the fixup queue size is zero and operations are being limited, then
//     the pacer will raise allowed ops/sec, since it might be set too low (as
//     evidenced by an empty queue).
//  2. If the fixup queue size is > allowedQueuedFixups, then the pacer will
//     reduce allowed ops/sec in an attempt to reduce queue size. It does this
//     in increments, with the goal of reducing queue size over time rather than
//     all at once.
//
// Changes to ops/sec are proportional to the long-term average number of
// operations that generate one fixup. For example, if we want to reduce the
// rate of change in fixup queue size by 2, and we know that ~50 operations
// generate an average of one fixup, then we reduce ops/sec by 100.
type pacer struct {
	// monoNow measures elapsed time and can be mocked for testing.
	monoNow func() crtime.Mono

	mu struct {
		syncutil.Mutex

		// lastOpAt records the time of the last insert or delete operation.
		lastOpAt crtime.Mono

		// lastQueuedFixups remembers the size of the fixup queue when the last
		// insert or delete operation was executed. It's used to observe the delta
		// in queue size since that time.
		lastQueuedFixups int

		// totalOpCount counts the total number of insert or delete operations
		// that have been executed so far.
		totalOpCount int

		// totalFixupCount counts the total number of fixups that have been
		// processed so far.
		totalFixupCount int

		// queueSizeRate estimates how much the size of the fixup queue has changed
		// over the last second. It is computed as an exponential moving average
		// (EMA) and clamped to +-maxQueueSizeRate.
		queueSizeRate float64

		// delayed is true if the last insert or delete operation was delayed by
		// the pacer.
		delayed bool

		// allowedOpsPerSec is the maximum rate of insert or delete operations
		// that is allowed by the pacer.
		allowedOpsPerSec float64

		// currentTokens tracks how many tokens are currently in the bucket. Each
		// token represents one insert or delete operation. When currentTokens
		// drops below zero, operations will be delayed.
		currentTokens float64
	}
}

// Init sets up the pacer. "initialOpsPerSec" initializes the token bucket
// refill rate that governs how many insert or delete operations run per second.
// "initialOpsPerFixup" gives a starting guess for how many operations, on
// average, cause one fixup to be generated. Both of these values will
// automatically change over time, but more accurate initial values can decrease
// "ramp up" time for the pacer as it learns the optimal pace.
func (p *pacer) Init(initialOpsPerSec int, initialOpsPerFixup int, monoNow func() crtime.Mono) {
	p.monoNow = monoNow
	p.mu.lastOpAt = monoNow()
	p.mu.allowedOpsPerSec = float64(initialOpsPerSec)

	// Avoid division by zero errors by initializing query/fixup counts.
	p.mu.totalOpCount = initialOpsPerFixup
	p.mu.totalFixupCount = 1
}

// OnFixup is called when a fixup has been processed by the vector index. This
// is used by the pacer to match the incoming rate of new fixups with how fast
// they can be resolved.
func (p *pacer) OnFixup() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.mu.totalFixupCount++
}

// OnInsertOrDelete is called when an insert or delete operation is run by the
// vector index. It takes the current size of the fixup queue and based on that,
// returns how much time to delay before running the operation. This ensures
// that background index maintenance operations do not fall too far behind
// foreground operations.
func (p *pacer) OnInsertOrDelete(queuedFixups int) time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.mu.totalOpCount++

	// Compute elapsed time since the last query.
	now := p.monoNow()
	elapsed := now.Sub(p.mu.lastOpAt)
	if elapsed == 0 {
		// Avoid division by zero.
		elapsed = 1
	}
	p.mu.lastOpAt = now

	// Compute rate of change in the size of the fixup queue over the elapsed
	// time interval.
	queueSizeRate := float64(queuedFixups-p.mu.lastQueuedFixups) / elapsed.Seconds()
	p.mu.lastQueuedFixups = queuedFixups

	// Compute exponential moving average (EMA) of the fixup queue size rate
	// over the last second. The sample is weighted according to the elapsed time
	// (clamped to 1 second max).
	alpha := min(elapsed.Seconds(), 1)
	p.mu.queueSizeRate = alpha*queueSizeRate + (1-alpha)*p.mu.queueSizeRate

	// Clamp the overall rate of change in order to prevent anomalies when a large
	// number of fixups are generated in a short amount of time (e.g. because of
	// statistical clustering or a backlog of fixups that is suddenly added to
	// the queue).
	p.mu.queueSizeRate = max(min(p.mu.queueSizeRate, maxQueueSizeRate), -maxQueueSizeRate)

	// Determine how much the fixup queue size needs to change.
	var queueSizeRateDelta float64
	if queuedFixups < allowedQueuedFixups {
		// Queue is allowed to be longer, but only allow increase if the last query
		// was delayed. Otherwise, if there's little or no activity, the pacer
		// would never stop increasing allowed ops/sec. Also, only increase when
		// the queue is empty, since that indicates ops/sec may be set too low. If
		// the queue is not empty, then simply try to maintain its size at its
		// current level (i.e. queueSizeRateDelta = 0).
		if p.mu.delayed && queuedFixups == 0 {
			queueSizeRateDelta = desiredQueueSizeRate
		}
	} else {
		// Queue is longer than allowed, but do not attempt to reduce its size all
		// at once, as that could require heavy throttling. Instead, reduce it in
		// increments over time.
		queueSizeRateDelta = float64(max(allowedQueuedFixups-queuedFixups, -desiredQueueSizeRate))
	}

	// Adjust for rate at which fixups are already changing. For example, if we
	// want to decrease the queue size by 2 fixups/sec, but the queue is currently
	// growing at 2 fixups/sec, then we need a net decrease of 4 fixups/sec.
	queueSizeRateDelta = queueSizeRateDelta - p.mu.queueSizeRate

	// Compute the average number of queries that result in one fixup. Allow an
	// increase or decrease in ops/sec that corresponds to the desired increase
	// or decrease in queue size. Don't let ops/sec fall below 1, even in case
	// where, for example, fixups are somehow blocked.
	queriesPerFixup := float64(p.mu.totalOpCount) / float64(p.mu.totalFixupCount)
	deltaOpsPerSec := float64(queueSizeRateDelta) * queriesPerFixup * elapsed.Seconds()
	p.mu.allowedOpsPerSec = max(p.mu.allowedOpsPerSec+deltaOpsPerSec, 1.0)

	// Add tokens to the bucket based on elapsed time and remove 1 token for the
	// current query. Allow bucket to contain 1 second of excess tokens.
	p.mu.currentTokens += p.mu.allowedOpsPerSec * elapsed.Seconds()
	if p.mu.currentTokens > p.mu.allowedOpsPerSec {
		p.mu.currentTokens = p.mu.allowedOpsPerSec
	}
	p.mu.currentTokens--

	// If the token bucket is empty, return the required pacing delay.
	p.mu.delayed = p.mu.currentTokens < 0
	if !p.mu.delayed {
		return 0
	}
	return time.Duration(float64(time.Second) * -p.mu.currentTokens / p.mu.allowedOpsPerSec)
}
