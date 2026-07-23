// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// cpuTimeTokenGranter uses token buckets to limit CPU usage. There is one
// token bucket per burst qualification (canBurst and noBurst). Requests
// are only admitted (tryGet returns true) if the bucket for the request's
// burst qualification has positive tokens. Before a request is admitted,
// tokens are deducted from all buckets, not just the one that was checked.
// This enables a priority hierarchy where burstable work can use more CPU
// than non-burstable work.
//
// For example, on an 8 vCPU machine, it might be set up like this:
//
// - Burstable work -> 6.8 seconds of CPU time per second
// - Non-burstable work -> 6.4 seconds of CPU time per second
//
// A request for 6s of burstable work would be admitted immediately,
// since the burstable bucket is positive. It would deduct from both
// buckets, resulting in a balance of (0.8, 0.4). Non-burstable work
// would be throttled sooner while burstable work is still admissible.
//
// Note that cpuTimeTokenGranter does not handle replenishing the buckets.
//
// For more, see the initial design sketch:
// https://docs.google.com/document/d/1-Kr2gRFTk0QV8kBs7AXRXUwFpK2ZxR1cqIwWCuOx22Q/edit?tab=t.0
type cpuTimeTokenGranter struct {
	requester  requester
	metrics    *cpuTimeTokenMetrics
	timeSource timeutil.TimeSource
	mu         struct {
		syncutil.Mutex
		// Invariant: the canBurst bucket always has at least as many
		// tokens as the noBurst bucket, because canBurst has a higher
		// refill rate and admission deducts from all buckets equally.
		buckets    [numBurstQualifications]tokenBucket
		tokensUsed int64
	}
}

var _ granter = &cpuTimeTokenGranter{}

func newCPUTimeTokenGranter(
	metrics *cpuTimeTokenMetrics, timeSource timeutil.TimeSource,
) *cpuTimeTokenGranter {
	g := &cpuTimeTokenGranter{metrics: metrics, timeSource: timeSource}
	// Buckets start at 0 tokens (exhausted) before the first refill, so
	// initialize exhaustedStart and wire the per-bucket counters.
	now := timeSource.Now()
	for qual := burstQualification(0); qual < numBurstQualifications; qual++ {
		g.mu.buckets[qual].exhaustedStart = now
		g.mu.buckets[qual].exhaustedDuration =
			metrics.ExhaustedDurationNanos[qual]
	}
	return g
}

type tokenBucket struct {
	tokens int64
	// exhaustedStart is the time at which the bucket entered the exhausted
	// state (tokens <= 0). Zero when the bucket is not exhausted.
	exhaustedStart time.Time
	// exhaustedDuration is a cumulative counter of nanoseconds spent exhausted.
	exhaustedDuration *metric.Counter
}

// updateTokenCount sets the bucket's token count and updates the
// exhausted-duration counter based on the transition into or out of the
// exhausted state (tokens <= 0).
//
// Three transitions are handled:
//  1. wasExhausted && !isExhausted — recovery: flush elapsed time to counter.
//  2. !wasExhausted && isExhausted — entering exhaustion: record start time.
//  3. isExhausted && flushToMetricNow — still exhausted but in this case,
//     updateTokenCount flushes accumulated duration to the counter. This
//     way, sustained exhaustion is visible in metrics even over shorter
//     periods such as 1m. flushToMetricNow is set to true on a call to
//     updateTokenCount once every second.
func (tb *tokenBucket) updateTokenCount(newTokens int64, now time.Time, flushToMetricNow bool) {
	wasExhausted := tb.tokens <= 0
	tb.tokens = newTokens
	isExhausted := tb.tokens <= 0
	switch {
	case wasExhausted && !isExhausted:
		tb.exhaustedDuration.Inc(now.Sub(tb.exhaustedStart).Nanoseconds())
		tb.exhaustedStart = time.Time{}
	case !wasExhausted && isExhausted:
		tb.exhaustedStart = now
	case isExhausted && flushToMetricNow:
		tb.exhaustedDuration.Inc(now.Sub(tb.exhaustedStart).Nanoseconds())
		tb.exhaustedStart = now
	}
}

func (stg *cpuTimeTokenGranter) String() string {
	return redact.StringWithoutMarkers(stg)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (stg *cpuTimeTokenGranter) SafeFormat(s redact.SafePrinter, _ rune) {
	stg.mu.Lock()
	defer stg.mu.Unlock()
	s.SafeString("cpuTTG ")
	for qual := canBurst; qual < numBurstQualifications; qual++ {
		if qual > canBurst {
			s.SafeRune(' ')
		}
		s.Printf("%s=%d", qual, stg.mu.buckets[qual].tokens)
	}
	s.SafeRune('\n')
}

// tryGet implements granter.
func (stg *cpuTimeTokenGranter) tryGet(qual burstQualification, count int64) bool {
	stg.mu.Lock()
	defer stg.mu.Unlock()
	if stg.mu.buckets[qual].tokens <= 0 {
		return false
	}
	stg.tookWithoutPermissionLocked(count)
	return true
}

// returnGrant implements granter.
func (stg *cpuTimeTokenGranter) returnGrant(count int64) {
	stg.mu.Lock()
	defer stg.mu.Unlock()
	stg.tookWithoutPermissionLocked(-count)
	// count must be positive. Thus above always adds tokens to the buckets.
	// Thus returnGrant should always attempt to grant admission to waiting requests.
	stg.grantUntilNoWaitingRequestsLocked()
}

// tookWithoutPermission implements granter.
func (stg *cpuTimeTokenGranter) tookWithoutPermission(count int64) {
	stg.mu.Lock()
	defer stg.mu.Unlock()
	stg.tookWithoutPermissionLocked(count)
}

func (stg *cpuTimeTokenGranter) tookWithoutPermissionLocked(count int64) {
	stg.mu.tokensUsed += count
	// Token usage is split into two cumulative counters (consumed and
	// returned) rather than a single net gauge, so that DD/Prometheus can
	// compute rate(consumed) - rate(returned) over arbitrary windows
	// (1m, 30m, etc.).
	if count > 0 {
		stg.metrics.UsageConsumed.Inc(count)
	} else {
		stg.metrics.UsageReturned.Inc(-count)
	}
	now := stg.timeSource.Now()
	for qual := range stg.mu.buckets {
		newTokenCount := stg.mu.buckets[qual].tokens - count
		stg.mu.buckets[qual].updateTokenCount(
			newTokenCount, now, false /* flushToMetricNow */)
	}
}

// grantUntilNoWaitingRequestsLocked grants admission to all queued requests
// that can be granted, given the current state of the token buckets.
func (stg *cpuTimeTokenGranter) grantUntilNoWaitingRequestsLocked() {
	// TODO(josh): If there are a lot of tokens, this could hold the mutex for a long
	// time. We may want to drop and reacquire the mutex after every 1000 requests or so.
	for stg.tryGrantLocked() {
	}
}

// tryGrantLocked attempts to grant admission to a single queued request.
func (stg *cpuTimeTokenGranter) tryGrantLocked() bool {
	if buildutil.CrdbTestBuild && stg.requester == nil {
		panic(errors.AssertionFailedf("cpuTimeTokenGranter.requester is nil"))
	}
	hasWaitingRequests, qual := stg.requester.hasWaitingRequests()
	if !hasWaitingRequests {
		return false
	}
	if stg.mu.buckets[qual].tokens <= 0 {
		return false
	}
	tokens := stg.requester.granted(noGrantChain)
	if tokens == 0 {
		// Did not accept grant.
		return false
	}
	stg.tookWithoutPermissionLocked(tokens)
	return true
}

// continueGrantChain implements granter.
func (stg *cpuTimeTokenGranter) continueGrantChain(grantChainID grantChainID) {
	// Grant chains are not used.
}

// resetTokensUsedInInterval resets the tracked used tokens to zero. The previous
// value is returned.
func (stg *cpuTimeTokenGranter) resetTokensUsedInInterval() int64 {
	stg.mu.Lock()
	defer stg.mu.Unlock()
	tokensUsed := stg.mu.tokensUsed
	stg.mu.tokensUsed = 0
	return tokensUsed
}

// refill adds toAdd tokens to the corresponding buckets, while respecting
// the capacity info stored in bucketCapacities and enforcing per-bucket
// minimums from bucketMinimums. Tokens that would bring the bucket above
// capacity will be discarded, and if the token count is below the minimum,
// it will be raised to the minimum. The minimums bound recovery time after
// periods of overuse, preventing a bucket from accumulating unbounded token
// debt. refill attempts to grant admission to waiting requests in case
// where tokens are added to some bucket. updateMetrics controls whether
// gauge and exhausted-duration metrics are updated.
func (stg *cpuTimeTokenGranter) refill(
	toAdd tokenCounts, bucketCapacities capacities, bucketMinimums minimums, updateMetrics bool,
) {
	stg.mu.Lock()
	defer stg.mu.Unlock()

	now := stg.timeSource.Now()
	var shouldGrant bool
	for qual := range stg.mu.buckets {
		if toAdd[qual] > 0 {
			shouldGrant = true
		}
		newTokenCount := stg.mu.buckets[qual].tokens + toAdd[qual]
		newTokenCount = min(newTokenCount, bucketCapacities[qual])
		newTokenCount = max(newTokenCount, bucketMinimums[qual])
		stg.mu.buckets[qual].updateTokenCount(
			newTokenCount, now, updateMetrics /* flushToMetricNow */)
	}

	// Grant if tokens are added to any of the buckets.
	if shouldGrant {
		stg.grantUntilNoWaitingRequestsLocked()
	}
}
