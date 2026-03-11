// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
	"github.com/olekukonko/tablewriter"
)

// resourceTier specifies the tier of a resource group, in descending levels of importance.
// That is, tier 0 is the most important. The token bucket sizes must be such that the
// non-burstable token bucket size of tier-i must be greater than the burstable token bucket
// size of tier-(i+1); see cpuTimeTokenGranter for details on this.
//
// The tier determination for a request happens at a layer outside the admission package.
//
// TODO(josh): Add docs re: tentative usage after a discussion with Sumeer.
//
// NB: Inter-tenant fair sharing only works within a tier.
//
// TODO(josh): Move this definition to admission.go. Export publicly.
type resourceTier uint8

const (
	// systemTenant is the tier associated with all system tenant work.
	//
	// Note that currently resourceTier is only used in CPU time token AC, which is
	// only used in Serverless. So there is always both a system tenant and at least
	// one app tenant, and customer SQL is run via one of the app tenants.
	systemTenant resourceTier = iota
	// appTenant is the tier associated with all app tenant work.
	//
	// Note that currently resourceTier is only used in CPU time token AC, which is
	// only used in Serverless. So there is always both a system tenant and at least
	// one app tenant, and customer SQL is run via one of the app tenants. All app
	// tenant work, regardless of which app tenant is used, uses appTenant (that is, all
	// Serverless customer SQL uses appTenant).
	appTenant
	numResourceTiers
)

func (rt resourceTier) String() string {
	return redact.StringWithoutMarkers(rt)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (rt resourceTier) SafeFormat(s redact.SafePrinter, _ rune) {
	switch rt {
	case systemTenant:
		s.SafeString("system_tenant")
	case appTenant:
		s.SafeString("app_tenant")
	default:
		s.Printf("resourceTier(%d)", redact.Safe(rt))
	}
}

// cpuTimeTokenChildGranter implements granter. It stores resourceTier and proxies
// proxies to cpuTimeTokenGranter. See the declaration comment for cpuTimeTokenGranter
// for more details.
//
// Each "child" granter is paired with a requester, since the requester (in practice, a
// WorkQueue for a certain resourceTier) does not need to know about the others.
// An alternative would be to make resourceTier an argument to the various granter methods,
// but this approach seems cleaner.
type cpuTimeTokenChildGranter struct {
	tier   resourceTier
	parent *cpuTimeTokenGranter
}

var _ granter = &cpuTimeTokenChildGranter{}

// tryGet implements granter.
func (cg *cpuTimeTokenChildGranter) tryGet(qual burstQualification, count int64) bool {
	return cg.parent.tryGet(cg.tier, qual, count)
}

// returnGrant implements granter.
func (cg *cpuTimeTokenChildGranter) returnGrant(count int64) {
	cg.parent.returnGrant(count)
}

// tookWithoutPermission implements granter.
func (cg *cpuTimeTokenChildGranter) tookWithoutPermission(count int64) {
	cg.parent.tookWithoutPermission(count)
}

// continueGrantChain implements granter.
func (cg *cpuTimeTokenChildGranter) continueGrantChain(grantChainID grantChainID) {
	// Ignore since grant chains are not used.
}

// cpuTimeTokenGranter uses token buckets to limit CPU usage. There is one
// token bucket per type of request. Requests are only admitted (tryGet
// only returns true), if the bucket for the type of request to be done has
// positive tokens. Before a request is admitted, tokens are deducated from all
// buckets, not just the bucket that was checked initially. This enables
// setting up a hierarchy of types of requests, where some types can use more
// CPU than others.
//
// For example, on an 8 vCPU machine, it might be set up like this:
//
// - Burstable tier-0 work -> 6 seconds of CPU time per second
// - Non-burstable tier-0 work -> 5 seconds of CPU time per second
// - Burstable tier-1 work -> 2 seconds of CPU time per second
// - Non-burstable tier-1 work -> 1 seconds of CPU time per second
//
// A request for 5s of burstable tier-0 work would be admitted immediately,
// since the burstable tier-0 bucket is positive. It would deduct from all
// four buckets, resulting in a balance of (1,0,-3,-4). Non-burstable tier-0
// work and all tier-1 work would now have to wait for their respective buckets
// to refill, while burstable tier-0 work is still admissible.
//
// The immediate purpose of this is to achieve low goroutine scheduling latencies
// even in the case of a Serverless tenant sending a large workload to a multi-host
// cluster. The above rates will be set so as to limit CPU utilization to some
// cluster-setting-configurable maximum. For example, if the target max is 80%, and
// if the machine has 8 vCPUs, then the non-burstable tier-0 work will be allowed
// 6.4 seconds of CPU time per second. In limiting CPU usage to some max, goroutine
// scheduling latency can be kept low.
//
// TODO(josh): Add docs about resourceTier after a discussion with Sumeer.
//
// Note that cpuTimeTokenGranter does not handle replenishing the buckets.
//
// For more, see the initial design sketch:
// https://docs.google.com/document/d/1-Kr2gRFTk0QV8kBs7AXRXUwFpK2ZxR1cqIwWCuOx22Q/edit?tab=t.0
// TODO(josh): Turn into a proper design documnet.
type cpuTimeTokenGranter struct {
	requester  [numResourceTiers]requester
	metrics    *cpuTimeTokenMetrics
	timeSource timeutil.TimeSource
	mu         struct {
		// TODO(josh): I suspect putting the mutex here is better than in
		// CPUTimeTokenGrantCoordinator, but for now the decision is tentative.
		// Think better to decice when I put up a PR that introduces
		// CPUTimeTokenGrantCoordinator & cpuTimeTokenAdjusterNew.
		syncutil.Mutex
		// Invariant #1: For any two buckets A & B, if A has a lower ordinal resourceTier,
		// then A must have more tokens than B.
		// Invariant #2: For any two buckets A & B, if A & B have the same resourceTier,
		// and if A has a lower ordinal burstQualification, then A must have more tokens than B.
		//
		// Since admission deducts from all buckets, these invariants are true, so long as token bucket
		// replenishing respects it also. Token bucket replenishing is not yet implemented. See
		// tryGrantLocked for a situation where invariant #1 is relied on.
		buckets    [numResourceTiers][numBurstQualifications]tokenBucket
		tokensUsed int64
	}
}

func newCPUTimeTokenGranter(
	metrics *cpuTimeTokenMetrics, timeSource timeutil.TimeSource,
) *cpuTimeTokenGranter {
	g := &cpuTimeTokenGranter{metrics: metrics, timeSource: timeSource}
	// Buckets start at 0 tokens (exhausted) before the first refill, so
	// initialize exhaustedStart and wire the per-bucket counters.
	now := timeSource.Now()
	for tier := resourceTier(0); tier < numResourceTiers; tier++ {
		for qual := burstQualification(0); qual < numBurstQualifications; qual++ {
			g.mu.buckets[tier][qual].exhaustedStart = now
			g.mu.buckets[tier][qual].exhaustedDuration =
				metrics.ExhaustedDurationNanos[perBucketIdx(tier, qual)]
		}
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
	var buf strings.Builder
	tw := tablewriter.NewWriter(&buf)
	hdrs := [numBurstQualifications + 1]string{}
	hdrs[0] = "cpuTTG"
	for gk := canBurst; gk < numBurstQualifications; gk++ {
		hdrs[1+gk] = gk.String()
	}
	tw.SetAlignment(tablewriter.ALIGN_LEFT)
	tw.SetAutoFormatHeaders(false)
	tw.SetBorder(false)
	tw.SetColumnSeparator("")
	tw.SetHeader(hdrs[:])
	tw.SetHeaderLine(false)
	tw.SetNoWhiteSpace(true)
	tw.SetTablePadding(" ")
	tw.SetTrimWhiteSpaceAtEOL(true)

	for tier := 0; tier < int(numResourceTiers); tier++ {
		row := [1 + numBurstQualifications]string{}
		row[0] = "tier" + strconv.Itoa(tier)
		for gk := canBurst; gk < numBurstQualifications; gk++ {
			row[gk+1] = fmt.Sprint(stg.mu.buckets[tier][gk].tokens)
		}
		tw.Append(row[:])
	}
	tw.Render()
	s.SafeString(redact.SafeString(buf.String()))
}

// tryGet is the helper for implementing granter.tryGet.
func (stg *cpuTimeTokenGranter) tryGet(
	tier resourceTier, qual burstQualification, count int64,
) bool {
	stg.mu.Lock()
	defer stg.mu.Unlock()
	if stg.mu.buckets[tier][qual].tokens <= 0 {
		return false
	}
	stg.tookWithoutPermissionLocked(count)
	return true
}

// returnGrant is the helper for implementing granter.returnGrant.
func (stg *cpuTimeTokenGranter) returnGrant(count int64) {
	stg.mu.Lock()
	defer stg.mu.Unlock()
	stg.tookWithoutPermissionLocked(-count)
	// count must be positive. Thus above always adds tokens to the buckets.
	// Thus returnGrant should always attempt to grant admission to waiting requests.
	stg.grantUntilNoWaitingRequestsLocked()
}

// tookWithoutPermission is the helper for implementing granter.tookWithoutPermission.
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
		stg.metrics.TokensConsumed.Inc(count)
	} else {
		stg.metrics.TokensReturned.Inc(-count)
	}
	now := stg.timeSource.Now()
	for tier := range stg.mu.buckets {
		for qual := range stg.mu.buckets[tier] {
			newTokenCount := stg.mu.buckets[tier][qual].tokens - count
			stg.mu.buckets[tier][qual].updateTokenCount(
				newTokenCount, now, false /* flushToMetricNow */)
		}
	}
}

// grantUntilNoWaitingRequestsLocked grants admission to all queued requests
// that can be granted, given the current state of the token buckets, etc.
// It prioritizes requesters from higher class work in the sense of resourceTier
// That is, multiple waiting tier-0 requests will be granted before a single tier-1
// request.
func (stg *cpuTimeTokenGranter) grantUntilNoWaitingRequestsLocked() {
	// TODO(josh): If there are a lot of tokens, this could hold the mutex for a long
	// time. We may want to drop and reacquire the mutex after every 1000 requests or so.
	for stg.tryGrantLocked() {
	}
}

// tryGrantLocked attempts to grant admission to a single queued request.
// It prioritizes requesters from higher class work, in the sense of
// resourceTier.
func (stg *cpuTimeTokenGranter) tryGrantLocked() bool {
	for tier := range stg.requester {
		hasWaitingRequests, qual := stg.requester[tier].hasWaitingRequests()
		if !hasWaitingRequests {
			continue
		}
		if stg.mu.buckets[tier][qual].tokens <= 0 {
			// tryGrantLocked does not need to continue here, since there are
			// no more requests to grant. The detailed reason for this is:
			//
			// - stg.requester is ordered by resourceTier.
			// - Given two buckets A & B, if A is for a lower ordinal resourceTier,
			//   more tokens will be in bucket A than bucket B (see cpuTimeTokenGranter
			//   for more on this invariant).
			// - Thus, if no tokens in A, there are no tokens in B.
			//
			// Note that it is up to the requester which is the next request
			// to admit. So tryGrantLocked only needs to check the bucket that
			// corresponds to the burstQualification of that request, as is done
			// below.
			return false
		}
		tokens := stg.requester[tier].granted(noGrantChain)
		if tokens == 0 {
			// Did not accept grant.
			continue
		}
		stg.tookWithoutPermissionLocked(tokens)
		return true
	}
	return false
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
// the capacity info stored in bucketCapacities. That is, tokens that would
// bring the bucket above capacity will be discarded instead. refill attempts
// to grant admission to waiting requests in case where tokens are added to
// some bucket. updateMetrics controls whether gauge and exhausted-duration
// metrics are updated.
func (stg *cpuTimeTokenGranter) refill(
	toAdd tokenCounts, bucketCapacities capacities, updateMetrics bool,
) {
	stg.mu.Lock()
	defer stg.mu.Unlock()

	now := stg.timeSource.Now()
	var shouldGrant bool
	for tier := range stg.mu.buckets {
		for qual := range stg.mu.buckets[tier] {
			if toAdd[tier][qual] > 0 {
				shouldGrant = true
			}
			newTokenCount := stg.mu.buckets[tier][qual].tokens + toAdd[tier][qual]
			if newTokenCount > bucketCapacities[tier][qual] {
				newTokenCount = bucketCapacities[tier][qual]
			}
			stg.mu.buckets[tier][qual].updateTokenCount(
				newTokenCount, now, updateMetrics /* flushToMetricNow */)
		}
	}

	// Grant if tokens are added to any of the buckets.
	if shouldGrant {
		stg.grantUntilNoWaitingRequestsLocked()
	}
}
