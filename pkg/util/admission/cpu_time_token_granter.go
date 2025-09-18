// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
// TODO(josh): Move ths definition to admission.go. Export publicly.
type resourceTier uint8

const numResourceTiers resourceTier = 2

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
	requester [numResourceTiers]requester
	mu        struct {
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
		buckets [numResourceTiers][numBurstQualifications]tokenBucket
	}
}

// TODO(josh): Make this observable. See here for one approach:
// https://github.com/cockroachdb/cockroach/commit/06967f5fa72115348d57fc66fe895aec514261d5#diff-6212d039fab53dd464bd989bdbd537947b11d37a9c8fe77ca497870b49e28a9cR367
type tokenBucket struct {
	tokens int64
}

func (stg *cpuTimeTokenGranter) String() string {
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
	return buf.String()
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
	for tier := range stg.mu.buckets {
		for qual := range stg.mu.buckets[tier] {
			stg.mu.buckets[tier][qual].tokens -= count
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
