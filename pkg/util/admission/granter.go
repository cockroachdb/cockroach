// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/olekukonko/tablewriter"
)

// noGrantChain is a sentinel value representing that the grant is not
// responsible for continuing a grant chain. It is only used internally in
// this file -- requester implementations do not need to concern themselves
// with this value.
var noGrantChain grantChainID = 0

type requesterClose interface {
	close()
}

// slotGranter implements granterWithLockedCalls.
type slotGranter struct {
	coord      *GrantCoordinator
	workKind   WorkKind
	requester  requester
	usedSlots  int
	totalSlots int
	// skipSlotEnforcement is a dynamic value that changes based on the sampling
	// period of cpu load. It is always true when !goschedstats.Supported (see
	// https://github.com/cockroachdb/cockroach/issues/142262).
	skipSlotEnforcement bool

	usedSlotsMetric              *metric.Gauge
	slotsExhaustedDurationMetric *metric.Counter
	exhaustedStart               time.Time
}

var _ granterWithLockedCalls = &slotGranter{}
var _ granter = &slotGranter{}

// tryGet implements granter.
func (sg *slotGranter) tryGet(_ burstQualification, count int64) bool {
	return sg.coord.tryGet(sg.workKind, count, 0 /*arbitrary*/)
}

// tryGetLocked implements granterWithLockedCalls.
func (sg *slotGranter) tryGetLocked(count int64, _ int8) grantResult {
	if count != 1 {
		panic(errors.AssertionFailedf("unexpected count: %d", count))
	}
	if sg.usedSlots < sg.totalSlots || sg.skipSlotEnforcement {
		sg.usedSlots++
		if sg.usedSlots == sg.totalSlots {
			sg.exhaustedStart = timeutil.Now()
		}
		sg.usedSlotsMetric.Update(int64(sg.usedSlots))
		return grantSuccess
	}
	if sg.workKind == KVWork {
		return grantFailDueToSharedResource
	}
	return grantFailLocal
}

// returnGrant implements granter.
func (sg *slotGranter) returnGrant(count int64) {
	sg.coord.returnGrant(sg.workKind, count, 0 /*arbitrary*/)
}

// returnGrantLocked implements granterWithLockedCalls.
func (sg *slotGranter) returnGrantLocked(count int64, _ int8) {
	if count != 1 {
		panic(errors.AssertionFailedf("unexpected count: %d", count))
	}
	if sg.usedSlots == sg.totalSlots {
		now := timeutil.Now()
		exhaustedMicros := now.Sub(sg.exhaustedStart).Microseconds()
		sg.slotsExhaustedDurationMetric.Inc(exhaustedMicros)
	}
	sg.usedSlots--
	if sg.usedSlots < 0 {
		panic(errors.AssertionFailedf("used slots is negative %d", sg.usedSlots))
	}
	sg.usedSlotsMetric.Update(int64(sg.usedSlots))
}

// tookWithoutPermission implements granter.
func (sg *slotGranter) tookWithoutPermission(count int64) {
	sg.coord.tookWithoutPermission(sg.workKind, count, 0 /*arbitrary*/)
}

// tookWithoutPermissionLocked implements granterWithLockedCalls.
func (sg *slotGranter) tookWithoutPermissionLocked(count int64, _ int8) {
	if count != 1 {
		panic(errors.AssertionFailedf("unexpected count: %d", count))
	}
	sg.usedSlots++
	if sg.usedSlots == sg.totalSlots {
		sg.exhaustedStart = timeutil.Now()
	}
	sg.usedSlotsMetric.Update(int64(sg.usedSlots))
}

// continueGrantChain implements granter.
func (sg *slotGranter) continueGrantChain(grantChainID grantChainID) {
	sg.coord.continueGrantChain(sg.workKind, grantChainID)
}

// requesterHasWaitingRequests implements granterWithLockedCalls.
func (sg *slotGranter) requesterHasWaitingRequests() bool {
	hasWaiting, _ := sg.requester.hasWaitingRequests()
	return hasWaiting
}

// tryGrantLocked implements granterWithLockedCalls.
func (sg *slotGranter) tryGrantLocked(grantChainID grantChainID) grantResult {
	res := sg.tryGetLocked(1, 0 /*arbitrary*/)
	if res == grantSuccess {
		slots := sg.requester.granted(grantChainID)
		if slots == 0 {
			// Did not accept grant.
			sg.returnGrantLocked(1, 0 /*arbitrary*/)
			return grantFailLocal
		} else if slots != 1 {
			panic(errors.AssertionFailedf("unexpected count %d", slots))
		}
	}
	return res
}

//gcassert:inline
func (sg *slotGranter) setTotalSlotsLocked(totalSlots int) {
	// Mid-stack inlining.
	if totalSlots == sg.totalSlots {
		return
	}
	sg.setTotalSlotsLockedInternal(totalSlots)
}

func (sg *slotGranter) setTotalSlotsLockedInternal(totalSlots int) {
	if totalSlots > sg.totalSlots {
		if sg.totalSlots <= sg.usedSlots && totalSlots > sg.usedSlots {
			now := timeutil.Now()
			exhaustedMicros := now.Sub(sg.exhaustedStart).Microseconds()
			sg.slotsExhaustedDurationMetric.Inc(exhaustedMicros)
		}
	} else if totalSlots < sg.totalSlots {
		if sg.totalSlots > sg.usedSlots && totalSlots <= sg.usedSlots {
			sg.exhaustedStart = timeutil.Now()
		}
	}

	sg.totalSlots = totalSlots
}

// tokenGranter implements granterWithLockedCalls.
type tokenGranter struct {
	coord                *GrantCoordinator
	workKind             WorkKind
	requester            requester
	availableBurstTokens int64
	maxBurstTokens       int64
	skipTokenEnforcement bool
	// Non-nil for all uses of tokenGranter (SQLKVResponseWork and
	// SQLSQLResponseWork).
	cpuOverload cpuOverloadIndicator
}

var _ granterWithLockedCalls = &tokenGranter{}
var _ granter = &tokenGranter{}

func (tg *tokenGranter) refillBurstTokens(skipTokenEnforcement bool) {
	tg.availableBurstTokens = tg.maxBurstTokens
	tg.skipTokenEnforcement = skipTokenEnforcement
}

// tryGet implements granter.
func (tg *tokenGranter) tryGet(_ burstQualification, count int64) bool {
	return tg.coord.tryGet(tg.workKind, count, 0 /*arbitrary*/)
}

// tryGetLocked implements granterWithLockedCalls.
func (tg *tokenGranter) tryGetLocked(count int64, _ int8) grantResult {
	if tg.cpuOverload.isOverloaded() {
		return grantFailDueToSharedResource
	}
	if tg.availableBurstTokens > 0 || tg.skipTokenEnforcement {
		tg.availableBurstTokens -= count
		return grantSuccess
	}
	return grantFailLocal
}

// returnGrant implements granter.
func (tg *tokenGranter) returnGrant(count int64) {
	tg.coord.returnGrant(tg.workKind, count, 0 /*arbitrary*/)
}

// returnGrantLocked implements granterWithLockedCalls.
func (tg *tokenGranter) returnGrantLocked(count int64, _ int8) {
	tg.availableBurstTokens += count
	if tg.availableBurstTokens > tg.maxBurstTokens {
		tg.availableBurstTokens = tg.maxBurstTokens
	}
}

// tookWithoutPermission implements granter.
func (tg *tokenGranter) tookWithoutPermission(count int64) {
	tg.coord.tookWithoutPermission(tg.workKind, count, 0 /*arbitrary*/)
}

// tookWithoutPermissionLocked implements granterWithLockedCalls.
func (tg *tokenGranter) tookWithoutPermissionLocked(count int64, _ int8) {
	tg.availableBurstTokens -= count
}

// continueGrantChain implements granter.
func (tg *tokenGranter) continueGrantChain(grantChainID grantChainID) {
	tg.coord.continueGrantChain(tg.workKind, grantChainID)
}

// requesterHasWaitingRequests implements granterWithLockedCalls.
func (tg *tokenGranter) requesterHasWaitingRequests() bool {
	hasWaiting, _ := tg.requester.hasWaitingRequests()
	return hasWaiting
}

// tryGrantLocked implements granterWithLockedCalls.
func (tg *tokenGranter) tryGrantLocked(grantChainID grantChainID) grantResult {
	res := tg.tryGetLocked(1, 0 /*arbitrary*/)
	if res == grantSuccess {
		tokens := tg.requester.granted(grantChainID)
		if tokens == 0 {
			// Did not accept grant.
			tg.returnGrantLocked(1, 0 /*arbitrary*/)
			return grantFailLocal
		} else if tokens > 1 {
			tg.tookWithoutPermissionLocked(tokens-1, 0 /*arbitrary*/)
		}
	}
	return res
}

// resourceTier specifies the tier of a resource group, in descending levels of importance.
// That is, tier 0 is the most important. The token bucket sizes must be such that the
// non-burstable token bucket size of tier-i must be greater than the burstable token bucket
// size of tier-(i+1); see cpuTimeTokenGranter for details on this.
//
// The tier determination for a request happens at a layer outside the admission package.
//
// TODO(josh): Add docs re: tentative usage after a discussion with Sumeer.
//
// NB: Inter-tenant fair sharing only works within a tier. In the regular case above, the
// same tenant can be in multiple tiers based on priority. This is fine since we expect
// regular clusters will classify workloads into tiers and workloads that share the same
// tier will want fair sharing.
//
// TODO(josh): Move ths definition to admission.go & export publicly.
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
		tokens := stg.requester[tier].granted(0)
		if tokens == 0 {
			// Did not accept grant.
			continue
		}
		stg.tookWithoutPermissionLocked(tokens)
		return true
	}
	return false
}

// kvStoreTokenGranter is used for grants to KVWork to a store, that is
// limited by IO tokens. It encapsulates three granter-requester pairs, for
// the two workClasses and for incoming snapshots. The granter in these pairs
// is implemented by kvStoreTokenChildGranter, and the requester either by
// WorkQueue or SnapshotQueue. We have separate WorkQueues for the work
// classes so that we don't have a situation where tenant1's elastic work is
// queued ahead of tenant2's regular work (due to inter-tenant fairness) and
// blocks the latter from getting tokens, because elastic tokens are exhausted
// (and tokens for regular work are not exhausted).
//
// The kvStoreTokenChildGranters delegate the actual interaction to
// their "parent", kvStoreTokenGranter. For elasticWorkClass, multiple kinds
// of tokens need to be acquired, (a) the usual IO tokens (based on
// compactions out of L0 and flushes into L0) and (b) elastic disk bandwidth
// tokens, which are based on disk bandwidth as a constrained resource, and
// apply to all the elastic incoming bytes into the LSM.
type kvStoreTokenGranter struct {
	knobs             *TestingKnobs
	regularRequester  requester
	elasticRequester  requester
	snapshotRequester requester

	mu struct {
		syncutil.Mutex
		// There is no rate limiting in granting these tokens. That is, they are
		// all burst tokens.

		// The "IO" tokens represent flush/compaction capacity into/out of L0. All
		// work deducts from both availableIOTokens and availableElasticIOTokens.
		// Regular work blocks if availableIOTokens is <= 0 and elastic work
		// blocks if availableElasticIOTokens <= 0.
		availableIOTokens            [admissionpb.NumWorkClasses]int64
		elasticIOTokensUsedByElastic int64
		// TODO(aaditya): add support for read/IOPS tokens.
		// Disk bandwidth tokens.
		diskTokensAvailable diskTokens
		diskTokensError     struct {
			// prevObserved{Writes,Reads} is the observed disk metrics in the last
			// call to adjustDiskTokenErrorLocked. These are used to compute the
			// delta.
			prevObservedWrites             uint64
			prevObservedReads              uint64
			diskWriteTokensAlreadyDeducted int64
			diskReadTokensAlreadyDeducted  int64
		}
		diskTokensUsed [admissionpb.NumStoreWorkTypes]diskTokens
		// exhaustedStart is the time when the corresponding availableIOTokens
		// became <= 0. Ignored when the corresponding availableIOTokens is > 0.
		exhaustedStart [admissionpb.NumWorkClasses]time.Time
		// startingIOTokens is the number of tokens set by setAvailableTokens for
		// regular work. It is used to compute the tokens used, by computing
		// startingIOTokens-availableIOTokens[RegularWorkClass].
		startingIOTokens int64

		// Estimation models.
		l0WriteLM, l0IngestLM, ingestLM, writeAmpLM tokensLinearModel
	}

	ioTokensExhaustedDurationMetric [admissionpb.NumWorkClasses]*metric.Counter
	availableTokensMetric           [admissionpb.NumWorkClasses]*metric.Gauge
	tokensReturnedMetric            *metric.Counter
	tokensTakenMetric               *metric.Counter
}

var _ granterWithIOTokens = &kvStoreTokenGranter{}

// kvStoreTokenChildGranter handles a particular workClass. Its methods
// pass-through to the parent after adding the workClass as a parameter.
type kvStoreTokenChildGranter struct {
	workType admissionpb.StoreWorkType
	parent   *kvStoreTokenGranter
}

var _ granterWithStoreReplicatedWorkAdmitted = &kvStoreTokenChildGranter{}
var _ granter = &kvStoreTokenChildGranter{}

// tryGet implements granter.
func (cg *kvStoreTokenChildGranter) tryGet(_ burstQualification, count int64) bool {
	return cg.parent.tryGet(cg.workType, count)
}

// returnGrant implements granter.
func (cg *kvStoreTokenChildGranter) returnGrant(count int64) {
	cg.parent.returnGrant(cg.workType, count)
}

// tookWithoutPermission implements granter.
func (cg *kvStoreTokenChildGranter) tookWithoutPermission(count int64) {
	cg.parent.tookWithoutPermission(cg.workType, count)
}

// continueGrantChain implements granter.
func (cg *kvStoreTokenChildGranter) continueGrantChain(grantChainID grantChainID) {
	// Ignore since grant chains are not used for store tokens.
}

// storeWriteDone implements granterWithStoreReplicatedWorkAdmitted.
func (cg *kvStoreTokenChildGranter) storeWriteDone(
	originalTokens int64, doneInfo StoreWorkDoneInfo,
) (additionalTokens int64) {
	// NB: the token/metric adjustments we want to make here are the same as we
	// want to make through the storeReplicatedWorkAdmittedLocked, so we (ab)use
	// it. The one difference is that post token adjustments, if we observe the
	// granter was previously exhausted but is no longer so, we're allowed to
	// admit other waiting requests.
	return cg.parent.storeReplicatedWorkAdmitted(
		cg.workType, originalTokens, storeReplicatedWorkAdmittedInfo(doneInfo), true /* canGrantAnother */)
}

// storeReplicatedWorkAdmitted implements granterWithStoreReplicatedWorkAdmitted.
func (cg *kvStoreTokenChildGranter) storeReplicatedWorkAdmittedLocked(
	originalTokens int64, admittedInfo storeReplicatedWorkAdmittedInfo,
) (additionalTokens int64) {
	return cg.parent.storeReplicatedWorkAdmittedLocked(cg.workType, originalTokens, admittedInfo, false /* canGrantAnother */)
}

func (sg *kvStoreTokenGranter) tryGet(workType admissionpb.StoreWorkType, count int64) bool {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	return sg.tryGetLocked(count, workType)
}

// tryGetLocked is the real implementation of tryGet from the granter
// interface.
func (sg *kvStoreTokenGranter) tryGetLocked(count int64, wt admissionpb.StoreWorkType) bool {
	// NB: ideally if regularRequester.hasWaitingRequests() returns true and
	// wc==elasticWorkClass we should reject this request, since it means that
	// more important regular work is waiting. However, we rely on the
	// assumption that elasticWorkClass, once throttled, will have a non-empty
	// queue, and since the only case where tryGetLocked is called for
	// elasticWorkClass is when the queue is empty, this case should be rare
	// (and not cause a performance isolation failure).

	// NB: For disk write tokens, we apply `writeAmpLM` to account for write
	// amplification. This model is used again later in
	// `storeReplicatedWorkAdmittedLocked()`. There is an obvious gap in
	// accounting here, since the model could change between the two calls for the
	// same request. For example:
	// (1) We have a request that requests 50 tokens, and write-amp LM is
	//     currently 10.0x + 1. We will deduct 501 tokens for disk writes.
	// (2) Before we adjust the actual write bytes used by the write request, the
	//     write-amp model is updated to 5.0x + 1.
	// (3) In `storeReplicatedWorkAdmittedLocked()`, we learn that the request
	//     used 200 actual bytes. Now we will apply the new LM to get 251 tokens
	//     initially deducted, and apply the LM for 1001 actual bytes used. We
	//     will now deduct 750 additional tokens from the bucket. Now we have
	//     deducted 1251 tokens rather than 1001.
	// This can also go the other way, where we deduct fewer tokens than actually
	// needed. We are generally okay with this since the model changes
	// infrequently (every 15s), and the disk bandwidth limiter is designed to
	// generally under admit and only pace elastic work.
	diskWriteTokens := count
	if wt != admissionpb.SnapshotIngestStoreWorkType {
		// Snapshot ingests do not incur the write amplification described above, so
		// we skip applying the model for those writes.
		diskWriteTokens = sg.mu.writeAmpLM.applyLinearModel(count)
	}
	switch wt {
	case admissionpb.RegularStoreWorkType:
		if sg.mu.availableIOTokens[admissionpb.RegularWorkClass] > 0 {
			sg.subtractIOTokensLocked(count, count, false)
			sg.mu.diskTokensAvailable.writeByteTokens -= diskWriteTokens
			sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted += diskWriteTokens
			sg.mu.diskTokensUsed[wt].writeByteTokens += diskWriteTokens
			return true
		}
	case admissionpb.ElasticStoreWorkType:
		if sg.mu.diskTokensAvailable.writeByteTokens > 0 &&
			sg.mu.availableIOTokens[admissionpb.RegularWorkClass] > 0 &&
			sg.mu.availableIOTokens[admissionpb.ElasticWorkClass] > 0 {
			sg.subtractIOTokensLocked(count, count, false)
			sg.mu.elasticIOTokensUsedByElastic += count
			sg.mu.diskTokensAvailable.writeByteTokens -= diskWriteTokens
			sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted += diskWriteTokens
			sg.mu.diskTokensUsed[wt].writeByteTokens += diskWriteTokens
			return true
		}
	case admissionpb.SnapshotIngestStoreWorkType:
		// Snapshot ingests do not go into L0, so we only subject them to
		// writeByteTokens.
		if sg.mu.diskTokensAvailable.writeByteTokens > 0 {
			sg.mu.diskTokensAvailable.writeByteTokens -= diskWriteTokens
			sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted += diskWriteTokens
			sg.mu.diskTokensUsed[wt].writeByteTokens += diskWriteTokens
			return true
		}
	}
	return false
}

func (sg *kvStoreTokenGranter) returnGrant(workType admissionpb.StoreWorkType, count int64) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	sg.returnGrantLocked(count, workType)
	// Try granting, since tokens have been returned.
	sg.tryGrantLocked()
}

// returnGrantLocked is the real implementation of returnGrant from the
// granter interface.
func (sg *kvStoreTokenGranter) returnGrantLocked(count int64, wt admissionpb.StoreWorkType) {
	// Return store tokens.
	sg.subtractTokensForStoreWorkTypeLocked(wt, -count)
}

// subtractTokensForStoreWorkTypeLocked is a helper function that subtracts
// tokens from all store tokens for a given admissionpb.StoreWorkType. Count can
// be negative to add tokens.
func (sg *kvStoreTokenGranter) subtractTokensForStoreWorkTypeLocked(
	wt admissionpb.StoreWorkType, count int64,
) {
	if wt != admissionpb.SnapshotIngestStoreWorkType {
		// Adjust count tokens for "IO tokens".
		sg.subtractIOTokensLocked(count, count, false)
	}
	if wt == admissionpb.ElasticStoreWorkType {
		sg.mu.elasticIOTokensUsedByElastic += count
	}
	// Adjust tokens for disk bandwidth bucket.
	switch wt {
	case admissionpb.RegularStoreWorkType, admissionpb.ElasticStoreWorkType:
		diskTokenCount := sg.mu.writeAmpLM.applyLinearModel(count)
		sg.mu.diskTokensAvailable.writeByteTokens -= diskTokenCount
		sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted += diskTokenCount
		sg.mu.diskTokensUsed[wt].writeByteTokens += diskTokenCount
	case admissionpb.SnapshotIngestStoreWorkType:
		// Do not apply the writeAmpLM since these writes do not incur additional
		// write-amp.
		sg.mu.diskTokensAvailable.writeByteTokens -= count
		sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted += count
		sg.mu.diskTokensUsed[wt].writeByteTokens += count
	}
}

func (sg *kvStoreTokenGranter) adjustDiskTokenError(m StoreMetrics) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	sg.adjustDiskTokenErrorLocked(m.DiskStats.BytesRead, m.DiskStats.BytesWritten)
}

// adjustDiskTokenErrorLocked is used to account for extra reads and writes that
// are in excess of tokens already deducted.
//
// (1) For writes, we deduct tokens at admission for each request. If the actual
// writes seen on disk for a given interval is higher than the tokens already
// deducted, the delta is the write error. This value is then subtracted from
// available disk tokens.
//
// (2) For reads, we do not deduct any tokens at admission. However, we deduct
// tokens in advance during token estimation in the diskBandwidthLimiter for the
// next adjustment interval. These pre-deducted tokens are then allocated at the
// same interval as write tokens. Any additional reads in the interval are
// considered error and are subtracted from the available disk write tokens.
//
// For both reads, and writes, we reset the
// disk{read,write}TokensAlreadyDeducted to 0 for the next adjustment interval.
// For writes, we do this so that we are accounting for errors only in the given
// interval, and not across them. For reads, this is so that we don't grow
// arbitrarily large "burst" tokens, since they are not capped to an allocation
// period.
func (sg *kvStoreTokenGranter) adjustDiskTokenErrorLocked(readBytes uint64, writeBytes uint64) {
	intWrites := int64(writeBytes - sg.mu.diskTokensError.prevObservedWrites)
	intReads := int64(readBytes - sg.mu.diskTokensError.prevObservedReads)

	// Compensate for error due to writes.
	writeError := intWrites - sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted
	if writeError > 0 {
		sg.mu.diskTokensAvailable.writeByteTokens -= writeError
	}

	// Compensate for error due to reads.
	readError := intReads - sg.mu.diskTokensError.diskReadTokensAlreadyDeducted
	if readError > 0 {
		sg.mu.diskTokensAvailable.writeByteTokens -= readError
	}

	// We have compensated for error, if any, in this interval, so we reset the
	// deducted count for the next compensation interval.
	sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted = 0
	sg.mu.diskTokensError.diskReadTokensAlreadyDeducted = 0

	sg.mu.diskTokensError.prevObservedWrites = writeBytes
	sg.mu.diskTokensError.prevObservedReads = readBytes
}

func (sg *kvStoreTokenGranter) tookWithoutPermission(
	workType admissionpb.StoreWorkType, count int64,
) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	sg.tookWithoutPermissionLocked(count, workType)
}

// tookWithoutPermissionLocked is the real implementation of
// tookWithoutPermission from the granter interface.
func (sg *kvStoreTokenGranter) tookWithoutPermissionLocked(
	count int64, wt admissionpb.StoreWorkType,
) {
	// Deduct store tokens.
	sg.subtractTokensForStoreWorkTypeLocked(wt, count)
}

// subtractIOTokensLocked is a helper function that subtracts count tokens (count
// can be negative, in which case this is really an addition).
func (sg *kvStoreTokenGranter) subtractIOTokensLocked(
	count int64, elasticCount int64, settingAvailableTokens bool,
) {
	sg.subtractTokensLockedForWorkClass(admissionpb.RegularWorkClass, count, settingAvailableTokens)
	sg.subtractTokensLockedForWorkClass(admissionpb.ElasticWorkClass, elasticCount, settingAvailableTokens)
	if !settingAvailableTokens {
		if count > 0 {
			sg.tokensTakenMetric.Inc(count)
		} else {
			sg.tokensReturnedMetric.Inc(-count)
		}
	}
}

func (sg *kvStoreTokenGranter) subtractTokensLockedForWorkClass(
	wc admissionpb.WorkClass, count int64, settingAvailableTokens bool,
) {
	avail := sg.mu.availableIOTokens[wc]
	sg.mu.availableIOTokens[wc] -= count
	sg.availableTokensMetric[wc].Update(sg.mu.availableIOTokens[wc])
	if count > 0 && avail > 0 && sg.mu.availableIOTokens[wc] <= 0 {
		// Transition from > 0 to <= 0.
		sg.mu.exhaustedStart[wc] = timeutil.Now()
	} else if count < 0 && avail <= 0 && (sg.mu.availableIOTokens[wc] > 0 || settingAvailableTokens) {
		// Transition from <= 0 to > 0, or if we're newly setting available
		// tokens. The latter ensures that if the available tokens stay <= 0, we
		// don't show a sudden change in the metric after minutes of exhaustion
		// (we had observed such behavior prior to this change).
		now := timeutil.Now()
		exhaustedMicros := now.Sub(sg.mu.exhaustedStart[wc]).Microseconds()
		sg.ioTokensExhaustedDurationMetric[wc].Inc(exhaustedMicros)
		if sg.mu.availableIOTokens[wc] <= 0 {
			sg.mu.exhaustedStart[wc] = now
		}
	}
}

func (sg *kvStoreTokenGranter) tryGrant() {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	sg.tryGrantLocked()
}

// tryGrantLocked attempts to grant to as many requests as possible.
func (sg *kvStoreTokenGranter) tryGrantLocked() {
	for sg.tryGrantLockedOne() {
	}
}

// tryGrantLocked is used to attempt to grant to waiting requests. Used by
// storeGrantCoordinator. It successfully grants to at most one waiting
// request. If there are no waiting requests, or all waiters reject the grant,
// it returns false.
func (sg *kvStoreTokenGranter) tryGrantLockedOne() bool {
	// NB: We grant work in the following priority order: regular, snapshot
	// ingest, elastic work. Snapshot ingests are a special type of elastic work.
	// They queue separately in the SnapshotQueue and get priority over other
	// elastic work since they are used for node re-balancing and up-replication,
	// which are typically higher priority than other background writes.
	for wt := admissionpb.StoreWorkType(0); wt < admissionpb.NumStoreWorkTypes; wt++ {
		req := sg.regularRequester
		if wt == admissionpb.ElasticStoreWorkType {
			req = sg.elasticRequester
		} else if wt == admissionpb.SnapshotIngestStoreWorkType {
			req = sg.snapshotRequester
		}
		hasWaiting, _ := req.hasWaitingRequests()
		if hasWaiting {
			res := sg.tryGetLocked(1, wt)
			if res {
				tookTokenCount := req.granted(noGrantChain)
				if tookTokenCount == 0 {
					// Did not accept grant.
					sg.returnGrantLocked(1, wt)
					// Continue with the loop since this requester does not have waiting
					// requests. If the loop terminates we will correctly return
					// grantFailLocal.
				} else {
					// May have taken more.
					if tookTokenCount > 1 {
						sg.tookWithoutPermissionLocked(tookTokenCount-1, wt)
					}
					return true
				}
			} else {
				// Was not able to get token. Do not continue with looping to grant to
				// less important work (though it would be harmless since won't be
				// able to get a token for that either).
				return res
			}
		}
	}
	return false
}

// setAvailableTokens implements granterWithIOTokens.
func (sg *kvStoreTokenGranter) setAvailableTokens(
	ioTokens int64,
	elasticIOTokens int64,
	diskWriteTokens int64,
	diskReadTokens int64,
	ioTokenCapacity int64,
	elasticIOTokenCapacity int64,
	diskWriteTokensCapacity int64,
	lastTick bool,
) (ioTokensUsed int64, ioTokensUsedByElasticWork int64) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	ioTokensUsed = sg.mu.startingIOTokens - sg.mu.availableIOTokens[admissionpb.RegularWorkClass]
	ioTokensUsedByElasticWork = sg.mu.elasticIOTokensUsedByElastic
	sg.mu.elasticIOTokensUsedByElastic = 0

	// It is possible for availableIOTokens to be negative because of
	// tookWithoutPermission or because tryGet will satisfy requests until
	// availableIOTokens become <= 0. We want to remember this previous
	// over-allocation.
	sg.subtractIOTokensLocked(-ioTokens, -elasticIOTokens, true)
	if sg.mu.availableIOTokens[admissionpb.RegularWorkClass] > ioTokenCapacity {
		sg.mu.availableIOTokens[admissionpb.RegularWorkClass] = ioTokenCapacity
	}
	if sg.mu.availableIOTokens[admissionpb.ElasticWorkClass] > elasticIOTokenCapacity {
		sg.mu.availableIOTokens[admissionpb.ElasticWorkClass] = elasticIOTokenCapacity
	}
	// availableIOTokens[ElasticWorkClass] can become very negative since it can
	// be fewer than the tokens for regular work, and regular work deducts from it
	// without blocking. This behavior is desirable, but we don't want deficits to
	// accumulate indefinitely. We've found that resetting on the lastTick
	// provides a good enough frequency for resetting the deficit. That is, we are
	// resetting every 15s.
	if lastTick {
		sg.mu.availableIOTokens[admissionpb.ElasticWorkClass] =
			max(sg.mu.availableIOTokens[admissionpb.ElasticWorkClass], 0)
		// It is possible that availableIOTokens[RegularWorkClass] is negative, in
		// which case we want availableIOTokens[ElasticWorkClass] to not exceed it.
		sg.mu.availableIOTokens[admissionpb.ElasticWorkClass] =
			min(sg.mu.availableIOTokens[admissionpb.ElasticWorkClass], sg.mu.availableIOTokens[admissionpb.RegularWorkClass])
		// We also want to avoid very negative disk write tokens, so we reset them.
		sg.mu.diskTokensAvailable.writeByteTokens = max(sg.mu.diskTokensAvailable.writeByteTokens, 0)
	}
	var w admissionpb.WorkClass
	for w = 0; w < admissionpb.NumWorkClasses; w++ {
		sg.availableTokensMetric[w].Update(sg.mu.availableIOTokens[w])
	}
	sg.mu.startingIOTokens = sg.mu.availableIOTokens[admissionpb.RegularWorkClass]

	// Allocate disk write and read tokens.
	sg.mu.diskTokensAvailable.writeByteTokens += diskWriteTokens
	if sg.mu.diskTokensAvailable.writeByteTokens > diskWriteTokensCapacity {
		sg.mu.diskTokensAvailable.writeByteTokens = diskWriteTokensCapacity
	}
	// NB: We don't cap the disk read tokens as they are only deducted during the
	// error accounting loop. So essentially, we give reads the "burst" capacity
	// of the error accounting interval. See `adjustDiskTokenErrorLocked` for the
	// error accounting logic, and where we reset this bucket to 0.
	sg.mu.diskTokensError.diskReadTokensAlreadyDeducted += diskReadTokens

	return ioTokensUsed, ioTokensUsedByElasticWork
}

// getDiskTokensUsedAndResetLocked implements granterWithIOTokens.
func (sg *kvStoreTokenGranter) getDiskTokensUsedAndReset() (
	usedTokens [admissionpb.NumStoreWorkTypes]diskTokens,
) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	for i := 0; i < admissionpb.NumStoreWorkTypes; i++ {
		usedTokens[i] = sg.mu.diskTokensUsed[i]
		sg.mu.diskTokensUsed[i] = diskTokens{}
	}
	return usedTokens
}

// setAdmittedModelsLocked implements granterWithIOTokens.
func (sg *kvStoreTokenGranter) setLinearModels(
	l0WriteLM tokensLinearModel,
	l0IngestLM tokensLinearModel,
	ingestLM tokensLinearModel,
	writeAmpLM tokensLinearModel,
) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	sg.mu.l0WriteLM = l0WriteLM
	sg.mu.l0IngestLM = l0IngestLM
	sg.mu.ingestLM = ingestLM
	sg.mu.writeAmpLM = writeAmpLM
}

func (sg *kvStoreTokenGranter) storeReplicatedWorkAdmitted(
	wt admissionpb.StoreWorkType,
	originalTokens int64,
	admittedInfo storeReplicatedWorkAdmittedInfo,
	canGrantAnother bool,
) (additionalTokens int64) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	return sg.storeReplicatedWorkAdmittedLocked(wt, originalTokens, admittedInfo, canGrantAnother)
}

func (sg *kvStoreTokenGranter) storeReplicatedWorkAdmittedLocked(
	wt admissionpb.StoreWorkType,
	originalTokens int64,
	admittedInfo storeReplicatedWorkAdmittedInfo,
	canGrantAnother bool,
) (additionalTokens int64) {
	// Reminder: coord.mu protects the state in the kvStoreTokenGranter.
	wc := admissionpb.WorkClassFromStoreWorkType(wt)
	exhaustedFunc := func() bool {
		return sg.mu.availableIOTokens[admissionpb.RegularWorkClass] <= 0 ||
			(wc == admissionpb.ElasticWorkClass && (sg.mu.diskTokensAvailable.writeByteTokens <= 0 ||
				sg.mu.availableIOTokens[admissionpb.ElasticWorkClass] <= 0))
	}
	wasExhausted := exhaustedFunc()
	actualL0WriteTokens := sg.mu.l0WriteLM.applyLinearModel(admittedInfo.WriteBytes)
	actualL0IngestTokens := sg.mu.l0IngestLM.applyLinearModel(admittedInfo.IngestedBytes)
	actualL0Tokens := actualL0WriteTokens + actualL0IngestTokens
	additionalL0TokensNeeded := actualL0Tokens - originalTokens
	sg.subtractIOTokensLocked(additionalL0TokensNeeded, additionalL0TokensNeeded, false)
	if wt == admissionpb.ElasticStoreWorkType {
		sg.mu.elasticIOTokensUsedByElastic += additionalL0TokensNeeded
	}

	// Adjust disk write tokens.
	ingestIntoLSM := sg.mu.ingestLM.applyLinearModel(admittedInfo.IngestedBytes)
	totalBytesIntoLSM := actualL0WriteTokens + ingestIntoLSM
	actualDiskWriteTokens := sg.mu.writeAmpLM.applyLinearModel(totalBytesIntoLSM)
	originalDiskTokens := sg.mu.writeAmpLM.applyLinearModel(originalTokens)
	additionalDiskWriteTokens := actualDiskWriteTokens - originalDiskTokens
	sg.mu.diskTokensAvailable.writeByteTokens -= additionalDiskWriteTokens
	sg.mu.diskTokensUsed[wt].writeByteTokens += additionalDiskWriteTokens

	if canGrantAnother && (additionalL0TokensNeeded < 0) {
		isExhausted := exhaustedFunc()
		if (wasExhausted && !isExhausted) || sg.knobs.AlwaysTryGrantWhenAdmitted {
			sg.tryGrantLocked()
		}
	}
	// For multi-tenant fairness accounting, we choose to ignore disk bandwidth
	// tokens. Ideally, we'd have multiple resource dimensions for the fairness
	// decisions, but we don't necessarily need something more sophisticated
	// like "Dominant Resource Fairness".
	return additionalL0TokensNeeded
}

// StoreMetrics are the metrics and some config information for a store.
type StoreMetrics struct {
	StoreID roachpb.StoreID
	*pebble.Metrics
	WriteStallCount int64
	// Optional.
	DiskStats DiskStats
	// Config.
	MemTableSizeForStopWrites uint64
}

// DiskStats provide low-level stats about the disk resources used for a
// store. We assume that the disk is not shared across multiple stores.
// However, transient and moderate usage that is not due to the store is
// tolerable, since the diskBandwidthLimiter is only using this to compute
// elastic tokens and is designed to deal with significant attribution
// uncertainty.
//
// DiskStats are not always populated. A ProvisionedBandwidth of 0 represents
// that the stats should be ignored.
type DiskStats struct {
	// BytesRead is the cumulative bytes read.
	BytesRead uint64
	// BytesWritten is the cumulative bytes written.
	BytesWritten uint64
	// ProvisionedBandwidth is the total provisioned bandwidth in bytes/s.
	ProvisionedBandwidth int64
}

var (
	totalSlots = metric.Metadata{
		Name:        "admission.granter.total_slots.kv",
		Help:        "Total slots for kv work",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	usedSlots = metric.Metadata{
		// Note: we append a WorkKind string to this name.
		Name:        "admission.granter.used_slots.",
		Help:        "Used slots",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	// NB: this metric is independent of whether slots enforcement is happening
	// or not.
	kvSlotsExhaustedDuration = metric.Metadata{
		Name:        "admission.granter.slots_exhausted_duration.kv",
		Help:        "Total duration when KV slots were exhausted, in micros",
		Measurement: "Microseconds",
		Unit:        metric.Unit_COUNT,
	}
	// We have a metric for both short and long period. These metrics use the
	// period provided in CPULoad and not wall time. So if the sum of the rate
	// of these two is < 1sec/sec, the CPULoad ticks are not happening at the
	// expected frequency (this could happen due to CPU overload).
	kvCPULoadShortPeriodDuration = metric.Metadata{
		Name:        "admission.granter.cpu_load_short_period_duration.kv",
		Help:        "Total duration when CPULoad was being called with a short period, in micros",
		Measurement: "Microseconds",
		Unit:        metric.Unit_COUNT,
	}
	kvCPULoadLongPeriodDuration = metric.Metadata{
		Name:        "admission.granter.cpu_load_long_period_duration.kv",
		Help:        "Total duration when CPULoad was being called with a long period, in micros",
		Measurement: "Microseconds",
		Unit:        metric.Unit_COUNT,
	}
	kvSlotAdjusterIncrements = metric.Metadata{
		Name:        "admission.granter.slot_adjuster_increments.kv",
		Help:        "Number of increments of the total KV slots",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	kvSlotAdjusterDecrements = metric.Metadata{
		Name:        "admission.granter.slot_adjuster_decrements.kv",
		Help:        "Number of decrements of the total KV slots",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	kvIOTokensExhaustedDuration = metric.Metadata{
		Name:        "admission.granter.io_tokens_exhausted_duration.kv",
		Help:        "Total duration when IO tokens were exhausted, in micros",
		Measurement: "Microseconds",
		Unit:        metric.Unit_COUNT,
	}
	kvElasticIOTokensExhaustedDuration = metric.Metadata{
		Name:        "admission.granter.elastic_io_tokens_exhausted_duration.kv",
		Help:        "Total duration when Elastic IO tokens were exhausted, in micros",
		Measurement: "Microseconds",
		Unit:        metric.Unit_COUNT,
	}
	kvIOTokensTaken = metric.Metadata{
		Name:        "admission.granter.io_tokens_taken.kv",
		Help:        "Total number of tokens taken",
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}
	kvIOTokensReturned = metric.Metadata{
		Name:        "admission.granter.io_tokens_returned.kv",
		Help:        "Total number of tokens returned",
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}
	kvIOTokensBypassed = metric.Metadata{
		Name:        "admission.granter.io_tokens_bypassed.kv",
		Help:        "Total number of tokens taken by work bypassing admission control (for example, follower writes without flow control)",
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}
	kvIOTokensAvailable = metric.Metadata{
		Name:        "admission.granter.io_tokens_available.kv",
		Help:        "Number of tokens available",
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}
	kvElasticIOTokensAvailable = metric.Metadata{
		Name:        "admission.granter.elastic_io_tokens_available.kv",
		Help:        "Number of tokens available",
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}
	l0CompactedBytes = metric.Metadata{
		Name:        "admission.l0_compacted_bytes.kv",
		Help:        "Total bytes compacted out of L0 (used to generate IO tokens)",
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}
	l0TokensProduced = metric.Metadata{
		Name:        "admission.l0_tokens_produced.kv",
		Help:        "Total bytes produced for L0 writes",
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}
)

// TODO(irfansharif): we are lacking metrics for IO tokens and load, including
// metrics from helper classes used by ioLoadListener, like the code in
// disk_bandwidth.go and store_token_estimation.go. Additionally, what we have
// below is per node, while we want such metrics per store. We should add
// these metrics via StoreGrantCoordinators.SetPebbleMetricsProvider, which is
// used to construct the per-store GrantCoordinator. These metrics should be
// embedded in kvserver.StoreMetrics. We should also separate the metrics
// related to cpu slots from the IO metrics.
