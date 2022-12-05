// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package admission

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// EnabledSoftSlotGranting can be set to false to disable soft slot granting.
var EnabledSoftSlotGranting = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"admission.soft_slot_granting.enabled",
	"soft slot granting is disabled if this setting is set to false",
	true,
)

// noGrantChain is a sentinel value representing that the grant is not
// responsible for continuing a grant chain. It is only used internally in
// this file -- requester implementations do not need to concern themselves
// with this value.
var noGrantChain grantChainID = 0

type requesterClose interface {
	close()
}

// For the cpu-bound slot case we have background activities (like Pebble
// compactions) that would like to utilize additional slots if available (e.g.
// to do concurrent compression of ssblocks). These activities do not want to
// wait for a slot, since they can proceed without the slot at their usual
// slower pace (e.g. without doing concurrent compression). They also are
// sensitive to small overheads in their tight loops, and cannot afford the
// overhead of interacting with admission control at a fine granularity (like
// asking for a slot when compressing each ssblock). A coarse granularity
// interaction causes a delay in returning slots to admission control, and we
// don't want that delay to cause admission delay for normal work. Hence, we
// model slots granted to background activities as "soft-slots". Think of
// regular used slots as "hard-slots", in that we assume that the holder of
// the slot is still "using" it, while a soft-slot is "squishy" and in some
// cases we can pretend that it is not being used. Say we are allowed
// to allocate up to M slots. In this scheme, when allocating a soft-slot
// one must conform to usedSoftSlots+usedSlots <= M, and when allocating
// a regular (hard) slot one must conform to usedSlots <= M.
//
// That is, soft-slots allow for over-commitment until the soft-slots are
// returned, which may mean some additional queueing in the goroutine
// scheduler.
//
// We have another wrinkle in that we do not want to maintain a single M. For
// these optional background activities we desire to do them only when the
// load is low enough. This is because at high load, all work suffers from
// additional queueing in the goroutine scheduler. So we want to make sure
// regular work does not suffer such goroutine scheduler queueing because we
// granted too many soft-slots and caused CPU utilization to be high. So we
// maintain two kinds of M, totalHighLoadSlots and totalModerateLoadSlots.
// totalHighLoadSlots are estimated so as to allow CPU utilization to be high,
// while totalModerateLoadSlots are trying to keep queuing in the goroutine
// scheduler to a lower level. So the revised equations for allocation are:
// - Allocating a soft-slot: usedSoftSlots+usedSlots <= totalModerateLoadSlots
// - Allocating a regular slot: usedSlots <= totalHighLoadSlots
//
// NB: we may in the future add other kinds of background activities that do
// not have a lag in interacting with admission control, but want to schedule
// them only under moderate load. Those activities will be counted in
// usedSlots but when granting a slot to such an activity, the equation will
// be usedSoftSlots+usedSlots <= totalModerateLoadSlots.
//
// That is, let us not confuse that moderate load slot allocation is only for
// soft-slots. Soft-slots are introduced only for squishiness.
//
// slotGranter implements granterWithLockedCalls.
type slotGranter struct {
	coord                  *GrantCoordinator
	workKind               WorkKind
	requester              requester
	usedSlots              int
	usedSoftSlots          int
	totalHighLoadSlots     int
	totalModerateLoadSlots int
	skipSlotEnforcement    bool

	// Optional. Nil for a slotGranter used for KVWork since the slots for that
	// slotGranter are directly adjusted by the kvSlotAdjuster (using the
	// kvSlotAdjuster here would provide a redundant identical signal).
	cpuOverload cpuOverloadIndicator

	usedSlotsMetric     *metric.Gauge
	usedSoftSlotsMetric *metric.Gauge
}

var _ granterWithLockedCalls = &slotGranter{}
var _ granter = &slotGranter{}

// grantKind implements granter.
func (sg *slotGranter) grantKind() grantKind {
	return slot
}

// tryGet implements granter.
func (sg *slotGranter) tryGet(count int64) bool {
	return sg.coord.tryGet(sg.workKind, count, 0 /*arbitrary*/)
}

// tryGetLocked implements granterWithLockedCalls.
func (sg *slotGranter) tryGetLocked(count int64, _ int8) grantResult {
	if count != 1 {
		panic(errors.AssertionFailedf("unexpected count: %d", count))
	}
	if sg.cpuOverload != nil && sg.cpuOverload.isOverloaded() {
		return grantFailDueToSharedResource
	}
	if sg.usedSlots < sg.totalHighLoadSlots || sg.skipSlotEnforcement {
		sg.usedSlots++
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

func (sg *slotGranter) tryGetSoftSlots(count int) int {
	sg.coord.mu.Lock()
	defer sg.coord.mu.Unlock()
	spareModerateLoadSlots := sg.totalModerateLoadSlots - sg.usedSoftSlots - sg.usedSlots
	if spareModerateLoadSlots <= 0 {
		return 0
	}
	allocatedSlots := count
	if allocatedSlots > spareModerateLoadSlots {
		allocatedSlots = spareModerateLoadSlots
	}
	sg.usedSoftSlots += allocatedSlots
	sg.usedSoftSlotsMetric.Update(int64(sg.usedSoftSlots))
	return allocatedSlots
}

func (sg *slotGranter) returnSoftSlots(count int) {
	sg.coord.mu.Lock()
	defer sg.coord.mu.Unlock()
	sg.usedSoftSlots -= count
	sg.usedSoftSlotsMetric.Update(int64(sg.usedSoftSlots))
	if sg.usedSoftSlots < 0 {
		panic("used soft slots is negative")
	}
}

// returnGrantLocked implements granterWithLockedCalls.
func (sg *slotGranter) returnGrantLocked(count int64, _ int8) {
	if count != 1 {
		panic(errors.AssertionFailedf("unexpected count: %d", count))
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
	sg.usedSlotsMetric.Update(int64(sg.usedSlots))
}

// continueGrantChain implements granter.
func (sg *slotGranter) continueGrantChain(grantChainID grantChainID) {
	sg.coord.continueGrantChain(sg.workKind, grantChainID)
}

// requesterHasWaitingRequests implements granterWithLockedCalls.
func (sg *slotGranter) requesterHasWaitingRequests() bool {
	return sg.requester.hasWaitingRequests()
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

// tokenGranter implements granterWithLockedCalls.
type tokenGranter struct {
	coord                *GrantCoordinator
	workKind             WorkKind
	requester            requester
	availableBurstTokens int64
	maxBurstTokens       int64
	skipTokenEnforcement bool
	// Optional. Practically, both uses of tokenGranter, for SQLKVResponseWork
	// and SQLSQLResponseWork have a non-nil value. We don't expect to use
	// memory overload indicators here since memory accounting and disk spilling
	// is what should be tasked with preventing OOMs, and we want to finish
	// processing this lower-level work.
	cpuOverload cpuOverloadIndicator
}

var _ granterWithLockedCalls = &tokenGranter{}
var _ granter = &tokenGranter{}

func (tg *tokenGranter) refillBurstTokens(skipTokenEnforcement bool) {
	tg.availableBurstTokens = tg.maxBurstTokens
	tg.skipTokenEnforcement = skipTokenEnforcement
}

// grantKind implements granter.
func (tg *tokenGranter) grantKind() grantKind {
	return token
}

// tryGet implements granter.
func (tg *tokenGranter) tryGet(count int64) bool {
	return tg.coord.tryGet(tg.workKind, count, 0 /*arbitrary*/)
}

// tryGetLocked implements granterWithLockedCalls.
func (tg *tokenGranter) tryGetLocked(count int64, _ int8) grantResult {
	if tg.cpuOverload != nil && tg.cpuOverload.isOverloaded() {
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
	return tg.requester.hasWaitingRequests()
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

type workClass int8

const (
	// regularWorkClass is for work corresponding to workloads that are
	// throughput and latency sensitive.
	regularWorkClass workClass = iota
	// elasticWorkClass is for work corresponding to workloads that can handle
	// reduced throughput, possibly by taking longer to finish a workload. It is
	// not latency sensitive.
	elasticWorkClass
	numWorkClasses
)

func workClassFromPri(pri admissionpb.WorkPriority) workClass {
	class := regularWorkClass
	if pri < admissionpb.NormalPri {
		class = elasticWorkClass
	}
	return class
}

func (w workClass) String() string {
	switch w {
	case regularWorkClass:
		return "regular"
	case elasticWorkClass:
		return "elastic"
	default:
		return "<unknown>"
	}
}

// kvStoreTokenGranter implements granterWithLockedCalls. It is used for
// grants to KVWork to a store, that is limited by IO tokens. It encapsulates
// two granter-requester pairs, for the two workClasses. The granter in these
// pairs is implemented by kvStoreTokenChildGranter, and the requester by
// WorkQueue. We have separate WorkQueues for these work classes so that we
// don't have a situation where tenant1's elastic work is queued ahead of
// tenant2's regular work (due to inter-tenant fairness) and blocks the latter
// from getting tokens, because elastic tokens are exhausted (and tokens for
// regular work are not exhausted).
//
// The kvStoreTokenChildGranters delegate the actual interaction to
// their "parent", kvStoreTokenGranter. For elasticWorkClass, multiple kinds
// of tokens need to be acquired, (a) the usual IO tokens (based on
// compactions out of L0 and flushes into L0) and (b) elastic disk bandwidth
// tokens, which are based on disk bandwidth as a constrained resource, and
// apply to all the elastic incoming bytes into the LSM.
type kvStoreTokenGranter struct {
	coord            *GrantCoordinator
	regularRequester requester
	elasticRequester requester
	// There is no rate limiting in granting these tokens. That is, they are all
	// burst tokens.
	availableIOTokens int64
	// startingIOTokens is the number of tokens set by
	// setAvailableIOTokensLocked. It is used to compute the tokens used, by
	// computing startingIOTokens-availableIOTokens.
	startingIOTokens                int64
	ioTokensExhaustedDurationMetric *metric.Counter
	exhaustedStart                  time.Time

	// Disk bandwidth tokens.
	elasticDiskBWTokensAvailable int64
	diskBWTokensUsed             [numWorkClasses]int64

	// Estimation models.
	l0WriteLM, l0IngestLM, ingestLM tokensLinearModel
}

var _ granterWithLockedCalls = &kvStoreTokenGranter{}
var _ granterWithIOTokens = &kvStoreTokenGranter{}

// kvStoreTokenChildGranter handles a particular workClass. Its methods
// pass-through to the parent after adding the workClass as a parameter.
type kvStoreTokenChildGranter struct {
	workClass workClass
	parent    *kvStoreTokenGranter
}

var _ granterWithStoreWriteDone = &kvStoreTokenChildGranter{}
var _ granter = &kvStoreTokenChildGranter{}

// grantKind implements granter.
func (cg *kvStoreTokenChildGranter) grantKind() grantKind {
	return token
}

// tryGet implements granter.
func (cg *kvStoreTokenChildGranter) tryGet(count int64) bool {
	return cg.parent.tryGet(cg.workClass, count)
}

// returnGrant implements granter.
func (cg *kvStoreTokenChildGranter) returnGrant(count int64) {
	cg.parent.returnGrant(cg.workClass, count)
}

// tookWithoutPermission implements granter.
func (cg *kvStoreTokenChildGranter) tookWithoutPermission(count int64) {
	cg.parent.tookWithoutPermission(cg.workClass, count)
}

// continueGrantChain implements granter.
func (cg *kvStoreTokenChildGranter) continueGrantChain(grantChainID grantChainID) {
	// Ignore since grant chains are not used for store tokens.
}

// storeWriteDone implements granterWithStoreWriteDone.
func (cg *kvStoreTokenChildGranter) storeWriteDone(
	originalTokens int64, doneInfo StoreWorkDoneInfo,
) (additionalTokens int64) {
	return cg.parent.storeWriteDone(cg.workClass, originalTokens, doneInfo)
}

// storeWriteDoneLocked implements granterWithStoreWriteDone.
func (cg *kvStoreTokenChildGranter) storeWriteDoneLocked(
	originalTokens int64, doneInfo StoreWorkDoneInfo,
) (additionalTokensNeeded int64) {
	return cg.parent.storeWriteDoneLocked(cg.workClass, originalTokens, doneInfo)
}

func (sg *kvStoreTokenGranter) tryGet(workClass workClass, count int64) bool {
	return sg.coord.tryGet(KVWork, count, int8(workClass))
}

// tryGetLocked implements granterWithLockedCalls.
func (sg *kvStoreTokenGranter) tryGetLocked(count int64, demuxHandle int8) grantResult {
	wc := workClass(demuxHandle)
	// NB: ideally if regularRequester.hasWaitingRequests() returns true and
	// wc==elasticWorkClass we should reject this request, since it means that
	// more important regular work is waiting. However, we rely on the
	// assumption that elasticWorkClass, once throttled, will have a non-empty
	// queue, and since the only case where tryGetLocked is called for
	// elasticWorkClass is when the queue is empty, this case should be rare
	// (and not cause a performance isolation failure).
	switch wc {
	case regularWorkClass:
		if sg.availableIOTokens > 0 {
			sg.subtractTokens(count, false)
			sg.diskBWTokensUsed[wc] += count
			return grantSuccess
		}
	case elasticWorkClass:
		if sg.elasticDiskBWTokensAvailable > 0 && sg.availableIOTokens > 0 {
			sg.elasticDiskBWTokensAvailable -= count
			sg.subtractTokens(count, false)
			sg.diskBWTokensUsed[wc] += count
			return grantSuccess
		}
	}
	return grantFailLocal
}

func (sg *kvStoreTokenGranter) returnGrant(workClass workClass, count int64) {
	sg.coord.returnGrant(KVWork, count, int8(workClass))
}

// returnGrantLocked implements granterWithLockedCalls.
func (sg *kvStoreTokenGranter) returnGrantLocked(count int64, demuxHandle int8) {
	wc := workClass(demuxHandle)
	// Return count tokens to the "IO tokens".
	sg.subtractTokens(-count, false)
	if wc == elasticWorkClass {
		// Return count tokens to the elastic disk bandwidth tokens.
		sg.elasticDiskBWTokensAvailable += count
	}
	sg.diskBWTokensUsed[wc] -= count
}

func (sg *kvStoreTokenGranter) tookWithoutPermission(workClass workClass, count int64) {
	sg.coord.tookWithoutPermission(KVWork, count, int8(workClass))
}

// tookWithoutPermissionLocked implements granterWithLockedCalls.
func (sg *kvStoreTokenGranter) tookWithoutPermissionLocked(count int64, demuxHandle int8) {
	wc := workClass(demuxHandle)
	sg.subtractTokens(count, false)
	if wc == elasticWorkClass {
		sg.elasticDiskBWTokensAvailable -= count
	}
	sg.diskBWTokensUsed[wc] += count
}

// subtractTokens is a helper function that subtracts count tokens (count can
// be negative, in which case this is really an addition).
func (sg *kvStoreTokenGranter) subtractTokens(count int64, forceTickMetric bool) {
	avail := sg.availableIOTokens
	sg.availableIOTokens -= count
	if count > 0 && avail > 0 && sg.availableIOTokens <= 0 {
		// Transition from > 0 to <= 0.
		sg.exhaustedStart = timeutil.Now()
	} else if count < 0 && avail <= 0 && (sg.availableIOTokens > 0 || forceTickMetric) {
		// Transition from <= 0 to > 0, or forced to tick the metric. The latter
		// ensures that if the available tokens stay <= 0, we don't show a sudden
		// change in the metric after minutes of exhaustion (we had observed such
		// behavior prior to this change).
		now := timeutil.Now()
		exhaustedMicros := now.Sub(sg.exhaustedStart).Microseconds()
		sg.ioTokensExhaustedDurationMetric.Inc(exhaustedMicros)
		if sg.availableIOTokens <= 0 {
			sg.exhaustedStart = now
		}
	}
}

// requesterHasWaitingRequests implements granterWithLockedCalls.
func (sg *kvStoreTokenGranter) requesterHasWaitingRequests() bool {
	return sg.regularRequester.hasWaitingRequests() || sg.elasticRequester.hasWaitingRequests()
}

// tryGrantLocked implements granterWithLockedCalls.
func (sg *kvStoreTokenGranter) tryGrantLocked(grantChainID grantChainID) grantResult {
	// First try granting to regular requester.
	for wc := range sg.diskBWTokensUsed {
		req := sg.regularRequester
		if workClass(wc) == elasticWorkClass {
			req = sg.elasticRequester
		}
		if req.hasWaitingRequests() {
			res := sg.tryGetLocked(1, int8(wc))
			if res == grantSuccess {
				tookTokenCount := req.granted(grantChainID)
				if tookTokenCount == 0 {
					// Did not accept grant.
					sg.returnGrantLocked(1, int8(wc))
					// Continue with the loop since this requester does not have waiting
					// requests. If the loop terminates we will correctly return
					// grantFailLocal.
				} else {
					// May have taken more.
					if tookTokenCount > 1 {
						sg.tookWithoutPermissionLocked(tookTokenCount-1, int8(wc))
					}
					return grantSuccess
				}
			} else {
				// Was not able to get token. Do not continue with looping to grant to
				// less important work (though it would be harmless since won't be
				// able to get a token for that either).
				return res
			}
		}
	}
	return grantFailLocal
}

// setAvailableIOTokensLocked implements granterWithIOTokens.
func (sg *kvStoreTokenGranter) setAvailableIOTokensLocked(tokens int64) (tokensUsed int64) {
	tokensUsed = sg.startingIOTokens - sg.availableIOTokens
	// It is possible for availableIOTokens to be negative because of
	// tookWithoutPermission or because tryGet will satisfy requests until
	// availableIOTokens become <= 0. We want to remember this previous
	// over-allocation.
	sg.subtractTokens(-tokens, true)
	if sg.availableIOTokens > tokens {
		// Clamp to tokens.
		sg.availableIOTokens = tokens
	}
	sg.startingIOTokens = tokens
	return tokensUsed
}

// setAvailableElasticDiskBandwidthTokensLocked implements
// granterWithIOTokens.
func (sg *kvStoreTokenGranter) setAvailableElasticDiskBandwidthTokensLocked(tokens int64) {
	sg.elasticDiskBWTokensAvailable += tokens
	if sg.elasticDiskBWTokensAvailable > tokens {
		sg.elasticDiskBWTokensAvailable = tokens
	}
}

// getDiskTokensUsedAndResetLocked implements granterWithIOTokens.
func (sg *kvStoreTokenGranter) getDiskTokensUsedAndResetLocked() [numWorkClasses]int64 {
	result := sg.diskBWTokensUsed
	for i := range sg.diskBWTokensUsed {
		sg.diskBWTokensUsed[i] = 0
	}
	return result
}

// setAdmittedModelsLocked implements granterWithIOTokens.
func (sg *kvStoreTokenGranter) setAdmittedDoneModelsLocked(
	l0WriteLM tokensLinearModel, l0IngestLM tokensLinearModel, ingestLM tokensLinearModel,
) {
	sg.l0WriteLM = l0WriteLM
	sg.l0IngestLM = l0IngestLM
	sg.ingestLM = ingestLM
}

// storeWriteDone implements granterWithStoreWriteDone.
func (sg *kvStoreTokenGranter) storeWriteDone(
	wc workClass, originalTokens int64, doneInfo StoreWorkDoneInfo,
) (additionalTokens int64) {
	// Normally, we follow the structure of a foo() method calling into a foo()
	// method on the GrantCoordinator, which then calls fooLocked() on the
	// kvStoreTokenGranter. For example, returnGrant follows this structure.
	// This allows the GrantCoordinator to do two things (a) acquire the mu
	// before calling into kvStoreTokenGranter, (b) do side-effects, like
	// terminating grant chains and doing more grants after the call into the
	// fooLocked() method.
	// For storeWriteDone we don't bother with this structure involving the
	// GrantCoordinator (which has served us well across various methods and
	// various granter implementations), since the decision on when the
	// GrantCoordinator should call tryGrant is more complicated. And since this
	// storeWriteDone is unique to the kvStoreTokenGranter (and not implemented
	// by other granters) this approach seems acceptable.

	// Reminder: coord.mu protects the state in the kvStoreTokenGranter.
	sg.coord.mu.Lock()
	additionalL0TokensNeeded := sg.storeWriteDoneLocked(wc, originalTokens, doneInfo)
	sg.coord.mu.Unlock()
	// For multi-tenant fairness accounting, we choose to ignore disk bandwidth
	// tokens. Ideally, we'd have multiple resource dimensions for the fairness
	// decisions, but we don't necessarily need something more sophisticated
	// like "Dominant Resource Fairness".
	return additionalL0TokensNeeded
}

// storeWriteDoneLocked implements granterWithStoreWriteDone.
func (sg *kvStoreTokenGranter) storeWriteDoneLocked(
	wc workClass, originalTokens int64, doneInfo StoreWorkDoneInfo,
) (additionalTokensNeeded int64) {
	// Normally, we follow the structure of a foo() method calling into a foo()
	// method on the GrantCoordinator, which then calls fooLocked() on the
	// kvStoreTokenGranter. For example, returnGrant follows this structure.
	// This allows the GrantCoordinator to do two things (a) acquire the mu
	// before calling into kvStoreTokenGranter, (b) do side-effects, like
	// terminating grant chains and doing more grants after the call into the
	// fooLocked() method.
	// For storeWriteDone we don't bother with this structure involving the
	// GrantCoordinator (which has served us well across various methods and
	// various granter implementations), since the decision on when the
	// GrantCoordinator should call tryGrant is more complicated. And since this
	// storeWriteDone is unique to the kvStoreTokenGranter (and not implemented
	// by other granters) this approach seems acceptable.

	// Reminder: coord.mu protects the state in the kvStoreTokenGranter.
	exhaustedFunc := func() bool {
		return sg.availableIOTokens <= 0 ||
			(wc == elasticWorkClass && sg.elasticDiskBWTokensAvailable <= 0)
	}
	wasExhausted := exhaustedFunc()
	actualL0WriteTokens := sg.l0WriteLM.applyLinearModel(doneInfo.WriteBytes)
	actualL0IngestTokens := sg.l0IngestLM.applyLinearModel(doneInfo.IngestedBytes)
	actualL0Tokens := actualL0WriteTokens + actualL0IngestTokens
	additionalL0TokensNeeded := actualL0Tokens - originalTokens
	sg.subtractTokens(additionalL0TokensNeeded, false)
	actualIngestTokens := sg.ingestLM.applyLinearModel(doneInfo.IngestedBytes)
	additionalDiskBWTokensNeeded := (actualL0WriteTokens + actualIngestTokens) - originalTokens
	if wc == elasticWorkClass {
		sg.elasticDiskBWTokensAvailable -= additionalDiskBWTokensNeeded
	}
	sg.diskBWTokensUsed[wc] += additionalDiskBWTokensNeeded
	if additionalL0TokensNeeded < 0 || additionalDiskBWTokensNeeded < 0 {
		isExhausted := exhaustedFunc()
		if wasExhausted && !isExhausted {
			sg.coord.tryGrant()
		}
	}
	// For multi-tenant fairness accounting, we choose to ignore disk bandwidth
	// tokens. Ideally, we'd have multiple resource dimensions for the fairness
	// decisions, but we don't necessarily need something more sophisticated
	// like "Dominant Resource Fairness".
	return additionalL0TokensNeeded
}

// PebbleMetricsProvider provides the pebble.Metrics for all stores.
type PebbleMetricsProvider interface {
	GetPebbleMetrics() []StoreMetrics
}

// IOThresholdConsumer is informed about updated IOThresholds.
type IOThresholdConsumer interface {
	UpdateIOThreshold(roachpb.StoreID, *admissionpb.IOThreshold)
}

// StoreMetrics are the metrics for a store.
type StoreMetrics struct {
	StoreID int32
	*pebble.Metrics
	WriteStallCount int64
	// Optional.
	DiskStats DiskStats
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
	totalModerateSlots = metric.Metadata{
		Name:        "admission.granter.total_moderate_slots.kv",
		Help:        "Total moderate load slots for low priority work",
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
	usedSoftSlots = metric.Metadata{
		Name:        "admission.granter.used_soft_slots.kv",
		Help:        "Used soft slots",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	kvIOTokensExhaustedDuration = metric.Metadata{
		Name:        "admission.granter.io_tokens_exhausted_duration.kv",
		Help:        "Total duration when IO tokens were exhausted, in micros",
		Measurement: "Microseconds",
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

// TODO(sumeer): experiment with approaches to adjust slots for
// SQLStatementLeafStartWork and SQLStatementRootStartWork for SQL nodes. Note
// that for these WorkKinds we are currently setting very high slot counts
// since we rely on other signals like memory and cpuOverloadIndicator to gate
// admission. One could debate whether we should be using rate limiting
// instead of counting slots for such work. The only reason the above code
// uses the term "slot" for these is that we have a completion indicator, and
// when we do have such an indicator it can be beneficial to be able to keep
// track of how many ongoing work items we have.

// SoftSlotGranter grants soft slots without queueing. See the comment with
// kvGranter.
type SoftSlotGranter struct {
	kvGranter *slotGranter
}

// MakeSoftSlotGranter constructs a SoftSlotGranter given a GrantCoordinator
// that is responsible for KV and lower layers.
func MakeSoftSlotGranter(gc *GrantCoordinator) (*SoftSlotGranter, error) {
	kvGranter, ok := gc.granters[KVWork].(*slotGranter)
	if !ok {
		return nil, errors.Errorf("GrantCoordinator does not support soft slots")
	}
	return &SoftSlotGranter{
		kvGranter: kvGranter,
	}, nil
}

// TryGetSlots attempts to acquire count slots and returns what was acquired
// (possibly 0).
func (ssg *SoftSlotGranter) TryGetSlots(count int) int {
	if !EnabledSoftSlotGranting.Get(&ssg.kvGranter.coord.settings.SV) {
		return 0
	}
	return ssg.kvGranter.tryGetSoftSlots(count)
}

// ReturnSlots returns count slots (count must be >= 0).
func (ssg *SoftSlotGranter) ReturnSlots(count int) {
	ssg.kvGranter.returnSoftSlots(count)
}
