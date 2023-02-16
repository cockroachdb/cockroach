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
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
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
	coord               *GrantCoordinator
	workKind            WorkKind
	requester           requester
	usedSlots           int
	totalSlots          int
	skipSlotEnforcement bool

	// Optional. Nil for a slotGranter used for KVWork since the slots for that
	// slotGranter are directly adjusted by the kvSlotAdjuster (using the
	// kvSlotAdjuster here would provide a redundant identical signal).
	cpuOverload cpuOverloadIndicator

	usedSlotsMetric *metric.Gauge
	// Non-nil for KV slots.
	slotsExhaustedDurationMetric *metric.Counter
	exhaustedStart               time.Time
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
	if sg.usedSlots < sg.totalSlots || sg.skipSlotEnforcement {
		sg.usedSlots++
		if sg.usedSlots == sg.totalSlots && sg.slotsExhaustedDurationMetric != nil {
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
	if sg.usedSlots == sg.totalSlots && sg.slotsExhaustedDurationMetric != nil {
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
	if sg.usedSlots == sg.totalSlots && sg.slotsExhaustedDurationMetric != nil {
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

//gcassert:inline
func (sg *slotGranter) setTotalSlotsLocked(totalSlots int) {
	// Mid-stack inlining.
	if totalSlots == sg.totalSlots {
		return
	}
	sg.setTotalSlotsLockedInternal(totalSlots)
}

func (sg *slotGranter) setTotalSlotsLockedInternal(totalSlots int) {
	if sg.slotsExhaustedDurationMetric != nil {
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

	coordMu struct { // holds fields protected by coord.mu.Lock
		// There is no rate limiting in granting these tokens. That is, they are
		// all burst tokens.
		availableIOTokens int64
		// Disk bandwidth tokens.
		elasticDiskBWTokensAvailable int64

		diskBWTokensUsed [admissionpb.NumWorkClasses]int64
	}

	// startingIOTokens is the number of tokens set by
	// setAvailableTokens. It is used to compute the tokens used, by
	// computing startingIOTokens-availableIOTokens.
	startingIOTokens                int64
	ioTokensExhaustedDurationMetric *metric.Counter
	exhaustedStart                  time.Time

	// Estimation models.
	l0WriteLM, l0IngestLM, ingestLM tokensLinearModel
}

var _ granterWithLockedCalls = &kvStoreTokenGranter{}
var _ granterWithIOTokens = &kvStoreTokenGranter{}

// kvStoreTokenChildGranter handles a particular workClass. Its methods
// pass-through to the parent after adding the workClass as a parameter.
type kvStoreTokenChildGranter struct {
	workClass admissionpb.WorkClass
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

func (sg *kvStoreTokenGranter) tryGet(workClass admissionpb.WorkClass, count int64) bool {
	return sg.coord.tryGet(KVWork, count, int8(workClass))
}

// tryGetLocked implements granterWithLockedCalls.
func (sg *kvStoreTokenGranter) tryGetLocked(count int64, demuxHandle int8) grantResult {
	wc := admissionpb.WorkClass(demuxHandle)
	// NB: ideally if regularRequester.hasWaitingRequests() returns true and
	// wc==elasticWorkClass we should reject this request, since it means that
	// more important regular work is waiting. However, we rely on the
	// assumption that elasticWorkClass, once throttled, will have a non-empty
	// queue, and since the only case where tryGetLocked is called for
	// elasticWorkClass is when the queue is empty, this case should be rare
	// (and not cause a performance isolation failure).
	switch wc {
	case admissionpb.RegularWorkClass:
		if sg.coordMu.availableIOTokens > 0 {
			sg.subtractTokensLocked(count, false)
			sg.coordMu.diskBWTokensUsed[wc] += count
			return grantSuccess
		}
	case admissionpb.ElasticWorkClass:
		if sg.coordMu.elasticDiskBWTokensAvailable > 0 && sg.coordMu.availableIOTokens > 0 {
			sg.coordMu.elasticDiskBWTokensAvailable -= count
			sg.subtractTokensLocked(count, false)
			sg.coordMu.diskBWTokensUsed[wc] += count
			return grantSuccess
		}
	}
	return grantFailLocal
}

func (sg *kvStoreTokenGranter) returnGrant(workClass admissionpb.WorkClass, count int64) {
	sg.coord.returnGrant(KVWork, count, int8(workClass))
}

// returnGrantLocked implements granterWithLockedCalls.
func (sg *kvStoreTokenGranter) returnGrantLocked(count int64, demuxHandle int8) {
	wc := admissionpb.WorkClass(demuxHandle)
	// Return count tokens to the "IO tokens".
	sg.subtractTokensLocked(-count, false)
	if wc == admissionpb.ElasticWorkClass {
		// Return count tokens to the elastic disk bandwidth tokens.
		sg.coordMu.elasticDiskBWTokensAvailable += count
	}
	sg.coordMu.diskBWTokensUsed[wc] -= count
}

func (sg *kvStoreTokenGranter) tookWithoutPermission(workClass admissionpb.WorkClass, count int64) {
	sg.coord.tookWithoutPermission(KVWork, count, int8(workClass))
}

// tookWithoutPermissionLocked implements granterWithLockedCalls.
func (sg *kvStoreTokenGranter) tookWithoutPermissionLocked(count int64, demuxHandle int8) {
	wc := admissionpb.WorkClass(demuxHandle)
	sg.subtractTokensLocked(count, false)
	if wc == admissionpb.ElasticWorkClass {
		sg.coordMu.elasticDiskBWTokensAvailable -= count
	}
	sg.coordMu.diskBWTokensUsed[wc] += count
}

// subtractTokensLocked is a helper function that subtracts count tokens (count
// can be negative, in which case this is really an addition).
func (sg *kvStoreTokenGranter) subtractTokensLocked(count int64, forceTickMetric bool) {
	avail := sg.coordMu.availableIOTokens
	sg.coordMu.availableIOTokens -= count
	if count > 0 && avail > 0 && sg.coordMu.availableIOTokens <= 0 {
		// Transition from > 0 to <= 0.
		sg.exhaustedStart = timeutil.Now()
	} else if count < 0 && avail <= 0 && (sg.coordMu.availableIOTokens > 0 || forceTickMetric) {
		// Transition from <= 0 to > 0, or forced to tick the metric. The latter
		// ensures that if the available tokens stay <= 0, we don't show a sudden
		// change in the metric after minutes of exhaustion (we had observed such
		// behavior prior to this change).
		now := timeutil.Now()
		exhaustedMicros := now.Sub(sg.exhaustedStart).Microseconds()
		sg.ioTokensExhaustedDurationMetric.Inc(exhaustedMicros)
		if sg.coordMu.availableIOTokens <= 0 {
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
	for wc := range sg.coordMu.diskBWTokensUsed {
		req := sg.regularRequester
		if admissionpb.WorkClass(wc) == admissionpb.ElasticWorkClass {
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

// setAvailableTokens implements granterWithIOTokens.
func (sg *kvStoreTokenGranter) setAvailableTokens(
	ioTokens int64, elasticDiskBandwidthTokens int64,
) (ioTokensUsed int64) {
	sg.coord.mu.Lock()
	defer sg.coord.mu.Unlock()
	ioTokensUsed = sg.startingIOTokens - sg.coordMu.availableIOTokens
	// It is possible for availableIOTokens to be negative because of
	// tookWithoutPermission or because tryGet will satisfy requests until
	// availableIOTokens become <= 0. We want to remember this previous
	// over-allocation.
	sg.subtractTokensLocked(-ioTokens, true)
	if sg.coordMu.availableIOTokens > ioTokens {
		// Clamp to tokens.
		sg.coordMu.availableIOTokens = ioTokens
	}
	sg.startingIOTokens = ioTokens

	sg.coordMu.elasticDiskBWTokensAvailable += elasticDiskBandwidthTokens
	if sg.coordMu.elasticDiskBWTokensAvailable > elasticDiskBandwidthTokens {
		sg.coordMu.elasticDiskBWTokensAvailable = elasticDiskBandwidthTokens
	}

	return ioTokensUsed
}

// getDiskTokensUsedAndResetLocked implements granterWithIOTokens.
func (sg *kvStoreTokenGranter) getDiskTokensUsedAndReset() [admissionpb.NumWorkClasses]int64 {
	sg.coord.mu.Lock()
	defer sg.coord.mu.Unlock()
	result := sg.coordMu.diskBWTokensUsed
	for i := range sg.coordMu.diskBWTokensUsed {
		sg.coordMu.diskBWTokensUsed[i] = 0
	}
	return result
}

// setAdmittedModelsLocked implements granterWithIOTokens.
func (sg *kvStoreTokenGranter) setAdmittedDoneModels(
	l0WriteLM tokensLinearModel, l0IngestLM tokensLinearModel, ingestLM tokensLinearModel,
) {
	sg.coord.mu.Lock()
	defer sg.coord.mu.Unlock()
	sg.l0WriteLM = l0WriteLM
	sg.l0IngestLM = l0IngestLM
	sg.ingestLM = ingestLM
}

// storeWriteDone implements granterWithStoreWriteDone.
func (sg *kvStoreTokenGranter) storeWriteDone(
	wc admissionpb.WorkClass, originalTokens int64, doneInfo StoreWorkDoneInfo,
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
	// GrantCoordinator should call tryGrantLocked is more complicated. And since this
	// storeWriteDone is unique to the kvStoreTokenGranter (and not implemented
	// by other granters) this approach seems acceptable.

	// Reminder: coord.mu protects the state in the kvStoreTokenGranter.
	sg.coord.mu.Lock()
	exhaustedFunc := func() bool {
		return sg.coordMu.availableIOTokens <= 0 ||
			(wc == admissionpb.ElasticWorkClass && sg.coordMu.elasticDiskBWTokensAvailable <= 0)
	}
	wasExhausted := exhaustedFunc()
	actualL0WriteTokens := sg.l0WriteLM.applyLinearModel(doneInfo.WriteBytes)
	actualL0IngestTokens := sg.l0IngestLM.applyLinearModel(doneInfo.IngestedBytes)
	actualL0Tokens := actualL0WriteTokens + actualL0IngestTokens
	additionalL0TokensNeeded := actualL0Tokens - originalTokens
	sg.subtractTokensLocked(additionalL0TokensNeeded, false)
	actualIngestTokens := sg.ingestLM.applyLinearModel(doneInfo.IngestedBytes)
	additionalDiskBWTokensNeeded := (actualL0WriteTokens + actualIngestTokens) - originalTokens
	if wc == admissionpb.ElasticWorkClass {
		sg.coordMu.elasticDiskBWTokensAvailable -= additionalDiskBWTokensNeeded
	}
	sg.coordMu.diskBWTokensUsed[wc] += additionalDiskBWTokensNeeded
	if additionalL0TokensNeeded < 0 || additionalDiskBWTokensNeeded < 0 {
		isExhausted := exhaustedFunc()
		if wasExhausted && !isExhausted {
			sg.coord.tryGrantLocked()
		}
	}
	sg.coord.mu.Unlock()
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
	StoreID roachpb.StoreID
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
