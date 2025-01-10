// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	coord             *GrantCoordinator
	regularRequester  requester
	elasticRequester  requester
	snapshotRequester requester

	coordMu struct { // holds fields protected by coord.mu.Lock
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
	}

	ioTokensExhaustedDurationMetric [admissionpb.NumWorkClasses]*metric.Counter
	availableTokensMetric           [admissionpb.NumWorkClasses]*metric.Gauge
	exhaustedStart                  [admissionpb.NumWorkClasses]time.Time
	// startingIOTokens is the number of tokens set by
	// setAvailableTokens. It is used to compute the tokens used, by
	// computing startingIOTokens-availableIOTokens.
	startingIOTokens     int64
	tokensReturnedMetric *metric.Counter
	tokensTakenMetric    *metric.Counter

	// Estimation models.
	l0WriteLM, l0IngestLM, ingestLM, writeAmpLM tokensLinearModel
}

var _ granterWithLockedCalls = &kvStoreTokenGranter{}
var _ granterWithIOTokens = &kvStoreTokenGranter{}

// kvStoreTokenChildGranter handles a particular workClass. Its methods
// pass-through to the parent after adding the workClass as a parameter.
type kvStoreTokenChildGranter struct {
	workType admissionpb.StoreWorkType
	parent   *kvStoreTokenGranter
}

var _ granterWithStoreReplicatedWorkAdmitted = &kvStoreTokenChildGranter{}
var _ granter = &kvStoreTokenChildGranter{}

// grantKind implements granter.
func (cg *kvStoreTokenChildGranter) grantKind() grantKind {
	return token
}

// tryGet implements granter.
func (cg *kvStoreTokenChildGranter) tryGet(count int64) bool {
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
	cg.parent.coord.mu.Lock()
	defer cg.parent.coord.mu.Unlock()
	// NB: the token/metric adjustments we want to make here are the same as we
	// want to make through the storeReplicatedWorkAdmittedLocked, so we (ab)use
	// it. The one difference is that post token adjustments, if we observe the
	// granter was previously exhausted but is no longer so, we're allowed to
	// admit other waiting requests.
	return cg.parent.storeReplicatedWorkAdmittedLocked(
		cg.workType, originalTokens, storeReplicatedWorkAdmittedInfo(doneInfo), true /* canGrantAnother */)
}

// storeReplicatedWorkAdmitted implements granterWithStoreReplicatedWorkAdmitted.
func (cg *kvStoreTokenChildGranter) storeReplicatedWorkAdmittedLocked(
	originalTokens int64, admittedInfo storeReplicatedWorkAdmittedInfo,
) (additionalTokens int64) {
	return cg.parent.storeReplicatedWorkAdmittedLocked(cg.workType, originalTokens, admittedInfo, false /* canGrantAnother */)
}

func (sg *kvStoreTokenGranter) tryGet(workType admissionpb.StoreWorkType, count int64) bool {
	return sg.coord.tryGet(KVWork, count, int8(workType))
}

// tryGetLocked implements granterWithLockedCalls.
func (sg *kvStoreTokenGranter) tryGetLocked(count int64, demuxHandle int8) grantResult {
	wt := admissionpb.StoreWorkType(demuxHandle)
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
		diskWriteTokens = sg.writeAmpLM.applyLinearModel(count)
	}
	switch wt {
	case admissionpb.RegularStoreWorkType:
		if sg.coordMu.availableIOTokens[admissionpb.RegularWorkClass] > 0 {
			sg.subtractIOTokensLocked(count, count, false)
			sg.coordMu.diskTokensAvailable.writeByteTokens -= diskWriteTokens
			sg.coordMu.diskTokensError.diskWriteTokensAlreadyDeducted += diskWriteTokens
			sg.coordMu.diskTokensUsed[wt].writeByteTokens += diskWriteTokens
			return grantSuccess
		}
	case admissionpb.ElasticStoreWorkType:
		if sg.coordMu.diskTokensAvailable.writeByteTokens > 0 &&
			sg.coordMu.availableIOTokens[admissionpb.RegularWorkClass] > 0 &&
			sg.coordMu.availableIOTokens[admissionpb.ElasticWorkClass] > 0 {
			sg.subtractIOTokensLocked(count, count, false)
			sg.coordMu.elasticIOTokensUsedByElastic += count
			sg.coordMu.diskTokensAvailable.writeByteTokens -= diskWriteTokens
			sg.coordMu.diskTokensError.diskWriteTokensAlreadyDeducted += diskWriteTokens
			sg.coordMu.diskTokensUsed[wt].writeByteTokens += diskWriteTokens
			return grantSuccess
		}
	case admissionpb.SnapshotIngestStoreWorkType:
		// Snapshot ingests do not go into L0, so we only subject them to
		// writeByteTokens.
		if sg.coordMu.diskTokensAvailable.writeByteTokens > 0 {
			sg.coordMu.diskTokensAvailable.writeByteTokens -= diskWriteTokens
			sg.coordMu.diskTokensError.diskWriteTokensAlreadyDeducted += diskWriteTokens
			sg.coordMu.diskTokensUsed[wt].writeByteTokens += diskWriteTokens
			return grantSuccess
		}
	}
	return grantFailLocal
}

func (sg *kvStoreTokenGranter) returnGrant(workType admissionpb.StoreWorkType, count int64) {
	sg.coord.returnGrant(KVWork, count, int8(workType))
}

// returnGrantLocked implements granterWithLockedCalls.
func (sg *kvStoreTokenGranter) returnGrantLocked(count int64, demuxHandle int8) {
	wt := admissionpb.StoreWorkType(demuxHandle)
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
		sg.coordMu.elasticIOTokensUsedByElastic += count
	}
	// Adjust tokens for disk bandwidth bucket.
	switch wt {
	case admissionpb.RegularStoreWorkType, admissionpb.ElasticStoreWorkType:
		diskTokenCount := sg.writeAmpLM.applyLinearModel(count)
		sg.coordMu.diskTokensAvailable.writeByteTokens -= diskTokenCount
		sg.coordMu.diskTokensError.diskWriteTokensAlreadyDeducted += diskTokenCount
		sg.coordMu.diskTokensUsed[wt].writeByteTokens += diskTokenCount
	case admissionpb.SnapshotIngestStoreWorkType:
		// Do not apply the writeAmpLM since these writes do not incur additional
		// write-amp.
		sg.coordMu.diskTokensAvailable.writeByteTokens -= count
		sg.coordMu.diskTokensError.diskWriteTokensAlreadyDeducted += count
		sg.coordMu.diskTokensUsed[wt].writeByteTokens += count
	}
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
	intWrites := int64(writeBytes - sg.coordMu.diskTokensError.prevObservedWrites)
	intReads := int64(readBytes - sg.coordMu.diskTokensError.prevObservedReads)

	// Compensate for error due to writes.
	writeError := intWrites - sg.coordMu.diskTokensError.diskWriteTokensAlreadyDeducted
	if writeError > 0 {
		sg.coordMu.diskTokensAvailable.writeByteTokens -= writeError
	}

	// Compensate for error due to reads.
	readError := intReads - sg.coordMu.diskTokensError.diskReadTokensAlreadyDeducted
	if readError > 0 {
		sg.coordMu.diskTokensAvailable.writeByteTokens -= readError
	}

	// We have compensated for error, if any, in this interval, so we reset the
	// deducted count for the next compensation interval.
	sg.coordMu.diskTokensError.diskWriteTokensAlreadyDeducted = 0
	sg.coordMu.diskTokensError.diskReadTokensAlreadyDeducted = 0

	sg.coordMu.diskTokensError.prevObservedWrites = writeBytes
	sg.coordMu.diskTokensError.prevObservedReads = readBytes
}

func (sg *kvStoreTokenGranter) tookWithoutPermission(
	workType admissionpb.StoreWorkType, count int64,
) {
	sg.coord.tookWithoutPermission(KVWork, count, int8(workType))
}

// tookWithoutPermissionLocked implements granterWithLockedCalls.
func (sg *kvStoreTokenGranter) tookWithoutPermissionLocked(count int64, demuxHandle int8) {
	wt := admissionpb.StoreWorkType(demuxHandle)
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
	avail := sg.coordMu.availableIOTokens[wc]
	sg.coordMu.availableIOTokens[wc] -= count
	sg.availableTokensMetric[wc].Update(sg.coordMu.availableIOTokens[wc])
	if count > 0 && avail > 0 && sg.coordMu.availableIOTokens[wc] <= 0 {
		// Transition from > 0 to <= 0.
		sg.exhaustedStart[wc] = timeutil.Now()
	} else if count < 0 && avail <= 0 && (sg.coordMu.availableIOTokens[wc] > 0 || settingAvailableTokens) {
		// Transition from <= 0 to > 0, or if we're newly setting available
		// tokens. The latter ensures that if the available tokens stay <= 0, we
		// don't show a sudden change in the metric after minutes of exhaustion
		// (we had observed such behavior prior to this change).
		now := timeutil.Now()
		exhaustedMicros := now.Sub(sg.exhaustedStart[wc]).Microseconds()
		sg.ioTokensExhaustedDurationMetric[wc].Inc(exhaustedMicros)
		if sg.coordMu.availableIOTokens[wc] <= 0 {
			sg.exhaustedStart[wc] = now
		}
	}
}

// requesterHasWaitingRequests implements granterWithLockedCalls.
func (sg *kvStoreTokenGranter) requesterHasWaitingRequests() bool {
	return sg.regularRequester.hasWaitingRequests() ||
		sg.elasticRequester.hasWaitingRequests() ||
		sg.snapshotRequester.hasWaitingRequests()
}

// tryGrantLocked implements granterWithLockedCalls.
func (sg *kvStoreTokenGranter) tryGrantLocked(grantChainID grantChainID) grantResult {
	// NB: We grant work in the following priority order: regular, snapshot
	// ingest, elastic work. Snapshot ingests are a special type of elastic work.
	// They queue separately in the SnapshotQueue and get priority over other
	// elastic work since they are used for node re-balancing and up-replication,
	// which are typically higher priority than other background writes.
	for wt := 0; wt < admissionpb.NumStoreWorkTypes; wt++ {
		req := sg.regularRequester
		if admissionpb.StoreWorkType(wt) == admissionpb.ElasticStoreWorkType {
			req = sg.elasticRequester
		} else if admissionpb.StoreWorkType(wt) == admissionpb.SnapshotIngestStoreWorkType {
			req = sg.snapshotRequester
		}
		if req.hasWaitingRequests() {
			res := sg.tryGetLocked(1, int8(wt))
			if res == grantSuccess {
				tookTokenCount := req.granted(grantChainID)
				if tookTokenCount == 0 {
					// Did not accept grant.
					sg.returnGrantLocked(1, int8(wt))
					// Continue with the loop since this requester does not have waiting
					// requests. If the loop terminates we will correctly return
					// grantFailLocal.
				} else {
					// May have taken more.
					if tookTokenCount > 1 {
						sg.tookWithoutPermissionLocked(tookTokenCount-1, int8(wt))
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
	ioTokens int64,
	elasticIOTokens int64,
	diskWriteTokens int64,
	diskReadTokens int64,
	ioTokenCapacity int64,
	elasticIOTokenCapacity int64,
	diskWriteTokensCapacity int64,
	lastTick bool,
) (ioTokensUsed int64, ioTokensUsedByElasticWork int64) {
	sg.coord.mu.Lock()
	defer sg.coord.mu.Unlock()
	ioTokensUsed = sg.startingIOTokens - sg.coordMu.availableIOTokens[admissionpb.RegularWorkClass]
	ioTokensUsedByElasticWork = sg.coordMu.elasticIOTokensUsedByElastic
	sg.coordMu.elasticIOTokensUsedByElastic = 0

	// It is possible for availableIOTokens to be negative because of
	// tookWithoutPermission or because tryGet will satisfy requests until
	// availableIOTokens become <= 0. We want to remember this previous
	// over-allocation.
	sg.subtractIOTokensLocked(-ioTokens, -elasticIOTokens, true)
	if sg.coordMu.availableIOTokens[admissionpb.RegularWorkClass] > ioTokenCapacity {
		sg.coordMu.availableIOTokens[admissionpb.RegularWorkClass] = ioTokenCapacity
	}
	if sg.coordMu.availableIOTokens[admissionpb.ElasticWorkClass] > elasticIOTokenCapacity {
		sg.coordMu.availableIOTokens[admissionpb.ElasticWorkClass] = elasticIOTokenCapacity
	}
	// availableIOTokens[ElasticWorkClass] can become very negative since it can
	// be fewer than the tokens for regular work, and regular work deducts from it
	// without blocking. This behavior is desirable, but we don't want deficits to
	// accumulate indefinitely. We've found that resetting on the lastTick
	// provides a good enough frequency for resetting the deficit. That is, we are
	// resetting every 15s.
	if lastTick {
		sg.coordMu.availableIOTokens[admissionpb.ElasticWorkClass] =
			max(sg.coordMu.availableIOTokens[admissionpb.ElasticWorkClass], 0)
		// It is possible that availableIOTokens[RegularWorkClass] is negative, in
		// which case we want availableIOTokens[ElasticWorkClass] to not exceed it.
		sg.coordMu.availableIOTokens[admissionpb.ElasticWorkClass] =
			min(sg.coordMu.availableIOTokens[admissionpb.ElasticWorkClass], sg.coordMu.availableIOTokens[admissionpb.RegularWorkClass])
		// We also want to avoid very negative disk write tokens, so we reset them.
		sg.coordMu.diskTokensAvailable.writeByteTokens = max(sg.coordMu.diskTokensAvailable.writeByteTokens, 0)
	}
	var w admissionpb.WorkClass
	for w = 0; w < admissionpb.NumWorkClasses; w++ {
		sg.availableTokensMetric[w].Update(sg.coordMu.availableIOTokens[w])
	}
	sg.startingIOTokens = sg.coordMu.availableIOTokens[admissionpb.RegularWorkClass]

	// Allocate disk write and read tokens.
	sg.coordMu.diskTokensAvailable.writeByteTokens += diskWriteTokens
	if sg.coordMu.diskTokensAvailable.writeByteTokens > diskWriteTokensCapacity {
		sg.coordMu.diskTokensAvailable.writeByteTokens = diskWriteTokensCapacity
	}
	// NB: We don't cap the disk read tokens as they are only deducted during the
	// error accounting loop. So essentially, we give reads the "burst" capacity
	// of the error accounting interval. See `adjustDiskTokenErrorLocked` for the
	// error accounting logic, and where we reset this bucket to 0.
	sg.coordMu.diskTokensError.diskReadTokensAlreadyDeducted += diskReadTokens

	return ioTokensUsed, ioTokensUsedByElasticWork
}

// getDiskTokensUsedAndResetLocked implements granterWithIOTokens.
func (sg *kvStoreTokenGranter) getDiskTokensUsedAndReset() (
	usedTokens [admissionpb.NumStoreWorkTypes]diskTokens,
) {
	sg.coord.mu.Lock()
	defer sg.coord.mu.Unlock()
	for i := 0; i < admissionpb.NumStoreWorkTypes; i++ {
		usedTokens[i] = sg.coordMu.diskTokensUsed[i]
		sg.coordMu.diskTokensUsed[i] = diskTokens{}
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
	sg.coord.mu.Lock()
	defer sg.coord.mu.Unlock()
	sg.l0WriteLM = l0WriteLM
	sg.l0IngestLM = l0IngestLM
	sg.ingestLM = ingestLM
	sg.writeAmpLM = writeAmpLM
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
		return sg.coordMu.availableIOTokens[admissionpb.RegularWorkClass] <= 0 ||
			(wc == admissionpb.ElasticWorkClass && (sg.coordMu.diskTokensAvailable.writeByteTokens <= 0 ||
				sg.coordMu.availableIOTokens[admissionpb.ElasticWorkClass] <= 0))
	}
	wasExhausted := exhaustedFunc()
	actualL0WriteTokens := sg.l0WriteLM.applyLinearModel(admittedInfo.WriteBytes)
	actualL0IngestTokens := sg.l0IngestLM.applyLinearModel(admittedInfo.IngestedBytes)
	actualL0Tokens := actualL0WriteTokens + actualL0IngestTokens
	additionalL0TokensNeeded := actualL0Tokens - originalTokens
	sg.subtractIOTokensLocked(additionalL0TokensNeeded, additionalL0TokensNeeded, false)
	if wt == admissionpb.ElasticStoreWorkType {
		sg.coordMu.elasticIOTokensUsedByElastic += additionalL0TokensNeeded
	}

	// Adjust disk write tokens.
	ingestIntoLSM := sg.ingestLM.applyLinearModel(admittedInfo.IngestedBytes)
	totalBytesIntoLSM := actualL0WriteTokens + ingestIntoLSM
	actualDiskWriteTokens := sg.writeAmpLM.applyLinearModel(totalBytesIntoLSM)
	originalDiskTokens := sg.writeAmpLM.applyLinearModel(originalTokens)
	additionalDiskWriteTokens := actualDiskWriteTokens - originalDiskTokens
	sg.coordMu.diskTokensAvailable.writeByteTokens -= additionalDiskWriteTokens
	sg.coordMu.diskTokensUsed[wt].writeByteTokens += additionalDiskWriteTokens

	if canGrantAnother && (additionalL0TokensNeeded < 0) {
		isExhausted := exhaustedFunc()
		if (wasExhausted && !isExhausted) || sg.coord.knobs.AlwaysTryGrantWhenAdmitted {
			sg.coord.tryGrantLocked()
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
	Close()
}

// MetricsRegistryProvider provides the store metric.Registry for a given store.
type MetricsRegistryProvider interface {
	GetMetricsRegistry(roachpb.StoreID) *metric.Registry
}

// IOThresholdConsumer is informed about updated IOThresholds.
type IOThresholdConsumer interface {
	UpdateIOThreshold(roachpb.StoreID, *admissionpb.IOThreshold)
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

// TODO(sumeer): experiment with approaches to adjust slots for
// SQLStatementLeafStartWork and SQLStatementRootStartWork for SQL nodes. Note
// that for these WorkKinds we are currently setting very high slot counts
// since we rely on other signals like memory and cpuOverloadIndicator to gate
// admission. One could debate whether we should be using rate limiting
// instead of counting slots for such work. The only reason the above code
// uses the term "slot" for these is that we have a completion indicator, and
// when we do have such an indicator it can be beneficial to be able to keep
// track of how many ongoing work items we have.
