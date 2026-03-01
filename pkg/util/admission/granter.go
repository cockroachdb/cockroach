// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
		exhaustedDuration := now.Sub(sg.exhaustedStart)
		sg.slotsExhaustedDurationMetric.Inc(exhaustedDuration.Nanoseconds())
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
			exhaustedDuration := now.Sub(sg.exhaustedStart)
			sg.slotsExhaustedDurationMetric.Inc(exhaustedDuration.Nanoseconds())
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
		//
		// Disk tokens available represents the current size of the token bucket.
		// The diskTokens.readByteTokens is currently unused, since estimated
		// future reads are added to the diskReadTokensAlreadyDeducted directly
		// and then compared with observed reads. That is, we are pretending as if
		// we observed token deductions from (the non-existent) read token bucket,
		// corresponding to reads observed in the last adjustment interval. We
		// need to do this since read code is not instrumented to interface with
		// admission control.
		diskTokensAvailable diskTokens
		// The capacity of the token bucket for disk write bytes.
		diskWriteByteTokensCapacity int64
		diskTokensError             struct {
			initialized bool
			// prevObserved{Writes,Reads} is the observed disk metrics in the last
			// call to adjustDiskTokenErrorLocked. These are used to compute the
			// delta.
			prevObservedWrites uint64
			prevObservedReads  uint64
			// staleStats tracks consecutive intervals where disk stats don't change.
			staleStats staleStatTracker
			// alreadyDeductedTokens is the number of tokens deducted that can be
			// used to partially explain the observed writes and reads. The write
			// tokens here correspond to deductions from
			// diskTokensAvailable.writeByteTokens. These are reset on each error
			// correction tick (every 1ms when loaded).
			alreadyDeductedTokens diskTokens
			// Error details for the latest adjustmentInterval, across the various
			// adjustDiskTokenErrorLocked calls (which happen every 1ms when loaded).
			// We don't track beyond an adjustmentInterval since the last interval's
			// observations of disk reads/writes are already used in the modeling (of
			// write amplification and the alreadyDeductedTokens.readByteTokens).
			//
			// - cumError is the error summed across the various
			//   adjustDiskTokenErrorLocked calls. Since errors can be positive and
			//   negative, they can cancel out.
			//
			// - absError is the sum of absolute values of error across these calls,
			//   so they don't cancel out. This is only used for observability since
			//   large positive and negative errors become observable here, and helps
			//   with understanding the system behavior.
			//
			// On each tick, the full interval error (observed - deducted) is
			// immediately applied to the available disk write tokens. Since error
			// correction now happens at 1ms granularity (matching the token
			// allocation tick rate), the per-tick error magnitude is small, avoiding
			// the large oscillations that were problematic when error correction
			// happened at coarser intervals. See #160964 for context on earlier
			// approaches that used partial error accounting with dampening.
			cumError diskTokens
			absError diskTokens
		}
		diskTokensUsed [admissionpb.NumStoreWorkTypes]diskTokens
		// prevIntervalDiskWriteTokensRemaining is the value of
		// diskTokensAvailable.writeByteTokens at the end of the previous
		// adjustment interval. A negative value indicates overadmission.
		prevIntervalDiskWriteTokensRemaining int64
		// exhaustedStart is the time when the corresponding availableIOTokens
		// became <= 0. Ignored when the corresponding availableIOTokens is > 0.
		exhaustedStart [admissionpb.NumWorkClasses]time.Time
		// startingIOTokens is the number of tokens set by addAvailableTokens for
		// regular work. It is used to compute the tokens used, by computing
		// startingIOTokens-availableIOTokens[RegularWorkClass].
		startingIOTokens int64
		// diskWriteByteTokensExhaustedStart is the time when
		// diskTokensAvailable.writeByteTokens became <= 0. Ignored when
		// diskTokensAvailable.writeByteTokens is > 0.
		diskWriteByteTokensExhaustedStart time.Time

		// Estimation models.
		l0WriteLM, l0IngestLM, ingestLM, writeAmpLM tokensLinearModel
	}

	ioTokensExhaustedDurationMetric            [admissionpb.NumWorkClasses]*metric.Counter
	availableTokensMetric                      [admissionpb.NumWorkClasses]*metric.Gauge
	tokensReturnedMetric                       *metric.Counter
	tokensTakenMetric                          *metric.Counter
	diskWriteByteTokensExhaustedDurationMetric *metric.Counter
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
			sg.subtractDiskWriteTokensLocked(diskWriteTokens, false)
			sg.mu.diskTokensUsed[wt].writeByteTokens += diskWriteTokens
			return true
		}
	case admissionpb.ElasticStoreWorkType:
		if sg.mu.diskTokensAvailable.writeByteTokens > 0 &&
			sg.mu.availableIOTokens[admissionpb.RegularWorkClass] > 0 &&
			sg.mu.availableIOTokens[admissionpb.ElasticWorkClass] > 0 {
			sg.subtractIOTokensLocked(count, count, false)
			sg.mu.elasticIOTokensUsedByElastic += count
			sg.subtractDiskWriteTokensLocked(diskWriteTokens, false)
			sg.mu.diskTokensUsed[wt].writeByteTokens += diskWriteTokens
			return true
		}
	case admissionpb.SnapshotIngestStoreWorkType:
		// Snapshot ingests do not go into L0, so we only subject them to
		// writeByteTokens.
		if sg.mu.diskTokensAvailable.writeByteTokens > 0 {
			sg.subtractDiskWriteTokensLocked(diskWriteTokens, false)
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
		sg.subtractDiskWriteTokensLocked(diskTokenCount, false)
		sg.mu.diskTokensUsed[wt].writeByteTokens += diskTokenCount
	case admissionpb.SnapshotIngestStoreWorkType:
		// Do not apply the writeAmpLM since these writes do not incur additional
		// write-amp.
		sg.subtractDiskWriteTokensLocked(count, false)
		sg.mu.diskTokensUsed[wt].writeByteTokens += count
	}
}

func (sg *kvStoreTokenGranter) adjustDiskTokenError(stats DiskStats) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	sg.adjustDiskTokenErrorLocked(stats.BytesRead, stats.BytesWritten)
}

// staleStatTracker tracks consecutive intervals where disk stats don't change,
// and rate-limits logging at configurable thresholds. This helps assess whether
// the 1ms error correction tick rate is effective given the kernel's disk stat
// update frequency.
type staleStatTracker struct {
	// consecutiveCount is the current number of consecutive ticks where disk
	// stats have not changed.
	consecutiveCount int
	// thresholds defines the consecutive count values at which we log warnings.
	thresholds []int // e.g., [20, 50, 100]
	// cumExceededCounts tracks how many times consecutiveCount exceeded each
	// threshold before stats changed. Index i corresponds to thresholds[i].
	cumExceededCounts []uint64
	logLimiter        log.EveryN
}

func newStaleStatTracker() staleStatTracker {
	return staleStatTracker{
		thresholds:        []int{20, 50, 100},
		cumExceededCounts: make([]uint64, 3),
		logLimiter:        log.Every(time.Minute),
	}
}

// recordNoChange increments consecutiveCount and logs if a threshold is exceeded.
func (t *staleStatTracker) recordNoChange(ctx context.Context) {
	t.consecutiveCount++
	if t.shouldLog() {
		log.Dev.Infof(ctx,
			"disk token error adjustment: no stat change for %d consecutive intervals "+
				"(cumulative runs exceeding 20/50/100: %d/%d/%d)",
			t.consecutiveCount, t.cumExceededCounts[0], t.cumExceededCounts[1], t.cumExceededCounts[2])
	}
}

// recordChange records the completed run in the appropriate threshold bucket
// and resets consecutiveCount.
func (t *staleStatTracker) recordChange() {
	if t.consecutiveCount == 0 {
		return
	}

	// Record in highest matching threshold bucket.
	for i := len(t.thresholds) - 1; i >= 0; i-- {
		if t.consecutiveCount >= t.thresholds[i] {
			t.cumExceededCounts[i]++
			break
		}
	}
	t.consecutiveCount = 0
}

func (t *staleStatTracker) shouldLog() bool {
	for _, thresh := range t.thresholds {
		if t.consecutiveCount == thresh {
			// Always log if we've just exceeded a threshold
			return true
		} else if t.consecutiveCount > thresh {
			return t.logLimiter.ShouldLog()
		}
	}
	return false
}

// adjustDiskTokenErrorLocked is used to account for extra reads and writes that
// are in excess of tokens already deducted.
//
// (1) For writes, we deduct tokens at admission for each request. If the actual
// writes seen on disk for a given tick is higher than the tokens already
// deducted, the delta is the write error. This value is then subtracted from
// available disk tokens.
//
// (2) For reads, we do not deduct any tokens at admission. However, we deduct
// tokens in advance during token estimation in the diskBandwidthLimiter for the
// next adjustment interval. These pre-deducted tokens are then allocated at the
// same interval as write tokens. Any additional reads in the tick interval are
// considered error and are subtracted from the available disk write tokens.
//
// For both reads and writes, we reset the alreadyDeductedTokens to 0 for the
// next error tick interval, since we will use those to compare with the
// observed disk reads and writes for the next error tick interval.
//
// readBytes and writeBytes are the cumulative values retrieved from the disk stats.
func (sg *kvStoreTokenGranter) adjustDiskTokenErrorLocked(readBytes uint64, writeBytes uint64) {
	// On the first call, we only record the baseline cumulative values. We can't
	// compute error yet because we need a valid previous observation to compute
	// the delta (otherwise we'd treat total bytes since boot as "error").
	if !sg.mu.diskTokensError.initialized {
		sg.mu.diskTokensError.prevObservedWrites = writeBytes
		sg.mu.diskTokensError.prevObservedReads = readBytes
		sg.mu.diskTokensError.initialized = true
		return
	}

	intWrites := int64(writeBytes - sg.mu.diskTokensError.prevObservedWrites)
	intReads := int64(readBytes - sg.mu.diskTokensError.prevObservedReads)

	if intWrites == 0 && intReads == 0 {
		sg.mu.diskTokensError.staleStats.recordNoChange(context.Background())
	} else {
		sg.mu.diskTokensError.staleStats.recordChange()
	}

	errorAdjFunc := func(
		intObserved int64, intTokensDeducted int64, cumError *int64, absError *int64) {
		intError := intObserved - intTokensDeducted

		if intError == 0 {
			return
		}

		*cumError += intError
		absIntError := intError
		if absIntError < 0 {
			absIntError = -absIntError
		}
		*absError += absIntError

		// NB: the following also updates alreadyDeductedTokens.writeByteTokens,
		// which is harmless, since we reset it to 0 later in this function.
		sg.subtractDiskWriteTokensLocked(intError, false)
	}

	// Compensate for error due to writes.
	errorAdjFunc(intWrites, sg.mu.diskTokensError.alreadyDeductedTokens.writeByteTokens,
		&sg.mu.diskTokensError.cumError.writeByteTokens, &sg.mu.diskTokensError.absError.writeByteTokens)

	// Compensate for error due to reads.
	errorAdjFunc(intReads, sg.mu.diskTokensError.alreadyDeductedTokens.readByteTokens,
		&sg.mu.diskTokensError.cumError.readByteTokens, &sg.mu.diskTokensError.absError.readByteTokens)

	// Reset the deducted count for the next compensation interval.
	sg.mu.diskTokensError.alreadyDeductedTokens = diskTokens{}

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
		exhaustedDuration := now.Sub(sg.mu.exhaustedStart[wc])
		sg.ioTokensExhaustedDurationMetric[wc].Inc(exhaustedDuration.Nanoseconds())
		if sg.mu.availableIOTokens[wc] <= 0 {
			sg.mu.exhaustedStart[wc] = now
		}
	}
}

func (sg *kvStoreTokenGranter) subtractDiskWriteTokensLocked(
	count int64, settingAvailableTokens bool,
) {
	avail := sg.mu.diskTokensAvailable.writeByteTokens
	sg.mu.diskTokensAvailable.writeByteTokens -= count
	if settingAvailableTokens && sg.mu.diskTokensAvailable.writeByteTokens > sg.mu.diskWriteByteTokensCapacity {
		sg.mu.diskTokensAvailable.writeByteTokens = sg.mu.diskWriteByteTokensCapacity
	}
	if !settingAvailableTokens {
		sg.mu.diskTokensError.alreadyDeductedTokens.writeByteTokens += count
	}
	if count > 0 && avail > 0 && sg.mu.diskTokensAvailable.writeByteTokens <= 0 {
		// Transition from > 0 to <= 0.
		sg.mu.diskWriteByteTokensExhaustedStart = timeutil.Now()
	} else if count < 0 && avail <= 0 &&
		(sg.mu.diskTokensAvailable.writeByteTokens > 0 || settingAvailableTokens) {
		// Transition from <= 0 to > 0, or if we're setting available tokens. The
		// latter ensures that if the available tokens stay <= 0, we don't show a
		// sudden change in the metric after minutes of exhaustion.
		now := timeutil.Now()
		exhaustedDur := now.Sub(sg.mu.diskWriteByteTokensExhaustedStart)
		sg.diskWriteByteTokensExhaustedDurationMetric.Inc(exhaustedDur.Nanoseconds())
		if sg.mu.diskTokensAvailable.writeByteTokens <= 0 {
			sg.mu.diskWriteByteTokensExhaustedStart = now
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

// addAvailableTokens implements granterWithIOTokens.
func (sg *kvStoreTokenGranter) addAvailableTokens(
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

		// Stash the balance before resetting, so getDiskTokensUsedAndReset can
		// return it to the ioLoadListener for logging.
		sg.mu.prevIntervalDiskWriteTokensRemaining = sg.mu.diskTokensAvailable.writeByteTokens
		// We also want to avoid very negative disk write tokens, so we reset them.
		sg.mu.diskTokensAvailable.writeByteTokens = max(sg.mu.diskTokensAvailable.writeByteTokens, 0)
	}
	var w admissionpb.WorkClass
	for w = 0; w < admissionpb.NumWorkClasses; w++ {
		sg.availableTokensMetric[w].Update(sg.mu.availableIOTokens[w])
	}
	sg.mu.startingIOTokens = sg.mu.availableIOTokens[admissionpb.RegularWorkClass]

	// Allocate disk write and read tokens.
	sg.mu.diskWriteByteTokensCapacity = diskWriteTokensCapacity
	sg.subtractDiskWriteTokensLocked(-diskWriteTokens, true)
	// The read tokens are not added to a token bucket, and instead are
	// considered "already deducted" since they were pre-deducted when computing
	// the write tokens in the diskBandwidthLimiter. The actual observed reads
	// in the error accounting loop will be compared against these pre-deducted
	// tokens to compute read error.
	//
	// NB: We don't cap the disk read tokens as they are only compared during
	// the error accounting loop. So essentially, we give reads the "burst"
	// capacity of the error accounting interval. See
	// `adjustDiskTokenErrorLocked` for the error accounting logic, and where we
	// reset this bucket to 0.
	sg.mu.diskTokensError.alreadyDeductedTokens.readByteTokens += diskReadTokens

	return ioTokensUsed, ioTokensUsedByElasticWork
}

// diskErrorStats holds various error stats for disk token accounting. See the
// corresponding fields in kvStoreTokenGranter.mu.diskTokensError for the
// explanation.
type diskErrorStats struct {
	cumError diskTokens
	absError diskTokens
}

// getDiskTokensUsedAndResetLocked implements granterWithIOTokens. It is
// called at the start of each adjustment interval to get various stats useful
// for calculating the next interval's token allocation.
func (sg *kvStoreTokenGranter) getDiskTokensUsedAndReset() (
	usedTokens [admissionpb.NumStoreWorkTypes]diskTokens,
	errStats diskErrorStats,
	remainingDiskWriteTokens int64,
) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	for i := 0; i < admissionpb.NumStoreWorkTypes; i++ {
		usedTokens[i] = sg.mu.diskTokensUsed[i]
		sg.mu.diskTokensUsed[i] = diskTokens{}
	}
	diskErrStats := diskErrorStats{
		cumError: sg.mu.diskTokensError.cumError,
		absError: sg.mu.diskTokensError.absError,
	}
	sg.mu.diskTokensError.cumError = diskTokens{}
	sg.mu.diskTokensError.absError = diskTokens{}
	remainingDiskWriteTokens = sg.mu.prevIntervalDiskWriteTokensRemaining
	sg.mu.prevIntervalDiskWriteTokensRemaining = 0
	return usedTokens, diskErrStats, remainingDiskWriteTokens
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
	// NB: there is a small inconsistency here in that the linear model applied
	// to the originalTokens may have been different, since the linear model
	// changes every 15s. So we may have previously deducted something different
	// from originalDiskTokens. We accept that inconsistency in token accounting
	// for simplicity.
	originalDiskTokens := sg.mu.writeAmpLM.applyLinearModel(originalTokens)
	additionalDiskWriteTokens := actualDiskWriteTokens - originalDiskTokens
	sg.subtractDiskWriteTokensLocked(additionalDiskWriteTokens, false)
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

func (sg *kvStoreTokenGranter) hasExhaustedDiskTokens() bool {
	return sg.diskWriteByteTokensExhaustedDurationMetric.Count() > 0
}

// StoreMetrics are the metrics and some config information for a store.
type StoreMetrics struct {
	StoreID roachpb.StoreID
	*pebble.Metrics
	WriteStallCount int64
	// DiskUnhealthy corresponds to Engine.GetDiskUnhealthy.
	DiskUnhealthy bool
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
		Name: "admission.granter.slots_exhausted_duration.kv",
		Help: "Total duration when KV slots were exhausted, as observed by the slot " +
			"granter (not waiters). This is reported in nanoseconds from 26.1 onwards, " +
			"and was microseconds before that.",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	// We have a metric for both short and long period. These metrics use the
	// period provided in CPULoad and not wall time. So if the sum of the rate
	// of these two is < 1sec/sec, the CPULoad ticks are not happening at the
	// expected frequency (this could happen due to CPU overload).
	kvCPULoadShortPeriodDuration = metric.Metadata{
		Name: "admission.granter.cpu_load_short_period_duration.kv",
		Help: "Total duration when CPULoad was being called with a short period. This is " +
			"reported in nanoseconds from 26.1 onwards, and was microseconds before that.",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	kvCPULoadLongPeriodDuration = metric.Metadata{
		Name: "admission.granter.cpu_load_long_period_duration.kv",
		Help: "Total duration when CPULoad was being called with a long period. This is " +
			"reported in nanoseconds from 26.1 onwards, and was microseconds before that.",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
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
		Name: "admission.granter.io_tokens_exhausted_duration.kv",
		Help: "Total duration when IO tokens were exhausted, as observed by the token granter " +
			"(not waiters). This is reported in nanoseconds from 26.1 onwards, and was " +
			"microseconds before that.",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	kvElasticIOTokensExhaustedDuration = metric.Metadata{
		Name: "admission.granter.elastic_io_tokens_exhausted_duration.kv",
		Help: "Total duration when Elastic IO tokens were exhausted, as observed by the token " +
			"granter (not waiters). This is reported in nanoseconds from 26.1 onwards, and was " +
			"microseconds before that.",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
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
	kvDiskWriteByteTokensExhaustedDuration = metric.Metadata{
		Name: "admission.granter.disk_write_byte_tokens_exhausted_duration.kv",
		Help: "Total duration (in nanos) when disk write byte tokens were exhausted, as observed by " +
			"the token granter (not waiters)",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
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
