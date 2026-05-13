// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"cmp"
	"context"
	"fmt"
	"math/rand"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

// rangeSkippedDueToFailedConstraintsLogEvery rate limits the warning log for
// ranges skipped due to failed constraint analysis to avoid spamming.
var rangeSkippedDueToFailedConstraintsLogEvery = log.Every(time.Minute)

// rangeSkippedDueToFailedConstraintsShouldLog returns true if the warning for
// ranges skipped due to failed constraints should be logged. In test builds, it
// always returns true; in production, it rate-limits to once per minute.
func rangeSkippedDueToFailedConstraintsShouldLog() bool {
	return buildutil.CrdbTestBuild || rangeSkippedDueToFailedConstraintsLogEvery.ShouldLog()
}

var storeAndLeasePreferencePool = newSlicePool[storeAndLeasePreference](8)
var storeIDPool = newSlicePool[roachpb.StoreID](10)
var candidateInfoPool = newSlicePool[candidateInfo](8)

var nodeLoadMapPool = newMapPool[roachpb.NodeID, *NodeLoad](8)
var storeIDStructMapPool = newMapPool[roachpb.StoreID, struct{}](8)

// rebalanceEnv tracks the state and outcomes of a rebalanceStores invocation.
// Recall that such an invocation is on behalf of a local store, but will
// iterate over a slice of shedding stores - these are not necessarily local
// but are instead stores which are overloaded and need to shed load. Typically,
// the particular calling local store has at least some leases (i.e. is
// capable of making decisions), though in edge cases this may not be true,
// in which case the rebalanceStores invocation will simply not be able to
// do anything.
type rebalanceEnv struct {
	*clusterState
	// rng is the random number generator for non-deterministic decisions.
	rng *rand.Rand
	// dsm is the diversity scoring memo for computing diversity scores.
	dsm *diversityScoringMemo
	// changes accumulates the pending range changes made during rebalancing.
	changes []ExternalRangeChange
	// rangeMoveCount tracks the number of range moves made.
	rangeMoveCount int
	// maxRangeMoveCount is the maximum number of range moves allowed in the
	// context of the current rebalanceStores invocation (across multiple
	// shedding stores).
	maxRangeMoveCount int
	// maxLeaseTransferCount is the maximum number of lease transfers allowed in
	// the context of the current rebalanceStores invocation. Note that because
	// leases can only be shed from the particular local store on whose behalf
	// rebalanceStores was called, this limit applies to this particular one
	// store, and thus needs to be checked against only in that single
	// rebalanceLeasesFromLocalStoreID invocation.
	maxLeaseTransferCount int
	// If a store's maxFractionPendingDecrease is at least this threshold, no
	// further changes should be made at this time. This is because the inflight
	// operation's changes to the load are estimated, and we don't want to pile up
	// too many possibly inaccurate estimates.
	fractionPendingIncreaseOrDecreaseThreshold float64
	// lastFailedChangeDelayDuration is the delay after a failed change before retrying.
	lastFailedChangeDelayDuration time.Duration
	// now is the timestamp representing the start time of the current
	// rebalanceStores invocation.
	now time.Time
	// passObs is used to collect gauge metrics and logging for this rebalancing
	// pass. Can be nil.
	passObs *rebalancingPassMetricsAndLogger
}

// passObv can be nil.
func newRebalanceEnv(
	cs *clusterState,
	rng *rand.Rand,
	dsm *diversityScoringMemo,
	now time.Time,
	passObs *rebalancingPassMetricsAndLogger,
) *rebalanceEnv {

	// NB: these consts are intentionally local to the constructor, proving
	// that they're not accessed anywhere else.
	const (
		// The current usage of the multi-metric allocator has rebalanceStores called
		// in a loop while it emits work, and on a timer otherwise. This means that
		// we don't need to emit many changes per invocation. Especially for range
		// moves, which require moving significant amounts of data, emitting them one
		// by one is fine and by doing so we operate on more recent information at each
		// turn. Even if the caller carried out multiple changes concurrently, we'd
		// want to be careful not to emit too many range moves at once, since they are
		// expensive.
		// Lease transfers are cheap and fast, so we emit a few more to avoid frequent
		// trips through rebalanceStores which could cause some logging noise.
		maxRangeMoveCount     = 1
		maxLeaseTransferCount = 8
		// TODO(sumeer): revisit this value.
		fractionPendingIncreaseOrDecreaseThreshold = 0.1

		// See the long comment where rangeState.lastFailedChange is declared.
		lastFailedChangeDelayDuration time.Duration = 60 * time.Second
	)

	re := &rebalanceEnv{
		clusterState:          cs,
		rng:                   rng,
		dsm:                   dsm,
		now:                   now,
		passObs:               passObs,
		maxRangeMoveCount:     maxRangeMoveCount,
		maxLeaseTransferCount: maxLeaseTransferCount,
		fractionPendingIncreaseOrDecreaseThreshold: fractionPendingIncreaseOrDecreaseThreshold,
		lastFailedChangeDelayDuration:              lastFailedChangeDelayDuration,
	}
	return re
}

type sheddingStore struct {
	roachpb.StoreID
	// storeLoadSummary is relative to the entire cluster (not a set of valid
	// replacement stores for some particular replica), see the comment where
	// sheddingStores are constructed.
	storeLoadSummary
	// withinLeaseSheddingGracePeriod and iLevel are the result of
	// classifyOverload, computed once when the store is identified as
	// overloaded and reused by both the shedding path (rebalanceStore) and
	// the no-shed path so both paths attribute the store to the same
	// overload bucket.
	withinLeaseSheddingGracePeriod bool
	iLevel                         ignoreLevel
	// cannotShed is true when the store is overloaded but rebalanceStores
	// decided up front not to attempt shedding from it this pass — e.g.
	// because of pending decrease/increase. Such stores are still recorded
	// (storeOverloaded + finishStore) so they contribute to the per-bucket
	// `skipped` outcome; rebalanceStore is not invoked on them. The
	// detection-site log line records why.
	cannotShed bool
}

// classifyOverload returns the overload classification for the given store,
// derived from how long the store has been overloaded and whether its CPU
// dimension is currently above overloadSlow.
//
// withinLeaseSheddingGracePeriod is true when the store is overloaded on the
// CPU dimension but still within the initial grace period where the overloaded
// store is expected to shed its own leases. For remote stores, MMA skips
// shedding during this period. For the local store, MMA proceeds with shedding
// and the lease_grace metrics track whether that self-shedding succeeds.
//
// iLevel selects how strict MMA is about target stores during replica
// shedding: it relaxes as overloadDur grows past the corresponding grace
// thresholds.
func classifyOverload(
	ss *storeState, sls storeLoadSummary, now time.Time,
) (withinLeaseSheddingGracePeriod bool, iLevel ignoreLevel) {
	overloadDur := now.Sub(ss.overloadStartTime)
	iLevel = ignoreLoadNoChangeAndHigher
	if overloadDur > ignoreHigherThanLoadThresholdGraceDuration {
		iLevel = ignoreHigherThanLoadThreshold
	} else if overloadDur > ignoreLoadThresholdAndHigherGraceDuration {
		iLevel = ignoreLoadThresholdAndHigher
	}
	withinLeaseSheddingGracePeriod = sls.dimSummary[CPURate] >= overloadSlow &&
		overloadDur < leaseSheddingGraceDuration
	return withinLeaseSheddingGracePeriod, iLevel
}

// Called periodically, say every 10s.
//
// We do not want to shed replicas for CPU from a remote store until its had a
// chance to shed leases.
func (re *rebalanceEnv) rebalanceStores(
	ctx context.Context, localStoreID roachpb.StoreID,
) []ExternalRangeChange {
	re.mmaid++
	id := re.mmaid
	ctx = logtags.AddTag(ctx, "mmaid", id)

	// Promote the outer-loop narrative to Infof once per outerLogInterval,
	// only on the first iteration of the production rebalance loop
	// (passObs != nil). When not promoted, the bridge falls back to
	// VEventfDepth at each call site, gated by vmodule/verbose-span at
	// the source location of the logf call (not at this construction
	// site).
	passVerboseToInfof := re.passObs != nil && re.outerLogEvery.ShouldProcess(re.now)
	passML := makeMMALogger(passVerboseToInfof)

	passML.logf(ctx, 2, "rebalanceStores begins")
	// To select which stores are overloaded, we use a notion of overload that
	// is based on cluster means (and of course individual store/node
	// capacities). We do not want to loop through all ranges in the cluster,
	// and for each range and its constraints expression decide whether any of
	// the replica stores is overloaded, since O(num-ranges) work during each
	// allocator pass is not scalable.
	//
	// If cluster mean is too low, more will be considered overloaded. This is
	// ok, since then when we look at ranges we will have a different mean for
	// the constraint satisfying candidates and if that mean is higher we may
	// not do anything. There is wasted work, but we can bound it by typically
	// only looking at a random K ranges for each store.
	//
	// If the cluster mean is too high, we will not rebalance across subsets
	// that have a low mean. Seems fine, if we accept that rebalancing is not
	// responsible for equalizing load across two nodes that have 30% and 50%
	// cpu utilization while the cluster mean is 70% utilization (as an
	// example).
	clusterMeans, ok := re.meansMemo.getMeans(nil)
	if !ok {
		passML.logf(ctx, 2, "no stores known to allocator; skipping rebalancing pass")
		return nil
	}
	var sheddingStores []sheddingStore
	passML.logf(ctx, 2,
		"cluster means: (stores-load %s) (stores-capacity %s) (nodes-cpu-load %d) (nodes-cpu-capacity %d)",
		clusterMeans.storeLoad.load, clusterMeans.storeLoad.capacity,
		clusterMeans.nodeLoad.loadCPU, clusterMeans.nodeLoad.capacityCPU)
	// NB: We don't attempt to shed replicas or leases from a store which is
	// fdDrain or fdDead, nor do we attempt to shed replicas from a store which
	// is storeMembershipRemoving (decommissioning). These are currently handled
	// via replicate_queue.go.

	// Need deterministic order for datadriven testing.
	storeStateSlice := make([]*storeState, 0, len(re.stores))
	for _, ss := range re.stores {
		storeStateSlice = append(storeStateSlice, ss)
	}
	slices.SortFunc(storeStateSlice, func(a, b *storeState) int {
		return cmp.Compare(a.StoreID, b.StoreID)
	})

	for _, ss := range storeStateSlice {
		storeID := ss.StoreID
		sls := re.meansMemo.getStoreLoadSummary(ctx, clusterMeans, storeID, ss.loadSeqNum)
		passML.logf(ctx, 2, "evaluating s%d: node load %s, store load %s, worst dim %s",
			storeID, sls.nls, sls.sls, sls.worstDim)

		if sls.sls >= overloadSlow {
			if ss.overloadEndTime != (time.Time{}) {
				if re.now.Sub(ss.overloadEndTime) > overloadGracePeriod {
					ss.overloadStartTime = re.now
					clear(ss.lastDetailedLogTimes)
					passML.logf(ctx, 2, "overload-start s%v (%v) - grace period expired", storeID, sls)
				} else {
					// Else, extend the previous overload interval.
					passML.logf(ctx, 2, "overload-continued s%v (%v) - within grace period", storeID, sls)
				}
				ss.overloadEndTime = time.Time{}
			}
			// The store is overloaded. Classify once and append it to
			// sheddingStores. The single processing loop below pairs each
			// entry with storeOverloaded + finishStore exactly once, even
			// for entries that turn out to be unsheddable (pending
			// decrease/increase, or per-pass range-move budget exhausted).
			withinGrace, iLevel := classifyOverload(ss, sls, re.now)
			// The pending decrease must be small enough to continue shedding,
			// and there should be no pending increase (since pending-increase
			// estimates can be overestimates).
			cannotShed := ss.maxFractionPendingDecrease >= re.fractionPendingIncreaseOrDecreaseThreshold ||
				ss.maxFractionPendingIncrease >= epsilon
			passML.logf(ctx, 2,
				"adding overloaded store s%d to shedding list (worst dim: %s, cannotShed: %t, "+
					"pending decrease %.2f vs threshold %.2f, pending increase %.2f vs epsilon %.2f); pending: %s",
				storeID, sls.worstDim, cannotShed,
				ss.maxFractionPendingDecrease, re.fractionPendingIncreaseOrDecreaseThreshold,
				ss.maxFractionPendingIncrease, epsilon,
				formatLoadPendingChanges(ss.adjusted.loadPendingChanges))
			sheddingStores = append(sheddingStores, sheddingStore{
				StoreID:                        storeID,
				storeLoadSummary:               sls,
				withinLeaseSheddingGracePeriod: withinGrace,
				iLevel:                         iLevel,
				cannotShed:                     cannotShed,
			})
		} else if sls.sls < loadNoChange && ss.overloadEndTime == (time.Time{}) {
			// NB: we don't stop the overloaded interval if the store is at
			// loadNoChange, since a store can hover at the border of the two.
			passML.logf(ctx, 2, "overload-end s%v (%v) - load dropped below no-change threshold", storeID, sls)
			ss.overloadEndTime = re.now
		}
	}

	// We have used storeLoadSummary.sls to filter above. But when sorting, we
	// first compare using nls. Consider the following scenario with 4 stores
	// per node: node n1 is at 90% cpu utilization and has 4 stores each
	// contributing 22.5% cpu utilization. Node n2 is at 60% utilization, and
	// has 1 store contributing all 60%. When looking at rebalancing, we want to
	// first shed load from the stores of n1, before we shed load from the store
	// on n2. Sheddable stores sort before unsheddable (cannotShed) ones so
	// the budget cutoff below applies to actionable candidates first.
	slices.SortFunc(sheddingStores, func(a, b sheddingStore) int {
		// Sheddable stores first.
		if a.cannotShed != b.cannotShed {
			if a.cannotShed {
				return 1
			}
			return -1
		}
		// Prefer to shed from the local store first.
		if a.StoreID == localStoreID {
			return -1
		} else if b.StoreID == localStoreID {
			return 1
		}
		// Use StoreID for tie-breaker for determinism.
		return cmp.Or(-cmp.Compare(a.nls, b.nls), -cmp.Compare(a.sls, b.sls),
			cmp.Compare(a.StoreID, b.StoreID))
	})

	if re.passObs != nil && len(sheddingStores) > 0 {
		var buf redact.StringBuilder
		buf.SafeString("sorted shedding stores:")
		for _, store := range sheddingStores {
			buf.Printf(" (s%d: %s, cannotShed: %t)", store.StoreID, store.sls, store.cannotShed)
		}
		log.KvDistribution.Infof(ctx, "%s", buf.RedactableString())
	}

	// Single processing loop. Each entry — sheddable or not, in-budget or
	// not — is bookended by exactly one storeOverloaded/finishStore pair,
	// so every overloaded store is classified once and the per-bucket
	// invariant in computePassSummary holds by construction.
	rangeMoveLimitReachedLogged := false
	for _, store := range sheddingStores {
		ss := re.stores[store.StoreID]
		// Decide per-shedding-store whether to promote detailed logs to
		// Infof: persistently overloaded and not recently logged. Gated
		// on passObs != nil so only the first iteration of the production
		// rebalance loop emits. When not promoted, the bridge falls back
		// to VEventfDepth at each call site, gated by vmodule/verbose-
		// span at the source location of the logf call.
		overloadDur := re.now.Sub(ss.overloadStartTime)
		persistentOverload := re.passObs != nil &&
			overloadDur >= persistentOverloadThreshold &&
			re.now.Sub(ss.lastDetailedLogTimes[localStoreID]) >= detailedLogInterval
		ml := makeMMALogger(persistentOverload)

		re.passObs.storeOverloaded(ctx, store.StoreID, store.withinLeaseSheddingGracePeriod, store.iLevel)
		// NB: we don't have to check the maxLeaseTransferCount here since only one
		// store can transfer leases - the local store. So the limit is only checked
		// inside of the corresponding rebalanceLeasesFromLocalStoreID call, but
		// not in this outer loop.
		switch {
		case store.cannotShed:
			// Detection-site log line already explained the reason.
		case re.rangeMoveCount >= re.maxRangeMoveCount:
			if !rangeMoveLimitReachedLogged {
				ml.logf(ctx, 2, "reached max range move count %d, stopping further rebalancing",
					re.maxRangeMoveCount)
				rangeMoveLimitReachedLogged = true
			}
		default:
			re.rebalanceStore(ctx, store, localStoreID, ml)
			// Only update the timer for persistent overload, not when the
			// bridge was set up via verbose logging. Otherwise, turning off
			// verbose logging would suppress the next persistent-overload
			// burst for detailedLogInterval.
			if persistentOverload {
				if ss.lastDetailedLogTimes == nil {
					ss.lastDetailedLogTimes = make(map[roachpb.StoreID]time.Time)
				}
				ss.lastDetailedLogTimes[localStoreID] = re.now
			}
		}
		re.passObs.finishStore(ctx)
	}
	re.passObs.finishRebalancingPass(ctx, len(sheddingStores) /* for assertion only */)
	return re.changes
}

// rebalanceStore attempts to shed load from a single overloaded store via lease
// transfers and/or replica rebalances.
//
// # Candidate Filtering Strategy
//
// When selecting rebalance targets, we filter candidates in two phases. The key
// consideration is which stores should be included when computing load means,
// since the means determine whether a store looks "underloaded" (good target)
// or "overloaded" (bad target).
//
// **Pre-means filtering** excludes stores with a non-OK disposition from the
// mean. The disposition serves as the source of truth for "can this store
// accept work?" — whether due to drain, maintenance, disk capacity, or any
// other reason. Note that unhealthy stores always have non-OK disposition, so
// disposition indirectly reflects health.
//
// Ill-disposed stores cannot participate as targets and it is correct to
// exclude them from the mean, as the mean helps us answer the question "among
// viable targets, who is underloaded?". If ill-disposed storeswere included in
// the mean, they could skew it down, making viable targets look too overloaded
// to be considered, and preventing resolution of a load imbalance. They could
// also skew the mean up and make actually-overloaded targets look underloaded,
// but this is less of an issue: since both source and target are evaluated
// relative to the same mean before accepting a transfer, their relative
// positions are preserved, and we wouldn't accept a transfer that leaves the
// target worse off than the source.
//
// Note on ill-disposed vs overloaded: Ill-disposed stores (Dead, Suspect,
// Draining, Decommissioning, etc.) are excluded from means because they may
// report unreliable load signals. In contrast, overloaded stores (high load but
// otherwise healthy) are *always included* in means because they report
// accurate load and are essential for computing a stable mean. This principle
// also applies to the replicate and lease queue integration: even though those
// queues work towards count-based convergence, overloaded stores' replica and
// lease counts are still included when computing the count mean. This may
// produce a less precise mean where we think we can move replicas/leases but no
// good target exists, but it avoids the instability of excluding overloaded
// stores: if excluded, the mean shifts, the store calms down, gets re-included,
// and so on.
//
// TODO(mma): Revisit if we find cases where ill-disposed stores should be
// included in the mean (e.g., suspect stores with valid load signals). It makes
// sense to exclude them if their load signals are unreliable. But if a store
// becomes suspect due to high load (but is otherwise healthy with reliable
// signals), it should arguably not be marked ill-disposed. If excluded from
// the mean, it creates thrashing: the mean shifts, the store calms down, gets
// re-included, flares up again, and so on. Keeping it in the mean lets it shed
// load via ordinary rebalancing paths. However, we cannot tell if a store's
// load is high/low until we have computed the mean.
//
// **Post-means filtering** prevents some of the remaining candidates from being
// chosen as targets for tactical reasons:
//   - Stores that would be worse off than the source (relative to the mean
//     across the candidate set), to reduce thrashing. For example, if a store
//     were overloaded due to a single very hot range, moving that range to
//     another store would just shift the problem elsewhere and cause thrashing.
//   - Stores with too much pending inflight work (reduced confidence in load arithmetic).
//
// NB: TestClusterState tests various scenarios and edge cases that demonstrate
// filtering behavior and outcomes.
func (re *rebalanceEnv) rebalanceStore(
	ctx context.Context, store sheddingStore, localStoreID roachpb.StoreID, ml mmaLogger,
) {
	ml.logf(ctx, 2, "start processing shedding store s%d: cpu node load %s, store load %s, worst dim %s",
		store.StoreID, store.nls, store.sls, store.worstDim)
	ss := re.stores[store.StoreID]

	topKRanges := ss.adjusted.topKRanges[localStoreID]
	n := topKRanges.len()
	// Debug logging.
	if ml.V(ctx, 2) {
		if n > 0 {
			var buf redact.StringBuilder
			buf.Printf("top-K[%d] ranges for s%d with lease on local s%d:", topKRanges.dim,
				store.StoreID, localStoreID)
			for i := 0; i < n; i++ {
				rangeID := topKRanges.index(i)
				rstate := re.ranges[rangeID]
				load := rstate.load.Load
				if !ss.adjusted.replicas[rangeID].IsLeaseholder {
					load[CPURate] = rstate.load.RaftCPU
				}
				buf.Printf(" r%d:%v", rangeID, load)
			}
			ml.logf(ctx, 2, "%s", buf.RedactableString())
		} else {
			ml.logf(ctx, 2, "no top-K[%s] ranges found for s%d with lease on local s%d",
				topKRanges.dim, store.StoreID, localStoreID)
		}
	}
	if n == 0 {
		return
	}

	// Consider a cluster where s1 is overloadSlow, s2 is loadNoChange, and
	// s3, s4 are loadNormal. Now s4 is considering rebalancing load away
	// from s1, but the candidate top-k range has replicas {s1, s3, s4}. So
	// the only way to shed load from s1 is a s1 => s2 move. But there may
	// be other ranges at other leaseholder stores which can be moved from
	// s1 => {s3, s4}. So we should not be doing this sub-optimal transfer
	// of load from s1 => s2 unless s1 is not seeing any load shedding for
	// some interval of time. We need a way to capture this information in a
	// simple but effective manner. For now, we capture this using these
	// grace duration thresholds (see classifyOverload). The classification
	// was computed when the store was added to sheddingStores; we just read
	// it back here. The outer loop calls storeOverloaded/finishStore for us.
	if store.withinLeaseSheddingGracePeriod && store.StoreID != localStoreID {
		ml.logf(ctx, 2, "skipping remote store s%d: in lease shedding grace period", store.StoreID)
		return
	}

	if ss.StoreID == localStoreID {
		if store.dimSummary[CPURate] >= overloadSlow {
			// The store which called into rebalanceStore is overloaded. Try to shed
			// load from it via lease transfers first. Note that if we have multiple
			// stores, this rebalanceStore invocation is on behalf of exactly one of
			// them, and that's the one we're going to shed from here - other stores
			// will do it when they call into rebalanceStore.
			if numTransferred := re.rebalanceLeasesFromLocalStoreID(ctx, ss, store, localStoreID, ml); numTransferred > 0 {
				// If any leases were transferred, wait for these changes to be done
				// before shedding replicas from this store (which is more costly).
				// Otherwise, we may needlessly start moving replicas when we could
				// instead have moved more leases in the next invocation. Note that the
				// store rebalancer will call the rebalance method again after the lease
				// transfer is done, and we may still be considering those transfers as
				// pending from a load perspective, so we *may* not be able to do more
				// lease transfers -- so be it.
				//
				// TODO(tbg): rather than skipping replica transfers when there were ANY
				// lease transfers, we could instead skip them only if we hit a limit in
				// transferring leases. If we didn't hit a limit, this indicates that we
				// did consider all of the possible replicas to transfer a lease for,
				// and came up short - it then makes sense to consider replica transfers.
				// The current heuristic instead bails early, and an immediate call to
				// rebalanceStores would likely be made, so that the results could
				// ultimately be the same (mod potentially some logging noise as we
				// iterate through rebalanceStores more frequently).
				ml.logf(ctx, 2, "skipping replica transfers for s%d to try more leases next time",
					ss.StoreID)
				return
			}
		} else {
			ml.logf(ctx, 2, "skipping lease shedding for calling store s%s: not cpu overloaded: %v",
				localStoreID, store.dimSummary[CPURate])
		}
	}

	ml.logf(ctx, 2, "attempting to shed replicas next")
	re.rebalanceReplicas(ctx, store, ss, localStoreID, store.iLevel, ml)
}

func (re *rebalanceEnv) rebalanceReplicas(
	ctx context.Context,
	store sheddingStore,
	ss *storeState,
	localStoreID roachpb.StoreID,
	ignoreLevel ignoreLevel,
	ml mmaLogger,
) {
	if store.StoreID != localStoreID && store.dimSummary[CPURate] >= overloadSlow &&
		re.now.Sub(ss.overloadStartTime) < leaseSheddingGraceDuration {
		ml.logf(ctx, 2, "skipping remote store s%d: in lease shedding grace period", store.StoreID)
		return
	}
	// Iterate over top-K ranges first and try to move them.
	topKRanges := ss.adjusted.topKRanges[localStoreID]
	n := topKRanges.len()
	loadDim := topKRanges.dim
	pPostMeansExcl := storeIDPool.get()
	pExistingReplicas := storeIDPool.get()
	defer func() {
		storeIDPool.put(pPostMeansExcl)
		storeIDPool.put(pExistingReplicas)
	}()
	for i := 0; i < n; i++ {
		if re.rangeMoveCount >= re.maxRangeMoveCount {
			ml.logf(ctx, 2,
				"reached max range move count %d; done shedding", re.maxRangeMoveCount)
			return
		}
		if ss.maxFractionPendingDecrease >= re.fractionPendingIncreaseOrDecreaseThreshold {
			ml.logf(ctx, 2,
				"s%d has reached pending decrease threshold(%.2f>=%.2f) after rebalancing; done shedding",
				store.StoreID, ss.maxFractionPendingDecrease, re.fractionPendingIncreaseOrDecreaseThreshold)
			// TODO(sumeer): For regular rebalancing, we will wait until those top-K
			// move and then continue with the rest. There is a risk that the top-K
			// have some constraint that prevents rebalancing, while the rest can be
			// moved. Running with underprovisioned clusters and expecting load-based
			// rebalancing to work well is not in scope.
			return
		}

		rangeID := topKRanges.index(i)
		// TODO(sumeer): the following code belongs in a closure, since we will
		// repeat it for some random selection of non topKRanges.
		rstate := re.ranges[rangeID]
		if len(rstate.pendingChanges) > 0 {
			// If the range has pending changes, don't make more changes.
			ml.logf(ctx, 3, "skipping r%d: has pending changes", rangeID)
			re.passObs.replicaShed(ctx, rangeTransient)
			continue
		}
		if re.now.Sub(rstate.lastFailedChange) < re.lastFailedChangeDelayDuration {
			ml.logf(ctx, 3, "skipping r%d: too soon after failed change", rangeID)
			re.passObs.replicaShed(ctx, rangeTransient)
			continue
		}
		re.ensureAnalyzedConstraints(ctx, rstate)
		if rstate.constraints == nil {
			if rangeSkippedDueToFailedConstraintsShouldLog() {
				log.KvDistribution.Warningf(ctx,
					"skipping r%d: no constraints analyzed (conf=%v replicas=%v)",
					rangeID, rstate.conf, rstate.replicas)
			}
			re.passObs.replicaShed(ctx, rangeConstraintsError)
			continue
		}
		isVoter, isNonVoter := rstate.constraints.replicaRole(store.StoreID)
		if !isVoter && !isNonVoter {
			// Two staleness sources can land us here despite the
			// skip-on-pending check above: a stale topK[localStoreID] entry
			// pointing at a range where store.StoreID is no longer a replica
			// (topK is rebuilt only on StoreLeaseholderMsg — see the topKRanges
			// invariant in cluster_state.go), or a stale rs.constraints cache
			// from before a pending change mutated rs.replicas. A follow-up
			// commit converts this assertion into a skip+log so a benign cache
			// race no longer fatals the node.
			panic(errors.AssertionFailedf("internal state inconsistency: "+
				"store=%v range_id=%v pending-changes=%v "+
				"rstate_replicas=%v rstate_constraints=%v",
				store.StoreID, rangeID, rstate.pendingChanges, rstate.replicas, rstate.constraints))
		}
		// Get the constraint conjunction which will allow us to look up stores
		// that could replace the shedding store.
		var conj constraintsConj
		var err error
		if isVoter {
			conj, err = rstate.constraints.candidatesToReplaceVoterForRebalance(store.StoreID)
		} else {
			conj, err = rstate.constraints.candidatesToReplaceNonVoterForRebalance(store.StoreID)
		}
		if err != nil {
			// This range has some constraints that are violated. Let those be
			// fixed first.
			re.passObs.replicaShed(ctx, rangeConstraintsViolated)
			ml.logf(ctx, 3, "skipping r%d: constraint violation needs fixing first: %v", rangeID, err)
			continue
		}
		// Build post-means exclusions: stores whose load is included in the mean
		// (they're viable locations in principle) but aren't valid targets for
		// this specific transfer.
		//
		// NB: to prevent placing replicas on multiple CRDB nodes sharing a
		// host, we'd need to make changes here.
		// See: https://github.com/cockroachdb/cockroach/issues/153863
		postMeansExclusions := storeSet((*pPostMeansExcl)[:0])
		existingReplicas := storeSet((*pExistingReplicas)[:0])
		for _, replica := range rstate.replicas {
			storeID := replica.StoreID
			existingReplicas.insert(storeID)
			if storeID == store.StoreID {
				// Exclude the shedding store (we're moving away from it), but not
				// other stores on its node (within-node rebalance is allowed).
				postMeansExclusions.insert(storeID)
				continue
			}
			// Exclude all stores on nodes with other existing replicas.
			nodeID := re.stores[storeID].NodeID
			for _, storeID := range re.nodes[nodeID].stores {
				postMeansExclusions.insert(storeID)
			}
		}

		// Compute the candidates. These are already filtered down to only those stores
		// that we'll actually be happy to transfer the range to.
		// Note that existingReplicas is a subset of postMeansExclusions, so they'll
		// be included in the mean, but are never considered as candidates.
		*pPostMeansExcl = []roachpb.StoreID(postMeansExclusions)
		*pExistingReplicas = []roachpb.StoreID(existingReplicas)
		cands, ssSLS := re.computeCandidatesForReplicaTransfer(ctx, conj, existingReplicas, postMeansExclusions, store.StoreID, re.passObs, ml)
		ml.logf(ctx, 3, "considering replica-transfer r%v from s%v: store load %v",
			rangeID, store.StoreID, ss.adjusted.load)
		if ml.V(ctx, 3) {
			var buf redact.StringBuilder
			buf.Printf("candidates for r%d:", rangeID)
			for _, c := range cands.candidates {
				buf.Printf("\n\ts%d: %s", c.StoreID, c.storeLoadSummary)
			}
			ml.logf(ctx, 3, "%s", buf.RedactableString())
		}

		if len(cands.candidates) == 0 {
			ml.logf(ctx, 3, "result(failed): no candidates found for r%d after exclusions", rangeID)
			continue
		}
		var rlocalities replicasLocalityTiers
		if isVoter {
			rlocalities = rstate.constraints.voterLocalityTiers
		} else {
			rlocalities = rstate.constraints.replicaLocalityTiers
		}
		localities := re.dsm.getExistingReplicaLocalities(rlocalities)
		isLeaseholder := rstate.constraints.leaseholderID == store.StoreID
		// Set the diversity score and lease preference index of the candidates.
		for _, cand := range cands.candidates {
			cand.diversityScore = localities.getScoreChangeForRebalance(
				ss.localityTiers, re.stores[cand.StoreID].localityTiers)
			if isLeaseholder {
				cand.leasePreferenceIndex = matchedLeasePreferenceIndex(
					cand.StoreID, rstate.constraints.spanConfig.leasePreferences, re.constraintMatcher)
			}
		}
		switch ignoreLevel {
		case ignoreHigherThanLoadThreshold:
			ml.logf(ctx, 3, "using level %v (threshold:%v) for r%d based on overload duration %v",
				ignoreLevel, ssSLS.sls, rangeID, re.now.Sub(ss.overloadStartTime))
		case ignoreLoadThresholdAndHigher:
			ml.logf(ctx, 3, "using level %v (threshold:%v) for r%d based on overload duration %v",
				ignoreLevel, ssSLS.sls, rangeID, re.now.Sub(ss.overloadStartTime))
		}
		targetStoreID := sortTargetCandidateSetAndPick(
			ctx, cands, ssSLS.sls, ignoreLevel, loadDim, re.rng,
			re.fractionPendingIncreaseOrDecreaseThreshold, re.passObs.replicaShed, ml)
		if targetStoreID == 0 {
			ml.logf(ctx, 3, "result(failed): no suitable target found among candidates for r%d "+
				"(threshold %s; %s)", rangeID, ssSLS.sls, ignoreLevel)
			continue
		}
		targetSS := re.stores[targetStoreID]
		addedLoad := rstate.load.Load
		if !isLeaseholder {
			addedLoad[CPURate] = rstate.load.RaftCPU
		}
		if !re.canShedAndAddLoad(ctx, ss, targetSS, rangeID, addedLoad, cands.means, false, loadDim, ml) {
			re.passObs.replicaShed(ctx, noCandidateToAcceptLoad)
			continue
		}
		addTarget := roachpb.ReplicationTarget{
			NodeID:  targetSS.NodeID,
			StoreID: targetSS.StoreID,
		}
		removeTarget := roachpb.ReplicationTarget{
			NodeID:  ss.NodeID,
			StoreID: ss.StoreID,
		}
		if addTarget.StoreID == removeTarget.StoreID {
			panic(fmt.Sprintf("internal state inconsistency: "+
				"add=%v==remove_target=%v range_id=%v candidates=%v",
				addTarget, removeTarget, rangeID, cands.candidates))
		}
		replicaChanges := makeRebalanceReplicaChanges(ctx,
			rangeID, rstate.replicas, rstate.load, addTarget, removeTarget)
		rangeChange := MakePendingRangeChange(rangeID, replicaChanges[:])
		if err = re.preCheckOnApplyReplicaChanges(rangeChange); err != nil {
			panic(errors.Wrapf(err, "pre-check failed for replica changes: %v for %v",
				replicaChanges, rangeID))
		}
		re.addPendingRangeChange(ctx, rangeChange)
		re.changes = append(re.changes,
			MakeExternalRangeChange(originMMARebalance, localStoreID, rangeChange))
		re.rangeMoveCount++
		re.passObs.replicaShed(ctx, shedSuccess)
		log.KvDistribution.Infof(ctx,
			"result(success): rebalancing r%v from s%v to s%v [change: %v] with resulting loads source: %v target: %v",
			rangeID, removeTarget.StoreID, addTarget.StoreID, &re.changes[len(re.changes)-1], ss.adjusted.load, targetSS.adjusted.load)
	}
}

// rebalanceLeasesFromLocalStoreID attempts to move leases away from the
// provided store, which must be the local store which called into
// rebalanceStores, and must be overloaded on CPU.
//
// Transfers are attempted until we run out of leases to try, hit the max lease
// transfer count limit, or the maximum fractional pending decrease threshold.
//
// Returns the number of lease transfers made.
func (re *rebalanceEnv) rebalanceLeasesFromLocalStoreID(
	ctx context.Context,
	ss *storeState,
	store sheddingStore,
	localStoreID roachpb.StoreID,
	ml mmaLogger,
) int /* leaseTransferCount */ {
	ml.logf(ctx, 2, "local store s%d is CPU overloaded (%v >= %v), attempting lease transfers first",
		store.StoreID, store.dimSummary[CPURate], overloadSlow)
	// This store is local, and cpu overloaded. Shed leases first.
	topKRanges := ss.adjusted.topKRanges[localStoreID]
	var leaseTransferCount int
	n := topKRanges.len()
	pLeasePrefs := storeAndLeasePreferencePool.get()
	pCandsPL := storeIDPool.get()
	pCandInfos := candidateInfoPool.get()
	scratchNodes := nodeLoadMapPool.get()
	scratchStores := storeIDStructMapPool.get()
	defer func() {
		storeAndLeasePreferencePool.put(pLeasePrefs)
		storeIDPool.put(pCandsPL)
		candidateInfoPool.put(pCandInfos)
		nodeLoadMapPool.put(scratchNodes)
		storeIDStructMapPool.put(scratchStores)
	}()
	for i := 0; i < n; i++ {
		if leaseTransferCount >= re.maxLeaseTransferCount {
			ml.logf(ctx, 2, "reached max lease transfer count %d, returning", re.maxLeaseTransferCount)
			return leaseTransferCount
		}
		if ss.maxFractionPendingDecrease >= re.fractionPendingIncreaseOrDecreaseThreshold {
			ml.logf(ctx, 2, "s%d has reached pending decrease threshold(%.2f>=%."+
				"2f) after %d lease transfers",
				store.StoreID, ss.maxFractionPendingDecrease, re.fractionPendingIncreaseOrDecreaseThreshold, leaseTransferCount)
			return leaseTransferCount
		}

		rangeID := topKRanges.index(i)
		rstate := re.ranges[rangeID]
		if len(rstate.pendingChanges) > 0 {
			// If the range has pending changes, don't make more changes.
			ml.logf(ctx, 3, "skipping r%d: has pending changes", rangeID)
			re.passObs.leaseShed(ctx, rangeTransient)
			continue
		}
		foundLocalReplica := false
		for _, repl := range rstate.replicas {
			if repl.StoreID != localStoreID { // NB: localStoreID == ss.StoreID == store.StoreID
				continue
			}
			if !repl.IsLeaseholder {
				// We are iterating ss.adjusted.topKRanges[localStoreID], which is
				// rebuilt only on a StoreLeaseholderMsg from localStoreID (see the
				// topKRanges invariant in cluster_state.go). If the local store
				// transferred its lease away after that msg and the change has
				// since enacted, topK still lists r but rs.replicas no longer
				// marks the local store as leaseholder. The skip-on-pending check
				// above only masks the in-progress window, not the post-enact
				// window. A follow-up commit converts this assertion into a
				// skip+log so a benign cache race no longer fatals the node.
				panic(errors.AssertionFailedf(
					"internal state inconsistency: r%d considered for lease shedding "+
						"on s%d has no pending changes but s%d is not leaseholder: replicas=%v",
					rangeID, localStoreID, localStoreID, rstate.replicas))
			}
			foundLocalReplica = true
			break
		}
		if !foundLocalReplica {
			// Same staleness source as above, with a removal pending instead of a
			// lease transfer: topK still lists r for localStoreID, but the local
			// store is no longer in rs.replicas because the removal pending
			// change has enacted. Converted to a skip+log in a follow-up commit.
			panic(errors.AssertionFailedf(
				"internal state inconsistency: r%d considered for lease shedding "+
					"on s%d but s%d is not in replicas=%v",
				rangeID, localStoreID, localStoreID, rstate.replicas))
		}
		if re.now.Sub(rstate.lastFailedChange) < re.lastFailedChangeDelayDuration {
			ml.logf(ctx, 3, "skipping r%d: too soon after failed change", rangeID)
			re.passObs.leaseShed(ctx, rangeTransient)
			continue
		}
		re.ensureAnalyzedConstraints(ctx, rstate)
		if rstate.constraints == nil {
			if rangeSkippedDueToFailedConstraintsShouldLog() {
				log.KvDistribution.Warningf(ctx,
					"skipping r%d: no constraints analyzed (conf=%v replicas=%v)",
					rangeID, rstate.conf, rstate.replicas)
			}
			re.passObs.leaseShed(ctx, rangeConstraintsError)
			continue
		}
		if rstate.constraints.leaseholderID != store.StoreID {
			// rs.constraints is a cached blob (see clearAnalyzedConstraints in
			// cluster_state.go); leaseholderID reflects rs.replicas at cache
			// time. If a prior pass cached the blob while the previous
			// leaseholder held the lease and a subsequent external change moved
			// the lease here without invalidating, the cached leaseholderID
			// disagrees with the now-correct local store. This is the #170112
			// panic. A follow-up commit invalidates rs.constraints in
			// addPendingRangeChange to close that gap; another follow-up converts
			// this assertion into a skip+log as defense-in-depth for any future
			// path that mutates rs.replicas without invalidating.
			panic(errors.AssertionFailedf("internal state inconsistency: "+
				"store=%v range_id=%v should be leaseholder but isn't",
				store.StoreID, rangeID))
		}

		// Get the stores from the replica set that are at least as good as the
		// current leaseholder wrt satisfaction of lease preferences. This means
		// that mma will never make lease preferences violations worse when
		// moving the lease.
		//
		// Example:
		// s1 and s2 in us-east, s3 in us-central, lease preference for us-east.
		// - if s3 has the lease: candsPL = [s1, s2, s3]
		// - if s1 has the lease: candsPL = [s2, s1] (s3 filtered out)
		// - if s2 has the lease: candsPL = [s1, s2] (s3 filtered out)
		//
		// In effect, we interpret each replica whose store is worse than the current
		// leaseholder as ill-disposed for the lease and (pre-means) filter then out.
		cands, _ := rstate.constraints.candidatesToMoveLease(*pLeasePrefs)
		*pLeasePrefs = cands
		// candsPL is the set of stores to consider the mean. This should
		// include the current leaseholder, so we add it in, but only in a
		// little while.
		candsPL := storeSet((*pCandsPL)[:0])
		for _, cand := range cands {
			candsPL.insert(cand.storeID)
		}
		if len(candsPL) == 0 {
			// No candidates to move the lease to. We bail early to avoid some
			// logging below that is not helpful if we didn't have any real
			// candidates to begin with.
			re.passObs.leaseShed(ctx, noCandidate)
			continue
		}
		// NB: intentionally log before re-adding the current leaseholder so
		// we don't list it as a candidate.
		// TODO(tbg): allocates 207x/op (logging and candidate building).
		if ml.V(ctx, 3) {
			ml.logf(ctx, 3, "considering lease-transfer r%v from s%v: candidates are %v", rangeID, store.StoreID, candsPL)
		}
		// Now candsPL is ready for computing the means.
		candsPL.insert(store.StoreID)

		// Filter by disposition. Note that we pass the shedding store in to
		// make sure that its disposition does not matter. In other words, the
		// leaseholder is always going to include itself in the mean, even if it
		// is ill-disposed towards leases.
		candsPL = retainReadyLeaseTargetStoresOnly(ctx, candsPL, re.stores, rangeID, store.StoreID, ml)
		// Save back to pool slice so capacity is preserved across iterations.
		*pCandsPL = []roachpb.StoreID(candsPL)

		// INVARIANT: candsPL - {store.StoreID} \subset cands
		if len(candsPL) == 0 || (len(candsPL) == 1 && candsPL[0] == store.StoreID) {
			re.passObs.leaseShed(ctx, noHealthyCandidate)
			ml.logf(ctx, 3,
				"result(failed): no candidates to move lease from n%vs%v for r%v after retainReadyLeaseTargetStoresOnly",
				ss.NodeID, ss.StoreID, rangeID)
			continue
		}
		// INVARIANT: candsPL has at least one candidate other than store.StoreID,
		// which is also in cands.

		// NB: candsPL is not empty - it includes at least the current leaseholder
		// and one additional candidate.
		means, ok := computeMeansForStoreSet(re, candsPL, scratchNodes, scratchStores)
		if !ok {
			// Unreachable: candsPL is non-empty. Gate the assert so variadic
			// arg boxing doesn't allocate on the success path (this site is in
			// a hot inner loop).
			assertTruef(ctx, false, "computeMeansForStoreSet returned !ok for non-empty candsPL=%v", candsPL)
		}
		sls := re.computeLoadSummary(ctx, store.StoreID, &means.storeLoad, &means.nodeLoad, ml)
		if sls.dimSummary[CPURate] < overloadSlow {
			// This store is not cpu overloaded relative to these candidates for
			// this range.
			ml.logf(ctx, 3, "result(failed): skipping r%d since store not overloaded relative to candidates", rangeID)
			re.passObs.leaseShed(ctx, notOverloaded)
			continue
		}
		candsSet := candidateSet{candidates: (*pCandInfos)[:0]}
		for _, cand := range cands {
			if cand.storeID == store.StoreID {
				panic(errors.AssertionFailedf("current leaseholder can't be a candidate: %v", cand))
			}
			if !candsPL.contains(cand.storeID) {
				// Skip candidates that are filtered out by
				// retainReadyLeaseTargetStoresOnly.
				continue
			}
			candSls := re.computeLoadSummary(ctx, cand.storeID, &means.storeLoad, &means.nodeLoad, ml)
			candsSet.candidates = append(candsSet.candidates, candidateInfo{
				StoreID:              cand.storeID,
				storeLoadSummary:     candSls,
				diversityScore:       0,
				leasePreferenceIndex: cand.leasePreferenceIndex,
			})
		}
		// Save back to pool slice so capacity is preserved across iterations.
		*pCandInfos = candsSet.candidates
		// Have candidates. We set ignoreLevel to
		// ignoreHigherThanLoadThreshold since this is the only allocator that
		// can shed leases for this store, and lease shedding is cheap, and it
		// will only add CPU to the target store (so it is ok to ignore other
		// dimensions on the target).
		targetStoreID := sortTargetCandidateSetAndPick(
			ctx, candsSet, sls.sls, ignoreHigherThanLoadThreshold, CPURate, re.rng,
			re.fractionPendingIncreaseOrDecreaseThreshold, re.passObs.leaseShed, ml)
		if targetStoreID == 0 {
			ml.logf(ctx, 3,
				"result(failed): no candidates to move lease from n%vs%v for r%v after sortTargetCandidateSetAndPick",
				ss.NodeID, ss.StoreID, rangeID)
			continue
		}
		targetSS := re.stores[targetStoreID]
		var addedLoad LoadVector
		// Only adding leaseholder CPU.
		addedLoad[CPURate] = rstate.load.Load[CPURate] - rstate.load.RaftCPU
		if addedLoad[CPURate] < 0 {
			// TODO(sumeer): remove this panic once we are not in an
			// experimental phase.
			addedLoad[CPURate] = 0
			panic("raft cpu higher than total cpu")
		}
		if !re.canShedAndAddLoad(ctx, ss, targetSS, rangeID, addedLoad, &means, true, CPURate, ml) {
			re.passObs.leaseShed(ctx, noCandidateToAcceptLoad)
			continue
		}
		addTarget := roachpb.ReplicationTarget{
			NodeID:  targetSS.NodeID,
			StoreID: targetSS.StoreID,
		}
		removeTarget := roachpb.ReplicationTarget{
			NodeID:  ss.NodeID,
			StoreID: ss.StoreID,
		}
		replicaChanges := MakeLeaseTransferChanges(
			rangeID, rstate.replicas, rstate.load, addTarget, removeTarget)
		leaseChange := MakePendingRangeChange(rangeID, replicaChanges[:])
		if err := re.preCheckOnApplyReplicaChanges(leaseChange); err != nil {
			panic(errors.Wrapf(err, "pre-check failed for lease transfer %v", leaseChange))
		}
		re.addPendingRangeChange(ctx, leaseChange)
		re.changes = append(re.changes,
			MakeExternalRangeChange(originMMARebalance, store.StoreID, leaseChange))
		re.passObs.leaseShed(ctx, shedSuccess)
		log.KvDistribution.Infof(ctx,
			"result(success): shedding r%v lease from s%v to s%v [change:%v] with "+
				"resulting loads source:%v target:%v (means: %v) (frac_pending: (src:%.2f,target:%.2f) (src:%.2f,target:%.2f))",
			rangeID, removeTarget.StoreID, addTarget.StoreID, &re.changes[len(re.changes)-1],
			ss.adjusted.load, targetSS.adjusted.load, means.storeLoad.load,
			ss.maxFractionPendingIncrease, ss.maxFractionPendingDecrease,
			targetSS.maxFractionPendingIncrease, targetSS.maxFractionPendingDecrease)
		leaseTransferCount++
	}
	// We iterated through all top-K ranges without running into any limits.
	return leaseTransferCount
}

// retainReadyLeaseTargetStoresOnly filters the input set to only those stores that
// are ready to accept a lease for the given range. A store is not ready if it
// is not healthy, or does not accept leases at either the store or replica level.
//
// The input storeSet is mutated (and used to for the returned result).
func retainReadyLeaseTargetStoresOnly(
	ctx context.Context,
	in storeSet,
	stores map[roachpb.StoreID]*storeState,
	rangeID roachpb.RangeID,
	existingLeaseholder roachpb.StoreID,
	ml mmaLogger,
) storeSet {
	out := in[:0]
	for _, storeID := range in {
		if storeID == existingLeaseholder {
			// The existing leaseholder is always included in the mean, even if
			// it is ill-disposed towards leases. Because it is holding the lease,
			// we know that its load is recent.
			//
			// Example: Consider a range with leaseholder on s1 and voters on s2
			// and s3. All stores have CPU capacity of 100 units. s1 has load 40,
			// s2 has load 80, s3 has load 80. The mean CPU utilization (total
			// load / total capacity) is (40+80+80)/(100+100+100) = 66% if we
			// include s1 and (80+80)/(100+100) = 80% if we don't.
			// If we filtered out s1 just because it is ill-disposed towards
			// leases, s2 and s3 would be exactly on the mean and we might
			// consider transferring the lease to them, but we should not.
			out = append(out, storeID)
			continue
		}
		s := stores[storeID].status
		switch {
		case s.Disposition.Lease != LeaseDispositionOK:
			ml.logf(ctx, 3, "skipping s%d for lease transfer: lease disposition %v (health %v)", storeID, s.Disposition.Lease, s.Health)
		case stores[storeID].adjusted.replicas[rangeID].LeaseDisposition != LeaseDispositionOK:
			ml.logf(ctx, 3, "skipping s%d for lease transfer: replica lease disposition %v (health %v)", storeID, stores[storeID].adjusted.replicas[rangeID].LeaseDisposition, s.Health)
		default:
			out = append(out, storeID)
		}
	}
	return out
}
