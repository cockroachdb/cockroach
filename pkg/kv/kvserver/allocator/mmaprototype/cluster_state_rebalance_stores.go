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
	"strings"
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
	// Scratch variables reused across iterations.
	// TODO(tbg): these are a potential source of errors (imagine two nested
	// calls using the same scratch variable). Just make a global variable
	// that wraps a bunch of sync.Pools for the types we need.
	scratch struct {
		postMeansExclusions storeSet
		nodes               map[roachpb.NodeID]*NodeLoad
		stores              map[roachpb.StoreID]struct{}
	}
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
	re.scratch.nodes = map[roachpb.NodeID]*NodeLoad{}
	re.scratch.stores = map[roachpb.StoreID]struct{}{}
	return re
}

type sheddingStore struct {
	roachpb.StoreID
	// storeLoadSummary is relative to the entire cluster (not a set of valid
	// replacement stores for some particular replica), see the comment where
	// sheddingStores are constructed.
	storeLoadSummary
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

	log.KvDistribution.VEventf(ctx, 2, "rebalanceStores begins")
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
	clusterMeans := re.meansMemo.getMeans(nil)
	var sheddingStores []sheddingStore
	log.KvDistribution.Infof(ctx,
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
		log.KvDistribution.VEventf(ctx, 2, "evaluating s%d: node load %s, store load %s, worst dim %s",
			storeID, sls.nls, sls.sls, sls.worstDim)

		if sls.sls >= overloadSlow {
			if ss.overloadEndTime != (time.Time{}) {
				if re.now.Sub(ss.overloadEndTime) > overloadGracePeriod {
					ss.overloadStartTime = re.now
					log.KvDistribution.Infof(ctx, "overload-start s%v (%v) - grace period expired", storeID, sls)
				} else {
					// Else, extend the previous overload interval.
					log.KvDistribution.Infof(ctx, "overload-continued s%v (%v) - within grace period", storeID, sls)
				}
				ss.overloadEndTime = time.Time{}
			}
			// The pending decrease must be small enough to continue shedding
			if ss.maxFractionPendingDecrease < re.fractionPendingIncreaseOrDecreaseThreshold &&
				// There should be no pending increase, since that can be an overestimate.
				ss.maxFractionPendingIncrease < epsilon {
				log.KvDistribution.VEventf(ctx, 2, "store s%v was added to shedding store list", storeID)
				sheddingStores = append(sheddingStores, sheddingStore{StoreID: storeID, storeLoadSummary: sls})
			} else {
				log.KvDistribution.VEventf(ctx, 2,
					"skipping overloaded store s%d (worst dim: %s): pending decrease %.2f >= threshold %.2f or pending increase %.2f >= epsilon",
					storeID, sls.worstDim, ss.maxFractionPendingDecrease, re.fractionPendingIncreaseOrDecreaseThreshold, ss.maxFractionPendingIncrease)
			}
		} else if sls.sls < loadNoChange && ss.overloadEndTime == (time.Time{}) {
			// NB: we don't stop the overloaded interval if the store is at
			// loadNoChange, since a store can hover at the border of the two.
			log.KvDistribution.Infof(ctx, "overload-end s%v (%v) - load dropped below no-change threshold", storeID, sls)
			ss.overloadEndTime = re.now
		}
	}

	// We have used storeLoadSummary.sls to filter above. But when sorting, we
	// first compare using nls. Consider the following scenario with 4 stores
	// per node: node n1 is at 90% cpu utilization and has 4 stores each
	// contributing 22.5% cpu utilization. Node n2 is at 60% utilization, and
	// has 1 store contributing all 60%. When looking at rebalancing, we want to
	// first shed load from the stores of n1, before we shed load from the store
	// on n2.
	slices.SortFunc(sheddingStores, func(a, b sheddingStore) int {
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

	if log.V(2) {
		log.KvDistribution.Info(ctx, "sorted shedding stores:")
		for _, store := range sheddingStores {
			log.KvDistribution.Infof(ctx, "  (s%d: %s)", store.StoreID, store.sls)
		}
	}

	i := 0
	n := len(sheddingStores)
	for ; i < n; i++ {
		store := sheddingStores[i]
		// NB: we don't have to check the maxLeaseTransferCount here since only one
		// store can transfer leases - the local store. So the limit is only checked
		// inside of the corresponding rebalanceLeasesFromLocalStoreID call, but
		// not in this outer loop.
		if re.rangeMoveCount >= re.maxRangeMoveCount {
			log.KvDistribution.VEventf(ctx, 2, "reached max range move count %d, stopping further rebalancing",
				re.maxRangeMoveCount)
			break
		}
		re.rebalanceStore(ctx, store, localStoreID)
	}
	for ; i < n; i++ {
		re.passObs.skippedStore(sheddingStores[i].StoreID)
	}
	re.passObs.finishRebalancingPass(ctx)
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
//
// TODO(tbg): The above describes the intended design. Lease transfers follow this
// pattern (see retainReadyLeaseTargetStoresOnly). Replica transfers are still
// being cleaned up: they currently compute means over all constraint-matching
// stores and filter only afterward, which is not intentional.
// Tracking issue: https://github.com/cockroachdb/cockroach/pull/158373
func (re *rebalanceEnv) rebalanceStore(
	ctx context.Context, store sheddingStore, localStoreID roachpb.StoreID,
) {
	log.KvDistribution.Infof(ctx, "start processing shedding store s%d: cpu node load %s, store load %s, worst dim %s",
		store.StoreID, store.nls, store.sls, store.worstDim)
	ss := re.stores[store.StoreID]

	topKRanges := ss.adjusted.topKRanges[localStoreID]
	n := topKRanges.len()
	if true {
		// Debug logging.
		if n > 0 {
			var b strings.Builder
			for i := 0; i < n; i++ {
				rangeID := topKRanges.index(i)
				rstate := re.ranges[rangeID]
				load := rstate.load.Load
				if !ss.adjusted.replicas[rangeID].IsLeaseholder {
					load[CPURate] = rstate.load.RaftCPU
				}
				_, _ = fmt.Fprintf(&b, " r%d:%v", rangeID, load)
			}
			log.KvDistribution.Infof(ctx, "top-K[%s] ranges for s%d with lease on local s%d:%s",
				topKRanges.dim, store.StoreID, localStoreID, redact.SafeString(b.String()))
		} else {
			log.KvDistribution.Infof(ctx, "no top-K[%s] ranges found for s%d with lease on local s%d",
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
	// grace duration thresholds.
	iLevel := ignoreLoadNoChangeAndHigher
	overloadDur := re.now.Sub(ss.overloadStartTime)
	if overloadDur > ignoreHigherThanLoadThresholdGraceDuration {
		iLevel = ignoreHigherThanLoadThreshold
	} else if overloadDur > ignoreLoadThresholdAndHigherGraceDuration {
		iLevel = ignoreLoadThresholdAndHigher
	}
	// Will only become true for remote stores.
	withinLeaseSheddingGracePeriod := false
	if store.StoreID != localStoreID && store.dimSummary[CPURate] >= overloadSlow &&
		re.now.Sub(ss.overloadStartTime) < remoteStoreLeaseSheddingGraceDuration {
		withinLeaseSheddingGracePeriod = true
	}
	re.passObs.storeOverloaded(ss.StoreID, withinLeaseSheddingGracePeriod, iLevel)
	defer func() {
		re.passObs.finishStore()
	}()
	if withinLeaseSheddingGracePeriod {
		log.KvDistribution.VEventf(ctx, 2, "skipping remote store s%d: in lease shedding grace period", store.StoreID)
		return
	}

	if ss.StoreID == localStoreID {
		if store.dimSummary[CPURate] >= overloadSlow {
			// The store which called into rebalanceStore is overloaded. Try to shed
			// load from it via lease transfers first. Note that if we have multiple
			// stores, this rebalanceStore invocation is on behalf of exactly one of
			// them, and that's the one we're going to shed from here - other stores
			// will do it when they call into rebalanceStore.
			if numTransferred := re.rebalanceLeasesFromLocalStoreID(ctx, ss, store, localStoreID); numTransferred > 0 {
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
				log.KvDistribution.VEventf(ctx, 2, "skipping replica transfers for s%d to try more leases next time",
					ss.StoreID)
				return
			}
		} else {
			log.KvDistribution.VEventf(ctx, 2, "skipping lease shedding for calling store s%s: not cpu overloaded: %v",
				localStoreID, store.dimSummary[CPURate])
		}
	}

	log.KvDistribution.VEventf(ctx, 2, "attempting to shed replicas next")
	re.rebalanceReplicas(ctx, store, ss, localStoreID, iLevel)
}

func (re *rebalanceEnv) rebalanceReplicas(
	ctx context.Context,
	store sheddingStore,
	ss *storeState,
	localStoreID roachpb.StoreID,
	ignoreLevel ignoreLevel,
) {
	if store.StoreID != localStoreID && store.dimSummary[CPURate] >= overloadSlow &&
		re.now.Sub(ss.overloadStartTime) < remoteStoreLeaseSheddingGraceDuration {
		log.KvDistribution.VEventf(ctx, 2, "skipping remote store s%d: in lease shedding grace period", store.StoreID)
		return
	}
	// Iterate over top-K ranges first and try to move them.
	topKRanges := ss.adjusted.topKRanges[localStoreID]
	n := topKRanges.len()
	loadDim := topKRanges.dim
	for i := 0; i < n; i++ {
		if re.rangeMoveCount >= re.maxRangeMoveCount {
			log.KvDistribution.VEventf(ctx, 2,
				"reached max range move count %d; done shedding", re.maxRangeMoveCount)
			return
		}
		if ss.maxFractionPendingDecrease >= re.fractionPendingIncreaseOrDecreaseThreshold {
			log.KvDistribution.VEventf(ctx, 2,
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
			log.KvDistribution.VEventf(ctx, 2, "skipping r%d: has pending changes", rangeID)
			continue
		}
		if re.now.Sub(rstate.lastFailedChange) < re.lastFailedChangeDelayDuration {
			log.KvDistribution.VEventf(ctx, 2, "skipping r%d: too soon after failed change", rangeID)
			continue
		}
		re.ensureAnalyzedConstraints(rstate)
		if rstate.constraints == nil {
			if rangeSkippedDueToFailedConstraintsShouldLog() {
				log.KvDistribution.Warningf(ctx,
					"skipping r%d: no constraints analyzed (conf=%v replicas=%v)",
					rangeID, rstate.conf, rstate.replicas)
			}
			continue
		}
		isVoter, isNonVoter := rstate.constraints.replicaRole(store.StoreID)
		if !isVoter && !isNonVoter {
			// Due to REQUIREMENT(change-computation), the top-k is up to date, so
			// this must never happen.
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
			re.passObs.replicaShed(rangeConstraintsViolated)
			log.KvDistribution.VEventf(ctx, 2, "skipping r%d: constraint violation needs fixing first: %v", rangeID, err)
			continue
		}
		// Build post-means exclusions: stores whose load is included in the mean
		// (they're viable locations in principle) but aren't valid targets for
		// this specific transfer.
		//
		// NB: to prevent placing replicas on multiple CRDB nodes sharing a
		// host, we'd need to make changes here.
		// See: https://github.com/cockroachdb/cockroach/issues/153863
		re.scratch.postMeansExclusions = re.scratch.postMeansExclusions[:0]
		existingReplicas := storeSet{} // TODO(tbg): avoid allocation
		for _, replica := range rstate.replicas {
			storeID := replica.StoreID
			existingReplicas.insert(storeID)
			if storeID == store.StoreID {
				// Exclude the shedding store (we're moving away from it), but not
				// other stores on its node (within-node rebalance is allowed).
				re.scratch.postMeansExclusions.insert(storeID)
				continue
			}
			// Exclude all stores on nodes with other existing replicas.
			nodeID := re.stores[storeID].NodeID
			for _, storeID := range re.nodes[nodeID].stores {
				re.scratch.postMeansExclusions.insert(storeID)
			}
		}

		// Compute the candidates. These are already filtered down to only those stores
		// that we'll actually be happy to transfer the range to.
		// Note that existingReplicas is a subset of postMeansExclusions, so they'll
		// be included in the mean, but are never considered as candidates.
		//
		// TODO(sumeer): eliminate cands allocations by passing a scratch slice.
		cands, ssSLS := re.computeCandidatesForReplicaTransfer(ctx, conj, existingReplicas, re.scratch.postMeansExclusions, store.StoreID, re.passObs)
		log.KvDistribution.VEventf(ctx, 2, "considering replica-transfer r%v from s%v: store load %v",
			rangeID, store.StoreID, ss.adjusted.load)
		log.KvDistribution.VEventf(ctx, 3, "candidates are:")
		for _, c := range cands.candidates {
			log.KvDistribution.VEventf(ctx, 3, " s%d: %s", c.StoreID, c.storeLoadSummary)
		}

		if len(cands.candidates) == 0 {
			re.passObs.replicaShed(noCandidate)
			log.KvDistribution.VEventf(ctx, 2, "result(failed): no candidates found for r%d after exclusions", rangeID)
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
			log.KvDistribution.VEventf(ctx, 3, "using level %v (threshold:%v) for r%d based on overload duration %v",
				ignoreLevel, ssSLS.sls, rangeID, re.now.Sub(ss.overloadStartTime))
		case ignoreLoadThresholdAndHigher:
			log.KvDistribution.VEventf(ctx, 3, "using level %v (threshold:%v) for r%d based on overload duration %v",
				ignoreLevel, ssSLS.sls, rangeID, re.now.Sub(ss.overloadStartTime))
		}
		targetStoreID := sortTargetCandidateSetAndPick(
			ctx, cands, ssSLS.sls, ignoreLevel, loadDim, re.rng,
			re.fractionPendingIncreaseOrDecreaseThreshold, re.passObs.replicaShed)
		if targetStoreID == 0 {
			log.KvDistribution.VEventf(ctx, 2, "result(failed): no suitable target found among candidates for r%d "+
				"(threshold %s; %s)", rangeID, ssSLS.sls, ignoreLevel)
			continue
		}
		targetSS := re.stores[targetStoreID]
		addedLoad := rstate.load.Load
		if !isLeaseholder {
			addedLoad[CPURate] = rstate.load.RaftCPU
		}
		if !re.canShedAndAddLoad(ctx, ss, targetSS, addedLoad, cands.means, false, loadDim) {
			log.KvDistribution.VEventf(ctx, 2, "result(failed): cannot shed from s%d to s%d for r%d: delta load %v",
				store.StoreID, targetStoreID, rangeID, addedLoad)
			re.passObs.replicaShed(noCandidateToAcceptLoad)
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
		replicaChanges := makeRebalanceReplicaChanges(
			rangeID, rstate.replicas, rstate.load, addTarget, removeTarget)
		rangeChange := MakePendingRangeChange(rangeID, replicaChanges[:])
		if err = re.preCheckOnApplyReplicaChanges(rangeChange); err != nil {
			panic(errors.Wrapf(err, "pre-check failed for replica changes: %v for %v",
				replicaChanges, rangeID))
		}
		re.addPendingRangeChange(rangeChange)
		re.changes = append(re.changes,
			MakeExternalRangeChange(originMMARebalance, localStoreID, rangeChange))
		re.rangeMoveCount++
		re.passObs.replicaShed(shedSuccess)
		log.KvDistribution.VEventf(ctx, 2,
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
	ctx context.Context, ss *storeState, store sheddingStore, localStoreID roachpb.StoreID,
) int /* leaseTransferCount */ {
	log.KvDistribution.VEventf(ctx, 2, "local store s%d is CPU overloaded (%v >= %v), attempting lease transfers first",
		store.StoreID, store.dimSummary[CPURate], overloadSlow)
	// This store is local, and cpu overloaded. Shed leases first.
	topKRanges := ss.adjusted.topKRanges[localStoreID]
	var leaseTransferCount int
	n := topKRanges.len()
	for i := 0; i < n; i++ {
		if leaseTransferCount >= re.maxLeaseTransferCount {
			log.KvDistribution.VEventf(ctx, 2, "reached max lease transfer count %d, returning", re.maxLeaseTransferCount)
			return leaseTransferCount
		}
		if ss.maxFractionPendingDecrease >= re.fractionPendingIncreaseOrDecreaseThreshold {
			log.KvDistribution.VEventf(ctx, 2, "s%d has reached pending decrease threshold(%.2f>=%."+
				"2f) after %d lease transfers",
				store.StoreID, ss.maxFractionPendingDecrease, re.fractionPendingIncreaseOrDecreaseThreshold, leaseTransferCount)
			return leaseTransferCount
		}

		rangeID := topKRanges.index(i)
		rstate := re.ranges[rangeID]
		if len(rstate.pendingChanges) > 0 {
			// If the range has pending changes, don't make more changes.
			log.KvDistribution.VEventf(ctx, 2, "skipping r%d: has pending changes", rangeID)
			continue
		}
		foundLocalReplica := false
		for _, repl := range rstate.replicas {
			if repl.StoreID != localStoreID { // NB: localStoreID == ss.StoreID == store.StoreID
				continue
			}
			if !repl.IsLeaseholder {
				// NB: due to REQUIREMENT(change-computation), the top-k
				// ranges for ss reflect the latest adjusted state, including
				// pending changes. Thus, this store must be a replica and the
				// leaseholder, hence this assertion, and other assertions
				// below. Additionally, the code above ignored the range if it
				// has pending changes, which while not necessary for the
				// is-leaseholder assertion, makes the case where we assert
				// even narrower.
				log.KvDistribution.Fatalf(ctx,
					"internal state inconsistency: replica considered for lease shedding has no pending"+
						" changes but is not leaseholder: %+v", rstate)
			}
			foundLocalReplica = true
			break
		}
		if !foundLocalReplica {
			log.KvDistribution.Fatalf(
				ctx, "internal state inconsistency: local store is not a replica: %+v", rstate)
		}
		if re.now.Sub(rstate.lastFailedChange) < re.lastFailedChangeDelayDuration {
			log.KvDistribution.VEventf(ctx, 2, "skipping r%d: too soon after failed change", rangeID)
			continue
		}
		re.ensureAnalyzedConstraints(rstate)
		if rstate.constraints == nil {
			if rangeSkippedDueToFailedConstraintsShouldLog() {
				log.KvDistribution.Warningf(ctx,
					"skipping r%d: no constraints analyzed (conf=%v replicas=%v)",
					rangeID, rstate.conf, rstate.replicas)
			}
			continue
		}
		if rstate.constraints.leaseholderID != store.StoreID {
			// See the earlier comment about assertions.
			panic(fmt.Sprintf("internal state inconsistency: "+
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
		cands, _ := rstate.constraints.candidatesToMoveLease()
		// candsPL is the set of stores to consider the mean. This should
		// include the current leaseholder, so we add it in, but only in a
		// little while.
		var candsPL storeSet // TODO(tbg): avoid allocation
		for _, cand := range cands {
			candsPL.insert(cand.storeID)
		}
		if len(candsPL) == 0 {
			// No candidates to move the lease to. We bail early to avoid some
			// logging below that is not helpful if we didn't have any real
			// candidates to begin with.
			re.passObs.leaseShed(noCandidate)
			continue
		}
		// NB: intentionally log before re-adding the current leaseholder so
		// we don't list it as a candidate.
		log.KvDistribution.VEventf(ctx, 2, "considering lease-transfer r%v from s%v: candidates are %v", rangeID, store.StoreID, candsPL)
		// Now candsPL is ready for computing the means.
		candsPL.insert(store.StoreID)

		// Filter by disposition. Note that we pass the shedding store in to
		// make sure that its disposition does not matter. In other words, the
		// leaseholder is always going to include itself in the mean, even if it
		// is ill-disposed towards leases.
		candsPL = retainReadyLeaseTargetStoresOnly(ctx, candsPL, re.stores, rangeID, store.StoreID)

		// INVARIANT: candsPL - {store.StoreID} \subset cands
		if len(candsPL) == 0 || (len(candsPL) == 1 && candsPL[0] == store.StoreID) {
			re.passObs.leaseShed(noHealthyCandidate)
			log.KvDistribution.VEventf(ctx, 2,
				"result(failed): no candidates to move lease from n%vs%v for r%v after retainReadyLeaseTargetStoresOnly",
				ss.NodeID, ss.StoreID, rangeID)
			continue
		}
		// INVARIANT: candsPL has at least one candidate other than store.StoreID,
		// which is also in cands.

		clear(re.scratch.nodes)
		// NB: candsPL is not empty - it includes at least the current leaseholder
		// and one additional candidate.
		means := computeMeansForStoreSet(re, candsPL, re.scratch.nodes, re.scratch.stores)
		sls := re.computeLoadSummary(ctx, store.StoreID, &means.storeLoad, &means.nodeLoad)
		if sls.dimSummary[CPURate] < overloadSlow {
			// This store is not cpu overloaded relative to these candidates for
			// this range.
			log.KvDistribution.VEventf(ctx, 2, "result(failed): skipping r%d since store not overloaded relative to candidates", rangeID)
			re.passObs.leaseShed(notOverloaded)
			continue
		}
		var candsSet candidateSet
		for _, cand := range cands {
			if cand.storeID == store.StoreID {
				panic(errors.AssertionFailedf("current leaseholder can't be a candidate: %v", cand))
			}
			if !candsPL.contains(cand.storeID) {
				// Skip candidates that are filtered out by
				// retainReadyLeaseTargetStoresOnly.
				continue
			}
			candSls := re.computeLoadSummary(ctx, cand.storeID, &means.storeLoad, &means.nodeLoad)
			candsSet.candidates = append(candsSet.candidates, candidateInfo{
				StoreID:              cand.storeID,
				storeLoadSummary:     candSls,
				diversityScore:       0,
				leasePreferenceIndex: cand.leasePreferenceIndex,
			})
		}
		// Have candidates. We set ignoreLevel to
		// ignoreHigherThanLoadThreshold since this is the only allocator that
		// can shed leases for this store, and lease shedding is cheap, and it
		// will only add CPU to the target store (so it is ok to ignore other
		// dimensions on the target).
		targetStoreID := sortTargetCandidateSetAndPick(
			ctx, candsSet, sls.sls, ignoreHigherThanLoadThreshold, CPURate, re.rng,
			re.fractionPendingIncreaseOrDecreaseThreshold, re.passObs.leaseShed)
		if targetStoreID == 0 {
			log.KvDistribution.Infof(
				ctx,
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
		if !re.canShedAndAddLoad(ctx, ss, targetSS, addedLoad, &means, true, CPURate) {
			re.passObs.leaseShed(noCandidateToAcceptLoad)
			log.KvDistribution.VEventf(ctx, 2, "result(failed): cannot shed from s%d to s%d for r%d: delta load %v",
				store.StoreID, targetStoreID, rangeID, addedLoad)
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
		re.addPendingRangeChange(leaseChange)
		re.changes = append(re.changes,
			MakeExternalRangeChange(originMMARebalance, store.StoreID, leaseChange))
		re.passObs.leaseShed(shedSuccess)
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
			log.KvDistribution.VEventf(ctx, 2, "skipping s%d for lease transfer: lease disposition %v (health %v)", storeID, s.Disposition.Lease, s.Health)
		case stores[storeID].adjusted.replicas[rangeID].LeaseDisposition != LeaseDispositionOK:
			log.KvDistribution.VEventf(ctx, 2, "skipping s%d for lease transfer: replica lease disposition %v (health %v)", storeID, stores[storeID].adjusted.replicas[rangeID].LeaseDisposition, s.Health)
		default:
			out = append(out, storeID)
		}
	}
	return out
}
