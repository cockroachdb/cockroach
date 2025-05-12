// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mma

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type allocatorState struct {
	cs *clusterState

	// Ranges that are under-replicated, over-replicated, don't satisfy
	// constraints, have low diversity etc. Avoids iterating through all ranges.
	// A range is removed from this map if it has pending changes -- when those
	// pending changes go away it gets added back so we can check on it.
	rangesNeedingAttention map[roachpb.RangeID]struct{}

	diversityScoringMemo *diversityScoringMemo

	changeRateLimiter *storeChangeRateLimiter

	changeIDCounter ChangeID

	rand *rand.Rand
}

var _ Allocator = &allocatorState{}

// TODO(sumeer): temporary constants.
const (
	RebalanceInterval                  time.Duration = 10 * time.Second
	rateChangeLimiterGCInterval                      = 15 * time.Second
	numAllocators                                    = 20
	maxFractionPendingThreshold                      = 0.1
	waitBeforeSheddingRemoteReplicaCPU               = 120 * time.Second
)

func NewAllocatorState(ts timeutil.TimeSource, rand *rand.Rand) *allocatorState {
	interner := newStringInterner()
	cs := newClusterState(ts, interner)
	return &allocatorState{
		cs:                     cs,
		rangesNeedingAttention: map[roachpb.RangeID]struct{}{},
		diversityScoringMemo:   newDiversityScoringMemo(),
		changeRateLimiter:      newStoreChangeRateLimiter(rateChangeLimiterGCInterval),
		rand:                   rand,
	}
}

// TODO(sumeer): lease shedding:
//
// - remote store: don't start moving ranges for a cpu overloaded remote store
//   if it still has leases. Give it the opportunity to shed *all* its leases
//   first, or have its aggregate non-raft cpu fall to a level where it doesn't
//   change the fact that it is cpu overloaded.

// Called periodically, say every 10s.
//
// We do not want to shed replicas for CPU from a remote store until its had a
// chance to shed leases.
func (a *allocatorState) rebalanceStores(
	ctx context.Context, localStoreID roachpb.StoreID,
) []PendingRangeChange {
	now := timeutil.Now()
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
	clusterMeans := a.cs.meansMemo.getMeans(nil)
	a.changeRateLimiter.initForRebalancePass(numAllocators, clusterMeans.storeLoad)
	type sheddingStore struct {
		roachpb.StoreID
		storeLoadSummary
	}
	var sheddingStores []sheddingStore
	log.Infof(ctx, "cluster means (cap): store %s(%s) node-cpu %d(%d)",
		clusterMeans.storeLoad.load, clusterMeans.storeLoad.capacity,
		clusterMeans.nodeLoad.loadCPU, clusterMeans.nodeLoad.capacityCPU)
	// NB: We don't attempt to shed replicas or leases from a store which is
	// fdDrain or fdDead, nor do we attempt to shed replicas from a store which
	// is storeMembershipRemoving (decommissioning). These are currently handled
	// via replicate_queue.go.
	for storeID, ss := range a.cs.stores {
		a.changeRateLimiter.updateForRebalancePass(&ss.adjusted.enactedHistory, now)
		sls := a.cs.meansMemo.getStoreLoadSummary(clusterMeans, storeID, ss.loadSeqNum)
		if sls.sls >= overloadSlow && ss.adjusted.enactedHistory.allowLoadBasedChanges() &&
			ss.maxFractionPending < maxFractionPendingThreshold {
			sheddingStores = append(sheddingStores, sheddingStore{StoreID: storeID, storeLoadSummary: sls})
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
	var changes []PendingRangeChange
	var disj [1]constraintsConj
	var storesToExclude storeIDPostingList
	var storesToExcludeForRange storeIDPostingList
	scratchNodes := map[roachpb.NodeID]*NodeLoad{}
	// The caller has a fixed concurrency limit it can move ranges at, when it
	// is the sender of the snapshot. So we don't want to create too many
	// changes, since then the allocator gets too far ahead of what has been
	// enacted, and its decision-making is no longer based on recent
	// information. We don't have this issue with lease transfers since they are
	// very fast, so we set a much higher limit.
	//
	// TODO: revisit these constants.
	const maxRangeMoveCount = 1
	const maxLeaseTransferCount = 8
	rangeMoveCount := 0
	leaseTransferCount := 0
	for _, store := range sheddingStores {
		log.Infof(ctx, "shedding store %v sls=%v", store.StoreID, store.storeLoadSummary)
		ss := a.cs.stores[store.StoreID]
		// TODO(sumeer): For remote stores that are cpu overloaded, wait for them
		// to shed leases first. See earlier longer to do.
		//
		// When there is CPU overload on the remote store, we should wait out the
		// grace period before moving ranges.
		//
		// TODO: Below is example logic, which doesn't consider when this store
		// became CPU overloaded, only that it hasn't recently shed any leases. We
		// should also consider the last time it became CPU overloaded in addition
		// to it not having shed leases recently.
		//
		// if store.StoreID != localStoreID && store.dimSummary[CPURate] >= overloadSlow &&
		// 	!ss.lastLeaseShedAt.IsZero() &&
		// 	now.Sub(ss.lastLeaseShedAt) < waitBeforeSheddingRemoteReplicaCPU {
		// 	// We wait out the grace period.
		// 	continue
		// }
		doneShedding := false
		if true {
			// Debug logging.
			topKRanges := ss.adjusted.topKRanges[localStoreID]
			n := topKRanges.len()
			if n > 0 {
				var b strings.Builder
				for i := 0; i < n; i++ {
					rangeID := topKRanges.index(i)
					rstate := a.cs.ranges[rangeID]
					load := rstate.load.Load
					if !ss.adjusted.replicas[rangeID].IsLeaseholder {
						load[CPURate] = rstate.load.RaftCPU
					}
					fmt.Fprintf(&b, " r%d: %v(raft %d)", rangeID, load, rstate.load.RaftCPU)
				}
				log.Infof(ctx, "top-K ranges %s: %s", topKRanges.dim, b.String())
			}
		}

		if ss.StoreID == localStoreID && store.dimSummary[CPURate] >= overloadSlow {
			// This store is local, and cpu overloaded. Shed leases first.
			//
			// NB: any ranges at this store that don't have pending changes must
			// have this local store as the leaseholder.
			topKRanges := ss.adjusted.topKRanges[localStoreID]
			n := topKRanges.len()
			for i := 0; i < n; i++ {
				rangeID := topKRanges.index(i)
				rstate := a.cs.ranges[rangeID]
				if len(rstate.pendingChanges) > 0 {
					// If the range has pending changes, don't make more changes.
					continue
				}
				if !a.ensureAnalyzedConstraints(rstate) {
					continue
				}
				if rstate.constraints.leaseholderID != store.StoreID {
					panic(fmt.Sprintf("internal state inconsistency: "+
						"store=%v range_id=%v should be leaseholder but isn't",
						store.StoreID, rangeID))
				}
				cands, _ := rstate.constraints.candidatesToMoveLease()
				var candsPL storeIDPostingList
				for _, cand := range cands {
					candsPL.insert(cand.storeID)
				}
				candsPL.insert(store.StoreID)
				var means meansForStoreSet
				clear(scratchNodes)
				means.stores = candsPL
				computeMeansForStoreSet(a.cs, &means, scratchNodes)
				sls := a.cs.computeLoadSummary(store.StoreID, &means.storeLoad, &means.nodeLoad)
				log.Infof(ctx, "range %v store %v sls=%v means %v store %v", rangeID, store.StoreID,
					sls, means.storeLoad.load, ss.adjusted.load)
				if sls.dimSummary[CPURate] < overloadSlow {
					// This store is not cpu overloaded relative to these candidates for
					// this range.
					continue
				}
				var candsSet candidateSet
				for _, cand := range cands {
					candSls := a.cs.computeLoadSummary(cand.storeID, &means.storeLoad, &means.nodeLoad)
					if sls.fd != fdOK {
						continue
					}
					candsSet.candidates = append(candsSet.candidates, candidateInfo{
						StoreID:              cand.storeID,
						storeLoadSummary:     candSls,
						diversityScore:       0,
						leasePreferenceIndex: cand.leasePreferenceIndex,
					})
				}
				if len(candsSet.candidates) == 0 {
					log.Infof(
						ctx,
						"shedding=n%vs%v no candidates to move lease for r%v [pre_filter_candidates=%v]",
						ss.NodeID, ss.StoreID, rangeID, candsPL)
					continue
				}
				// Have candidates.
				targetStoreID := sortTargetCandidateSetAndPick(
					ctx, candsSet, sls.sls, false, a.rand)
				if targetStoreID == 0 {
					continue
				}
				targetSS := a.cs.stores[targetStoreID]
				var addedLoad LoadVector
				// Only adding leaseholder CPU.
				addedLoad[CPURate] = rstate.load.Load[CPURate] - rstate.load.RaftCPU
				if addedLoad[CPURate] < 0 {
					// TODO(sumeer): remove this panic once we are not in an
					// experimental phase.
					panic("raft cpu higher than total cpu")
					addedLoad[CPURate] = 0
				}
				if !a.cs.canShedAndAddLoad(ctx, ss, targetSS, addedLoad, &means, true) {
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
				leaseChanges := MakeLeaseTransferChanges(
					rangeID, rstate.replicas, rstate.load, addTarget, removeTarget)
				pendingChanges := a.cs.createPendingChanges(leaseChanges[:]...)
				changes = append(changes, PendingRangeChange{
					RangeID:               rangeID,
					pendingReplicaChanges: pendingChanges[:],
				})
				leaseTransferCount++
				if changes[len(changes)-1].IsChangeReplicas() || !changes[len(changes)-1].IsTransferLease() {
					panic(fmt.Sprintf("lease transfer is invalid: %v", changes[len(changes)-1]))
				}
				log.Infof(ctx,
					"shedding=n%vs%v range %v lease from store %v to store %v [%v] with "+
						"resulting loads %v %v (means: %v) (frac_pending: %.2f %.2f)",
					ss.NodeID, store.StoreID, rangeID,
					removeTarget.StoreID, addTarget.StoreID, changes[len(changes)-1],
					ss.adjusted.load, targetSS.adjusted.load, means.storeLoad.load, ss.maxFractionPending, targetSS.maxFractionPending)
				if leaseTransferCount >= maxLeaseTransferCount {
					return changes
				}
				doneShedding = ss.maxFractionPending >= maxFractionPendingThreshold
				if doneShedding {
					break
				}
			}
			if doneShedding || leaseTransferCount > 0 {
				// If managed to transfer a lease, wait for it to be done, before
				// shedding replicas from this store (which is more costly). Otherwise
				// we may needlessly start moving replicas. Note that the store
				// rebalancer will call the rebalance method again after the lease
				// transfer is done and we may still be considering those transfers as
				// pending from a load perspective, so we *may* not be able to do more
				// lease transfers -- so be it.
				continue
			}
		}
		// If the node is cpu overloaded, or the store/node is not fdOK, exclude
		// the other stores on this node from receiving replicas shed by this
		// store.
		excludeStoresOnNode := store.nls > overloadSlow || store.fd != fdOK
		storesToExclude = storesToExclude[:0]
		if excludeStoresOnNode {
			nodeID := ss.NodeID
			for _, storeID := range a.cs.nodes[nodeID].stores {
				storesToExclude.insert(storeID)
			}
		} else {
			// This store is excluded of course.
			storesToExclude.insert(store.StoreID)
		}

		// Iterate over top-K ranges first and try to move them.
		topKRanges := ss.adjusted.topKRanges[localStoreID]
		n := topKRanges.len()
		for i := 0; i < n; i++ {
			rangeID := topKRanges.index(i)
			// TODO(sumeer): the following code belongs in a closure, since we will
			// repeat it for some random selection of non topKRanges.
			rstate := a.cs.ranges[rangeID]
			if len(rstate.pendingChanges) > 0 {
				// If the range has pending changes, don't make more changes.
				continue
			}
			if !a.ensureAnalyzedConstraints(rstate) {
				continue
			}
			isVoter, isNonVoter := rstate.constraints.replicaRole(store.StoreID)
			if !isVoter && !isNonVoter {
				panic(fmt.Sprintf("internal state inconsistency: "+
					"store=%v range_id=%v pending-changes=%v "+
					"rstate_replicas=%v rstate_constraints=%v",
					store.StoreID, rangeID, rstate.pendingChanges, rstate.replicas, rstate.constraints))
			}
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
				continue
			}
			disj[0] = conj
			storesToExcludeForRange = append(storesToExcludeForRange[:0], storesToExclude...)
			// Also exclude all stores on nodes that have existing replicas.
			for _, replica := range rstate.replicas {
				storeID := replica.StoreID
				nodeID := a.cs.stores[storeID].NodeID
				for _, storeID := range a.cs.nodes[nodeID].stores {
					storesToExcludeForRange.insert(storeID)
				}
			}
			// TODO(sumeer): eliminate cands allocations by passing a scratch slice.
			cands, ssSLS := a.computeCandidatesForRange(disj[:], storesToExcludeForRange, store.StoreID)
			if len(cands.candidates) == 0 {
				continue
			}
			var rlocalities replicasLocalityTiers
			if isVoter {
				rlocalities = rstate.constraints.voterLocalityTiers
			} else {
				rlocalities = rstate.constraints.replicaLocalityTiers
			}
			localities := a.diversityScoringMemo.getExistingReplicaLocalities(rlocalities)
			isLeaseholder := rstate.constraints.leaseholderID == store.StoreID
			// Set the diversity score and lease preference index of the candidates.
			for _, cand := range cands.candidates {
				cand.diversityScore = localities.getScoreChangeForRebalance(
					ss.localityTiers, a.cs.stores[cand.StoreID].localityTiers)
				if isLeaseholder {
					cand.leasePreferenceIndex = matchedLeasePreferenceIndex(
						cand.StoreID, rstate.constraints.spanConfig.leasePreferences, a.cs.constraintMatcher)
				}
			}
			// TODO(sumeer): remove this override. Consider a cluster where s1 is
			// overloadSlow, s2 is loadNoChange, and s3, s4 are loadNormal. Now s4 is
			// considering rebalancing load away from s1, but the candidate top-k range
			// has replicas {s1, s3, s4}. So the only way to shed load from s1 is a s1
			// => s2 move. But there may be other ranges at other leaseholder stores
			// which can be moved from s1 => {s3, s4}. So we should not be doing this
			// sub-optimal transfer of load from s1 => s2 unless s1 is not seeing any
			// load shedding for some interval of time. We need a way to capture this
			// information in a simple but effective manner.
			const tempOverrideToIgnoreLoadNoChangeAndHigher = true
			targetStoreID := sortTargetCandidateSetAndPick(
				ctx, cands, ssSLS.sls, tempOverrideToIgnoreLoadNoChangeAndHigher, a.rand)
			if targetStoreID == 0 {
				continue
			}
			targetSS := a.cs.stores[targetStoreID]
			addedLoad := rstate.load.Load
			if !isLeaseholder {
				addedLoad[CPURate] = rstate.load.RaftCPU
			}
			if !a.cs.canShedAndAddLoad(ctx, ss, targetSS, addedLoad, cands.means, false) {
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
			pendingChanges := a.cs.createPendingChanges(replicaChanges[:]...)
			changes = append(changes, PendingRangeChange{
				RangeID:               rangeID,
				pendingReplicaChanges: pendingChanges[:],
			})
			rangeMoveCount++
			log.Infof(ctx,
				"shedding=n%vs%v range %v(%s) from store %v to store %v "+
					"[%v] with resulting loads %v %v",
				ss.NodeID, store.StoreID, rangeID, addedLoad, removeTarget.StoreID,
				addTarget.StoreID, changes[len(changes)-1], ss.adjusted.load,
				targetSS.adjusted.load)
			if rangeMoveCount >= maxRangeMoveCount {
				return changes
			}
			doneShedding = ss.maxFractionPending >= maxFractionPendingThreshold
			if doneShedding {
				break
			}
		}
		// TODO(sumeer): For regular rebalancing, we will wait until those top-K
		// move and then continue with the rest. There is a risk that the top-K
		// have some constraint that prevents rebalancing, while the rest can be
		// moved. Running with underprovisioned clusters and expecting load-based
		// rebalancing to work well is not in scope.
		if doneShedding {
			continue
		}
	}
	return changes
}

// SetStore implements the Allocator interface.
func (a *allocatorState) SetStore(store roachpb.StoreDescriptor) {
	a.cs.setStore(store)
}

// RemoveNodeAndStores implements the Allocator interface.
func (a *allocatorState) RemoveNodeAndStores(nodeID roachpb.NodeID) error {
	panic("unimplemented")
}

// UpdateFailureDetectionSummary implements the Allocator interface.
func (a *allocatorState) UpdateFailureDetectionSummary(
	nodeID roachpb.NodeID, fd failureDetectionSummary,
) error {
	panic("unimplemented")
}

// ProcessStoreLeaseholderMsg implements the Allocator interface.
func (a *allocatorState) ProcessStoreLoadMsg(msg *StoreLoadMsg) {
	a.cs.processStoreLoadMsg(msg)
}

// ProcessStoreLeaseholderMsg implements the Allocator interface.
func (a *allocatorState) ProcessStoreLeaseholderMsg(msg *StoreLeaseholderMsg) {
	a.cs.processStoreLeaseholderMsg(msg)
}

// AdjustPendingChangesDisposition implements the Allocator interface.
func (a *allocatorState) AdjustPendingChangesDisposition(changeIDs []ChangeID, success bool) {
	for _, changeID := range changeIDs {
		if success {
			a.cs.markPendingChangeEnacted(changeID, a.cs.ts.Now())
		} else {
			a.cs.undoPendingChange(changeID)
		}
	}
}

// RegisterExternalChanges implements the Allocator interface.
func (a *allocatorState) RegisterExternalChanges(changes []ReplicaChange) []ChangeID {
	pendingChanges := a.cs.createPendingChanges(changes...)
	changeIDs := make([]ChangeID, len(pendingChanges))
	for i, pendingChange := range pendingChanges {
		changeIDs[i] = pendingChange.ChangeID
	}
	return changeIDs
}

// ComputeChanges implements the Allocator interface.
func (a *allocatorState) ComputeChanges(
	ctx context.Context, opts ChangeOptions,
) []PendingRangeChange {
	return a.rebalanceStores(ctx, opts.LocalStoreID)
}

// AdminRelocateOne implements the Allocator interface.
func (a *allocatorState) AdminRelocateOne(
	desc *roachpb.RangeDescriptor,
	conf *roachpb.SpanConfig,
	leaseholderStore roachpb.StoreID,
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
	transferLeaseToFirstVoter bool,
) ([]pendingReplicaChange, error) {
	panic("unimplemented")
}

// AdminScatterOne implements the Allocator interface.
func (a *allocatorState) AdminScatterOne(
	rangeID roachpb.RangeID, canTransferLease bool, opts ChangeOptions,
) ([]pendingReplicaChange, error) {
	panic("unimplemented")
}

type candidateInfo struct {
	roachpb.StoreID
	storeLoadSummary
	// Higher is better.
	diversityScore float64
	// Lower is better.
	leasePreferenceIndex int32
}

type candidateSet struct {
	candidates []candidateInfo
	means      *meansForStoreSet
}

// In the set of candidates it is possible that some are overloaded, or have
// too many pending changes. It divides them into sets with equal diversity
// score and sorts such that the set with higher diversity score is considered
// before one with lower diversity score. Then it finds the best diversity set
// which has some candidates that are not overloaded wrt disk capacity. Within
// this set it will exclude candidates that are overloaded or have too many
// pending changes, and then pick randomly among the least loaded ones.
//
// Our enum for loadSummary is quite coarse. This has pros and cons: the pro
// is that the coarseness increases the probability that we do eventually find
// something that can handle the multi-dimensional load of this range. The con
// is that we may not select the candidate that is the very best. For instance
// if a new store is added it will also be loadLow like many others, but we
// want more ranges to go to it. One refinement we can try is to make the
// summary more fine-grained, and increment a count of times this store has
// tried to shed this range and failed, and widen the scope of what we pick
// from after some failures.
//
// TODO(sumeer): implement that refinement after some initial
// experimentation and learnings.

// The caller must not exclude any candidates based on load or
// maxFractionPending. That filtering must happen here. Only candidates <
// loadThreshold will be considered.
func sortTargetCandidateSetAndPick(
	ctx context.Context,
	cands candidateSet,
	loadThreshold loadSummary,
	overrideToIgnoreLoadNoChangeAndHigher bool,
	rng *rand.Rand,
) roachpb.StoreID {
	if loadThreshold <= loadNoChange {
		panic("loadThreshold must be > loadNoChange")
	} else {
		log.Infof(ctx, "sortTargetCandidateSetAndPick: loadThreshold=%v", loadThreshold)
	}
	slices.SortFunc(cands.candidates, func(a, b candidateInfo) int {
		if diversityScoresAlmostEqual(a.diversityScore, b.diversityScore) {
			return cmp.Or(cmp.Compare(a.sls, b.sls),
				cmp.Compare(a.leasePreferenceIndex, b.leasePreferenceIndex),
				cmp.Compare(a.StoreID, b.StoreID))
		}
		return -cmp.Compare(a.diversityScore, b.diversityScore)
	})
	bestDiversity := cands.candidates[0].diversityScore
	j := 0
	// Iterate over candidates with the same diversity. First such set that is
	// not disk capacity constrained is where we stop. Even if they can't accept
	// because they have too many pending changes or can't handle the addition
	// of the range. That is, we are not willing to reduce diversity when
	// rebalancing ranges. When rebalancing leases, the diversityScore of all
	// the candidates will be 0.
	for _, cand := range cands.candidates {
		if !diversityScoresAlmostEqual(bestDiversity, cand.diversityScore) {
			if j == 0 {
				// Don't have any candidates yet.
				bestDiversity = cand.diversityScore
			} else {
				// Have a set of candidates.
				break
			}
		}
		// Diversity is the same. Include if not reaching disk capacity.
		if !cand.highDiskSpaceUtilization {
			cands.candidates[j] = cand
			j++
		}
	}
	if j == 0 {
		return 0
	}
	// Every candidate in [0:j] has same diversity and is sorted by increasing
	// load, and within the same load by increasing leasePreferenceIndex.
	cands.candidates = cands.candidates[:j]
	j = 0
	// Filter out the candidates that are overloaded or have too many pending
	// changes.
	//
	// lowestLoad is the load of the set of candidates with the lowest load,
	// among which we will later pick. If the set is found to be empty in the
	// following loop, the lowestLoad is updated.
	lowestLoad := cands.candidates[0].sls
	discardedCandsAtOrBeforeLowestLoadHadPendingChanges := false
	for _, cand := range cands.candidates {
		if cand.sls > lowestLoad {
			if j == 0 {
				// This is the lowestLoad set being considered now.
				lowestLoad = cand.sls
			} else {
				// Past the lowestLoad set. We don't care about these.
				break
			}
		}
		if cand.sls >= loadThreshold || cand.nls >= loadThreshold ||
			cand.maxFractionPending >= maxFractionPendingThreshold {
			// Discard this candidate.
			discardedCandsAtOrBeforeLowestLoadHadPendingChanges =
				discardedCandsAtOrBeforeLowestLoadHadPendingChanges || (cand.maxFractionPending > epsilon)
			log.Infof(ctx,
				"store %v not a candidate sls=%v", cand.StoreID, cand.storeLoadSummary)
			continue
		}
		cands.candidates[j] = cand
		j++
	}
	if j == 0 {
		return 0
	}
	cands.candidates = cands.candidates[:j]
	// The set of candidates we will consider all have lowestLoad.
	//
	// If this set has load >= loadNoChange, we have a set that we would not
	// ordinarily consider as candidates. But we are willing to shed to from
	// overloadUrgent => {overloadSlow, loadNoChange} or overloadSlow =>
	// loadNoChange, when absolutely necessary. This necessity is defined by the
	// fact that we didn't have any candidate in an earlier or this set that was
	// ignored because of pending changes. Because if a candidate was ignored
	// because of pending work, we want to wait for that pending work to finish
	// and then see if we can transfer to those. Note that we used the condition
	// cand.maxFractionPending>epsilon and not
	// cand.maxFractionPending>=maxFractionPendingThreshold when setting
	// discardedCandsAtOrBeforeLowestLoadHadPendingChanges. This is an
	// additional conservative choice, since pending added work is slightly
	// inflated in size, and we want to have a true picture of all of these
	// potential candidates before we start using the ones with load >=
	// loadNoChange.
	if lowestLoad >= loadNoChange &&
		(discardedCandsAtOrBeforeLowestLoadHadPendingChanges || overrideToIgnoreLoadNoChangeAndHigher) {
		return 0
	}
	// Candidates have equal load value and sorted by non-decreasing
	// leasePreferenceIndex. Eliminate ones that have
	// notMatchedLeasePreferenceIndex. Also eliminate ones that are >= loadNoChange,
	// if discardedCandsAtOrBeforeLowestLoadHadPendingChanges is true, or the cand
	// has some pending changes. The idea here is that we will only pick a candidate
	// with load >= loadNoChange once it has no pending changes, or any candidates
	// ordered before it also had
	j = 0
	for _, cand := range cands.candidates {
		if cand.leasePreferenceIndex == notMatchedLeasePreferencIndex {
			break
		}
		j++
	}
	if j == 0 {
		return 0
	}
	cands.candidates = cands.candidates[:j]
	var b strings.Builder
	for i := range cands.candidates {
		fmt.Fprintf(&b, " s%v(%v)", cands.candidates[i].StoreID, cands.candidates[i].sls)
	}
	j = rng.Intn(j)
	log.Infof(ctx, "candidates:%s, picked s%v", b.String(), cands.candidates[j].StoreID)
	if cands.candidates[j].sls >= loadNoChange && overrideToIgnoreLoadNoChangeAndHigher {
		panic("saw higher load candidate than expected")
	}
	return cands.candidates[j].StoreID
}

func (a *allocatorState) ensureAnalyzedConstraints(rstate *rangeState) bool {
	if rstate.constraints != nil {
		return true
	}
	// Populate the constraints.
	rac := newRangeAnalyzedConstraints()
	buf := rac.stateForInit()
	leaseholder := roachpb.StoreID(-1)
	for _, replica := range rstate.replicas {
		buf.tryAddingStore(replica.StoreID, replica.ReplicaIDAndType.ReplicaType.ReplicaType,
			a.cs.stores[replica.StoreID].localityTiers)
		if replica.IsLeaseholder {
			leaseholder = replica.StoreID
		}
	}
	if leaseholder < 0 {
		// Very dubious why the leaseholder (which must be a local store since there
		// are no pending changes) is not known.
		// TODO(sumeer): log an error.
		releaseRangeAnalyzedConstraints(rac)
		return false
	}
	rac.finishInit(rstate.conf, a.cs.constraintMatcher, leaseholder)
	rstate.constraints = rac
	return true
}

// Consider the core logic for a change, rebalancing or recovery.
//
// - There is a constraint expression for the target: constraintDisj
//
// - There are stores to exclude, since may already be playing roles for that
//   range (or have another replica on the same node -- so not all of these
//   stores to exclude will satisfy the constraint expression). For something
//   like candidatesVoterConstraintsUnsatisfied(), the toRemoveVoters should
//   be in the nodes to exclude.
//
// - Optional store that is trying to shed load, since it is possible that
//   when we consider the context of this constraint expression, the store
//   won't be considered overloaded. This node is not specified if shedding
//   because of failure.
//
// Given these, we need to compute:
//
// - Mean load for constraint expression. We can remember this for the whole
//   rebalancing pass since many ranges will have the same constraint
//   expression. Done via meansMemo.
//
// - Set of nodes that satisfy constraint expression. Again, we can remember this.
//   Done via meansMemo.
//
// - loadSummary for all nodes that satisfy the constraint expression. This
//   may change during rebalancing when changes have been made to node. But
//   can be cached and invalidated. Done via meansMemo.
//
// - Need diversity change for each candidate.
//
// The first 3 bullets are encapsulated in the helper function
// computeCandidatesForRange. It works for both replica additions and
// rebalancing.
//
// For the last bullet (diversity), the caller of computeCandidatesForRange
// needs to populate candidateInfo.diversityScore for each candidate in
// candidateSet. It does so via diversityScoringMemo. Then the (loadSummary,
// diversityScore) pair can be used to order candidates for attempts to add.
//
// TODO(sumeer): caching of information should mostly make the allocator pass
// efficient, but we have one unaddressed problem: When moving a range we are
// having to do O(num-stores) amount of work to iterate over the cached
// information to populate candidateSet. Same to populate the diversity
// information for each candidate (which is also cached and looked up in a
// map). Then this candidateSet needs to be sorted which is O(num-stores*log
// num-stores). Allocator-like components that scale to large clusters try to
// have a constant cost per range that is being examined for rebalancing. If
// cpu profiles of microbenchmarks indicate we have a problem here we could
// try the following optimization:
//
// Assume most ranges in the cluster have exactly the same set of localities
// for their replicas, and the same set of constraints. We have computed the
// candidateSet fully for range R1 that is our current range being examined
// for rebalancing. And say we picked a candidate N1 and changed its adjusted
// load. We can recalculate N1's loadSummary and reposition (or remove) N1
// within this sorted set -- this can be done with cost O(log num-stores) (and
// typically we won't need to reposition). If the next range considered for
// rebalancing, R2, has the same set of replica localities and constraints, we
// can start with this existing computed set.

// loadSheddingStore is only specified if this candidate computation is
// happening because of overload.
func (a *allocatorState) computeCandidatesForRange(
	expr constraintsDisj, storesToExclude storeIDPostingList, loadSheddingStore roachpb.StoreID,
) (_ candidateSet, sheddingSLS storeLoadSummary) {
	means := a.cs.meansMemo.getMeans(expr)
	if loadSheddingStore > 0 {
		sheddingSS := a.cs.stores[loadSheddingStore]
		sheddingSLS = a.cs.meansMemo.getStoreLoadSummary(means, loadSheddingStore, sheddingSS.loadSeqNum)
		if sheddingSLS.sls <= loadNoChange && sheddingSLS.nls <= loadNoChange {
			// In this set of stores, this store no longer looks overloaded.
			return candidateSet{}, sheddingSLS
		}
	}
	// We only filter out stores that are not fdOK. The rest of the filtering
	// happens later.
	var cset candidateSet
	for _, storeID := range means.stores {
		if storesToExclude.contains(storeID) {
			continue
		}
		ss := a.cs.stores[storeID]
		csls := a.cs.meansMemo.getStoreLoadSummary(means, storeID, ss.loadSeqNum)
		if csls.fd != fdOK {
			continue
		}
		cset.candidates = append(cset.candidates, candidateInfo{
			StoreID:          storeID,
			storeLoadSummary: csls,
		})
	}
	cset.means = means
	return cset, sheddingSLS
}

// Diversity scoring is very amenable to caching, since the set of unique
// locality tiers for range replicas is likely to be small. And the cache does
// not need to be cleared after every allocator pass. This caching is done via
// diversityScoringMemo which contains existingReplicaLocalities.

// existingReplicaLocalities is the cache state for a set of replicas with
// locality tiers corresponding to replicasLocalityTiers (which serves as the
// key in diversityScoringMemo). NB: For a range, scoring for non-voters uses
// the set of all replicas, and for voters uses the set of existing voters.
//
// Cases:
type existingReplicaLocalities struct {
	replicasLocalityTiers

	// For a prospective candidate with localityTiers represented by the key,
	// this caches the sum of calls to replicas[i].diversityScore(candidate).
	//
	// If this is an addition, this value can be used directly if the candidate
	// is not already in replicas, else the diversity change is 0.
	//
	// If this is a removal, this is the reduction in diversity. Note that this
	// calculation includes diversityScore with self, but that value is 0.
	scoreSums map[string]float64
}

func (erl *existingReplicaLocalities) clear() {
	erl.replicas = erl.replicas[:0]
	for k := range erl.scoreSums {
		delete(erl.scoreSums, k)
	}
}

var _ mapEntry = &existingReplicaLocalities{}

// replicasLocalityTiers represents the set of localityTiers corresponding to
// a set of replicas (one localityTiers per replica).
type replicasLocalityTiers struct {
	// replicas are sorted in ascending order of localityTiers.str (this is
	// a set, so the order doesn't semantically matter).
	replicas []localityTiers
}

var _ mapKey = replicasLocalityTiers{}

func makeReplicasLocalityTiers(replicas []localityTiers) replicasLocalityTiers {
	slices.SortFunc(replicas, func(a, b localityTiers) int {
		return cmp.Compare(a.str, b.str)
	})
	return replicasLocalityTiers{replicas: replicas}
}

// hash implements the mapKey interface, using the FNV-1a hash algorithm.
func (rlt replicasLocalityTiers) hash() uint64 {
	h := uint64(offset64)
	for i := range rlt.replicas {
		for _, code := range rlt.replicas[i].tiers {
			h ^= uint64(code)
			h *= prime64
		}
		// Separator between different replicas. This is equivalent to
		// encountering the 0 code, which is the empty string. We don't expect to
		// see an empty string in a locality.
		h *= prime64
	}
	return h
}

// isEqual implements the mapKey interface.
func (rlt replicasLocalityTiers) isEqual(b mapKey) bool {
	other := b.(replicasLocalityTiers)
	if len(rlt.replicas) != len(other.replicas) {
		return false
	}
	for i := range rlt.replicas {
		if len(rlt.replicas[i].tiers) != len(other.replicas[i].tiers) {
			return false
		}
		for j := range rlt.replicas[i].tiers {
			if rlt.replicas[i].tiers[j] != other.replicas[i].tiers[j] {
				return false
			}
		}
	}
	return true
}

func (rlt replicasLocalityTiers) clone() replicasLocalityTiers {
	var r replicasLocalityTiers
	r.replicas = append(r.replicas, rlt.replicas...)
	return r
}

var existingReplicaLocalitiesSlicePool = sync.Pool{
	New: func() interface{} {
		return &mapEntrySlice[*existingReplicaLocalities]{}
	},
}

type existingReplicaLocalitiesSlicePoolImpl struct{}

func (p existingReplicaLocalitiesSlicePoolImpl) newEntry() *mapEntrySlice[*existingReplicaLocalities] {
	return existingReplicaLocalitiesSlicePool.New().(*mapEntrySlice[*existingReplicaLocalities])
}

func (p existingReplicaLocalitiesSlicePoolImpl) releaseEntry(
	slice *mapEntrySlice[*existingReplicaLocalities],
) {
	existingReplicaLocalitiesSlicePool.Put(slice)
}

type existingReplicaLocalitiesAllocator struct{}

func (a existingReplicaLocalitiesAllocator) ensureNonNilMapEntry(
	entry *existingReplicaLocalities,
) *existingReplicaLocalities {
	if entry == nil {
		return &existingReplicaLocalities{}
	}
	return entry
}

// diversityScoringMemo provides support for efficiently scoring the diversity
// for a set of replicas, and for adding/removing replicas.
type diversityScoringMemo struct {
	replicasMap *clearableMemoMap[replicasLocalityTiers, *existingReplicaLocalities]
}

func newDiversityScoringMemo() *diversityScoringMemo {
	return &diversityScoringMemo{
		replicasMap: newClearableMapMemo[replicasLocalityTiers, *existingReplicaLocalities](
			existingReplicaLocalitiesAllocator{}, existingReplicaLocalitiesSlicePoolImpl{}),
	}
}

// getExistingReplicaLocalities returns the existingReplicaLocalities object
// for the given replicas. The parameter can be mutated by the callee.
func (dsm *diversityScoringMemo) getExistingReplicaLocalities(
	existingReplicas replicasLocalityTiers,
) *existingReplicaLocalities {
	erl, ok := dsm.replicasMap.get(existingReplicas)
	if ok {
		return erl
	}
	erl.replicasLocalityTiers = existingReplicas.clone()
	erl.scoreSums = map[string]float64{}
	return erl
}

// If this is a NON_VOTER being added, then existingReplicaLocalities must
// represent both VOTER and NON_VOTER replicas, and replicaToAdd must not be
// an existing VOTER. Note that for the case where replicaToAdd is an existing
// VOTER, there is no score change, so this method should not be called.
//
// If this is a VOTER being added, then existingReplicaLocalities must
// represent only VOTER replicas.
func (erl *existingReplicaLocalities) getScoreChangeForNewReplica(
	replicaToAdd localityTiers,
) float64 {
	return erl.getScoreSum(replicaToAdd)
}

// If this is a NON_VOTER being removed, then existingReplicaLocalities must
// represent both VOTER and NON_VOTER replicas.
//
// If this is a VOTER being removed, then existingReplicaLocalities must
// represent only VOTER replicas.
func (erl *existingReplicaLocalities) getScoreChangeForReplicaRemoval(
	replicaToRemove localityTiers,
) float64 {
	// NB: we don't need to compensate for counting the pair-wise score of
	// replicaToRemove with itself, because that score is 0.
	return -erl.getScoreSum(replicaToRemove)
}

// If this is a VOTER being added and removed, replicaToRemove is of course a
// VOTER, and it doesn't matter if the one being added is already a NON_VOTER
// (since the existingReplicaLocalities represents only VOTERs).
//
// If this a NON_VOTER being added and removed, existingReplicaLocalities represents
// all VOTERs and NON_VOTERs. The replicaToAdd must not be an
// existing VOTER. For the case where replicaToAdd is an existing VOTER, there
// is no score change for the addition (to the full set of replicas), and:
//
//   - If replicaToRemove is not becoming a VOTER,
//     getScoreChangeForReplicaRemoval(replicaToRemove) should be called.
//
//   - If replicaToRemove is becoming a VOTER, the score change is 0 for the
//     full set of replicas.
//
// In the preceding cases where the VOTER set is also changing, of course the
// score change to the existingReplicaLocalities that represents only the
// VOTERs must also be considered.
func (erl *existingReplicaLocalities) getScoreChangeForRebalance(
	replicaToRemove localityTiers, replicaToAdd localityTiers,
) float64 {
	score := erl.getScoreSum(replicaToAdd) - erl.getScoreSum(replicaToRemove)
	// We have included the pair-wise diversity of (replicaToRemove,
	// replicaToAdd) above, but since replicaToRemove is being removed, we need
	// to subtract that pair.
	score -= replicaToRemove.diversityScore(replicaToAdd)
	return score
}

func (erl *existingReplicaLocalities) getScoreSum(replica localityTiers) float64 {
	score, ok := erl.scoreSums[replica.str]
	if !ok {
		for i := range erl.replicas {
			score += erl.replicas[i].diversityScore(replica)
		}
		erl.scoreSums[replica.str] = score
	}
	return score
}

func diversityScoresAlmostEqual(score1, score2 float64) bool {
	return math.Abs(score1-score2) < epsilon
}

// This is a somehow arbitrary chosen upper bound on the relative error to be
// used when comparing doubles for equality. The assumption that comes with it
// is that any sequence of operations on doubles won't produce a result with
// accuracy that is worse than this relative error. There is no guarantee
// however that this will be the case. A programmer writing code using
// floating point numbers will still need to be aware of the effect of the
// operations on the results and the possible loss of accuracy.
// More info https://en.wikipedia.org/wiki/Machine_epsilon
// https://en.wikipedia.org/wiki/Floating-point_arithmetic
const epsilon = 1e-10

// Avoid unused lint errors.

var _ = allocatorState{}.changeRateLimiter
var _ = (&existingReplicaLocalities{}).clear
var _ = replicasLocalityTiers{}.hash
var _ = replicasLocalityTiers{}.isEqual
var _ = existingReplicaLocalitiesSlicePoolImpl{}.newEntry
var _ = existingReplicaLocalitiesSlicePoolImpl{}.releaseEntry
var _ = existingReplicaLocalitiesAllocator{}.ensureNonNilMapEntry
var _ = (&diversityScoringMemo{}).getExistingReplicaLocalities
var _ = (&existingReplicaLocalities{}).getScoreChangeForNewReplica
var _ = (&existingReplicaLocalities{}).getScoreChangeForReplicaRemoval
var _ = (&existingReplicaLocalities{}).getScoreChangeForRebalance
var _ = candidateInfo{}.diversityScore
