// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package aimpl

import (
	"math/rand"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// What are we trying to do here?
//
// Allocator kernel, with no integration and interfaces that don't necessarily
// align with what is needed for integration. It is a proof-of-concept for
// rapid iteration to get something that can be integrated as an alternative
// allocator implementation.
//
// - Load and capacity of resources: load on stores and nodes is expressed
//   mostly in terms of multiple real resource dimensions (CPU, disk
//   bandwidth, stored bytes) and synthetic resource dimensions (write
//   bandwidth). Load balancing takes all these resource dimensions into
//   account.
//
//   - Attempts to reduce scoring (allocatorimpl.candidate has many scores) as
//     scores are hard to reason about since they don't necessarily correspond
//     to real cluster concepts. The diversity scoring is an exception to
//     this. Somewhat magical scoring also makes it harder to be sure about
//     the behavior implications of a change, which necessitates more
//     investment in understanding the behavior (such as via extensive
//     simulation). The goal here is to make the implications of the
//     algorithms more obvious just by reading them. We are simply adding and
//     subtracting load vectors.
//
// - Fast and efficient decision-making: pushes more of the work into
//   incrementally updated data-structures, and pre-processed state, so that
//   less work is needed when making change decisions. This includes
//   pre-populated sets (posting lists) for constraint checking.
//
// - Low lag: related to the previous bullet, we want the allocator to react
//   quickly when something bad happens (e.g. node failure). So we want to
//   avoid slow periodic scans of ranges.
//
// - Easily extendable to additional resource dimensions.
//
// - Scale to large and heterogeneous clusters (500+ nodes, 2000+ stores, 10M+
//   ranges): Heterogeneity is possible due to explicit consideration of
//   resource capacity. Scaling is possible because we attempt to be efficient
//   with decision-making.
//
//   - Avoid periodic O(range) scans of the allocator state: all ranges that
//     need some attention are added to a ranges-needing-attention set when
//     some state transition happens. O(range) scans are expensive and we do
//     not want infrequent periodic scans to delay action on ranges that need
//     attention.
//
// - Centralized or distributed: This first pass is written as-if centralized,
//   but an attempt has been made that loadState.ranges does not need to be
//   complete (it may be only the ranges known to the stores on this node),
//   and storeLoadAdjusted.load.{topKRanges,meanNonTopKRangeLoad,...} may only
//   be populated for stores on this node. So with some minor tweaks this
//   should work for a sharded allocator.
//
// - Consistent and efficient load updates in the presence of concurrent
//   changes (especially for centralized allocator): load updates from stores
//   and nodes can be presented as diffs with seqnums so that missing diffs
//   can be noticed. Pending changes adjust the load locally, and are kept in
//   a queue so that load updates can do partial discard of these changes.
//   There is no requirement that changes be applied in order, since they are
//   being applied by different raft groups.
//
// Centralized allocator will need additional machinery/work to de-risk:
//
// - Need to measure the CPU, memory and network cost of the allocator work to
//   check viability.
// - Rough master election: don't need the guarantee of at most one master. And
//   can tolerate ~30s delay with no master.
// - Load updates to master: can keep a connection open to every other node in
//   the cluster. Incremental load updates every 10s.

// TODO(sumeer): initialization of state.

// TODO(sumeer): include merge behavior into the allocator since we neeed to
// move all the adjacent range replicas to the same set of nodes before doing
// the merge and we don't want that movement to be undone by the allocator.

// TODO(sumeer): support splitting of the range.

type allocator struct {
	loadState               loadState
	constraintMatchingState constraintMatchingState
	rand                    *rand.Rand
}

// The load information periodically received from a node. This is the full
// state and not a diff.
type nodeLoadState struct {
	nodeID      roachpb.NodeID
	loadCPU     loadValue
	capacityCPU loadValue
	// TODO(sumeer): fdSummary doesn't really belong here, since its not sent by
	// the node itself. Add a method
	// allocator.updateFDSummary(failureDetectionSummary).
	fdSummary failureDetectionSummary
	stores    map[roachpb.StoreID]storeLoad
}

func (a *allocator) receivedLoadFromNode(nls nodeLoadState) {
	nl := a.loadState.nodes[nls.nodeID]
	nl.loadCPU = nls.loadCPU
	nl.capacityCPU = nls.capacityCPU
	nl.loadCPUAdjusted = nls.loadCPU
	nl.fdSummary = nls.fdSummary
	now := time.Now()
	for storeID, storeLoad := range nls.stores {
		sl := a.loadState.stores[storeID]
		sl.updateLoad(
			now, storeLoad, a.loadState.ranges, &a.loadState.meanStoreLoad, loadLow,
			a.loadState.rangesNeedingAttention)
	}
	nl.updateLoadSummary(a.loadState.meanNodeLoad)
	for storeID := range nls.stores {
		a.loadState.stores[storeID].updateLoadSummaryAndMaxFractionPending(
			&a.loadState.meanStoreLoad, nl.loadSummary)
	}
}

// TODO(sumeer): rebalancing to improve diversity. Do this after every other
// kind of rebalancing.

// Called periodically, say every 10s.
func (a *allocator) computeChanges() []*pendingChange {
	var changes []*pendingChange
	now := time.Now()
	for _, store := range a.loadState.stores {
		// TODO(sumeer): there is some circularity here that we can tease apart
		// with better interfaces. We want to GC and recompute the loadSummary,
		// but the loadSummary also depends on the node's loadSummary, which can't
		// be up-to-date until we have done the GC for its constituent stores. For
		// now we just pass loadLow in since we will recompute the summary in
		// another pass. We don't need to waste the work of computing the summary
		// here.
		store.gcStoreLoad(now, a.loadState.ranges, &a.loadState.meanStoreLoad, loadLow,
			a.loadState.rangesNeedingAttention)
	}
	a.loadState.computeMeans()
	for _, node := range a.loadState.nodes {
		node.updateLoadSummary(a.loadState.meanNodeLoad)
	}
	for _, store := range a.loadState.stores {
		store.updateLoadSummaryAndMaxFractionPending(&a.loadState.meanStoreLoad, store.nodeLoad.loadSummary)
	}
	// TODO(sumeer): when we are unsuccessful in constructing a change for a
	// range in rangesNeedingAttention or in rebalancing off from a store,
	// record that lack of success and add backoff in considering in this
	// periodic work. Can do an exponential backoff using time.Time to next
	// consider that is recorded in rangeState or in storeLoadAdjusted.
	for rangeID := range a.loadState.rangesNeedingAttention {
		rState := a.loadState.ranges[rangeID]
		if len(rState.pendingChanges) != 0 {
			delete(a.loadState.rangesNeedingAttention, rangeID)
		}
		// May need attention.
		a.ensureAnalyzedConstraints(rangeID, rState)
		// What kind of attention.
		//
		// TODO(sumeer): Decide on what is needed for the range and add ranges a
		// slice that we will reorder based on priority of the attention needed.
		// Then examine ranges in that order. Down-replication can happen with
		// arbitrary concurrency in the cluster. But we want to prioritize
		// up-replication before lack of constraint satisfaction, before lack of
		// diversity.
		//
		// TODO(sumeer): the following if-else block is incomplete, and assumes
		// that rState.constraints.voterConstraints == nil and there are no
		// non-voters. We need to rethink the normalization of spanConfig or at
		// least what we compute in rangeAnalyzedConstraints. What we are really
		// after is to quickly determine whether we need to add/remove/move a
		// voter or a non-voter, and then quickly figure out the candidates. We
		// could normalize to voterGeneralConstraints and
		// voterSpecificConstraints, such that we can independently look at their
		// satisfaction. And normalize to nonVoterGeneralConstraints.
		//
		// TODO(sumeer): what about leaseholder preference. Whenever the replicas
		// of a range change, it will be in rangesNeedingAttention because we need
		// to check the replica constraints. So we will be here. If the
		// constraints are satisfied, then we should also see if the leaseholder
		// placement is the best among the current voter replicas. If not, then
		// look at the prospective leaseholders in decreasing order of preference
		// and pick the first that can handle the increased load. And then remove
		// the range from rangesNeedingAttention. There is a slight issue in that
		// if the best candidate is not able to handle the lease load now, but
		// later has lower load, we still want to eventually move the lease -- a
		// slow periodic scan of all ranges may be unavoidable for this. Make a
		// list of all cases where we still need a slow periodic scan of all
		// ranges (so far I have only encountered this one case).
		oversatisfiedVoter := rState.constraints.replicaConstraints.hasOversatisfiedConstraint()
		unsatisfiedVoter := rState.constraints.replicaConstraints.hasUnsatisfiedConstraint()
		if oversatisfiedVoter && !unsatisfiedVoter {
			// Drop a voter replica
			candidates := voterCandidatesThatOversatisfy(rState.constraints.replicaConstraints,
				rState.constraints.voterConstraints)
			a.orderByIncDiversityDecLoadForRemoval(rangeID, candidates)
			change := a.loadState.pendingChangeToDropReplica(rangeID, candidates[0])
			changes = append(changes, change)
			delete(a.loadState.rangesNeedingAttention, rangeID)
		} else if unsatisfiedVoter && !oversatisfiedVoter {
			// Add a voter replica
			existingVoters, rLoad := a.getExistingVotersAndSampleLoad(rangeID, rState)
			lowLoadStores := a.getTargetStores()
			candidates := a.constraintMatchingState.storeCandidatesForAddVoterReplica(
				lowLoadStores, existingVoters, rState.constraints.replicaConstraints,
				rState.constraints.voterConstraints)
			change := makePartialPendingChangeToAddVoter(rangeID, rLoad)
			diversities := map[string]float64{}
			diversityFunc := func(_ roachpb.StoreID, candidateLocState localityState) float64 {
				return a.loadState.diversityAddReplicaScore(rangeID, candidateLocState, roachpb.VOTER_FULL,
					diversities)
			}
			picked := a.orderAndPickTwoTargetStores(candidates, diversityFunc)
			for _, storeID := range picked {
				if storeID == 0 {
					break
				}
				sLoad := a.loadState.stores[storeID]
				if sLoad.canAddLoad(
					change.rangeLoadDelta.load, a.loadState.meanNodeLoad, &a.loadState.meanStoreLoad) {
					change.storeID = storeID
					a.loadState.pendingChangeToAddVoterReplica(change)
					changes = append(changes, change)
					delete(a.loadState.rangesNeedingAttention, rangeID)
					break
				}
			}
		} else if oversatisfiedVoter && unsatisfiedVoter {
			// Move a voter replica.
			//
			// TODO(sumeer): if the most loaded is a leaseholder, move the lease
			// elsewhere first.
			sourceCandidates := voterCandidatesThatOversatisfy(rState.constraints.replicaConstraints,
				rState.constraints.voterConstraints)
			a.orderByIncDiversityDecLoadForRemoval(rangeID, sourceCandidates)
			sourceCandidate := sourceCandidates[0]
			sourceLocState := a.loadState.stores[sourceCandidate].nodeLoad.localityState
			lowLoadStores := a.getTargetStores()
			existingVoters, rLoad := a.getExistingVotersAndSampleLoad(rangeID, rState)
			targetCandidates := a.constraintMatchingState.storeCandidatesForAddVoterReplica(
				lowLoadStores, existingVoters, rState.constraints.replicaConstraints,
				rState.constraints.voterConstraints)
			changeAdd := makePartialPendingChangeToAddVoter(rangeID, rLoad)
			diversities := map[string]float64{}
			diversityFunc := func(candidateStoreID roachpb.StoreID, candidateLocState localityState) float64 {
				return a.loadState.diversityRebalanceReplicaScore(
					rangeID, sourceCandidate, sourceLocState, candidateStoreID, candidateLocState,
					roachpb.VOTER_FULL, diversities)
			}
			picked := a.orderAndPickTwoTargetStores(targetCandidates, diversityFunc)
			for _, storeID := range picked {
				if storeID == 0 {
					break
				}
				sLoad := a.loadState.stores[storeID]
				if sLoad.canAddLoad(
					changeAdd.rangeLoadDelta.load, a.loadState.meanNodeLoad, &a.loadState.meanStoreLoad) {
					changeAdd.storeID = storeID
					a.loadState.pendingChangeToAddVoterReplica(changeAdd)
					changeRemove := a.loadState.pendingChangeToDropReplica(rangeID, sourceCandidate)
					// TODO(sumeer): change the interface for suggested changes to external
					// entity to allow us to suggest a pair of changes for the range.
					changes = append(changes, changeAdd, changeRemove)
					delete(a.loadState.rangesNeedingAttention, rangeID)
					break
				}
			}
		}
	}
	changes = append(changes, a.rebalanceStores()...)
	return changes
}

type storeLoadAndDiversity struct {
	diversity float64
	load      *storeLoadAdjusted
}

type storeByIncDiversityDecLoad []storeLoadAndDiversity

func (s storeByIncDiversityDecLoad) Len() int {
	return len(s)
}

func (s storeByIncDiversityDecLoad) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s storeByIncDiversityDecLoad) Less(i, j int) bool {
	if s[i].diversity == s[j].diversity {
		return s[i].load.loadSummary > s[j].load.loadSummary
	}
	return s[i].diversity < s[j].diversity
}

func (a *allocator) orderByIncDiversityDecLoadForRemoval(
	rangeID roachpb.RangeID, stores []roachpb.StoreID,
) {
	if len(stores) == 1 {
		return
	}
	diversities := map[string]float64{}
	var storesWithDiversity []storeLoadAndDiversity
	for _, storeID := range stores {
		sLoad := a.loadState.stores[storeID]
		ls := sLoad.nodeLoad.localityState
		replicaType := sLoad.adjusted.replicas[rangeID].replicaType
		diversity := a.loadState.diversityRemoveReplicaScore(
			rangeID, storeID, ls, replicaType, diversities)
		storesWithDiversity = append(storesWithDiversity,
			storeLoadAndDiversity{diversity: diversity, load: sLoad})
	}
	sort.Sort(storeByIncDiversityDecLoad(storesWithDiversity))
	for i := range storesWithDiversity {
		stores[i] = storesWithDiversity[i].load.storeID
	}
}

func (a *allocator) ensureAnalyzedConstraints(rangeID roachpb.RangeID, rangeState *rangeState) {
	if rangeState.constraints != nil {
		return
	}
	var stores storeIDsForAnalysis
	for _, storeID := range rangeState.replicas {
		replicaIDAndType := a.loadState.stores[storeID].adjusted.replicas[rangeID]
		stores.tryAddingStore(storeID, replicaIDAndType.replicaType)
	}
	rangeState.constraints =
		a.constraintMatchingState.computeAnalyzedConstraints(stores, rangeState.conf)
}

func (a *allocator) getTargetStores() postingList {
	var targetStores []roachpb.StoreID
	for _, store := range a.loadState.stores {
		if store.loadSummary >= loadNormal {
			targetStores = append(targetStores, store.storeID)
		}
	}
	return makePostingList(targetStores)
}

func (a *allocator) rebalanceStores() []*pendingChange {
	var changes []*pendingChange
	var sourceStores []*storeLoadAdjusted
	targetStores := a.getTargetStores()
	for _, store := range a.loadState.stores {
		if store.loadSummary <= overloadSlow {
			sourceStores = append(sourceStores, store)
		}
	}
	a.orderSourceStoresForRebalance(sourceStores)
	for _, store := range sourceStores {
		if len(targetStores) == 0 {
			break
		}
		// Iterate over top-K ranges first and try to move them.
		for rangeID, rLoad := range store.adjusted.topKRanges {
			a.tryMovingRange(store, rangeID, rLoad, &targetStores, &changes)
			if store.loadSummary >= loadNoChange || len(targetStores) == 0 {
				// Done with this store.
				break
			}
		}
		// Only continue with non-top-K iteration for failureImminent. For regular
		// rebalancing, we will wait until those top-K move and then continue with
		// the rest. There is a risk that the top-K have some constraint that
		// prevents rebalancing, while the rest can be moved. Running with
		// underprovisioned clusters and expecting load-based rebalancing to work
		// well is not in scope.
		if store.loadSummary == failureImminent && len(targetStores) > 0 {
			for rangeID := range store.adjusted.replicas {
				a.tryMovingRange(store, rangeID, store.adjusted.meanNonTopKRangeLoad, &targetStores, &changes)
				if store.loadSummary >= loadNoChange || len(targetStores) == 0 {
					// Done with this store.
					break
				}
			}
		}
	}
	return changes
}

func (a *allocator) tryMovingRange(
	store *storeLoadAdjusted,
	rangeID roachpb.RangeID,
	rLoad rangeLoad,
	targetStores *postingList,
	changes *[]*pendingChange,
) {
	rState := a.loadState.ranges[rangeID]
	if len(rState.pendingChanges) > 0 {
		// Have some pending changes that are not related to rebalancing away from
		// this store. If they were related to rebalancing away from this store we
		// would not even be considering this range. It is possible this is a
		// range that is being rebalanced into this store, though that should be
		// rare since we wouldn't have picked this store for rebalancing into if
		// it was likely to get overloaded.
		return
	}
	a.ensureAnalyzedConstraints(rangeID, rState)
	replicaInfo := store.adjusted.replicas[rangeID]
	if replicaInfo.replicaType == roachpb.VOTER_FULL {
		if replicaInfo.isLeaseholder {
			// Move the leaseholder to somewhere else first.
			var candidateLeaseholders []roachpb.StoreID
			for _, storeID := range rState.replicas {
				if storeID == store.storeID {
					continue
				}
				if a.loadState.stores[storeID].adjusted.replicas[rangeID].replicaType == roachpb.VOTER_FULL {
					candidateLeaseholders = append(candidateLeaseholders, storeID)
				}
			}
			pl := makePostingList(candidateLeaseholders)
			pl.intersect(*targetStores)
			var alreadyTriedCandidates postingList
			addChange := makePartialPendingChangeToAddLeaseholder(rangeID, rLoad)
			// Try the lease preferences in order.
			for _, leasePreference := range rState.conf.leasePreferences {
				pl := pl.clone()
				pl.intersect(a.constraintMatchingState.constrainStores(leasePreference.Constraints))
				pl.subtract(alreadyTriedCandidates)
				alreadyTriedCandidates.union(pl)
				a.orderTargetStoresForLeaseholder(pl)
				for _, targetStoreID := range pl {
					targetStoreLoad := a.loadState.stores[targetStoreID]
					if targetStoreLoad.canAddLoad(addChange.rangeLoadDelta.load, a.loadState.meanNodeLoad,
						&a.loadState.meanStoreLoad) {
						addChange.storeID = targetStoreID
						addChange.prev = targetStoreLoad.adjusted.replicas[rangeID]
						addChange.next = addChange.prev
						addChange.next.isLeaseholder = true
						a.loadState.pendingChangeToAddLease(addChange)
						// Removal will automatically happen as a side-effect of the
						// addition of the lease, since there can be only 1 leaseholder.
						// We don't need to tell the external entity about the removal
						// since there is no choice in the matter. But we keep a different
						// pending change for that in our internal data-structure.
						_ = a.loadState.pendingChangeToRemoveLease(store, rLoad)
						*changes = append(*changes, addChange)
						if targetStoreLoad.loadSummary <= loadNoChange {
							targetStores.subtract([]roachpb.StoreID{targetStoreID})
						}
						break
					} else {
						alreadyTriedCandidates.union([]roachpb.StoreID{targetStoreID})
					}
				}
			}
		} else {
			existingVoters, _ := a.getExistingVotersAndSampleLoad(rangeID, rState)
			pl := a.constraintMatchingState.storeCandidatesForReplacingVoterReplica(
				*targetStores, existingVoters, store.storeID, rState.constraints.replicaConstraints,
				rState.constraints.voterConstraints)
			changeAdd := makePartialPendingChangeToAddVoter(rangeID, rLoad)
			diversities := map[string]float64{}
			diversityFunc := func(candidateStoreID roachpb.StoreID, candidateLocState localityState) float64 {
				return a.loadState.diversityRebalanceReplicaScore(
					rangeID, store.storeID, store.nodeLoad.localityState, candidateStoreID, candidateLocState,
					roachpb.VOTER_FULL, diversities)
			}
			picked := a.orderAndPickTwoTargetStores(pl, diversityFunc)
			for _, storeID := range picked {
				if storeID == 0 {
					break
				}
				targetStoreLoad := a.loadState.stores[storeID]
				if targetStoreLoad.canAddLoad(
					changeAdd.rangeLoadDelta.load, a.loadState.meanNodeLoad, &a.loadState.meanStoreLoad) {
					changeAdd.storeID = storeID
					a.loadState.pendingChangeToAddVoterReplica(changeAdd)
					changeRemove := a.loadState.pendingChangeToDropReplica(rangeID, store.storeID)
					// TODO(sumeer): change the interface for suggested changes to external
					// entity to allow us to suggest a pair of changes for the range.
					*changes = append(*changes, changeAdd, changeRemove)
					if targetStoreLoad.loadSummary <= loadNoChange {
						targetStores.subtract([]roachpb.StoreID{storeID})
					}
					break
				}
			}
		}
	} else if replicaInfo.replicaType == roachpb.NON_VOTER {
		// TODO(sumeer): handle non voter case.
	}
}

func (a *allocator) orderSourceStoresForRebalance(stores []*storeLoadAdjusted) {
	sort.Sort(storeLoadByDecreasingLoad(stores))
	// Permute each of the n loadSummary sub-slices, so that examine nodes in different
	// sort order in each rebalancing pass.
	curLoad := failureImminent
	startIndex := 0
	i := 0
	for {
		done := i == len(stores)
		if done || stores[i].loadSummary != curLoad {
			if i > startIndex+1 {
				a.rand.Shuffle(i-startIndex, func(i, j int) {
					stores[startIndex+i], stores[startIndex+j] = stores[startIndex+j], stores[startIndex+i]
				})
				if done {
					break
				}
				startIndex = i
				curLoad = stores[i].loadSummary
			}
		}
		i++
	}
}

type diversityFunc func(candidateStoreID roachpb.StoreID, DcandidateLocState localityState) float64

func (a *allocator) orderTargetStoresForLeaseholder(pl postingList) {
	var stores []*storeLoadAdjusted
	for _, storeID := range pl {
		sLoad := a.loadState.stores[storeID]
		stores = append(stores, sLoad)
	}
	sort.Sort(storeLoadByDecreasingLoad(stores))
	n := len(stores) - 1
	for i := range stores {
		pl[n-i] = stores[i].storeID
	}
}

// Don't want to iterate over all target stores. So we randomly pick among the
// "best stores". The only thing that will prevent using one of these target
// stores is the extra load we are trying to add. Due to the randomization we
// shouldn't keep getting unlucky.
//
// TODO(sumeer): select 10-ish targets. It should not be expensive to check
// all of them. We are merely concerned about about cases with 100s of
// eligible stores and trying all of them.
//
// TODO(sumeer): the randomization was sufficient when we were only using
// loadSummary. But it is possible that the best diversity set of stores do
// not have enough capacity for the extra load (because of the
// multi-dimensional load vector). We can add a retry parameter that is
// increased whenever a store that needs to shed load is unable to shed
// anything, or a range that is under-replicated is unable to add a replica.
// And use that retry parameter to broaden the stores we can possibly pick
// from pl.
func (a *allocator) orderAndPickTwoTargetStores(
	pl postingList, diversityFunc diversityFunc,
) (picked [2]roachpb.StoreID) {
	if len(pl) == 0 {
		return picked
	}
	var stores []storeLoadAndDiversity
	for _, storeID := range pl {
		sLoad := a.loadState.stores[storeID]
		stores = append(stores,
			storeLoadAndDiversity{diversity: diversityFunc(
				storeID, sLoad.nodeLoad.localityState), load: sLoad})
	}
	// Sort by least desirable to most desirable. Then grab all the equivalent
	// ones in most desirable and pick two random ones. Repeat if don't yet have
	// two.
	sort.Sort(storeByIncDiversityDecLoad(stores))
	n := len(stores)
	curDiversityAndLoad := struct {
		diversity   float64
		loadSummary loadSummary
	}{
		diversity:   stores[n-1].diversity,
		loadSummary: stores[n-1].load.loadSummary,
	}
	startIndex := n - 1
	i := n - 1
	numPicked := 0
	for {
		done := i < 0
		if done || (stores[i].load.loadSummary != curDiversityAndLoad.loadSummary ||
			stores[i].diversity != curDiversityAndLoad.diversity) {
			if i < startIndex {
				n := startIndex - i
				j := a.rand.Intn(n)
				picked[numPicked] = stores[startIndex-j].load.storeID
				numPicked++
				if numPicked == 2 {
					break
				}
				if n > 1 {
					stores[startIndex-j], stores[startIndex] = stores[startIndex], stores[startIndex-j]
					startIndex--
					n--
					j := a.rand.Intn(n)
					picked[numPicked] = stores[startIndex-j].load.storeID
					break
				}
				if done {
					break
				}
				startIndex = i
				curDiversityAndLoad.diversity = stores[i].diversity
				curDiversityAndLoad.loadSummary = stores[i].load.loadSummary
			}
		}
		i--
	}
	return picked
}

func (a *allocator) getExistingVotersAndSampleLoad(
	rangeID roachpb.RangeID, rState *rangeState,
) (existingVoters []roachpb.StoreID, rLoad rangeLoad) {
	for _, storeID := range rState.replicas {
		sLoad := a.loadState.stores[storeID].adjusted
		replicaType := sLoad.replicas[rangeID].replicaType
		// TODO(sumeer): this classification needs to be the same as used in
		// storeIDsForAnalysis, so unify with that code.
		if replicaType == roachpb.VOTER_FULL || replicaType == roachpb.VOTER_INCOMING {
			if len(existingVoters) == 0 {
				rLoad, ok := sLoad.topKRanges[rangeID]
				if !ok {
					rLoad = sLoad.meanNonTopKRangeLoad
				}
				rLoad.load[cpu] -= rLoad.raftCPU
				rLoad.raftCPU = 0
			}
			existingVoters = append(existingVoters, storeID)
		}
	}
	return existingVoters, rLoad
}
