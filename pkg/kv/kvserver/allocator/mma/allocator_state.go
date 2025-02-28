// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mma

import (
	"cmp"
	"math"
	"math/rand"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type allocatorState struct {
	cs     *clusterState
	nodeID roachpb.NodeID

	// Ranges that are under-replicated, over-replicated, don't satisfy
	// constraints, have low diversity etc. Avoids iterating through all ranges.
	// A range is removed from this map if it has pending changes -- when those
	// pending changes go away it gets added back so we can check on it.
	rangesNeedingAttention map[roachpb.RangeID]struct{}

	meansMemo            *meansMemo
	diversityScoringMemo *diversityScoringMemo

	changeRateLimiter *storeChangeRateLimiter

	changeIDCounter changeID

	rand *rand.Rand
}

// TODO(sumeer): temporary constants.
const (
	rebalanceInterval           time.Duration = 10 * time.Second
	rateChangeLimiterGCInterval               = 15 * time.Second
	numAllocators                             = 20
	maxFractionPendingThreshold               = 0.1
)

func newAllocatorState() *allocatorState {
	interner := newStringInterner()
	cs := newClusterState(interner)
	return &allocatorState{
		cs:                     cs,
		rangesNeedingAttention: map[roachpb.RangeID]struct{}{},
		meansMemo:              newMeansMemo(cs, cs.constraintMatcher),
		diversityScoringMemo:   newDiversityScoringMemo(),
		changeRateLimiter:      newStoreChangeRateLimiter(rateChangeLimiterGCInterval),
	}
}

// TODO(sumeer): lease shedding:
//
// - treat CPU as a special load dimension since that is the only dimension
//   we can shed by moving the leaseholder.
//
// - make gossip should propagate the number of leases along with the load.
//   And the aggregate non-raft cpu.
//
// - remote store: don't start moving ranges for a cpu overloaded remote store
//   if it still has leases. Give it the opportunity to shed *all* its leases
//   first, or have its aggregate non-raft cpu fall to a level where it doesn't
//   change the fact that it is cpu overloaded.
//
// - allocatorState needs to know the difference between local and remote stores.
//   For local store, the allocator will obviously shed leases first in the case
//   of cpu overload.

// Called periodically, say every 10s.
func (a *allocatorState) rebalanceStores() []*pendingReplicaChange {
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
	clusterMeans := a.meansMemo.getMeans(nil)
	a.changeRateLimiter.initForRebalancePass(numAllocators, clusterMeans.storeLoad)
	type sheddingStore struct {
		roachpb.StoreID
		storeLoadSummary
	}
	var sheddingStores []sheddingStore
	// TODO: change clusterState load stuff so that cpu util is distributed
	// across the stores. If cpu util of a node is higher than the mean across
	// nodes of the cluster, then cpu util of at least one store on that node
	// will be higher than the mean across all stores in the cluster (since the
	// cpu util of a node is simply the mean across all its stores). The
	// following code assumes this has already been done.

	for storeID, ss := range a.cs.stores {
		a.changeRateLimiter.updateForRebalancePass(&ss.adjusted.enactedHistory, now)
		sls := a.meansMemo.getStoreLoadSummary(clusterMeans, storeID, ss.loadSeqNum)
		if (sls.sls <= overloadSlow && ss.adjusted.enactedHistory.allowLoadBasedChanges() &&
			ss.maxFractionPending < maxFractionPendingThreshold) || sls.fd >= fdDrain {
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
		return cmp.Or(cmp.Compare(b.fd, a.fd), cmp.Compare(a.nls, b.nls), cmp.Compare(a.sls, b.sls))
	})
	var changes []*pendingReplicaChange
	var disj [1]constraintsConj
	var storesToExclude storeIDPostingList
	var storesToExcludeForRange storeIDPostingList
	scratchNodes := map[roachpb.NodeID]*nodeLoad{}
	for _, store := range sheddingStores {
		// TODO(sumeer): For remote stores that are cpu overloaded, wait for them
		// to shed leases first. See earlier longer to do.

		doneShedding := false
		ss := a.cs.stores[store.StoreID]
		if ss.NodeID == a.nodeID && store.storeCPUSummary <= overloadSlow {
			// This store is local, and cpu overloaded. Shed leases first.
			//
			// TODO: is this path (for StoreRebalancer) also responsible for
			// shedding leases for fdDrain and fdDead?

			// NB: any ranges at this store that don't have pending changes must
			// have this local store as the leaseholder.
			for rangeID := range ss.adjusted.topKRanges {
				rstate := a.cs.ranges[rangeID]
				if len(rstate.pendingChanges) > 0 {
					// If the range has pending changes, don't make more changes.
					continue
				}
				if !a.ensureAnalyzedConstraints(rstate) {
					continue
				}
				cands, _ := rstate.constraints.candidatesToMoveLease()
				var candsPL storeIDPostingList
				for _, cand := range cands {
					candsPL.insert(cand.storeID)
				}
				candsPL.insert(store.StoreID)
				var means meansForStoreSet
				clear(scratchNodes)
				computeMeansForStoreSet(candsPL, a.cs, &means, scratchNodes)
				var candsSet candidateSet
				for _, cand := range cands {
					sls := a.cs.computeLoadSummary(cand.storeID, &means.storeLoad, &means.nodeLoad)
					if sls.nls <= loadNoChange || sls.fd != fdOK || sls.sls <= loadNoChange {
						continue
					}
					candsSet.candidates = append(candsSet.candidates, candidateInfo{
						StoreID:          cand.storeID,
						storeLoadSummary: sls,
						// TODO: should we change this field to say score and set it to
						// negative of cand.leasePreferenceIndex.
						diversityScore: 0,
					})
				}
				if len(candsSet.candidates) == 0 {
					continue
				}
				// Have underloaded candidates.
				//
				// TODO: this is questionable. We should refactor
				// sortTargetCandidateSetAndPick into multiple functions, one for
				// sorting, one for picking etc. and compose them differently for
				// different use cases. And we possibly don't want to pick just one
				// since we can possibly afford to go through all (since a range has
				// few replicas).
				targetStoreID := sortTargetCandidateSetAndPick(candsSet, a.rand)
				if targetStoreID == 0 {
					continue
				}
				targetSS := a.cs.stores[targetStoreID]
				if !a.cs.canAddLoad(targetSS, rstate.load.load, &means) {
					continue
				}
				pendingChanges := makeLeaseTransferChanges(
					rangeID, rstate.replicas, rstate.load, targetStoreID, ss.StoreID)
				for i := range pendingChanges {
					pendingChanges[i].changeID = a.allocChangeID()
				}
				changes = append(changes, pendingChanges[:]...)
				a.cs.addPendingChanges(rangeID, pendingChanges[:])
				doneShedding = ss.maxFractionPending >= maxFractionPendingThreshold
				if doneShedding {
					break
				}
			}
			if doneShedding {
				continue
			}
		}
		// If the node is cpu overloaded, or the store/node is not fdOK, exclude
		// the other stores on this node from receiving replicas shed by this
		// store.
		excludeStoresOnNode := store.nls < overloadSlow || store.fd != fdOK
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
		var loadSheddingStore roachpb.StoreID
		if store.fd != fdDead {
			// We only shed replicas (not just leases), for non-overloaded stores,
			// when they are dead. Since this store is not dead, it must be trying
			// to shed due to load.
			loadSheddingStore = store.StoreID
		}
		// Iterate over top-K ranges first and try to move them.
		//
		// TODO: Don't include rangeLoad as the value in the topKRanges map -- we
		// don't need it since we lookup rangeState anyway.
		for rangeID := range ss.adjusted.topKRanges {
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
				panic("mma internal state inconsistency")
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
			cands := a.computeCandidatesForRange(disj[:], storesToExcludeForRange, loadSheddingStore)
			n := len(cands.candidates)
			for i := 0; i < n; {
				if cands.candidates[i].fd != fdOK {
					// Remove it.
					n--
					cands.candidates[i], cands.candidates[n] = cands.candidates[n], cands.candidates[i]
					continue
				}
				i++
			}
			if len(cands.candidates) == 0 {
				continue
			}
			var rlocalities replicasLocalityTiers
			if isVoter {
				rlocalities = rstate.constraints.voterLocalityTiers
			} else {
				rlocalities = rstate.constraints.replicaLocalityTiers
			}
			localities := a.diversityScoringMemo.getExistingReplicaLocalities(rlocalities.replicas)
			// Set the diversity score of the candidates.
			for _, cand := range cands.candidates {
				cand.diversityScore = localities.getScoreChangeForRebalance(
					ss.localityTiers, a.cs.stores[cand.StoreID].localityTiers)
			}
			targetStoreID := sortTargetCandidateSetAndPick(cands, a.rand)
			if targetStoreID == 0 {
				continue
			}
			targetSS := a.cs.stores[targetStoreID]
			isLeaseholder := rstate.constraints.leaseholderID == store.StoreID
			addedLoad := rstate.load.load
			if !isLeaseholder {
				addedLoad[cpu] = rstate.load.raftCPU
			}
			if !a.cs.canAddLoad(targetSS, addedLoad, cands.means) {
				continue
			}
			pendingChanges := makeRebalanceReplicaChanges(
				rangeID, rstate.replicas, rstate.load, targetStoreID, ss.StoreID)
			for i := range pendingChanges {
				pendingChanges[i].changeID = a.allocChangeID()
			}
			changes = append(changes, pendingChanges[:]...)
			a.cs.addPendingChanges(rangeID, pendingChanges[:])
			doneShedding = ss.maxFractionPending >= maxFractionPendingThreshold
			if doneShedding {
				break
			}
		}
		// TODO(sumeer): continue with non-top-K iteration for fdDead. For regular
		// rebalancing, we will wait until those top-K move and then continue with
		// the rest. There is a risk that the top-K have some constraint that
		// prevents rebalancing, while the rest can be moved. Running with
		// underprovisioned clusters and expecting load-based rebalancing to work
		// well is not in scope.
		if doneShedding {
			continue
		}
	}
	return changes
}

func (a *allocatorState) allocChangeID() changeID {
	id := a.changeIDCounter
	a.changeIDCounter++
	return id
}

// TODO(sumeer): look at support methods for allocatorState.tryMovingRange in
// the allocator kernel draft PR.

type candidateInfo struct {
	roachpb.StoreID
	storeLoadSummary
	diversityScore float64
}

type candidateSet struct {
	candidates []candidateInfo
	means      *meansForStoreSet
}

//
// TODO:

// Once have that set, it is in non-increasing order of load. None of
// these decrease diversity. We need to group ones that are similar in
// terms of load and pick a random one. See candidateList.selectBest in
// allocator_scorer.go. Our enum for loadSummary is quite coarse. This
// has pros and cons: the pro is that the coarseness increases the
// probability that we do eventually find something that can handle the
// multi-dimensional load of this range. The con is that we may not
// select the candidate that is the very best. For instance if a new
// store is added it will also be loadLow like many others, but we want
// more ranges to go to it. One refinement we can try is to make the
// summary more fine-grained, and increment a count of times this store
// has tried to shed this range and failed, and widen the scope of what
// we pick from after some failures.
//
// TODO(sumeer): implement that refinement after some initial
// experimentation and learnings.
func sortTargetCandidateSetAndPick(cands candidateSet, rng *rand.Rand) roachpb.StoreID {
	slices.SortFunc(cands.candidates, func(a, b candidateInfo) int {
		if diversityScoresAlmostEqual(a.diversityScore, b.diversityScore) {
			// Since we have already excluded overloaded nodes, we only consider sls.
			return -cmp.Compare(a.sls, b.sls)
		}
		return -cmp.Compare(a.diversityScore, b.diversityScore)
	})
	bestDiversity := cands.candidates[0].diversityScore
	j := 0
	// Iterate over candidates with the same diversity. First such set that is
	// not disk capacity constrained is where we stop. Even if they can't accept
	// because they have too many pending changes or can't handle the addition
	// of the range. That is, we are not willing to reduce diversity when
	// rebalancing.
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
	cands.candidates = cands.candidates[:j]
	// TODO: see the to do in computeCandidatesForRange about early filtering.
	// If we haven't filtered for overloaded stores, need to do that now by
	// looking at the value of lowestLoad.
	lowestLoad := cands.candidates[0].sls
	for j = 1; j < len(cands.candidates); j++ {
		if cands.candidates[j].sls != lowestLoad {
			break
		}
	}
	cands.candidates = cands.candidates[:j]
	j = rng.Intn(j)
	return cands.candidates[j].StoreID
}

func (a *allocatorState) ensureAnalyzedConstraints(rstate *rangeState) bool {
	if rstate.constraints != nil {
		return true
	}
	// Populate the constraints.
	rac := newRangeAnalyzedConstraints()
	buf := rstate.constraints.stateForInit()
	leaseholder := roachpb.StoreID(-1)
	for _, replica := range rstate.replicas {
		buf.tryAddingStore(replica.StoreID, replica.replicaIDAndType.replicaType.replicaType,
			a.cs.stores[replica.StoreID].localityTiers)
		if replica.isLeaseholder {
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
) candidateSet {
	means := a.meansMemo.getMeans(expr)
	sheddingThreshold := overloadUrgent
	if loadSheddingStore > 0 {
		sheddingSS := a.cs.stores[loadSheddingStore]
		sheddingSLS := a.meansMemo.getStoreLoadSummary(means, loadSheddingStore, sheddingSS.loadSeqNum)
		if sheddingSLS.sls > sheddingThreshold {
			sheddingThreshold = sheddingSLS.sls
		}
		if sheddingSLS.nls > sheddingThreshold {
			sheddingThreshold = sheddingSLS.nls
		}
		if sheddingSLS.sls >= loadNoChange && sheddingSLS.nls >= loadNoChange {
			// In this set of stores, this store no longer looks overloaded.
			return candidateSet{}
		}
	}
	// Not going to try to add to stores that are <= sheddingThreshold.
	//
	// TODO: Early filtering based on sheddingThreshold is not desirable when we
	// are unwilling to reduce diversity. For instance, we are already not
	// filtering below based on maxFractionPending. Figure out which situations
	// are ok with early filtering and only do those here (based on a parameter
	// to computeCandidatesForRange). For others, the only filtering would be
	// storesToExclude.
	var cset candidateSet
	for _, storeID := range means.stores {
		if storesToExclude.contains(storeID) {
			continue
		}
		ss := a.cs.stores[storeID]
		csls := a.meansMemo.getStoreLoadSummary(means, storeID, ss.loadSeqNum)
		if csls.sls <= sheddingThreshold || csls.nls <= sheddingThreshold || csls.fd != fdOK {
			continue
		}
		cset.candidates = append(cset.candidates, candidateInfo{
			StoreID:          storeID,
			storeLoadSummary: csls,
		})
	}
	cset.means = means
	return cset
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
//
// TODO(sumeer): change parameter to replicaLocalityTiers.
func (dsm *diversityScoringMemo) getExistingReplicaLocalities(
	existingReplicas []localityTiers,
) *existingReplicaLocalities {
	lt := makeReplicasLocalityTiers(existingReplicas)
	erl, ok := dsm.replicasMap.get(lt)
	if ok {
		return erl
	}
	erl.replicasLocalityTiers = lt.clone()
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

// TODO: remove once #141945 is merged.
func makeRebalanceReplicaChanges(
	rangeID roachpb.RangeID,
	existingReplicas []storeIDAndReplicaState,
	rLoad rangeLoad,
	addStoreID, removeStoreID roachpb.StoreID,
) [2]*pendingReplicaChange {
	return [2]*pendingReplicaChange{}
}

// TODO: remove once #141945 is merged.
func makeLeaseTransferChanges(
	rangeID roachpb.RangeID,
	existingReplicas []storeIDAndReplicaState,
	rLoad rangeLoad,
	addStoreID, removeStoreID roachpb.StoreID,
) [2]*pendingReplicaChange {
	return [2]*pendingReplicaChange{}
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

var _ = newAllocatorState
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
