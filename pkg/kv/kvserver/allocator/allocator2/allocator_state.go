// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package allocator2

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type allocatorState struct {
	cs *clusterState

	// Ranges that are under-replicated, over-replicated, don't satisfy
	// constraints, have low diversity etc. Avoids iterating through all ranges.
	// A range is removed from this map if it has pending changes -- when those
	// pending changes go away it gets added back so we can check on it.
	rangesNeedingAttention map[roachpb.RangeID]struct{}

	meansMemo            *meansMemo
	diversityScoringMemo *diversityScoringMemo
}

func newAllocatorState(clock *hlc.Clock) *allocatorState {
	interner := newStringInterner()
	cs := newClusterState(clock, interner)
	return &allocatorState{
		cs:                     cs,
		rangesNeedingAttention: map[roachpb.RangeID]struct{}{},
		meansMemo:              newMeansMemo(cs, cs.constraintMatcher),
		diversityScoringMemo:   newDiversityScoringMemo(),
	}
}

// Called periodically, say every 10s.
func (a *allocatorState) computeChanges(ctx context.Context) []*pendingReplicaChange {
	// TODO(sumeer): rebalancing. To select which stores are overloaded, we will
	// use a notion of overload that is based on cluster means (and of course
	// individual store/node capacities). We do not want to loop through all
	// ranges in the cluster, and for each range and its constraints expression
	// decide whether any of the replica stores is overloaded, since
	// O(num-ranges) work during each allocator pass is not scalable.
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
	return a.rebalanceStores(ctx)
}

// TODO(sumeer): look at support methods for allocatorState.tryMovingRange in
// the allocator kernel draft PR.
type candidateInfo struct {
	roachpb.StoreID
	sls            loadSummary
	nls            loadSummary
	fd             failureDetectionSummary
	diversityScore float64
}

func (ci candidateInfo) String() string {
	if ci.StoreID > 0 {
		return fmt.Sprintf("%v: (store=%v node=%v failure=%v diversity-score=%.2f)",
			ci.StoreID, ci.sls, ci.nls, ci.fd, ci.diversityScore)
	}
	return "none"
}

type candidateSet struct {
	candidates []candidateInfo
	means      *meansForStoreSet
}

func (cs candidateSet) String() string {
	return fmt.Sprintf("%v %v", cs.means, cs.candidates)
}

// orderByDiversityDescAndLoadAsc orders the candidateSet.candidates first by
// diversity, then on ties by the node load summary, then again on ties by
// store load summary.
func (cs candidateSet) orderByDiversityDescAndLoadAsc() {
	sort.SliceStable(cs.candidates, func(i, j int) bool {
		if cs.candidates[i].diversityScore == cs.candidates[j].diversityScore {
			if cs.candidates[i].nls == cs.candidates[j].nls {
				return cs.candidates[i].sls < cs.candidates[j].sls
			}
			return cs.candidates[i].nls < cs.candidates[j].nls
		}
		return cs.candidates[i].diversityScore > cs.candidates[j].diversityScore
	})
}

func (a *allocatorState) rebalanceStores(ctx context.Context) []*pendingReplicaChange {
	var changes []*pendingReplicaChange
	var sourceStores, targetStores []roachpb.StoreID

	// Get the means for all stores and nodes we know of.
	means := a.meansMemo.getMeans(nil)
	sheddingThreshold := overloadSlow
	// Find the stores which have too much load, these will be the source stores
	// to rebalance load away from. The remaining stores do not have to much
	// load, these will be the target stores to rebalance load towards.
	for _, storeID := range a.cs.storeList {
		sls := a.meansMemo.getStoreLoadSummary(means, storeID, todoLoadSeqNum)
		if sls.sls >= sheddingThreshold {
			sourceStores = append(sourceStores, storeID)
		} else {
			targetStores = append(targetStores, storeID)
		}
	}
	for _, storeID := range sourceStores {
		if len(targetStores) == 0 {
			break
		}
		for _, rLoad := range a.cs.stores[storeID].topKRanges {
			a.tryMovingRange(ctx, storeID, rLoad, targetStores, &changes)
			sls := a.meansMemo.getStoreLoadSummary(means, storeID, todoLoadSeqNum)
			if sls.sls >= loadNoChange || len(targetStores) == 0 {
				// Done with this store.
				break
			}
		}
	}
	return changes
}

func (a *allocatorState) tryMovingRange(
	ctx context.Context,
	storeID roachpb.StoreID,
	rLoad rangeLoad,
	targetStores storeIDPostingList,
	changes *[]*pendingReplicaChange,
) {
	repl := a.cs.stores[storeID].adjusted.replicas[rLoad.RangeID]
	if repl.replicaType.isLeaseholder {
		// Try and move the lease first, if the store's replica has the lease -
		// this is generally cheaper than rebalancing replicas.
		if target, err := a.rebalanceLeaseTarget(ctx, storeID, rLoad, targetStores); err != nil {
			log.VEventf(ctx, 2,
				"error finding lease rebalance for store %d range %d err=%v",
				storeID, rLoad.RangeID, err)
		} else if target.StoreID != 0 {
			transferReplicaChanges := makeLeaseTransferChanges(
				rLoad, a.cs.ranges[rLoad.RangeID], target.StoreID, storeID,
			)
			log.Infof(ctx, "found move: transfer lease %+v", transferReplicaChanges)
			pendingChanges := a.cs.makePendingChanges(
				rLoad.RangeID,
				transferReplicaChanges[:],
			)
			*changes = append(*changes, pendingChanges...)
			return
		}
		// Fall through to try replica rebalancing.
	}
	if target, err := a.rebalanceReplicaTarget(ctx, storeID, rLoad, targetStores); err != nil {
		log.VEventf(ctx, 2,
			"error finding replica rebalance for store %d range %d err=%v",
			storeID, rLoad.RangeID, err)
	} else if target.StoreID != 0 {
		rebalanceReplicaChanges := makeRebalanceReplicaChanges(
			rLoad, a.cs.ranges[rLoad.RangeID], target.StoreID, storeID,
		)
		log.Infof(ctx, "found move: rebalance replicas %+v", rebalanceReplicaChanges)
		pendingChanges := a.cs.makePendingChanges(
			rLoad.RangeID,
			rebalanceReplicaChanges[:],
		)
		*changes = append(*changes, pendingChanges...)
		return
	}
}

func (a *allocatorState) rebalanceLeaseTarget(
	ctx context.Context,
	loadSheddingStore roachpb.StoreID,
	rLoad rangeLoad,
	targetStores storeIDPostingList,
) (candidateInfo, error) {
	rState := a.cs.ranges[rLoad.RangeID]
	a.ensureAnalyzedConstraints(rLoad.RangeID, rState)
	// The lease will only ever be transferred to a voter.
	targetStores.intersect(rState.replicas.voters().stores())

	pickCandidate := func(cs candidateSet) candidateInfo {
		cs.orderByDiversityDescAndLoadAsc()
		for _, candidate := range cs.candidates {
			if targetStores.contains(candidate.StoreID) {
				return candidate
			}
		}
		return candidateInfo{}
	}
	// Find the first preference which has at least 1 candidate.
	var picked candidateInfo
	for _, preference := range rState.conf.leasePreferences {
		candidates := a.computeCandidatesForRange(
			constraintsDisj{preference.constraints},
			targetStores,
			nil,
			loadSheddingStore,
		)
		if picked = pickCandidate(candidates); picked.StoreID != 0 {
			break
		}
	}
	// There were no candidates which could satisfy the lease preference. Try one
	// more time, ignoring the lease preference.
	if picked.StoreID == 0 {
		candidates := a.computeCandidatesForRange(nil, nil, nil, loadSheddingStore)
		picked = pickCandidate(candidates)
	}
	return picked, nil
}

func (a *allocatorState) rebalanceReplicaTarget(
	ctx context.Context,
	loadSheddingStore roachpb.StoreID,
	rLoad rangeLoad,
	targetStores storeIDPostingList,
) (candidateInfo, error) {
	rState := a.cs.ranges[rLoad.RangeID]
	a.ensureAnalyzedConstraints(rLoad.RangeID, rState)
	existingVoters := rState.replicas.voters().stores()
	existingNonVoters := rState.replicas.nonVoters().stores()
	existing := rState.replicas.stores()
	// TODO(kvoli): Bug with clone().
	// existing := existingVoters.clone()
	// existing.union(existingNonVoters)
	// existing.union(existingVoters)
	var toReplace constraintsConj
	var err error
	if existingVoters.contains(loadSheddingStore) {
		toReplace, err = rState.constraints.candidatesToReplaceVoterForRebalance(loadSheddingStore)
	} else if existingNonVoters.contains(loadSheddingStore) {
		toReplace, err = rState.constraints.candidatesToReplaceNonVoterForRebalance(loadSheddingStore)
	} else {
		// The range doesn't have a voter or non-voter on the shedding store. Skip
		// the replica rebalance, nothing to do.
		return candidateInfo{}, nil
	}
	if err != nil {
		return candidateInfo{}, err
	}
	log.Infof(ctx, "replace conjunction %+v, existing=%v", toReplace, existing)
	// Exclude the existing replica stores as rebalance target candidates.
	candSet := a.computeCandidatesForRange(
		constraintsDisj{toReplace},
		existing, /* storesToExclude */
		targetStores,
		loadSheddingStore,
	)
	log.Infof(ctx, "candidates=%v", candSet.candidates)
	if len(candSet.candidates) < 1 {
		return candidateInfo{}, nil
	}
	// Populate the diveristy score for each candidate.
	existingLocalities := a.diversityScoringMemo.getExistingReplicaLocalities(
		a.cs.getStoreLocalities(existing...),
	)
	removeLocality := a.cs.getStoreLocalities(loadSheddingStore)[0]
	for i := range candSet.candidates {
		addLocality := a.cs.getStoreLocalities(candSet.candidates[i].StoreID)[0]
		diversityRebalanceScore := existingLocalities.getScoreChangeForRebalance(
			removeLocality,
			addLocality,
		)
		candSet.candidates[i].diversityScore = diversityRebalanceScore
	}
	candSet.orderByDiversityDescAndLoadAsc()
	return candSet.candidates[0], nil
}

func (a *allocatorState) ensureAnalyzedConstraints(
	rangeID roachpb.RangeID, rangeState *rangeState,
) {
	if rangeState.constraints != nil {
		return
	}
	rac := rangeAnalyzedConstraintsPool.Get().(*rangeAnalyzedConstraints)
	buf := rac.stateForInit()
	for _, repl := range rangeState.replicas {
		tiers := a.cs.stores[repl.StoreID].localityTiers
		buf.tryAddingStore(repl.StoreID, repl.replicaType.replicaType, tiers)
	}
	rac.finishInit(rangeState.conf, a.cs.constraintMatcher)
	rangeState.constraints = rac
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
// targets is specified if this candidate computation should only select
// candidates that are also in targets.
func (a *allocatorState) computeCandidatesForRange(
	expr constraintsDisj,
	storesToExclude, targets storeIDPostingList,
	loadSheddingStore roachpb.StoreID,
) candidateSet {
	means := a.meansMemo.getMeans(expr)
	log.Infof(context.Background(), "%v", means)

	if means == nil {
		return candidateSet{}
	}

	nodeSheddingThreshold := overloadUrgent
	storeSheddingThreshold := overloadUrgent
	if loadSheddingStore > 0 {
		sheddingSS := a.cs.stores[loadSheddingStore]
		sheddingSLS := a.meansMemo.getStoreLoadSummary(means, loadSheddingStore, sheddingSS.loadSeqNum)
		log.Infof(context.Background(), "shedding store %d summary=%v",
			loadSheddingStore,
			sheddingSLS,
		)
		if sheddingSLS.nls > nodeSheddingThreshold {
			nodeSheddingThreshold = sheddingSLS.nls
		}
		if sheddingSLS.sls > storeSheddingThreshold {
			storeSheddingThreshold = sheddingSLS.sls
		}
		if sheddingSLS.sls >= loadNoChange && sheddingSLS.nls >= loadNoChange {
			// In this set of stores, this store no longer looks overloaded.
			log.Infof(context.Background(), "store %d no longer appears overloaded", loadSheddingStore)
			return candidateSet{}
		}
	}
	// Not going to try to add to stores that are <= sheddingThreshold.
	var cset candidateSet
	for _, storeID := range means.stores {
		if storesToExclude.contains(storeID) {
			continue
		}
		if targets != nil && !targets.contains(storeID) {
			continue
		}
		ss := a.cs.stores[storeID]
		csls := a.meansMemo.getStoreLoadSummary(means, storeID, ss.loadSeqNum)
		if (csls.sls <= storeSheddingThreshold && csls.nls <= nodeSheddingThreshold) || csls.fd != fdOK {
			log.Infof(context.Background(), "store %d doesn't meet criteria to be added %v shredding-threshold: store=%v node=%v",
				storeID, csls, storeSheddingThreshold, nodeSheddingThreshold)
			continue
		}
		cset.candidates = append(cset.candidates, candidateInfo{
			StoreID: storeID,
			sls:     csls.sls,
			nls:     csls.nls,
			fd:      csls.fd,
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

type replicasLocalityTiers struct {
	// replicas are sorted in ascending order of localityTiers.str.
	replicas []localityTiers
}

func makeReplicasLocalityTiers(replicas []localityTiers) replicasLocalityTiers {
	// TODO(sumeer): remember to sort.
	return replicasLocalityTiers{}
}

func (rlt replicasLocalityTiers) hash() uint64 {
	// TODO(sumeer):
	return 0
}

func (rlt replicasLocalityTiers) isEqual(b mapKey) bool {
	// TODO(sumeer):
	return false
}

var _ mapKey = replicasLocalityTiers{}

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

type diversityScoringMemo struct {
	replicasMap *clearableMemoMap[replicasLocalityTiers, *existingReplicaLocalities]
}

func newDiversityScoringMemo() *diversityScoringMemo {
	return &diversityScoringMemo{
		replicasMap: newClearableMapMemo[replicasLocalityTiers, *existingReplicaLocalities](
			existingReplicaLocalitiesAllocator{}, existingReplicaLocalitiesSlicePoolImpl{}),
	}
}

func (dsm *diversityScoringMemo) getExistingReplicaLocalities(
	existingReplicas []localityTiers,
) *existingReplicaLocalities {
	lt := makeReplicasLocalityTiers(existingReplicas)
	erl, ok := dsm.replicasMap.get(lt)
	if ok {
		return erl
	}
	erl.replicasLocalityTiers = lt
	erl.scoreSums = map[string]float64{}
	return erl
}

// If this is a NON_VOTER being added, then existingReplicaLocalities
// represents VOTER and NON_VOTER replicas and replicaToAdd is not an existing
// VOTER. For the case where replicaToAdd is an existing VOTER, there is no
// score change.
//
// If this is a VOTER being added, then existingReplicaLocalities represents
// VOTER replicas.
func (erl *existingReplicaLocalities) getScoreChangeForNewReplica(
	replicaToAdd localityTiers,
) float64 {
	return erl.getScoreSum(replicaToAdd)
}

func (erl *existingReplicaLocalities) getScoreChangeForReplicaRemoval(
	replicaToRemove localityTiers,
) float64 {
	return -erl.getScoreSum(replicaToRemove)
}

// If this is a VOTER being added and removed, it doesn't matter if the one
// being added is already a NON_VOTER.
//
// If this a NON_VOTER being added and removed, the replicaToAdd is not an
// existing VOTER. For the case where replicaToAdd is an existing VOTER, there
// is no score change for the addition, and
// getScoreChangeForReplicaRemoval(replicaToRemove) should be called (if
// replicaToRemove is not becoming a VOTER).
//
// TODO(sumeer): these interfaces probably need reworking when we start
// using them.
func (erl *existingReplicaLocalities) getScoreChangeForRebalance(
	replicaToRemove localityTiers, replicaToAdd localityTiers,
) float64 {
	score := erl.getScoreSum(replicaToAdd) - erl.getScoreSum(replicaToRemove)
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

// Avoid unused lint errors.
var _ = newAllocatorState
var _ = (&allocatorState{}).computeChanges
var _ = (&allocatorState{}).computeCandidatesForRange
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
