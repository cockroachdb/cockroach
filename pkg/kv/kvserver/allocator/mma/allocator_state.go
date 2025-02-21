// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mma

import (
	"cmp"
	"slices"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

	// TODO(kvoli,sumeer): initialize and use.
	changeRangeLimiter *storeChangeRateLimiter
}

func newAllocatorState() *allocatorState {
	interner := newStringInterner()
	cs := newClusterState(interner)
	return &allocatorState{
		cs:                     cs,
		rangesNeedingAttention: map[roachpb.RangeID]struct{}{},
		meansMemo:              newMeansMemo(cs, cs.constraintMatcher),
		diversityScoringMemo:   newDiversityScoringMemo(),
	}
}

// Called periodically, say every 10s.
func (a *allocatorState) computeChanges() []*pendingReplicaChange {
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
	return nil
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

type candidateSet struct {
	candidates []candidateInfo
	means      *meansForStoreSet
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

// Avoid unused lint errors.

var _ = newAllocatorState
var _ = (&allocatorState{}).computeChanges
var _ = (&allocatorState{}).computeCandidatesForRange
var _ = allocatorState{}.changeRangeLimiter
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
