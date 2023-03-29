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
	"fmt"
	"strings"

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
}

// Called periodically, say every 10s.
func (a *allocatorState) computeChanges() []*pendingReplicaChange {
	// TODO: rebalancing. To select which stores are overloaded, we will use a
	// notion of overload that is based on cluster means (and of course individual
	// store/node capacities). We do not want to loop through all ranges in the
	// cluster, and for each range and its constraints expression decide whether
	// any of the replica stores is overloaded, since O(num-ranges) work during each
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
	//
	// TODO: discuss with Austen.
	return nil
}

// TODO: what support methods do we need for allocator.tryMovingRange in
// the allocator kernel.

type candidateInfo struct {
	roachpb.StoreID
	sls            loadSummary
	nls            loadSummary
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
//   expression.
//
// - Set of nodes that satisfy constraint expression. Again, we can remember this.
//   We do this also via meansMemo.
//
// - loadSummary for all nodes that satisfy the constraint expression. This
//   may change during rebalancing when changes have been made to node. But
//   can be cached and invalidated. This is done via
//   meansForStoreSet.storeSummaries and the cache invalidation via
//   storeLoadSummary.loadSeqNum.
//
// - Need diversity change for each candidate.
//
// The first 3 bullets are encapsulated in the helper function
// computeCandidatesForRange. It works for both replica additions and rebalancing.
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
	c constraintsDisj, storesToExclude storeIDPostingList, loadSheddingStore roachpb.StoreID,
) candidateSet {
	// The same disjunction will occur many times during an allocator pass.
	// There is no reason to recompute the posting list which is solely a
	// function of the constraints. Additionally, most of the stores will not
	// have any load added or removed, so their loadSummary (scoped to this set
	// of stores) also won't need to be recomputed.
	//
	// TODO: cannot afford allocations here. Fix this to use a hash.
	disjString := c.toString()
	means := a.meansMemo.tryGetMeans(disjString)
	if means == nil {
		var ss storeIDPostingList
		for i := range c {
			var storeSet storeIDPostingList
			a.cs.constraintMatcher.constrainStores(c[i].Constraints, &storeSet)
			if len(storeSet) != 0 {
				if len(ss) == 0 {
					ss = storeSet
				} else {
					ss.union(storeSet)
				}
			}
		}
		means = a.meansMemo.getMeans(ss, a.cs, disjString)
	}
	sheddingThreshold := overloadUrgent
	if loadSheddingStore > 0 {
		sheddingSS := a.cs.stores[loadSheddingStore]
		sheddingSLS, ok := means.tryGetStoreLoadSummary(loadSheddingStore, sheddingSS.loadSeqNum)
		if !ok {
			sls, nls, fdSummary := a.cs.computeLoadSummary(sheddingSS, &means.storeLoad, &means.nodeLoad)
			sheddingSLS = storeLoadSummary{
				sls:        sls,
				nls:        nls,
				fd:         fdSummary,
				loadSeqNum: sheddingSS.loadSeqNum,
			}
			means.putStoreLoadSummary(loadSheddingStore, sheddingSLS)
		}
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
		csls, ok := means.tryGetStoreLoadSummary(storeID, ss.loadSeqNum)
		if !ok {
			sls, nls, fdSummary := a.cs.computeLoadSummary(ss, &means.storeLoad, &means.nodeLoad)
			csls = storeLoadSummary{
				sls:        sls,
				nls:        nls,
				fd:         fdSummary,
				loadSeqNum: ss.loadSeqNum,
			}
			means.putStoreLoadSummary(storeID, csls)
		}
		if csls.sls <= sheddingThreshold || csls.nls <= sheddingThreshold || csls.fd != fdOK {
			continue
		}
		cset.candidates = append(cset.candidates, candidateInfo{
			StoreID: storeID,
			sls:     csls.sls,
			nls:     csls.nls,
		})
	}
	return cset
}

/*

TODO: cleanup this comment

- Diversity scoring stuff:

  https://github.com/cockroachdb/cockroach/blob/50a4f1705ad14bce86ba4f95e9ed5193e7f2f91a/pkg/kv/kvserver/allocator/aimpl/load.go#L1028

  The scoring in current code is driven by getReplicasForDiversityCalc, which
  for non-voters looks at all replicas and for voters at existing voters.

  Cases:
  - Adding a new voter: doesn't matter if non-voter or completely new replica,
    since only looking at existing voter

  - Adding a new non-voter:
    *- existing voter: need to do something special. Well, there is no change in diversity, so trivial.
    - not existing replica: fine
  - Rebalancing a voter
  - Rebalancing a non-voter:
*/

type existingReplicaLocalities struct {
	// replicas are sorted in ascending order of localityTiers.str.
	replicas []localityTiers
	// String representation of replicas.
	key string

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

type diversityScoringMemo struct {
	replicasMap map[string]*existingReplicaLocalities
}

func (dsm *diversityScoringMemo) getExistingReplicaLocalities(
	existingReplicas []localityTiers,
) *existingReplicaLocalities {
	// TODO: using replicaTiersToString will be costly due to allocations. Use a
	// hash and equality comparison. See the TODO elsewhere for using Go
	// generics to create a map that can use a hash and equality.
	return nil
}

// If this is a NON_VOTER being added, then existingReplicaLocalities
// represents VOTER and NON_VOTER replicas and replicaToAdd is not an existing
// VOTER.
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

func replicaTiersToString(existingReplicas []localityTiers) string {
	var b strings.Builder
	for i := range existingReplicas {
		fmt.Fprintf(&b, "%s-", existingReplicas[i].str)
	}
	return b.String()
}
