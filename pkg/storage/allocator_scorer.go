// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"bytes"
	"fmt"
	"math"
	"sort"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const (
	// The number of random candidates to select from a larger list of possible
	// candidates. Because the allocator heuristics are being run on every node it
	// is actually not desirable to set this value higher. Doing so can lead to
	// situations where the allocator determistically selects the "best" node for a
	// decision and all of the nodes pile on allocations to that node. See "power
	// of two random choices":
	// https://brooker.co.za/blog/2012/01/17/two-random.html and
	// https://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf.
	allocatorRandomCount = 2
)

type balanceDimensions struct {
	ranges rangeCountStatus
	bytes  float64
	writes float64
}

func (bd *balanceDimensions) totalScore() float64 {
	return float64(bd.ranges) + bd.bytes + bd.writes
}

func (bd balanceDimensions) String() string {
	return fmt.Sprintf("%.2f(ranges=%d, bytes=%.2f, writes=%.2f)",
		bd.totalScore(), int(bd.ranges), bd.bytes, bd.writes)
}

// candidate store for allocation.
type candidate struct {
	store           roachpb.StoreDescriptor
	valid           bool
	constraintScore float64
	convergesScore  int
	balanceScore    balanceDimensions
	rangeCount      int
	details         string
}

func (c candidate) String() string {
	return fmt.Sprintf("s%d, valid:%t, constraint:%.2f, converges:%d, balance:%s, rangeCount:%d, details:(%s)",
		c.store.StoreID, c.valid, c.constraintScore, c.convergesScore, c.balanceScore, c.rangeCount, c.details)
}

// less returns true if o is a better fit for some range than c is.
func (c candidate) less(o candidate) bool {
	if !o.valid {
		return false
	}
	if !c.valid {
		return true
	}
	if c.constraintScore != o.constraintScore {
		return c.constraintScore < o.constraintScore
	}
	if c.convergesScore != o.convergesScore {
		return c.convergesScore < o.convergesScore
	}
	if c.balanceScore.totalScore() != o.balanceScore.totalScore() {
		return c.balanceScore.totalScore() < o.balanceScore.totalScore()
	}
	return c.rangeCount > o.rangeCount
}

type candidateList []candidate

func (cl candidateList) String() string {
	if len(cl) == 0 {
		return "[]"
	}
	var buffer bytes.Buffer
	buffer.WriteRune('[')
	for _, c := range cl {
		buffer.WriteRune('\n')
		buffer.WriteString(c.String())
	}
	buffer.WriteRune(']')
	return buffer.String()
}

// byScore implements sort.Interface to sort by scores.
type byScore candidateList

var _ sort.Interface = byScore(nil)

func (c byScore) Len() int           { return len(c) }
func (c byScore) Less(i, j int) bool { return c[i].less(c[j]) }
func (c byScore) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

// byScoreAndID implements sort.Interface to sort by scores and ids.
type byScoreAndID candidateList

var _ sort.Interface = byScoreAndID(nil)

func (c byScoreAndID) Len() int { return len(c) }
func (c byScoreAndID) Less(i, j int) bool {
	if c[i].constraintScore == c[j].constraintScore &&
		c[i].convergesScore == c[j].convergesScore &&
		c[i].balanceScore.totalScore() == c[j].balanceScore.totalScore() &&
		c[i].rangeCount == c[j].rangeCount &&
		c[i].valid == c[j].valid {
		return c[i].store.StoreID < c[j].store.StoreID
	}
	return c[i].less(c[j])
}
func (c byScoreAndID) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

// onlyValid returns all the elements in a sorted (by score reversed) candidate
// list that are valid.
func (cl candidateList) onlyValid() candidateList {
	for i := len(cl) - 1; i >= 0; i-- {
		if cl[i].valid {
			return cl[:i+1]
		}
	}
	return candidateList{}
}

// best returns all the elements in a sorted (by score reversed) candidate list
// that share the highest constraint score and are valid.
func (cl candidateList) best() candidateList {
	cl = cl.onlyValid()
	if len(cl) <= 1 {
		return cl
	}
	for i := 1; i < len(cl); i++ {
		if cl[i].constraintScore < cl[0].constraintScore ||
			(cl[i].constraintScore == cl[len(cl)-1].constraintScore &&
				cl[i].convergesScore < cl[len(cl)-1].convergesScore) {
			return cl[:i]
		}
	}
	return cl
}

// worst returns all the elements in a sorted (by score reversed) candidate
// list that share the lowest constraint score.
func (cl candidateList) worst() candidateList {
	if len(cl) <= 1 {
		return cl
	}
	// Are there invalid candidates? If so, pick those.
	if !cl[len(cl)-1].valid {
		for i := len(cl) - 2; i >= 0; i-- {
			if cl[i].valid {
				return cl[i+1:]
			}
		}
	}
	// Find the worst constraint values.
	for i := len(cl) - 2; i >= 0; i-- {
		if cl[i].constraintScore > cl[len(cl)-1].constraintScore ||
			(cl[i].constraintScore == cl[len(cl)-1].constraintScore &&
				cl[i].convergesScore > cl[len(cl)-1].convergesScore) {
			return cl[i+1:]
		}
	}
	return cl
}

// betterThan returns all elements from a sorted (by score reversed) candidate
// list that have a higher score than the candidate
func (cl candidateList) betterThan(c candidate) candidateList {
	for i := 0; i < len(cl); i++ {
		if !c.less(cl[i]) {
			return cl[:i]
		}
	}
	return cl
}

// selectGood randomly chooses a good candidate store from a sorted (by score
// reversed) candidate list using the provided random generator.
func (cl candidateList) selectGood(randGen allocatorRand) *roachpb.StoreDescriptor {
	if len(cl) == 0 {
		return nil
	}
	cl = cl.best()
	if len(cl) == 1 {
		return &cl[0].store
	}
	randGen.Lock()
	order := randGen.Perm(len(cl))
	randGen.Unlock()
	best := &cl[order[0]]
	for i := 1; i < allocatorRandomCount; i++ {
		if best.less(cl[order[i]]) {
			best = &cl[order[i]]
		}
	}
	return &best.store
}

// selectBad randomly chooses a bad candidate store from a sorted (by score
// reversed) candidate list using the provided random generator.
func (cl candidateList) selectBad(randGen allocatorRand) *roachpb.StoreDescriptor {
	if len(cl) == 0 {
		return nil
	}
	cl = cl.worst()
	if len(cl) == 1 {
		return &cl[0].store
	}
	randGen.Lock()
	order := randGen.Perm(len(cl))
	randGen.Unlock()
	worst := &cl[order[0]]
	for i := 1; i < allocatorRandomCount; i++ {
		if cl[order[i]].less(*worst) {
			worst = &cl[order[i]]
		}
	}
	return &worst.store
}

// allocateCandidates creates a candidate list of all stores that can be used
// for allocating a new replica ordered from the best to the worst. Only
// stores that meet the criteria are included in the list.
func allocateCandidates(
	st *cluster.Settings,
	sl StoreList,
	constraints config.Constraints,
	existing []roachpb.ReplicaDescriptor,
	rangeInfo RangeInfo,
	existingNodeLocalities map[roachpb.NodeID]roachpb.Locality,
	deterministic bool,
) candidateList {
	var candidates candidateList
	for _, s := range sl.stores {
		if !preexistingReplicaCheck(s.Node.NodeID, existing) {
			continue
		}
		constraintsOk, preferredMatched := constraintCheck(s, constraints)
		if !constraintsOk {
			continue
		}
		if !maxCapacityCheck(s) {
			continue
		}
		diversityScore := diversityScore(s, existingNodeLocalities)
		balanceScore := balanceScore(st, sl, s.Capacity, rangeInfo)
		candidates = append(candidates, candidate{
			store:           s,
			valid:           true,
			constraintScore: diversityScore + float64(preferredMatched),
			balanceScore:    balanceScore,
			rangeCount:      int(s.Capacity.RangeCount),
			details:         fmt.Sprintf("diversity=%.2f, preferred=%d", diversityScore, preferredMatched),
		})
	}
	if deterministic {
		sort.Sort(sort.Reverse(byScoreAndID(candidates)))
	} else {
		sort.Sort(sort.Reverse(byScore(candidates)))
	}
	return candidates
}

// removeCandidates creates a candidate list of all existing replicas' stores
// ordered from least qualified for removal to most qualified. Stores that are
// marked as not valid, are in violation of a required criteria.
func removeCandidates(
	st *cluster.Settings,
	sl StoreList,
	constraints config.Constraints,
	rangeInfo RangeInfo,
	existingNodeLocalities map[roachpb.NodeID]roachpb.Locality,
	deterministic bool,
) candidateList {
	var candidates candidateList
	for _, s := range sl.stores {
		constraintsOk, preferredMatched := constraintCheck(s, constraints)
		if !constraintsOk {
			candidates = append(candidates, candidate{
				store:   s,
				valid:   false,
				details: "constraint check fail",
			})
			continue
		}
		if !maxCapacityCheck(s) {
			candidates = append(candidates, candidate{
				store:   s,
				valid:   false,
				details: "max capacity check fail",
			})
			continue
		}
		diversityScore := diversityRemovalScore(s.Node.NodeID, existingNodeLocalities)
		balanceScore := balanceScore(st, sl, s.Capacity, rangeInfo)
		var convergesScore int
		if !rebalanceFromConvergesOnMean(st, sl, s.Capacity, rangeInfo) {
			// If removing this candidate replica does not converge the store
			// stats to their means, we make it less attractive for removal by
			// adding 1 to the constraint score. Note that when selecting a
			// candidate for removal the candidates with the lowest scores are
			// more likely to be removed.
			convergesScore = 1
		}
		candidates = append(candidates, candidate{
			store:           s,
			valid:           true,
			constraintScore: diversityScore + float64(preferredMatched),
			convergesScore:  convergesScore,
			balanceScore:    balanceScore,
			rangeCount:      int(s.Capacity.RangeCount),
			details:         fmt.Sprintf("diversity=%.2f, preferred=%d", diversityScore, preferredMatched),
		})
	}
	if deterministic {
		sort.Sort(sort.Reverse(byScoreAndID(candidates)))
	} else {
		sort.Sort(sort.Reverse(byScore(candidates)))
	}
	return candidates
}

// rebalanceCandidates creates two candidate list. The first contains all
// existing replica's stores, order from least qualified for rebalancing to
// most qualified. The second list is of all potential stores that could be
// used as rebalancing receivers, ordered from best to worst.
func rebalanceCandidates(
	ctx context.Context,
	st *cluster.Settings,
	sl StoreList,
	constraints config.Constraints,
	existing []roachpb.ReplicaDescriptor,
	rangeInfo RangeInfo,
	existingNodeLocalities map[roachpb.NodeID]roachpb.Locality,
	deterministic bool,
) (candidateList, candidateList) {
	// Load the exiting storesIDs into a map to eliminate having to loop
	// through the existing descriptors more than once.
	existingStoreIDs := make(map[roachpb.StoreID]struct{})
	for _, repl := range existing {
		existingStoreIDs[repl.StoreID] = struct{}{}
	}

	// Go through all the stores and find all that match the constraints so that
	// we can have accurate stats for rebalance calculations.
	var constraintsOkStoreDescriptors []roachpb.StoreDescriptor

	type constraintInfo struct {
		ok      bool
		matched int
	}
	storeInfos := make(map[roachpb.StoreID]constraintInfo)
	var rebalanceConstraintsCheck bool
	for _, s := range sl.stores {
		constraintsOk, preferredMatched := constraintCheck(s, constraints)
		storeInfos[s.StoreID] = constraintInfo{ok: constraintsOk, matched: preferredMatched}
		_, exists := existingStoreIDs[s.StoreID]
		if constraintsOk {
			constraintsOkStoreDescriptors = append(constraintsOkStoreDescriptors, s)
		} else if exists {
			rebalanceConstraintsCheck = true
			if log.V(2) {
				log.Infof(ctx, "must rebalance from s%d due to constraint check", s.StoreID)
			}
		}
	}

	constraintsOkStoreList := makeStoreList(constraintsOkStoreDescriptors)
	var shouldRebalanceCheck bool
	if !rebalanceConstraintsCheck {
		for _, store := range sl.stores {
			if _, ok := existingStoreIDs[store.StoreID]; ok {
				if shouldRebalance(ctx, st, store, constraintsOkStoreList, rangeInfo) {
					shouldRebalanceCheck = true
					break
				}
			}
		}
	}

	// Only rebalance away if the constraints don't match or shouldRebalance
	// indicated that we should consider moving the range away from one of its
	// existing stores.
	if !rebalanceConstraintsCheck && !shouldRebalanceCheck {
		return nil, nil
	}

	var existingCandidates candidateList
	var candidates candidateList
	for _, s := range sl.stores {
		storeInfo := storeInfos[s.StoreID]
		maxCapacityOK := maxCapacityCheck(s)
		if _, ok := existingStoreIDs[s.StoreID]; ok {
			if !storeInfo.ok {
				existingCandidates = append(existingCandidates, candidate{
					store:   s,
					valid:   false,
					details: "constraint check fail",
				})
				continue
			}
			if !maxCapacityOK {
				existingCandidates = append(existingCandidates, candidate{
					store:   s,
					valid:   false,
					details: "max capacity check fail",
				})
				continue
			}
			diversityScore := diversityRemovalScore(s.Node.NodeID, existingNodeLocalities)
			balanceScore := balanceScore(st, sl, s.Capacity, rangeInfo)
			var convergesScore int
			if !rebalanceFromConvergesOnMean(st, sl, s.Capacity, rangeInfo) {
				// Similarly to in removeCandidates, any replica whose removal
				// would not converge the range stats to their means is given a
				// constraint score boost of 1 to make it less attractive for
				// removal.
				convergesScore = 1
			}
			existingCandidates = append(existingCandidates, candidate{
				store:           s,
				valid:           true,
				constraintScore: diversityScore + float64(storeInfo.matched),
				convergesScore:  convergesScore,
				balanceScore:    balanceScore,
				rangeCount:      int(s.Capacity.RangeCount),
				details:         fmt.Sprintf("diversity=%.2f, preferred=%d", diversityScore, storeInfo.matched),
			})
		} else {
			if !storeInfo.ok || !maxCapacityOK {
				continue
			}
			balanceScore := balanceScore(st, sl, s.Capacity, rangeInfo)
			var convergesScore int
			if rebalanceToConvergesOnMean(st, sl, s.Capacity, rangeInfo) {
				// This is the counterpart of !rebalanceFromConvergesOnMean from
				// the existing candidates. Candidates whose addition would
				// converge towards the range count mean are promoted.
				convergesScore = 1
			} else if !rebalanceConstraintsCheck {
				// Only consider this candidate if we must rebalance due to a
				// constraint check requirements.
				if log.V(3) {
					log.Infof(ctx, "not considering %+v as a candidate for range %+v: score=%s scoreList=%+v", s, rangeInfo, balanceScore, sl)
				}
				continue
			}
			diversityScore := diversityScore(s, existingNodeLocalities)
			candidates = append(candidates, candidate{
				store:           s,
				valid:           true,
				constraintScore: diversityScore + float64(storeInfo.matched),
				convergesScore:  convergesScore,
				balanceScore:    balanceScore,
				rangeCount:      int(s.Capacity.RangeCount),
				details:         fmt.Sprintf("diversity=%.2f, preferred=%d", diversityScore, storeInfo.matched),
			})
		}
	}

	if deterministic {
		sort.Sort(sort.Reverse(byScoreAndID(existingCandidates)))
		sort.Sort(sort.Reverse(byScoreAndID(candidates)))
	} else {
		sort.Sort(sort.Reverse(byScore(existingCandidates)))
		sort.Sort(sort.Reverse(byScore(candidates)))
	}

	return existingCandidates, candidates
}

// shouldRebalance returns whether the specified store is a candidate for
// having a replica removed from it given the candidate store list.
func shouldRebalance(
	ctx context.Context,
	st *cluster.Settings,
	store roachpb.StoreDescriptor,
	sl StoreList,
	rangeInfo RangeInfo,
) bool {
	if store.Capacity.FractionUsed() >= maxFractionUsedThreshold {
		if log.V(2) {
			log.Infof(ctx, "s%d: should-rebalance(disk-full): fraction-used=%.2f, capacity=%+v",
				store.StoreID, store.Capacity.FractionUsed(), store.Capacity)
		}
		return true
	}

	if !st.EnableStatsBasedRebalancing.Get() {
		return shouldRebalanceNoStats(ctx, st, store, sl)
	}

	// Rebalance if this store is full enough that the range is a bad fit.
	score := balanceScore(st, sl, store.Capacity, rangeInfo)
	if rangeIsBadFit(score) {
		if log.V(2) {
			log.Infof(ctx,
				"s%d: should-rebalance(bad-fit): - balanceScore=%s, capacity=%+v, rangeInfo=%+v, "+
					"(meanRangeCount=%.1f, meanDiskUsage=%.2f, meanWritesPerSecond=%.2f), ",
				store.StoreID, score, store.Capacity, rangeInfo,
				sl.candidateRanges.mean, sl.candidateDiskUsage.mean, sl.candidateWritesPerSecond.mean)
		}
		return true
	}

	// Rebalance if there exists another store that is very in need of the
	// range and this store is a somewhat bad match for it.
	if rangeIsPoorFit(score) {
		for _, desc := range sl.stores {
			otherScore := balanceScore(st, sl, desc.Capacity, rangeInfo)
			if !rangeIsGoodFit(otherScore) {
				continue
			}
			if !preexistingReplicaCheck(desc.Node.NodeID, rangeInfo.Desc.Replicas) {
				continue
			}
			if log.V(2) {
				log.Infof(ctx,
					"s%d: should-rebalance(better-fit=s%d): balanceScore=%s, capacity=%+v, rangeInfo=%+v, "+
						"otherScore=%s, otherCapacity=%+v, "+
						"(meanRangeCount=%.1f, meanDiskUsage=%.2f, meanWritesPerSecond=%.2f), ",
					store.StoreID, desc.StoreID, score, store.Capacity, rangeInfo,
					otherScore, desc.Capacity,
					sl.candidateRanges.mean, sl.candidateDiskUsage.mean, sl.candidateWritesPerSecond.mean)
			}
			return true
		}
	}

	// If we reached this point, we're happy with the range where it is.
	if log.V(3) {
		log.Infof(ctx,
			"s%d: should-not-rebalance: - balanceScore=%s, capacity=%+v, rangeInfo=%+v, "+
				"(meanRangeCount=%.1f, meanDiskUsage=%.2f, meanWritesPerSecond=%.2f), ",
			store.StoreID, score, store.Capacity, rangeInfo,
			sl.candidateRanges.mean, sl.candidateDiskUsage.mean, sl.candidateWritesPerSecond.mean)
	}
	return false
}

// shouldRebalance implements the decision of whether to rebalance for the case
// when EnableStatsBasedRebalancing is disabled and decisions should thus be
// made based only on range counts.
func shouldRebalanceNoStats(
	ctx context.Context, st *cluster.Settings, store roachpb.StoreDescriptor, sl StoreList,
) bool {
	overfullThreshold := int32(math.Ceil(overfullRangeThreshold(st, sl.candidateRanges.mean)))
	if store.Capacity.RangeCount > overfullThreshold {
		log.Infof(ctx,
			"s%d: should-rebalance(ranges-overfull): rangeCount=%d, mean=%.2f, overfull-threshold=%d",
			store.StoreID, store.Capacity.RangeCount, sl.candidateRanges.mean, overfullThreshold)
		return true
	}

	if float64(store.Capacity.RangeCount) > sl.candidateRanges.mean {
		underfullThreshold := int32(math.Floor(underfullRangeThreshold(st, sl.candidateRanges.mean)))
		for _, desc := range sl.stores {
			if desc.Capacity.RangeCount < underfullThreshold {
				log.Infof(ctx,
					"s%d: should-rebalance(better-fit-ranges=s%d): rangeCount=%d, otherRangeCount=%d, "+
						"mean=%.2f, underfull-threshold=%d",
					store.StoreID, desc.StoreID, store.Capacity.RangeCount, desc.Capacity.RangeCount,
					sl.candidateRanges.mean, underfullThreshold)
				return true
			}
		}
	}

	// If we reached this point, we're happy with the range where it is.
	return false
}

// preexistingReplicaCheck returns true if no existing replica is present on
// the candidate's node.
func preexistingReplicaCheck(nodeID roachpb.NodeID, existing []roachpb.ReplicaDescriptor) bool {
	for _, r := range existing {
		if r.NodeID == nodeID {
			return false
		}
	}
	return true
}

// storeHasConstraint returns whether a store's attributes or node's locality
// matches the key value pair in the constraint.
func storeHasConstraint(store roachpb.StoreDescriptor, c config.Constraint) bool {
	if c.Key == "" {
		for _, attrs := range []roachpb.Attributes{store.Attrs, store.Node.Attrs} {
			for _, attr := range attrs.Attrs {
				if attr == c.Value {
					return true
				}
			}
		}
	} else {
		for _, tier := range store.Node.Locality.Tiers {
			if c.Key == tier.Key && c.Value == tier.Value {
				return true
			}
		}
	}
	return false
}

// constraintCheck returns true iff all required and prohibited constraints are
// satisfied. Stores with attributes or localities that match the most positive
// constraints return higher scores.
func constraintCheck(store roachpb.StoreDescriptor, constraints config.Constraints) (bool, int) {
	if len(constraints.Constraints) == 0 {
		return true, 0
	}
	positive := 0
	for _, constraint := range constraints.Constraints {
		hasConstraint := storeHasConstraint(store, constraint)
		switch {
		case constraint.Type == config.Constraint_REQUIRED && !hasConstraint:
			return false, 0
		case constraint.Type == config.Constraint_PROHIBITED && hasConstraint:
			return false, 0
		case (constraint.Type == config.Constraint_POSITIVE && hasConstraint):
			positive++
		}
	}
	return true, positive
}

// diversityScore returns a score between 1 and 0 where higher scores are stores
// with the fewest locality tiers in common with already existing replicas.
func diversityScore(
	store roachpb.StoreDescriptor, existingNodeLocalities map[roachpb.NodeID]roachpb.Locality,
) float64 {
	minScore := 1.0
	for _, locality := range existingNodeLocalities {
		if newScore := store.Node.Locality.DiversityScore(locality); newScore < minScore {
			minScore = newScore
		}
	}
	return minScore
}

// diversityRemovalScore is similar to diversityScore but instead of calculating
// the score if a new node is added, it calculates the remaining diversity if a
// node is removed.
func diversityRemovalScore(
	nodeID roachpb.NodeID, existingNodeLocalities map[roachpb.NodeID]roachpb.Locality,
) float64 {
	var maxScore float64
	for nodeIDx, localityX := range existingNodeLocalities {
		if nodeIDx == nodeID {
			continue
		}
		for nodeIDy, localityY := range existingNodeLocalities {
			if nodeIDy == nodeID || nodeIDx >= nodeIDy {
				continue
			}
			if newScore := localityX.DiversityScore(localityY); newScore > maxScore {
				maxScore = newScore
			}
		}
	}
	return maxScore
}

type rangeCountStatus int

const (
	overfull  rangeCountStatus = -1
	balanced  rangeCountStatus = 0
	underfull rangeCountStatus = 1
)

func oppositeStatus(rcs rangeCountStatus) rangeCountStatus {
	return -rcs
}

// balanceScore returns an arbitrarily scaled score where higher scores are for
// stores where the range is a better fit based on various balance factors
// like range count, disk usage, and QPS.
func balanceScore(
	st *cluster.Settings, sl StoreList, sc roachpb.StoreCapacity, rangeInfo RangeInfo,
) balanceDimensions {
	var dimensions balanceDimensions
	if float64(sc.RangeCount) > overfullRangeThreshold(st, sl.candidateRanges.mean) {
		dimensions.ranges = overfull
	} else if float64(sc.RangeCount) < underfullRangeThreshold(st, sl.candidateRanges.mean) {
		dimensions.ranges = underfull
	} else {
		dimensions.ranges = balanced
	}
	if st.EnableStatsBasedRebalancing.Get() {
		dimensions.bytes = balanceContribution(
			st,
			dimensions.ranges,
			sl.candidateDiskUsage.mean,
			sc.FractionUsed(),
			sc.BytesPerReplica,
			float64(rangeInfo.LiveBytes))
		dimensions.writes = balanceContribution(
			st,
			dimensions.ranges,
			sl.candidateWritesPerSecond.mean,
			sc.WritesPerSecond,
			sc.WritesPerReplica,
			rangeInfo.WritesPerSecond)
	}
	return dimensions
}

// balanceContribution generates a single dimension's contribution to a range's
// balanceScore, where larger values mean a store is a better fit for a given
// range.
func balanceContribution(
	st *cluster.Settings,
	rcs rangeCountStatus,
	mean float64,
	storeVal float64,
	percentiles roachpb.Percentiles,
	rangeVal float64,
) float64 {
	if storeVal > overfullStatThreshold(st, mean) {
		return percentileScore(rcs, percentiles, rangeVal)
	} else if storeVal < underfullStatThreshold(st, mean) {
		// To ensure that we behave symmetrically when underfull compared to
		// when we're overfull, inverse both the rangeCountStatus and the
		// result returned by percentileScore. This makes it so that being
		// overfull on ranges and on the given dimension behaves symmetrically to
		// being underfull on ranges and the given dimension (and ditto for
		// overfull on ranges and underfull on a dimension, etc.).
		return -percentileScore(oppositeStatus(rcs), percentiles, rangeVal)
	}
	return 0
}

// percentileScore returns a score for how desirable it is to put a range
// onto a particular store given the assumption that the store is overfull
// along a particular dimension. Takes as parameters:
// * How the number of ranges on the store compares to the norm
// * The distribution of values in the store for the dimension
// * The range's value for the dimension
// A higher score means that the range is a better fit for the store.
func percentileScore(
	rcs rangeCountStatus, percentiles roachpb.Percentiles, rangeVal float64,
) float64 {
	// Note that there is not any great research behind these values. If they're
	// causing thrashing or a bad imbalance, rethink them and modify them as
	// appropriate.
	if rcs == balanced {
		// If the range count is balanced, we should prefer ranges that are
		// very small on this particular dimension to try to rebalance this dimension
		// without messing up the replica counts.
		if rangeVal < percentiles.P10 {
			return 1
		} else if rangeVal < percentiles.P25 {
			return 0.5
		} else if rangeVal > percentiles.P90 {
			return -1
		} else if rangeVal > percentiles.P75 {
			return -0.5
		}
		// It may be better to return more than 0 here, since taking on an
		// average range isn't necessarily bad, but for now let's see how this works.
		return 0
	} else if rcs == overfull {
		// If this store has too many ranges, we're ok with moving any range that's
		// at least somewhat sizable in this dimension, since we want to reduce both
		// the range count and this metric. Moving extreme outliers may be
		// undesirable, though.
		if rangeVal < percentiles.P10 || rangeVal > percentiles.P90 {
			return 1
		} else if rangeVal <= percentiles.P25 || rangeVal >= percentiles.P75 {
			return 0
		}
		return -1
	} else if rcs == underfull {
		// If this store has too few ranges but is overloaded on some other
		// dimension, we need to prioritize moving away replicas that are
		// high in that dimension and accepting replicas that are low in it.
		//
		// TODO(a-robinson): Will this cause thrashing if one range has
		// significantly more QPS than all other ranges?
		if rangeVal < percentiles.P10 {
			return 1
		} else if rangeVal < percentiles.P25 {
			return 0.5
		} else if rangeVal > percentiles.P90 {
			return -1
		} else if rangeVal > percentiles.P75 {
			return -0.5
		}
		return 0
	}
	panic(fmt.Sprintf("reached unreachable code: %+v; %+v; %+v", rcs, percentiles, rangeVal))
}

func rangeIsGoodFit(bd balanceDimensions) bool {
	// A score greater than 1 means that more than one dimension improves
	// without being canceled out by the third, since each dimension can only
	// contribute a value from [-1,1] to the score.
	return bd.totalScore() > 1
}

func rangeIsBadFit(bd balanceDimensions) bool {
	// This is the same logic as for rangeIsGoodFit, just reversed.
	return bd.totalScore() < -1
}

func rangeIsPoorFit(bd balanceDimensions) bool {
	// A score less than -0.5 isn't a great fit for a range, since the
	// bad dimensions outweigh the good by at least one entire dimension.
	return bd.totalScore() < -0.5
}

func overfullRangeThreshold(st *cluster.Settings, mean float64) float64 {
	if !st.EnableStatsBasedRebalancing.Get() {
		return mean * (1 + st.RangeRebalanceThreshold.Get())
	}
	return math.Max(mean*(1+st.RangeRebalanceThreshold.Get()), mean+5)
}

func underfullRangeThreshold(st *cluster.Settings, mean float64) float64 {
	if !st.EnableStatsBasedRebalancing.Get() {
		return mean * (1 - st.RangeRebalanceThreshold.Get())
	}
	return math.Min(mean*(1-st.RangeRebalanceThreshold.Get()), mean-5)
}

func overfullStatThreshold(st *cluster.Settings, mean float64) float64 {
	return mean * (1 + st.StatRebalanceThreshold.Get())
}

func underfullStatThreshold(st *cluster.Settings, mean float64) float64 {
	return mean * (1 - st.StatRebalanceThreshold.Get())
}

func rebalanceFromConvergesOnMean(
	st *cluster.Settings, sl StoreList, sc roachpb.StoreCapacity, rangeInfo RangeInfo,
) bool {
	return rebalanceConvergesOnMean(
		st,
		sl,
		sc,
		rangeInfo,
		sc.RangeCount-1,
		float64(sc.Capacity-(sc.Available-rangeInfo.LiveBytes))/float64(sc.Capacity),
		sc.WritesPerSecond-rangeInfo.WritesPerSecond)
}

func rebalanceToConvergesOnMean(
	st *cluster.Settings, sl StoreList, sc roachpb.StoreCapacity, rangeInfo RangeInfo,
) bool {
	return rebalanceConvergesOnMean(
		st,
		sl,
		sc,
		rangeInfo,
		sc.RangeCount+1,
		float64(sc.Capacity-(sc.Available+rangeInfo.LiveBytes))/float64(sc.Capacity),
		sc.WritesPerSecond+rangeInfo.WritesPerSecond)
}

func rebalanceConvergesOnMean(
	st *cluster.Settings,
	sl StoreList,
	sc roachpb.StoreCapacity,
	rangeInfo RangeInfo,
	newRangeCount int32,
	newFractionUsed float64,
	newWritesPerSecond float64,
) bool {
	if !st.EnableStatsBasedRebalancing.Get() {
		return convergesOnMean(float64(sc.RangeCount), float64(newRangeCount), sl.candidateRanges.mean)
	}
	var convergeCount int
	if convergesOnMean(float64(sc.RangeCount), float64(newRangeCount), sl.candidateRanges.mean) {
		convergeCount++
	} else {
		convergeCount--
	}
	if convergesOnMean(sc.FractionUsed(), newFractionUsed, sl.candidateDiskUsage.mean) {
		convergeCount++
	} else {
		convergeCount--
	}
	if convergesOnMean(sc.WritesPerSecond, newWritesPerSecond, sl.candidateWritesPerSecond.mean) {
		convergeCount++
	} else {
		convergeCount--
	}
	return convergeCount > 0
}

func convergesOnMean(oldVal, newVal, mean float64) bool {
	return math.Abs(newVal-mean) < math.Abs(oldVal-mean)
}

// maxCapacityCheck returns true if the store has room for a new replica.
func maxCapacityCheck(store roachpb.StoreDescriptor) bool {
	return store.Capacity.FractionUsed() < maxFractionUsedThreshold
}
