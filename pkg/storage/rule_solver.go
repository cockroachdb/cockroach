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
//
// Author: Tristan Rice (rice@fn.lc)

package storage

import (
	"bytes"
	"fmt"
	"math"
	"sort"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

	// TODO: Is this a defined constant anywhere in a standard library?
	bytesPerGiB = 1 << 30
)

func rebalanceFromConvergesOnMean(sl StoreList, candidate roachpb.StoreDescriptor) bool {
	return rangesPerGiB(candidate.Capacity) > sl.candidateRangesPerGiB.mean+0.5
}

func rebalanceToConvergesOnMean(sl StoreList, candidate roachpb.StoreDescriptor) bool {
	return rangesPerGiB(candidate.Capacity) < sl.candidateRangesPerGiB.mean-0.5
}

// candidate store for allocation.
type candidate struct {
	store           roachpb.StoreDescriptor
	valid           bool
	constraintScore float64
	rangesPerGiB    float64
	details         string
}

func (c candidate) String() string {
	return fmt.Sprintf("s%d, valid:%t, con:%.2f, rangesPerGiB:%.2f, details:(%s)",
		c.store.StoreID, c.valid, c.constraintScore, c.rangesPerGiB, c.details)
}

// less first compares valid, then constraint scores, then range counts.
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
	return c.rangesPerGiB > o.rangesPerGiB
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
		c[i].rangesPerGiB == c[j].rangesPerGiB &&
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
		if cl[i].constraintScore < cl[0].constraintScore {
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
		if cl[i].constraintScore > cl[len(cl)-1].constraintScore {
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
// reserved) candidate list using the provided random generator.
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

func rangesPerGiB(capacity roachpb.StoreCapacity) float64 {
	// Don't consider differences in store size for stores of less than 1GB -
	// this is primarily useful to avoid huge rangesPerGiB numbers for unit tests
	// that use 1MB store capacities.
	capacityGB := math.Max(float64(capacity.Capacity/bytesPerGiB), 1.0)
	return float64(capacity.RangeCount) / capacityGB
}

// allocateCandidates creates a candidate list of all stores that can used for
// allocating a new replica ordered from the best to the worst. Only stores
// that meet the criteria are included in the list.
func allocateCandidates(
	sl StoreList,
	constraints config.Constraints,
	existing []roachpb.ReplicaDescriptor,
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
		candidates = append(candidates, candidate{
			store:           s,
			valid:           true,
			constraintScore: diversityScore + float64(preferredMatched),
			rangesPerGiB:    rangesPerGiB(s.Capacity),
			details: fmt.Sprintf("diversity=%.2f, preferred=%d",
				diversityScore, preferredMatched),
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
	sl StoreList,
	constraints config.Constraints,
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
		var convergesScore float64
		if !rebalanceFromConvergesOnMean(sl, s) {
			// If removing this candidate replica does not converge the range
			// counts to the mean, we make it less attractive for removal by
			// adding 1 to the constraint score. Note that when selecting a
			// candidate for removal the candidates with the lowest scores are
			// more likely to be removed.
			convergesScore = 1
		}
		candidates = append(candidates, candidate{
			store:           s,
			valid:           true,
			constraintScore: diversityScore + float64(preferredMatched) + convergesScore,
			rangesPerGiB:    rangesPerGiB(s.Capacity),
			details: fmt.Sprintf("diversity=%.2f, preferred=%d, converge=%.2f",
				diversityScore, preferredMatched, convergesScore),
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
	sl StoreList,
	constraints config.Constraints,
	existing []roachpb.ReplicaDescriptor,
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
		for _, s := range sl.stores {
			if _, ok := existingStoreIDs[s.StoreID]; ok {
				if shouldRebalance(ctx, s, constraintsOkStoreList) {
					shouldRebalanceCheck = true
					break
				}
			}
		}
	}

	// Only rebalance away if the constraints don't match or the max
	// capacity check fails.
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
			var convergesScore float64
			if !rebalanceFromConvergesOnMean(constraintsOkStoreList, s) {
				// Similarly to in removeCandidates, any replica whose removal
				// would not converge the range counts to the mean is given a
				// constraint score boost of 1 to make it less attractive for
				// removal.
				convergesScore = 1
			}
			existingCandidates = append(existingCandidates, candidate{
				store:           s,
				valid:           true,
				constraintScore: diversityScore + float64(storeInfo.matched) + convergesScore,
				rangesPerGiB:    rangesPerGiB(s.Capacity),
				details: fmt.Sprintf("diversity=%.2f, preferred=%d, converge=%.2f",
					diversityScore, storeInfo.matched, convergesScore),
			})
		} else {
			if !storeInfo.ok || !maxCapacityOK {
				continue
			}
			var convergesScore float64
			if rebalanceToConvergesOnMean(constraintsOkStoreList, s) {
				// This is the counterpart of !rebalanceFromConvergesOnMean from
				// the existing candidates. Candidates whose addition would
				// converge towards the range count mean are promoted.
				convergesScore = 1
			} else if !rebalanceConstraintsCheck {
				// Only consider this candidate if we must rebalance due to a
				// constraint check requirements.
				continue
			}
			diversityScore := diversityScore(s, existingNodeLocalities)
			candidates = append(candidates, candidate{
				store:           s,
				valid:           true,
				constraintScore: diversityScore + float64(storeInfo.matched) + convergesScore,
				rangesPerGiB:    rangesPerGiB(s.Capacity),
				details: fmt.Sprintf("diversity=%.2f, preferred=%d, converge=%.2f",
					diversityScore, storeInfo.matched, convergesScore),
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
func shouldRebalance(ctx context.Context, store roachpb.StoreDescriptor, sl StoreList) bool {
	// TODO(peter,bram,cuong): The FractionUsed check seems suspicious. When a
	// node becomes fuller than maxFractionUsedThreshold we will always select it
	// for rebalancing. This is currently utilized by tests.
	maxCapacityUsed := store.Capacity.FractionUsed() >= maxFractionUsedThreshold

	// Rebalance if we're above the overfull threshold, which is
	// mean*(1+rebalanceThreshold).
	overfullThreshold := math.Ceil(sl.candidateRangesPerGiB.mean * (1 + baseRebalanceThreshold))
	rangeCountAboveOverfullThreshold := rangesPerGiB(store.Capacity) > overfullThreshold

	// Rebalance if the candidate store has a range count above the mean, and
	// there exists another store that is underfull: its range count is smaller
	// than mean*(1-rebalanceThreshold).
	var rebalanceToUnderfullStore bool
	if rangesPerGiB(store.Capacity) > sl.candidateRangesPerGiB.mean {
		underfullThreshold := math.Floor(sl.candidateRangesPerGiB.mean * (1 - baseRebalanceThreshold))
		for _, desc := range sl.stores {
			if rangesPerGiB(desc.Capacity) < underfullThreshold {
				rebalanceToUnderfullStore = true
				break
			}
		}
	}

	shouldRebalance := maxCapacityUsed || rangeCountAboveOverfullThreshold || rebalanceToUnderfullStore
	if log.V(2) && shouldRebalance {
		log.Infof(ctx,
			"s%d: should-rebalance: fraction-used=%.2f range-count=%d ranges-per-gb=%.2f"+
				"(mean=%.1f, overfull-threshold=%.2f, fraction-used=%t, "+
				"above-overfull-threshold=%t, rebalance-to-underfull=%t)",
			store.StoreID, store.Capacity.FractionUsed(), store.Capacity.RangeCount, rangesPerGiB(store.Capacity),
			sl.candidateRangesPerGiB.mean, overfullThreshold, maxCapacityUsed,
			rangeCountAboveOverfullThreshold, rebalanceToUnderfullStore)
	}
	return shouldRebalance
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

// maxCapacityCheck returns true if the store has room for a new replica.
func maxCapacityCheck(store roachpb.StoreDescriptor) bool {
	return store.Capacity.FractionUsed() < maxFractionUsedThreshold
}
