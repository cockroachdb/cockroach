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
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// candidate store for allocation.
type candidate struct {
	store      roachpb.StoreDescriptor
	constraint float64 // Score used to pick the top candidates.
	balance    float64 // Score used to choose between top candidates.
	replica    roachpb.ReplicaDescriptor
}

// less first compares constraint scores, then balance scores.
func (c candidate) less(o candidate) bool {
	if c.constraint != o.constraint {
		return c.constraint < o.constraint
	}
	return c.balance < o.balance
}

type candidateList []candidate

// bestConstraint returns all the elements in a sorted candidate list that share
// the highest constraint score.
func (cl candidateList) bestConstraint() candidateList {
	if len(cl) <= 1 {
		return cl
	}
	for i := 1; i < len(cl); i++ {
		if cl[i].constraint < cl[0].constraint {
			return cl[0:i]
		}
	}
	return cl
}

// worstConstraint returns all the elements in a sorted candidate list that
// share the lowest constraint score.
func (cl candidateList) worstConstraint() candidateList {
	if len(cl) <= 1 {
		return cl
	}
	for i := len(cl) - 2; i >= 0; i-- {
		if cl[i].constraint < cl[len(cl)-1].constraint {
			return cl[i+1:]
		}
	}
	return cl
}

// selectGood randomly chooses a good candidate from a sorted candidate list
// using the provided random generator.
func (cl candidateList) selectGood(randGen allocatorRand) candidate {
	cl = cl.bestConstraint()
	if len(cl) == 1 {
		return cl[0]
	}
	randGen.Lock()
	order := randGen.Perm(len(cl))
	randGen.Unlock()
	best := cl[order[0]]
	for i := 1; i < allocatorRandomCount; i++ {
		if best.less(cl[order[i]]) {
			best = cl[order[i]]
		}
	}
	return best
}

// selectBad randomly chooses a bad candidate from a sorted candidate list using
//
func (cl candidateList) selectBad(randGen allocatorRand) candidate {
	cl = cl.worstConstraint()
	if len(cl) == 1 {
		return cl[0]
	}
	randGen.Lock()
	order := randGen.Perm(len(cl))
	randGen.Unlock()
	worst := cl[order[0]]
	for i := 1; i < allocatorRandomCount; i++ {
		if cl[order[i]].less(worst) {
			worst = cl[order[i]]
		}
	}
	return worst
}

// solveState is used to pass solution state information into a rule.
type solveState struct {
	constraints            config.Constraints
	sl                     StoreList
	existing               []roachpb.ReplicaDescriptor
	existingNodeLocalities map[roachpb.NodeID]roachpb.Locality
}

// rule is a function that given a solveState will score and possibly
// disqualify a store. The store in solveState can be disqualified by
// returning false. Unless disqualified, the higher the returned score, the
// more likely the store will be picked as a candidate.
type rule func(roachpb.StoreDescriptor, solveState) (bool, float64, float64)

// ruleSolver is used to test a collection of rules against stores.
type ruleSolver []rule

// defaultRuleSolver is the set of rules used.
var defaultRuleSolver = ruleSolver{
	ruleReplicasUniqueNodes,
	ruleConstraints,
	ruleCapacity,
	ruleDiversity,
}

// Solve runs the rules against the stores in the store list and returns all
// passing stores and their scores ordered from best to worst score.
func (rs ruleSolver) Solve(
	sl StoreList,
	c config.Constraints,
	existing []roachpb.ReplicaDescriptor,
	existingNodeLocalities map[roachpb.NodeID]roachpb.Locality,
) (candidateList, error) {
	candidates := make(candidateList, 0, len(sl.stores))
	state := solveState{
		constraints:            c,
		sl:                     sl,
		existing:               existing,
		existingNodeLocalities: existingNodeLocalities,
	}

	for _, store := range sl.stores {
		if cand, ok := rs.computeCandidate(store, state); ok {
			candidates = append(candidates, cand)
		}
	}
	sort.Sort(sort.Reverse(byScore(candidates)))
	return candidates, nil
}

// computeCandidate runs the rules against a candidate store using the provided
// state and returns each candidate's score and if the candidate is valid.
func (rs ruleSolver) computeCandidate(
	store roachpb.StoreDescriptor, state solveState,
) (candidate, bool) {
	var totalConstraintScore, totalBalanceScore float64
	for _, rule := range rs {
		valid, constraintScore, balanceScore := rule(store, state)
		if !valid {
			return candidate{}, false
		}
		totalConstraintScore += constraintScore
		totalBalanceScore += balanceScore
	}
	return candidate{
		store:      store,
		constraint: totalConstraintScore,
		balance:    totalBalanceScore,
	}, true
}

// ruleReplicasUniqueNodes returns true iff no existing replica is present on
// the candidate's node. All other scores are always 0.
func ruleReplicasUniqueNodes(
	store roachpb.StoreDescriptor, state solveState,
) (bool, float64, float64) {
	for _, r := range state.existing {
		if r.NodeID == store.Node.NodeID {
			return false, 0, 0
		}
	}
	return true, 0, 0
}

// storeHasConstraint returns whether a store descriptor attributes or locality
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

// ruleConstraints returns true iff all required and prohibited constraints are
// satisfied. Stores with attributes or localities that match the most positive
// constraints return higher scores.
func ruleConstraints(store roachpb.StoreDescriptor, state solveState) (bool, float64, float64) {
	if len(state.constraints.Constraints) == 0 {
		return true, 0, 0
	}
	matched := 0
	for _, c := range state.constraints.Constraints {
		hasConstraint := storeHasConstraint(store, c)
		switch {
		case c.Type == config.Constraint_REQUIRED && !hasConstraint:
			return false, 0, 0
		case c.Type == config.Constraint_PROHIBITED && hasConstraint:
			return false, 0, 0
		case (c.Type == config.Constraint_POSITIVE && hasConstraint) ||
			(c.Type == config.Constraint_REQUIRED && hasConstraint) ||
			(c.Type == config.Constraint_PROHIBITED && !hasConstraint):
			matched++
		}
	}

	return true, float64(matched) / float64(len(state.constraints.Constraints)), 0
}

// ruleDiversity returns higher scores for stores with the fewest locality tiers
// in common with already existing replicas. It always returns true.
func ruleDiversity(store roachpb.StoreDescriptor, state solveState) (bool, float64, float64) {
	minScore := math.Inf(0)
	for _, locality := range state.existingNodeLocalities {
		if newScore := store.Node.Locality.DiversityScore(locality); newScore < minScore {
			minScore = newScore
		}
	}

	if !math.IsInf(minScore, 0) {
		return true, minScore, 0
	}
	return true, 0, 0
}

// ruleCapacity returns true iff a new replica won't overfill the store. The
// score returned is inversely proportional to the number of ranges on the
// candidate store, with the most empty nodes having the highest scores.
// TODO(bram): consider splitting this into two rules.
func ruleCapacity(store roachpb.StoreDescriptor, state solveState) (bool, float64, float64) {
	// Don't overfill stores.
	if store.Capacity.FractionUsed() > maxFractionUsedThreshold {
		return false, 0, 0
	}

	return true, 0, 1 / float64(store.Capacity.RangeCount+1)
}

// byScore implements sort.Interface to sort by scores.
type byScore candidateList

var _ sort.Interface = byScore(nil)

func (c byScore) Len() int           { return len(c) }
func (c byScore) Less(i, j int) bool { return c[i].less(c[j]) }
func (c byScore) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

type byScoreAndID candidateList

var _ sort.Interface = byScoreAndID(nil)

func (c byScoreAndID) Len() int { return len(c) }
func (c byScoreAndID) Less(i, j int) bool {
	if c[i].constraint == c[j].constraint && c[i].balance == c[j].balance {
		return c[i].store.StoreID < c[j].store.StoreID
	}
	return c[i].less(c[j])
}
func (c byScoreAndID) Swap(i, j int) { c[i], c[j] = c[j], c[i] }
