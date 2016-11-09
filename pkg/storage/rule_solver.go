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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// candidate store for allocation.
type candidate struct {
	store roachpb.StoreDescriptor
	score float64
}

// solveState is used to pass solution state information into a rule.
type solveState struct {
	constraints config.Constraints
	store       roachpb.StoreDescriptor
	existing    []roachpb.ReplicaDescriptor
	sl          StoreList
	tiers       map[roachpb.StoreID]map[string]roachpb.Tier
	tierOrder   []roachpb.Tier
}

// rule is a generic rule that can be used to solve a constraint problem.
type rule struct {
	weight float64
	// run is a function that given a solveState will score and possibly
	// disqualify a store. The store in solveState can be disqualified by
	// returning false.	Unless disqualified, the higher the returned score, the
	// more likely the store will be picked as a candidate. When calculating the
	// best candidate, the rule's weight is used as a multiplier for the
	// score.
	run func(state solveState) (float64, bool)
}

// defaultSolverRules is the default rule set to use.
var defaultSolverRules = []rule{
	{
		weight: 1.0,
		run:    ruleReplicasUniqueNodes,
	},
	{
		weight: 1.0,
		run:    ruleConstraints,
	},
	{
		weight: 0.01,
		run:    ruleCapacity,
	},
	{
		weight: 0.1,
		run:    ruleDiversity,
	},
}

// makeRuleSolver makes a new ruleSolver. The order of the rules is the order in
// which they are run. For optimization purposes, less computationally expensive
// rules should run first to eliminate candidates.
func makeRuleSolver(rules []rule) ruleSolver {
	return ruleSolver{
		rules: rules,
	}
}

// ruleSolver solves a set of rules for a store.
type ruleSolver struct {
	rules []rule
}

// Solve runs the rules against the stores in the store list and returns all
// passing stores and their scores ordered from best to worst score.
func (rs ruleSolver) Solve(
	sl StoreList, c config.Constraints, existing []roachpb.ReplicaDescriptor,
) ([]candidate, error) {
	candidates := make([]candidate, 0, len(sl.stores))
	state := solveState{
		constraints: c,
		existing:    existing,
		sl:          sl,
		tierOrder:   canonicalTierOrder(sl),
		tiers:       storeTierMap(sl),
	}

	for _, store := range sl.stores {
		state.store = store
		if cand, ok := rs.computeCandidate(state); ok {
			candidates = append(candidates, cand)
		}
	}
	sort.Sort(byScoreDescending(candidates))
	return candidates, nil
}

// computeCandidate runs the rules against a candidate store using the provided
// state and returns each candidate's score and if the candidate is valid.
func (rs ruleSolver) computeCandidate(state solveState) (candidate, bool) {
	var totalScore float64
	for _, rule := range rs.rules {
		score, valid := rule.run(state)
		if !valid {
			return candidate{}, false
		}
		totalScore += score * rule.weight
	}
	return candidate{store: state.store, score: totalScore}, true
}

// ruleReplicasUniqueNodes returns true iff no existing replica is present on
// the candidate's node.
func ruleReplicasUniqueNodes(state solveState) (float64, bool) {
	for _, r := range state.existing {
		if r.NodeID == state.store.Node.NodeID {
			return 0, false
		}
	}
	return 0, true
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
// satisfied. Stores with more positive constraints return higher scores.
func ruleConstraints(state solveState) (float64, bool) {
	if len(state.constraints.Constraints) == 0 {
		return 0, true
	}
	matched := 0
	for _, c := range state.constraints.Constraints {
		hasConstraint := storeHasConstraint(state.store, c)
		switch {
		case c.Type == config.Constraint_POSITIVE && hasConstraint:
			matched++
		case c.Type == config.Constraint_REQUIRED && !hasConstraint:
			return 0, false
		case c.Type == config.Constraint_PROHIBITED && hasConstraint:
			return 0, false
		}
	}

	return float64(matched) / float64(len(state.constraints.Constraints)), true
}

// ruleDiversity returns higher scores for stores with the fewest locality tiers
// in common with already existing replicas. It always returns true.
func ruleDiversity(state solveState) (float64, bool) {
	storeTiers := state.tiers[state.store.StoreID]
	var maxScore, score float64
	for i, tier := range state.tierOrder {
		storeTier, ok := storeTiers[tier.Key]
		if !ok {
			continue
		}
		tierScore := 1 / (float64(i) + 1)
		for _, existing := range state.existing {
			existingTier, ok := state.tiers[existing.StoreID][tier.Key]
			if ok && existingTier.Value != storeTier.Value {
				score += tierScore
			}
			maxScore += tierScore
		}
	}
	if maxScore == 0 {
		return 0, true
	}
	return score / maxScore, true
}

// ruleCapacity returns true iff a new replica won't overfill the store. The
// score returned is inversely proportional to the number of ranges on the
// candidate store, with the most empty nodes having the highest scores.
// TODO(bram): consider splitting this into two rules.
func ruleCapacity(state solveState) (float64, bool) {
	// Don't overfill stores.
	if state.store.Capacity.FractionUsed() > maxFractionUsedThreshold {
		return 0, false
	}

	return 1 / float64(state.store.Capacity.RangeCount+1), true
}

// canonicalTierOrder returns the most common key at each tier level in
// order from the most important tier down to the least important, cutting off
// if the majority of stores have no tier at a level.
func canonicalTierOrder(sl StoreList) []roachpb.Tier {
	maxTierCount := 0
	for _, store := range sl.stores {
		if count := len(store.Node.Locality.Tiers); maxTierCount < count {
			maxTierCount = count
		}
	}

	// Might have up to maxTierCount of tiers.
	tiers := make([]roachpb.Tier, 0, maxTierCount)
	for i := 0; i < maxTierCount; i++ {
		// At each tier, count the number of occurrences of each key.
		counts := map[string]int{}
		maxKey := ""
		for _, store := range sl.stores {
			key := ""
			if i < len(store.Node.Locality.Tiers) {
				key = store.Node.Locality.Tiers[i].Key
			}
			counts[key]++
			if counts[key] > counts[maxKey] {
				maxKey = key
			}
		}
		// Don't add the tier if most nodes don't have that many tiers.
		if maxKey != "" {
			tiers = append(tiers, roachpb.Tier{Key: maxKey})
		} else {
			break
		}
	}
	return tiers
}

// storeTierMap indexes a store list so it can be used to look up the locality
// tier value from store ID and tier key.
func storeTierMap(sl StoreList) map[roachpb.StoreID]map[string]roachpb.Tier {
	m := map[roachpb.StoreID]map[string]roachpb.Tier{}
	for _, store := range sl.stores {
		sm := map[string]roachpb.Tier{}
		m[store.StoreID] = sm
		for _, tier := range store.Node.Locality.Tiers {
			sm[tier.Key] = tier
		}
	}
	return m
}

// byScore implements sort.Interface to sort by scores descending from highest
// to lowest.
type byScoreDescending []candidate

var _ sort.Interface = byScoreDescending(nil)

func (c byScoreDescending) Len() int           { return len(c) }
func (c byScoreDescending) Less(i, j int) bool { return c[i].score > c[j].score }
func (c byScoreDescending) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
