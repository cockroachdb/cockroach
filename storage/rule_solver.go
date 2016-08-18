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

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/pkg/errors"
)

// defaultRules is the default rule set to use.
var defaultRules = []rule{
	ruleReplicasUniqueNodes,
	ruleNoProhibitedConstraints,
	ruleRequiredConstraints,
	ruleCapacity,
	rulePositiveConstraints,
	ruleDiversity,
}

// makeDefaultRuleSolver returns a ruleSolver with defaultRules.
func makeDefaultRuleSolver(storePool *StorePool) *ruleSolver {
	return makeRuleSolver(storePool, defaultRules)
}

// makeRuleSolver makes a new ruleSolver. The order of the rules is the order in
// which they are run. For optimization purposes, less computationally intense
// rules should run first to eliminate candidates.
func makeRuleSolver(storePool *StorePool, rules []rule) *ruleSolver {
	return &ruleSolver{
		storePool: storePool,
		rules:     rules,
	}
}

// rule is a generic rule that can be used to solve a constraint problem.
type rule func(
	c config.Constraints,
	store roachpb.StoreDescriptor,
	existing []roachpb.ReplicaDescriptor,
	sl StoreList,
) (candidate bool, score float64)

// ruleSolver solves a set of rules for a store.
type ruleSolver struct {
	storePool *StorePool
	rules     []rule
}

// solveInternal solves given constraints. See (*ruleSolver).solveInternal.
func (rs *ruleSolver) solve(
	c config.Constraints, existing []roachpb.ReplicaDescriptor,
) ([]roachpb.StoreDescriptor, error) {
	candidates, err := rs.solveScores(c, existing)
	if err != nil {
		return nil, err
	}

	candidateStores := make([]roachpb.StoreDescriptor, len(candidates))
	for i, candidate := range candidates {
		candidateStores[i] = candidate.store
	}
	return candidateStores, nil
}

// solvel solves given constraints and returns the score.
func (rs *ruleSolver) solveScores(
	c config.Constraints, existing []roachpb.ReplicaDescriptor,
) ([]candidate, error) {
	sl, _, throttledStoreCount := rs.storePool.getStoreList()

	// When there are throttled stores that do match, we shouldn't send
	// the replica to purgatory or even consider relaxing the constraints.
	if throttledStoreCount > 0 {
		return nil, errors.Errorf("%d matching stores are currently throttled", throttledStoreCount)
	}

	candidates := make([]candidate, 0, len(sl.stores))

Stores:
	for _, store := range sl.stores {
		var totalScore float64
		for _, rule := range rs.rules {
			candidate, score := rule(c, store, existing, sl)
			if !candidate {
				continue Stores
			}
			if !math.IsNaN(score) {
				totalScore += score
			}
		}
		candidates = append(candidates, candidate{store: store, score: totalScore})
	}
	sort.Sort(byScore(candidates))
	return candidates, nil
}

type candidate struct {
	store roachpb.StoreDescriptor
	score float64
}

type byScore []candidate

func (c byScore) Len() int           { return len(c) }
func (c byScore) Less(i, j int) bool { return c[i].score > c[j].score }
func (c byScore) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

// ruleReplicasUniqueNodes ensures that no two replicas are put on the same
// node.
func ruleReplicasUniqueNodes(
	_ config.Constraints,
	store roachpb.StoreDescriptor,
	existing []roachpb.ReplicaDescriptor,
	_ StoreList,
) (candidate bool, score float64) {
	for _, r := range existing {
		if r.NodeID == store.Node.NodeID {
			return false, 0
		}
	}
	return true, 0
}

func storeHasConstraint(store roachpb.StoreDescriptor, c config.Constraint) bool {
	var found bool
	if c.Key == "" {
		for _, attrs := range []roachpb.Attributes{store.Attrs, store.Node.Attrs} {
			for _, attr := range attrs.Attrs {
				if attr == c.Value {
					return true
				}
			}
		}
	} else {
		for _, tier := range store.Locality.Tiers {
			if c.Key == tier.Key && c.Value == tier.Value {
				return true
			}
		}
	}
	return found
}

// ruleNoProhibitedConstraints ensures that the candidate store has no
// prohibited constraints.
func ruleNoProhibitedConstraints(
	constraints config.Constraints,
	store roachpb.StoreDescriptor,
	_ []roachpb.ReplicaDescriptor,
	_ StoreList,
) (candidate bool, score float64) {
	for _, c := range constraints.Constraints {
		if c.Type != config.Constraint_PROHIBITED {
			continue
		}

		if storeHasConstraint(store, c) {
			return false, 0
		}
	}
	return true, 0
}

// ruleRequiredConstraints ensures that the candidate store has the required
// constraints.
func ruleRequiredConstraints(
	constraints config.Constraints,
	store roachpb.StoreDescriptor,
	_ []roachpb.ReplicaDescriptor,
	_ StoreList,
) (candidate bool, score float64) {
	for _, c := range constraints.Constraints {
		if c.Type != config.Constraint_REQUIRED {
			continue
		}

		if !storeHasConstraint(store, c) {
			return false, 0
		}
	}
	return true, 0
}

// rulePositiveConstraints ensures that nodes that match more the positive
// constraints are given higher priority.
func rulePositiveConstraints(
	constraints config.Constraints,
	store roachpb.StoreDescriptor,
	_ []roachpb.ReplicaDescriptor,
	_ StoreList,
) (candidate bool, score float64) {
	const weight = 1.0

	matched := 0
	for _, c := range constraints.Constraints {
		if c.Type != config.Constraint_POSITIVE {
			continue
		}

		if storeHasConstraint(store, c) {
			matched++
		}
	}
	return true, weight * float64(matched) / float64(len(constraints.Constraints))
}

// ruleDiversity ensures that nodes that have the fewest locality tiers in
// common are given higher priority.
func ruleDiversity(
	_ config.Constraints,
	store roachpb.StoreDescriptor,
	existing []roachpb.ReplicaDescriptor,
	sl StoreList,
) (candidate bool, score float64) {
	const weight = 0.1

	stores := map[roachpb.StoreID]roachpb.StoreDescriptor{}
	for _, store := range sl.stores {
		stores[store.StoreID] = store
	}

	var maxScore float64
	tiers := store.Locality.Tiers
	for i, tier := range tiers {
		tierScore := float64(int(1) << uint(len(tiers)-i-1))
		for _, existing := range existing {
			store := stores[existing.StoreID]
			st := store.Locality.Tiers
			if len(st) < i || st[i].Key != tier.Key {
				panic("TODO(d4l3k): Node locality configurations are not equivalent")
			}
			if st[i].Value != tier.Value {
				score += tierScore
			}
			maxScore += tierScore
		}
	}
	return true, weight * score / maxScore
}

// ruleCapacity prioritizes placing data on empty nodes when the choice is
// available and prevents data from going onto mostly full nodes.
func ruleCapacity(
	_ config.Constraints,
	store roachpb.StoreDescriptor,
	_ []roachpb.ReplicaDescriptor,
	_ StoreList,
) (candidate bool, score float64) {
	const weight = 0.01

	// Don't overfill stores.
	if store.Capacity.FractionUsed() > maxFractionUsedThreshold {
		return false, 0
	}

	return true, weight * (1 - store.Capacity.FractionUsed())
}
