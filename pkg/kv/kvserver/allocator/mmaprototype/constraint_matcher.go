// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// constraintMatcher is used for fast constraint matching.
//
//   - When descriptor for a store changes, or there is a new store, call
//     cm.setStore(...)
//
// - When store is removed, call cm.removeStore(...)
//
// - Match using:
//   - cm.storeMatches(...): when we have a store and are checking whether it
//     matches a constraint conjunction.
//   - cm.constrainStoresForConjunction(...): set of stores matching a
//     conjunction
//   - cm.constrainStoresForExpr(...): set of stores matching a
//     constraintsDisj.
type constraintMatcher struct {
	interner *stringInterner
	// For each store, the set of matched constraints. The store will be in
	// exactly this set of posting lists.
	stores map[roachpb.StoreID]*matchedConstraints
	// For each constraint, the set of stores that match that constraint. This
	// map is populated lazily as constraints are encountered. Currently, there
	// is no removal from this map, but if needed we could evict based on size
	// using LRU.
	constraints map[internedConstraint]*matchedSet
	allStores   *matchedSet
}

type matchedConstraints struct {
	matched map[internedConstraint]struct{}
	sal     StoreAttributesAndLocality
}

type matchedSet struct {
	storeSet
}

func newConstraintMatcher(interner *stringInterner) *constraintMatcher {
	return &constraintMatcher{
		interner:    interner,
		stores:      map[roachpb.StoreID]*matchedConstraints{},
		constraints: map[internedConstraint]*matchedSet{},
		allStores:   &matchedSet{},
	}
}

// setStore is called for a new store, or when the attributes and locality changes.
func (cm *constraintMatcher) setStore(sal StoreAttributesAndLocality) {
	mc := cm.stores[sal.StoreID]
	if mc == nil {
		mc = &matchedConstraints{
			matched: map[internedConstraint]struct{}{},
		}
		cm.stores[sal.StoreID] = mc
		cm.allStores.insert(sal.StoreID)
	}
	mc.sal = sal
	// Update the matching info for the existing constraints.
	for c, matchedSet := range cm.constraints {
		matches := cm.storeMatchesConstraint(sal, c)
		_, existingMatch := mc.matched[c]
		if matches == existingMatch {
			continue
		}
		if !existingMatch {
			// Did not match before, but matches now.
			mc.matched[c] = struct{}{}
			notInSet := matchedSet.insert(sal.StoreID)
			if !notInSet {
				panic(errors.AssertionFailedf(
					"inconsistent state: store %d already in set", sal.StoreID))
			}
		} else if existingMatch {
			// No longer matches.
			delete(mc.matched, c)
			found := matchedSet.remove(sal.StoreID)
			if !found {
				panic(errors.AssertionFailedf(
					"inconsistent state: store %d not found", sal.StoreID))
			}
		}
	}
}

// storeMatchesConstraint is an internal helper method.
func (cm *constraintMatcher) storeMatchesConstraint(
	sal StoreAttributesAndLocality, c internedConstraint,
) bool {
	matches := false
	if c.key == emptyStringCode {
		for _, attrs := range []roachpb.Attributes{sal.StoreAttrs, sal.NodeAttrs} {
			for _, attr := range attrs.Attrs {
				if cm.interner.toCode(attr) == c.value {
					matches = true
					break
				}
			}
			if matches {
				break
			}
		}
	} else {
		for _, tier := range sal.NodeLocality.Tiers {
			if c.key == cm.interner.toCode(tier.Key) && c.value == cm.interner.toCode(tier.Value) {
				matches = true
				break
			}
		}
	}
	if c.typ == roachpb.Constraint_PROHIBITED {
		matches = !matches
	}
	return matches
}

// getMatchedSetForConstraint is an internal helper method.
func (cm *constraintMatcher) getMatchedSetForConstraint(c internedConstraint) *matchedSet {
	ms, ok := cm.constraints[c]
	if !ok {
		// New constraint.
		ms = &matchedSet{}
		cm.constraints[c] = ms
		for storeID, mc := range cm.stores {
			if cm.storeMatchesConstraint(mc.sal, c) {
				ms.insert(storeID)
				mc.matched[c] = struct{}{}
			}
		}
	}
	return ms
}

// constrainStoresForConjunction populates storeSet with the stores matching
// the given conjunction of constraints.
//
// TODO(sumeer): make storeSet a struct and use a sync.Pool.
func (cm *constraintMatcher) constrainStoresForConjunction(
	constraints []internedConstraint, storeSet *storeSet,
) {
	*storeSet = (*storeSet)[:0]
	if len(constraints) == 0 {
		*storeSet = append(*storeSet, cm.allStores.storeSet...)
		return
	}
	for i := range constraints {
		matchedSet := cm.getMatchedSetForConstraint(constraints[i])
		if i == 0 {
			*storeSet = append(*storeSet, matchedSet.storeSet...)
		} else {
			storeSet.intersect(matchedSet.storeSet)
		}
		if len(*storeSet) == 0 {
			return
		}
	}
}

var _ storeMatchesConstraintInterface = &constraintMatcher{}

// storeMatches returns whether the given storeID matches the given
// conjunction of constraints.
func (cm *constraintMatcher) storeMatches(
	storeID roachpb.StoreID, constraints constraintsConj,
) bool {
	mc := cm.stores[storeID]
	if mc == nil {
		return false
	}
	for _, c := range constraints {
		// Ensure the constraint is known.
		cm.getMatchedSetForConstraint(c)
		_, ok := mc.matched[c]
		if !ok {
			return false
		}
	}
	return true
}

// constrainStoresForExpr populates storeSet with the stores matching the
// given expression.
func (cm *constraintMatcher) constrainStoresForExpr(expr constraintsDisj, set *storeSet) {
	if len(expr) == 0 {
		*set = append(*set, cm.allStores.storeSet...)
		return
	}
	// Optimize for a single conjunction, by using set directly in the call
	// to constrainStoresForConjunction.
	var scratch storeSet
	scratchPtr := set
	for i := range expr {
		cm.constrainStoresForConjunction(expr[i], scratchPtr)
		if len(*scratchPtr) == 0 {
			continue
		}
		if scratchPtr != set {
			set.union(*scratchPtr)
		} else {
			// The set contains the first non-empty set. Collect the remaining
			// sets in scratch.
			scratchPtr = &scratch
		}
	}
}

// Avoid unused lint errors.

var _ = (&constraintMatcher{}).setStore
var _ = (&constraintMatcher{}).removeStore
var _ = (&constraintMatcher{}).checkConsistency
