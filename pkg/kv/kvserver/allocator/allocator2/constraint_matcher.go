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
//     matches a constraint
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
}

type matchedConstraints struct {
	matched map[internedConstraint]struct{}
}

type matchedSet struct {
	storeIDPostingList
}

func newConstraintMatcher(interner *stringInterner) *constraintMatcher {
	return &constraintMatcher{
		interner:    interner,
		stores:      map[roachpb.StoreID]*matchedConstraints{},
		constraints: map[internedConstraint]*matchedSet{},
	}
}

// setStore is called for a new store, or when the attributes and locality changes.
func (cm *constraintMatcher) setStore(store roachpb.StoreDescriptor) {
	mc := cm.stores[store.StoreID]
	if mc == nil {
		mc = &matchedConstraints{
			matched: map[internedConstraint]struct{}{},
		}
		cm.stores[store.StoreID] = mc
	}
	for c, matchedSet := range cm.constraints {
		matches := cm.storeMatchesConstraint(store, c)
		if c.typ == roachpb.Constraint_PROHIBITED {
			matches = !matches
		}
		_, existingMatch := mc.matched[c]
		if matches == existingMatch {
			continue
		}
		if !existingMatch {
			// Did not match before, but matches now.
			mc.matched[c] = struct{}{}
			notInSet := matchedSet.insert(store.StoreID)
			if !notInSet {
				panic(errors.AssertionFailedf(
					"inconsistent state: store %d already in set", store.StoreID))
			}
		} else if existingMatch {
			// No longer matches.
			delete(mc.matched, c)
			found := matchedSet.remove(store.StoreID)
			if !found {
				panic(errors.AssertionFailedf(
					"inconsistent state: store %d not found", store.StoreID))
			}
		}
	}
}

// removeStore is called for a store that is removed from the cluster.
func (cm *constraintMatcher) removeStore(storeID roachpb.StoreID) {
	mc := cm.stores[storeID]
	if mc == nil {
		return
	}
	delete(cm.stores, storeID)
	for c := range mc.matched {
		matchedSet := cm.constraints[c]
		if matchedSet == nil {
			panic(errors.AssertionFailedf(
				"inconsistent state: store %d not found", storeID))
		}
		found := matchedSet.remove(storeID)
		if !found {
			panic(errors.AssertionFailedf(
				"inconsistent state: store %d not found", storeID))
		}
	}
}

func (cm *constraintMatcher) storeMatchesConstraint(
	store roachpb.StoreDescriptor, c internedConstraint,
) bool {
	if c.key == emptyStringCode {
		for _, attrs := range []roachpb.Attributes{store.Attrs, store.Node.Attrs} {
			for _, attr := range attrs.Attrs {
				if cm.interner.toCode(attr) == c.value {
					return true
				}
			}
		}
		return false
	}
	for _, tier := range store.Node.Locality.Tiers {
		if c.key == cm.interner.toCode(tier.Key) && c.value == cm.interner.toCode(tier.Value) {
			return true
		}
	}
	return false
}

// constrainStoresForConjunction populates storeSet with the stores matching
// the given conjunction of constraints.
//
// TODO(sumeer): make storeIDPostingList a struct and use a sync.Pool.
func (cm *constraintMatcher) constrainStoresForConjunction(
	constraints []internedConstraint, storeSet *storeIDPostingList,
) {
	*storeSet = (*storeSet)[:0]
	for i := range constraints {
		matchedSet := cm.constraints[constraints[i]]
		if matchedSet == nil || len(matchedSet.storeIDPostingList) == 0 {
			*storeSet = (*storeSet)[:0]
			return
		}
		if i == 0 {
			*storeSet = append(*storeSet, matchedSet.storeIDPostingList...)
		} else {
			storeSet.intersect(matchedSet.storeIDPostingList)
			if len(*storeSet) == 0 {
				return
			}
		}
	}
}

var _ storeMatchesConstraintInterface = &constraintMatcher{}

// storeMatches returns whether the given storeID matches the given
// conjunction of constraints.
func (cm *constraintMatcher) storeMatches(
	storeID roachpb.StoreID, constraints []internedConstraint,
) bool {
	mc := cm.stores[storeID]
	if mc == nil {
		return false
	}
	for _, c := range constraints {
		_, ok := mc.matched[c]
		if !ok {
			return false
		}
	}
	return true
}

// constrainStoresForExpr populates storeSet with the stores matching the
// given expression.
func (cm *constraintMatcher) constrainStoresForExpr(
	expr constraintsDisj, storeSet *storeIDPostingList,
) {
	// Optimize for a single conjunction, by using storeSet directly in the call
	// to constrainStoresForConjunction.
	var scratch storeIDPostingList
	scratchPtr := storeSet
	for i := range expr {
		cm.constrainStoresForConjunction(expr[i].constraints, scratchPtr)
		if len(*scratchPtr) == 0 {
			continue
		}
		if scratchPtr != storeSet {
			storeSet.union(*scratchPtr)
		} else {
			// The storeSet contains the first non-empty set. Collect the remaining
			// sets in scratch.
			scratchPtr = &scratch
		}
	}
}

// Avoid unused lint errors.

var _ = (&constraintMatcher{}).setStore
var _ = (&constraintMatcher{}).removeStore
