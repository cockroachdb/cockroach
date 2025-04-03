// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mma

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
	matched    map[internedConstraint]struct{}
	descriptor roachpb.StoreDescriptor
}

type matchedSet struct {
	storeIDPostingList
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
func (cm *constraintMatcher) setStore(store roachpb.StoreDescriptor) {
	mc := cm.stores[store.StoreID]
	if mc == nil {
		mc = &matchedConstraints{
			matched: map[internedConstraint]struct{}{},
		}
		cm.stores[store.StoreID] = mc
		cm.allStores.insert(store.StoreID)
	}
	mc.descriptor = store
	// Update the matching info for the existing constraints.
	for c, matchedSet := range cm.constraints {
		matches := cm.storeMatchesConstraint(store, c)
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
	cm.allStores.remove(storeID)
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

// storeMatchesConstraint is an internal helper method.
func (cm *constraintMatcher) storeMatchesConstraint(
	store roachpb.StoreDescriptor, c internedConstraint,
) bool {
	matches := false
	if c.key == emptyStringCode {
		for _, attrs := range []roachpb.Attributes{store.Attrs, store.Node.Attrs} {
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
		for _, tier := range store.Node.Locality.Tiers {
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
			if cm.storeMatchesConstraint(mc.descriptor, c) {
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
// TODO(sumeer): make storeIDPostingList a struct and use a sync.Pool.
func (cm *constraintMatcher) constrainStoresForConjunction(
	constraints []internedConstraint, storeSet *storeIDPostingList,
) {
	*storeSet = (*storeSet)[:0]
	if len(constraints) == 0 {
		*storeSet = append(*storeSet, cm.allStores.storeIDPostingList...)
		return
	}
	for i := range constraints {
		matchedSet := cm.getMatchedSetForConstraint(constraints[i])
		if i == 0 {
			*storeSet = append(*storeSet, matchedSet.storeIDPostingList...)
		} else {
			storeSet.intersect(matchedSet.storeIDPostingList)
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
func (cm *constraintMatcher) constrainStoresForExpr(
	expr constraintsDisj, storeSet *storeIDPostingList,
) {
	if len(expr) == 0 {
		*storeSet = append(*storeSet, cm.allStores.storeIDPostingList...)
		return
	}
	// Optimize for a single conjunction, by using storeSet directly in the call
	// to constrainStoresForConjunction.
	var scratch storeIDPostingList
	scratchPtr := storeSet
	for i := range expr {
		cm.constrainStoresForConjunction(expr[i], scratchPtr)
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

func (cm *constraintMatcher) checkConsistency() error {
	for storeID, mc := range cm.stores {
		for c := range mc.matched {
			pl, ok := cm.constraints[c]
			if !ok {
				return errors.AssertionFailedf("constraint not found")
			}
			if !pl.contains(storeID) {
				return errors.AssertionFailedf("constraint set does not include storeID %d", storeID)
			}
		}
	}
	for c, pl := range cm.constraints {
		for _, storeID := range pl.storeIDPostingList {
			store, ok := cm.stores[storeID]
			if !ok {
				return errors.AssertionFailedf("constraint set mentions unknown storeID %d", storeID)
			}
			_, ok = store.matched[c]
			if !ok {
				return errors.AssertionFailedf("stores and constraints map are out of sync")
			}
		}
	}
	return nil
}

// Avoid unused lint errors.

var _ = (&constraintMatcher{}).setStore
var _ = (&constraintMatcher{}).removeStore
var _ = (&constraintMatcher{}).checkConsistency
