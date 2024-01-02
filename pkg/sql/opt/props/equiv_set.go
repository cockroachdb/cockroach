// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package props

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
)

// EquivSet describes a set of equivalence groups of columns. It can answer
// queries about which columns are equivalent to one another. Equivalence groups
// are always non-empty and disjoint.
type EquivSet struct {
	// groups contains the equiv groups in no particular order. It should never be
	// accessed outside the EquivSet methods. Each ColSet should be considered
	// immutable once it becomes part of the groups slice. To update an equiv
	// group, replace it with a new ColSet.
	groups []opt.ColSet
}

// Reset prepares the EquivSet for reuse, maintaining references to any
// allocated slice memory.
func (eq *EquivSet) Reset() {
	for i := range eq.groups {
		// Release any references to the large portion of ColSets.
		eq.groups[i] = opt.ColSet{}
	}
	eq.groups = eq.groups[:0]
}

// AddFromFDs adds all equivalence relations from the given FuncDepSet to the
// EquivSet.
func (eq *EquivSet) AddFromFDs(fdset *FuncDepSet) {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	for i := range fdset.equiv.groups {
		// No copy is necessary because the equiv groups are immutable.
		eq.addNoCopy(fdset.equiv.groups[i])
	}
}

// AreColsEquiv indicates whether the given columns are equivalent.
func (eq *EquivSet) AreColsEquiv(left, right opt.ColumnID) bool {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	if left == right {
		return true
	}
	for i := range eq.groups {
		if eq.groups[i].Contains(left) {
			return eq.groups[i].Contains(right)
		}
		if eq.groups[i].Contains(right) {
			return eq.groups[i].Contains(left)
		}
	}
	return false
}

// Empty returns true if the set stores no equalities.
func (eq *EquivSet) empty() bool {
	return len(eq.groups) == 0
}

// Count returns the number of equiv groups stored in the set.
func (eq *EquivSet) count() int {
	return len(eq.groups)
}

// get returns the equiv group at the given index. The returned ColSet should be
// considered immutable.
func (eq *EquivSet) get(idx int) opt.ColSet {
	return eq.groups[idx]
}

// areAllColsEquiv returns true if all columns in the given set are equivalent
// to all others in the set.
func (eq *EquivSet) areAllColsEquiv(cols opt.ColSet) bool {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	if cols.Len() <= 1 {
		return true
	}
	for i := range eq.groups {
		if eq.groups[i].Intersects(cols) {
			return cols.SubsetOf(eq.groups[i])
		}
	}
	return false
}

// computeEquivClosureNoCopy returns the equivalence closure of the given
// columns. Note that the given ColSet is mutated and returned directly.
func (eq *EquivSet) computeEquivClosureNoCopy(cols opt.ColSet) opt.ColSet {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	for i := range eq.groups {
		if eq.groups[i].Intersects(cols) {
			cols.UnionWith(eq.groups[i])
			if cols.Len() == eq.groups[i].Len() {
				// Since we just took the union, equal lengths means all columns in cols
				// were within the same equivalence group, so we can short-circuit.
				break
			}
		}
	}
	return cols
}

// add adds the given equivalent columns to the EquivSet. If possible, the
// columns are added to an existing group. Otherwise, a new one is created.
// NOTE: the given ColSet may be added to the EquivSet without being copied, so
// it must be considered immutable after it is passed to addNoCopy.
func (eq *EquivSet) addNoCopy(equivCols opt.ColSet) {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	if equivCols.Len() <= 1 {
		// This is a trivial equivalence.
		return
	}
	// Attempt to add the equivalence to an existing group.
	for i := range eq.groups {
		if eq.groups[i].Intersects(equivCols) {
			if equivCols.SubsetOf(eq.groups[i]) {
				// The equivalence is already contained in the set.
				return
			}
			if eq.groups[i].SubsetOf(equivCols) {
				// Avoid the copy.
				eq.groups[i] = equivCols
			} else {
				eq.groups[i] = eq.groups[i].Union(equivCols)
			}
			eq.tryMergeGroups(i)
			return
		}
	}
	// Make a new equivalence group.
	eq.groups = append(eq.groups, equivCols)
}

// tryMergeGroups attempts to merge the equality group at the given index with
// any of the *following* groups. If a group can be merged, it is removed after
// its columns are added to the given group.
func (eq *EquivSet) tryMergeGroups(idx int) {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	for i := idx + 1; i < len(eq.groups); i++ {
		if eq.groups[idx].Intersects(eq.groups[i]) {
			eq.groups[idx] = eq.groups[idx].Union(eq.groups[i])
			eq.groups[i] = eq.groups[len(eq.groups)-1]
			eq.groups[len(eq.groups)-1] = opt.ColSet{}
			eq.groups = eq.groups[:len(eq.groups)-1]
		}
	}
}

// copyFrom copies the given EquivSet into this EquivSet, replacing any existing
// data.
func (eq *EquivSet) copyFrom(other *EquivSet) {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	eq.Reset()
	eq.appendFrom(other)
}

// appendFrom unions the equiv groups from the given EquivSet with this one,
// assuming the groups are disjoint.
func (eq *EquivSet) appendFrom(other *EquivSet) {
	if buildutil.CrdbTestBuild {
		other.verify()
		defer eq.verify()
	}
	neededCap := len(eq.groups) + len(other.groups)
	if cap(eq.groups) < neededCap {
		// Make sure to copy the old equiv groups into the new slice.
		newGroups := make([]opt.ColSet, len(eq.groups), neededCap)
		copy(newGroups, eq.groups)
		eq.groups = newGroups
	}
	// There is no need to deep-copy the equiv groups, since they are never
	// modified in-place.
	eq.groups = append(eq.groups, other.groups...)
}

// translateCols remaps the column IDs of each equiv group according to the
// given "from" and "to" lists.
func (eq *EquivSet) translateCols(fromCols, toCols opt.ColList) {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	for i := range eq.groups {
		eq.groups[i] = opt.TranslateColSet(eq.groups[i], fromCols, toCols)
	}
}

// projectCols removes all columns from the EquivSet that are not in the given
// ColSet, removing equiv groups that become empty.
func (eq *EquivSet) projectCols(cols opt.ColSet) {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	for i := range eq.groups {
		if !eq.groups[i].SubsetOf(cols) {
			eq.groups[i] = eq.groups[i].Intersection(cols)
		}
	}
	eq.removeTrivialGroups()
}

// makePartition divides the equiv groups according to the given columns. If an
// equiv group intersects the given ColSet but is not a subset, it is split into
// the intersection and difference with the given ColSet.
func (eq *EquivSet) makePartition(cols opt.ColSet) {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	for i := len(eq.groups) - 1; i >= 0; i-- {
		if eq.groups[i].Intersects(cols) && !eq.groups[i].SubsetOf(cols) {
			// This group references both sides of the join, so split it.
			left, right := eq.groups[i].Intersection(cols), eq.groups[i].Difference(cols)
			eq.groups[i] = opt.ColSet{}
			if left.Len() > 1 {
				eq.groups = append(eq.groups, left)
			}
			if right.Len() > 1 {
				eq.groups = append(eq.groups, right)
			}
		}
	}
	eq.removeTrivialGroups()
}

// removeTrivialGroups removes groups with zero or one columns, which may be
// added by methods like makePartition.
func (eq *EquivSet) removeTrivialGroups() {
	for i := len(eq.groups) - 1; i >= 0; i-- {
		if eq.groups[i].Len() <= 1 {
			eq.groups[i] = eq.groups[len(eq.groups)-1]
			eq.groups[len(eq.groups)-1] = opt.ColSet{}
			eq.groups = eq.groups[:len(eq.groups)-1]
		}
	}
}

// testOnlySetGroup is used to set the equiv group at a particular index. It is
// only allowed during tests.
func (eq *EquivSet) testOnlySetGroup(idx int, newGroup opt.ColSet) {
	eq.groups[idx] = newGroup
}

func (eq *EquivSet) verify() {
	var seen opt.ColSet
	for _, group := range eq.groups {
		if group.Len() <= 1 {
			panic(errors.AssertionFailedf("expected non-trivial equiv group"))
		}
		if seen.Intersects(group) {
			panic(errors.AssertionFailedf("expected non-intersecting equiv groups"))
		}
		seen.UnionWith(group)
	}
}

func (eq *EquivSet) String() string {
	ret := "["
	for i := range eq.groups {
		if i > 0 {
			ret += ", "
		}
		ret += eq.groups[i].String()
	}
	return ret + "]"
}
