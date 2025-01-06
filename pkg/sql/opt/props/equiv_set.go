// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package props

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
)

// EquivGroups describes a set of equivalence groups of columns. It can answer
// queries about which columns are equivalent to one another. Equivalence groups
// are always non-empty and disjoint.
type EquivGroups struct {
	// groups contains the equiv groups in no particular order. It should never be
	// accessed outside the EquivSet methods. Each ColSet should be considered
	// immutable once it becomes part of the groups slice. To update an equiv
	// group, replace it with a new ColSet.
	groups []opt.ColSet
}

// Reset prepares the EquivGroups for reuse, maintaining references to any
// allocated slice memory.
func (eq *EquivGroups) Reset() {
	for i := range eq.groups {
		// Release any references to the large portion of ColSets.
		eq.groups[i] = opt.ColSet{}
	}
	eq.groups = eq.groups[:0]
}

// Empty returns true if the set stores no equalities.
func (eq *EquivGroups) Empty() bool {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	return len(eq.groups) == 0
}

// GroupCount returns the number of equiv groups stored in the set.
func (eq *EquivGroups) GroupCount() int {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	return len(eq.groups)
}

// Group returns the equiv group at the given index. The returned ColSet should
// be considered immutable. The index must be less than GroupCount().
func (eq *EquivGroups) Group(idx int) opt.ColSet {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	if idx >= len(eq.groups) {
		panic(errors.AssertionFailedf("invalid equiv group index %d", idx))
	}
	return eq.groups[idx]
}

// GroupForCol returns the group of columns equivalent to the given column. It
// returns the empty set if no such group exists. The returned ColSet should not
// be mutated without being copied first.
func (eq *EquivGroups) GroupForCol(col opt.ColumnID) opt.ColSet {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	for i := range eq.groups {
		if eq.groups[i].Contains(col) {
			return eq.groups[i]
		}
	}
	return opt.ColSet{}
}

// ContainsCol returns true if the given column is contained in any of the equiv
// groups (it will be in at most one group).
func (eq *EquivGroups) ContainsCol(col opt.ColumnID) bool {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	for i := range eq.groups {
		if eq.groups[i].Contains(col) {
			return true
		}
	}
	return false
}

// AreColsEquiv indicates whether the given columns are equivalent.
func (eq *EquivGroups) AreColsEquiv(left, right opt.ColumnID) bool {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	if left == right {
		return true
	}
	for i := range eq.groups {
		containsLeft, containsRight := eq.groups[i].Contains(left), eq.groups[i].Contains(right)
		if containsLeft || containsRight {
			return containsLeft && containsRight
		}
	}
	return false
}

// AreAllColsEquiv returns true if all columns in the given set are equivalent
// to all others in the set.
func (eq *EquivGroups) AreAllColsEquiv(cols opt.ColSet) bool {
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

// AreAllColsEquiv2 returns true if all columns in both the given sets are
// equivalent to all others. It is provided in addition to AreAllColsEquiv so
// that two sets can be tested without having to union them first.
func (eq *EquivGroups) AreAllColsEquiv2(colsA, colsB opt.ColSet) bool {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	lenA, lenB := colsA.Len(), colsB.Len()
	if lenA+lenB <= 1 {
		return true
	}
	if lenA == 1 && lenB == 1 {
		// Two single-columns sets with the same column are always equivalent.
		if colsA.SingleColumn() == colsB.SingleColumn() {
			return true
		}
	}
	for i := range eq.groups {
		if eq.groups[i].Intersects(colsA) || eq.groups[i].Intersects(colsB) {
			return colsA.SubsetOf(eq.groups[i]) && colsB.SubsetOf(eq.groups[i])
		}
	}
	return false
}

// ComputeEquivClosureNoCopy returns the equivalence closure of the given
// columns. Note that the given ColSet is mutated and returned directly.
func (eq *EquivGroups) ComputeEquivClosureNoCopy(cols opt.ColSet) opt.ColSet {
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

// AddNoCopy adds the given equivalent columns to the EquivGroups. If possible,
// the columns are added to an existing group. Otherwise, a new one is created.
// NOTE: the given ColSet may be added to the EquivGroups without being copied,
// so it must be considered immutable after it is passed to addNoCopy.
//
// AddNoCopy returns the equiv group to which the given columns were added.
func (eq *EquivGroups) AddNoCopy(equivCols opt.ColSet) opt.ColSet {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	if equivCols.Len() <= 1 {
		// This is a trivial equivalence.
		return opt.ColSet{}
	}
	// Attempt to add the equivalence to an existing group.
	for i := range eq.groups {
		if eq.groups[i].Intersects(equivCols) {
			if equivCols.SubsetOf(eq.groups[i]) {
				// The equivalence is already contained in the set.
				return eq.groups[i]
			}
			if eq.groups[i].SubsetOf(equivCols) {
				// Avoid the copy.
				eq.groups[i] = equivCols
			} else {
				eq.groups[i] = eq.groups[i].Union(equivCols)
			}
			eq.tryMergeGroups(i)
			return eq.groups[i]
		}
	}
	// Make a new equivalence group.
	eq.groups = append(eq.groups, equivCols)
	return eq.groups[len(eq.groups)-1]
}

// AddFromFDs adds all equivalence relations from the given FuncDepSet to the
// EquivGroups.
func (eq *EquivGroups) AddFromFDs(fdset *FuncDepSet) {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	for i := range fdset.equiv.groups {
		// It's safe to not copy here because the equiv groups are immutable.
		eq.AddNoCopy(fdset.equiv.groups[i])
	}
}

// TranslateColsStrict remaps the column IDs of each equiv group according to
// the given "from" and "to" lists. It requires that all columns in each group
// are present in the "from" list, and that the "from" and "to" lists are the
// same length.
func (eq *EquivGroups) TranslateColsStrict(fromCols, toCols opt.ColList) {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	// It is possible that the same column shows up more than once in either of
	// the lists. In other words, a column can map to more than one column, and
	// two different columns can map to the same column. The former is handled
	// by TranslateColSetStrict, and may add a column to an equiv group. The
	// latter can merge two equiv groups, so we need to handle it here.
	var seenCols, dupCols opt.ColSet
	for _, toCol := range toCols {
		if seenCols.Contains(toCol) && !dupCols.Contains(toCol) {
			var equiv opt.ColSet
			for i, fromCol := range fromCols {
				if toCols[i] == toCol {
					equiv.Add(fromCol)
				}
			}
			eq.AddNoCopy(equiv)
			dupCols.Add(toCol)
		}
		seenCols.Add(toCol)
	}
	for i := range eq.groups {
		eq.groups[i] = opt.TranslateColSetStrict(eq.groups[i], fromCols, toCols)
	}
	// Handle the case when multiple "in" columns map to the same "out" column,
	// which could result in removal of an equiv group.
	eq.removeTrivialGroups()
}

// ProjectCols removes all columns from the EquivGroups that are not in the
// given ColSet, removing equiv groups that become empty.
func (eq *EquivGroups) ProjectCols(cols opt.ColSet) {
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

// PartitionBy divides the equiv groups according to the given columns. If an
// equiv group intersects the given ColSet but is not a subset, it is split into
// the intersection and difference with the given ColSet. Ex:
//
//	eq := [(1-3), (4-8), (9-12)]
//	eq.PartitionBy(1,5,6)
//	eq == [(2,3), (4,7,8), (5,6), (9-12)]
//
// * In the example, the (1-3) group is split into (1) and (2,3). Since the (1)
// group only has a single column, it is discarded as a trivial equivalence.
// * The (4-8) group is split into (4,7,8) and (5,6). Since both subsets have at
// least two columns, both are kept in the EquivGroups.
// * Finally, the (9-12) group does not intersect the given cols, and so is not
// split.
func (eq *EquivGroups) PartitionBy(cols opt.ColSet) {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	for i := len(eq.groups) - 1; i >= 0; i-- {
		if eq.groups[i].Intersects(cols) && !eq.groups[i].SubsetOf(cols) {
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

// CopyFrom copies the given EquivGroups into this EquivGroups, replacing any
// existing data.
func (eq *EquivGroups) CopyFrom(other *EquivGroups) {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	eq.Reset()
	eq.AppendFromDisjoint(other)
}

// AppendFromDisjoint unions the equiv groups from the given EquivGroups with
// this one. The given EquivGroups *must* be disjoint from this one, or the
// result will be incorrect.
func (eq *EquivGroups) AppendFromDisjoint(other *EquivGroups) {
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

func (eq *EquivGroups) String() string {
	if buildutil.CrdbTestBuild {
		defer eq.verify()
	}
	ret := "["
	for i := range eq.groups {
		if i > 0 {
			ret += ", "
		}
		ret += eq.groups[i].String()
	}
	return ret + "]"
}

// tryMergeGroups attempts to merge the equality group at the given index with
// any of the *following* groups. If a group can be merged, it is removed after
// its columns are added to the given group.
func (eq *EquivGroups) tryMergeGroups(idx int) {
	for i := len(eq.groups) - 1; i > idx; i-- {
		if eq.groups[idx].Intersects(eq.groups[i]) {
			eq.groups[idx] = eq.groups[idx].Union(eq.groups[i])
			eq.groups[i] = eq.groups[len(eq.groups)-1]
			eq.groups[len(eq.groups)-1] = opt.ColSet{}
			eq.groups = eq.groups[:len(eq.groups)-1]
		}
	}
}

// removeTrivialGroups removes groups with zero or one columns, which may be
// added by methods like makePartition.
func (eq *EquivGroups) removeTrivialGroups() {
	for i := len(eq.groups) - 1; i >= 0; i-- {
		if eq.groups[i].Len() <= 1 {
			eq.groups[i] = eq.groups[len(eq.groups)-1]
			eq.groups[len(eq.groups)-1] = opt.ColSet{}
			eq.groups = eq.groups[:len(eq.groups)-1]
		}
	}
}

// verify asserts that the EquivGroups invariants are maintained. It should only
// be used in test builds, and should be sprinkled even in read-only methods to
// catch cases where a ColSet (e.g. one returned by Group()) is incorrectly
// modified.
func (eq *EquivGroups) verify() {
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
