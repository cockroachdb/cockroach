// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package props

import "github.com/cockroachdb/cockroach/pkg/sql/opt"

// EquivSet describes a set of equivalence groups of columns. It can answer
// queries about which columns are equivalent to one another. Equivalence groups
// are always non-empty and disjoint.
//
// TODO(drewk): incorporate EquivSets into FuncDepSets.
type EquivSet struct {
	buf    [equalityBufferSize]opt.ColSet
	groups []opt.ColSet
}

const equalityBufferSize = 1

// NewEquivSet returns a new equality set with a starting capacity of one
// equivalence group. This optimizes for the common case when only one
// equivalence group is stored.
func NewEquivSet() EquivSet {
	set := EquivSet{}
	set.groups = set.buf[:0]
	return set
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

// Add adds the given equivalent columns to the EquivSet. If possible, the
// columns are added to an existing group. Otherwise, a new one is created.
func (eq *EquivSet) Add(equivCols opt.ColSet) {
	// Attempt to add the equivalence to an existing group.
	for i := range eq.groups {
		if eq.groups[i].Intersects(equivCols) {
			if equivCols.SubsetOf(eq.groups[i]) {
				// No-op
				return
			}
			eq.groups[i].UnionWith(equivCols)
			eq.tryMergeGroups(i)
			return
		}
	}
	// Make a new equivalence group.
	eq.groups = append(eq.groups, equivCols.Copy())
}

// AddFromFDs adds all equivalence relations from the given FuncDepSet to the
// EquivSet.
func (eq *EquivSet) AddFromFDs(fdset *FuncDepSet) {
	for i := range fdset.deps {
		fd := &fdset.deps[i]
		if fd.equiv {
			eq.Add(fd.from.Union(fd.to))
		}
	}
}

// AreColsEquiv indicates whether the given columns are equivalent.
func (eq *EquivSet) AreColsEquiv(left, right opt.ColumnID) bool {
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

// tryMergeGroups attempts to merge the equality group at the given index with
// any of the *following* groups. If a group can be merged, it is removed after
// its columns are added to the given group.
func (eq *EquivSet) tryMergeGroups(idx int) {
	for i := idx + 1; i < len(eq.groups); i++ {
		if eq.groups[idx].Intersects(eq.groups[i]) {
			eq.groups[idx].UnionWith(eq.groups[i])
			eq.groups[i] = eq.groups[len(eq.groups)-1]
			eq.groups[len(eq.groups)-1] = opt.ColSet{}
			eq.groups = eq.groups[:len(eq.groups)-1]
		}
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
