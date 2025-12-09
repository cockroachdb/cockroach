// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catid

import "github.com/cockroachdb/cockroach/pkg/util/intsets"

// ConstraintSet efficiently stores an unordered set of constraint ids.
type ConstraintSet struct {
	set intsets.Fast
}

// MakeConstraintIDSet returns a set initialized with the given values.
func MakeConstraintIDSet(vals ...ConstraintID) ConstraintSet {
	var res ConstraintSet
	for _, v := range vals {
		res.Add(v)
	}
	return res
}

// Add adds a constraint to the set. No-op if the constraint is already in the set.
func (s *ConstraintSet) Add(constraintID ConstraintID) { s.set.Add(int(constraintID)) }

// Contains returns true if the set contains the constraint.
func (s ConstraintSet) Contains(constraintID ConstraintID) bool {
	return s.set.Contains(int(constraintID))
}

// Empty returns true if the set is empty.
func (s ConstraintSet) Empty() bool { return s.set.Empty() }

// Len returns the number of the constraints in the set.
func (s ConstraintSet) Len() int { return s.set.Len() }

// Next returns the first value in the set which is >= startVal. If there is no
// value, the second return value is false.
func (s ConstraintSet) Next(startVal ConstraintID) (ConstraintID, bool) {
	c, ok := s.set.Next(int(startVal))
	return ConstraintID(c), ok
}

// ForEach calls a function for each constraint in the set (in increasing order).
func (s ConstraintSet) ForEach(f func(col ConstraintID)) {
	s.set.ForEach(func(i int) { f(ConstraintID(i)) })
}

// SubsetOf returns true if s is a subset of other.
func (s ConstraintSet) SubsetOf(other ConstraintSet) bool {
	return s.set.SubsetOf(other.set)
}

// Intersection returns the intersection between s and other.
func (s ConstraintSet) Intersection(other ConstraintSet) ConstraintSet {
	return ConstraintSet{set: s.set.Intersection(other.set)}
}

// Difference returns the constraint IDs in s which are not in other.
func (s ConstraintSet) Difference(other ConstraintSet) ConstraintSet {
	return ConstraintSet{set: s.set.Difference(other.set)}
}

// Ordered returns a slice with all the ConstraintIDs in the set, in
// increasing order.
func (s ConstraintSet) Ordered() []ConstraintID {
	if s.Empty() {
		return nil
	}
	result := make([]ConstraintID, 0, s.Len())
	s.ForEach(func(i ConstraintID) {
		result = append(result, i)
	})
	return result
}

// UnionWith adds all the constraints from rhs to this set.
func (s *ConstraintSet) UnionWith(rhs ConstraintSet) { s.set.UnionWith(rhs.set) }

// String returns a list representation of elements. Sequential runs of positive
// numbers are shown as ranges. For example, for the set {1, 2, 3  5, 6, 10},
// the output is "(1-3,5,6,10)".
func (s ConstraintSet) String() string { return s.set.String() }
