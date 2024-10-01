// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package intsets

// oracle implements the same API as Sparse using a map. It is only used for
// testing.
type oracle struct {
	m map[int]struct{}
}

// Clear empties the set.
func (o *oracle) Clear() {
	o.m = nil
}

// Add adds an integer to the set.
func (o *oracle) Add(i int) {
	if o.m == nil {
		o.m = make(map[int]struct{})
	}
	o.m[i] = struct{}{}
}

// Remove removes an integer from the set.
func (o *oracle) Remove(i int) {
	delete(o.m, i)
}

// Contains returns true if the set contains the given integer.
func (o oracle) Contains(i int) bool {
	_, ok := o.m[i]
	return ok
}

// Empty returns true if the set contains no integers.
func (o oracle) Empty() bool {
	return len(o.m) == 0
}

// Len returns the number of integers in the set.
func (o oracle) Len() int {
	return len(o.m)
}

// LowerBound returns the smallest element >= startVal, or MaxInt if there is no
// such element.
func (o *oracle) LowerBound(startVal int) int {
	lb := MaxInt
	for i := range o.m {
		if i >= startVal && i < lb {
			lb = i
		}
	}
	return lb
}

// Min returns the minimum value in the set. If the set is empty, MaxInt is
// returned.
func (o *oracle) Min() int {
	return o.LowerBound(MinInt)
}

// Copy sets the receiver to a copy of rhs, which can then be modified
// independently.
func (o *oracle) Copy(rhs *oracle) {
	o.Clear()
	for i := range rhs.m {
		o.Add(i)
	}
}

// UnionWith adds all the elements from rhs to this set.
func (o *oracle) UnionWith(rhs *oracle) {
	for i := range rhs.m {
		o.Add(i)
	}
}

// IntersectionWith removes any elements not in rhs from this set.
func (o *oracle) IntersectionWith(rhs *oracle) {
	for i := range o.m {
		if !rhs.Contains(i) {
			o.Remove(i)
		}
	}
}

// Intersects returns true if s has any elements in common with rhs.
func (o *oracle) Intersects(rhs *oracle) bool {
	for i := range o.m {
		if rhs.Contains(i) {
			return true
		}
	}
	return false
}

// DifferenceWith removes any elements in rhs from this set.
func (o *oracle) DifferenceWith(rhs *oracle) {
	for i := range rhs.m {
		o.Remove(i)
	}
}

// Equals returns true if the two sets are identical.
func (o *oracle) Equals(rhs *oracle) bool {
	if len(o.m) != len(rhs.m) {
		return false
	}
	for i := range o.m {
		if !rhs.Contains(i) {
			return false
		}
	}
	for i := range rhs.m {
		if !o.Contains(i) {
			return false
		}
	}
	return true
}

// SubsetOf returns true if rhs contains all the elements in s.
func (o *oracle) SubsetOf(rhs *oracle) bool {
	for i := range o.m {
		if !rhs.Contains(i) {
			return false
		}
	}
	return true
}
