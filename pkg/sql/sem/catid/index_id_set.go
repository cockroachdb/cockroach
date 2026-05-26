// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catid

import "github.com/cockroachdb/cockroach/pkg/util/intsets"

// IndexSet efficiently stores an unordered set of index ids.
type IndexSet struct {
	set intsets.Fast
}

// MakeIndexIDSet returns a set initialized with the given values.
func MakeIndexIDSet(vals ...IndexID) IndexSet {
	var res IndexSet
	for _, v := range vals {
		res.Add(v)
	}
	return res
}

// Add adds an index to the set. No-op if the index is already in the set.
func (s *IndexSet) Add(indexID IndexID) { s.set.Add(int(indexID)) }

// Contains returns true if the set contains the index.
func (s IndexSet) Contains(indexID IndexID) bool { return s.set.Contains(int(indexID)) }

// Empty returns true if the set is empty.
func (s IndexSet) Empty() bool { return s.set.Empty() }

// Len returns the number of the indexes in the set.
func (s IndexSet) Len() int { return s.set.Len() }

// Next returns the first value in the set which is >= startVal. If there is no
// value, the second return value is false.
func (s IndexSet) Next(startVal IndexID) (IndexID, bool) {
	c, ok := s.set.Next(int(startVal))
	return IndexID(c), ok
}

// ForEach calls a function for each index in the set (in increasing order).
func (s IndexSet) ForEach(f func(col IndexID)) {
	s.set.ForEach(func(i int) { f(IndexID(i)) })
}

// SubsetOf returns true if s is a subset of other.
func (s IndexSet) SubsetOf(other IndexSet) bool {
	return s.set.SubsetOf(other.set)
}

// Intersection returns the intersection between s and other.
func (s IndexSet) Intersection(other IndexSet) IndexSet {
	return IndexSet{set: s.set.Intersection(other.set)}
}

// Difference returns the index IDs in s which are not in other.
func (s IndexSet) Difference(other IndexSet) IndexSet {
	return IndexSet{set: s.set.Difference(other.set)}
}

// Ordered returns a slice with all the IndexIDs in the set, in
// increasing order.
func (s IndexSet) Ordered() []IndexID {
	if s.Empty() {
		return nil
	}
	result := make([]IndexID, 0, s.Len())
	s.ForEach(func(i IndexID) {
		result = append(result, i)
	})
	return result
}

// UnionWith adds all the indexes from rhs to this set.
func (s *IndexSet) UnionWith(rhs IndexSet) { s.set.UnionWith(rhs.set) }

// String returns a list representation of elements. Sequential runs of positive
// numbers are shown as ranges. For example, for the set {1, 2, 3  5, 6, 10},
// the output is "(1-3,5,6,10)".
func (s IndexSet) String() string { return s.set.String() }
