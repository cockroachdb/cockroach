// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// IndexIDSet efficiently stores an unordered set of index ids.
type IndexIDSet struct {
	set util.FastIntSet
}

// MakeIndexIDSet returns a set initialized with the given values.
func MakeIndexIDSet(vals ...descpb.IndexID) IndexIDSet {
	var res IndexIDSet
	for _, v := range vals {
		res.Add(v)
	}
	return res
}

// Add adds an index to the set. No-op if the index is already in the set.
func (s *IndexIDSet) Add(col descpb.IndexID) { s.set.Add(int(col)) }

// Contains returns true if the set contains the index.
func (s IndexIDSet) Contains(col descpb.IndexID) bool { return s.set.Contains(int(col)) }

// Empty returns true if the set is empty.
func (s IndexIDSet) Empty() bool { return s.set.Empty() }

// Len returns the number of the indexes in the set.
func (s IndexIDSet) Len() int { return s.set.Len() }

// Next returns the first value in the set which is >= startVal. If there is no
// value, the second return value is false.
func (s IndexIDSet) Next(startVal descpb.IndexID) (descpb.IndexID, bool) {
	c, ok := s.set.Next(int(startVal))
	return descpb.IndexID(c), ok
}

// ForEach calls a function for each index in the set (in increasing order).
func (s IndexIDSet) ForEach(f func(col descpb.IndexID)) {
	s.set.ForEach(func(i int) { f(descpb.IndexID(i)) })
}

// SubsetOf returns true if s is a subset of other.
func (s IndexIDSet) SubsetOf(other IndexIDSet) bool {
	return s.set.SubsetOf(other.set)
}

// Intersection returns the intersection between s and other.
func (s IndexIDSet) Intersection(other IndexIDSet) IndexIDSet {
	return IndexIDSet{set: s.set.Intersection(other.set)}
}

// Difference returns the index IDs in s which are not in other.
func (s IndexIDSet) Difference(other IndexIDSet) IndexIDSet {
	return IndexIDSet{set: s.set.Difference(other.set)}
}

// Ordered returns a slice with all the descpb.IndexIDs in the set, in
// increasing order.
func (s IndexIDSet) Ordered() []descpb.IndexID {
	if s.Empty() {
		return nil
	}
	result := make([]descpb.IndexID, 0, s.Len())
	s.ForEach(func(i descpb.IndexID) {
		result = append(result, i)
	})
	return result
}

// UnionWith adds all the indexes from rhs to this set.
func (s *IndexIDSet) UnionWith(rhs IndexIDSet) { s.set.UnionWith(rhs.set) }

// String returns a list representation of elements. Sequential runs of positive
// numbers are shown as ranges. For example, for the set {1, 2, 3  5, 6, 10},
// the output is "(1-3,5,6,10)".
func (s IndexIDSet) String() string { return s.set.String() }
