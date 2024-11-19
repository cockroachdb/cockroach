// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catalog

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
)

// DescriptorIDSet efficiently stores an unordered set of descriptor ids.
type DescriptorIDSet struct {
	set intsets.Fast
}

// SafeFormat implements SafeFormatter for DescriptorIDSet.
func (d *DescriptorIDSet) SafeFormat(s interfaces.SafePrinter, verb rune) {
	s.SafeString(redact.SafeString(d.String()))
}

// String implement fmt.Stringer for DescriptorIDSet.
func (d *DescriptorIDSet) String() string {
	return d.set.String()
}

// MakeDescriptorIDSet returns a set initialized with the given values.
func MakeDescriptorIDSet(ids ...descpb.ID) DescriptorIDSet {
	s := DescriptorIDSet{}
	for _, id := range ids {
		s.Add(id)
	}
	return s
}

// Suppress the linter.
var _ = MakeDescriptorIDSet

var _ redact.SafeFormatter = (*DescriptorIDSet)(nil)

// Add adds an id to the set. No-op if the id is already in the set.
func (d *DescriptorIDSet) Add(id descpb.ID) {
	d.set.Add(int(id))
}

// Len returns the number of the ids in the set.
func (d DescriptorIDSet) Len() int {
	return d.set.Len()
}

// Contains returns true if the set contains the column.
func (d DescriptorIDSet) Contains(id descpb.ID) bool {
	return d.set.Contains(int(id))
}

// ForEach calls a function for each column in the set (in increasing order).
func (d DescriptorIDSet) ForEach(f func(id descpb.ID)) {
	d.set.ForEach(func(i int) { f(descpb.ID(i)) })
}

// Empty returns true if the set is empty.
func (d DescriptorIDSet) Empty() bool { return d.set.Empty() }

// Ordered returns a slice with all the descpb.IDs in the set, in
// increasing order.
func (d DescriptorIDSet) Ordered() []descpb.ID {
	if d.Empty() {
		return nil
	}
	result := make([]descpb.ID, 0, d.Len())
	d.ForEach(func(i descpb.ID) {
		result = append(result, i)
	})
	return result
}

// Remove removes the ID from the set.
func (d *DescriptorIDSet) Remove(id descpb.ID) {
	d.set.Remove(int(id))
}

// Difference returns the elements of d that are not in o as a new set.
func (d DescriptorIDSet) Difference(o DescriptorIDSet) DescriptorIDSet {
	return DescriptorIDSet{set: d.set.Difference(o.set)}
}

// Union returns the union of d and o as a new set.
func (d DescriptorIDSet) Union(o DescriptorIDSet) DescriptorIDSet {
	return DescriptorIDSet{set: d.set.Union(o.set)}
}

// ConstraintIDSet stores an unordered set of descriptor ids.
type ConstraintIDSet struct {
	set intsets.Fast
}

// MakeConstraintIDSet returns a set initialized with the given values.
func MakeConstraintIDSet(ids ...descpb.ConstraintID) ConstraintIDSet {
	s := ConstraintIDSet{}
	for _, id := range ids {
		s.Add(id)
	}
	return s
}

// Add adds an id to the set. No-op if the id is already in the set.
func (s *ConstraintIDSet) Add(id descpb.ConstraintID) {
	s.set.Add(int(id))
}

// Remove removes the ID from the set.
func (s *ConstraintIDSet) Remove(id descpb.ConstraintID) {
	s.set.Remove(int(id))
}

// Empty returns true if the set is empty.
func (s *ConstraintIDSet) Empty() bool {
	return s.set.Empty()
}

// Len returns the number of ids in the set.
func (s *ConstraintIDSet) Len() int {
	return s.set.Len()
}

// Contains checks if the set contains the given id.
func (s *ConstraintIDSet) Contains(id descpb.ConstraintID) bool {
	return s.set.Contains(int(id))
}

// Ordered returns all ids as a ordered slice.
func (s *ConstraintIDSet) Ordered() []descpb.ConstraintID {
	if s.Empty() {
		return nil
	}
	result := make([]descpb.ConstraintID, 0, s.Len())
	s.set.ForEach(func(i int) {
		result = append(result, descpb.ConstraintID(i))
	})
	return result
}
