// Copyright 2020 The Cockroach Authors.
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

// DescriptorIDSet efficiently stores an unordered set of descriptor ids.
type DescriptorIDSet struct {
	set util.FastIntSet
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
