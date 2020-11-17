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

// TableColMap is a map from descpb.ColumnID to int. It is typically used to
// store a mapping from column id to ordinal position within a row, but can be
// used for any similar purpose.
type TableColMap struct {
	m util.FastIntMap
}

// Set maps a key to the given value.
func (s *TableColMap) Set(col descpb.ColumnID, val int) { s.m.Set(int(col), val) }

// Get returns the current value mapped to key, or ok=false if the
// key is unmapped.
func (s *TableColMap) Get(col descpb.ColumnID) (val int, ok bool) { return s.m.Get(int(col)) }

// GetDefault returns the current value mapped to key, or 0 if the key is
// unmapped.
func (s *TableColMap) GetDefault(col descpb.ColumnID) (val int) { return s.m.GetDefault(int(col)) }

// Len returns the number of keys in the map.
func (s *TableColMap) Len() (val int) { return s.m.Len() }

// ForEach calls the given function for each key/value pair in the map (in
// arbitrary order).
func (s *TableColMap) ForEach(f func(colID descpb.ColumnID, returnIndex int)) {
	s.m.ForEach(func(k, v int) {
		f(descpb.ColumnID(k), v)
	})
}

// String prints out the contents of the map in the following format:
//   map[key1:val1 key2:val2 ...]
// The keys are in ascending order.
func (s *TableColMap) String() string {
	return s.m.String()
}
