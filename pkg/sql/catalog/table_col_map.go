// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catalog

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// TableColMap is a map from descpb.ColumnID to int. It is typically used to
// store a mapping from column id to ordinal position within a row, but can be
// used for any similar purpose.
//
// It stores the mapping for ColumnIDs of the system columns separately since
// those IDs are very large and incur an allocation in util.FastIntMap all the
// time.
type TableColMap struct {
	m util.FastIntMap
	// systemColMap maps all system columns to their values. Columns here are
	// in increasing order of their IDs (in other words, since we started giving
	// out IDs from math.MaxUint32 and are going down, the newer system columns
	// appear here earlier).
	systemColMap [NumSystemColumns]int
	// systemColIsSet indicates whether a value has been set for the
	// corresponding system column in systemColMap (needed in order to
	// differentiate between unset 0 and set 0).
	systemColIsSet [NumSystemColumns]bool
}

// Set maps a key to the given value.
func (s *TableColMap) Set(col descpb.ColumnID, val int) {
	if col < SmallestSystemColumnColumnID {
		s.m.Set(int(col), val)
	} else {
		pos := col - SmallestSystemColumnColumnID
		s.systemColMap[pos] = val
		s.systemColIsSet[pos] = true
	}
}

// Get returns the current value mapped to key, or ok=false if the
// key is unmapped.
func (s *TableColMap) Get(col descpb.ColumnID) (val int, ok bool) {
	if col < SmallestSystemColumnColumnID {
		return s.m.Get(int(col))
	}
	pos := col - SmallestSystemColumnColumnID
	return s.systemColMap[pos], s.systemColIsSet[pos]
}

// GetDefault returns the current value mapped to key, or 0 if the key is
// unmapped.
func (s *TableColMap) GetDefault(col descpb.ColumnID) (val int) {
	if col < SmallestSystemColumnColumnID {
		return s.m.GetDefault(int(col))
	}
	pos := col - SmallestSystemColumnColumnID
	return s.systemColMap[pos]
}

// Len returns the number of keys in the map.
func (s *TableColMap) Len() (val int) {
	l := s.m.Len()
	for _, isSet := range s.systemColIsSet {
		if isSet {
			l++
		}
	}
	return l
}

// ForEach calls the given function for each key/value pair in the map (in
// arbitrary order).
func (s *TableColMap) ForEach(f func(colID descpb.ColumnID, returnIndex int)) {
	s.m.ForEach(func(k, v int) {
		f(descpb.ColumnID(k), v)
	})
	for pos, isSet := range s.systemColIsSet {
		if isSet {
			id := SmallestSystemColumnColumnID + pos
			f(descpb.ColumnID(id), s.systemColMap[pos])
		}
	}
}

// String prints out the contents of the map in the following format:
//
//	map[key1:val1 key2:val2 ...]
//
// The keys are in ascending order.
func (s *TableColMap) String() string {
	var buf bytes.Buffer
	buf.WriteString("map[")
	s.m.ContentsIntoBuffer(&buf)
	first := buf.Len() == len("map[")
	for pos, isSet := range s.systemColIsSet {
		if isSet {
			if !first {
				buf.WriteByte(' ')
			}
			first = false
			id := SmallestSystemColumnColumnID + pos
			fmt.Fprintf(&buf, "%d:%d", id, s.systemColMap[pos])
		}
	}
	buf.WriteByte(']')
	return buf.String()
}
