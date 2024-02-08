// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execbuilder

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/errors"
)

// colOrdMap is a map from column IDs to ordinals.
//
// The map is implemented as a slice of integers, with the slice's indexes
// representing column IDs and the slice's elements representing ordinals. This
// makes Set and Get operations on the map extremely fast.
//
// This implementation does have some drawbacks. First, the map can only store
// column IDs less than or equal to the length of the slice. Column IDs are
// assigned as sequential integers starting at 1, so in most cases the length of
// the slice should be in the hundreds or thousands.
//
// Second, the map currently does not permit resizing, so the maximum column ID
// must be known when the map is initialized. This makes the map suitable for
// use within execbuilder after all column IDs have been assigned, but not
// elsewhere.
//
// Finally, the memory footprint of the map is dependent on the maximum column
// ID it can store, rather than on the number of entries in the map. A map with
// only a few entries has the same memory footprint as a map with every column
// ID set. This can be mitigated by reusing already-allocated maps when
// possible.
type colOrdMap struct {
	// ords is a mapping from column ID to an ordinal. The values are biased by
	// 1, which allows the map to store the zero ordinal and have the zero value
	// in the map represent an unset column ID.
	ords []int32
}

// newColOrdMap returns a new column mapping that can store column IDs less than
// or equal to maxCol.
func newColOrdMap(maxCol opt.ColumnID) colOrdMap {
	return colOrdMap{
		ords: make([]int32, maxCol+1),
	}
}

// Set maps a column to the given ordinal.
func (m *colOrdMap) Set(col opt.ColumnID, ord int) {
	if int(col) >= len(m.ords) {
		panic(errors.AssertionFailedf("column %d exceeds max column of map %d", col, len(m.ords)-1))
	}
	// Bias the ordinal by 1 when adding it to the map.
	ord++
	if ord > math.MaxInt32 {
		panic(errors.AssertionFailedf("ordinal %d exceeds max ordinal %d", ord-1, math.MaxInt32-1))
	}
	m.ords[col] = int32(ord)
}

// Get returns the current value mapped to key, or (-1, false) if the
// key is unmapped.
func (m colOrdMap) Get(col opt.ColumnID) (ord int, ok bool) {
	if int(col) >= len(m.ords) {
		return -1, false
	}
	ord = int(m.ords[col])
	if ord == 0 {
		return -1, false
	}
	// Reverse the bias when fetching from the map.
	return ord - 1, true
}

// MaxOrd returns the maximum ordinal stored in the map, or -1 if the map is
// empty.
func (m colOrdMap) MaxOrd() int {
	maxOrd := -1
	for _, ord := range m.ords {
		if ord == 0 {
			continue
		}
		// Reverse the bias when fetching the max ordinal from the map.
		ord--
		maxOrd = max(maxOrd, int(ord))
	}
	return maxOrd
}

// ForEach calls the given function for each column ID and ordinal pair in the
// map.
func (m colOrdMap) ForEach(fn func(col opt.ColumnID, ord int)) {
	for col, ord := range m.ords {
		if ord == 0 {
			continue
		}
		// Reverse the bias when fetching from the map.
		fn(opt.ColumnID(col), int(ord-1))
	}
}

// CopyFrom copies all entries from the given map, and unsets any column IDs not
// in the given map.
func (m *colOrdMap) CopyFrom(other colOrdMap) {
	if len(m.ords) < len(other.ords) {
		panic(errors.AssertionFailedf("map of size %d is too small to copy from map of size %d",
			len(m.ords), len(other.ords)))
	}
	copy(m.ords, other.ords)
}

// clear clears the map. The allocated memory is retained for future reuse.
func (m *colOrdMap) clear() {
	for i := range m.ords {
		m.ords[i] = 0
	}
}
