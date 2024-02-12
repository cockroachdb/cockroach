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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
)

// colOrdMapAllocator is used to allocate colOrdMaps. It must be initialized
// with Init before use.
//
// Allocated maps can be returned to the allocator via the Free method. Freed
// maps will be returned in future calls to Alloc.
//
// WARNING: Do not mix-and-match maps allocated by separate allocators.
type colOrdMapAllocator struct {
	maxCol opt.ColumnID
	freed  []colOrdMap
}

// Init initialized the allocator that can allocate maps that support column IDs
// up to maxCol.
func (a *colOrdMapAllocator) Init(maxCol opt.ColumnID) {
	a.maxCol = maxCol
}

// Alloc returns an empty colOrdMap. It will return a previously freed
// colOrdMap, if one is available.
func (a *colOrdMapAllocator) Alloc() colOrdMap {
	if len(a.freed) == 0 {
		// There are no freed maps, so allocate a new one.
		return newColOrdMap(a.maxCol)
	}
	m := a.freed[len(a.freed)-1]
	a.freed = a.freed[:len(a.freed)-1]
	return m
}

// Copy returns a copy of the given colOrdMap.
func (a *colOrdMapAllocator) Copy(from colOrdMap) colOrdMap {
	m := a.Alloc()
	m.CopyFrom(from)
	return m
}

// Free returns the given map to the allocator for future reuse.
//
// WARNING: Do not use a map once it has been freed.
// WARNING: Do not free a map more than once.
// WARNING: Do not free a map that was allocated by a different allocator.
func (a *colOrdMapAllocator) Free(m colOrdMap) {
	// Check that the map has not already been freed.
	m.Clear()
	a.freed = append(a.freed, m)
}

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
	//
	// TODO(mgartner): It is probably unreasonable to have more than 2^31
	// ordinals in an execution node, so this could be []int32.
	ords []int
	// TODO(mgartner): Explain that this is an approximate.
	ordUpperBound int
}

// newColOrdMap returns a new column mapping that can store column IDs less than
// or equal to maxCol.
func newColOrdMap(maxCol opt.ColumnID) colOrdMap {
	return colOrdMap{
		ords: make([]int, maxCol+1),
	}
}

// Set maps a column to the given ordinal.
func (m *colOrdMap) Set(col opt.ColumnID, ord int) {
	if buildutil.CrdbTestBuild && int(col) >= len(m.ords) {
		panic(errors.AssertionFailedf("column %d exceeds max column of map %d", col, len(m.ords)-1))
	}
	// Bias the ordinal by 1 when adding it to the map.
	ord++
	m.ords[col] = ord
	m.ordUpperBound = max(m.ordUpperBound, ord)
}

// Get returns the current value mapped to key, or (-1, false) if the
// key is unmapped.
func (m colOrdMap) Get(col opt.ColumnID) (ord int, ok bool) {
	if int(col) >= len(m.ords) {
		return -1, false
	}
	ord = m.ords[col]
	if ord == 0 {
		return -1, false
	}
	// Reverse the bias when fetching from the map.
	return ord - 1, true
}

// OrdUpperBound returns the maximum ordinal Set since the map was initialized
// or cleared, or -1 if the map is empty. The returned value is greater than or
// equal to the maximum ordinal currently in the map. This method does not
// return the current maximum ordinal when a larger ordinal previously existed
// in the map. For example:
//
//	var m colOrdMap
//	m.Set(opt.ColumnID(1), 10)
//	m.OrdUpperBound()           // returns 10
//	m.Set(opt.ColumnID(1), 5)
//	m.OrdUpperBound()           // returns 10
//
// By returning the maximum ordinal ever Set, rather than the current maximum
// ordinal, the method neither needs to scan through each key/value pair to find
// the current maximum ordinal, nor keep complex data structures to track it.
func (m colOrdMap) OrdUpperBound() int {
	// Reverse the bias when fetching the max ordinal from the map.
	return m.ordUpperBound - 1
}

// ForEach calls the given function for each column ID and ordinal pair in the
// map.
func (m colOrdMap) ForEach(fn func(col opt.ColumnID, ord int)) {
	for col, ord := range m.ords {
		if ord == 0 {
			continue
		}
		// Reverse the bias when fetching from the map.
		fn(opt.ColumnID(col), ord-1)
	}
}

// CopyFrom copies all entries from the given map, and unsets any column IDs not
// in the given map.
func (m *colOrdMap) CopyFrom(other colOrdMap) {
	if buildutil.CrdbTestBuild && len(m.ords) < len(other.ords) {
		panic(errors.AssertionFailedf("map of size %d is too small to copy from map of size %d",
			len(m.ords), len(other.ords)))
	}
	copy(m.ords, other.ords)
	m.ordUpperBound = other.ordUpperBound
}

// Clear clears the map. The allocated memory is retained for future reuse.
func (m *colOrdMap) Clear() {
	for i := range m.ords {
		m.ords[i] = 0
	}
	m.ordUpperBound = 0
}
