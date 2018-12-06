// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package props

import "github.com/cockroachdb/cockroach/pkg/sql/opt"

const (
	// initialColStatsCap is the initial number of column statistics that can be
	// stored in a ColStatsMap without triggering allocations.
	initialColStatsCap = 3
)

type prefixID uint32

type colStatKey struct {
	prefix prefixID
	id     opt.ColumnID
}

type colStatVal struct {
	prefix prefixID
	pos    int32
}

// ColStatsMap stores a set of column statistics, each of which is keyed by the
// set of columns over which that statistic is defined. Statistics can be added,
// removed, and efficiently accessed (by opt.ColumnSet key or by ordinal
// position) and enumerated.
//
// Since most expressions have just a few column statistics attached to them,
// ColStatsMap optimizes for this case by storing the first 3 column statistics
// inline. Additional column statistics trigger the creation of a slice to store
// them, as well as a lookup index for efficient lookup by opt.ColSet.
//
// Because opt.ColSet contains a pointer, it is not useful as a map key for fast
// statistic lookup. So instead of directly using opt.ColSet as a key in a Go
// map, ColStatsMap uses a prefix tree index. Each opt.ColSet key is treated as
// a string of ascending opt.ColumnID values that are each hashed by its own
// value plus a prefix id that uniquely identifies the set of smaller values.
// For example, if an opt.ColSet contains (2, 3, 6), then its index looks like:
//
//   (prefix: 0, id: 2)           => (prefix: 1, pos: -1)
//    └── (prefix: 1, id: 3)      => (prefix: 2, pos: -1)
//         └── (prefix: 2, id: 6) => (prefix: 3, pos: 0)
//
// Where pos is the ordinal position of the statistic in ColStatsMap, and pos=-1
// signifies that there is not yet any statistic for that column set. If an
// additional opt.ColSet containing (2, 4) is added to the index, then it shares
// the initial lookup node, but then diverges:
//
//   (prefix: 0, id: 2)           => (prefix: 1, pos: -1)
//    ├── (prefix: 1, id: 3)      => (prefix: 2, pos: -1)
//    │    └── (prefix: 2, id: 6) => (prefix: 3, pos: 0)
//    └── (prefix: 1, id: 4)      => (prefix: 4, pos: 1)
//
// This algorithm can be implemented by a single Go map that uses efficient
// int64 keys and values. It requires O(N) accesses to add and find a column
// statistic, where N is the number of values in the column set key.
type ColStatsMap struct {
	// initial is a small list of inlined column statistics. No allocations are
	// made by ColStatsMap if all column statistics fit here.
	initial [initialColStatsCap]ColumnStatistic

	// other contains spillover column statistics that don't fit into the initial
	// field. If this is used, then the index field will be maintained as well.
	other []ColumnStatistic

	// index implements a prefix tree for fast lookup when there are many stats
	// in the ColStatsMap. It is only maintained when there are more column
	// statistics than can fit into the initial field.
	index map[colStatKey]colStatVal

	// count is the number of column statistics in the ColStatsMap.
	count int

	// unique is an increasing counter that's used to generate a unique id that
	// represents a set of opt.ColumnID values that form a prefix in the tree.
	unique prefixID
}

// Count returns the number of column statistics in the map.
func (m *ColStatsMap) Count() int {
	return m.count
}

// Get returns the nth statistic in the map, by its ordinal position. This
// position is stable across calls to Get or Add (but not RemoveIntersecting).
func (m *ColStatsMap) Get(nth int) *ColumnStatistic {
	if nth < initialColStatsCap {
		return &m.initial[nth]
	}
	return &m.other[nth-initialColStatsCap]
}

// Lookup returns the column statistic indexed by the given column set. If no
// such statistic exists in the map, then ok=false.
func (m *ColStatsMap) Lookup(cols opt.ColSet) (colStat *ColumnStatistic, ok bool) {
	// Scan the inlined statistics if there are only a few statistics in the map.
	if m.count <= initialColStatsCap {
		for i := 0; i < m.count; i++ {
			colStat = &m.initial[i]
			if colStat.Cols.Equals(cols) {
				return colStat, true
			}
		}
		return nil, false
	}

	// Use the prefix tree index to look up the column statistic.
	val := colStatVal{prefix: 0, pos: -1}
	curr := 0
	for {
		curr, ok = cols.Next(curr + 1)
		if !ok {
			// No more columns in set, so consult last value to determine whether
			// a match was located.
			if val.pos == -1 {
				// No stat exists for this column set.
				return nil, false
			}

			// A stat exists, so return it.
			return m.Get(int(val.pos)), true
		}

		// Fetch index entry for next prefix+col combo.
		key := colStatKey{prefix: val.prefix, id: opt.ColumnID(curr)}
		val, ok = m.index[key]
		if !ok {
			// No entry exists, so lookup fails.
			return nil, false
		}
	}
}

// Add ensures that a ColumnStatistic over the given columns is in the map. If
// it does not yet exist in the map, then Add adds a new blank ColumnStatistic
// and returns it, along with added=true. Otherwise, Add returns the existing
// ColumnStatistic with added=false.
func (m *ColStatsMap) Add(cols opt.ColSet) (_ *ColumnStatistic, added bool) {
	// Only add column set if it is not already present in the map.
	colStat, ok := m.Lookup(cols)
	if ok {
		return colStat, false
	}

	if cols.Empty() {
		panic("stats cols should never be empty")
	}

	// Fast path for case where there are only a few stats in the map.
	if m.count < initialColStatsCap {
		colStat = &m.initial[m.count]
		*colStat = ColumnStatistic{Cols: cols}
		m.count++
		return colStat, true
	}

	// Fall back on map with arbitrary number of stats.
	if m.index == nil {
		m.other = make([]ColumnStatistic, 0, initialColStatsCap)

		// Add the initial stats to the index.
		for i := range m.initial {
			m.addToIndex(m.initial[i].Cols, i)
		}
	}
	m.other = append(m.other, ColumnStatistic{Cols: cols})
	colStat = &m.other[m.count-initialColStatsCap]
	m.addToIndex(cols, m.count)
	m.count++
	return colStat, true
}

// RemoveIntersecting scans the set of column statistics in the ColStatsMap and
// removes any that are defined over any of the columns in the given set. For
// example, if the map contains stats for (1), (1,2), and (3), then removing
// (1) would remove the (1) and (1,2) stats from the map.
func (m *ColStatsMap) RemoveIntersecting(cols opt.ColSet) {
	// Iterate over the map, removing any stats that intersect.
	n := 0
	for i := 0; i < m.count; i++ {
		colStat := m.Get(i)
		if colStat.Cols.Intersects(cols) {
			continue
		}

		if n < i {
			*m.Get(n) = *colStat
		}
		n++
	}

	// Update state to reflect any items that were removed.
	if n < m.count {
		m.count = n
		m.index = nil
		if n <= initialColStatsCap {
			m.other = m.other[:0]
		} else {
			m.other = m.other[:n-initialColStatsCap]
			m.rebuildIndex()
		}
	}
}

// Clear empties the map of all column statistics.
func (m *ColStatsMap) Clear() {
	m.count = 0
	m.other = m.other[:0]
	m.index = nil
	m.unique = 0
}

// addToIndex adds the column statistic at the given ordinal position to the
// prefix tree index. The caller must have verified that it does not yet exist
// in the index.
func (m *ColStatsMap) addToIndex(cols opt.ColSet, pos int) {
	if m.index == nil {
		m.index = make(map[colStatKey]colStatVal)
	}

	prefix := prefixID(0)
	prev := 0
	curr, _ := cols.Next(prev)
	for {
		key := colStatKey{prefix: prefix, id: opt.ColumnID(curr)}
		val, ok := m.index[key]
		if ok {
			// Index entry exists, so get its prefix value.
			prefix = val.prefix
		} else {
			// No index entry exists, so create one now with a new prefix value.
			// Initialize the "nth" field to -1, indicating that there is not yet
			// a ColumnStatistic for the prefix of columns.
			m.unique++
			prefix = m.unique
			m.index[key] = colStatVal{prefix: prefix, pos: -1}
		}

		// Get the next column from the set.
		prev = curr
		curr, ok = cols.Next(curr + 1)
		if !ok {
			// Done adding columns, so set the "nth" field to the ordinal position
			// of the ColumnStatistic in the map.
			m.index[key] = colStatVal{prefix: prefix, pos: int32(pos)}
			break
		}
	}
}

// rebuildIndex creates the prefix tree index from scratch.
func (m *ColStatsMap) rebuildIndex() {
	m.index = nil
	for i := 0; i < m.Count(); i++ {
		m.addToIndex(m.Get(i).Cols, i)
	}
}
