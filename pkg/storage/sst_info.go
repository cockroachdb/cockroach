// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// SSTableInfo contains metadata about a single sstable. Note this mirrors
// the C.DBSSTable struct contents for compatibility with RocksDB.
type SSTableInfo struct {
	Level int
	Size  int64
	Start MVCCKey
	End   MVCCKey
}

// SSTableInfos is a slice of SSTableInfo structures.
type SSTableInfos []SSTableInfo

func (s SSTableInfos) Len() int {
	return len(s)
}

func (s SSTableInfos) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SSTableInfos) Less(i, j int) bool {
	switch {
	case s[i].Level < s[j].Level:
		return true
	case s[i].Level > s[j].Level:
		return false
	case s[i].Size > s[j].Size:
		return true
	case s[i].Size < s[j].Size:
		return false
	default:
		return s[i].Start.Less(s[j].Start)
	}
}

// readAmplification returns the store's worst case read amplification, which is
// the number of levels (other than L0) with at least one sstable, and for
// L0, either the value of `l0Sublevels` if that's >= 0, or the number
// of sstables in L0 in all.
//
// This definition comes from here (minus the sublevel handling, which is a
// Pebble-specific optimization):
// https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#level-style-compaction
func (s SSTableInfos) readAmplification(l0Sublevels int) int64 {
	if l0Sublevels < 0 {
		l0Sublevels = 0
		for i := range s {
			if s[i].Level == 0 {
				l0Sublevels++
			}
		}
	}
	readAmp := int64(l0Sublevels)
	seenLevel := make(map[int]bool)
	for _, t := range s {
		if t.Level > 0 && !seenLevel[t.Level] {
			readAmp++
			seenLevel[t.Level] = true
		}
	}
	return readAmp
}

// SSTableInfosByLevel maintains slices of SSTableInfo objects, one
// per level. The slice for each level contains the SSTableInfo
// objects for SSTables at that level, sorted by start key.
type SSTableInfosByLevel struct {
	// Each level is a slice of SSTableInfos.
	levels [][]SSTableInfo
}

// NewSSTableInfosByLevel returns a new SSTableInfosByLevel object
// based on the supplied SSTableInfos slice.
func NewSSTableInfosByLevel(s SSTableInfos) SSTableInfosByLevel {
	var result SSTableInfosByLevel
	for _, t := range s {
		for i := len(result.levels); i <= t.Level; i++ {
			result.levels = append(result.levels, []SSTableInfo{})
		}
		result.levels[t.Level] = append(result.levels[t.Level], t)
	}
	// Sort each level by start key.
	for _, l := range result.levels {
		sort.Slice(l, func(i, j int) bool { return l[i].Start.Less(l[j].Start) })
	}
	return result
}

// MaxLevel returns the maximum level for which there are SSTables.
func (s *SSTableInfosByLevel) MaxLevel() int {
	return len(s.levels) - 1
}

// MaxLevelSpanOverlapsContiguousSSTables returns the maximum level at
// which the specified key span overlaps either none, one, or at most
// two contiguous SSTables. Level 0 is returned if no level qualifies.
//
// This is useful when considering when to merge two compactions. In
// this case, the method is called with the "gap" between the two
// spans to be compacted. When the result is that the gap span touches
// at most two SSTables at a high level, it suggests that merging the
// two compactions is a good idea (as the up to two SSTables touched
// by the gap span, due to containing endpoints of the existing
// compactions, would be rewritten anyway).
//
// As an example, consider the following sstables in a small database:
//
// Level 0.
//  {Level: 0, Size: 20, Start: key("a"), End: key("z")},
//  {Level: 0, Size: 15, Start: key("a"), End: key("k")},
// Level 2.
//  {Level: 2, Size: 200, Start: key("a"), End: key("j")},
//  {Level: 2, Size: 100, Start: key("k"), End: key("o")},
//  {Level: 2, Size: 100, Start: key("r"), End: key("t")},
// Level 6.
//  {Level: 6, Size: 201, Start: key("a"), End: key("c")},
//  {Level: 6, Size: 200, Start: key("d"), End: key("f")},
//  {Level: 6, Size: 300, Start: key("h"), End: key("r")},
//  {Level: 6, Size: 405, Start: key("s"), End: key("z")},
//
// - The span "a"-"c" overlaps only a single SSTable at the max level
//   (L6). That's great, so we definitely want to compact that.
// - The span "s"-"t" overlaps zero SSTables at the max level (L6).
//   Again, great! That means we're going to compact the 3rd L2
//   SSTable and maybe push that directly to L6.
func (s *SSTableInfosByLevel) MaxLevelSpanOverlapsContiguousSSTables(span roachpb.Span) int {
	// Note overlapsMoreTHanTwo should not be called on level 0, where
	// the SSTables are not guaranteed disjoint.
	overlapsMoreThanTwo := func(tables []SSTableInfo) bool {
		// Search to find the first sstable which might overlap the span.
		i := sort.Search(len(tables), func(i int) bool { return span.Key.Compare(tables[i].End.Key) < 0 })
		// If no SSTable is overlapped, return false.
		if i == -1 || i == len(tables) || span.EndKey.Compare(tables[i].Start.Key) < 0 {
			return false
		}
		// Return true if the span is not subsumed by the combination of
		// this sstable and the next. This logic is complicated and is
		// covered in the unittest. There are three successive conditions
		// which together ensure the span doesn't overlap > 2 SSTables.
		//
		// - If the first overlapped SSTable is the last.
		// - If the span does not exceed the end of the next SSTable.
		// - If the span does not overlap the start of the next next SSTable.
		if i >= len(tables)-1 {
			// First overlapped SSTable is the last (right-most) SSTable.
			//    Span:   [c-----f)
			//    SSTs: [a---d)
			// or
			//    SSTs: [a-----------q)
			return false
		}
		if span.EndKey.Compare(tables[i+1].End.Key) <= 0 {
			// Span does not reach outside of this SSTable's right neighbor.
			//    Span:    [c------f)
			//    SSTs: [a---d) [e-f) ...
			return false
		}
		if i >= len(tables)-2 {
			// Span reaches outside of this SSTable's right neighbor, but
			// there are no more SSTables to the right.
			//    Span:    [c-------------x)
			//    SSTs: [a---d) [e---q)
			return false
		}
		if span.EndKey.Compare(tables[i+2].Start.Key) <= 0 {
			// There's another SSTable two to the right, but the span doesn't
			// reach into it.
			//    Span:    [c------------x)
			//    SSTs: [a---d) [e---q) [x--z) ...
			return false
		}

		// Touching at least three SSTables.
		//    Span:    [c-------------y)
		//    SSTs: [a---d) [e---q) [x--z) ...
		return true
	}
	// Note that we never consider level 0, where SSTables can overlap.
	// Level 0 is instead returned as a catch-all which means that there
	// is no level where the span overlaps only two or fewer SSTables.
	for i := len(s.levels) - 1; i > 0; i-- {
		if !overlapsMoreThanTwo(s.levels[i]) {
			return i
		}
	}
	return 0
}
