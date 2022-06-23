// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/vfs"
)

// Compare exports the base.Compare type.
type Compare = base.Compare

// InternalKey exports the base.InternalKey type.
type InternalKey = base.InternalKey

// TableInfo contains the common information for table related events.
type TableInfo struct {
	// FileNum is the internal DB identifier for the table.
	FileNum base.FileNum
	// Size is the size of the file in bytes.
	Size uint64
	// Smallest is the smallest internal key in the table.
	Smallest InternalKey
	// Largest is the largest internal key in the table.
	Largest InternalKey
	// SmallestSeqNum is the smallest sequence number in the table.
	SmallestSeqNum uint64
	// LargestSeqNum is the largest sequence number in the table.
	LargestSeqNum uint64
}

// TableStats contains statistics on a table used for compaction heuristics.
type TableStats struct {
	// The total number of entries in the table.
	NumEntries uint64
	// The number of point and range deletion entries in the table.
	NumDeletions uint64
	// NumRangeKeySets is the total number of range key sets in the table.
	NumRangeKeySets uint64
	// Estimate of the total disk space that may be dropped by this table's
	// point deletions by compacting them.
	PointDeletionsBytesEstimate uint64
	// Estimate of the total disk space that may be dropped by this table's
	// range deletions by compacting them. This estimate is at data-block
	// granularity and is not updated if compactions beneath the table reduce
	// the amount of reclaimable disk space. It also does not account for
	// overlapping data in L0 and ignores L0 sublevels, but the error that
	// introduces is expected to be small.
	//
	// Tables in the bottommost level of the LSM may have a nonzero estimate
	// if snapshots or move compactions prevented the elision of their range
	// tombstones.
	RangeDeletionsBytesEstimate uint64
}

// boundType represents the type of key (point or range) present as the smallest
// and largest keys.
type boundType uint8

const (
	boundTypePointKey boundType = iota + 1
	boundTypeRangeKey
)

// CompactionState is the compaction state of a file.
//
// The following shows the valid state transitions:
//
//    NotCompacting --> Compacting --> Compacted
//          ^               |
//          |               |
//          +-------<-------+
//
// Input files to a compaction transition to Compacting when a compaction is
// picked. A file that has finished compacting typically transitions into the
// Compacted state, at which point it is effectively obsolete ("zombied") and
// will eventually be removed from the LSM. A file that has been move-compacted
// will transition from Compacting back into the NotCompacting state, signaling
// that the file may be selected for a subsequent compaction. A failed
// compaction will result in all input tables transitioning from Compacting to
// NotCompacting.
//
// This state is in-memory only. It is not persisted to the manifest.
type CompactionState uint8

// CompactionStates.
const (
	CompactionStateNotCompacting CompactionState = iota
	CompactionStateCompacting
	CompactionStateCompacted
)

// String implements fmt.Stringer.
func (s CompactionState) String() string {
	switch s {
	case CompactionStateNotCompacting:
		return "NotCompacting"
	case CompactionStateCompacting:
		return "Compacting"
	case CompactionStateCompacted:
		return "Compacted"
	default:
		panic(fmt.Sprintf("pebble: unknown compaction state %d", s))
	}
}

// FileMetadata holds the metadata for an on-disk table.
type FileMetadata struct {
	// Atomic contains fields which are accessed atomically. Go allocations
	// are guaranteed to be 64-bit aligned which we take advantage of by
	// placing the 64-bit fields which we access atomically at the beginning
	// of the FileMetadata struct. For more information, see
	// https://golang.org/pkg/sync/atomic/#pkg-note-BUG.
	Atomic struct {
		// AllowedSeeks is used to determine if a file should be picked for
		// a read triggered compaction. It is decremented when read sampling
		// in pebble.Iterator after every after every positioning operation
		// that returns a user key (eg. Next, Prev, SeekGE, SeekLT, etc).
		AllowedSeeks int64

		// statsValid is 1 if stats have been loaded for the table. The
		// TableStats structure is populated only if valid is 1.
		statsValid uint32
	}

	// InitAllowedSeeks is the inital value of allowed seeks. This is used
	// to re-set allowed seeks on a file once it hits 0.
	InitAllowedSeeks int64

	// Reference count for the file: incremented when a file is added to a
	// version and decremented when the version is unreferenced. The file is
	// obsolete when the reference count falls to zero.
	refs int32
	// FileNum is the file number.
	FileNum base.FileNum
	// Size is the size of the file, in bytes.
	Size uint64
	// File creation time in seconds since the epoch (1970-01-01 00:00:00
	// UTC). For ingested sstables, this corresponds to the time the file was
	// ingested.
	CreationTime int64
	// Smallest and largest sequence numbers in the table, across both point and
	// range keys.
	SmallestSeqNum uint64
	LargestSeqNum  uint64
	// SmallestPointKey and LargestPointKey are the inclusive bounds for the
	// internal point keys stored in the table. This includes RANGEDELs, which
	// alter point keys.
	// NB: these field should be set using ExtendPointKeyBounds. They are left
	// exported for reads as an optimization.
	SmallestPointKey InternalKey
	LargestPointKey  InternalKey
	// SmallestRangeKey and LargestRangeKey are the inclusive bounds for the
	// internal range keys stored in the table.
	// NB: these field should be set using ExtendRangeKeyBounds. They are left
	// exported for reads as an optimization.
	SmallestRangeKey InternalKey
	LargestRangeKey  InternalKey
	// Smallest and Largest are the inclusive bounds for the internal keys stored
	// in the table, across both point and range keys.
	// NB: these fields are derived from their point and range key equivalents,
	// and are updated via the MaybeExtend{Point,Range}KeyBounds methods.
	Smallest InternalKey
	Largest  InternalKey
	// Stats describe table statistics. Protected by DB.mu.
	Stats TableStats

	SubLevel         int
	L0Index          int
	minIntervalIndex int
	maxIntervalIndex int

	// NB: the alignment of this struct is 8 bytes. We pack all the bools to
	// ensure an optimal packing.

	// For L0 files only. Protected by DB.mu. Used to generate L0 sublevels and
	// pick L0 compactions. Only accurate for the most recent Version.
	//
	// IsIntraL0Compacting is set to True if this file is part of an intra-L0
	// compaction. When it's true, IsCompacting must also return true. If
	// Compacting is true and IsIntraL0Compacting is false for an L0 file, the
	// file must be part of a compaction to Lbase.
	IsIntraL0Compacting bool
	CompactionState     CompactionState
	// True if compaction of this file has been explicitly requested.
	// Previously, RocksDB and earlier versions of Pebble allowed this
	// flag to be set by a user table property collector. Some earlier
	// versions of Pebble respected this flag, while other more recent
	// versions ignored this flag.
	//
	// More recently this flag has been repurposed to facilitate the
	// compaction of 'atomic compaction units'. Files marked for
	// compaction are compacted in a rewrite compaction at the lowest
	// possible compaction priority.
	//
	// NB: A count of files marked for compaction is maintained on
	// Version, and compaction picking reads cached annotations
	// determined by this field.
	//
	// Protected by DB.mu.
	MarkedForCompaction bool
	// HasPointKeys tracks whether the table contains point keys (including
	// RANGEDELs). If a table contains only range deletions, HasPointsKeys is
	// still true.
	HasPointKeys bool
	// HasRangeKeys tracks whether the table contains any range keys.
	HasRangeKeys bool
	// smallestSet and largestSet track whether the overall bounds have been set.
	boundsSet bool
	// boundTypeSmallest and boundTypeLargest provide an indication as to which
	// key type (point or range) corresponds to the smallest and largest overall
	// table bounds.
	boundTypeSmallest, boundTypeLargest boundType
}

// SetCompactionState transitions this file's compaction state to the given
// state. Protected by DB.mu.
func (m *FileMetadata) SetCompactionState(to CompactionState) {
	if invariants.Enabled {
		transitionErr := func() error {
			return errors.Newf("pebble: invalid compaction state transition: %s -> %s", m.CompactionState, to)
		}
		switch m.CompactionState {
		case CompactionStateNotCompacting:
			if to != CompactionStateCompacting {
				panic(transitionErr())
			}
		case CompactionStateCompacting:
			if to != CompactionStateCompacted && to != CompactionStateNotCompacting {
				panic(transitionErr())
			}
		case CompactionStateCompacted:
			panic(transitionErr())
		default:
			panic(fmt.Sprintf("pebble: unknown compaction state: %d", m.CompactionState))
		}
	}
	m.CompactionState = to
}

// IsCompacting returns true if this file's compaction state is
// CompactionStateCompacting. Protected by DB.mu.
func (m *FileMetadata) IsCompacting() bool {
	return m.CompactionState == CompactionStateCompacting
}

// StatsValid returns true if the table stats have been populated. If StatValid
// returns true, the Stats field may be read (with or without holding the
// database mutex).
func (m *FileMetadata) StatsValid() bool {
	return atomic.LoadUint32(&m.Atomic.statsValid) == 1
}

// StatsValidLocked returns true if the table stats have been populated.
// StatsValidLocked requires DB.mu is held when it's invoked, and it avoids the
// overhead of an atomic load. This is possible because table stats validity is
// only set while DB.mu is held.
func (m *FileMetadata) StatsValidLocked() bool {
	return m.Atomic.statsValid == 1
}

// StatsMarkValid marks the TableStats as valid. The caller must hold DB.mu
// while populating TableStats and calling StatsMarkValud. Once stats are
// populated, they must not be mutated.
func (m *FileMetadata) StatsMarkValid() {
	atomic.StoreUint32(&m.Atomic.statsValid, 1)
}

// ExtendPointKeyBounds attempts to extend the lower and upper point key bounds
// and overall table bounds with the given smallest and largest keys. The
// smallest and largest bounds may not be extended if the table already has a
// bound that is smaller or larger, respectively. The receiver is returned.
// NB: calling this method should be preferred to manually setting the bounds by
// manipulating the fields directly, to maintain certain invariants.
func (m *FileMetadata) ExtendPointKeyBounds(
	cmp Compare, smallest, largest InternalKey,
) *FileMetadata {
	// Update the point key bounds.
	if !m.HasPointKeys {
		m.SmallestPointKey, m.LargestPointKey = smallest, largest
		m.HasPointKeys = true
	} else {
		if base.InternalCompare(cmp, smallest, m.SmallestPointKey) < 0 {
			m.SmallestPointKey = smallest
		}
		if base.InternalCompare(cmp, largest, m.LargestPointKey) > 0 {
			m.LargestPointKey = largest
		}
	}
	// Update the overall bounds.
	m.extendOverallBounds(cmp, m.SmallestPointKey, m.LargestPointKey, boundTypePointKey)
	return m
}

// ExtendRangeKeyBounds attempts to extend the lower and upper range key bounds
// and overall table bounds with the given smallest and largest keys. The
// smallest and largest bounds may not be extended if the table already has a
// bound that is smaller or larger, respectively. The receiver is returned.
// NB: calling this method should be preferred to manually setting the bounds by
// manipulating the fields directly, to maintain certain invariants.
func (m *FileMetadata) ExtendRangeKeyBounds(
	cmp Compare, smallest, largest InternalKey,
) *FileMetadata {
	// Update the range key bounds.
	if !m.HasRangeKeys {
		m.SmallestRangeKey, m.LargestRangeKey = smallest, largest
		m.HasRangeKeys = true
	} else {
		if base.InternalCompare(cmp, smallest, m.SmallestRangeKey) < 0 {
			m.SmallestRangeKey = smallest
		}
		if base.InternalCompare(cmp, largest, m.LargestRangeKey) > 0 {
			m.LargestRangeKey = largest
		}
	}
	// Update the overall bounds.
	m.extendOverallBounds(cmp, m.SmallestRangeKey, m.LargestRangeKey, boundTypeRangeKey)
	return m
}

// extendOverallBounds attempts to extend the overall table lower and upper
// bounds. The given bounds may not be used if a lower or upper bound already
// exists that is smaller or larger than the given keys, respectively. The given
// boundType will be used if the bounds are updated.
func (m *FileMetadata) extendOverallBounds(
	cmp Compare, smallest, largest InternalKey, bTyp boundType,
) {
	if !m.boundsSet {
		m.Smallest, m.Largest = smallest, largest
		m.boundsSet = true
		m.boundTypeSmallest, m.boundTypeLargest = bTyp, bTyp
	} else {
		if base.InternalCompare(cmp, smallest, m.Smallest) < 0 {
			m.Smallest = smallest
			m.boundTypeSmallest = bTyp
		}
		if base.InternalCompare(cmp, largest, m.Largest) > 0 {
			m.Largest = largest
			m.boundTypeLargest = bTyp
		}
	}
}

const (
	maskContainsPointKeys = 1 << 0
	maskSmallest          = 1 << 1
	maskLargest           = 1 << 2
)

// boundsMarker returns a marker byte whose bits encode the following
// information (in order from least significant bit):
// - if the table contains point keys
// - if the table's smallest key is a point key
// - if the table's largest key is a point key
func (m *FileMetadata) boundsMarker() (sentinel uint8, err error) {
	if m.HasPointKeys {
		sentinel |= maskContainsPointKeys
	}
	switch m.boundTypeSmallest {
	case boundTypePointKey:
		sentinel |= maskSmallest
	case boundTypeRangeKey:
		// No op - leave bit unset.
	default:
		return 0, base.CorruptionErrorf("file %s has neither point nor range key as smallest key", m.FileNum)
	}
	switch m.boundTypeLargest {
	case boundTypePointKey:
		sentinel |= maskLargest
	case boundTypeRangeKey:
		// No op - leave bit unset.
	default:
		return 0, base.CorruptionErrorf("file %s has neither point nor range key as largest key", m.FileNum)
	}
	return
}

// String implements fmt.Stringer, printing the file number and the overall
// table bounds.
func (m *FileMetadata) String() string {
	return fmt.Sprintf("%s:[%s-%s]", m.FileNum, m.Smallest, m.Largest)
}

// DebugString returns a verbose representation of FileMetadata, typically for
// use in tests and debugging, returning the file number and the point, range
// and overall bounds for the table.
func (m *FileMetadata) DebugString(format base.FormatKey, verbose bool) string {
	var b bytes.Buffer
	fmt.Fprintf(&b, "%s:[%s-%s]",
		m.FileNum, m.Smallest.Pretty(format), m.Largest.Pretty(format))
	if !verbose {
		return b.String()
	}
	if m.HasPointKeys {
		fmt.Fprintf(&b, " points:[%s-%s]",
			m.SmallestPointKey.Pretty(format), m.LargestPointKey.Pretty(format))
	}
	if m.HasRangeKeys {
		fmt.Fprintf(&b, " ranges:[%s-%s]",
			m.SmallestRangeKey.Pretty(format), m.LargestRangeKey.Pretty(format))
	}
	return b.String()
}

// ParseFileMetadataDebug parses a FileMetadata from its DebugString
// representation.
func ParseFileMetadataDebug(s string) (m FileMetadata, err error) {
	// Split lines of the form:
	//  000000:[a#0,SET-z#0,SET] points:[...] ranges:[...]
	fields := strings.FieldsFunc(s, func(c rune) bool {
		switch c {
		case ':', '[', '-', ']':
			return true
		default:
			return unicode.IsSpace(c) // NB: also trim whitespace padding.
		}
	})
	if len(fields)%3 != 0 {
		return m, errors.Newf("malformed input: %s", s)
	}
	for len(fields) > 0 {
		prefix := fields[0]
		smallest := base.ParsePrettyInternalKey(fields[1])
		largest := base.ParsePrettyInternalKey(fields[2])
		switch prefix {
		case "points":
			m.SmallestPointKey, m.LargestPointKey = smallest, largest
			m.HasPointKeys = true
		case "ranges":
			m.SmallestRangeKey, m.LargestRangeKey = smallest, largest
			m.HasRangeKeys = true
		default:
			fileNum, err := strconv.ParseUint(prefix, 10, 64)
			if err != nil {
				return m, errors.Newf("malformed input: %s: %s", s, err)
			}
			m.FileNum = base.FileNum(fileNum)
			m.Smallest, m.Largest = smallest, largest
			m.boundsSet = true
		}
		fields = fields[3:]
	}
	// By default, when the parser sees just the overall bounds, we set the point
	// keys. This preserves backwards compatability with existing test cases that
	// specify only the overall bounds.
	if !m.HasPointKeys && !m.HasRangeKeys {
		m.SmallestPointKey, m.LargestPointKey = m.Smallest, m.Largest
		m.HasPointKeys = true
	}
	return
}

// Validate validates the metadata for consistency with itself, returning an
// error if inconsistent.
func (m *FileMetadata) Validate(cmp Compare, formatKey base.FormatKey) error {
	// Combined range and point key validation.

	if !m.HasPointKeys && !m.HasRangeKeys {
		return base.CorruptionErrorf("file %s has neither point nor range keys",
			errors.Safe(m.FileNum))
	}
	if base.InternalCompare(cmp, m.Smallest, m.Largest) > 0 {
		return base.CorruptionErrorf("file %s has inconsistent bounds: %s vs %s",
			errors.Safe(m.FileNum), m.Smallest.Pretty(formatKey),
			m.Largest.Pretty(formatKey))
	}
	if m.SmallestSeqNum > m.LargestSeqNum {
		return base.CorruptionErrorf("file %s has inconsistent seqnum bounds: %d vs %d",
			errors.Safe(m.FileNum), m.SmallestSeqNum, m.LargestSeqNum)
	}

	// Point key validation.

	if m.HasPointKeys {
		if base.InternalCompare(cmp, m.SmallestPointKey, m.LargestPointKey) > 0 {
			return base.CorruptionErrorf("file %s has inconsistent point key bounds: %s vs %s",
				errors.Safe(m.FileNum), m.SmallestPointKey.Pretty(formatKey),
				m.LargestPointKey.Pretty(formatKey))
		}
		if base.InternalCompare(cmp, m.SmallestPointKey, m.Smallest) < 0 ||
			base.InternalCompare(cmp, m.LargestPointKey, m.Largest) > 0 {
			return base.CorruptionErrorf(
				"file %s has inconsistent point key bounds relative to overall bounds: "+
					"overall = [%s-%s], point keys = [%s-%s]",
				errors.Safe(m.FileNum),
				m.Smallest.Pretty(formatKey), m.Largest.Pretty(formatKey),
				m.SmallestPointKey.Pretty(formatKey), m.LargestPointKey.Pretty(formatKey),
			)
		}
	}

	// Range key validation.

	if m.HasRangeKeys {
		if base.InternalCompare(cmp, m.SmallestRangeKey, m.LargestRangeKey) > 0 {
			return base.CorruptionErrorf("file %s has inconsistent range key bounds: %s vs %s",
				errors.Safe(m.FileNum), m.SmallestRangeKey.Pretty(formatKey),
				m.LargestRangeKey.Pretty(formatKey))
		}
		if base.InternalCompare(cmp, m.SmallestRangeKey, m.Smallest) < 0 ||
			base.InternalCompare(cmp, m.LargestRangeKey, m.Largest) > 0 {
			return base.CorruptionErrorf(
				"file %s has inconsistent range key bounds relative to overall bounds: "+
					"overall = [%s-%s], range keys = [%s-%s]",
				errors.Safe(m.FileNum),
				m.Smallest.Pretty(formatKey), m.Largest.Pretty(formatKey),
				m.SmallestRangeKey.Pretty(formatKey), m.LargestRangeKey.Pretty(formatKey),
			)
		}
	}

	return nil
}

// TableInfo returns a subset of the FileMetadata state formatted as a
// TableInfo.
func (m *FileMetadata) TableInfo() TableInfo {
	return TableInfo{
		FileNum:        m.FileNum,
		Size:           m.Size,
		Smallest:       m.Smallest,
		Largest:        m.Largest,
		SmallestSeqNum: m.SmallestSeqNum,
		LargestSeqNum:  m.LargestSeqNum,
	}
}

func cmpUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return +1
	default:
		return 0
	}
}

func (m *FileMetadata) cmpSeqNum(b *FileMetadata) int {
	// NB: This is the same ordering that RocksDB uses for L0 files.

	// Sort first by largest sequence number.
	if m.LargestSeqNum != b.LargestSeqNum {
		return cmpUint64(m.LargestSeqNum, b.LargestSeqNum)
	}
	// Then by smallest sequence number.
	if m.SmallestSeqNum != b.SmallestSeqNum {
		return cmpUint64(m.SmallestSeqNum, b.SmallestSeqNum)
	}
	// Break ties by file number.
	return cmpUint64(uint64(m.FileNum), uint64(b.FileNum))
}

func (m *FileMetadata) lessSeqNum(b *FileMetadata) bool {
	return m.cmpSeqNum(b) < 0
}

func (m *FileMetadata) cmpSmallestKey(b *FileMetadata, cmp Compare) int {
	return base.InternalCompare(cmp, m.Smallest, b.Smallest)
}

// KeyRange returns the minimum smallest and maximum largest internalKey for
// all the FileMetadata in iters.
func KeyRange(ucmp Compare, iters ...LevelIterator) (smallest, largest InternalKey) {
	first := true
	for _, iter := range iters {
		for meta := iter.First(); meta != nil; meta = iter.Next() {
			if first {
				first = false
				smallest, largest = meta.Smallest, meta.Largest
				continue
			}
			if base.InternalCompare(ucmp, smallest, meta.Smallest) >= 0 {
				smallest = meta.Smallest
			}
			if base.InternalCompare(ucmp, largest, meta.Largest) <= 0 {
				largest = meta.Largest
			}
		}
	}
	return smallest, largest
}

type bySeqNum []*FileMetadata

func (b bySeqNum) Len() int { return len(b) }
func (b bySeqNum) Less(i, j int) bool {
	return b[i].lessSeqNum(b[j])
}
func (b bySeqNum) Swap(i, j int) { b[i], b[j] = b[j], b[i] }

// SortBySeqNum sorts the specified files by increasing sequence number.
func SortBySeqNum(files []*FileMetadata) {
	sort.Sort(bySeqNum(files))
}

type bySmallest struct {
	files []*FileMetadata
	cmp   Compare
}

func (b bySmallest) Len() int { return len(b.files) }
func (b bySmallest) Less(i, j int) bool {
	return b.files[i].cmpSmallestKey(b.files[j], b.cmp) < 0
}
func (b bySmallest) Swap(i, j int) { b.files[i], b.files[j] = b.files[j], b.files[i] }

// SortBySmallest sorts the specified files by smallest key using the supplied
// comparison function to order user keys.
func SortBySmallest(files []*FileMetadata, cmp Compare) {
	sort.Sort(bySmallest{files, cmp})
}

func overlaps(iter LevelIterator, cmp Compare, start, end []byte, exclusiveEnd bool) LevelSlice {
	startIter := iter.Clone()
	startIter.SeekGE(cmp, start)

	// SeekGE compares user keys. The user key `start` may be equal to the
	// f.Largest because f.Largest is a range deletion sentinel, indicating that
	// the user key `start` is NOT contained within the file f. If that's the
	// case, we can narrow the overlapping bounds to exclude the file with the
	// sentinel.
	if f := startIter.Current(); f != nil && f.Largest.IsExclusiveSentinel() &&
		cmp(f.Largest.UserKey, start) == 0 {
		startIter.Next()
	}

	endIter := iter.Clone()
	endIter.SeekGE(cmp, end)

	if !exclusiveEnd {
		// endIter is now pointing at the *first* file with a largest key >= end.
		// If there are multiple files including the user key `end`, we want all
		// of them, so move forward.
		for f := endIter.Current(); f != nil && cmp(f.Largest.UserKey, end) == 0; {
			f = endIter.Next()
		}
	}

	// LevelSlice uses inclusive bounds, so if we seeked to the end sentinel
	// or nexted too far because Largest.UserKey equaled `end`, go back.
	//
	// Consider !exclusiveEnd and end = 'f', with the following file bounds:
	//
	//     [b,d] [e, f] [f, f] [g, h]
	//
	// the above for loop will Next until it arrives at [g, h]. We need to
	// observe that g > f, and Prev to the file with bounds [f, f].
	if !endIter.iter.valid() {
		endIter.Prev()
	} else if c := cmp(endIter.Current().Smallest.UserKey, end); c > 0 || c == 0 && exclusiveEnd {
		endIter.Prev()
	}

	iter = startIter.Clone()
	return LevelSlice{
		iter:  iter.iter,
		start: &startIter.iter,
		end:   &endIter.iter,
	}
}

// NumLevels is the number of levels a Version contains.
const NumLevels = 7

// NewVersion constructs a new Version with the provided files. It requires
// the provided files are already well-ordered. It's intended for testing.
func NewVersion(
	cmp Compare, formatKey base.FormatKey, flushSplitBytes int64, files [NumLevels][]*FileMetadata,
) *Version {
	var v Version
	for l := range files {
		// NB: We specifically insert `files` into the B-Tree in the order
		// they appear within `files`. Some tests depend on this behavior in
		// order to test consistency checking, etc. Once we've constructed the
		// initial B-Tree, we swap out the btreeCmp for the correct one.
		// TODO(jackson): Adjust or remove the tests and remove this.
		v.Levels[l].tree, _ = makeBTree(btreeCmpSpecificOrder(files[l]), files[l])
		v.Levels[l].level = l
		if l == 0 {
			v.Levels[l].tree.cmp = btreeCmpSeqNum
		} else {
			v.Levels[l].tree.cmp = btreeCmpSmallestKey(cmp)
		}
	}
	if err := v.InitL0Sublevels(cmp, formatKey, flushSplitBytes); err != nil {
		panic(err)
	}
	return &v
}

// Version is a collection of file metadata for on-disk tables at various
// levels. In-memory DBs are written to level-0 tables, and compactions
// migrate data from level N to level N+1. The tables map internal keys (which
// are a user key, a delete or set bit, and a sequence number) to user values.
//
// The tables at level 0 are sorted by largest sequence number. Due to file
// ingestion, there may be overlap in the ranges of sequence numbers contain in
// level 0 sstables. In particular, it is valid for one level 0 sstable to have
// the seqnum range [1,100] while an adjacent sstable has the seqnum range
// [50,50]. This occurs when the [50,50] table was ingested and given a global
// seqnum. The ingestion code will have ensured that the [50,50] sstable will
// not have any keys that overlap with the [1,100] in the seqnum range
// [1,49]. The range of internal keys [fileMetadata.smallest,
// fileMetadata.largest] in each level 0 table may overlap.
//
// The tables at any non-0 level are sorted by their internal key range and any
// two tables at the same non-0 level do not overlap.
//
// The internal key ranges of two tables at different levels X and Y may
// overlap, for any X != Y.
//
// Finally, for every internal key in a table at level X, there is no internal
// key in a higher level table that has both the same user key and a higher
// sequence number.
type Version struct {
	refs int32

	// The level 0 sstables are organized in a series of sublevels. Similar to
	// the seqnum invariant in normal levels, there is no internal key in a
	// higher level table that has both the same user key and a higher sequence
	// number. Within a sublevel, tables are sorted by their internal key range
	// and any two tables at the same sublevel do not overlap. Unlike the normal
	// levels, sublevel n contains older tables (lower sequence numbers) than
	// sublevel n+1.
	//
	// The L0Sublevels struct is mostly used for compaction picking. As most
	// internal data structures in it are only necessary for compaction picking
	// and not for iterator creation, the reference to L0Sublevels is nil'd
	// after this version becomes the non-newest version, to reduce memory
	// usage.
	//
	// L0Sublevels.Levels contains L0 files ordered by sublevels. All the files
	// in Files[0] are in L0Sublevels.Levels. L0SublevelFiles is also set to
	// a reference to that slice, as that slice is necessary for iterator
	// creation and needs to outlast L0Sublevels.
	L0Sublevels     *L0Sublevels
	L0SublevelFiles []LevelSlice

	Levels [NumLevels]LevelMetadata

	// RangeKeyLevels holds a subset of the same files as Levels that contain range
	// keys (i.e. fileMeta.HasRangeKeys == true). The memory amplification of this
	// duplication should be minimal, as range keys are expected to be rare.
	RangeKeyLevels [NumLevels]LevelMetadata

	// The callback to invoke when the last reference to a version is
	// removed. Will be called with list.mu held.
	Deleted func(obsolete []*FileMetadata)

	// Stats holds aggregated stats about the version maintained from
	// version to version.
	Stats struct {
		// MarkedForCompaction records the count of files marked for
		// compaction within the version.
		MarkedForCompaction int
	}

	// The list the version is linked into.
	list *VersionList

	// The next/prev link for the versionList doubly-linked list of versions.
	prev, next *Version
}

// String implements fmt.Stringer, printing the FileMetadata for each level in
// the Version.
func (v *Version) String() string {
	return v.string(base.DefaultFormatter, false)
}

// DebugString returns an alternative format to String() which includes sequence
// number and kind information for the sstable boundaries.
func (v *Version) DebugString(format base.FormatKey) string {
	return v.string(format, true)
}

func (v *Version) string(format base.FormatKey, verbose bool) string {
	var buf bytes.Buffer
	if len(v.L0SublevelFiles) > 0 {
		for sublevel := len(v.L0SublevelFiles) - 1; sublevel >= 0; sublevel-- {
			fmt.Fprintf(&buf, "0.%d:\n", sublevel)
			v.L0SublevelFiles[sublevel].Each(func(f *FileMetadata) {
				fmt.Fprintf(&buf, "  %s\n", f.DebugString(format, verbose))
			})
		}
	}
	for level := 1; level < NumLevels; level++ {
		if v.Levels[level].Empty() {
			continue
		}
		fmt.Fprintf(&buf, "%d:\n", level)
		iter := v.Levels[level].Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			fmt.Fprintf(&buf, "  %s\n", f.DebugString(format, verbose))
		}
	}
	return buf.String()
}

// ParseVersionDebug parses a Version from its DebugString output.
func ParseVersionDebug(
	cmp Compare, formatKey base.FormatKey, flushSplitBytes int64, s string,
) (*Version, error) {
	var level int
	var files [NumLevels][]*FileMetadata
	for _, l := range strings.Split(s, "\n") {
		l = strings.TrimSpace(l)

		switch l[:2] {
		case "0.", "0:", "1:", "2:", "3:", "4:", "5:", "6:":
			var err error
			level, err = strconv.Atoi(l[:1])
			if err != nil {
				return nil, err
			}
		default:
			m, err := ParseFileMetadataDebug(l)
			if err != nil {
				return nil, err
			}
			// If we only parsed overall bounds, default to setting the point bounds.
			if !m.HasPointKeys && !m.HasRangeKeys {
				m.SmallestPointKey, m.LargestPointKey = m.Smallest, m.Largest
				m.HasPointKeys = true
			}
			files[level] = append(files[level], &m)
		}
	}
	// Reverse the order of L0 files. This ensures we construct the same
	// sublevels. (They're printed from higher sublevel to lower, which means in
	// a partial order that represents newest to oldest).
	for i := 0; i < len(files[0])/2; i++ {
		files[0][i], files[0][len(files[0])-i-1] = files[0][len(files[0])-i-1], files[0][i]
	}
	return NewVersion(cmp, formatKey, flushSplitBytes, files), nil
}

// Refs returns the number of references to the version.
func (v *Version) Refs() int32 {
	return atomic.LoadInt32(&v.refs)
}

// Ref increments the version refcount.
func (v *Version) Ref() {
	atomic.AddInt32(&v.refs, 1)
}

// Unref decrements the version refcount. If the last reference to the version
// was removed, the version is removed from the list of versions and the
// Deleted callback is invoked. Requires that the VersionList mutex is NOT
// locked.
func (v *Version) Unref() {
	if atomic.AddInt32(&v.refs, -1) == 0 {
		obsolete := v.unrefFiles()
		l := v.list
		l.mu.Lock()
		l.Remove(v)
		v.Deleted(obsolete)
		l.mu.Unlock()
	}
}

// UnrefLocked decrements the version refcount. If the last reference to the
// version was removed, the version is removed from the list of versions and
// the Deleted callback is invoked. Requires that the VersionList mutex is
// already locked.
func (v *Version) UnrefLocked() {
	if atomic.AddInt32(&v.refs, -1) == 0 {
		v.list.Remove(v)
		v.Deleted(v.unrefFiles())
	}
}

func (v *Version) unrefFiles() []*FileMetadata {
	var obsolete []*FileMetadata
	for _, lm := range v.Levels {
		obsolete = append(obsolete, lm.release()...)
	}
	return obsolete
}

// Next returns the next version in the list of versions.
func (v *Version) Next() *Version {
	return v.next
}

// InitL0Sublevels initializes the L0Sublevels
func (v *Version) InitL0Sublevels(
	cmp Compare, formatKey base.FormatKey, flushSplitBytes int64,
) error {
	var err error
	v.L0Sublevels, err = NewL0Sublevels(&v.Levels[0], cmp, formatKey, flushSplitBytes)
	if err == nil && v.L0Sublevels != nil {
		v.L0SublevelFiles = v.L0Sublevels.Levels
	}
	return err
}

// Contains returns a boolean indicating whether the provided file exists in
// the version at the given level. If level is non-zero then Contains binary
// searches among the files. If level is zero, Contains scans the entire
// level.
func (v *Version) Contains(level int, cmp Compare, m *FileMetadata) bool {
	iter := v.Levels[level].Iter()
	if level > 0 {
		overlaps := v.Overlaps(level, cmp, m.Smallest.UserKey, m.Largest.UserKey,
			m.Largest.IsExclusiveSentinel())
		iter = overlaps.Iter()
	}
	for f := iter.First(); f != nil; f = iter.Next() {
		if f == m {
			return true
		}
	}
	return false
}

// Overlaps returns all elements of v.files[level] whose user key range
// intersects the inclusive range [start, end]. If level is non-zero then the
// user key ranges of v.files[level] are assumed to not overlap (although they
// may touch). If level is zero then that assumption cannot be made, and the
// [start, end] range is expanded to the union of those matching ranges so far
// and the computation is repeated until [start, end] stabilizes.
// The returned files are a subsequence of the input files, i.e., the ordering
// is not changed.
func (v *Version) Overlaps(
	level int, cmp Compare, start, end []byte, exclusiveEnd bool,
) LevelSlice {
	if level == 0 {
		// Indices that have been selected as overlapping.
		l0 := v.Levels[level]
		l0Iter := l0.Iter()
		selectedIndices := make([]bool, l0.Len())
		numSelected := 0
		var slice LevelSlice
		for {
			restart := false
			for i, meta := 0, l0Iter.First(); meta != nil; i, meta = i+1, l0Iter.Next() {
				selected := selectedIndices[i]
				if selected {
					continue
				}
				smallest := meta.Smallest.UserKey
				largest := meta.Largest.UserKey
				if c := cmp(largest, start); c < 0 || c == 0 && meta.Largest.IsExclusiveSentinel() {
					// meta is completely before the specified range; skip it.
					continue
				}
				if c := cmp(smallest, end); c > 0 || c == 0 && exclusiveEnd {
					// meta is completely after the specified range; skip it.
					continue
				}
				// Overlaps.
				selectedIndices[i] = true
				numSelected++

				// Since level == 0, check if the newly added fileMetadata has
				// expanded the range. We expand the range immediately for files
				// we have remaining to check in this loop. All already checked
				// and unselected files will need to be rechecked via the
				// restart below.
				if cmp(smallest, start) < 0 {
					start = smallest
					restart = true
				}
				if v := cmp(largest, end); v > 0 {
					end = largest
					exclusiveEnd = meta.Largest.IsExclusiveSentinel()
					restart = true
				} else if v == 0 && exclusiveEnd && !meta.Largest.IsExclusiveSentinel() {
					// Only update the exclusivity of our existing `end`
					// bound.
					exclusiveEnd = false
					restart = true
				}
			}

			if !restart {
				// Construct a B-Tree containing only the matching items.
				var tr btree
				tr.cmp = v.Levels[level].tree.cmp
				for i, meta := 0, l0Iter.First(); meta != nil; i, meta = i+1, l0Iter.Next() {
					if selectedIndices[i] {
						err := tr.insert(meta)
						if err != nil {
							panic(err)
						}
					}
				}
				slice = LevelSlice{iter: tr.iter(), length: tr.length}
				// TODO(jackson): Avoid the oddity of constructing and
				// immediately releasing a B-Tree. Make LevelSlice an
				// interface?
				tr.release()
				break
			}
			// Continue looping to retry the files that were not selected.
		}
		return slice
	}

	return overlaps(v.Levels[level].Iter(), cmp, start, end, exclusiveEnd)
}

// CheckOrdering checks that the files are consistent with respect to
// increasing file numbers (for level 0 files) and increasing and non-
// overlapping internal key ranges (for level non-0 files).
func (v *Version) CheckOrdering(cmp Compare, format base.FormatKey) error {
	for sublevel := len(v.L0SublevelFiles) - 1; sublevel >= 0; sublevel-- {
		sublevelIter := v.L0SublevelFiles[sublevel].Iter()
		if err := CheckOrdering(cmp, format, L0Sublevel(sublevel), sublevelIter); err != nil {
			return base.CorruptionErrorf("%s\n%s", err, v.DebugString(format))
		}
	}

	for level, lm := range v.Levels {
		if err := CheckOrdering(cmp, format, Level(level), lm.Iter()); err != nil {
			return base.CorruptionErrorf("%s\n%s", err, v.DebugString(format))
		}
	}
	return nil
}

// CheckConsistency checks that all of the files listed in the version exist
// and their on-disk sizes match the sizes listed in the version.
func (v *Version) CheckConsistency(dirname string, fs vfs.FS) error {
	var buf bytes.Buffer
	var args []interface{}

	for level, files := range v.Levels {
		iter := files.Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			path := base.MakeFilepath(fs, dirname, base.FileTypeTable, f.FileNum)
			info, err := fs.Stat(path)
			if err != nil {
				buf.WriteString("L%d: %s: %v\n")
				args = append(args, errors.Safe(level), errors.Safe(f.FileNum), err)
				continue
			}
			if info.Size() != int64(f.Size) {
				buf.WriteString("L%d: %s: file size mismatch (%s): %d (disk) != %d (MANIFEST)\n")
				args = append(args, errors.Safe(level), errors.Safe(f.FileNum), path,
					errors.Safe(info.Size()), errors.Safe(f.Size))
				continue
			}
		}
	}

	if buf.Len() == 0 {
		return nil
	}
	return errors.Errorf(buf.String(), args...)
}

// VersionList holds a list of versions. The versions are ordered from oldest
// to newest.
type VersionList struct {
	mu   *sync.Mutex
	root Version
}

// Init initializes the version list.
func (l *VersionList) Init(mu *sync.Mutex) {
	l.mu = mu
	l.root.next = &l.root
	l.root.prev = &l.root
}

// Empty returns true if the list is empty, and false otherwise.
func (l *VersionList) Empty() bool {
	return l.root.next == &l.root
}

// Front returns the oldest version in the list. Note that this version is only
// valid if Empty() returns true.
func (l *VersionList) Front() *Version {
	return l.root.next
}

// Back returns the newest version in the list. Note that this version is only
// valid if Empty() returns true.
func (l *VersionList) Back() *Version {
	return l.root.prev
}

// PushBack adds a new version to the back of the list. This new version
// becomes the "newest" version in the list.
func (l *VersionList) PushBack(v *Version) {
	if v.list != nil || v.prev != nil || v.next != nil {
		panic("pebble: version list is inconsistent")
	}
	v.prev = l.root.prev
	v.prev.next = v
	v.next = &l.root
	v.next.prev = v
	v.list = l
	// Let L0Sublevels on the second newest version get GC'd, as it is no longer
	// necessary. See the comment in Version.
	v.prev.L0Sublevels = nil
}

// Remove removes the specified version from the list.
func (l *VersionList) Remove(v *Version) {
	if v == &l.root {
		panic("pebble: cannot remove version list root node")
	}
	if v.list != l {
		panic("pebble: version list is inconsistent")
	}
	v.prev.next = v.next
	v.next.prev = v.prev
	v.next = nil // avoid memory leaks
	v.prev = nil // avoid memory leaks
	v.list = nil // avoid memory leaks
}

// CheckOrdering checks that the files are consistent with respect to
// seqnums (for level 0 files -- see detailed comment below) and increasing and non-
// overlapping internal key ranges (for non-level 0 files).
func CheckOrdering(cmp Compare, format base.FormatKey, level Level, files LevelIterator) error {
	// The invariants to check for L0 sublevels are the same as the ones to
	// check for all other levels. However, if L0 is not organized into
	// sublevels, or if all L0 files are being passed in, we do the legacy L0
	// checks, defined in the detailed comment below.
	if level == Level(0) {
		// We have 2 kinds of files:
		// - Files with exactly one sequence number: these could be either ingested files
		//   or flushed files. We cannot tell the difference between them based on FileMetadata,
		//   so our consistency checking here uses the weaker checks assuming it is a narrow
		//   flushed file. We cannot error on ingested files having sequence numbers coincident
		//   with flushed files as the seemingly ingested file could just be a flushed file
		//   with just one key in it which is a truncated range tombstone sharing sequence numbers
		//   with other files in the same flush.
		// - Files with multiple sequence numbers: these are necessarily flushed files.
		//
		// Three cases of overlapping sequence numbers:
		// Case 1:
		// An ingested file contained in the sequence numbers of the flushed file -- it must be
		// fully contained (not coincident with either end of the flushed file) since the memtable
		// must have been at [a, b-1] (where b > a) when the ingested file was assigned sequence
		// num b, and the memtable got a subsequent update that was given sequence num b+1, before
		// being flushed.
		//
		// So a sequence [1000, 1000] [1002, 1002] [1000, 2000] is invalid since the first and
		// third file are inconsistent with each other. So comparing adjacent files is insufficient
		// for consistency checking.
		//
		// Visually we have something like
		// x------y x-----------yx-------------y (flushed files where x, y are the endpoints)
		//     y       y  y        y             (y's represent ingested files)
		// And these are ordered in increasing order of y. Note that y's must be unique.
		//
		// Case 2:
		// A flushed file that did not overlap in keys with any file in any level, but does overlap
		// in the file key intervals. This file is placed in L0 since it overlaps in the file
		// key intervals but since it has no overlapping data, it is assigned a sequence number
		// of 0 in RocksDB. We handle this case for compatibility with RocksDB.
		//
		// Case 3:
		// A sequence of flushed files that overlap in sequence numbers with one another,
		// but do not overlap in keys inside the sstables. These files correspond to
		// partitioned flushes or the results of intra-L0 compactions of partitioned
		// flushes.
		//
		// Since these types of SSTables violate most other sequence number
		// overlap invariants, and handling this case is important for compatibility
		// with future versions of pebble, this method relaxes most L0 invariant
		// checks.

		var prev *FileMetadata
		for f := files.First(); f != nil; f, prev = files.Next(), f {
			if prev == nil {
				continue
			}
			// Validate that the sorting is sane.
			if prev.LargestSeqNum == 0 && f.LargestSeqNum == prev.LargestSeqNum {
				// Multiple files satisfying case 2 mentioned above.
			} else if !prev.lessSeqNum(f) {
				return base.CorruptionErrorf("L0 files %s and %s are not properly ordered: <#%d-#%d> vs <#%d-#%d>",
					errors.Safe(prev.FileNum), errors.Safe(f.FileNum),
					errors.Safe(prev.SmallestSeqNum), errors.Safe(prev.LargestSeqNum),
					errors.Safe(f.SmallestSeqNum), errors.Safe(f.LargestSeqNum))
			}
		}
	} else {
		var prev *FileMetadata
		for f := files.First(); f != nil; f, prev = files.Next(), f {
			if err := f.Validate(cmp, format); err != nil {
				return errors.Wrapf(err, "%s ", level)
			}
			if prev != nil {
				if prev.cmpSmallestKey(f, cmp) >= 0 {
					return base.CorruptionErrorf("%s files %s and %s are not properly ordered: [%s-%s] vs [%s-%s]",
						errors.Safe(level), errors.Safe(prev.FileNum), errors.Safe(f.FileNum),
						prev.Smallest.Pretty(format), prev.Largest.Pretty(format),
						f.Smallest.Pretty(format), f.Largest.Pretty(format))
				}
				if base.InternalCompare(cmp, prev.Largest, f.Smallest) >= 0 {
					return base.CorruptionErrorf("%s files %s and %s have overlapping ranges: [%s-%s] vs [%s-%s]",
						errors.Safe(level), errors.Safe(prev.FileNum), errors.Safe(f.FileNum),
						prev.Smallest.Pretty(format), prev.Largest.Pretty(format),
						f.Smallest.Pretty(format), f.Largest.Pretty(format))
				}
			}
		}
	}
	return nil
}
