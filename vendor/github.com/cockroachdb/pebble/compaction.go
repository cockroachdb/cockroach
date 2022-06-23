// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"runtime/pprof"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

var errEmptyTable = errors.New("pebble: empty table")
var errFlushInvariant = errors.New("pebble: flush next log number is unset")

var compactLabels = pprof.Labels("pebble", "compact")
var flushLabels = pprof.Labels("pebble", "flush")
var gcLabels = pprof.Labels("pebble", "gc")

// expandedCompactionByteSizeLimit is the maximum number of bytes in all
// compacted files. We avoid expanding the lower level file set of a compaction
// if it would make the total compaction cover more than this many bytes.
func expandedCompactionByteSizeLimit(opts *Options, level int, availBytes uint64) uint64 {
	v := uint64(25 * opts.Level(level).TargetFileSize)

	// Never expand a compaction beyond half the available capacity, divided
	// by the maximum number of concurrent compactions. Each of the concurrent
	// compactions may expand up to this limit, so this attempts to limit
	// compactions to half of available disk space. Note that this will not
	// prevent compaction picking from pursuing compactions that are larger
	// than this threshold before expansion.
	diskMax := (availBytes / 2) / uint64(opts.MaxConcurrentCompactions())
	if v > diskMax {
		v = diskMax
	}
	return v
}

// maxGrandparentOverlapBytes is the maximum bytes of overlap with level+1
// before we stop building a single file in a level-1 to level compaction.
func maxGrandparentOverlapBytes(opts *Options, level int) uint64 {
	return uint64(10 * opts.Level(level).TargetFileSize)
}

// maxReadCompactionBytes is used to prevent read compactions which
// are too wide.
func maxReadCompactionBytes(opts *Options, level int) uint64 {
	return uint64(10 * opts.Level(level).TargetFileSize)
}

// noCloseIter wraps around a FragmentIterator, intercepting and eliding
// calls to Close. It is used during compaction to ensure that rangeDelIters
// are not closed prematurely.
type noCloseIter struct {
	keyspan.FragmentIterator
}

func (i noCloseIter) Close() error {
	return nil
}

type compactionLevel struct {
	level int
	files manifest.LevelSlice
}

// Return output from compactionOutputSplitters. See comment on
// compactionOutputSplitter.shouldSplitBefore() on how this value is used.
type compactionSplitSuggestion int

const (
	noSplit compactionSplitSuggestion = iota
	splitNow
)

// String implements the Stringer interface.
func (c compactionSplitSuggestion) String() string {
	if c == noSplit {
		return "no-split"
	}
	return "split-now"
}

// compactionOutputSplitter is an interface for encapsulating logic around
// switching the output of a compaction to a new output file. Additional
// constraints around switching compaction outputs that are specific to that
// compaction type (eg. flush splits) are implemented in
// compactionOutputSplitters that compose other child compactionOutputSplitters.
type compactionOutputSplitter interface {
	// shouldSplitBefore returns whether we should split outputs before the
	// specified "current key". The return value is splitNow or noSplit.
	// splitNow means a split is advised before the specified key, and noSplit
	// means no split is advised. If shouldSplitBefore(a) advises a split then
	// shouldSplitBefore(b) should also advise a split given b >= a, until
	// onNewOutput is called.
	shouldSplitBefore(key *InternalKey, tw *sstable.Writer) compactionSplitSuggestion
	// onNewOutput updates internal splitter state when the compaction switches
	// to a new sstable, and returns the next limit for the new output which
	// would get used to truncate range tombstones if the compaction iterator
	// runs out of keys. The limit returned MUST be > key according to the
	// compaction's comparator. The specified key is the first key in the new
	// output, or nil if this sstable will only contain range tombstones already
	// in the fragmenter.
	onNewOutput(key *InternalKey) []byte
}

// fileSizeSplitter is a compactionOutputSplitter that makes a determination
// to split outputs based on the estimated file size of the current output.
// Note that, unlike most other splitters, this splitter does not guarantee
// that it will advise splits only at user key change boundaries.
type fileSizeSplitter struct {
	maxFileSize uint64
}

func (f *fileSizeSplitter) shouldSplitBefore(
	key *InternalKey, tw *sstable.Writer,
) compactionSplitSuggestion {
	// The Kind != RangeDelete part exists because EstimatedSize doesn't grow
	// rightaway when a range tombstone is added to the fragmenter. It's always
	// better to make a sequence of range tombstones visible to the fragmenter.
	if key.Kind() != InternalKeyKindRangeDelete && tw != nil &&
		tw.EstimatedSize() >= f.maxFileSize {
		return splitNow
	}
	return noSplit
}

func (f *fileSizeSplitter) onNewOutput(key *InternalKey) []byte {
	return nil
}

type limitFuncSplitter struct {
	c         *compaction
	limitFunc func(userKey []byte) []byte
	limit     []byte
}

func (lf *limitFuncSplitter) shouldSplitBefore(
	key *InternalKey, tw *sstable.Writer,
) compactionSplitSuggestion {
	// NB: The limit must be applied using >= since lf.limit may be used as the
	// `splitterSuggestion` ultimately passed to `compactionIter.Tombstones` to
	// serve as an *exclusive* end boundary truncation point. If we used > then,
	// we may have already added a key with the user key `lf.limit` to the
	// previous sstable.
	if lf.limit != nil && lf.c.cmp(key.UserKey, lf.limit) >= 0 {
		return splitNow
	}
	return noSplit
}

func (lf *limitFuncSplitter) onNewOutput(key *InternalKey) []byte {
	lf.limit = nil
	if key != nil {
		lf.limit = lf.limitFunc(key.UserKey)
	} else {
		// Use the start key of the first pending tombstone to find the
		// next limit. All pending tombstones have the same start key.
		// We use this as opposed to the end key of the
		// last written sstable to effectively handle cases like these:
		//
		// a.SET.3
		// (lf.limit at b)
		// d.RANGEDEL.4:f
		//
		// In this case, the partition after b has only range deletions,
		// so if we were to find the limit after the last written key at
		// the split point (key a), we'd get the limit b again, and
		// finishOutput() would not advance any further because the next
		// range tombstone to write does not start until after the L0
		// split point.
		if startKey := lf.c.rangeDelFrag.Start(); startKey != nil {
			lf.limit = lf.limitFunc(startKey)
		}
	}
	return lf.limit
}

// splitterGroup is a compactionOutputSplitter that splits whenever one of its
// child splitters advises a compaction split.
type splitterGroup struct {
	cmp       Compare
	splitters []compactionOutputSplitter
}

func (a *splitterGroup) shouldSplitBefore(
	key *InternalKey, tw *sstable.Writer,
) (suggestion compactionSplitSuggestion) {
	for _, splitter := range a.splitters {
		if splitter.shouldSplitBefore(key, tw) == splitNow {
			return splitNow
		}
	}
	return noSplit
}

func (a *splitterGroup) onNewOutput(key *InternalKey) []byte {
	var earliestLimit []byte
	for _, splitter := range a.splitters {
		limit := splitter.onNewOutput(key)
		if limit == nil {
			continue
		}
		if earliestLimit == nil || a.cmp(limit, earliestLimit) < 0 {
			earliestLimit = limit
		}
	}
	return earliestLimit
}

// userKeyChangeSplitter is a compactionOutputSplitter that takes in a child
// splitter, and splits when 1) that child splitter has advised a split, and 2)
// the compaction output is at the boundary between two user keys (also
// the boundary between atomic compaction units). Use this splitter to wrap
// any splitters that don't guarantee user key splits (i.e. splitters that make
// their determination in ways other than comparing the current key against a
// limit key.) If a wrapped splitter advises a split, it must continue
// to advise a split until a new output.
type userKeyChangeSplitter struct {
	cmp               Compare
	splitter          compactionOutputSplitter
	unsafePrevUserKey func() []byte
}

func (u *userKeyChangeSplitter) shouldSplitBefore(
	key *InternalKey, tw *sstable.Writer,
) compactionSplitSuggestion {
	if split := u.splitter.shouldSplitBefore(key, tw); split != splitNow {
		return split
	}
	if u.cmp(key.UserKey, u.unsafePrevUserKey()) > 0 {
		return splitNow
	}
	return noSplit
}

func (u *userKeyChangeSplitter) onNewOutput(key *InternalKey) []byte {
	return u.splitter.onNewOutput(key)
}

// compactionFile is a vfs.File wrapper that, on every write, updates a metric
// in `versions` on bytes written by in-progress compactions so far. It also
// increments a per-compaction `written` int.
type compactionFile struct {
	vfs.File

	versions *versionSet
	written  *int64
}

// Write implements the io.Writer interface.
func (c *compactionFile) Write(p []byte) (n int, err error) {
	n, err = c.File.Write(p)
	if err != nil {
		return n, err
	}

	*c.written += int64(n)
	c.versions.incrementCompactionBytes(int64(n))
	return n, err
}

type compactionKind int

const (
	compactionKindDefault compactionKind = iota
	compactionKindFlush
	compactionKindMove
	compactionKindDeleteOnly
	compactionKindElisionOnly
	compactionKindRead
	compactionKindRewrite
)

func (k compactionKind) String() string {
	switch k {
	case compactionKindDefault:
		return "default"
	case compactionKindFlush:
		return "flush"
	case compactionKindMove:
		return "move"
	case compactionKindDeleteOnly:
		return "delete-only"
	case compactionKindElisionOnly:
		return "elision-only"
	case compactionKindRead:
		return "read"
	case compactionKindRewrite:
		return "rewrite"
	}
	return "?"
}

// rangeKeyCompactionTransform is used to transform range key spans as part of the
// keyspan.MergingIter. As part of this transformation step, we can elide range
// keys in the last snapshot stripe, as well as coalesce range keys within
// snapshot stripes.
func rangeKeyCompactionTransform(
	snapshots []uint64, elideRangeKey func(start, end []byte) bool,
) keyspan.Transformer {
	return keyspan.TransformerFunc(func(cmp base.Compare, s keyspan.Span, dst *keyspan.Span) error {
		elideInLastStripe := func(keys []keyspan.Key) []keyspan.Key {
			// Unsets and deletes in the last snapshot stripe can be elided.
			k := 0
			for j := range keys {
				if elideRangeKey(s.Start, s.End) &&
					(keys[j].Kind() == InternalKeyKindRangeKeyUnset || keys[j].Kind() == InternalKeyKindRangeKeyDelete) {
					continue
				}
				keys[k] = keys[j]
				k++
			}
			keys = keys[:k]
			return keys
		}
		// snapshots are in ascending order, while s.keys are in descending seqnum
		// order. Partition s.keys by snapshot stripes, and call rangekey.Coalesce
		// on each partition.
		dst.Start = s.Start
		dst.End = s.End
		dst.Keys = dst.Keys[:0]
		i, j := len(snapshots)-1, 0
		usedLen := 0
		for i >= 0 {
			start := j
			for j < len(s.Keys) && !base.Visible(s.Keys[j].SeqNum(), snapshots[i]) {
				// Include j in current partition.
				j++
			}
			if j > start {
				keysDst := dst.Keys[usedLen:cap(dst.Keys)]
				if err := rangekey.Coalesce(cmp, s.Keys[start:j], &keysDst); err != nil {
					return err
				}
				if j == len(s.Keys) {
					// This is the last snapshot stripe. Unsets and deletes can be elided.
					keysDst = elideInLastStripe(keysDst)
				}
				usedLen += len(keysDst)
				dst.Keys = append(dst.Keys, keysDst...)
			}
			i--
		}
		if j < len(s.Keys) {
			keysDst := dst.Keys[usedLen:cap(dst.Keys)]
			if err := rangekey.Coalesce(cmp, s.Keys[j:], &keysDst); err != nil {
				return err
			}
			keysDst = elideInLastStripe(keysDst)
			usedLen += len(keysDst)
			dst.Keys = append(dst.Keys, keysDst...)
		}
		return nil
	})
}

// compaction is a table compaction from one level to the next, starting from a
// given version.
type compaction struct {
	kind      compactionKind
	cmp       Compare
	equal     Equal
	comparer  *base.Comparer
	formatKey base.FormatKey
	logger    Logger
	version   *version
	stats     base.InternalIteratorStats

	score float64

	// startLevel is the level that is being compacted. Inputs from startLevel
	// and outputLevel will be merged to produce a set of outputLevel files.
	startLevel *compactionLevel

	// outputLevel is the level that files are being produced in. outputLevel is
	// equal to startLevel+1 except when:
	//    - if startLevel is 0, the output level equals compactionPicker.baseLevel().
	//    - in multilevel compaction, the output level is the lowest level involved in
	//      the compaction
	outputLevel *compactionLevel

	// extraLevels point to additional levels in between the input and output
	// levels that get compacted in multilevel compactions
	extraLevels []*compactionLevel

	inputs []compactionLevel

	// maxOutputFileSize is the maximum size of an individual table created
	// during compaction.
	maxOutputFileSize uint64
	// maxOverlapBytes is the maximum number of bytes of overlap allowed for a
	// single output table with the tables in the grandparent level.
	maxOverlapBytes uint64
	// disableSpanElision disables elision of range tombstones and range keys. Used
	// by tests to allow range tombstones or range keys to be added to tables where
	// they would otherwise be elided.
	disableSpanElision bool

	// flushing contains the flushables (aka memtables) that are being flushed.
	flushing flushableList
	// bytesIterated contains the number of bytes that have been flushed/compacted.
	bytesIterated uint64
	// bytesWritten contains the number of bytes that have been written to outputs.
	bytesWritten int64

	// The boundaries of the input data.
	smallest InternalKey
	largest  InternalKey

	// The range deletion tombstone fragmenter. Adds range tombstones as they are
	// returned from `compactionIter` and fragments them for output to files.
	// Referenced by `compactionIter` which uses it to check whether keys are deleted.
	rangeDelFrag keyspan.Fragmenter
	// The range key fragmenter. Similar to rangeDelFrag in that it gets range
	// keys from the compaction iter and fragments them for output to files.
	rangeKeyFrag keyspan.Fragmenter
	// The range deletion tombstone iterator, that merges and fragments
	// tombstones across levels. This iterator is included within the compaction
	// input iterator as a single level.
	// TODO(jackson): Remove this when the refactor of FragmentIterator,
	// InterleavingIterator, etc is complete.
	rangeDelIter keyspan.InternalIteratorShim
	// rangeKeyInterleaving is the interleaving iter for range keys.
	rangeKeyInterleaving keyspan.InterleavingIter

	// A list of objects to close when the compaction finishes. Used by input
	// iteration to keep rangeDelIters open for the lifetime of the compaction,
	// and only close them when the compaction finishes.
	closers []io.Closer

	// grandparents are the tables in level+2 that overlap with the files being
	// compacted. Used to determine output table boundaries. Do not assume that the actual files
	// in the grandparent when this compaction finishes will be the same.
	grandparents manifest.LevelSlice

	// Boundaries at which flushes to L0 should be split. Determined by
	// L0Sublevels. If nil, flushes aren't split.
	l0Limits [][]byte

	// L0 sublevel info is used for compactions out of L0. It is nil for all
	// other compactions.
	l0SublevelInfo []sublevelInfo

	// List of disjoint inuse key ranges the compaction overlaps with in
	// grandparent and lower levels. See setupInuseKeyRanges() for the
	// construction. Used by elideTombstone() and elideRangeTombstone() to
	// determine if keys affected by a tombstone possibly exist at a lower level.
	inuseKeyRanges []manifest.UserKeyRange
	// inuseEntireRange is set if the above inuse key ranges wholly contain the
	// compaction's key range. This allows compactions in higher levels to often
	// elide key comparisons.
	inuseEntireRange    bool
	elideTombstoneIndex int

	// allowedZeroSeqNum is true if seqnums can be zeroed if there are no
	// snapshots requiring them to be kept. This determination is made by
	// looking for an sstable which overlaps the bounds of the compaction at a
	// lower level in the LSM during runCompaction.
	allowedZeroSeqNum bool

	metrics map[int]*LevelMetrics
}

func (c *compaction) makeInfo(jobID int) CompactionInfo {
	info := CompactionInfo{
		JobID:  jobID,
		Reason: c.kind.String(),
		Input:  make([]LevelInfo, 0, len(c.inputs)),
	}
	for _, cl := range c.inputs {
		inputInfo := LevelInfo{Level: cl.level, Tables: nil}
		iter := cl.files.Iter()
		for m := iter.First(); m != nil; m = iter.Next() {
			inputInfo.Tables = append(inputInfo.Tables, m.TableInfo())
		}
		info.Input = append(info.Input, inputInfo)
	}
	if c.outputLevel != nil {
		info.Output.Level = c.outputLevel.level

		// If there are no inputs from the output level (eg, a move
		// compaction), add an empty LevelInfo to info.Input.
		if len(c.inputs) > 0 && c.inputs[len(c.inputs)-1].level != c.outputLevel.level {
			info.Input = append(info.Input, LevelInfo{Level: c.outputLevel.level})
		}
	} else {
		// For a delete-only compaction, set the output level to L6. The
		// output level is not meaningful here, but complicating the
		// info.Output interface with a pointer doesn't seem worth the
		// semantic distinction.
		info.Output.Level = numLevels - 1
	}
	return info
}

func newCompaction(pc *pickedCompaction, opts *Options) *compaction {
	c := &compaction{
		kind:              compactionKindDefault,
		cmp:               pc.cmp,
		equal:             opts.equal(),
		comparer:          opts.Comparer,
		formatKey:         opts.Comparer.FormatKey,
		score:             pc.score,
		inputs:            pc.inputs,
		smallest:          pc.smallest,
		largest:           pc.largest,
		logger:            opts.Logger,
		version:           pc.version,
		maxOutputFileSize: pc.maxOutputFileSize,
		maxOverlapBytes:   pc.maxOverlapBytes,
		l0SublevelInfo:    pc.l0SublevelInfo,
	}
	c.startLevel = &c.inputs[0]
	c.outputLevel = &c.inputs[1]

	if len(pc.extraLevels) > 0 {
		c.extraLevels = pc.extraLevels
		c.outputLevel = &c.inputs[len(c.inputs)-1]
	}
	// Compute the set of outputLevel+1 files that overlap this compaction (these
	// are the grandparent sstables).
	if c.outputLevel.level+1 < numLevels {
		c.grandparents = c.version.Overlaps(c.outputLevel.level+1, c.cmp,
			c.smallest.UserKey, c.largest.UserKey, c.largest.IsExclusiveSentinel())
	}
	c.setupInuseKeyRanges()

	c.kind = pc.kind
	if c.kind == compactionKindDefault && c.outputLevel.files.Empty() && !c.hasExtraLevelData() &&
		c.startLevel.files.Len() == 1 && c.grandparents.SizeSum() <= c.maxOverlapBytes {
		// This compaction can be converted into a trivial move from one level
		// to the next. We avoid such a move if there is lots of overlapping
		// grandparent data. Otherwise, the move could create a parent file
		// that will require a very expensive merge later on.
		c.kind = compactionKindMove
	}
	return c
}

func newDeleteOnlyCompaction(opts *Options, cur *version, inputs []compactionLevel) *compaction {
	c := &compaction{
		kind:      compactionKindDeleteOnly,
		cmp:       opts.Comparer.Compare,
		equal:     opts.equal(),
		comparer:  opts.Comparer,
		formatKey: opts.Comparer.FormatKey,
		logger:    opts.Logger,
		version:   cur,
		inputs:    inputs,
	}

	// Set c.smallest, c.largest.
	files := make([]manifest.LevelIterator, 0, len(inputs))
	for _, in := range inputs {
		files = append(files, in.files.Iter())
	}
	c.smallest, c.largest = manifest.KeyRange(opts.Comparer.Compare, files...)
	return c
}

func adjustGrandparentOverlapBytesForFlush(c *compaction, flushingBytes uint64) {
	// Heuristic to place a lower bound on compaction output file size
	// caused by Lbase. Prior to this heuristic we have observed an L0 in
	// production with 310K files of which 290K files were < 10KB in size.
	// Our hypothesis is that it was caused by L1 having 2600 files and
	// ~10GB, such that each flush got split into many tiny files due to
	// overlapping with most of the files in Lbase.
	//
	// The computation below is general in that it accounts
	// for flushing different volumes of data (e.g. we may be flushing
	// many memtables). For illustration, we consider the typical
	// example of flushing a 64MB memtable. So 12.8MB output,
	// based on the compression guess below. If the compressed bytes
	// guess is an over-estimate we will end up with smaller files,
	// and if an under-estimate we will end up with larger files.
	// With a 2MB target file size, 7 files. We are willing to accept
	// 4x the number of files, if it results in better write amplification
	// when later compacting to Lbase, i.e., ~450KB files (target file
	// size / 4).
	//
	// Note that this is a pessimistic heuristic in that
	// fileCountUpperBoundDueToGrandparents could be far from the actual
	// number of files produced due to the grandparent limits. For
	// example, in the extreme, consider a flush that overlaps with 1000
	// files in Lbase f0...f999, and the initially calculated value of
	// maxOverlapBytes will cause splits at f10, f20,..., f990, which
	// means an upper bound file count of 100 files. Say the input bytes
	// in the flush are such that acceptableFileCount=10. We will fatten
	// up maxOverlapBytes by 10x to ensure that the upper bound file count
	// drops to 10. However, it is possible that in practice, even without
	// this change, we would have produced no more than 10 files, and that
	// this change makes the files unnecessarily wide. Say the input bytes
	// are distributed such that 10% are in f0...f9, 10% in f10...f19, ...
	// 10% in f80...f89 and 10% in f990...f999. The original value of
	// maxOverlapBytes would have actually produced only 10 sstables. But
	// by increasing maxOverlapBytes by 10x, we may produce 1 sstable that
	// spans f0...f89, i.e., a much wider sstable than necessary.
	//
	// We could produce a tighter estimate of
	// fileCountUpperBoundDueToGrandparents if we had knowledge of the key
	// distribution of the flush. The 4x multiplier mentioned earlier is
	// a way to try to compensate for this pessimism.
	//
	// TODO(sumeer): we don't have compression info for the data being
	// flushed, but it is likely that existing files that overlap with
	// this flush in Lbase are representative wrt compression ratio. We
	// could store the uncompressed size in FileMetadata and estimate
	// the compression ratio.
	const approxCompressionRatio = 0.2
	approxOutputBytes := approxCompressionRatio * float64(flushingBytes)
	approxNumFilesBasedOnTargetSize :=
		int(math.Ceil(approxOutputBytes / float64(c.maxOutputFileSize)))
	acceptableFileCount := float64(4 * approxNumFilesBasedOnTargetSize)
	// The byte calculation is linear in numGrandparentFiles, but we will
	// incur this linear cost in findGrandparentLimit too, so we are also
	// willing to pay it now. We could approximate this cheaply by using
	// the mean file size of Lbase.
	grandparentFileBytes := c.grandparents.SizeSum()
	fileCountUpperBoundDueToGrandparents :=
		float64(grandparentFileBytes) / float64(c.maxOverlapBytes)
	if fileCountUpperBoundDueToGrandparents > acceptableFileCount {
		c.maxOverlapBytes = uint64(
			float64(c.maxOverlapBytes) *
				(fileCountUpperBoundDueToGrandparents / acceptableFileCount))
	}
}

func newFlush(opts *Options, cur *version, baseLevel int, flushing flushableList) *compaction {
	c := &compaction{
		kind:              compactionKindFlush,
		cmp:               opts.Comparer.Compare,
		equal:             opts.equal(),
		comparer:          opts.Comparer,
		formatKey:         opts.Comparer.FormatKey,
		logger:            opts.Logger,
		version:           cur,
		inputs:            []compactionLevel{{level: -1}, {level: 0}},
		maxOutputFileSize: math.MaxUint64,
		maxOverlapBytes:   math.MaxUint64,
		flushing:          flushing,
	}
	c.startLevel = &c.inputs[0]
	c.outputLevel = &c.inputs[1]
	if cur.L0Sublevels != nil {
		c.l0Limits = cur.L0Sublevels.FlushSplitKeys()
	}

	smallestSet, largestSet := false, false
	updatePointBounds := func(iter internalIterator) {
		if key, _ := iter.First(); key != nil {
			if !smallestSet ||
				base.InternalCompare(c.cmp, c.smallest, *key) > 0 {
				smallestSet = true
				c.smallest = key.Clone()
			}
		}
		if key, _ := iter.Last(); key != nil {
			if !largestSet ||
				base.InternalCompare(c.cmp, c.largest, *key) < 0 {
				largestSet = true
				c.largest = key.Clone()
			}
		}
	}

	updateRangeBounds := func(iter keyspan.FragmentIterator) {
		// File bounds require s != nil && !s.Empty(). We only need to check for
		// s != nil here, as the memtable's FragmentIterator would never surface
		// empty spans.
		if s := iter.First(); s != nil {
			if key := s.SmallestKey(); !smallestSet ||
				base.InternalCompare(c.cmp, c.smallest, key) > 0 {
				smallestSet = true
				c.smallest = key.Clone()
			}
		}
		if s := iter.Last(); s != nil {
			if key := s.LargestKey(); !largestSet ||
				base.InternalCompare(c.cmp, c.largest, key) < 0 {
				largestSet = true
				c.largest = key.Clone()
			}
		}
	}

	var flushingBytes uint64
	for i := range flushing {
		f := flushing[i]
		updatePointBounds(f.newIter(nil))
		if rangeDelIter := f.newRangeDelIter(nil); rangeDelIter != nil {
			updateRangeBounds(rangeDelIter)
		}
		if rangeKeyIter := f.newRangeKeyIter(nil); rangeKeyIter != nil {
			updateRangeBounds(rangeKeyIter)
		}
		flushingBytes += f.inuseBytes()
	}

	if opts.FlushSplitBytes > 0 {
		c.maxOutputFileSize = uint64(opts.Level(0).TargetFileSize)
		c.maxOverlapBytes = maxGrandparentOverlapBytes(opts, 0)
		c.grandparents = c.version.Overlaps(baseLevel, c.cmp, c.smallest.UserKey,
			c.largest.UserKey, c.largest.IsExclusiveSentinel())
		adjustGrandparentOverlapBytesForFlush(c, flushingBytes)
	}

	c.setupInuseKeyRanges()
	return c
}

func (c *compaction) hasExtraLevelData() bool {
	if len(c.extraLevels) == 0 {
		// not a multi level compaction
		return false
	} else if c.extraLevels[0].files.Empty() {
		// a multi level compaction without data in the intermediate input level;
		// e.g. for a multi level compaction with levels 4,5, and 6, this could
		// occur if there is no files to compact in 5, or in 5 and 6 (i.e. a move).
		return false
	}
	return true
}

func (c *compaction) setupInuseKeyRanges() {
	level := c.outputLevel.level + 1
	if c.outputLevel.level == 0 {
		level = 0
	}
	// calculateInuseKeyRanges will return a series of sorted spans. Overlapping
	// or abutting spans have already been merged.
	c.inuseKeyRanges = calculateInuseKeyRanges(
		c.version, c.cmp, level, numLevels-1, c.smallest.UserKey, c.largest.UserKey,
	)
	// Check if there's a single in-use span that encompasses the entire key
	// range of the compaction. This is an optimization to avoid key comparisons
	// against inuseKeyRanges during the compaction when every key within the
	// compaction overlaps with an in-use span.
	if len(c.inuseKeyRanges) > 0 {
		c.inuseEntireRange = c.cmp(c.inuseKeyRanges[0].Start, c.smallest.UserKey) <= 0 &&
			c.cmp(c.inuseKeyRanges[0].End, c.largest.UserKey) >= 0
	}
}

func calculateInuseKeyRanges(
	v *version, cmp base.Compare, level, maxLevel int, smallest, largest []byte,
) []manifest.UserKeyRange {
	// Use two slices, alternating which one is input and which one is output
	// as we descend the LSM.
	var input, output []manifest.UserKeyRange

	// L0 requires special treatment, since sstables within L0 may overlap.
	// We use the L0 Sublevels structure to efficiently calculate the merged
	// in-use key ranges.
	if level == 0 {
		output = v.L0Sublevels.InUseKeyRanges(smallest, largest)
		level++
	}

	for ; level <= maxLevel; level++ {
		// NB: We always treat `largest` as inclusive for simplicity, because
		// there's little consequence to calculating slightly broader in-use key
		// ranges.
		overlaps := v.Overlaps(level, cmp, smallest, largest, false /* exclusiveEnd */)
		iter := overlaps.Iter()

		// We may already have in-use key ranges from higher levels. Iterate
		// through both our accumulated in-use key ranges and this level's
		// files, merging the two.
		//
		// Tables higher within the LSM have broader key spaces. We use this
		// when possible to seek past a level's files that are contained by
		// our current accumulated in-use key ranges. This helps avoid
		// per-sstable work during flushes or compactions in high levels which
		// overlap the majority of the LSM's sstables.
		input, output = output, input
		output = output[:0]

		var currFile *fileMetadata
		var currAccum *manifest.UserKeyRange
		if len(input) > 0 {
			currAccum, input = &input[0], input[1:]
		}

		// If we have an accumulated key range and its start is ≤ smallest,
		// we can seek to the accumulated range's end. Otherwise, we need to
		// start at the first overlapping file within the level.
		if currAccum != nil && cmp(currAccum.Start, smallest) <= 0 {
			currFile = seekGT(&iter, cmp, currAccum.End)
		} else {
			currFile = iter.First()
		}

		for currFile != nil || currAccum != nil {
			// If we've exhausted either the files in the level or the
			// accumulated key ranges, we just need to append the one we have.
			// If we have both a currFile and a currAccum, they either overlap
			// or they're disjoint. If they're disjoint, we append whichever
			// one sorts first and move on to the next file or range. If they
			// overlap, we merge them into currAccum and proceed to the next
			// file.
			switch {
			case currAccum == nil || (currFile != nil && cmp(currFile.Largest.UserKey, currAccum.Start) < 0):
				// This file is strictly before the current accumulated range,
				// or there are no more accumulated ranges.
				output = append(output, manifest.UserKeyRange{
					Start: currFile.Smallest.UserKey,
					End:   currFile.Largest.UserKey,
				})
				currFile = iter.Next()
			case currFile == nil || (currAccum != nil && cmp(currAccum.End, currFile.Smallest.UserKey) < 0):
				// The current accumulated key range is strictly before the
				// current file, or there are no more files.
				output = append(output, *currAccum)
				currAccum = nil
				if len(input) > 0 {
					currAccum, input = &input[0], input[1:]
				}
			default:
				// The current accumulated range and the current file overlap.
				// Adjust the accumulated range to be the union.
				if cmp(currFile.Smallest.UserKey, currAccum.Start) < 0 {
					currAccum.Start = currFile.Smallest.UserKey
				}
				if cmp(currFile.Largest.UserKey, currAccum.End) > 0 {
					currAccum.End = currFile.Largest.UserKey
				}

				// Extending `currAccum`'s end boundary may have caused it to
				// overlap with `input` key ranges that we haven't processed
				// yet. Merge any such key ranges.
				for len(input) > 0 && cmp(input[0].Start, currAccum.End) <= 0 {
					if cmp(input[0].End, currAccum.End) > 0 {
						currAccum.End = input[0].End
					}
					input = input[1:]
				}
				// Seek the level iterator past our current accumulated end.
				currFile = seekGT(&iter, cmp, currAccum.End)
			}
		}
	}
	return output
}

func seekGT(iter *manifest.LevelIterator, cmp base.Compare, key []byte) *manifest.FileMetadata {
	f := iter.SeekGE(cmp, key)
	for f != nil && cmp(f.Largest.UserKey, key) == 0 {
		f = iter.Next()
	}
	return f
}

// findGrandparentLimit takes the start user key for a table and returns the
// user key to which that table can extend without excessively overlapping
// the grandparent level. If no limit is needed considering the grandparent
// files, this function returns nil. This is done in order to prevent a table
// at level N from overlapping too much data at level N+1. We want to avoid
// such large overlaps because they translate into large compactions. The
// current heuristic stops output of a table if the addition of another key
// would cause the table to overlap more than 10x the target file size at
// level N. See maxGrandparentOverlapBytes.
func (c *compaction) findGrandparentLimit(start []byte) []byte {
	iter := c.grandparents.Iter()
	var overlappedBytes uint64
	var greater bool
	for f := iter.SeekGE(c.cmp, start); f != nil; f = iter.Next() {
		overlappedBytes += f.Size
		// To ensure forward progress we always return a larger user
		// key than where we started. See comments above clients of
		// this function for how this is used.
		greater = greater || c.cmp(f.Smallest.UserKey, start) > 0
		if !greater {
			continue
		}

		// We return the smallest bound of a sstable rather than the
		// largest because the smallest is always inclusive, and limits
		// are used exlusively when truncating range tombstones. If we
		// truncated an output to the largest key while there's a
		// pending tombstone, the next output file would also overlap
		// the same grandparent f.
		if overlappedBytes > c.maxOverlapBytes {
			return f.Smallest.UserKey
		}
	}
	return nil
}

// findL0Limit takes the start key for a table and returns the user key to which
// that table can be extended without hitting the next l0Limit. Having flushed
// sstables "bridging across" an l0Limit could lead to increased L0 -> LBase
// compaction sizes as well as elevated read amplification.
func (c *compaction) findL0Limit(start []byte) []byte {
	if c.startLevel.level > -1 || c.outputLevel.level != 0 || len(c.l0Limits) == 0 {
		return nil
	}
	index := sort.Search(len(c.l0Limits), func(i int) bool {
		return c.cmp(c.l0Limits[i], start) > 0
	})
	if index < len(c.l0Limits) {
		return c.l0Limits[index]
	}
	return nil
}

// errorOnUserKeyOverlap returns an error if the last two written sstables in
// this compaction have revisions of the same user key present in both sstables,
// when it shouldn't (eg. when splitting flushes).
func (c *compaction) errorOnUserKeyOverlap(ve *versionEdit) error {
	if n := len(ve.NewFiles); n > 1 {
		meta := ve.NewFiles[n-1].Meta
		prevMeta := ve.NewFiles[n-2].Meta
		if !prevMeta.Largest.IsExclusiveSentinel() &&
			c.cmp(prevMeta.Largest.UserKey, meta.Smallest.UserKey) >= 0 {
			return errors.Errorf("pebble: compaction split user key across two sstables: %s in %s and %s",
				prevMeta.Largest.Pretty(c.formatKey),
				prevMeta.FileNum,
				meta.FileNum)
		}
	}
	return nil
}

// allowZeroSeqNum returns true if seqnum's can be zeroed if there are no
// snapshots requiring them to be kept. It performs this determination by
// looking for an sstable which overlaps the bounds of the compaction at a
// lower level in the LSM.
func (c *compaction) allowZeroSeqNum() bool {
	return c.elideRangeTombstone(c.smallest.UserKey, c.largest.UserKey)
}

// elideTombstone returns true if it is ok to elide a tombstone for the
// specified key. A return value of true guarantees that there are no key/value
// pairs at c.level+2 or higher that possibly contain the specified user
// key. The keys in multiple invocations to elideTombstone must be supplied in
// order.
func (c *compaction) elideTombstone(key []byte) bool {
	if c.inuseEntireRange || len(c.flushing) != 0 {
		return false
	}

	for ; c.elideTombstoneIndex < len(c.inuseKeyRanges); c.elideTombstoneIndex++ {
		r := &c.inuseKeyRanges[c.elideTombstoneIndex]
		if c.cmp(key, r.End) <= 0 {
			if c.cmp(key, r.Start) >= 0 {
				return false
			}
			break
		}
	}
	return true
}

// elideRangeTombstone returns true if it is ok to elide the specified range
// tombstone. A return value of true guarantees that there are no key/value
// pairs at c.outputLevel.level+1 or higher that possibly overlap the specified
// tombstone.
func (c *compaction) elideRangeTombstone(start, end []byte) bool {
	// Disable range tombstone elision if the testing knob for that is enabled,
	// or if we are flushing memtables. The latter requirement is due to
	// inuseKeyRanges not accounting for key ranges in other memtables that are
	// being flushed in the same compaction. It's possible for a range tombstone
	// in one memtable to overlap keys in a preceding memtable in c.flushing.
	//
	// This function is also used in setting allowZeroSeqNum, so disabling
	// elision of range tombstones also disables zeroing of SeqNums.
	//
	// TODO(peter): we disable zeroing of seqnums during flushing to match
	// RocksDB behavior and to avoid generating overlapping sstables during
	// DB.replayWAL. When replaying WAL files at startup, we flush after each
	// WAL is replayed building up a single version edit that is
	// applied. Because we don't apply the version edit after each flush, this
	// code doesn't know that L0 contains files and zeroing of seqnums should
	// be disabled. That is fixable, but it seems safer to just match the
	// RocksDB behavior for now.
	if c.disableSpanElision || len(c.flushing) != 0 {
		return false
	}

	lower := sort.Search(len(c.inuseKeyRanges), func(i int) bool {
		return c.cmp(c.inuseKeyRanges[i].End, start) >= 0
	})
	upper := sort.Search(len(c.inuseKeyRanges), func(i int) bool {
		return c.cmp(c.inuseKeyRanges[i].Start, end) > 0
	})
	return lower >= upper
}

// elideRangeKey returns true if it is ok to elide the specified range key. A
// return value of true guarantees that there are no key/value pairs at
// c.outputLevel.level+1 or higher that possibly overlap the specified range key.
func (c *compaction) elideRangeKey(start, end []byte) bool {
	// TODO(bilal): Track inuseKeyRanges separately for the range keyspace as
	// opposed to the point keyspace. Once that is done, elideRangeTombstone
	// can just check in the point keyspace, and this function can check for
	// inuseKeyRanges in the range keyspace.
	return c.elideRangeTombstone(start, end)
}

// newInputIter returns an iterator over all the input tables in a compaction.
func (c *compaction) newInputIter(
	newIters tableNewIters, newRangeKeyIter keyspan.TableNewSpanIter, snapshots []uint64,
) (_ internalIterator, retErr error) {
	var rangeDelIters []keyspan.FragmentIterator
	var rangeKeyIters []keyspan.FragmentIterator

	if len(c.flushing) != 0 {
		if len(c.flushing) == 1 {
			f := c.flushing[0]
			iter := f.newFlushIter(nil, &c.bytesIterated)
			if rangeDelIter := f.newRangeDelIter(nil); rangeDelIter != nil {
				c.rangeDelIter.Init(c.cmp, rangeDelIter)
				iter = newMergingIter(c.logger, &c.stats, c.cmp, nil, iter, &c.rangeDelIter)
			}
			if rangeKeyIter := f.newRangeKeyIter(nil); rangeKeyIter != nil {
				mi := &keyspan.MergingIter{}
				mi.Init(c.cmp, rangeKeyCompactionTransform(snapshots, c.elideRangeKey), rangeKeyIter)
				c.rangeKeyInterleaving.Init(c.comparer, iter, mi, nil /* hooks */, nil /* lowerBound */, nil /* upperBound */)
				iter = &c.rangeKeyInterleaving
			}
			return iter, nil
		}
		iters := make([]internalIterator, 0, len(c.flushing)+1)
		rangeDelIters = make([]keyspan.FragmentIterator, 0, len(c.flushing))
		rangeKeyIters = make([]keyspan.FragmentIterator, 0, len(c.flushing))
		for i := range c.flushing {
			f := c.flushing[i]
			iters = append(iters, f.newFlushIter(nil, &c.bytesIterated))
			rangeDelIter := f.newRangeDelIter(nil)
			if rangeDelIter != nil {
				rangeDelIters = append(rangeDelIters, rangeDelIter)
			}
			if rangeKeyIter := f.newRangeKeyIter(nil); rangeKeyIter != nil {
				rangeKeyIters = append(rangeKeyIters, rangeKeyIter)
			}
		}
		if len(rangeDelIters) > 0 {
			c.rangeDelIter.Init(c.cmp, rangeDelIters...)
			iters = append(iters, &c.rangeDelIter)
		}
		var iter internalIterator = newMergingIter(c.logger, &c.stats, c.cmp, nil, iters...)
		if len(rangeKeyIters) > 0 {
			mi := &keyspan.MergingIter{}
			mi.Init(c.cmp, rangeKeyCompactionTransform(snapshots, c.elideRangeKey), rangeKeyIters...)
			c.rangeKeyInterleaving.Init(c.comparer, iter, mi, nil /* hooks */, nil /* lowerBound */, nil /* upperBound */)
			iter = &c.rangeKeyInterleaving
		}
		return iter, nil
	}

	if c.startLevel.level >= 0 {
		err := manifest.CheckOrdering(c.cmp, c.formatKey,
			manifest.Level(c.startLevel.level), c.startLevel.files.Iter())
		if err != nil {
			return nil, err
		}
	}
	err := manifest.CheckOrdering(c.cmp, c.formatKey,
		manifest.Level(c.outputLevel.level), c.outputLevel.files.Iter())
	if err != nil {
		return nil, err
	}

	if c.startLevel.level == 0 {
		if c.l0SublevelInfo == nil {
			panic("l0SublevelInfo not created for compaction out of L0")
		}

		for _, info := range c.l0SublevelInfo {
			err := manifest.CheckOrdering(c.cmp, c.formatKey,
				info.sublevel, info.Iter())
			if err != nil {
				return nil, err
			}
		}
	}

	if len(c.extraLevels) > 0 {
		if len(c.extraLevels) > 1 {
			panic("n>2 multi level compaction not implemented yet")
		}
		interLevel := c.extraLevels[0]
		err := manifest.CheckOrdering(c.cmp, c.formatKey,
			manifest.Level(interLevel.level), interLevel.files.Iter())
		if err != nil {
			return nil, err
		}
	}
	iters := make([]internalIterator, 0, len(c.inputs)*c.startLevel.files.Len()+1)
	defer func() {
		if retErr != nil {
			for _, iter := range iters {
				if iter != nil {
					iter.Close()
				}
			}
			for _, rangeDelIter := range rangeDelIters {
				rangeDelIter.Close()
			}
		}
	}()

	// In normal operation, levelIter iterates over the point operations in a
	// level, and initializes a rangeDelIter pointer for the range deletions in
	// each table. During compaction, we want to iterate over the merged view of
	// point operations and range deletions. In order to do this we create one
	// levelIter per level to iterate over the point operations, and collect up
	// all the range deletion files.
	//
	// The range deletion levels are first combined with a keyspan.MergingIter
	// (currently wrapped by a keyspan.InternalIteratorShim to satisfy the
	// internal iterator interface). The resulting merged rangedel iterator is
	// then included with the point levels in a single mergingIter.
	newRangeDelIter := func(
		f *manifest.FileMetadata, slice manifest.LevelSlice, _ *IterOptions, bytesIterated *uint64,
	) (keyspan.FragmentIterator, error) {
		iter, rangeDelIter, err := newIters(f, nil, /* iter options */
			internalIterOpts{bytesIterated: &c.bytesIterated})
		if err == nil {
			// TODO(peter): It is mildly wasteful to open the point iterator only to
			// immediately close it. One way to solve this would be to add new
			// methods to tableCache for creating point and range-deletion iterators
			// independently. We'd only want to use those methods here,
			// though. Doesn't seem worth the hassle in the near term.
			if err = iter.Close(); err != nil {
				rangeDelIter.Close()
				rangeDelIter = nil
			}
		}
		if rangeDelIter != nil {
			// Ensure that rangeDelIter is not closed until the compaction is
			// finished. This is necessary because range tombstone processing
			// requires the range tombstones to be held in memory for up to the
			// lifetime of the compaction.
			c.closers = append(c.closers, rangeDelIter)
			rangeDelIter = noCloseIter{rangeDelIter}

			// Truncate the range tombstones returned by the iterator to the
			// upper bound of the atomic compaction unit. Note that we need do
			// this truncation at read time in order to handle sstables
			// generated by RocksDB and earlier versions of Pebble which do not
			// truncate range tombstones to atomic compaction unit boundaries at
			// write time.
			//
			// The current Pebble compaction logic DOES truncate tombstones to
			// atomic unit boundaries at compaction time too.
			atomicUnit, _ := expandToAtomicUnit(c.cmp, slice, true /* disableIsCompacting */)
			lowerBound, upperBound := manifest.KeyRange(c.cmp, atomicUnit.Iter())
			// Range deletion tombstones are often written to sstables
			// untruncated on the end key side. However, they are still only
			// valid within a given file's bounds. The logic for writing range
			// tombstones to an output file sometimes has an incomplete view
			// of range tombstones outside the file's internal key bounds. Skip
			// any range tombstones completely outside file bounds.
			rangeDelIter = keyspan.Truncate(
				c.cmp, rangeDelIter, lowerBound.UserKey, upperBound.UserKey, &f.Smallest, &f.Largest)
		}
		if rangeDelIter == nil {
			rangeDelIter = emptyKeyspanIter
		}
		return rangeDelIter, err
	}

	iterOpts := IterOptions{logger: c.logger}
	// TODO(bananabrick): Get rid of the extra manifest.Level parameter and fold it into
	// compactionLevel.
	addItersForLevel := func(level *compactionLevel, l manifest.Level) error {
		iters = append(iters, newLevelIter(iterOpts, c.cmp, nil /* split */, newIters,
			level.files.Iter(), l, &c.bytesIterated))
		// Create a wrapping closure to turn newRangeDelIter into a
		// keyspan.TableNewSpanIter, and return a LevelIter that lazily creates
		// rangedel iterators. This is safe now that range deletions are truncated
		// at file bounds; the merging iterator no longer needs to see all range
		// deletes for correctness.
		wrapper := func(file *manifest.FileMetadata, iterOptions *keyspan.SpanIterOptions) (keyspan.FragmentIterator, error) {
			return newRangeDelIter(file, level.files, nil, &c.bytesIterated)
		}
		li := &keyspan.LevelIter{}
		li.Init(keyspan.SpanIterOptions{}, c.cmp, wrapper, level.files.Iter(), l, c.logger, manifest.KeyTypePoint)
		rangeDelIters = append(rangeDelIters, li)
		// Check if this level has any range keys.
		hasRangeKeys := false
		iter := level.files.Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			if f.HasRangeKeys {
				hasRangeKeys = true
				break
			}
		}
		if hasRangeKeys {
			li := &keyspan.LevelIter{}
			newRangeKeyIterWrapper := func(file *manifest.FileMetadata, iterOptions *keyspan.SpanIterOptions) (keyspan.FragmentIterator, error) {
				iter, err := newRangeKeyIter(file, iterOptions)
				if iter != nil {
					// Ensure that the range key iter is not closed until the compaction is
					// finished. This is necessary because range key processing
					// requires the range keys to be held in memory for up to the
					// lifetime of the compaction.
					c.closers = append(c.closers, iter)
					iter = noCloseIter{iter}

					// We do not need to truncate range keys to sstable boundaries, or
					// only read within the file's atomic compaction units, unlike with
					// range tombstones. This is because range keys were added after we
					// stopped splitting user keys across sstables, so all the range keys
					// in this sstable must wholly lie within the file's bounds.
				}
				if iter == nil {
					iter = emptyKeyspanIter
				}
				return iter, err
			}
			li.Init(keyspan.SpanIterOptions{}, c.cmp, newRangeKeyIterWrapper, level.files.Iter(), l, c.logger, manifest.KeyTypeRange)
			rangeKeyIters = append(rangeKeyIters, li)
		}
		return nil
	}

	if c.startLevel.level != 0 {
		if err = addItersForLevel(c.startLevel, manifest.Level(c.startLevel.level)); err != nil {
			return nil, err
		}
	} else {
		for _, info := range c.l0SublevelInfo {
			if err = addItersForLevel(
				&compactionLevel{0, info.LevelSlice}, info.sublevel); err != nil {
				return nil, err
			}
		}
	}
	if len(c.extraLevels) > 0 {
		if err = addItersForLevel(c.extraLevels[0], manifest.Level(c.extraLevels[0].level)); err != nil {
			return nil, err
		}
	}
	if err = addItersForLevel(c.outputLevel, manifest.Level(c.outputLevel.level)); err != nil {
		return nil, err
	}

	// Combine all the rangedel iterators using a keyspan.MergingIterator and a
	// InternalIteratorShim so that the range deletions may be interleaved in
	// the compaction input.
	// TODO(jackson): Replace the InternalIteratorShim with an interleaving
	// iterator.
	if len(rangeDelIters) > 0 {
		c.rangeDelIter.Init(c.cmp, rangeDelIters...)
		iters = append(iters, &c.rangeDelIter)
	}
	pointKeyIter := newMergingIter(c.logger, &c.stats, c.cmp, nil, iters...)
	if len(rangeKeyIters) > 0 {
		mi := &keyspan.MergingIter{}
		mi.Init(c.cmp, rangeKeyCompactionTransform(snapshots, c.elideRangeKey), rangeKeyIters...)
		di := &keyspan.DefragmentingIter{}
		di.Init(c.comparer, mi, keyspan.DefragmentInternal, keyspan.StaticDefragmentReducer)
		c.rangeKeyInterleaving.Init(c.comparer, pointKeyIter, di, nil /* hooks */, nil /* lowerBound */, nil /* upperBound */)
		return &c.rangeKeyInterleaving, nil
	}

	return pointKeyIter, nil
}

func (c *compaction) String() string {
	if len(c.flushing) != 0 {
		return "flush\n"
	}

	var buf bytes.Buffer
	for level := c.startLevel.level; level <= c.outputLevel.level; level++ {
		i := level - c.startLevel.level
		fmt.Fprintf(&buf, "%d:", level)
		iter := c.inputs[i].files.Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			fmt.Fprintf(&buf, " %s:%s-%s", f.FileNum, f.Smallest, f.Largest)
		}
		fmt.Fprintf(&buf, "\n")
	}
	return buf.String()
}

type manualCompaction struct {
	// Count of the retries either due to too many concurrent compactions, or a
	// concurrent compaction to overlapping levels.
	retries     int
	level       int
	outputLevel int
	done        chan error
	start       []byte
	end         []byte
	split       bool
}

type readCompaction struct {
	level int
	// [start, end] key ranges are used for de-duping.
	start []byte
	end   []byte

	// The file associated with the compaction.
	// If the file no longer belongs in the same
	// level, then we skip the compaction.
	fileNum base.FileNum
}

func (d *DB) addInProgressCompaction(c *compaction) {
	d.mu.compact.inProgress[c] = struct{}{}
	var isBase, isIntraL0 bool
	for _, cl := range c.inputs {
		iter := cl.files.Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			if f.IsCompacting() {
				d.opts.Logger.Fatalf("L%d->L%d: %s already being compacted", c.startLevel.level, c.outputLevel.level, f.FileNum)
			}
			f.SetCompactionState(manifest.CompactionStateCompacting)
			if c.startLevel != nil && c.outputLevel != nil && c.startLevel.level == 0 {
				if c.outputLevel.level == 0 {
					f.IsIntraL0Compacting = true
					isIntraL0 = true
				} else {
					isBase = true
				}
			}
		}
	}

	if (isIntraL0 || isBase) && c.version.L0Sublevels != nil {
		l0Inputs := []manifest.LevelSlice{c.startLevel.files}
		if isIntraL0 {
			l0Inputs = append(l0Inputs, c.outputLevel.files)
		}
		if err := c.version.L0Sublevels.UpdateStateForStartedCompaction(l0Inputs, isBase); err != nil {
			d.opts.Logger.Fatalf("could not update state for compaction: %s", err)
		}
	}

	if false {
		// TODO(peter): Do we want to keep this? It is useful for seeing the
		// concurrent compactions/flushes that are taking place. Right now, this
		// spams the logs and output to tests. Figure out a way to useful expose
		// it.
		strs := make([]string, 0, len(d.mu.compact.inProgress))
		for c := range d.mu.compact.inProgress {
			var s string
			if c.startLevel.level == -1 {
				s = fmt.Sprintf("mem->L%d", c.outputLevel.level)
			} else {
				s = fmt.Sprintf("L%d->L%d:%.1f", c.startLevel.level, c.outputLevel.level, c.score)
			}
			strs = append(strs, s)
		}
		// This odd sorting function is intended to sort "mem" before "L*".
		sort.Slice(strs, func(i, j int) bool {
			if strs[i][0] == strs[j][0] {
				return strs[i] < strs[j]
			}
			return strs[i] > strs[j]
		})
		d.opts.Logger.Infof("compactions: %s", strings.Join(strs, " "))
	}
}

// Removes compaction markers from files in a compaction. The rollback parameter
// indicates whether the compaction state should be rolled back to its original
// state in the case of an unsuccessful compaction.
//
// DB.mu must be held when calling this method. All writes to the manifest for
// this compaction should have completed by this point.
func (d *DB) removeInProgressCompaction(c *compaction, rollback bool) {
	for _, cl := range c.inputs {
		iter := cl.files.Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			if !f.IsCompacting() {
				d.opts.Logger.Fatalf("L%d->L%d: %s not being compacted", c.startLevel.level, c.outputLevel.level, f.FileNum)
			}
			if !rollback {
				// On success all compactions other than move-compactions transition the
				// file into the Compacted state. Move-compacted files become eligible
				// for compaction again and transition back to NotCompacting.
				if c.kind != compactionKindMove {
					f.SetCompactionState(manifest.CompactionStateCompacted)
				} else {
					f.SetCompactionState(manifest.CompactionStateNotCompacting)
				}
			} else {
				// Else, on rollback, all input files unconditionally transition back to
				// NotCompacting.
				f.SetCompactionState(manifest.CompactionStateNotCompacting)
			}
			f.IsIntraL0Compacting = false
		}
	}
	delete(d.mu.compact.inProgress, c)

	l0InProgress := inProgressL0Compactions(d.getInProgressCompactionInfoLocked(c))
	d.mu.versions.currentVersion().L0Sublevels.InitCompactingFileInfo(l0InProgress)
}

func (d *DB) calculateDiskAvailableBytes() uint64 {
	if space, err := d.opts.FS.GetDiskUsage(d.dirname); err == nil {
		atomic.StoreUint64(&d.atomic.diskAvailBytes, space.AvailBytes)
		return space.AvailBytes
	} else if !errors.Is(err, vfs.ErrUnsupported) {
		d.opts.EventListener.BackgroundError(err)
	}
	return atomic.LoadUint64(&d.atomic.diskAvailBytes)
}

func (d *DB) getDiskAvailableBytesCached() uint64 {
	return atomic.LoadUint64(&d.atomic.diskAvailBytes)
}

func (d *DB) getDeletionPacerInfo() deletionPacerInfo {
	var pacerInfo deletionPacerInfo
	// Call GetDiskUsage after every file deletion. This may seem inefficient,
	// but in practice this was observed to take constant time, regardless of
	// volume size used, at least on linux with ext4 and zfs. All invocations
	// take 10 microseconds or less.
	pacerInfo.freeBytes = d.calculateDiskAvailableBytes()
	d.mu.Lock()
	pacerInfo.obsoleteBytes = d.mu.versions.metrics.Table.ObsoleteSize
	pacerInfo.liveBytes = uint64(d.mu.versions.metrics.Total().Size)
	d.mu.Unlock()
	return pacerInfo
}

// maybeScheduleFlush schedules a flush if necessary.
//
// d.mu must be held when calling this.
func (d *DB) maybeScheduleFlush() {
	if d.mu.compact.flushing || d.closed.Load() != nil || d.opts.ReadOnly {
		return
	}
	if len(d.mu.mem.queue) <= 1 {
		return
	}

	if !d.passedFlushThreshold() {
		return
	}

	d.mu.compact.flushing = true
	go d.flush()
}

func (d *DB) passedFlushThreshold() bool {
	var n int
	var size uint64
	for ; n < len(d.mu.mem.queue)-1; n++ {
		if !d.mu.mem.queue[n].readyForFlush() {
			break
		}
		if d.mu.mem.queue[n].flushForced {
			// A flush was forced. Pretend the memtable size is the configured
			// size. See minFlushSize below.
			size += uint64(d.opts.MemTableSize)
		} else {
			size += d.mu.mem.queue[n].totalBytes()
		}
	}
	if n == 0 {
		// None of the immutable memtables are ready for flushing.
		return false
	}

	// Only flush once the sum of the queued memtable sizes exceeds half the
	// configured memtable size. This prevents flushing of memtables at startup
	// while we're undergoing the ramp period on the memtable size. See
	// DB.newMemTable().
	minFlushSize := uint64(d.opts.MemTableSize) / 2
	return size >= minFlushSize
}

func (d *DB) maybeScheduleDelayedFlush(tbl *memTable, dur time.Duration) {
	var mem *flushableEntry
	for _, m := range d.mu.mem.queue {
		if m.flushable == tbl {
			mem = m
			break
		}
	}
	if mem == nil || mem.flushForced {
		return
	}
	deadline := d.timeNow().Add(dur)
	if !mem.delayedFlushForcedAt.IsZero() && deadline.After(mem.delayedFlushForcedAt) {
		// Already scheduled to flush sooner than within `dur`.
		return
	}
	mem.delayedFlushForcedAt = deadline
	go func() {
		timer := time.NewTimer(dur)
		defer timer.Stop()

		select {
		case <-d.closedCh:
			return
		case <-mem.flushed:
			return
		case <-timer.C:
			d.commit.mu.Lock()
			defer d.commit.mu.Unlock()
			d.mu.Lock()
			defer d.mu.Unlock()

			// NB: The timer may fire concurrently with a call to Close.  If a
			// Close call beat us to acquiring d.mu, d.closed holds ErrClosed,
			// and it's too late to flush anything. Otherwise, the Close call
			// will block on locking d.mu until we've finished scheduling the
			// flush and set `d.mu.compact.flushing` to true. Close will wait
			// for the current flush to complete.
			if d.closed.Load() != nil {
				return
			}

			if d.mu.mem.mutable == tbl {
				d.makeRoomForWrite(nil)
			} else {
				mem.flushForced = true
				d.maybeScheduleFlush()
			}
		}
	}()
}

func (d *DB) flush() {
	pprof.Do(context.Background(), flushLabels, func(context.Context) {
		flushingWorkStart := time.Now()
		d.mu.Lock()
		defer d.mu.Unlock()
		idleDuration := flushingWorkStart.Sub(d.mu.compact.noOngoingFlushStartTime)
		var bytesFlushed uint64
		var err error
		if bytesFlushed, err = d.flush1(); err != nil {
			// TODO(peter): count consecutive flush errors and backoff.
			d.opts.EventListener.BackgroundError(err)
		}
		d.mu.compact.flushing = false
		d.mu.compact.noOngoingFlushStartTime = time.Now()
		workDuration := d.mu.compact.noOngoingFlushStartTime.Sub(flushingWorkStart)
		d.mu.compact.flushWriteThroughput.Bytes += int64(bytesFlushed)
		d.mu.compact.flushWriteThroughput.WorkDuration += workDuration
		d.mu.compact.flushWriteThroughput.IdleDuration += idleDuration
		// More flush work may have arrived while we were flushing, so schedule
		// another flush if needed.
		d.maybeScheduleFlush()
		// The flush may have produced too many files in a level, so schedule a
		// compaction if needed.
		d.maybeScheduleCompaction()
		d.mu.compact.cond.Broadcast()
	})
}

// flush runs a compaction that copies the immutable memtables from memory to
// disk.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) flush1() (bytesFlushed uint64, err error) {
	var n int
	for ; n < len(d.mu.mem.queue)-1; n++ {
		if !d.mu.mem.queue[n].readyForFlush() {
			break
		}
	}
	if n == 0 {
		// None of the immutable memtables are ready for flushing.
		return 0, nil
	}

	// Require that every memtable being flushed has a log number less than the
	// new minimum unflushed log number.
	minUnflushedLogNum := d.mu.mem.queue[n].logNum
	if !d.opts.DisableWAL {
		for i := 0; i < n; i++ {
			logNum := d.mu.mem.queue[i].logNum
			if logNum >= minUnflushedLogNum {
				return 0, errFlushInvariant
			}
		}
	}

	c := newFlush(d.opts, d.mu.versions.currentVersion(),
		d.mu.versions.picker.getBaseLevel(), d.mu.mem.queue[:n])
	d.addInProgressCompaction(c)

	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	d.opts.EventListener.FlushBegin(FlushInfo{
		JobID: jobID,
		Input: n,
	})
	startTime := d.timeNow()

	ve, pendingOutputs, err := d.runCompaction(jobID, c)

	info := FlushInfo{
		JobID:    jobID,
		Input:    n,
		Duration: d.timeNow().Sub(startTime),
		Done:     true,
		Err:      err,
	}
	if err == nil {
		for i := range ve.NewFiles {
			e := &ve.NewFiles[i]
			info.Output = append(info.Output, e.Meta.TableInfo())
		}
		if len(ve.NewFiles) == 0 {
			info.Err = errEmptyTable
		}

		// The flush succeeded or it produced an empty sstable. In either case we
		// want to bump the minimum unflushed log number to the log number of the
		// oldest unflushed memtable.
		ve.MinUnflushedLogNum = minUnflushedLogNum
		metrics := c.metrics[0]
		for i := 0; i < n; i++ {
			metrics.BytesIn += d.mu.mem.queue[i].logSize
		}

		d.mu.versions.logLock()
		err = d.mu.versions.logAndApply(jobID, ve, c.metrics, false, /* forceRotation */
			func() []compactionInfo { return d.getInProgressCompactionInfoLocked(c) })
		if err != nil {
			info.Err = err
			// TODO(peter): untested.
			d.mu.versions.obsoleteTables = append(d.mu.versions.obsoleteTables, pendingOutputs...)
			d.mu.versions.incrementObsoleteTablesLocked(pendingOutputs)
		}
	}

	bytesFlushed = c.bytesIterated
	d.maybeUpdateDeleteCompactionHints(c)
	d.removeInProgressCompaction(c, err != nil)
	d.mu.versions.incrementCompactions(c.kind, c.extraLevels)
	d.mu.versions.incrementCompactionBytes(-c.bytesWritten)

	var flushed flushableList
	if err == nil {
		flushed = d.mu.mem.queue[:n]
		d.mu.mem.queue = d.mu.mem.queue[n:]
		d.updateReadStateLocked(d.opts.DebugCheck)
		d.updateTableStatsLocked(ve.NewFiles)
	}
	// Signal FlushEnd after installing the new readState. This helps for unit
	// tests that use the callback to trigger a read using an iterator with
	// IterOptions.OnlyReadGuaranteedDurable.
	info.TotalDuration = d.timeNow().Sub(startTime)
	d.opts.EventListener.FlushEnd(info)

	d.deleteObsoleteFiles(jobID, false /* waitForOngoing */)

	// Mark all the memtables we flushed as flushed. Note that we do this last so
	// that a synchronous call to DB.Flush() will not return until the deletion
	// of obsolete files from this job have completed. This makes testing easier
	// and provides similar behavior to manual compactions where the compaction
	// is not marked as completed until the deletion of obsolete files job has
	// completed.
	for i := range flushed {
		// The order of these operations matters here for ease of testing. Removing
		// the reader reference first allows tests to be guaranteed that the
		// memtable reservation has been released by the time a synchronous flush
		// returns.
		flushed[i].readerUnref()
		close(flushed[i].flushed)
	}
	return bytesFlushed, err
}

// maybeScheduleCompactionAsync should be used when
// we want to possibly schedule a compaction, but don't
// want to eat the cost of running maybeScheduleCompaction.
// This method should be launched in a separate goroutine.
// d.mu must not be held when this is called.
func (d *DB) maybeScheduleCompactionAsync() {
	defer d.compactionSchedulers.Done()

	d.mu.Lock()
	d.maybeScheduleCompaction()
	d.mu.Unlock()
}

// maybeScheduleCompaction schedules a compaction if necessary.
//
// d.mu must be held when calling this.
func (d *DB) maybeScheduleCompaction() {
	d.maybeScheduleCompactionPicker(pickAuto)
}

func pickAuto(picker compactionPicker, env compactionEnv) *pickedCompaction {
	return picker.pickAuto(env)
}

func pickElisionOnly(picker compactionPicker, env compactionEnv) *pickedCompaction {
	return picker.pickElisionOnlyCompaction(env)
}

// maybeScheduleCompactionPicker schedules a compaction if necessary,
// calling `pickFunc` to pick automatic compactions.
//
// d.mu must be held when calling this.
func (d *DB) maybeScheduleCompactionPicker(
	pickFunc func(compactionPicker, compactionEnv) *pickedCompaction,
) {
	if d.closed.Load() != nil || d.opts.ReadOnly {
		return
	}
	maxConcurrentCompactions := d.opts.MaxConcurrentCompactions()
	if d.mu.compact.compactingCount >= maxConcurrentCompactions {
		if len(d.mu.compact.manual) > 0 {
			// Inability to run head blocks later manual compactions.
			d.mu.compact.manual[0].retries++
		}
		return
	}

	// Compaction picking needs a coherent view of a Version. In particular, we
	// need to exlude concurrent ingestions from making a decision on which level
	// to ingest into that conflicts with our compaction
	// decision. versionSet.logLock provides the necessary mutual exclusion.
	d.mu.versions.logLock()
	defer d.mu.versions.logUnlock()

	// Check for the closed flag again, in case the DB was closed while we were
	// waiting for logLock().
	if d.closed.Load() != nil {
		return
	}

	env := compactionEnv{
		earliestSnapshotSeqNum:  d.mu.snapshots.earliest(),
		earliestUnflushedSeqNum: d.getEarliestUnflushedSeqNumLocked(),
	}

	// Check for delete-only compactions first, because they're expected to be
	// cheap and reduce future compaction work.
	if len(d.mu.compact.deletionHints) > 0 &&
		d.mu.compact.compactingCount < maxConcurrentCompactions &&
		!d.opts.DisableAutomaticCompactions {
		v := d.mu.versions.currentVersion()
		snapshots := d.mu.snapshots.toSlice()
		inputs, unresolvedHints := checkDeleteCompactionHints(d.cmp, v, d.mu.compact.deletionHints, snapshots)
		d.mu.compact.deletionHints = unresolvedHints

		if len(inputs) > 0 {
			c := newDeleteOnlyCompaction(d.opts, v, inputs)
			d.mu.compact.compactingCount++
			d.addInProgressCompaction(c)
			go d.compact(c, nil)
		}
	}

	for len(d.mu.compact.manual) > 0 && d.mu.compact.compactingCount < maxConcurrentCompactions {
		manual := d.mu.compact.manual[0]
		env.inProgressCompactions = d.getInProgressCompactionInfoLocked(nil)
		pc, retryLater := d.mu.versions.picker.pickManual(env, manual)
		if pc != nil {
			c := newCompaction(pc, d.opts)
			d.mu.compact.manual = d.mu.compact.manual[1:]
			d.mu.compact.compactingCount++
			d.addInProgressCompaction(c)
			go d.compact(c, manual.done)
		} else if !retryLater {
			// Noop
			d.mu.compact.manual = d.mu.compact.manual[1:]
			manual.done <- nil
		} else {
			// Inability to run head blocks later manual compactions.
			manual.retries++
			break
		}
	}

	for !d.opts.DisableAutomaticCompactions && d.mu.compact.compactingCount < maxConcurrentCompactions {
		env.inProgressCompactions = d.getInProgressCompactionInfoLocked(nil)
		env.readCompactionEnv = readCompactionEnv{
			readCompactions:          &d.mu.compact.readCompactions,
			flushing:                 d.mu.compact.flushing || d.passedFlushThreshold(),
			rescheduleReadCompaction: &d.mu.compact.rescheduleReadCompaction,
		}
		pc := pickFunc(d.mu.versions.picker, env)
		if pc == nil {
			break
		}
		c := newCompaction(pc, d.opts)
		d.mu.compact.compactingCount++
		d.addInProgressCompaction(c)
		go d.compact(c, nil)
	}
}

// deleteCompactionHintType indicates whether the deleteCompactionHint was
// generated from a span containing a range del (point key only), a range key
// delete (range key only), or both a point and range key.
type deleteCompactionHintType uint8

const (
	// NOTE: While these are primarily used as enumeration types, they are also
	// used for some bitwise operations. Care should be taken when updating.
	deleteCompactionHintTypeUnknown deleteCompactionHintType = iota
	deleteCompactionHintTypePointKeyOnly
	deleteCompactionHintTypeRangeKeyOnly
	deleteCompactionHintTypePointAndRangeKey
)

// String implements fmt.Stringer.
func (h deleteCompactionHintType) String() string {
	switch h {
	case deleteCompactionHintTypeUnknown:
		return "unknown"
	case deleteCompactionHintTypePointKeyOnly:
		return "point-key-only"
	case deleteCompactionHintTypeRangeKeyOnly:
		return "range-key-only"
	case deleteCompactionHintTypePointAndRangeKey:
		return "point-and-range-key"
	default:
		panic(fmt.Sprintf("unknown hint type: %d", h))
	}
}

// compactionHintFromKeys returns a deleteCompactionHintType given a slice of
// keyspan.Keys.
func compactionHintFromKeys(keys []keyspan.Key) deleteCompactionHintType {
	var hintType deleteCompactionHintType
	for _, k := range keys {
		switch k.Kind() {
		case base.InternalKeyKindRangeDelete:
			hintType |= deleteCompactionHintTypePointKeyOnly
		case base.InternalKeyKindRangeKeyDelete:
			hintType |= deleteCompactionHintTypeRangeKeyOnly
		default:
			panic(fmt.Sprintf("unsupported key kind: %s", k.Kind()))
		}
	}
	return hintType
}

// A deleteCompactionHint records a user key and sequence number span that has been
// deleted by a range tombstone. A hint is recorded if at least one sstable
// falls completely within both the user key and sequence number spans.
// Once the tombstones and the observed completely-contained sstables fall
// into the same snapshot stripe, a delete-only compaction may delete any
// sstables within the range.
type deleteCompactionHint struct {
	// The type of key span that generated this hint (point key, range key, or
	// both).
	hintType deleteCompactionHintType
	// start and end are user keys specifying a key range [start, end) of
	// deleted keys.
	start []byte
	end   []byte
	// The level of the file containing the range tombstone(s) when the hint
	// was created. Only lower levels need to be searched for files that may
	// be deleted.
	tombstoneLevel int
	// The file containing the range tombstone(s) that created the hint.
	tombstoneFile *fileMetadata
	// The smallest and largest sequence numbers of the abutting tombstones
	// merged to form this hint. All of a tables' keys must be less than the
	// tombstone smallest sequence number to be deleted. All of a tables'
	// sequence numbers must fall into the same snapshot stripe as the
	// tombstone largest sequence number to be deleted.
	tombstoneLargestSeqNum  uint64
	tombstoneSmallestSeqNum uint64
	// The smallest sequence number of a sstable that was found to be covered
	// by this hint. The hint cannot be resolved until this sequence number is
	// in the same snapshot stripe as the largest tombstone sequence number.
	// This is set when a hint is created, so the LSM may look different and
	// notably no longer contain the sstable that contained the key at this
	// sequence number.
	fileSmallestSeqNum uint64
}

func (h deleteCompactionHint) String() string {
	return fmt.Sprintf(
		"L%d.%s %s-%s seqnums(tombstone=%d-%d, file-smallest=%d, type=%s)",
		h.tombstoneLevel, h.tombstoneFile.FileNum, h.start, h.end,
		h.tombstoneSmallestSeqNum, h.tombstoneLargestSeqNum, h.fileSmallestSeqNum,
		h.hintType,
	)
}

func (h *deleteCompactionHint) canDelete(cmp Compare, m *fileMetadata, snapshots []uint64) bool {
	// The file can only be deleted if all of its keys are older than the
	// earliest tombstone aggregated into the hint.
	if m.LargestSeqNum >= h.tombstoneSmallestSeqNum || m.SmallestSeqNum < h.fileSmallestSeqNum {
		return false
	}

	// The file's oldest key must  be in the same snapshot stripe as the
	// newest tombstone. NB: We already checked the hint's sequence numbers,
	// but this file's oldest sequence number might be lower than the hint's
	// smallest sequence number despite the file falling within the key range
	// if this file was constructed after the hint by a compaction.
	ti, _ := snapshotIndex(h.tombstoneLargestSeqNum, snapshots)
	fi, _ := snapshotIndex(m.SmallestSeqNum, snapshots)
	if ti != fi {
		return false
	}

	switch h.hintType {
	case deleteCompactionHintTypePointKeyOnly:
		// A hint generated by a range del span cannot delete tables that contain
		// range keys.
		if m.HasRangeKeys {
			return false
		}
	case deleteCompactionHintTypeRangeKeyOnly:
		// A hint generated by a range key del span cannot delete tables that
		// contain point keys.
		if m.HasPointKeys {
			return false
		}
	case deleteCompactionHintTypePointAndRangeKey:
		// A hint from a span that contains both range dels *and* range keys can
		// only be deleted if both bounds fall within the hint. The next check takes
		// care of this.
	default:
		panic(fmt.Sprintf("pebble: unknown delete compaction hint type: %d", h.hintType))
	}

	// The file's keys must be completely contained within the hint range.
	return cmp(h.start, m.Smallest.UserKey) <= 0 && cmp(m.Largest.UserKey, h.end) < 0
}

func (d *DB) maybeUpdateDeleteCompactionHints(c *compaction) {
	// Compactions that zero sequence numbers can interfere with compaction
	// deletion hints. Deletion hints apply to tables containing keys older
	// than a threshold. If a key more recent than the threshold is zeroed in
	// a compaction, a delete-only compaction may mistake it as meeting the
	// threshold and drop a table containing live data.
	//
	// To avoid this scenario, compactions that zero sequence numbers remove
	// any conflicting deletion hints. A deletion hint is conflicting if both
	// of the following conditions apply:
	// * its key space overlaps with the compaction
	// * at least one of its inputs contains a key as recent as one of the
	//   hint's tombstones.
	//
	if !c.allowedZeroSeqNum {
		return
	}

	updatedHints := d.mu.compact.deletionHints[:0]
	for _, h := range d.mu.compact.deletionHints {
		// If the compaction's key space is disjoint from the hint's key
		// space, the zeroing of sequence numbers won't affect the hint. Keep
		// the hint.
		keysDisjoint := d.cmp(h.end, c.smallest.UserKey) < 0 || d.cmp(h.start, c.largest.UserKey) > 0
		if keysDisjoint {
			updatedHints = append(updatedHints, h)
			continue
		}

		// All of the compaction's inputs must be older than the hint's
		// tombstones.
		inputsOlder := true
		for _, in := range c.inputs {
			iter := in.files.Iter()
			for f := iter.First(); f != nil; f = iter.Next() {
				inputsOlder = inputsOlder && f.LargestSeqNum < h.tombstoneSmallestSeqNum
			}
		}
		if inputsOlder {
			updatedHints = append(updatedHints, h)
			continue
		}

		// Drop h, because the compaction c may have zeroed sequence numbers
		// of keys more recent than some of h's tombstones.
	}
	d.mu.compact.deletionHints = updatedHints
}

func checkDeleteCompactionHints(
	cmp Compare, v *version, hints []deleteCompactionHint, snapshots []uint64,
) ([]compactionLevel, []deleteCompactionHint) {
	var files map[*fileMetadata]bool
	var byLevel [numLevels][]*fileMetadata

	unresolvedHints := hints[:0]
	for _, h := range hints {
		// Check each compaction hint to see if it's resolvable. Resolvable
		// hints are removed and trigger a delete-only compaction if any files
		// in the current LSM still meet their criteria. Unresolvable hints
		// are saved and don't trigger a delete-only compaction.
		//
		// When a compaction hint is created, the sequence numbers of the
		// range tombstones and the covered file with the oldest key are
		// recorded. The largest tombstone sequence number and the smallest
		// file sequence number must be in the same snapshot stripe for the
		// hint to be resolved. The below graphic models a compaction hint
		// covering the keyspace [b, r). The hint completely contains two
		// files, 000002 and 000003. The file 000003 contains the lowest
		// covered sequence number at #90. The tombstone b.RANGEDEL.230:h has
		// the highest tombstone sequence number incorporated into the hint.
		// The hint may be resolved only once the snapshots at #100, #180 and
		// #210 are all closed. File 000001 is not included within the hint
		// because it extends beyond the range tombstones in user key space.
		//
		// 250
		//
		//       |-b...230:h-|
		// _____________________________________________________ snapshot #210
		// 200               |--h.RANGEDEL.200:r--|
		//
		// _____________________________________________________ snapshot #180
		//
		// 150                     +--------+
		//           +---------+   | 000003 |
		//           | 000002  |   |        |
		//           +_________+   |        |
		// 100_____________________|________|___________________ snapshot #100
		//                         +--------+
		// _____________________________________________________ snapshot #70
		//                             +---------------+
		//  50                         | 000001        |
		//                             |               |
		//                             +---------------+
		// ______________________________________________________________
		//     a b c d e f g h i j k l m n o p q r s t u v w x y z

		ti, _ := snapshotIndex(h.tombstoneLargestSeqNum, snapshots)
		fi, _ := snapshotIndex(h.fileSmallestSeqNum, snapshots)
		if ti != fi {
			// Cannot resolve yet.
			unresolvedHints = append(unresolvedHints, h)
			continue
		}

		// The hint h will be resolved and dropped, regardless of whether
		// there are any tables that can be deleted.
		for l := h.tombstoneLevel + 1; l < numLevels; l++ {
			overlaps := v.Overlaps(l, cmp, h.start, h.end, true /* exclusiveEnd */)
			iter := overlaps.Iter()
			for m := iter.First(); m != nil; m = iter.Next() {
				if m.IsCompacting() || !h.canDelete(cmp, m, snapshots) || files[m] {
					continue
				}
				if files == nil {
					// Construct files lazily, assuming most calls will not
					// produce delete-only compactions.
					files = make(map[*fileMetadata]bool)
				}
				files[m] = true
				byLevel[l] = append(byLevel[l], m)
			}
		}
	}

	var compactLevels []compactionLevel
	for l, files := range byLevel {
		if len(files) == 0 {
			continue
		}
		compactLevels = append(compactLevels, compactionLevel{
			level: l,
			files: manifest.NewLevelSliceKeySorted(cmp, files),
		})
	}
	return compactLevels, unresolvedHints
}

// compact runs one compaction and maybe schedules another call to compact.
func (d *DB) compact(c *compaction, errChannel chan error) {
	pprof.Do(context.Background(), compactLabels, func(context.Context) {
		d.mu.Lock()
		defer d.mu.Unlock()
		if err := d.compact1(c, errChannel); err != nil {
			// TODO(peter): count consecutive compaction errors and backoff.
			d.opts.EventListener.BackgroundError(err)
		}
		d.mu.compact.compactingCount--
		// The previous compaction may have produced too many files in a
		// level, so reschedule another compaction if needed.
		d.maybeScheduleCompaction()
		d.mu.compact.cond.Broadcast()
	})
}

// compact1 runs one compaction.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) compact1(c *compaction, errChannel chan error) (err error) {
	if errChannel != nil {
		defer func() {
			errChannel <- err
		}()
	}

	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	info := c.makeInfo(jobID)
	d.opts.EventListener.CompactionBegin(info)
	startTime := d.timeNow()

	ve, pendingOutputs, err := d.runCompaction(jobID, c)

	info.Duration = d.timeNow().Sub(startTime)
	if err == nil {
		d.mu.versions.logLock()
		err = d.mu.versions.logAndApply(jobID, ve, c.metrics, false /* forceRotation */, func() []compactionInfo {
			return d.getInProgressCompactionInfoLocked(c)
		})
		if err != nil {
			// TODO(peter): untested.
			d.mu.versions.obsoleteTables = append(d.mu.versions.obsoleteTables, pendingOutputs...)
			d.mu.versions.incrementObsoleteTablesLocked(pendingOutputs)
		}
	}

	info.Done = true
	info.Err = err
	if err == nil {
		for i := range ve.NewFiles {
			e := &ve.NewFiles[i]
			info.Output.Tables = append(info.Output.Tables, e.Meta.TableInfo())
		}
	}

	d.maybeUpdateDeleteCompactionHints(c)
	d.removeInProgressCompaction(c, err != nil)
	d.mu.versions.incrementCompactions(c.kind, c.extraLevels)
	d.mu.versions.incrementCompactionBytes(-c.bytesWritten)

	info.TotalDuration = d.timeNow().Sub(startTime)
	d.opts.EventListener.CompactionEnd(info)

	// Update the read state before deleting obsolete files because the
	// read-state update will cause the previous version to be unref'd and if
	// there are no references obsolete tables will be added to the obsolete
	// table list.
	if err == nil {
		d.updateReadStateLocked(d.opts.DebugCheck)
		d.updateTableStatsLocked(ve.NewFiles)
	}
	d.deleteObsoleteFiles(jobID, true /* waitForOngoing */)

	return err
}

// runCompactions runs a compaction that produces new on-disk tables from
// memtables or old on-disk tables.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) runCompaction(
	jobID int, c *compaction,
) (ve *versionEdit, pendingOutputs []*fileMetadata, retErr error) {
	// As a sanity check, confirm that the smallest / largest keys for new and
	// deleted files in the new versionEdit pass a validation function before
	// returning the edit.
	defer func() {
		if ve != nil {
			err := validateVersionEdit(ve, d.opts.Experimental.KeyValidationFunc, d.opts.Comparer.FormatKey)
			if err != nil {
				d.opts.Logger.Fatalf("pebble: version edit validation failed: %s", err)
			}
		}
	}()

	// Check for a delete-only compaction. This can occur when wide range
	// tombstones completely contain sstables.
	if c.kind == compactionKindDeleteOnly {
		c.metrics = make(map[int]*LevelMetrics, len(c.inputs))
		ve := &versionEdit{
			DeletedFiles: map[deletedFileEntry]*fileMetadata{},
		}
		for _, cl := range c.inputs {
			levelMetrics := &LevelMetrics{}
			iter := cl.files.Iter()
			for f := iter.First(); f != nil; f = iter.Next() {
				levelMetrics.NumFiles--
				levelMetrics.Size -= int64(f.Size)
				ve.DeletedFiles[deletedFileEntry{
					Level:   cl.level,
					FileNum: f.FileNum,
				}] = f
			}
			c.metrics[cl.level] = levelMetrics
		}
		return ve, nil, nil
	}

	// Check for a trivial move of one table from one level to the next. We avoid
	// such a move if there is lots of overlapping grandparent data. Otherwise,
	// the move could create a parent file that will require a very expensive
	// merge later on.
	if c.kind == compactionKindMove {
		iter := c.startLevel.files.Iter()
		meta := iter.First()
		c.metrics = map[int]*LevelMetrics{
			c.startLevel.level: {
				NumFiles: -1,
				Size:     -int64(meta.Size),
			},
			c.outputLevel.level: {
				NumFiles:    1,
				Size:        int64(meta.Size),
				BytesMoved:  meta.Size,
				TablesMoved: 1,
			},
		}
		ve := &versionEdit{
			DeletedFiles: map[deletedFileEntry]*fileMetadata{
				{Level: c.startLevel.level, FileNum: meta.FileNum}: meta,
			},
			NewFiles: []newFileEntry{
				{Level: c.outputLevel.level, Meta: meta},
			},
		}
		return ve, nil, nil
	}

	defer func() {
		if retErr != nil {
			pendingOutputs = nil
		}
	}()

	snapshots := d.mu.snapshots.toSlice()
	formatVers := d.mu.formatVers.vers
	// The table is written at the maximum allowable format implied by the current
	// format major version of the DB.
	tableFormat := formatVers.MaxTableFormat()

	// Release the d.mu lock while doing I/O.
	// Note the unusual order: Unlock and then Lock.
	d.mu.Unlock()
	defer d.mu.Lock()

	iiter, err := c.newInputIter(d.newIters, d.tableNewRangeKeyIter, snapshots)
	if err != nil {
		return nil, pendingOutputs, err
	}
	c.allowedZeroSeqNum = c.allowZeroSeqNum()
	iter := newCompactionIter(c.cmp, c.equal, c.formatKey, d.merge, iiter, snapshots,
		&c.rangeDelFrag, &c.rangeKeyFrag, c.allowedZeroSeqNum, c.elideTombstone,
		c.elideRangeTombstone, d.FormatMajorVersion())

	var (
		filenames []string
		tw        *sstable.Writer
	)
	defer func() {
		if iter != nil {
			retErr = firstError(retErr, iter.Close())
		}
		if tw != nil {
			retErr = firstError(retErr, tw.Close())
		}
		if retErr != nil {
			for _, filename := range filenames {
				d.opts.FS.Remove(filename)
			}
		}
		for _, closer := range c.closers {
			retErr = firstError(retErr, closer.Close())
		}
	}()

	ve = &versionEdit{
		DeletedFiles: map[deletedFileEntry]*fileMetadata{},
	}

	outputMetrics := &LevelMetrics{
		BytesIn:   c.startLevel.files.SizeSum(),
		BytesRead: c.outputLevel.files.SizeSum(),
	}
	if len(c.extraLevels) > 0 {
		outputMetrics.BytesIn += c.extraLevels[0].files.SizeSum()
	}
	outputMetrics.BytesRead += outputMetrics.BytesIn

	c.metrics = map[int]*LevelMetrics{
		c.outputLevel.level: outputMetrics,
	}
	if len(c.flushing) == 0 && c.metrics[c.startLevel.level] == nil {
		c.metrics[c.startLevel.level] = &LevelMetrics{}
	}
	if len(c.extraLevels) > 0 {
		c.metrics[c.extraLevels[0].level] = &LevelMetrics{}
	}

	writerOpts := d.opts.MakeWriterOptions(c.outputLevel.level, tableFormat)
	if formatVers < FormatBlockPropertyCollector {
		// Cannot yet write block properties.
		writerOpts.BlockPropertyCollectors = nil
	}

	// prevPointKey is a sstable.WriterOption that provides access to
	// the last point key written to a writer's sstable. When a new
	// output begins in newOutput, prevPointKey is updated to point to
	// the new output's sstable.Writer. This allows the compaction loop
	// to access the last written point key without requiring the
	// compaction loop to make a copy of each key ahead of time. Users
	// must be careful, because the byte slice returned by UnsafeKey
	// points directly into the Writer's block buffer.
	var prevPointKey sstable.PreviousPointKeyOpt
	var additionalCPUProcs int
	defer func() {
		if additionalCPUProcs > 0 {
			d.opts.Experimental.CPUWorkPermissionGranter.ReturnProcs(additionalCPUProcs)
		}
	}()

	newOutput := func() error {
		fileMeta := &fileMetadata{}
		d.mu.Lock()
		fileNum := d.mu.versions.getNextFileNum()
		fileMeta.FileNum = fileNum
		pendingOutputs = append(pendingOutputs, fileMeta)
		d.mu.Unlock()

		filename := base.MakeFilepath(d.opts.FS, d.dirname, fileTypeTable, fileNum)
		file, err := d.opts.FS.Create(filename)
		if err != nil {
			return err
		}
		reason := "flushing"
		if c.flushing == nil {
			reason = "compacting"
		}
		d.opts.EventListener.TableCreated(TableCreateInfo{
			JobID:   jobID,
			Reason:  reason,
			Path:    filename,
			FileNum: fileNum,
		})
		file = vfs.NewSyncingFile(file, vfs.SyncingFileOptions{
			NoSyncOnClose: d.opts.NoSyncOnClose,
			BytesPerSync:  d.opts.BytesPerSync,
		})
		file = &compactionFile{
			File:     file,
			versions: d.mu.versions,
			written:  &c.bytesWritten,
		}
		filenames = append(filenames, filename)
		cacheOpts := private.SSTableCacheOpts(d.cacheID, fileNum).(sstable.WriterOption)
		internalTableOpt := private.SSTableInternalTableOpt.(sstable.WriterOption)
		if d.opts.Experimental.CPUWorkPermissionGranter != nil {
			additionalCPUProcs = d.opts.Experimental.CPUWorkPermissionGranter.TryGetProcs(1)
		}
		writerOpts.Parallelism =
			d.opts.Experimental.MaxWriterConcurrency > 0 &&
				(additionalCPUProcs > 0 || d.opts.Experimental.ForceWriterParallelism)
		tw = sstable.NewWriter(file, writerOpts, cacheOpts, internalTableOpt, &prevPointKey)

		fileMeta.CreationTime = time.Now().Unix()
		ve.NewFiles = append(ve.NewFiles, newFileEntry{
			Level: c.outputLevel.level,
			Meta:  fileMeta,
		})
		return nil
	}

	// splitL0Outputs is true during flushes and intra-L0 compactions with flush
	// splits enabled.
	splitL0Outputs := c.outputLevel.level == 0 && d.opts.FlushSplitBytes > 0

	// finishOutput is called with the a user key up to which all tombstones
	// should be flushed. Typically, this is the first key of the next
	// sstable or an empty key if this output is the final sstable.
	finishOutput := func(splitKey []byte) error {
		// If we haven't output any point records to the sstable (tw == nil) then the
		// sstable will only contain range tombstones and/or range keys. The smallest
		// key in the sstable will be the start key of the first range tombstone or
		// range key added. We need to ensure that this start key is distinct from
		// the splitKey passed to finishOutput (if set), otherwise we would generate
		// an sstable where the largest key is smaller than the smallest key due to
		// how the largest key boundary is set below. NB: It is permissible for the
		// range tombstone / range key start key to be the empty string.
		//
		// TODO: It is unfortunate that we have to do this check here rather than
		// when we decide to finish the sstable in the runCompaction loop. A better
		// structure currently eludes us.
		if tw == nil {
			startKey := c.rangeDelFrag.Start()
			if len(iter.tombstones) > 0 {
				startKey = iter.tombstones[0].Start
			}
			if startKey == nil {
				startKey = c.rangeKeyFrag.Start()
				if len(iter.rangeKeys) > 0 {
					startKey = iter.rangeKeys[0].Start
				}
			}
			if splitKey != nil && d.cmp(startKey, splitKey) == 0 {
				return nil
			}
		}

		// NB: clone the key because the data can be held on to by the call to
		// compactionIter.Tombstones via keyspan.Fragmenter.FlushTo, and by the
		// WriterMetadata.LargestRangeDel.UserKey.
		splitKey = append([]byte(nil), splitKey...)
		for _, v := range iter.Tombstones(splitKey) {
			if tw == nil {
				if err := newOutput(); err != nil {
					return err
				}
			}
			// The tombstone being added could be completely outside the
			// eventual bounds of the sstable. Consider this example (bounds
			// in square brackets next to table filename):
			//
			// ./000240.sst   [tmgc#391,MERGE-tmgc#391,MERGE]
			// tmgc#391,MERGE [786e627a]
			// tmgc-udkatvs#331,RANGEDEL
			//
			// ./000241.sst   [tmgc#384,MERGE-tmgc#384,MERGE]
			// tmgc#384,MERGE [666c7070]
			// tmgc-tvsalezade#383,RANGEDEL
			// tmgc-tvsalezade#331,RANGEDEL
			//
			// ./000242.sst   [tmgc#383,RANGEDEL-tvsalezade#72057594037927935,RANGEDEL]
			// tmgc-tvsalezade#383,RANGEDEL
			// tmgc#375,SET [72646c78766965616c72776865676e79]
			// tmgc-tvsalezade#356,RANGEDEL
			//
			// Note that both of the top two SSTables have range tombstones
			// that start after the file's end keys. Since the file bound
			// computation happens well after all range tombstones have been
			// added to the writer, eliding out-of-file range tombstones based
			// on sequence number at this stage is difficult, and necessitates
			// read-time logic to ignore range tombstones outside file bounds.
			if err := rangedel.Encode(&v, tw.Add); err != nil {
				return err
			}
		}
		for _, v := range iter.RangeKeys(splitKey) {
			// Same logic as for range tombstones, except added using tw.AddRangeKey.
			if tw == nil {
				if err := newOutput(); err != nil {
					return err
				}
			}
			if err := rangekey.Encode(&v, tw.AddRangeKey); err != nil {
				return err
			}
		}

		if tw == nil {
			return nil
		}

		if err := tw.Close(); err != nil {
			tw = nil
			return err
		}
		if additionalCPUProcs > 0 {
			d.opts.Experimental.CPUWorkPermissionGranter.ReturnProcs(additionalCPUProcs)
			additionalCPUProcs = 0
		}
		writerMeta, err := tw.Metadata()
		if err != nil {
			tw = nil
			return err
		}
		tw = nil
		meta := ve.NewFiles[len(ve.NewFiles)-1].Meta
		meta.Size = writerMeta.Size
		meta.SmallestSeqNum = writerMeta.SmallestSeqNum
		meta.LargestSeqNum = writerMeta.LargestSeqNum
		// If the file didn't contain any range deletions, we can fill its
		// table stats now, avoiding unnecessarily loading the table later.
		maybeSetStatsFromProperties(meta, &writerMeta.Properties)

		if c.flushing == nil {
			outputMetrics.TablesCompacted++
			outputMetrics.BytesCompacted += meta.Size
		} else {
			outputMetrics.TablesFlushed++
			outputMetrics.BytesFlushed += meta.Size
		}
		outputMetrics.Size += int64(meta.Size)
		outputMetrics.NumFiles++

		if n := len(ve.NewFiles); n > 1 {
			// This is not the first output file. Ensure the sstable boundaries
			// are nonoverlapping.
			prevMeta := ve.NewFiles[n-2].Meta
			if writerMeta.SmallestRangeDel.UserKey != nil {
				c := d.cmp(writerMeta.SmallestRangeDel.UserKey, prevMeta.Largest.UserKey)
				if c < 0 {
					return errors.Errorf(
						"pebble: smallest range tombstone start key is less than previous sstable largest key: %s < %s",
						writerMeta.SmallestRangeDel.Pretty(d.opts.Comparer.FormatKey),
						prevMeta.Largest.Pretty(d.opts.Comparer.FormatKey))
				} else if c == 0 && !prevMeta.Largest.IsExclusiveSentinel() {
					// The user key portion of the range boundary start key is
					// equal to the previous table's largest key user key, and
					// the previous table's largest key is not exclusive. This
					// violates the invariant that tables are key-space
					// partitioned.
					return errors.Errorf(
						"pebble: invariant violation: previous sstable largest key %s, current sstable smallest rangedel: %s",
						prevMeta.Largest.Pretty(d.opts.Comparer.FormatKey),
						writerMeta.SmallestRangeDel.Pretty(d.opts.Comparer.FormatKey),
					)
				}
			}
		}

		// Verify that all range deletions outputted to the sstable are
		// truncated to split key.
		if splitKey != nil && writerMeta.LargestRangeDel.UserKey != nil &&
			d.cmp(writerMeta.LargestRangeDel.UserKey, splitKey) > 0 {
			return errors.Errorf(
				"pebble: invariant violation: rangedel largest key %q extends beyond split key %q",
				writerMeta.LargestRangeDel.Pretty(d.opts.Comparer.FormatKey),
				d.opts.Comparer.FormatKey(splitKey),
			)
		}

		if writerMeta.HasPointKeys {
			meta.ExtendPointKeyBounds(d.cmp, writerMeta.SmallestPoint, writerMeta.LargestPoint)
		}
		if writerMeta.HasRangeDelKeys {
			meta.ExtendPointKeyBounds(d.cmp, writerMeta.SmallestRangeDel, writerMeta.LargestRangeDel)
		}
		if writerMeta.HasRangeKeys {
			meta.ExtendRangeKeyBounds(d.cmp, writerMeta.SmallestRangeKey, writerMeta.LargestRangeKey)
		}

		// Verify that the sstable bounds fall within the compaction input
		// bounds. This is a sanity check that we don't have a logic error
		// elsewhere that causes the sstable bounds to accidentally expand past the
		// compaction input bounds as doing so could lead to various badness such
		// as keys being deleted by a range tombstone incorrectly.
		if c.smallest.UserKey != nil {
			switch v := d.cmp(meta.Smallest.UserKey, c.smallest.UserKey); {
			case v >= 0:
				// Nothing to do.
			case v < 0:
				return errors.Errorf("pebble: compaction output grew beyond bounds of input: %s < %s",
					meta.Smallest.Pretty(d.opts.Comparer.FormatKey),
					c.smallest.Pretty(d.opts.Comparer.FormatKey))
			}
		}
		if c.largest.UserKey != nil {
			switch v := d.cmp(meta.Largest.UserKey, c.largest.UserKey); {
			case v <= 0:
				// Nothing to do.
			case v > 0:
				return errors.Errorf("pebble: compaction output grew beyond bounds of input: %s > %s",
					meta.Largest.Pretty(d.opts.Comparer.FormatKey),
					c.largest.Pretty(d.opts.Comparer.FormatKey))
			}
		}
		// Verify that we never split different revisions of the same user key
		// across two different sstables.
		if err := c.errorOnUserKeyOverlap(ve); err != nil {
			return err
		}
		if err := meta.Validate(d.cmp, d.opts.Comparer.FormatKey); err != nil {
			return err
		}
		return nil
	}

	// compactionOutputSplitters contain all logic to determine whether the
	// compaction loop should stop writing to one output sstable and switch to
	// a new one. Some splitters can wrap other splitters, and
	// the splitterGroup can be composed of multiple splitters. In this case,
	// we start off with splitters for file sizes, grandparent limits, and (for
	// L0 splits) L0 limits, before wrapping them in an splitterGroup.
	outputSplitters := []compactionOutputSplitter{
		// We do not split the same user key across different sstables within
		// one flush or compaction. The fileSizeSplitter may request a split in
		// the middle of a user key, so the userKeyChangeSplitter ensures we are
		// at a user key change boundary when doing a split.
		&userKeyChangeSplitter{
			cmp:      c.cmp,
			splitter: &fileSizeSplitter{maxFileSize: c.maxOutputFileSize},
			unsafePrevUserKey: func() []byte {
				// Return the largest point key written to tw or the start of
				// the current range deletion in the fragmenter, whichever is
				// greater.
				prevPoint := prevPointKey.UnsafeKey()
				if c.cmp(prevPoint.UserKey, c.rangeDelFrag.Start()) > 0 {
					return prevPoint.UserKey
				}
				return c.rangeDelFrag.Start()
			},
		},
		&limitFuncSplitter{c: c, limitFunc: c.findGrandparentLimit},
	}
	if splitL0Outputs {
		outputSplitters = append(outputSplitters, &limitFuncSplitter{c: c, limitFunc: c.findL0Limit})
	}
	splitter := &splitterGroup{cmp: c.cmp, splitters: outputSplitters}

	// Each outer loop iteration produces one output file. An iteration that
	// produces a file containing point keys (and optionally range tombstones)
	// guarantees that the input iterator advanced. An iteration that produces
	// a file containing only range tombstones guarantees the limit passed to
	// `finishOutput()` advanced to a strictly greater user key corresponding
	// to a grandparent file largest key, or nil. Taken together, these
	// progress guarantees ensure that eventually the input iterator will be
	// exhausted and the range tombstone fragments will all be flushed.
	for key, val := iter.First(); key != nil || !c.rangeDelFrag.Empty() || !c.rangeKeyFrag.Empty(); {
		splitterSuggestion := splitter.onNewOutput(key)

		// Each inner loop iteration processes one key from the input iterator.
		for ; key != nil; key, val = iter.Next() {
			if split := splitter.shouldSplitBefore(key, tw); split == splitNow {
				break
			}

			switch key.Kind() {
			case InternalKeyKindRangeDelete:
				// Range tombstones are handled specially. They are fragmented,
				// and they're not written until later during `finishOutput()`.
				// We add them to the `Fragmenter` now to make them visible to
				// `compactionIter` so covered keys in the same snapshot stripe
				// can be elided.

				// The interleaved range deletion might only be one of many with
				// these bounds. Some fragmenting is performed ahead of time by
				// keyspan.MergingIter.
				if s := c.rangeDelIter.Span(); !s.Empty() {
					// The memory management here is subtle. Range deletions
					// blocks do NOT use prefix compression, which ensures that
					// range deletion spans' memory is available as long we keep
					// the iterator open. However, the keyspan.MergingIter that
					// merges spans across levels only guarantees the lifetime
					// of the [start, end) bounds until the next positioning
					// method is called.
					//
					// Additionally, the Span.Keys slice is owned by the the
					// range deletion iterator stack, and it may be overwritten
					// when we advance.
					//
					// Clone the Keys slice and the start and end keys.
					//
					// TODO(jackson): Avoid the clone by removing c.rangeDelFrag
					// and performing explicit truncation of the pending
					// rangedel span as necessary.
					clone := keyspan.Span{
						Start: iter.cloneKey(s.Start),
						End:   iter.cloneKey(s.End),
						Keys:  make([]keyspan.Key, len(s.Keys)),
					}
					copy(clone.Keys, s.Keys)
					c.rangeDelFrag.Add(clone)
				}
				continue
			case InternalKeyKindRangeKeySet, InternalKeyKindRangeKeyUnset, InternalKeyKindRangeKeyDelete:
				// Range keys are handled in the same way as range tombstones, except
				// with a dedicated fragmenter.
				if s := c.rangeKeyInterleaving.Span(); !s.Empty() {
					clone := keyspan.Span{
						Start: iter.cloneKey(s.Start),
						End:   iter.cloneKey(s.End),
						Keys:  make([]keyspan.Key, len(s.Keys)),
					}
					// Since the keys' Suffix and Value fields are not deep cloned, the
					// underlying blockIter must be kept open for the lifetime of the
					// compaction.
					copy(clone.Keys, s.Keys)
					c.rangeKeyFrag.Add(clone)
				}
				continue
			}
			if tw == nil {
				if err := newOutput(); err != nil {
					return nil, pendingOutputs, err
				}
			}
			if err := tw.Add(*key, val); err != nil {
				return nil, pendingOutputs, err
			}
		}

		// A splitter requested a split, and we're ready to finish the output.
		// We need to choose the key at which to split any pending range
		// tombstones. There are two options:
		// 1. splitterSuggestion — The key suggested by the splitter. This key
		//    is guaranteed to be greater than the last key written to the
		//    current output.
		// 2. key.UserKey — the first key of the next sstable output. This user
		//     key is also guaranteed to be greater than the last user key
		//     written to the current output (see userKeyChangeSplitter).
		//
		// Use whichever is smaller. Using the smaller of the two limits
		// overlap with grandparents. Consider the case where the
		// grandparent limit is calculated to be 'b', key is 'x', and
		// there exist many sstables between 'b' and 'x'. If the range
		// deletion fragmenter has a pending tombstone [a,x), splitting
		// at 'x' would cause the output table to overlap many
		// grandparents well beyond the calculated grandparent limit
		// 'b'. Splitting at the smaller `splitterSuggestion` avoids
		// this unbounded overlap with grandparent tables.
		splitKey := splitterSuggestion
		if key != nil && (splitKey == nil || c.cmp(splitKey, key.UserKey) > 0) {
			splitKey = key.UserKey
		}
		if err := finishOutput(splitKey); err != nil {
			return nil, pendingOutputs, err
		}
	}

	for _, cl := range c.inputs {
		iter := cl.files.Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			c.metrics[cl.level].NumFiles--
			c.metrics[cl.level].Size -= int64(f.Size)
			ve.DeletedFiles[deletedFileEntry{
				Level:   cl.level,
				FileNum: f.FileNum,
			}] = f
		}
	}

	if err := d.dataDir.Sync(); err != nil {
		return nil, pendingOutputs, err
	}

	// Refresh the disk available statistic whenever a compaction/flush
	// completes, before re-acquiring the mutex.
	_ = d.calculateDiskAvailableBytes()

	return ve, pendingOutputs, nil
}

// validateVersionEdit validates that start and end keys across new and deleted
// files in a versionEdit pass the given validation function.
func validateVersionEdit(
	ve *versionEdit, validateFn func([]byte) error, format base.FormatKey,
) error {
	validateMetaFn := func(f *manifest.FileMetadata) error {
		for _, key := range []InternalKey{f.Smallest, f.Largest} {
			if err := validateFn(key.UserKey); err != nil {
				return errors.Wrapf(err, "key=%q; file=%s", format(key.UserKey), f)
			}
		}
		return nil
	}

	// Validate both new and deleted files.
	for _, f := range ve.NewFiles {
		if err := validateMetaFn(f.Meta); err != nil {
			return err
		}
	}
	for _, m := range ve.DeletedFiles {
		if err := validateMetaFn(m); err != nil {
			return err
		}
	}

	return nil
}

// scanObsoleteFiles scans the filesystem for files that are no longer needed
// and adds those to the internal lists of obsolete files. Note that the files
// are not actually deleted by this method. A subsequent call to
// deleteObsoleteFiles must be performed. Must be not be called concurrently
// with compactions and flushes. db.mu must be held when calling this function.
func (d *DB) scanObsoleteFiles(list []string) {
	// Disable automatic compactions temporarily to avoid concurrent compactions /
	// flushes from interfering. The original value is restored on completion.
	disabledPrev := d.opts.DisableAutomaticCompactions
	defer func() {
		d.opts.DisableAutomaticCompactions = disabledPrev
	}()
	d.opts.DisableAutomaticCompactions = true

	// Wait for any ongoing compaction to complete before continuing.
	if d.mu.compact.compactingCount > 0 || d.mu.compact.flushing {
		d.mu.compact.cond.Wait()
	}

	liveFileNums := make(map[FileNum]struct{})
	d.mu.versions.addLiveFileNums(liveFileNums)
	minUnflushedLogNum := d.mu.versions.minUnflushedLogNum
	manifestFileNum := d.mu.versions.manifestFileNum

	var obsoleteLogs []fileInfo
	var obsoleteTables []*fileMetadata
	var obsoleteManifests []fileInfo
	var obsoleteOptions []fileInfo

	for _, filename := range list {
		fileType, fileNum, ok := base.ParseFilename(d.opts.FS, filename)
		if !ok {
			continue
		}
		switch fileType {
		case fileTypeLog:
			if fileNum >= minUnflushedLogNum {
				continue
			}
			fi := fileInfo{fileNum: fileNum}
			if stat, err := d.opts.FS.Stat(filename); err == nil {
				fi.fileSize = uint64(stat.Size())
			}
			obsoleteLogs = append(obsoleteLogs, fi)
		case fileTypeManifest:
			if fileNum >= manifestFileNum {
				continue
			}
			fi := fileInfo{fileNum: fileNum}
			if stat, err := d.opts.FS.Stat(filename); err == nil {
				fi.fileSize = uint64(stat.Size())
			}
			obsoleteManifests = append(obsoleteManifests, fi)
		case fileTypeOptions:
			if fileNum >= d.optionsFileNum {
				continue
			}
			fi := fileInfo{fileNum: fileNum}
			if stat, err := d.opts.FS.Stat(filename); err == nil {
				fi.fileSize = uint64(stat.Size())
			}
			obsoleteOptions = append(obsoleteOptions, fi)
		case fileTypeTable:
			if _, ok := liveFileNums[fileNum]; ok {
				continue
			}
			fileMeta := &fileMetadata{
				FileNum: fileNum,
			}
			if stat, err := d.opts.FS.Stat(filename); err == nil {
				fileMeta.Size = uint64(stat.Size())
			}
			obsoleteTables = append(obsoleteTables, fileMeta)
		default:
			// Don't delete files we don't know about.
			continue
		}
	}

	d.mu.log.queue = merge(d.mu.log.queue, obsoleteLogs)
	d.mu.versions.metrics.WAL.Files += int64(len(obsoleteLogs))
	d.mu.versions.obsoleteTables = mergeFileMetas(d.mu.versions.obsoleteTables, obsoleteTables)
	d.mu.versions.incrementObsoleteTablesLocked(obsoleteTables)
	d.mu.versions.obsoleteManifests = merge(d.mu.versions.obsoleteManifests, obsoleteManifests)
	d.mu.versions.obsoleteOptions = merge(d.mu.versions.obsoleteOptions, obsoleteOptions)
}

// disableFileDeletions disables file deletions and then waits for any
// in-progress deletion to finish. The caller is required to call
// enableFileDeletions in order to enable file deletions again. It is ok for
// multiple callers to disable file deletions simultaneously, though they must
// all invoke enableFileDeletions in order for file deletions to be re-enabled
// (there is an internal reference count on file deletion disablement).
//
// d.mu must be held when calling this method.
func (d *DB) disableFileDeletions() {
	d.mu.cleaner.disabled++
	for d.mu.cleaner.cleaning {
		d.mu.cleaner.cond.Wait()
	}
	d.mu.cleaner.cond.Broadcast()
}

// enableFileDeletions enables previously disabled file deletions. Note that if
// file deletions have been re-enabled, the current goroutine will be used to
// perform the queued up deletions.
//
// d.mu must be held when calling this method.
func (d *DB) enableFileDeletions() {
	if d.mu.cleaner.disabled <= 0 || d.mu.cleaner.cleaning {
		panic("pebble: file deletion disablement invariant violated")
	}
	d.mu.cleaner.disabled--
	if d.mu.cleaner.disabled > 0 {
		return
	}
	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	d.deleteObsoleteFiles(jobID, true /* waitForOngoing */)
}

// d.mu must be held when calling this.
func (d *DB) acquireCleaningTurn(waitForOngoing bool) bool {
	// Only allow a single delete obsolete files job to run at a time.
	for d.mu.cleaner.cleaning && d.mu.cleaner.disabled == 0 && waitForOngoing {
		d.mu.cleaner.cond.Wait()
	}
	if d.mu.cleaner.cleaning {
		return false
	}
	if d.mu.cleaner.disabled > 0 {
		// File deletions are currently disabled. When they are re-enabled a new
		// job will be created to catch up on file deletions.
		return false
	}
	d.mu.cleaner.cleaning = true
	return true
}

// d.mu must be held when calling this.
func (d *DB) releaseCleaningTurn() {
	d.mu.cleaner.cleaning = false
	d.mu.cleaner.cond.Broadcast()
}

// deleteObsoleteFiles deletes those files that are no longer needed. If
// waitForOngoing is true, it waits for any ongoing cleaning turns to complete,
// and if false, it returns rightaway if a cleaning turn is ongoing.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) deleteObsoleteFiles(jobID int, waitForOngoing bool) {
	if !d.acquireCleaningTurn(waitForOngoing) {
		return
	}
	d.doDeleteObsoleteFiles(jobID)
	d.releaseCleaningTurn()
}

// obsoleteFile holds information about a file that needs to be deleted soon.
type obsoleteFile struct {
	dir      string
	fileNum  base.FileNum
	fileType fileType
	fileSize uint64
}

type fileInfo struct {
	fileNum  FileNum
	fileSize uint64
}

// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) doDeleteObsoleteFiles(jobID int) {
	var obsoleteTables []fileInfo

	defer func() {
		for _, tbl := range obsoleteTables {
			delete(d.mu.versions.zombieTables, tbl.fileNum)
		}
	}()

	var obsoleteLogs []fileInfo
	for i := range d.mu.log.queue {
		// NB: d.mu.versions.minUnflushedLogNum is the log number of the earliest
		// log that has not had its contents flushed to an sstable. We can recycle
		// the prefix of d.mu.log.queue with log numbers less than
		// minUnflushedLogNum.
		if d.mu.log.queue[i].fileNum >= d.mu.versions.minUnflushedLogNum {
			obsoleteLogs = d.mu.log.queue[:i]
			d.mu.log.queue = d.mu.log.queue[i:]
			d.mu.versions.metrics.WAL.Files -= int64(len(obsoleteLogs))
			break
		}
	}

	for _, table := range d.mu.versions.obsoleteTables {
		obsoleteTables = append(obsoleteTables, fileInfo{
			fileNum:  table.FileNum,
			fileSize: table.Size,
		})
	}
	d.mu.versions.obsoleteTables = nil

	// Sort the manifests cause we want to delete some contiguous prefix
	// of the older manifests.
	sort.Slice(d.mu.versions.obsoleteManifests, func(i, j int) bool {
		return d.mu.versions.obsoleteManifests[i].fileNum <
			d.mu.versions.obsoleteManifests[j].fileNum
	})

	var obsoleteManifests []fileInfo
	manifestsToDelete := len(d.mu.versions.obsoleteManifests) - d.opts.NumPrevManifest
	if manifestsToDelete > 0 {
		obsoleteManifests = d.mu.versions.obsoleteManifests[:manifestsToDelete]
		d.mu.versions.obsoleteManifests = d.mu.versions.obsoleteManifests[manifestsToDelete:]
		if len(d.mu.versions.obsoleteManifests) == 0 {
			d.mu.versions.obsoleteManifests = nil
		}
	}

	obsoleteOptions := d.mu.versions.obsoleteOptions
	d.mu.versions.obsoleteOptions = nil

	// Release d.mu while doing I/O
	// Note the unusual order: Unlock and then Lock.
	d.mu.Unlock()
	defer d.mu.Lock()

	files := [4]struct {
		fileType fileType
		obsolete []fileInfo
	}{
		{fileTypeLog, obsoleteLogs},
		{fileTypeTable, obsoleteTables},
		{fileTypeManifest, obsoleteManifests},
		{fileTypeOptions, obsoleteOptions},
	}
	_, noRecycle := d.opts.Cleaner.(base.NeedsFileContents)
	filesToDelete := make([]obsoleteFile, 0, len(files))
	for _, f := range files {
		// We sort to make the order of deletions deterministic, which is nice for
		// tests.
		sort.Slice(f.obsolete, func(i, j int) bool {
			return f.obsolete[i].fileNum < f.obsolete[j].fileNum
		})
		for _, fi := range f.obsolete {
			dir := d.dirname
			switch f.fileType {
			case fileTypeLog:
				if !noRecycle && d.logRecycler.add(fi) {
					continue
				}
				dir = d.walDirname
			case fileTypeTable:
				d.tableCache.evict(fi.fileNum)
			}

			filesToDelete = append(filesToDelete, obsoleteFile{
				dir:      dir,
				fileNum:  fi.fileNum,
				fileType: f.fileType,
				fileSize: fi.fileSize,
			})
		}
	}
	if len(filesToDelete) > 0 {
		d.deleters.Add(1)
		// Delete asynchronously if that could get held up in the pacer.
		if d.opts.Experimental.MinDeletionRate > 0 {
			go d.paceAndDeleteObsoleteFiles(jobID, filesToDelete)
		} else {
			d.paceAndDeleteObsoleteFiles(jobID, filesToDelete)
		}
	}
}

// Paces and eventually deletes the list of obsolete files passed in. db.mu
// must NOT be held when calling this method.
func (d *DB) paceAndDeleteObsoleteFiles(jobID int, files []obsoleteFile) {
	defer d.deleters.Done()
	pacer := (pacer)(nilPacer)
	if d.opts.Experimental.MinDeletionRate > 0 {
		pacer = newDeletionPacer(d.deletionLimiter, d.getDeletionPacerInfo)
	}

	for _, of := range files {
		path := base.MakeFilepath(d.opts.FS, of.dir, of.fileType, of.fileNum)
		if of.fileType == fileTypeTable {
			_ = pacer.maybeThrottle(of.fileSize)
			d.mu.Lock()
			d.mu.versions.metrics.Table.ObsoleteCount--
			d.mu.versions.metrics.Table.ObsoleteSize -= of.fileSize
			d.mu.Unlock()
		}
		d.deleteObsoleteFile(of.fileType, jobID, path, of.fileNum)
	}
}

func (d *DB) maybeScheduleObsoleteTableDeletion() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.mu.versions.obsoleteTables) == 0 {
		return
	}
	if !d.acquireCleaningTurn(false) {
		return
	}

	go func() {
		pprof.Do(context.Background(), gcLabels, func(context.Context) {
			d.mu.Lock()
			defer d.mu.Unlock()

			jobID := d.mu.nextJobID
			d.mu.nextJobID++
			d.doDeleteObsoleteFiles(jobID)
			d.releaseCleaningTurn()
		})
	}()
}

// deleteObsoleteFile deletes file that is no longer needed.
func (d *DB) deleteObsoleteFile(fileType fileType, jobID int, path string, fileNum FileNum) {
	// TODO(peter): need to handle this error, probably by re-adding the
	// file that couldn't be deleted to one of the obsolete slices map.
	err := d.opts.Cleaner.Clean(d.opts.FS, fileType, path)
	if oserror.IsNotExist(err) {
		return
	}

	switch fileType {
	case fileTypeLog:
		d.opts.EventListener.WALDeleted(WALDeleteInfo{
			JobID:   jobID,
			Path:    path,
			FileNum: fileNum,
			Err:     err,
		})
	case fileTypeManifest:
		d.opts.EventListener.ManifestDeleted(ManifestDeleteInfo{
			JobID:   jobID,
			Path:    path,
			FileNum: fileNum,
			Err:     err,
		})
	case fileTypeTable:
		d.opts.EventListener.TableDeleted(TableDeleteInfo{
			JobID:   jobID,
			Path:    path,
			FileNum: fileNum,
			Err:     err,
		})
	}
}

func merge(a, b []fileInfo) []fileInfo {
	if len(b) == 0 {
		return a
	}

	a = append(a, b...)
	sort.Slice(a, func(i, j int) bool {
		return a[i].fileNum < a[j].fileNum
	})

	n := 0
	for i := 0; i < len(a); i++ {
		if n == 0 || a[i].fileNum != a[n-1].fileNum {
			a[n] = a[i]
			n++
		}
	}
	return a[:n]
}

func mergeFileMetas(a, b []*fileMetadata) []*fileMetadata {
	if len(b) == 0 {
		return a
	}

	a = append(a, b...)
	sort.Slice(a, func(i, j int) bool {
		return a[i].FileNum < a[j].FileNum
	})

	n := 0
	for i := 0; i < len(a); i++ {
		if n == 0 || a[i].FileNum != a[n-1].FileNum {
			a[n] = a[i]
			n++
		}
	}
	return a[:n]
}
