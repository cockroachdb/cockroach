// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"math"
	"sort"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// The minimum count for an intra-L0 compaction. This matches the RocksDB
// heuristic.
const minIntraL0Count = 4

const levelMultiplier = 10

type compactionEnv struct {
	earliestUnflushedSeqNum uint64
	earliestSnapshotSeqNum  uint64
	inProgressCompactions   []compactionInfo
	readCompactionEnv       readCompactionEnv
}

type compactionPicker interface {
	getScores([]compactionInfo) [numLevels]float64
	getBaseLevel() int
	getEstimatedMaxWAmp() float64
	estimatedCompactionDebt(l0ExtraSize uint64) uint64
	pickAuto(env compactionEnv) (pc *pickedCompaction)
	pickManual(env compactionEnv, manual *manualCompaction) (c *pickedCompaction, retryLater bool)
	pickElisionOnlyCompaction(env compactionEnv) (pc *pickedCompaction)
	pickRewriteCompaction(env compactionEnv) (pc *pickedCompaction)
	pickReadTriggeredCompaction(env compactionEnv) (pc *pickedCompaction)
	forceBaseLevel1()
}

// readCompactionEnv is used to hold data required to perform read compactions
type readCompactionEnv struct {
	rescheduleReadCompaction *bool
	readCompactions          *readCompactionQueue
	flushing                 bool
}

// Information about in-progress compactions provided to the compaction picker. These are used to
// constrain the new compactions that will be picked.
type compactionInfo struct {
	inputs      []compactionLevel
	outputLevel int
	smallest    InternalKey
	largest     InternalKey
}

func (info compactionInfo) String() string {
	var buf bytes.Buffer
	var largest int
	for i, in := range info.inputs {
		if i > 0 {
			fmt.Fprintf(&buf, " -> ")
		}
		fmt.Fprintf(&buf, "L%d", in.level)
		in.files.Each(func(m *fileMetadata) {
			fmt.Fprintf(&buf, " %s", m.FileNum)
		})
		if largest < in.level {
			largest = in.level
		}
	}
	if largest != info.outputLevel || len(info.inputs) == 1 {
		fmt.Fprintf(&buf, " -> L%d", info.outputLevel)
	}
	return buf.String()
}

type sortCompactionLevelsDecreasingScore []candidateLevelInfo

func (s sortCompactionLevelsDecreasingScore) Len() int {
	return len(s)
}
func (s sortCompactionLevelsDecreasingScore) Less(i, j int) bool {
	if s[i].score != s[j].score {
		return s[i].score > s[j].score
	}
	return s[i].level < s[j].level
}
func (s sortCompactionLevelsDecreasingScore) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// sublevelInfo is used to tag a LevelSlice for an L0 sublevel with the
// sublevel.
type sublevelInfo struct {
	manifest.LevelSlice
	sublevel manifest.Level
}

// generateSublevelInfo will generate the level slices for each of the sublevels
// from the level slice for all of L0.
func generateSublevelInfo(cmp base.Compare, levelFiles manifest.LevelSlice) []sublevelInfo {
	sublevelMap := make(map[uint64][]*fileMetadata)
	it := levelFiles.Iter()
	for f := it.First(); f != nil; f = it.Next() {
		sublevelMap[uint64(f.SubLevel)] = append(sublevelMap[uint64(f.SubLevel)], f)
	}

	var sublevels []int
	for level := range sublevelMap {
		sublevels = append(sublevels, int(level))
	}
	sort.Ints(sublevels)

	var levelSlices []sublevelInfo
	for _, sublevel := range sublevels {
		metas := sublevelMap[uint64(sublevel)]
		levelSlices = append(
			levelSlices,
			sublevelInfo{
				manifest.NewLevelSliceKeySorted(cmp, metas),
				manifest.L0Sublevel(sublevel),
			},
		)
	}
	return levelSlices
}

// pickedCompaction contains information about a compaction that has already
// been chosen, and is being constructed. Compaction construction info lives in
// this struct, and is copied over into the compaction struct when that's
// created.
type pickedCompaction struct {
	cmp Compare

	// score of the chosen compaction. Taken from candidateLevelInfo.
	score float64

	// kind indicates the kind of compaction.
	kind compactionKind

	// startLevel is the level that is being compacted. Inputs from startLevel
	// and outputLevel will be merged to produce a set of outputLevel files.
	startLevel *compactionLevel

	// outputLevel is the level that files are being produced in. outputLevel is
	// equal to startLevel+1 except when:
	//    - if startLevel is 0, the output level equals compactionPicker.baseLevel().
	//    - in multilevel compaction, the output level is the lowest level involved in
	//      the compaction
	outputLevel *compactionLevel

	// extraLevels contain additional levels in between the input and output
	// levels that get compacted in multi level compactions
	extraLevels []*compactionLevel

	// adjustedOutputLevel is the output level used for the purpose of
	// determining the target output file size, overlap bytes, and expanded
	// bytes, taking into account the base level.
	adjustedOutputLevel int

	inputs []compactionLevel

	// L0-specific compaction info. Set to a non-nil value for all compactions
	// where startLevel == 0 that were generated by L0Sublevels.
	lcf *manifest.L0CompactionFiles

	// L0SublevelInfo is used for compactions out of L0. It is nil for all
	// other compactions.
	l0SublevelInfo []sublevelInfo

	// maxOutputFileSize is the maximum size of an individual table created
	// during compaction.
	maxOutputFileSize uint64
	// maxOverlapBytes is the maximum number of bytes of overlap allowed for a
	// single output table with the tables in the grandparent level.
	maxOverlapBytes uint64
	// maxReadCompactionBytes is the maximum bytes a read compaction is allowed to
	// overlap in its output level with. If the overlap is greater than
	// maxReadCompaction bytes, then we don't proceed with the compaction.
	maxReadCompactionBytes uint64

	// The boundaries of the input data.
	smallest InternalKey
	largest  InternalKey

	version *version
}

func defaultOutputLevel(startLevel, baseLevel int) int {
	outputLevel := startLevel + 1
	if startLevel == 0 {
		outputLevel = baseLevel
	}
	if outputLevel >= numLevels-1 {
		outputLevel = numLevels - 1
	}
	return outputLevel
}

func newPickedCompaction(
	opts *Options, cur *version, startLevel, outputLevel, baseLevel int,
) *pickedCompaction {
	if startLevel > 0 && startLevel < baseLevel {
		panic(fmt.Sprintf("invalid compaction: start level %d should not be empty (base level %d)",
			startLevel, baseLevel))
	}

	adjustedOutputLevel := outputLevel
	if adjustedOutputLevel > 0 {
		// Output level is in the range [baseLevel,numLevels]. For the purpose of
		// determining the target output file size, overlap bytes, and expanded
		// bytes, we want to adjust the range to [1,numLevels].
		adjustedOutputLevel = 1 + outputLevel - baseLevel
	}

	pc := &pickedCompaction{
		cmp:                    opts.Comparer.Compare,
		version:                cur,
		inputs:                 []compactionLevel{{level: startLevel}, {level: outputLevel}},
		adjustedOutputLevel:    adjustedOutputLevel,
		maxOutputFileSize:      uint64(opts.Level(adjustedOutputLevel).TargetFileSize),
		maxOverlapBytes:        maxGrandparentOverlapBytes(opts, adjustedOutputLevel),
		maxReadCompactionBytes: maxReadCompactionBytes(opts, adjustedOutputLevel),
	}
	pc.startLevel = &pc.inputs[0]
	pc.outputLevel = &pc.inputs[1]
	return pc
}

func newPickedCompactionFromL0(
	lcf *manifest.L0CompactionFiles, opts *Options, vers *version, baseLevel int, isBase bool,
) *pickedCompaction {
	outputLevel := baseLevel
	if !isBase {
		outputLevel = 0 // Intra L0
	}

	pc := newPickedCompaction(opts, vers, 0, outputLevel, baseLevel)
	pc.lcf = lcf
	pc.outputLevel.level = outputLevel

	// Manually build the compaction as opposed to calling
	// pickAutoHelper. This is because L0Sublevels has already added
	// any overlapping L0 SSTables that need to be added, and
	// because compactions built by L0SSTables do not necessarily
	// pick contiguous sequences of files in pc.version.Levels[0].
	files := make([]*manifest.FileMetadata, 0, len(lcf.Files))
	iter := vers.Levels[0].Iter()
	for f := iter.First(); f != nil; f = iter.Next() {
		if lcf.FilesIncluded[f.L0Index] {
			files = append(files, f)
		}
	}
	pc.startLevel.files = manifest.NewLevelSliceSeqSorted(files)
	return pc
}

// maybeExpandedBounds is a helper function for setupInputs which ensures the
// pickedCompaction's smallest and largest internal keys are updated iff
// the candidate keys expand the key span. This avoids a bug for multi-level
// compactions: during the second call to setupInputs, the picked compaction's
// smallest and largest keys should not decrease the key span.
func (pc *pickedCompaction) maybeExpandBounds(smallest InternalKey, largest InternalKey) {
	emptyKey := InternalKey{}
	if base.InternalCompare(pc.cmp, smallest, emptyKey) == 0 {
		if base.InternalCompare(pc.cmp, largest, emptyKey) != 0 {
			panic("either both candidate keys are empty or neither are empty")
		}
		return
	}
	if base.InternalCompare(pc.cmp, pc.smallest, emptyKey) == 0 {
		if base.InternalCompare(pc.cmp, pc.largest, emptyKey) != 0 {
			panic("either both pc keys are empty or neither are empty")
		}
		pc.smallest = smallest
		pc.largest = largest
		return
	}
	if base.InternalCompare(pc.cmp, pc.smallest, smallest) >= 0 {
		pc.smallest = smallest
	}
	if base.InternalCompare(pc.cmp, pc.largest, largest) <= 0 {
		pc.largest = largest
	}
}

func (pc *pickedCompaction) setupInputs(
	opts *Options, diskAvailBytes uint64, startLevel *compactionLevel,
) bool {
	// maxExpandedBytes is the maximum size of an expanded compaction. If
	// growing a compaction results in a larger size, the original compaction
	// is used instead.
	maxExpandedBytes := expandedCompactionByteSizeLimit(
		opts, pc.adjustedOutputLevel, diskAvailBytes,
	)

	// Expand the initial inputs to a clean cut.
	var isCompacting bool
	startLevel.files, isCompacting = expandToAtomicUnit(pc.cmp, startLevel.files, false /* disableIsCompacting */)
	if isCompacting {
		return false
	}
	pc.maybeExpandBounds(manifest.KeyRange(pc.cmp, startLevel.files.Iter()))

	// Determine the sstables in the output level which overlap with the input
	// sstables, and then expand those tables to a clean cut. No need to do
	// this for intra-L0 compactions; outputLevel.files is left empty for those.
	if startLevel.level != pc.outputLevel.level {
		pc.outputLevel.files = pc.version.Overlaps(pc.outputLevel.level, pc.cmp, pc.smallest.UserKey,
			pc.largest.UserKey, pc.largest.IsExclusiveSentinel())
		pc.outputLevel.files, isCompacting = expandToAtomicUnit(pc.cmp, pc.outputLevel.files,
			false /* disableIsCompacting */)
		if isCompacting {
			return false
		}
		pc.maybeExpandBounds(manifest.KeyRange(pc.cmp,
			startLevel.files.Iter(), pc.outputLevel.files.Iter()))
	}

	// Grow the sstables in startLevel.level as long as it doesn't affect the number
	// of sstables included from pc.outputLevel.level.
	if pc.lcf != nil && startLevel.level == 0 && pc.outputLevel.level != 0 {
		// Call the L0-specific compaction extension method. Similar logic as
		// pc.grow. Additional L0 files are optionally added to the compaction at
		// this step. Note that the bounds passed in are not the bounds of the
		// compaction, but rather the smallest and largest internal keys that
		// the compaction cannot include from L0 without pulling in more Lbase
		// files. Consider this example:
		//
		// L0:        c-d e+f g-h
		// Lbase: a-b     e+f     i-j
		//        a b c d e f g h i j
		//
		// The e-f files have already been chosen in the compaction. As pulling
		// in more LBase files is undesirable, the logic below will pass in
		// smallest = b and largest = i to ExtendL0ForBaseCompactionTo, which
		// will expand the compaction to include c-d and g-h from L0. The
		// bounds passed in are exclusive; the compaction cannot be expanded
		// to include files that "touch" it.
		smallestBaseKey := base.InvalidInternalKey
		largestBaseKey := base.InvalidInternalKey
		if pc.outputLevel.files.Empty() {
			baseIter := pc.version.Levels[pc.outputLevel.level].Iter()
			if sm := baseIter.SeekLT(pc.cmp, pc.smallest.UserKey); sm != nil {
				smallestBaseKey = sm.Largest
			}
			if la := baseIter.SeekGE(pc.cmp, pc.largest.UserKey); la != nil {
				largestBaseKey = la.Smallest
			}
		} else {
			// NB: We use Reslice to access the underlying level's files, but
			// we discard the returned slice. The pc.outputLevel.files slice
			// is not modified.
			_ = pc.outputLevel.files.Reslice(func(start, end *manifest.LevelIterator) {
				if sm := start.Prev(); sm != nil {
					smallestBaseKey = sm.Largest
				}
				if la := end.Next(); la != nil {
					largestBaseKey = la.Smallest
				}
			})
		}

		oldLcf := *pc.lcf
		if pc.version.L0Sublevels.ExtendL0ForBaseCompactionTo(smallestBaseKey, largestBaseKey, pc.lcf) {
			var newStartLevelFiles []*fileMetadata
			iter := pc.version.Levels[0].Iter()
			var sizeSum uint64
			for j, f := 0, iter.First(); f != nil; j, f = j+1, iter.Next() {
				if pc.lcf.FilesIncluded[f.L0Index] {
					newStartLevelFiles = append(newStartLevelFiles, f)
					sizeSum += f.Size
				}
			}
			if sizeSum+pc.outputLevel.files.SizeSum() < maxExpandedBytes {
				startLevel.files = manifest.NewLevelSliceSeqSorted(newStartLevelFiles)
				pc.smallest, pc.largest = manifest.KeyRange(pc.cmp,
					startLevel.files.Iter(), pc.outputLevel.files.Iter())
			} else {
				*pc.lcf = oldLcf
			}
		}
	} else if pc.grow(pc.smallest, pc.largest, maxExpandedBytes, startLevel) {
		pc.maybeExpandBounds(manifest.KeyRange(pc.cmp,
			startLevel.files.Iter(), pc.outputLevel.files.Iter()))
	}

	if pc.startLevel.level == 0 {
		// We don't change the input files for the compaction beyond this point.
		pc.l0SublevelInfo = generateSublevelInfo(pc.cmp, pc.startLevel.files)
	}

	return true
}

// grow grows the number of inputs at c.level without changing the number of
// c.level+1 files in the compaction, and returns whether the inputs grew. sm
// and la are the smallest and largest InternalKeys in all of the inputs.
func (pc *pickedCompaction) grow(
	sm, la InternalKey, maxExpandedBytes uint64, startLevel *compactionLevel,
) bool {
	if pc.outputLevel.files.Empty() {
		return false
	}
	grow0 := pc.version.Overlaps(startLevel.level, pc.cmp, sm.UserKey,
		la.UserKey, la.IsExclusiveSentinel())
	grow0, isCompacting := expandToAtomicUnit(pc.cmp, grow0, false /* disableIsCompacting */)
	if isCompacting {
		return false
	}
	if grow0.Len() <= startLevel.files.Len() {
		return false
	}
	if grow0.SizeSum()+pc.outputLevel.files.SizeSum() >= maxExpandedBytes {
		return false
	}
	// We need to include the outputLevel iter because without it, in a multiLevel scenario,
	// sm1 and la1 could shift the output level keyspace when pc.outputLevel.files is set to grow1.
	sm1, la1 := manifest.KeyRange(pc.cmp, grow0.Iter(), pc.outputLevel.files.Iter())
	grow1 := pc.version.Overlaps(pc.outputLevel.level, pc.cmp, sm1.UserKey,
		la1.UserKey, la1.IsExclusiveSentinel())
	grow1, isCompacting = expandToAtomicUnit(pc.cmp, grow1, false /* disableIsCompacting */)
	if isCompacting {
		return false
	}
	if grow1.Len() != pc.outputLevel.files.Len() {
		return false
	}
	startLevel.files = grow0
	pc.outputLevel.files = grow1
	return true
}

// initMultiLevelCompaction returns true if it initiated a multilevel input
// compaction. This currently never inits a multiLevel compaction.
func (pc *pickedCompaction) initMultiLevelCompaction(
	opts *Options, vers *version, levelMaxBytes [7]int64, diskAvailBytes uint64,
) bool {
	return false
}

// expandToAtomicUnit expands the provided level slice within its level both
// forwards and backwards to its "atomic compaction unit" boundaries, if
// necessary.
//
// While picking compaction inputs, this is required to maintain the invariant
// that the versions of keys at level+1 are older than the versions of keys at
// level. Tables are added to the right of the current slice tables such that
// the rightmost table has a "clean cut". A clean cut is either a change in
// user keys, or when the largest key in the left sstable is a range tombstone
// sentinel key (InternalKeyRangeDeleteSentinel).
//
// In addition to maintaining the seqnum invariant, expandToAtomicUnit is used
// to provide clean boundaries for range tombstone truncation during
// compaction. In order to achieve these clean boundaries, expandToAtomicUnit
// needs to find a "clean cut" on the left edge of the compaction as well.
// This is necessary in order for "atomic compaction units" to always be
// compacted as a unit. Failure to do this leads to a subtle bug with
// truncation of range tombstones to atomic compaction unit boundaries.
// Consider the scenario:
//
//   L3:
//     12:[a#2,15-b#1,1]
//     13:[b#0,15-d#72057594037927935,15]
//
// These sstables contain a range tombstone [a-d)#2 which spans the two
// sstables. The two sstables need to always be kept together. Compacting
// sstable 13 independently of sstable 12 would result in:
//
//   L3:
//     12:[a#2,15-b#1,1]
//   L4:
//     14:[b#0,15-d#72057594037927935,15]
//
// This state is still ok, but when sstable 12 is next compacted, its range
// tombstones will be truncated at "b" (the largest key in its atomic
// compaction unit). In the scenario here, that could result in b#1 becoming
// visible when it should be deleted.
//
// isCompacting is returned true for any atomic units that contain files that
// have in-progress compactions, i.e. FileMetadata.Compacting == true. If
// disableIsCompacting is true, isCompacting always returns false. This helps
// avoid spurious races from being detected when this method is used outside
// of compaction picking code.
//
// TODO(jackson): Compactions and flushes no longer split a user key between two
// sstables. We could perform a migration, re-compacting any sstables with split
// user keys, which would allow us to remove atomic compaction unit expansion
// code.
func expandToAtomicUnit(
	cmp Compare, inputs manifest.LevelSlice, disableIsCompacting bool,
) (slice manifest.LevelSlice, isCompacting bool) {
	// NB: Inputs for L0 can't be expanded and *version.Overlaps guarantees
	// that we get a 'clean cut.' For L0, Overlaps will return a slice without
	// access to the rest of the L0 files, so it's OK to try to reslice.
	if inputs.Empty() {
		// Nothing to expand.
		return inputs, false
	}

	inputs = inputs.Reslice(func(start, end *manifest.LevelIterator) {
		iter := start.Clone()
		iter.Prev()
		for cur, prev := start.Current(), iter.Current(); prev != nil; cur, prev = start.Prev(), iter.Prev() {
			if cur.IsCompacting() {
				isCompacting = true
			}
			if cmp(prev.Largest.UserKey, cur.Smallest.UserKey) < 0 {
				break
			}
			if prev.Largest.IsExclusiveSentinel() {
				// The table prev has a largest key indicating that the user key
				// prev.largest.UserKey doesn't actually exist in the table.
				break
			}
			// prev.Largest.UserKey == cur.Smallest.UserKey, so we need to
			// include prev in the compaction.
		}

		iter = end.Clone()
		iter.Next()
		for cur, next := end.Current(), iter.Current(); next != nil; cur, next = end.Next(), iter.Next() {
			if cur.IsCompacting() {
				isCompacting = true
			}
			if cmp(cur.Largest.UserKey, next.Smallest.UserKey) < 0 {
				break
			}
			if cur.Largest.IsExclusiveSentinel() {
				// The table cur has a largest key indicating that the user key
				// cur.largest.UserKey doesn't actually exist in the table.
				break
			}
			// cur.Largest.UserKey == next.Smallest.UserKey, so we need to
			// include next in the compaction.
		}
	})
	inputIter := inputs.Iter()
	isCompacting = !disableIsCompacting &&
		(isCompacting || inputIter.First().IsCompacting() || inputIter.Last().IsCompacting())
	return inputs, isCompacting
}

func newCompactionPicker(
	v *version,
	opts *Options,
	inProgressCompactions []compactionInfo,
	levelSizes [numLevels]int64,
	diskAvailBytes func() uint64,
) compactionPicker {
	p := &compactionPickerByScore{
		opts:           opts,
		vers:           v,
		levelSizes:     levelSizes,
		diskAvailBytes: diskAvailBytes,
	}
	p.initLevelMaxBytes(inProgressCompactions)
	return p
}

// Information about a candidate compaction level that has been identified by
// the compaction picker.
type candidateLevelInfo struct {
	// The score of the level to be compacted.
	score     float64
	origScore float64
	level     int
	// The level to compact to.
	outputLevel int
	// The file in level that will be compacted. Additional files may be
	// picked by the compaction, and a pickedCompaction created for the
	// compaction.
	file manifest.LevelFile
}

// compensatedSize returns f's file size, inflated according to compaction
// priorities.
func compensatedSize(f *fileMetadata) uint64 {
	sz := f.Size
	// Add in the estimate of disk space that may be reclaimed by compacting
	// the file's tombstones.
	sz += f.Stats.PointDeletionsBytesEstimate
	sz += f.Stats.RangeDeletionsBytesEstimate
	return sz
}

// compensatedSizeAnnotator implements manifest.Annotator, annotating B-Tree
// nodes with the sum of the files' compensated sizes. Its annotation type is
// a *uint64. Compensated sizes may change once a table's stats are loaded
// asynchronously, so its values are marked as cacheable only if a file's
// stats have been loaded.
type compensatedSizeAnnotator struct{}

var _ manifest.Annotator = compensatedSizeAnnotator{}

func (a compensatedSizeAnnotator) Zero(dst interface{}) interface{} {
	if dst == nil {
		return new(uint64)
	}
	v := dst.(*uint64)
	*v = 0
	return v
}

func (a compensatedSizeAnnotator) Accumulate(
	f *fileMetadata, dst interface{},
) (v interface{}, cacheOK bool) {
	vptr := dst.(*uint64)
	*vptr = *vptr + compensatedSize(f)
	return vptr, f.StatsValidLocked()
}

func (a compensatedSizeAnnotator) Merge(src interface{}, dst interface{}) interface{} {
	srcV := src.(*uint64)
	dstV := dst.(*uint64)
	*dstV = *dstV + *srcV
	return dstV
}

// totalCompensatedSize computes the compensated size over a file metadata
// iterator. Note that this function is linear in the files available to the
// iterator. Use the compensatedSizeAnnotator if querying the total
// compensated size of a level.
func totalCompensatedSize(iter manifest.LevelIterator) uint64 {
	var sz uint64
	for f := iter.First(); f != nil; f = iter.Next() {
		sz += compensatedSize(f)
	}
	return sz
}

// compactionPickerByScore holds the state and logic for picking a compaction. A
// compaction picker is associated with a single version. A new compaction
// picker is created and initialized every time a new version is installed.
type compactionPickerByScore struct {
	opts *Options
	vers *version

	// The level to target for L0 compactions. Levels L1 to baseLevel must be
	// empty.
	baseLevel int

	// estimatedMaxWAmp is the estimated maximum write amp per byte that is
	// added to L0.
	estimatedMaxWAmp float64

	// levelMaxBytes holds the dynamically adjusted max bytes setting for each
	// level.
	levelMaxBytes [numLevels]int64

	// levelSizes holds the current size of each level.
	levelSizes [numLevels]int64

	// diskAvailBytes returns a cached statistic on the number of bytes
	// available on disk, as reported by the filesystem. It's used to be more
	// restrictive in expanding compactions if available disk space is
	// limited.
	//
	// The cached value is updated whenever a file is deleted and
	// whenever a compaction or flush completes. Since file removal is
	// the primary means of reclaiming space, there is a rough bound on
	// the statistic's staleness when available bytes is growing.
	// Compactions and flushes are longer, slower operations and provide
	// a much looser bound when available bytes is decreasing.
	diskAvailBytes func() uint64
}

var _ compactionPicker = &compactionPickerByScore{}

func (p *compactionPickerByScore) getScores(inProgress []compactionInfo) [numLevels]float64 {
	var scores [numLevels]float64
	for _, info := range p.calculateScores(inProgress) {
		scores[info.level] = info.score
	}
	return scores
}

func (p *compactionPickerByScore) getBaseLevel() int {
	if p == nil {
		return 1
	}
	return p.baseLevel
}

func (p *compactionPickerByScore) getEstimatedMaxWAmp() float64 {
	return p.estimatedMaxWAmp
}

// estimatedCompactionDebt estimates the number of bytes which need to be
// compacted before the LSM tree becomes stable.
func (p *compactionPickerByScore) estimatedCompactionDebt(l0ExtraSize uint64) uint64 {
	if p == nil {
		return 0
	}

	// We assume that all the bytes in L0 need to be compacted to Lbase. This is
	// unlike the RocksDB logic that figures out whether L0 needs compaction.
	bytesAddedToNextLevel := l0ExtraSize + uint64(p.levelSizes[0])
	nextLevelSize := uint64(p.levelSizes[p.baseLevel])

	var compactionDebt uint64
	if bytesAddedToNextLevel > 0 && nextLevelSize > 0 {
		// We only incur compaction debt if both L0 and Lbase contain data. If L0
		// is empty, no compaction is necessary. If Lbase is empty, a move-based
		// compaction from L0 would occur.
		compactionDebt += bytesAddedToNextLevel + nextLevelSize
	}

	for level := p.baseLevel; level < numLevels-1; level++ {
		levelSize := nextLevelSize + bytesAddedToNextLevel
		nextLevelSize = uint64(p.levelSizes[level+1])
		if levelSize > uint64(p.levelMaxBytes[level]) {
			bytesAddedToNextLevel = levelSize - uint64(p.levelMaxBytes[level])
			if nextLevelSize > 0 {
				// We only incur compaction debt if the next level contains data. If the
				// next level is empty, a move-based compaction would be used.
				levelRatio := float64(nextLevelSize) / float64(levelSize)
				// The current level contributes bytesAddedToNextLevel to compactions.
				// The next level contributes levelRatio * bytesAddedToNextLevel.
				compactionDebt += uint64(float64(bytesAddedToNextLevel) * (levelRatio + 1))
			}
		}
	}

	return compactionDebt
}

func (p *compactionPickerByScore) initLevelMaxBytes(inProgressCompactions []compactionInfo) {
	// The levelMaxBytes calculations here differ from RocksDB in two ways:
	//
	// 1. The use of dbSize vs maxLevelSize. RocksDB uses the size of the maximum
	//    level in L1-L6, rather than determining the size of the bottom level
	//    based on the total amount of data in the dB. The RocksDB calculation is
	//    problematic if L0 contains a significant fraction of data, or if the
	//    level sizes are roughly equal and thus there is a significant fraction
	//    of data outside of the largest level.
	//
	// 2. Not adjusting the size of Lbase based on L0. RocksDB computes
	//    baseBytesMax as the maximum of the configured LBaseMaxBytes and the
	//    size of L0. This is problematic because baseBytesMax is used to compute
	//    the max size of lower levels. A very large baseBytesMax will result in
	//    an overly large value for the size of lower levels which will caused
	//    those levels not to be compacted even when they should be
	//    compacted. This often results in "inverted" LSM shapes where Ln is
	//    larger than Ln+1.

	// Determine the first non-empty level and the total DB size.
	firstNonEmptyLevel := -1
	var dbSize int64
	for level := 1; level < numLevels; level++ {
		if p.levelSizes[level] > 0 {
			if firstNonEmptyLevel == -1 {
				firstNonEmptyLevel = level
			}
			dbSize += p.levelSizes[level]
		}
	}
	for _, c := range inProgressCompactions {
		if c.outputLevel == 0 || c.outputLevel == -1 {
			continue
		}
		if c.inputs[0].level == 0 && (firstNonEmptyLevel == -1 || c.outputLevel < firstNonEmptyLevel) {
			firstNonEmptyLevel = c.outputLevel
		}
	}

	// Initialize the max-bytes setting for each level to "infinity" which will
	// disallow compaction for that level. We'll fill in the actual value below
	// for levels we want to allow compactions from.
	for level := 0; level < numLevels; level++ {
		p.levelMaxBytes[level] = math.MaxInt64
	}

	if dbSize == 0 {
		// No levels for L1 and up contain any data. Target L0 compactions for the
		// last level or to the level to which there is an ongoing L0 compaction.
		p.baseLevel = numLevels - 1
		if firstNonEmptyLevel >= 0 {
			p.baseLevel = firstNonEmptyLevel
		}
		return
	}

	dbSize += p.levelSizes[0]
	bottomLevelSize := dbSize - dbSize/levelMultiplier

	curLevelSize := bottomLevelSize
	for level := numLevels - 2; level >= firstNonEmptyLevel; level-- {
		curLevelSize = int64(float64(curLevelSize) / levelMultiplier)
	}

	// Compute base level (where L0 data is compacted to).
	baseBytesMax := p.opts.LBaseMaxBytes
	p.baseLevel = firstNonEmptyLevel
	for p.baseLevel > 1 && curLevelSize > baseBytesMax {
		p.baseLevel--
		curLevelSize = int64(float64(curLevelSize) / levelMultiplier)
	}

	smoothedLevelMultiplier := 1.0
	if p.baseLevel < numLevels-1 {
		smoothedLevelMultiplier = math.Pow(
			float64(bottomLevelSize)/float64(baseBytesMax),
			1.0/float64(numLevels-p.baseLevel-1))
	}

	p.estimatedMaxWAmp = float64(numLevels-p.baseLevel) * (smoothedLevelMultiplier + 1)

	levelSize := float64(baseBytesMax)
	for level := p.baseLevel; level < numLevels; level++ {
		if level > p.baseLevel && levelSize > 0 {
			levelSize *= smoothedLevelMultiplier
		}
		// Round the result since test cases use small target level sizes, which
		// can be impacted by floating-point imprecision + integer truncation.
		roundedLevelSize := math.Round(levelSize)
		if roundedLevelSize > float64(math.MaxInt64) {
			p.levelMaxBytes[level] = math.MaxInt64
		} else {
			p.levelMaxBytes[level] = int64(roundedLevelSize)
		}
	}
}

func calculateSizeAdjust(inProgressCompactions []compactionInfo) [numLevels]int64 {
	// Compute a size adjustment for each level based on the in-progress
	// compactions. We subtract the compensated size of start level inputs.
	// Since compensated file sizes may be compensated because they reclaim
	// space from the output level's files, we add the real file size to the
	// output level. This is slightly different from RocksDB's behavior, which
	// simply elides compacting files from the level size calculation.
	var sizeAdjust [numLevels]int64
	for i := range inProgressCompactions {
		c := &inProgressCompactions[i]

		for _, input := range c.inputs {
			real := int64(input.files.SizeSum())
			compensated := int64(totalCompensatedSize(input.files.Iter()))

			if input.level != c.outputLevel {
				sizeAdjust[input.level] -= compensated
				if c.outputLevel != -1 {
					sizeAdjust[c.outputLevel] += real
				}
			}
		}
	}
	return sizeAdjust
}

func levelCompensatedSize(lm manifest.LevelMetadata) uint64 {
	return *lm.Annotation(compensatedSizeAnnotator{}).(*uint64)
}

func (p *compactionPickerByScore) calculateScores(
	inProgressCompactions []compactionInfo,
) [numLevels]candidateLevelInfo {
	var scores [numLevels]candidateLevelInfo
	for i := range scores {
		scores[i].level = i
		scores[i].outputLevel = i + 1
	}
	scores[0] = p.calculateL0Score(inProgressCompactions)

	sizeAdjust := calculateSizeAdjust(inProgressCompactions)
	for level := 1; level < numLevels; level++ {
		levelSize := int64(levelCompensatedSize(p.vers.Levels[level])) + sizeAdjust[level]
		scores[level].score = float64(levelSize) / float64(p.levelMaxBytes[level])
		scores[level].origScore = scores[level].score
	}

	// Adjust each level's score by the score of the next level. If the next
	// level has a high score, and is thus a priority for compaction, this
	// reduces the priority for compacting the current level. If the next level
	// has a low score (i.e. it is below its target size), this increases the
	// priority for compacting the current level.
	//
	// The effect of this adjustment is to help prioritize compactions in lower
	// levels. The following shows the new score and original score. In this
	// scenario, L0 has 68 sublevels. L3 (a.k.a. Lbase) is significantly above
	// its target size. The original score prioritizes compactions from those two
	// levels, but doing so ends up causing a future problem: data piles up in
	// the higher levels, starving L5->L6 compactions, and to a lesser degree
	// starving L4->L5 compactions.
	//
	//        adjusted   original
	//           score      score       size   max-size
	//   L0        3.2       68.0      2.2 G          -
	//   L3        3.2       21.1      1.3 G       64 M
	//   L4        3.4        6.7      3.1 G      467 M
	//   L5        3.4        2.0      6.6 G      3.3 G
	//   L6        0.6        0.6       14 G       24 G
	var prevLevel int
	for level := p.baseLevel; level < numLevels; level++ {
		if scores[prevLevel].score >= 1 {
			// Avoid absurdly large scores by placing a floor on the score that we'll
			// adjust a level by. The value of 0.01 was chosen somewhat arbitrarily
			const minScore = 0.01
			if scores[level].score >= minScore {
				scores[prevLevel].score /= scores[level].score
			} else {
				scores[prevLevel].score /= minScore
			}
		}
		prevLevel = level
	}

	sort.Sort(sortCompactionLevelsDecreasingScore(scores[:]))
	return scores
}

func (p *compactionPickerByScore) calculateL0Score(
	inProgressCompactions []compactionInfo,
) candidateLevelInfo {
	var info candidateLevelInfo
	info.outputLevel = p.baseLevel

	// If L0Sublevels are present, use the sublevel count to calculate the
	// score. The base vs intra-L0 compaction determination happens in pickAuto,
	// not here.
	info.score = float64(2*p.vers.L0Sublevels.MaxDepthAfterOngoingCompactions()) /
		float64(p.opts.L0CompactionThreshold)

	// Also calculate a score based on the file count but use it only if it
	// produces a higher score than the sublevel-based one. This heuristic is
	// designed to accommodate cases where L0 is accumulating non-overlapping
	// files in L0. Letting too many non-overlapping files accumulate in few
	// sublevels is undesirable, because:
	// 1) we can produce a massive backlog to compact once files do overlap.
	// 2) constructing L0 sublevels has a runtime that grows superlinearly with
	//    the number of files in L0 and must be done while holding D.mu.
	noncompactingFiles := p.vers.Levels[0].Len()
	for _, c := range inProgressCompactions {
		for _, cl := range c.inputs {
			if cl.level == 0 {
				noncompactingFiles -= cl.files.Len()
			}
		}
	}
	fileScore := float64(noncompactingFiles) / float64(p.opts.L0CompactionFileThreshold)
	if info.score < fileScore {
		info.score = fileScore
	}
	return info
}

func (p *compactionPickerByScore) pickFile(
	level, outputLevel int, earliestSnapshotSeqNum uint64,
) (manifest.LevelFile, bool) {
	// Select the file within the level to compact. We want to minimize write
	// amplification, but also ensure that deletes are propagated to the
	// bottom level in a timely fashion so as to reclaim disk space. A table's
	// smallest sequence number provides a measure of its age. The ratio of
	// overlapping-bytes / table-size gives an indication of write
	// amplification (a smaller ratio is preferrable).
	//
	// The current heuristic is based off the the RocksDB kMinOverlappingRatio
	// heuristic. It chooses the file with the minimum overlapping ratio with
	// the target level, which minimizes write amplification.
	//
	// It uses a "compensated size" for the denominator, which is the file
	// size but artificially inflated by an estimate of the space that may be
	// reclaimed through compaction. Currently, we only compensate for range
	// deletions and only with a rough estimate of the reclaimable bytes. This
	// differs from RocksDB which only compensates for point tombstones and
	// only if they exceed the number of non-deletion entries in table.
	//
	// TODO(peter): For concurrent compactions, we may want to try harder to
	// pick a seed file whose resulting compaction bounds do not overlap with
	// an in-progress compaction.

	cmp := p.opts.Comparer.Compare
	startIter := p.vers.Levels[level].Iter()
	outputIter := p.vers.Levels[outputLevel].Iter()

	var file manifest.LevelFile
	smallestRatio := uint64(math.MaxUint64)

	outputFile := outputIter.First()

	for f := startIter.First(); f != nil; f = startIter.Next() {
		var overlappingBytes uint64

		// Trim any output-level files smaller than f.
		for outputFile != nil && base.InternalCompare(cmp, outputFile.Largest, f.Smallest) < 0 {
			outputFile = outputIter.Next()
		}

		compacting := f.IsCompacting()
		for outputFile != nil && base.InternalCompare(cmp, outputFile.Smallest, f.Largest) < 0 {
			overlappingBytes += outputFile.Size
			compacting = compacting || outputFile.IsCompacting()

			// For files in the bottommost level of the LSM, the
			// Stats.RangeDeletionsBytesEstimate field is set to the estimate
			// of bytes /within/ the file itself that may be dropped by
			// recompacting the file. These bytes from obsolete keys would not
			// need to be rewritten if we compacted `f` into `outputFile`, so
			// they don't contribute to write amplification. Subtracting them
			// out of the overlapping bytes helps prioritize these compactions
			// that are cheaper than their file sizes suggest.
			if outputLevel == numLevels-1 && outputFile.LargestSeqNum < earliestSnapshotSeqNum {
				overlappingBytes -= outputFile.Stats.RangeDeletionsBytesEstimate
			}

			// If the file in the next level extends beyond f's largest key,
			// break out and don't advance outputIter because f's successor
			// might also overlap.
			if base.InternalCompare(cmp, outputFile.Largest, f.Largest) > 0 {
				break
			}
			outputFile = outputIter.Next()
		}

		// If the input level file or one of the overlapping files is
		// compacting, we're not going to be able to compact this file
		// anyways, so skip it.
		if compacting {
			continue
		}

		scaledRatio := overlappingBytes * 1024 / compensatedSize(f)
		if scaledRatio < smallestRatio && !f.IsCompacting() {
			smallestRatio = scaledRatio
			file = startIter.Take()
		}
	}
	return file, file.FileMetadata != nil
}

// pickAuto picks the best compaction, if any.
//
// On each call, pickAuto computes per-level size adjustments based on
// in-progress compactions, and computes a per-level score. The levels are
// iterated over in decreasing score order trying to find a valid compaction
// anchored at that level.
//
// If a score-based compaction cannot be found, pickAuto falls back to looking
// for an elision-only compaction to remove obsolete keys.
func (p *compactionPickerByScore) pickAuto(env compactionEnv) (pc *pickedCompaction) {
	// Compaction concurrency is controlled by L0 read-amp. We allow one
	// additional compaction per L0CompactionConcurrency sublevels, as well as
	// one additional compaction per CompactionDebtConcurrency bytes of
	// compaction debt. Compaction concurrency is tied to L0 sublevels as that
	// signal is independent of the database size. We tack on the compaction
	// debt as a second signal to prevent compaction concurrency from dropping
	// significantly right after a base compaction finishes, and before those
	// bytes have been compacted further down the LSM.
	if n := len(env.inProgressCompactions); n > 0 {
		l0ReadAmp := p.vers.L0Sublevels.MaxDepthAfterOngoingCompactions()
		compactionDebt := int(p.estimatedCompactionDebt(0))
		ccSignal1 := n * p.opts.Experimental.L0CompactionConcurrency
		ccSignal2 := n * p.opts.Experimental.CompactionDebtConcurrency
		if l0ReadAmp < ccSignal1 && compactionDebt < ccSignal2 {
			return nil
		}
	}

	scores := p.calculateScores(env.inProgressCompactions)

	// TODO(peter): Either remove, or change this into an event sent to the
	// EventListener.
	logCompaction := func(pc *pickedCompaction) {
		var buf bytes.Buffer
		for i := 0; i < numLevels; i++ {
			if i != 0 && i < p.baseLevel {
				continue
			}

			var info *candidateLevelInfo
			for j := range scores {
				if scores[j].level == i {
					info = &scores[j]
					break
				}
			}

			marker := " "
			if pc.startLevel.level == info.level {
				marker = "*"
			}
			fmt.Fprintf(&buf, "  %sL%d: %5.1f  %5.1f  %8s  %8s",
				marker, info.level, info.score, info.origScore,
				humanize.Int64(int64(totalCompensatedSize(p.vers.Levels[info.level].Iter()))),
				humanize.Int64(p.levelMaxBytes[info.level]),
			)

			count := 0
			for i := range env.inProgressCompactions {
				c := &env.inProgressCompactions[i]
				if c.inputs[0].level != info.level {
					continue
				}
				count++
				if count == 1 {
					fmt.Fprintf(&buf, "  [")
				} else {
					fmt.Fprintf(&buf, " ")
				}
				fmt.Fprintf(&buf, "L%d->L%d", c.inputs[0].level, c.outputLevel)
			}
			if count > 0 {
				fmt.Fprintf(&buf, "]")
			}
			fmt.Fprintf(&buf, "\n")
		}
		p.opts.Logger.Infof("pickAuto: L%d->L%d\n%s",
			pc.startLevel.level, pc.outputLevel.level, buf.String())
	}

	// Check for a score-based compaction. "scores" has been sorted in order of
	// decreasing score. For each level with a score >= 1, we attempt to find a
	// compaction anchored at at that level.
	for i := range scores {
		info := &scores[i]
		if info.score < 1 {
			break
		}
		if info.level == numLevels-1 {
			continue
		}

		if info.level == 0 {
			pc = pickL0(env, p.opts, p.vers, p.baseLevel, p.diskAvailBytes)
			// Fail-safe to protect against compacting the same sstable
			// concurrently.
			if pc != nil && !inputRangeAlreadyCompacting(env, pc) {
				pc.score = info.score
				// TODO(peter): remove
				if false {
					logCompaction(pc)
				}
				return pc
			}
			continue
		}

		// info.level > 0
		var ok bool
		info.file, ok = p.pickFile(info.level, info.outputLevel, env.earliestSnapshotSeqNum)
		if !ok {
			continue
		}

		pc := pickAutoLPositive(env, p.opts, p.vers, *info, p.baseLevel, p.diskAvailBytes, p.levelMaxBytes)
		// Fail-safe to protect against compacting the same sstable concurrently.
		if pc != nil && !inputRangeAlreadyCompacting(env, pc) {
			pc.score = info.score
			// TODO(peter): remove
			if false {
				logCompaction(pc)
			}
			return pc
		}
	}

	// Check for L6 files with tombstones that may be elided. These files may
	// exist if a snapshot prevented the elision of a tombstone or because of
	// a move compaction. These are low-priority compactions because they
	// don't help us keep up with writes, just reclaim disk space.
	if pc := p.pickElisionOnlyCompaction(env); pc != nil {
		return pc
	}

	if pc := p.pickReadTriggeredCompaction(env); pc != nil {
		return pc
	}

	// NB: This should only be run if a read compaction wasn't
	// scheduled.
	//
	// We won't be scheduling a read compaction right now, and in
	// read heavy workloads, compactions won't be scheduled frequently
	// because flushes aren't frequent. So we need to signal to the
	// iterator to schedule a compaction when it adds compactions to
	// the read compaction queue.
	//
	// We need the nil check here because without it, we have some
	// tests which don't set that variable fail. Since there's a
	// chance that one of those tests wouldn't want extra compactions
	// to be scheduled, I added this check here, instead of
	// setting rescheduleReadCompaction in those tests.
	if env.readCompactionEnv.rescheduleReadCompaction != nil {
		*env.readCompactionEnv.rescheduleReadCompaction = true
	}

	// At the lowest possible compaction-picking priority, look for files marked
	// for compaction. Pebble will mark files for compaction if they have atomic
	// compaction units that span multiple files. While current Pebble code does
	// not construct such sstables, RocksDB and earlier versions of Pebble may
	// have created them. These split user keys form sets of files that must be
	// compacted together for correctness (referred to as "atomic compaction
	// units" within the code). Rewrite them in-place.
	//
	// It's also possible that a file may have been marked for compaction by
	// even earlier versions of Pebble code, since FileMetadata's
	// MarkedForCompaction field is persisted in the manifest. That's okay. We
	// previously would've ignored the designation, whereas now we'll re-compact
	// the file in place.
	if p.vers.Stats.MarkedForCompaction > 0 {
		if pc := p.pickRewriteCompaction(env); pc != nil {
			return pc
		}
	}

	return nil
}

// elisionOnlyAnnotator implements the manifest.Annotator interface,
// annotating B-Tree nodes with the *fileMetadata of a file meeting the
// obsolete keys criteria for an elision-only compaction within the subtree.
// If multiple files meet the criteria, it chooses whichever file has the
// lowest LargestSeqNum. The lowest LargestSeqNum file will be the first
// eligible for an elision-only compaction once snapshots less than or equal
// to its LargestSeqNum are closed.
type elisionOnlyAnnotator struct{}

var _ manifest.Annotator = elisionOnlyAnnotator{}

func (a elisionOnlyAnnotator) Zero(interface{}) interface{} {
	return nil
}

func (a elisionOnlyAnnotator) Accumulate(f *fileMetadata, dst interface{}) (interface{}, bool) {
	if f.IsCompacting() {
		return dst, true
	}
	if !f.StatsValidLocked() {
		return dst, false
	}
	// Bottommost files are large and not worthwhile to compact just
	// to remove a few tombstones. Consider a file ineligible if its
	// own range deletions delete less than 10% of its data and its
	// deletion tombstones make up less than 10% of its entries.
	//
	// TODO(jackson): This does not account for duplicate user keys
	// which may be collapsed. Ideally, we would have 'obsolete keys'
	// statistics that would include tombstones, the keys that are
	// dropped by tombstones and duplicated user keys. See #847.
	//
	// Note that tables that contain exclusively range keys (i.e. no point keys,
	// `NumEntries` and `RangeDeletionsBytesEstimate` are both zero) are excluded
	// from elision-only compactions.
	// TODO(travers): Consider an alternative heuristic for elision of range-keys.
	if f.Stats.RangeDeletionsBytesEstimate*10 < f.Size &&
		f.Stats.NumDeletions*10 <= f.Stats.NumEntries {
		return dst, true
	}
	if dst == nil {
		return f, true
	} else if dstV := dst.(*fileMetadata); dstV.LargestSeqNum > f.LargestSeqNum {
		return f, true
	}
	return dst, true
}

func (a elisionOnlyAnnotator) Merge(v interface{}, accum interface{}) interface{} {
	if v == nil {
		return accum
	}
	// If we haven't accumulated an eligible file yet, or f's LargestSeqNum is
	// less than the accumulated file's, use f.
	if accum == nil {
		return v
	}
	f := v.(*fileMetadata)
	accumV := accum.(*fileMetadata)
	if accumV == nil || accumV.LargestSeqNum > f.LargestSeqNum {
		return f
	}
	return accumV
}

// markedForCompactionAnnotator implements the manifest.Annotator interface,
// annotating B-Tree nodes with the *fileMetadata of a file that is marked for
// compaction within the subtree. If multiple files meet the criteria, it
// chooses whichever file has the lowest LargestSeqNum.
type markedForCompactionAnnotator struct{}

var _ manifest.Annotator = markedForCompactionAnnotator{}

func (a markedForCompactionAnnotator) Zero(interface{}) interface{} {
	return nil
}

func (a markedForCompactionAnnotator) Accumulate(
	f *fileMetadata, dst interface{},
) (interface{}, bool) {
	if !f.MarkedForCompaction {
		// Not marked for compaction; return dst.
		return dst, true
	}
	return markedMergeHelper(f, dst)
}

func (a markedForCompactionAnnotator) Merge(v interface{}, accum interface{}) interface{} {
	if v == nil {
		return accum
	}
	accum, _ = markedMergeHelper(v.(*fileMetadata), accum)
	return accum
}

// REQUIRES: f is non-nil, and f.MarkedForCompaction=true.
func markedMergeHelper(f *fileMetadata, dst interface{}) (interface{}, bool) {
	if dst == nil {
		return f, true
	} else if dstV := dst.(*fileMetadata); dstV.LargestSeqNum > f.LargestSeqNum {
		return f, true
	}
	return dst, true
}

// pickElisionOnlyCompaction looks for compactions of sstables in the
// bottommost level containing obsolete records that may now be dropped.
func (p *compactionPickerByScore) pickElisionOnlyCompaction(
	env compactionEnv,
) (pc *pickedCompaction) {
	v := p.vers.Levels[numLevels-1].Annotation(elisionOnlyAnnotator{})
	if v == nil {
		return nil
	}
	candidate := v.(*fileMetadata)
	if candidate.IsCompacting() || candidate.LargestSeqNum >= env.earliestSnapshotSeqNum {
		return nil
	}
	lf := p.vers.Levels[numLevels-1].Find(p.opts.Comparer.Compare, candidate)
	if lf == nil {
		panic(fmt.Sprintf("file %s not found in level %d as expected", candidate.FileNum, numLevels-1))
	}

	// Construct a picked compaction of the elision candidate's atomic
	// compaction unit.
	pc = newPickedCompaction(p.opts, p.vers, numLevels-1, numLevels-1, p.baseLevel)
	pc.kind = compactionKindElisionOnly
	var isCompacting bool
	pc.startLevel.files, isCompacting = expandToAtomicUnit(p.opts.Comparer.Compare, lf.Slice(), false /* disableIsCompacting */)
	if isCompacting {
		return nil
	}
	pc.smallest, pc.largest = manifest.KeyRange(pc.cmp, pc.startLevel.files.Iter())
	// Fail-safe to protect against compacting the same sstable concurrently.
	if !inputRangeAlreadyCompacting(env, pc) {
		return pc
	}
	return nil
}

// pickRewriteCompaction attempts to construct a compaction that
// rewrites a file marked for compaction. pickRewriteCompaction will
// pull in adjacent files in the file's atomic compaction unit if
// necessary. A rewrite compaction outputs files to the same level as
// the input level.
func (p *compactionPickerByScore) pickRewriteCompaction(env compactionEnv) (pc *pickedCompaction) {
	for l := numLevels - 1; l >= 0; l-- {
		v := p.vers.Levels[l].Annotation(markedForCompactionAnnotator{})
		if v == nil {
			// Try the next level.
			continue
		}
		candidate := v.(*fileMetadata)
		if candidate.IsCompacting() {
			// Try the next level.
			continue
		}
		lf := p.vers.Levels[l].Find(p.opts.Comparer.Compare, candidate)
		if lf == nil {
			panic(fmt.Sprintf("file %s not found in level %d as expected", candidate.FileNum, numLevels-1))
		}

		inputs := lf.Slice()
		// L0 files generated by a flush have never been split such that
		// adjacent files can contain the same user key. So we do not need to
		// rewrite an atomic compaction unit for L0. Note that there is nothing
		// preventing two different flushes from producing files that are
		// non-overlapping from an InternalKey perspective, but span the same
		// user key. However, such files cannot be in the same L0 sublevel,
		// since each sublevel requires non-overlapping user keys (unlike other
		// levels).
		if l > 0 {
			// Find this file's atomic compaction unit. This is only relevant
			// for levels L1+.
			var isCompacting bool
			inputs, isCompacting = expandToAtomicUnit(
				p.opts.Comparer.Compare,
				inputs,
				false, /* disableIsCompacting */
			)
			if isCompacting {
				// Try the next level.
				continue
			}
		}

		pc = newPickedCompaction(p.opts, p.vers, l, l, p.baseLevel)
		pc.outputLevel.level = l
		pc.kind = compactionKindRewrite
		pc.startLevel.files = inputs
		pc.smallest, pc.largest = manifest.KeyRange(pc.cmp, pc.startLevel.files.Iter())

		// Fail-safe to protect against compacting the same sstable concurrently.
		if !inputRangeAlreadyCompacting(env, pc) {
			if pc.startLevel.level == 0 {
				pc.l0SublevelInfo = generateSublevelInfo(pc.cmp, pc.startLevel.files)
			}
			return pc
		}
	}
	return nil
}

// pickAutoLPositive picks an automatic compaction for the candidate
// file in a positive-numbered level. This function must not be used for
// L0.
func pickAutoLPositive(
	env compactionEnv,
	opts *Options,
	vers *version,
	cInfo candidateLevelInfo,
	baseLevel int,
	diskAvailBytes func() uint64,
	levelMaxBytes [7]int64,
) (pc *pickedCompaction) {
	if cInfo.level == 0 {
		panic("pebble: pickAutoLPositive called for L0")
	}

	pc = newPickedCompaction(opts, vers, cInfo.level, defaultOutputLevel(cInfo.level, baseLevel), baseLevel)
	if pc.outputLevel.level != cInfo.outputLevel {
		panic("pebble: compaction picked unexpected output level")
	}
	pc.startLevel.files = cInfo.file.Slice()
	// Files in level 0 may overlap each other, so pick up all overlapping ones.
	if pc.startLevel.level == 0 {
		cmp := opts.Comparer.Compare
		smallest, largest := manifest.KeyRange(cmp, pc.startLevel.files.Iter())
		pc.startLevel.files = vers.Overlaps(0, cmp, smallest.UserKey,
			largest.UserKey, largest.IsExclusiveSentinel())
		if pc.startLevel.files.Empty() {
			panic("pebble: empty compaction")
		}
	}

	if !pc.setupInputs(opts, diskAvailBytes(), pc.startLevel) {
		return nil
	}
	if opts.Experimental.MultiLevelCompaction &&
		pc.initMultiLevelCompaction(opts, vers, levelMaxBytes, diskAvailBytes()) {
		if !pc.setupInputs(opts, diskAvailBytes(), pc.extraLevels[len(pc.extraLevels)-1]) {
			return nil
		}
	}
	return pc
}

// Helper method to pick compactions originating from L0. Uses information about
// sublevels to generate a compaction.
func pickL0(
	env compactionEnv, opts *Options, vers *version, baseLevel int, diskAvailBytes func() uint64,
) (pc *pickedCompaction) {
	// It is important to pass information about Lbase files to L0Sublevels
	// so it can pick a compaction that does not conflict with an Lbase => Lbase+1
	// compaction. Without this, we observed reduced concurrency of L0=>Lbase
	// compactions, and increasing read amplification in L0.
	//
	// TODO(bilal) Remove the minCompactionDepth parameter once fixing it at 1
	// has been shown to not cause a performance regression.
	lcf, err := vers.L0Sublevels.PickBaseCompaction(1, vers.Levels[baseLevel].Slice())
	if err != nil {
		opts.Logger.Infof("error when picking base compaction: %s", err)
		return
	}
	if lcf != nil {
		pc = newPickedCompactionFromL0(lcf, opts, vers, baseLevel, true)
		pc.setupInputs(opts, diskAvailBytes(), pc.startLevel)
		if pc.startLevel.files.Empty() {
			opts.Logger.Fatalf("empty compaction chosen")
		}
		return pc
	}

	// Couldn't choose a base compaction. Try choosing an intra-L0
	// compaction. Note that we pass in L0CompactionThreshold here as opposed to
	// 1, since choosing a single sublevel intra-L0 compaction is
	// counterproductive.
	lcf, err = vers.L0Sublevels.PickIntraL0Compaction(env.earliestUnflushedSeqNum, minIntraL0Count)
	if err != nil {
		opts.Logger.Infof("error when picking intra-L0 compaction: %s", err)
		return
	}
	if lcf != nil {
		pc = newPickedCompactionFromL0(lcf, opts, vers, 0, false)
		if !pc.setupInputs(opts, diskAvailBytes(), pc.startLevel) {
			return nil
		}
		if pc.startLevel.files.Empty() {
			opts.Logger.Fatalf("empty compaction chosen")
		}
		{
			iter := pc.startLevel.files.Iter()
			if iter.First() == nil || iter.Next() == nil {
				// A single-file intra-L0 compaction is unproductive.
				return nil
			}
		}

		pc.smallest, pc.largest = manifest.KeyRange(pc.cmp, pc.startLevel.files.Iter())
	}
	return pc
}

func (p *compactionPickerByScore) pickManual(
	env compactionEnv, manual *manualCompaction,
) (pc *pickedCompaction, retryLater bool) {
	if p == nil {
		return nil, false
	}

	outputLevel := manual.level + 1
	if manual.level == 0 {
		outputLevel = p.baseLevel
	} else if manual.level < p.baseLevel {
		// The start level for a compaction must be >= Lbase. A manual
		// compaction could have been created adhering to that condition, and
		// then an automatic compaction came in and compacted all of the
		// sstables in Lbase to Lbase+1 which caused Lbase to change. Simply
		// ignore this manual compaction as there is nothing to do (manual.level
		// points to an empty level).
		return nil, false
	}
	// This conflictsWithInProgress call is necessary for the manual compaction to
	// be retried when it conflicts with an ongoing automatic compaction. Without
	// it, the compaction is dropped due to pc.setupInputs returning false since
	// the input/output range is already being compacted, and the manual
	// compaction ends with a non-compacted LSM.
	if conflictsWithInProgress(manual, outputLevel, env.inProgressCompactions, p.opts.Comparer.Compare) {
		return nil, true
	}
	pc = pickManualHelper(p.opts, manual, p.vers, p.baseLevel, p.diskAvailBytes, p.levelMaxBytes)
	if pc == nil {
		return nil, false
	}
	if pc.outputLevel.level != outputLevel {
		if len(pc.extraLevels) > 0 {
			// multilevel compactions relax this invariant
		} else {
			panic("pebble: compaction picked unexpected output level")
		}
	}
	// Fail-safe to protect against compacting the same sstable concurrently.
	if inputRangeAlreadyCompacting(env, pc) {
		return nil, true
	}
	return pc, false
}

func pickManualHelper(
	opts *Options,
	manual *manualCompaction,
	vers *version,
	baseLevel int,
	diskAvailBytes func() uint64,
	levelMaxBytes [7]int64,
) (pc *pickedCompaction) {
	pc = newPickedCompaction(opts, vers, manual.level, defaultOutputLevel(manual.level, baseLevel), baseLevel)
	manual.outputLevel = pc.outputLevel.level
	cmp := opts.Comparer.Compare
	pc.startLevel.files = vers.Overlaps(manual.level, cmp, manual.start, manual.end, false)
	if pc.startLevel.files.Empty() {
		// Nothing to do
		return nil
	}
	if !pc.setupInputs(opts, diskAvailBytes(), pc.startLevel) {
		return nil
	}
	if opts.Experimental.MultiLevelCompaction && pc.startLevel.level > 0 &&
		pc.initMultiLevelCompaction(opts, vers, levelMaxBytes, diskAvailBytes()) {
		if !pc.setupInputs(opts, diskAvailBytes(), pc.extraLevels[len(pc.extraLevels)-1]) {
			return nil
		}
	}
	return pc
}

func (p *compactionPickerByScore) pickReadTriggeredCompaction(
	env compactionEnv,
) (pc *pickedCompaction) {
	// If a flush is in-progress or expected to happen soon, it means more writes are taking place. We would
	// soon be scheduling more write focussed compactions. In this case, skip read compactions as they are
	// lower priority.
	if env.readCompactionEnv.flushing || env.readCompactionEnv.readCompactions == nil {
		return nil
	}
	for env.readCompactionEnv.readCompactions.size > 0 {
		rc := env.readCompactionEnv.readCompactions.remove()
		if pc = pickReadTriggeredCompactionHelper(p, rc, env); pc != nil {
			break
		}
	}
	return pc
}

func pickReadTriggeredCompactionHelper(
	p *compactionPickerByScore, rc *readCompaction, env compactionEnv,
) (pc *pickedCompaction) {
	cmp := p.opts.Comparer.Compare
	overlapSlice := p.vers.Overlaps(rc.level, cmp, rc.start, rc.end, false /* exclusiveEnd */)
	if overlapSlice.Empty() {
		// If there is no overlap, then the file with the key range
		// must have been compacted away. So, we don't proceed to
		// compact the same key range again.
		return nil
	}

	iter := overlapSlice.Iter()
	var fileMatches bool
	for f := iter.First(); f != nil; f = iter.Next() {
		if f.FileNum == rc.fileNum {
			fileMatches = true
			break
		}
	}
	if !fileMatches {
		return nil
	}

	pc = newPickedCompaction(p.opts, p.vers, rc.level, defaultOutputLevel(rc.level, p.baseLevel), p.baseLevel)

	pc.startLevel.files = overlapSlice
	if !pc.setupInputs(p.opts, p.diskAvailBytes(), pc.startLevel) {
		return nil
	}
	if inputRangeAlreadyCompacting(env, pc) {
		return nil
	}
	pc.kind = compactionKindRead

	// Prevent read compactions which are too wide.
	outputOverlaps := pc.version.Overlaps(
		pc.outputLevel.level, pc.cmp, pc.smallest.UserKey,
		pc.largest.UserKey, pc.largest.IsExclusiveSentinel())
	if outputOverlaps.SizeSum() > pc.maxReadCompactionBytes {
		return nil
	}

	// Prevent compactions which start with a small seed file X, but overlap
	// with over allowedCompactionWidth * X file sizes in the output layer.
	const allowedCompactionWidth = 35
	if outputOverlaps.SizeSum() > overlapSlice.SizeSum()*allowedCompactionWidth {
		return nil
	}

	return pc
}

func (p *compactionPickerByScore) forceBaseLevel1() {
	p.baseLevel = 1
}

func inputRangeAlreadyCompacting(env compactionEnv, pc *pickedCompaction) bool {
	for _, cl := range pc.inputs {
		iter := cl.files.Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			if f.IsCompacting() {
				return true
			}
		}
	}

	// Look for active compactions outputting to the same region of the key
	// space in the same output level. Two potential compactions may conflict
	// without sharing input files if there are no files in the output level
	// that overlap with the intersection of the compactions' key spaces.
	//
	// Consider an active L0->Lbase compaction compacting two L0 files one
	// [a-f] and the other [t-z] into Lbase.
	//
	// L0
	//     ↦ 000100  ↤                           ↦  000101   ↤
	// L1
	//     ↦ 000004  ↤
	//     a b c d e f g h i j k l m n o p q r s t u v w x y z
	//
	// If a new file 000102 [j-p] is flushed while the existing compaction is
	// still ongoing, new file would not be in any compacting sublevel
	// intervals and would not overlap with any Lbase files that are also
	// compacting. However, this compaction cannot be picked because the
	// compaction's output key space [j-p] would overlap the existing
	// compaction's output key space [a-z].
	//
	// L0
	//     ↦ 000100* ↤       ↦   000102  ↤       ↦  000101*  ↤
	// L1
	//     ↦ 000004* ↤
	//     a b c d e f g h i j k l m n o p q r s t u v w x y z
	//
	// * - currently compacting
	if pc.outputLevel != nil && pc.outputLevel.level != 0 {
		for _, c := range env.inProgressCompactions {
			if pc.outputLevel.level != c.outputLevel {
				continue
			}
			if base.InternalCompare(pc.cmp, c.largest, pc.smallest) < 0 ||
				base.InternalCompare(pc.cmp, c.smallest, pc.largest) > 0 {
				continue
			}

			// The picked compaction and the in-progress compaction c are
			// outputting to the same region of the key space of the same
			// level.
			return true
		}
	}
	return false
}

// conflictsWithInProgress checks if there are any in-progress compactions with overlapping keyspace.
func conflictsWithInProgress(
	manual *manualCompaction, outputLevel int, inProgressCompactions []compactionInfo, cmp Compare,
) bool {
	for _, c := range inProgressCompactions {
		if (c.outputLevel == manual.level || c.outputLevel == outputLevel) &&
			isUserKeysOverlapping(manual.start, manual.end, c.smallest.UserKey, c.largest.UserKey, cmp) {
			return true
		}
		for _, in := range c.inputs {
			if in.files.Empty() {
				continue
			}
			iter := in.files.Iter()
			smallest := iter.First().Smallest.UserKey
			largest := iter.Last().Largest.UserKey
			if (in.level == manual.level || in.level == outputLevel) &&
				isUserKeysOverlapping(manual.start, manual.end, smallest, largest, cmp) {
				return true
			}
		}
	}
	return false
}

func isUserKeysOverlapping(x1, x2, y1, y2 []byte, cmp Compare) bool {
	return cmp(x1, y2) <= 0 && cmp(y1, x2) <= 0
}
