// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"math"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/sstable"
)

// In-memory statistics about tables help inform compaction picking, but may
// be expensive to calculate or load from disk. Every time a database is
// opened, these statistics must be reloaded or recalculated. To minimize
// impact on user activity and compactions, we load these statistics
// asynchronously in the background and store loaded statistics in each
// table's *FileMetadata.
//
// This file implements the asynchronous loading of statistics by maintaining
// a list of files that require statistics, alongside their LSM levels.
// Whenever new files are added to the LSM, the files are appended to
// d.mu.tableStats.pending. If a stats collection job is not currently
// running, one is started in a separate goroutine.
//
// The stats collection job grabs and clears the pending list, computes table
// statistics relative to the current readState and updates the tables' file
// metadata. New pending files may accumulate during a stats collection job,
// so a completing job triggers a new job if necessary. Only one job runs at a
// time.
//
// When an existing database is opened, all files lack in-memory statistics.
// These files' stats are loaded incrementally whenever the pending list is
// empty by scanning a current readState for files missing statistics. Once a
// job completes a scan without finding any remaining files without
// statistics, it flips a `loadedInitial` flag. From then on, the stats
// collection job only needs to load statistics for new files appended to the
// pending list.

func (d *DB) maybeCollectTableStatsLocked() {
	if d.shouldCollectTableStatsLocked() {
		go d.collectTableStats()
	}
}

// updateTableStatsLocked is called when new files are introduced, after the
// read state has been updated. It may trigger a new stat collection.
// DB.mu must be locked when calling.
func (d *DB) updateTableStatsLocked(newFiles []manifest.NewFileEntry) {
	var needStats bool
	for _, nf := range newFiles {
		if !nf.Meta.StatsValidLocked() {
			needStats = true
			break
		}
	}
	if !needStats {
		return
	}

	d.mu.tableStats.pending = append(d.mu.tableStats.pending, newFiles...)
	d.maybeCollectTableStatsLocked()
}

func (d *DB) shouldCollectTableStatsLocked() bool {
	return !d.mu.tableStats.loading &&
		d.closed.Load() == nil &&
		!d.opts.private.disableTableStats &&
		(len(d.mu.tableStats.pending) > 0 || !d.mu.tableStats.loadedInitial)
}

// collectTableStats runs a table stats collection job, returning true if the
// invocation did the collection work, false otherwise (e.g. if another job was
// already running).
func (d *DB) collectTableStats() bool {
	const maxTableStatsPerScan = 50

	d.mu.Lock()
	if !d.shouldCollectTableStatsLocked() {
		d.mu.Unlock()
		return false
	}

	pending := d.mu.tableStats.pending
	d.mu.tableStats.pending = nil
	d.mu.tableStats.loading = true
	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	loadedInitial := d.mu.tableStats.loadedInitial
	// Drop DB.mu before performing IO.
	d.mu.Unlock()

	// Every run of collectTableStats either collects stats from the pending
	// list (if non-empty) or from scanning the version (loadedInitial is
	// false). This job only runs if at least one of those conditions holds.

	// Grab a read state to scan for tables.
	rs := d.loadReadState()
	var collected []collectedStats
	var hints []deleteCompactionHint
	if len(pending) > 0 {
		collected, hints = d.loadNewFileStats(rs, pending)
	} else {
		var moreRemain bool
		var buf [maxTableStatsPerScan]collectedStats
		collected, hints, moreRemain = d.scanReadStateTableStats(rs, buf[:0])
		loadedInitial = !moreRemain
	}
	rs.unref()

	// Update the FileMetadata with the loaded stats while holding d.mu.
	d.mu.Lock()
	defer d.mu.Unlock()
	d.mu.tableStats.loading = false
	if loadedInitial && !d.mu.tableStats.loadedInitial {
		d.mu.tableStats.loadedInitial = loadedInitial
		d.opts.EventListener.TableStatsLoaded(TableStatsInfo{
			JobID: jobID,
		})
	}

	maybeCompact := false
	for _, c := range collected {
		c.fileMetadata.Stats = c.TableStats
		maybeCompact = maybeCompact || c.TableStats.RangeDeletionsBytesEstimate > 0
		c.fileMetadata.StatsMarkValid()
	}
	d.mu.tableStats.cond.Broadcast()
	d.maybeCollectTableStatsLocked()
	if len(hints) > 0 {
		// Verify that all of the hint tombstones' files still exist in the
		// current version. Otherwise, the tombstone itself may have been
		// compacted into L6 and more recent keys may have had their sequence
		// numbers zeroed.
		//
		// Note that it's possible that the tombstone file is being compacted
		// presently. In that case, the file will be present in v. When the
		// compaction finishes compacting the tombstone file, it will detect
		// and clear the hint.
		//
		// See DB.maybeUpdateDeleteCompactionHints.
		v := d.mu.versions.currentVersion()
		keepHints := hints[:0]
		for _, h := range hints {
			if v.Contains(h.tombstoneLevel, d.cmp, h.tombstoneFile) {
				keepHints = append(keepHints, h)
			}
		}
		d.mu.compact.deletionHints = append(d.mu.compact.deletionHints, keepHints...)
	}
	if maybeCompact {
		d.maybeScheduleCompaction()
	}
	return true
}

type collectedStats struct {
	*fileMetadata
	manifest.TableStats
}

func (d *DB) loadNewFileStats(
	rs *readState, pending []manifest.NewFileEntry,
) ([]collectedStats, []deleteCompactionHint) {
	var hints []deleteCompactionHint
	collected := make([]collectedStats, 0, len(pending))
	for _, nf := range pending {
		// A file's stats might have been populated by an earlier call to
		// loadNewFileStats if the file was moved.
		// NB: We're not holding d.mu which protects f.Stats, but only
		// collectTableStats updates f.Stats for active files, and we
		// ensure only one goroutine runs it at a time through
		// d.mu.tableStats.loading.
		if nf.Meta.StatsValidLocked() {
			continue
		}

		// The file isn't guaranteed to still be live in the readState's
		// version. It may have been deleted or moved. Skip it if it's not in
		// the expected level.
		if !rs.current.Contains(nf.Level, d.cmp, nf.Meta) {
			continue
		}

		stats, newHints, err := d.loadTableStats(rs.current, nf.Level, nf.Meta)
		if err != nil {
			d.opts.EventListener.BackgroundError(err)
			continue
		}
		// NB: We don't update the FileMetadata yet, because we aren't
		// holding DB.mu. We'll copy it to the FileMetadata after we're
		// finished with IO.
		collected = append(collected, collectedStats{
			fileMetadata: nf.Meta,
			TableStats:   stats,
		})
		hints = append(hints, newHints...)
	}
	return collected, hints
}

// scanReadStateTableStats is run by an active stat collection job when there
// are no pending new files, but there might be files that existed at Open for
// which we haven't loaded table stats.
func (d *DB) scanReadStateTableStats(
	rs *readState, fill []collectedStats,
) ([]collectedStats, []deleteCompactionHint, bool) {
	moreRemain := false
	var hints []deleteCompactionHint
	for l, levelMetadata := range rs.current.Levels {
		iter := levelMetadata.Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			// NB: We're not holding d.mu which protects f.Stats, but only the
			// active stats collection job updates f.Stats for active files,
			// and we ensure only one goroutine runs it at a time through
			// d.mu.tableStats.loading. This makes it safe to read validity
			// through f.Stats.ValidLocked despite not holding d.mu.
			if f.StatsValidLocked() {
				continue
			}

			// Limit how much work we do per read state. The older the read
			// state is, the higher the likelihood files are no longer being
			// used in the current version. If we've exhausted our allowance,
			// return true for the last return value to signal there's more
			// work to do.
			if len(fill) == cap(fill) {
				moreRemain = true
				return fill, hints, moreRemain
			}

			stats, newHints, err := d.loadTableStats(rs.current, l, f)
			if err != nil {
				// Set `moreRemain` so we'll try again.
				moreRemain = true
				d.opts.EventListener.BackgroundError(err)
				continue
			}
			fill = append(fill, collectedStats{
				fileMetadata: f,
				TableStats:   stats,
			})
			hints = append(hints, newHints...)
		}
	}
	return fill, hints, moreRemain
}

func (d *DB) loadTableStats(
	v *version, level int, meta *fileMetadata,
) (manifest.TableStats, []deleteCompactionHint, error) {
	var stats manifest.TableStats
	var compactionHints []deleteCompactionHint
	err := d.tableCache.withReader(meta, func(r *sstable.Reader) (err error) {
		stats.NumEntries = r.Properties.NumEntries
		stats.NumDeletions = r.Properties.NumDeletions
		if r.Properties.NumPointDeletions() > 0 {
			if err = d.loadTablePointKeyStats(r, v, level, meta, &stats); err != nil {
				return
			}
		}
		if r.Properties.NumRangeDeletions > 0 || r.Properties.NumRangeKeyDels > 0 {
			if compactionHints, err = d.loadTableRangeDelStats(r, v, level, meta, &stats); err != nil {
				return
			}
		}
		// TODO(travers): Once we have real-world data, consider collecting
		// additional stats that may provide improved heuristics for compaction
		// picking.
		stats.NumRangeKeySets = r.Properties.NumRangeKeySets
		return
	})
	if err != nil {
		return stats, nil, err
	}
	return stats, compactionHints, nil
}

// loadTablePointKeyStats calculates the point key statistics for the given
// table. The provided manifest.TableStats are updated.
func (d *DB) loadTablePointKeyStats(
	r *sstable.Reader, v *version, level int, meta *fileMetadata, stats *manifest.TableStats,
) error {
	// TODO(jackson): If the file has a wide keyspace, the average
	// value size beneath the entire file might not be representative
	// of the size of the keys beneath the point tombstones.
	// We could write the ranges of 'clusters' of point tombstones to
	// a sstable property and call averageValueSizeBeneath for each of
	// these narrower ranges to improve the estimate.
	avgKeySize, avgValSize, err := d.averageEntrySizeBeneath(v, level, meta)
	if err != nil {
		return err
	}
	stats.PointDeletionsBytesEstimate =
		pointDeletionsBytesEstimate(&r.Properties, avgKeySize, avgValSize)
	return nil
}

// loadTableRangeDelStats calculates the range deletion and range key deletion
// statistics for the given table.
func (d *DB) loadTableRangeDelStats(
	r *sstable.Reader, v *version, level int, meta *fileMetadata, stats *manifest.TableStats,
) ([]deleteCompactionHint, error) {
	iter, err := newCombinedDeletionKeyspanIter(d.opts.Comparer, r, meta)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var compactionHints []deleteCompactionHint
	// We iterate over the defragmented range tombstones and range key deletions,
	// which ensures we don't double count ranges deleted at different sequence
	// numbers. Also, merging abutting tombstones reduces the number of calls to
	// estimateReclaimedSizeBeneath which is costly, and improves the accuracy of
	// our overall estimate.
	for s := iter.First(); s != nil; s = iter.Next() {
		start, end := s.Start, s.End
		// We only need to consider deletion size estimates for tables that contain
		// point keys.
		var hasPoints bool
		for _, k := range s.Keys {
			if k.Kind() == base.InternalKeyKindRangeDelete {
				hasPoints = true
				break
			}
		}

		// If the file is in the last level of the LSM, there is no data beneath
		// it. The fact that there is still a range tombstone in a bottommost file
		// suggests that an open snapshot kept the tombstone around. Estimate disk
		// usage within the file itself.
		// NOTE: If the span `s` wholly contains a table containing range keys,
		// the returned size estimate will be slightly inflated by the range key
		// block. However, in practice, range keys are expected to be rare, and
		// the size of the range key block relative to the overall size of the
		// table is expected to be small.
		if hasPoints && level == numLevels-1 {
			size, err := r.EstimateDiskUsage(start, end)
			if err != nil {
				return nil, err
			}
			stats.RangeDeletionsBytesEstimate += size

			// As the file is in the bottommost level, there is no need to collect a
			// deletion hint.
			continue
		}

		// While the size estimates for point keys should only be updated if this
		// span contains a range del, the sequence numbers are required for the
		// hint. Unconditionally descend, but conditionally update the estimates.
		hintType := compactionHintFromKeys(s.Keys)
		estimate, hintSeqNum, err := d.estimateReclaimedSizeBeneath(v, level, start, end, hintType)
		if err != nil {
			return nil, err
		}
		stats.RangeDeletionsBytesEstimate += estimate

		// If any files were completely contained with the range,
		// hintSeqNum is the smallest sequence number contained in any
		// such file.
		if hintSeqNum == math.MaxUint64 {
			continue
		}
		hint := deleteCompactionHint{
			hintType:                hintType,
			start:                   make([]byte, len(start)),
			end:                     make([]byte, len(end)),
			tombstoneFile:           meta,
			tombstoneLevel:          level,
			tombstoneLargestSeqNum:  s.LargestSeqNum(),
			tombstoneSmallestSeqNum: s.SmallestSeqNum(),
			fileSmallestSeqNum:      hintSeqNum,
		}
		copy(hint.start, start)
		copy(hint.end, end)
		compactionHints = append(compactionHints, hint)
	}
	return compactionHints, err
}

func (d *DB) averageEntrySizeBeneath(
	v *version, level int, meta *fileMetadata,
) (avgKeySize, avgValueSize uint64, err error) {
	// Find all files in lower levels that overlap with meta,
	// summing their value sizes and entry counts.
	var fileSum, keySum, valSum, entryCount uint64
	for l := level + 1; l < numLevels; l++ {
		overlaps := v.Overlaps(l, d.cmp, meta.Smallest.UserKey,
			meta.Largest.UserKey, meta.Largest.IsExclusiveSentinel())
		iter := overlaps.Iter()
		for file := iter.First(); file != nil; file = iter.Next() {
			err := d.tableCache.withReader(file, func(r *sstable.Reader) (err error) {
				fileSum += file.Size
				entryCount += r.Properties.NumEntries
				keySum += r.Properties.RawKeySize
				valSum += r.Properties.RawValueSize
				return nil
			})
			if err != nil {
				return 0, 0, err
			}
		}
	}
	if entryCount == 0 {
		return 0, 0, nil
	}
	// RawKeySize and RawValueSize are uncompressed totals. Scale them
	// according to the data size to account for compression, index blocks and
	// metadata overhead. Eg:
	//
	//    Compression rate        ×  Average uncompressed key size
	//
	//                            ↓
	//
	//         FileSize              RawKeySize
	//   -----------------------  ×  ----------
	//   RawKeySize+RawValueSize     NumEntries
	//
	// We refactor the calculation to avoid error from rounding/truncation.
	totalSizePerEntry := fileSum / entryCount
	uncompressedSum := keySum + valSum
	avgKeySize = keySum * totalSizePerEntry / uncompressedSum
	avgValueSize = valSum * totalSizePerEntry / uncompressedSum
	return avgKeySize, avgValueSize, err
}

func (d *DB) estimateReclaimedSizeBeneath(
	v *version, level int, start, end []byte, hintType deleteCompactionHintType,
) (estimate uint64, hintSeqNum uint64, err error) {
	// Find all files in lower levels that overlap with the deleted range
	// [start, end).
	//
	// An overlapping file might be completely contained by the range
	// tombstone, in which case we can count the entire file size in
	// our estimate without doing any additional I/O.
	//
	// Otherwise, estimating the range for the file requires
	// additional I/O to read the file's index blocks.
	hintSeqNum = math.MaxUint64
	for l := level + 1; l < numLevels; l++ {
		overlaps := v.Overlaps(l, d.cmp, start, end, true /* exclusiveEnd */)
		iter := overlaps.Iter()
		for file := iter.First(); file != nil; file = iter.Next() {
			startCmp := d.cmp(start, file.Smallest.UserKey)
			endCmp := d.cmp(file.Largest.UserKey, end)
			if startCmp <= 0 && (endCmp < 0 || endCmp == 0 && file.Largest.IsExclusiveSentinel()) {
				// The range fully contains the file, so skip looking it up in table
				// cache/looking at its indexes and add the full file size. Whether the
				// disk estimate and hint seqnums are updated depends on a) the type of
				// hint that requested the estimate and b) the keys contained in this
				// current file.
				var updateEstimates, updateHints bool
				switch hintType {
				case deleteCompactionHintTypePointKeyOnly:
					// The range deletion byte estimates should only be updated if this
					// table contains point keys. This ends up being an overestimate in
					// the case that table also has range keys, but such keys are expected
					// to contribute a negligible amount of the table's overall size,
					// relative to point keys.
					if file.HasPointKeys {
						updateEstimates = true
					}
					// As the initiating span contained only range dels, hints can only be
					// updated if this table does _not_ contain range keys.
					if !file.HasRangeKeys {
						updateHints = true
					}
				case deleteCompactionHintTypeRangeKeyOnly:
					// The initiating span contained only range key dels. The estimates
					// apply only to point keys, and are therefore not updated.
					updateEstimates = false
					// As the initiating span contained only range key dels, hints can
					// only be updated if this table does _not_ contain point keys.
					if !file.HasPointKeys {
						updateHints = true
					}
				case deleteCompactionHintTypePointAndRangeKey:
					// Always update the estimates and hints, as this hint type can drop a
					// file, irrespective of the mixture of keys. Similar to above, the
					// range del bytes estimates is an overestimate.
					updateEstimates, updateHints = true, true
				default:
					panic(fmt.Sprintf("pebble: unknown hint type %s", hintType))
				}
				if updateEstimates {
					estimate += file.Size
				}
				if updateHints && hintSeqNum > file.SmallestSeqNum {
					hintSeqNum = file.SmallestSeqNum
				}
			} else if d.cmp(file.Smallest.UserKey, end) <= 0 && d.cmp(start, file.Largest.UserKey) <= 0 {
				// Partial overlap.
				if hintType == deleteCompactionHintTypeRangeKeyOnly {
					// If the hint that generated this overlap contains only range keys,
					// there is no need to calculate disk usage, as the reclaimable space
					// is expected to be minimal relative to point keys.
					continue
				}
				var size uint64
				err := d.tableCache.withReader(file, func(r *sstable.Reader) (err error) {
					size, err = r.EstimateDiskUsage(start, end)
					return err
				})
				if err != nil {
					return 0, hintSeqNum, err
				}
				estimate += size
			}
		}
	}
	return estimate, hintSeqNum, nil
}

func maybeSetStatsFromProperties(meta *fileMetadata, props *sstable.Properties) bool {
	// If a table contains range deletions or range key deletions, we defer the
	// stats collection. There are two main reasons for this:
	//
	//  1. Estimating the potential for reclaimed space due to a range deletion
	//     tombstone requires scanning the LSM - a potentially expensive operation
	//     that should be deferred.
	//  2. Range deletions and / or range key deletions present an opportunity to
	//     compute "deletion hints", which also requires a scan of the LSM to
	//     compute tables that would be eligible for deletion.
	//
	// These two tasks are deferred to the table stats collector goroutine.
	if props.NumRangeDeletions != 0 || props.NumRangeKeyDels != 0 {
		return false
	}

	// If a table is more than 10% point deletions, don't calculate the
	// PointDeletionsBytesEstimate statistic using our limited knowledge. The
	// table stats collector can populate the stats and calculate an average
	// of value size of all the tables beneath the table in the LSM, which
	// will be more accurate.
	if props.NumDeletions > props.NumEntries/10 {
		return false
	}

	var pointEstimate uint64
	if props.NumEntries > 0 {
		// Use the file's own average key and value sizes as an estimate. This
		// doesn't require any additional IO and since the number of point
		// deletions in the file is low, the error introduced by this crude
		// estimate is expected to be small.
		avgKeySize, avgValSize := estimateEntrySizes(meta.Size, props)
		pointEstimate = pointDeletionsBytesEstimate(props, avgKeySize, avgValSize)
	}

	meta.Stats.NumEntries = props.NumEntries
	meta.Stats.NumDeletions = props.NumDeletions
	meta.Stats.NumRangeKeySets = props.NumRangeKeySets
	meta.Stats.PointDeletionsBytesEstimate = pointEstimate
	meta.Stats.RangeDeletionsBytesEstimate = 0
	meta.StatsMarkValid()
	return true
}

func pointDeletionsBytesEstimate(props *sstable.Properties, avgKeySize, avgValSize uint64) uint64 {
	if props.NumEntries == 0 {
		return 0
	}
	// Estimate the potential space to reclaim using the table's own
	// properties. There may or may not be keys covered by any individual
	// point tombstone. If not, compacting the point tombstone into L6 will at
	// least allow us to drop the point deletion key and will reclaim the key
	// bytes. If there are covered key(s), we also get to drop key and value
	// bytes for each covered key.
	//
	// We estimate assuming that each point tombstone on average covers 1 key.
	// This is almost certainly an overestimate, but that's probably okay
	// because point tombstones can slow range iterations even when they don't
	// cover a key. It may be beneficial in the future to more accurately
	// estimate which tombstones cover keys and which do not.
	numPointDels := props.NumPointDeletions()
	return numPointDels*avgKeySize + numPointDels*(avgKeySize+avgValSize)
}

func estimateEntrySizes(
	fileSize uint64, props *sstable.Properties,
) (avgKeySize, avgValSize uint64) {
	// RawKeySize and RawValueSize are uncompressed totals. Scale them
	// according to the data size to account for compression, index blocks and
	// metadata overhead. Eg:
	//
	//    Compression rate        ×  Average uncompressed key size
	//
	//                            ↓
	//
	//         FileSize              RawKeySize
	//   -----------------------  ×  ----------
	//   RawKeySize+RawValueSize     NumEntries
	//
	// We refactor the calculation to avoid error from rounding/truncation.
	fileSizePerEntry := fileSize / props.NumEntries
	uncompressedSum := props.RawKeySize + props.RawValueSize
	avgKeySize = props.RawKeySize * fileSizePerEntry / uncompressedSum
	avgValSize = props.RawValueSize * fileSizePerEntry / uncompressedSum
	return avgKeySize, avgValSize
}

// newCombinedDeletionKeyspanIter returns a keyspan.FragmentIterator that
// returns "ranged deletion" spans for a single table, providing a combined view
// of both range deletion and range key deletion spans. The
// tableRangedDeletionIter is intended for use in the specific case of computing
// the statistics and deleteCompactionHints for a single table.
//
// As an example, consider the following set of spans from the range deletion
// and range key blocks of a table:
//
//         |---------|     |---------|         |-------| RANGEKEYDELs
//   |-----------|-------------|           |-----|       RANGEDELs
// __________________________________________________________
//   a b c d e f g h i j k l m n o p q r s t u v w x y z
//
// The tableRangedDeletionIter produces the following set of output spans, where
// '1' indicates a span containing only range deletions, '2' is a span
// containing only range key deletions, and '3' is a span containing a mixture
// of both range deletions and range key deletions.
//
//      1       3       1    3    2          1  3   2
//   |-----|---------|-----|---|-----|     |---|-|-----|
// __________________________________________________________
//   a b c d e f g h i j k l m n o p q r s t u v w x y z
//
// Algorithm.
//
// The iterator first defragments the range deletion and range key blocks
// separately. During this defragmentation, the range key block is also filtered
// so that keys other than range key deletes are ignored. The range delete and
// range key delete keyspaces are then merged.
//
// Note that the only fragmentation introduced by merging is from where a range
// del span overlaps with a range key del span. Within the bounds of any overlap
// there is guaranteed to be no further fragmentation, as the constituent spans
// have already been defragmented. To the left and right of any overlap, the
// same reasoning applies. For example,
//
//            |--------|         |-------| RANGEKEYDEL
//   |---------------------------|         RANGEDEL
//   |----1---|----3---|----1----|---2---| Merged, fragmented spans.
// __________________________________________________________
//   a b c d e f g h i j k l m n o p q r s t u v w x y z
//
// Any fragmented abutting spans produced by the merging iter will be of
// differing types (i.e. a transition from a span with homogenous key kinds to a
// heterogeneous span, or a transition from a span with exclusively range dels
// to a span with exclusively range key dels). Therefore, further
// defragmentation is not required.
//
// Each span returned by the tableRangeDeletionIter will have at most four keys,
// corresponding to the largest and smallest sequence numbers encountered across
// the range deletes and range keys deletes that comprised the merged spans.
func newCombinedDeletionKeyspanIter(
	comparer *base.Comparer, r *sstable.Reader, m *fileMetadata,
) (keyspan.FragmentIterator, error) {
	// The range del iter and range key iter are each wrapped in their own
	// defragmenting iter. For each iter, abutting spans can always be merged.
	var equal = keyspan.DefragmentMethodFunc(func(_ base.Equal, a, b *keyspan.Span) bool { return true })
	// Reduce keys by maintaining a slice of at most length two, corresponding to
	// the largest and smallest keys in the defragmented span. This maintains the
	// contract that the emitted slice is sorted by (SeqNum, Kind) descending.
	reducer := func(current, incoming []keyspan.Key) []keyspan.Key {
		if len(current) == 0 && len(incoming) == 0 {
			// While this should never occur in practice, a defensive return is used
			// here to preserve correctness.
			return current
		}
		var largest, smallest keyspan.Key
		var set bool
		for _, keys := range [2][]keyspan.Key{current, incoming} {
			if len(keys) == 0 {
				continue
			}
			first, last := keys[0], keys[len(keys)-1]
			if !set {
				largest, smallest = first, last
				set = true
				continue
			}
			if first.Trailer > largest.Trailer {
				largest = first
			}
			if last.Trailer < smallest.Trailer {
				smallest = last
			}
		}
		if largest.Equal(comparer.Equal, smallest) {
			current = append(current[:0], largest)
		} else {
			current = append(current[:0], largest, smallest)
		}
		return current
	}

	// The separate iters for the range dels and range keys are wrapped in a
	// merging iter to join the keyspaces into a single keyspace. The separate
	// iters are only added if the particular key kind is present.
	mIter := &keyspan.MergingIter{}
	var transform = keyspan.TransformerFunc(func(cmp base.Compare, in keyspan.Span, out *keyspan.Span) error {
		if in.KeysOrder != keyspan.ByTrailerDesc {
			panic("pebble: combined deletion iter encountered keys in non-trailer descending order")
		}
		out.Start, out.End = in.Start, in.End
		out.Keys = append(out.Keys[:0], in.Keys...)
		out.KeysOrder = keyspan.ByTrailerDesc
		// NB: The order of by-trailer descending may have been violated,
		// because we've layered rangekey and rangedel iterators from the same
		// sstable into the same keyspan.MergingIter. The MergingIter will
		// return the keys in the order that the child iterators were provided.
		// Sort the keys to ensure they're sorted by trailer descending.
		keyspan.SortKeysByTrailer(&out.Keys)
		return nil
	})
	mIter.Init(comparer.Compare, transform)

	iter, err := r.NewRawRangeDelIter()
	if err != nil {
		return nil, err
	}
	if iter != nil {
		dIter := &keyspan.DefragmentingIter{}
		dIter.Init(comparer, iter, equal, reducer)
		iter = dIter
		// Truncate tombstones to the containing file's bounds if necessary.
		// See docs/range_deletions.md for why this is necessary.
		iter = keyspan.Truncate(
			comparer.Compare, iter, m.Smallest.UserKey, m.Largest.UserKey, nil, nil,
		)
		mIter.AddLevel(iter)
	}

	iter, err = r.NewRawRangeKeyIter()
	if err != nil {
		return nil, err
	}
	if iter != nil {
		// Wrap the range key iterator in a filter that elides keys other than range
		// key deletions.
		iter = keyspan.Filter(iter, func(in *keyspan.Span, out *keyspan.Span) (keep bool) {
			out.Start, out.End = in.Start, in.End
			out.Keys = out.Keys[:0]
			for _, k := range in.Keys {
				if k.Kind() != base.InternalKeyKindRangeKeyDelete {
					continue
				}
				out.Keys = append(out.Keys, k)
			}
			return len(out.Keys) > 0
		})
		dIter := &keyspan.DefragmentingIter{}
		dIter.Init(comparer, iter, equal, reducer)
		iter = dIter
		mIter.AddLevel(iter)
	}

	return mIter, nil
}

// rangeKeySetsAnnotator implements manifest.Annotator, annotating B-Tree nodes
// with the sum of the files' counts of range key fragments. Its annotation type
// is a *uint64. The count of range key sets may change once a table's stats are
// loaded asynchronously, so its values are marked as cacheable only if a file's
// stats have been loaded.
type rangeKeySetsAnnotator struct{}

var _ manifest.Annotator = rangeKeySetsAnnotator{}

func (a rangeKeySetsAnnotator) Zero(dst interface{}) interface{} {
	if dst == nil {
		return new(uint64)
	}
	v := dst.(*uint64)
	*v = 0
	return v
}

func (a rangeKeySetsAnnotator) Accumulate(
	f *fileMetadata, dst interface{},
) (v interface{}, cacheOK bool) {
	vptr := dst.(*uint64)
	*vptr = *vptr + f.Stats.NumRangeKeySets
	return vptr, f.StatsValidLocked()
}

func (a rangeKeySetsAnnotator) Merge(src interface{}, dst interface{}) interface{} {
	srcV := src.(*uint64)
	dstV := dst.(*uint64)
	*dstV = *dstV + *srcV
	return dstV
}

// countRangeKeySetFragments counts the number of RANGEKEYSET keys across all
// files of the LSM. It only counts keys in files for which table stats have
// been loaded. It uses a b-tree annotator to cache intermediate values between
// calculations when possible.
func countRangeKeySetFragments(v *version) (count uint64) {
	for l := 0; l < numLevels; l++ {
		if v.RangeKeyLevels[l].Empty() {
			continue
		}
		count += *v.RangeKeyLevels[l].Annotation(rangeKeySetsAnnotator{}).(*uint64)
	}
	return count
}
