// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
)

type intervalSpan roachpb.Span

var _ interval.Interface = intervalSpan{}

// ID is part of `interval.Interface` but seemed unused by backupccl usage.
func (ie intervalSpan) ID() uintptr { return 0 }

// Range is part of `interval.Interface`.
func (ie intervalSpan) Range() interval.Range {
	return interval.Range{Start: []byte(ie.Key), End: []byte(ie.EndKey)}
}

// targetRestoreSpanSize defines a minimum size for the sum of sizes of files in
// the initial level (base backup or first inc covering some span) of a restore
// span. As each restore span is explicitly split, scattered, processed, and
// explicitly flushed, large numbers of tiny restore spans are costly, both in
// terms of overhead associated with the splits, scatters and flushes and then
// in subsequently merging the tiny ranges. Thus making sure this is at least in
// the neighborhood of a typical range size minimizes that overhead. A restore
// span may well grow beyond this size when later incremental layer files are
// added to it, but that's fine: we will split as we fill if needed. 384mb is
// big enough to avoid the worst of tiny-span overhead, while small enough to be
// a granular unit of work distribution and progress tracking. If progress were
// tracked within restore spans, this could become dynamic and much larger (e.g.
// totalSize/numNodes*someConstant).
const targetRestoreSpanSize = 384 << 20

// makeSimpleImportSpans partitions the spans of requiredSpans into a covering
// of RestoreSpanEntry's which each have all overlapping files from the passed
// backups assigned to them. The spans of requiredSpans are trimmed/removed
// based on the lowWaterMark before the covering for them is generated. Consider
// a chain of backups with files f1, f2… which cover spans as follows:
//
//	backup
//	0|     a___1___c c__2__e          h__3__i
//	1|         b___4___d           g____5___i
//	2|     a___________6______________h         j_7_k
//	3|                                  h_8_i              l_9_m
//	 keys--a---b---c---d---e---f---g---h----i---j---k---l----m------p---->
//
// spans: |-------span1-------||---span2---|           |---span3---|
//
// The cover for those spans would look like:
//
//	[a, c): 1, 4, 6
//	[c, e): 2, 4, 6
//	[e, f): 6
//	[f, i): 3, 5, 6, 8
//	[l, m): 9
//
// This example is tested in TestRestoreEntryCoverExample.
//
// If targetSize > 0, then spans which would be added to the right-hand side of
// the first level are instead used to extend the current rightmost span in
// if its current data size plus that of the new span is less than the target
// size.
func makeSimpleImportSpans(
	requiredSpans []roachpb.Span,
	backups []backuppb.BackupManifest,
	backupLocalityMap map[int]storeByLocalityKV,
	lowWaterMark roachpb.Key,
	targetSize int64,
) []execinfrapb.RestoreSpanEntry {
	if len(backups) < 1 {
		return nil
	}

	for i := range backups {
		sort.Sort(backupinfo.BackupFileDescriptors(backups[i].Files))
	}

	var cover []execinfrapb.RestoreSpanEntry
	for _, span := range requiredSpans {
		if span.EndKey.Compare(lowWaterMark) < 0 {
			continue
		}
		if span.Key.Compare(lowWaterMark) < 0 {
			span.Key = lowWaterMark
		}

		spanCoverStart := len(cover)

		for layer := range backups {
			covPos := spanCoverStart

			// lastCovSpanSize is the size of files added to the right-most span of
			// the cover so far.
			var lastCovSpanSize int64

			// TODO(dt): binary search to the first file in required span?
			for _, f := range backups[layer].Files {
				if sp := span.Intersect(f.Span); sp.Valid() {
					fileSpec := execinfrapb.RestoreFileSpec{Path: f.Path, Dir: backups[layer].Dir}
					if dir, ok := backupLocalityMap[layer][f.LocalityKV]; ok {
						fileSpec = execinfrapb.RestoreFileSpec{Path: f.Path, Dir: dir}
					}

					// Lookup the size of the file being added; if the backup didn't
					// record a file size, just assume it is 16mb for estimating.
					sz := f.EntryCounts.DataSize
					if sz == 0 {
						sz = 16 << 20
					}

					if len(cover) == spanCoverStart {
						cover = append(cover, makeEntry(span.Key, sp.EndKey, fileSpec))
						lastCovSpanSize = sz
					} else {
						// If this file extends beyond the end of the last partition of the
						// cover, either append a new partition for the uncovered span or
						// grow the last one if size allows.
						if covEnd := cover[len(cover)-1].Span.EndKey; sp.EndKey.Compare(covEnd) > 0 {
							// If adding the item size to the current rightmost span size will
							// exceed the target size, make a new span, otherwise extend the
							// rightmost span to include the item.
							if lastCovSpanSize+sz > targetSize {
								cover = append(cover, makeEntry(covEnd, sp.EndKey, fileSpec))
								lastCovSpanSize = sz
							} else {
								cover[len(cover)-1].Span.EndKey = sp.EndKey
								cover[len(cover)-1].Files = append(cover[len(cover)-1].Files, fileSpec)
								lastCovSpanSize += sz
							}
						}
						// Now ensure the file is included in any partition in the existing
						// cover which overlaps.
						for i := covPos; i < len(cover) && cover[i].Span.Key.Compare(sp.EndKey) < 0; i++ {
							// If file overlaps, it needs to be in this partition.
							if cover[i].Span.Overlaps(sp) {
								// If this is the last partition, we might have added it above.
								if i == len(cover)-1 {
									if last := len(cover[i].Files) - 1; last < 0 || cover[i].Files[last] != fileSpec {
										cover[i].Files = append(cover[i].Files, fileSpec)
										lastCovSpanSize += sz
									}
								} else {
									// If it isn't the last partition, we always need to add it.
									cover[i].Files = append(cover[i].Files, fileSpec)
								}
							}
							// If partition i of the cover ends before this file starts, we
							// know it also ends before any remaining files start too, as the
							// files are sorted above by start key, so remaining files can
							// start their search after this partition.
							if cover[i].Span.EndKey.Compare(sp.Key) <= 0 {
								covPos = i + 1
							}
						}
					}
				} else if span.EndKey.Compare(f.Span.Key) <= 0 {
					// If this file starts after the needed span ends, then all the files
					// remaining do too so we're done checking files for this span.
					break
				}
			}
		}
	}

	return cover
}

func makeEntry(start, end roachpb.Key, f execinfrapb.RestoreFileSpec) execinfrapb.RestoreSpanEntry {
	return execinfrapb.RestoreSpanEntry{
		Span:  roachpb.Span{Key: start, EndKey: end},
		Files: []execinfrapb.RestoreFileSpec{f},
	}
}
