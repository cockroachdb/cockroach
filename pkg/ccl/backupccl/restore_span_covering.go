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

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
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

var spanSizeUpperBound = settings.RegisterIntSetting(
	settings.TenantWritable,
	"restore.restore_span_size_upper_bound",
	"the upper bound on the unit of work for each sstBatcher in a restore processor. If set to 0, "+
		"restore span creation will not consider expected work size",
	512,
)

// makeSimpleImportSpans partitions the spans of requiredSpans into a covering
// of RestoreSpanEntry's which each have all overlapping files from the passed
// backups assigned to them. The spans of requiredSpans are trimmed/removed
// based on the lowWaterMark before the covering for them is generated. Consider
// a chain of backups with files f1, f2… which cover spans as follows:
//
//  backup
//  0|     a___1___c c__2__e          h__3__i
//  1|         b___4___d           g____5___i
//  2|     a___________6______________h         j_7_k
//  3|                                  h_8_i              l_9_m
//   keys--a---b---c---d---e---f---g---h----i---j---k---l----m------p---->
// spans: |-------span1-------||---span2---|           |---span3---|
//
// The cover for those spans would look like:
//  [a, c): 1, 4, 6
//  [c, e): 2, 4, 6
//  [e, f): 6
//  [f, i): 3, 5, 6, 8
//  [l, m): 9
// This example is tested in TestRestoreEntryCoverExample.
//
// If spanSizeUpperBound > 0, restoreSpanEntry's are combined greedily combined
// such that each restoreSpan entry's logical data size is close to
// spanSizeUpperBound.
func makeSimpleImportSpans(
	requiredSpans []roachpb.Span,
	backups []backuppb.BackupManifest,
	backupLocalityMap map[int]storeByLocalityKV,
	lowWaterMark roachpb.Key,
	spanSizeUpperBound int64,
) []execinfrapb.RestoreSpanEntry {

	if len(backups) < 1 {
		return nil
	}

	for i := range backups {
		sort.Sort(BackupFileDescriptors(backups[i].Files))
	}

	var cover []execinfrapb.RestoreSpanEntry
	fillingSpanIdx := 0

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
			// TODO(dt): binary search to the first file in required span?
			for _, f := range backups[layer].Files {
				if sp := span.Intersect(f.Span); sp.Valid() {
					fileSpec := execinfrapb.RestoreFileSpec{Path: f.Path, Dir: backups[layer].Dir}
					if dir, ok := backupLocalityMap[layer][f.LocalityKV]; ok {
						fileSpec = execinfrapb.RestoreFileSpec{Path: f.Path, Dir: dir}
					}
					if len(cover) == spanCoverStart {
						cover = append(cover, makeEntry(span.Key, sp.EndKey, fileSpec, f.Span,
							spanSizeUpperBound))
					} else {
						// Add each file to every matching partition in the cover.
						for i := covPos; i < len(cover) && cover[i].Span.Key.Compare(sp.EndKey) < 0; i++ {
							if cover[i].Span.Overlaps(sp) {
								cover[i].Files = append(cover[i].Files, fileSpec)
								if spanSizeUpperBound > 0 {
									cover[i].WorkSize += addSize(cover[i].Span, f.Span)
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
						// If this file extends beyond the end of the last partition of the
						// cover, append a new partition for the uncovered span.
						if covEnd := cover[len(cover)-1].Span.EndKey; sp.EndKey.Compare(covEnd) > 0 {
							cover = append(cover, makeEntry(covEnd, sp.EndKey, fileSpec, f.Span, spanSizeUpperBound))
						}
					}
				} else if span.EndKey.Compare(f.Span.Key) <= 0 {
					// If this file starts after the needed span ends, then all the files
					// remaining do too, so we're done checking files for this span.
					break
				}
			}
		}

		if spanSizeUpperBound > 0 {
			// After processing a requiredSpan, dump as many restoreSpanEntry's into
			// cover[fillingSpanIdx] until a dump would overfill
			// cover[fillingSpanIdx]. As an example, let the spanSizeUpperBound = 5, and imagine
			// the first requiredSpan created these two restoreSpanEntries:

			// span: datasize
			// [a,c): 2
			// [c,e): 1
			//
			// [c,e) would dump into [a-c), resulting in the following cover:
			// [a,c): 3
			//
			// Next, the following restoreSpan entries were created:
			//  [e, f): 1
			//  [f, i): 4
			//  [l, m): 2
			//
			// [e,f) would dump into the existing [a,c), [f,i) could not dump into
			// [a,f) because  4+4<5. Further, [l,m) could not dump into [f,i) becasue 4+2>6.
			// The resulting cover is:
			// [a,c): 4
			// [f,i): 4
			// [l,m): 2

			// The next set of spans will try to fill [l,m).

			for dumpIdx := fillingSpanIdx; dumpIdx < len(cover); dumpIdx++ {

				if cover[fillingSpanIdx].WorkSize+cover[dumpIdx].WorkSize > spanSizeUpperBound {
					fillingSpanIdx++
					// Once cover[fillingSpanIdx] fills up, the next span to fill is cover[dumpIdx],
					// as it couldn't get dumped in to cover[fillingSpanIdx].
					// The spans between cover[fillingSpanIdx] and cover[dumpIdx] have already been dumped
					//into cover[fillingSpanIdx]; therefore, they can be overwritten.
					cover[fillingSpanIdx] = cover[dumpIdx]
				} else {
					combineSpans(&cover[fillingSpanIdx], &cover[dumpIdx])
				}
			}
			// Once we've attempted to combine all spans in the cover,
			// remove all stale spans above the current filling span.
			cover = cover[:fillingSpanIdx+1]
		}
	}

	return cover
}

func makeEntry(
	start, end roachpb.Key,
	f execinfrapb.RestoreFileSpec,
	fileSpan roachpb.Span,
	spanUpperBound int64,
) execinfrapb.RestoreSpanEntry {

	spanEntry := execinfrapb.RestoreSpanEntry{
		Span: roachpb.Span{Key: start, EndKey: end}, Files: []execinfrapb.RestoreFileSpec{f},
	}
	if spanUpperBound > 0 {
		spanEntry.WorkSize = addSize(spanEntry.Span, fileSpan)
	}
	return spanEntry
}

// addSize estimates the logical amount of data in a file that covers a given span.
// The function assumes that the file's keys are uniformly distributed.
func addSize(subsetSpan roachpb.Span, fileSpan roachpb.Span) int64 {
	subsetSpan = subsetSpan.Intersect(fileSpan)
	contribution := float64(subsetSpan.Size()) / float64(fileSpan.Size())
	return int64(contribution * float64(fileSpan.Size()))
}

func combineSpans(
	fillingSpan *execinfrapb.RestoreSpanEntry, dumpSpan *execinfrapb.RestoreSpanEntry,
) {
	fillingSpan.Span = fillingSpan.Span.Combine(dumpSpan.Span)
	fillingSpan.WorkSize += dumpSpan.WorkSize

	fillingFileMap := make(map[string]struct{}, len(fillingSpan.Files))
	for i := 0; i < len(fillingSpan.Files); i++ {
		fillingFileMap[fillingSpan.Files[i].Path] = struct{}{}
	}

	for i := 0; i < len(dumpSpan.Files); i++ {
		if _, ok := fillingFileMap[dumpSpan.Files[i].Path]; !ok {
			fillingSpan.Files = append(fillingSpan.Files, dumpSpan.Files[i])
		}
	}
}
