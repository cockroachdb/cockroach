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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	spanUtils "github.com/cockroachdb/cockroach/pkg/util/span"
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
var targetRestoreSpanSize = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
	"backup.restore_span.target_size",
	"target size to which base spans of a restore are merged to produce a restore span (0 disables)",
	384<<20,
)

// makeSimpleImportSpans partitions the spans of requiredSpans into a covering
// of RestoreSpanEntry's which each have all overlapping files from the passed
// backups assigned to them. The spans of requiredSpans are trimmed/removed
// based on the lowWaterMark before the covering for them is generated.
//
// Note that because of https://github.com/cockroachdb/cockroach/issues/101963,
// the spans of files are end key _inclusive_. Because the current definition
// of spans are all end key _exclusive_, we work around this by assuming that
// the end key of each file span is actually the next key of the end key.
//
// Consider a chain of backups with files f1, f2… which cover spans as follows:
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
//	[c, e): 1, 2, 4, 6
//	[e, f): 2, 6
//	[f, i): 3, 5, 6, 8
//	[l, p): 9
//
// This example is tested in TestRestoreEntryCoverExample.
//
// If targetSize > 0, then spans which would be added to the right-hand side of
// the first level are instead used to extend the current rightmost span in
// if its current data size plus that of the new span is less than the target
// size.
func makeSimpleImportSpans(
	requiredSpans roachpb.Spans,
	backups []backuppb.BackupManifest,
	backupLocalityMap map[int]storeByLocalityKV,
	introducedSpanFrontier *spanUtils.Frontier,
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
		// endKeyNotCoveredFiles is a collection of files that, due to the end key
		// inclusive nature of their spans, do not have their end key covered by
		// the current cover. These are kept around so that they can be included
		// in the next entry's file list whenever the cover is extended. This
		// collection is populated from files in two cases:
		//
		//  1. A file has an end key equal to the end key of the last cover
		//     entry's span. This means that we are still creating the cover and next
		//     cover entry or an extension of the current cover should cover the end
		//     key of this file. If we are done with creating the cover of a required
		//     span, then a last step of extending the last cover span to the end key
		//     of the required span should also cover this file. The most common
		//     example of this case is when a file span causes a new cover entry
		//     to be added. For example, if the current cover is {[a, b), [b, d)},
		//     and we encounter a file with span [c, e], this will create a new
		//     cover with span [d, e). However, the file that created the cover
		//     span will not have its end key "e" covered yet, and thus must be added
		//     to endKeyNotCoveredFiles. If we next encounter a file with span [d, e],
		//     this file will not create a new cover entry as its end key does
		//     not extend beyond the cover, but the file will also be added to
		//     endKeyNotCoveredFiles as its end key "e" is equal to the end key of
		//     the final cover span and thus not covered.
		//
		//  2. A file has an end key equal to the start key of the current
		//     (filtered) required span. This means that we've just begun processing
		//     this span and this file should be covered as soon as we start creating
		//     the cover for this span.
		var endKeyNotCoveredFiles restoreFileSpecs

		spanCoverStart := len(cover)
		for layer := range backups {
			var coveredLater bool
			introducedSpanFrontier.SpanEntries(span, func(s roachpb.Span,
				ts hlc.Timestamp) (done spanUtils.OpResult) {
				if backups[layer].EndTime.Less(ts) {
					coveredLater = true
				}
				return spanUtils.StopMatch
			})
			if coveredLater {
				// Don't use this backup to cover this span if the span was reintroduced
				// after the backup's endTime. In this case, this backup may have
				// invalid data, and further, a subsequent backup will contain all of
				// this span's data. Consider the following example:
				//
				// T0: Begin IMPORT INTO on existing table foo, ingest some data
				// T1: Backup foo
				// T2: Rollback IMPORT via clearRange
				// T3: Incremental backup of foo, with a full reintroduction of foo’s span
				// T4: RESTORE foo: should only restore foo from the incremental backup.
				//    If data from the full backup were also restored,
				//    the imported-but-then-clearRanged data will leak in the restored cluster.
				//    This logic seeks to avoid this form of data corruption.
				continue
			}
			covPos := spanCoverStart

			// lastCovSpanSize is the size of files added to the right-most span of
			// the cover so far.
			var lastCovSpanSize int64

			// TODO(dt): binary search to the first file in required span?
			for _, f := range backups[layer].Files {
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

				if intersectingFileSpan, valid := getIntersectingFileSpan(span, f.Span); valid {
					if len(cover) == spanCoverStart {
						if intersectingFileSpan.EndKey.Compare(span.Key) > 0 {
							// If we can make a first cover span with the end key, do so.
							entry := makeEntry(span.Key, intersectingFileSpan.EndKey)
							lastCovSpanSize = 0
							lastCovSpanSize += endKeyNotCoveredFiles.drain(&entry)

							entry.Files = append(entry.Files, fileSpec)
							lastCovSpanSize += sz

							cover = append(cover, entry)
						} else {
							// Otherwise this is case 2 above where the file intersects only
							// the start key of the span, so we add the file to
							// endKeyNotCoveredFiles without creating a cover entry.
							endKeyNotCoveredFiles.add(fileSpec, sz)
						}
					} else {
						// If this file extends beyond the end of the last partition of the
						// cover, either append a new partition for the uncovered span or
						// grow the last one if size allows.
						if covEnd := cover[len(cover)-1].Span.EndKey; intersectingFileSpan.EndKey.Compare(covEnd) > 0 {
							// If adding the item size to the current rightmost span size will
							// exceed the target size, make a new span, otherwise extend the
							// rightmost span to include the item.
							if lastCovSpanSize+sz > targetSize {
								entry := makeEntry(covEnd, intersectingFileSpan.EndKey)
								lastCovSpanSize = 0
								lastCovSpanSize += endKeyNotCoveredFiles.drain(&entry)

								entry.Files = append(entry.Files, fileSpec)
								lastCovSpanSize += sz

								cover = append(cover, entry)
							} else {
								cover[len(cover)-1].Span.EndKey = intersectingFileSpan.EndKey
								cover[len(cover)-1].Files = append(cover[len(cover)-1].Files, fileSpec)
								lastCovSpanSize += sz

								// Drain endKeyNotCoveredFiles if we extended the last cover span, as
								// their end keys should be covered by any extension.
								lastCovSpanSize += endKeyNotCoveredFiles.drain(&cover[len(cover)-1])
							}
						}
						// Now ensure the file is included in any partition in the existing
						// cover which overlaps.
						for i := covPos; i < len(cover) && cover[i].Span.Key.Compare(intersectingFileSpan.EndKey) <= 0; i++ {
							// If file overlaps, it needs to be in this partition.
							if inclusiveOverlap(cover[i].Span, f.Span) {
								// If this is the last partition, we might have added it above.
								if i == len(cover)-1 {
									if last := len(cover[i].Files) - 1; last < 0 || cover[i].Files[last].Path != fileSpec.Path {
										cover[i].Files = append(cover[i].Files, fileSpec)
										lastCovSpanSize += sz
									}
								} else {
									// If it isn't the last partition, we always need to add it.
									cover[i].Files = append(cover[i].Files, fileSpec)
									lastCovSpanSize += sz
								}
							}

							// If partition i is not the final partition of the cover and if
							// it ends before this file starts, we know it also ends before
							// any remaining files start too, as the files are sorted above
							// by start key, so remaining files can start their search after
							// this partition. If partition i is the final partition of the
							// cover, then it can still be extended by the next file, so we
							// can't skip it.
							if i < len(cover)-1 && cover[i].Span.EndKey.Compare(intersectingFileSpan.Key) <= 0 {
								covPos = i + 1
							}
						}
					}
					// Add file to endKeyNotCoveredFiles if the file span's end key is
					// the same as the last cover entry's span end key, as the end key
					// is currently not covered by any entry, but will be covered by the
					// next.
					if len(cover) == 0 || intersectingFileSpan.EndKey.Compare(cover[len(cover)-1].Span.EndKey) == 0 {
						endKeyNotCoveredFiles.add(fileSpec, sz)
					}
				} else if span.EndKey.Compare(f.Span.Key) <= 0 {
					// If this file starts after the needed span ends, then all the files
					// remaining do too so we're done checking files for this span.
					break
				}
			}
		}

		// If we have some files in endKeyNotCoveredFiles and there are some cover
		// entries for this required span, we can simply extend the end key of the
		// last cover span so it covers the end keys of these files as well. If
		// there is no cover entry for this span, then we create a new cover entry
		// for the entire span and add these files.
		if !endKeyNotCoveredFiles.empty() {
			if len(cover) != spanCoverStart {
				cover[len(cover)-1].Span.EndKey = span.EndKey
				endKeyNotCoveredFiles.drain(&cover[len(cover)-1])
			} else {
				entry := makeEntry(span.Key, span.EndKey)
				endKeyNotCoveredFiles.drain(&entry)
				cover = append(cover, entry)
			}
		}
	}

	return cover
}

// createIntroducedSpanFrontier creates a span frontier that tracks the end time
// of the latest incremental backup of each introduced span in the backup chain.
// See ReintroducedSpans( ) for more information. Note: this function assumes
// that manifests are sorted in increasing EndTime.
func createIntroducedSpanFrontier(
	manifests []backuppb.BackupManifest, asOf hlc.Timestamp,
) (*spanUtils.Frontier, error) {
	introducedSpanFrontier, err := spanUtils.MakeFrontier(roachpb.Span{})
	if err != nil {
		return nil, err
	}
	for i, m := range manifests {
		if i == 0 {
			continue
		}
		if !asOf.IsEmpty() && asOf.Less(m.StartTime) {
			break
		}
		if err := introducedSpanFrontier.AddSpansAt(m.EndTime, m.IntroducedSpans...); err != nil {
			return nil, err
		}
	}
	return introducedSpanFrontier, nil
}

func makeEntry(start, end roachpb.Key) execinfrapb.RestoreSpanEntry {
	return execinfrapb.RestoreSpanEntry{
		Span: roachpb.Span{Key: start, EndKey: end},
	}
}

// inclusiveOverlap returns true if sp, which is end key exclusive, overlaps
// isp, which is end key inclusive.
func inclusiveOverlap(sp roachpb.Span, isp roachpb.Span) bool {
	return sp.Overlaps(isp) || sp.ContainsKey(isp.EndKey)
}

// getIntersectingFileSpan returns the intersection of sp, an end key exclusive
// span, and ifsp, and end key inclusive file span. If a valid intersection
// exists, then the function will return the intersection and true, otherwise it
// will return an empty span and false. Note that the intersection span should
// be used as an end key inclusive file span. It could have its start key equal
// to its end key if the intersection is a point.
func getIntersectingFileSpan(sp roachpb.Span, ifsp roachpb.Span) (roachpb.Span, bool) {
	if !inclusiveOverlap(sp, ifsp) {
		return roachpb.Span{}, false
	}

	if intersect := sp.Intersect(ifsp); intersect.Valid() {
		// If there's a non-zero sized intersection, use that.
		return intersect, true
	}

	// Otherwise, the inclusive overlap must be due to a point intersection
	// between the end key of ifsp and the start key of sp. Just return a zero
	// sized span with the same start and end key in this case.
	return roachpb.Span{Key: ifsp.EndKey, EndKey: ifsp.EndKey}, true
}

// restoreFileSpecs wraps a slice of execinfrapb.RestoreFileSpec and keeps track
// of the sizes of all of the files.
type restoreFileSpecs struct {
	files []execinfrapb.RestoreFileSpec
	sizes []int64
}

// empty returns true if there are no files.
func (rf *restoreFileSpecs) empty() bool {
	return len(rf.files) == 0
}

// add adds an entry to files, and adds its size to the total file size.
func (rf *restoreFileSpecs) add(f execinfrapb.RestoreFileSpec, sz int64) {
	rf.files = append(rf.files, f)
	rf.sizes = append(rf.sizes, sz)
}

// drain drains all files into the Files slice in entry and returns the total
// size of the new files that were added.
func (rf *restoreFileSpecs) drain(entry *execinfrapb.RestoreSpanEntry) (sz int64) {
	for i, f := range rf.files {
		found := false
		for i := range entry.Files {
			if entry.Files[i].Path == f.Path {
				found = true
				break
			}
		}

		if !found {
			entry.Files = append(entry.Files, f)
			sz += rf.sizes[i]
		}
	}

	rf.files = rf.files[:0]
	rf.sizes = rf.sizes[:0]

	return sz
}
