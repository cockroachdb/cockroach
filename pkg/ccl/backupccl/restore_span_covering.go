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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/errors"
)

type intervalSpan roachpb.Span

var _ interval.Interface = intervalSpan{}

// ID is part of `interval.Interface` but unused in makeImportSpans.
func (ie intervalSpan) ID() uintptr { return 0 }

// Range is part of `interval.Interface`.
func (ie intervalSpan) Range() interval.Range {
	return interval.Range{Start: []byte(ie.Key), End: []byte(ie.EndKey)}
}

type importEntryType int

const (
	backupSpan importEntryType = iota
	backupFile
	tableSpan
	completedSpan
)

type importEntry struct {
	roachpb.Span
	entryType importEntryType

	// Only set if entryType is backupSpan
	start, end hlc.Timestamp

	// Only set if entryType is backupFile
	dir  roachpb.ExternalStorage
	file BackupManifest_File
}

// makeImportSpans pivots the backups, which are grouped by time, into
// spans for import, which are grouped by keyrange.
//
// The core logic of this is in OverlapCoveringMerge, which accepts sets of
// non-overlapping key ranges (aka coverings) each with a payload, and returns
// them aligned with the payloads in the same order as in the input.
//
// Example (input):
// - [A, C) backup t0 to t1 -> /file1
// - [C, D) backup t0 to t1 -> /file2
// - [A, B) backup t1 to t2 -> /file3
// - [B, C) backup t1 to t2 -> /file4
// - [C, D) backup t1 to t2 -> /file5
// - [B, D) requested table data to be restored
//
// Example (output):
// - [A, B) -> /file1, /file3
// - [B, C) -> /file1, /file4, requested (note that file1 was split into two ranges)
// - [C, D) -> /file2, /file5, requested
//
// This would be turned into two Import spans, one restoring [B, C) out of
// /file1 and /file4, the other restoring [C, D) out of /file2 and /file5.
// Nothing is restored out of /file3 and only part of /file1 is used.
//
// NB: All grouping operates in the pre-rewrite keyspace, meaning the keyranges
// as they were backed up, not as they're being restored.
//
// If a span is not covered, the onMissing function is called with the span and
// time missing to determine what error, if any, should be returned.
func makeImportSpans(
	tableSpans []roachpb.Span,
	backups []BackupManifest,
	backupLocalityMap map[int]storeByLocalityKV,
	lowWaterMark roachpb.Key,
	onMissing func(span covering.Range, start, end hlc.Timestamp) error,
) ([]execinfrapb.RestoreSpanEntry, hlc.Timestamp, error) {
	// Put the covering for the already-completed spans into the
	// OverlapCoveringMerge input first. Payloads are returned in the same order
	// that they appear in the input; putting the completedSpan first means we'll
	// see it first when iterating over the output of OverlapCoveringMerge and
	// avoid doing unnecessary work.
	completedCovering := covering.Covering{
		{
			Start:   []byte(keys.MinKey),
			End:     []byte(lowWaterMark),
			Payload: importEntry{entryType: completedSpan},
		},
	}

	// Put the merged table data covering into the OverlapCoveringMerge input
	// next.
	var tableSpanCovering covering.Covering
	for _, span := range tableSpans {
		tableSpanCovering = append(tableSpanCovering, covering.Range{
			Start: span.Key,
			End:   span.EndKey,
			Payload: importEntry{
				Span:      span,
				entryType: tableSpan,
			},
		})
	}

	backupCoverings := []covering.Covering{completedCovering, tableSpanCovering}

	// Iterate over backups creating two coverings for each. First the spans
	// that were backed up, then the files in the backup. The latter is a subset
	// when some of the keyranges in the former didn't change since the previous
	// backup. These alternate (backup1 spans, backup1 files, backup2 spans,
	// backup2 files) so they will retain that alternation in the output of
	// OverlapCoveringMerge.
	var maxEndTime hlc.Timestamp
	for i, b := range backups {
		if maxEndTime.Less(b.EndTime) {
			maxEndTime = b.EndTime
		}

		var backupNewSpanCovering covering.Covering
		for _, s := range b.IntroducedSpans {
			backupNewSpanCovering = append(backupNewSpanCovering, covering.Range{
				Start:   s.Key,
				End:     s.EndKey,
				Payload: importEntry{Span: s, entryType: backupSpan, start: hlc.Timestamp{}, end: b.StartTime},
			})
		}
		backupCoverings = append(backupCoverings, backupNewSpanCovering)

		var backupSpanCovering covering.Covering
		for _, s := range b.Spans {
			backupSpanCovering = append(backupSpanCovering, covering.Range{
				Start:   s.Key,
				End:     s.EndKey,
				Payload: importEntry{Span: s, entryType: backupSpan, start: b.StartTime, end: b.EndTime},
			})
		}
		backupCoverings = append(backupCoverings, backupSpanCovering)
		var backupFileCovering covering.Covering

		var storesByLocalityKV map[string]roachpb.ExternalStorage
		if storesByLocalityKVMap, ok := backupLocalityMap[i]; ok {
			storesByLocalityKV = storesByLocalityKVMap
		}

		for _, f := range b.Files {
			dir := b.Dir
			if storesByLocalityKV != nil {
				if newDir, ok := storesByLocalityKV[f.LocalityKV]; ok {
					dir = newDir
				}
			}
			backupFileCovering = append(backupFileCovering, covering.Range{
				Start: f.Span.Key,
				End:   f.Span.EndKey,
				Payload: importEntry{
					Span:      f.Span,
					entryType: backupFile,
					dir:       dir,
					file:      f,
				},
			})
		}
		backupCoverings = append(backupCoverings, backupFileCovering)
	}

	// Group ranges covered by backups with ones needed to restore the selected
	// tables. Note that this breaks intervals up as necessary to align them.
	// See the function godoc for details.
	importRanges := covering.OverlapCoveringMerge(backupCoverings)

	// Translate the output of OverlapCoveringMerge into requests.
	var requestEntries []execinfrapb.RestoreSpanEntry
rangeLoop:
	for _, importRange := range importRanges {
		needed := false
		var latestCoveredTime hlc.Timestamp
		var files []execinfrapb.RestoreFileSpec
		payloads := importRange.Payload.([]interface{})
		for _, p := range payloads {
			ie := p.(importEntry)
			switch ie.entryType {
			case completedSpan:
				continue rangeLoop
			case tableSpan:
				needed = true
			case backupSpan:
				// The latest time we've backed up this span may be ahead of the start
				// time of this entry. This is because some spans can be
				// "re-introduced", meaning that they were previously backed up but
				// still appear in introducedSpans. Spans are re-introduced when they
				// were taken OFFLINE (and therefore processed non-transactional writes)
				// and brought back online (PUBLIC). For more information see #62564.
				if latestCoveredTime.Less(ie.start) {
					return nil, hlc.Timestamp{}, errors.Errorf(
						"no backup covers time [%s,%s) for range [%s,%s) or backups listed out of order (mismatched start time)",
						latestCoveredTime, ie.start,
						roachpb.Key(importRange.Start), roachpb.Key(importRange.End))
				}
				if !ie.end.Less(latestCoveredTime) {
					latestCoveredTime = ie.end
				}
			case backupFile:
				if len(ie.file.Path) > 0 {
					files = append(files, execinfrapb.RestoreFileSpec{
						Dir:  ie.dir,
						Path: ie.file.Path,
					})
				}
			}
		}
		if needed {
			if latestCoveredTime != maxEndTime {
				if err := onMissing(importRange, latestCoveredTime, maxEndTime); err != nil {
					return nil, hlc.Timestamp{}, err
				}
			}
			if len(files) == 0 {
				// There may be import entries that refer to no data, and hence
				// no files. These are caused because file spans start at a
				// specific key. E.g. consider the first file backing up data
				// from table 51. It will cover span ‹/Table/51/1/0/0› -
				// ‹/Table/51/1/3273›. When merged with the backup span:
				// ‹/Table/51› - ‹/Table/52›, we get an empty span with no
				// files: ‹/Table/51› - ‹/Table/51/1/0/0›. We should ignore
				// these to avoid thrashing during restore's split and scatter.
				continue
			}
			// If needed is false, we have data backed up that is not necessary
			// for this restore. Skip it.
			requestEntries = append(requestEntries, execinfrapb.RestoreSpanEntry{
				Span:  roachpb.Span{Key: importRange.Start, EndKey: importRange.End},
				Files: files,
			})
		}
	}
	return requestEntries, maxEndTime, nil
}

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
func makeSimpleImportSpans(
	requiredSpans []roachpb.Span,
	backups []BackupManifest,
	backupLocalityMap map[int]storeByLocalityKV,
	lowWaterMark roachpb.Key,
) []execinfrapb.RestoreSpanEntry {
	if len(backups) < 1 {
		return nil
	}

	for i := range backups {
		sort.Sort(BackupFileDescriptors(backups[i].Files))
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
			// TODO(dt): binary search to the first file in required span?
			for _, f := range backups[layer].Files {
				if sp := span.Intersect(f.Span); sp.Valid() {
					fileSpec := execinfrapb.RestoreFileSpec{Path: f.Path, Dir: backups[layer].Dir}
					if dir, ok := backupLocalityMap[layer][f.LocalityKV]; ok {
						fileSpec = execinfrapb.RestoreFileSpec{Path: f.Path, Dir: dir}
					}
					if len(cover) == spanCoverStart {
						cover = append(cover, makeEntry(span.Key, sp.EndKey, fileSpec))
					} else {
						// Add each file to every matching partition in the cover.
						for i := covPos; i < len(cover) && cover[i].Span.Key.Compare(sp.EndKey) < 0; i++ {
							if cover[i].Span.Overlaps(sp) {
								cover[i].Files = append(cover[i].Files, fileSpec)
							}
							// Later files start later, so this cover ends before this file,
							// it ends before them too and they can start searching after it.
							if cover[i].Span.EndKey.Compare(sp.Key) <= 0 {
								covPos = i + 1
							}
						}
						if covEnd := cover[len(cover)-1].Span.EndKey; sp.EndKey.Compare(covEnd) > 0 {
							cover = append(cover, makeEntry(covEnd, sp.EndKey, fileSpec))
						}
					}
				} else if span.EndKey.Compare(f.Span.Key) <= 0 {
					// This file is already after the end, so rest are too.
					break
				}
			}
		}
	}

	return cover
}

func makeEntry(start, end roachpb.Key, f execinfrapb.RestoreFileSpec) execinfrapb.RestoreSpanEntry {
	return execinfrapb.RestoreSpanEntry{
		Span: roachpb.Span{Key: start, EndKey: end}, Files: []execinfrapb.RestoreFileSpec{f},
	}
}
