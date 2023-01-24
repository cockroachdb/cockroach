// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"container/heap"
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	spanUtils "github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
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

// backupManifestFileIterator exposes methods that can be used to iterate over
// the `BackupManifest_Files` field of a manifest.
type backupManifestFileIterator interface {
	next() (backuppb.BackupManifest_File, bool)
	peek() (backuppb.BackupManifest_File, bool)
	err() error
	close()
	reset()
}

// inMemoryFileIterator iterates over the `BackupManifest_Files` field stored
// in-memory in the manifest.
type inMemoryFileIterator struct {
	manifest *backuppb.BackupManifest
	curIdx   int
}

func (i *inMemoryFileIterator) next() (backuppb.BackupManifest_File, bool) {
	f, hasNext := i.peek()
	i.curIdx++
	return f, hasNext
}

func (i *inMemoryFileIterator) peek() (backuppb.BackupManifest_File, bool) {
	if i.curIdx >= len(i.manifest.Files) {
		return backuppb.BackupManifest_File{}, false
	}
	f := i.manifest.Files[i.curIdx]
	return f, true
}

func (i *inMemoryFileIterator) err() error {
	return nil
}

func (i *inMemoryFileIterator) close() {}

func (i *inMemoryFileIterator) reset() {
	i.curIdx = 0
}

var _ backupManifestFileIterator = &inMemoryFileIterator{}

// makeBackupManifestFileIterator returns a backupManifestFileIterator that can
// be used to iterate over the `BackupManifest_Files` of the manifest.
func makeBackupManifestFileIterator(
	ctx context.Context,
	storeFactory cloud.ExternalStorageFactory,
	m backuppb.BackupManifest,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) (backupManifestFileIterator, error) {
	if m.HasExternalFilesList {
		es, err := storeFactory(ctx, m.Dir)
		if err != nil {
			return nil, err
		}
		storeFile := storageccl.StoreFile{
			Store:    es,
			FilePath: backupinfo.BackupMetadataFilesListPath,
		}
		var encOpts *roachpb.FileEncryptionOptions
		if encryption != nil {
			key, err := backupencryption.GetEncryptionKey(ctx, encryption, kmsEnv)
			if err != nil {
				return nil, err
			}
			encOpts = &roachpb.FileEncryptionOptions{Key: key}
		}
		it, err := backupinfo.NewFileSSTIter(ctx, storeFile, encOpts)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create new FileSST iterator")
		}
		return &sstFileIterator{fi: it}, nil
	}

	return &inMemoryFileIterator{
		manifest: &m,
		curIdx:   0,
	}, nil
}

// sstFileIterator uses an underlying `backupinfo.FileIterator` to read the
// `BackupManifest_Files` from the SST file.
type sstFileIterator struct {
	fi *backupinfo.FileIterator
}

func (s *sstFileIterator) next() (backuppb.BackupManifest_File, bool) {
	f, ok := s.peek()
	if ok {
		s.fi.Next()
	}

	return f, ok
}

func (s *sstFileIterator) peek() (backuppb.BackupManifest_File, bool) {
	if ok, _ := s.fi.Valid(); !ok {
		return backuppb.BackupManifest_File{}, false
	}
	return *s.fi.Value(), true
}

func (s *sstFileIterator) err() error {
	_, err := s.fi.Valid()
	return err
}

func (s *sstFileIterator) close() {
	s.fi.Close()
}

func (s *sstFileIterator) reset() {
	s.fi.Reset()
}

var _ backupManifestFileIterator = &sstFileIterator{}

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
	requiredSpans roachpb.Spans,
	backups []backuppb.BackupManifest,
	layerToBackupManifestFileIterFactory layerToBackupManifestFileIterFactory,
	backupLocalityMap map[int]storeByLocalityKV,
	introducedSpanFrontier *spanUtils.Frontier,
	lowWaterMark roachpb.Key,
	targetSize int64,
) ([]execinfrapb.RestoreSpanEntry, error) {
	if len(backups) < 1 {
		return nil, nil
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

			// If the manifest for this backup layer is a `BACKUP_METADATA` then
			// we reach out to ExternalStorage to read the accompanying SST that
			// contains the BackupManifest_Files.
			iterFactory := layerToBackupManifestFileIterFactory[layer]
			it, err := iterFactory()
			if err != nil {
				return nil, err
			}
			defer it.close()

			covPos := spanCoverStart

			// lastCovSpanSize is the size of files added to the right-most span of
			// the cover so far.
			var lastCovSpanSize int64
			for f, hasNext := it.next(); hasNext; f, hasNext = it.next() {
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
			if err := it.err(); err != nil {
				return nil, err
			}
		}
	}

	return cover, nil
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

func makeEntry(start, end roachpb.Key, f execinfrapb.RestoreFileSpec) execinfrapb.RestoreSpanEntry {
	return execinfrapb.RestoreSpanEntry{
		Span:  roachpb.Span{Key: start, EndKey: end},
		Files: []execinfrapb.RestoreFileSpec{f},
	}
}

type layerToBackupManifestFileIterFactory map[int]func() (backupManifestFileIterator, error)

// getBackupManifestFileIters constructs a mapping from the idx of the backup
// layer to a factory method to construct a backupManifestFileIterator. This
// iterator can be used to iterate over the `BackupManifest_Files` in a
// `BackupManifest`. It is the callers responsibility to close the returned
// iterators.
func getBackupManifestFileIters(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	backupManifests []backuppb.BackupManifest,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) (map[int]func() (backupManifestFileIterator, error), error) {
	layerToFileIterFactory := make(map[int]func() (backupManifestFileIterator, error))
	for layer := range backupManifests {
		layer := layer
		layerToFileIterFactory[layer] = func() (backupManifestFileIterator, error) {
			manifest := backupManifests[layer]
			return makeBackupManifestFileIterator(ctx, execCfg.DistSQLSrv.ExternalStorage, manifest, encryption, kmsEnv)
		}
	}

	return layerToFileIterFactory, nil
}

// generateAndSendImportSpans partitions the spans of requiredSpans into a
// covering of RestoreSpanEntry's which each have all overlapping files from the
// passed backups assigned to them. The spans of requiredSpans are
// trimmed/removed based on the lowWaterMark before the covering for them is
// generated. These spans are generated one at a time and then sent to spanCh.
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
//	[a, b): 1, 6
//	[b, c): 1, 4, 6
//	[c, f): 2, 4, 6
//	[f, g): 6
//	[g, h): 5, 6
//	[h, i): 3, 5, 8
//	[l, m): 9
//
// This cover is created by iterating through the start and end keys of all the
// files in the backup in key order via fileSpanStartAndEndKeyIterator. The
// cover spans are initially just the spans between each pair of adjacent keys
// yielded by the iterator. We then iterate through each cover span and find all
// the overlapping files. If the files that overlap a cover span is a subset of
// the files that overlap the span before it, then the two spans are merged into
// a single span in the final cover. Additionally, if targetSize > 0, we can
// merge the current cover span with the previous cover span if the merged set
// of files have a total data size below the target size.
//
// The above example is tested in TestRestoreEntryCoverExample.
//
// If useSimpleImportSpans is true, the above covering method is not used and
// the covering is created by makeSimpleImportSpans instead.
func generateAndSendImportSpans(
	ctx context.Context,
	requiredSpans roachpb.Spans,
	backups []backuppb.BackupManifest,
	layerToBackupManifestFileIterFactory layerToBackupManifestFileIterFactory,
	backupLocalityMap map[int]storeByLocalityKV,
	introducedSpanFrontier *spanUtils.Frontier,
	lowWaterMark roachpb.Key,
	targetSize int64,
	spanCh chan execinfrapb.RestoreSpanEntry,
	useSimpleImportSpans bool,
) error {
	if useSimpleImportSpans {
		importSpans, err := makeSimpleImportSpans(requiredSpans, backups, layerToBackupManifestFileIterFactory, backupLocalityMap, introducedSpanFrontier, lowWaterMark, targetSize)
		if err != nil {
			return err
		}

		for _, sp := range importSpans {
			spanCh <- sp
		}
		return nil
	}

	startEndKeyIt, err := newFileSpanStartAndEndKeyIterator(backups, layerToBackupManifestFileIterFactory)
	if err != nil {
		return err
	}

	fileIterByLayer := make([]backupManifestFileIterator, 0, len(backups))
	for layer := range backups {
		iter, err := layerToBackupManifestFileIterFactory[layer]()
		if err != nil {
			return err
		}

		fileIterByLayer = append(fileIterByLayer, iter)
	}

	// lastCovSpanSize is the size of files added to the right-most span of
	// the cover so far.
	var lastCovSpanSize int64
	var lastCovSpan roachpb.Span
	var covFilesByLayer [][]backuppb.BackupManifest_File
	var firstInSpan bool

	flush := func() {
		entry := execinfrapb.RestoreSpanEntry{
			Span: lastCovSpan,
		}

		for layer := range covFilesByLayer {
			for _, f := range covFilesByLayer[layer] {
				fileSpec := execinfrapb.RestoreFileSpec{Path: f.Path, Dir: backups[layer].Dir}
				if dir, ok := backupLocalityMap[layer][f.LocalityKV]; ok {
					fileSpec = execinfrapb.RestoreFileSpec{Path: f.Path, Dir: dir}
				}
				entry.Files = append(entry.Files, fileSpec)
			}
		}

		if len(entry.Files) > 0 {
			spanCh <- entry
		}
	}

	for _, span := range requiredSpans {
		firstInSpan = true
		if span.EndKey.Compare(lowWaterMark) < 0 {
			continue
		}
		if span.Key.Compare(lowWaterMark) < 0 {
			span.Key = lowWaterMark
		}

		layersCoveredLater := make(map[int]bool)
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
				layersCoveredLater[layer] = true
			}
		}

		for {
			if ok, err := startEndKeyIt.valid(); !ok {
				if err != nil {
					return err
				}
				break
			}

			key := startEndKeyIt.value()
			if span.Key.Compare(key) >= 0 {
				startEndKeyIt.next()
				continue
			}

			var coverSpan roachpb.Span
			if firstInSpan {
				coverSpan.Key = span.Key
			} else {
				coverSpan.Key = lastCovSpan.EndKey
			}

			if span.ContainsKey(key) {
				coverSpan.EndKey = startEndKeyIt.value()
			} else {
				coverSpan.EndKey = span.EndKey
			}

			newFilesByLayer, err := getNewIntersectingFilesByLayer(coverSpan, layersCoveredLater, fileIterByLayer)
			if err != nil {
				return err
			}

			var filesByLayer [][]backuppb.BackupManifest_File
			var covSize int64
			var newCovFilesSize int64

			for layer := range newFilesByLayer {
				for _, file := range newFilesByLayer[layer] {
					sz := file.EntryCounts.DataSize
					if sz == 0 {
						sz = 16 << 20
					}
					newCovFilesSize += sz
				}
				filesByLayer = append(filesByLayer, newFilesByLayer[layer])
			}

			for layer := range covFilesByLayer {
				for _, file := range covFilesByLayer[layer] {
					sz := file.EntryCounts.DataSize
					if sz == 0 {
						sz = 16 << 20
					}

					if coverSpan.Overlaps(file.Span) {
						covSize += sz
						filesByLayer[layer] = append(filesByLayer[layer], file)
					}
				}
			}

			if covFilesByLayer == nil {
				covFilesByLayer = newFilesByLayer
				lastCovSpan = coverSpan
				lastCovSpanSize = newCovFilesSize
			} else {
				if (newCovFilesSize == 0 || lastCovSpanSize+newCovFilesSize <= targetSize) && !firstInSpan {
					// If there are no new files that cover this span or if we can add the
					// files in the new span's cover to the last span's cover and still stay
					// below targetSize, then we should merge the two spans.
					for layer := range newFilesByLayer {
						covFilesByLayer[layer] = append(covFilesByLayer[layer], newFilesByLayer[layer]...)
					}
					lastCovSpan.EndKey = coverSpan.EndKey
					lastCovSpanSize = lastCovSpanSize + newCovFilesSize
				} else {
					flush()
					lastCovSpan = coverSpan
					covFilesByLayer = filesByLayer
					lastCovSpanSize = covSize
				}
			}
			firstInSpan = false

			if lastCovSpan.EndKey.Equal(span.EndKey) {
				break
			}

			startEndKeyIt.next()
		}
	}

	flush()
	return nil
}

// fileSpanStartAndEndKeyIterator yields (almost) all of the start and end keys
// of the spans from the files in a backup chain in key order. A start or end
// key from a file span will be yielded by the iterator if the key is not
// covered by another file span within the same layer before it in FileCmp
// order. In particular, this means that if all layers in a backup chain have
// files with non-overlapping spans, then this iterator would return all start
// and end keys for all file spans in order. For example:
//
//	backup
//	0|     a___1___c c__2__e          h__3__i
//	1|         b___4___d           g____5___i
//	2|     a___________6______________h         j_7_k
//	3|                                  h_8_i              l_9_m
//	 keys--a---b---c---d---e---f---g---h----i---j---k---l----m------p---->
//
// In this case, since no file span overlaps with another file span within the same layer,
// the iterator will yield all start and end keys:
// [a, b, c, d, e, g, h, i, j, k, l, m]
//
// Another example, but with file spans that do overlap within a layer:
//
//	backup
//	0|     a___1___c
//	1|         b_____2_____e
//	 |                 d___3___f
//	2|     a___________4___________g
//	3|
//	 keys--a---b---c---d---e---f---g--->
//
// In this case, there is overlap between files 2 and 3 within layer 1. Since
// the start and end keys 'b' and 'e' of file 2 will be yielded by the iterator
// since there are no files before it within the same layer. Start key 'd' of
// file 3 will not be yielded since it's covered by 2's span. The end key 'f'
// will still be yielded since it's not covered by 2's span. So the iterator
// will yield:
// [a, b, c, e, f, g]
type fileSpanStartAndEndKeyIterator struct {
	heap     *fileHeap
	allIters []backupManifestFileIterator
	err      error
}

func newFileSpanStartAndEndKeyIterator(
	backups []backuppb.BackupManifest, layerToIterFactory layerToBackupManifestFileIterFactory,
) (*fileSpanStartAndEndKeyIterator, error) {
	it := &fileSpanStartAndEndKeyIterator{}
	for layer := range backups {
		iter, err := layerToIterFactory[layer]()
		if err != nil {
			return nil, err
		}

		it.allIters = append(it.allIters, iter)
	}
	it.reset()
	return it, nil
}

func (i *fileSpanStartAndEndKeyIterator) next() {
	if ok, _ := i.valid(); !ok {
		return
	}

	prevKey := i.value()
	for i.heap.Len() > 0 {
		minItem := heap.Pop(i.heap).(fileHeapItem)

		curKey := minItem.key()
		if curKey.Compare(prevKey) > 0 {
			heap.Push(i.heap, minItem)
			break
		}

		if minItem.cmpEndKey {
			file, ok := minItem.fileIter.next()
			if err := minItem.fileIter.err(); err != nil {
				i.err = err
				return
			}
			if ok {
				minItem.cmpEndKey = false
				minItem.file = file
				heap.Push(i.heap, minItem)
			}
		} else {
			minItem.cmpEndKey = true
			heap.Push(i.heap, minItem)
		}
	}
}

func (i *fileSpanStartAndEndKeyIterator) valid() (bool, error) {
	if i.err != nil {
		return false, i.err
	}
	return i.heap.Len() > 0, nil
}

func (i *fileSpanStartAndEndKeyIterator) value() roachpb.Key {
	if ok, _ := i.valid(); !ok {
		return nil
	}

	return i.heap.fileHeapItems[0].key()
}
func (i *fileSpanStartAndEndKeyIterator) reset() {
	i.heap = &fileHeap{}
	i.err = nil

	for _, iter := range i.allIters {
		iter.reset()

		file, ok := iter.next()
		if err := iter.err(); err != nil {
			i.err = err
			return
		}
		if ok {
			i.heap.fileHeapItems = append(i.heap.fileHeapItems, fileHeapItem{
				fileIter:  iter,
				file:      file,
				cmpEndKey: false,
			})
		}
	}
	heap.Init(i.heap)
}

type fileHeapItem struct {
	fileIter  backupManifestFileIterator
	file      backuppb.BackupManifest_File
	cmpEndKey bool
}

func (f fileHeapItem) key() roachpb.Key {
	if f.cmpEndKey {
		return f.file.Span.EndKey
	}
	return f.file.Span.Key
}

type fileHeap struct {
	fileHeapItems []fileHeapItem
}

func (f *fileHeap) Len() int {
	return len(f.fileHeapItems)
}

func (f *fileHeap) Less(i, j int) bool {
	return f.fileHeapItems[i].key().Compare(f.fileHeapItems[j].key()) < 0
}

func (f *fileHeap) Swap(i, j int) {
	f.fileHeapItems[i], f.fileHeapItems[j] = f.fileHeapItems[j], f.fileHeapItems[i]
}

func (f *fileHeap) Push(x any) {
	item, ok := x.(fileHeapItem)
	if !ok {
		panic("pushed value not fileHeapItem")
	}

	f.fileHeapItems = append(f.fileHeapItems, item)
}

func (f *fileHeap) Pop() any {
	old := f.fileHeapItems
	n := len(old)
	item := old[n-1]
	f.fileHeapItems = old[0 : n-1]
	return item
}

func getNewIntersectingFilesByLayer(
	span roachpb.Span, layersCoveredLater map[int]bool, fileIters []backupManifestFileIterator,
) ([][]backuppb.BackupManifest_File, error) {
	var files [][]backuppb.BackupManifest_File

	for l, iter := range fileIters {
		var layerFiles []backuppb.BackupManifest_File
		if !layersCoveredLater[l] {
			for ; ; iter.next() {
				f, ok := iter.peek()
				if !ok {
					break
				}

				if span.Overlaps(f.Span) {
					layerFiles = append(layerFiles, f)
				}

				if span.EndKey.Compare(f.Span.Key) <= 0 {
					break
				}
			}
			if iter.err() != nil {
				return nil, iter.err()
			}
		}
		files = append(files, layerFiles)
	}

	return files, nil
}
