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
	if m.IsSlimManifest {
		es, err := storeFactory(ctx, m.Dir)
		if err != nil {
			return nil, err
		}
		storeFile := storageccl.StoreFile{
			Store:    es,
			FilePath: backupinfo.SlimManifestFileInfoPath,
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

// makeStreamingImportSpans partitions the spans of requiredSpans into a covering
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
//	[a, b): 1, 6
//	[b, c): 1, 4, 6
//	[c, f): 2, 4, 6
//	[f, g): 6
//	[g, h): 5, 6
//	[h, i): 3, 5, 8
//	[l, m): 9
//
// This example is tested in TestRestoreEntryCoverExample.
//
// If targetSize > 0, then spans which would be added to the right-hand side of
// the first level are instead used to extend the current rightmost span in
// if its current data size plus that of the new span is less than the target
// size.
func makeStreamingImportSpans(
	ctx context.Context,
	requiredSpans roachpb.Spans,
	backups []backuppb.BackupManifest,
	layerToBackupManifestFileIterFactory layerToBackupManifestFileIterFactory,
	backupLocalityMap map[int]storeByLocalityKV,
	introducedSpanFrontier *spanUtils.Frontier,
	lowWaterMark roachpb.Key,
	targetSize int64,
	spanCh chan execinfrapb.RestoreSpanEntry,
) error {
	interestingKeyIt, err := newManifestInterestingPointIterator(backups, layerToBackupManifestFileIterFactory)
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
			if ok, err := interestingKeyIt.valid(); !ok {
				if err != nil {
					return err
				}
				break
			}

			interestingKey := interestingKeyIt.value()
			if span.Key.Compare(interestingKey) >= 0 {
				interestingKeyIt.next()
				continue
			}

			var coverSpan roachpb.Span
			if firstInSpan {
				coverSpan.Key = span.Key
			} else {
				coverSpan.Key = lastCovSpan.EndKey
			}

			if span.ContainsKey(interestingKey) {
				coverSpan.EndKey = interestingKeyIt.value()
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

			interestingKeyIt.next()
		}
	}

	flush()
	return nil
}

// manifestInterestingPointIterator yields (almost) all of the start and end
// keys of the spans from the files in a backup chain in key order. This is done
// by iterating through all layers' files in FileCmp order while keeping the
// start and end keys of the current file of each layer in a min heap. If all
// layers in a backup chain have files with non-overlapping spans, then this
// iterator would return all start and end keys. If file span overlaps exist
// within a layer, then keys for some files may be skipped depending on how the
// file overlaps with the layer's current file in the heap.
type manifestInterestingPointIterator struct {
	backups  []*backupinfo.BackupMetadata
	heap     *fileHeap
	allIters []backupManifestFileIterator
	err      error
}

func newManifestInterestingPointIterator(
	backups []backuppb.BackupManifest, layerToIterFactory layerToBackupManifestFileIterFactory,
) (*manifestInterestingPointIterator, error) {
	m := &manifestInterestingPointIterator{}
	for layer := range backups {
		iter, err := layerToIterFactory[layer]()
		if err != nil {
			return nil, err
		}

		m.allIters = append(m.allIters, iter)
	}
	m.reset()
	return m, nil
}

func (m *manifestInterestingPointIterator) next() {
	if ok, _ := m.valid(); !ok {
		return
	}

	prevKey := m.value()
	for m.heap.Len() > 0 {
		minItem := heap.Pop(m.heap).(fileHeapItem)

		curKey := minItem.key()
		if curKey.Compare(prevKey) > 0 {
			heap.Push(m.heap, minItem)
			break
		}

		if minItem.cmpEndKey {
			file, ok := minItem.fileIter.next()
			if err := minItem.fileIter.err(); err != nil {
				m.err = err
				return
			}
			if ok {
				minItem.cmpEndKey = false
				minItem.file = file
				heap.Push(m.heap, minItem)
			}
		} else {
			minItem.cmpEndKey = true
			heap.Push(m.heap, minItem)
		}
	}
}

func (m *manifestInterestingPointIterator) valid() (bool, error) {
	if m.err != nil {
		return false, m.err
	}
	return m.heap.Len() > 0, nil
}

func (m *manifestInterestingPointIterator) value() roachpb.Key {
	if ok, _ := m.valid(); !ok {
		return nil
	}

	return m.heap.fileHeapItems[0].key()
}
func (m *manifestInterestingPointIterator) reset() {
	m.heap = &fileHeap{}
	m.err = nil

	for _, iter := range m.allIters {
		iter.reset()

		file, ok := iter.next()
		if err := iter.err(); err != nil {
			m.err = err
			return
		}
		if ok {
			m.heap.fileHeapItems = append(m.heap.fileHeapItems, fileHeapItem{
				fileIter:  iter,
				file:      file,
				cmpEndKey: false,
			})
		}
	}
	heap.Init(m.heap)
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
