// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"container/heap"
	"context"

	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	spanUtils "github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
)

type intervalSpan roachpb.Span

var _ interval.Interface = intervalSpan{}

// ID is part of `interval.Interface` but seemed unused by backup usage.
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
	settings.ApplicationLevel,
	"backup.restore_span.target_size",
	"target size to which base spans of a restore are merged to produce a restore span (0 disables)",
	384<<20,
)

var targetOnlineRestoreSpanSize = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"backup.restore_span.online_target_size",
	"target size to which base spans of an online restore are merged to produce a restore span (0 disables)",
	16<<30,
)

var maxFileCount = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"backup.restore_span.max_file_count",
	"the maximum number of backup files an extending restore span may contain",
	defaultMaxFileCount,
	settings.PositiveInt,
)

const defaultMaxFileCount = 200

// backupManifestFileIterator exposes methods that can be used to iterate over
// the `BackupManifest_Files` field of a manifest.
type backupManifestFileIterator interface {
	next() (backuppb.BackupManifest_File, bool)
	peek() (backuppb.BackupManifest_File, bool)
	err() error
	close()
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

var _ backupManifestFileIterator = &sstFileIterator{}

// createIntroducedSpanFrontier creates a span frontier that tracks the end time
// of the latest incremental backup of each introduced span in the backup chain.
// Note: this function assumes that manifests are sorted in increasing EndTime.
func createIntroducedSpanFrontier(
	manifests []backuppb.BackupManifest, asOf hlc.Timestamp,
) (spanUtils.Frontier, error) {
	introducedSpanFrontier, err := spanUtils.MakeFrontier()
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

// spanCoveringFilter holds metadata that filters which backups and required spans are used to
// populate a restoreSpanEntry
type spanCoveringFilter struct {
	checkpointFrontier     spanUtils.Frontier
	introducedSpanFrontier spanUtils.Frontier
	targetSize             int64
	maxFileCount           int
}

func makeSpanCoveringFilter(
	requiredSpans roachpb.Spans,
	checkpointedSpans []jobspb.RestoreProgress_FrontierEntry,
	introducedSpanFrontier spanUtils.Frontier,
	targetSize int64,
	maxFileCount int64,
) (spanCoveringFilter, error) {
	f, err := loadCheckpointFrontier(requiredSpans, checkpointedSpans)
	if err != nil {
		return spanCoveringFilter{}, err
	}
	if maxFileCount == 0 {
		// A 0 valued maxFileCount may get passed in a mixed version cluster:
		// specifically, when the job coordinator is on an older version and the
		// generative split and scatter processor is on a newer version. In this
		// case, ensure the maxFileCount is set to default.
		maxFileCount = defaultMaxFileCount
	}
	sh := spanCoveringFilter{
		introducedSpanFrontier: introducedSpanFrontier,
		targetSize:             targetSize,
		maxFileCount:           int(maxFileCount),
		checkpointFrontier:     f,
	}
	return sh, nil
}

// filterCompleted returns the subspans of the requiredSpan that still need to be
// restored.
func (f spanCoveringFilter) filterCompleted(requiredSpan roachpb.Span) roachpb.Spans {
	return f.findToDoSpans(requiredSpan)
}

// findToDoSpans returns the sub spans within the required span that have not completed.
func (f spanCoveringFilter) findToDoSpans(requiredSpan roachpb.Span) roachpb.Spans {
	toDoSpans := make(roachpb.Spans, 0)
	f.checkpointFrontier.SpanEntries(requiredSpan, func(s roachpb.Span,
		ts hlc.Timestamp) (done spanUtils.OpResult) {
		if !ts.Equal(completedSpanTime) {
			toDoSpans = append(toDoSpans, s)
		}
		return spanUtils.ContinueMatch
	})
	return toDoSpans
}

// getLayersCoveredLater creates a map which indicates which backup layers introduced the
// given span.
func (f spanCoveringFilter) getLayersCoveredLater(
	span roachpb.Span, backups []backuppb.BackupManifest,
) map[int]bool {
	layersCoveredLater := make(map[int]bool)
	for layer := range backups {
		var coveredLater bool
		f.introducedSpanFrontier.SpanEntries(span, func(s roachpb.Span,
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
	return layersCoveredLater
}

func (f spanCoveringFilter) close() {
	f.checkpointFrontier.Release()
}

// generateAndSendImportSpans partitions the spans of requiredSpans into a
// covering of RestoreSpanEntry's which each have all overlapping files from the
// passed backups assigned to them. The spans of requiredSpans are
// trimmed/removed based on the lowWaterMark before the covering for them is
// generated. These spans are generated one at a time and then sent to spanCh.
//
// Spans are considered for inclusion using the fileSpanComparator. Note that
// because of https://github.com/cockroachdb/cockroach/issues/101963, most
// backups use a comparator that treats a file's end key as _inclusive_.
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
//	[c, f): 1, 2, 4, 6
//	[f, g): 6
//	[g, h): 5, 6
//	[h, i): 3, 5, 6, 8
//	[l, m): 9
//
// This cover is created by iterating through the start keys of all the
// files in the backup in key order via fileSpanStartKeyIterator. The
// cover spans are initially just the spans between each pair of adjacent keys
// yielded by the iterator. We then iterate through each cover span and find all
// the overlapping files. If the files that overlap a cover span is a subset of
// the files that overlap the span before it, then the two spans are merged into
// a single span in the final cover. Additionally, if targetSize > 0, we can
// merge the current cover span with the previous cover span if the merged set
// of files have a total data size below the target size.
//
// The above example is tested in TestRestoreEntryCoverExample.
func generateAndSendImportSpans(
	ctx context.Context,
	requiredSpans roachpb.Spans,
	backups []backuppb.BackupManifest,
	layerToBackupManifestFileIterFactory backupinfo.LayerToBackupManifestFileIterFactory,
	backupLocalityMap map[int]storeByLocalityKV,
	filter spanCoveringFilter,
	fsc fileSpanComparator,
	spanCh chan execinfrapb.RestoreSpanEntry,
) error {

	startKeyIt, err := newFileSpanStartKeyIterator(ctx, backups, layerToBackupManifestFileIterFactory)
	if err != nil {
		return err
	}
	defer startKeyIt.Close()

	var key roachpb.Key

	fileIterByLayer := make([]bulk.Iterator[*backuppb.BackupManifest_File], 0, len(backups))
	defer func() {
		for _, i := range fileIterByLayer {
			i.Close()
		}
	}()
	for layer := range backups {
		iter, err := layerToBackupManifestFileIterFactory[layer].NewFileIter(ctx)
		if err != nil {
			return err
		}
		fileIterByLayer = append(fileIterByLayer, iter)
	}

	// lastCovSpanSize is the size of files added to the right-most span of
	// the cover so far.
	var lastCovSpanSize int64
	var lastCovSpanCount int
	var lastCovSpan roachpb.Span
	var covFilesByLayer [][]*backuppb.BackupManifest_File
	var firstInSpan bool

	flush := func(ctx context.Context) error {
		entry := execinfrapb.RestoreSpanEntry{
			Span:         lastCovSpan,
			ElidedPrefix: backups[0].ElidedPrefix,
		}
		for layer := range covFilesByLayer {
			for _, f := range covFilesByLayer[layer] {
				fileSpec := execinfrapb.RestoreFileSpec{
					Path:                    f.Path,
					Dir:                     backups[layer].Dir,
					BackupFileEntrySpan:     f.Span,
					BackupFileEntryCounts:   f.EntryCounts,
					BackingFileSize:         f.BackingFileSize,
					ApproximatePhysicalSize: f.ApproximatePhysicalSize,
					Layer:                   int32(layer),
					HasRangeKeys:            f.HasRangeKeys,
				}
				if dir, ok := backupLocalityMap[layer][f.LocalityKV]; ok {
					fileSpec.Dir = dir
				}
				entry.Files = append(entry.Files, fileSpec)
			}
		}
		if len(entry.Files) > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case spanCh <- entry:
			}
		}

		return nil
	}

	for _, requiredSpan := range requiredSpans {
		filteredSpans := filter.filterCompleted(requiredSpan)
		for _, span := range filteredSpans {
			firstInSpan = true
			layersCoveredLater := filter.getLayersCoveredLater(span, backups)
			for {
				// NB: we iterate through all of the keys yielded by the iterator in order to
				// construct spans for the cover. If the iterator's last key is less than the
				// EndKey of the last required span, we additionally need to yield the last
				// span's EndKey in order to complete the cover.
				if ok, err := startKeyIt.valid(); !ok {
					if err != nil {
						return err
					}

					if key.Compare(span.EndKey) < 0 {
						key = span.EndKey
					} else {
						break
					}
				} else {
					key = startKeyIt.value()
				}

				if span.Key.Compare(key) >= 0 {
					startKeyIt.next()
					continue
				}

				var coverSpan roachpb.Span
				if firstInSpan {
					coverSpan.Key = span.Key
				} else {
					coverSpan.Key = lastCovSpan.EndKey
				}

				if span.ContainsKey(key) {
					coverSpan.EndKey = startKeyIt.value()
				} else {
					coverSpan.EndKey = span.EndKey
				}

				newFilesByLayer, err := getNewIntersectingFilesByLayer(coverSpan, layersCoveredLater, fileIterByLayer, fsc)
				if err != nil {
					return err
				}

				var filesByLayer [][]*backuppb.BackupManifest_File
				var covSize, newCovFilesSize int64
				var covCount, newCovFilesCount int

				for layer := range newFilesByLayer {
					for _, file := range newFilesByLayer[layer] {
						sz := file.EntryCounts.DataSize
						if sz == 0 {
							sz = 16 << 20
						}
						newCovFilesSize += sz
					}
					newCovFilesCount += len(newFilesByLayer[layer])
					filesByLayer = append(filesByLayer, newFilesByLayer[layer])
				}

				for layer := range covFilesByLayer {
					for _, file := range covFilesByLayer[layer] {
						sz := file.EntryCounts.DataSize
						if sz == 0 {
							sz = 16 << 20
						}

						if fsc.overlaps(coverSpan, file.Span) {
							covSize += sz
							covCount++
							filesByLayer[layer] = append(filesByLayer[layer], file)
						}
					}
				}

				if covFilesByLayer == nil {
					covFilesByLayer = newFilesByLayer
					lastCovSpan = coverSpan
					lastCovSpanSize = newCovFilesSize
					lastCovSpanCount = newCovFilesCount
				} else {
					// We have room to add to the last span if doing so would remain below
					// both the target byte size and maxFileCount total files. We limit the number
					// of files since we default to running multiple concurrent workers so
					// we want to bound sum total open files across all of them to <= 1k.
					// We bound the span byte size to improve work distribution and make
					// the progress more granular.
					fits := lastCovSpanSize+newCovFilesSize <= filter.targetSize && lastCovSpanCount+newCovFilesCount <= filter.maxFileCount

					if (newCovFilesCount == 0 || fits) && !firstInSpan {
						// If there are no new files that cover this span or if we can add the
						// files in the new span's cover to the last span's cover and still stay
						// below targetSize, then we should merge the two spans.
						for layer := range newFilesByLayer {
							covFilesByLayer[layer] = append(covFilesByLayer[layer], newFilesByLayer[layer]...)
						}
						lastCovSpan.EndKey = coverSpan.EndKey
						lastCovSpanSize = lastCovSpanSize + newCovFilesSize
						lastCovSpanCount = lastCovSpanCount + newCovFilesCount
					} else {
						if err := flush(ctx); err != nil {
							return err
						}
						lastCovSpan = coverSpan
						covFilesByLayer = filesByLayer
						lastCovSpanSize = covSize
						lastCovSpanCount = covCount
					}
				}
				firstInSpan = false

				if lastCovSpan.EndKey.Equal(span.EndKey) {
					break
				}

				startKeyIt.next()
			}
		}
	}
	return flush(ctx)
}

// fileSpanStartKeyIterator yields all of the unique start keys of the spans
// from the files in a backup chain in key order by using a min heap.
//
//	backup
//	0|     a___1___c c__2__e          h__3__i
//	1|         b___4___d           g____5___i
//	2|     a___________6______________h         j_7_k
//	3|                                  h_8_i              l_9_m
//	 keys--a---b---c---d---e---f---g---h----i---j---k---l----m------p---->
//
// In this case the iterator will yield all start keys:
// [a, b, c, g, h, j, l]
type fileSpanStartKeyIterator struct {
	heap     *fileHeap
	allIters []bulk.Iterator[*backuppb.BackupManifest_File]
	err      error
}

func newFileSpanStartKeyIterator(
	ctx context.Context,
	backups []backuppb.BackupManifest,
	layerToBackupManifestFileIterFactory backupinfo.LayerToBackupManifestFileIterFactory,
) (*fileSpanStartKeyIterator, error) {
	it := &fileSpanStartKeyIterator{}
	for layer := range backups {
		iter, err := layerToBackupManifestFileIterFactory[layer].NewFileIter(ctx)
		if err != nil {
			return nil, err
		}

		it.allIters = append(it.allIters, iter)
	}
	it.reset()
	return it, nil
}

func (i *fileSpanStartKeyIterator) Close() {
	for _, iter := range i.allIters {
		iter.Close()
	}
}

func (i *fileSpanStartKeyIterator) next() {
	if ok, _ := i.valid(); !ok {
		return
	}

	// To find the next key for the iterator, we peek at the key of the
	// min fileHeapItem in the heap. As long as the key is less than or
	// equal to the previous key yielded by the iterator, we advance
	// the iterator of the min fileHeapItem and push it back onto the heap
	// until the key of the min fileHeapItem is greater than the previously
	// yielded key.
	prevKey := i.value()
	for i.heap.Len() > 0 {
		minItem := heap.Pop(i.heap).(fileHeapItem)

		curKey := minItem.key()
		if curKey.Compare(prevKey) > 0 {
			heap.Push(i.heap, minItem)
			break
		}

		minItem.fileIter.Next()
		if ok, err := minItem.fileIter.Valid(); err != nil {
			i.err = err
			return
		} else if ok {
			minItem.file = minItem.fileIter.Value()
			heap.Push(i.heap, minItem)
		}
	}
	if ok, _ := i.valid(); !ok {
		return
	}
	if prevKey.Compare(i.value()) >= 0 {
		i.err = errors.New("out of order backup keys")
		return
	}
}

func (i *fileSpanStartKeyIterator) valid() (bool, error) {
	if i.err != nil {
		return false, i.err
	}
	return i.heap.Len() > 0, nil
}

func (i *fileSpanStartKeyIterator) value() roachpb.Key {
	if ok, _ := i.valid(); !ok {
		return nil
	}

	return i.heap.fileHeapItems[0].key()
}
func (i *fileSpanStartKeyIterator) reset() {
	i.heap = &fileHeap{}
	i.err = nil

	for _, iter := range i.allIters {
		if ok, err := iter.Valid(); err != nil {
			i.err = err
			return
		} else if !ok {
			continue
		}

		i.heap.fileHeapItems = append(i.heap.fileHeapItems, fileHeapItem{
			fileIter: iter,
			file:     iter.Value(),
		})
	}
	heap.Init(i.heap)
}

// fileHeapItem is a wrapper for an iterator of *backuppb.BackupManifest_File
// so that it can be used in a heap. The key that's being compared in this
// wrapper is the start key of the span of the iterator's current
// BackupManifest_File value.
type fileHeapItem struct {
	fileIter bulk.Iterator[*backuppb.BackupManifest_File]
	file     *backuppb.BackupManifest_File
}

func (f fileHeapItem) key() roachpb.Key {
	return f.file.Span.Key
}

// fileHeap is a min heap of fileHeapItems.
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
	span roachpb.Span,
	layersCoveredLater map[int]bool,
	fileIters []bulk.Iterator[*backuppb.BackupManifest_File],
	fsc fileSpanComparator,
) ([][]*backuppb.BackupManifest_File, error) {
	var files [][]*backuppb.BackupManifest_File

	for l, iter := range fileIters {
		var layerFiles []*backuppb.BackupManifest_File
		if !layersCoveredLater[l] {
			for ; ; iter.Next() {
				if ok, err := iter.Valid(); err != nil {
					return nil, err
				} else if !ok {
					break
				}

				f := iter.Value()
				// NB: a backup file can currently have keys equal to its span's
				// EndKey due to the bug:
				// https://github.com/cockroachdb/cockroach/issues/101963,
				// effectively meaning that we have to treat the span as end key
				// inclusive. Because roachpb.Span and its associated operations
				// are end key exclusive, we work around this by replacing the
				// end key with its next value in order to include the end key.
				if fsc.overlaps(span, f.Span) {
					layerFiles = append(layerFiles, f)
				}

				if span.EndKey.Compare(f.Span.Key) <= 0 {
					break
				}
			}
		}
		files = append(files, layerFiles)
	}

	return files, nil
}

type fileSpanComparator interface {
	overlaps(targetSpan, fileSpan roachpb.Span) bool
	isExclusive() bool
}

// inclusiveEndKeyComparator assumes that file spans have inclusive
// end keys.
type inclusiveEndKeyComparator struct{}

func (inclusiveEndKeyComparator) overlaps(targetSpan, inclusiveEndKeyFileSpan roachpb.Span) bool {
	// TODO(ssd): Could ContainsKey here be replaced with targetSpan.Key.Equal(inclusiveEndKeyFileSpan.EndKey)?
	return targetSpan.Overlaps(inclusiveEndKeyFileSpan) || targetSpan.ContainsKey(inclusiveEndKeyFileSpan.EndKey)
}

func (inclusiveEndKeyComparator) isExclusive() bool { return false }

// exclusiveEndKeyComparator assumes that file spans have exclusive
// end keys.
type exclusiveEndKeyComparator struct{}

func (exclusiveEndKeyComparator) overlaps(targetSpan, inclusiveEndKeyFileSpan roachpb.Span) bool {
	return targetSpan.Overlaps(inclusiveEndKeyFileSpan)
}

func (exclusiveEndKeyComparator) isExclusive() bool { return true }
