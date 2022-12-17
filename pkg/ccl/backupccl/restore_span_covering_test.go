// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	spanUtils "github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// MockBackupChain returns a chain of mock backup manifests that have spans and
// file spans suitable for checking coverage computations. Every 3rd inc backup
// reintroduces a span. On a random backup, one random span is dropped and
// another is added. Incremental backups have half as many files as the base.
// Files spans are ordered by start key but may overlap.
func MockBackupChain(
	ctx context.Context,
	length, spans, baseFiles int,
	r *rand.Rand,
	slimManifest bool,
	execCfg sql.ExecutorConfig,
) ([]backuppb.BackupManifest, error) {
	backups := make([]backuppb.BackupManifest, length)
	ts := hlc.Timestamp{WallTime: time.Second.Nanoseconds()}

	// spanIdxToDrop represents that span that will get dropped during this mock backup chain.
	spanIdxToDrop := r.Intn(spans)

	// backupWithDroppedSpan represents the first backup that will observe the dropped span.
	backupWithDroppedSpan := r.Intn(len(backups))

	genTableID := func(j int) uint32 {
		return uint32(10 + j*2)
	}

	for i := range backups {
		backups[i].IsSlimManifest = slimManifest
		backups[i].Spans = make(roachpb.Spans, spans)
		backups[i].IntroducedSpans = make(roachpb.Spans, 0)
		for j := range backups[i].Spans {
			tableID := genTableID(j)
			backups[i].Spans[j] = makeTableSpan(keys.SystemSQLCodec, tableID)
		}
		backups[i].EndTime = ts.Add(time.Minute.Nanoseconds()*int64(i), 0)
		if i > 0 {
			backups[i].StartTime = backups[i-1].EndTime

			if i >= backupWithDroppedSpan {
				// At and after the backupWithDroppedSpan, drop the span at
				// span[spanIdxToDrop], present in the first i backups, and add a new
				// one.
				newTableID := genTableID(spanIdxToDrop) + 1
				backups[i].Spans[spanIdxToDrop] = makeTableSpan(keys.SystemSQLCodec, newTableID)
				backups[i].IntroducedSpans = append(backups[i].IntroducedSpans, backups[i].Spans[spanIdxToDrop])
			}

			if i%3 == 0 {
				// Reintroduce an existing span
				spanIdx := r.Intn(spans)
				backups[i].IntroducedSpans = append(backups[i].IntroducedSpans, backups[i].Spans[spanIdx])
			}
		}

		files := baseFiles
		if i == 0 {
			backups[i].Files = make([]backuppb.BackupManifest_File, files)
		} else {
			files = baseFiles / 2
			backups[i].Files = make([]backuppb.BackupManifest_File, files)
		}

		for f := range backups[i].Files {
			start := f*5 + r.Intn(4)
			end := start + 1 + r.Intn(25)
			k := encoding.EncodeVarintAscending(backups[i].Spans[f*spans/files].Key, 1)
			k = k[:len(k):len(k)]
			backups[i].Files[f].Span.Key = encoding.EncodeVarintAscending(k, int64(start))
			backups[i].Files[f].Span.EndKey = encoding.EncodeVarintAscending(k, int64(end))
			backups[i].Files[f].Path = fmt.Sprintf("12345-b%d-f%d.sst", i, f)
			backups[i].Files[f].EntryCounts.DataSize = 1 << 20
		}

		config := cloudpb.ExternalStorage{S3Config: &cloudpb.ExternalStorage_S3{}}
		if backups[i].IsSlimManifest {
			// Write the Files to an SST and put them at a well known location.
			es, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx,
				fmt.Sprintf("nodelocal://1/mock%s", timeutil.Now().String()), username.RootUserName())
			if err != nil {
				return nil, err
			}
			config = es.Conf()
			manifestCopy := backups[i]
			err = backupinfo.WriteBackupManifestFilesSST(ctx, es, nil, nil, &manifestCopy,
				backupinfo.SlimManifestFileInfoPath)
			if err != nil {
				return nil, err
			}
			backups[i].Files = nil
		}
		// A non-nil Dir more accurately models the footprint of produced coverings.
		backups[i].Dir = config
	}
	return backups, nil
}

// checkRestoreCovering verifies that a covering actually uses every span of
// every file in the passed backups that overlaps with any part of the passed
// spans. It does by constructing a map from every file name to a SpanGroup that
// contains the overlap of that file span with every required span, and then
// iterating through the partitions of the cover and removing that partition's
// span from the group for every file specified by that partition, and then
// checking that all the groups are empty, indicating no needed span was missed.
// It also checks that each file that the cover has an expected number of
// partitions (i.e. isn't just one big partition of all files), by comparing its
// length to the number of files a file's end key was greater than any prior end
// key when walking files in order by start key in the backups. This check is
// thus sensitive to ordering; the coverage correctness check however is not.
//
// The function also verifies that a cover does not cross a span boundary.
func checkRestoreCovering(
	ctx context.Context,
	backups []backuppb.BackupManifest,
	spans roachpb.Spans,
	cov []execinfrapb.RestoreSpanEntry,
	merged bool,
	storageFactory cloud.ExternalStorageFactory,
) error {
	var expectedPartitions int
	required := make(map[string]*roachpb.SpanGroup)

	introducedSpanFrontier, err := createIntroducedSpanFrontier(backups, hlc.Timestamp{})
	if err != nil {
		return err
	}

	for _, span := range spans {
		var last roachpb.Key
		for _, b := range backups {
			var coveredLater bool
			introducedSpanFrontier.Entries(func(s roachpb.Span,
				ts hlc.Timestamp) (done spanUtils.OpResult) {
				if span.Overlaps(s) {
					if b.EndTime.Less(ts) {
						coveredLater = true
					}
					return spanUtils.StopMatch
				}
				return spanUtils.ContinueMatch
			})
			if coveredLater {
				// Skip spans that were later re-introduced. See makeSimpleImportSpans
				// for explanation.
				continue
			}
			it, err := makeBackupManifestFileIterator(ctx, storageFactory, b,
				nil, nil)
			if err != nil {
				return err
			}
			defer it.close()
			for f, hasNext := it.next(); hasNext; f, hasNext = it.next() {
				if sp := span.Intersect(f.Span); sp.Valid() {
					if required[f.Path] == nil {
						required[f.Path] = &roachpb.SpanGroup{}
					}
					required[f.Path].Add(sp)
					if sp.EndKey.Compare(last) > 0 {
						last = sp.EndKey
						expectedPartitions++
					}
				}
			}
			if it.err() != nil {
				return it.err()
			}
		}
	}
	var spanIdx int
	for _, c := range cov {
		for _, f := range c.Files {
			required[f.Path].Sub(c.Span)
		}
		for spans[spanIdx].EndKey.Compare(c.Span.Key) < 0 {
			spanIdx++
		}
		// Assert that every cover is contained by a required span.
		requiredSpan := spans[spanIdx]
		if requiredSpan.Overlaps(c.Span) && !requiredSpan.Contains(c.Span) {
			return errors.Errorf("cover with requiredSpan %v is not contained by required requiredSpan"+
				" %v", c.Span, requiredSpan)
		}

	}
	for name, uncovered := range required {
		for _, missing := range uncovered.Slice() {
			return errors.Errorf("file %s was supposed to cover span %s", name, missing)
		}
	}
	if got := len(cov); got != expectedPartitions && !merged {
		return errors.Errorf("expected %d partitions, got %d", expectedPartitions, got)
	}
	return nil
}

const noSpanTargetSize = 0

func TestRestoreEntryCoverExample(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc, _, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 1, InitManualReplication)
	defer cleanupFn()

	sp := func(start, end string) roachpb.Span {
		return roachpb.Span{Key: roachpb.Key(start), EndKey: roachpb.Key(end)}
	}
	f := func(start, end, path string) backuppb.BackupManifest_File {
		return backuppb.BackupManifest_File{Span: sp(start, end), Path: path}
	}
	paths := func(names ...string) []execinfrapb.RestoreFileSpec {
		r := make([]execinfrapb.RestoreFileSpec, len(names))
		for i := range names {
			r[i].Path = names[i]
		}
		return r
	}

	// Setup and test the example in the comment of makeSimpleImportSpans.
	spans := []roachpb.Span{sp("a", "f"), sp("f", "i"), sp("l", "m")}
	backups := []backuppb.BackupManifest{
		{Files: []backuppb.BackupManifest_File{f("a", "c", "1"), f("c", "e", "2"), f("h", "i", "3")}},
		{Files: []backuppb.BackupManifest_File{f("b", "d", "4"), f("g", "i", "5")}},
		{Files: []backuppb.BackupManifest_File{f("a", "h", "6"), f("j", "k", "7")}},
		{Files: []backuppb.BackupManifest_File{f("h", "i", "8"), f("l", "m", "9")}},
	}

	for i := range backups {
		backups[i].StartTime = hlc.Timestamp{WallTime: int64(i)}
		backups[i].EndTime = hlc.Timestamp{WallTime: int64(i + 1)}

		for j := range backups[i].Files {
			// Pretend every span has 1MB.
			backups[i].Files[j].EntryCounts.DataSize = 1 << 20
		}
	}

	emptySpanFrontier, err := spanUtils.MakeFrontier(roachpb.Span{})
	require.NoError(t, err)

	execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)
	layerToBackupManifestFileIterFactory, err := getBackupManifestFileIters(ctx, &execCfg,
		backups, nil, nil)
	require.NoError(t, err)
	cover, err := makeSimpleImportSpans(spans, backups, layerToBackupManifestFileIterFactory, nil,
		emptySpanFrontier, nil, noSpanTargetSize)
	require.NoError(t, err)
	require.Equal(t, []execinfrapb.RestoreSpanEntry{
		{Span: sp("a", "c"), Files: paths("1", "4", "6")},
		{Span: sp("c", "e"), Files: paths("2", "4", "6")},
		{Span: sp("e", "f"), Files: paths("6")},
		{Span: sp("f", "i"), Files: paths("3", "5", "6", "8")},
		{Span: sp("l", "m"), Files: paths("9")},
	}, cover)

	coverSized, err := makeSimpleImportSpans(spans, backups, layerToBackupManifestFileIterFactory,
		nil, emptySpanFrontier, nil, 2<<20)
	require.NoError(t, err)
	require.Equal(t, []execinfrapb.RestoreSpanEntry{
		{Span: sp("a", "f"), Files: paths("1", "2", "4", "6")},
		{Span: sp("f", "i"), Files: paths("3", "5", "6", "8")},
		{Span: sp("l", "m"), Files: paths("9")},
	}, coverSized)

	// check that introduced spans are properly elided
	backups[2].IntroducedSpans = []roachpb.Span{sp("a", "f")}
	introducedSpanFrontier, err := createIntroducedSpanFrontier(backups, hlc.Timestamp{})
	require.NoError(t, err)

	coverIntroduced, err := makeSimpleImportSpans(spans, backups, layerToBackupManifestFileIterFactory, nil, introducedSpanFrontier, nil,
		noSpanTargetSize)
	require.NoError(t, err)
	require.Equal(t, []execinfrapb.RestoreSpanEntry{
		{Span: sp("a", "f"), Files: paths("6")},
		{Span: sp("f", "i"), Files: paths("3", "5", "6", "8")},
		{Span: sp("l", "m"), Files: paths("9")},
	}, coverIntroduced)

}

type mockBackupInfo struct {
	// tableIDs identifies the tables included in the backup.
	tableIDs []int

	// indexIDs defines a map from tableID to tableIndexes included in the backup.
	indexIDs map[int][]int

	// reintroducedTableIDs identifies a set of TableIDs to reintroduce in the backup.
	reintroducedTableIDs map[int]struct{}

	// expectedBackupSpanCount defines the number of backup spans created by spansForAllTableIndexes.
	expectedBackupSpanCount int
}

func createMockTables(
	info mockBackupInfo,
) (tables []catalog.TableDescriptor, reIntroducedTables []catalog.TableDescriptor) {
	tables = make([]catalog.TableDescriptor, 0)
	reIntroducedTables = make([]catalog.TableDescriptor, 0)
	for _, tableID := range info.tableIDs {
		indexes := make([]descpb.IndexDescriptor, 0)
		for _, indexID := range info.indexIDs[tableID] {
			indexes = append(indexes, getMockIndexDesc(descpb.IndexID(indexID)))
		}
		table := getMockTableDesc(descpb.ID(tableID), indexes[0], indexes, nil, nil)
		tables = append(tables, table)
		if _, ok := info.reintroducedTableIDs[tableID]; ok {
			reIntroducedTables = append(reIntroducedTables, table)
		}
	}
	return tables, reIntroducedTables
}

func createMockManifest(
	t *testing.T,
	execCfg *sql.ExecutorConfig,
	info mockBackupInfo,
	endTime hlc.Timestamp,
	path string,
) backuppb.BackupManifest {
	tables, _ := createMockTables(info)

	spans, err := spansForAllTableIndexes(execCfg, tables,
		nil /* revs */)
	require.NoError(t, err)
	require.Equal(t, info.expectedBackupSpanCount, len(spans))

	files := make([]backuppb.BackupManifest_File, len(spans))
	for _, sp := range spans {
		files = append(files, backuppb.BackupManifest_File{Span: sp, Path: path})
	}

	return backuppb.BackupManifest{Spans: spans,
		EndTime: endTime, Files: files}
}

// TestRestoreEntryCoverReIntroducedSpans checks that all reintroduced spans are
// covered in RESTORE by files in the incremental backup that reintroduced the
// spans. The test also checks the invariants required during RESTORE to elide
// files from the full backup that are later reintroduced. These include:
//
//   - During BackupManifest creation, spansForAllTableIndexes will merge
//     adjacent indexes within a table, but not indexes across tables.
//
//   - During spansForAllRestoreTableIndexes, each restored index will have its
//     own span.
func TestRestoreEntryCoverReIntroducedSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc, _, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 1, InitManualReplication)
	defer cleanupFn()
	execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)

	testCases := []struct {
		name string
		full mockBackupInfo
		inc  mockBackupInfo

		// expectedRestoreSpanCount defines the number of required spans passed to
		// makeSimpleImportSpans.
		expectedRestoreSpanCount int
	}{
		{
			name: "adjacent indexes",
			full: mockBackupInfo{
				tableIDs:                []int{1, 2},
				indexIDs:                map[int][]int{1: {1, 2}, 2: {1}},
				expectedBackupSpanCount: 2,
			},
			inc: mockBackupInfo{
				tableIDs:                []int{1, 2},
				indexIDs:                map[int][]int{1: {1, 2}, 2: {1}},
				reintroducedTableIDs:    map[int]struct{}{1: {}},
				expectedBackupSpanCount: 2,
			},
			expectedRestoreSpanCount: 3,
		},
		{
			name: "non-adjacent indexes",
			full: mockBackupInfo{
				tableIDs:                []int{1, 2},
				indexIDs:                map[int][]int{1: {1, 3}, 2: {1}},
				expectedBackupSpanCount: 3,
			},
			inc: mockBackupInfo{
				tableIDs:                []int{1, 2},
				indexIDs:                map[int][]int{1: {1, 3}, 2: {1}},
				reintroducedTableIDs:    map[int]struct{}{1: {}},
				expectedBackupSpanCount: 3,
			},
			expectedRestoreSpanCount: 3,
		},
		{
			name: "dropped non-adjacent index",
			full: mockBackupInfo{
				tableIDs:                []int{1, 2},
				indexIDs:                map[int][]int{1: {1, 3}, 2: {1}},
				expectedBackupSpanCount: 3,
			},
			inc: mockBackupInfo{
				tableIDs:                []int{1, 2},
				indexIDs:                map[int][]int{1: {1}, 2: {1}},
				reintroducedTableIDs:    map[int]struct{}{1: {}},
				expectedBackupSpanCount: 2,
			},
			expectedRestoreSpanCount: 2,
		},
		{
			name: "new non-adjacent index",
			full: mockBackupInfo{
				tableIDs:                []int{1, 2},
				indexIDs:                map[int][]int{1: {1}, 2: {1}},
				expectedBackupSpanCount: 2,
			},
			inc: mockBackupInfo{
				tableIDs:                []int{1, 2},
				indexIDs:                map[int][]int{1: {1, 3}, 2: {1}},
				reintroducedTableIDs:    map[int]struct{}{1: {}},
				expectedBackupSpanCount: 3,
			},
			expectedRestoreSpanCount: 3,
		},
		{
			name: "new adjacent index",
			full: mockBackupInfo{
				tableIDs:                []int{1, 2},
				indexIDs:                map[int][]int{1: {1}, 2: {1}},
				expectedBackupSpanCount: 2,
			},
			inc: mockBackupInfo{
				tableIDs:                []int{1, 2},
				indexIDs:                map[int][]int{1: {1, 2}, 2: {1}},
				reintroducedTableIDs:    map[int]struct{}{1: {}},
				expectedBackupSpanCount: 2,
			},
			expectedRestoreSpanCount: 3,
		},
		{
			name: "new in-between index",
			full: mockBackupInfo{
				tableIDs:                []int{1, 2},
				indexIDs:                map[int][]int{1: {1, 3}, 2: {1}},
				expectedBackupSpanCount: 3,
			},
			inc: mockBackupInfo{
				tableIDs:                []int{1, 2},
				indexIDs:                map[int][]int{1: {1, 2, 3}, 2: {1}},
				reintroducedTableIDs:    map[int]struct{}{1: {}},
				expectedBackupSpanCount: 2,
			},
			expectedRestoreSpanCount: 4,
		},
	}

	fullBackupPath := "full"
	incBackupPath := "inc"
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			backups := []backuppb.BackupManifest{
				createMockManifest(t, &execCfg, test.full, hlc.Timestamp{WallTime: int64(1)}, fullBackupPath),
				createMockManifest(t, &execCfg, test.inc, hlc.Timestamp{WallTime: int64(2)}, incBackupPath),
			}

			// Create the IntroducedSpans field for incremental backup.
			incTables, reIntroducedTables := createMockTables(test.inc)

			newSpans := filterSpans(backups[1].Spans, backups[0].Spans)
			reIntroducedSpans, err := spansForAllTableIndexes(&execCfg, reIntroducedTables, nil)
			require.NoError(t, err)
			backups[1].IntroducedSpans = append(newSpans, reIntroducedSpans...)

			restoreSpans := spansForAllRestoreTableIndexes(execCfg.Codec, incTables, nil, false)
			require.Equal(t, test.expectedRestoreSpanCount, len(restoreSpans))

			introducedSpanFrontier, err := createIntroducedSpanFrontier(backups, hlc.Timestamp{})
			require.NoError(t, err)

			layerToBackupManifestFileIterFactory, err := getBackupManifestFileIters(ctx,
				&execCfg, backups, nil, nil)
			require.NoError(t, err)
			cover, err := makeSimpleImportSpans(restoreSpans, backups, layerToBackupManifestFileIterFactory,
				nil, introducedSpanFrontier, nil, 0)
			require.NoError(t, err)

			for _, reIntroTable := range reIntroducedTables {
				var coveredReIntroducedGroup roachpb.SpanGroup
				for _, entry := range cover {
					// If a restoreSpanEntry overlaps with re-introduced span,
					// assert the entry only contains files from the incremental backup.
					if reIntroTable.TableSpan(execCfg.Codec).Overlaps(entry.Span) {
						coveredReIntroducedGroup.Add(entry.Span)
						for _, files := range entry.Files {
							require.Equal(t, incBackupPath, files.Path)
						}
					}
				}
				// Assert that all re-introduced indexes are included in the restore
				for _, reIntroIndexSpan := range reIntroTable.AllIndexSpans(execCfg.Codec) {
					require.Equal(t, true, coveredReIntroducedGroup.Encloses(reIntroIndexSpan))
				}
			}
		})
	}
}

func TestRestoreEntryCover(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r, _ := randutil.NewTestRand()
	ctx := context.Background()
	tc, _, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 1, InitManualReplication)
	defer cleanupFn()
	execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)

	for _, numBackups := range []int{1, 2, 3, 5, 9, 10, 11, 12} {
		for _, spans := range []int{1, 2, 3, 5, 9, 11, 12} {
			for _, files := range []int{0, 1, 2, 3, 4, 10, 12, 50} {
				for _, slimManifest := range []bool{true, false} {
					backups, err := MockBackupChain(ctx, numBackups, spans, files, r, slimManifest, execCfg)
					require.NoError(t, err)

					for _, target := range []int64{0, 1, 4, 100, 1000} {
						t.Run(fmt.Sprintf("numBackups=%d, numSpans=%d, numFiles=%d, merge=%d, slim=%t",
							numBackups, spans, files, target, slimManifest), func(t *testing.T) {
							introducedSpanFrontier, err := createIntroducedSpanFrontier(backups, hlc.Timestamp{})
							require.NoError(t, err)

							layerToBackupManifestFileIterFactory, err := getBackupManifestFileIters(ctx,
								&execCfg, backups, nil, nil)
							require.NoError(t, err)
							cover, err := makeSimpleImportSpans(backups[numBackups-1].Spans, backups,
								layerToBackupManifestFileIterFactory, nil, introducedSpanFrontier,
								nil, target<<20)
							require.NoError(t, err)
							require.NoError(t, checkRestoreCovering(ctx, backups, backups[numBackups-1].Spans,
								cover, target != noSpanTargetSize, execCfg.DistSQLSrv.ExternalStorage))
						})
					}
				}
			}
		}
	}
}
