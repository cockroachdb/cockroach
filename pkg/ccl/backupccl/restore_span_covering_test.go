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

	"github.com/cockroachdb/cockroach/pkg/base"
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
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
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
	hasExternalFilesList bool,
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
		backups[i].HasExternalManifestSSTs = hasExternalFilesList
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

		es, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx,
			fmt.Sprintf("nodelocal://1/mock%s", timeutil.Now().String()), username.RootUserName())
		if err != nil {
			return nil, err
		}
		config := es.Conf()
		if backups[i].HasExternalManifestSSTs {
			// Write the Files to an SST and put them at a well known location.
			manifestCopy := backups[i]
			err = backupinfo.WriteFilesListSST(ctx, es, nil, nil, &manifestCopy,
				backupinfo.BackupMetadataFilesListPath)
			if err != nil {
				return nil, err
			}
			backups[i].Files = nil

			err = backupinfo.WriteDescsSST(ctx, &manifestCopy, es, nil, nil, backupinfo.BackupMetadataDescriptorsListPath)
			if err != nil {
				return nil, err
			}
			backups[i].Descriptors = nil
			backups[i].DescriptorChanges = nil
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
//
// The function also verifies that a cover does not cross a span boundary.
//
// TODO(rui): this check previously contained a partition count check.
// Partitions are now generated differently, so this is a reminder to add this
// check back in when I figure out what the expected partition count should be.
func checkRestoreCovering(
	ctx context.Context,
	backups []backuppb.BackupManifest,
	spans roachpb.Spans,
	cov []execinfrapb.RestoreSpanEntry,
	merged bool,
	storageFactory cloud.ExternalStorageFactory,
) error {
	required := make(map[string]*roachpb.SpanGroup)

	introducedSpanFrontier, err := createIntroducedSpanFrontier(backups, hlc.Timestamp{})
	if err != nil {
		return err
	}

	layerToIterFactory, err := backupinfo.GetBackupManifestIterFactories(ctx, storageFactory, backups, nil, nil)
	if err != nil {
		return err
	}

	for _, span := range spans {
		var last roachpb.Key
		for i, b := range backups {
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
			it, err := layerToIterFactory[i].NewFileIter(ctx)
			if err != nil {
				return err
			}
			defer it.Close()
			for ; ; it.Next() {
				if ok, err := it.Valid(); err != nil {
					return err
				} else if !ok {
					break
				}
				f := it.Value()
				if sp := span.Intersect(f.Span); sp.Valid() {
					if required[f.Path] == nil {
						required[f.Path] = &roachpb.SpanGroup{}
					}
					required[f.Path].Add(sp)
					if sp.EndKey.Compare(last) > 0 {
						last = sp.EndKey
					}
				}
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
	return nil
}

const noSpanTargetSize = 0

func makeImportSpans(
	ctx context.Context,
	spans []roachpb.Span,
	backups []backuppb.BackupManifest,
	layerToIterFactory backupinfo.LayerToBackupManifestFileIterFactory,
	targetSize int64,
	introducedSpanFrontier *spanUtils.Frontier,
	useSimpleImportSpans bool,
) ([]execinfrapb.RestoreSpanEntry, error) {
	var cover []execinfrapb.RestoreSpanEntry
	spanCh := make(chan execinfrapb.RestoreSpanEntry)
	g := ctxgroup.WithContext(context.Background())
	g.Go(func() error {
		for entry := range spanCh {
			cover = append(cover, entry)
		}
		return nil
	})

	err := generateAndSendImportSpans(ctx, spans, backups, layerToIterFactory, nil, introducedSpanFrontier, nil, targetSize, spanCh, useSimpleImportSpans)
	close(spanCh)

	if err != nil {
		return nil, err
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return cover, nil
}

type coverutils struct {
	dir cloudpb.ExternalStorage
}

func makeCoverUtils(ctx context.Context, t *testing.T, execCfg *sql.ExecutorConfig) coverutils {
	es, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx,
		fmt.Sprintf("nodelocal://1/mock%s", timeutil.Now().String()), username.RootUserName())
	require.NoError(t, err)
	dir := es.Conf()
	return coverutils{
		dir: dir,
	}
}

func (c coverutils) sp(start, end string) roachpb.Span {
	return roachpb.Span{Key: roachpb.Key(start), EndKey: roachpb.Key(end)}
}

func (c coverutils) makeManifests(manifests []roachpb.Spans) []backuppb.BackupManifest {
	ms := make([]backuppb.BackupManifest, len(manifests))
	fileCount := 1
	for i, manifest := range manifests {
		ms[i].StartTime = hlc.Timestamp{WallTime: int64(i)}
		ms[i].EndTime = hlc.Timestamp{WallTime: int64(i + 1)}
		ms[i].Files = make([]backuppb.BackupManifest_File, len(manifest))
		ms[i].Dir = c.dir
		for j, sp := range manifest {
			ms[i].Files[j] = backuppb.BackupManifest_File{
				Span: sp,
				Path: fmt.Sprintf("%d", fileCount),

				// Pretend every span has 1MB.
				EntryCounts: roachpb.RowCount{DataSize: 1 << 20},
			}
			fileCount++
		}
	}
	return ms
}

func (c coverutils) paths(names ...string) []execinfrapb.RestoreFileSpec {
	r := make([]execinfrapb.RestoreFileSpec, len(names))
	for i := range names {
		r[i].Path = names[i]
		r[i].Dir = c.dir
	}
	return r
}
func TestRestoreEntryCoverExample(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1
	ctx := context.Background()

	tc, _, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts,
		InitManualReplication)
	defer cleanupFn()

	execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)
	c := makeCoverUtils(ctx, t, &execCfg)

	// Setup and test the example in the comment of makeSimpleImportSpans.
	spans := []roachpb.Span{c.sp("a", "f"), c.sp("f", "i"), c.sp("l", "m")}

	backups := c.makeManifests([]roachpb.Spans{
		{c.sp("a", "c"), c.sp("c", "e"), c.sp("h", "i")},
		{c.sp("b", "d"), c.sp("g", "i")},
		{c.sp("a", "h"), c.sp("j", "k")},
		{c.sp("h", "i"), c.sp("l", "m")}})

	emptySpanFrontier, err := spanUtils.MakeFrontier(roachpb.Span{})
	require.NoError(t, err)

	layerToIterFactory, err := backupinfo.GetBackupManifestIterFactories(ctx, execCfg.DistSQLSrv.ExternalStorage,
		backups, nil, nil)
	require.NoError(t, err)
	cover, err := makeImportSpans(ctx, spans, backups, layerToIterFactory, noSpanTargetSize, emptySpanFrontier, false)
	require.NoError(t, err)
	require.Equal(t, []execinfrapb.RestoreSpanEntry{
		{Span: c.sp("a", "b"), Files: c.paths("1", "6")},
		{Span: c.sp("b", "c"), Files: c.paths("1", "4", "6")},
		{Span: c.sp("c", "f"), Files: c.paths("2", "4", "6")},
		{Span: c.sp("f", "g"), Files: c.paths("6")},
		{Span: c.sp("g", "h"), Files: c.paths("5", "6")},
		{Span: c.sp("h", "i"), Files: c.paths("3", "5", "8")},
		{Span: c.sp("l", "m"), Files: c.paths("9")},
	}, cover)

	coverSized, err := makeImportSpans(ctx, spans, backups, layerToIterFactory, 2<<20, emptySpanFrontier, false)
	require.NoError(t, err)
	require.Equal(t, []execinfrapb.RestoreSpanEntry{
		{Span: c.sp("a", "b"), Files: c.paths("1", "6")},
		{Span: c.sp("b", "c"), Files: c.paths("1", "4", "6")},
		{Span: c.sp("c", "f"), Files: c.paths("2", "4", "6")},
		{Span: c.sp("f", "h"), Files: c.paths("5", "6")},
		{Span: c.sp("h", "i"), Files: c.paths("3", "5", "8")},
		{Span: c.sp("l", "m"), Files: c.paths("9")},
	}, coverSized)

	// check that introduced spans are properly elided
	backups[2].IntroducedSpans = []roachpb.Span{c.sp("a", "f")}
	introducedSpanFrontier, err := createIntroducedSpanFrontier(backups, hlc.Timestamp{})
	require.NoError(t, err)

	coverIntroduced, err := makeImportSpans(ctx, spans, backups, layerToIterFactory, noSpanTargetSize, introducedSpanFrontier, false)
	require.NoError(t, err)
	require.Equal(t, []execinfrapb.RestoreSpanEntry{
		{Span: c.sp("a", "f"), Files: c.paths("6")},
		{Span: c.sp("f", "g"), Files: c.paths("6")},
		{Span: c.sp("g", "h"), Files: c.paths("5", "6")},
		{Span: c.sp("h", "i"), Files: c.paths("3", "5", "8")},
		{Span: c.sp("l", "m"), Files: c.paths("9")},
	}, coverIntroduced)
}

func TestFileSpanStartKeyIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	c := makeCoverUtils(ctx, t, &execCfg)

	type testSpec struct {
		manifestFiles []roachpb.Spans
		keysSurfaced  []string
		expectedError string
	}

	for _, sp := range []testSpec{
		{
			// adjacent and disjoint files.
			manifestFiles: []roachpb.Spans{
				{c.sp("a", "b"), c.sp("c", "d"), c.sp("d", "e")},
			},
			keysSurfaced: []string{"a", "b", "c", "d", "e"},
		},
		{
			// shadow start key (b) if another span covers it.
			manifestFiles: []roachpb.Spans{
				{c.sp("a", "c"), c.sp("b", "d")},
			},
			keysSurfaced: []string{"a", "c", "d"},
		},
		{
			// swap the file order and expect an error.
			manifestFiles: []roachpb.Spans{
				{c.sp("b", "d"), c.sp("a", "c")},
			},
			keysSurfaced:  []string{"b", "d", "a", "c"},
			expectedError: "out of order backup keys",
		},
		{
			// overlapping files within a level.
			manifestFiles: []roachpb.Spans{
				{c.sp("b", "f"), c.sp("c", "d"), c.sp("e", "g")},
			},
			keysSurfaced: []string{"b", "f", "g"},
		},
		{
			// overlapping files within and across levels.
			manifestFiles: []roachpb.Spans{
				{c.sp("a", "e"), c.sp("d", "f")},
				{c.sp("b", "c")},
			},
			keysSurfaced: []string{"a", "b", "c", "e", "f"},
		},
		{
			// overlapping start key in one level, but non overlapping in another level.
			manifestFiles: []roachpb.Spans{
				{c.sp("a", "c"), c.sp("b", "d")},
				{c.sp("b", "c")},
			},
			keysSurfaced: []string{"a", "b", "c", "d"},
		},
		{
			// overlapping files in both levels.
			manifestFiles: []roachpb.Spans{
				{c.sp("b", "e"), c.sp("d", "i")},
				{c.sp("a", "c"), c.sp("b", "h")},
			},
			keysSurfaced: []string{"a", "b", "c", "e", "h", "i"},
		},
		{
			// ensure everything works with 3 layers.
			manifestFiles: []roachpb.Spans{
				{c.sp("a", "e"), c.sp("e", "f")},
				{c.sp("b", "e"), c.sp("e", "f")},
				{c.sp("c", "e"), c.sp("d", "f")},
			},
			keysSurfaced: []string{"a", "b", "c", "e", "f"},
		},
	} {
		backups := c.makeManifests(sp.manifestFiles)

		// randomly shuffle the order of the manifests, as order should not matter.
		for i := range backups {
			j := rand.Intn(i + 1)
			backups[i], backups[j] = backups[j], backups[i]
		}

		// ensure all the expected keys are surfaced.
		layerToBackupManifestFileIterFactory, err := backupinfo.GetBackupManifestIterFactories(ctx, execCfg.DistSQLSrv.ExternalStorage,
			backups, nil, nil)
		require.NoError(t, err)

		sanityCheckFileIterator(ctx, t, layerToBackupManifestFileIterFactory[0], backups[0])

		startEndKeyIt, err := newFileSpanStartAndEndKeyIterator(ctx, backups, layerToBackupManifestFileIterFactory)
		require.NoError(t, err)

		for _, expectedKey := range sp.keysSurfaced {
			if ok, err := startEndKeyIt.valid(); !ok {
				if err != nil {
					require.Error(t, err, sp.expectedError)
				}
				break
			}
			expected := roachpb.Key(expectedKey)
			require.Equal(t, expected, startEndKeyIt.value())
			startEndKeyIt.next()
		}
	}
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

	ctx := context.Background()
	es, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx,
		fmt.Sprintf("nodelocal://1/mock%s", timeutil.Now().String()), username.RootUserName())
	require.NoError(t, err)

	return backuppb.BackupManifest{Spans: spans,
		EndTime: endTime, Files: files, Dir: es.Conf()}
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

			layerToIterFactory, err := backupinfo.GetBackupManifestIterFactories(ctx,
				execCfg.DistSQLSrv.ExternalStorage, backups, nil, nil)
			require.NoError(t, err)
			cover, err := makeImportSpans(ctx, restoreSpans, backups, layerToIterFactory,
				0, introducedSpanFrontier, false)
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

// sanityCheckFileIterator ensures the backup files are surfaced in the order they are stored in
// the manifest.
func sanityCheckFileIterator(
	ctx context.Context,
	t *testing.T,
	iterFactory *backupinfo.IterFactory,
	backup backuppb.BackupManifest,
) {
	iter, err := iterFactory.NewFileIter(ctx)
	require.NoError(t, err)
	defer iter.Close()

	for _, expectedFile := range backup.Files {
		if ok, err := iter.Valid(); err != nil {
			t.Fatal(err)
		} else if !ok {
			t.Fatalf("file iterator should have file with path %s", expectedFile.Path)
		}

		file := iter.Value()
		require.Equal(t, expectedFile, *file)
		iter.Next()
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
				for _, hasExternalFilesList := range []bool{true, false} {
					for _, simpleImportSpans := range []bool{true, false} {
						backups, err := MockBackupChain(ctx, numBackups, spans, files, r, hasExternalFilesList, execCfg)
						require.NoError(t, err)
						layerToIterFactory, err := backupinfo.GetBackupManifestIterFactories(ctx,
							execCfg.DistSQLSrv.ExternalStorage, backups, nil, nil)
						require.NoError(t, err)
						randLayer := rand.Intn(len(backups))
						randBackup := backups[randLayer]
						sanityCheckFileIterator(ctx, t, layerToIterFactory[randLayer], randBackup)
						for _, target := range []int64{0, 1, 4, 100, 1000} {
							t.Run(fmt.Sprintf("numBackups=%d, numSpans=%d, numFiles=%d, merge=%d, slim=%t, simple=%t",
								numBackups, spans, files, target, hasExternalFilesList, simpleImportSpans), func(t *testing.T) {
								introducedSpanFrontier, err := createIntroducedSpanFrontier(backups, hlc.Timestamp{})
								require.NoError(t, err)
								cover, err := makeImportSpans(ctx, backups[numBackups-1].Spans, backups,
									layerToIterFactory, target<<20, introducedSpanFrontier, simpleImportSpans)
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
}
