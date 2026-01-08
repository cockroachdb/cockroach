// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupinfo

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"path"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/backup/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/backup/backuputils"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestGetBackupIndexFileName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type coverageTime struct {
		// startTime and endTime represents hours in a single day. Because the
		// generated filenames are only millisecond specific, it's just easier this
		// way to ensure that the gaps between start and end time are large enough
		// to have a meaningful difference in the generated filenames.
		startTime int
		endTime   int
	}
	// Inputs should be sorted in the order you expect their outputted filenames
	// to be sorted in.
	inputs := []coverageTime{
		{startTime: 10, endTime: 12},
		{startTime: 2, endTime: 10}, // Compacted backup timestamp
		{startTime: 8, endTime: 10},
		{startTime: 4, endTime: 8},
		{startTime: 2, endTime: 4},
		{startTime: 0, endTime: 2},
	}

	var sortedFilenames []string
	timeToFilenames := make(map[coverageTime]string)
	for _, input := range inputs {
		start := time.Date(2025, 7, 18, input.startTime, 0, 0, 0, time.UTC)
		end := time.Date(2025, 7, 18, input.endTime, 0, 0, 0, time.UTC)
		name := getBackupIndexFileName(
			hlc.Timestamp{WallTime: start.UnixNano()},
			hlc.Timestamp{WallTime: end.UnixNano()},
		)
		if _, exists := timeToFilenames[input]; exists {
			t.Fatalf("duplicate index file name generated for %v", input)
		}
		timeToFilenames[input] = name
		sortedFilenames = append(sortedFilenames, name)
	}

	slices.Sort(sortedFilenames)
	expectedSortedFilenames := util.Map(
		inputs,
		func(input coverageTime) string {
			return timeToFilenames[input]
		},
	)

	require.Equal(
		t, expectedSortedFilenames, sortedFilenames, "sort order of index filenames does not match",
	)
}

func TestWriteBackupIndexMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.Latest.Version(),
		clusterversion.Latest.Version(),
		true,
	)
	const collectionURI = "nodelocal://1/backup"
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	externalStorage, err := cloud.ExternalStorageFromURI(
		ctx,
		collectionURI,
		base.ExternalIODirConfig{},
		st,
		blobs.TestBlobServiceClient(dir),
		username.RootUserName(),
		nil, /* db */
		nil, /* limiters */
		cloud.NilMetrics,
	)
	require.NoError(t, err)
	makeExternalStorage := func(
		_ context.Context, _ string, _ username.SQLUsername, _ ...cloud.ExternalStorageOption,
	) (cloud.ExternalStorage, error) {
		return externalStorage, nil
	}

	execCfg := &sql.ExecutorConfig{Settings: st}
	start := hlc.Timestamp{WallTime: 0}
	end := hlc.Timestamp{WallTime: time.Date(2025, 7, 30, 0, 0, 0, 0, time.UTC).UnixNano()}
	subdir := "/2025/07/18-143826.00"

	details := jobspb.BackupDetails{
		Destination: jobspb.BackupDetails_Destination{
			To:     []string{collectionURI},
			Subdir: subdir,
		},
		StartTime:     start,
		EndTime:       end,
		CollectionURI: collectionURI,
		URI:           collectionURI + subdir,
	}

	require.NoError(t, WriteBackupIndexMetadata(
		ctx, execCfg, username.RootUserName(), makeExternalStorage, details, hlc.Timestamp{},
	))

	filepath, err := getBackupIndexFilePath(subdir, start, end)
	require.NoError(t, err)

	reader, nBytes, err := externalStorage.ReadFile(ctx, filepath, cloud.ReadOptions{})
	require.NoError(t, err)

	contents := make([]byte, nBytes)
	_, err = reader.Read(ctx, contents)
	require.NoError(t, err)

	var metadata backuppb.BackupIndexMetadata
	require.NoError(t, protoutil.Unmarshal(contents, &metadata))

	require.Equal(t, start, metadata.StartTime)
	require.Equal(t, end, metadata.EndTime)
	require.Equal(t, subdir, metadata.Path)
}

func TestWriteBackupIndexMetadataWithLocalityAwareBackups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tempDir, tempDirCleanup := testutils.TempDir(t)
	defer tempDirCleanup()
	_, sqlDB, _, cleanup := backuptestutils.StartBackupRestoreTestCluster(
		t, 1, backuptestutils.WithTempDir(tempDir),
	)
	defer cleanup()

	collections := `('nodelocal://1/us-west?COCKROACH_LOCALITY=region%3Dus-west',
		'nodelocal://1/us-east?COCKROACH_LOCALITY=default')`

	sqlDB.Exec(t, fmt.Sprintf(`BACKUP INTO %s`, collections))
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP INTO LATEST IN %s`, collections))

	indexDir := path.Join(tempDir, "us-east", backupbase.BackupIndexDirectoryPath)
	fullIndexes, err := os.ReadDir(indexDir)
	require.NoError(t, err)
	require.Len(t, fullIndexes, 1)

	chainIndexes, err := os.ReadDir(path.Join(indexDir, fullIndexes[0].Name()))
	require.NoError(t, err)
	require.Len(t, chainIndexes, 2)

	var indexes []backuppb.BackupIndexMetadata
	for _, indexFile := range chainIndexes {
		indexPath := path.Join(indexDir, fullIndexes[0].Name(), indexFile.Name())
		contents, err := os.ReadFile(indexPath)
		require.NoError(t, err)

		var metadata backuppb.BackupIndexMetadata
		require.NoError(t, protoutil.Unmarshal(contents, &metadata))
		indexes = append(indexes, metadata)
	}

	fullIndex, incrIndex := indexes[0], indexes[1]
	if incrIndex.StartTime.IsEmpty() {
		fullIndex, incrIndex = incrIndex, fullIndex
	}

	require.True(t, fullIndex.StartTime.IsEmpty())
	require.False(t, incrIndex.StartTime.IsEmpty())
	require.True(t, strings.HasPrefix(
		incrIndex.Path,
		"/"+path.Join(backupbase.DefaultIncrementalsSubdir, fullIndex.Path),
	))
}

func TestListIndexesHandlesInvalidFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.Latest.Version(),
		clusterversion.Latest.Version(),
		true,
	)
	execCfg := &sql.ExecutorConfig{Settings: st}
	const collectionURI = "nodelocal://1/backup"
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	externalStorage, err := cloud.ExternalStorageFromURI(
		ctx,
		collectionURI,
		base.ExternalIODirConfig{},
		st,
		blobs.TestBlobServiceClient(dir),
		username.RootUserName(),
		nil, /* db */
		nil, /* limiters */
		cloud.NilMetrics,
	)
	require.NoError(t, err)
	makeExternalStorage := func(
		_ context.Context, _ string, _ username.SQLUsername, _ ...cloud.ExternalStorageOption,
	) (cloud.ExternalStorage, error) {
		return externalStorage, nil
	}

	subdir := "/2025/07/18-120000.00"
	// Write 3 valid index files.
	zeroTime := time.Unix(0, 0).UTC()
	fullBackupEndTime := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	incBackup1EndTime := time.Date(2025, 7, 18, 13, 0, 0, 0, time.UTC)
	incBackup2EndTime := time.Date(2025, 7, 18, 14, 0, 0, 0, time.UTC)
	backupTimes := [][2]time.Time{
		{zeroTime, fullBackupEndTime},
		{fullBackupEndTime, incBackup1EndTime},
		{incBackup1EndTime, incBackup2EndTime},
	}

	for _, times := range backupTimes {
		details := jobspb.BackupDetails{
			Destination: jobspb.BackupDetails_Destination{
				To:     []string{collectionURI},
				Subdir: subdir,
			},
			StartTime:     hlc.Timestamp{WallTime: times[0].UnixNano()},
			EndTime:       hlc.Timestamp{WallTime: times[1].UnixNano()},
			CollectionURI: collectionURI,
			URI:           collectionURI + subdir,
		}
		require.NoError(t, WriteBackupIndexMetadata(
			ctx, execCfg, username.RootUserName(), makeExternalStorage, details, hlc.Timestamp{},
		))
	}

	indexDir := path.Join(
		backupbase.BackupIndexDirectoryPath,
		backuputils.EncodeDescendingTS(fullBackupEndTime)+"_"+
			fullBackupEndTime.Format(backupbase.BackupIndexFilenameTimestampFormat),
	)

	t.Run("non .pb files should be skipped", func(t *testing.T) {
		validFilename := getBackupIndexFileName(
			hlc.Timestamp{WallTime: zeroTime.UnixNano()},
			hlc.Timestamp{WallTime: fullBackupEndTime.UnixNano()},
		)
		tmpFile := path.Join(indexDir, validFilename+"123.tmp")
		writer1, err := externalStorage.Writer(ctx, tmpFile)
		require.NoError(t, err)
		require.NoError(t, writer1.Close())
		defer func() {
			err := externalStorage.Delete(ctx, tmpFile)
			require.NoError(t, err)
		}()

		indexes, err := ListIndexes(ctx, externalStorage, subdir)
		require.NoError(t, err)
		require.Len(t, indexes, 3)
	})

	t.Run("invalid .pb files should error", func(t *testing.T) {
		invalidTSFile := path.Join(indexDir, "invalid_badts_notreal_metadata.pb")
		writer2, err := externalStorage.Writer(ctx, invalidTSFile)
		require.NoError(t, err)
		require.NoError(t, writer2.Close())
		defer func() {
			err := externalStorage.Delete(ctx, invalidTSFile)
			require.NoError(t, err)
		}()

		_, err = ListIndexes(ctx, externalStorage, subdir)
		require.Error(t, err)
	})
}

func TestDontWriteBackupIndexMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var externalStorage cloud.ExternalStorage
	var err error
	makeExternalStorage := func(
		_ context.Context, _ string, _ username.SQLUsername, _ ...cloud.ExternalStorageOption,
	) (cloud.ExternalStorage, error) {
		return externalStorage, nil
	}

	subdir := "/2025/07/18-143826.00"
	details := jobspb.BackupDetails{
		Destination: jobspb.BackupDetails_Destination{
			To:     []string{"nodelocal://1/backup"},
			Subdir: subdir,
		},
		CollectionURI: "nodelocal://1/backup",
	}

	t.Run("pre v25.4 version", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettingsWithVersions(
			clusterversion.V25_3.Version(),
			clusterversion.V25_3.Version(),
			true,
		)
		const collectionURI = "nodelocal://1/backup"
		dir, dirCleanupFn := testutils.TempDir(t)
		defer dirCleanupFn()
		externalStorage, err = cloud.ExternalStorageFromURI(
			ctx,
			collectionURI,
			base.ExternalIODirConfig{},
			st,
			blobs.TestBlobServiceClient(dir),
			username.RootUserName(),
			nil, /* db */
			nil, /* limiters */
			cloud.NilMetrics,
		)
		require.NoError(t, err)
		execCfg := &sql.ExecutorConfig{Settings: st}

		start := hlc.Timestamp{}
		end := hlc.Timestamp{WallTime: 20}
		details.StartTime = start
		details.EndTime = end

		require.NoError(t, WriteBackupIndexMetadata(
			ctx, execCfg, username.RootUserName(), makeExternalStorage, details, hlc.Timestamp{},
		))

		filepath, err := getBackupIndexFilePath(subdir, start, end)
		require.NoError(t, err)

		_, _, err = externalStorage.ReadFile(ctx, filepath, cloud.ReadOptions{})
		require.ErrorContains(t, err, "does not exist")
	})

	t.Run("missing full backup index", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettingsWithVersions(
			clusterversion.Latest.Version(),
			clusterversion.Latest.Version(),
			true,
		)
		const collectionURI = "nodelocal://1/backup"
		dir, dirCleanupFn := testutils.TempDir(t)
		defer dirCleanupFn()
		externalStorage, err = cloud.ExternalStorageFromURI(
			ctx,
			collectionURI,
			base.ExternalIODirConfig{},
			st,
			blobs.TestBlobServiceClient(dir),
			username.RootUserName(),
			nil, /* db */
			nil, /* limiters */
			cloud.NilMetrics,
		)
		require.NoError(t, err)
		execCfg := &sql.ExecutorConfig{Settings: st}

		start := hlc.Timestamp{WallTime: 10}
		end := hlc.Timestamp{WallTime: 20}
		details.StartTime = start
		details.EndTime = end

		require.NoError(t, WriteBackupIndexMetadata(
			ctx, execCfg, username.RootUserName(), makeExternalStorage, details, hlc.Timestamp{},
		))

		filepath, err := getBackupIndexFilePath(subdir, start, end)
		require.NoError(t, err)

		_, _, err = externalStorage.ReadFile(ctx, filepath, cloud.ReadOptions{})
		require.ErrorContains(t, err, "does not exist")
	})
}

func TestIndexExists(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type indexTime struct {
		start time.Time
		end   time.Time
	}
	type subdir struct {
		name    string
		backups []indexTime
	}
	type testcase struct {
		name           string
		subdirs        []subdir
		targetSubdir   string
		expectedExists bool
	}

	ctx := context.Background()

	const collectionURI = "nodelocal://1/backup"
	const subdir1 = "/2025/07/18-222500.00"
	const subdir2 = "/2025/07/19-123456.00"
	st := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.Latest.Version(),
		clusterversion.Latest.Version(),
		true,
	)
	execCfg := &sql.ExecutorConfig{Settings: st}

	zeroTime := time.Unix(0, 0).UTC()
	fullBackupEndTime := time.Date(2025, 7, 18, 12, 0, 0, 0, time.UTC)
	incBackup1EndTime := time.Date(2025, 7, 18, 13, 0, 0, 0, time.UTC)
	incBackup2EndTime := time.Date(2025, 7, 18, 14, 0, 0, 0, time.UTC)

	testcases := []testcase{
		{
			name: "just full backup",
			subdirs: []subdir{
				{
					name: subdir1,
					backups: []indexTime{
						{start: zeroTime, end: fullBackupEndTime},
					},
				},
			},
			targetSubdir:   subdir1,
			expectedExists: true,
		},
		{
			name: "full backup and incrementals",
			subdirs: []subdir{
				{
					name: subdir1,
					backups: []indexTime{
						{start: zeroTime, end: fullBackupEndTime},
						{start: fullBackupEndTime, end: incBackup1EndTime},
						{start: incBackup1EndTime, end: incBackup2EndTime},
					},
				},
			},
			targetSubdir:   subdir1,
			expectedExists: true,
		},
		{
			name: "found index with multiple non-empty backup chains",
			subdirs: []subdir{
				{
					name: subdir1,
					backups: []indexTime{
						{start: zeroTime, end: fullBackupEndTime},
						{start: fullBackupEndTime, end: incBackup1EndTime},
						{start: incBackup1EndTime, end: incBackup2EndTime},
					},
				},
				{
					name: subdir2,
					backups: []indexTime{
						{start: zeroTime, end: fullBackupEndTime},
						{start: fullBackupEndTime, end: incBackup1EndTime},
					},
				},
			},
			targetSubdir:   subdir2,
			expectedExists: true,
		},
		{
			name: "incremental backups but no full backup",
			subdirs: []subdir{
				{
					name: subdir1,
					backups: []indexTime{
						{start: fullBackupEndTime, end: incBackup1EndTime},
						{start: incBackup1EndTime, end: incBackup2EndTime},
					},
				},
			},
			targetSubdir:   subdir1,
			expectedExists: false,
		},
		{
			name:           "no indexes",
			subdirs:        nil,
			targetSubdir:   subdir1,
			expectedExists: false,
		},
		{
			name: "non-empty index but missing subdir",
			subdirs: []subdir{
				{
					name: subdir1,
					backups: []indexTime{
						{start: zeroTime, end: fullBackupEndTime},
					},
				},
			},
			targetSubdir:   subdir2,
			expectedExists: false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			dir, dirCleanupFn := testutils.TempDir(t)
			defer dirCleanupFn()
			externalStorage, err := cloud.ExternalStorageFromURI(
				ctx,
				collectionURI,
				base.ExternalIODirConfig{},
				st,
				blobs.TestBlobServiceClient(dir),
				username.RootUserName(),
				nil, /* db */
				nil, /* limiters */
				cloud.NilMetrics,
			)
			require.NoError(t, err)
			makeExternalStorage := func(
				_ context.Context, _ string, _ username.SQLUsername, _ ...cloud.ExternalStorageOption,
			) (cloud.ExternalStorage, error) {
				return externalStorage, nil
			}

			// Fill up our external storage with the index files
			for _, sub := range tc.subdirs {
				for _, file := range sub.backups {
					details := jobspb.BackupDetails{
						Destination: jobspb.BackupDetails_Destination{
							To:     []string{collectionURI},
							Subdir: sub.name,
						},
						StartTime:     hlc.Timestamp{WallTime: file.start.UnixNano()},
						EndTime:       hlc.Timestamp{WallTime: file.end.UnixNano()},
						CollectionURI: collectionURI,
						// URI doesn't need to be set properly for this test since we are
						// not opening the index files.
						URI: collectionURI + "/" + sub.name,
					}
					require.NoError(t, WriteBackupIndexMetadata(
						ctx, execCfg, username.RootUserName(), makeExternalStorage, details, hlc.Timestamp{},
					))
				}
			}

			exists, err := IndexExists(
				ctx, externalStorage, tc.targetSubdir,
			)
			require.NoError(t, err)
			require.Equal(t, tc.expectedExists, exists)
		})
	}
}

func TestGetBackupTreeIndexMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.Latest.Version(),
		clusterversion.Latest.Version(),
		true,
	)
	execCfg := &sql.ExecutorConfig{Settings: st}

	const collectionURI = "nodelocal://1/backup"
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	externalStorage, err := cloud.ExternalStorageFromURI(
		ctx,
		collectionURI,
		base.ExternalIODirConfig{},
		st,
		blobs.TestBlobServiceClient(dir),
		username.RootUserName(),
		nil, /* db */
		nil, /* limiters */
		cloud.NilMetrics,
	)
	require.NoError(t, err)
	storageFactory := func(
		_ context.Context, _ string, _ username.SQLUsername, _ ...cloud.ExternalStorageOption,
	) (cloud.ExternalStorage, error) {
		return externalStorage, nil
	}

	simpleChain := chain(b(0, 2), b(2, 4), b(4, 6), b(6, 8))
	compactedChain := chain(b(0, 10), b(10, 11), b(10, 12), b(11, 12), b(12, 14), b(14, 16))
	doubleCompactedChain := chain(b(0, 18), b(18, 20), b(18, 22), b(20, 22), b(22, 24), b(18, 26), b(24, 26))
	fullOnly := chain(b(0, 28))

	fakeBackupCollection{
		simpleChain,
		compactedChain,
		doubleCompactedChain,
		fullOnly,
	}.writeIndexes(t, ctx, execCfg, storageFactory, collectionURI)

	testcases := []struct {
		name  string
		chain fakeBackupChain
		error string
		// expectedIndexTimes should be sorted in ascending order by end time, with
		// ties broken by ascending start time.
		expectedIndexTimes [][2]int
	}{
		{
			name:               "fetch all indexes from chain with no compacted backups",
			chain:              simpleChain,
			expectedIndexTimes: [][2]int{{0, 2}, {2, 4}, {4, 6}, {6, 8}},
		},
		{
			name:               "fetch all indexes from tree with compacted backups",
			chain:              compactedChain,
			expectedIndexTimes: [][2]int{{0, 10}, {10, 11}, {10, 12}, {11, 12}, {12, 14}, {14, 16}},
		},
		{
			name:  "fetch all indexes from tree with double compacted backups",
			chain: doubleCompactedChain,
			expectedIndexTimes: [][2]int{
				{0, 18}, {18, 20}, {18, 22}, {20, 22}, {22, 24}, {18, 26}, {24, 26},
			},
		},
		{
			name:               "index only contains a full backup",
			chain:              fullOnly,
			expectedIndexTimes: [][2]int{{0, 28}},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			subdirTS := intToTimeWithNano(tc.chain[0].end).GoTime()
			subdir := subdirTS.Format(backupbase.DateBasedIntoFolderName)

			metadatas, err := GetBackupTreeIndexMetadata(ctx, externalStorage, subdir)
			if tc.error != "" {
				require.ErrorContains(t, err, tc.error)
				return
			}

			require.NoError(t, err)
			require.Len(t, metadatas, len(tc.expectedIndexTimes))

			// Using [2]int64 makes test outputs in event of failure more readable.
			expectedIndexTimes := util.Map(tc.expectedIndexTimes, func(t [2]int) [2]int64 {
				return [...]int64{
					intToTimeWithNano(t[0]).WallTime,
					intToTimeWithNano(t[1]).WallTime,
				}
			})
			actualIndexTimes := util.Map(metadatas, func(m backuppb.BackupIndexMetadata) [2]int64 {
				return [...]int64{m.StartTime.WallTime, m.EndTime.WallTime}
			})

			require.Equal(t, expectedIndexTimes, actualIndexTimes)
		})
	}
}

func TestListRestorableBackups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	execCfg := &sql.ExecutorConfig{Settings: st}
	const collectionURI = "nodelocal://1/backup"
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	externalStorage, err := cloud.ExternalStorageFromURI(
		ctx,
		collectionURI,
		base.ExternalIODirConfig{},
		st,
		blobs.TestBlobServiceClient(dir),
		username.RootUserName(),
		nil, /* db */
		nil, /* limiters */
		cloud.NilMetrics,
	)
	require.NoError(t, err)
	makeExternalStorage := func(
		_ context.Context, _ string, _ username.SQLUsername, _ ...cloud.ExternalStorageOption,
	) (cloud.ExternalStorage, error) {
		return externalStorage, nil
	}

	fakeBackupCollection{
		{
			// Simple chain.
			b(0, 2), b(2, 4), b(4, 6),
		},
		{
			// Chain with compacted backup and multiple backups intersects next chain.
			b(0, 10), b(10, 14), b(14, 18), b(10, 22), b(18, 22), b(22, 26), b(22, 28),
		},
		{
			// Chain with double compacted backups.
			b(0, 24), b(24, 30), b(30, 32), b(24, 36), b(32, 36), b(36, 40), b(24, 44), b(40, 44),
		},
		{
			// Chain with more interesting overlaps with next chain
			// (inc with same time as full, compacted backup during full).
			b(0, 46), b(46, 48), b(48, 50), b(50, 52), b(52, 54), b(50, 56), b(54, 56),
		},
		{
			b(0, 50), b(50, 58),
		},
		{
			// Chain with revision history.
			bRH(0, 60), bRH(60, 62), bRH(62, 64), bRH(60, 66), bRH(64, 66),
		},
	}.writeIndexes(t, ctx, execCfg, makeExternalStorage, collectionURI)

	type output struct {
		end int
		rev bool
	}
	// Some helpers to make test cases more readable.
	o := func(endTimes ...int) []output {
		return util.Map(endTimes, func(t int) output { return output{end: t} })
	}
	oRH := func(endTimes ...int) []output {
		return util.Map(endTimes, func(t int) output { return output{end: t, rev: true} })
	}

	testcases := []struct {
		name           string
		after, before  int
		expectedOutput []output
	}{
		{
			"simple chain/full chain inclusive",
			1, 6,
			o(6, 4, 2),
		},
		{
			"simple chain/only incs",
			3, 6,
			o(6, 4),
		},
		{
			"simple chain/one matching backup",
			3, 5,
			o(4),
		},
		{
			"compacted chain/elided duplicates",
			15, 23,
			o(22, 18),
		},
		{
			"double compacted chain/elided duplicates",
			27, 45,
			o(44, 40, 36, 32, 30, 28),
		},
		{
			"collection/intersecting chains",
			24, 30,
			o(30, 28, 26, 24),
		},
		{
			"collection/intersecting excluding full",
			54, 58,
			o(58, 56, 54),
		},
		{
			"collection/inc with same end as full",
			48, 54,
			o(54, 52, 50, 50, 48),
		},
		{
			"revision history/ignore compacted",
			61, 66,
			oRH(66, 64, 62),
		},
		{
			"collection/all backups",
			0, 1000,
			append(
				oRH(66, 64, 62, 60),
				o(58, 56, 54, 52, 50, 50, 48, 46, 44, 40, 36,
					32, 30, 28, 26, 24, 22, 18, 14, 10, 6, 4, 2)...,
			),
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			afterTS := hlc.Timestamp{WallTime: int64(tc.after) * 1e9}.GoTime()
			beforeTS := hlc.Timestamp{WallTime: int64(tc.before) * 1e9}.GoTime()

			backups, err := ListRestorableBackups(
				ctx, externalStorage, afterTS, beforeTS,
			)
			require.NoError(t, err)

			actualOutput := util.Map(backups, func(b RestorableBackup) output {
				return output{end: int(b.EndTime.WallTime / 1e9), rev: !b.RevisionStartTime.IsEmpty()}
			})
			require.Equal(t, tc.expectedOutput, actualOutput)
		})
	}
}

func TestConvertIndexSubdirToSubdir(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	endTime := time.Date(2025, 12, 5, 0, 0, 0, 0, time.UTC)
	endTimeAsSubdir := endTime.Format(backupbase.DateBasedIntoFolderName)
	endTimeDescEnc := backuputils.EncodeDescendingTS(endTime)
	endTimeIndexSuffix := endTime.Format(backupbase.BackupIndexFilenameTimestampFormat)

	testcases := []struct {
		name           string
		indexSubdir    string
		expectedSubdir string
		error          string
	}{
		{
			name:           "valid index subdir",
			indexSubdir:    endTimeDescEnc + "_" + endTimeIndexSuffix,
			expectedSubdir: endTimeAsSubdir,
		},
		{
			name:        "index subdir missing two parts",
			indexSubdir: endTimeDescEnc,
			error:       "invalid index subdir format",
		},
		{
			name:        "index subdir with extra parts",
			indexSubdir: endTimeDescEnc + "_" + endTimeIndexSuffix + "_extra",
			error:       "invalid index subdir format",
		},
		{
			name:        "index subdir with invalid descending timestamp",
			indexSubdir: "invalid" + "_" + endTimeIndexSuffix,
			error:       "could not be decoded",
		},
		{
			name:        "index subdir with invalid timestamp suffix",
			indexSubdir: endTimeDescEnc + "_invalid",
			error:       "could not be decoded",
		},
		{
			name:        "index subdir with mismatched timestamps",
			indexSubdir: endTimeDescEnc + "_" + endTime.Add(time.Second).Format(backupbase.BackupIndexFilenameTimestampFormat),
			error:       "mismatched timestamps",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			subdir, err := convertIndexSubdirToSubdir(tc.indexSubdir)
			if tc.error != "" {
				require.ErrorContains(t, err, tc.error)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedSubdir, subdir)
		})
	}
}

// intToTimeWithNano converts the integer time an easy to read hlc.Timestamp
// (i.e. XX000000000XX). Note that the int is also added as nanoseconds to
// stress backup's handling of nanosecond precision timestamps. We use ints to
// more easily write times for test cases.
func intToTimeWithNano(t int) hlc.Timestamp {
	if t == 0 {
		return hlc.Timestamp{}
	}
	// Value needs to be large enough to be represented in milliseconds and be
	// larger than GoTime zero.
	return hlc.Timestamp{WallTime: int64(t)*1e9 + int64(t)}
}

// chain is a helper to concisely create a fakeBackupChain.
func chain(specs ...fakeBackupSpec) fakeBackupChain {
	return fakeBackupChain(specs)
}

// b is a helper to quickly create a fakeBackupSpec.
func b(start, end int) fakeBackupSpec {
	return fakeBackupSpec{start: start, end: end}
}

// bRH is a helper to quickly create a fakeBackupSpec with revision history.
func bRH(start, end int) fakeBackupSpec {
	return fakeBackupSpec{start: start, end: end, revHistory: true}
}

// fakeBackupCollection represents a collection of backup chains.
type fakeBackupCollection []fakeBackupChain

// fakeBackupChain represents a chain of backups. Every chain must contain a
// full backup (i.e. start == 0). Compacted backups are represented by having
// fullBackupSpecs with duplicate end times.
//
// This is used to easily create indexes that represent backup chains.
type fakeBackupChain []fakeBackupSpec

// fakeBackupSpec represents a single backup within a backup chain. The times
// are integers that will be converted to hlc.Timestamps using
// intToTimeWithNano.
type fakeBackupSpec struct {
	start, end int
	revHistory bool
}

// writeIndexes writes index metadata files for every backup in the collection.
func (c fakeBackupCollection) writeIndexes(
	t *testing.T,
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	storageFactory cloud.ExternalStorageFromURIFactory,
	collectionURI string,
) {
	t.Helper()
	for _, chain := range c {
		chain.writeIndexes(t, ctx, execCfg, storageFactory, collectionURI)
	}
}

// writeIndexes writes index metadata files for every backup in the chain.
func (c fakeBackupChain) writeIndexes(
	t *testing.T,
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	storageFactory cloud.ExternalStorageFromURIFactory,
	collectionURI string,
) {
	t.Helper()
	sorted := slices.Clone(c)
	slices.SortFunc(sorted, func(a, b fakeBackupSpec) int {
		if a.end < b.end {
			return -1
		} else if a.end > b.end {
			return 1
		}
		if a.start > b.start {
			return 1
		} else {
			return -1
		}
	})
	if sorted[0].start != 0 {
		t.Fatalf("backup chain does not contain a full backup")
	}
	subdir := intToTimeWithNano(sorted[0].end).GoTime().Format(backupbase.DateBasedIntoFolderName)

	// We write the indexes in a random order to test that the order they are
	// written does not matter. But fulls must always be written first or else
	// incrementals are not able to be written.
	shuffledIdx := append([]int{0}, util.Map(rand.Perm(len(sorted)-1), func(i int) int {
		return i + 1
	})...)
	for _, idx := range shuffledIdx {
		spec := sorted[idx]
		startTS, endTS := intToTimeWithNano(spec.start), intToTimeWithNano(spec.end)

		uri, err := url.Parse(collectionURI)
		require.NoError(t, err)
		if spec.start != 0 {
			uri.Path = path.Join(
				uri.Path,
				backupbase.DefaultIncrementalsSubdir,
				subdir,
				ConstructDateBasedIncrementalFolderName(startTS.GoTime(), endTS.GoTime()),
			)
		} else {
			uri.Path = path.Join(uri.Path, subdir)
		}

		isCompacted := idx < len(sorted)-1 && sorted[idx].end == sorted[idx+1].end
		revStartTS := hlc.Timestamp{}
		if !isCompacted && spec.revHistory {
			if startTS.IsEmpty() {
				revStartTS = hlc.Timestamp{WallTime: endTS.WallTime / 2}
			} else {
				revStartTS = startTS
			}
		}

		details := jobspb.BackupDetails{
			Destination: jobspb.BackupDetails_Destination{
				To:     []string{collectionURI},
				Subdir: subdir,
			},
			StartTime:       startTS,
			EndTime:         endTS,
			Compact:         isCompacted,
			CollectionURI:   collectionURI,
			URI:             uri.String(),
			RevisionHistory: !revStartTS.IsEmpty(),
		}
		require.NoError(
			t,
			WriteBackupIndexMetadata(
				ctx, execCfg, username.RootUserName(),
				storageFactory, details, revStartTS,
			),
		)
	}
}
