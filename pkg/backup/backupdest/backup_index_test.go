// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupdest

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"os"
	"path"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/backup/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
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

func TestGetBackupIndexFilePath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	start, end := hlc.Timestamp{WallTime: 10}, hlc.Timestamp{WallTime: 20}
	t.Run("fails if subdir is 'LATEST' and unresolved", func(t *testing.T) {
		_, err := getBackupIndexFilePath("LATEST", start, end)
		require.Error(t, err)
	})
	t.Run("returns correct path for resolved subdir", func(t *testing.T) {
		subdir := "/2025/07/17-152115.00"
		flattenedSubdir := "2025-07-17-152115.00"
		indexPath, err := getBackupIndexFilePath(subdir, start, end)
		require.NoError(t, err)
		require.True(
			t, strings.HasPrefix(
				indexPath,
				path.Join(backupbase.BackupIndexDirectoryPath, flattenedSubdir),
			),
		)
	})
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
	execCfg := &sql.ExecutorConfig{Settings: st}
	externalStorage := newFakeExternalStorage()
	makeExternalStorage := func(
		_ context.Context, _ string, _ username.SQLUsername, _ ...cloud.ExternalStorageOption,
	) (cloud.ExternalStorage, error) {
		return externalStorage, nil
	}

	start := hlc.Timestamp{WallTime: 0}
	end := hlc.Timestamp{WallTime: time.Date(2025, 7, 30, 0, 0, 0, 0, time.UTC).UnixNano()}
	collectionURI := "nodelocal://1/backup"
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
		ctx, execCfg, username.RootUserName(), makeExternalStorage, details,
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
}

func TestWriteBackupindexMetadataWithSpecifiedIncrementalLocation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tempDir, tempDirCleanup := testutils.TempDir(t)
	defer tempDirCleanup()
	_, sqlDB, _, cleanup := backuptestutils.StartBackupRestoreTestCluster(
		t, 1, backuptestutils.WithTempDir(tempDir),
	)
	defer cleanup()

	const collectionURI = "nodelocal://1/backup"
	const incLoc = "nodelocal://1/incremental_backup"

	sqlDB.Exec(t, "BACKUP INTO $1", collectionURI)
	sqlDB.Exec(t, "BACKUP INTO LATEST IN $1 WITH incremental_location=$2", collectionURI, incLoc)

	indexDir := path.Join(tempDir, "backup", backupbase.BackupIndexDirectoryPath)
	fullIndexes, err := os.ReadDir(indexDir)
	require.NoError(t, err)
	require.Len(t, fullIndexes, 1)

	chainIndexes, err := os.ReadDir(path.Join(indexDir, fullIndexes[0].Name()))
	require.NoError(t, err)

	// Since we specified an incremental location, we should not see an index
	// being written for the incremental backup.
	require.Len(t, chainIndexes, 1)
}

func TestDontWriteBackupIndexMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var externalStorage cloud.ExternalStorage
	makeExternalStorage := func(
		_ context.Context, _ string, _ username.SQLUsername, _ ...cloud.ExternalStorageOption,
	) (cloud.ExternalStorage, error) {
		return externalStorage, nil
	}

	subdir := "2025/07/18-143826.00"
	details := jobspb.BackupDetails{
		Destination: jobspb.BackupDetails_Destination{
			To:     []string{"nodelocal://1/backup"},
			Subdir: subdir,
		},
		CollectionURI: "nodelocal://1/backup",
	}

	t.Run("pre v25.4 version", func(t *testing.T) {
		externalStorage = newFakeExternalStorage()
		st := cluster.MakeTestingClusterSettingsWithVersions(
			clusterversion.V25_3.Version(),
			clusterversion.V25_3.Version(),
			true,
		)

		execCfg := &sql.ExecutorConfig{Settings: st}

		start := hlc.Timestamp{}
		end := hlc.Timestamp{WallTime: 20}
		details.StartTime = start
		details.EndTime = end

		require.NoError(t, WriteBackupIndexMetadata(
			ctx, execCfg, username.RootUserName(), makeExternalStorage, details,
		))

		filepath, err := getBackupIndexFilePath(subdir, start, end)
		require.NoError(t, err)

		_, _, err = externalStorage.ReadFile(ctx, filepath, cloud.ReadOptions{})
		require.ErrorContains(t, err, "does not exist")
	})

	t.Run("missing full backup index", func(t *testing.T) {
		externalStorage = newFakeExternalStorage()
		st := cluster.MakeTestingClusterSettingsWithVersions(
			clusterversion.Latest.Version(),
			clusterversion.Latest.Version(),
			true,
		)
		execCfg := &sql.ExecutorConfig{Settings: st}

		start := hlc.Timestamp{WallTime: 10}
		end := hlc.Timestamp{WallTime: 20}
		details.StartTime = start
		details.EndTime = end

		require.NoError(t, WriteBackupIndexMetadata(
			ctx, execCfg, username.RootUserName(), makeExternalStorage, details,
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
	const subdir1 = "/2025/07/18-222500.00/"
	const subdir2 = "/2025/07/19-123456.00/"
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
			externalStorage := newFakeExternalStorage()
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
						ctx, execCfg, username.RootUserName(), makeExternalStorage, details,
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

func TestListIndexes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	st := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.Latest.Version(),
		clusterversion.Latest.Version(),
		true,
	)
	execCfg := &sql.ExecutorConfig{Settings: st}

	const collectionURI = "nodelocal://1/test"
	storage, err := cloud.ExternalStorageFromURI(
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
	defer storage.Close()
	storageFactory := func(
		_ context.Context, _ string, _ username.SQLUsername, _ ...cloud.ExternalStorageOption,
	) (cloud.ExternalStorage, error) {
		return storage, nil
	}

	fullEnd := time.Date(2025, 8, 13, 0, 0, 0, 0, time.UTC)
	fullSubdir := fullEnd.Format(backupbase.DateBasedIntoFolderName)
	details := jobspb.BackupDetails{
		Destination: jobspb.BackupDetails_Destination{
			To:     []string{collectionURI},
			Subdir: fullSubdir,
		},
		StartTime:     hlc.Timestamp{},
		EndTime:       hlc.Timestamp{WallTime: fullEnd.UnixNano()},
		CollectionURI: collectionURI,
		// URI does not need to be set properly for this test since we are not
		// reading the contents of the files.
		URI: collectionURI + "/" + fullSubdir,
	}

	const numBackups = 5
	var start time.Time
	end := fullEnd
	for range numBackups - 1 {
		require.NoError(
			t, WriteBackupIndexMetadata(ctx, execCfg, username.RootUserName(), storageFactory, details),
		)
		start = end
		end = end.Add(1 * time.Hour)
		details.StartTime = hlc.Timestamp{WallTime: start.UnixNano()}
		details.EndTime = hlc.Timestamp{WallTime: end.UnixNano()}
	}
	// Write a compacted backup as well
	details.StartTime = hlc.Timestamp{WallTime: fullEnd.UnixNano()}
	details.EndTime = hlc.Timestamp{WallTime: start.UnixNano()}
	require.NoError(
		t, WriteBackupIndexMetadata(ctx, execCfg, username.RootUserName(), storageFactory, details),
	)

	indexes, err := ListIndexes(ctx, storage, fullSubdir)
	require.NoError(t, err)

	require.Len(t, indexes, numBackups)

	require.True(t, slices.IsSortedFunc(indexes, func(a, b string) int {
		aStart, aEnd, err := parseIndexFilename(a)
		require.NoError(t, err)
		bStart, bEnd, err := parseIndexFilename(b)
		require.NoError(t, err)
		if aEnd.Before(bEnd) {
			return -1
		} else if aEnd.After(bEnd) {
			return 1
		}
		// end times are equal, compare start times
		if bStart.Before(aStart) {
			return 1
		} else {
			return -1
		}
	}), "indexes are not sorted by end time and then start time")
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

	writeIndex := func(
		t *testing.T, subdirEnd, start, end int,
	) {
		t.Helper()
		subdirTS := time.Date(2025, 8, 12, subdirEnd, 0, 0, 0, time.UTC)
		startTS := hlc.Timestamp{WallTime: time.Date(2025, 8, 12, start, 0, 0, 0, time.UTC).UnixNano()}
		if start == 0 {
			startTS = hlc.Timestamp{}
		}
		endTS := hlc.Timestamp{WallTime: time.Date(2025, 8, 12, end, 0, 0, 0, time.UTC).UnixNano()}
		subdir := subdirTS.Format(backupbase.DateBasedIntoFolderName)

		details := jobspb.BackupDetails{
			Destination: jobspb.BackupDetails_Destination{
				To:     []string{collectionURI},
				Subdir: subdir,
			},
			StartTime:     startTS,
			EndTime:       endTS,
			CollectionURI: collectionURI,
			// This test doesn't look at the URI stored in the index metadata, so it
			// doesn't need to be accurate to the exact path differences between full
			// and incremental backups. We can just set URI to something that looks
			// valid.
			URI: collectionURI + subdir,
		}
		require.NoError(t, WriteBackupIndexMetadata(
			ctx, execCfg, username.RootUserName(), storageFactory, details,
		))
	}

	// Indexes represents the set of index files in the backup collection that the
	// test cases will be running against. We use ints to represent hours within a
	// single day to simplify test data.
	indexes := []struct {
		// endTime of the full backup.
		subdirEnd int
		// start and end times of all backups in the chain, including the full.
		times [][2]int
	}{
		{
			subdirEnd: 2,
			times:     [][2]int{{0, 2}, {2, 4}, {4, 6}, {6, 8}},
		},
		// Contains compacted backup covering t=10-14
		{
			subdirEnd: 10,
			times:     [][2]int{{0, 10}, {10, 11}, {11, 12}, {12, 14}, {10, 14}, {14, 16}, {16, 18}},
		},
		{
			subdirEnd: 20,
			times:     [][2]int{{0, 20}},
		},
	}
	for _, index := range indexes {
		for _, time := range index.times {
			writeIndex(t, index.subdirEnd, time[0], time[1])
		}
	}

	testcases := []struct {
		name string
		// subdir to get index metadata from. Refer to indexes above for valid
		// subdir end times.
		subdirEnd int
		// endTime filter. Set to 0 for no filter.
		endTime int
		error   string
		// expectedIndexTimes should be sorted in ascending order by end time, which
		// ties broken by ascending start time.
		expectedIndexTimes [][2]int
	}{
		{
			name:               "fetch all indexes from subdir",
			subdirEnd:          2,
			expectedIndexTimes: [][2]int{{0, 2}, {2, 4}, {4, 6}, {6, 8}},
		},
		{
			name:               "exact end time match",
			subdirEnd:          2,
			endTime:            6,
			expectedIndexTimes: [][2]int{{0, 2}, {2, 4}, {4, 6}},
		},
		{
			name:               "end time between an incremental",
			subdirEnd:          2,
			endTime:            5,
			expectedIndexTimes: [][2]int{{0, 2}, {2, 4}, {4, 6}},
		},
		{
			name:      "end time after the chain",
			subdirEnd: 2,
			endTime:   10,
			error:     "do not cover end time",
		},
		{
			name:               "fetch all indexes from tree with compacted backups",
			subdirEnd:          10,
			expectedIndexTimes: [][2]int{{0, 10}, {10, 11}, {11, 12}, {10, 14}, {12, 14}, {14, 16}, {16, 18}},
		},
		{
			name:               "end time of compacted backup",
			subdirEnd:          10,
			endTime:            14,
			expectedIndexTimes: [][2]int{{0, 10}, {10, 11}, {11, 12}, {10, 14}, {12, 14}},
		},
		{
			name:               "end time between incremental after compacted backup",
			subdirEnd:          10,
			endTime:            15,
			expectedIndexTimes: [][2]int{{0, 10}, {10, 11}, {11, 12}, {10, 14}, {12, 14}, {14, 16}},
		},
		{
			name:               "end time between compacted backup",
			subdirEnd:          10,
			endTime:            13,
			expectedIndexTimes: [][2]int{{0, 10}, {10, 11}, {11, 12}, {10, 14}, {12, 14}},
		},
		{
			name:               "end time before compacted backup",
			subdirEnd:          10,
			endTime:            12,
			expectedIndexTimes: [][2]int{{0, 10}, {10, 11}, {11, 12}},
		},
		{
			name:               "index only contains a full backup",
			subdirEnd:          20,
			expectedIndexTimes: [][2]int{{0, 20}},
		},
		{
			name:               "end time before full backup end",
			subdirEnd:          10,
			endTime:            5,
			expectedIndexTimes: [][2]int{{0, 10}},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			subdirTS := time.Date(2025, 8, 12, tc.subdirEnd, 0, 0, 0, time.UTC)
			subdir := subdirTS.Format(backupbase.DateBasedIntoFolderName)

			var end hlc.Timestamp
			if tc.endTime != 0 {
				end = hlc.Timestamp{WallTime: time.Date(2025, 8, 12, tc.endTime, 0, 0, 0, time.UTC).UnixNano()}
			}

			metadatas, err := GetBackupTreeIndexMetadata(ctx, externalStorage, subdir, end)

			if tc.error != "" {
				require.ErrorContains(t, err, tc.error)
				return
			}

			require.NoError(t, err)
			require.Len(t, metadatas, len(tc.expectedIndexTimes))

			expectedIndexTimes := util.Map(tc.expectedIndexTimes, func(t [2]int) [2]hlc.Timestamp {
				var startTS hlc.Timestamp
				if t[0] != 0 {
					startTS.WallTime = time.Date(2025, 8, 12, t[0], 0, 0, 0, time.UTC).UnixNano()
				}
				return [...]hlc.Timestamp{
					startTS,
					{WallTime: time.Date(2025, 8, 12, t[1], 0, 0, 0, time.UTC).UnixNano()},
				}
			})
			actualIndexTimes := util.Map(metadatas, func(m backuppb.BackupIndexMetadata) [2]hlc.Timestamp {
				return [...]hlc.Timestamp{m.StartTime, m.EndTime}
			})

			require.Equal(t, expectedIndexTimes, actualIndexTimes)
		})
	}
}

type fakeExternalStorage struct {
	cloud.ExternalStorage
	files map[string]*closableBytesWriter
}

var _ cloud.ExternalStorage = &fakeExternalStorage{}

func newFakeExternalStorage() *fakeExternalStorage {
	return &fakeExternalStorage{
		files: make(map[string]*closableBytesWriter),
	}
}

func (f *fakeExternalStorage) Close() error {
	return nil
}

func (f *fakeExternalStorage) Conf() cloudpb.ExternalStorage {
	return cloudpb.ExternalStorage{
		Provider: cloudpb.ExternalStorageProvider_Unknown,
	}
}

type closableBytesWriter struct {
	bytes.Buffer
}

func (b *closableBytesWriter) Close() error {
	// No-op for bytes.Buffer, but satisfies io.WriteCloser interface.
	return nil
}

func (f *fakeExternalStorage) Writer(ctx context.Context, filename string) (io.WriteCloser, error) {
	if _, exists := f.files[filename]; exists {
		return nil, errors.Errorf("file %s already exists", filename)
	}
	buf := closableBytesWriter{}
	f.files[filename] = &buf
	return &buf, nil
}

type bytesReaderCtx struct {
	*bufio.Reader
}

func (br *bytesReaderCtx) Read(_ context.Context, p []byte) (n int, err error) {
	// Use the context to satisfy the ReaderCtx interface, but ignore it.
	return br.Reader.Read(p)
}

func (b *bytesReaderCtx) Close(_ context.Context) error {
	// No-op for bufio.Reader, but satisfies io.ReadCloser interface.
	return nil
}

func (f *fakeExternalStorage) ReadFile(
	ctx context.Context, filename string, _ cloud.ReadOptions,
) (ioctx.ReadCloserCtx, int64, error) {
	bytes, exists := f.files[filename]
	if !exists {
		return nil, 0, errors.Errorf("file %s does not exist", filename)
	}
	reader := bytesReaderCtx{
		Reader: bufio.NewReader(bytes),
	}
	return &reader, int64(bytes.Len()), nil
}

// List lists files in the fake external storage, optionally filtering by prefix.
func (f *fakeExternalStorage) List(
	ctx context.Context, prefix string, delimiter string, cb cloud.ListingFn,
) error {
	var matchedFiles []string
	if prefix == "" {
		matchedFiles = slices.Collect(maps.Keys(f.files))
	} else {
		for file := range f.files {
			if strings.HasPrefix(file, prefix) {
				matchedFiles = append(matchedFiles, file)
			}
		}
	}

	delimited := make(map[string]struct{})
	if delimiter != "" {
		for _, file := range matchedFiles {
			cutIdx := strings.Index(file[len(prefix):], delimiter) + len(prefix)
			cut := file[:cutIdx]
			if _, ok := delimited[cut]; !ok {
				delimited[cut] = struct{}{}
			}
		}
		matchedFiles = slices.Collect(maps.Keys(delimited))
	}

	slices.Sort(matchedFiles)

	for _, file := range matchedFiles {
		if err := cb(file); err != nil {
			return err
		}
	}
	return nil
}
