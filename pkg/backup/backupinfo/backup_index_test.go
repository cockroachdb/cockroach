// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupinfo

import (
	"context"
	"fmt"
	"math/rand"
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

	writeIndex := func(
		t *testing.T, subdirEnd, start, end int,
	) {
		t.Helper()
		subdirTS := intToTimeWithNano(subdirEnd).GoTime()
		startTS := intToTimeWithNano(start)
		endTS := intToTimeWithNano(end)
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
			ctx, execCfg, username.RootUserName(), storageFactory, details, hlc.Timestamp{},
		))
	}

	randIdx := func(n int) []int {
		idxs := make([]int, n)
		for i := 0; i < n; i++ {
			idxs[i] = i
		}
		rand.Shuffle(n, func(i, j int) {
			// Don't shuffle the full backup as it must be written before incs.
			if i == 0 || j == 0 {
				return
			}
			idxs[i], idxs[j] = idxs[j], idxs[i]
		})
		return idxs
	}

	type chain = [][2]int
	simpleChain := chain{{0, 2}, {2, 4}, {4, 6}, {6, 8}}
	compactedChain := chain{{0, 10}, {10, 11}, {10, 12}, {11, 12}, {12, 14}, {14, 16}}
	doubleCompactedChain := chain{{0, 18}, {18, 20}, {18, 22}, {20, 22}, {22, 24}, {18, 26}, {24, 26}}
	fullOnly := chain{{0, 28}}

	indexes := []chain{
		simpleChain,
		compactedChain,
		doubleCompactedChain,
		fullOnly,
	}

	for _, index := range indexes {
		// Write the index files in random time order to ensure the read always
		// returns in them in the correct order.
		rndIdxs := randIdx(len(index))
		for _, idx := range rndIdxs {
			writeIndex(t, index[0][1], index[idx][0], index[idx][1])
		}
	}

	testcases := []struct {
		name  string
		chain chain
		error string
		// expectedIndexTimes should be sorted in ascending order by end time, with
		// ties broken by ascending start time.
		expectedIndexTimes chain
	}{
		{
			name:               "fetch all indexes from chain with no compacted backups",
			chain:              simpleChain,
			expectedIndexTimes: [][2]int{{0, 2}, {2, 4}, {4, 6}, {6, 8}},
		},
		{
			name:               "fetch all indexes from tree with compacted backups",
			chain:              compactedChain,
			expectedIndexTimes: chain{{0, 10}, {10, 11}, {10, 12}, {11, 12}, {12, 14}, {14, 16}},
		},
		{
			name:  "fetch all indexes from tree with double compacted backups",
			chain: doubleCompactedChain,
			expectedIndexTimes: chain{
				{0, 18}, {18, 20}, {18, 22}, {20, 22}, {22, 24}, {18, 26}, {24, 26},
			},
		},
		{
			name:               "index only contains a full backup",
			chain:              fullOnly,
			expectedIndexTimes: chain{{0, 28}},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			subdirTS := intToTimeWithNano(tc.chain[0][1]).GoTime()
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
	// Value needs to be large enough to be represented in milliseconds and be
	// larger than GoTime zero.
	return hlc.Timestamp{WallTime: int64(t)*1e9 + int64(t)}
}
