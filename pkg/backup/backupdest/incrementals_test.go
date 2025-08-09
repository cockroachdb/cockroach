// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupdest_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/backup/backupdest"
	"github.com/cockroachdb/cockroach/pkg/backup/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/backup/backuputils"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCollectionsAndSubdir(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type testcase struct {
		name                string
		paths               []string
		subdir              string
		expectedCollections []string
		expectedSubdir      string
		expectedError       string
	}
	testcases := []testcase{
		{
			name:                "non-empty subdir returns unmodified collection",
			paths:               []string{"nodelocal://1/backup-dest/2023/05/10-160331.72/"},
			subdir:              "foo",
			expectedCollections: []string{"nodelocal://1/backup-dest/2023/05/10-160331.72/"},
			expectedSubdir:      "foo",
		},
		{
			name: "non-empty subdir returns all parts of unmodified collection",
			paths: []string{
				"nodelocal://1/backup-dest/",
				"nodelocal://2/backup-dest/",
			},
			subdir: "foo",
			expectedCollections: []string{
				"nodelocal://1/backup-dest/",
				"nodelocal://2/backup-dest/",
			},
			expectedSubdir: "foo",
		},
		{
			name:                "date-based-path is returned as subdir if no subdir is provided",
			paths:               []string{"nodelocal://1/backup-dest/2023/05/10-160331.72/"},
			expectedCollections: []string{"nodelocal://1/backup-dest"},
			expectedSubdir:      "2023/05/10-160331.72/",
		},
		{
			name: "multiple date-based paths are returned as subdir if no subdir is provided",
			paths: []string{
				"nodelocal://1/backup-dest/2023/05/10-160331.72/",
				"nodelocal://2/backup-dest/2023/05/10-160331.72/",
			},
			expectedCollections: []string{
				"nodelocal://1/backup-dest",
				"nodelocal://2/backup-dest",
			},
			expectedSubdir: "2023/05/10-160331.72/",
		},
		{
			name: "paths that don't match are returned unmodified",
			paths: []string{
				"nodelocal://1/backup-dest/2023/05/9999/",
				"nodelocal://2/backup-dest/2023/05/9999/",
			},
			expectedCollections: []string{
				"nodelocal://1/backup-dest/2023/05/9999/",
				"nodelocal://2/backup-dest/2023/05/9999/",
			},
			expectedSubdir: "",
		},
		{
			name: "different date-based paths results in an error",
			paths: []string{
				"nodelocal://1/backup-dest/2023/05/10-160331.72/",
				"nodelocal://2/backup-dest/2023/05/10-160331.73/",
			},
			expectedError: "provided backup locations appear to reference different full backups",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			collections, subdir, err := backupdest.CollectionsAndSubdir(tc.paths, tc.subdir)
			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
			} else {
				require.Equal(t, tc.expectedSubdir, subdir)
				require.Equal(t, tc.expectedCollections, collections)
			}
		})
	}
}

func TestJoinURLPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// path.Join has identical behavior for these inputs.
	require.Equal(t, "/top/path", backuputils.JoinURLPath("/top", "path"))
	require.Equal(t, "top/path", backuputils.JoinURLPath("top", "path"))

	require.Equal(t, "/path", backuputils.JoinURLPath("/top", "../path"))
	require.Equal(t, "path", backuputils.JoinURLPath("top", "../path"))

	require.Equal(t, "../path", backuputils.JoinURLPath("top", "../../path"))

	// path.Join has different behavior for this input.
	require.Equal(t, "/../path", backuputils.JoinURLPath("/top", "../../path"))
}

func TestFindAllIncrementals(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc, _, _, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(t, backuptestutils.SingleNode)
	defer cleanupFn()

	ctx := context.Background()
	execCfg := tc.Server(0).ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)
	emptyReader := bytes.NewReader(nil)

	writeBackup := func(
		t *testing.T, store cloud.ExternalStorage, indexed bool,
		collectionURI string, start, end, fullEnd time.Time,
	) {
		subdir := fullEnd.Format(backupbase.DateBasedIntoFolderName)
		backupPath := subdir
		if start.UnixNano() != 0 {
			backupPath = backuputils.JoinURLPath(
				backupbase.DefaultIncrementalsSubdir,
				backupdest.ConstructDateBasedIncrementalFolderName(start, end),
			)
		}
		backupURI, err := backuputils.AppendPath(collectionURI, backupPath)
		require.NoError(t, err)

		manifestPath := backuputils.JoinURLPath(backupPath, backupbase.BackupManifestName)
		require.NoError(t, cloud.WriteFile(ctx, store, manifestPath, emptyReader))

		if indexed {
			backupDetails := jobspb.BackupDetails{
				StartTime:     hlc.Timestamp{WallTime: start.UnixNano()},
				EndTime:       hlc.Timestamp{WallTime: end.UnixNano()},
				CollectionURI: collectionURI,
				Destination: jobspb.BackupDetails_Destination{
					To:     []string{collectionURI},
					Exists: start.UnixNano() != 0,
					Subdir: subdir,
				},
				URI: backupURI,
			}
			require.NoError(
				t,
				backupdest.WriteBackupIndexMetadata(
					ctx, &execCfg, username.RootUserName(),
					execCfg.DistSQLSrv.ExternalStorageFromURI, backupDetails,
				),
			)
		}
	}

	type backup struct {
		// For simplicity, start and end will represent hours in a day in 24 hour
		// format (0 will be treated as null time).
		start int
		end   int
	}

	type backupChain struct {
		backups   []backup
		skipIndex bool
	}

	testCases := []struct {
		name         string
		backupChains []backupChain
		targetChain  int
	}{
		{
			name: "single indexed full backup with incrementals",
			backupChains: []backupChain{
				{
					backups: []backup{
						{start: 0, end: 1},
						{start: 1, end: 2},
						{start: 1, end: 3},
					},
				},
			},
		},
		{
			name: "single indexed full backup",
			backupChains: []backupChain{
				{
					backups: []backup{
						{start: 0, end: 1},
					},
				},
			},
		},
		{
			name: "single indexed full backup with compacted backups",
			backupChains: []backupChain{
				{
					backups: []backup{
						{start: 0, end: 1},
						{start: 1, end: 2},
						{start: 2, end: 3},
						{start: 1, end: 3},
					},
				},
			},
		},
		{
			name: "multiple indexed full backups",
			backupChains: []backupChain{
				{
					backups: []backup{
						{start: 0, end: 1},
						{start: 1, end: 2},
						{start: 2, end: 3},
						{start: 1, end: 3},
					},
				},
				{
					backups: []backup{
						{start: 0, end: 4},
						{start: 4, end: 5},
					},
				},
			},
			targetChain: 1,
		},
		{
			name: "non-indexed full backup should fallback to legacy logic",
			backupChains: []backupChain{
				{
					backups: []backup{
						{start: 0, end: 1},
						{start: 1, end: 2},
						{start: 1, end: 3},
					},
					skipIndex: true,
				},
			},
		},
	}

	toTime := func(hour int) time.Time {
		if hour == 0 {
			return time.Unix(0, 0) // 0 is treated as a null time
		}
		return time.Date(2025, 7, 31, hour, 0, 0, 0, time.UTC)
	}

	for tcIdx, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			collectionURI := fmt.Sprintf("nodelocal://1/backup/%d", tcIdx)
			store, err := execCfg.DistSQLSrv.ExternalStorageFromURI(
				ctx, collectionURI, username.RootUserName(),
			)
			require.NoError(t, err)
			defer store.Close()

			incStoreURI, err := backuputils.AppendPath(
				collectionURI, backupbase.DefaultIncrementalsSubdir,
			)
			require.NoError(t, err)

			incStore, err := execCfg.DistSQLSrv.ExternalStorageFromURI(
				ctx, incStoreURI, username.RootUserName(),
			)
			require.NoError(t, err)
			defer incStore.Close()

			for _, chain := range tc.backupChains {
				fullEnd := toTime(chain.backups[0].end)

				for idx, b := range chain.backups {
					if idx == 0 {
						require.Zero(t, b.start, "first backup in chain should be a full backup with 0 start")
					}
					start := toTime(b.start)
					end := toTime(b.end)
					writeBackup(t, store, !chain.skipIndex, collectionURI, start, end, fullEnd)
				}
			}

			targetChain := tc.backupChains[tc.targetChain]
			targetSubdir := toTime(targetChain.backups[0].end).Format(backupbase.DateBasedIntoFolderName)
			incs, err := backupdest.FindAllIncrementals(
				ctx, incStore, store, targetSubdir, false, /* includeManifest */
			)
			require.NoError(t, err)
			require.Len(t, incs, len(targetChain.backups)-1)

			var expectedPaths []string
			for _, b := range targetChain.backups[1:] {
				expectedPaths = append(expectedPaths, backupdest.ConstructDateBasedIncrementalFolderName(
					toTime(b.start), toTime(b.end),
				))
			}

			require.Equal(t, expectedPaths, incs)
		})
	}
}

func TestLegacyFindPriorBackups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc, _, _, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(t, backuptestutils.SingleNode)
	defer cleanupFn()

	ctx := context.Background()
	execCfg := tc.Server(0).ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)
	emptyReader := bytes.NewReader(nil)

	writeManifest := func(t *testing.T, store cloud.ExternalStorage, path string, useOldBackup bool) {
		manifestName := backupbase.BackupManifestName
		if useOldBackup {
			manifestName = backupbase.BackupOldManifestName
		}
		err := cloud.WriteFile(ctx, store, backuputils.JoinURLPath(path, manifestName), emptyReader)
		require.NoError(t, err)
	}

	type backupPath struct {
		path         string
		useOldBackup bool
	}
	type testcase struct {
		name          string
		paths         []backupPath
		expectedPaths []string
	}

	for idx, tc := range []testcase{
		{
			name: "all suffixed paths",
			paths: []backupPath{
				{path: "/20250320/001000.00-20250320-000000.00", useOldBackup: false},
				{path: "/20250320/002000.00-20250320-001000.00", useOldBackup: false},
				{path: "/20250320/003000.00-20250320-002000.00", useOldBackup: false},
			},
			expectedPaths: []string{
				"/20250320/001000.00-20250320-000000.00",
				"/20250320/002000.00-20250320-001000.00",
				"/20250320/003000.00-20250320-002000.00",
			},
		},
		{
			name: "all non-suffixed paths",
			paths: []backupPath{
				{path: "/20250320/001000.00", useOldBackup: false},
				{path: "/20250320/002000.00", useOldBackup: false},
				{path: "/20250320/003000.00", useOldBackup: false},
			},
			expectedPaths: []string{
				"/20250320/001000.00",
				"/20250320/002000.00",
				"/20250320/003000.00",
			},
		},
		{
			name: "all old backup paths",
			paths: []backupPath{
				{path: "/20250320/001000.00", useOldBackup: true},
				{path: "/20250320/002000.00", useOldBackup: true},
				{path: "/20250320/003000.00", useOldBackup: true},
			},
			expectedPaths: []string{
				"/20250320/001000.00",
				"/20250320/002000.00",
				"/20250320/003000.00",
			},
		},
		{
			name: "mixed new and old backup paths",
			paths: []backupPath{
				{path: "/20250320/001000.00", useOldBackup: false},
				{path: "/20250320/002000.00", useOldBackup: true},
				{path: "/20250320/003000.00", useOldBackup: false},
			},
			expectedPaths: []string{
				"/20250320/001000.00",
				"/20250320/002000.00",
				"/20250320/003000.00",
			},
		},
		{
			name: "mixed suffixed and non-suffixed paths",
			paths: []backupPath{
				{path: "/20250320/001000.00-20250320-000000.00", useOldBackup: false},
				{path: "/20250320/002000.00", useOldBackup: false},
				{path: "/20250320/003000.00-20250320-002000.00", useOldBackup: false},
			},
			expectedPaths: []string{
				"/20250320/001000.00-20250320-000000.00",
				"/20250320/002000.00",
				"/20250320/003000.00-20250320-002000.00",
			},
		},
		{
			name: "invalid backup paths with valid backup paths",
			paths: []backupPath{
				{path: "/20250320/001000.00-20250320-000000.00", useOldBackup: false},
				{path: "/20250320/001500.000", useOldBackup: false}, // invalid
				{path: "/20250320/002000.00", useOldBackup: false},
				{path: "/2025/03/20/002500.00", useOldBackup: false}, // invalid
				{path: "/20250320/003000.00", useOldBackup: true},
			},
			expectedPaths: []string{
				"/20250320/001000.00-20250320-000000.00",
				"/20250320/002000.00",
				"/20250320/003000.00",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := execCfg.DistSQLSrv.ExternalStorageFromURI(
				ctx, fmt.Sprintf("nodelocal://1/%d", idx), username.RootUserName(),
			)
			require.NoError(t, err)
			defer store.Close()
			// Shuffle the paths to ensure that FindPriorBackups return the paths in
			// ascending order.
			rand.Shuffle(len(tc.paths), func(i, j int) {
				tc.paths[i], tc.paths[j] = tc.paths[j], tc.paths[i]
			})
			for _, path := range tc.paths {
				writeManifest(t, store, path.path, path.useOldBackup)
			}
			prev, err := backupdest.LegacyFindPriorBackups(ctx, store, false)
			require.NoError(t, err)
			require.Equal(t, tc.expectedPaths, prev)
		})
	}
}
