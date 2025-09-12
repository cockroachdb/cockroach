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

func TestFindAllIncrementalPaths(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc, db, _, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(t, backuptestutils.SingleNode)
	defer cleanupFn()

	ctx := context.Background()
	execCfg := tc.Server(0).ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)
	db.Exec(t, "SET CLUSTER SETTING backup.index.read.enabled = true")

	type backup struct {
		// For simplicity, start and end will represent hours in a day in 24 hour
		// format (0 will be treated as null time).
		start int
		end   int
	}

	testCases := []struct {
		name         string
		backupChains [][]backup
		targetChain  int
	}{
		{
			name: "single indexed full backup with incrementals",
			backupChains: [][]backup{
				{
					{start: 0, end: 1},
					{start: 1, end: 2},
					{start: 2, end: 3},
				},
			},
		},
		{
			name: "single indexed full backup with compacted backups",
			backupChains: [][]backup{
				{
					{start: 0, end: 1},
					{start: 1, end: 2},
					{start: 1, end: 3},
					{start: 2, end: 3},
				},
			},
		},
		{
			name: "multiple indexed full backups",
			backupChains: [][]backup{
				{
					{start: 0, end: 1},
					{start: 1, end: 2},
					{start: 2, end: 3},
					{start: 1, end: 3},
				},
				{
					{start: 0, end: 4},
					{start: 4, end: 5},
				},
			},
			targetChain: 1,
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

			targetChain := tc.backupChains[tc.targetChain]
			targetSubdir := toTime(targetChain[0].end).Format(backupbase.DateBasedIntoFolderName)

			incStoreURI, err := backuputils.AppendPath(
				collectionURI, backupbase.DefaultIncrementalsSubdir, targetSubdir,
			)
			require.NoError(t, err)
			incStore, err := execCfg.DistSQLSrv.ExternalStorageFromURI(
				ctx, incStoreURI, username.RootUserName(),
			)
			require.NoError(t, err)

			for _, chain := range tc.backupChains {
				require.Zero(t, chain[0].start, "first backup in chain should be a full backup with 0 start")
				fullEnd := toTime(chain[0].end)

				writeEmptyBackupManifest(
					t, &execCfg, collectionURI, defaultIncrementalLocation, fullEnd,
					toTime(chain[0].start), fullEnd, true, /* indexed */
				)

				// Write the incrementals out of order to test `ListIndexes`.
				perm := rand.Perm(len(chain) - 1)
				for _, idx := range perm {
					b := chain[idx+1]
					start := toTime(b.start)
					end := toTime(b.end)
					writeEmptyBackupManifest(
						t, &execCfg, collectionURI, defaultIncrementalLocation, fullEnd,
						start, end, true, /* indexed */
					)
				}
			}

			incs, err := backupdest.FindAllIncrementalPaths(
				ctx, &execCfg, incStore, store,
				targetSubdir, false /* includeManifest */, false, /* customIncLocation */
			)
			require.NoError(t, err)
			require.Len(t, incs, len(targetChain)-1)

			var expectedPaths []string
			for _, b := range targetChain[1:] {
				expectedPaths = append(expectedPaths, backupdest.ConstructDateBasedIncrementalFolderName(
					toTime(b.start), toTime(b.end),
				))
			}

			require.Equal(t, expectedPaths, incs)
		})
	}
}

func TestFindAllIncrementalPathsFallbackLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc, db, folder, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(t, backuptestutils.SingleNode)
	fmt.Println(folder)
	defer cleanupFn()
	db.Exec(t, "SET CLUSTER SETTING backup.index.read.enabled = true")

	ctx := context.Background()
	execCfg := tc.Server(0).ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)
	storeFactory := execCfg.DistSQLSrv.ExternalStorageFromURI

	testIdx := 0
	// getStores returns a set of stores rooted at the root of the collection URI,
	// the full backup subdir for incrementals, and the full backup subdir for
	// custom incremental locations. It also returns the collection URI and the
	// URI specified by `incremental_location`.
	getStores := func(t *testing.T, subdir string) (
		store struct{ root, inc, customInc cloud.ExternalStorage },
		uris struct{ root, inc, customInc string },
	) {
		t.Helper()
		var err error
		uris.root = fmt.Sprintf("nodelocal://1/backup/%d", testIdx)
		store.root, err = storeFactory(ctx, uris.root, username.RootUserName())
		require.NoError(t, err)

		uris.inc, err = backuputils.AppendPath(uris.root, backupbase.DefaultIncrementalsSubdir)
		require.NoError(t, err)
		incSubdir, err := backuputils.AppendPath(uris.inc, subdir)
		require.NoError(t, err)
		store.inc, err = storeFactory(ctx, incSubdir, username.RootUserName())
		require.NoError(t, err)

		uris.customInc, err = backuputils.AppendPath(uris.root, "custom-incrementals")
		require.NoError(t, err)
		customIncSubdir, err := backuputils.AppendPath(uris.customInc, subdir)
		require.NoError(t, err)
		store.customInc, err = storeFactory(ctx, customIncSubdir, username.RootUserName())
		require.NoError(t, err)

		testIdx++
		return store, uris
	}

	t.Run("unindexed backup tree", func(t *testing.T) {
		fullEnd := time.Now().UTC()
		subdir := fullEnd.Format(backupbase.DateBasedIntoFolderName)
		stores, uris := getStores(t, subdir)

		start, end := time.Time{}, fullEnd
		const numBackups = 4
		for range numBackups {
			writeEmptyBackupManifest(
				t, &execCfg, uris.root, defaultIncrementalLocation,
				fullEnd, start, end, false, /* indexed */
			)
			start = end
			end = end.Add(time.Hour)
		}

		incs, err := backupdest.FindAllIncrementalPaths(
			ctx, &execCfg, stores.inc, stores.root,
			subdir, false /* includeManifest */, false, /* customIncLocation */
		)
		require.NoError(t, err)
		require.Len(t, incs, numBackups-1)
	})

	t.Run("custom incremental location with indexed full", func(t *testing.T) {
		fullEnd := time.Now().UTC()
		subdir := fullEnd.Format(backupbase.DateBasedIntoFolderName)
		stores, uris := getStores(t, subdir)

		start, end := time.Time{}, fullEnd
		const numBackups = 4
		for i := range numBackups {
			writeEmptyBackupManifest(
				t, &execCfg, uris.root, uris.customInc,
				fullEnd, start, end, i == 0, /* only full can be indexed */
			)
			start = end
			end = end.Add(time.Hour)
		}

		incs, err := backupdest.FindAllIncrementalPaths(
			ctx, &execCfg, stores.customInc, stores.root,
			subdir, false /* includeManifest */, true, /* customIncLocation */
		)
		require.NoError(t, err)
		require.Len(t, incs, numBackups-1)
	})

	t.Run("mix of custom and default incremental locations", func(t *testing.T) {
		fullEnd := time.Now().UTC()
		subdir := fullEnd.Format(backupbase.DateBasedIntoFolderName)
		stores, uris := getStores(t, subdir)

		// Write a full backup
		writeEmptyBackupManifest(
			t, &execCfg, uris.root, defaultIncrementalLocation,
			fullEnd, time.Time{}, fullEnd, true, /* indexed */
		)

		const numDefaultIncs = 5
		start, end := fullEnd, fullEnd.Add(time.Hour)
		for range numDefaultIncs {
			writeEmptyBackupManifest(
				t, &execCfg, uris.root, defaultIncrementalLocation,
				fullEnd, start, end, true, /* indexed */
			)
			start = end
			end = end.Add(time.Hour)
		}

		const numCustomIncs = 4
		start = fullEnd
		for range numCustomIncs {
			writeEmptyBackupManifest(
				t, &execCfg, uris.root, uris.customInc,
				fullEnd, start, end, false, /* indexed */
			)
			start = end
			end = end.Add(time.Hour)
		}

		// List from both default and custom incremental locations.
		incs, err := backupdest.FindAllIncrementalPaths(
			ctx, &execCfg, stores.inc, stores.root,
			subdir, false /* includeManifest */, false, /* customIncLocation */
		)
		require.NoError(t, err)
		require.Len(t, incs, numDefaultIncs)

		incs, err = backupdest.FindAllIncrementalPaths(
			ctx, &execCfg, stores.customInc, stores.root,
			subdir, false /* includeManifest */, true, /* customIncLocation */
		)
		require.NoError(t, err)
		require.Len(t, incs, numCustomIncs)
	})
}

const defaultIncrementalLocation = ""

// writeEmptyBackupManifest creates an empty backup manifest/metadata file under
// the provided collection URI and a corresponding index file (if set).
// fullEnd indicates the end time of the full backup the backup belongs to.
// If start is empty or unix 0, it is treated as a full backup, otherwise it is
// treated as an incremental backup.
// If `incLocation` is non-empty, the backup will be written to the URI
// specified by `incLocation`.
func writeEmptyBackupManifest(
	t *testing.T,
	execCfg *sql.ExecutorConfig,
	collectionURI, incURI string,
	fullEnd, start, end time.Time,
	indexed bool,
) {
	t.Helper()
	isIncBackup := start.UnixNano() != 0 && !start.IsZero()
	if isIncBackup && incURI != "" && indexed {
		require.Fail(t, "backup indexing is not supported for custom incremental locations")
	}

	subdir := fullEnd.Format(backupbase.DateBasedIntoFolderName)
	uri := collectionURI
	backupPath := subdir

	if isIncBackup {
		uri = incURI
		if incURI == "" {
			var err error
			uri, err = backuputils.AppendPath(collectionURI, backupbase.DefaultIncrementalsSubdir)
			require.NoError(t, err)
		}
		backupPath = backuputils.JoinURLPath(
			subdir,
			backupdest.ConstructDateBasedIncrementalFolderName(start, end),
		)
	}

	backupURI, err := backuputils.AppendPath(uri, backupPath)
	require.NoError(t, err)

	emptyReader := bytes.NewReader(nil)
	backupStore, err := execCfg.DistSQLSrv.ExternalStorageFromURI(
		context.Background(), backupURI, username.RootUserName(),
	)
	defer backupStore.Close()
	require.NoError(t, err)
	require.NoError(
		t,
		cloud.WriteFile(context.Background(), backupStore, backupbase.BackupMetadataName, emptyReader),
	)
	require.NoError(
		t,
		cloud.WriteFile(context.Background(), backupStore, backupbase.DeprecatedBackupManifestName, emptyReader),
	)

	if !indexed {
		return
	}

	var startWallTime int64
	if isIncBackup {
		startWallTime = start.UnixNano()
	}
	backupDetails := jobspb.BackupDetails{
		StartTime:     hlc.Timestamp{WallTime: startWallTime},
		EndTime:       hlc.Timestamp{WallTime: end.UnixNano()},
		CollectionURI: collectionURI,
		Destination: jobspb.BackupDetails_Destination{
			To:     []string{collectionURI},
			Exists: isIncBackup,
			Subdir: subdir,
		},
		URI: backupURI,
	}
	if incURI != "" {
		backupDetails.Destination.IncrementalStorage = []string{incURI}
	}

	require.NoError(
		t,
		backupdest.WriteBackupIndexMetadata(
			context.Background(), execCfg, username.RootUserName(),
			execCfg.DistSQLSrv.ExternalStorageFromURI, backupDetails, hlc.Timestamp{},
		),
	)
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
		manifestName := backupbase.DeprecatedBackupManifestName
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
