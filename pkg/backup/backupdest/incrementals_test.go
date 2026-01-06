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
	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
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
					t, &execCfg, collectionURI, fullEnd, toTime(chain[0].start), fullEnd, true, /* indexed */
				)

				// Write the incrementals out of order to test `ListIndexes`.
				perm := rand.Perm(len(chain) - 1)
				for _, idx := range perm {
					b := chain[idx+1]
					start := toTime(b.start)
					end := toTime(b.end)
					writeEmptyBackupManifest(
						t, &execCfg, collectionURI, fullEnd, start, end, true, /* indexed */
					)
				}
			}

			incs, err := backupdest.FindAllIncrementalPaths(ctx, &execCfg, incStore, store, targetSubdir)
			require.NoError(t, err)
			require.Len(t, incs, len(targetChain)-1)

			var expectedPaths []string
			for _, b := range targetChain[1:] {
				expectedPaths = append(expectedPaths, backupinfo.ConstructDateBasedIncrementalFolderName(
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

	tc, db, _, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(t, backuptestutils.SingleNode)
	defer cleanupFn()
	db.Exec(t, "SET CLUSTER SETTING backup.index.read.enabled = true")

	ctx := context.Background()
	execCfg := tc.Server(0).ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)
	storeFactory := execCfg.DistSQLSrv.ExternalStorageFromURI

	testIdx := 0
	// getStores returns a set of stores rooted at the root of the collection URI,
	// the full backup subdir for incrementals.
	getStores := func(t *testing.T, subdir string) (
		store struct {
			cleanup func()
			root    cloud.ExternalStorage
			inc     cloud.ExternalStorage
		},
		uris struct{ root, inc string },
	) {
		t.Helper()
		store.cleanup = func() {
			if store.root != nil {
				store.root.Close()
			}
			if store.inc != nil {
				store.inc.Close()
			}
		}

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

		testIdx++
		return store, uris
	}

	t.Run("unindexed backup tree", func(t *testing.T) {
		fullEnd := time.Now().UTC()
		subdir := fullEnd.Format(backupbase.DateBasedIntoFolderName)
		stores, uris := getStores(t, subdir)
		defer stores.cleanup()

		start, end := time.Time{}, fullEnd
		const numBackups = 4
		for range numBackups {
			writeEmptyBackupManifest(
				t, &execCfg, uris.root, fullEnd, start, end, false, /* indexed */
			)
			start = end
			end = end.Add(time.Hour)
		}

		incs, err := backupdest.FindAllIncrementalPaths(ctx, &execCfg, stores.inc, stores.root, subdir)
		require.NoError(t, err)
		require.Len(t, incs, numBackups-1)
	})
}

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
	collectionURI string,
	fullEnd, start, end time.Time,
	indexed bool,
) {
	t.Helper()
	isIncBackup := start.UnixNano() != 0 && !start.IsZero()

	subdir := fullEnd.Format(backupbase.DateBasedIntoFolderName)
	uri := collectionURI
	backupPath := subdir

	if isIncBackup {
		var err error
		uri, err = backuputils.AppendPath(collectionURI, backupbase.DefaultIncrementalsSubdir)
		require.NoError(t, err)
		backupPath = backuputils.JoinURLPath(
			subdir,
			backupinfo.ConstructDateBasedIncrementalFolderName(start, end),
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

	require.NoError(
		t,
		backupinfo.WriteBackupIndexMetadata(
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

	writeManifest := func(t *testing.T, store cloud.ExternalStorage, path string) {
		manifestName := backupbase.DeprecatedBackupManifestName
		err := cloud.WriteFile(ctx, store, backuputils.JoinURLPath(path, manifestName), emptyReader)
		require.NoError(t, err)
	}

	type backupPath struct {
		path string
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
				{path: "/20250320/001000.00-20250320-000000.00"},
				{path: "/20250320/002000.00-20250320-001000.00"},
				{path: "/20250320/003000.00-20250320-002000.00"},
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
				{path: "/20250320/001000.00"},
				{path: "/20250320/002000.00"},
				{path: "/20250320/003000.00"},
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
				{path: "/20250320/001000.00-20250320-000000.00"},
				{path: "/20250320/002000.00"},
				{path: "/20250320/003000.00-20250320-002000.00"},
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
				{path: "/20250320/001000.00-20250320-000000.00"},
				{path: "/20250320/001500.000"}, // invalid
				{path: "/20250320/002000.00"},
				{path: "/2025/03/20/002500.00"}, // invalid
			},
			expectedPaths: []string{
				"/20250320/001000.00-20250320-000000.00",
				"/20250320/002000.00",
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
				writeManifest(t, store, path.path)
			}
			prev, err := backupdest.LegacyFindPriorBackups(ctx, store, false)
			require.NoError(t, err)
			require.Equal(t, tc.expectedPaths, prev)
		})
	}
}

func TestCleanupMakeBackupDestinationStores(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// This test ensures that in the event that MakeBackupDestinationStores
	// encounters an error either during store creation or during cleanup that all
	// stores that were opened have Close called.
	ctx := context.Background()

	const maxStores = 10
	failAfterN := rand.Intn(maxStores + 1)

	stores := make([]*fakeStore, 0, maxStores)
	mkStore := func(
		_ context.Context,
		_ string,
		_ username.SQLUsername,
		_ ...cloud.ExternalStorageOption,
	) (cloud.ExternalStorage, error) {
		if len(stores) == failAfterN {
			return nil, fmt.Errorf("simulated failure after %d stores", failAfterN)
		}
		s := &fakeStore{closeErrProbability: 0.1}
		stores = append(stores, s)
		return s, nil
	}

	destinations := make([]string, maxStores)
	_, cleanup, err := backupdest.MakeBackupDestinationStores(
		ctx, username.RootUserName(), mkStore, destinations,
	)

	countOpenStores := func() int {
		count := 0
		for _, s := range stores {
			if !s.calledClose {
				count++
			}
		}
		return count
	}

	if failAfterN < maxStores {
		require.Error(t, err)
		require.Equal(t, 0, countOpenStores())
	} else {
		require.NoError(t, err)
		require.Equal(t, maxStores, countOpenStores())
		_ = cleanup()
		require.Equal(t, 0, countOpenStores())
	}
}

type fakeStore struct {
	cloud.ExternalStorage
	calledClose         bool
	closeErrProbability float32
}

// Close() simulates closing the store and probabilistically returns an error.
func (s *fakeStore) Close() error {
	s.calledClose = true
	if rand.Float32() < s.closeErrProbability {
		return fmt.Errorf("simulated close error")
	}
	return nil
}
