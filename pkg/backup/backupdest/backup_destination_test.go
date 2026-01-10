// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupdest

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/backup/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/backup/backuputils"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl" // register cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// TestBackupRestoreResolveDestination is an integration style tests that tests
// all of the expected ways of organizing backups.
func TestBackupRestoreResolveDestination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "multinode clusters are slow under race")
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	var params base.TestClusterArgs

	tc, _, _, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(t,
		backuptestutils.MultiNode, backuptestutils.WithParams(params))
	defer cleanupFn()

	execCfg := tc.Server(0).ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)

	externalStorageFromURI := execCfg.DistSQLSrv.ExternalStorageFromURI

	// writeManifest writes a backup manifest file containing only an endtime to the given URI.
	writeManifest := func(t *testing.T, uri string, subdir string, endTime time.Time) {
		storage, err := externalStorageFromURI(ctx, uri, username.RootUserName())
		require.NoError(t, err)
		defer storage.Close()
		endTS := hlc.Timestamp{WallTime: endTime.UnixNano()}
		manifest := backuppb.BackupManifest{EndTime: endTS}
		manifestBytes, err := protoutil.Marshal(&manifest)
		require.NoError(t, err)
		reader := bytes.NewReader(manifestBytes)
		require.NoError(t, err)
		require.NoError(t, cloud.WriteFile(ctx, storage, backupbase.DeprecatedBackupManifestName, reader))

		parsedURI, err := url.Parse(uri)
		require.NoError(t, err)
		parsedURI.Path = ""
		collectionURI := parsedURI.String()
		writeBackupIndexFromManifest(
			t, ctx, &execCfg, collectionURI, uri, subdir, manifest,
		)
	}

	// writeLatest writes latestBackupSuffix to the LATEST file in the given
	// collection.
	writeLatest := func(t *testing.T, collectionURI, latestBackupSuffix string) {
		storage, err := externalStorageFromURI(ctx, collectionURI, username.RootUserName())
		defer storage.Close()
		require.NoError(t, err)
		require.NoError(t, WriteNewLatestFile(ctx, storage.Settings(), storage, latestBackupSuffix))
	}

	// localizeURI returns a slice of just the base URI if localities is nil.
	// Otherwise, it returns a slice of URIs. The first will be the URI for the
	// default locality, and then one will be returned for each locality in
	// `localities`. The path for each locality will be augmented with the
	// locality to ensure that each locality references a different directory and
	// the appropriate COCKROACH_LOCALITY argument will be added to the URI.
	localizeURI := func(t *testing.T, baseURI string, localities []string) []string {
		if localities == nil {
			return []string{baseURI}
		}
		allLocalities := append([]string{DefaultLocalityValue}, localities...)
		localizedURIs := make([]string, len(allLocalities))
		for i, locality := range allLocalities {
			parsedURI, err := url.Parse(baseURI)
			require.NoError(t, err)
			if locality != DefaultLocalityValue {
				parsedURI.Path = backuputils.JoinURLPath(parsedURI.Path, locality)
			}
			q := parsedURI.Query()
			q.Add(cloud.LocalityURLParam, locality)
			parsedURI.RawQuery = q.Encode()
			localizedURIs[i] = parsedURI.String()
		}
		return localizedURIs
	}

	for _, localityAware := range []bool{true, false} {
		var localities []string
		if localityAware {
			localities = []string{"dc=EN", "dc=FR"}
		}
		t.Run(fmt.Sprintf("locality-aware-%t", localityAware), func(t *testing.T) {
			// When testing auto-append backup locations, we'll be testing the name
			// resolution on backup directory structures created when running a sequence
			// of backups like:
			// - BACKUP INTO collection (full 1) @ 6
			// - BACKUP INTO LATEST IN collection @ 6:30
			// - BACKUP INTO LATEST IN collection @ 7
			// - BACKUP INTO collection (full 2) @ 7:30
			// - BACKUP INTO LATEST IN collection @ 8
			// - BACKUP INTO full1 IN collection @ 8:30
			t.Run("collection", func(t *testing.T) {
				collectionLoc := fmt.Sprintf("nodelocal://1/%s?AUTH=implicit", t.Name())
				collectionTo := localizeURI(t, collectionLoc, localities)

				fullTime := time.Date(2020, 12, 25, 6, 0, 0, 0, time.UTC)
				inc1Time := fullTime.Add(time.Minute * 30)
				inc2Time := inc1Time.Add(time.Minute * 30)
				full2Time := inc2Time.Add(time.Minute * 30)
				inc3Time := full2Time.Add(time.Minute * 30)
				inc4Time := inc3Time.Add(time.Minute * 30)

				// firstBackupChain is maintained throughout the tests as the history of
				// backups that were taken based on the initial full backup.
				firstBackupChain := []string(nil)
				// An explicit sub-directory is used in backups of the form BACKUP INTO
				// X IN Y. Otherwise, it should be empty string.
				noExplicitSubDir := ""

				testCollectionBackup := func(t *testing.T, backupTime time.Time,
					expectedDefault, expectedSuffix, expectedIncDir string, expectedPrevBackups []string,
					appendToLatest bool, subdir string) {
					t.Helper()

					endTime := hlc.Timestamp{WallTime: backupTime.UnixNano()}

					if appendToLatest {
						subdir = backupbase.LatestFileName
					} else if subdir == "" {
						subdir = endTime.GoTime().Format(backupbase.DateBasedIntoFolderName)
					}

					_, localityCollections, err := GetURIsByLocalityKV(collectionTo, "")
					require.NoError(t, err)

					if expectedIncDir != "" {
						incTo, err := backuputils.AppendPaths(collectionTo, backupbase.DefaultIncrementalsSubdir)
						require.NoError(t, err)
						_, localityCollections, err = GetURIsByLocalityKV(incTo, "")
						require.NoError(t, err)
					}

					fullBackupExists := expectedIncDir != ""
					kmsEnv := &cloud.TestKMSEnv{
						ExternalIOConfig: &base.ExternalIODirConfig{},
					}
					backupDest, err := ResolveDest(
						ctx, username.RootUserName(),
						jobspb.BackupDetails_Destination{
							To: collectionTo, Subdir: subdir, Exists: fullBackupExists,
						},
						hlc.Timestamp{}, endTime, &execCfg, nil, kmsEnv,
					)
					require.NoError(t, err)

					localityDests := make(map[string]string, len(localityCollections))
					for locality, localityDest := range localityCollections {
						u, err := url.Parse(localityDest)
						require.NoError(t, err)
						u.Path = u.Path + expectedSuffix + expectedIncDir
						localityDests[locality] = u.String()
					}

					require.Equal(t, collectionLoc, backupDest.CollectionURI)
					require.Equal(t, expectedSuffix, backupDest.ChosenSubdir)
					require.Equal(t, expectedDefault, backupDest.DefaultURI)
					require.Equal(t, localityDests, backupDest.URIsByLocalityKV)
					require.Equal(t, expectedPrevBackups, backupDest.PrevBackupURIs)
				}

				// Initial: BACKUP INTO collection
				{
					expectedSuffix := "/2020/12/25-060000.00"
					expectedIncDir := ""
					expectedDefault := fmt.Sprintf("nodelocal://1/%s%s?AUTH=implicit", t.Name(), expectedSuffix)

					testCollectionBackup(t, fullTime,
						expectedDefault, expectedSuffix, expectedIncDir, firstBackupChain,
						false /* intoLatest */, noExplicitSubDir)
					firstBackupChain = append(firstBackupChain, expectedDefault)
					writeManifest(t, expectedDefault, expectedSuffix, fullTime)
					// We also wrote a new full backup, so let's update the latest.
					writeLatest(t, collectionLoc, expectedSuffix)
				}

				// Incremental: BACKUP INTO LATEST IN collection
				{
					// We're backing up to the full backup at 6am.
					expectedSuffix := "/2020/12/25-060000.00"
					expectedIncDir := "/20201225/063000.00-20201225-060000.00"
					expectedDefault := fmt.Sprintf("nodelocal://1/%s/incrementals%s%s?AUTH=implicit", t.Name(), expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc1Time,
						expectedDefault, expectedSuffix, expectedIncDir, firstBackupChain,
						true /* intoLatest */, noExplicitSubDir)
					firstBackupChain = append(firstBackupChain, expectedDefault)
					writeManifest(t, expectedDefault, expectedSuffix, inc1Time)
				}

				// Another incremental: BACKUP INTO LATEST IN collection
				{
					// We're backing up to the full backup at 6am.
					expectedSuffix := "/2020/12/25-060000.00"
					expectedIncDir := "/20201225/070000.00-20201225-063000.00"
					expectedDefault := fmt.Sprintf("nodelocal://1/%s/incrementals%s%s?AUTH=implicit", t.Name(), expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc2Time,
						expectedDefault, expectedSuffix, expectedIncDir, firstBackupChain,
						true /* intoLatest */, noExplicitSubDir)
					firstBackupChain = append(firstBackupChain, expectedDefault)
					writeManifest(t, expectedDefault, expectedSuffix, inc2Time)
				}

				// A new full backup: BACKUP INTO collection
				var backup2Location string
				{
					expectedSuffix := "/2020/12/25-073000.00"
					expectedIncDir := ""
					expectedDefault := fmt.Sprintf("nodelocal://1/%s%s?AUTH=implicit", t.Name(), expectedSuffix)
					backup2Location = expectedDefault

					testCollectionBackup(t, full2Time,
						expectedDefault, expectedSuffix, expectedIncDir, []string(nil),
						false /* intoLatest */, noExplicitSubDir)
					writeManifest(t, expectedDefault, expectedSuffix, full2Time)
					// We also wrote a new full backup, so let's update the latest.
					writeLatest(t, collectionLoc, expectedSuffix)
				}

				// An incremental into the new latest: BACKUP INTO LATEST IN collection
				{
					// We're backing up to the full backup at 7:30am.
					expectedSuffix := "/2020/12/25-073000.00"
					expectedIncDir := "/20201225/080000.00-20201225-073000.00"
					expectedDefault := fmt.Sprintf("nodelocal://1/%s/incrementals%s%s?AUTH=implicit", t.Name(), expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc3Time,
						expectedDefault, expectedSuffix, expectedIncDir, []string{backup2Location},
						true /* intoLatest */, noExplicitSubDir)
					writeManifest(t, expectedDefault, expectedSuffix, inc3Time)
				}

				// An explicit incremental into the first full: BACKUP INTO full1 IN collection
				{
					expectedSuffix := "/2020/12/25-060000.00"
					expectedIncDir := "/20201225/083000.00-20201225-070000.00"
					expectedSubdir := expectedSuffix
					expectedDefault := fmt.Sprintf("nodelocal://1/%s/incrementals%s%s?AUTH=implicit", t.Name(), expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc4Time,
						expectedDefault, expectedSuffix, expectedIncDir, firstBackupChain,
						false /* intoLatest */, expectedSubdir)
					writeManifest(t, expectedDefault, expectedSuffix, inc4Time)
				}
			})
		})
	}
}

func TestResolveBackupManifests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, db, _, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(
		t, backuptestutils.SingleNode,
	)
	defer cleanupFn()

	execCfg := tc.Server(0).ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)

	collections := []string{
		"nodelocal://1/backup/default?COCKROACH_LOCALITY=default",
		"nodelocal://1/backup/west?COCKROACH_LOCALITY=region%3Dwest",
		"nodelocal://1/backup/south?COCKROACH_LOCALITY=region%3Dsouth",
	}

	var aost1 string
	db.QueryRow(t, "SELECT now()").Scan(&aost1)
	db.Exec(
		t, "BACKUP INTO ($1, $2, $3) AS OF SYSTEM TIME $4::STRING",
		collections[0], collections[1], collections[2], aost1,
	)
	db.Exec(
		t, "BACKUP INTO LATEST IN ($1, $2, $3)",
		collections[0], collections[1], collections[2],
	)

	aost1TS, err := time.Parse(time.RFC3339, aost1)
	require.NoError(t, err)
	aost1Hlc := hlc.Timestamp{WallTime: aost1TS.UnixNano()}

	var fullSubdir string
	db.QueryRow(
		t, "SHOW BACKUPS IN ($1, $2, $3)",
		collections[0], collections[1], collections[2],
	).Scan(&fullSubdir)
	require.NotEmpty(t, fullSubdir)

	mem := execCfg.RootMemoryMonitor.MakeBoundAccount()
	defer mem.Close(ctx)

	// TODO (kev-cao): Remove all following variables during cleanup of
	// `ResolveBackupManifests` parameters.
	fullyResolvedBaseDirs, err := backuputils.AppendPaths(collections, fullSubdir)
	require.NoError(t, err)

	fullyResolvedIncDirs, err := util.MapE(collections, func(uri string) (string, error) {
		u, err := url.Parse(uri)
		if err != nil {
			return "", err
		}
		u.Path = backuputils.JoinURLPath(u.Path, "incrementals", fullSubdir)
		return u.String(), nil
	})
	require.NoError(t, err)

	t.Run("resolve backup manifests with latest AOST", func(t *testing.T) {
		uris, manifests, locality, memSize, err := ResolveBackupManifests(
			ctx,
			&execCfg,
			&mem,
			collections[0],
			collections,
			execCfg.DistSQLSrv.ExternalStorageFromURI,
			fullSubdir,
			fullyResolvedBaseDirs,
			fullyResolvedIncDirs,
			hlc.Timestamp{},
			nil, /* encryption */
			nil, /* kms */
			username.RootUserName(),
			false, /* includeSkipped */
			true,  /* includeCompacted */
		)
		defer mem.Shrink(ctx, memSize)
		require.NoError(t, err)

		require.Len(t, uris, 2)
		require.Len(t, manifests, 2)
		require.Len(t, locality, 2)
	})

	t.Run("resolve backup manifests with AOST in middle of chain", func(t *testing.T) {
		uris, manifests, locality, memSize, err := ResolveBackupManifests(
			ctx,
			&execCfg,
			&mem,
			collections[0],
			collections,
			execCfg.DistSQLSrv.ExternalStorageFromURI,
			fullSubdir,
			fullyResolvedBaseDirs,
			fullyResolvedIncDirs,
			aost1Hlc,
			nil, /* encryption */
			nil, /* kms */
			username.RootUserName(),
			false, /* includeSkipped */
			true,  /* includeCompacted */
		)
		defer mem.Shrink(ctx, memSize)
		require.NoError(t, err)

		require.Len(t, uris, 1)
		require.Len(t, manifests, 1)
		require.Len(t, locality, 1)
	})
}

func TestIndexedResolveBackupManifestsWithCompactedBackups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, db, _, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(
		t, backuptestutils.SingleNode,
	)
	defer cleanupFn()

	const collectionURI = "nodelocal://1/backup"

	execCfg := tc.Server(0).ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)
	mem := execCfg.RootMemoryMonitor.MakeBoundAccount()

	db.Exec(t, "SET CLUSTER SETTING backup.index.read.enabled = true")

	var aostFull string
	db.QueryRow(t, "SELECT now()").Scan(&aostFull)
	aostFullTS, err := time.Parse(time.RFC3339, aostFull)
	require.NoError(t, err)
	aostFullHlc := hlc.Timestamp{WallTime: aostFullTS.UnixNano()}

	db.Exec(
		t, "BACKUP INTO $1 AS OF SYSTEM TIME $2::STRING",
		collectionURI, aostFullHlc.AsOfSystemTime(),
	)
	db.Exec(t, "BACKUP INTO LATEST IN $1", collectionURI)
	db.Exec(t, "BACKUP INTO LATEST IN $1", collectionURI)

	var aostEnd string
	db.QueryRow(t, "SELECT now()").Scan(&aostEnd)
	aostEndTS, err := time.Parse(time.RFC3339, aostEnd)
	require.NoError(t, err)
	aostEndHlc := hlc.Timestamp{WallTime: aostEndTS.UnixNano()}
	db.Exec(
		t, "BACKUP INTO LATEST IN $1 AS OF SYSTEM TIME $2::STRING",
		collectionURI, aostEndHlc.AsOfSystemTime(),
	)

	var fullSubdir string
	db.QueryRow(
		t, `SELECT path FROM [SHOW BACKUPS IN $1] ORDER BY path DESC`, collectionURI,
	).Scan(&fullSubdir)
	require.NotEmpty(t, fullSubdir)

	var compactionJob jobspb.JobID
	db.QueryRow(
		t,
		`SELECT crdb_internal.backup_compaction(
				0, $1, $2, $3::DECIMAL, $4::DECIMAL
			)`,
		fmt.Sprintf("BACKUP INTO LATEST IN '%s'", collectionURI),
		fullSubdir,
		aostFullHlc.AsOfSystemTime(),
		aostEndHlc.AsOfSystemTime(),
	).Scan(&compactionJob)
	jobutils.WaitForJobToSucceed(t, db, compactionJob)

	// Backup chain: f@t=0, i1@t=1, i2@t=2, i3@t=4, c@t=4
	// where c compacts i1, i2, and i3.
	const totalBackups = 5
	const filesPerManifest = 2 // Opening a manifest requires opening the metadata file and checksum
	for _, tt := range []struct {
		includeSkipped   bool
		includeCompacted bool
		expectedCount    int
	}{
		{
			includeSkipped:   false,
			includeCompacted: true,
			expectedCount:    2,
		},
		{
			includeSkipped:   true,
			includeCompacted: true,
			expectedCount:    5,
		},
		{
			includeSkipped:   false,
			includeCompacted: false,
			expectedCount:    4,
		},
		{
			includeSkipped:   true,
			includeCompacted: false,
			expectedCount:    4,
		},
	} {
		t.Run(
			fmt.Sprintf("includeSkipped=%t,includeCompacted=%t", tt.includeSkipped, tt.includeCompacted),
			func(t *testing.T) {
				baseReadersOpened := tc.Servers[0].MustGetSQLCounter("cloud.readers_opened")
				uris, manifests, locality, memSize, err := indexedResolveBackupManifests(
					ctx,
					&mem,
					[]string{collectionURI},
					execCfg.DistSQLSrv.ExternalStorageFromURI,
					fullSubdir,
					hlc.Timestamp{},
					nil, /* encryption */
					nil, /* kms */
					username.RootUserName(),
					tt.includeSkipped,
					tt.includeCompacted,
				)
				defer mem.Shrink(ctx, memSize)
				require.NoError(t, err)
				require.Len(t, uris, tt.expectedCount)
				require.Len(t, manifests, tt.expectedCount)
				require.Len(t, locality, tt.expectedCount)

				// When using the index for ResolveBackupManifests, we use the following
				// flow:
				// 1. Open the index of every backup in the chain (<totalBackups> files)
				// 2. Validate and truncate the chain to only keep relevant backups
				// 3. Read the files required for the manifests of the relevant backups
				//
				// So to compute step 3, we take the total number of files opened and
				// subtract the index files, aka readersOpened - totalBackups.
				//
				// And for each manifest, we open <filesPerManifest> files to read the
				// manifest. So to compute the number of manifests opened, we take
				// readersOpened - totalBackups and divide by filesPerManifest.
				readersOpened := tc.Servers[0].MustGetSQLCounter("cloud.readers_opened") - baseReadersOpened
				manifestsOpened := (readersOpened - totalBackups) / filesPerManifest
				require.Equal(t, tt.expectedCount, int(manifestsOpened))
			},
		)
	}
}

// writeBackupIndexFromManifest is a helper function that writes a backup
// index metadata file using a manifest. This may be useful in tests that
// manually construct backup collections and need to write a corresponding index
// file for a manifest.
func writeBackupIndexFromManifest(
	t *testing.T,
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	collectionURI string,
	backupURI string,
	subdir string,
	manifest backuppb.BackupManifest,
) {
	t.Helper()
	details := jobspb.BackupDetails{
		Destination: jobspb.BackupDetails_Destination{
			To:     []string{collectionURI},
			Subdir: subdir,
			Exists: !manifest.StartTime.IsEmpty(),
		},
		EndTime:         manifest.EndTime,
		StartTime:       manifest.StartTime,
		CollectionURI:   collectionURI,
		URI:             backupURI,
		Compact:         manifest.IsCompacted,
		RevisionHistory: manifest.MVCCFilter == backuppb.MVCCFilter_All,
	}
	err := backupinfo.WriteBackupIndexMetadata(
		ctx,
		execCfg,
		username.RootUserName(),
		execCfg.DistSQLSrv.ExternalStorageFromURI,
		details,
		manifest.RevisionStartTime,
	)
	require.NoError(t, err)
}
