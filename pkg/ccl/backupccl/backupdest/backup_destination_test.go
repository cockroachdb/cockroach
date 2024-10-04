// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupdest_test

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupdest"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuputils"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl" // register cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestBackupRestoreResolveDestination is an integration style tests that tests
// all of the expected ways of organizing backups.
func TestBackupRestoreResolveDestination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc, _, _, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(t, backuptestutils.MultiNode)
	defer cleanupFn()

	ctx := context.Background()
	execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)
	emptyReader := bytes.NewReader([]byte{})

	externalStorageFromURI := execCfg.DistSQLSrv.ExternalStorageFromURI

	// writeManifest writes an empty backup manifest file to the given URI.
	writeManifest := func(t *testing.T, uri string) {
		storage, err := externalStorageFromURI(ctx, uri, username.RootUserName())
		defer storage.Close()
		require.NoError(t, err)
		require.NoError(t, cloud.WriteFile(ctx, storage, backupbase.BackupManifestName, emptyReader))
	}

	// writeLatest writes latestBackupSuffix to the LATEST file in the given
	// collection.
	writeLatest := func(t *testing.T, collectionURI, latestBackupSuffix string) {
		storage, err := externalStorageFromURI(ctx, collectionURI, username.RootUserName())
		defer storage.Close()
		require.NoError(t, err)
		require.NoError(t, backupdest.WriteNewLatestFile(ctx, storage.Settings(), storage, latestBackupSuffix))
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
		allLocalities := append([]string{backupdest.DefaultLocalityValue}, localities...)
		localizedURIs := make([]string, len(allLocalities))
		for i, locality := range allLocalities {
			parsedURI, err := url.Parse(baseURI)
			require.NoError(t, err)
			if locality != backupdest.DefaultLocalityValue {
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

			// When testing explicit backup locations, we'll be testing the name
			// resolution on backup directory structures created when running a
			// sequence of backups like:
			// - BACKUP TO full
			// - BACKUP TO inc1 INCREMENTAL FROM full
			// - BACKUP TO inc1 INCREMENTAL FROM full, inc1
			//
			// We write backup manifests as we test as if we were actually running the
			// backup.
			t.Run("explicit", func(t *testing.T) {
				fullLoc := fmt.Sprintf("nodelocal://1/%s?AUTH=implicit", t.Name())
				inc1Loc := fmt.Sprintf("nodelocal://1/%s/inc1?AUTH=implicit", t.Name())
				inc2Loc := fmt.Sprintf("nodelocal://1/%s/inc2?AUTH=implicit", t.Name())

				testExplicitBackup := func(t *testing.T, to []string, incrementalFrom []string) {
					// Time doesn't matter for these since we don't create any date-based
					// subdirectory. Let's just use now.
					endTime := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

					expectedPrevBackups := make([]string, len(incrementalFrom))
					for i, incrementalLoc := range incrementalFrom {
						expectedPrevBackups[i] = localizeURI(t, incrementalLoc, localities)[0]
					}
					defaultDest, localitiesDest, err := backupdest.GetURIsByLocalityKV(to, "")
					require.NoError(t, err)

					backupDest, err := backupdest.ResolveDest(
						ctx, username.RootUserName(),
						jobspb.BackupDetails_Destination{To: to},
						endTime,
						incrementalFrom,
						&execCfg,
					)
					require.NoError(t, err)

					// Not an INTO backup, so no collection of suffix info.
					require.Equal(t, "", backupDest.CollectionURI)
					require.Equal(t, "", backupDest.ChosenSubdir)

					require.Equal(t, defaultDest, backupDest.DefaultURI)
					require.Equal(t, localitiesDest, backupDest.URIsByLocalityKV)
					require.Equal(t, incrementalFrom, backupDest.PrevBackupURIs)
				}

				// The first initial full backup: BACKUP TO full.
				{
					incrementalFrom := []string(nil)
					to := localizeURI(t, fullLoc, localities)
					testExplicitBackup(t, to, incrementalFrom)

					// Write the manifest files as if this backup succeeded.
					writeManifest(t, fullLoc)
				}

				// An incremental on top if it: BACKUP TO inc1 INCREMENTAL FROM full.
				{
					incrementalFrom := []string{fullLoc}
					to := localizeURI(t, inc1Loc, localities)
					testExplicitBackup(t, to, incrementalFrom)

					// Write the manifest files as if this backup succeeded.
					writeManifest(t, inc1Loc)
				}

				// Another incremental on top of the incremental: BACKUP TO inc2
				// INCREMENTAL FROM full, inc1.
				{
					incrementalFrom := []string{fullLoc, inc1Loc}
					to := localizeURI(t, inc2Loc, localities)
					testExplicitBackup(t, to, incrementalFrom)

					writeManifest(t, inc2Loc)
				}
			})

			// When testing auto-append backup locations, we'll be testing the name
			// resolution on backup directory structures created when running a sequence
			// of backups like:
			// - BACKUP TO full
			// - BACKUP TO full
			// - BACKUP TO full
			t.Run("auto-append", func(t *testing.T) {
				baseDir := fmt.Sprintf("nodelocal://1/%s?AUTH=implicit", t.Name())
				fullTime := time.Date(2020, 12, 25, 6, 0, 0, 0, time.UTC)
				inc1Time := fullTime.Add(time.Minute * 30)
				inc2Time := inc1Time.Add(time.Minute * 30)
				prevBackups := []string(nil)

				testAutoAppendBackup := func(t *testing.T, to []string, backupTime time.Time,
					expectedDefault string, expectedLocalities map[string]string, expectedPrevBackups []string,
				) {
					endTime := hlc.Timestamp{WallTime: backupTime.UnixNano()}

					backupDest, err := backupdest.ResolveDest(
						ctx, username.RootUserName(),
						jobspb.BackupDetails_Destination{To: to},
						endTime,
						nil, /* incrementalFrom */
						&execCfg,
					)
					require.NoError(t, err)

					// Not a backup collection.
					require.Equal(t, "", backupDest.CollectionURI)
					require.Equal(t, "", backupDest.ChosenSubdir)
					require.Equal(t, expectedDefault, backupDest.DefaultURI)
					require.Equal(t, expectedLocalities, backupDest.URIsByLocalityKV)
					require.Equal(t, expectedPrevBackups, backupDest.PrevBackupURIs)
				}

				// Initial full backup: BACKUP TO baseDir.
				{
					to := localizeURI(t, baseDir, localities)
					// The full backup should go into the baseDir.
					expectedDefault := baseDir
					expectedLocalities := make(map[string]string)
					for _, locality := range localities {
						expectedLocalities[locality] = fmt.Sprintf("nodelocal://1/%s/%s?AUTH=implicit", t.Name(), locality)
					}

					testAutoAppendBackup(t, to, fullTime, expectedDefault, expectedLocalities, prevBackups)

					prevBackups = append(prevBackups, expectedDefault)
					writeManifest(t, expectedDefault)
				}

				// Incremental: BACKUP TO baseDir.
				{
					to := localizeURI(t, baseDir, localities)
					// The full backup should go into the baseDir.
					expectedDefault := fmt.Sprintf("nodelocal://1/%s/incrementals/20201225/063000.00?AUTH=implicit", t.Name())
					expectedLocalities := make(map[string]string)
					for _, locality := range localities {
						expectedLocalities[locality] = fmt.Sprintf("nodelocal://1/%s/%s/incrementals/20201225/063000.00?AUTH=implicit", t.Name(), locality)
					}

					testAutoAppendBackup(t, to, inc1Time, expectedDefault, expectedLocalities, prevBackups)

					prevBackups = append(prevBackups, expectedDefault)
					writeManifest(t, expectedDefault)
				}

				// Another incremental: BACKUP TO baseDir.
				{
					to := localizeURI(t, baseDir, localities)
					// We expect another incremental to go into the appropriate time
					// formatted sub-directory.
					expectedDefault := fmt.Sprintf(
						"nodelocal://1/%s/incrementals/20201225/070000.00?AUTH=implicit", t.Name())
					expectedLocalities := make(map[string]string)
					for _, locality := range localities {
						expectedLocalities[locality] = fmt.Sprintf(
							"nodelocal://1/%s/%s/incrementals/20201225/070000.00?AUTH=implicit",
							t.Name(), locality)
					}

					testAutoAppendBackup(t, to, inc2Time, expectedDefault, expectedLocalities, prevBackups)
					writeManifest(t, expectedDefault)
				}
			})

			// When testing auto-append backup locations, we'll be testing the name
			// resolution on backup directory structures created when running a sequence
			// of backups like:
			// - BACKUP INTO collection
			// - BACKUP INTO LATEST IN collection
			// - BACKUP INTO LATEST IN collection
			// - BACKUP INTO collection
			// - BACKUP INTO LATEST IN collection
			// - BACKUP INTO full1 IN collection
			// - BACKUP INTO full1 IN collection, incremental_location = inc_storage_path
			// - BACKUP INTO full1 IN collection, incremental_location = inc_storage_path
			// - BACKUP INTO LATEST IN collection, incremental_location = inc_storage_path
			t.Run("collection", func(t *testing.T) {
				collectionLoc := fmt.Sprintf("nodelocal://1/%s?AUTH=implicit", t.Name())
				// Note that this default is NOT arbitrary, but rather hard-coded as
				// the `/incrementals` subdir in the collection.
				defaultIncrementalStorageLoc := fmt.Sprintf("nodelocal://1/%s/incrementals?AUTH=implicit", t.Name())

				collectionTo := localizeURI(t, collectionLoc, localities)
				defaultIncrementalTo := localizeURI(t, defaultIncrementalStorageLoc, localities)

				// This custom location is arbitrary.
				customIncrementalStorageLoc := fmt.Sprintf("nodelocal://2/custom-incremental/%s?AUTH=implicit", t.Name())
				customIncrementalTo := localizeURI(t, customIncrementalStorageLoc, localities)

				fullTime := time.Date(2020, 12, 25, 6, 0, 0, 0, time.UTC)
				inc1Time := fullTime.Add(time.Minute * 30)
				inc2Time := inc1Time.Add(time.Minute * 30)
				full2Time := inc2Time.Add(time.Minute * 30)
				inc3Time := full2Time.Add(time.Minute * 30)
				inc4Time := inc3Time.Add(time.Minute * 30)
				inc5Time := inc4Time.Add(time.Minute * 30)
				inc6Time := inc5Time.Add(time.Minute * 30)
				inc7Time := inc6Time.Add(time.Minute * 30)
				full3Time := inc7Time.Add(time.Minute * 30)
				inc8Time := full3Time.Add(time.Minute * 30)
				inc9Time := inc8Time.Add(time.Minute * 30)
				full4Time := inc9Time.Add(time.Minute * 30)

				// firstBackupChain is maintained throughout the tests as the history of
				// backups that were taken based on the initial full backup.
				firstBackupChain := []string(nil)
				// An explicit sub-directory is used in backups of the form BACKUP INTO
				// X IN Y. Otherwise, it should be empty string.
				noExplicitSubDir := ""

				// An explicit path(s) is used for incremental backups that live in a
				// separate path relative to the full backup in their chain. Otherwise,
				// it should be an empty array of strings
				noIncrementalStorage := []string(nil)

				firstRemoteBackupChain := []string(nil)

				testCollectionBackup := func(t *testing.T, backupTime time.Time,
					expectedDefault, expectedSuffix, expectedIncDir string, expectedPrevBackups []string,
					appendToLatest bool, subdir string, incrementalTo []string) {

					endTime := hlc.Timestamp{WallTime: backupTime.UnixNano()}
					incrementalFrom := []string(nil)

					if appendToLatest {
						subdir = backupbase.LatestFileName
					} else if subdir == "" {
						subdir = endTime.GoTime().Format(backupbase.DateBasedIntoFolderName)
					}

					_, localityCollections, err := backupdest.GetURIsByLocalityKV(collectionTo, "")
					require.NoError(t, err)

					if len(incrementalTo) > 0 {
						_, localityCollections, err = backupdest.GetURIsByLocalityKV(incrementalTo, "")
						require.NoError(t, err)
					}

					fullBackupExists := false
					if expectedIncDir != "" {
						fullBackupExists = true
					}
					backupDest, err := backupdest.ResolveDest(
						ctx, username.RootUserName(),
						jobspb.BackupDetails_Destination{To: collectionTo, Subdir: subdir,
							IncrementalStorage: incrementalTo, Exists: fullBackupExists},
						endTime,
						incrementalFrom,
						&execCfg,
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
						false /* intoLatest */, noExplicitSubDir, noIncrementalStorage)
					firstBackupChain = append(firstBackupChain, expectedDefault)
					firstRemoteBackupChain = append(firstRemoteBackupChain, expectedDefault)
					writeManifest(t, expectedDefault)
					// We also wrote a new full backup, so let's update the latest.
					writeLatest(t, collectionLoc, expectedSuffix)
				}

				// Incremental: BACKUP INTO LATEST IN collection
				{
					// We're backing up to the full backup at 6am.
					expectedSuffix := "/2020/12/25-060000.00"
					expectedIncDir := "/20201225/063000.00"
					expectedDefault := fmt.Sprintf("nodelocal://1/%s/incrementals%s%s?AUTH=implicit", t.Name(), expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc1Time,
						expectedDefault, expectedSuffix, expectedIncDir, firstBackupChain,
						true /* intoLatest */, noExplicitSubDir, defaultIncrementalTo)
					firstBackupChain = append(firstBackupChain, expectedDefault)
					writeManifest(t, expectedDefault)
				}

				// Another incremental: BACKUP INTO LATEST IN collection
				{
					// We're backing up to the full backup at 6am.
					expectedSuffix := "/2020/12/25-060000.00"
					expectedIncDir := "/20201225/070000.00"
					expectedDefault := fmt.Sprintf("nodelocal://1/%s/incrementals%s%s?AUTH=implicit", t.Name(), expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc2Time,
						expectedDefault, expectedSuffix, expectedIncDir, firstBackupChain,
						true /* intoLatest */, noExplicitSubDir, defaultIncrementalTo)
					firstBackupChain = append(firstBackupChain, expectedDefault)
					writeManifest(t, expectedDefault)
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
						false /* intoLatest */, noExplicitSubDir, noIncrementalStorage)
					writeManifest(t, expectedDefault)
					// We also wrote a new full backup, so let's update the latest.
					writeLatest(t, collectionLoc, expectedSuffix)
				}

				// An incremental into the new latest: BACKUP INTO LATEST IN collection
				{
					// We're backing up to the full backup at 7:30am.
					expectedSuffix := "/2020/12/25-073000.00"
					expectedIncDir := "/20201225/080000.00"
					expectedDefault := fmt.Sprintf("nodelocal://1/%s/incrementals%s%s?AUTH=implicit", t.Name(), expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc3Time,
						expectedDefault, expectedSuffix, expectedIncDir, []string{backup2Location},
						true /* intoLatest */, noExplicitSubDir, defaultIncrementalTo)
					writeManifest(t, expectedDefault)
				}

				// An explicit incremental into the first full: BACKUP INTO full1 IN collection
				{
					expectedSuffix := "/2020/12/25-060000.00"
					expectedIncDir := "/20201225/083000.00"
					expectedSubdir := expectedSuffix
					expectedDefault := fmt.Sprintf("nodelocal://1/%s/incrementals%s%s?AUTH=implicit", t.Name(), expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc4Time,
						expectedDefault, expectedSuffix, expectedIncDir, firstBackupChain,
						false /* intoLatest */, expectedSubdir, defaultIncrementalTo)
					writeManifest(t, expectedDefault)
				}

				// A remote incremental into the first full: BACKUP INTO full1 IN collection, incremental_location = inc_storage_path
				{
					expectedSuffix := "/2020/12/25-060000.00"
					expectedIncDir := "/20201225/090000.00"
					expectedSubdir := expectedSuffix

					expectedDefault := fmt.Sprintf("nodelocal://2/custom-incremental/%s%s%s?AUTH=implicit",
						t.Name(),
						expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc5Time,
						expectedDefault, expectedSuffix, expectedIncDir, firstRemoteBackupChain,
						false /* intoLatest */, expectedSubdir, customIncrementalTo)
					writeManifest(t, expectedDefault)

					firstRemoteBackupChain = append(firstRemoteBackupChain, expectedDefault)
				}

				// Another remote incremental into the first full: BACKUP INTO full1 IN collection, incremental_location = inc_storage_path
				{
					expectedSuffix := "/2020/12/25-060000.00"
					expectedIncDir := "/20201225/093000.00"
					expectedSubdir := expectedSuffix

					expectedDefault := fmt.Sprintf("nodelocal://2/custom-incremental/%s%s%s?AUTH=implicit",
						t.Name(),
						expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc6Time,
						expectedDefault, expectedSuffix, expectedIncDir, firstRemoteBackupChain,
						false /* intoLatest */, expectedSubdir, customIncrementalTo)
					writeManifest(t, expectedDefault)
				}

				// A remote incremental into the second full backup: BACKUP INTO LATEST IN collection,
				//incremental_location = inc_storage_path
				{
					expectedSuffix := "/2020/12/25-073000.00"
					expectedIncDir := "/20201225/100000.00"
					expectedSubdir := expectedSuffix

					expectedDefault := fmt.Sprintf("nodelocal://2/custom-incremental/%s%s%s?AUTH=implicit",
						t.Name(),
						expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc7Time,
						expectedDefault, expectedSuffix, expectedIncDir, []string{backup2Location},
						true /* intoLatest */, expectedSubdir, customIncrementalTo)
					writeManifest(t, expectedDefault)
				}

				// A new full backup: BACKUP INTO collection
				var backup3Location string
				{
					expectedSuffix := "/2020/12/25-103000.00"
					expectedIncDir := ""
					expectedDefault := fmt.Sprintf("nodelocal://1/%s%s?AUTH=implicit", t.Name(), expectedSuffix)
					backup3Location = expectedDefault

					testCollectionBackup(t, full3Time,
						expectedDefault, expectedSuffix, expectedIncDir, []string(nil),
						false /* intoLatest */, noExplicitSubDir, noIncrementalStorage)
					writeManifest(t, expectedDefault)
					// We also wrote a new full backup, so let's update the latest.
					writeLatest(t, collectionLoc, expectedSuffix)
				}

				// A remote incremental into the third full backup: BACKUP INTO LATEST
				// IN collection, BUT with a trick. Write a (fake) incremental backup
				// to the old directory, to be sure that subsequent incremental backups
				// go there as well (and not the newer incrementals/ subdir.)
				{
					expectedSuffix := "/2020/12/25-103000.00"
					expectedIncDir := "/20201225/110000.00"

					// Writes the (fake) incremental backup.
					oldStyleDefault := fmt.Sprintf("nodelocal://1/%s%s%s?AUTH=implicit",
						t.Name(),
						expectedSuffix, expectedIncDir)
					writeManifest(t, oldStyleDefault)

					expectedSuffix = "/2020/12/25-103000.00"
					expectedIncDir = "/20201225/113000.00"
					expectedSubdir := expectedSuffix
					expectedDefault := fmt.Sprintf("nodelocal://1/%s%s%s?AUTH=implicit",
						t.Name(),
						expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc9Time,
						expectedDefault, expectedSuffix, expectedIncDir, []string{backup3Location, oldStyleDefault},
						true /* intoLatest */, expectedSubdir, noIncrementalStorage)
					writeManifest(t, expectedDefault)
				}

				// A new full backup: BACKUP INTO collection
				{
					expectedSuffix := "/2020/12/25-120000.00"
					expectedIncDir := ""
					expectedDefault := fmt.Sprintf("nodelocal://1/%s%s?AUTH=implicit", t.Name(), expectedSuffix)

					testCollectionBackup(t, full4Time,
						expectedDefault, expectedSuffix, expectedIncDir, []string(nil),
						false /* intoLatest */, noExplicitSubDir, noIncrementalStorage)
					writeManifest(t, expectedDefault)
					// We also wrote a new full backup, so let's update the latest.
					writeLatest(t, collectionLoc, expectedSuffix)
				}
			})
		})
	}
}

// TODO(pbardea): Add tests for resolveBackupCollection.
