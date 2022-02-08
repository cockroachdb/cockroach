// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"path"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func newTestStorageFactory(t *testing.T) (cloud.ExternalStorageFromURIFactory, func()) {
	dir, dirCleanupFn := testutils.TempDir(t)
	settings := cluster.MakeTestingClusterSettings()
	settings.ExternalIODir = dir
	clientFactory := blobs.TestBlobServiceClient(settings.ExternalIODir)
	externalStorageFromURI := func(ctx context.Context, uri string, user security.SQLUsername) (cloud.ExternalStorage,
		error) {
		conf, err := cloud.ExternalStorageConfFromURI(uri, user)
		require.NoError(t, err)
		return nodelocal.TestingMakeLocalStorage(ctx, conf.LocalFile, settings, clientFactory, base.ExternalIODirConfig{})
	}
	return externalStorageFromURI, dirCleanupFn
}

// TestBackupRestoreResolveDestination is an integration style tests that tests
// all of the expected ways of organizing backups.
func TestBackupRestoreResolveDestination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	emptyReader := bytes.NewReader([]byte{})

	// Setup a storage factory.
	externalStorageFromURI, cleanup := newTestStorageFactory(t)
	defer cleanup()

	// writeManifest writes an empty backup manifest file to the given URI.
	writeManifest := func(t *testing.T, uri string) {
		storage, err := externalStorageFromURI(ctx, uri, security.RootUserName())
		defer storage.Close()
		require.NoError(t, err)
		require.NoError(t, cloud.WriteFile(ctx, storage, backupManifestName, emptyReader))
	}

	// writeLatest writes latestBackupSuffix to the LATEST file in the given
	// collection.
	writeLatest := func(t *testing.T, collectionURI, latestBackupSuffix string) {
		storage, err := externalStorageFromURI(ctx, collectionURI, security.RootUserName())
		defer storage.Close()
		require.NoError(t, err)
		require.NoError(t, writeNewLatestFile(ctx, storage.Settings(), storage, latestBackupSuffix))
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
		allLocalities := append([]string{defaultLocalityValue}, localities...)
		localizedURIs := make([]string, len(allLocalities))
		for i, locality := range allLocalities {
			parsedURI, err := url.Parse(baseURI)
			require.NoError(t, err)
			if locality != defaultLocalityValue {
				parsedURI.Path = path.Join(parsedURI.Path, locality)
			}
			q := parsedURI.Query()
			q.Add(localityURLParam, locality)
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
					defaultDest, localitiesDest, err := getURIsByLocalityKV(to, "")
					require.NoError(t, err)

					collectionURI, defaultURI, chosenSuffix, urisByLocalityKV, prevBackupURIs, err := resolveDest(
						ctx, security.RootUserName(),
						jobspb.BackupDetails_Destination{To: to},
						externalStorageFromURI, endTime,
						incrementalFrom,
					)
					require.NoError(t, err)

					// Not an INTO backup, so no collection of suffix info.
					require.Equal(t, "", collectionURI)
					require.Equal(t, "", chosenSuffix)

					require.Equal(t, defaultDest, defaultURI)
					require.Equal(t, localitiesDest, urisByLocalityKV)
					require.Equal(t, incrementalFrom, prevBackupURIs)
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

					collectionURI, defaultURI, chosenSuffix, urisByLocalityKV, prevBackupURIs, err := resolveDest(
						ctx, security.RootUserName(),
						jobspb.BackupDetails_Destination{To: to},
						externalStorageFromURI, endTime,
						nil, /* incrementalFrom */
					)
					require.NoError(t, err)

					// Not a backup collection.
					require.Equal(t, "", collectionURI)
					require.Equal(t, "", chosenSuffix)
					require.Equal(t, expectedDefault, defaultURI)
					require.Equal(t, expectedLocalities, urisByLocalityKV)
					require.Equal(t, expectedPrevBackups, prevBackupURIs)
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
					expectedDefault := fmt.Sprintf("nodelocal://1/%s/20201225/063000.00?AUTH=implicit", t.Name())
					expectedLocalities := make(map[string]string)
					for _, locality := range localities {
						expectedLocalities[locality] = fmt.Sprintf("nodelocal://1/%s/%s/20201225/063000.00?AUTH=implicit", t.Name(), locality)
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
						"nodelocal://1/%s/20201225/070000.00?AUTH=implicit", t.Name())
					expectedLocalities := make(map[string]string)
					for _, locality := range localities {
						expectedLocalities[locality] = fmt.Sprintf(
							"nodelocal://1/%s/%s/20201225/070000.00?AUTH=implicit",
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
				collectionLoc := fmt.Sprintf("nodelocal://1/%s/?AUTH=implicit", t.Name())
				collectionTo := localizeURI(t, collectionLoc, localities)
				incrementalStorageLoc := fmt.Sprintf("nodelocal://2/incremental/%s/?AUTH=implicit", t.Name())
				incrementalTo := localizeURI(t, incrementalStorageLoc, localities)
				fullTime := time.Date(2020, 12, 25, 6, 0, 0, 0, time.UTC)
				inc1Time := fullTime.Add(time.Minute * 30)
				inc2Time := inc1Time.Add(time.Minute * 30)
				full2Time := inc2Time.Add(time.Minute * 30)
				inc3Time := full2Time.Add(time.Minute * 30)
				inc4Time := inc3Time.Add(time.Minute * 30)
				inc5Time := inc4Time.Add(time.Minute * 30)
				inc6Time := inc5Time.Add(time.Minute * 30)
				inc7Time := inc6Time.Add(time.Minute * 30)

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
						subdir = latestFileName
					} else if subdir == "" {
						subdir = endTime.GoTime().Format(DateBasedIntoFolderName)
					}

					_, localityCollections, err := getURIsByLocalityKV(collectionTo, "")
					require.NoError(t, err)

					if len(incrementalTo) > 0 {
						_, localityCollections, err = getURIsByLocalityKV(incrementalTo, "")
						require.NoError(t, err)
					}
					collectionURI, defaultURI, chosenSuffix, urisByLocalityKV, prevBackupURIs, err := resolveDest(
						ctx, security.RootUserName(),
						jobspb.BackupDetails_Destination{To: collectionTo, Subdir: subdir, IncrementalStorage: incrementalTo},
						externalStorageFromURI,
						endTime,
						incrementalFrom,
					)
					require.NoError(t, err)

					localityDests := make(map[string]string, len(localityCollections))
					for locality, localityDest := range localityCollections {
						u, err := url.Parse(localityDest)
						require.NoError(t, err)
						u.Path = u.Path + expectedSuffix + expectedIncDir
						localityDests[locality] = u.String()
					}

					require.Equal(t, collectionLoc, collectionURI)
					require.Equal(t, expectedSuffix, chosenSuffix)

					require.Equal(t, expectedDefault, defaultURI)
					require.Equal(t, localityDests, urisByLocalityKV)

					require.Equal(t, expectedPrevBackups, prevBackupURIs)
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
					expectedDefault := fmt.Sprintf("nodelocal://1/%s%s%s?AUTH=implicit", t.Name(), expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc1Time,
						expectedDefault, expectedSuffix, expectedIncDir, firstBackupChain,
						true /* intoLatest */, noExplicitSubDir, noIncrementalStorage)
					firstBackupChain = append(firstBackupChain, expectedDefault)
					writeManifest(t, expectedDefault)
				}

				// Another incremental: BACKUP INTO LATEST IN collection
				{
					// We're backing up to the full backup at 6am.
					expectedSuffix := "/2020/12/25-060000.00"
					expectedIncDir := "/20201225/070000.00"
					expectedDefault := fmt.Sprintf("nodelocal://1/%s%s%s?AUTH=implicit", t.Name(), expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc2Time,
						expectedDefault, expectedSuffix, expectedIncDir, firstBackupChain,
						true /* intoLatest */, noExplicitSubDir, noIncrementalStorage)
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
					expectedDefault := fmt.Sprintf("nodelocal://1/%s%s%s?AUTH=implicit", t.Name(), expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc3Time,
						expectedDefault, expectedSuffix, expectedIncDir, []string{backup2Location},
						true /* intoLatest */, noExplicitSubDir, noIncrementalStorage)
					writeManifest(t, expectedDefault)
				}

				// An explicit incremental into the first full: BACKUP INTO full1 IN collection
				{
					expectedSuffix := "/2020/12/25-060000.00"
					expectedIncDir := "/20201225/083000.00"
					expectedSubdir := expectedSuffix
					expectedDefault := fmt.Sprintf("nodelocal://1/%s%s%s?AUTH=implicit", t.Name(), expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc4Time,
						expectedDefault, expectedSuffix, expectedIncDir, firstBackupChain,
						false /* intoLatest */, expectedSubdir, noIncrementalStorage)
					writeManifest(t, expectedDefault)
				}

				// A remote incremental into the first full: BACKUP INTO full1 IN collection, incremental_location = inc_storage_path
				{
					expectedSuffix := "/2020/12/25-060000.00"
					expectedIncDir := "/20201225/090000.00"
					expectedSubdir := expectedSuffix

					expectedDefault := fmt.Sprintf("nodelocal://2/incremental/%s%s%s?AUTH=implicit",
						t.Name(),
						expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc5Time,
						expectedDefault, expectedSuffix, expectedIncDir, firstRemoteBackupChain,
						false /* intoLatest */, expectedSubdir, incrementalTo)
					writeManifest(t, expectedDefault)

					firstRemoteBackupChain = append(firstRemoteBackupChain, expectedDefault)
				}

				// Another remote incremental into the first full: BACKUP INTO full1 IN collection, incremental_location = inc_storage_path
				{
					expectedSuffix := "/2020/12/25-060000.00"
					expectedIncDir := "/20201225/093000.00"
					expectedSubdir := expectedSuffix

					expectedDefault := fmt.Sprintf("nodelocal://2/incremental/%s%s%s?AUTH=implicit",
						t.Name(),
						expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc6Time,
						expectedDefault, expectedSuffix, expectedIncDir, firstRemoteBackupChain,
						false /* intoLatest */, expectedSubdir, incrementalTo)
					writeManifest(t, expectedDefault)
				}

				// A remote incremental into the second full backup: BACKUP INTO LATEST IN collection,
				//incremental_location = inc_storage_path
				{
					expectedSuffix := "/2020/12/25-073000.00"
					expectedIncDir := "/20201225/100000.00"
					expectedSubdir := expectedSuffix

					expectedDefault := fmt.Sprintf("nodelocal://2/incremental/%s%s%s?AUTH=implicit",
						t.Name(),
						expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc7Time,
						expectedDefault, expectedSuffix, expectedIncDir, []string{backup2Location},
						true /* intoLatest */, expectedSubdir, incrementalTo)
					writeManifest(t, expectedDefault)
				}

			})
		})
	}
}

// TODO(pbardea): Add tests for resolveBackupCollection.
