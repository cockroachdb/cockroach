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

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/backup/backupdest"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/backup/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/backup/backuputils"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl" // register cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
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

	tc, _, _, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(t, backuptestutils.MultiNode)
	defer cleanupFn()

	ctx := context.Background()
	execCfg := tc.Server(0).ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)

	externalStorageFromURI := execCfg.DistSQLSrv.ExternalStorageFromURI

	// writeManifest writes a backup manifest file containing only an endtime to the given URI.
	writeManifest := func(t *testing.T, uri string, endTime time.Time) {
		storage, err := externalStorageFromURI(ctx, uri, username.RootUserName())
		require.NoError(t, err)
		defer storage.Close()
		endTS := hlc.Timestamp{WallTime: endTime.UnixNano()}
		manifestBytes, err := protoutil.Marshal(&backuppb.BackupManifest{EndTime: endTS})
		require.NoError(t, err)
		reader := bytes.NewReader(manifestBytes)
		require.NoError(t, err)
		require.NoError(t, cloud.WriteFile(ctx, storage, backupbase.BackupManifestName, reader))
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
			// When testing auto-append backup locations, we'll be testing the name
			// resolution on backup directory structures created when running a sequence
			// of backups like:
			// - BACKUP INTO collection (full 1) @ 6
			// - BACKUP INTO LATEST IN collection @ 6:30
			// - BACKUP INTO LATEST IN collection @ 7
			// - BACKUP INTO collection (full 2) @ 7:30
			// - BACKUP INTO LATEST IN collection @ 8
			// - BACKUP INTO full1 IN collection @ 8:30
			// - BACKUP INTO full1 IN collection, incremental_location = inc_storage_path @ 9
			// - BACKUP INTO full1 IN collection, incremental_location = inc_storage_path @ 9:30
			// - BACKUP INTO LATEST IN collection, incremental_location = inc_storage_path @ 10
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

					fullBackupExists := expectedIncDir != ""
					kmsEnv := &cloud.TestKMSEnv{
						ExternalIOConfig: &base.ExternalIODirConfig{},
					}
					backupDest, err := backupdest.ResolveDest(
						ctx, username.RootUserName(),
						jobspb.BackupDetails_Destination{To: collectionTo, Subdir: subdir,
							IncrementalStorage: incrementalTo, Exists: fullBackupExists},
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
						false /* intoLatest */, noExplicitSubDir, noIncrementalStorage)
					firstBackupChain = append(firstBackupChain, expectedDefault)
					firstRemoteBackupChain = append(firstRemoteBackupChain, expectedDefault)
					writeManifest(t, expectedDefault, fullTime)
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
						true /* intoLatest */, noExplicitSubDir, defaultIncrementalTo)
					firstBackupChain = append(firstBackupChain, expectedDefault)
					writeManifest(t, expectedDefault, inc1Time)
				}

				// Another incremental: BACKUP INTO LATEST IN collection
				{
					// We're backing up to the full backup at 6am.
					expectedSuffix := "/2020/12/25-060000.00"
					expectedIncDir := "/20201225/070000.00-20201225-063000.00"
					expectedDefault := fmt.Sprintf("nodelocal://1/%s/incrementals%s%s?AUTH=implicit", t.Name(), expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc2Time,
						expectedDefault, expectedSuffix, expectedIncDir, firstBackupChain,
						true /* intoLatest */, noExplicitSubDir, defaultIncrementalTo)
					firstBackupChain = append(firstBackupChain, expectedDefault)
					writeManifest(t, expectedDefault, inc2Time)
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
					writeManifest(t, expectedDefault, full2Time)
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
						true /* intoLatest */, noExplicitSubDir, defaultIncrementalTo)
					writeManifest(t, expectedDefault, inc3Time)
				}

				// An explicit incremental into the first full: BACKUP INTO full1 IN collection
				{
					expectedSuffix := "/2020/12/25-060000.00"
					expectedIncDir := "/20201225/083000.00-20201225-070000.00"
					expectedSubdir := expectedSuffix
					expectedDefault := fmt.Sprintf("nodelocal://1/%s/incrementals%s%s?AUTH=implicit", t.Name(), expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc4Time,
						expectedDefault, expectedSuffix, expectedIncDir, firstBackupChain,
						false /* intoLatest */, expectedSubdir, defaultIncrementalTo)
					writeManifest(t, expectedDefault, inc4Time)
				}

				// A remote incremental into the first full: BACKUP INTO full1 IN collection, incremental_location = inc_storage_path
				{
					expectedSuffix := "/2020/12/25-060000.00"
					expectedIncDir := "/20201225/090000.00-20201225-060000.00"
					expectedSubdir := expectedSuffix

					expectedDefault := fmt.Sprintf("nodelocal://2/custom-incremental/%s%s%s?AUTH=implicit",
						t.Name(),
						expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc5Time,
						expectedDefault, expectedSuffix, expectedIncDir, firstRemoteBackupChain,
						false /* intoLatest */, expectedSubdir, customIncrementalTo)
					writeManifest(t, expectedDefault, inc5Time)

					firstRemoteBackupChain = append(firstRemoteBackupChain, expectedDefault)
				}

				// Another remote incremental into the first full: BACKUP INTO full1 IN collection, incremental_location = inc_storage_path
				{
					expectedSuffix := "/2020/12/25-060000.00"
					expectedIncDir := "/20201225/093000.00-20201225-090000.00"
					expectedSubdir := expectedSuffix

					expectedDefault := fmt.Sprintf("nodelocal://2/custom-incremental/%s%s%s?AUTH=implicit",
						t.Name(),
						expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc6Time,
						expectedDefault, expectedSuffix, expectedIncDir, firstRemoteBackupChain,
						false /* intoLatest */, expectedSubdir, customIncrementalTo)
					writeManifest(t, expectedDefault, inc6Time)
				}

				// A remote incremental into the second full backup: BACKUP INTO LATEST IN collection,
				//incremental_location = inc_storage_path
				{
					expectedSuffix := "/2020/12/25-073000.00"
					expectedIncDir := "/20201225/100000.00-20201225-073000.00"
					expectedSubdir := expectedSuffix

					expectedDefault := fmt.Sprintf("nodelocal://2/custom-incremental/%s%s%s?AUTH=implicit",
						t.Name(),
						expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc7Time,
						expectedDefault, expectedSuffix, expectedIncDir, []string{backup2Location},
						true /* intoLatest */, expectedSubdir, customIncrementalTo)
					writeManifest(t, expectedDefault, inc7Time)
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
					writeManifest(t, expectedDefault, full3Time)
					// We also wrote a new full backup, so let's update the latest.
					writeLatest(t, collectionLoc, expectedSuffix)
				}

				// A remote incremental into the third full backup: BACKUP INTO LATEST
				// IN collection, BUT with a trick. Write a (fake) incremental backup
				// to the old directory, to be sure that subsequent incremental backups
				// go there as well (and not the newer incrementals/ subdir.)
				{
					expectedSuffix := "/2020/12/25-103000.00"
					expectedIncDir := "/20201225/110000.00-20201225-103000.00"

					// Writes the (fake) incremental backup.
					oldStyleDefault := fmt.Sprintf("nodelocal://1/%s%s%s?AUTH=implicit",
						t.Name(),
						expectedSuffix, expectedIncDir)
					writeManifest(t, oldStyleDefault, inc8Time)

					expectedSuffix = "/2020/12/25-103000.00"
					expectedIncDir = "/20201225/113000.00-20201225-110000.00"
					expectedSubdir := expectedSuffix
					expectedDefault := fmt.Sprintf("nodelocal://1/%s%s%s?AUTH=implicit",
						t.Name(),
						expectedSuffix, expectedIncDir)

					testCollectionBackup(t, inc9Time,
						expectedDefault, expectedSuffix, expectedIncDir, []string{backup3Location, oldStyleDefault},
						true /* intoLatest */, expectedSubdir, noIncrementalStorage)
					writeManifest(t, expectedDefault, inc9Time)
				}

				// A new full backup: BACKUP INTO collection
				{
					expectedSuffix := "/2020/12/25-120000.00"
					expectedIncDir := ""
					expectedDefault := fmt.Sprintf("nodelocal://1/%s%s?AUTH=implicit", t.Name(), expectedSuffix)

					testCollectionBackup(t, full4Time,
						expectedDefault, expectedSuffix, expectedIncDir, []string(nil),
						false /* intoLatest */, noExplicitSubDir, noIncrementalStorage)
					writeManifest(t, expectedDefault, full4Time)
					// We also wrote a new full backup, so let's update the latest.
					writeLatest(t, collectionLoc, expectedSuffix)
				}
			})
		})
	}
}

// TODO(pbardea): Add tests for resolveBackupCollection.
