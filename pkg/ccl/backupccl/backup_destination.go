// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"net/url"
	"path"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// fetchPreviousBackups takes a list of URIs of previous backups and returns
// their manifest as well as the encryption options of the first backup in the
// chain.
func fetchPreviousBackups(
	ctx context.Context,
	mem *mon.BoundAccount,
	user security.SQLUsername,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	prevBackupURIs []string,
	encryptionParams jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) ([]BackupManifest, *jobspb.BackupEncryptionOptions, int64, error) {
	if len(prevBackupURIs) == 0 {
		return nil, nil, 0, nil
	}

	baseBackup := prevBackupURIs[0]
	encryptionOptions, err := getEncryptionFromBase(ctx, user, makeCloudStorage, baseBackup,
		encryptionParams, kmsEnv)
	if err != nil {
		return nil, nil, 0, err
	}
	prevBackups, size, err := getBackupManifests(ctx, mem, user, makeCloudStorage, prevBackupURIs,
		encryptionOptions)
	if err != nil {
		return nil, nil, 0, err
	}

	return prevBackups, encryptionOptions, size, nil
}

// resolveDest resolves the true destination of a backup. The backup command
// provided by the user may point to a backup collection, or a backup location
// which auto-appends incremental backups to it. This method checks for these
// cases and finds the actual directory where we'll write this new backup.
//
// In addition, in this case that this backup is an incremental backup (either
// explicitly, or due to the auto-append feature), it will resolve the
// encryption options based on the base backup, as well as find all previous
// backup manifests in the backup chain.
func resolveDest(
	ctx context.Context,
	user security.SQLUsername,
	dest jobspb.BackupDetails_Destination,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	endTime hlc.Timestamp,
	incrementalFrom []string,
) (
	string, /* collectionURI */
	string, /* defaultURI - the full path for the planned backup */
	string, /* chosenSuffix - the resolved subdir for the planned backup */
	map[string]string, /* urisByLocalityKV */
	[]string, /* prevBackupURIs - list of full paths for previous backups in the chain */
	error,
) {
	// chosenSuffix is the automatically chosen suffix within the collection path
	// if we're backing up INTO a collection.
	var collectionURI string
	var prevBackupURIs []string
	var chosenSuffix string
	var err error

	defaultURI, urisByLocalityKV, err := getURIsByLocalityKV(dest.To, "")
	if err != nil {
		return "", "", "", nil, nil, err
	}

	if dest.Subdir != "" {
		collectionURI = defaultURI
		chosenSuffix = dest.Subdir
		if chosenSuffix == latestFileName {
			latest, err := readLatestFile(ctx, defaultURI, makeCloudStorage, user)
			if err != nil {
				return "", "", "", nil, nil, err
			}
			chosenSuffix = latest
		}
		defaultURI, urisByLocalityKV, err = getURIsByLocalityKV(dest.To, chosenSuffix)
		if err != nil {
			return "", "", "", nil, nil, err
		}
	}

	// At this point, the defaultURI is the full path for the backup. For BACKUP
	// INTO, this path includes the chosenSuffix Once this function returns, the
	// defaultURI will be the full path for this backup in planning.
	if len(incrementalFrom) != 0 {
		prevBackupURIs = incrementalFrom
	} else {
		defaultStore, err := makeCloudStorage(ctx, defaultURI, user)
		if err != nil {
			return "", "", "", nil, nil, err
		}
		defer defaultStore.Close()
		exists, err := containsManifest(ctx, defaultStore)
		if err != nil {
			return "", "", "", nil, nil, err
		}
		if exists {
			// The defaultStore contains a full backup; consequently,
			// we're conducting an incremental backup.
			prevBackupURIs = append(prevBackupURIs, defaultURI)

			var priors []string
			var backupChainURI string
			if len(dest.IncrementalStorage) > 0 {
				// Implies the incremental backup chain lives in
				// incrementalStorage/chosenSuffix, while the full backup lives in
				// defaultStore/chosenSuffix. The incremental backup chain in
				// incrementalStorage/chosenSuffix will not contain any incremental backups that live
				// elsewhere.
				backupChainURI, _, err = getURIsByLocalityKV(dest.IncrementalStorage,
					chosenSuffix)
				if err != nil {
					return "", "", "", nil, nil, err
				}
				incrementalStore, err := makeCloudStorage(ctx, backupChainURI, user)
				if err != nil {
					return "", "", "", nil, nil, err
				}
				defer incrementalStore.Close()
				priors, err = FindPriorBackups(ctx, incrementalStore, OmitManifest)
				if err != nil {
					return "", "", "", nil, nil, err
				}
			} else {
				backupChainURI = defaultURI
				priors, err = FindPriorBackups(ctx, defaultStore, OmitManifest)
				if err != nil {
					return "", "", "", nil, nil, err
				}
			}
			for _, prior := range priors {
				priorURI, err := url.Parse(backupChainURI)
				if err != nil {
					return "", "", "", nil, nil, errors.Wrapf(err, "parsing default backup location %s",
						backupChainURI)
				}
				priorURI.Path = path.Join(priorURI.Path, prior)
				prevBackupURIs = append(prevBackupURIs, priorURI.String())
			}
			if err != nil {
				return "", "", "", nil, nil, errors.Wrap(err, "finding previous backups")
			}

			// Within the chosenSuffix dir, differentiate files with partName.
			partName := endTime.GoTime().Format(DateBasedIncFolderName)
			partName = path.Join(chosenSuffix, partName)
			if len(dest.IncrementalStorage) > 0 {
				defaultURI, urisByLocalityKV, err = getURIsByLocalityKV(dest.IncrementalStorage, partName)
			} else {
				defaultURI, urisByLocalityKV, err = getURIsByLocalityKV(dest.To, partName)
			}
			if err != nil {
				return "", "", "", nil, nil, errors.Wrap(err, "adjusting backup destination to append new layer to existing backup")
			}
		}
	}
	return collectionURI, defaultURI, chosenSuffix, urisByLocalityKV, prevBackupURIs, nil
}

// getBackupManifests fetches the backup manifest from a list of backup URIs.
func getBackupManifests(
	ctx context.Context,
	mem *mon.BoundAccount,
	user security.SQLUsername,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	backupURIs []string,
	encryption *jobspb.BackupEncryptionOptions,
) ([]BackupManifest, int64, error) {
	manifests := make([]BackupManifest, len(backupURIs))
	if len(backupURIs) == 0 {
		return manifests, 0, nil
	}

	memMu := struct {
		syncutil.Mutex
		total int64
		mem   *mon.BoundAccount
	}{}
	memMu.mem = mem

	g := ctxgroup.WithContext(ctx)
	for i := range backupURIs {
		i := i
		// boundAccount isn't threadsafe so we'll make a new one this goroutine to
		// pass while reading. When it is done, we'll lock an mu, reserve its size
		// from the main one tracking the total amount reserved.
		subMem := mem.Monitor().MakeBoundAccount()
		g.GoCtx(func(ctx context.Context) error {
			defer subMem.Close(ctx)
			// TODO(lucy): We may want to upgrade the table descs to the newer
			// foreign key representation here, in case there are backups from an
			// older cluster. Keeping the descriptors as they are works for now
			// since all we need to do is get the past backups' table/index spans,
			// but it will be safer for future code to avoid having older-style
			// descriptors around.
			uri := backupURIs[i]
			desc, size, err := ReadBackupManifestFromURI(
				ctx, &subMem, uri, user, makeCloudStorage, encryption,
			)
			if err != nil {
				return errors.Wrapf(err, "failed to read backup from %q",
					RedactURIForErrorMessage(uri))
			}

			memMu.Lock()
			err = memMu.mem.Grow(ctx, size)

			if err == nil {
				memMu.total += size
				manifests[i] = desc
			}
			subMem.Shrink(ctx, size)
			memMu.Unlock()

			return err
		})
	}

	if err := g.Wait(); err != nil {
		mem.Shrink(ctx, memMu.total)
		return nil, 0, err
	}

	return manifests, memMu.total, nil
}

// getEncryptionFromBase retrieves the encryption options of a base backup. It
// is expected that incremental backups use the same encryption options as the
// base backups.
func getEncryptionFromBase(
	ctx context.Context,
	user security.SQLUsername,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	baseBackupURI string,
	encryptionParams jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) (*jobspb.BackupEncryptionOptions, error) {
	var encryptionOptions *jobspb.BackupEncryptionOptions
	if encryptionParams.Mode != jobspb.EncryptionMode_None {
		exportStore, err := makeCloudStorage(ctx, baseBackupURI, user)
		if err != nil {
			return nil, err
		}
		defer exportStore.Close()
		opts, err := readEncryptionOptions(ctx, exportStore)
		if err != nil {
			return nil, err
		}

		switch encryptionParams.Mode {
		case jobspb.EncryptionMode_Passphrase:
			encryptionOptions = &jobspb.BackupEncryptionOptions{
				Mode: jobspb.EncryptionMode_Passphrase,
				Key:  storageccl.GenerateKey([]byte(encryptionParams.RawPassphrae), opts[0].Salt),
			}
		case jobspb.EncryptionMode_KMS:
			var defaultKMSInfo *jobspb.BackupEncryptionOptions_KMSInfo
			for _, encFile := range opts {
				defaultKMSInfo, err = validateKMSURIsAgainstFullBackup(encryptionParams.RawKmsUris,
					newEncryptedDataKeyMapFromProtoMap(encFile.EncryptedDataKeyByKMSMasterKeyID), kmsEnv)
				if err == nil {
					break
				}
			}
			if err != nil {
				return nil, err
			}
			encryptionOptions = &jobspb.BackupEncryptionOptions{
				Mode:    jobspb.EncryptionMode_KMS,
				KMSInfo: defaultKMSInfo}
		}
	}
	return encryptionOptions, nil
}

func readLatestFile(
	ctx context.Context,
	collectionURI string,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	user security.SQLUsername,
) (string, error) {
	collection, err := makeCloudStorage(ctx, collectionURI, user)
	if err != nil {
		return "", err
	}
	defer collection.Close()

	latestFile, err := findLatestFile(ctx, collection)

	if err != nil {
		if errors.Is(err, cloud.ErrFileDoesNotExist) {
			return "", pgerror.Wrapf(err, pgcode.UndefinedFile, "path does not contain a completed latest backup")
		}
		return "", pgerror.WithCandidateCode(err, pgcode.Io)
	}
	latest, err := ioctx.ReadAll(ctx, latestFile)
	if err != nil {
		return "", err
	}
	if len(latest) == 0 {
		return "", errors.Errorf("malformed LATEST file")
	}
	return string(latest), nil
}

// findLatestFile returns a ioctx.ReaderCloserCtx of the most recent LATEST
// file. First it tries reading from the latest-history directory. If
// the backup is from an older version, it may not exist there yet so
// it tries reading in the base directory if the first attempt fails.
func findLatestFile(
	ctx context.Context, exportStore cloud.ExternalStorage,
) (ioctx.ReadCloserCtx, error) {
	latestFile, err := exportStore.ReadFile(ctx, latestHistoryDirectory+"/"+latestFileName)
	if err != nil {
		if !errors.Is(err, cloud.ErrFileDoesNotExist) {
			return nil, err
		}

		latestFile, err = exportStore.ReadFile(ctx, latestFileName)
	}
	return latestFile, err
}

// writeNewLatestFile writes a new LATEST file to both the base directory
// and latest-history directory, depending on cluster version.
func writeNewLatestFile(
	ctx context.Context, settings *cluster.Settings, exportStore cloud.ExternalStorage, suffix string,
) error {
	// If the cluster is still running on a mixed version, we want to write
	// to the base directory as well the progress directory. That way if
	// an old node resumes a backup, it doesn't have to start over.
	if !settings.Version.IsActive(ctx, clusterversion.BackupDoesNotOverwriteLatestAndCheckpoint) {
		err := cloud.WriteFile(ctx, exportStore, latestFileName, strings.NewReader(suffix))
		if err != nil {
			return err
		}
	}

	return cloud.WriteFile(ctx, exportStore, latestHistoryDirectory+"/"+latestFileName, strings.NewReader(suffix))
}
