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
	"io/ioutil"
	"net/url"
	"path"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// fetchPreviousBackups takes a list of URIs of previous backups and returns
// their manifest as well as the encryption options of the first backup in the
// chain.
func fetchPreviousBackups(
	ctx context.Context,
	user security.SQLUsername,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	prevBackupURIs []string,
	encryptionParams backupEncryptionParams,
) ([]BackupManifest, *jobspb.BackupEncryptionOptions, error) {
	if len(prevBackupURIs) == 0 {
		return nil, nil, nil
	}

	baseBackup := prevBackupURIs[0]
	encryptionOptions, err := getEncryptionFromBase(ctx, user, makeCloudStorage, baseBackup,
		encryptionParams)
	if err != nil {
		return nil, nil, err
	}
	prevBackups, err := getBackupManifests(ctx, user, makeCloudStorage, prevBackupURIs,
		encryptionOptions)
	if err != nil {
		return nil, nil, err
	}

	return prevBackups, encryptionOptions, nil
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
//
// TODO(pbardea): Cleanup list for after stability
//  - We shouldn't need to pass `to` and (`defaultURI`, `urisByLocalityKV`). We
//  can determine the latter from the former.
func resolveDest(
	ctx context.Context,
	user security.SQLUsername,
	nested, appendToLatest bool,
	defaultURI string,
	urisByLocalityKV map[string]string,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	endTime hlc.Timestamp,
	to []string,
	incrementalFrom []string,
	subdir string,
) (
	string, /* collectionURI */
	string, /* defaultURI */
	string, /* chosenSuffix */
	map[string]string, /* urisByLocalityKV */
	[]string, /* prevBackupURIs */
	error,
) {
	// chosenSuffix is the automatically chosen suffix within the collection path
	// if we're backing up INTO a collection.
	var collectionURI string
	var prevBackupURIs []string
	var chosenSuffix string
	var err error

	if nested {
		collectionURI, chosenSuffix, err = resolveBackupCollection(ctx, user, defaultURI,
			appendToLatest, makeCloudStorage, endTime, subdir)
		if err != nil {
			return "", "", "", nil, nil, err
		}

		defaultURI, urisByLocalityKV, err = getURIsByLocalityKV(to, chosenSuffix)
		if err != nil {
			return "", "", "", nil, nil, err
		}
	}

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
			// The backup in the auto-append directory is the full backup.
			prevBackupURIs = append(prevBackupURIs, defaultURI)
			priors, err := FindPriorBackups(ctx, defaultStore, OmitManifest)
			for _, prior := range priors {
				priorURI, err := url.Parse(defaultURI)
				if err != nil {
					return "", "", "", nil, nil, errors.Wrapf(err, "parsing default backup location %s", defaultURI)
				}
				priorURI.Path = path.Join(priorURI.Path, prior)
				prevBackupURIs = append(prevBackupURIs, priorURI.String())
			}
			if err != nil {
				return "", "", "", nil, nil, errors.Wrap(err, "finding previous backups")
			}

			// Pick a piece-specific suffix and update the destination path(s).
			partName := endTime.GoTime().Format(DateBasedIncFolderName)
			partName = path.Join(chosenSuffix, partName)
			defaultURI, urisByLocalityKV, err = getURIsByLocalityKV(to, partName)
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
	user security.SQLUsername,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	backupURIs []string,
	encryption *jobspb.BackupEncryptionOptions,
) ([]BackupManifest, error) {
	manifests := make([]BackupManifest, len(backupURIs))
	if len(backupURIs) == 0 {
		return manifests, nil
	}

	g := ctxgroup.WithContext(ctx)
	for i := range backupURIs {
		i := i
		g.GoCtx(func(ctx context.Context) error {
			// TODO(lucy): We may want to upgrade the table descs to the newer
			// foreign key representation here, in case there are backups from an
			// older cluster. Keeping the descriptors as they are works for now
			// since all we need to do is get the past backups' table/index spans,
			// but it will be safer for future code to avoid having older-style
			// descriptors around.
			uri := backupURIs[i]
			desc, err := ReadBackupManifestFromURI(
				ctx, uri, user, makeCloudStorage, encryption,
			)
			if err != nil {
				return errors.Wrapf(err, "failed to read backup from %q",
					RedactURIForErrorMessage(uri))
			}
			manifests[i] = desc
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return manifests, nil
}

// getEncryptionFromBase retrieves the encryption options of a base backup. It
// is expected that incremental backups use the same encryption options as the
// base backups.
func getEncryptionFromBase(
	ctx context.Context,
	user security.SQLUsername,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	baseBackupURI string,
	encryptionParams backupEncryptionParams,
) (*jobspb.BackupEncryptionOptions, error) {
	var encryptionOptions *jobspb.BackupEncryptionOptions
	if encryptionParams.encryptMode != noEncryption {
		exportStore, err := makeCloudStorage(ctx, baseBackupURI, user)
		if err != nil {
			return nil, err
		}
		defer exportStore.Close()
		opts, err := readEncryptionOptions(ctx, exportStore)
		if err != nil {
			return nil, err
		}

		switch encryptionParams.encryptMode {
		case passphrase:
			encryptionOptions = &jobspb.BackupEncryptionOptions{
				Mode: jobspb.EncryptionMode_Passphrase,
				Key:  storageccl.GenerateKey(encryptionParams.encryptionPassphrase, opts.Salt),
			}
		case kms:
			defaultKMSInfo, err := validateKMSURIsAgainstFullBackup(encryptionParams.kmsURIs,
				newEncryptedDataKeyMapFromProtoMap(opts.EncryptedDataKeyByKMSMasterKeyID), encryptionParams.kmsEnv)
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

// resolveBackupCollection returns the collectionURI and chosenSuffix that we
// should use for a backup that is pointing to a collection.
func resolveBackupCollection(
	ctx context.Context,
	user security.SQLUsername,
	defaultURI string,
	appendToLatest bool,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	endTime hlc.Timestamp,
	subdir string,
) (string, string, error) {
	var chosenSuffix string
	collectionURI := defaultURI
	if appendToLatest {
		collection, err := makeCloudStorage(ctx, collectionURI, user)
		if err != nil {
			return "", "", err
		}
		defer collection.Close()
		latestFile, err := collection.ReadFile(ctx, latestFileName)
		if err != nil {
			if errors.Is(err, cloud.ErrFileDoesNotExist) {
				return "", "", pgerror.Wrapf(err, pgcode.UndefinedFile, "path does not contain a completed latest backup")
			}
			return "", "", pgerror.WithCandidateCode(err, pgcode.Io)
		}
		latest, err := ioutil.ReadAll(latestFile)
		if err != nil {
			return "", "", err
		}
		if len(latest) == 0 {
			return "", "", errors.Errorf("malformed LATEST file")
		}
		chosenSuffix = string(latest)
	} else if subdir != "" {
		// User has specified a subdir via `BACKUP INTO 'subdir' IN...`.
		chosenSuffix = strings.TrimPrefix(subdir, "/")
		chosenSuffix = "/" + chosenSuffix
	} else {
		chosenSuffix = endTime.GoTime().Format(DateBasedIntoFolderName)
	}
	return collectionURI, chosenSuffix, nil
}
