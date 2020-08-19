package backupccl

import (
	"context"
	"io/ioutil"
	"path"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

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
// TODO(pbardea): Split this method's responsibility into 3 parts:
//  - Update the backupDestination (defaultURI, urisByLocalityKV)
//  - Fetch prevBackups
//  - Resolve the backup encryption options
func resolveDest(
	ctx context.Context,
	p sql.PlanHookState,
	backupStmt *annotatedBackupStatement,
	defaultURI string,
	urisByLocalityKV map[string]string,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	endTime hlc.Timestamp,
	to []string,
	incrementalFrom []string,
	encryptionCtx encryptionContext,
) (
	string, /* collectionURI */
	string, /* defaultURI */
	map[string]string, /* urisByLocalityKV */
	[]BackupManifest, /* prevBackups */
	*jobspb.BackupEncryptionOptions, /* encryptionOptions */
	error,
) {
	// chosenSuffix is the automatically chosen suffix within the collection path
	// if we're backing up INTO a collection.
	var chosenSuffix string
	var collectionURI string
	var encryption *jobspb.BackupEncryptionOptions
	var prevBackups []BackupManifest
	var err error

	if backupStmt.Nested {
		collectionURI, chosenSuffix, err = resolveBackupCollection(ctx, p, defaultURI, backupStmt,
			makeCloudStorage, endTime)
		if err != nil {
			return "", "", nil, nil, nil, err
		}

		defaultURI, urisByLocalityKV, err = getURIsByLocalityKV(to, chosenSuffix)
		if err != nil {
			return "", "", nil, nil, nil, err
		}
	}

	if len(incrementalFrom) > 0 {
		baseBackup := incrementalFrom[0]
		encryption, err := getEncryptionFromBase(ctx, p, makeCloudStorage, baseBackup, encryptionCtx)
		if err != nil {
			return "", "", nil, nil, nil, err
		}
		prevBackups, err = getPrevBackupsFromIncremental(ctx, p, incrementalFrom, makeCloudStorage, encryption)
		if err != nil {
			return "", "", nil, nil, nil, err
		}
	} else {
		defaultStore, err := makeCloudStorage(ctx, defaultURI, p.User())
		if err != nil {
			return "", "", nil, nil, nil, err
		}
		defer defaultStore.Close()
		exists, err := containsManifest(ctx, defaultStore)
		if err != nil {
			return "", "", nil, nil, nil, err
		}
		if exists {
			encryption, err = getEncryptionFromBase(ctx, p, makeCloudStorage, defaultURI, encryptionCtx)
			if err != nil {
				return "", "", nil, nil, nil, err
			}

			m, err := readBackupManifestFromStore(ctx, defaultStore, encryption)
			if err != nil {
				return "", "", nil, nil, nil, errors.Wrap(err, "loading base backup manifest")
			}

			prevBackups, err = getPrevBackupsFromAutoAppend(ctx, defaultStore, m, backupStmt, encryption)
			if err != nil {
				return "", "", nil, nil, nil, err
			}

			// Pick a piece-specific suffix and update the destination path(s).
			partName := endTime.GoTime().Format(dateBasedFolderName)
			partName = path.Join(chosenSuffix, partName)
			defaultURI, urisByLocalityKV, err = getURIsByLocalityKV(to, partName)
			if err != nil {
				return "", "", nil, nil, nil, errors.Wrap(err, "adjusting backup destination to append new layer to existing backup")
			}
		} else if backupStmt.AppendToLatest {
			// If we came here because the LATEST file told us to but didn't find an
			// existing backup here we should raise an error.
			return "", "", nil, nil, nil, pgerror.Newf(pgcode.UndefinedFile, "backup not found in location recorded latest file: %q", chosenSuffix)
		}
	}
	return collectionURI, defaultURI, urisByLocalityKV, prevBackups, encryption, nil
}

// getPrevBackupsFromAutoAppend will retrieve the previous backups from this
// incremental chain if the backup destination is pointing to an auto-append
// directory.
func getPrevBackupsFromAutoAppend(
	ctx context.Context,
	defaultStore cloud.ExternalStorage,
	baseManifest BackupManifest,
	backupStmt *annotatedBackupStatement,
	encryption *jobspb.BackupEncryptionOptions,
) ([]BackupManifest, error) {
	prev, err := findPriorBackupNames(ctx, defaultStore)
	if err != nil {
		return nil, errors.Wrapf(err, "determining base for incremental backup")
	}
	prevBackups := make([]BackupManifest, len(prev)+1)
	prevBackups[0] = baseManifest

	if baseManifest.DescriptorCoverage == tree.AllDescriptors &&
		backupStmt.Coverage() != tree.AllDescriptors {
		return nil, errors.Errorf("cannot append a backup of specific tables or databases to a full-cluster backup")
	}

	g := ctxgroup.WithContext(ctx)
	for i := range prev {
		i := i
		g.GoCtx(func(ctx context.Context) error {
			inc := prev[i]
			m, err := readBackupManifest(ctx, defaultStore, inc, encryption)
			if err != nil {
				return errors.Wrapf(err, "loading prior backup part manifest %q", inc)
			}
			prevBackups[i+1] = m
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return prevBackups, nil
}

// getPrevBackupsFromIncremental retrieves the previous backup manifests from an
// explicit incremental chain.
func getPrevBackupsFromIncremental(
	ctx context.Context,
	p sql.PlanHookState,
	incrementalFrom []string,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	encryption *jobspb.BackupEncryptionOptions,
) ([]BackupManifest, error) {
	g := ctxgroup.WithContext(ctx)
	prevBackups := make([]BackupManifest, len(incrementalFrom))
	for i := range incrementalFrom {
		i := i
		g.GoCtx(func(ctx context.Context) error {
			// TODO(lucy): We may want to upgrade the table descs to the newer
			// foreign key representation here, in case there are backups from an
			// older cluster. Keeping the descriptors as they are works for now
			// since all we need to do is get the past backups' table/index spans,
			// but it will be safer for future code to avoid having older-style
			// descriptors around.
			uri := incrementalFrom[i]
			desc, err := ReadBackupManifestFromURI(
				ctx, uri, p.User(), makeCloudStorage, encryption,
			)
			if err != nil {
				return errors.Wrapf(err, "failed to read backup from %q", uri)
			}
			prevBackups[i] = desc
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return prevBackups, nil
}

// getEncryptionFromBase retrieves the encryption options of a base backup. It
// is expected that incremental backups use the same encryption options as the
// base backups.
func getEncryptionFromBase(
	ctx context.Context,
	p sql.PlanHookState,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	baseBackupURI string,
	encryptionCtx encryptionContext,
) (*jobspb.BackupEncryptionOptions, error) {
	var encryption *jobspb.BackupEncryptionOptions
	if encryptionCtx.encryptMode != noEncryption {
		exportStore, err := makeCloudStorage(ctx, baseBackupURI, p.User())
		if err != nil {
			return nil, err
		}
		defer exportStore.Close()
		opts, err := readEncryptionOptions(ctx, exportStore)
		if err != nil {
			return nil, err
		}

		switch encryptionCtx.encryptMode {
		case passphrase:
			encryption = &jobspb.BackupEncryptionOptions{
				Mode: jobspb.EncryptionMode_Passphrase,
				Key:  storageccl.GenerateKey(encryptionCtx.encryptionPassphrase, opts.Salt),
			}
		case kms:
			defaultKMSInfo, err := validateKMSURIsAgainstFullBackup(encryptionCtx.kmsURIs,
				newEncryptedDataKeyMapFromProtoMap(opts.EncryptedDataKeyByKMSMasterKeyID), encryptionCtx.kmsEnv)
			if err != nil {
				return nil, err
			}
			encryption = &jobspb.BackupEncryptionOptions{
				Mode:    jobspb.EncryptionMode_KMS,
				KMSInfo: defaultKMSInfo}
		}
	}
	return encryption, nil
}

// resolveBackupCollection returns the collectionURI and chosenSuffix that we
// should use for a backup that is pointing to a collection.
func resolveBackupCollection(
	ctx context.Context,
	p sql.PlanHookState,
	defaultURI string,
	backupStmt *annotatedBackupStatement,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	endTime hlc.Timestamp,
) (string, string, error) {
	var chosenSuffix string
	collectionURI := defaultURI
	if backupStmt.AppendToLatest {
		collection, err := makeCloudStorage(ctx, collectionURI, p.User())
		if err != nil {
			return "", "", err
		}
		defer collection.Close()
		latestFile, err := collection.ReadFile(ctx, latestFileName)
		if err != nil {
			if errors.Is(err, cloudimpl.ErrFileDoesNotExist) {
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
	} else {
		chosenSuffix = endTime.GoTime().Format(dateBasedFolderName)
	}
	return collectionURI, chosenSuffix, nil
}
