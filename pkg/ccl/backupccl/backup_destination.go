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
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// fetchPreviousBackups takes a list of URIs of previous backups and returns
// their manifest as well as the encryption options of the first backup in the
// chain.
func fetchPreviousBackups(
	ctx context.Context,
	mem *mon.BoundAccount,
	user username.SQLUsername,
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
	user username.SQLUsername,
	dest jobspb.BackupDetails_Destination,
	endTime hlc.Timestamp,
	incrementalFrom []string,
	execCfg *sql.ExecutorConfig,
) (
	collectionURI string,
	plannedBackupDefaultURI string, /* the full path for the planned backup */
	/* chosenSuffix is the automatically chosen suffix within the collection path
	   if we're backing up INTO a collection. */
	chosenSuffix string,
	urisByLocalityKV map[string]string,
	prevBackupURIs []string, /* list of full paths for previous backups in the chain */
	err error,
) {
	makeCloudStorage := execCfg.DistSQLSrv.ExternalStorageFromURI

	defaultURI, _, err := getURIsByLocalityKV(dest.To, "")
	if err != nil {
		return "", "", "", nil, nil, err
	}

	chosenSuffix = dest.Subdir

	if chosenSuffix != "" {
		// The legacy backup syntax, BACKUP TO, leaves the dest.Subdir and collection parameters empty.
		collectionURI = defaultURI

		if chosenSuffix == latestFileName {
			latest, err := readLatestFile(ctx, defaultURI, makeCloudStorage, user)
			if err != nil {
				return "", "", "", nil, nil, err
			}
			chosenSuffix = latest
		}
	}

	plannedBackupDefaultURI, urisByLocalityKV, err = getURIsByLocalityKV(dest.To, chosenSuffix)
	if err != nil {
		return "", "", "", nil, nil, err
	}

	// At this point, the plannedBackupDefaultURI is the full path for the backup. For BACKUP
	// INTO, this path includes the chosenSuffix. Once this function returns, the
	// plannedBackupDefaultURI will be the full path for this backup in planning.
	if len(incrementalFrom) != 0 {
		// Legacy backup with deprecated BACKUP TO-syntax.
		prevBackupURIs = incrementalFrom
		return collectionURI, plannedBackupDefaultURI, chosenSuffix, urisByLocalityKV, prevBackupURIs, nil
	}

	defaultStore, err := makeCloudStorage(ctx, plannedBackupDefaultURI, user)
	if err != nil {
		return "", "", "", nil, nil, err
	}
	defer defaultStore.Close()
	exists, err := containsManifest(ctx, defaultStore)
	if err != nil {
		return "", "", "", nil, nil, err
	}
	if exists && !dest.Exists && chosenSuffix != "" && execCfg.Settings.Version.IsActive(ctx,
		clusterversion.Start22_1) {
		// We disallow a user from writing a full backup to a path in a collection containing an
		// existing backup iff we're 99.9% confident this backup was planned on a 22.1 node.
		return "",
			"",
			"",
			nil,
			nil,
			errors.Newf("A full backup already exists in %s. "+
				"Consider running an incremental backup to this full backup via `BACKUP INTO '%s' IN '%s'`",
				plannedBackupDefaultURI, chosenSuffix, dest.To[0])

	} else if !exists {
		if dest.Exists {
			// Implies the user passed a subdirectory in their backup command, either
			// explicitly or using LATEST; however, we could not find an existing
			// backup in that subdirectory.
			// - Pre 22.1: this was fine. we created a full backup in their specified subdirectory.
			// - 22.1: throw an error: full backups with an explicit subdirectory are deprecated.
			// User can use old behavior by switching the 'bulkio.backup.full_backup_with_subdir.
			// enabled' to true.
			// - 22.2+: the backup will fail unconditionally.
			// TODO (msbutler): throw error in 22.2
			if !featureFullBackupUserSubdir.Get(execCfg.SV()) {
				return "", "", "", nil, nil,
					errors.Errorf("A full backup cannot be written to %q, a user defined subdirectory. "+
						"To take a full backup, remove the subdirectory from the backup command "+
						"(i.e. run 'BACKUP ... INTO <collectionURI>'). "+
						"Or, to take a full backup at a specific subdirectory, "+
						"enable the deprecated syntax by switching the %q cluster setting to true; "+
						"however, note this deprecated syntax will not be available in a future release.",
						chosenSuffix, featureFullBackupUserSubdir.Key())
			}
		}
		// There's no full backup in the resolved subdirectory; therefore, we're conducting a full backup.
		return collectionURI, plannedBackupDefaultURI, chosenSuffix, urisByLocalityKV, prevBackupURIs, nil
	}

	// The defaultStore contains a full backup; consequently, we're conducting an incremental backup.
	fullyResolvedIncrementalsLocation, err := resolveIncrementalsBackupLocation(
		ctx,
		user,
		execCfg,
		dest.IncrementalStorage,
		dest.To,
		chosenSuffix)
	if err != nil {
		return "", "", "", nil, nil, err
	}

	priorsDefaultURI, _, err := getURIsByLocalityKV(fullyResolvedIncrementalsLocation, "")
	if err != nil {
		return "", "", "", nil, nil, err
	}
	incrementalStore, err := makeCloudStorage(ctx, priorsDefaultURI, user)
	if err != nil {
		return "", "", "", nil, nil, err
	}
	defer incrementalStore.Close()

	priors, err := FindPriorBackups(ctx, incrementalStore, OmitManifest)
	if err != nil {
		return "", "", "", nil, nil, errors.Wrap(err, "adjusting backup destination to append new layer to existing backup")
	}

	for _, prior := range priors {
		priorURI, err := url.Parse(priorsDefaultURI)
		if err != nil {
			return "", "", "", nil, nil, errors.Wrapf(err, "parsing default backup location %s",
				priorsDefaultURI)
		}
		priorURI.Path = JoinURLPath(priorURI.Path, prior)
		prevBackupURIs = append(prevBackupURIs, priorURI.String())
	}
	prevBackupURIs = append([]string{plannedBackupDefaultURI}, prevBackupURIs...)

	// Within the chosenSuffix dir, differentiate incremental backups with partName.
	partName := endTime.GoTime().Format(DateBasedIncFolderName)
	defaultIncrementalsURI, urisByLocalityKV, err := getURIsByLocalityKV(fullyResolvedIncrementalsLocation, partName)
	if err != nil {
		return "", "", "", nil, nil, err
	}
	return collectionURI, defaultIncrementalsURI, chosenSuffix, urisByLocalityKV, prevBackupURIs, nil
}

// getBackupManifests fetches the backup manifest from a list of backup URIs.
func getBackupManifests(
	ctx context.Context,
	mem *mon.BoundAccount,
	user username.SQLUsername,
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

func readLatestFile(
	ctx context.Context,
	collectionURI string,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	user username.SQLUsername,
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
// file. First it tries reading from the latest directory. If
// the backup is from an older version, it may not exist there yet so
// it tries reading in the base directory if the first attempt fails.
func findLatestFile(
	ctx context.Context, exportStore cloud.ExternalStorage,
) (ioctx.ReadCloserCtx, error) {
	var latestFile string
	var latestFileFound bool
	// First try reading from the metadata/latest directory. If the backup
	// is from an older version, it may not exist there yet so try reading
	// in the base directory if the first attempt fails.

	// We name files such that the most recent latest file will always
	// be at the top, so just grab the first filename.
	err := exportStore.List(ctx, latestHistoryDirectory, "", func(p string) error {
		p = strings.TrimPrefix(p, "/")
		latestFile = p
		latestFileFound = true
		// We only want the first latest file so return an error that it is
		// done listing.
		return cloud.ErrListingDone
	})
	// If the list failed because the storage used does not support listing,
	// such as http, we can try reading the non-timestamped backup latest
	// file directly. This can still fail if it is a mixed cluster and the
	// latest file was written in the base directory.
	if errors.Is(err, cloud.ErrListingUnsupported) {
		r, err := exportStore.ReadFile(ctx, latestHistoryDirectory+"/"+latestFileName)
		if err == nil {
			return r, nil
		}
	} else if err != nil && !errors.Is(err, cloud.ErrListingDone) {
		return nil, err
	}

	if latestFileFound {
		return exportStore.ReadFile(ctx, latestHistoryDirectory+"/"+latestFile)
	}

	// The latest file couldn't be found in the latest directory,
	// try the base directory instead.
	r, err := exportStore.ReadFile(ctx, latestFileName)
	if err != nil {
		return nil, errors.Wrap(err, "LATEST file could not be read in base or metadata directory")
	}
	return r, nil
}

// writeNewLatestFile writes a new LATEST file to both the base directory
// and latest-history directory, depending on cluster version.
func writeNewLatestFile(
	ctx context.Context, settings *cluster.Settings, exportStore cloud.ExternalStorage, suffix string,
) error {
	// If the cluster is still running on a mixed version, we want to write
	// to the base directory instead of the metadata/latest directory. That
	// way an old node can still find the LATEST file.
	if !settings.Version.IsActive(ctx, clusterversion.BackupDoesNotOverwriteLatestAndCheckpoint) {
		return cloud.WriteFile(ctx, exportStore, latestFileName, strings.NewReader(suffix))
	}

	// HTTP storage does not support listing and so we cannot rely on the
	// above-mentioned List method to return us the most recent latest file.
	// Instead, we disregard write once semantics and always read and write
	// a non-timestamped latest file for HTTP.
	if exportStore.Conf().Provider == roachpb.ExternalStorageProvider_http {
		return cloud.WriteFile(ctx, exportStore, latestFileName, strings.NewReader(suffix))
	}

	// We timestamp the latest files in order to enforce write once backups.
	// When the job goes to read these timestamped files, it will List
	// the latest files and pick the file whose name is lexicographically
	// sorted to the top. This will be the last latest file we write. It
	// Takes the one's complement of the timestamp so that files are sorted
	// lexicographically such that the most recent is always the top.
	return cloud.WriteFile(ctx, exportStore, newTimestampedLatestFileName(), strings.NewReader(suffix))
}

// newTimestampedLatestFileName returns a string of a new latest filename
// with a suffixed version. It returns it in the format of LATEST-<version>
// where version is a hex encoded one's complement of the timestamp.
// This means that as long as the supplied timestamp is correct, the filenames
// will adhere to a lexicographical/utf-8 ordering such that the most
// recent file is at the top.
func newTimestampedLatestFileName() string {
	var buffer []byte
	buffer = encoding.EncodeStringDescending(buffer, timeutil.Now().String())
	return fmt.Sprintf("%s/%s-%s", latestHistoryDirectory, latestFileName, hex.EncodeToString(buffer))
}
