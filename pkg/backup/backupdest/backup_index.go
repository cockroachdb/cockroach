// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupdest

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/backup/backuputils"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// WriteBackupIndexMetadata writes an index file for the backup described by the
// job details. The provided ExternalStorage needs to be rooted at the specific
// directory that the index file should be written to.
//
// Note: This file is not encrypted, so it should not contain any sensitive
// information.
func WriteBackupIndexMetadata(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	user username.SQLUsername,
	makeExternalStorageFromURI cloud.ExternalStorageFromURIFactory,
	details jobspb.BackupDetails,
) error {
	indexStore, err := makeExternalStorageFromURI(
		ctx, details.CollectionURI, user,
	)
	if err != nil {
		return errors.Wrapf(err, "creating external storage")
	}

	if shouldWrite, err := shouldWriteIndex(
		ctx, execCfg, indexStore, details,
	); !shouldWrite {
		return err
	}

	ctx, sp := tracing.ChildSpan(ctx, "backupinfo.WriteBackupIndexMetadata")
	defer sp.Finish()

	if details.EndTime.IsEmpty() {
		return errors.AssertionFailedf("end time must be set in backup details")
	}
	if details.Destination.Exists && details.StartTime.IsEmpty() {
		return errors.AssertionFailedf("incremental backup details missing a start time")
	}

	path, err := backuputils.AbsoluteBackupPathInCollectionURI(details.CollectionURI, details.URI)
	if err != nil {
		return errors.Wrapf(err, "get relative backup path")
	}
	metadata := &backuppb.BackupIndexMetadata{
		StartTime: details.StartTime,
		EndTime:   details.EndTime,
		Path:      path,
	}
	metadataBytes, err := protoutil.Marshal(metadata)
	if err != nil {
		return errors.Wrapf(err, "marshal backup index metadata")
	}

	indexFilePath, err := getBackupIndexFilePath(
		details.Destination.Subdir,
		details.StartTime,
		details.EndTime,
	)
	if err != nil {
		return errors.Wrapf(err, "getting index file path")
	}

	return cloud.WriteFile(
		ctx, indexStore, indexFilePath, bytes.NewReader(metadataBytes),
	)
}

// IndexExists checks if for a given full backup subdirectory there exists a
// corresponding index in the backup collection. This is used to determine when
// we should use the index or the legacy path.
//
// This works under the assumption that we only ever write an index iff:
//  1. For an incremental backup, an index exists for its full backup.
//  2. The backup was taken on a v25.4+ cluster.
//
// The store should be rooted at the default collection URI (the one that
// contains the `index/` directory).
//
// TODO (kev-cao): v25.4+ backups will always contain an index file. In other
// words, we can remove these checks in v26.2+.
func IndexExists(ctx context.Context, store cloud.ExternalStorage, subdir string) (bool, error) {
	flattenedSubdir, err := flattenSubdirForIndex(subdir)
	if err != nil {
		return false, err
	}
	var indexExists bool
	indexSubdir := path.Join(backupbase.BackupIndexDirectoryPath, flattenedSubdir)
	if err := store.List(
		ctx,
		indexSubdir,
		"/",
		func(file string) error {
			indexExists = true
			// Because we delimit on `/` and the index subdir does not contain a
			// trailing slash, we should only find one file as a result of this list.
			// The error is just being returned defensively just in case.
			return errors.New("found index")
		},
	); err != nil && !indexExists {
		return false, errors.Wrapf(err, "checking index exists in %s", subdir)
	}
	return indexExists, nil
}

// ListSubdirsFromIndex lists the paths of all full backup subdirectories that
// have an entry in the index. The store should be rooted at the default
// collection URI. The subdirs are returned in chronological order.
func ListSubdirsFromIndex(ctx context.Context, store cloud.ExternalStorage) ([]string, error) {
	var subdirs []string
	if err := store.List(
		ctx,
		backupbase.BackupIndexDirectoryPath,
		"/",
		func(indexSubdir string) error {
			indexSubdir = strings.TrimSuffix(indexSubdir, "/")
			subdir, err := unflattenIndexSubdir(indexSubdir)
			if err != nil {
				return err
			}
			subdirs = append(subdirs, subdir)
			return nil
		},
	); err != nil {
		return nil, errors.Wrapf(err, "listing index subdirs")
	}
	return subdirs, nil
}

// shouldWriteIndex determines if a backup index file should be written for a
// given backup. The rule is:
//  1. An index should only be written on a v25.4+ cluster.
//  2. An incremental backup only writes an index if its parent full has written
//     an index file.
//
// This ensures that if a backup chain exists in the index directory, then every
// backup in that chain has an index file, ensuring that the index is usable.
func shouldWriteIndex(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	store cloud.ExternalStorage,
	details jobspb.BackupDetails,
) (bool, error) {
	// This version check can be removed in v26.1 when we no longer need to worry
	// about a mixed-version cluster where we have both v25.4+ nodes and pre-v25.4
	// nodes.
	if !execCfg.Settings.Version.IsActive(ctx, clusterversion.V25_4) {
		return false, nil
	}

	// As we are going to be deprecating the `incremental_location` option, we
	// will avoid writing an index for any backups that specify an `incremental`
	// location. Note that if `incremental_location` is explicitly set to the
	// default location, then we will have some backups containing an index and
	// others not. We are treating this as an unsupported state and the user
	// should not use `incremental_location` in this manner.
	if len(details.Destination.IncrementalStorage) != 0 {
		return false, nil
	}

	// Full backups can write an index as long as the cluster is on v25.4+.
	if details.StartTime.IsEmpty() {
		return true, nil
	}

	return IndexExists(ctx, store, details.Destination.Subdir)
}

// getBackupIndexFilePath returns the path to the backup index file representing
// a backup that starts and ends at the given timestamps, including
// the filename and extension. The path is relative to the collection URI.
func getBackupIndexFilePath(subdir string, startTime, endTime hlc.Timestamp) (string, error) {
	if strings.EqualFold(subdir, backupbase.LatestFileName) {
		return "", errors.AssertionFailedf("expected subdir to be resolved and not be 'LATEST'")
	}
	flattened, err := flattenSubdirForIndex(subdir)
	if err != nil {
		return "", err
	}
	return backuputils.JoinURLPath(
		backupbase.BackupIndexDirectoryPath,
		flattened,
		getBackupIndexFileName(startTime, endTime),
	), nil
}

// getBackupIndexFilename generates the filename (including the extension) for a
// backup index file that represents a backup that starts ad ends at the given
// timestamps.
func getBackupIndexFileName(startTime, endTime hlc.Timestamp) string {
	descEndTs := backuputils.EncodeDescendingTS(endTime.GoTime())
	formattedStartTime := startTime.GoTime().Format(backupbase.BackupIndexFilenameTimestampFormat)
	if startTime.IsEmpty() {
		formattedStartTime = "0" // Use a placeholder for empty start time.
	}
	formattedEndTime := endTime.GoTime().Format(backupbase.BackupIndexFilenameTimestampFormat)
	return fmt.Sprintf(
		"%s_%s_%s_metadata.pb",
		descEndTs, formattedStartTime, formattedEndTime,
	)
}

// flattenSubdirForIndex flattens a full backup subdirectory to be used in the
// index. Note that this path does not contain a trailing or leading slash.
// It assumes subdir is not `LATEST` and has been resolved.
// We flatten the subdir so that when listing from the index, we can list with
// the `index/` prefix and delimit on `/`. e.g.:
//
// index/
//
//	|_ 2025-08-13-120000.00/
//	|  |_ <index_meta>.pb
//	|_ 2025-08-14-120000.00/
//	|  |_ <index_meta>.pb
//	|_ 2025-08-14-120000.00/
//		 |_ <index_meta>.pb
//
// Listing on `index/` and delimiting on `/` will return the subdirectories
// without listing the files in them.
func flattenSubdirForIndex(subdir string) (string, error) {
	subdirTime, err := time.Parse(backupbase.DateBasedIntoFolderName, subdir)
	if err != nil {
		return "", errors.Wrapf(err, "parsing subdir %q for flattening", subdir)
	}
	return subdirTime.Format(backupbase.BackupIndexFlattenedSubdir), nil
}

// unflattenIndexSubdir is the inverse of flattenSubdirForIndex. It converts a
// flattened index subdir back to the original full backup subdir.
func unflattenIndexSubdir(flattened string) (string, error) {
	subdirTime, err := time.Parse(backupbase.BackupIndexFlattenedSubdir, flattened)
	if err != nil {
		return "", errors.Wrapf(err, "parsing flattened index subdir %q for unflattening", flattened)
	}
	unflattened := subdirTime.Format(backupbase.DateBasedIntoFolderName)
	return unflattened, nil
}
