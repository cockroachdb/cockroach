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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

var (
	ReadBackupIndexEnabled = settings.RegisterBoolSetting(
		settings.ApplicationLevel,
		"backup.index.read.enabled",
		"if true, the backup index will be read when reading from a backup collection",
		false,
	)
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
	defer indexStore.Close()

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

	path, err := backuputils.RelativeBackupPathInCollectionURI(details.CollectionURI, details.URI)
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
	var indexExists bool
	if err := store.List(
		ctx,
		indexSubdir(subdir),
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

// ListIndexes lists all the index files for a backup chain rooted by the full
// backup indicated by the subdir. The store should be rooted at the default
// collection URI (the one that contains the `index/` directory). It returns
// the basenames of the listed index files. It assumes that the subdir is
// resolved and not `LATEST`.
//
// The indexes are returned in reverse chronological order, i.e. the most recent
// index is first in the list.
func ListIndexes(
	ctx context.Context, store cloud.ExternalStorage, subdir string,
) ([]string, error) {
	var indexBasenames []string
	if err := store.List(
		ctx,
		indexSubdir(subdir)+"/",
		"",
		func(file string) error {
			indexBasenames = append(indexBasenames, path.Base(file))
			return nil
		},
	); err != nil {
		return nil, errors.Wrapf(err, "listing indexes in %s", subdir)
	}
	return indexBasenames, nil
}

// GetBackupTreeIndexMetadata concurrently retrieves the index metadata for all
// backups within the specified subdir, up to the specified end time, inclusive.
// It also returns the total size of memory allocated to store the index files.
// The returned indexes are sorted in reverse chronological order, i.e. the
// latest index is first in the list. The store should be rooted at the
// collection URI that contains the `index/` directory.
//
// Note: Even if an error is returned, the total memory reserved in the monitor
// is still returned as it may be non-zero.
func GetBackupTreeIndexMetadata(
	ctx context.Context,
	mem *mon.BoundAccount,
	store cloud.ExternalStorage,
	subdir string,
	endTime hlc.Timestamp,
) ([]backuppb.BackupIndexMetadata, int64, error) {
	indexBasenames, err := ListIndexes(ctx, store, subdir)
	if err != nil {
		return nil, 0, err
	}
	// Note that the following logic depends on the fact that ListIndexes
	// returns the index files in reverse chronological order.
	startIdx := 0
	if !endTime.IsEmpty() {
		// We will still need to open each of the index files to ensure that the
		// backup end times (which are nano-second specific) are within the
		// specified endTime, but we can do some preliminary filtering first based
		// on the filename.
		for startIdx = len(indexBasenames) - 1; startIdx >= 0; startIdx-- {
			_, indexEnd, err := parseIndexFilename(indexBasenames[startIdx])
			if err != nil {
				return nil, 0, err
			}
			// GoTime always truncates an hlc.Timestamp, so to ensure we don't
			// accidentally leave out an index, we wait until the truncated index end
			// time exceeds the truncated hlc.Timestamp end time.
			if indexEnd.After(endTime.GoTime()) {
				break
			}
		}
		indexBasenames = indexBasenames[max(startIdx-1, 0):]
	}

	indexes := make([]backuppb.BackupIndexMetadata, len(indexBasenames))
	memMu := struct {
		syncutil.Mutex
		total int64
		mem   *mon.BoundAccount
	}{
		mem: mem,
	}
	g := ctxgroup.WithContext(ctx)
	for i, basename := range indexBasenames {
		g.GoCtx(func(ctx context.Context) error {
			reader, size, err := store.ReadFile(
				ctx, path.Join(indexSubdir(subdir), basename), cloud.ReadOptions{},
			)
			if err != nil {
				return errors.Wrapf(err, "reading index file %s", basename)
			}
			defer reader.Close(ctx)

			if err := func() error {
				memMu.Lock()
				defer memMu.Unlock()
				if err := memMu.mem.Grow(ctx, size); err != nil {
					return errors.Wrapf(err, "growing memory account for index file %s", basename)
				}
				memMu.total += size
				return nil
			}(); err != nil {
				return err
			}

			bytes := make([]byte, size)
			if _, err := reader.Read(ctx, bytes); err != nil {
				return errors.Wrapf(err, "reading index file %s bytes", basename)
			}

			index := backuppb.BackupIndexMetadata{}
			if err := protoutil.Unmarshal(bytes, &index); err != nil {
				return errors.Wrapf(err, "unmarshalling index file %s", basename)
			}

			if !endTime.IsEmpty() && index.EndTime.After(endTime) {
				return nil
			}
			indexes[i] = index
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		memMu.Lock()
		defer memMu.Unlock()
		return nil, memMu.total, errors.Wrapf(err, "getting backup index metadata")
	}

	if endTime.IsEmpty() {
		return indexes, memMu.total, nil
	}

	for startIdx = 0; startIdx < len(indexes); startIdx++ {
		if !indexes[startIdx].EndTime.After(endTime) {
			break
		}
	}

	return indexes[startIdx:], memMu.total, nil
}

// ParseIndexFilename parses the start and end timestamps from the index
// filename.
//
// Note: The timestamps are only millisecond-precise and so do not represent the
// exact nano-specific times in the corresponding backup manifest.
func parseIndexFilename(basename string) (start time.Time, end time.Time, err error) {
	invalidFmtErr := errors.Newf("invalid index filename format: %s", basename)

	if !strings.HasSuffix(basename, "_metadata.pb") {
		return time.Time{}, time.Time{}, invalidFmtErr
	}
	parts := strings.Split(basename, "_")
	if len(parts) != 4 {
		return time.Time{}, time.Time{}, invalidFmtErr
	}

	if parts[1] != "0" {
		start, err = time.Parse(backupbase.BackupIndexFilenameTimestampFormat, parts[1])
		if err != nil {
			return time.Time{}, time.Time{}, errors.Join(invalidFmtErr, err)
		}
	}
	end, err = time.Parse(backupbase.BackupIndexFilenameTimestampFormat, parts[2])
	if err != nil {
		return time.Time{}, time.Time{}, errors.Join(invalidFmtErr, err)
	}

	return start, end, nil
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
	return backuputils.JoinURLPath(
		indexSubdir(subdir),
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

// indexSubdir is a convenient helper function to get the corresponding index
// path for a given full backup subdir. The path is relative to the root of the
// collection URI and does not contain a trailing slash. It assumes that subdir
// has been resolved and is not `LATEST`.
func indexSubdir(subdir string) string {
	return path.Join(backupbase.BackupIndexDirectoryPath, flattenSubdirForIndex(subdir))
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
func flattenSubdirForIndex(subdir string) string {
	return strings.ReplaceAll(
		// Trimming any trailing and leading slashes guarantees a specific format when
		// returning the flattened subdir, so callers can expect a consistent result.
		strings.TrimSuffix(strings.TrimPrefix(subdir, "/"), "/"),
		"/", "-",
	)
}
