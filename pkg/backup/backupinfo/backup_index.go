// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupinfo

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/backup/backuputils"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/besteffort"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

var (
	ReadBackupIndexEnabled = settings.RegisterBoolSetting(
		settings.ApplicationLevel,
		"backup.index.read.enabled",
		"if true, the backup index will be read when reading from a backup collection",
		metamorphic.ConstantWithTestBool("backup.index.read.enabled", false),
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
	revisionStartTS hlc.Timestamp,
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

	path, err := backuputils.AbsoluteBackupPathInCollectionURI(details.CollectionURI, details.URI)
	if err != nil {
		return err
	}
	mvccFilter := backuppb.MVCCFilter_Latest
	if details.RevisionHistory {
		mvccFilter = backuppb.MVCCFilter_All
	}
	metadata := &backuppb.BackupIndexMetadata{
		StartTime:         details.StartTime,
		EndTime:           details.EndTime,
		Path:              path,
		IsCompacted:       details.Compact,
		MVCCFilter:        mvccFilter,
		RevisionStartTime: revisionStartTS,
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
// contains the `metadata/` directory).
//
// TODO (kev-cao): v25.4+ backups will always contain an index file. In other
// words, we can remove these checks in v26.2+.
func IndexExists(ctx context.Context, store cloud.ExternalStorage, subdir string) (bool, error) {
	var indexExists bool
	indexDir, err := indexSubdir(subdir)
	if err != nil {
		return false, err
	}
	if err := store.List(
		ctx,
		indexDir,
		cloud.ListOptions{Delimiter: "/"},
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
// collection URI (the one that contains the `metadata/` directory). It returns
// the basenames of the listed index files. It assumes that the subdir is
// resolved and not `LATEST`.
//
// Note: The indexes are returned in ascending end time order, with ties broken
// by ascending start time order. This matches the order that backup manifests
// are returned in.
func ListIndexes(
	ctx context.Context, store cloud.ExternalStorage, subdir string,
) ([]string, error) {
	type indexTimes struct {
		file       string
		start, end time.Time
	}
	var indexes []indexTimes
	indexDir, err := indexSubdir(subdir)
	if err != nil {
		return nil, err
	}
	if err := store.List(
		ctx,
		indexDir+"/",
		cloud.ListOptions{},
		func(file string) error {
			// We assert that if a file ends with .pb in the index, it should be a
			// parsable index file. Otherwise, we ignore it. This circumvents any temp
			// files that may be created by the external storage implementation.
			if !strings.HasSuffix(file, ".pb") {
				log.Dev.Warningf(ctx, "unexpected file %s in index directory", file)
				return nil
			}

			base := path.Base(file)
			i := indexTimes{
				file: base,
			}
			i.start, i.end, err = parseIndexBasename(base)
			if err != nil {
				return err
			}
			indexes = append(indexes, i)
			return nil
		},
	); err != nil {
		return nil, errors.Wrapf(err, "listing indexes in %s", subdir)
	}

	slices.SortFunc(indexes, func(a, b indexTimes) int {
		if a.end.Before(b.end) {
			return -1
		} else if a.end.After(b.end) {
			return 1
		}
		// End times are equal, so break tie with start time.
		if a.start.Before(b.start) {
			return -1
		} else {
			return 1
		}
	})

	return util.Map(indexes, func(i indexTimes) string {
		return i.file
	}), nil
}

// RestorableBackup represents a row in the `SHOW BACKUPS` output
type RestorableBackup struct {
	ID string
	// EndTime is the exact end time of the backup if .OpenedIndex() is true.
	// Otherwise, it will only be as precise as backup end times encoded in the
	// index filename, e.g. tens of milliseconds.
	EndTime hlc.Timestamp

	// The following fields are only set if `SHOW BACKUPS` was ran with WITH
	// REVISION START TIME.
	openedIndex       bool
	mvccFilter        backuppb.MVCCFilter
	revisionStartTime hlc.Timestamp
}

// OpenedIndex returns whether the backup index was opened to populate additional
// metadata about the backup.
func (b RestorableBackup) OpenedIndex() bool {
	return b.openedIndex
}

// RevisionStartTime returns the revision start time of the backup. You must
// call .OpenedIndex() first to ensure that the index was opened; otherwise, this
// method will panic.
func (b RestorableBackup) RevisionStartTime(
	ctx context.Context, sv *settings.Values,
) hlc.Timestamp {
	if !b.openedIndex {
		logcrash.ReportOrPanic(
			ctx, sv, "backup index was not opened; cannot retrieve revision start time",
		)
	}
	return b.revisionStartTime
}

// MVCCFilter returns the MVCC filter used for the backup. You must call
// .OpenedIndex() first to ensure that the index was opened; otherwise, this
// method will panic.
func (b RestorableBackup) MVCCFilter(ctx context.Context, sv *settings.Values) backuppb.MVCCFilter {
	if !b.openedIndex {
		logcrash.ReportOrPanic(
			ctx, sv, "backup index was not opened; cannot retrieve MVCC filter",
		)
	}
	return b.mvccFilter
}

// ListRestorableBackups lists all restorable backups from the backup index
// within the specified time interval (inclusive at both ends). The store should
// be rooted at the default collection URI (the one that contains the
// `metadata/` directory). A maxCount of 0 indicates no limit on the number
// of backups to return, otherwise, if the number of backups found exceeds
// maxCount, iteration will stop early and the boolean return value will be
// set to true. If withRevStartTime is true, the index files will be opened to
// populate revision history metadata about the backup as well as fetch the
// exact end time of the backup.
//
// NB: Duplicate end times within a chain are elided, as IDs only identify
// unique end times within a chain. For the purposes of determining which
// backup's metadata we use to populate the fields, we always pick the backup
// with the newest start time among those with the same end time. Also note that
// elision of the duplicate end times only applies within a chain; if two
// different chains happen to have backups that end at the same time, both will
// be included in the results.
//
// NB: Filtering is applied to backup end times truncated to tens of
// milliseconds. As such, it is possible that a backup with an end time slightly
// ahead of `before` may be included in the results.
func ListRestorableBackups(
	ctx context.Context,
	store cloud.ExternalStorage,
	newerThan, olderThan time.Time,
	maxCount uint,
	withRevStartTime bool,
) ([]RestorableBackup, bool, error) {
	ctx, trace := tracing.ChildSpan(ctx, "backupinfo.ListRestorableBackups")
	defer trace.Finish()

	var filteredIdxs []parsedIndex
	var exceededMax bool
	if err := listIndexesWithinRange(
		ctx, store, newerThan, olderThan,
		func(index parsedIndex) error {
			if len(filteredIdxs) > 0 {
				lastIdx := len(filteredIdxs) - 1
				// Elide duplicate end times within a chain. Because indexes are fetched
				// in descending order with ties broken by ascending start time, keeping
				// the last one ensures that we keep the non-compacted backup.
				if filteredIdxs[lastIdx].end.Equal(index.end) &&
					filteredIdxs[lastIdx].fullEnd.Equal(index.fullEnd) {
					if buildutil.CrdbTestBuild {
						// Sanity check that start times are in ascending order for indexes
						// with the same end time.
						if index.start.Before(filteredIdxs[lastIdx].start) {
							return errors.Newf(
								"expected index start times to be in ascending order: %s vs %s",
								index.start, filteredIdxs[lastIdx].start,
							)
						}
					}
					filteredIdxs[lastIdx] = index
					return nil
				}
			}
			filteredIdxs = append(filteredIdxs, index)
			if maxCount > 0 && uint(len(filteredIdxs)) > maxCount {
				exceededMax = true
				return cloud.ErrListingDone
			}
			return nil
		},
	); err != nil {
		return nil, false, err
	}
	if exceededMax {
		filteredIdxs = filteredIdxs[:maxCount]
	}

	if withRevStartTime {
		var readTrace *tracing.Span
		ctx, readTrace = tracing.ChildSpan(ctx, "backupinfo.ReadIndexFiles")
		defer readTrace.Finish()
	}

	backups, err := util.MapE(filteredIdxs, func(index parsedIndex) (RestorableBackup, error) {
		backupID, err := encodeBackupID(index.fullEnd, index.end)
		if err != nil {
			return RestorableBackup{}, err
		}
		if !withRevStartTime {
			return RestorableBackup{
				ID:      backupID,
				EndTime: hlc.Timestamp{WallTime: index.end.UnixNano()},
			}, nil
		}

		idxMeta, err := readIndexFile(ctx, store, index.filePath)
		if err != nil {
			return RestorableBackup{}, err
		}
		return RestorableBackup{
			ID:                backupID,
			EndTime:           idxMeta.EndTime,
			openedIndex:       true,
			mvccFilter:        idxMeta.MVCCFilter,
			revisionStartTime: idxMeta.RevisionStartTime,
		}, nil
	})
	if err != nil {
		return nil, false, err
	}

	return backups, exceededMax, nil
}

type parsedIndex struct {
	filePath            string // path to the index relative to the backup collection root
	fullEnd, start, end time.Time
}

// listIndexesWithinRange lists all index files whose end time falls within the
// specified time interval (inclusive at both ends). The store should be rooted
// at the default collection URI (the one that contains the `metadata/`
// directory). The indexes are passed to the callback in descending end time
// order, with ties broken by ascending start time order. To stop iteration
// early, the callback can return cloud.ErrListingDone. Any other returned error
// by the callback will be propagated back to the caller.
//
// NB: Filtering is applied to backup end times truncated to tens of
// milliseconds.
//
// NB: The returned index times are only as precise as the timestamps encoded in
// the index filenames.
func listIndexesWithinRange(
	ctx context.Context,
	store cloud.ExternalStorage,
	newerThan, olderThan time.Time,
	cb func(parsedIndex) error,
) error {
	// First, find the full backup end time prefix we begin listing from. Since
	// full backup end times are stored in descending order in the index, we add
	// the maximum granularity of the timestamp encoding to ensure an inclusive
	// start.
	maxEndTime := olderThan.Add(backupbase.BackupIndexFilenameTSGranularity)
	maxEndTimeSubdir, err := endTimeToIndexSubdir(maxEndTime)
	if err != nil {
		return err
	}

	// We don't immediately emit an index when we see it; instead, we hold onto
	// it until the next index is seen. This is because we may need to swap with
	// the next index in order to maintain descending end tinme order. This occurs
	// when incremental backups are created and appended to the previous chain
	// while the full backup for a new chain is still being run. Note that this
	// swapping of the last two seen indexes only maintains a sorted order due to
	// the way the backup index is sorted and the invariant that the existence of
	// an incremental backup in a chain ensures that no backup in an older chain
	// can have an end time greater than or equal to the incremental's end time.
	var pendingEmit parsedIndex
	err = store.List(
		ctx,
		backupbase.BackupIndexDirectoryPath+"/",
		cloud.ListOptions{AfterKey: maxEndTimeSubdir},
		func(file string) error {
			if !strings.HasSuffix(file, ".pb") {
				return nil
			}
			full, start, end, err := parseTimesFromIndexFilepath(file)
			if err != nil {
				return err
			}
			// Once we see an *incremental* backup with an end time before `after`, we
			// can stop iterating as we have found all backups within the time range.
			if !start.IsZero() && end.Before(newerThan) {
				return cloud.ErrListingDone
			}
			if end.After(olderThan) || end.Before(newerThan) {
				return nil
			}
			nextEntry := parsedIndex{
				filePath: path.Join(backupbase.BackupIndexDirectoryPath, file),
				fullEnd:  full,
				start:    start,
				end:      end,
			}
			if pendingEmit == (parsedIndex{}) {
				pendingEmit = nextEntry
				return nil
			}
			if !nextEntry.end.After(pendingEmit.end) {
				// The pending emit has an end time less than or equal to the new entry,
				// so we can guarantee that the pending emit is the next index to be
				// flushed.
				if err := cb(pendingEmit); err != nil {
					return err
				}
				pendingEmit = nextEntry
			} else {
				// This new entry does have an end time newer than the last index, so we
				// need to emit this one first and continue holding onto that previous
				// index.
				if err := cb(nextEntry); err != nil {
					return err
				}
			}
			return nil
		},
	)
	if err != nil && !errors.Is(err, cloud.ErrListingDone) {
		return err
	}

	// Loop has ended, we can flush any pending index.
	if pendingEmit != (parsedIndex{}) {
		if err := cb(pendingEmit); err != nil && !errors.Is(err, cloud.ErrListingDone) {
			return err
		}
	}
	return nil
}

// GetBackupTreeIndexMetadata concurrently retrieves the index metadata for all
// backups within the specified subdir. The store should be rooted at the
// collection URI that contains the `metadata/` directory. Indexes are returned in
// ascending end time order, with ties broken by ascending start time order.
func GetBackupTreeIndexMetadata(
	ctx context.Context, store cloud.ExternalStorage, subdir string,
) ([]backuppb.BackupIndexMetadata, error) {
	indexBasenames, err := ListIndexes(ctx, store, subdir)
	if err != nil {
		return nil, err
	}

	indexes := make([]backuppb.BackupIndexMetadata, len(indexBasenames))
	g := ctxgroup.WithContext(ctx)
	for i, basename := range indexBasenames {
		g.GoCtx(func(ctx context.Context) error {
			indexDir, err := indexSubdir(subdir)
			if err != nil {
				return err
			}
			indexFilePath := path.Join(indexDir, basename)
			index, err := readIndexFile(ctx, store, indexFilePath)
			if err != nil {
				return err
			}
			indexes[i] = index
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, errors.Wrapf(err, "getting backup index metadata")
	}

	return indexes, nil
}

// FindLatestBackup finds the index of the latest backup taken in the
// collection. This backup does not necessarily belong to the latest chain
// (incrementals from an older chain may be taken during a full backup).
// The provided store must be rooted at the default collection URI (the one that
// contains the `metadata/` directory). The ID of the discovered backup is also
// returned.
//
// NB: If a full and incremental have the same end time, the latest chain (i.e.
// the full) is chosen. If there are multiple incrementals within the same chain
// that share the same end time, the incremental with the latest start time is
// chosen. This ensures that the latest backup we pick is always the same backup
// as what would be shown by ListRestorableBackups.
func FindLatestBackup(
	ctx context.Context, store cloud.ExternalStorage,
) (backuppb.BackupIndexMetadata, string, error) {
	var latestIdxFilepath string
	var latestEnd time.Time
	var lastSeenFullEnd time.Time
	if err := store.List(
		ctx,
		backupbase.BackupIndexDirectoryPath,
		cloud.ListOptions{},
		func(file string) error {
			fullEnd, _, endTime, err := parseTimesFromIndexFilepath(file)
			if err != nil {
				return err
			}
			if endTime.Before(latestEnd) {
				// The moment we see an end time before the latest we've seen, we know
				// that we found the latest backup in the previous iteration.
				return cloud.ErrListingDone
			} else if endTime.Equal(latestEnd) && !fullEnd.Equal(lastSeenFullEnd) {
				// If the end time is equal, but the full end time has changed, we know
				// that we have found the latest end time, and we should pick the newer
				// chain.
				return cloud.ErrListingDone
			}
			// Otherwise, we have either found a new latest end time, or we have
			// found an incremental backup with the same end time as the latest and
			// we always choose the latest start time to break ties, so we update.
			latestEnd = endTime
			latestIdxFilepath = path.Join(
				backupbase.BackupIndexDirectoryPath, file,
			)
			lastSeenFullEnd = fullEnd
			return nil
		},
	); err != nil && !errors.Is(err, cloud.ErrListingDone) {
		return backuppb.BackupIndexMetadata{}, "", err
	}
	if latestIdxFilepath == "" {
		return backuppb.BackupIndexMetadata{}, "", errors.Newf("no backups found in collection")
	}

	index, err := readIndexFile(ctx, store, latestIdxFilepath)
	if err != nil {
		return backuppb.BackupIndexMetadata{}, "", err
	}
	backupID, err := encodeBackupID(lastSeenFullEnd, latestEnd)
	if err != nil {
		return backuppb.BackupIndexMetadata{}, "", err
	}
	return index, backupID, nil
}

// ResolveBackupIDtoIndex takes a backup ID and resolves it to the corresponding
// backup index. The store should be rooted at the default collection URI
// (the one that contains the `metadata/` directory).
//
// NB: If there are multiple backups with the same end time (i.e. compacted
// backups), the backup with the *latest* start time is chosen. This ensures
// that the backup we choose is the same one we encoded in ListRestorableBackups.
func ResolveBackupIDtoIndex(
	ctx context.Context, store cloud.ExternalStorage, backupID string,
) (backuppb.BackupIndexMetadata, error) {
	fullEnd, backupEnd, err := DecodeBackupID(backupID)
	if err != nil {
		return backuppb.BackupIndexMetadata{}, err
	}
	indexSubdir, err := endTimeToIndexSubdir(fullEnd)
	if err != nil {
		return backuppb.BackupIndexMetadata{}, err
	}

	var matchingIdxFile string
	if err := store.List(
		ctx,
		indexSubdir+"/",
		cloud.ListOptions{},
		func(file string) error {
			_, end, err := parseIndexBasename(file)
			if err != nil {
				return err
			}
			if end.Equal(backupEnd) {
				matchingIdxFile = path.Join(indexSubdir, file)
			} else if end.Before(backupEnd) {
				// By choosing to only stop listing after we see a backup OLDER than the
				// end time encoded in the ID, we ensure that if there are multiple
				// backups with the same end time, we pick the one with the latest start
				// time as indexes are sorted with ascending start time breaking ties.
				return cloud.ErrListingDone
			}
			return nil
		},
	); err != nil && !errors.Is(err, cloud.ErrListingDone) {
		return backuppb.BackupIndexMetadata{}, errors.Wrapf(err, "listing index subdir %s", indexSubdir)
	}
	if matchingIdxFile == "" {
		return backuppb.BackupIndexMetadata{}, errors.Newf(
			"backup with ID %s not found", backupID,
		)
	}
	index, err := readIndexFile(ctx, store, matchingIdxFile)
	if err != nil {
		return backuppb.BackupIndexMetadata{}, err
	}
	return index, nil
}

// ParseBackupFilePathFromIndexFileName parses the path to a backup given the
// basename of its index file and its subdirectory. For full backups, the
// returned path is relative to the root of the backup. For incremental
// backups, the path is relative to the incremental storage location starting
// from the subdir (e.g. 20250730/130000.00-20250730-120000.00). It expects
// that subdir is resolved and not `LATEST`.
//
// Note: While the path is stored in the index file, we can take a shortcut here
// and derive it from the filename solely because backup paths are
// millisecond-precise and so are the timestamps encoded in the filename.
func ParseBackupFilePathFromIndexFileName(subdir, basename string) (string, error) {
	start, end, err := parseIndexBasename(basename)
	if err != nil {
		return "", err
	}
	if start.IsZero() {
		return subdir, nil
	}

	return ConstructDateBasedIncrementalFolderName(start, end), nil
}

// ParseIndexFilename parses the start and end timestamps from the index
// filename.
//
// Note: The timestamps are only millisecond-precise and so do not represent the
// exact nano-specific times in the corresponding backup manifest.
func parseIndexBasename(basename string) (start time.Time, end time.Time, err error) {
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
// given backup. An incremental backup only writes an index if its parent full
// has written an index file. This ensures that if a backup chain exists in the
// index directory, then every backup in that chain has an index file, ensuring
// that the index is usable.
func shouldWriteIndex(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	store cloud.ExternalStorage,
	details jobspb.BackupDetails,
) (bool, error) {
	// While `incremental_location` has been removed in 26.2, we still need to
	// keep this check for one major version. A backup with custom incremental
	// locations could be started on a 25.4 node, then the cluster could be
	// upgraded and the job dropped. It could then be picked up by a 26.2 node. If
	// this check were removed, we'd end up writing an index for a backup with a
	// custom incremental location.
	// TODO (kev-cao): Remove this check in v26.4.
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
	indexDir, err := indexSubdir(subdir)
	if err != nil {
		return "", err
	}
	return backuputils.JoinURLPath(
		indexDir,
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

// endTimeToIndexSubdir converts an end time to the full path to its
// corresponding index subdir.
//
// Example:
// 2025-08-13 12:00:00.00 -> metadata/index/<encoded_full_end>_20250813-120000.00
func endTimeToIndexSubdir(endTime time.Time) (string, error) {
	subdir := endTime.Format(backupbase.DateBasedIntoFolderName)
	return indexSubdir(subdir)
}

// indexSubdirToEndTime extracts the end time from an index subdir.
//
// Example:
// <encoded_full_end>_20250813-120000.00 -> 2025-08-13 12:00:00.00
func indexSubdirToEndTime(indexSubdir string) (time.Time, error) {
	parts := strings.Split(indexSubdir, "_")
	if len(parts) != 2 {
		return time.Time{}, errors.Newf(
			"invalid index subdir format: %s", indexSubdir,
		)
	}
	endTime, err := time.Parse(backupbase.BackupIndexFilenameTimestampFormat, parts[1])
	if err != nil {
		return time.Time{}, errors.Wrapf(
			err, "index subdir %s could not be decoded", indexSubdir,
		)
	}
	return endTime, nil
}

// indexSubdir is a convenient helper function to get the corresponding index
// path for a given full backup subdir. The path is relative to the root of the
// collection URI and does not contain a trailing slash. It assumes that subdir
// has been resolved and is not `LATEST`.
//
// Example:
// /2025/08/13-120000.00 -> metadata/index/<encoded_full_end>_20250813-120000.00
func indexSubdir(subdir string) (string, error) {
	flattened, err := convertSubdirToIndexSubdir(subdir)
	if err != nil {
		return "", err
	}
	return path.Join(backupbase.BackupIndexDirectoryPath, flattened), nil
}

// convertSubdirToIndexSubdir flattens a full backup subdirectory to be used in
// the index. Note that this path does not contain a trailing or leading slash.
// It assumes subdir is not `LATEST` and has been resolved.
// We flatten the subdir so that when listing from the index, we can list with
// the index prefix and delimit on `/`. e.g.:
//
// metadata/index/
//
//	|_ <desc_end_time>_20250813-120000.00/
//	|  |_ <index_meta>.pb
//	|_ <desc_end_time>_20250814-120000.00/
//	|  |_ <index_meta>.pb
//	|_ <desc_end_time>_20250814-120000.00/
//		 |_ <index_meta>.pb
//
// Listing on `metadata/index/` and delimiting on `/` will return the
// subdirectories without listing the files in them.
//
// Example:
// /2025/08/13-120000.00 -> <encoded_full_end>_20250813-120000.00
func convertSubdirToIndexSubdir(subdir string) (string, error) {
	subdirTime, err := time.Parse(backupbase.DateBasedIntoFolderName, subdir)
	if err != nil {
		return "", errors.Wrapf(
			err, "invalid subdir format: %s", subdir,
		)
	}
	return fmt.Sprintf(
		"%s_%s",
		backuputils.EncodeDescendingTS(subdirTime),
		subdirTime.Format(backupbase.BackupIndexFilenameTimestampFormat),
	), nil
}

// convertIndexSubdirToSubdir converts an index subdir back to the
// original full backup subdir.
//
// Example:
// <encoded_full_end>_20250813-120000.00 -> /2025/08/13-120000.00
func convertIndexSubdirToSubdir(flattened string) (string, error) {
	parts := strings.Split(flattened, "_")
	if len(parts) != 2 {
		return "", errors.Newf(
			"invalid index subdir format: %s", flattened,
		)
	}
	descSubdirTime, err := backuputils.DecodeDescendingTS(parts[0])
	if err != nil {
		return "", errors.Wrapf(
			err, "index subdir %s could not be decoded", flattened,
		)
	}
	// Validate that the two parts of the index subdir correspond to the same time.
	subdirTime, err := time.Parse(backupbase.BackupIndexFilenameTimestampFormat, parts[1])
	if err != nil {
		return "", errors.Wrapf(
			err, "index subdir %s could not be decoded", flattened,
		)
	}
	if !descSubdirTime.Equal(subdirTime) {
		return "", errors.Newf(
			"index subdir %s has mismatched timestamps", flattened,
		)
	}
	unflattened := descSubdirTime.Format(backupbase.DateBasedIntoFolderName)
	return unflattened, nil
}

// parseTimesFromIndexFilepath extracts the full end time, start time, and end
// time from the index file path. The filepath is relative to the index
// directory.
//
// Example:
// <encoded_full_end>_<full_end>/<encoded_end>_<start>_<end>_metadata.pb ->
//
// full_end, start, end
func parseTimesFromIndexFilepath(filepath string) (fullEnd, start, end time.Time, err error) {
	parts := strings.Split(filepath, "/")
	if len(parts) != 2 {
		return time.Time{}, time.Time{}, time.Time{}, errors.Newf(
			"invalid index filepath format: %s", filepath,
		)
	}

	fullEnd, err = indexSubdirToEndTime(parts[0])
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, err
	}

	start, end, err = parseIndexBasename(path.Base(parts[1]))
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, err
	}

	return fullEnd, start, end, nil
}

// readIndexFile reads and unmarshals the backup index file at the given path.
// store should be rooted at the default collection URI (the one that contains
// the `metadata/` directory). The indexFilePath is relative to the collection
// URI.
func readIndexFile(
	ctx context.Context, store cloud.ExternalStorage, indexFilePath string,
) (backuppb.BackupIndexMetadata, error) {
	reader, _, err := store.ReadFile(ctx, indexFilePath, cloud.ReadOptions{})
	if err != nil {
		return backuppb.BackupIndexMetadata{}, errors.Wrapf(err, "reading index file %s", indexFilePath)
	}
	defer besteffort.Error(ctx, "cleanup-index-reader", func(ctx context.Context) error {
		return reader.Close(ctx)
	})

	bytes, err := ioctx.ReadAll(ctx, reader)
	if err != nil {
		return backuppb.BackupIndexMetadata{}, errors.Wrapf(err, "reading index file %s", indexFilePath)
	}

	idxMeta := backuppb.BackupIndexMetadata{}
	if err := protoutil.Unmarshal(bytes, &idxMeta); err != nil {
		return backuppb.BackupIndexMetadata{}, errors.Wrapf(err, "unmarshalling index file %s", indexFilePath)
	}
	return idxMeta, nil
}

// encodeBackupID generates a backup ID for a backup identified by its parent
// full end time and its own end time.
//
// NB: The times passed to this function are expected to be as precise as the
// timestamps encoded in the index file names, no more or less.
func encodeBackupID(fullEnd time.Time, backupEnd time.Time) (string, error) {
	if !fullEnd.Equal(fullEnd.Truncate(backupbase.BackupIndexFilenameTSGranularity)) ||
		!backupEnd.Equal(backupEnd.Truncate(backupbase.BackupIndexFilenameTSGranularity)) {
		return "", errors.AssertionFailedf(
			"end times encoded in backup ID can have a maximum granularity of %s: "+
				"received full end %s and backup end %s",
			backupbase.BackupIndexFilenameTSGranularity, fullEnd, backupEnd,
		)
	}
	var buf []byte
	buf = encoding.EncodeUint64Ascending(buf, uint64(fullEnd.UnixMilli()))
	buf = encoding.EncodeUint64Ascending(buf, uint64(backupEnd.UnixMilli()))
	// Because backups with the same chain share a full end time, we XOR the
	// backup end time with the full end time and reverse the bytes to provide
	// more easily distinguishable IDs.
	for i := range 8 {
		buf[i] = buf[i] ^ buf[i+8]
	}
	slices.Reverse(buf)
	// Many backups will end up ending with trailing zeroes since incremental
	// backups tend to share a YYYY/MM/DD with their fulls. We can truncate these
	// in the encoding and re-add them during decoding.
	buf = bytes.TrimRight(buf, "\x00")
	return base64.URLEncoding.EncodeToString(buf), nil
}

// DecodeBackupID decodes a backup ID into its corresponding full end time and
// backup end time.
//
// NB: Because the backup IDs are encodings of timestamps that are as precise as
// the timestamps encoded in the index file names, the returned times are only
// as precise as that.
func DecodeBackupID(backupID string) (fullEnd time.Time, backupEnd time.Time, err error) {
	if backupID == "" {
		return time.Time{}, time.Time{}, errors.Newf("failed decoding backup ID: cannot be empty")
	}

	decoded, err := base64.URLEncoding.DecodeString(backupID)
	if err != nil {
		return time.Time{}, time.Time{}, errors.Wrapf(err, "failed decoding backup ID %s", backupID)
	} else if len(decoded) > 16 {
		return time.Time{}, time.Time{}, errors.Newf("failed decoding backup ID %s: invalid encoding", backupID)
	}
	// Re-add trailing zeroes that may have been trimmed during encoding.
	padded := make([]byte, 16)
	copy(padded[:len(decoded)], decoded)
	slices.Reverse(padded)
	// Reverse the XOR obfuscation.
	for i := range 8 {
		padded[i] = padded[i] ^ padded[i+8]
	}

	_, fullEndMillis, err := encoding.DecodeUint64Ascending(padded)
	if err != nil {
		return time.Time{}, time.Time{}, errors.Wrapf(err, "failed decoding full end time from backup ID %s", backupID)
	}
	_, backupEndMillis, err := encoding.DecodeUint64Ascending(padded[8:])
	if err != nil {
		return time.Time{}, time.Time{}, errors.Wrapf(err, "failed decoding backup end time from backup ID %s", backupID)
	}

	return time.UnixMilli(int64(fullEndMillis)).UTC(), time.UnixMilli(int64(backupEndMillis)).UTC(), nil
}

// BackupIDToFullSubdir converts a backup ID to its corresponding full backup
// subdirectory.
func BackupIDToFullSubdir(backupID string) (string, error) {
	fullEnd, _, err := DecodeBackupID(backupID)
	if err != nil {
		return "", err
	}
	return fullEnd.Format(backupbase.DateBasedIntoFolderName), nil
}
