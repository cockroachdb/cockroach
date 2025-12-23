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
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/besteffort"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	ID                string
	EndTime           hlc.Timestamp
	MVCCFilter        backuppb.MVCCFilter
	RevisionStartTime hlc.Timestamp
}

// ListRestorableBackups lists all restorable backups from the backup index
// within the specified time interval (inclusive at both ends). The store should
// be rooted at the default collection URI (the one that contains the
// `metadata/` directory).
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
	ctx context.Context, store cloud.ExternalStorage, after, before time.Time,
) ([]RestorableBackup, error) {
	idxInRange, err := listIndexesWithinRange(ctx, store, after, before)
	if err != nil {
		return nil, err
	}

	var filteredIndexes []parsedIndex
	for _, index := range idxInRange {
		if len(filteredIndexes) > 0 {
			last := &filteredIndexes[len(filteredIndexes)-1]
			// Elide duplicate end times within a chain. Because the indexes are
			// sorted with ascending start times breaking ties, keeping the last one
			// ensures that we keep the non-compacted backup.
			if last.end.Equal(index.end) && last.fullEnd.Equal(index.fullEnd) {
				last.filePath = index.filePath
				continue
			}
		}
		filteredIndexes = append(filteredIndexes, index)
	}

	backups := make([]RestorableBackup, 0, len(filteredIndexes))
	for _, index := range filteredIndexes {
		reader, _, err := store.ReadFile(ctx, index.filePath, cloud.ReadOptions{})
		if err != nil {
			return nil, errors.Wrapf(err, "reading index file %s", index.filePath)
		}

		bytes, err := ioctx.ReadAll(ctx, reader)
		besteffort.Error(ctx, "cleanup-index-reader", func(ctx context.Context) error {
			return reader.Close(ctx)
		})
		if err != nil {
			return nil, errors.Wrapf(err, "reading index file %s", index.filePath)
		}

		idxMeta := backuppb.BackupIndexMetadata{}
		if err := protoutil.Unmarshal(bytes, &idxMeta); err != nil {
			return nil, errors.Wrapf(err, "unmarshalling index file %s", index.filePath)
		}

		backups = append(backups, RestorableBackup{
			ID:                encodeBackupID(index.fullEnd, index.end),
			EndTime:           idxMeta.EndTime,
			MVCCFilter:        idxMeta.MVCCFilter,
			RevisionStartTime: idxMeta.RevisionStartTime,
		})
	}
	return backups, nil
}

type parsedIndex struct {
	filePath     string // path to the index relative to the backup collection root
	fullEnd, end time.Time
}

// listIndexesWithinRange lists all index files whose end time falls within the
// specified time interval (inclusive at both ends). The store should be rooted
// at the default collection URI (the one that contains the `metadata/`
// directory). The returned index filenames are relative to the `metadata/index`
// directory and sorted in descending order by end time, with ties broken by
// ascending start time.
//
// NB: Filtering is applied to backup end times truncated to tens of
// milliseconds.
func listIndexesWithinRange(
	ctx context.Context, store cloud.ExternalStorage, after, before time.Time,
) ([]parsedIndex, error) {
	// First, find the full backup end time prefix we begin listing from. Since
	// full backup end times are stored in descending order in the index, we add
	// ten milliseconds (the maximum granularity of the timestamp encoding) to
	// ensure an inclusive start.
	maxEndTime := before.Add(10 * time.Millisecond)
	maxEndTimeSubdir, err := endTimeToIndexSubdir(maxEndTime)
	if err != nil {
		return nil, err
	}

	var idxInRange []parsedIndex
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
			if !start.IsZero() && end.Before(after) {
				return cloud.ErrListingDone
			}
			if end.After(before) || end.Before(after) {
				return nil
			}
			entry := parsedIndex{
				filePath: path.Join(backupbase.BackupIndexDirectoryPath, file),
				fullEnd:  full,
				end:      end,
			}
			// We may need to swap with the last index appended to maintain descending
			// end time order. This occurs when incremental backups are created and
			// appended to the previous chain while the full backup for a new chain
			// is still being run. Note that this swapping of the last two elements
			// only maintains a sorted order due to the way the backup index is sorted
			// and the invariant that the existence of an incremental backup in a
			// chain ensures that no backup in an older chain can have an end time
			// greater than or equal to the incremental's end time.
			if len(idxInRange) > 0 && end.After(idxInRange[len(idxInRange)-1].end) {
				tmp := idxInRange[len(idxInRange)-1]
				idxInRange[len(idxInRange)-1] = entry
				entry = tmp
			}
			idxInRange = append(idxInRange, entry)
			return nil
		},
	)
	if err != nil && !errors.Is(err, cloud.ErrListingDone) {
		return nil, err
	}

	return idxInRange, nil
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
			reader, _, err := store.ReadFile(
				ctx, path.Join(indexDir, basename), cloud.ReadOptions{},
			)
			if err != nil {
				return errors.Wrapf(err, "reading index file %s", basename)
			}
			defer reader.Close(ctx)

			bytes, err := ioctx.ReadAll(ctx, reader)
			if err != nil {
				return errors.Wrapf(err, "reading index file %s", basename)
			}

			index := backuppb.BackupIndexMetadata{}
			if err := protoutil.Unmarshal(bytes, &index); err != nil {
				return errors.Wrapf(err, "unmarshalling index file %s", basename)
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

// encodeBackupID generates a backup ID for a backup identified by its parent
// full end time and its own end time.
func encodeBackupID(fullEnd time.Time, backupEnd time.Time) string {
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
	return base64.URLEncoding.EncodeToString(buf)
}
