// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupinfo

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/backup/backuputils"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
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
	user username.SQLUsername,
	makeExternalStorageFromURI cloud.ExternalStorageFromURIFactory,
	details jobspb.BackupDetails,
) error {
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
		return errors.Wrapf(err, "failed to get relative backup path")
	}
	metadata := &backuppb.BackupIndexMetadata{
		StartTime: details.StartTime,
		EndTime:   details.EndTime,
		Path:      path,
	}
	metadataBytes, err := protoutil.Marshal(metadata)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal backup index metadata")
	}

	indexStore, err := makeExternalStorageFromURI(
		ctx, details.CollectionURI, user,
	)
	if err != nil {
		return errors.Wrapf(err, "creating external storage")
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
// If an index exists for that subdirectory but no index file for a full backup
// exists, this will return false. An index is only usable if it contains index
// files for all backups in the chain.
//
// Note: v25.4+ backups will always contain an index file. In other words, we
// can remove these checks in v26.2+.
func IndexExists(
	ctx context.Context,
	user username.SQLUsername,
	makeExternalStorageFromURI cloud.ExternalStorageFromURIFactory,
	collectionURI string,
	subdir string,
) (bool, error) {
	indexStore, err := makeExternalStorageFromURI(ctx, collectionURI, user)
	if err != nil {
		return false, errors.Wrapf(err, "creating external storage")
	}

	var lastFilename string
	indexDirPathPrefix := path.Join(backupbase.BackupIndexDirectoryPath, subdir)
	if err := indexStore.List(
		ctx,
		indexDirPathPrefix,
		"",
		func(file string) error {
			lastFilename = file
			return nil
		},
	); err != nil {
		return false, errors.Wrapf(err, "listing index files in subdir %s", subdir)
	}

	if lastFilename == "" {
		return false, nil
	}

	lastFilename, err = filepath.Rel(indexDirPathPrefix, lastFilename)
	if err != nil {
		return false, errors.Wrapf(err, "unexpectedly could not get relative path from index dir")
	}

	start, _, err := ParseIndexFileName(lastFilename)
	if err != nil {
		return false, err
	}

	return start.IsEmpty(), nil
}

// ParseIndexFileName parses out the start and end timestamps from a backup
// index file.
//
// Note: Because the encoding of the filenames is of a Go time, the timestamps
// are only millisecond accurate. They will not necessarily match the exact
// times of the backup itself.
func ParseIndexFileName(filename string) (start hlc.Timestamp, end hlc.Timestamp, err error) {
	filename, found := strings.CutSuffix(filename, ".pb")
	if !found {
		return start, end, errors.AssertionFailedf(
			"expected index filename to end with .pb, got %s", filename,
		)
	}
	parts := strings.Split(filename, "_")
	if len(parts) != 5 {
		return start, end, errors.AssertionFailedf(
			"expected index filename to have 5 parts, got %d: %s", len(parts), filename,
		)
	}
	if parts[0] != "backup" {
		return start, end, errors.AssertionFailedf(
			"expected index filename to start with 'backup', got %s", parts[0],
		)
	}
	if parts[2] == "0" {
		// This is a special case for full backups, where the start time is not set.
		start = hlc.Timestamp{}
	} else {
		startTs, err := time.Parse(backupbase.BackupIndexFilenameTimestampFormat, parts[2])
		if err != nil {
			return start, end, errors.Wrapf(err, "parsing start time from index filename %s", filename)
		}
		start = hlc.Timestamp{WallTime: startTs.UnixNano()}
	}

	endTs, err := time.Parse(backupbase.BackupIndexFilenameTimestampFormat, parts[3])
	if err != nil {
		return start, end, errors.Wrapf(err, "parsing end time from index filename %s", filename)
	}
	end = hlc.Timestamp{WallTime: endTs.UnixNano()}

	if start.After(end) {
		return start, end, errors.AssertionFailedf(
			"start time %s cannot be after end time %s in index filename %s",
		)
	}

	return start, end, nil
}

// getBackupIndexFilePath returns the path to the backup index file representing
// a backup that starts and ends at the given timestamps, including
// the filename and extension. The path is relative to the collection URI.
func getBackupIndexFilePath(subdir string, startTime, endTime hlc.Timestamp) (string, error) {
	if strings.EqualFold(subdir, backupbase.LatestFileName) {
		return "", errors.AssertionFailedf("expected subdir to be resolved and not be 'LATEST'")
	}
	return backuputils.JoinURLPath(
		backupbase.BackupIndexDirectoryPath,
		subdir,
		getBackupIndexFileName(startTime, endTime),
	), nil
}

// getBackupIndexFilename generates the filename (including the extension) for a
// backup index file that represents a backup that starts ad ends at the given
// timestamps.
func getBackupIndexFileName(startTime, endTime hlc.Timestamp) string {
	var buffer []byte
	buffer = encoding.EncodeStringDescending(buffer, endTime.GoTime().String())
	descEndTs := hex.EncodeToString(buffer)
	formattedStartTime := startTime.GoTime().Format(backupbase.BackupIndexFilenameTimestampFormat)
	if startTime.IsEmpty() {
		formattedStartTime = "0" // Use a placeholder for empty start time.
	}
	formattedEndTime := endTime.GoTime().Format(backupbase.BackupIndexFilenameTimestampFormat)
	return fmt.Sprintf(
		"backup_%s_%s_%s_metadata.pb",
		descEndTs, formattedStartTime, formattedEndTime,
	)
}
