// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupdest

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"strings"

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
	ctx, sp := tracing.ChildSpan(ctx, "backupdest.WriteBackupIndexMetadata")
	defer sp.Finish()

	if details.EndTime.IsEmpty() {
		return errors.AssertionFailedf("end time must be set in backup details")
	}
	if details.Destination.Exists && details.StartTime.IsEmpty() {
		return errors.AssertionFailedf("incremental backup details missing a start time")
	}

	var backupCollectionURI string
	// Find the root of the collection URI that the backup is being written to so
	// that we can determine the relative path of the backup.
	if details.StartTime.IsEmpty() || details.Destination.IncrementalStorage == nil {
		backupCollectionURI = details.CollectionURI
	} else {
		var err error
		backupCollectionURI, _, err = GetURIsByLocalityKV(details.Destination.IncrementalStorage, "")
		if err != nil {
			return errors.Wrapf(err, "get default incremental backup collection URI")
		}
	}

	path, err := backuputils.RelativeBackupPathInCollectionURI(backupCollectionURI, details.URI)
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

// getBackupIndexFilePath returns the path to the backup index file representing
// a backup that starts and ends at the given timestamps, including
// the filename and extension. The path is relative to the collection URI.
func getBackupIndexFilePath(subdir string, startTime, endTime hlc.Timestamp) (string, error) {
	if strings.EqualFold(subdir, backupbase.LatestFileName) {
		return "", errors.AssertionFailedf("expected subdir to be resolved and not be 'LATEST'")
	}
	// We flatten the subdir so that when listing from the index, we can list with
	// the `index/` prefix and delimit on `/`.
	flattenedSubdir := strings.ReplaceAll(
		strings.TrimPrefix(subdir, "/"),
		"/", "-",
	)
	return backuputils.JoinURLPath(
		backupbase.BackupIndexDirectoryPath,
		flattenedSubdir,
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
		"%s_%s_%s_metadata.pb",
		descEndTs, formattedStartTime, formattedEndTime,
	)
}
