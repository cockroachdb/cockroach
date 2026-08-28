// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupbase

import "time"

// TODO(adityamaru): Move constants to relevant backup packages.
const (
	// LatestFileName is the name of a file in the collection which contains the
	// path of the most recently taken full backup in the backup collection.
	LatestFileName = "LATEST"

	// backupMetadataDirectory is the directory where metadata about a backup
	// collection is stored. In v22.1 it contains the latest directory.
	backupMetadataDirectory = "metadata"

	// LatestHistoryDirectory is the directory where all 22.1 and beyond
	// LATEST files will be stored as we no longer want to overwrite it.
	LatestHistoryDirectory = backupMetadataDirectory + "/" + "latest"

	// BackupPartitionDescriptorPrefix is the file name prefix for serialized
	// BackupPartitionDescriptor protos.
	BackupPartitionDescriptorPrefix = "BACKUP_PART"

	// DateBasedIncFolderName is the date format used when creating sub-directories
	// storing incremental backups for auto-appendable backups.
	// It is exported for testing backup inspection tooling.
	DateBasedIncFolderName = "/20060102/150405.00"

	// DateBasedIncFolderNameSuffix is the date format appended to incremental
	// backup directories to ensure uniqueness among incrementals with the same
	// end time. It is set to the start time of the backup's coverage.
	// This is used for all backups taken on or after v25.2.
	DateBasedIncFolderNameSuffix = "20060102-150405.00"

	// DateBasedIntoFolderName is the date format used when creating sub-directories
	// for storing backups in a collection.
	// Also exported for testing backup inspection tooling.
	DateBasedIntoFolderName = "/2006/01/02-150405.00"

	// DeprecatedBackupManifestName is the file name used for serialized
	// BackupManifest protos.
	//
	// TODO(msbutler): Remove 26.3 when we're guaranteed that no backup wrote
	// exclusively the backup_manifest, and not the slim manifest.
	DeprecatedBackupManifestName = "BACKUP_MANIFEST"

	// BackupMetadataName is the file name used for serialized BackupManifest
	// protos written by 23.1 nodes and later. This manifest has the alloc heavy
	// Files repeated fields nil'ed out, and is used in conjunction with SSTs for
	// each of those elided fields.
	BackupMetadataName = "BACKUP_METADATA"

	// DefaultIncrementalsSubdir is the default name of the subdirectory to which
	// incremental backups will be written.
	DefaultIncrementalsSubdir = "incrementals"

	// ListingDelimDataSlash is used when listing to find backups/backup metadata.
	// Listing groups every name sharing a prefix up to the first occurrence of
	// the delimiter into one result, so the delimiter has to appear in the names
	// we want to skip -- the data SSTs, the only files whose count grows with the
	// size of the data backed up -- and in none of the names we need to see:
	//
	//	2026/08/24-120000.00/BACKUP_MANIFEST          intact, matched
	//	2026/08/24-120000.00/BACKUP_METADATA          intact, ignored
	//	2026/08/24-120000.00/data/<id>.sst            elided to ".../d"
	//	2026/08/24-120000.00/descriptorslist.sst      elided to ".../d"
	//	metadata/index/<chain>/<backup>_metadata.pb   elided to "metad"
	//	incrementals/<full>/<inc>/BACKUP_MANIFEST     intact, ignored
	//
	// "d" satisfies that because backup subdirectories are built from digits and
	// "-./" (see backuputils.NormalizeSubdir) and the names we match are upper
	// case, so lower case letters occur only in names we are content to collapse.
	// One "<backup>/d" result then stands in for the whole of that backup's
	// data/ directory however many files it holds.
	//
	// The delimiter must stay a single character: several S3-compatible stores
	// reject longer ones (Alibaba OSS: "The length of delimiter must be 1").
	ListingDelimDataSlash = "d"

	// BackupIndexDirectoryName is the path from the root of the backup collection
	// to the directory containing the index files for the backup collection.
	BackupIndexDirectoryPath = backupMetadataDirectory + "/index/"

	// BackupIndexFilenameTimestampFormat is the format used for the human
	// readable start and end times in the index file names.
	// NB: If this is for whatever reason updated, make sure to update the
	// granularity specified by BackupIndexFilenameTimestampGranularity.
	BackupIndexFilenameTimestampFormat = "20060102-150405.00"

	// BackupIndexFilenameTimestampGranularity represents the granularity of the
	// times encoded in the backup index filenames.
	BackupIndexFilenameTSGranularity = 10 * time.Millisecond
)
