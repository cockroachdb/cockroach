// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupbase

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

	// DateBasedIncFolderName is the date format used when creating sub-directories
	// storing incremental backups for auto-appendable backups.
	// It is exported for testing backup inspection tooling.
	DateBasedIncFolderName = "/20060102/150405.00"

	// DateBasedIntoFolderName is the date format used when creating sub-directories
	// for storing backups in a collection.
	// Also exported for testing backup inspection tooling.
	DateBasedIntoFolderName = "/2006/01/02-150405.00"

	// BackupOldManifestName is an old name for the serialized BackupManifest
	// proto. It is used by 20.1 nodes and earlier.
	//
	// TODO(adityamaru): Remove this in 22.2 as part of disallowing backups
	// from >1 major version in the past.
	BackupOldManifestName = "BACKUP"

	// BackupManifestName is the file name used for serialized BackupManifest
	// protos.
	//
	// TODO(adityamaru): Remove in 23.2 since at that point all nodes will be
	// writing a SlimBackupManifest instead.
	BackupManifestName = "BACKUP_MANIFEST"

	// BackupMetadataName is the file name used for serialized BackupManifest
	// protos written by 23.1 nodes and later. This manifest has the alloc heavy
	// Files repeated fields nil'ed out, and is used in conjunction with SSTs for
	// each of those elided fields.
	BackupMetadataName = "BACKUP_METADATA"

	// DefaultIncrementalsSubdir is the default name of the subdirectory to which
	// incremental backups will be written.
	DefaultIncrementalsSubdir = "incrementals"

	// ListingDelimDataSlash is used when listing to find backups/backup metadata
	// and groups all the data sst files in each backup, which start with "data/",
	// into a single result that can be skipped over quickly.
	ListingDelimDataSlash = "data/"
)
