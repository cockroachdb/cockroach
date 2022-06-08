// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupbase

// TODO(adityamaru): Move constants to relevant backupccl packages.
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

	// BackupManifestName is the file name used for serialized BackupManifest
	// protos.
	BackupManifestName = "BACKUP_MANIFEST"

	// BackupOldManifestName is an old name for the serialized BackupManifest
	// proto. It is used by 20.1 nodes and earlier.
	BackupOldManifestName = "BACKUP"

	// DefaultIncrementalsSubdir is the default name of the subdirectory to which
	// incremental backups will be written.
	DefaultIncrementalsSubdir = "incrementals"
)
