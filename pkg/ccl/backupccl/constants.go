// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

// IncrementalsBackupSource describes the location of a set of incremental backups.
type IncrementalsBackupSource int

// Describes the location of a set of incremental backups.
const (
	None IncrementalsBackupSource = iota // Unused.
	// The old default. Incremental backups go in the same collection as the
	// full backup they're based on.
	SameAsFull
	// The user has specified a custom collection for incremental backups.
	Custom
	// The new default. Incremental backups go in the subdirectory "incrementals"
	// in same collection as the full backup they're based on.
	Incrementals
)

// The default subdirectory for incremental backups.
const (
	DefaultIncrementalsSubdir = "incrementals"
)
