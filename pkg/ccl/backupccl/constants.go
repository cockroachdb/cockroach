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
	None IncrementalsBackupSource = iota
	SameAsFull
	Custom
	Incrementals
)

// The default subdirectory for incremental backups.
const (
	DefaultIncrementalsSubdir = "incrementals"
)
