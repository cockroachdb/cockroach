// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import (
	"strings"

	"github.com/cockroachdb/pebble/vfs/errorfs"
)

// StallAllOps is a predicate that matches all VFS operations.
// Use this when you want to stall all I/O.
func StallAllOps(op errorfs.Op) bool {
	return true
}

// StallWriteOps is a predicate that matches only write operations.
// This includes Create, Write, Sync, Flush, Remove, Rename, etc.
func StallWriteOps(op errorfs.Op) bool {
	return op.Kind.IsWrite()
}

// StallReadOps is a predicate that matches only read operations.
// This includes Open, Read, Stat, List, etc.
func StallReadOps(op errorfs.Op) bool {
	return op.Kind.IsRead()
}

// StallSyncOps is a predicate that matches only sync/fsync operations.
// This is useful for testing durability-related scenarios.
func StallSyncOps(op errorfs.Op) bool {
	switch op.Kind {
	case errorfs.OpFileSync, errorfs.OpFileSyncData, errorfs.OpFileSyncTo, errorfs.OpFileFlush:
		return true
	default:
		return false
	}
}

// StallFileDataOps is a predicate that matches file data operations.
// This includes reads, writes, and syncs on files (not directory operations).
func StallFileDataOps(op errorfs.Op) bool {
	switch op.Kind {
	case errorfs.OpFileRead, errorfs.OpFileReadAt,
		errorfs.OpFileWrite, errorfs.OpFileWriteAt,
		errorfs.OpFileSync, errorfs.OpFileSyncData, errorfs.OpFileSyncTo, errorfs.OpFileFlush:
		return true
	default:
		return false
	}
}

// StallWALOps is a predicate that matches operations on WAL (Write-Ahead Log) files.
// This is a heuristic based on common WAL file naming patterns.
func StallWALOps(op errorfs.Op) bool {
	// WAL files typically have specific patterns in their names.
	// Pebble uses files like "000001.log" for WAL.
	if strings.HasSuffix(op.Path, ".log") {
		return true
	}
	// Also match files in the "wal" directory.
	if strings.Contains(op.Path, "/wal/") || strings.Contains(op.Path, "\\wal\\") {
		return true
	}
	return false
}

// StallSSTOps is a predicate that matches operations on SSTable files.
// SSTable files have the ".sst" extension.
func StallSSTOps(op errorfs.Op) bool {
	return strings.HasSuffix(op.Path, ".sst")
}

// StallManifestOps is a predicate that matches operations on MANIFEST files.
// MANIFEST files track the state of the LSM tree.
func StallManifestOps(op errorfs.Op) bool {
	return strings.Contains(op.Path, "MANIFEST")
}

// CombinePredicates returns a predicate that returns true if any of the
// provided predicates return true (logical OR).
func CombinePredicates(predicates ...func(errorfs.Op) bool) func(errorfs.Op) bool {
	return func(op errorfs.Op) bool {
		for _, pred := range predicates {
			if pred(op) {
				return true
			}
		}
		return false
	}
}

// InvertPredicate returns a predicate that inverts the result of the given predicate.
func InvertPredicate(pred func(errorfs.Op) bool) func(errorfs.Op) bool {
	return func(op errorfs.Op) bool {
		return !pred(op)
	}
}

// AndPredicates returns a predicate that returns true only if all of the
// provided predicates return true (logical AND).
func AndPredicates(predicates ...func(errorfs.Op) bool) func(errorfs.Op) bool {
	return func(op errorfs.Op) bool {
		for _, pred := range predicates {
			if !pred(op) {
				return false
			}
		}
		return true
	}
}
