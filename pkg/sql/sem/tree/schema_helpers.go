// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import "slices"

// HasSetOrResetSchemaLocked checks if the statement contains a command to
// set/reset the "schema_locked" storage parameter.
func HasSetOrResetSchemaLocked(n Statement) (hasSchemaLockedChange bool) {
	alterStmt, ok := n.(*AlterTable)
	if !ok {
		return false
	}
	for _, cmd := range alterStmt.Cmds {
		switch cmd := cmd.(type) {
		case *AlterTableSetStorageParams:
			if cmd.StorageParams.GetVal("schema_locked") != nil {
				return true
			}
		case *AlterTableResetStorageParams:
			if slices.Contains(cmd.Params, "schema_locked") {
				return true
			}
		}
	}
	return false
}

// IsAllowedLDRSchemaChange returns true if the schema change statement is
// allowed to occur while the table is being referenced by a logical data
// replication job as a destination table.
func IsAllowedLDRSchemaChange(n Statement, virtualColNames []string, kvWriterEnabled bool) bool {
	switch s := n.(type) {
	case *CreateIndex:
		if s.Unique {
			// Disallow creating unique indexes until we understand how users want to
			// coordinate unique index creation on both sides of LDR.
			return false
		}
		if s.Sharded != nil {
			// Disallow creating hash-sharded indexes as it creates a virtual computed
			// column and we require the number of columns of both sides of LDR to be
			// the same.
			return false
		}
		if !kvWriterEnabled {
			// All other indexes are supported by sql writer.
			return true
		}
		// Don't allow creating an index on a virtual column for kv writer.
		for _, col := range s.Columns {
			if slices.Contains(virtualColNames, string(col.Column)) {
				return false
			}
		}
		if s.Predicate != nil {
			// Partial indexes not supported in kv writer as they require expr
			// evaluation.
			return false
		}
		return true
	case *DropIndex:
		return true
	case *AlterIndexVisible:
		return true
	case *RenameIndex:
		return true
	case *SetZoneConfig:
		return true
	case *AlterTable:
		onlySafeStorageParams := true
		for _, cmd := range s.Cmds {
			switch c := cmd.(type) {
			case *AlterTableSetVisible:
				return true
			case *AlterTableSetDefault:
				return true
			// Allow safe storage parameter changes.
			case *AlterTableSetStorageParams:
				// ttl_expire_after is not safe since it creates a new column and
				// backfills it.
				if c.StorageParams.GetVal("ttl_expire_after") != nil {
					onlySafeStorageParams = false
				}
			case *AlterTableResetStorageParams:
				if slices.Contains(c.Params, "ttl_expire_after") {
					// Resetting `ttl_expire_after` is not safe since it drops a column
					// and rebuilds the primary index.
					onlySafeStorageParams = false
				} else if slices.Contains(c.Params, "ttl") {
					// Resetting `ttl` can also result in the expiration column being
					// dropped.
					onlySafeStorageParams = false
				}
			default:
				onlySafeStorageParams = false
			}
		}
		return onlySafeStorageParams
	}
	return false
}
