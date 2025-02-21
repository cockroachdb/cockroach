// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import "slices"

// IsSetOrResetSchemaLocked returns true if `n` contains a command to
// set/reset "schema_locked" storage parameter.
func IsSetOrResetSchemaLocked(n Statement) bool {
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
func IsAllowedLDRSchemaChange(n Statement, virtualColNames []string) bool {
	switch s := n.(type) {
	case *CreateIndex:
		// Don't allow creating an index on a virtual column.
		for _, col := range s.Columns {
			if slices.Contains(virtualColNames, string(col.Column)) {
				return false
			}
		}
		// Disallow unique, partial, or hash-sharded indexes. Having these indexes
		// on a destination table could cause inserts to fail.
		// NB: hash-sharded indexes are disallowed since they create an index on a
		// virtual column. Since it also implicitly creates the virtual column
		// at the same time, the check above on virtualColNames would not block it.
		return !s.Unique && s.Predicate == nil && s.Sharded == nil
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
