// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// This file provides reference implementations of the schema accessor
// interface defined in schema_accessors.go.
//
// They are meant to be used to access stored descriptors only.
// For a higher-level implementation that also knows about
// virtual schemas, check out logical_schema_accessors.go.
//
// The following implementations are provided:
//
// - UncachedPhysicalAccessor, for uncached db accessors
//
// - CachedPhysicalAccessor, which adds an object cache
//   - plugged on top another SchemaAccessor.
//   - uses a `*TableCollection` (table.go) as cache.
//

// UncachedPhysicalAccessor implements direct access to DB descriptors,
// without any kind of caching.
type UncachedPhysicalAccessor struct{}

var _ SchemaAccessor = UncachedPhysicalAccessor{}

// GetDatabaseDesc implements the SchemaAccessor interface.
func (a UncachedPhysicalAccessor) GetDatabaseDesc(
	ctx context.Context, txn *client.Txn, name string, flags DatabaseLookupFlags,
) (desc *DatabaseDescriptor, err error) {
	if name == sqlbase.SystemDB.Name {
		// We can't return a direct reference to SystemDB, because the
		// caller expects a private object that can be modified in-place.
		sysDB := sqlbase.MakeSystemDatabaseDesc()
		return &sysDB, nil
	}

	descID, err := getDescriptorID(ctx, txn, sqlbase.NewDatabaseKey(name))
	if err != nil {
		return nil, err
	}
	if descID == sqlbase.InvalidID {
		if flags.required {
			return nil, sqlbase.NewUndefinedDatabaseError(name)
		}
		return nil, nil
	}

	desc = &sqlbase.DatabaseDescriptor{}
	if err := getDescriptorByID(ctx, txn, descID, desc); err != nil {
		return nil, err
	}

	return desc, nil
}

// IsValidSchema implements the SchemaAccessor interface.
func (a UncachedPhysicalAccessor) IsValidSchema(dbDesc *DatabaseDescriptor, scName string) bool {
	// At this point, only the public schema is recognized.
	return scName == tree.PublicSchema
}

// GetObjectNames implements the SchemaAccessor interface.
func (a UncachedPhysicalAccessor) GetObjectNames(
	ctx context.Context,
	txn *client.Txn,
	dbDesc *DatabaseDescriptor,
	scName string,
	flags DatabaseListFlags,
) (TableNames, error) {
	if ok := a.IsValidSchema(dbDesc, scName); !ok {
		if flags.required {
			tn := tree.MakeTableNameWithSchema(tree.Name(dbDesc.Name), tree.Name(scName), "")
			return nil, sqlbase.NewUnsupportedSchemaUsageError(tree.ErrString(&tn.TableNamePrefix))
		}
		return nil, nil
	}

	log.Eventf(ctx, "fetching list of objects for %q", dbDesc.Name)
	prefix := sqlbase.MakeNameMetadataKey(dbDesc.ID, "")
	sr, err := txn.Scan(ctx, prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}

	var tableNames tree.TableNames
	for _, row := range sr {
		_, tableName, err := encoding.DecodeUnsafeStringAscending(
			bytes.TrimPrefix(row.Key, prefix), nil)
		if err != nil {
			return nil, err
		}
		tn := tree.MakeTableName(tree.Name(dbDesc.Name), tree.Name(tableName))
		tn.ExplicitCatalog = flags.explicitPrefix
		tn.ExplicitSchema = flags.explicitPrefix
		tableNames = append(tableNames, tn)
	}
	return tableNames, nil
}

// GetObjectDesc implements the SchemaAccessor interface.
func (a UncachedPhysicalAccessor) GetObjectDesc(
	ctx context.Context, txn *client.Txn, name *ObjectName, flags ObjectLookupFlags,
) (ObjectDescriptor, error) {
	// At this point, only the public schema is recognized.
	if name.Schema() != tree.PublicSchema {
		if flags.required {
			return nil, sqlbase.NewUnsupportedSchemaUsageError(tree.ErrString(name))
		}
		return nil, nil
	}

	// Look up the database ID.
	dbID, err := getDatabaseID(ctx, txn, name.Catalog(), flags.required)
	if err != nil || dbID == sqlbase.InvalidID {
		// dbID can still be invalid if required is false and the database is not found.
		return nil, err
	}

	// Try to use the system name resolution bypass. This avoids a hotspot.
	// Note: we can only bypass name to ID resolution. The desc
	// lookup below must still go through KV because system descriptors
	// can be modified on a running cluster.
	descID := sqlbase.LookupSystemTableDescriptorID(dbID, name.Table())
	if descID == sqlbase.InvalidID {
		descID, err = getDescriptorID(ctx, txn, sqlbase.NewTableKey(dbID, name.Table()))
		if err != nil {
			return nil, err
		}
	}
	if descID == sqlbase.InvalidID {
		// KV name resolution failed.
		if flags.required {
			return nil, sqlbase.NewUndefinedRelationError(name)
		}
		return nil, nil
	}

	// Look up the table using the discovered database descriptor.
	desc := &sqlbase.TableDescriptor{}
	err = getDescriptorByID(ctx, txn, descID, desc)
	if err != nil {
		return nil, err
	}

	// We have a descriptor. Is it in the right state? We'll keep it if
	// it is in the ADD state.
	if err := filterTableState(desc); err == nil || err == errTableAdding {
		// Immediately after a RENAME an old name still points to the
		// descriptor during the drain phase for the name. Do not
		// return a descriptor during draining.
		if desc.Name == name.Table() {
			if flags.requireMutable {
				return sqlbase.NewMutableExistingTableDescriptor(*desc), nil
			}
			return sqlbase.NewImmutableTableDescriptor(*desc), nil
		}
	}

	return nil, nil
}

// CachedPhysicalAccessor adds a cache on top of any SchemaAccessor.
type CachedPhysicalAccessor struct {
	SchemaAccessor
	tc *TableCollection
}

var _ SchemaAccessor = &CachedPhysicalAccessor{}

// GetDatabaseDesc implements the SchemaAccessor interface.
func (a *CachedPhysicalAccessor) GetDatabaseDesc(
	ctx context.Context, txn *client.Txn, name string, flags DatabaseLookupFlags,
) (desc *DatabaseDescriptor, err error) {
	isSystemDB := name == sqlbase.SystemDB.Name
	if !(flags.avoidCached || isSystemDB || testDisableTableLeases) {
		refuseFurtherLookup, dbID, err := a.tc.getUncommittedDatabaseID(name, flags.required)
		if refuseFurtherLookup || err != nil {
			return nil, err
		}

		if dbID != sqlbase.InvalidID {
			// Some database ID was found in the list of uncommitted DB changes.
			// Use that to get the descriptor.
			desc, err := a.tc.databaseCache.getDatabaseDescByID(ctx, txn, dbID)
			if desc == nil && flags.required {
				return nil, sqlbase.NewUndefinedDatabaseError(name)
			}
			return desc, err
		}

		// The database was not known in the uncommitted list. Have the db
		// cache look it up by name for us.
		return a.tc.databaseCache.getDatabaseDesc(ctx,
			a.tc.leaseMgr.db.Txn, name, flags.required)
	}

	// We avoided the cache. Go lower.
	return a.SchemaAccessor.GetDatabaseDesc(ctx, txn, name, flags)
}

// GetObjectDesc implements the SchemaAccessor interface.
func (a *CachedPhysicalAccessor) GetObjectDesc(
	ctx context.Context, txn *client.Txn, name *ObjectName, flags ObjectLookupFlags,
) (ObjectDescriptor, error) {
	if flags.requireMutable {
		table, err := a.tc.getMutableTableDescriptor(ctx, txn, name, flags)
		if table == nil {
			// return nil interface.
			return nil, err
		}
		return table, err
	}
	table, err := a.tc.getTableVersion(ctx, txn, name, flags)
	if table == nil {
		// return nil interface.
		return nil, err
	}
	return table, err
}
