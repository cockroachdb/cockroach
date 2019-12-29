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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
	ctx context.Context, txn *client.Txn, name string, flags tree.DatabaseLookupFlags,
) (desc *DatabaseDescriptor, err error) {
	if name == sqlbase.SystemDB.Name {
		// We can't return a direct reference to SystemDB, because the
		// caller expects a private object that can be modified in-place.
		sysDB := sqlbase.MakeSystemDatabaseDesc()
		return &sysDB, nil
	}

	found, descID, err := sqlbase.LookupDatabaseID(ctx, txn, name)
	if err != nil {
		return nil, err
	} else if !found {
		if flags.Required {
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
func (a UncachedPhysicalAccessor) IsValidSchema(
	ctx context.Context, txn *client.Txn, dbID sqlbase.ID, scName string,
) (bool, sqlbase.ID, error) {
	// Try to use the system name resolution bypass. Avoids a hotspot by explicitly
	// checking for public schema.
	if scName == tree.PublicSchema {
		return true, keys.PublicSchemaID, nil
	}

	sKey := sqlbase.NewSchemaKey(dbID, scName)
	schemaID, err := getDescriptorID(ctx, txn, sKey)
	if err != nil || schemaID == sqlbase.InvalidID {
		return false, sqlbase.InvalidID, err
	}

	return true, schemaID, nil
}

// GetObjectNames implements the SchemaAccessor interface.
func (a UncachedPhysicalAccessor) GetObjectNames(
	ctx context.Context,
	txn *client.Txn,
	dbDesc *DatabaseDescriptor,
	scName string,
	flags tree.DatabaseListFlags,
) (TableNames, error) {
	ok, schemaID, err := a.IsValidSchema(ctx, txn, dbDesc.ID, scName)
	if err != nil {
		return nil, err
	}
	if !ok {
		if flags.Required {
			tn := tree.MakeTableNameWithSchema(tree.Name(dbDesc.Name), tree.Name(scName), "")
			return nil, sqlbase.NewUnsupportedSchemaUsageError(tree.ErrString(&tn.TableNamePrefix))
		}
		return nil, nil
	}

	log.Eventf(ctx, "fetching list of objects for %q", dbDesc.Name)
	prefix := sqlbase.NewTableKey(dbDesc.ID, schemaID, "").Key()
	sr, err := txn.Scan(ctx, prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}
	// We scan both the deprecated and new system.namespace table to get the
	// complete list of tables. Duplicate entries may be present in both the tables,
	// so we filter those out. If a duplicate entry is present, it doesn't matter
	// which table it is read from -- system.namespace entries are never modified,
	// they are only added/deleted. Entries are written to only one table, so
	// duplicate entries must have been copied over during migration. Thus, it
	// doesn't matter which table (newer/deprecated) the value is read from.
	//
	// It may seem counter-intuitive to read both tables if we have found data in
	// the newer version. The migration copied all entries from the deprecated
	// system.namespace and all new entries after the cluster version bump are added
	// to the new system.namespace. Why do we do this then?
	// This is to account the scenario where a table was created before
	// the cluster version was bumped, but after the older system.namespace was
	// copied into the newer system.namespace. Objects created in this window
	// will only be present in the older system.namespace. To account for this
	// scenario, we must do this filtering logic.
	// TODO(solon): This complexity can be removed in  20.2.
	dprefix := sqlbase.NewDeprecatedTableKey(dbDesc.ID, "").Key()
	dsr, err := txn.Scan(ctx, dprefix, dprefix.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}

	alreadySeen := make(map[string]bool)
	var tableNames tree.TableNames

	for _, row := range sr {
		_, tableName, err := encoding.DecodeUnsafeStringAscending(bytes.TrimPrefix(
			row.Key, prefix), nil)
		if err != nil {
			return nil, err
		}
		alreadySeen[tableName] = true
		tn := tree.MakeTableNameWithSchema(tree.Name(dbDesc.Name), tree.Name(scName), tree.Name(tableName))
		tn.ExplicitCatalog = flags.ExplicitPrefix
		tn.ExplicitSchema = flags.ExplicitPrefix
		tableNames = append(tableNames, tn)
	}

	for _, row := range dsr {
		// Decode using the deprecated key prefix.
		_, tableName, err := encoding.DecodeUnsafeStringAscending(
			bytes.TrimPrefix(row.Key, dprefix), nil)
		if err != nil {
			return nil, err
		}
		if alreadySeen[tableName] {
			continue
		}
		tn := tree.MakeTableNameWithSchema(tree.Name(dbDesc.Name), tree.Name(scName), tree.Name(tableName))
		tn.ExplicitCatalog = flags.ExplicitPrefix
		tn.ExplicitSchema = flags.ExplicitPrefix
		tableNames = append(tableNames, tn)
	}
	return tableNames, nil
}

// GetObjectDesc implements the SchemaAccessor interface.
func (a UncachedPhysicalAccessor) GetObjectDesc(
	ctx context.Context,
	txn *client.Txn,
	settings *cluster.Settings,
	name *ObjectName,
	flags tree.ObjectLookupFlags,
) (ObjectDescriptor, error) {
	// Look up the database ID.
	dbID, err := getDatabaseID(ctx, txn, name.Catalog(), flags.Required)
	if err != nil || dbID == sqlbase.InvalidID {
		// dbID can still be invalid if required is false and the database is not found.
		return nil, err
	}

	ok, schemaID, err := a.IsValidSchema(ctx, txn, dbID, name.Schema())
	if err != nil {
		return nil, err
	}
	if !ok {
		if flags.Required {
			return nil, sqlbase.NewUnsupportedSchemaUsageError(tree.ErrString(name))
		}
		return nil, nil
	}

	// Try to use the system name resolution bypass. This avoids a hotspot.
	// Note: we can only bypass name to ID resolution. The desc
	// lookup below must still go through KV because system descriptors
	// can be modified on a running cluster.
	descID := sqlbase.LookupSystemTableDescriptorID(ctx, settings, dbID, name.Table())
	if descID == sqlbase.InvalidID {
		var found bool
		found, descID, err = sqlbase.LookupObjectID(ctx, txn, dbID, schemaID, name.Table())
		if err != nil {
			return nil, err
		}
		if !found {
			// KV name resolution failed.
			if flags.Required {
				return nil, sqlbase.NewUndefinedRelationError(name)
			}
			return nil, nil
		}
	}

	// Look up the table using the discovered database descriptor.
	desc := &sqlbase.TableDescriptor{}
	err = getDescriptorByID(ctx, txn, descID, desc)
	if err != nil {
		return nil, err
	}

	// We have a descriptor, allow it to be in the PUBLIC or ADD state. Possibly
	// OFFLINE if the relevant flag is set.
	acceptableStates := map[sqlbase.TableDescriptor_State]bool{
		sqlbase.TableDescriptor_ADD:     true,
		sqlbase.TableDescriptor_PUBLIC:  true,
		sqlbase.TableDescriptor_OFFLINE: flags.IncludeOffline,
	}
	if acceptableStates[desc.State] {
		// Immediately after a RENAME an old name still points to the
		// descriptor during the drain phase for the name. Do not
		// return a descriptor during draining.
		//
		// The second or condition ensures that clusters < 20.1 access the
		// system.namespace_deprecated table when selecting from system.namespace.
		// As this table can not be renamed by users, it is okay that the first
		// check fails.
		if desc.Name == name.Table() ||
			name.Table() == sqlbase.NamespaceTable.Name && name.Catalog() == sqlbase.SystemDB.Name {
			if flags.RequireMutable {
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
	ctx context.Context, txn *client.Txn, name string, flags tree.DatabaseLookupFlags,
) (desc *DatabaseDescriptor, err error) {
	isSystemDB := name == sqlbase.SystemDB.Name
	if !(flags.AvoidCached || isSystemDB || testDisableTableLeases) {
		refuseFurtherLookup, dbID, err := a.tc.getUncommittedDatabaseID(name, flags.Required)
		if refuseFurtherLookup || err != nil {
			return nil, err
		}

		if dbID != sqlbase.InvalidID {
			// Some database ID was found in the list of uncommitted DB changes.
			// Use that to get the descriptor.
			desc, err := a.tc.databaseCache.getDatabaseDescByID(ctx, txn, dbID)
			if desc == nil && flags.Required {
				return nil, sqlbase.NewUndefinedDatabaseError(name)
			}
			return desc, err
		}

		// The database was not known in the uncommitted list. Have the db
		// cache look it up by name for us.
		return a.tc.databaseCache.getDatabaseDesc(ctx, a.tc.leaseMgr.db.Txn, name, flags.Required)
	}

	// We avoided the cache. Go lower.
	return a.SchemaAccessor.GetDatabaseDesc(ctx, txn, name, flags)
}

// GetObjectDesc implements the SchemaAccessor interface.
func (a *CachedPhysicalAccessor) GetObjectDesc(
	ctx context.Context,
	txn *client.Txn,
	settings *cluster.Settings,
	name *ObjectName,
	flags tree.ObjectLookupFlags,
) (ObjectDescriptor, error) {
	// TODO(arul): Actually fix this to return the cached descriptor, by adding a
	//  schema cache to table collection. Until this is fixed, public tables with
	//  the same name as temporary tables might return the wrong data, as the wrong descriptor
	//  might be cached.
	if name.Schema() != tree.PublicSchema {
		phyAccessor := UncachedPhysicalAccessor{}
		obj, err := phyAccessor.GetObjectDesc(ctx, txn, settings, name, flags)
		if obj == nil {
			return nil, err
		}
		if flags.RequireMutable {
			return obj.(*sqlbase.MutableTableDescriptor), err
		}
		return obj.(*sqlbase.ImmutableTableDescriptor), err
	}
	if flags.RequireMutable {
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
