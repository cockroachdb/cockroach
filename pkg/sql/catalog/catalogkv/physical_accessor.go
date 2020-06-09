// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalogkv

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// UncachedPhysicalAccessor implements direct access to sql object descriptors
// stored in system tables without any kind of caching.
type UncachedPhysicalAccessor struct {
	// Used to avoid allocations.
	tn tree.TableName
}

var _ catalog.Accessor = UncachedPhysicalAccessor{}

// GetDatabaseDesc implements the Accessor interface.
func (a UncachedPhysicalAccessor) GetDatabaseDesc(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	name string,
	flags tree.DatabaseLookupFlags,
) (desc sqlbase.DatabaseDescriptorInterface, err error) {
	if name == sqlbase.SystemDatabaseName {
		// We can't return a direct reference to SystemDB, because the
		// caller expects a private object that can be modified in-place.
		sysDB := sqlbase.MakeSystemDatabaseDesc()
		return sysDB, nil
	}

	found, descID, err := sqlbase.LookupDatabaseID(ctx, txn, codec, name)
	if err != nil {
		return nil, err
	} else if !found {
		if flags.Required {
			return nil, sqlbase.NewUndefinedDatabaseError(name)
		}
		return nil, nil
	}

	// NB: Take care to actually return nil here rather than a typed nil which
	// will not compare to nil when wrapped in the returned interface.
	desc, err = GetDatabaseDescByID(ctx, txn, codec, descID)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		return nil, nil
	}
	return desc, err
}

// IsValidSchema implements the Accessor interface.
func (a UncachedPhysicalAccessor) IsValidSchema(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, dbID sqlbase.ID, scName string,
) (bool, sqlbase.ID, error) {
	return ResolveSchemaID(ctx, txn, codec, dbID, scName)
}

// GetObjectNames implements the Accessor interface.
func (a UncachedPhysicalAccessor) GetObjectNames(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	dbDesc sqlbase.DatabaseDescriptorInterface,
	scName string,
	flags tree.DatabaseListFlags,
) (tree.TableNames, error) {
	ok, schemaID, err := a.IsValidSchema(ctx, txn, codec, dbDesc.GetID(), scName)
	if err != nil {
		return nil, err
	}
	if !ok {
		if flags.Required {
			tn := tree.MakeTableNameWithSchema(tree.Name(dbDesc.GetName()), tree.Name(scName), "")
			return nil, sqlbase.NewUnsupportedSchemaUsageError(tree.ErrString(&tn.ObjectNamePrefix))
		}
		return nil, nil
	}

	log.Eventf(ctx, "fetching list of objects for %q", dbDesc.GetName())
	prefix := sqlbase.NewTableKey(dbDesc.GetID(), schemaID, "").Key(codec)
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
	dprefix := sqlbase.NewDeprecatedTableKey(dbDesc.GetID(), "").Key(codec)
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
		tn := tree.MakeTableNameWithSchema(tree.Name(dbDesc.GetName()), tree.Name(scName), tree.Name(tableName))
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
		tn := tree.MakeTableNameWithSchema(tree.Name(dbDesc.GetName()), tree.Name(scName), tree.Name(tableName))
		tn.ExplicitCatalog = flags.ExplicitPrefix
		tn.ExplicitSchema = flags.ExplicitPrefix
		tableNames = append(tableNames, tn)
	}
	return tableNames, nil
}

// GetObjectDesc implements the Accessor interface.
func (a UncachedPhysicalAccessor) GetObjectDesc(
	ctx context.Context,
	txn *kv.Txn,
	settings *cluster.Settings,
	codec keys.SQLCodec,
	db, schema, object string,
	flags tree.ObjectLookupFlags,
) (catalog.Descriptor, error) {
	// Look up the database ID.
	dbID, err := GetDatabaseID(ctx, txn, codec, db, flags.Required)
	if err != nil || dbID == sqlbase.InvalidID {
		// dbID can still be invalid if required is false and the database is not found.
		return nil, err
	}

	ok, schemaID, err := a.IsValidSchema(ctx, txn, codec, dbID, schema)
	if err != nil {
		return nil, err
	}
	if !ok {
		if flags.Required {
			a.tn = tree.MakeTableNameWithSchema(tree.Name(db), tree.Name(schema), tree.Name(object))
			return nil, sqlbase.NewUnsupportedSchemaUsageError(tree.ErrString(&a.tn))
		}
		return nil, nil
	}

	// Try to use the system name resolution bypass. This avoids a hotspot.
	// Note: we can only bypass name to ID resolution. The desc
	// lookup below must still go through KV because system descriptors
	// can be modified on a running cluster.
	descID := sqlbase.LookupSystemTableDescriptorID(ctx, settings, codec, dbID, object)
	if descID == sqlbase.InvalidID {
		var found bool
		found, descID, err = sqlbase.LookupObjectID(ctx, txn, codec, dbID, schemaID, object)
		if err != nil {
			return nil, err
		}
		if !found {
			// KV name resolution failed.
			if flags.Required {
				a.tn = tree.MakeTableNameWithSchema(tree.Name(db), tree.Name(schema), tree.Name(object))
				return nil, sqlbase.NewUndefinedObjectError(&a.tn, flags.DesiredObjectKind)
			}
			return nil, nil
		}
	}

	// Look up the object using the discovered database descriptor.
	// TODO(ajwerner): Consider pushing mutability down to GetDescriptorByID.
	desc, err := GetDescriptorByID(ctx, txn, codec, descID)
	if err != nil {
		return nil, err
	}
	switch desc := desc.(type) {
	case *sqlbase.ImmutableTableDescriptor:
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
			if desc.Name == object ||
				object == sqlbase.NamespaceTableName && db == sqlbase.SystemDatabaseName {
				if flags.RequireMutable {
					return sqlbase.NewMutableExistingTableDescriptor(*desc.TableDesc()), nil
				}
				return desc, nil
			}
		}
		return nil, nil
	case *sqlbase.ImmutableTypeDescriptor:
		if flags.RequireMutable {
			return sqlbase.NewMutableExistingTypeDescriptor(*desc.TypeDesc()), nil
		}
		return desc, nil
	}
	return nil, nil
}
