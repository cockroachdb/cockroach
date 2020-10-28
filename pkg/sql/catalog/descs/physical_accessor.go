// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs

import (
	"bytes"
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// getDatabaseDesc reads a database descriptor from the store.
func getDatabaseDesc(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	name string,
	flags tree.DatabaseLookupFlags,
) (desc catalog.DatabaseDescriptor, err error) {
	if name == systemschema.SystemDatabaseName {
		if flags.RequireMutable {
			return dbdesc.NewExistingMutable(
				*systemschema.MakeSystemDatabaseDesc().DatabaseDesc()), nil
		}
		return systemschema.MakeSystemDatabaseDesc(), nil
	}

	found, descID, err := catalogkv.LookupDatabaseID(ctx, txn, codec, name)
	if err != nil {
		return nil, err
	} else if !found {
		if flags.Required {
			return nil, sqlerrors.NewUndefinedDatabaseError(name)
		}
		return nil, nil
	}

	// NB: Take care to actually return nil here rather than a typed nil which
	// will not compare to nil when wrapped in the returned interface.
	untypedDesc, err := catalogkv.GetAnyDescriptorByID(ctx, txn, codec, descID, catalogkv.Mutability(flags.RequireMutable))
	if err != nil {
		return nil, err
	}
	db, ok := untypedDesc.(catalog.DatabaseDescriptor)
	if !ok {
		return nil, nil
	}
	if err := catalog.FilterDescriptorState(db, flags); err != nil {
		if flags.Required {
			return nil, err
		}
		return nil, nil
	}
	// Immediately after a RENAME an old name still points to the descriptor
	// during the drain phase for the name. Do not return a descriptor during
	// draining.
	if db.GetName() != name {
		if flags.Required {
			return nil, sqlerrors.NewUndefinedDatabaseError(name)
		}
		return nil, nil
	}
	return db, nil
}

// getSchema reads a schema descriptor from the store.
func getSchema(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	dbID descpb.ID,
	scName string,
	flags tree.SchemaLookupFlags,
) (bool, catalog.ResolvedSchema, error) {
	// Fast path public schema, as it is always found.
	if scName == tree.PublicSchema {
		return true, catalog.ResolvedSchema{
			ID: keys.PublicSchemaID, Kind: catalog.SchemaPublic, Name: scName,
		}, nil
	}

	// Lookup the schema ID.
	exists, schemaID, err := catalogkv.ResolveSchemaID(ctx, txn, codec, dbID, scName)
	if err != nil {
		return false, catalog.ResolvedSchema{}, err
	} else if !exists {
		if flags.Required {
			return false, catalog.ResolvedSchema{}, sqlerrors.NewUndefinedSchemaError(scName)
		}
		return false, catalog.ResolvedSchema{}, nil
	}

	// The temporary schema doesn't have a descriptor, only a namespace entry.
	// Note that just performing this string check on the schema name is safe
	// because no user defined schemas can have the prefix "pg_".
	if strings.HasPrefix(scName, sessiondata.PgTempSchemaName) {
		return true, catalog.ResolvedSchema{
			ID: schemaID, Kind: catalog.SchemaTemporary, Name: scName,
		}, nil
	}

	// Get the descriptor from disk.
	untypedDesc, err := catalogkv.GetAnyDescriptorByID(ctx, txn, codec, schemaID, catalogkv.Mutability(flags.RequireMutable))
	if err != nil {
		return false, catalog.ResolvedSchema{}, err
	}
	sc, ok := untypedDesc.(catalog.SchemaDescriptor)
	if !ok {
		return false, catalog.ResolvedSchema{}, nil
	}
	if err := catalog.FilterDescriptorState(sc, flags); err != nil {
		if flags.Required {
			return false, catalog.ResolvedSchema{}, err
		}
		return false, catalog.ResolvedSchema{}, nil
	}
	// Immediately after a RENAME an old name still points to the descriptor
	// during the drain phase for the name. Do not return a descriptor during
	// draining.
	if sc.GetName() != scName {
		if flags.Required {
			return false, catalog.ResolvedSchema{}, sqlerrors.NewUndefinedSchemaError(scName)
		}
		return false, catalog.ResolvedSchema{}, nil
	}
	return true, catalog.ResolvedSchema{
		ID:   sc.GetID(),
		Kind: catalog.SchemaUserDefined,
		Desc: sc,
		Name: scName,
	}, nil
}

// GetObjectNames returns the names of all objects in a database and schema.
// TODO (lucy): This is exported as a standalone function for now, but it should
// either go on descs.Collection or not exist. Since it calls getSchema(), it's
// not entirely straightforward to put it in another package for now.
func GetObjectNames(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	dbDesc catalog.DatabaseDescriptor,
	scName string,
	flags tree.DatabaseListFlags,
) (tree.TableNames, error) {
	ok, schema, err := getSchema(ctx, txn, codec, dbDesc.GetID(), scName, flags.CommonLookupFlags)
	if err != nil {
		return nil, err
	}
	if !ok {
		if flags.Required {
			tn := tree.MakeTableNameWithSchema(tree.Name(dbDesc.GetName()), tree.Name(scName), "")
			return nil, sqlerrors.NewUnsupportedSchemaUsageError(tree.ErrString(&tn.ObjectNamePrefix))
		}
		return nil, nil
	}

	log.Eventf(ctx, "fetching list of objects for %q", dbDesc.GetName())
	prefix := catalogkeys.NewTableKey(dbDesc.GetID(), schema.ID, "").Key(codec)
	sr, err := txn.Scan(ctx, prefix, prefix.PrefixEnd(), 0)
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

	// When constructing the list of entries under the `public` schema (and only
	// when constructing the list for the `public` schema), We scan both the
	// deprecated and new system.namespace table to get the complete list of
	// tables. Duplicate entries may be present in both the tables, so we filter
	// those out. If a duplicate entry is present, it doesn't matter which table
	// it is read from -- system.namespace entries are never modified, they are
	// only added/deleted. Entries are written to only one table, so duplicate
	// entries must have been copied over during migration. Thus, it doesn't
	// matter which table (newer/deprecated) the value is read from.
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
	if scName != tree.PublicSchema {
		return tableNames, nil
	}

	dprefix := catalogkeys.NewDeprecatedTableKey(dbDesc.GetID(), "").Key(codec)
	dsr, err := txn.Scan(ctx, dprefix, dprefix.PrefixEnd(), 0)
	if err != nil {
		return nil, err
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

// getObjectDesc reads an object descriptor from the store.
func getObjectDesc(
	ctx context.Context,
	txn *kv.Txn,
	settings *cluster.Settings,
	codec keys.SQLCodec,
	db, scName, object string,
	flags tree.ObjectLookupFlags,
) (catalog.Descriptor, error) {
	// Look up the database ID.
	dbID, err := catalogkv.GetDatabaseID(ctx, txn, codec, db, flags.Required)
	if err != nil || dbID == descpb.InvalidID {
		// dbID can still be invalid if required is false and the database is not found.
		return nil, err
	}

	ok, schema, err := getSchema(ctx, txn, codec, dbID, scName,
		tree.SchemaLookupFlags{
			Required:       flags.Required,
			AvoidCached:    flags.AvoidCached,
			IncludeDropped: flags.IncludeDropped,
			IncludeOffline: flags.IncludeOffline,
		})
	if err != nil {
		return nil, err
	}
	if !ok {
		if flags.Required {
			tn := tree.MakeTableNameWithSchema(tree.Name(db), tree.Name(scName), tree.Name(object))
			return nil, sqlerrors.NewUnsupportedSchemaUsageError(tree.ErrString(&tn))
		}
		return nil, nil
	}

	// Try to use the system name resolution bypass. This avoids a hotspot.
	// Note: we can only bypass name to ID resolution. The desc
	// lookup below must still go through KV because system descriptors
	// can be modified on a running cluster.
	descID := bootstrap.LookupSystemTableDescriptorID(ctx, settings, codec, dbID, object)
	if descID == descpb.InvalidID {
		var found bool
		found, descID, err = catalogkv.LookupObjectID(ctx, txn, codec, dbID, schema.ID, object)
		if err != nil {
			return nil, err
		}
		if !found {
			// KV name resolution failed.
			if flags.Required {
				tn := tree.MakeTableNameWithSchema(tree.Name(db), tree.Name(scName), tree.Name(object))
				return nil, sqlerrors.NewUndefinedObjectError(&tn, flags.DesiredObjectKind)
			}
			return nil, nil
		}
	}

	// Look up the object using the discovered database descriptor.
	desc, err := catalogkv.GetAnyDescriptorByID(ctx, txn, codec, descID, catalogkv.Mutability(flags.RequireMutable))
	if err != nil {
		return nil, err
	}
	// We have a descriptor, allow it to be in the PUBLIC or ADD state. Possibly
	// OFFLINE if the relevant flag is set.
	if err := catalog.FilterDescriptorState(desc, flags.CommonLookupFlags); err != nil {
		if flags.Required {
			return nil, err
		}
		return nil, nil
	}
	// Immediately after a RENAME an old name still points to the descriptor
	// during the drain phase for the name. Do not return a descriptor during
	// draining.
	if _, ok := desc.(catalog.TableDescriptor); ok {
		// This condition ensures that clusters < 20.1 access the
		// system.namespace_deprecated table when selecting from system.namespace.
		// As this table can not be renamed by users, it is okay that the subsequent
		// check fails.
		if object == systemschema.NamespaceTableName &&
			db == systemschema.SystemDatabaseName {
			return desc, nil
		}
	}
	if desc.GetName() != object {
		if flags.Required {
			tn := tree.MakeTableNameWithSchema(tree.Name(db), tree.Name(scName), tree.Name(object))
			return nil, sqlerrors.NewUndefinedObjectError(&tn, flags.DesiredObjectKind)
		}
		return nil, nil
	}
	return desc, nil
}
