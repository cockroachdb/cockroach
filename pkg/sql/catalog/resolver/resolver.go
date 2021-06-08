// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package resolver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// SchemaResolver abstracts the interfaces needed from the logical
// planner to perform name resolution below.
//
// We use an interface instead of passing *planner directly to make
// the resolution methods able to work even when we evolve the code to
// use a different plan builder.
// TODO(rytaft,andyk): study and reuse this.
type SchemaResolver interface {
	ObjectNameExistingResolver
	ObjectNameTargetResolver
	tree.QualifiedNameResolver

	// Accessor is a crufty name and interface that wraps the *descs.Collection.
	Accessor() catalog.Accessor

	Txn() *kv.Txn
	CurrentDatabase() string
	CurrentSearchPath() sessiondata.SearchPath
	CommonLookupFlags(required bool) tree.CommonLookupFlags
	ObjectLookupFlags(required bool, requireMutable bool) tree.ObjectLookupFlags
	LookupTableByID(ctx context.Context, id descpb.ID) (catalog.TableDescriptor, error)

	tree.TypeReferenceResolver
}

// ObjectNameExistingResolver is the helper interface to resolve table
// names when the object is expected to exist already. The boolean passed
// is used to specify if a MutableTableDescriptor is to be returned in the
// result.
type ObjectNameExistingResolver interface {
	LookupObject(ctx context.Context, flags tree.ObjectLookupFlags, dbName, scName, obName string) (
		found bool, objMeta catalog.Descriptor, err error,
	)
}

// ObjectNameTargetResolver is the helper interface to resolve object
// names when the object is not expected to exist. The planner implements
// LookupSchema to return an object consisting of the parent database and
// resolved target schema.
type ObjectNameTargetResolver interface {
	LookupSchema(
		ctx context.Context, dbName, scName string,
	) (found bool, scMeta catalog.ResolvedObjectPrefix, err error)
}

// ErrNoPrimaryKey is returned when resolving a table object and the
// AllowWithoutPrimaryKey flag is not set.
var ErrNoPrimaryKey = pgerror.Newf(pgcode.NoPrimaryKey,
	"requested table does not have a primary key")

// GetObjectNamesAndIDs retrieves the names and IDs of all objects in the
// target database/schema. If explicitPrefix is set, the returned
// table names will have an explicit schema and catalog name.
func GetObjectNamesAndIDs(
	ctx context.Context,
	txn *kv.Txn,
	sc SchemaResolver,
	codec keys.SQLCodec,
	dbDesc catalog.DatabaseDescriptor,
	scName string,
	explicitPrefix bool,
) (tree.TableNames, descpb.IDs, error) {
	return sc.Accessor().GetObjectNamesAndIDs(ctx, txn, dbDesc, scName, tree.DatabaseListFlags{
		CommonLookupFlags: sc.CommonLookupFlags(true /* required */),
		ExplicitPrefix:    explicitPrefix,
	})
}

// ResolveExistingTableObject looks up an existing object.
// If required is true, an error is returned if the object does not exist.
// Optionally, if a desired descriptor type is specified, that type is checked.
//
// The object name is modified in-place with the result of the name
// resolution, if successful. It is not modified in case of error or
// if no object is found.
func ResolveExistingTableObject(
	ctx context.Context, sc SchemaResolver, tn *tree.TableName, lookupFlags tree.ObjectLookupFlags,
) (res catalog.TableDescriptor, err error) {
	// TODO: As part of work for #34240, an UnresolvedObjectName should be
	//  passed as an argument to this function.
	un := tn.ToUnresolvedObjectName()
	desc, prefix, err := ResolveExistingObject(ctx, sc, un, lookupFlags)
	if err != nil || desc == nil {
		return nil, err
	}
	tn.ObjectNamePrefix = prefix
	return desc.(catalog.TableDescriptor), nil
}

// ResolveMutableExistingTableObject looks up an existing mutable object.
// If required is true, an error is returned if the object does not exist.
// Optionally, if a desired descriptor type is specified, that type is checked.
//
// The object name is modified in-place with the result of the name
// resolution, if successful. It is not modified in case of error or
// if no object is found.
func ResolveMutableExistingTableObject(
	ctx context.Context,
	sc SchemaResolver,
	tn *tree.TableName,
	required bool,
	requiredType tree.RequiredTableKind,
) (res *tabledesc.Mutable, err error) {
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags:    tree.CommonLookupFlags{Required: required, RequireMutable: true},
		DesiredObjectKind:    tree.TableObject,
		DesiredTableDescKind: requiredType,
	}
	// TODO: As part of work for #34240, an UnresolvedObjectName should be
	// passed as an argument to this function.
	un := tn.ToUnresolvedObjectName()
	desc, prefix, err := ResolveExistingObject(ctx, sc, un, lookupFlags)
	if err != nil || desc == nil {
		return nil, err
	}
	tn.ObjectNamePrefix = prefix
	return desc.(*tabledesc.Mutable), nil
}

// ResolveMutableType resolves a type descriptor for mutable access. It
// returns the resolved descriptor, as well as the fully qualified resolved
// object name.
func ResolveMutableType(
	ctx context.Context, sc SchemaResolver, un *tree.UnresolvedObjectName, required bool,
) (*tree.TypeName, *typedesc.Mutable, error) {
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{Required: required, RequireMutable: true},
		DesiredObjectKind: tree.TypeObject,
	}
	desc, prefix, err := ResolveExistingObject(ctx, sc, un, lookupFlags)
	if err != nil || desc == nil {
		return nil, nil, err
	}
	tn := tree.MakeNewQualifiedTypeName(prefix.Catalog(), prefix.Schema(), un.Object())
	return &tn, desc.(*typedesc.Mutable), nil
}

// ResolveExistingObject resolves an object with the given flags.
func ResolveExistingObject(
	ctx context.Context,
	sc SchemaResolver,
	un *tree.UnresolvedObjectName,
	lookupFlags tree.ObjectLookupFlags,
) (res catalog.Descriptor, prefix tree.ObjectNamePrefix, err error) {
	found, prefix, obj, err := ResolveExisting(ctx, un, sc, lookupFlags, sc.CurrentDatabase(), sc.CurrentSearchPath())
	if err != nil {
		return nil, prefix, err
	}
	// Construct the resolved table name for use in error messages.
	resolvedTn := tree.MakeTableNameFromPrefix(prefix, tree.Name(un.Object()))
	if !found {
		if lookupFlags.Required {
			return nil, prefix, sqlerrors.NewUndefinedObjectError(&resolvedTn, lookupFlags.DesiredObjectKind)
		}
		return nil, prefix, nil
	}

	switch lookupFlags.DesiredObjectKind {
	case tree.TypeObject:
		typ, isType := obj.(catalog.TypeDescriptor)
		if !isType {
			return nil, prefix, sqlerrors.NewUndefinedTypeError(&resolvedTn)
		}
		if lookupFlags.RequireMutable {
			return obj.(*typedesc.Mutable), prefix, nil
		}
		return typ, prefix, nil
	case tree.TableObject:
		table, ok := obj.(catalog.TableDescriptor)
		if !ok {
			return nil, prefix, sqlerrors.NewUndefinedRelationError(&resolvedTn)
		}
		goodType := true
		switch lookupFlags.DesiredTableDescKind {
		case tree.ResolveRequireTableDesc:
			goodType = table.IsTable()
		case tree.ResolveRequireViewDesc:
			goodType = table.IsView()
		case tree.ResolveRequireTableOrViewDesc:
			goodType = table.IsTable() || table.IsView()
		case tree.ResolveRequireSequenceDesc:
			goodType = table.IsSequence()
		}
		if !goodType {
			return nil, prefix, sqlerrors.NewWrongObjectTypeError(&resolvedTn, lookupFlags.DesiredTableDescKind.String())
		}

		// If the table does not have a primary key, return an error
		// that the requested descriptor is invalid for use.
		if !lookupFlags.AllowWithoutPrimaryKey &&
			table.IsTable() &&
			!table.HasPrimaryKey() {
			return nil, prefix, ErrNoPrimaryKey
		}

		return obj.(catalog.TableDescriptor), prefix, nil
	default:
		return nil, prefix, errors.AssertionFailedf(
			"unknown desired object kind %d", lookupFlags.DesiredObjectKind)
	}
}

// ResolveTargetObject determines a valid target path for an object
// that may not exist yet. It returns the descriptor for the database
// where the target object lives. It also returns the resolved name
// prefix for the input object.
func ResolveTargetObject(
	ctx context.Context, sc SchemaResolver, un *tree.UnresolvedObjectName,
) (catalog.ResolvedObjectPrefix, tree.ObjectNamePrefix, error) {
	found, prefix, scInfo, err := ResolveTarget(ctx, un, sc, sc.CurrentDatabase(), sc.CurrentSearchPath())
	if err != nil {
		return catalog.ResolvedObjectPrefix{}, prefix, err
	}
	if !found {
		if !un.HasExplicitSchema() && !un.HasExplicitCatalog() {
			return catalog.ResolvedObjectPrefix{}, prefix,
				pgerror.New(pgcode.InvalidName, "no database specified")
		}
		err = pgerror.Newf(pgcode.InvalidSchemaName,
			"cannot create %q because the target database or schema does not exist",
			tree.ErrString(un))
		err = errors.WithHint(err, "verify that the current database and search_path are valid and/or the target database exists")
		return catalog.ResolvedObjectPrefix{}, prefix, err
	}
	if scInfo.Schema.SchemaKind() == catalog.SchemaVirtual {
		return catalog.ResolvedObjectPrefix{}, prefix, pgerror.Newf(pgcode.InsufficientPrivilege,
			"schema cannot be modified: %q", tree.ErrString(&prefix))
	}
	return scInfo, prefix, nil
}

// ResolveSchemaNameByID resolves a schema's name based on db and schema id.
// Instead, we have to rely on a scan of the kv table.
// TODO (SQLSchema): The remaining uses of this should be plumbed through
//  the desc.Collection's ResolveSchemaByID.
func ResolveSchemaNameByID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, dbID descpb.ID, schemaID descpb.ID,
) (string, error) {
	// Fast-path for public schema and virtual schemas, to avoid hot lookups.
	if schemaName, ok := catconstants.StaticSchemaIDMap[uint32(schemaID)]; ok {
		return schemaName, nil
	}
	schemas, err := GetForDatabase(ctx, txn, codec, dbID)
	if err != nil {
		return "", err
	}
	if schema, ok := schemas[schemaID]; ok {
		return schema, nil
	}
	return "", errors.Newf("unable to resolve schema id %d for db %d", schemaID, dbID)
}

// GetForDatabase looks up and returns all available
// schema ids to names for a given database.
func GetForDatabase(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, dbID descpb.ID,
) (map[descpb.ID]string, error) {
	log.Eventf(ctx, "fetching all schema descriptor IDs for %d", dbID)

	nameKey := catalogkeys.MakeSchemaNameKey(codec, dbID, "" /* name */)
	kvs, err := txn.Scan(ctx, nameKey, nameKey.PrefixEnd(), 0 /* maxRows */)
	if err != nil {
		return nil, err
	}

	// Always add public schema ID.
	// TODO(solon): This can be removed in 20.2, when this is always written.
	// In 20.1, in a migrating state, it may be not included yet.
	ret := make(map[descpb.ID]string, len(kvs)+1)
	ret[descpb.ID(keys.PublicSchemaID)] = tree.PublicSchema

	for _, kv := range kvs {
		id := descpb.ID(kv.ValueInt())
		if _, ok := ret[id]; ok {
			continue
		}
		k, err := catalogkeys.DecodeNameMetadataKey(codec, kv.Key)
		if err != nil {
			return nil, err
		}
		ret[id] = k.GetName()
	}
	return ret, nil
}

// ResolveExisting performs name resolution for an object name when
// the target object is expected to exist already. It does not
// mutate the input name. It additionally returns the resolved
// prefix qualification for the object. For example, if the unresolved
// name was "a.b" and the name was resolved to "a.public.b", the
// prefix "a.public" is returned.
func ResolveExisting(
	ctx context.Context,
	u *tree.UnresolvedObjectName,
	r ObjectNameExistingResolver,
	lookupFlags tree.ObjectLookupFlags,
	curDb string,
	searchPath sessiondata.SearchPath,
) (bool, tree.ObjectNamePrefix, catalog.Descriptor, error) {
	namePrefix := tree.ObjectNamePrefix{
		SchemaName:      tree.Name(u.Schema()),
		ExplicitSchema:  u.HasExplicitSchema(),
		CatalogName:     tree.Name(u.Catalog()),
		ExplicitCatalog: u.HasExplicitCatalog(),
	}
	if u.HasExplicitSchema() {
		// pg_temp can be used as an alias for the current sessions temporary schema.
		// We must perform this resolution before looking up the object. This
		// resolution only succeeds if the session already has a temporary schema.
		scName, err := searchPath.MaybeResolveTemporarySchema(u.Schema())
		if err != nil {
			return false, namePrefix, nil, err
		}
		if u.HasExplicitCatalog() {
			// Already 3 parts: nothing to search. Delegate to the resolver.
			namePrefix.CatalogName = tree.Name(u.Catalog())
			namePrefix.SchemaName = tree.Name(scName)
			found, result, err := r.LookupObject(ctx, lookupFlags, u.Catalog(), scName, u.Object())
			return found, namePrefix, result, err
		}
		// Two parts: D.T.
		// Try to use the current database, and be satisfied if it's sufficient to find the object.
		//
		// Note: CockroachDB supports querying virtual schemas even when the current
		// database is not set. For example, `select * from pg_catalog.pg_tables` is
		// meant to show all tables across all databases when there is no current
		// database set. Therefore, we test this even if curDb == "", as long as the
		// schema name is for a virtual schema.

		if _, isVirtualSchema := catconstants.VirtualSchemaNames[scName]; isVirtualSchema || curDb != "" {
			if found, objMeta, err := r.LookupObject(ctx, lookupFlags, curDb, scName, u.Object()); found || err != nil {
				if err == nil {
					namePrefix.CatalogName = tree.Name(curDb)
					namePrefix.SchemaName = tree.Name(scName)
				}
				return found, namePrefix, objMeta, err
			}
		}

		// No luck so far. Compatibility with CockroachDB v1.1: try D.public.T instead.
		if found, objMeta, err := r.LookupObject(ctx, lookupFlags, u.Schema(), tree.PublicSchema, u.Object()); found || err != nil {
			if err == nil {
				namePrefix.CatalogName = tree.Name(u.Schema())
				namePrefix.SchemaName = tree.PublicSchemaName
				namePrefix.ExplicitCatalog = true
			}
			return found, namePrefix, objMeta, err
		}
		// Welp, really haven't found anything.
		return false, namePrefix, nil, nil
	}

	// This is a naked table name. Use the search path.
	iter := searchPath.Iter()
	for next, ok := iter.Next(); ok; next, ok = iter.Next() {
		if found, objMeta, err := r.LookupObject(ctx, lookupFlags, curDb, next,
			u.Object()); found || err != nil {
			if err == nil {
				namePrefix.CatalogName = tree.Name(curDb)
				namePrefix.SchemaName = tree.Name(next)
			}
			return found, namePrefix, objMeta, err
		}
	}
	return false, namePrefix, nil, nil
}

// ResolveTarget performs name resolution for an object name when
// the target object is not expected to exist already. It does not
// mutate the input name. It additionally returns the resolved
// prefix qualification for the object. For example, if the unresolved
// name was "a.b" and the name was resolved to "a.public.b", the
// prefix "a.public" is returned.
func ResolveTarget(
	ctx context.Context,
	u *tree.UnresolvedObjectName,
	r ObjectNameTargetResolver,
	curDb string,
	searchPath sessiondata.SearchPath,
) (found bool, namePrefix tree.ObjectNamePrefix, scMeta catalog.ResolvedObjectPrefix, err error) {
	namePrefix = tree.ObjectNamePrefix{
		SchemaName:      tree.Name(u.Schema()),
		ExplicitSchema:  u.HasExplicitSchema(),
		CatalogName:     tree.Name(u.Catalog()),
		ExplicitCatalog: u.HasExplicitCatalog(),
	}
	if u.HasExplicitSchema() {
		// pg_temp can be used as an alias for the current sessions temporary schema.
		// We must perform this resolution before looking up the object. This
		// resolution only succeeds if the session already has a temporary schema.
		scName, err := searchPath.MaybeResolveTemporarySchema(u.Schema())
		if err != nil {
			return false, namePrefix, scMeta, err
		}
		if u.HasExplicitCatalog() {
			// Already 3 parts: nothing to do.
			namePrefix.CatalogName = tree.Name(u.Catalog())
			namePrefix.SchemaName = tree.Name(scName)
			found, scMeta, err = r.LookupSchema(ctx, u.Catalog(), scName)
			return found, namePrefix, scMeta, err
		}
		// Two parts: D.T.
		// Try to use the current database, and be satisfied if it's sufficient to find the object.
		if found, scMeta, err = r.LookupSchema(ctx, curDb, scName); found || err != nil {
			if err == nil {
				namePrefix.CatalogName = tree.Name(curDb)
				namePrefix.SchemaName = tree.Name(scName)
			}
			return found, namePrefix, scMeta, err
		}
		// No luck so far. Compatibility with CockroachDB v1.1: use D.public.T instead.
		if found, scMeta, err = r.LookupSchema(ctx, u.Schema(), tree.PublicSchema); found || err != nil {
			if err == nil {
				namePrefix.CatalogName = tree.Name(u.Schema())
				namePrefix.SchemaName = tree.PublicSchemaName
				namePrefix.ExplicitCatalog = true
			}
			return found, namePrefix, scMeta, err
		}
		// Welp, really haven't found anything.
		return false, namePrefix, catalog.ResolvedObjectPrefix{}, nil
	}

	// This is a naked table name. Use the current schema = the first
	// valid item in the search path.
	iter := searchPath.IterWithoutImplicitPGSchemas()
	for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
		if found, scMeta, err = r.LookupSchema(ctx, curDb, scName); found || err != nil {
			if err == nil {
				namePrefix.CatalogName = tree.Name(curDb)
				namePrefix.SchemaName = tree.Name(scName)
			}
			break
		}
	}
	return found, namePrefix, scMeta, err
}

// ResolveObjectNamePrefix is used for table prefixes. This is adequate for table
// patterns with stars, e.g. AllTablesSelector.
func ResolveObjectNamePrefix(
	ctx context.Context,
	r ObjectNameTargetResolver,
	curDb string,
	searchPath sessiondata.SearchPath,
	tp *tree.ObjectNamePrefix,
) (found bool, scMeta catalog.ResolvedObjectPrefix, err error) {
	if tp.ExplicitSchema {
		// pg_temp can be used as an alias for the current sessions temporary schema.
		// We must perform this resolution before looking up the object. This
		// resolution only succeeds if the session already has a temporary schema.
		scName, err := searchPath.MaybeResolveTemporarySchema(tp.Schema())
		if err != nil {
			return false, catalog.ResolvedObjectPrefix{}, err
		}
		if tp.ExplicitCatalog {
			// Catalog name is explicit; nothing to do.
			tp.SchemaName = tree.Name(scName)
			return r.LookupSchema(ctx, tp.Catalog(), scName)
		}
		// Try with the current database. This may be empty, because
		// virtual schemas exist even when the db name is empty
		// (CockroachDB extension).
		if found, scMeta, err = r.LookupSchema(ctx, curDb, scName); found || err != nil {
			if err == nil {
				tp.CatalogName = tree.Name(curDb)
				tp.SchemaName = tree.Name(scName)
			}
			return found, scMeta, err
		}
		// No luck so far. Compatibility with CockroachDB v1.1: use D.public.T instead.
		if found, scMeta, err = r.LookupSchema(ctx, tp.Schema(), tree.PublicSchema); found || err != nil {
			if err == nil {
				tp.CatalogName = tp.SchemaName
				tp.SchemaName = tree.PublicSchemaName
				tp.ExplicitCatalog = true
			}
			return found, scMeta, err
		}
		// No luck.
		return false, catalog.ResolvedObjectPrefix{}, nil
	}
	// This is a naked table name. Use the current schema = the first
	// valid item in the search path.
	iter := searchPath.IterWithoutImplicitPGSchemas()
	for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
		if found, scMeta, err = r.LookupSchema(ctx, curDb, scName); found || err != nil {
			if err == nil {
				tp.CatalogName = tree.Name(curDb)
				tp.SchemaName = tree.Name(scName)
			}
			break
		}
	}
	return found, scMeta, err
}
