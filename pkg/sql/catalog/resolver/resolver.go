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
	tree.ObjectNameExistingResolver
	tree.ObjectNameTargetResolver
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
) (res tree.NameResolutionResult, prefix tree.ObjectNamePrefix, err error) {
	found, prefix, descI, err := tree.ResolveExisting(ctx, un, sc, lookupFlags, sc.CurrentDatabase(), sc.CurrentSearchPath())
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

	obj := descI.(catalog.Descriptor)
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

		if lookupFlags.RequireMutable {
			return descI.(*tabledesc.Mutable), prefix, nil
		}

		return descI.(catalog.TableDescriptor), prefix, nil
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
) (*catalog.ResolvedObjectPrefix, tree.ObjectNamePrefix, error) {
	found, prefix, scMeta, err := tree.ResolveTarget(ctx, un, sc, sc.CurrentDatabase(), sc.CurrentSearchPath())
	if err != nil {
		return nil, prefix, err
	}
	if !found {
		if !un.HasExplicitSchema() && !un.HasExplicitCatalog() {
			return nil, prefix, pgerror.New(pgcode.InvalidName, "no database specified")
		}
		err = pgerror.Newf(pgcode.InvalidSchemaName,
			"cannot create %q because the target database or schema does not exist",
			tree.ErrString(un))
		err = errors.WithHint(err, "verify that the current database and search_path are valid and/or the target database exists")
		return nil, prefix, err
	}
	scInfo := scMeta.(*catalog.ResolvedObjectPrefix)
	if scInfo.Schema.Kind == catalog.SchemaVirtual {
		return nil, prefix, pgerror.Newf(pgcode.InsufficientPrivilege,
			"schema cannot be modified: %q", tree.ErrString(&prefix))
	}
	return scInfo, prefix, nil
}

// StaticSchemaIDMap is a map of statically known schema IDs.
var StaticSchemaIDMap = map[descpb.ID]string{
	keys.PublicSchemaID:              tree.PublicSchema,
	catconstants.PgCatalogID:         sessiondata.PgCatalogName,
	catconstants.InformationSchemaID: sessiondata.InformationSchemaName,
	catconstants.CrdbInternalID:      sessiondata.CRDBInternalSchemaName,
	catconstants.PgExtensionSchemaID: sessiondata.PgExtensionSchemaName,
}

// ResolveSchemaNameByID resolves a schema's name based on db and schema id.
// Instead, we have to rely on a scan of the kv table.
// TODO (SQLSchema): The remaining uses of this should be plumbed through
//  the desc.Collection's ResolveSchemaByID.
func ResolveSchemaNameByID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, dbID descpb.ID, schemaID descpb.ID,
) (string, error) {
	// Fast-path for public schema and virtual schemas, to avoid hot lookups.
	for id, schemaName := range StaticSchemaIDMap {
		if id == schemaID {
			return schemaName, nil
		}
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

	nameKey := catalogkeys.NewSchemaKey(dbID, "" /* name */).Key(codec)
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
		_, _, name, err := catalogkeys.DecodeNameMetadataKey(codec, kv.Key)
		if err != nil {
			return nil, err
		}
		ret[id] = name
	}
	return ret, nil
}
