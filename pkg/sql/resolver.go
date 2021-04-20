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
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

var _ resolver.SchemaResolver = &planner{}

// ResolveUncachedDatabaseByName looks up a database name from the store.
// TODO (lucy): See if we can rework the PlanHookState interface to just use
// the desc.Collection methods directly.
func (p *planner) ResolveUncachedDatabaseByName(
	ctx context.Context, dbName string, required bool,
) (res catalog.DatabaseDescriptor, err error) {
	_, res, err = p.Descriptors().GetImmutableDatabaseByName(ctx, p.txn, dbName,
		tree.DatabaseLookupFlags{Required: required, AvoidCached: true})
	return res, err
}

// ResolveUncachedSchemaDescriptor looks up a schema from the store.
func (p *planner) ResolveUncachedSchemaDescriptor(
	ctx context.Context, dbID descpb.ID, name string, required bool,
) (found bool, schema catalog.ResolvedSchema, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		found, schema, err = p.LogicalSchemaAccessor().GetSchema(
			ctx, p.txn, p.ExecCfg().Codec, dbID, name,
			tree.SchemaLookupFlags{Required: required, RequireMutable: true},
		)
	})
	return found, schema, err
}

// ResolveUncachedSchemaDescriptor looks up a mutable descriptor for a schema
// from the store.
func (p *planner) ResolveMutableSchemaDescriptor(
	ctx context.Context, dbID descpb.ID, name string, required bool,
) (found bool, schema catalog.ResolvedSchema, err error) {
	return p.LogicalSchemaAccessor().GetSchema(
		ctx, p.txn, p.ExecCfg().Codec, dbID, name, tree.SchemaLookupFlags{
			Required:       required,
			RequireMutable: true,
		})
}

// runWithOptions sets the provided resolution flags for the
// duration of the call of the passed argument fn.
//
// This is meant to be used like this (for example):
//
// var someVar T
// var err error
// p.runWithOptions(resolveFlags{skipCache: true}, func() {
//    someVar, err = ResolveExistingTableObject(ctx, p, ...)
// })
// if err != nil { ... }
// use(someVar)
func (p *planner) runWithOptions(flags resolveFlags, fn func()) {
	if flags.skipCache {
		defer func(prev bool) { p.avoidCachedDescriptors = prev }(p.avoidCachedDescriptors)
		p.avoidCachedDescriptors = true
	}
	if flags.contextDatabaseID != descpb.InvalidID {
		defer func(prev descpb.ID) { p.contextDatabaseID = prev }(p.contextDatabaseID)
		p.contextDatabaseID = flags.contextDatabaseID
	}
	fn()
}

type resolveFlags struct {
	skipCache         bool
	contextDatabaseID descpb.ID
}

func (p *planner) ResolveMutableTableDescriptor(
	ctx context.Context, tn *tree.TableName, required bool, requiredType tree.RequiredTableKind,
) (table *tabledesc.Mutable, err error) {
	desc, err := resolver.ResolveMutableExistingTableObject(ctx, p, tn, required, requiredType)
	if err != nil {
		return nil, err
	}

	// Ensure that the current user can access the target schema.
	if desc != nil {
		if err := p.canResolveDescUnderSchema(ctx, desc.GetParentSchemaID(), desc); err != nil {
			return nil, err
		}
	}

	return desc, nil
}

func (p *planner) ResolveUncachedTableDescriptor(
	ctx context.Context, tn *tree.TableName, required bool, requiredType tree.RequiredTableKind,
) (table catalog.TableDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		lookupFlags := tree.ObjectLookupFlags{
			CommonLookupFlags:    tree.CommonLookupFlags{Required: required},
			DesiredObjectKind:    tree.TableObject,
			DesiredTableDescKind: requiredType,
		}
		table, err = resolver.ResolveExistingTableObject(ctx, p, tn, lookupFlags)
	})
	if err != nil {
		return nil, err
	}

	// Ensure that the current user can access the target schema.
	if table != nil {
		if err := p.canResolveDescUnderSchema(ctx, table.GetParentSchemaID(), table); err != nil {
			return nil, err
		}
	}

	return table, nil
}

func (p *planner) ResolveTargetObject(
	ctx context.Context, un *tree.UnresolvedObjectName,
) (
	db catalog.DatabaseDescriptor,
	schema catalog.ResolvedSchema,
	namePrefix tree.ObjectNamePrefix,
	err error,
) {
	var prefix *catalog.ResolvedObjectPrefix
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		prefix, namePrefix, err = resolver.ResolveTargetObject(ctx, p, un)
	})
	if err != nil {
		return nil, catalog.ResolvedSchema{}, namePrefix, err
	}
	return prefix.Database, prefix.Schema, namePrefix, err
}

// LookupSchema implements the tree.ObjectNameTargetResolver interface.
func (p *planner) LookupSchema(
	ctx context.Context, dbName, scName string,
) (found bool, scMeta tree.SchemaMeta, err error) {
	found, dbDesc, err := p.Descriptors().GetImmutableDatabaseByName(ctx, p.txn, dbName,
		tree.DatabaseLookupFlags{AvoidCached: p.avoidCachedDescriptors})
	if err != nil || !found {
		return false, nil, err
	}
	sc := p.LogicalSchemaAccessor()
	var resolvedSchema catalog.ResolvedSchema
	found, resolvedSchema, err = sc.GetSchema(ctx, p.txn, p.ExecCfg().Codec, dbDesc.GetID(), scName,
		p.CommonLookupFlags(false /* required */))
	if err != nil {
		return false, nil, err
	}
	return found, &catalog.ResolvedObjectPrefix{
		Database: dbDesc,
		Schema:   resolvedSchema,
	}, nil
}

// LookupObject implements the tree.ObjectNameExistingResolver interface.
func (p *planner) LookupObject(
	ctx context.Context, lookupFlags tree.ObjectLookupFlags, dbName, scName, tbName string,
) (found bool, objMeta tree.NameResolutionResult, err error) {
	sc := p.LogicalSchemaAccessor()
	lookupFlags.CommonLookupFlags.Required = false
	lookupFlags.CommonLookupFlags.AvoidCached = p.avoidCachedDescriptors
	objDesc, err := sc.GetObjectDesc(ctx, p.txn, p.ExecCfg().Settings, p.ExecCfg().Codec, dbName, scName, tbName, lookupFlags)

	return objDesc != nil, objDesc, err
}

// CommonLookupFlags is part of the resolver.SchemaResolver interface.
func (p *planner) CommonLookupFlags(required bool) tree.CommonLookupFlags {
	return tree.CommonLookupFlags{
		Required:    required,
		AvoidCached: p.avoidCachedDescriptors,
	}
}

// IsTableVisible is part of the tree.EvalDatabase interface.
func (p *planner) IsTableVisible(
	ctx context.Context, curDB string, searchPath sessiondata.SearchPath, tableID oid.Oid,
) (isVisible, exists bool, err error) {
	tableDesc, err := p.LookupTableByID(ctx, descpb.ID(tableID))
	if err != nil {
		// If a "not found" error happened here, we return "not exists" rather than
		// the error.
		if errors.Is(err, catalog.ErrDescriptorNotFound) ||
			errors.Is(err, catalog.ErrDescriptorDropped) ||
			pgerror.GetPGCode(err) == pgcode.UndefinedTable ||
			pgerror.GetPGCode(err) == pgcode.UndefinedObject {
			return false, false, nil //nolint:returnerrcheck
		}
		return false, false, err
	}
	schemaID := tableDesc.GetParentSchemaID()
	schemaDesc, err := p.Descriptors().GetImmutableSchemaByID(ctx, p.Txn(), schemaID,
		tree.SchemaLookupFlags{
			Required:    true,
			AvoidCached: p.avoidCachedDescriptors})
	if err != nil {
		return false, false, err
	}
	if schemaDesc.Kind != catalog.SchemaVirtual {
		dbID := tableDesc.GetParentID()
		_, dbDesc, err := p.Descriptors().GetImmutableDatabaseByID(ctx, p.Txn(), dbID,
			tree.DatabaseLookupFlags{
				Required:    true,
				AvoidCached: p.avoidCachedDescriptors})
		if err != nil {
			return false, false, err
		}
		if dbDesc.GetName() != curDB {
			// If the table is in a different database, then it's considered to be
			// "not existing" instead of just "not visible"; this matches PostgreSQL.
			return false, false, nil
		}
	}
	iter := searchPath.Iter()
	for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
		if schemaDesc.Name == scName {
			return true, true, nil
		}
	}
	return false, true, nil
}

// IsTypeVisible is part of the tree.EvalDatabase interface.
func (p *planner) IsTypeVisible(
	ctx context.Context, curDB string, searchPath sessiondata.SearchPath, typeID oid.Oid,
) (isVisible bool, exists bool, err error) {
	// Check builtin types first. They are always globally visible.
	if _, ok := types.OidToType[typeID]; ok {
		return true, true, nil
	}
	typName, _, err := p.GetTypeDescriptor(ctx, typedesc.UserDefinedTypeOIDToID(typeID))
	if err != nil {
		// If a "not found" error happened here, we return "not exists" rather than
		// the error.
		if errors.Is(err, catalog.ErrDescriptorNotFound) ||
			errors.Is(err, catalog.ErrDescriptorDropped) ||
			pgerror.GetPGCode(err) == pgcode.UndefinedObject {
			return false, false, nil //nolint:returnerrcheck
		}
		return false, false, err
	}
	if typName.CatalogName.String() != curDB {
		// If the type is in a different database, then it's considered to be
		// "not existing" instead of just "not visible"; this matches PostgreSQL.
		return false, false, nil
	}
	iter := searchPath.Iter()
	for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
		if typName.SchemaName.String() == scName {
			return true, true, nil
		}
	}
	return false, true, nil
}

// GetTypeDescriptor implements the descpb.TypeDescriptorResolver interface.
func (p *planner) GetTypeDescriptor(
	ctx context.Context, id descpb.ID,
) (tree.TypeName, catalog.TypeDescriptor, error) {
	desc, err := p.Descriptors().GetImmutableTypeByID(ctx, p.txn, id, tree.ObjectLookupFlags{})
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	// Note that the value of required doesn't matter for lookups by ID.
	_, dbDesc, err := p.Descriptors().GetImmutableDatabaseByID(ctx, p.txn, desc.GetParentID(), p.CommonLookupFlags(true /* required */))
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	sc, err := p.Descriptors().GetImmutableSchemaByID(
		ctx, p.txn, desc.GetParentSchemaID(), tree.SchemaLookupFlags{})
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	name := tree.MakeNewQualifiedTypeName(dbDesc.GetName(), sc.Name, desc.GetName())
	return name, desc, nil
}

// ResolveType implements the TypeReferenceResolver interface.
func (p *planner) ResolveType(
	ctx context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{Required: true, RequireMutable: false},
		DesiredObjectKind: tree.TypeObject,
	}
	desc, prefix, err := resolver.ResolveExistingObject(ctx, p, name, lookupFlags)
	if err != nil {
		return nil, err
	}
	tn := tree.MakeNewQualifiedTypeName(prefix.Catalog(), prefix.Schema(), name.Object())
	tdesc := desc.(catalog.TypeDescriptor)

	// Disllow cross-database type resolution. Note that we check
	// p.contextDatabaseID != descpb.InvalidID when we have been restricted to
	// accessing types in the database with ID = p.contextDatabaseID by
	// p.runWithOptions. So, check to see if the resolved descriptor's parentID
	// matches, unless the descriptor's parentID is invalid. This could happen
	// when the type being resolved is a builtin type prefaced with a virtual
	// schema like `pg_catalog.int`. Resolution for these types returns a dummy
	// TypeDescriptor, so ignore those cases.
	if p.contextDatabaseID != descpb.InvalidID && tdesc.GetParentID() != descpb.InvalidID && tdesc.GetParentID() != p.contextDatabaseID {
		return nil, pgerror.Newf(
			pgcode.FeatureNotSupported, "cross database type references are not supported: %s", tn.String())
	}

	// Ensure that the user can access the target schema.
	if err := p.canResolveDescUnderSchema(ctx, tdesc.GetParentSchemaID(), tdesc); err != nil {
		return nil, err
	}

	return tdesc.MakeTypesT(ctx, &tn, p)
}

// ResolveTypeByOID implements the tree.TypeResolver interface.
func (p *planner) ResolveTypeByOID(ctx context.Context, oid oid.Oid) (*types.T, error) {
	name, desc, err := p.GetTypeDescriptor(ctx, typedesc.UserDefinedTypeOIDToID(oid))
	if err != nil {
		return nil, err
	}
	return desc.MakeTypesT(ctx, &name, p)
}

// ObjectLookupFlags is part of the resolver.SchemaResolver interface.
func (p *planner) ObjectLookupFlags(required, requireMutable bool) tree.ObjectLookupFlags {
	flags := p.CommonLookupFlags(required)
	flags.RequireMutable = requireMutable
	return tree.ObjectLookupFlags{CommonLookupFlags: flags}
}

// getDescriptorsFromTargetListForPrivilegeChange fetches the descriptors for the targets.
func getDescriptorsFromTargetListForPrivilegeChange(
	ctx context.Context, p *planner, targets tree.TargetList,
) ([]catalog.Descriptor, error) {
	if targets.Databases != nil {
		if len(targets.Databases) == 0 {
			return nil, errNoDatabase
		}
		descs := make([]catalog.Descriptor, 0, len(targets.Databases))
		for _, database := range targets.Databases {
			_, descriptor, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn,
				string(database), tree.DatabaseLookupFlags{Required: true})
			if err != nil {
				return nil, err
			}
			descs = append(descs, descriptor)
		}
		if len(descs) == 0 {
			return nil, errNoMatch
		}
		return descs, nil
	}

	if targets.Types != nil {
		if len(targets.Types) == 0 {
			return nil, errNoType
		}
		descs := make([]catalog.Descriptor, 0, len(targets.Types))
		for _, typ := range targets.Types {
			descriptor, err := p.ResolveMutableTypeDescriptor(ctx, typ, true /* required */)
			if err != nil {
				return nil, err
			}

			descs = append(descs, descriptor)
		}

		if len(descs) == 0 {
			return nil, errNoMatch
		}
		return descs, nil
	}

	if targets.Schemas != nil {
		if len(targets.Schemas) == 0 {
			return nil, errNoSchema
		}
		descs := make([]catalog.Descriptor, 0, len(targets.Schemas))

		// Resolve the databases being changed
		type schemaWithDBDesc struct {
			schema string
			dbDesc *dbdesc.Mutable
		}
		var targetSchemas []schemaWithDBDesc
		for _, sc := range targets.Schemas {
			dbName := p.CurrentDatabase()
			if sc.ExplicitCatalog {
				dbName = sc.Catalog()
			}
			_, db, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, dbName,
				tree.DatabaseLookupFlags{Required: true})
			if err != nil {
				return nil, err
			}
			targetSchemas = append(targetSchemas, schemaWithDBDesc{schema: sc.Schema(), dbDesc: db})
		}

		for _, sc := range targetSchemas {
			_, resSchema, err := p.ResolveMutableSchemaDescriptor(
				ctx, sc.dbDesc.ID, sc.schema, true /* required */)
			if err != nil {
				return nil, err
			}
			switch resSchema.Kind {
			case catalog.SchemaUserDefined:
				descs = append(descs, resSchema.Desc)
			default:
				return nil, pgerror.Newf(pgcode.InvalidSchemaName,
					"cannot change privileges on schema %q", resSchema.Name)
			}
		}
		return descs, nil
	}

	if len(targets.Tables) == 0 {
		return nil, errNoTable
	}
	descs := make([]catalog.Descriptor, 0, len(targets.Tables))
	for _, tableTarget := range targets.Tables {
		tableGlob, err := tableTarget.NormalizeTablePattern()
		if err != nil {
			return nil, err
		}
		objectNames, objectIDs, err := expandTableGlob(ctx, p, tableGlob)
		if err != nil {
			return nil, err
		}

		for i := range objectIDs {
			descriptor, err := p.Descriptors().GetMutableDescriptorByID(ctx, objectIDs[i], p.txn)
			if err != nil {
				// If the table is virtual, the above lookup will not find the table.
				// So, try resolving the table instead.
				if errors.Is(err, catalog.ErrDescriptorNotFound) {
					descriptor, err = resolver.ResolveMutableExistingTableObject(ctx, p,
						&objectNames[i], false /* required */, tree.ResolveAnyTableKind)
					if err != nil {
						return nil, err
					}
				} else {
					return nil, err
				}
			}
			if descriptor != nil && descriptor.DescriptorType() == catalog.Table {
				descs = append(descs, descriptor)
			}
		}
	}
	if len(descs) == 0 {
		return nil, errNoMatch
	}
	return descs, nil
}

// getFullyQualifiedTableNamesFromIDs resolves a list of table IDs to their
// fully qualified names.
func (p *planner) getFullyQualifiedTableNamesFromIDs(
	ctx context.Context, ids []descpb.ID,
) (fullyQualifiedNames []*tree.TableName, _ error) {
	for _, id := range ids {
		desc, err := p.Descriptors().GetImmutableTableByID(ctx, p.txn, id, tree.ObjectLookupFlags{
			CommonLookupFlags: tree.CommonLookupFlags{
				AvoidCached:    true,
				IncludeDropped: true,
				IncludeOffline: true,
			},
		})
		if err != nil {
			return nil, err
		}
		fqName, err := p.getQualifiedTableName(ctx, desc)
		if err != nil {
			return nil, err
		}
		fullyQualifiedNames = append(fullyQualifiedNames, fqName)
	}
	return fullyQualifiedNames, nil
}

// getQualifiedTableName returns the database-qualified name of the table
// or view represented by the provided descriptor. It is a sort of
// reverse of the Resolve() functions.
func (p *planner) getQualifiedTableName(
	ctx context.Context, desc catalog.TableDescriptor,
) (*tree.TableName, error) {
	_, dbDesc, err := p.Descriptors().GetImmutableDatabaseByID(ctx, p.txn, desc.GetParentID(),
		tree.DatabaseLookupFlags{
			Required:       true,
			IncludeOffline: true,
			IncludeDropped: true,
		})
	if err != nil {
		return nil, err
	}
	schemaID := desc.GetParentSchemaID()
	resolvedSchema, err := p.Descriptors().GetImmutableSchemaByID(ctx, p.txn, schemaID,
		tree.SchemaLookupFlags{
			IncludeOffline: true,
			IncludeDropped: true,
		})
	if err != nil {
		return nil, err
	}
	tbName := tree.MakeTableNameWithSchema(
		tree.Name(dbDesc.GetName()),
		tree.Name(resolvedSchema.Name),
		tree.Name(desc.GetName()),
	)
	return &tbName, nil
}

// GetQualifiedTableNameByID returns the qualified name of the table,
// view or sequence represented by the provided ID and table kind.
func (p *planner) GetQualifiedTableNameByID(
	ctx context.Context, id int64, requiredType tree.RequiredTableKind,
) (*tree.TableName, error) {
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags:    tree.CommonLookupFlags{Required: true},
		DesiredObjectKind:    tree.TableObject,
		DesiredTableDescKind: requiredType,
	}

	table, err := p.Descriptors().GetImmutableTableByID(
		ctx, p.txn, descpb.ID(id), lookupFlags)
	if err != nil {
		return nil, err
	}
	return p.getQualifiedTableName(ctx, table)
}

// getQualifiedSchemaName returns the database-qualified name of the
// schema represented by the provided descriptor.
func (p *planner) getQualifiedSchemaName(
	ctx context.Context, desc catalog.SchemaDescriptor,
) (*tree.ObjectNamePrefix, error) {
	_, dbDesc, err := p.Descriptors().GetImmutableDatabaseByID(ctx, p.txn, desc.GetParentID(),
		tree.DatabaseLookupFlags{
			Required: true,
		})
	if err != nil {
		return nil, err
	}
	return &tree.ObjectNamePrefix{
		CatalogName:     tree.Name(dbDesc.GetName()),
		SchemaName:      tree.Name(desc.GetName()),
		ExplicitCatalog: true,
		ExplicitSchema:  true,
	}, nil
}

// getQualifiedTypeName returns the database-qualified name of the type
// represented by the provided descriptor.
func (p *planner) getQualifiedTypeName(
	ctx context.Context, desc catalog.TypeDescriptor,
) (*tree.TypeName, error) {
	_, dbDesc, err := p.Descriptors().GetImmutableDatabaseByID(ctx, p.txn, desc.GetParentID(),
		tree.DatabaseLookupFlags{
			Required: true,
		})
	if err != nil {
		return nil, err
	}

	schemaID := desc.GetParentSchemaID()
	resolvedSchema, err := p.Descriptors().GetImmutableSchemaByID(ctx, p.txn, schemaID, tree.SchemaLookupFlags{})
	if err != nil {
		return nil, err
	}

	typeName := tree.MakeNewQualifiedTypeName(
		dbDesc.GetName(),
		resolvedSchema.Name,
		desc.GetName(),
	)

	return &typeName, nil
}

// findTableContainingIndex returns the descriptor of a table
// containing the index of the given name.
// This is used by expandMutableIndexName().
//
// An error is returned if the index name is ambiguous (i.e. exists in
// multiple tables). If no table is found and requireTable is true, an
// error will be returned, otherwise the TableName and descriptor
// returned will be nil.
func findTableContainingIndex(
	ctx context.Context,
	txn *kv.Txn,
	sc resolver.SchemaResolver,
	codec keys.SQLCodec,
	dbName, scName string,
	idxName tree.UnrestrictedName,
	lookupFlags tree.CommonLookupFlags,
) (result *tree.TableName, desc *tabledesc.Mutable, err error) {
	sa := sc.LogicalSchemaAccessor()
	dbDesc, err := sa.GetDatabaseDesc(ctx, txn, codec, dbName, lookupFlags)
	if dbDesc == nil || err != nil {
		return nil, nil, err
	}

	tns, _, err := sa.GetObjectNamesAndIDs(ctx, txn, codec, dbDesc, scName,
		tree.DatabaseListFlags{CommonLookupFlags: lookupFlags, ExplicitPrefix: true})
	if err != nil {
		return nil, nil, err
	}

	result = nil
	for i := range tns {
		tn := &tns[i]
		tableDesc, err := resolver.ResolveMutableExistingTableObject(ctx, sc, tn, false /*required*/, tree.ResolveAnyTableKind)
		if err != nil {
			return nil, nil, err
		}
		if tableDesc == nil || !(tableDesc.IsTable() || tableDesc.MaterializedView()) {
			continue
		}

		idx, err := tableDesc.FindIndexWithName(string(idxName))
		if err != nil || idx.Dropped() {
			// err is nil if the index does not exist on the table.
			continue
		}
		if result != nil {
			return nil, nil, pgerror.Newf(pgcode.AmbiguousParameter,
				"index name %q is ambiguous (found in %s and %s)",
				idxName, tn.String(), result.String())
		}
		result = tn
		desc = tableDesc
	}
	if result == nil && lookupFlags.Required {
		return nil, nil, pgerror.Newf(pgcode.UndefinedObject,
			"index %q does not exist", idxName)
	}
	return result, desc, nil
}

// expandMutableIndexName ensures that the index name is qualified with a table
// name, and searches the table name if not yet specified.
//
// It returns the TableName of the underlying table for convenience.
// If no table is found and requireTable is true an error will be
// returned, otherwise the TableName returned will be nil.
//
// It *may* return the descriptor of the underlying table, depending
// on the lookup path. This can be used in the caller to avoid a 2nd
// lookup.
func expandMutableIndexName(
	ctx context.Context, p *planner, index *tree.TableIndexName, requireTable bool,
) (tn *tree.TableName, desc *tabledesc.Mutable, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		tn, desc, err = expandIndexName(ctx, p.txn, p, p.ExecCfg().Codec, index, requireTable)
	})
	return tn, desc, err
}

func expandIndexName(
	ctx context.Context,
	txn *kv.Txn,
	sc resolver.SchemaResolver,
	codec keys.SQLCodec,
	index *tree.TableIndexName,
	requireTable bool,
) (tn *tree.TableName, desc *tabledesc.Mutable, err error) {
	tn = &index.Table
	if tn.Table() != "" {
		// The index and its table prefix must exist already. Resolve the table.
		desc, err = resolver.ResolveMutableExistingTableObject(ctx, sc, tn, requireTable, tree.ResolveRequireTableOrViewDesc)
		if err != nil {
			return nil, nil, err
		}
		if desc != nil && desc.IsView() && !desc.MaterializedView() {
			return nil, nil, pgerror.Newf(pgcode.WrongObjectType,
				"%q is not a table or materialized view", tn.Table())
		}
		return tn, desc, nil
	}

	// On the first call to expandMutableIndexName(), index.Table.Table() is empty.
	// Once the table name is resolved for the index below, index.Table
	// references the table name.

	// Look up the table prefix.
	found, _, err := tn.ObjectNamePrefix.Resolve(ctx, sc, sc.CurrentDatabase(), sc.CurrentSearchPath())
	if err != nil {
		return nil, nil, err
	}
	if !found {
		if requireTable {
			err = pgerror.Newf(pgcode.UndefinedObject,
				"schema or database was not found while searching index: %q",
				tree.ErrString(&index.Index))
			err = errors.WithHint(err, "check the current database and search_path are valid")
			return nil, nil, err
		}
		return nil, nil, nil
	}

	lookupFlags := sc.CommonLookupFlags(requireTable)
	var foundTn *tree.TableName
	foundTn, desc, err = findTableContainingIndex(ctx, txn, sc, codec, tn.Catalog(), tn.Schema(), index.Index, lookupFlags)
	if err != nil {
		return nil, nil, err
	}

	if foundTn != nil {
		// Memoize the table name that was found. tn is a reference to the table name
		// stored in index.Table.
		*tn = *foundTn
	}
	return tn, desc, nil
}

// getTableAndIndex returns the table and index descriptors for a
// TableIndexName.
//
// It can return indexes that are being rolled out.
func (p *planner) getTableAndIndex(
	ctx context.Context, tableWithIndex *tree.TableIndexName, privilege privilege.Kind,
) (*tabledesc.Mutable, *descpb.IndexDescriptor, error) {
	var catalog optCatalog
	catalog.init(p)
	catalog.reset()

	idx, qualifiedName, err := cat.ResolveTableIndex(
		ctx, &catalog, cat.Flags{AvoidDescriptorCaches: true}, tableWithIndex,
	)
	if err != nil {
		return nil, nil, err
	}
	if err := catalog.CheckPrivilege(ctx, idx.Table(), privilege); err != nil {
		return nil, nil, err
	}
	optIdx := idx.(*optIndex)

	// Resolve the object name for logging if
	// its missing.
	if tableWithIndex.Table.ObjectName == "" {
		tableWithIndex.Table = tree.MakeTableNameFromPrefix(qualifiedName.ObjectNamePrefix, qualifiedName.ObjectName)
	}

	return tabledesc.NewBuilder(optIdx.tab.desc.TableDesc()).BuildExistingMutableTable(), optIdx.desc, nil
}

// expandTableGlob expands pattern into a list of objects represented
// as a tree.TableNames.
func expandTableGlob(
	ctx context.Context, p *planner, pattern tree.TablePattern,
) (tree.TableNames, descpb.IDs, error) {
	var catalog optCatalog
	catalog.init(p)
	catalog.reset()

	return cat.ExpandDataSourceGlob(ctx, &catalog, cat.Flags{}, pattern)
}

// fkSelfResolver is a SchemaResolver that inserts itself between a
// user of name resolution and another SchemaResolver, and will answer
// lookups of the new table being created. This is needed in the case
// of CREATE TABLE with a foreign key self-reference: the target of
// the FK definition is a table that does not exist yet.
type fkSelfResolver struct {
	resolver.SchemaResolver
	newTableName *tree.TableName
	newTableDesc *tabledesc.Mutable
}

var _ resolver.SchemaResolver = &fkSelfResolver{}

// LookupObject implements the tree.ObjectNameExistingResolver interface.
func (r *fkSelfResolver) LookupObject(
	ctx context.Context, lookupFlags tree.ObjectLookupFlags, dbName, scName, tbName string,
) (found bool, objMeta tree.NameResolutionResult, err error) {
	if dbName == r.newTableName.Catalog() &&
		scName == r.newTableName.Schema() &&
		tbName == r.newTableName.Table() {
		table := r.newTableDesc
		if lookupFlags.RequireMutable {
			return true, table, nil
		}
		return true, table.ImmutableCopy(), nil
	}
	lookupFlags.IncludeOffline = false
	return r.SchemaResolver.LookupObject(ctx, lookupFlags, dbName, scName, tbName)
}

// internalLookupCtx can be used in contexts where all descriptors
// have been recently read, to accelerate the lookup of
// inter-descriptor relationships.
//
// This is used mainly in the generators for virtual tables,
// aliased as tableLookupFn below.
//
// It only reveals physical descriptors (not virtual descriptors).
// It also implements catalog.DescGetter for table validation. In this scenario
// it may fall back to utilizing the fallback DescGetter to resolve references
// outside of the dbContext in which it was initialized.
//
// TODO(ajwerner): remove in 21.2 or whenever cross-database references are
// fully removed.
type internalLookupCtx struct {
	dbNames     map[descpb.ID]string
	dbIDs       []descpb.ID
	dbDescs     map[descpb.ID]catalog.DatabaseDescriptor
	schemaDescs map[descpb.ID]catalog.SchemaDescriptor
	schemaNames map[descpb.ID]string
	schemaIDs   []descpb.ID
	tbDescs     map[descpb.ID]catalog.TableDescriptor
	tbIDs       []descpb.ID
	typDescs    map[descpb.ID]catalog.TypeDescriptor
	typIDs      []descpb.ID

	// fallback is utilized in GetDesc and GetNamespaceEntry.
	fallback catalog.DescGetter
}

// GetDesc implements the catalog.DescGetter interface.
func (l *internalLookupCtx) GetDesc(ctx context.Context, id descpb.ID) (catalog.Descriptor, error) {
	if desc, ok := l.dbDescs[id]; ok {
		return desc, nil
	}
	if desc, ok := l.schemaDescs[id]; ok {
		return desc, nil
	}
	if desc, ok := l.typDescs[id]; ok {
		return desc, nil
	}
	if desc, ok := l.tbDescs[id]; ok {
		return desc, nil
	}
	if l.fallback != nil {
		return l.fallback.GetDesc(ctx, id)
	}
	return nil, nil
}

// GetNamespaceEntry implements the catalog.DescGetter interface.
func (l *internalLookupCtx) GetNamespaceEntry(
	ctx context.Context, parentID, parentSchemaID descpb.ID, name string,
) (descpb.ID, error) {
	if l.fallback != nil {
		return l.fallback.GetNamespaceEntry(ctx, parentID, parentSchemaID, name)
	}
	return descpb.InvalidID, nil
}

// tableLookupFn can be used to retrieve a table descriptor and its corresponding
// database descriptor using the table's ID.
type tableLookupFn = *internalLookupCtx

// newInternalLookupCtxFromDescriptors "unwraps" the descriptors into the
// appropriate implementation of Descriptor before constructing a new
// internalLookupCtx. It also hydrates any table descriptors with enum
// information. It is intended only for use when dealing with backups.
func newInternalLookupCtxFromDescriptors(
	ctx context.Context, rawDescs []descpb.Descriptor, prefix catalog.DatabaseDescriptor,
) (*internalLookupCtx, error) {
	descriptors := make([]catalog.Descriptor, len(rawDescs))
	for i := range rawDescs {
		descriptors[i] = catalogkv.NewBuilder(&rawDescs[i]).BuildImmutable()
	}
	lCtx := newInternalLookupCtx(ctx, descriptors, prefix, nil /* fallback */)
	if err := descs.HydrateGivenDescriptors(ctx, descriptors); err != nil {
		return nil, err
	}
	return lCtx, nil
}

// newInternalLookupCtx provides cached access to a set of descriptors for use
// in virtual tables.
func newInternalLookupCtx(
	ctx context.Context,
	descs []catalog.Descriptor,
	prefix catalog.DatabaseDescriptor,
	fallback catalog.DescGetter,
) *internalLookupCtx {
	dbNames := make(map[descpb.ID]string)
	dbDescs := make(map[descpb.ID]catalog.DatabaseDescriptor)
	schemaDescs := make(map[descpb.ID]catalog.SchemaDescriptor)
	schemaNames := map[descpb.ID]string{
		keys.PublicSchemaID: tree.PublicSchema,
	}
	tbDescs := make(map[descpb.ID]catalog.TableDescriptor)
	typDescs := make(map[descpb.ID]catalog.TypeDescriptor)
	var tbIDs, typIDs, dbIDs, schemaIDs []descpb.ID
	// Record descriptors for name lookups.
	for i := range descs {
		switch desc := descs[i].(type) {
		case catalog.DatabaseDescriptor:
			dbNames[desc.GetID()] = desc.GetName()
			dbDescs[desc.GetID()] = desc
			if prefix == nil || prefix.GetID() == desc.GetID() {
				// Only make the database visible for iteration if the prefix was included.
				dbIDs = append(dbIDs, desc.GetID())
			}
		case catalog.TableDescriptor:
			tbDescs[desc.GetID()] = desc
			if prefix == nil || prefix.GetID() == desc.GetParentID() {
				// Only make the table visible for iteration if the prefix was included.
				tbIDs = append(tbIDs, desc.GetID())
			}
		case catalog.TypeDescriptor:
			typDescs[desc.GetID()] = desc
			if prefix == nil || prefix.GetID() == desc.GetParentID() {
				// Only make the type visible for iteration if the prefix was included.
				typIDs = append(typIDs, desc.GetID())
			}
		case catalog.SchemaDescriptor:
			schemaDescs[desc.GetID()] = desc
			if prefix == nil || prefix.GetID() == desc.GetParentID() {
				// Only make the schema visible for iteration if the prefix was included.
				schemaIDs = append(schemaIDs, desc.GetID())
				schemaNames[desc.GetID()] = desc.GetName()
			}
		}
	}

	return &internalLookupCtx{
		dbNames:     dbNames,
		dbDescs:     dbDescs,
		schemaDescs: schemaDescs,
		schemaNames: schemaNames,
		schemaIDs:   schemaIDs,
		tbDescs:     tbDescs,
		typDescs:    typDescs,
		tbIDs:       tbIDs,
		dbIDs:       dbIDs,
		typIDs:      typIDs,
		fallback:    fallback,
	}
}

var _ catalog.DescGetter = (*internalLookupCtx)(nil)

func (l *internalLookupCtx) getDatabaseByID(id descpb.ID) (catalog.DatabaseDescriptor, error) {
	db, ok := l.dbDescs[id]
	if !ok {
		return nil, sqlerrors.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", id))
	}
	return db, nil
}

func (l *internalLookupCtx) getTableByID(id descpb.ID) (catalog.TableDescriptor, error) {
	tb, ok := l.tbDescs[id]
	if !ok {
		return nil, sqlerrors.NewUndefinedRelationError(
			tree.NewUnqualifiedTableName(tree.Name(fmt.Sprintf("[%d]", id))))
	}
	return tb, nil
}

func (l *internalLookupCtx) getTypeByID(id descpb.ID) (catalog.TypeDescriptor, error) {
	typ, ok := l.typDescs[id]
	if !ok {
		return nil, sqlerrors.NewUndefinedRelationError(
			tree.NewUnqualifiedTableName(tree.Name(fmt.Sprintf("[%d]", id))))
	}
	return typ, nil
}

func (l *internalLookupCtx) getSchemaByID(id descpb.ID) (catalog.SchemaDescriptor, error) {
	sc, ok := l.schemaDescs[id]
	if !ok {
		return nil, sqlerrors.NewUndefinedSchemaError(fmt.Sprintf("[%d]", id))
	}
	return sc, nil
}

// getSchemaNameByID returns the schema name given an ID for a schema.
func (l *internalLookupCtx) getSchemaNameByID(id descpb.ID) (string, error) {
	if id == keys.PublicSchemaID {
		return tree.PublicSchema, nil
	}
	schema, err := l.getSchemaByID(id)
	if err != nil {
		return "", err
	}
	return schema.GetName(), nil
}

func (l *internalLookupCtx) getDatabaseName(table catalog.Descriptor) string {
	parentName := l.dbNames[table.GetParentID()]
	if parentName == "" {
		// The parent database was deleted. This is possible e.g. when
		// a database is dropped with CASCADE, and someone queries
		// this table before the dropped table descriptors are
		// effectively deleted.
		parentName = fmt.Sprintf("[%d]", table.GetParentID())
	}
	return parentName
}

func (l *internalLookupCtx) getSchemaName(table catalog.TableDescriptor) string {
	schemaName := l.schemaNames[table.GetParentSchemaID()]
	if schemaName == "" {
		// The parent schema was deleted. This is possible e.g. when
		// a schema is dropped with CASCADE, and someone queries
		// this table before the dropped table descriptors are
		// effectively deleted.
		schemaName = fmt.Sprintf("[%d]", table.GetParentSchemaID())
	}
	return schemaName
}

// getParentAsTableName returns a TreeTable object of the parent table for a
// given table ID. Used to get the parent table of a table with interleaved
// indexes.
func getParentAsTableName(
	l simpleSchemaResolver, parentTableID descpb.ID, dbPrefix string,
) (tree.TableName, error) {
	var parentName tree.TableName
	parentTable, err := l.getTableByID(parentTableID)
	if err != nil {
		return tree.TableName{}, err
	}
	var parentSchemaName tree.Name
	if parentTable.GetParentSchemaID() == keys.PublicSchemaID {
		parentSchemaName = tree.PublicSchemaName
	} else {
		parentSchema, err := l.getSchemaByID(parentTable.GetParentSchemaID())
		if err != nil {
			return tree.TableName{}, err
		}
		parentSchemaName = tree.Name(parentSchema.GetName())
	}
	parentDbDesc, err := l.getDatabaseByID(parentTable.GetParentID())
	if err != nil {
		return tree.TableName{}, err
	}
	parentName = tree.MakeTableNameWithSchema(tree.Name(parentDbDesc.GetName()),
		parentSchemaName, tree.Name(parentTable.GetName()))
	parentName.ExplicitCatalog = parentDbDesc.GetName() != dbPrefix
	return parentName, nil
}

// getTableNameFromTableDescriptor returns a TableName object for a given
// TableDescriptor.
func getTableNameFromTableDescriptor(
	l simpleSchemaResolver, table catalog.TableDescriptor, dbPrefix string,
) (tree.TableName, error) {
	var tableName tree.TableName
	tableDbDesc, err := l.getDatabaseByID(table.GetParentID())
	if err != nil {
		return tree.TableName{}, err
	}
	var parentSchemaName tree.Name
	if table.GetParentSchemaID() == keys.PublicSchemaID {
		parentSchemaName = tree.PublicSchemaName
	} else {
		parentSchema, err := l.getSchemaByID(table.GetParentSchemaID())
		if err != nil {
			return tree.TableName{}, err
		}
		parentSchemaName = tree.Name(parentSchema.GetName())
	}
	tableName = tree.MakeTableNameWithSchema(tree.Name(tableDbDesc.GetName()),
		parentSchemaName, tree.Name(table.GetName()))
	tableName.ExplicitCatalog = tableDbDesc.GetName() != dbPrefix
	return tableName, nil
}

// getTypeNameFromTypeDescriptor returns a TypeName object for a given
// TableDescriptor.
func getTypeNameFromTypeDescriptor(
	l simpleSchemaResolver, typ catalog.TypeDescriptor,
) (tree.TypeName, error) {
	var typeName tree.TypeName
	tableDbDesc, err := l.getDatabaseByID(typ.GetParentID())
	if err != nil {
		return typeName, err
	}
	var parentSchemaName string
	if typ.GetParentSchemaID() == keys.PublicSchemaID {
		parentSchemaName = tree.PublicSchema
	} else {
		parentSchema, err := l.getSchemaByID(typ.GetParentSchemaID())
		if err != nil {
			return typeName, err
		}
		parentSchemaName = parentSchema.GetName()
	}
	typeName = tree.MakeNewQualifiedTypeName(tableDbDesc.GetName(),
		parentSchemaName, typ.GetName())
	return typeName, nil
}

// ResolveMutableTypeDescriptor resolves a type descriptor for mutable access.
func (p *planner) ResolveMutableTypeDescriptor(
	ctx context.Context, name *tree.UnresolvedObjectName, required bool,
) (*typedesc.Mutable, error) {
	tn, desc, err := resolver.ResolveMutableType(ctx, p, name, required)
	if err != nil {
		return nil, err
	}

	if desc != nil {
		// Ensure that the user can access the target schema.
		if err := p.canResolveDescUnderSchema(ctx, desc.GetParentSchemaID(), desc); err != nil {
			return nil, err
		}
		if tn != nil {
			name.SetAnnotation(&p.semaCtx.Annotations, tn)
		}
	}

	return desc, nil
}

// The versions below are part of the work for #34240.
// TODO(radu): clean these up when everything is switched over.

// See ResolveMutableTableDescriptor.
func (p *planner) ResolveMutableTableDescriptorEx(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType tree.RequiredTableKind,
) (*tabledesc.Mutable, error) {
	tn := name.ToTableName()
	table, err := resolver.ResolveMutableExistingTableObject(ctx, p, &tn, required, requiredType)
	if err != nil {
		return nil, err
	}
	name.SetAnnotation(&p.semaCtx.Annotations, &tn)

	if table != nil {
		// Ensure that the user can access the target schema.
		if err := p.canResolveDescUnderSchema(ctx, table.GetParentSchemaID(), table); err != nil {
			return nil, err
		}
	}

	return table, nil
}

// ResolveMutableTableDescriptorExAllowNoPrimaryKey performs the
// same logic as ResolveMutableTableDescriptorEx but allows for
// the resolved table to not have a primary key.
func (p *planner) ResolveMutableTableDescriptorExAllowNoPrimaryKey(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType tree.RequiredTableKind,
) (*tabledesc.Mutable, error) {
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags:      tree.CommonLookupFlags{Required: required, RequireMutable: true},
		AllowWithoutPrimaryKey: true,
		DesiredObjectKind:      tree.TableObject,
		DesiredTableDescKind:   requiredType,
	}
	desc, prefix, err := resolver.ResolveExistingObject(ctx, p, name, lookupFlags)
	if err != nil || desc == nil {
		return nil, err
	}
	tn := tree.MakeTableNameFromPrefix(prefix, tree.Name(name.Object()))
	name.SetAnnotation(&p.semaCtx.Annotations, &tn)
	table := desc.(*tabledesc.Mutable)

	// Ensure that the user can access the target schema.
	if err := p.canResolveDescUnderSchema(ctx, table.GetParentSchemaID(), table); err != nil {
		return nil, err
	}

	return table, nil
}

// See ResolveUncachedTableDescriptor.
func (p *planner) ResolveUncachedTableDescriptorEx(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType tree.RequiredTableKind,
) (table catalog.TableDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		table, err = p.ResolveExistingObjectEx(ctx, name, required, requiredType)
	})
	if err != nil {
		return nil, err
	}

	if table != nil {
		// Ensure that the user can access the target schema.
		if err := p.canResolveDescUnderSchema(ctx, table.GetParentSchemaID(), table); err != nil {
			return nil, err
		}
	}

	return table, nil
}

// See ResolveExistingTableObject.
func (p *planner) ResolveExistingObjectEx(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType tree.RequiredTableKind,
) (res catalog.TableDescriptor, err error) {
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags:    tree.CommonLookupFlags{Required: required},
		DesiredObjectKind:    tree.TableObject,
		DesiredTableDescKind: requiredType,
	}
	desc, prefix, err := resolver.ResolveExistingObject(ctx, p, name, lookupFlags)
	if err != nil || desc == nil {
		return nil, err
	}
	tn := tree.MakeTableNameFromPrefix(prefix, tree.Name(name.Object()))
	name.SetAnnotation(&p.semaCtx.Annotations, &tn)
	table := desc.(catalog.TableDescriptor)

	// Ensure that the user can access the target schema.
	if err := p.canResolveDescUnderSchema(ctx, table.GetParentSchemaID(), table); err != nil {
		return nil, err
	}

	return table, nil
}

// ResolvedName is a convenience wrapper for UnresolvedObjectName.Resolved.
func (p *planner) ResolvedName(u *tree.UnresolvedObjectName) tree.ObjectName {
	return u.Resolved(&p.semaCtx.Annotations)
}

type simpleSchemaResolver interface {
	getDatabaseByID(id descpb.ID) (catalog.DatabaseDescriptor, error)
	getSchemaByID(id descpb.ID) (catalog.SchemaDescriptor, error)
	getTableByID(id descpb.ID) (catalog.TableDescriptor, error)
}
