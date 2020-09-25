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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

var _ resolver.SchemaResolver = &planner{}

// ResolveUncachedDatabaseByName looks up a database name from the store.
func (p *planner) ResolveUncachedDatabaseByName(
	ctx context.Context, dbName string, required bool,
) (res *dbdesc.Immutable, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		var desc catalog.DatabaseDescriptor
		desc, err = p.LogicalSchemaAccessor().GetDatabaseDesc(
			ctx, p.txn, p.ExecCfg().Codec, dbName, p.CommonLookupFlags(required),
		)
		if desc != nil {
			res = desc.(*dbdesc.Immutable)
		}
	})
	return res, err
}

func (p *planner) ResolveMutableDatabaseDescriptor(
	ctx context.Context, name string, required bool,
) (*dbdesc.Mutable, error) {
	desc, err := p.LogicalSchemaAccessor().GetDatabaseDesc(
		ctx, p.txn, p.ExecCfg().Codec, name, tree.DatabaseLookupFlags{
			Required:       required,
			RequireMutable: true,
		})
	if err != nil || desc == nil {
		return nil, err
	}
	return desc.(*dbdesc.Mutable), nil
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
) (table *tabledesc.Immutable, err error) {
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
) (res catalog.DatabaseDescriptor, namePrefix tree.ObjectNamePrefix, err error) {
	var prefix *catalog.ResolvedObjectPrefix
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		prefix, namePrefix, err = resolver.ResolveTargetObject(ctx, p, un)
	})
	if err != nil {
		return nil, namePrefix, err
	}
	return prefix.Database, namePrefix, err
}

// LookupSchema implements the tree.ObjectNameTargetResolver interface.
func (p *planner) LookupSchema(
	ctx context.Context, dbName, scName string,
) (found bool, scMeta tree.SchemaMeta, err error) {
	sc := p.LogicalSchemaAccessor()
	dbDesc, err := sc.GetDatabaseDesc(ctx, p.txn, p.ExecCfg().Codec, dbName, p.CommonLookupFlags(false /* required */))
	if err != nil || dbDesc == nil {
		return false, nil, err
	}
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

// GetTypeDescriptor implements the descpb.TypeDescriptorResolver interface.
func (p *planner) GetTypeDescriptor(
	ctx context.Context, id descpb.ID,
) (tree.TypeName, catalog.TypeDescriptor, error) {
	desc, err := p.Descriptors().GetTypeVersionByID(ctx, p.txn, id, tree.ObjectLookupFlagsWithRequired())
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	dbDesc, err := p.Descriptors().GetDatabaseVersionByID(ctx, p.txn, desc.ParentID, p.CommonLookupFlags(true /* required */))
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	sc, err := p.Descriptors().ResolveSchemaByID(ctx, p.txn, desc.ParentSchemaID)
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	name := tree.MakeNewQualifiedTypeName(dbDesc.Name, sc.Name, desc.Name)
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
	tdesc := desc.(*typedesc.Immutable)

	// Disllow cross-database type resolution. Note that we check
	// p.contextDatabaseID != descpb.InvalidID when we have been restricted to
	// accessing types in the database with ID = p.contextDatabaseID by
	// p.runWithOptions. So, check to see if the resolved descriptor's parentID
	// matches, unless the descriptor's parentID is invalid. This could happen
	// when the type being resolved is a builtin type prefaced with a virtual
	// schema like `pg_catalog.int`. Resolution for these types returns a dummy
	// TypeDescriptor, so ignore those cases.
	if p.contextDatabaseID != descpb.InvalidID && tdesc.ParentID != descpb.InvalidID && tdesc.ParentID != p.contextDatabaseID {
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
			descriptor, err := p.ResolveMutableDatabaseDescriptor(ctx, string(database), true /*required*/)
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
		// Resolve the current database.
		curDB, err := p.ResolveMutableDatabaseDescriptor(ctx, p.CurrentDatabase(), true /* required */)
		if err != nil {
			return nil, err
		}
		for _, sc := range targets.Schemas {
			_, resSchema, err := p.ResolveMutableSchemaDescriptor(ctx, curDB.ID, sc, true /* required */)
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
		objectNames, err := expandTableGlob(ctx, p, tableGlob)
		if err != nil {
			return nil, err
		}

		for i := range objectNames {
			// We set required to false here, because there could be type names in
			// the returned set of names, so we don't want to error out if we
			// couldn't resolve a name into a table.
			descriptor, err := resolver.ResolveMutableExistingTableObject(ctx, p,
				&objectNames[i], false /* required */, tree.ResolveAnyTableKind)
			if err != nil {
				return nil, err
			}
			if descriptor != nil {
				descs = append(descs, descriptor)
			}
		}
	}
	if len(descs) == 0 {
		return nil, errNoMatch
	}
	return descs, nil
}

// getQualifiedTableName returns the database-qualified name of the table
// or view represented by the provided descriptor. It is a sort of
// reverse of the Resolve() functions.
func (p *planner) getQualifiedTableName(
	ctx context.Context, desc catalog.TableDescriptor,
) (*tree.TableName, error) {
	dbDesc, err := catalogkv.MustGetDatabaseDescByID(ctx, p.txn, p.ExecCfg().Codec, desc.GetParentID())
	if err != nil {
		return nil, err
	}
	schemaID := desc.GetParentSchemaID()
	schemaName, err := resolver.ResolveSchemaNameByID(ctx, p.txn, p.ExecCfg().Codec, desc.GetParentID(), schemaID)
	if err != nil {
		return nil, err
	}
	tbName := tree.MakeTableNameWithSchema(
		tree.Name(dbDesc.GetName()),
		tree.Name(schemaName),
		tree.Name(desc.GetName()),
	)
	return &tbName, nil
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

	tns, err := sa.GetObjectNames(ctx, txn, codec, dbDesc, scName,
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

		_, dropped, err := tableDesc.FindIndexByName(string(idxName))
		if err != nil || dropped {
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

	idx, _, err := cat.ResolveTableIndex(
		ctx, &catalog, cat.Flags{AvoidDescriptorCaches: true}, tableWithIndex,
	)
	if err != nil {
		return nil, nil, err
	}
	if err := catalog.CheckPrivilege(ctx, idx.Table(), privilege); err != nil {
		return nil, nil, err
	}
	optIdx := idx.(*optIndex)
	return tabledesc.NewExistingMutable(optIdx.tab.desc.TableDescriptor), optIdx.desc, nil
}

// expandTableGlob expands pattern into a list of objects represented
// as a tree.TableNames.
func expandTableGlob(
	ctx context.Context, p *planner, pattern tree.TablePattern,
) (tree.TableNames, error) {
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
		return true, &table.Immutable, nil
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
type internalLookupCtx struct {
	dbNames     map[descpb.ID]string
	dbIDs       []descpb.ID
	dbDescs     map[descpb.ID]*dbdesc.Immutable
	schemaDescs map[descpb.ID]*schemadesc.Immutable
	tbDescs     map[descpb.ID]*tabledesc.Immutable
	tbIDs       []descpb.ID
	typDescs    map[descpb.ID]*typedesc.Immutable
	typIDs      []descpb.ID
}

// tableLookupFn can be used to retrieve a table descriptor and its corresponding
// database descriptor using the table's ID.
type tableLookupFn = *internalLookupCtx

// newInternalLookupCtxFromDescriptors "unwraps" the descriptors into the
// appropriate implementation of Descriptor before constructing a new
// internalLookupCtx. It also hydrates any table descriptors with enum
// information. It is intended only for use when dealing with backups.
func newInternalLookupCtxFromDescriptors(
	ctx context.Context, rawDescs []descpb.Descriptor, prefix *dbdesc.Immutable,
) (*internalLookupCtx, error) {
	descriptors := make([]catalog.Descriptor, len(rawDescs))
	for i := range rawDescs {
		desc := &rawDescs[i]
		switch t := desc.Union.(type) {
		case *descpb.Descriptor_Database:
			descriptors[i] = dbdesc.NewImmutable(*t.Database)
		case *descpb.Descriptor_Table:
			descriptors[i] = tabledesc.NewImmutable(*t.Table)
		case *descpb.Descriptor_Type:
			descriptors[i] = typedesc.NewImmutable(*t.Type)
		case *descpb.Descriptor_Schema:
			descriptors[i] = schemadesc.NewImmutable(*t.Schema)
		}
	}
	lCtx := newInternalLookupCtx(ctx, descriptors, prefix)
	if err := descs.HydrateGivenDescriptors(ctx, descriptors); err != nil {
		return nil, err
	}
	return lCtx, nil
}

func newInternalLookupCtx(
	ctx context.Context, descs []catalog.Descriptor, prefix *dbdesc.Immutable,
) *internalLookupCtx {
	dbNames := make(map[descpb.ID]string)
	dbDescs := make(map[descpb.ID]*dbdesc.Immutable)
	schemaDescs := make(map[descpb.ID]*schemadesc.Immutable)
	tbDescs := make(map[descpb.ID]*tabledesc.Immutable)
	typDescs := make(map[descpb.ID]*typedesc.Immutable)
	var tbIDs, typIDs, dbIDs []descpb.ID
	// Record descriptors for name lookups.
	for i := range descs {
		switch desc := descs[i].(type) {
		case *dbdesc.Immutable:
			dbNames[desc.GetID()] = desc.GetName()
			dbDescs[desc.GetID()] = desc
			if prefix == nil || prefix.GetID() == desc.GetID() {
				dbIDs = append(dbIDs, desc.GetID())
			}
		case *tabledesc.Immutable:
			tbDescs[desc.GetID()] = desc
			if prefix == nil || prefix.GetID() == desc.ParentID {
				// Only make the table visible for iteration if the prefix was included.
				tbIDs = append(tbIDs, desc.ID)
			}
		case *typedesc.Immutable:
			typDescs[desc.GetID()] = desc
			if prefix == nil || prefix.GetID() == desc.ParentID {
				// Only make the type visible for iteration if the prefix was included.
				typIDs = append(typIDs, desc.GetID())
			}
		case *schemadesc.Immutable:
			schemaDescs[desc.GetID()] = desc
		}
	}

	return &internalLookupCtx{
		dbNames:     dbNames,
		dbDescs:     dbDescs,
		schemaDescs: schemaDescs,
		tbDescs:     tbDescs,
		typDescs:    typDescs,
		tbIDs:       tbIDs,
		dbIDs:       dbIDs,
		typIDs:      typIDs,
	}
}

func (l *internalLookupCtx) getDatabaseByID(id descpb.ID) (*dbdesc.Immutable, error) {
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

func (l *internalLookupCtx) getSchemaByID(id descpb.ID) (*schemadesc.Immutable, error) {
	sc, ok := l.schemaDescs[id]
	if !ok {
		return nil, sqlerrors.NewUndefinedSchemaError(fmt.Sprintf("[%d]", id))
	}
	return sc, nil
}

func (l *internalLookupCtx) getParentName(table catalog.TableDescriptor) string {
	parentName := l.dbNames[table.GetParentID()]
	if parentName == "" {
		// The parent database was deleted. This is possible e.g. when
		// a database is dropped with CASCADE, and someone queries
		// this virtual table before the dropped table descriptors are
		// effectively deleted.
		parentName = fmt.Sprintf("[%d]", table.GetParentID())
	}
	return parentName
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
		parentSchemaName = tree.Name(parentSchema.Name)
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
		parentSchemaName = tree.Name(parentSchema.Name)
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
		parentSchemaName = parentSchema.Name
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
	name.SetAnnotation(&p.semaCtx.Annotations, tn)

	if desc != nil {
		// Ensure that the user can access the target schema.
		if err := p.canResolveDescUnderSchema(ctx, desc.GetParentSchemaID(), desc); err != nil {
			return nil, err
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
) (table *tabledesc.Immutable, err error) {
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
) (res *tabledesc.Immutable, err error) {
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
	table := desc.(*tabledesc.Immutable)

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
	getDatabaseByID(id descpb.ID) (*dbdesc.Immutable, error)
	getSchemaByID(id descpb.ID) (*schemadesc.Immutable, error)
	getTableByID(id descpb.ID) (catalog.TableDescriptor, error)
}
