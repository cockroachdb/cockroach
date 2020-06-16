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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

var _ resolver.SchemaResolver = &planner{}

// ResolveUncachedDatabaseByName looks up a database name from the store.
func (p *planner) ResolveUncachedDatabaseByName(
	ctx context.Context, dbName string, required bool,
) (res *sqlbase.ImmutableDatabaseDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		var desc sqlbase.DatabaseDescriptorInterface
		desc, err = p.LogicalSchemaAccessor().GetDatabaseDesc(
			ctx, p.txn, p.ExecCfg().Codec, dbName, p.CommonLookupFlags(required),
		)
		if desc != nil {
			res = desc.(*sqlbase.ImmutableDatabaseDescriptor)
		}
	})
	return res, err
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
	if flags.contextDatabaseID != sqlbase.InvalidID {
		defer func(prev sqlbase.ID) { p.contextDatabaseID = prev }(p.contextDatabaseID)
		p.contextDatabaseID = flags.contextDatabaseID
	}
	fn()
}

type resolveFlags struct {
	skipCache         bool
	contextDatabaseID sqlbase.ID
}

func (p *planner) ResolveMutableTableDescriptor(
	ctx context.Context, tn *TableName, required bool, requiredType resolver.ResolveRequiredType,
) (table *MutableTableDescriptor, err error) {
	return resolver.ResolveMutableExistingTableObject(ctx, p, tn, required, requiredType)
}

func (p *planner) ResolveUncachedTableDescriptor(
	ctx context.Context, tn *TableName, required bool, requiredType resolver.ResolveRequiredType,
) (table *ImmutableTableDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		lookupFlags := tree.ObjectLookupFlags{CommonLookupFlags: tree.CommonLookupFlags{Required: required}}
		table, err = resolver.ResolveExistingTableObject(ctx, p, tn, lookupFlags, requiredType)
	})
	return table, err
}

func (p *planner) ResolveUncachedDatabase(
	ctx context.Context, un *tree.UnresolvedObjectName,
) (res *UncachedDatabaseDescriptor, namePrefix tree.ObjectNamePrefix, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		res, namePrefix, err = resolver.ResolveTargetObject(ctx, p, un)
	})
	return res, namePrefix, err
}

// LookupSchema implements the tree.ObjectNameTargetResolver interface.
func (p *planner) LookupSchema(
	ctx context.Context, dbName, scName string,
) (found bool, scMeta tree.SchemaMeta, err error) {
	sc := p.LogicalSchemaAccessor()
	dbDesc, err := sc.GetDatabaseDesc(ctx, p.txn, p.ExecCfg().Codec, dbName, p.CommonLookupFlags(false /*required*/))
	if err != nil || dbDesc == nil {
		return false, nil, err
	}
	found, _, err = sc.IsValidSchema(ctx, p.txn, p.ExecCfg().Codec, dbDesc.GetID(), scName)
	if err != nil {
		return false, nil, err
	}
	return found, dbDesc.(*sqlbase.ImmutableDatabaseDescriptor), nil
}

// LookupObject implements the tree.ObjectNameExistingResolver interface.
func (p *planner) LookupObject(
	ctx context.Context, lookupFlags tree.ObjectLookupFlags, dbName, scName, tbName string,
) (found bool, objMeta tree.NameResolutionResult, err error) {
	sc := p.LogicalSchemaAccessor()
	lookupFlags.CommonLookupFlags = p.CommonLookupFlags(false /* required */)
	objDesc, err := sc.GetObjectDesc(ctx, p.txn, p.ExecCfg().Settings, p.ExecCfg().Codec, dbName, scName, tbName, lookupFlags)

	// The returned object may contain types.T that need hydrating.
	if objDesc != nil {
		if err := p.maybeHydrateTypesInDescriptor(ctx, objDesc); err != nil {
			return false, nil, err
		}
	}

	return objDesc != nil, objDesc, err
}

// CommonLookupFlags is part of the resolver.SchemaResolver interface.
func (p *planner) CommonLookupFlags(required bool) tree.CommonLookupFlags {
	return tree.CommonLookupFlags{
		Required:    required,
		AvoidCached: p.avoidCachedDescriptors,
	}
}

func (p *planner) makeTypeLookupFn(ctx context.Context) sqlbase.TypeLookupFunc {
	return func(id sqlbase.ID) (*tree.TypeName, sqlbase.TypeDescriptorInterface, error) {
		return resolver.ResolveTypeDescByID(ctx, p.txn, p.ExecCfg().Codec, id, tree.ObjectLookupFlags{})
	}
}

// ResolveType implements the TypeReferenceResolver interface.
func (p *planner) ResolveType(
	ctx context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{Required: true},
		DesiredObjectKind: tree.TypeObject,
		RequireMutable:    false,
	}
	// TODO (rohany): The ResolveAnyDescType argument doesn't do anything here
	//  if we are looking for a type. This should be cleaned up.
	desc, prefix, err := resolver.ResolveExistingObject(ctx, p, name, lookupFlags, resolver.ResolveAnyDescType)
	if err != nil {
		return nil, err
	}
	tn := tree.MakeTypeNameFromPrefix(prefix, tree.Name(name.Object()))
	tdesc := desc.(*sqlbase.ImmutableTypeDescriptor)

	// Disllow cross-database type resolution. Note that we check
	// p.contextDatabaseID != sqlbase.InvalidID when we have been restricted to
	// accessing types in the database with ID = p.contextDatabaseID by
	// p.runWithOptions. So, check to see if the resolved descriptor's parentID
	// matches, unless the descriptor's parentID is invalid. This could happen
	// when the type being resolved is a builtin type prefaced with a virtual
	// schema like `pg_catalog.int`. Resolution for these types returns a dummy
	// TypeDescriptor, so ignore those cases.
	if p.contextDatabaseID != sqlbase.InvalidID && tdesc.ParentID != sqlbase.InvalidID && tdesc.ParentID != p.contextDatabaseID {
		return nil, pgerror.Newf(
			pgcode.FeatureNotSupported, "cross database type references are not supported: %s", tn.String())
	}

	return tdesc.MakeTypesT(&tn, p.makeTypeLookupFn(ctx))
}

// ResolveTypeByID implements the tree.TypeResolver interface.
func (p *planner) ResolveTypeByID(ctx context.Context, id uint32) (*types.T, error) {
	name, desc, err := resolver.ResolveTypeDescByID(
		ctx,
		p.txn,
		p.ExecCfg().Codec,
		sqlbase.ID(id),
		tree.ObjectLookupFlags{
			CommonLookupFlags: tree.CommonLookupFlags{Required: true},
		},
	)
	if err != nil {
		return nil, err
	}
	return desc.MakeTypesT(name, p.makeTypeLookupFn(ctx))
}

// maybeHydrateTypesInDescriptor hydrates any types.T's in the input descriptor.
// TODO (rohany): Once we lease types, this should be pushed down into the
//  leased object collection.
func (p *planner) maybeHydrateTypesInDescriptor(
	ctx context.Context, objDesc tree.NameResolutionResult,
) error {
	// As of now, only {Mutable,Immutable}TableDescriptor have types.T that
	// need to be hydrated.
	tableDesc := objDesc.(catalog.Descriptor).TableDesc()
	if tableDesc == nil {
		return nil
	}
	return sqlbase.HydrateTypesInTableDescriptor(tableDesc, p.makeTypeLookupFn(ctx))
}

// ObjectLookupFlags is part of the resolver.SchemaResolver interface.
func (p *planner) ObjectLookupFlags(required, requireMutable bool) tree.ObjectLookupFlags {
	return tree.ObjectLookupFlags{
		CommonLookupFlags: p.CommonLookupFlags(required),
		RequireMutable:    requireMutable,
	}
}

// getDescriptorsFromTargetList fetches the descriptors for the targets.
func getDescriptorsFromTargetList(
	ctx context.Context, p *planner, targets tree.TargetList,
) ([]sqlbase.DescriptorInterface, error) {
	if targets.Databases != nil {
		if len(targets.Databases) == 0 {
			return nil, errNoDatabase
		}
		descs := make([]sqlbase.DescriptorInterface, 0, len(targets.Databases))
		for _, database := range targets.Databases {
			descriptor, err := p.ResolveUncachedDatabaseByName(ctx, string(database), true /*required*/)
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

	if len(targets.Tables) == 0 {
		return nil, errNoTable
	}
	descs := make([]sqlbase.DescriptorInterface, 0, len(targets.Tables))
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
				&objectNames[i], false /* required */, resolver.ResolveAnyDescType)
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
	ctx context.Context, desc *sqlbase.TableDescriptor,
) (string, error) {
	dbDesc, err := sqlbase.GetDatabaseDescFromID(ctx, p.txn, p.ExecCfg().Codec, desc.ParentID)
	if err != nil {
		return "", err
	}
	schemaID := desc.GetParentSchemaID()
	schemaName, err := resolver.ResolveSchemaNameByID(ctx, p.txn, p.ExecCfg().Codec, desc.ParentID, schemaID)
	if err != nil {
		return "", err
	}
	tbName := tree.MakeTableNameWithSchema(
		tree.Name(dbDesc.GetName()),
		tree.Name(schemaName),
		tree.Name(desc.Name),
	)
	return tbName.String(), nil
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
) (result *tree.TableName, desc *MutableTableDescriptor, err error) {
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
		tableDesc, err := resolver.ResolveMutableExistingTableObject(ctx, sc, tn, false /*required*/, resolver.ResolveAnyDescType)
		if err != nil {
			return nil, nil, err
		}
		if tableDesc == nil || !tableDesc.IsTable() {
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
) (tn *tree.TableName, desc *MutableTableDescriptor, err error) {
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
) (tn *tree.TableName, desc *MutableTableDescriptor, err error) {
	tn = &index.Table
	if tn.Table() != "" {
		// The index and its table prefix must exist already. Resolve the table.
		desc, err = resolver.ResolveMutableExistingTableObject(ctx, sc, tn, requireTable, resolver.ResolveRequireTableDesc)
		if err != nil {
			return nil, nil, err
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
) (*MutableTableDescriptor, *sqlbase.IndexDescriptor, error) {
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
	return sqlbase.NewMutableExistingTableDescriptor(optIdx.tab.desc.TableDescriptor), optIdx.desc, nil
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
	newTableDesc *sqlbase.TableDescriptor
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
			return true, sqlbase.NewMutableExistingTableDescriptor(*table), nil
		}
		return true, sqlbase.NewImmutableTableDescriptor(*table), nil
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
	dbNames  map[sqlbase.ID]string
	dbIDs    []sqlbase.ID
	dbDescs  map[sqlbase.ID]*sqlbase.ImmutableDatabaseDescriptor
	tbDescs  map[sqlbase.ID]*ImmutableTableDescriptor
	tbIDs    []sqlbase.ID
	typDescs map[sqlbase.ID]*sqlbase.ImmutableTypeDescriptor
	typIDs   []sqlbase.ID
}

// tableLookupFn can be used to retrieve a table descriptor and its corresponding
// database descriptor using the table's ID.
type tableLookupFn = *internalLookupCtx

// newInternalLookupCtxFromDescriptors "unwraps" the descriptors into the
// appropriate implementation of DescriptorInterface before constructing a
// new internalLookupCtx. It is intended only for use when dealing with backups.
func newInternalLookupCtxFromDescriptors(
	rawDescs []sqlbase.Descriptor, prefix *sqlbase.ImmutableDatabaseDescriptor,
) *internalLookupCtx {
	descs := make([]sqlbase.DescriptorInterface, len(rawDescs))
	for i := range rawDescs {
		desc := &rawDescs[i]
		switch t := desc.Union.(type) {
		case *sqlbase.Descriptor_Database:
			descs[i] = sqlbase.NewImmutableDatabaseDescriptor(*t.Database)
		case *sqlbase.Descriptor_Table:
			descs[i] = sqlbase.NewImmutableTableDescriptor(*t.Table)
		case *sqlbase.Descriptor_Type:
			descs[i] = sqlbase.NewImmutableTypeDescriptor(*t.Type)
		case *sqlbase.Descriptor_Schema:
			descs[i] = sqlbase.NewImmutableSchemaDescriptor(*t.Schema)
		}
	}
	return newInternalLookupCtx(descs, prefix)
}

func newInternalLookupCtx(
	descs []sqlbase.DescriptorInterface, prefix *sqlbase.ImmutableDatabaseDescriptor,
) *internalLookupCtx {
	dbNames := make(map[sqlbase.ID]string)
	dbDescs := make(map[sqlbase.ID]*sqlbase.ImmutableDatabaseDescriptor)
	tbDescs := make(map[sqlbase.ID]*ImmutableTableDescriptor)
	typDescs := make(map[sqlbase.ID]*sqlbase.ImmutableTypeDescriptor)
	var tbIDs, typIDs, dbIDs []sqlbase.ID
	// Record database descriptors for name lookups.
	for i := range descs {
		switch desc := descs[i].(type) {
		case *sqlbase.ImmutableDatabaseDescriptor:
			dbNames[desc.GetID()] = desc.GetName()
			dbDescs[desc.GetID()] = desc
			if prefix == nil || prefix.GetID() == desc.GetID() {
				dbIDs = append(dbIDs, desc.GetID())
			}
		case *sqlbase.ImmutableTableDescriptor:
			tbDescs[desc.GetID()] = desc
			if prefix == nil || prefix.GetID() == desc.ParentID {
				// Only make the table visible for iteration if the prefix was included.
				tbIDs = append(tbIDs, desc.ID)
			}
		case *sqlbase.ImmutableTypeDescriptor:
			typDescs[desc.GetID()] = desc
			if prefix == nil || prefix.GetID() == desc.ParentID {
				// Only make the type visible for iteration if the prefix was included.
				typIDs = append(typIDs, desc.GetID())
			}
		}
	}
	return &internalLookupCtx{
		dbNames:  dbNames,
		dbDescs:  dbDescs,
		tbDescs:  tbDescs,
		typDescs: typDescs,
		tbIDs:    tbIDs,
		dbIDs:    dbIDs,
		typIDs:   typIDs,
	}
}

func (l *internalLookupCtx) getDatabaseByID(
	id sqlbase.ID,
) (*sqlbase.ImmutableDatabaseDescriptor, error) {
	db, ok := l.dbDescs[id]
	if !ok {
		return nil, sqlbase.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", id))
	}
	return db, nil
}

func (l *internalLookupCtx) getTableByID(id sqlbase.ID) (*TableDescriptor, error) {
	tb, ok := l.tbDescs[id]
	if !ok {
		return nil, sqlbase.NewUndefinedRelationError(
			tree.NewUnqualifiedTableName(tree.Name(fmt.Sprintf("[%d]", id))))
	}
	return tb.TableDesc(), nil
}

func (l *internalLookupCtx) getParentName(table *TableDescriptor) string {
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
	l simpleSchemaResolver, parentTableID sqlbase.ID, dbPrefix string,
) (tree.TableName, error) {
	var parentName tree.TableName
	parentTable, err := l.getTableByID(parentTableID)
	if err != nil {
		return tree.TableName{}, err
	}
	parentDbDesc, err := l.getDatabaseByID(parentTable.ParentID)
	if err != nil {
		return tree.TableName{}, err
	}
	parentName = tree.MakeTableName(tree.Name(parentDbDesc.GetName()), tree.Name(parentTable.Name))
	parentName.ExplicitSchema = parentDbDesc.GetName() != dbPrefix
	return parentName, nil
}

// getTableAsTableName returns a TableName object for a given TableDescriptor.
func getTableAsTableName(
	l simpleSchemaResolver, table *sqlbase.ImmutableTableDescriptor, dbPrefix string,
) (tree.TableName, error) {
	var tableName tree.TableName
	tableDbDesc, err := l.getDatabaseByID(table.ParentID)
	if err != nil {
		return tree.TableName{}, err
	}
	tableName = tree.MakeTableName(tree.Name(tableDbDesc.GetName()), tree.Name(table.Name))
	tableName.ExplicitSchema = tableDbDesc.GetName() != dbPrefix
	return tableName, nil
}

// The versions below are part of the work for #34240.
// TODO(radu): clean these up when everything is switched over.

// See ResolveMutableTableDescriptor.
func (p *planner) ResolveMutableTableDescriptorEx(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType resolver.ResolveRequiredType,
) (*MutableTableDescriptor, error) {
	tn := name.ToTableName()
	table, err := resolver.ResolveMutableExistingTableObject(ctx, p, &tn, required, requiredType)
	if err != nil {
		return nil, err
	}
	name.SetAnnotation(&p.semaCtx.Annotations, &tn)
	return table, nil
}

// ResolveMutableTableDescriptorExAllowNoPrimaryKey performs the
// same logic as ResolveMutableTableDescriptorEx but allows for
// the resolved table to not have a primary key.
func (p *planner) ResolveMutableTableDescriptorExAllowNoPrimaryKey(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType resolver.ResolveRequiredType,
) (*MutableTableDescriptor, error) {
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags:      tree.CommonLookupFlags{Required: required},
		RequireMutable:         true,
		AllowWithoutPrimaryKey: true,
	}
	desc, prefix, err := resolver.ResolveExistingObject(ctx, p, name, lookupFlags, requiredType)
	if err != nil || desc == nil {
		return nil, err
	}
	tn := tree.MakeTableNameFromPrefix(prefix, tree.Name(name.Object()))
	name.SetAnnotation(&p.semaCtx.Annotations, &tn)
	return desc.(*MutableTableDescriptor), nil
}

// See ResolveUncachedTableDescriptor.
func (p *planner) ResolveUncachedTableDescriptorEx(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType resolver.ResolveRequiredType,
) (table *ImmutableTableDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		table, err = p.ResolveExistingObjectEx(ctx, name, required, requiredType)
	})
	return table, err
}

// See ResolveExistingTableObject.
func (p *planner) ResolveExistingObjectEx(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType resolver.ResolveRequiredType,
) (res *ImmutableTableDescriptor, err error) {
	lookupFlags := tree.ObjectLookupFlags{CommonLookupFlags: tree.CommonLookupFlags{Required: required}}
	desc, prefix, err := resolver.ResolveExistingObject(ctx, p, name, lookupFlags, requiredType)
	if err != nil || desc == nil {
		return nil, err
	}
	tn := tree.MakeTableNameFromPrefix(prefix, tree.Name(name.Object()))
	name.SetAnnotation(&p.semaCtx.Annotations, &tn)
	return desc.(*ImmutableTableDescriptor), nil
}

// ResolvedName is a convenience wrapper for UnresolvedObjectName.Resolved.
func (p *planner) ResolvedName(u *tree.UnresolvedObjectName) tree.ObjectName {
	return u.Resolved(&p.semaCtx.Annotations)
}

type simpleSchemaResolver interface {
	getDatabaseByID(id sqlbase.ID) (*sqlbase.ImmutableDatabaseDescriptor, error)
	getTableByID(id sqlbase.ID) (*TableDescriptor, error)
}
