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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
		var desc sqlbase.DatabaseDescriptor
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
	ctx context.Context, tn *TableName, required bool, requiredType tree.RequiredTableKind,
) (table *MutableTableDescriptor, err error) {
	return resolver.ResolveMutableExistingTableObject(ctx, p, tn, required, requiredType)
}

func (p *planner) ResolveUncachedTableDescriptor(
	ctx context.Context, tn *TableName, required bool, requiredType tree.RequiredTableKind,
) (table *ImmutableTableDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		lookupFlags := tree.ObjectLookupFlags{
			CommonLookupFlags:    tree.CommonLookupFlags{Required: required},
			DesiredObjectKind:    tree.TableObject,
			DesiredTableDescKind: requiredType,
		}
		table, err = resolver.ResolveExistingTableObject(ctx, p, tn, lookupFlags)
	})
	return table, err
}

func (p *planner) ResolveUncachedDatabase(
	ctx context.Context, un *tree.UnresolvedObjectName,
) (res *UncachedDatabaseDescriptor, namePrefix tree.ObjectNamePrefix, err error) {
	var prefix *catalog.ResolvedObjectPrefix
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		prefix, namePrefix, err = resolver.ResolveTargetObject(ctx, p, un)
	})
	return prefix.Database, namePrefix, err
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
	var resolvedSchema sqlbase.ResolvedSchema
	found, resolvedSchema, err = sc.GetSchema(ctx, p.txn, p.ExecCfg().Codec, dbDesc.GetID(), scName)
	if err != nil {
		return false, nil, err
	}
	return found, &catalog.ResolvedObjectPrefix{
		Database: dbDesc.(*sqlbase.ImmutableDatabaseDescriptor),
		Schema:   resolvedSchema,
	}, nil
}

// LookupObject implements the tree.ObjectNameExistingResolver interface.
func (p *planner) LookupObject(
	ctx context.Context, lookupFlags tree.ObjectLookupFlags, dbName, scName, tbName string,
) (found bool, objMeta tree.NameResolutionResult, err error) {
	sc := p.LogicalSchemaAccessor()
	lookupFlags.CommonLookupFlags = p.CommonLookupFlags(false /* required */)
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
) (tree.TypeName, sqlbase.TypeDescriptor, error) {
	desc, err := p.Descriptors().GetTypeVersionByID(ctx, p.txn, id, tree.ObjectLookupFlagsWithRequired())
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	// TODO (lucy): This database access should go through the collection.
	//  When I try to use the DatabaseCache() here, a nil pointer deref occurs.
	dbDesc, err := catalogkv.MustGetDatabaseDescByID(ctx, p.txn, p.ExecCfg().Codec, desc.ParentID)
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	// TODO (rohany): Update this once user defined schemas exist.
	name := tree.MakeNewQualifiedTypeName(dbDesc.Name, tree.PublicSchema, desc.Name)
	return name, desc, nil
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
	desc, prefix, err := resolver.ResolveExistingObject(ctx, p, name, lookupFlags)
	if err != nil {
		return nil, err
	}
	tn := tree.MakeNewQualifiedTypeName(prefix.Catalog(), prefix.Schema(), name.Object())
	tdesc := desc.(*sqlbase.ImmutableTypeDescriptor)

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

	return tdesc.MakeTypesT(ctx, &tn, p)
}

// ResolveTypeByID implements the tree.TypeResolver interface.
func (p *planner) ResolveTypeByID(ctx context.Context, id uint32) (*types.T, error) {
	name, desc, err := p.GetTypeDescriptor(ctx, descpb.ID(id))
	if err != nil {
		return nil, err
	}
	return desc.MakeTypesT(ctx, &name, p)
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
) ([]sqlbase.Descriptor, error) {
	if targets.Databases != nil {
		if len(targets.Databases) == 0 {
			return nil, errNoDatabase
		}
		descs := make([]sqlbase.Descriptor, 0, len(targets.Databases))
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

	if targets.Types != nil {
		if len(targets.Types) == 0 {
			return nil, errNoType
		}
		descs := make([]sqlbase.Descriptor, 0, len(targets.Types))
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

	if len(targets.Tables) == 0 {
		return nil, errNoTable
	}
	descs := make([]sqlbase.Descriptor, 0, len(targets.Tables))
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
	ctx context.Context, desc sqlbase.TableDescriptor,
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
		tableDesc, err := resolver.ResolveMutableExistingTableObject(ctx, sc, tn, false /*required*/, tree.ResolveAnyTableKind)
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
		desc, err = resolver.ResolveMutableExistingTableObject(ctx, sc, tn, requireTable, tree.ResolveRequireTableDesc)
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
) (*MutableTableDescriptor, *descpb.IndexDescriptor, error) {
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
	newTableDesc *descpb.TableDescriptor
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
	dbNames     map[descpb.ID]string
	dbIDs       []descpb.ID
	dbDescs     map[descpb.ID]*sqlbase.ImmutableDatabaseDescriptor
	schemaDescs map[descpb.ID]*sqlbase.ImmutableSchemaDescriptor
	tbDescs     map[descpb.ID]*ImmutableTableDescriptor
	tbIDs       []descpb.ID
	typDescs    map[descpb.ID]*sqlbase.ImmutableTypeDescriptor
	typIDs      []descpb.ID
}

// tableLookupFn can be used to retrieve a table descriptor and its corresponding
// database descriptor using the table's ID.
type tableLookupFn = *internalLookupCtx

// newInternalLookupCtxFromDescriptors "unwraps" the descriptors into the
// appropriate implementation of Descriptor before constructing a
// new internalLookupCtx. It is intended only for use when dealing with backups.
func newInternalLookupCtxFromDescriptors(
	rawDescs []descpb.Descriptor, prefix *sqlbase.ImmutableDatabaseDescriptor,
) *internalLookupCtx {
	descs := make([]sqlbase.Descriptor, len(rawDescs))
	for i := range rawDescs {
		desc := &rawDescs[i]
		switch t := desc.Union.(type) {
		case *descpb.Descriptor_Database:
			descs[i] = sqlbase.NewImmutableDatabaseDescriptor(*t.Database)
		case *descpb.Descriptor_Table:
			descs[i] = sqlbase.NewImmutableTableDescriptor(*t.Table)
		case *descpb.Descriptor_Type:
			descs[i] = sqlbase.NewImmutableTypeDescriptor(*t.Type)
		case *descpb.Descriptor_Schema:
			descs[i] = sqlbase.NewImmutableSchemaDescriptor(*t.Schema)
		}
	}
	return newInternalLookupCtx(descs, prefix)
}

func newInternalLookupCtx(
	descs []sqlbase.Descriptor, prefix *sqlbase.ImmutableDatabaseDescriptor,
) *internalLookupCtx {
	dbNames := make(map[descpb.ID]string)
	dbDescs := make(map[descpb.ID]*sqlbase.ImmutableDatabaseDescriptor)
	schemaDescs := make(map[descpb.ID]*sqlbase.ImmutableSchemaDescriptor)
	tbDescs := make(map[descpb.ID]*ImmutableTableDescriptor)
	typDescs := make(map[descpb.ID]*sqlbase.ImmutableTypeDescriptor)
	var tbIDs, typIDs, dbIDs []descpb.ID
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
		case *sqlbase.ImmutableSchemaDescriptor:
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

func (l *internalLookupCtx) getDatabaseByID(
	id descpb.ID,
) (*sqlbase.ImmutableDatabaseDescriptor, error) {
	db, ok := l.dbDescs[id]
	if !ok {
		return nil, sqlbase.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", id))
	}
	return db, nil
}

func (l *internalLookupCtx) getTableByID(id descpb.ID) (sqlbase.TableDescriptor, error) {
	tb, ok := l.tbDescs[id]
	if !ok {
		return nil, sqlbase.NewUndefinedRelationError(
			tree.NewUnqualifiedTableName(tree.Name(fmt.Sprintf("[%d]", id))))
	}
	return tb, nil
}

func (l *internalLookupCtx) getSchemaByID(
	id descpb.ID,
) (*sqlbase.ImmutableSchemaDescriptor, error) {
	sc, ok := l.schemaDescs[id]
	if !ok {
		return nil, sqlbase.NewUndefinedSchemaError(fmt.Sprintf("[%d]", id))
	}
	return sc, nil
}

func (l *internalLookupCtx) getParentName(table sqlbase.TableDescriptor) string {
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

// getTableNameFromTableDescriptor returns a TableName object for a given TableDescriptor.
func getTableNameFromTableDescriptor(
	l simpleSchemaResolver, table sqlbase.TableDescriptor, dbPrefix string,
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

// ResolveMutableTypeDescriptor resolves a type descriptor for mutable access.
func (p *planner) ResolveMutableTypeDescriptor(
	ctx context.Context, name *tree.UnresolvedObjectName, required bool,
) (*sqlbase.MutableTypeDescriptor, error) {
	tn, desc, err := resolver.ResolveMutableType(ctx, p, name, required)
	if err != nil {
		return nil, err
	}
	name.SetAnnotation(&p.semaCtx.Annotations, tn)
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
	requiredType tree.RequiredTableKind,
) (*MutableTableDescriptor, error) {
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags:      tree.CommonLookupFlags{Required: required},
		RequireMutable:         true,
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
	return desc.(*MutableTableDescriptor), nil
}

// See ResolveUncachedTableDescriptor.
func (p *planner) ResolveUncachedTableDescriptorEx(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType tree.RequiredTableKind,
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
	requiredType tree.RequiredTableKind,
) (res *ImmutableTableDescriptor, err error) {
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
	return desc.(*ImmutableTableDescriptor), nil
}

// ResolvedName is a convenience wrapper for UnresolvedObjectName.Resolved.
func (p *planner) ResolvedName(u *tree.UnresolvedObjectName) tree.ObjectName {
	return u.Resolved(&p.semaCtx.Annotations)
}

type simpleSchemaResolver interface {
	getDatabaseByID(id descpb.ID) (*sqlbase.ImmutableDatabaseDescriptor, error)
	getSchemaByID(id descpb.ID) (*sqlbase.ImmutableSchemaDescriptor, error)
	getTableByID(id descpb.ID) (sqlbase.TableDescriptor, error)
}
