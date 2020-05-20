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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/schema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

	Txn() *kv.Txn
	LogicalSchemaAccessor() SchemaAccessor
	CurrentDatabase() string
	CurrentSearchPath() sessiondata.SearchPath
	CommonLookupFlags(required bool) tree.CommonLookupFlags
	ObjectLookupFlags(required bool, requireMutable bool) tree.ObjectLookupFlags
	LookupTableByID(ctx context.Context, id sqlbase.ID) (row.TableEntry, error)
}

var _ SchemaResolver = &planner{}

var errNoPrimaryKey = pgerror.Newf(pgcode.NoPrimaryKey,
	"requested table does not have a primary key")

// ResolveUncachedDatabaseByName looks up a database name from the store.
func (p *planner) ResolveUncachedDatabaseByName(
	ctx context.Context, dbName string, required bool,
) (res *UncachedDatabaseDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		res, err = p.LogicalSchemaAccessor().GetDatabaseDesc(
			ctx, p.txn, p.ExecCfg().Codec, dbName, p.CommonLookupFlags(required),
		)
	})
	return res, err
}

// GetObjectNames retrieves the names of all objects in the target database/
// schema. If explicitPrefix is set, the returned table names will have an
// explicit schema and catalog name.
func GetObjectNames(
	ctx context.Context,
	txn *kv.Txn,
	sc SchemaResolver,
	codec keys.SQLCodec,
	dbDesc *DatabaseDescriptor,
	scName string,
	explicitPrefix bool,
) (res TableNames, err error) {
	return sc.LogicalSchemaAccessor().GetObjectNames(ctx, txn, codec, dbDesc, scName,
		tree.DatabaseListFlags{
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
	ctx context.Context,
	sc SchemaResolver,
	tn *TableName,
	lookupFlags tree.ObjectLookupFlags,
	requiredType ResolveRequiredType,
) (res *ImmutableTableDescriptor, err error) {
	// TODO: As part of work for #34240, an UnresolvedObjectName should be
	//  passed as an argument to this function.
	un := tn.ToUnresolvedObjectName()
	desc, prefix, err := resolveExistingObjectImpl(ctx, sc, un, lookupFlags, requiredType)
	if err != nil || desc == nil {
		return nil, err
	}
	tn.ObjectNamePrefix = prefix
	return desc.(*ImmutableTableDescriptor), nil
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
	tn *TableName,
	required bool,
	requiredType ResolveRequiredType,
) (res *MutableTableDescriptor, err error) {
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{Required: required},
		RequireMutable:    true,
	}
	// TODO: As part of work for #34240, an UnresolvedObjectName should be
	//  passed as an argument to this function.
	un := tn.ToUnresolvedObjectName()
	desc, prefix, err := resolveExistingObjectImpl(ctx, sc, un, lookupFlags, requiredType)
	if err != nil || desc == nil {
		return nil, err
	}
	tn.ObjectNamePrefix = prefix
	return desc.(*MutableTableDescriptor), nil
}

// ResolveType implements the TypeReferenceResolver interface.
func (p *planner) ResolveType(name *tree.UnresolvedObjectName) (*types.T, error) {
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{Required: true},
		DesiredObjectKind: tree.TypeObject,
	}
	// TODO (rohany): The ResolveAnyDescType argument doesn't do anything here
	//  if we are looking for a type. This should be cleaned up.
	desc, prefix, err := resolveExistingObjectImpl(p.EvalContext().Context, p, name, lookupFlags, ResolveAnyDescType)
	if err != nil {
		return nil, err
	}
	tn := tree.MakeTypeNameFromPrefix(prefix, tree.Name(name.Object()))
	tdesc := desc.(*sqlbase.TypeDescriptor)
	// Hydrate the types.T from the resolved descriptor. Once we cache
	// descriptors, this hydration should install pointers to cached data.
	switch t := tdesc.Kind; t {
	case sqlbase.TypeDescriptor_ENUM:
		typ := types.MakeEnum(uint32(tdesc.ID))
		if err := tdesc.HydrateTypeInfo(typ); err != nil {
			return nil, err
		}
		// Override the hydrated name with the fully resolved type name.
		typ.TypeMeta.Name = &tn
		return typ, nil
	default:
		return nil, errors.AssertionFailedf("unknown type kind %s", t.String())
	}
}

// TODO (rohany): Once we start to cache type descriptors, this needs to
//  look into the set of leased copies.
// TODO (rohany): Once we lease types, this should be pushed down into the
//  leased object collection.
func (p *planner) getTypeDescByID(ctx context.Context, id sqlbase.ID) (*TypeDescriptor, error) {
	rawDesc, err := catalogkv.GetDescriptorByID(ctx, p.txn, p.ExecCfg().Codec, id)
	if err != nil {
		return nil, err
	}
	typDesc, ok := rawDesc.(*TypeDescriptor)
	if !ok {
		return nil, errors.AssertionFailedf("%s was not a type descriptor", rawDesc)
	}
	return typDesc, nil
}

// ResolveTypeByID implements the tree.TypeResolver interface. We disallow
// accessing types directly by their ID in standard SQL contexts, so error
// out nicely here.
// TODO (rohany): Is there a need to disable this in the general case?
func (p *planner) ResolveTypeByID(id uint32) (*types.T, error) {
	return nil, errors.Newf("type id reference @%d not allowed in this context", id)
}

// maybeHydrateTypesInDescriptor hydrates any types.T's in the input descriptor.
// TODO (rohany): Once we lease types, this should be pushed down into the
//  leased object collection.
func (p *planner) maybeHydrateTypesInDescriptor(
	ctx context.Context, objDesc tree.NameResolutionResult,
) error {
	// Helper method to hydrate the types within a TableDescriptor.
	hydrateDesc := func(desc *TableDescriptor) error {
		for i := range desc.Columns {
			col := &desc.Columns[i]
			if col.Type.UserDefined() {
				// Look up its type descriptor.
				typDesc, err := p.getTypeDescByID(ctx, sqlbase.ID(col.Type.StableTypeID()))
				if err != nil {
					return err
				}
				// TODO (rohany): This should be a noop if the hydrated type
				//  information present in the descriptor has the same version as
				//  the resolved type descriptor we found here.
				// TODO (rohany): Once types are leased we need to create a new
				//  ImmutableTableDescriptor when a type lease expires rather than
				//  overwriting the types information in the shared descriptor.
				if err := typDesc.HydrateTypeInfo(col.Type); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// As of now, only {Mutable,Immutable}TableDescriptor have types.T that
	// need to be hydrated.
	switch desc := objDesc.(type) {
	case *sqlbase.MutableTableDescriptor:
		if err := hydrateDesc(desc.TableDesc()); err != nil {
			return err
		}
	case *sqlbase.ImmutableTableDescriptor:
		if err := hydrateDesc(desc.TableDesc()); err != nil {
			return err
		}
	}
	return nil
}

func resolveExistingObjectImpl(
	ctx context.Context,
	sc SchemaResolver,
	un *tree.UnresolvedObjectName,
	lookupFlags tree.ObjectLookupFlags,
	requiredType ResolveRequiredType,
) (res tree.NameResolutionResult, prefix tree.ObjectNamePrefix, err error) {
	found, prefix, descI, err := tree.ResolveExisting(ctx, un, sc, lookupFlags, sc.CurrentDatabase(), sc.CurrentSearchPath())
	if err != nil {
		return nil, prefix, err
	}
	// Construct the resolved table name for use in error messages.
	resolvedTn := tree.MakeTableNameFromPrefix(prefix, tree.Name(un.Object()))
	if !found {
		if lookupFlags.Required {
			return nil, prefix, sqlbase.NewUndefinedObjectError(&resolvedTn, lookupFlags.DesiredObjectKind)
		}
		return nil, prefix, nil
	}

	obj := descI.(catalog.ObjectDescriptor)
	switch lookupFlags.DesiredObjectKind {
	case tree.TypeObject:
		if obj.TypeDesc() == nil {
			return nil, prefix, sqlbase.NewUndefinedTypeError(&resolvedTn)
		}
		return obj.TypeDesc(), prefix, nil
	case tree.TableObject:
		if obj.TableDesc() == nil {
			return nil, prefix, sqlbase.NewUndefinedRelationError(&resolvedTn)
		}
		goodType := true
		switch requiredType {
		case ResolveRequireTableDesc:
			goodType = obj.TableDesc().IsTable()
		case ResolveRequireViewDesc:
			goodType = obj.TableDesc().IsView()
		case ResolveRequireTableOrViewDesc:
			goodType = obj.TableDesc().IsTable() || obj.TableDesc().IsView()
		case ResolveRequireSequenceDesc:
			goodType = obj.TableDesc().IsSequence()
		}
		if !goodType {
			return nil, prefix, sqlbase.NewWrongObjectTypeError(&resolvedTn, requiredTypeNames[requiredType])
		}

		// If the table does not have a primary key, return an error
		// that the requested descriptor is invalid for use.
		if !lookupFlags.AllowWithoutPrimaryKey &&
			obj.TableDesc().IsTable() &&
			!obj.TableDesc().HasPrimaryKey() {
			return nil, prefix, errNoPrimaryKey
		}

		if lookupFlags.RequireMutable {
			return descI.(*MutableTableDescriptor), prefix, nil
		}

		return descI.(*ImmutableTableDescriptor), prefix, nil
	default:
		return nil, prefix, errors.AssertionFailedf(
			"unknown desired object kind %d", lookupFlags.DesiredObjectKind)
	}
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
	fn()
}

type resolveFlags struct {
	skipCache bool
}

func (p *planner) ResolveMutableTableDescriptor(
	ctx context.Context, tn *TableName, required bool, requiredType ResolveRequiredType,
) (table *MutableTableDescriptor, err error) {
	return ResolveMutableExistingTableObject(ctx, p, tn, required, requiredType)
}

func (p *planner) ResolveUncachedTableDescriptor(
	ctx context.Context, tn *TableName, required bool, requiredType ResolveRequiredType,
) (table *ImmutableTableDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		lookupFlags := tree.ObjectLookupFlags{CommonLookupFlags: tree.CommonLookupFlags{Required: required}}
		table, err = ResolveExistingTableObject(ctx, p, tn, lookupFlags, requiredType)
	})
	return table, err
}

// ResolveTargetObject determines a valid target path for an object
// that may not exist yet. It returns the descriptor for the database
// where the target object lives. It also returns the resolved name
// prefix for the input object.
func ResolveTargetObject(
	ctx context.Context, sc SchemaResolver, un *tree.UnresolvedObjectName,
) (*DatabaseDescriptor, tree.ObjectNamePrefix, error) {
	found, prefix, descI, err := tree.ResolveTarget(ctx, un, sc, sc.CurrentDatabase(), sc.CurrentSearchPath())
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
	if prefix.Schema() != tree.PublicSchema {
		return nil, prefix, pgerror.Newf(pgcode.InvalidName,
			"schema cannot be modified: %q", tree.ErrString(&prefix))
	}
	return descI.(*DatabaseDescriptor), prefix, nil
}

func (p *planner) ResolveUncachedDatabase(
	ctx context.Context, un *tree.UnresolvedObjectName,
) (res *UncachedDatabaseDescriptor, namePrefix tree.ObjectNamePrefix, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		res, namePrefix, err = ResolveTargetObject(ctx, p, un)
	})
	return res, namePrefix, err
}

// ResolveRequiredType can be passed to the ResolveExistingTableObject function to
// require the returned descriptor to be of a specific type.
type ResolveRequiredType int

// ResolveRequiredType options have descriptive names.
const (
	ResolveAnyDescType ResolveRequiredType = iota
	ResolveRequireTableDesc
	ResolveRequireViewDesc
	ResolveRequireTableOrViewDesc
	ResolveRequireSequenceDesc
)

var requiredTypeNames = [...]string{
	ResolveRequireTableDesc:       "table",
	ResolveRequireViewDesc:        "view",
	ResolveRequireTableOrViewDesc: "table or view",
	ResolveRequireSequenceDesc:    "sequence",
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
	found, _, err = sc.IsValidSchema(ctx, p.txn, p.ExecCfg().Codec, dbDesc.ID, scName)
	if err != nil {
		return false, nil, err
	}
	return found, dbDesc, nil
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

func (p *planner) CommonLookupFlags(required bool) tree.CommonLookupFlags {
	return tree.CommonLookupFlags{
		Required:    required,
		AvoidCached: p.avoidCachedDescriptors,
	}
}

func (p *planner) ObjectLookupFlags(required, requireMutable bool) tree.ObjectLookupFlags {
	return tree.ObjectLookupFlags{
		CommonLookupFlags: p.CommonLookupFlags(required),
		RequireMutable:    requireMutable,
	}
}

// getDescriptorsFromTargetList fetches the descriptors for the targets.
func getDescriptorsFromTargetList(
	ctx context.Context, p *planner, targets tree.TargetList,
) ([]sqlbase.DescriptorProto, error) {
	if targets.Databases != nil {
		if len(targets.Databases) == 0 {
			return nil, errNoDatabase
		}
		descs := make([]sqlbase.DescriptorProto, 0, len(targets.Databases))
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
	descs := make([]sqlbase.DescriptorProto, 0, len(targets.Tables))
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
			descriptor, err := ResolveMutableExistingTableObject(
				ctx, p, &objectNames[i], false /* required */, ResolveAnyDescType)
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
	schemaName, err := schema.ResolveNameByID(ctx, p.txn, p.ExecCfg().Codec, desc.ParentID, schemaID)
	if err != nil {
		return "", err
	}
	tbName := tree.MakeTableNameWithSchema(
		tree.Name(dbDesc.Name),
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
	sc SchemaResolver,
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
		tableDesc, err := ResolveMutableExistingTableObject(ctx, sc, tn, false /*required*/, ResolveAnyDescType)
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
	sc SchemaResolver,
	codec keys.SQLCodec,
	index *tree.TableIndexName,
	requireTable bool,
) (tn *tree.TableName, desc *MutableTableDescriptor, err error) {
	tn = &index.Table
	if tn.Table() != "" {
		// The index and its table prefix must exist already. Resolve the table.
		desc, err = ResolveMutableExistingTableObject(ctx, sc, tn, requireTable, ResolveRequireTableDesc)
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
	SchemaResolver
	newTableName *tree.TableName
	newTableDesc *sqlbase.TableDescriptor
}

var _ SchemaResolver = &fkSelfResolver{}

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
	dbNames map[sqlbase.ID]string
	dbIDs   []sqlbase.ID
	dbDescs map[sqlbase.ID]*DatabaseDescriptor
	tbDescs map[sqlbase.ID]*TableDescriptor
	tbIDs   []sqlbase.ID
}

// tableLookupFn can be used to retrieve a table descriptor and its corresponding
// database descriptor using the table's ID.
type tableLookupFn = *internalLookupCtx

func newInternalLookupCtx(
	descs []sqlbase.DescriptorProto, prefix *DatabaseDescriptor,
) *internalLookupCtx {
	wrappedDescs := make([]sqlbase.Descriptor, len(descs))
	for i, desc := range descs {
		wrappedDescs[i] = *sqlbase.WrapDescriptor(desc)
	}
	return newInternalLookupCtxFromDescriptors(wrappedDescs, prefix)
}

func newInternalLookupCtxFromDescriptors(
	descs []sqlbase.Descriptor, prefix *DatabaseDescriptor,
) *internalLookupCtx {
	dbNames := make(map[sqlbase.ID]string)
	dbDescs := make(map[sqlbase.ID]*DatabaseDescriptor)
	tbDescs := make(map[sqlbase.ID]*TableDescriptor)
	var tbIDs, dbIDs []sqlbase.ID
	// Record database descriptors for name lookups.
	for _, desc := range descs {
		if database := desc.GetDatabase(); database != nil {
			dbNames[database.ID] = database.Name
			dbDescs[database.ID] = database
			if prefix == nil || prefix.ID == database.ID {
				dbIDs = append(dbIDs, database.ID)
			}
		} else if table := desc.Table(hlc.Timestamp{}); table != nil {
			tbDescs[table.ID] = table
			if prefix == nil || prefix.ID == table.ParentID {
				// Only make the table visible for iteration if the prefix was included.
				tbIDs = append(tbIDs, table.ID)
			}
		}
	}
	return &internalLookupCtx{
		dbNames: dbNames,
		dbDescs: dbDescs,
		tbDescs: tbDescs,
		tbIDs:   tbIDs,
		dbIDs:   dbIDs,
	}
}

func (l *internalLookupCtx) getDatabaseByID(id sqlbase.ID) (*DatabaseDescriptor, error) {
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
	return tb, nil
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
	parentName = tree.MakeTableName(tree.Name(parentDbDesc.Name), tree.Name(parentTable.Name))
	parentName.ExplicitSchema = parentDbDesc.Name != dbPrefix
	return parentName, nil
}

// getTableAsTableName returns a TableName object for a given TableDescriptor.
func getTableAsTableName(
	l simpleSchemaResolver, table *sqlbase.TableDescriptor, dbPrefix string,
) (tree.TableName, error) {
	var tableName tree.TableName
	tableDbDesc, err := l.getDatabaseByID(table.ParentID)
	if err != nil {
		return tree.TableName{}, err
	}
	tableName = tree.MakeTableName(tree.Name(tableDbDesc.Name), tree.Name(table.Name))
	tableName.ExplicitSchema = tableDbDesc.Name != dbPrefix
	return tableName, nil
}

// The versions below are part of the work for #34240.
// TODO(radu): clean these up when everything is switched over.

// See ResolveMutableTableDescriptor.
func (p *planner) ResolveMutableTableDescriptorEx(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType ResolveRequiredType,
) (*MutableTableDescriptor, error) {
	tn := name.ToTableName()
	table, err := ResolveMutableExistingTableObject(ctx, p, &tn, required, requiredType)
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
	requiredType ResolveRequiredType,
) (*MutableTableDescriptor, error) {
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags:      tree.CommonLookupFlags{Required: required},
		RequireMutable:         true,
		AllowWithoutPrimaryKey: true,
	}
	desc, prefix, err := resolveExistingObjectImpl(ctx, p, name, lookupFlags, requiredType)
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
	requiredType ResolveRequiredType,
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
	requiredType ResolveRequiredType,
) (res *ImmutableTableDescriptor, err error) {
	lookupFlags := tree.ObjectLookupFlags{CommonLookupFlags: tree.CommonLookupFlags{Required: required}}
	desc, prefix, err := resolveExistingObjectImpl(ctx, p, name, lookupFlags, requiredType)
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
	getDatabaseByID(id sqlbase.ID) (*DatabaseDescriptor, error)
	getTableByID(id sqlbase.ID) (*TableDescriptor, error)
}
