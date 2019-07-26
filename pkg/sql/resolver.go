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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	tree.TableNameExistingResolver
	tree.TableNameTargetResolver

	Txn() *client.Txn
	LogicalSchemaAccessor() SchemaAccessor
	CurrentDatabase() string
	CurrentSearchPath() sessiondata.SearchPath
	CommonLookupFlags(required bool) CommonLookupFlags
	ObjectLookupFlags(required bool, requireMutable bool) ObjectLookupFlags
	LookupTableByID(ctx context.Context, id sqlbase.ID) (row.TableEntry, error)
}

var _ SchemaResolver = &planner{}

// LogicalSchema encapsulates the interfaces needed to be able to both look up
// schema objects and also resolve permissions on them.
type LogicalSchema interface {
	SchemaResolver
	AuthorizationAccessor
}

var _ LogicalSchema = &planner{}

// ResolveUncachedDatabaseByName looks up a database name from the store.
func (p *planner) ResolveUncachedDatabaseByName(
	ctx context.Context, dbName string, required bool,
) (res *UncachedDatabaseDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		res, err = p.LogicalSchemaAccessor().GetDatabaseDesc(ctx, p.txn, dbName,
			p.CommonLookupFlags(required))
	})
	return res, err
}

// GetObjectNames retrieves the names of all objects in the target database/schema.
func GetObjectNames(
	ctx context.Context,
	txn *client.Txn,
	sc SchemaResolver,
	dbDesc *DatabaseDescriptor,
	scName string,
	explicitPrefix bool,
) (res TableNames, err error) {
	return sc.LogicalSchemaAccessor().GetObjectNames(ctx, txn, dbDesc, scName,
		DatabaseListFlags{
			CommonLookupFlags: sc.CommonLookupFlags(true /*required*/),
			explicitPrefix:    explicitPrefix,
		})
}

// ResolveExistingObject looks up an existing object.
// If required is true, an error is returned if the object does not exist.
// Optionally, if a desired descriptor type is specified, that type is checked.
//
// The object name is modified in-place with the result of the name
// resolution, if successful. It is not modified in case of error or
// if no object is found.
func ResolveExistingObject(
	ctx context.Context,
	sc SchemaResolver,
	tn *ObjectName,
	required bool,
	requiredType ResolveRequiredType,
) (res *ImmutableTableDescriptor, err error) {
	desc, err := resolveExistingObjectImpl(ctx, sc, tn, required, false /* requiredMutable */, requiredType)
	if err != nil || desc == nil {
		return nil, err
	}
	return desc.(*ImmutableTableDescriptor), nil
}

// ResolveMutableExistingObject looks up an existing mutable object.
// If required is true, an error is returned if the object does not exist.
// Optionally, if a desired descriptor type is specified, that type is checked.
//
// The object name is modified in-place with the result of the name
// resolution, if successful. It is not modified in case of error or
// if no object is found.
func ResolveMutableExistingObject(
	ctx context.Context,
	sc SchemaResolver,
	tn *ObjectName,
	required bool,
	requiredType ResolveRequiredType,
) (res *MutableTableDescriptor, err error) {
	desc, err := resolveExistingObjectImpl(ctx, sc, tn, required, true /* requiredMutable */, requiredType)
	if err != nil || desc == nil {
		return nil, err
	}
	return desc.(*MutableTableDescriptor), nil
}

func resolveExistingObjectImpl(
	ctx context.Context,
	sc SchemaResolver,
	tn *ObjectName,
	required bool,
	requiredMutable bool,
	requiredType ResolveRequiredType,
) (res tree.NameResolutionResult, err error) {
	found, descI, err := tn.ResolveExisting(ctx, sc, requiredMutable, sc.CurrentDatabase(), sc.CurrentSearchPath())
	if err != nil {
		return nil, err
	}
	if !found {
		if required {
			return nil, sqlbase.NewUndefinedRelationError(tn)
		}
		return nil, nil
	}
	obj := descI.(ObjectDescriptor)

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
		return nil, sqlbase.NewWrongObjectTypeError(tn, requiredTypeNames[requiredType])
	}

	if requiredMutable {
		return descI.(*MutableTableDescriptor), nil
	}

	return descI.(*ImmutableTableDescriptor), nil
}

// runWithOptions sets the provided resolution flags for the
// duration of the call of the passed argument fn.
//
// This is meant to be used like this (for example):
//
// var someVar T
// var err error
// p.runWithOptions(resolveFlags{skipCache: true}, func() {
//    someVar, err = ResolveExistingObject(ctx, p, ...)
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
	ctx context.Context, tn *ObjectName, required bool, requiredType ResolveRequiredType,
) (table *MutableTableDescriptor, err error) {
	return ResolveMutableExistingObject(ctx, p, tn, required, requiredType)
}

func (p *planner) ResolveUncachedTableDescriptor(
	ctx context.Context, tn *ObjectName, required bool, requiredType ResolveRequiredType,
) (table *ImmutableTableDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		table, err = ResolveExistingObject(ctx, p, tn, required, requiredType)
	})
	return table, err
}

// ResolveTargetObject determines a valid target path for an object
// that may not exist yet. It returns the descriptor for the database
// where the target object lives.
//
// The object name is modified in-place with the result of the name
// resolution.
func ResolveTargetObject(
	ctx context.Context, sc SchemaResolver, tn *ObjectName,
) (res *DatabaseDescriptor, err error) {
	found, descI, err := tn.ResolveTarget(ctx, sc, sc.CurrentDatabase(), sc.CurrentSearchPath())
	if err != nil {
		return nil, err
	}
	if !found {
		if !tn.ExplicitSchema && !tn.ExplicitCatalog {
			return nil, pgerror.New(pgcode.InvalidName, "no database specified")
		}
		err = pgerror.Newf(pgcode.InvalidSchemaName,
			"cannot create %q because the target database or schema does not exist",
			tree.ErrString(tn))
		err = errors.WithHint(err, "verify that the current database and search_path are valid and/or the target database exists")
		return nil, err
	}
	if tn.Schema() != tree.PublicSchema {
		return nil, pgerror.Newf(pgcode.InvalidName,
			"schema cannot be modified: %q", tree.ErrString(&tn.TableNamePrefix))
	}
	return descI.(*DatabaseDescriptor), nil
}

func (p *planner) ResolveUncachedDatabase(
	ctx context.Context, tn *ObjectName,
) (res *UncachedDatabaseDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		res, err = ResolveTargetObject(ctx, p, tn)
	})
	return res, err
}

// ResolveRequiredType can be passed to the ResolveExistingObject function to
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

// LookupSchema implements the tree.TableNameTargetResolver interface.
func (p *planner) LookupSchema(
	ctx context.Context, dbName, scName string,
) (found bool, scMeta tree.SchemaMeta, err error) {
	sc := p.LogicalSchemaAccessor()
	dbDesc, err := sc.GetDatabaseDesc(ctx, p.txn, dbName, p.CommonLookupFlags(false /*required*/))
	if err != nil || dbDesc == nil {
		return false, nil, err
	}
	return sc.IsValidSchema(dbDesc, scName), dbDesc, nil
}

// LookupObject implements the tree.TableNameExistingResolver interface.
func (p *planner) LookupObject(
	ctx context.Context, requireMutable bool, dbName, scName, tbName string,
) (found bool, objMeta tree.NameResolutionResult, err error) {
	sc := p.LogicalSchemaAccessor()
	p.tableName = tree.MakeTableNameWithSchema(tree.Name(dbName), tree.Name(scName), tree.Name(tbName))
	objDesc, err := sc.GetObjectDesc(ctx, p.txn, &p.tableName, p.ObjectLookupFlags(false /*required*/, requireMutable))
	return objDesc != nil, objDesc, err
}

func (p *planner) CommonLookupFlags(required bool) CommonLookupFlags {
	return CommonLookupFlags{
		required:    required,
		avoidCached: p.avoidCachedDescriptors,
	}
}

func (p *planner) ObjectLookupFlags(required, requireMutable bool) ObjectLookupFlags {
	return ObjectLookupFlags{
		CommonLookupFlags: p.CommonLookupFlags(required),
		requireMutable:    requireMutable,
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
		tableNames, err := expandTableGlob(ctx, p, tableGlob)
		if err != nil {
			return nil, err
		}
		for i := range tableNames {
			descriptor, err := ResolveMutableExistingObject(ctx, p, &tableNames[i], true, ResolveAnyDescType)
			if err != nil {
				return nil, err
			}
			descs = append(descs, descriptor)
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
	dbDesc, err := sqlbase.GetDatabaseDescFromID(ctx, p.txn, desc.ParentID)
	if err != nil {
		return "", err
	}
	tbName := tree.MakeTableName(tree.Name(dbDesc.Name), tree.Name(desc.Name))
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
	txn *client.Txn,
	sc SchemaResolver,
	dbName, scName string,
	idxName tree.UnrestrictedName,
	lookupFlags CommonLookupFlags,
) (result *tree.TableName, desc *MutableTableDescriptor, err error) {
	sa := sc.LogicalSchemaAccessor()
	dbDesc, err := sa.GetDatabaseDesc(ctx, txn, dbName, lookupFlags)
	if dbDesc == nil || err != nil {
		return nil, nil, err
	}

	tns, err := sa.GetObjectNames(ctx, txn, dbDesc, scName,
		DatabaseListFlags{CommonLookupFlags: lookupFlags, explicitPrefix: true})
	if err != nil {
		return nil, nil, err
	}

	result = nil
	for i := range tns {
		tn := &tns[i]
		tableDesc, err := ResolveMutableExistingObject(ctx, sc, tn, true, ResolveAnyDescType)
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
	if result == nil && lookupFlags.required {
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
		tn, desc, err = expandIndexName(ctx, p.txn, p, index, requireTable)
	})
	return tn, desc, err
}

func expandIndexName(
	ctx context.Context,
	txn *client.Txn,
	sc SchemaResolver,
	index *tree.TableIndexName,
	requireTable bool,
) (tn *tree.TableName, desc *MutableTableDescriptor, err error) {
	tn = &index.Table
	if tn.Table() != "" {
		// The index and its table prefix must exist already. Resolve the table.
		desc, err = ResolveMutableExistingObject(ctx, sc, tn, requireTable, ResolveRequireTableDesc)
		if err != nil {
			return nil, nil, err
		}
		return tn, desc, nil
	}

	// On the first call to expandMutableIndexName(), index.Table.Table() is empty.
	// Once the table name is resolved for the index below, index.Table
	// references the table name.

	// Look up the table prefix.
	found, _, err := tn.TableNamePrefix.Resolve(ctx, sc, sc.CurrentDatabase(), sc.CurrentSearchPath())
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
	foundTn, desc, err = findTableContainingIndex(ctx, txn, sc, tn.Catalog(), tn.Schema(), index.Index, lookupFlags)
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

// expandTableGlob expands pattern into a list of tables represented
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

// LookupObject implements the tree.TableNameExistingResolver interface.
func (r *fkSelfResolver) LookupObject(
	ctx context.Context, requireMutable bool, dbName, scName, tbName string,
) (found bool, objMeta tree.NameResolutionResult, err error) {
	if dbName == r.newTableName.Catalog() &&
		scName == r.newTableName.Schema() &&
		tbName == r.newTableName.Table() {
		table := r.newTableDesc
		if requireMutable {
			return true, sqlbase.NewMutableExistingTableDescriptor(*table), nil
		}
		return true, sqlbase.NewImmutableTableDescriptor(*table), nil
	}
	return r.SchemaResolver.LookupObject(ctx, requireMutable, dbName, scName, tbName)
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
	dbNames := make(map[sqlbase.ID]string)
	dbDescs := make(map[sqlbase.ID]*DatabaseDescriptor)
	tbDescs := make(map[sqlbase.ID]*TableDescriptor)
	var tbIDs, dbIDs []sqlbase.ID
	// Record database descriptors for name lookups.
	for _, desc := range descs {
		switch d := desc.(type) {
		case *sqlbase.DatabaseDescriptor:
			dbNames[d.ID] = d.Name
			dbDescs[d.ID] = d
			if prefix == nil || prefix.ID == d.ID {
				dbIDs = append(dbIDs, d.ID)
			}
		case *sqlbase.TableDescriptor:
			tbDescs[d.ID] = d
			if prefix == nil || prefix.ID == d.ParentID {
				// Only make the table visible for iteration if the prefix was included.
				tbIDs = append(tbIDs, d.ID)
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
	table, err := ResolveMutableExistingObject(ctx, p, &tn, required, requiredType)
	if err != nil {
		return nil, err
	}
	name.SetAnnotation(&p.semaCtx.Annotations, &tn)
	return table, nil
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

// See ResolveExistingObject.
func (p *planner) ResolveExistingObjectEx(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType ResolveRequiredType,
) (res *ImmutableTableDescriptor, err error) {
	tn := name.ToTableName()
	desc, err := resolveExistingObjectImpl(ctx, p, &tn, required, false /* requiredMutable */, requiredType)
	if err != nil || desc == nil {
		return nil, err
	}
	name.SetAnnotation(&p.semaCtx.Annotations, &tn)
	return desc.(*ImmutableTableDescriptor), nil
}

// ResolvedName is a convenience wrapper for UnresolvedObjectName.Resolved.
func (p *planner) ResolvedName(u *tree.UnresolvedObjectName) *tree.TableName {
	return u.Resolved(&p.semaCtx.Annotations)
}
