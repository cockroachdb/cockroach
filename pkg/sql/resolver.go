// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	CommonLookupFlags(ctx context.Context, required bool) CommonLookupFlags
	ObjectLookupFlags(ctx context.Context, required bool) ObjectLookupFlags
	LookupTableByID(ctx context.Context, id sqlbase.ID) (row.TableLookup, error)
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
		res, err = p.LogicalSchemaAccessor().GetDatabaseDesc(dbName,
			p.CommonLookupFlags(ctx, required))
	})
	return res, err
}

// GetObjectNames retrieves the names of all objects in the target database/schema.
func GetObjectNames(
	ctx context.Context,
	sc SchemaResolver,
	dbDesc *DatabaseDescriptor,
	scName string,
	explicitPrefix bool,
) (res TableNames, err error) {
	return sc.LogicalSchemaAccessor().GetObjectNames(dbDesc, scName,
		DatabaseListFlags{
			CommonLookupFlags: sc.CommonLookupFlags(ctx, true /*required*/),
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
	ctx context.Context, sc SchemaResolver, tn *ObjectName, required bool, requiredType requiredType,
) (res *TableDescriptor, err error) {
	desc, err := resolveExistingObjectImpl(ctx, sc, tn, required, false /* requiredMutable */, requiredType)
	if err != nil || desc == nil {
		return nil, err
	}
	return desc.(*TableDescriptor), nil
}

// ResolveMutableExistingObject looks up an existing mutable object.
// If required is true, an error is returned if the object does not exist.
// Optionally, if a desired descriptor type is specified, that type is checked.
//
// The object name is modified in-place with the result of the name
// resolution, if successful. It is not modified in case of error or
// if no object is found.
func ResolveMutableExistingObject(
	ctx context.Context, sc SchemaResolver, tn *ObjectName, required bool, requiredType requiredType,
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
	requiredType requiredType,
) (res tree.NameResolutionResult, err error) {
	found, descI, err := tn.ResolveExisting(ctx, sc, sc.CurrentDatabase(), sc.CurrentSearchPath())
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
	case requireTableDesc:
		goodType = obj.TableDesc().IsTable()
	case requireViewDesc:
		goodType = obj.TableDesc().IsView()
	case requireTableOrViewDesc:
		goodType = obj.TableDesc().IsTable() || obj.TableDesc().IsView()
	case requireSequenceDesc:
		goodType = obj.TableDesc().IsSequence()
	}
	if !goodType {
		return nil, sqlbase.NewWrongObjectTypeError(tn, requiredTypeNames[requiredType])
	}

	if requiredMutable {
		if mutDesc, ok := descI.(*MutableTableDescriptor); ok {
			return mutDesc, nil
		}
		tbl := *descI.(*TableDescriptor)
		return NewMutableTableDescriptor(tbl, tbl), nil
	}

	return obj.TableDesc(), nil
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
	ctx context.Context, tn *ObjectName, required bool, requiredType requiredType,
) (table *MutableTableDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		table, err = ResolveMutableExistingObject(ctx, p, tn, required, requiredType)
	})
	return table, err
}

func (p *planner) ResolveUncachedTableDescriptor(
	ctx context.Context, tn *ObjectName, required bool, requiredType requiredType,
) (table *UncachedTableDescriptor, err error) {
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
		return nil, pgerror.NewErrorf(pgerror.CodeInvalidSchemaNameError,
			"cannot create %q because the target database or schema does not exist",
			tree.ErrString(tn)).SetHintf("verify that the current database and search_path are valid and/or the target database exists")
	}
	if tn.Schema() != tree.PublicSchema {
		return nil, pgerror.NewErrorf(pgerror.CodeInvalidNameError,
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

// requiredType can be passed to the ResolveExistingObject function to
// require the returned descriptor to be of a specific type.
type requiredType int

const (
	anyDescType requiredType = iota
	requireTableDesc
	requireViewDesc
	requireTableOrViewDesc
	requireSequenceDesc
)

var requiredTypeNames = [...]string{
	requireTableDesc:       "table",
	requireViewDesc:        "view",
	requireTableOrViewDesc: "table or view",
	requireSequenceDesc:    "sequence",
}

// LookupSchema implements the tree.TableNameTargetResolver interface.
func (p *planner) LookupSchema(
	ctx context.Context, dbName, scName string,
) (found bool, scMeta tree.SchemaMeta, err error) {
	sc := p.LogicalSchemaAccessor()
	dbDesc, err := sc.GetDatabaseDesc(dbName, p.CommonLookupFlags(ctx, false /*required*/))
	if err != nil || dbDesc == nil {
		return false, nil, err
	}
	return sc.IsValidSchema(dbDesc, scName), dbDesc, nil
}

// LookupObject implements the tree.TableNameExistingResolver interface.
func (p *planner) LookupObject(
	ctx context.Context, dbName, scName, tbName string,
) (found bool, objMeta tree.NameResolutionResult, err error) {
	sc := p.LogicalSchemaAccessor()
	p.tableName = tree.MakeTableNameWithSchema(tree.Name(dbName), tree.Name(scName), tree.Name(tbName))
	objDesc, _, err := sc.GetObjectDesc(&p.tableName, p.ObjectLookupFlags(ctx, false /*required*/))
	return objDesc != nil, objDesc, err
}

func (p *planner) CommonLookupFlags(ctx context.Context, required bool) CommonLookupFlags {
	return CommonLookupFlags{
		ctx:         ctx,
		txn:         p.txn,
		required:    required,
		avoidCached: p.avoidCachedDescriptors,
	}
}

func (p *planner) ObjectLookupFlags(ctx context.Context, required bool) ObjectLookupFlags {
	return ObjectLookupFlags{
		CommonLookupFlags: p.CommonLookupFlags(ctx, required),
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
			descriptor, err := ResolveMutableExistingObject(ctx, p, &tableNames[i], true, anyDescType)
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
	sc SchemaResolver,
	dbName, scName string,
	idxName tree.UnrestrictedName,
	lookupFlags CommonLookupFlags,
) (result *tree.TableName, desc *MutableTableDescriptor, err error) {
	sa := sc.LogicalSchemaAccessor()
	dbDesc, err := sa.GetDatabaseDesc(dbName, lookupFlags)
	if dbDesc == nil || err != nil {
		return nil, nil, err
	}

	tns, err := sa.GetObjectNames(dbDesc, scName,
		DatabaseListFlags{CommonLookupFlags: lookupFlags, explicitPrefix: true})
	if err != nil {
		return nil, nil, err
	}

	result = nil
	tblLookupFlags := ObjectLookupFlags{CommonLookupFlags: lookupFlags}
	tblLookupFlags.required = false
	for i := range tns {
		tn := &tns[i]
		tableDesc, err := ResolveMutableExistingObject(ctx, sc, tn, true, anyDescType)
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
			return nil, nil, pgerror.NewErrorf(pgerror.CodeAmbiguousParameterError,
				"index name %q is ambiguous (found in %s and %s)",
				idxName, tn.String(), result.String())
		}
		result = tn
		desc = tableDesc
	}
	if result == nil && lookupFlags.required {
		return nil, nil, pgerror.NewErrorf(pgerror.CodeUndefinedObjectError,
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
	ctx context.Context, p *planner, index *tree.TableNameWithIndex, requireTable bool,
) (tn *tree.TableName, desc *MutableTableDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		tn, desc, err = expandIndexName(ctx, p, index, requireTable)
	})
	return tn, desc, err
}

func expandIndexName(
	ctx context.Context, sc SchemaResolver, index *tree.TableNameWithIndex, requireTable bool,
) (tn *tree.TableName, desc *MutableTableDescriptor, err error) {
	tn = &index.Table
	if !index.SearchTable {
		// The index and its table prefix must exist already. Resolve the table.
		desc, err = ResolveMutableExistingObject(ctx, sc, tn, requireTable, requireTableDesc)
		if err != nil {
			return nil, nil, err
		}
	} else {
		// On the first call to expandMutableIndexName(), index.SearchTable is
		// true, index.Index is empty and tn.Table() is the index
		// name. Once the table name is resolved for the index below,
		// index.Table references a new table name (not the index), so a
		// subsequent call to expandMutableIndexName() will generate tn using the
		// new value of index.Table, which is a table name.

		// Just an assertion: if we got there, there cannot be a value in index.Index yet.
		if index.Index != "" {
			return nil, nil, pgerror.NewAssertionErrorf("programmer error: not-searched index name found already qualified: %s@%s", tn, index.Index)
		}

		index.Index = tree.UnrestrictedName(tn.TableName)

		// Look up the table prefix.
		found, _, err := tn.TableNamePrefix.Resolve(ctx, sc, sc.CurrentDatabase(), sc.CurrentSearchPath())
		if err != nil {
			return nil, nil, err
		}
		if !found {
			if requireTable {
				return nil, nil, pgerror.NewErrorf(pgerror.CodeUndefinedObjectError,
					"schema or database was not found while searching index: %q",
					tree.ErrString(&index.Index)).SetHintf(
					"check the current database and search_path are valid")
			}
			return nil, nil, nil
		}

		lookupFlags := sc.CommonLookupFlags(ctx, requireTable)
		var foundTn *tree.TableName
		foundTn, desc, err = findTableContainingIndex(ctx, sc, tn.Catalog(), tn.Schema(), index.Index, lookupFlags)
		if err != nil {
			return nil, nil, err
		} else if foundTn != nil {
			// Memoize the table name that was found. tn is a reference to the table name
			// stored in index.Table.
			*tn = *foundTn
		}
	}
	return tn, desc, nil
}

// getTableAndIndex returns the table and index descriptors for a table
// (primary index) or table-with-index. Only one of table and tableWithIndex can
// be set.  This is useful for statements that have both table and index
// variants (like `ALTER TABLE/INDEX ... SPLIT AT ...`).
// It can return indexes that are being rolled out.
func (p *planner) getTableAndIndex(
	ctx context.Context,
	table *tree.TableName,
	tableWithIndex *tree.TableNameWithIndex,
	privilege privilege.Kind,
) (*MutableTableDescriptor, *sqlbase.IndexDescriptor, error) {
	var tableDesc *MutableTableDescriptor
	var err error
	if tableWithIndex == nil {
		// Variant: ALTER TABLE
		tableDesc, err = p.ResolveMutableTableDescriptor(
			ctx, table, true /*required*/, requireTableDesc,
		)
	} else {
		// Variant: ALTER INDEX
		_, tableDesc, err = expandMutableIndexName(ctx, p, tableWithIndex, true /* requireTable */)
	}
	if err != nil {
		return nil, nil, err
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege); err != nil {
		return nil, nil, err
	}

	// Determine which index to use.
	var index sqlbase.IndexDescriptor
	if tableWithIndex == nil {
		index = tableDesc.PrimaryIndex
	} else {
		idx, dropped, err := tableDesc.FindIndexByName(string(tableWithIndex.Index))
		if err != nil {
			return nil, nil, err
		}
		if dropped {
			return nil, nil, fmt.Errorf("index %q being dropped", tableWithIndex.Index)
		}
		index = idx
	}
	return tableDesc, &index, nil
}

// expandTableGlob expands pattern into a list of tables represented
// as a tree.TableNames.
func expandTableGlob(
	ctx context.Context, sc SchemaResolver, pattern tree.TablePattern,
) (tree.TableNames, error) {
	if t, ok := pattern.(*tree.TableName); ok {
		_, err := ResolveExistingObject(ctx, sc, t, true /*required*/, anyDescType)
		if err != nil {
			return nil, err
		}
		return tree.TableNames{*t}, nil
	}

	glob := pattern.(*tree.AllTablesSelector)
	found, descI, err := glob.TableNamePrefix.Resolve(
		ctx, sc, sc.CurrentDatabase(), sc.CurrentSearchPath())
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, sqlbase.NewInvalidWildcardError(tree.ErrString(glob))
	}

	return GetObjectNames(ctx, sc, descI.(*DatabaseDescriptor), glob.Schema(), glob.ExplicitSchema)
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
	ctx context.Context, dbName, scName, tbName string,
) (found bool, objMeta tree.NameResolutionResult, err error) {
	if dbName == r.newTableName.Catalog() &&
		scName == r.newTableName.Schema() &&
		tbName == r.newTableName.Table() {
		return true, r.newTableDesc, nil
	}
	return r.SchemaResolver.LookupObject(ctx, dbName, scName, tbName)
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
