package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type SchemaResolver interface {
	tree.TableNameExistingResolver
	tree.TableNameTargetResolver

	Txn() *client.Txn
	LogicalSchemaAccessor() SchemaAccessor
	CurrentDatabase() string
	CurrentSearchPath() sessiondata.SearchPath
}

var _ SchemaResolver = &planner{}

// ResolveDatabase looks up a database name.
func ResolveDatabase(
	ctx context.Context, sc SchemaResolver, dbName string,
) (*DatabaseDescriptor, error) {
	return sc.LogicalSchemaAccessor().GetDatabaseDesc(dbName,
		DatabaseLookupFlags{ctx: ctx, txn: sc.Txn(), required: true})
}

// GetObjectNames retrieves the names of all objects in the target database/schema.
func GetObjectNames(
	ctx context.Context,
	sc SchemaResolver,
	dbDesc *DatabaseDescriptor,
	scName string,
	explicitPrefix bool,
) (TableNames, error) {
	return sc.LogicalSchemaAccessor().GetObjectNames(dbDesc, scName,
		DatabaseListFlags{
			DatabaseLookupFlags: DatabaseLookupFlags{ctx: ctx, txn: sc.Txn(), required: true},
			explicitPrefix:      explicitPrefix,
		})
}

// ResolveExistingObject looks up an existing object.
// If required is true, an error is returned if the object does not exist.
// Optionally, if a desired descriptor type is specified, that type is checked.
func ResolveExistingObject(
	ctx context.Context,
	sc SchemaResolver,
	tn *ObjectName,
	required bool,
	requiredType requiredType,
) (*ObjectDescriptor, error) {
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
	desc := descI.(*ObjectDescriptor)
	goodType := true
	switch requiredType {
	case requireTableDesc:
		goodType = desc.IsTable()
	case requireViewDesc:
		goodType = desc.IsView()
	case requireTableOrViewDesc:
		goodType = desc.IsTable() || desc.IsView()
	case requireSequenceDesc:
		goodType = desc.IsSequence()
	}
	if !goodType {
		return nil, sqlbase.NewWrongObjectTypeError(tn, requiredType.String())
	}
	return desc, nil
}

// ResolveTargetObject determines a valid target path for an object
// that may not exist yet. It returns the descriptor for the database
// where the target object lives.
func ResolveTargetObject(
	ctx context.Context,
	sc SchemaResolver,
	tn *ObjectName,
) (*DatabaseDescriptor, error) {
	found, descI, err := tn.ResolveTarget(ctx, sc, sc.CurrentDatabase(), sc.CurrentSearchPath())
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, pgerror.NewErrorf(pgerror.CodeInvalidNameError,
			"invalid target name: %q", tree.ErrString(tn))
	}
	return descI.(*DatabaseDescriptor), nil
}

// useNewDescriptors configures the planner so that future descriptors
// resolutions will be able to observe newly added descriptors still
// in the ADD state.
// The caller should use this as follows: defer p.useNewDescriptors()()
func (p *planner) useNewDescriptors() func() {
	save := p.revealNewDescriptors
	p.revealNewDescriptors = true
	return func() { p.revealNewDescriptors = save }
}

type requiredType int

const (
	anyDescType requiredType = iota
	requireTableDesc
	requireViewDesc
	requireTableOrViewDesc
	requireSequenceDesc
)

var requiredTypeNames = map[requiredType]string{
	requireTableDesc:       "table",
	requireViewDesc:        "view",
	requireTableOrViewDesc: "table or view",
	requireSequenceDesc:    "sequence",
}

func (r requiredType) String() {
	return requiredTypeNames[r]
}

// LookupSchema implements the tree.TableNameTargetResolver interface.
func (p *planner) LookupSchema(
	ctx context.Context, dbName, scName string,
) (found bool, scMeta interface{}, err error) {
	sc := p.LogicalSchemaAccessor()
	dbDesc, err := p.LogicalSchemaAccessor().GetDatabaseDesc(dbName, p.commonLookupFlags(ctx))
	if err != nil || dbDesc == nil {
		return false, nil, err
	}
	return n.sc.IsValidSchema(dbDesc, scName), dbDesc, nil
}

// LookupObject implements the TableNameExistingResolver interface.
func (p *planner) LookupObject(
	ctx context.Context, dbName, scName, tbName string,
) (found bool, objMeta interface{}, err error) {
	sc := p.LogicalSchemaAccessor()
	// TODO(knz): elide this allocation of TableName.
	tn := tree.NewTableNameWithSchema(dbName, scName, tbName)
	objDesc, _, err := sc.GetObjectDesc(&tn, p.objectLookupFlags(ctx))
	return objDesc != nil, objDesc, err
}

func (p *planner) commonLookupFlags(ctx context.Context) CommonLookupFlags {
	return CommonLookupFlags{ctx: ctx, txn: p.txn, required: false, avoidCached: p.avoidCachedDescriptors}
}

func (p *planner) objectLookupFlags(ctx context.Context) ObjectLookupFlags {
	return ObjectLookupFlags{
		CommonLookupFlags: p.commonLookupFlags(ctx),
		allowAdding:       p.revealNewDescriptors,
	}
}

func (p *planner) objectLookupFlagsExplicit(
	ctx context.Context, allowAdding bool,
) ObjectLookupFlags {
	return ObjectLookupFlags{
		CommonLookupFlags: p.commonLookupFlags(ctx),
		allowAdding:       allowAdding,
	}
}

// getDescriptorsFromTargetList fetches the descriptors for the targets.
func getDescriptorsFromTargetList(
	ctx context.Context, txn *client.Txn, db string, targets tree.TargetList,
) ([]sqlbase.DescriptorProto, error) {
	if targets.Databases != nil {
		if len(targets.Databases) == 0 {
			return nil, errNoDatabase
		}
		descs := make([]sqlbase.DescriptorProto, 0, len(targets.Databases))
		for _, database := range targets.Databases {
			descriptor, err := MustGetDatabaseDesc(ctx, txn, string(database))
			if err != nil {
				return nil, err
			}
			descs = append(descs, descriptor)
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
		tables, err := expandTableGlob(ctx, txn, vt, db, tableGlob)
		if err != nil {
			return nil, err
		}
		for i := range tables {
			descriptor, err := MustGetTableOrViewDesc(
				ctx, txn, &tables[i], true /*allowAdding*/)
			if err != nil {
				return nil, err
			}
			descs = append(descs, descriptor)
		}
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

// findTableContainingIndex returns the name of the table containing an
// index of the given name and its descriptor.
// This is used by expandIndexName().
//
// An error is returned if the index name is ambiguous (i.e. exists in
// multiple tables). If no table is found and requireTable is true, an
// error will be returned, otherwise the TableName and descriptor
// returned will be nil.
func findTableContainingIndex(
	sc SchemaAccessor,
	dbName tree.Name,
	idxName tree.UnrestrictedName,
	lookupFlags CommonLookupFlags,
) (result *tree.TableName, desc *sqlbase.TableDescriptor, err error) {
	dbDesc, err := sc.GetDatabaseDesc(dbName, lookupFlags)
	if dbDesc == nil || err != nil {
		return nil, nil, err
	}

	tns, err := sc.GetObjectNames(dbDesc, tree.PublicSchema,
		DatabaseListFlags{lookupFlags, explicitPrefix: true})
	if err != nil {
		return nil, nil, err
	}

	result = nil
	tblLookupFlags := lookupFlags
	tblLookupFlags.required = false
	for i := range tns {
		tn := &tns[i]
		tableDesc, err := getTableDesc(sc, tn, tblLookupFlags)
		if err != nil {
			return nil, nil, err
		}
		if tableDesc == nil {
			continue
		}

		_, dropped, err := tableDesc.FindIndexByName(string(idxName))
		if err != nil || dropped {
			// err is nil if the index does not exist on the table.
			continue
		}
		if result != nil {
			return nil, fmt.Errorf("index name %q is ambiguous (found in %s and %s)",
				idxName, tn.String(), result.String())
		}
		result = tn
		desc = tableDesc
	}
	if result == nil && requireTable {
		return nil, nil, fmt.Errorf("index %q not in any of the tables %v", idxName, tns)
	}
	return result, desc, nil
}

// expandIndexName ensures that the index name is qualified with a table
// name, and searches the table name if not yet specified.
//
// It returns the TableName of the underlying table for convenience.
// If no table is found and requireTable is true an error will be
// returned, otherwise the TableName returned will be nil.
//
// It *may* return the descriptor of the underlying table, depending
// on the lookup path. This can be used in the caller to avoid a 2nd
// lookup.
func expandIndexName(
	ctx context.Context, sc SchemaResolver, index *tree.TableNameWithIndex, requireTable bool,
) (*tree.TableName, *sqlbase.TableDescriptor, error) {
	tn, err := index.Table.Normalize()
	if err != nil {
		return nil, err
	}

	if !index.SearchTable {
		var found bool
		// The index and its table prefix must exist already. Resolve the table.
		found, desc, err = tn.ResolveExisting(
			sc.LogicalSchemaAccessor(), sc.CurrentDatabase(), sc.CurrentSearchPath())
		if err != nil {
			return nil, nil, err
		} else if !found && requireTable {
			return nil, nil, sqlbase.NewUndefinedRelationError(tn)
		} else if !found {
			return nil, nil, nil
		}
	} else {
		// On the first call to expandIndexName(), index.SearchTable is
		// true, index.Index is empty and tn.Table() is the index
		// name. Once the table name is resolved for the index below,
		// index.Table references a new table name (not the index), so a
		// subsequent call to expandIndexName() will generate tn using the
		// new value of index.Table, which is a table name.

		// Just an assertion: if we got there, there cannot be a path prefix
		// in tn or a value in index.Index yet.
		if tn.ExplicitSchema || tn.ExplicitCatalog || index.Index != "" {
			return nil, pgerror.NewErrorf(pgerror.CodeInternalError,
				"programmer error: not-searched index name found already qualified: %s@%s", tn, index.Index)
		}

		index.Index = tree.UnrestrictedName(index.Table.TableName)
		lookupFlags := CommonLookupFlags{ctx: ctx, txn: sc.Txn(), required: requireTable}
		tn, desc, err = findTableContainingIndex(
			sc.LogicalSchemaAccessor(), tn.CatalogName, index.Index, lookupFlags)
		if err != nil {
			return nil, nil, err
		} else if tn == nil {
			// NB: tn is nil here if and only if requireTable is
			// false, otherwise err would be non-nil.
			return nil, nil, nil
		}
		// Memoize the resolved table name in case expandIndexName() is called again.
		index.Table.TableNameReference = tn
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
	table *tree.NormalizableTableName,
	tableWithIndex *tree.TableNameWithIndex,
	privilege privilege.Kind,
) (*sqlbase.TableDescriptor, *sqlbase.IndexDescriptor, error) {
	var tn *tree.TableName
	var desc *sqlbase.TableDescriptor
	var err error
	defer p.useNewDescriptors()()
	if tableWithIndex == nil {
		// Variant: ALTER TABLE
		tn, err = table.Normalize()
		if err != nil {
			return nil, nil, err
		}
		desc, err = ResolveExistingObject(ctx, p, tn, true /*required*/, requireTableDesc)
		if err != nil {
			return nil, nil, err
		}
	} else {
		// Variant: ALTER INDEX
		tn, desc, err = p.expandIndexName(ctx, tableWithIndex, true /* requireTable */)
		if err != nil {
			return nil, nil, err
		}
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
	ctx context.Context,
	txn *client.Txn,
	vt VirtualTabler,
	database string,
	pattern tree.TablePattern,
) (tree.TableNames, error) {
	if t, ok := pattern.(*tree.TableName); ok {
		if err := t.QualifyWithDatabase(database); err != nil {
			return nil, err
		}
		return tree.TableNames{*t}, nil
	}

	glob := pattern.(*tree.AllTablesSelector)

	if err := glob.QualifyWithDatabase(database); err != nil {
		return nil, err
	}

	dbDesc, err := MustGetDatabaseDesc(ctx, txn, string(glob.CatalogName))
	if err != nil {
		return nil, err
	}

	tableNames, err := getTableNames(ctx, txn, dbDesc, glob.ExplicitSchema)
	if err != nil {
		return nil, err
	}
	return tableNames, nil
}

// ParseQualifiedTableName implements the tree.EvalPlanner interface.
func (p *planner) ParseQualifiedTableName(
	ctx context.Context, sql string,
) (*tree.TableName, error) {
	parsedNameWithIndex, err := p.ParseTableNameWithIndex(sql)
	if err != nil {
		return nil, err
	}
	parsedName := parsedNameWithIndex.Table
	return p.QualifyWithDatabase(ctx, &parsedName)
}
