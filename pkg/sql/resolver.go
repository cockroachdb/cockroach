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
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
			CommonLookupFlags: sc.CommonLookupFlags(ctx, true /*required*/),
			explicitPrefix:    explicitPrefix,
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
		return nil, sqlbase.NewWrongObjectTypeError(tn, requiredTypeNames[requiredType])
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
//
// This is meant for use by statements that can observe objects that
// were added within the same txn but not yet visible to other txns.
// In particular most DDL statements should set this.
//
// The caller should use this as follows: defer p.useNewDescriptors()()
func (p *planner) useNewDescriptors() func() {
	save := p.revealNewDescriptors
	p.revealNewDescriptors = true
	return func() { p.revealNewDescriptors = save }
}

// disableCaching configures the planner so that future descriptors
// resolutions will not use the cache.
//
// The caller should use this as follows: defer p.disableCaching()()
func (p *planner) disableCaching() func() {
	save := p.avoidCachedDescriptors
	p.avoidCachedDescriptors = true
	return func() { p.avoidCachedDescriptors = save }
}

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
) (found bool, scMeta interface{}, err error) {
	defer func() {
		log.VEventf(ctx, 2, "planner.LookupSchema(%s, %s) -> %v %v %v", dbName, scName, found, scMeta, err)
	}()
	sc := p.LogicalSchemaAccessor()
	dbDesc, err := p.LogicalSchemaAccessor().GetDatabaseDesc(dbName,
		p.CommonLookupFlags(ctx, false /*required*/))
	if err != nil || dbDesc == nil {
		return false, nil, err
	}
	return sc.IsValidSchema(dbDesc, scName), dbDesc, nil
}

// LookupObject implements the TableNameExistingResolver interface.
func (p *planner) LookupObject(
	ctx context.Context, dbName, scName, tbName string,
) (found bool, objMeta interface{}, err error) {
	defer func() {
		log.VEventf(ctx, 2, "planner.LookupObject(%s, %s, %s) -> %v %v %v", dbName, scName, tbName, found, objMeta, err)
	}()
	sc := p.LogicalSchemaAccessor()
	// TODO(knz): elide this allocation of TableName.
	tn := tree.MakeTableNameWithSchema(tree.Name(dbName), tree.Name(scName), tree.Name(tbName))
	objDesc, _, err := sc.GetObjectDesc(&tn, p.ObjectLookupFlags(ctx, false /*required*/))
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
		allowAdding:       p.revealNewDescriptors,
	}
}

func (p *planner) objectLookupFlagsExplicit(
	ctx context.Context, required, allowAdding bool,
) ObjectLookupFlags {
	return ObjectLookupFlags{
		CommonLookupFlags: p.CommonLookupFlags(ctx, required),
		allowAdding:       allowAdding,
	}
}

// getDescriptorsFromTargetList fetches the descriptors for the targets.
func getDescriptorsFromTargetList(
	ctx context.Context, sc SchemaResolver, targets tree.TargetList,
) ([]sqlbase.DescriptorProto, error) {
	if targets.Databases != nil {
		if len(targets.Databases) == 0 {
			return nil, errNoDatabase
		}
		descs := make([]sqlbase.DescriptorProto, 0, len(targets.Databases))
		for _, database := range targets.Databases {
			descriptor, err := ResolveDatabase(ctx, sc, string(database))
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
		tables, err := expandTableGlob(ctx, sc, tableGlob)
		if err != nil {
			return nil, err
		}
		for i := range tables {
			descriptor, _, err := sc.LogicalSchemaAccessor().GetObjectDesc(&tables[i],
				sc.ObjectLookupFlags(ctx, true /*required*/))
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
	dbName string,
	idxName tree.UnrestrictedName,
	lookupFlags CommonLookupFlags,
) (result *tree.TableName, desc *sqlbase.TableDescriptor, err error) {
	dbDesc, err := sc.GetDatabaseDesc(dbName, lookupFlags)
	if dbDesc == nil || err != nil {
		return nil, nil, err
	}

	tns, err := sc.GetObjectNames(dbDesc, tree.PublicSchema,
		DatabaseListFlags{CommonLookupFlags: lookupFlags, explicitPrefix: true})
	if err != nil {
		return nil, nil, err
	}

	result = nil
	tblLookupFlags := ObjectLookupFlags{CommonLookupFlags: lookupFlags}
	tblLookupFlags.required = false
	for i := range tns {
		tn := &tns[i]
		tableDesc, _, err := getTableDesc(sc, tn, tblLookupFlags)
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
			return nil, nil, fmt.Errorf("index name %q is ambiguous (found in %s and %s)",
				idxName, tn.String(), result.String())
		}
		result = tn
		desc = tableDesc
	}
	if result == nil && lookupFlags.required {
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
) (tn *tree.TableName, desc *sqlbase.TableDescriptor, err error) {
	tn, err = index.Table.Normalize()
	if err != nil {
		return nil, nil, err
	}

	if !index.SearchTable {
		// The index and its table prefix must exist already. Resolve the table.
		desc, err = ResolveExistingObject(ctx, sc, tn, requireTable, requireTableDesc)
		if err != nil {
			return nil, nil, err
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
			return nil, nil, pgerror.NewErrorf(pgerror.CodeInternalError,
				"programmer error: not-searched index name found already qualified: %s@%s", tn, index.Index)
		}

		curDb := sc.CurrentDatabase()
		if curDb == "" {
			return nil, nil, pgerror.NewErrorf(pgerror.CodeUndefinedObjectError,
				"no database specified: %q", tree.ErrString(index))
		}

		index.Index = tree.UnrestrictedName(tn.TableName)
		lookupFlags := sc.CommonLookupFlags(ctx, requireTable)
		tn, desc, err = findTableContainingIndex(
			sc.LogicalSchemaAccessor(), curDb, index.Index, lookupFlags)
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
	var tableDesc *sqlbase.TableDescriptor
	var err error
	defer p.useNewDescriptors()()
	if tableWithIndex == nil {
		// Variant: ALTER TABLE
		tn, err = table.Normalize()
		if err != nil {
			return nil, nil, err
		}
		tableDesc, err = ResolveExistingObject(ctx, p, tn, true /*required*/, requireTableDesc)
		if err != nil {
			return nil, nil, err
		}
	} else {
		// Variant: ALTER INDEX
		tn, tableDesc, err = expandIndexName(ctx, p, tableWithIndex, true /* requireTable */)
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
	sc SchemaResolver,
	pattern tree.TablePattern,
) (tree.TableNames, error) {
	if t, ok := pattern.(*tree.TableName); ok {
		_, err := ResolveExistingObject(ctx, sc, t, true /*required*/, anyDescType)
		if err == nil {
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

func newUnknownSourceError(tn *tree.TableName) error {
	return pgerror.NewErrorf(pgerror.CodeUndefinedTableError,
		"source name %q not found in FROM clause", tree.ErrString(tn))
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
