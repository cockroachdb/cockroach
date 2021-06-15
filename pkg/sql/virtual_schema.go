// Copyright 2016 The Cockroach Authors.
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
	"math"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

const virtualSchemaNotImplementedMessage = "virtual schema table not implemented: %s.%s"

//
// Programmer interface to define virtual schemas.
//

// virtualSchema represents a database with a set of virtual tables. Virtual
// tables differ from standard tables in that they are not persisted to storage,
// and instead their contents are populated whenever they are queried.
//
// The virtual database and its virtual tables also differ from standard databases
// and tables in that their descriptors are not distributed, but instead live statically
// in code. This means that they are accessed separately from standard descriptors.
type virtualSchema struct {
	name            string
	undefinedTables map[string]struct{}
	tableDefs       map[descpb.ID]virtualSchemaDef
	tableValidator  func(*descpb.TableDescriptor) error // optional
	// Some virtual tables can be used if there is no current database set; others can't.
	validWithNoDatabaseContext bool
	// Some virtual schemas (like pg_catalog) contain types that we can resolve.
	containsTypes bool
}

// virtualSchemaDef represents the interface of a table definition within a virtualSchema.
type virtualSchemaDef interface {
	getSchema() string
	initVirtualTableDesc(
		ctx context.Context, st *cluster.Settings, sc catalog.SchemaDescriptor, id descpb.ID,
	) (descpb.TableDescriptor, error)
	getComment() string
	isUnimplemented() bool
}

type virtualIndex struct {
	// populate populates the table given the constraint. matched is true if any
	// rows were generated.
	populate func(ctx context.Context, constraint tree.Datum, p *planner, db catalog.DatabaseDescriptor,
		addRow func(...tree.Datum) error,
	) (matched bool, err error)

	// partial is true if the virtual index isn't able to satisfy all constraints.
	// For example, the pg_class table contains both indexes and tables. Tables
	// can be looked up via a virtual index, since we can look up their descriptor
	// by their ID directly. But indexes can't - they're hashed identifiers with
	// no actual index. So we mark this index as partial, and if we get no match
	// during populate, we'll fall back on populating the entire table.
	partial bool
}

// virtualSchemaTable represents a table within a virtualSchema.
type virtualSchemaTable struct {
	// Exactly one of the populate and generator fields should be defined for
	// each virtualSchemaTable.
	schema string

	// comment represents comment of virtual schema table.
	comment string

	// populate, if non-nil, is a function that is used when creating a
	// valuesNode. This function eagerly loads every row of the virtual table
	// during initialization of the valuesNode.
	populate func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error

	// indexes, if non empty, is a slice of populate methods that also take a
	// constraint, only generating rows that match the constraint. The order of
	// indexes must match the order of the index definitions in the virtual table's
	// schema.
	indexes []virtualIndex

	// generator, if non-nil, is a function that is used when creating a
	// virtualTableNode. This function returns a virtualTableGenerator function
	// which generates the next row of the virtual table when called.
	generator func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, stopper *stop.Stopper) (virtualTableGenerator, cleanupFunc, error)

	// unimplemented indicates that we do not yet implement the contents of this
	// table. If the stub_catalog_tables session variable is enabled, the table
	// will be queryable but return no rows. Otherwise querying the table will
	// return an unimplemented error.
	unimplemented bool
}

// virtualSchemaView represents a view within a virtualSchema
type virtualSchemaView struct {
	schema        string
	resultColumns colinfo.ResultColumns
}

// getSchema is part of the virtualSchemaDef interface.
func (t virtualSchemaTable) getSchema() string {
	return t.schema
}

// initVirtualTableDesc is part of the virtualSchemaDef interface.
func (t virtualSchemaTable) initVirtualTableDesc(
	ctx context.Context, st *cluster.Settings, sc catalog.SchemaDescriptor, id descpb.ID,
) (descpb.TableDescriptor, error) {
	stmt, err := parser.ParseOne(t.schema)
	if err != nil {
		return descpb.TableDescriptor{}, err
	}

	create := stmt.AST.(*tree.CreateTable)
	var firstColDef *tree.ColumnTableDef
	for _, def := range create.Defs {
		if d, ok := def.(*tree.ColumnTableDef); ok {
			if d.HasDefaultExpr() {
				return descpb.TableDescriptor{},
					errors.Errorf("virtual tables are not allowed to use default exprs "+
						"because bootstrapping: %s:%s", &create.Table, d.Name)
			}
			if firstColDef == nil {
				firstColDef = d
			}
		}
		if _, ok := def.(*tree.UniqueConstraintTableDef); ok {
			return descpb.TableDescriptor{},
				errors.Errorf("virtual tables are not allowed to have unique constraints")
		}
	}
	if firstColDef == nil {
		return descpb.TableDescriptor{},
			errors.Errorf("can't have empty virtual tables")
	}

	// Virtual tables never use SERIAL so we need not process SERIAL
	// types here.
	mutDesc, err := NewTableDesc(
		ctx,
		nil, /* txn */
		nil, /* vs */
		st,
		create,
		nil,
		sc,
		id,
		nil,       /* regionConfig */
		startTime, /* creationTime */
		descpb.NewPublicSelectPrivilegeDescriptor(),
		nil,                        /* affected */
		nil,                        /* semaCtx */
		nil,                        /* evalCtx */
		&sessiondata.SessionData{}, /* sessionData */
		tree.PersistencePermanent,
	)
	if err != nil {
		err = errors.Wrapf(err, "initVirtualDesc problem with schema: \n%s", t.schema)
		return descpb.TableDescriptor{}, err
	}
	for _, index := range mutDesc.PublicNonPrimaryIndexes() {
		if index.NumKeyColumns() > 1 {
			panic("we don't know how to deal with virtual composite indexes yet")
		}
		idx := index.IndexDescDeepCopy()
		idx.StoreColumnNames, idx.StoreColumnIDs = nil, nil
		publicColumns := mutDesc.PublicColumns()
		presentInIndex := catalog.MakeTableColSet(idx.KeyColumnIDs...)
		for _, col := range publicColumns {
			if col.IsVirtual() || presentInIndex.Contains(col.GetID()) {
				continue
			}
			idx.StoreColumnIDs = append(idx.StoreColumnIDs, col.GetID())
			idx.StoreColumnNames = append(idx.StoreColumnNames, col.GetName())
		}
		mutDesc.SetPublicNonPrimaryIndex(index.Ordinal(), idx)
	}
	return mutDesc.TableDescriptor, nil
}

// getComment is part of the virtualSchemaDef interface.
func (t virtualSchemaTable) getComment() string {
	return t.comment
}

// getIndex returns the virtual index with the input ID.
func (t virtualSchemaTable) getIndex(id descpb.IndexID) *virtualIndex {
	// Subtract 2 from the index id to get the ordinal in def.indexes, since
	// the index with ID 1 is the "primary" index defined by def.populate.
	return &t.indexes[id-2]
}

// unimplemented retrieves whether the virtualSchemaDef is implemented or not.
func (t virtualSchemaTable) isUnimplemented() bool {
	return t.unimplemented
}

// getSchema is part of the virtualSchemaDef interface.
func (v virtualSchemaView) getSchema() string {
	return v.schema
}

// initVirtualTableDesc is part of the virtualSchemaDef interface.
func (v virtualSchemaView) initVirtualTableDesc(
	ctx context.Context, st *cluster.Settings, sc catalog.SchemaDescriptor, id descpb.ID,
) (descpb.TableDescriptor, error) {
	stmt, err := parser.ParseOne(v.schema)
	if err != nil {
		return descpb.TableDescriptor{}, err
	}

	create := stmt.AST.(*tree.CreateView)

	columns := v.resultColumns
	if len(create.ColumnNames) != 0 {
		columns = overrideColumnNames(columns, create.ColumnNames)
	}
	mutDesc, err := makeViewTableDesc(
		ctx,
		st,
		create.Name.Table(),
		tree.AsStringWithFlags(create.AsSource, tree.FmtParsable),
		0, /* parentID */
		sc.GetID(),
		id,
		columns,
		startTime, /* creationTime */
		descpb.NewPublicSelectPrivilegeDescriptor(),
		nil, /* semaCtx */
		nil, /* evalCtx */
		tree.PersistencePermanent,
		false, /* isMultiRegion */
		nil,   /* sc */
	)
	return mutDesc.TableDescriptor, err
}

// getComment is part of the virtualSchemaDef interface.
func (v virtualSchemaView) getComment() string {
	return ""
}

// isUnimplemented is part of the virtualSchemaDef interface.
func (v virtualSchemaView) isUnimplemented() bool {
	return false
}

// virtualSchemas holds a slice of statically registered virtualSchema objects.
//
// When adding a new virtualSchema, define a virtualSchema in a separate file, and
// add that object to this slice.
var virtualSchemas = map[descpb.ID]virtualSchema{
	catconstants.InformationSchemaID: informationSchema,
	catconstants.PgCatalogID:         pgCatalog,
	catconstants.CrdbInternalID:      crdbInternal,
	catconstants.PgExtensionSchemaID: pgExtension,
}

var startTime = hlc.Timestamp{
	WallTime: time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
}

//
// SQL-layer interface to work with virtual schemas.
//

// VirtualSchemaHolder is a type used to provide convenient access to virtual
// database and table descriptors. VirtualSchemaHolder, virtualSchemaEntry,
// and virtualDefEntry make up the generated data structure which the
// virtualSchemas slice is mapped to. Because of this, they should not be
// created directly, but instead will be populated in a post-startup hook
// on an Executor.
type VirtualSchemaHolder struct {
	schemasByName map[string]*virtualSchemaEntry
	schemasByID   map[descpb.ID]*virtualSchemaEntry
	defsByID      map[descpb.ID]*virtualDefEntry
	orderedNames  []string
}

var _ VirtualTabler = (*VirtualSchemaHolder)(nil)

// GetVirtualSchema makes VirtualSchemaHolder implement catalog.VirtualSchemas.
func (vs *VirtualSchemaHolder) GetVirtualSchema(schemaName string) (catalog.VirtualSchema, bool) {
	sc, ok := vs.schemasByName[schemaName]
	return sc, ok
}

// GetVirtualSchemaByID makes VirtualSchemaHolder implement catalog.VirtualSchemas.
func (vs *VirtualSchemaHolder) GetVirtualSchemaByID(id descpb.ID) (catalog.VirtualSchema, bool) {
	sc, ok := vs.schemasByID[id]
	return sc, ok
}

// GetVirtualObjectByID makes VirtualSchemaHolder implement catalog.VirtualSchemas.
func (vs *VirtualSchemaHolder) GetVirtualObjectByID(id descpb.ID) (catalog.VirtualObject, bool) {
	entry, ok := vs.defsByID[id]
	if !ok {
		return nil, false
	}
	return entry, true
}

var _ catalog.VirtualSchemas = (*VirtualSchemaHolder)(nil)

type virtualSchemaEntry struct {
	desc            catalog.SchemaDescriptor
	defs            map[string]*virtualDefEntry
	orderedDefNames []string
	undefinedTables map[string]struct{}
	containsTypes   bool
}

func (v *virtualSchemaEntry) Desc() catalog.SchemaDescriptor {
	return v.desc
}

func (v *virtualSchemaEntry) NumTables() int {
	return len(v.defs)
}

func (v *virtualSchemaEntry) VisitTables(f func(object catalog.VirtualObject)) {
	for _, name := range v.orderedDefNames {
		f(v.defs[name])
	}
}

func (v *virtualSchemaEntry) GetObjectByName(
	name string, flags tree.ObjectLookupFlags,
) (catalog.VirtualObject, error) {
	switch flags.DesiredObjectKind {
	case tree.TableObject:
		if def, ok := v.defs[name]; ok {
			if flags.RequireMutable {
				return &mutableVirtualDefEntry{
					desc: tabledesc.NewBuilder(def.desc.TableDesc()).BuildExistingMutableTable(),
				}, nil
			}
			return def, nil
		}
		if _, ok := v.undefinedTables[name]; ok {
			return nil, newUnimplementedVirtualTableError(v.desc.GetName(), name)
		}
		return nil, nil
	case tree.TypeObject:
		if !v.containsTypes {
			return nil, nil
		}
		// Currently, we don't allow creation of types in virtual schemas, so
		// the only types present in the virtual schemas that have types (i.e.
		// pg_catalog) are types that are known at parse time. So, attempt to
		// parse the input object as a statically known type. Note that an
		// invalid input type like "notatype" will be parsed successfully as
		// a ResolvableTypeReference, so the error here does not need to be
		// intercepted and inspected.
		typRef, err := parser.GetTypeReferenceFromName(tree.Name(name))
		if err != nil {
			return nil, err
		}
		// If the parsed reference is actually a statically known type, then
		// we can return it. We return a simple wrapping of this type as
		// TypeDescriptor that represents an alias of the result type.
		typ, ok := tree.GetStaticallyKnownType(typRef)
		if !ok {
			return nil, nil
		}

		return &virtualTypeEntry{
			desc:    typedesc.MakeSimpleAlias(typ, catconstants.PgCatalogID),
			mutable: flags.RequireMutable,
		}, nil
	default:
		return nil, errors.AssertionFailedf("unknown desired object kind %d", flags.DesiredObjectKind)
	}
}

type virtualDefEntry struct {
	virtualDef                 virtualSchemaDef
	desc                       catalog.TableDescriptor
	comment                    string
	validWithNoDatabaseContext bool
	unimplemented              bool
}

func (e *virtualDefEntry) Desc() catalog.Descriptor {
	return e.desc
}

func canQueryVirtualTable(evalCtx *tree.EvalContext, e *virtualDefEntry) bool {
	return !e.unimplemented ||
		evalCtx == nil ||
		evalCtx.SessionData == nil ||
		evalCtx.SessionData.StubCatalogTablesEnabled
}

type mutableVirtualDefEntry struct {
	desc *tabledesc.Mutable
}

func (e *mutableVirtualDefEntry) Desc() catalog.Descriptor {
	return e.desc
}

type virtualTypeEntry struct {
	desc    catalog.TypeDescriptor
	mutable bool
}

func (e *virtualTypeEntry) Desc() catalog.Descriptor {
	// TODO(ajwerner): Should this be allowed? I think no. Let's just store an
	// ImmutableTypeDesc off of this thing.
	return e.desc
}

type virtualTableConstructor func(context.Context, *planner, string) (planNode, error)

var errInvalidDbPrefix = errors.WithHint(
	pgerror.New(pgcode.UndefinedObject,
		"cannot access virtual schema in anonymous database"),
	"verify that the current database is set")

func newInvalidVirtualSchemaError() error {
	return errors.AssertionFailedf("virtualSchema cannot have both the populate and generator functions defined")
}

func newInvalidVirtualDefEntryError() error {
	return errors.AssertionFailedf("virtualDefEntry.virtualDef must be a virtualSchemaTable")
}

func (e *virtualDefEntry) validateRow(datums tree.Datums, columns colinfo.ResultColumns) error {
	if r, c := len(datums), len(columns); r != c {
		return errors.AssertionFailedf("datum row count and column count differ: %d vs %d", r, c)
	}
	for i := range columns {
		col := &columns[i]
		datum := datums[i]
		if datum == tree.DNull {
			if !e.desc.PublicColumns()[i].IsNullable() {
				return errors.AssertionFailedf("column %s.%s not nullable, but found NULL value",
					e.desc.GetName(), col.Name)
			}
		} else if !datum.ResolvedType().Equivalent(col.Typ) {
			return errors.AssertionFailedf("datum column %q expected to be type %s; found type %s",
				col.Name, col.Typ, datum.ResolvedType())
		}
	}
	return nil
}

// getPlanInfo returns the column metadata and a constructor for a new
// valuesNode for the virtual table. We use deferred construction here
// so as to avoid populating a RowContainer during query preparation,
// where we can't guarantee it will be Close()d in case of error.
func (e *virtualDefEntry) getPlanInfo(
	table catalog.TableDescriptor,
	index catalog.Index,
	idxConstraint *constraint.Constraint,
	stopper *stop.Stopper,
) (colinfo.ResultColumns, virtualTableConstructor) {
	var columns colinfo.ResultColumns
	for _, col := range e.desc.PublicColumns() {
		columns = append(columns, colinfo.ResultColumn{
			Name:           col.GetName(),
			Typ:            col.GetType(),
			TableID:        table.GetID(),
			PGAttributeNum: col.GetPGAttributeNum(),
		})
	}

	constructor := func(ctx context.Context, p *planner, dbName string) (planNode, error) {
		var dbDesc catalog.DatabaseDescriptor
		var err error
		if dbName != "" {
			dbDesc, err = p.Descriptors().GetImmutableDatabaseByName(ctx, p.txn,
				dbName, tree.DatabaseLookupFlags{
					Required: true, AvoidCached: p.avoidCachedDescriptors,
				})
			if err != nil {
				return nil, err
			}
		} else {
			if !e.validWithNoDatabaseContext {
				return nil, errInvalidDbPrefix
			}
		}

		switch def := e.virtualDef.(type) {
		case virtualSchemaTable:
			if def.generator != nil && def.populate != nil {
				return nil, newInvalidVirtualSchemaError()
			}

			if def.generator != nil {
				next, cleanup, err := def.generator(ctx, p, dbDesc, stopper)
				if err != nil {
					return nil, err
				}
				return p.newVirtualTableNode(columns, next, cleanup), nil
			}

			constrainedScan := idxConstraint != nil && !idxConstraint.IsUnconstrained()
			if !constrainedScan {
				generator, cleanup, setupError := setupGenerator(ctx, func(pusher rowPusher) error {
					return def.populate(ctx, p, dbDesc, func(row ...tree.Datum) error {
						if err := e.validateRow(row, columns); err != nil {
							return err
						}
						return pusher.pushRow(row...)
					})
				}, stopper)
				if setupError != nil {
					return nil, setupError
				}
				return p.newVirtualTableNode(columns, generator, cleanup), nil
			}

			// We are now dealing with a constrained virtual index scan.

			if index.GetID() == 1 {
				return nil, errors.AssertionFailedf(
					"programming error: can't constrain scan on primary virtual index of table %s", e.desc.GetName())
			}

			// Figure out the ordinal position of the column that we're filtering on.
			columnIdxMap := catalog.ColumnIDToOrdinalMap(table.PublicColumns())
			indexKeyDatums := make([]tree.Datum, index.NumKeyColumns())

			generator, cleanup, setupError := setupGenerator(ctx, e.makeConstrainedRowsGenerator(
				ctx, p, dbDesc, index, indexKeyDatums, columnIdxMap, idxConstraint, columns), stopper)
			if setupError != nil {
				return nil, setupError
			}
			return p.newVirtualTableNode(columns, generator, cleanup), nil

		default:
			return nil, newInvalidVirtualDefEntryError()
		}
	}

	return columns, constructor
}

// makeConstrainedRowsGenerator returns a generator function that can be invoked
// to push all rows from this virtual table that satisfy the input index
// constraint to a row pusher that's supplied to the generator function.
func (e *virtualDefEntry) makeConstrainedRowsGenerator(
	ctx context.Context,
	p *planner,
	dbDesc catalog.DatabaseDescriptor,
	index catalog.Index,
	indexKeyDatums []tree.Datum,
	columnIdxMap catalog.TableColMap,
	idxConstraint *constraint.Constraint,
	columns colinfo.ResultColumns,
) func(pusher rowPusher) error {
	def := e.virtualDef.(virtualSchemaTable)
	return func(pusher rowPusher) error {
		var span constraint.Span
		addRowIfPassesFilter := func(idxConstraint *constraint.Constraint) func(datums ...tree.Datum) error {
			return func(datums ...tree.Datum) error {
				for i := 0; i < index.NumKeyColumns(); i++ {
					id := index.GetKeyColumnID(i)
					indexKeyDatums[i] = datums[columnIdxMap.GetDefault(id)]
				}
				// Construct a single key span out of the current row, so that
				// we can test it for containment within the constraint span of the
				// filter that we're applying. The results of this containment check
				// will tell us whether or not to let the current row pass the filter.
				key := constraint.MakeCompositeKey(indexKeyDatums...)
				span.Init(key, constraint.IncludeBoundary, key, constraint.IncludeBoundary)
				var err error
				if idxConstraint.ContainsSpan(p.EvalContext(), &span) {
					if err := e.validateRow(datums, columns); err != nil {
						return err
					}
					return pusher.pushRow(datums...)
				}
				return err
			}
		}

		// We have a virtual index with a constraint. Run the constrained
		// populate routine for every span. If for some reason we can't use the
		// index for a given span, we exit the loop early and run a "full scan"
		// over the virtual table, filtering the output using the remaining
		// spans.
		var currentSpan int
		for ; currentSpan < idxConstraint.Spans.Count(); currentSpan++ {
			span := idxConstraint.Spans.Get(currentSpan)
			if span.StartKey().Length() > 1 {
				return errors.AssertionFailedf(
					"programming error: can't push down composite constraints into vtables")
			}
			if !span.HasSingleKey(p.EvalContext()) {
				// No hope - we can't deal with range scans on virtual indexes.
				break
			}
			constraintDatum := span.StartKey().Value(0)
			virtualIndex := def.getIndex(index.GetID())

			// For each span, run the index's populate method, constrained to the
			// constraint span's value.
			found, err := virtualIndex.populate(ctx, constraintDatum, p, dbDesc,
				addRowIfPassesFilter(idxConstraint))
			if err != nil {
				return err
			}
			if !found && virtualIndex.partial {
				// If we found nothing, and the index was partial, we have no choice
				// but to populate the entire table and search through it.
				break
			}
		}
		if currentSpan == idxConstraint.Spans.Count() {
			// We successfully processed all constraints, so we can leave now.
			return nil
		}

		// Fall back to a full scan of the table, using the remaining filters
		// that weren't able to be used as constraints.
		newConstraint := *idxConstraint
		newConstraint.Spans = constraint.Spans{}
		nSpans := idxConstraint.Spans.Count() - currentSpan
		newConstraint.Spans.Alloc(nSpans)
		for ; currentSpan < idxConstraint.Spans.Count(); currentSpan++ {
			newConstraint.Spans.Append(idxConstraint.Spans.Get(currentSpan))
		}
		return def.populate(ctx, p, dbDesc, addRowIfPassesFilter(&newConstraint))
	}
}

// NewVirtualSchemaHolder creates a new VirtualSchemaHolder.
func NewVirtualSchemaHolder(
	ctx context.Context, st *cluster.Settings,
) (*VirtualSchemaHolder, error) {
	vs := &VirtualSchemaHolder{
		schemasByName: make(map[string]*virtualSchemaEntry, len(virtualSchemas)),
		schemasByID:   make(map[descpb.ID]*virtualSchemaEntry, len(virtualSchemas)),
		orderedNames:  make([]string, len(virtualSchemas)),
		defsByID:      make(map[descpb.ID]*virtualDefEntry, math.MaxUint32-catconstants.MinVirtualID),
	}

	order := 0
	for schemaID, schema := range virtualSchemas {
		scDesc, ok := schemadesc.GetVirtualSchemaByID(schemaID)
		if !ok {
			return nil, errors.AssertionFailedf("failed to find virtual schema %d (%s)", schemaID, schema.name)
		}
		if scDesc.GetName() != schema.name {
			return nil, errors.AssertionFailedf("schema name mismatch for virtual schema %d: expected %s, found %s",
				schemaID, schema.name, scDesc.GetName())
		}
		defs := make(map[string]*virtualDefEntry, len(schema.tableDefs))
		orderedDefNames := make([]string, 0, len(schema.tableDefs))

		for id, def := range schema.tableDefs {
			tableDesc, err := def.initVirtualTableDesc(ctx, st, scDesc, id)

			if err != nil {
				return nil, errors.NewAssertionErrorWithWrappedErrf(err,
					"failed to initialize %s", errors.Safe(def.getSchema()))
			}

			if schema.tableValidator != nil {
				if err := schema.tableValidator(&tableDesc); err != nil {
					return nil, errors.NewAssertionErrorWithWrappedErrf(err, "programmer error")
				}
			}
			td := tabledesc.NewBuilder(&tableDesc).BuildImmutableTable()
			if err := catalog.ValidateSelf(td); err != nil {
				return nil, errors.NewAssertionErrorWithWrappedErrf(err,
					"failed to validate virtual table %s: programmer error", errors.Safe(td.GetName()))
			}

			entry := &virtualDefEntry{
				virtualDef:                 def,
				desc:                       td,
				validWithNoDatabaseContext: schema.validWithNoDatabaseContext,
				comment:                    def.getComment(),
				unimplemented:              def.isUnimplemented(),
			}
			defs[tableDesc.Name] = entry
			vs.defsByID[tableDesc.ID] = entry
			orderedDefNames = append(orderedDefNames, tableDesc.Name)
		}

		sort.Strings(orderedDefNames)

		vse := &virtualSchemaEntry{
			desc:            scDesc,
			defs:            defs,
			orderedDefNames: orderedDefNames,
			undefinedTables: schema.undefinedTables,
			containsTypes:   schema.containsTypes,
		}
		vs.schemasByName[scDesc.GetName()] = vse
		vs.schemasByID[scDesc.GetID()] = vse
		vs.orderedNames[order] = scDesc.GetName()
		order++
	}
	sort.Strings(vs.orderedNames)
	return vs, nil
}

func newUnimplementedVirtualTableError(schema, tableName string) error {
	return unimplemented.Newf(
		fmt.Sprintf("%s.%s", schema, tableName),
		virtualSchemaNotImplementedMessage,
		schema,
		tableName,
	)
}

// getEntries is part of the VirtualTabler interface.
func (vs *VirtualSchemaHolder) getSchemas() map[string]*virtualSchemaEntry {
	return vs.schemasByName
}

// getSchemaNames is part of the VirtualTabler interface.
func (vs *VirtualSchemaHolder) getSchemaNames() []string {
	return vs.orderedNames
}

// getVirtualSchemaEntry retrieves a virtual schema entry given a database name.
// getVirtualSchemaEntry is part of the VirtualTabler interface.
func (vs *VirtualSchemaHolder) getVirtualSchemaEntry(name string) (*virtualSchemaEntry, bool) {
	e, ok := vs.schemasByName[name]
	return e, ok
}

// getVirtualTableEntry checks if the provided name matches a virtual database/table
// pair. The function will return the table's virtual table entry if the name matches
// a specific table. It will return an error if the name references a virtual database
// but the table is non-existent.
// getVirtualTableEntry is part of the VirtualTabler interface.
func (vs *VirtualSchemaHolder) getVirtualTableEntry(tn *tree.TableName) (*virtualDefEntry, error) {
	if db, ok := vs.getVirtualSchemaEntry(tn.Schema()); ok {
		tableName := tn.Table()
		if t, ok := db.defs[tableName]; ok {
			sqltelemetry.IncrementGetVirtualTableEntry(tn.Schema(), tableName)
			return t, nil
		}
		if _, ok := db.undefinedTables[tableName]; ok {
			return nil, unimplemented.NewWithIssueDetailf(
				8675,
				fmt.Sprintf("%s.%s", tn.Schema(), tableName),
				virtualSchemaNotImplementedMessage,
				tn.Schema(),
				tableName,
			)
		}
		return nil, sqlerrors.NewUndefinedRelationError(tn)
	}
	return nil, nil
}

// VirtualTabler is used to fetch descriptors for virtual tables and databases.
type VirtualTabler interface {
	getVirtualTableDesc(tn *tree.TableName) (catalog.TableDescriptor, error)
	getVirtualTableEntry(tn *tree.TableName) (*virtualDefEntry, error)
	getSchemas() map[string]*virtualSchemaEntry
	getSchemaNames() []string
}

// getVirtualTableDesc checks if the provided name matches a virtual database/table
// pair, and returns its descriptor if it does.
// getVirtualTableDesc is part of the VirtualTabler interface.
func (vs *VirtualSchemaHolder) getVirtualTableDesc(
	tn *tree.TableName,
) (catalog.TableDescriptor, error) {
	t, err := vs.getVirtualTableEntry(tn)
	if err != nil || t == nil {
		return nil, err
	}
	return t.desc, nil
}
