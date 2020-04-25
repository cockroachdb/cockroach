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
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

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
	name           string
	allTableNames  map[string]struct{}
	tableDefs      map[sqlbase.ID]virtualSchemaDef
	tableValidator func(*sqlbase.TableDescriptor) error // optional
	// Some virtual tables can be used if there is no current database set; others can't.
	validWithNoDatabaseContext bool
}

// virtualSchemaDef represents the interface of a table definition within a virtualSchema.
type virtualSchemaDef interface {
	getSchema() string
	initVirtualTableDesc(
		ctx context.Context, st *cluster.Settings, parentSchemaID, id sqlbase.ID,
	) (sqlbase.TableDescriptor, error)
	getComment() string
}

type virtualIndex struct {
	// populate populates the table given the constraint. matched is true if any
	// rows were generated.
	populate func(ctx context.Context, constraint tree.Datum, p *planner, db *DatabaseDescriptor,
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
	populate func(ctx context.Context, p *planner, db *DatabaseDescriptor, addRow func(...tree.Datum) error) error

	// indexes, if non empty, is a slice of populate methods that also take a
	// constraint, only generating rows that match the constraint. The order of
	// indexes must match the order of the index definitions in the virtual table's
	// schema.
	indexes []virtualIndex

	// generator, if non-nil, is a function that is used when creating a
	// virtualTableNode. This function returns a virtualTableGenerator function
	// which generates the next row of the virtual table when called.
	generator func(ctx context.Context, p *planner, db *DatabaseDescriptor) (virtualTableGenerator, cleanupFunc, error)
}

// virtualSchemaView represents a view within a virtualSchema
type virtualSchemaView struct {
	schema        string
	resultColumns sqlbase.ResultColumns
}

// getSchema is part of the virtualSchemaDef interface.
func (t virtualSchemaTable) getSchema() string {
	return t.schema
}

// initVirtualTableDesc is part of the virtualSchemaDef interface.
func (t virtualSchemaTable) initVirtualTableDesc(
	ctx context.Context, st *cluster.Settings, parentSchemaID, id sqlbase.ID,
) (sqlbase.TableDescriptor, error) {
	stmt, err := parser.ParseOne(t.schema)
	if err != nil {
		return sqlbase.TableDescriptor{}, err
	}

	create := stmt.AST.(*tree.CreateTable)
	var firstColDef *tree.ColumnTableDef
	for _, def := range create.Defs {
		if d, ok := def.(*tree.ColumnTableDef); ok {
			if d.HasDefaultExpr() {
				return sqlbase.TableDescriptor{},
					errors.Errorf("virtual tables are not allowed to use default exprs "+
						"because bootstrapping: %s:%s", &create.Table, d.Name)
			}
			if firstColDef == nil {
				firstColDef = d
			}
		}
		if _, ok := def.(*tree.UniqueConstraintTableDef); ok {
			return sqlbase.TableDescriptor{},
				errors.Errorf("virtual tables are not allowed to have unique constraints")
		}
	}
	if firstColDef == nil {
		return sqlbase.TableDescriptor{},
			errors.Errorf("can't have empty virtual tables")
	}

	// Virtual tables never use SERIAL so we need not process SERIAL
	// types here.
	mutDesc, err := MakeTableDesc(
		ctx,
		nil, /* txn */
		nil, /* vt */
		st,
		create,
		0, /* parentID */
		parentSchemaID,
		id,
		hlc.Timestamp{}, /* creationTime */
		publicSelectPrivileges,
		nil,                        /* affected */
		nil,                        /* semaCtx */
		nil,                        /* evalCtx */
		&sessiondata.SessionData{}, /* sessionData */
		false,                      /* temporary */
	)
	if err != nil {
		return mutDesc.TableDescriptor, err
	}
	for i := range mutDesc.Indexes {
		idx := &mutDesc.Indexes[i]
		if len(idx.ColumnIDs) > 1 {
			panic("we don't know how to deal with virtual composite indexes yet")
		}
		// All indexes of virtual tables automatically STORE all other columns in
		// the table.
		idx.StoreColumnIDs = make([]sqlbase.ColumnID, len(mutDesc.Columns)-len(idx.ColumnIDs))
		idx.StoreColumnNames = make([]string, len(mutDesc.Columns)-len(idx.ColumnIDs))
		// Store all columns but the ones in the index.
		outputIdx := 0
	EACHCOLUMN:
		for j := range mutDesc.Columns {
			for _, id := range idx.ColumnIDs {
				if mutDesc.Columns[j].ID == id {
					// Skip columns in the index.
					continue EACHCOLUMN
				}
			}
			idx.StoreColumnIDs[outputIdx] = mutDesc.Columns[j].ID
			idx.StoreColumnNames[outputIdx] = mutDesc.Columns[j].Name
			outputIdx++
		}
	}
	return mutDesc.TableDescriptor, nil
}

// getComment is part of the virtualSchemaDef interface.
func (t virtualSchemaTable) getComment() string {
	return t.comment
}

// getSchema is part of the virtualSchemaDef interface.
func (v virtualSchemaView) getSchema() string {
	return v.schema
}

// initVirtualTableDesc is part of the virtualSchemaDef interface.
func (v virtualSchemaView) initVirtualTableDesc(
	ctx context.Context, st *cluster.Settings, parentSchemaID sqlbase.ID, id sqlbase.ID,
) (sqlbase.TableDescriptor, error) {
	stmt, err := parser.ParseOne(v.schema)
	if err != nil {
		return sqlbase.TableDescriptor{}, err
	}

	create := stmt.AST.(*tree.CreateView)

	columns := v.resultColumns
	if len(create.ColumnNames) != 0 {
		columns = overrideColumnNames(columns, create.ColumnNames)
	}
	mutDesc, err := makeViewTableDesc(
		create.Name.Table(),
		tree.AsStringWithFlags(create.AsSource, tree.FmtParsable),
		0, /* parentID */
		parentSchemaID,
		id,
		columns,
		hlc.Timestamp{}, /* creationTime */
		publicSelectPrivileges,
		nil,   /* semaCtx */
		nil,   /* evalCtx */
		false, /* temporary */
	)
	return mutDesc.TableDescriptor, err
}

// getComment is part of the virtualSchemaDef interface.
func (v virtualSchemaView) getComment() string {
	return ""
}

// virtualSchemas holds a slice of statically registered virtualSchema objects.
//
// When adding a new virtualSchema, define a virtualSchema in a separate file, and
// add that object to this slice.
var virtualSchemas = map[sqlbase.ID]virtualSchema{
	sqlbase.InformationSchemaID: informationSchema,
	sqlbase.PgCatalogID:         pgCatalog,
	sqlbase.CrdbInternalID:      crdbInternal,
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
	entries      map[string]virtualSchemaEntry
	defsByID     map[sqlbase.ID]*virtualDefEntry
	orderedNames []string
}

type virtualSchemaEntry struct {
	desc            *sqlbase.DatabaseDescriptor
	defs            map[string]virtualDefEntry
	orderedDefNames []string
	allTableNames   map[string]struct{}
}

type virtualDefEntry struct {
	virtualDef                 virtualSchemaDef
	desc                       *sqlbase.TableDescriptor
	comment                    string
	validWithNoDatabaseContext bool
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

// getPlanInfo returns the column metadata and a constructor for a new
// valuesNode for the virtual table. We use deferred construction here
// so as to avoid populating a RowContainer during query preparation,
// where we can't guarantee it will be Close()d in case of error.
func (e virtualDefEntry) getPlanInfo(
	table *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	idxConstraint *constraint.Constraint,
) (sqlbase.ResultColumns, virtualTableConstructor) {
	var columns sqlbase.ResultColumns
	for i := range e.desc.Columns {
		col := &e.desc.Columns[i]
		columns = append(columns, sqlbase.ResultColumn{
			Name: col.Name,
			Typ:  &col.Type,
		})
	}

	constructor := func(ctx context.Context, p *planner, dbName string) (planNode, error) {
		var dbDesc *DatabaseDescriptor
		if dbName != "" {
			var err error
			dbDesc, err = p.LogicalSchemaAccessor().GetDatabaseDesc(ctx, p.txn,
				dbName, tree.DatabaseLookupFlags{Required: true, AvoidCached: p.avoidCachedDescriptors})
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
				next, cleanup, err := def.generator(ctx, p, dbDesc)
				if err != nil {
					return nil, err
				}
				return p.newVirtualTableNode(columns, 0, next, cleanup), nil
			}

			v := p.newContainerValuesNode(columns, 0)

			constrainedScan := idxConstraint != nil && !idxConstraint.IsUnconstrained()

			validateRow := func(datums ...tree.Datum) {
				if r, c := len(datums), len(v.columns); r != c {
					log.Fatalf(ctx, "datum row count and column count differ: %d vs %d", r, c)
				}
				for i, col := range v.columns {
					datum := datums[i]
					if datum == tree.DNull {
						if !e.desc.Columns[i].Nullable {
							log.Fatalf(ctx, "column %s.%s not nullable, but found NULL value",
								e.desc.Name, col.Name)
						}
					} else if !datum.ResolvedType().Equivalent(col.Typ) {
						log.Fatalf(ctx, "datum column %q expected to be type %s; found type %s",
							col.Name, col.Typ, datum.ResolvedType())
					}
				}
			}
			if !constrainedScan {
				// If we don't have a constrained scan, just populate all the rows and
				// we're done.
				addRow := func(datums ...tree.Datum) error {
					validateRow(datums...)
					_, err := v.rows.AddRow(ctx, datums)
					return err
				}
				if err := def.populate(ctx, p, dbDesc, addRow); err != nil {
					v.Close(ctx)
					return nil, err
				}
				return v, nil
			}

			// We are now dealing with a constrained virtual index scan.

			if index.ID == 1 {
				return nil, errors.AssertionFailedf(
					"programming error: can't constrain scan on primary virtual index of table %s", e.desc.Name)
			}

			// Figure out the ordinal position of the column that we're filtering on.
			// TODO(jordan): there is probably a better way to do this.
			constraintColumnOrd := table.ColumnIdxMap()[index.ColumnIDs[0]]
			addRowIfPassesFilter := func(datums ...tree.Datum) error {
				validateRow(datums...)
				matchedFilter := false
				// If we have a constraint, apply it as a filter. If the filter passes,
				// or we had no filter at all, add the datums to the row container.
				datumToFilter := datums[constraintColumnOrd]
				for i := 0; i < idxConstraint.Spans.Count(); i++ {
					span := idxConstraint.Spans.Get(i)
					if span.HasSingleKey(p.EvalContext()) {
						matchedFilter = datumToFilter.Compare(p.EvalContext(), span.StartKey().Value(0)) == 0
						if matchedFilter {
							break
						}
					} else {
						// Check both boundaries - the datum must be greater than the
						// startKey, and less than the endKey.
						var matchedStart, matchedEnd bool
						if span.StartKey().IsEmpty() {
							matchedStart = true
						} else {
							startCmp := datumToFilter.Compare(p.EvalContext(), span.StartKey().Value(0))
							if span.StartBoundary() == constraint.IncludeBoundary {
								matchedStart = startCmp >= 0
							} else {
								matchedStart = startCmp > 0
							}
						}
						if span.EndKey().IsEmpty() {
							matchedEnd = true
						} else {
							endCmp := datumToFilter.Compare(p.EvalContext(), span.EndKey().Value(0))
							if span.EndBoundary() == constraint.IncludeBoundary {
								matchedEnd = endCmp <= 0
							} else {
								matchedEnd = endCmp < 0
							}
						}
						matchedFilter = matchedStart && matchedEnd
						if matchedFilter {
							break
						}
					}
				}

				var err error
				if matchedFilter {
					_, err = v.rows.AddRow(ctx, datums)
				}
				return err
			}

			giveUp := false

			// We have a virtual index with a constraint. Run the constrained
			// populate routine for every span. If for some reason we can't use the
			// index for a span, we set the giveUp flag to true, which will populate
			// the whole table, filtering the output.
			for i := 0; i < idxConstraint.Spans.Count(); i++ {
				span := idxConstraint.Spans.Get(i)
				if span.StartKey().Length() > 1 {
					return nil, errors.AssertionFailedf(
						"programming error: can't push down composite constraints into vtables")
				}
				if !span.HasSingleKey(p.EvalContext()) {
					// No hope - we can't deal with range scans on virtual indexes.
					giveUp = true
					break
				}
				constraintDatum := span.StartKey().Value(0)
				// Subtract 2 from the index id to get the ordinal in def.indexes, since
				// the index with ID 1 is the "primary" index defined by def.populate.
				virtualIndex := def.indexes[index.ID-2]

				// For each span, run populate.
				found, err := virtualIndex.populate(ctx, constraintDatum, p, dbDesc,
					addRowIfPassesFilter)
				if err != nil {
					v.Close(ctx)
					return nil, err
				}
				if !found && virtualIndex.partial {
					// If we found nothing, and the index was partial, we have no choice
					// but to populate the entire table and search through it.
					giveUp = true
					break
				}
			}
			if !giveUp {
				return v, nil
			}

			// Fall back to populating the whole table - clear what we have so far
			// and start over with a full populate and filter.
			v.rows.Clear(ctx)
			if err := def.populate(ctx, p, dbDesc, addRowIfPassesFilter); err != nil {
				v.Close(ctx)
				return nil, err
			}

			return v, nil
		default:
			return nil, newInvalidVirtualDefEntryError()
		}
	}

	return columns, constructor
}

// NewVirtualSchemaHolder creates a new VirtualSchemaHolder.
func NewVirtualSchemaHolder(
	ctx context.Context, st *cluster.Settings,
) (*VirtualSchemaHolder, error) {
	vs := &VirtualSchemaHolder{
		entries:      make(map[string]virtualSchemaEntry, len(virtualSchemas)),
		orderedNames: make([]string, len(virtualSchemas)),
		defsByID:     make(map[sqlbase.ID]*virtualDefEntry, math.MaxUint32-sqlbase.MinVirtualID),
	}

	order := 0
	for schemaID, schema := range virtualSchemas {
		dbName := schema.name
		dbDesc := initVirtualDatabaseDesc(schemaID, dbName)
		defs := make(map[string]virtualDefEntry, len(schema.tableDefs))
		orderedDefNames := make([]string, 0, len(schema.tableDefs))

		for id, def := range schema.tableDefs {
			tableDesc, err := def.initVirtualTableDesc(ctx, st, schemaID, id)

			if err != nil {
				return nil, errors.NewAssertionErrorWithWrappedErrf(err,
					"failed to initialize %s", errors.Safe(def.getSchema()))
			}

			if schema.tableValidator != nil {
				if err := schema.tableValidator(&tableDesc); err != nil {
					return nil, errors.NewAssertionErrorWithWrappedErrf(err, "programmer error")
				}
			}

			entry := virtualDefEntry{
				virtualDef:                 def,
				desc:                       &tableDesc,
				validWithNoDatabaseContext: schema.validWithNoDatabaseContext,
				comment:                    def.getComment(),
			}
			defs[tableDesc.Name] = entry
			vs.defsByID[tableDesc.ID] = &entry
			orderedDefNames = append(orderedDefNames, tableDesc.Name)
		}

		sort.Strings(orderedDefNames)

		vs.entries[dbName] = virtualSchemaEntry{
			desc:            dbDesc,
			defs:            defs,
			orderedDefNames: orderedDefNames,
			allTableNames:   schema.allTableNames,
		}
		vs.orderedNames[order] = dbName
		order++
	}
	sort.Strings(vs.orderedNames)
	return vs, nil
}

// Virtual databases and tables each have SELECT privileges for "public", which includes
// all users. However, virtual schemas have more fine-grained access control.
// For instance, information_schema will only expose rows to a given user which that
// user has access to.
var publicSelectPrivileges = sqlbase.NewPrivilegeDescriptor(sqlbase.PublicRole, privilege.List{privilege.SELECT})

func initVirtualDatabaseDesc(id sqlbase.ID, name string) *sqlbase.DatabaseDescriptor {
	return &sqlbase.DatabaseDescriptor{
		Name:       name,
		ID:         id,
		Privileges: publicSelectPrivileges,
	}
}

// getEntries is part of the VirtualTabler interface.
func (vs *VirtualSchemaHolder) getEntries() map[string]virtualSchemaEntry {
	return vs.entries
}

// getSchemaNames is part of the VirtualTabler interface.
func (vs *VirtualSchemaHolder) getSchemaNames() []string {
	return vs.orderedNames
}

// getVirtualSchemaEntry retrieves a virtual schema entry given a database name.
// getVirtualSchemaEntry is part of the VirtualTabler interface.
func (vs *VirtualSchemaHolder) getVirtualSchemaEntry(name string) (virtualSchemaEntry, bool) {
	e, ok := vs.entries[name]
	return e, ok
}

// getVirtualTableEntry checks if the provided name matches a virtual database/table
// pair. The function will return the table's virtual table entry if the name matches
// a specific table. It will return an error if the name references a virtual database
// but the table is non-existent.
// getVirtualTableEntry is part of the VirtualTabler interface.
func (vs *VirtualSchemaHolder) getVirtualTableEntry(tn *tree.TableName) (virtualDefEntry, error) {
	if db, ok := vs.getVirtualSchemaEntry(tn.Schema()); ok {
		tableName := tn.Table()
		if t, ok := db.defs[tableName]; ok {
			return t, nil
		}
		if _, ok := db.allTableNames[tableName]; ok {
			return virtualDefEntry{}, unimplemented.NewWithIssueDetailf(8675,
				tn.Schema()+"."+tableName,
				"virtual schema table not implemented: %s.%s", tn.Schema(), tableName)
		}
		return virtualDefEntry{}, sqlbase.NewUndefinedRelationError(tn)
	}
	return virtualDefEntry{}, nil
}

func (vs *VirtualSchemaHolder) getVirtualTableEntryByID(id sqlbase.ID) (virtualDefEntry, error) {
	entry, ok := vs.defsByID[id]
	if !ok {
		return virtualDefEntry{}, sqlbase.ErrDescriptorNotFound
	}
	return *entry, nil
}

// VirtualTabler is used to fetch descriptors for virtual tables and databases.
type VirtualTabler interface {
	getVirtualTableDesc(tn *tree.TableName) (*sqlbase.TableDescriptor, error)
	getVirtualSchemaEntry(name string) (virtualSchemaEntry, bool)
	getVirtualTableEntry(tn *tree.TableName) (virtualDefEntry, error)
	getVirtualTableEntryByID(id sqlbase.ID) (virtualDefEntry, error)
	getEntries() map[string]virtualSchemaEntry
	getSchemaNames() []string
}

// getVirtualTableDesc checks if the provided name matches a virtual database/table
// pair, and returns its descriptor if it does.
// getVirtualTableDesc is part of the VirtualTabler interface.
func (vs *VirtualSchemaHolder) getVirtualTableDesc(
	tn *tree.TableName,
) (*sqlbase.TableDescriptor, error) {
	t, err := vs.getVirtualTableEntry(tn)
	if err != nil {
		return nil, err
	}
	return t.desc, nil
}
