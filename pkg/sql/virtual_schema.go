// Copyright 2016 The Cockroach Authors.
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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	tables         []virtualSchemaTable
	tableValidator func(*sqlbase.TableDescriptor) error // optional
	// Some virtual tables can be used if there is no current database set; others can't.
	validWithNoDatabaseContext bool
}

// virtualSchemaTable represents a table within a virtualSchema.
type virtualSchemaTable struct {
	schema   string
	populate func(ctx context.Context, p *planner, db *DatabaseDescriptor, addRow func(...tree.Datum) error) error
}

// virtualSchemas holds a slice of statically registered virtualSchema objects.
//
// When adding a new virtualSchema, define a virtualSchema in a separate file, and
// add that object to this slice.
var virtualSchemas = []virtualSchema{
	informationSchema,
	pgCatalog,
	crdbInternal,
}

//
// SQL-layer interface to work with virtual schemas.
//

// VirtualSchemaHolder is a type used to provide convenient access to virtual
// database and table descriptors. VirtualSchemaHolder, virtualSchemaEntry,
// and virtualTableEntry make up the generated data structure which the
// virtualSchemas slice is mapped to. Because of this, they should not be
// created directly, but instead will be populated in a post-startup hook
// on an Executor.
type VirtualSchemaHolder struct {
	entries      map[string]virtualSchemaEntry
	orderedNames []string
}

type virtualSchemaEntry struct {
	desc              *sqlbase.DatabaseDescriptor
	tables            map[string]virtualTableEntry
	orderedTableNames []string
}

type virtualTableEntry struct {
	tableDef                   virtualSchemaTable
	desc                       *sqlbase.TableDescriptor
	validWithNoDatabaseContext bool
}

type virtualTableConstructor func(context.Context, *planner, string) (planNode, error)

// getPlanInfo returns the column metadata and a constructor for a new
// valuesNode for the virtual table. We use deferred construction here
// so as to avoid populating a RowContainer during query preparation,
// where we can't guarantee it will be Close()d in case of error.
func (e virtualTableEntry) getPlanInfo(
	ctx context.Context,
) (sqlbase.ResultColumns, virtualTableConstructor) {
	var columns sqlbase.ResultColumns
	for _, col := range e.desc.Columns {
		columns = append(columns, sqlbase.ResultColumn{
			Name: col.Name,
			Typ:  col.Type.ToDatumType(),
		})
	}

	constructor := func(ctx context.Context, p *planner, dbName string) (planNode, error) {
		var dbDesc *DatabaseDescriptor
		if dbName != "" {
			var err error
			dbDesc, err = p.LogicalSchemaAccessor().GetDatabaseDesc(dbName,
				DatabaseLookupFlags{ctx: ctx, txn: p.Txn(), required: true})
			if err != nil {
				return nil, err
			}
		} else {
			if !e.validWithNoDatabaseContext {
				return nil, errNoDatabase
			}
		}

		v := p.newContainerValuesNode(columns, 0)

		if err := e.tableDef.populate(ctx, p, dbDesc, func(datums ...tree.Datum) error {
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
			_, err := v.rows.AddRow(ctx, datums)
			return err
		}); err != nil {
			v.Close(ctx)
			return nil, err
		}
		return v, nil
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
	}
	for i, schema := range virtualSchemas {
		dbName := schema.name
		dbDesc := initVirtualDatabaseDesc(dbName)
		tables := make(map[string]virtualTableEntry, len(schema.tables))
		orderedTableNames := make([]string, 0, len(schema.tables))
		for _, table := range schema.tables {
			tableDesc, err := initVirtualTableDesc(ctx, table, st)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to initialize %s", table.schema)
			}

			if schema.tableValidator != nil {
				if err := schema.tableValidator(&tableDesc); err != nil {
					return nil, errors.Wrap(err, "programmer error")
				}
			}
			tables[tableDesc.Name] = virtualTableEntry{
				tableDef: table,
				desc:     &tableDesc,
				validWithNoDatabaseContext: schema.validWithNoDatabaseContext,
			}
			orderedTableNames = append(orderedTableNames, tableDesc.Name)
		}
		sort.Strings(orderedTableNames)
		vs.entries[dbName] = virtualSchemaEntry{
			desc:              dbDesc,
			tables:            tables,
			orderedTableNames: orderedTableNames,
		}
		vs.orderedNames[i] = dbName
	}
	sort.Strings(vs.orderedNames)
	return vs, nil
}

// Virtual databases and tables each have an empty set of privileges. In practice,
// all users have SELECT privileges on the database/tables, but this is handled
// separately from normal SELECT privileges, because the virtual schemas need more
// fine-grained access control. For instance, information_schema will only expose
// rows to a given user which that user has access to.
var emptyPrivileges = &sqlbase.PrivilegeDescriptor{}

func initVirtualDatabaseDesc(name string) *sqlbase.DatabaseDescriptor {
	return &sqlbase.DatabaseDescriptor{
		Name:       name,
		ID:         keys.VirtualDescriptorID,
		Privileges: emptyPrivileges,
	}
}

func initVirtualTableDesc(
	ctx context.Context, t virtualSchemaTable, st *cluster.Settings,
) (sqlbase.TableDescriptor, error) {
	stmt, err := parser.ParseOne(t.schema)
	if err != nil {
		return sqlbase.TableDescriptor{}, err
	}

	create := stmt.(*tree.CreateTable)
	for _, def := range create.Defs {
		if d, ok := def.(*tree.ColumnTableDef); ok && d.HasDefaultExpr() {
			return sqlbase.TableDescriptor{},
				errors.Errorf("virtual tables are not allowed to use default exprs "+
					"because bootstrapping: %s:%s", create.Table, d.Name)
		}
	}

	return MakeTableDesc(
		ctx,
		nil, /* txn */
		nil, /* vt */
		st,
		create,
		0, /* parentID */
		keys.VirtualDescriptorID,
		hlc.Timestamp{}, /* creationTime */
		emptyPrivileges,
		nil, /* affected */
		nil, /* semaCtx */
		nil, /* evalCtx */
	)
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
func (vs *VirtualSchemaHolder) getVirtualTableEntry(tn *tree.TableName) (virtualTableEntry, error) {
	if db, ok := vs.getVirtualSchemaEntry(tn.Schema()); ok {
		if t, ok := db.tables[tn.Table()]; ok {
			return t, nil
		}
		return virtualTableEntry{}, sqlbase.NewUndefinedRelationError(tn)
	}
	return virtualTableEntry{}, nil
}

// VirtualTabler is used to fetch descriptors for virtual tables and databases.
type VirtualTabler interface {
	getVirtualTableDesc(tn *tree.TableName) (*sqlbase.TableDescriptor, error)
	getVirtualSchemaEntry(name string) (virtualSchemaEntry, bool)
	getVirtualTableEntry(tn *tree.TableName) (virtualTableEntry, error)
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

// isVirtualDescriptor checks if the provided DescriptorProto is an instance of
// a Virtual Descriptor.
func isVirtualDescriptor(desc sqlbase.DescriptorProto) bool {
	return desc.GetID() == keys.VirtualDescriptorID
}
