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
//
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package sql

import (
	"fmt"
	"sort"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
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
	name   string
	tables []virtualSchemaTable
}

// virtualSchemaTable represents a table within a virtualSchema.
type virtualSchemaTable struct {
	schema   string
	populate func(ctx context.Context, p *planner, addRow func(...parser.Datum) error) error
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

// virtualSchemaHolder is a type used to provide convenient access to virtual
// database and table descriptors. virtualSchemaHolder, virtualSchemaEntry,
// and virtualTableEntry make up the generated data structure which the
// virtualSchemas slice is mapped to. Because of this, they should not be
// created directly, but instead will be populated in a post-startup hook
// on an Executor.
type virtualSchemaHolder struct {
	entries      map[string]virtualSchemaEntry
	orderedNames []string
}

type virtualSchemaEntry struct {
	desc              *sqlbase.DatabaseDescriptor
	tables            map[string]virtualTableEntry
	orderedTableNames []string
}

func (e virtualSchemaEntry) tableNames() parser.TableNames {
	var res parser.TableNames
	for _, tableName := range e.orderedTableNames {
		tn := parser.TableName{
			DatabaseName: parser.Name(e.desc.Name),
			TableName:    parser.Name(tableName),
		}
		res = append(res, tn)
	}
	return res
}

type virtualTableEntry struct {
	tableDef virtualSchemaTable
	desc     *sqlbase.TableDescriptor
}

// getPlanInfo returns the column metadata and a constructor for a new
// valuesNode for the virtual table. We use deferred construction here
// so as to avoid populating a RowContainer during query preparation,
// where we can't guarantee it will be Close()d in case of error.
func (e virtualTableEntry) getPlanInfo(
	ctx context.Context,
) (sqlbase.ResultColumns, nodeConstructor) {
	var columns sqlbase.ResultColumns
	for _, col := range e.desc.Columns {
		columns = append(columns, sqlbase.ResultColumn{
			Name: col.Name,
			Typ:  col.Type.ToDatumType(),
		})
	}

	constructor := func(ctx context.Context, p *planner) (planNode, error) {
		v := p.newContainerValuesNode(columns, 0)

		err := e.tableDef.populate(ctx, p, func(datums ...parser.Datum) error {
			if r, c := len(datums), len(v.columns); r != c {
				panic(fmt.Sprintf("datum row count and column count differ: %d vs %d", r, c))
			}
			for i, col := range v.columns {
				datum := datums[i]
				if !(datum == parser.DNull || datum.ResolvedType().Equivalent(col.Typ)) {
					panic(fmt.Sprintf("datum column %q expected to be type %s; found type %s",
						col.Name, col.Typ, datum.ResolvedType()))
				}
			}
			_, err := v.rows.AddRow(ctx, datums)
			return err
		})
		if err != nil {
			v.Close(ctx)
			return nil, err
		}
		return v, nil
	}

	return columns, constructor
}

func (vs *virtualSchemaHolder) init(ctx context.Context, p *planner) error {
	*vs = virtualSchemaHolder{
		entries:      make(map[string]virtualSchemaEntry, len(virtualSchemas)),
		orderedNames: make([]string, len(virtualSchemas)),
	}
	for i, schema := range virtualSchemas {
		dbName := schema.name
		dbDesc := initVirtualDatabaseDesc(dbName)
		tables := make(map[string]virtualTableEntry, len(schema.tables))
		orderedTableNames := make([]string, 0, len(schema.tables))
		for _, table := range schema.tables {
			tableDesc, err := initVirtualTableDesc(ctx, p, table)
			if err != nil {
				return err
			}
			tables[tableDesc.Name] = virtualTableEntry{
				tableDef: table,
				desc:     &tableDesc,
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
	return nil
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
	ctx context.Context, p *planner, t virtualSchemaTable,
) (sqlbase.TableDescriptor, error) {
	stmt, err := parser.ParseOne(t.schema)
	if err != nil {
		return sqlbase.TableDescriptor{}, err
	}
	create := stmt.(*parser.CreateTable)
	return p.makeTableDesc(ctx, create, 0, keys.VirtualDescriptorID, emptyPrivileges, nil)
}

// getVirtualSchemaEntry retrieves a virtual schema entry given a database name.
func (vs *virtualSchemaHolder) getVirtualSchemaEntry(name string) (virtualSchemaEntry, bool) {
	if vs == nil {
		return virtualSchemaEntry{}, false
	}
	e, ok := vs.entries[name]
	return e, ok
}

// getVirtualDatabaseDesc checks if the provided name matches a virtual database,
// and if so, returns that database's descriptor.
func (vs *virtualSchemaHolder) getVirtualDatabaseDesc(name string) *sqlbase.DatabaseDescriptor {
	if e, ok := vs.getVirtualSchemaEntry(name); ok {
		return e.desc
	}
	return nil
}

// isVirtualDatabase checks if the provided name corresponds to a virtual database.
func (vs *virtualSchemaHolder) isVirtualDatabase(name string) bool {
	_, ok := vs.getVirtualSchemaEntry(name)
	return ok
}

// IsVirtualDatabase checks if the provided name corresponds to a virtual database,
// exposing this information on the Executor object itself.
func (e *Executor) IsVirtualDatabase(name string) bool {
	return e.virtualSchemas.isVirtualDatabase(name)
}

// getVirtualTableEntry checks if the provided name matches a virtual database/table
// pair. The function will return the table's virtual table entry if the name matches
// a specific table. It will return an error if the name references a virtual database
// but the table is non-existent.
func (vs *virtualSchemaHolder) getVirtualTableEntry(
	tn *parser.TableName,
) (virtualTableEntry, error) {
	if db, ok := vs.getVirtualSchemaEntry(tn.DatabaseName.Normalize()); ok {
		if t, ok := db.tables[tn.TableName.Normalize()]; ok {
			return t, nil
		}
		return virtualTableEntry{}, sqlbase.NewUndefinedTableError(tn.String())
	}
	return virtualTableEntry{}, nil
}

// VirtualTabler is used to fetch descriptors for virtual tables and databases.
type VirtualTabler interface {
	getVirtualTableDesc(tn *parser.TableName) (*sqlbase.TableDescriptor, error)
	getVirtualDatabaseDesc(name string) *sqlbase.DatabaseDescriptor
	getVirtualSchemaEntry(name string) (virtualSchemaEntry, bool)
}

// getVirtualTableDesc checks if the provided name matches a virtual database/table
// pair, and returns its descriptor if it does.
func (vs *virtualSchemaHolder) getVirtualTableDesc(
	tn *parser.TableName,
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

// NilVirtualTabler implements VirtualTabler that returns nil.
var NilVirtualTabler nilVirtualTabler

type nilVirtualTabler struct{}

var _ VirtualTabler = nilVirtualTabler{}

func (nilVirtualTabler) getVirtualTableDesc(
	tn *parser.TableName,
) (*sqlbase.TableDescriptor, error) {
	return nil, nil
}

func (nilVirtualTabler) getVirtualDatabaseDesc(name string) *sqlbase.DatabaseDescriptor {
	return nil
}

func (nilVirtualTabler) getVirtualSchemaEntry(name string) (virtualSchemaEntry, bool) {
	return virtualSchemaEntry{}, false
}
