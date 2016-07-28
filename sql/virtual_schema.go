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
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
)

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
	populate func(*planner) planNode
}

// virtualSchemas holds a slice of statically registered virtualSchema objects.
//
// When adding a new virtualSchema, define a virtualSchema in a separate file, and
// add that object to this slice.
var virtualSchemas = []virtualSchema{
	informationSchema,
}

// Statically populated map used to provide convenient access to virtual
// database and table descriptors.
var virtualSchemaMap map[string]virtualSchemaEntry

type virtualSchemaEntry struct {
	desc   *sqlbase.DatabaseDescriptor
	tables map[string]*sqlbase.TableDescriptor
}

func (e virtualSchemaEntry) tableNames() (parser.QualifiedNames, error) {
	var qualifiedNames parser.QualifiedNames
	for tableName := range e.tables {
		qname, err := parser.NewQualifiedNameFromDBAndTable(e.desc.Name, tableName)
		if err != nil {
			return nil, err
		}
		qualifiedNames = append(qualifiedNames, qname)
	}
	return qualifiedNames, nil
}

func init() {
	virtualSchemaMap = make(map[string]virtualSchemaEntry)
	for _, schema := range virtualSchemas {
		dbName := schema.name
		dbDesc := initVirtualDatabaseDesc(dbName)
		tables := make(map[string]*sqlbase.TableDescriptor)
		for _, table := range schema.tables {
			tableDesc := initVirtualTableDesc(table)
			tables[tableDesc.Name] = tableDesc
		}
		virtualSchemaMap[dbName] = virtualSchemaEntry{
			desc:   dbDesc,
			tables: tables,
		}
	}
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

func initVirtualTableDesc(t virtualSchemaTable) *sqlbase.TableDescriptor {
	stmt, err := parser.ParseOneTraditional(t.schema)
	if err != nil {
		panic(err)
	}
	desc, err := sqlbase.MakeTableDesc(stmt.(*parser.CreateTable), 0)
	if err != nil {
		panic(err)
	}
	desc.ID = keys.VirtualDescriptorID
	desc.Privileges = emptyPrivileges
	return &desc
}

// getVirtualSchemaEntry retrieves a virtual schema entry given a database name.
func getVirtualSchemaEntry(name string) (virtualSchemaEntry, bool) {
	e, ok := virtualSchemaMap[name]
	return e, ok
}

// getVirtualDatabaseDesc checks if the provided name matches a virtual database,
// and if so, returns that database's descriptor.
func getVirtualDatabaseDesc(name string) *sqlbase.DatabaseDescriptor {
	if e, ok := getVirtualSchemaEntry(name); ok {
		return e.desc
	}
	return nil
}

// isVirtualDatabase checks if the provided name corresponds to a virtual database.
func isVirtualDatabase(name string) bool {
	_, ok := getVirtualSchemaEntry(name)
	return ok
}

// checkVirtualTableDesc checks if the provided qname matches a virtual database/table
// pair. The function will return the table's descriptor if the qname matches a specific
// table. It will also return a flag signifying if the qname matched a database. This can
// be used to differentiate if the qname is not referencing a virtual database or if the
// qname is referencing a non-existent table within an existing virtual database.
func checkVirtualTableDesc(qname *parser.QualifiedName) (*sqlbase.TableDescriptor, bool) {
	if e, ok := getVirtualSchemaEntry(qname.Database()); ok {
		return e.tables[qname.Table()], true
	}
	return nil, false
}

// isVirtualDescriptor checks if the provided DescriptorProto is an instance of
// a Virtual Descriptor.
func isVirtualDescriptor(desc sqlbase.DescriptorProto) bool {
	return desc.GetID() == keys.VirtualDescriptorID
}
