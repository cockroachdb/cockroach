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
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
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

// Statically populated maps which are used to provide access to virtual
// database and table descriptors.
var virtualDatabaseDescs map[string]*sqlbase.DatabaseDescriptor
var virtualTableDescs map[string]map[string]*sqlbase.TableDescriptor

func init() {
	virtualDatabaseDescs = make(map[string]*sqlbase.DatabaseDescriptor)
	virtualTableDescs = make(map[string]map[string]*sqlbase.TableDescriptor)
	for _, schema := range virtualSchemas {
		dbName := schema.name
		virtualDatabaseDescs[dbName] = initVirtualDatabaseDesc(dbName)
		virtualTableDescMap := make(map[string]*sqlbase.TableDescriptor)
		for _, table := range schema.tables {
			tableDesc := initVirtualTableDesc(table)
			virtualTableDescMap[tableDesc.Name] = tableDesc
		}
		virtualTableDescs[dbName] = virtualTableDescMap
	}
}

func initVirtualDatabaseDesc(name string) *sqlbase.DatabaseDescriptor {
	return &sqlbase.DatabaseDescriptor{
		Name: name,
		// TODO(nvanbenschoten) All users should have Read-only access to virtual databases.
		// Access control will be provided by the population routines.
		Privileges: sqlbase.NewPrivilegeDescriptor(security.RootUser, privilege.VirtualData),
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
	// TODO(nvanbenschoten) All users should have Read-only access to virtual tables.
	// Access control will be provided by the population routines.
	desc.Privileges = sqlbase.NewPrivilegeDescriptor(security.RootUser, privilege.VirtualData)
	return &desc
}

// checkVirtualDatabaseDesc checks if the provided name matches a virtual database,
// and if so, returns that database's descriptor.
func checkVirtualDatabaseDesc(name string) *sqlbase.DatabaseDescriptor {
	return virtualDatabaseDescs[name]
}

// isVirtualDatabase checks if the provided name corresponds to a virtual database.
func isVirtualDatabase(name string) bool {
	return checkVirtualDatabaseDesc(name) != nil
}
