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

package testcat

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/vtable"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var informationSchemaMap = map[string]*tree.CreateTable{}

var informationSchemaTables = []string{
	vtable.InformationSchemaColumns,
	vtable.InformationSchemaAdministrableRoleAuthorizations,
	vtable.InformationSchemaApplicableRoles,
	vtable.InformationSchemaColumnPrivileges,
	vtable.InformationSchemaSchemata,
	vtable.InformationSchemaTables,
}

func init() {
	// Build a map that maps the names of the various information_schema tables
	// to their CREATE TABLE AST.
	for _, table := range informationSchemaTables {
		parsed, err := parser.ParseOne(table)
		if err != nil {
			panic(fmt.Sprintf("error initializing virtual table map: %s", err))
		}

		ct, ok := parsed.(*tree.CreateTable)
		if !ok {
			panic("virtual table schemas must be CREATE TABLE statements")
		}

		ct.Table.SchemaName = tree.Name("information_schema")
		ct.Table.ExplicitSchema = true

		ct.Table.CatalogName = testDB
		ct.Table.ExplicitCatalog = true

		name := ct.Table
		informationSchemaMap[name.TableName.String()] = ct
	}
}

// Resolve returns true and the AST node describing the virtual table referenced.
// TODO(justin): make this complete for all virtual tables.
func resolveVTable(name *tree.TableName) (*tree.CreateTable, bool) {
	switch name.SchemaName {
	case "information_schema":
		schema, ok := informationSchemaMap[name.TableName.String()]
		return schema, ok
	}

	return nil, false
}
