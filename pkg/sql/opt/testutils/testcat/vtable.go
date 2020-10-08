// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testcat

import (
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/vtable"
	"github.com/cockroachdb/errors"
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
			panic(errors.Wrap(err, "error initializing virtual table map"))
		}

		ct, ok := parsed.AST.(*tree.CreateTable)
		if !ok {
			panic(errors.New("virtual table schemas must be CREATE TABLE statements"))
		}

		ct.Table.SchemaName = tree.Name("information_schema")
		ct.Table.ExplicitSchema = true

		ct.Table.CatalogName = testDB
		ct.Table.ExplicitCatalog = true

		name := ct.Table
		informationSchemaMap[name.ObjectName.String()] = ct
	}
}

// Resolve returns true and the AST node describing the virtual table referenced.
// TODO(justin): make this complete for all virtual tables.
func resolveVTable(name *tree.TableName) (*tree.CreateTable, bool) {
	switch name.SchemaName {
	case "information_schema":
		schema, ok := informationSchemaMap[name.ObjectName.String()]
		return schema, ok
	}

	return nil, false
}
