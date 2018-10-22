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

package vtable

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// InformationSchemaColumns describes the schema of the
// information_schema.columns table.
const InformationSchemaColumns = `
CREATE TABLE information_schema.columns (
	TABLE_CATALOG            STRING NOT NULL,
	TABLE_SCHEMA             STRING NOT NULL,
	TABLE_NAME               STRING NOT NULL,
	COLUMN_NAME              STRING NOT NULL,
	ORDINAL_POSITION         INT NOT NULL,
	COLUMN_DEFAULT           STRING,
	IS_NULLABLE              STRING NOT NULL,
	DATA_TYPE                STRING NOT NULL,
	CHARACTER_MAXIMUM_LENGTH INT,
	CHARACTER_OCTET_LENGTH   INT,
	NUMERIC_PRECISION        INT,
	NUMERIC_PRECISION_RADIX  INT,
	NUMERIC_SCALE            INT,
	DATETIME_PRECISION       INT,
	CHARACTER_SET_CATALOG    STRING,
	CHARACTER_SET_SCHEMA     STRING,
	CHARACTER_SET_NAME       STRING,
	GENERATION_EXPRESSION    STRING,          -- MySQL/CockroachDB extension.
	IS_HIDDEN                STRING NOT NULL, -- CockroachDB extension for SHOW COLUMNS / dump.
	CRDB_SQL_TYPE            STRING NOT NULL  -- CockroachDB extension for SHOW COLUMNS / dump.
);`

// InformationSchemaAdministrableRoleAuthorizations describes the schema of the
// information_schema.administrable_role_authorizations table.
const InformationSchemaAdministrableRoleAuthorizations = `
CREATE TABLE information_schema.administrable_role_authorizations (
	GRANTEE      STRING NOT NULL,
	ROLE_NAME    STRING NOT NULL,
	IS_GRANTABLE STRING NOT NULL
);`

// InformationSchemaApplicableRoles describes the schema of the
// information_schema.applicable_roles table.
const InformationSchemaApplicableRoles = `
CREATE TABLE information_schema.applicable_roles (
	GRANTEE      STRING NOT NULL,
	ROLE_NAME    STRING NOT NULL,
	IS_GRANTABLE STRING NOT NULL
);`

// InformationSchemaColumnPrivileges describes the schema of the
// information_schema.column_privileges table.
const InformationSchemaColumnPrivileges = `
CREATE TABLE information_schema.column_privileges (
	GRANTOR        STRING,
	GRANTEE        STRING NOT NULL,
	TABLE_CATALOG  STRING NOT NULL,
	TABLE_SCHEMA   STRING NOT NULL,
	TABLE_NAME     STRING NOT NULL,
	COLUMN_NAME    STRING NOT NULL,
	PRIVILEGE_TYPE STRING NOT NULL,
	IS_GRANTABLE   STRING
);`

// InformationSchemaSchemata describes the schema of the
// information_schema.schemata table.
const InformationSchemaSchemata = `
CREATE TABLE information_schema.schemata (
	CATALOG_NAME               STRING NOT NULL,
	SCHEMA_NAME                STRING NOT NULL,
	DEFAULT_CHARACTER_SET_NAME STRING,
	SQL_PATH                   STRING
);`

// InformationSchemaTables describes the schema of the
// information_schema.tables table.
const InformationSchemaTables = `
CREATE TABLE information_schema.tables (
	TABLE_CATALOG      STRING NOT NULL,
	TABLE_SCHEMA       STRING NOT NULL,
	TABLE_NAME         STRING NOT NULL,
	TABLE_TYPE         STRING NOT NULL,
	IS_INSERTABLE_INTO STRING NOT NULL,
	VERSION            INT
);`

var informationSchemaTables = []string{
	InformationSchemaColumns,
	InformationSchemaAdministrableRoleAuthorizations,
	InformationSchemaApplicableRoles,
	InformationSchemaColumnPrivileges,
	InformationSchemaSchemata,
	InformationSchemaTables,
}

var informationSchemaMap = map[string]*tree.CreateTable{}

func init() {
	// Build a map that maps the names of the various information_schema tables
	// to their CREATE TABLE AST.
	for _, table := range informationSchemaTables {
		parsed, err := parser.ParseOne(table)
		if err != nil {
			panic(fmt.Sprintf("error initializing virtual table map: %s", err))
		}

		name, err := parsed.(*tree.CreateTable).Table.Normalize()
		if err != nil {
			panic(fmt.Sprintf("error initializing virtual table map: %s", err))
		}

		ct, ok := parsed.(*tree.CreateTable)
		if !ok {
			panic("virtual table schemas must be CREATE TABLE statements")
		}

		informationSchemaMap[name.TableName.String()] = ct
	}
}

// Resolve returns true and the AST node describing the virtual table referenced.
// TODO(justin): make this complete for all virtual tables.
func Resolve(name *tree.TableName) (*tree.CreateTable, bool) {
	switch name.SchemaName {
	case "information_schema":
		schema, ok := informationSchemaMap[name.TableName.String()]
		return schema, ok
	}

	return nil, false
}
