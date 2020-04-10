// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package vtable

// InformationSchemaColumns describes the schema of the
// information_schema.columns table.
// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-columns.html
// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/columns-table.html
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
	INTERVAL_TYPE            STRING,
	INTERVAL_PRECISION       INT,
	CHARACTER_SET_CATALOG    STRING,
	CHARACTER_SET_SCHEMA     STRING,
	CHARACTER_SET_NAME       STRING,
	COLLATION_CATALOG        STRING,
	COLLATION_SCHEMA         STRING,
	COLLATION_NAME           STRING,
	DOMAIN_CATALOG           STRING,
	DOMAIN_SCHEMA            STRING,
	DOMAIN_NAME              STRING,
	UDT_CATALOG              STRING,
	UDT_SCHEMA               STRING,
	UDT_NAME                 STRING,
	SCOPE_CATALOG            STRING,
	SCOPE_SCHEMA             STRING,
	SCOPE_NAME               STRING,
	MAXIMUM_CARDINALITY      INT,
	DTD_IDENTIFIER           STRING,
	IS_SELF_REFERENCING      STRING,
	IS_IDENTITY              STRING,
	IDENTITY_GENERATION      STRING,
	IDENTITY_START           STRING,
	IDENTITY_INCREMENT       STRING,
	IDENTITY_MAXIMUM         STRING,
	IDENTITY_MINIMUM         STRING,
	IDENTITY_CYCLE           STRING,
	IS_GENERATED             STRING,
	GENERATION_EXPRESSION    STRING,          -- MySQL/CockroachDB extension.
	IS_UPDATABLE             STRING,
	IS_HIDDEN                STRING NOT NULL, -- CockroachDB extension for SHOW COLUMNS / dump.
	CRDB_SQL_TYPE            STRING NOT NULL  -- CockroachDB extension for SHOW COLUMNS / dump.
)`

// InformationSchemaAdministrableRoleAuthorizations describes the schema of the
// information_schema.administrable_role_authorizations table.
// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-administrable-role-authorizations.html
// MySQL:    missing
const InformationSchemaAdministrableRoleAuthorizations = `
CREATE TABLE information_schema.administrable_role_authorizations (
	GRANTEE      STRING NOT NULL,
	ROLE_NAME    STRING NOT NULL,
	IS_GRANTABLE STRING NOT NULL
)`

// InformationSchemaApplicableRoles describes the schema of the
// information_schema.applicable_roles table.
// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-applicable-roles.html
// MySQL:    missing
const InformationSchemaApplicableRoles = `
CREATE TABLE information_schema.applicable_roles (
	GRANTEE      STRING NOT NULL,
	ROLE_NAME    STRING NOT NULL,
	IS_GRANTABLE STRING NOT NULL
)`

// InformationSchemaCheckConstraints describes the schema of the
// information_schema.check_constraints table.
// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-check-constraints.html
// MySQL:    missing
const InformationSchemaCheckConstraints = `
CREATE TABLE information_schema.check_constraints (
	CONSTRAINT_CATALOG STRING NOT NULL,
	CONSTRAINT_SCHEMA  STRING NOT NULL,
	CONSTRAINT_NAME    STRING NOT NULL,
	CHECK_CLAUSE       STRING NOT NULL
)`

// InformationSchemaColumnPrivileges describes the schema of the
// information_schema.column_privileges table.
// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-column-privileges.html
// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/column-privileges-table.html
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
)`

// InformationSchemaSchemata describes the schema of the
// information_schema.schemata table.
const InformationSchemaSchemata = `
CREATE TABLE information_schema.schemata (
	CATALOG_NAME               STRING NOT NULL,
	SCHEMA_NAME                STRING NOT NULL,
	DEFAULT_CHARACTER_SET_NAME STRING,
	SQL_PATH                   STRING
)`

// InformationSchemaTables describes the schema of the
// information_schema.tables table.
// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-tables.html
// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/tables-table.html
const InformationSchemaTables = `
CREATE TABLE information_schema.tables (
	TABLE_CATALOG      STRING NOT NULL,
	TABLE_SCHEMA       STRING NOT NULL,
	TABLE_NAME         STRING NOT NULL,
	TABLE_TYPE         STRING NOT NULL,
	IS_INSERTABLE_INTO STRING NOT NULL,
	VERSION            INT,
  INDEX(TABLE_NAME)
)`
