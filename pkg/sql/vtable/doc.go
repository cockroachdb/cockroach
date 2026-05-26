// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package vtable holds schema definitions for CockroachDB's virtual tables.
// Virtual tables are system-provided tables that do not store data on disk;
// instead, their rows are generated on the fly from in-memory metadata. The
// three virtual schemas that use this package are pg_catalog,
// information_schema, and crdb_internal.
//
// Each schema definition is a CREATE TABLE string (e.g. PGCatalogClass,
// InformationSchemaColumns) that describes the table's columns and types. The
// populate logic that fills these tables with rows lives in pkg/sql/, not here.
// This separation keeps the schema declarations self-contained and easy to
// diff against upstream Postgres definitions.
//
// # Why pg_catalog Compatibility Matters
//
// pg_catalog is PostgreSQL's built-in system catalog. A large ecosystem of
// tools relies on it for introspection:
//
//   - ORMs such as Django, SQLAlchemy, ActiveRecord, and Prisma query
//     pg_catalog to discover table structure, column types, and constraints.
//   - GUI tools like pgAdmin, DBeaver, and DataGrip read pg_catalog to
//     populate object browsers and auto-complete.
//   - Drivers including JDBC, psycopg2, and pgx use pg_catalog to resolve
//     type OIDs and prepare statements.
//   - Migration frameworks query pg_catalog to detect existing schema before
//     applying changes.
//
// CockroachDB advertises a specific server_version (the PgServerVersion
// constant in pkg/sql/vars.go). Clients use this version to decide which catalog
// queries to send. When a table or column is missing, the result ranges from
// silent data bugs (an ORM silently skipping a column) to hard failures (a
// driver aborting connection setup).
//
// # Postgres Matching Strategy
//
// The goal is to match the pg_catalog schema for the Postgres version that
// CockroachDB advertises. Virtual tables fall into three categories:
//
//   - Fully implemented tables have a populate function that returns real
//     data. Examples include pg_class, pg_attribute, and pg_type. These are
//     the tables most commonly queried by the ecosystem.
//   - Stub tables are defined with unimplemented: true and return empty
//     result sets. They exist so that queries against rarely-used catalog
//     tables do not fail outright. Stubs are only visible when the
//     stub_catalog_tables session variable is enabled, which is the default.
//   - Undefined tables are listed in the undefinedTables field of the
//     virtualSchema struct. Querying one of these returns an
//     "unimplemented" error, signaling that the table is known but not yet
//     supported.
//
// New tables are typically added when bumping the advertised Postgres version.
// Tables that correspond to Postgres-specific features CockroachDB does not
// support (e.g. physical replication slots) can remain stubs indefinitely.
//
// # How Virtual Tables Work
//
// Virtual tables are implemented using three types defined in
// pkg/sql/virtual_schema.go:
//
//   - virtualSchema groups a set of virtual table and view definitions under
//     a schema name (e.g. "pg_catalog"). It also carries the undefinedTables
//     set and an optional table validator.
//   - virtualSchemaTable represents a single virtual table. It holds the
//     CREATE TABLE schema string from this package, an optional comment, and
//     one of two row-production strategies: a populate function or a
//     generator function.
//   - virtualSchemaView represents a virtual view, defined by a SQL query
//     rather than a populate function.
//
// The populate function eagerly loads every row into a valuesNode when the
// table is first scanned. This approach is simple but can use significant
// memory for large catalogs. The generator function streams rows lazily,
// producing one row at a time; it is preferred for tables that may return
// many rows.
//
// Virtual tables are database-scoped: the same virtual schema appears in
// every database, but its contents reflect only the objects visible in that
// database. This matters for pg_catalog tables like pg_class, which list
// tables and indexes for the current database.
//
// # Virtual Indexes
//
// Some virtual tables define a virtualIndex that enables constrained lookups,
// typically by OID. Without a virtual index, every query against the table
// must call the populate or generator function to scan all rows.
//
// A virtual index has two modes:
//
//   - A complete index can satisfy all possible constraint values. If the
//     index lookup returns no rows, the result is definitively empty.
//   - An incomplete index can satisfy some lookups but not all. For example,
//     the pg_class virtual index can look up tables by descriptor ID, but
//     indexes use hashed OIDs that cannot be reversed into a descriptor
//     lookup. When an incomplete index lookup returns no rows, the system
//     falls back to a full table scan.
//
// OID hashing is performed by the oidHasher type (in pkg/sql/pg_catalog.go),
// which uses FNV-32 to produce deterministic OIDs for database objects that
// do not have a native descriptor ID (e.g. indexes, columns, constraints).
// The IsMaybeHashedOid function checks whether a given OID falls in the
// hashed range, which is used to decide whether an incomplete index lookup
// can be trusted.
//
// When adding virtual indexes, use the
// makeAllRelationsVirtualTableWithDescriptorIDIndex helper for tables that
// iterate over all relations and support lookup by descriptor ID.
//
// # The pg_catalog Diff Tool
//
// CockroachDB includes tooling to compare its pg_catalog schema against a
// real Postgres instance:
//
//   - pkg/cmd/generate-metadata-tables connects to a Postgres server and
//     dumps its catalog schema to a JSON file.
//   - TestDiffTool in pkg/sql/pg_metadata_test.go compares the dumped
//     Postgres schema against the vtable definitions and reports missing
//     tables, missing columns, and type mismatches.
//   - Expected differences are recorded in
//     testdata/postgres_test_expected_diffs_on_pg_catalog.json. Intentional
//     divergences (e.g. columns CockroachDB does not plan to support) are
//     listed here so that TestDiffTool does not flag them.
//
// Useful flags for TestDiffTool:
//
//   - --rewrite-diffs regenerates the expected-diffs JSON file from the
//     current comparison output.
//   - --add-missing-tables adds stub definitions for tables present in
//     Postgres but absent from CockroachDB.
//   - --catalog selects which catalog to diff (default pg_catalog).
//   - --rdbms selects the reference database system.
//
// Step-by-step workflow for bumping the advertised Postgres version:
//
//  1. Stand up a Postgres instance at the target version.
//  2. Run generate-metadata-tables against it to produce a new JSON dump.
//  3. Run TestDiffTool to see the full set of differences.
//  4. Add new tables and columns to the vtable definitions as needed.
//  5. Run TestDiffTool --rewrite-diffs to update the expected-diffs file.
//  6. Run TestDiffTool again to verify no unexpected differences remain.
//  7. Update PgServerVersion and PgServerVersionNum in pkg/sql/vars.go.
//
// # Key Files
//
// Schema definitions (this package):
//
//   - pg_catalog.go: pg_catalog table schemas (PGCatalogClass, etc.)
//   - information_schema.go: information_schema table schemas
//   - crdb_internal.go: crdb_internal table schemas
//
// Populate logic and virtual schema registration (pkg/sql/):
//
//   - virtual_schema.go: virtualSchema, virtualSchemaTable, virtualSchemaView
//     types and the virtual schema infrastructure
//   - pg_catalog.go: pg_catalog populate functions, oidHasher, virtual index
//     definitions
//   - information_schema.go: information_schema populate functions
//   - crdb_internal.go: crdb_internal populate functions
//   - vars.go: PgServerVersion and PgServerVersionNum constants
//
// Diff tooling (pkg/cmd/ and pkg/sql/):
//
//   - pkg/cmd/generate-metadata-tables/main.go: dumps Postgres catalog
//     schema to JSON
//   - pkg/sql/pg_metadata_test.go: TestDiffTool and schema comparison logic
//   - pkg/sql/testdata/postgres_test_expected_diffs_on_pg_catalog.json:
//     expected differences
package vtable
