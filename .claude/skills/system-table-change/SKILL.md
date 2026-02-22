---
name: system-table-change
description: Checklist for modifying system table schemas in CockroachDB. Covers schema definitions, migrations, version gates, golden files, and test hashes. Use when adding, removing, or modifying columns/indexes on system tables.
---

# System Table Schema Change Checklist

When adding, removing, or modifying columns or indexes on a system table, multiple files and golden test artifacts must be updated in lockstep. Missing any of these causes test failures that can be confusing to debug.

## 1. Schema Definition

Update the table's schema string and descriptor literal in:

- **`pkg/sql/catalog/systemschema/system.go`**
  - The `CREATE TABLE` schema string (e.g., `StatementDiagnosticsRequestsTableSchema`)
  - The descriptor literal (e.g., `StatementDiagnosticsRequestsTable`): columns, column IDs, family column names/IDs, index store column names/IDs, `NextColumnID`

## 2. Version Gate

If the change requires a migration (any change to an existing table):

- **`pkg/clusterversion/cockroach_versions.go`**
  - Add a new version constant (e.g., `V26_2_MyChange`)
  - Add the version mapping in `versionsSingleton` (must use even `Internal` values, incrementing by 2)

- **`pkg/sql/catalog/systemschema/system.go`**
  - Update `SystemDatabaseSchemaBootstrapVersion` to your new version constant

## 3. Migration

- **`pkg/upgrade/upgrades/`**
  - Create a migration file (e.g., `v26_2_my_change.go`) with the `ALTER TABLE` / `CREATE INDEX` operations
  - Create a migration test file (e.g., `v26_2_my_change_test.go`) with the old descriptor and validation
  - Register the migration in `upgrades.go`
  - Add the old table descriptor constructor to `schema_changes.go` if needed
  - Update `helpers_test.go` if adding new helper functions
  - Update `BUILD.bazel` if adding new files

## 4. Golden Files (must regenerate, not manually edit)

These files contain serialized representations of the schema and must be regenerated after schema changes. Update hashes first, then run tests with `--rewrite`.

### Bootstrap test data

- **`pkg/sql/catalog/bootstrap/testdata/testdata`**
  - Contains `system hash=<sha256>` and `tenant hash=<sha256>` followed by KV data
  - **Update pattern**: Run the test once, grep for "Unexpected hash" in the output to get both new hashes, update both hashes in the file, then re-run with `--rewrite` to regenerate the KV data
  - Test: `./dev test pkg/sql/catalog/bootstrap -f TestInitialValuesToString -v --rewrite`

### Bootstrap schema golden files

- **`pkg/sql/catalog/systemschema_test/testdata/bootstrap_system`**
- **`pkg/sql/catalog/systemschema_test/testdata/bootstrap_tenant`**
  - Contain SQL schema output and JSON descriptor blobs
  - Test: `./dev test pkg/sql/catalog/systemschema_test -f TestValidateSystemSchemaAfterBootStrap -v --rewrite`

### Logic test golden files

- **`pkg/sql/logictest/testdata/logic_test/pg_catalog`** — pg_catalog column metadata
- **`pkg/sql/logictest/testdata/logic_test/crdb_internal_catalog`** — internal catalog metadata
  - Test: `./dev testlogic --files=pg_catalog --rewrite`
  - Test: `./dev testlogic --files=crdb_internal_catalog --rewrite`

### Previous-release bootstrap data (only if changing an existing release's schema)

- **`pkg/sql/catalog/bootstrap/data/{version}_system.keys`**
- **`pkg/sql/catalog/bootstrap/data/{version}_system.sha256`**
- **`pkg/sql/catalog/bootstrap/data/{version}_tenant.keys`**
- **`pkg/sql/catalog/bootstrap/data/{version}_tenant.sha256`**
  - These are for hardcoded previous release versions, NOT the current `Latest`
  - Only need updating if you're modifying a released schema (rare)

## 5. Runtime Version Gating

If the schema change adds a column/index used at runtime, gate usage on the version:

```go
if r.st.Version.IsActive(ctx, clusterversion.V26_2_MyChange) {
    // Use the new column/index
}
```

This ensures mixed-version clusters work during rolling upgrades.

## 6. Verification Tests

Run these tests to verify everything is consistent:

```bash
# Bootstrap golden files and hashes
./dev test pkg/sql/catalog/bootstrap -v

# Schema validation
./dev test pkg/sql/catalog/systemschema_test -v

# Migration
./dev test pkg/upgrade/upgrades -f TestMyMigration -v

# Logic tests (pg_catalog, crdb_internal_catalog)
./dev testlogic --files=pg_catalog
./dev testlogic --files=crdb_internal_catalog

# Package-specific tests
./dev test pkg/my/package -v
```

## 7. Rebase Conflicts

Golden files frequently conflict during rebases because multiple PRs change system tables concurrently. The resolution pattern:

1. Accept either side of the conflict (content doesn't matter)
2. Regenerate by running the tests with `--rewrite`
3. Update hashes in `bootstrap/testdata/testdata` (run test, grep "Unexpected hash", update, re-run with `--rewrite`)

Never try to manually merge golden file content — always regenerate.

## Common Mistakes

- **Forgetting the tenant hash**: `bootstrap/testdata/testdata` has TWO hashes (system and tenant). Both need updating.
- **Forgetting `pg_catalog`**: Column metadata changes show up here. Easy to miss.
- **Not incrementing `NextColumnID`**: Must be max(column IDs) + 1.
- **Not updating family column lists**: When adding/removing columns, update both `ColumnNames` and `ColumnIDs` in the family descriptor.
- **Not updating index STORING clauses**: If the column should be stored in a secondary index, update both the schema string and the descriptor literal's `StoreColumnNames`/`StoreColumnIDs`.
