---
name: system-table-change
description: Use when adding, removing, or modifying columns/indexes on system tables. Provides a checklist covering schema definitions, migrations, version gates, golden files, and test hashes.
---

# System Table Schema Change Checklist

When adding, removing, or modifying columns or indexes on a system table, multiple files and golden test artifacts must be updated in lockstep. Missing any of these causes test failures that can be confusing to debug.

## 1. Schema Definition

Update the table's schema string and descriptor literal in:

- **`pkg/sql/catalog/systemschema/system.go`**
  - The `CREATE TABLE` schema string (e.g., `StatementDiagnosticsRequestsTableSchema`)
  - The descriptor literal (e.g., `StatementDiagnosticsRequestsTable`): columns, column IDs, family column names/IDs, index store column names/IDs, `NextColumnID`
  - `NextColumnID` must be max(column IDs) + 1
  - When adding/removing columns, update both `ColumnNames` and `ColumnIDs` in the family descriptor
  - If the column should be stored in a secondary index, update both the schema string's `STORING` clause and the descriptor literal's `StoreColumnNames`/`StoreColumnIDs`

## 2. New Table Registration (skip if modifying existing table)

If creating a brand new system table, register it in these additional files:

- **`pkg/sql/sem/catconstants/constants.go`**
  - Add a SystemTableName constant for the new table

- **`pkg/sql/catalog/catprivilege/system.go`**
  - Register privileges (read-write, read-only, etc.) for the table

- **`pkg/sql/catalog/bootstrap/metadata.go`**
  - Add the table descriptor to the bootstrap metadata

- **`pkg/backup/system_schema.go`**
  - Define backup/restore behavior for the table

- **`pkg/cli/zip_table_registry.go`**
  - Decide whether the table should be included in `cockroach debug zip` output and register it if so

## 3. Version Gate

- **`pkg/clusterversion/cockroach_versions.go`**
  - Add a new version constant (e.g., `V26_2_MyChange`)
  - Add the version mapping in `versionTable` (must use even `Internal` values, incrementing by 2)

- **`pkg/sql/catalog/systemschema/system.go`**
  - Update `SystemDatabaseSchemaBootstrapVersion` to your new version constant

## 4. Migration

- **`pkg/upgrade/upgrades/`**
  - Create a migration file (e.g., `v26_2_my_change.go`). For new tables, this uses `CREATE TABLE`. For existing tables, this uses `ALTER TABLE` / `CREATE INDEX`.
  - Create a migration test file (e.g., `v26_2_my_change_test.go`) with the old descriptor and validation
  - Register the migration in `upgrades.go`
  - Add the old table descriptor constructor to `schema_changes.go` if needed (existing tables only)
  - Update `helpers_test.go` if adding new helper functions
  - Run `./dev gen bazel` if adding new files

## 5. Golden Files (must regenerate, not manually edit)

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

### Initial bootstrap keys and catalog cache test data

- **`pkg/sql/tests/testdata/initial_keys`** — initial bootstrap keys
  - Test: `./dev test pkg/sql/tests -f TestInitialKeys -v --rewrite`

- **`pkg/sql/catalog/internal/catkv/testdata/testdata_app`** — catalog cache test data for app tenant
- **`pkg/sql/catalog/internal/catkv/testdata/testdata_system`** — catalog cache test data for system tenant
  - Test: `./dev test pkg/sql/catalog/internal/catkv -v --rewrite`

### Additional golden files for new tables

When adding a new system table, these additional golden files may need regeneration:

- **`pkg/sql/logictest/testdata/logic_test/information_schema`** — information_schema metadata
  - Test: `./dev testlogic --files=information_schema --rewrite`

- **`pkg/sql/logictest/testdata/logic_test/system`** — system table tests
  - Test: `./dev testlogic --files=system --rewrite`

- **`pkg/cli/testdata/doctor/test_examine_cluster*`** — cluster doctor examination test data
  - Test: `./dev test pkg/cli -f TestDoctor -v --rewrite`

- **`pkg/ccl/spanconfigccl/spanconfigreconcilerccl/testdata/`** — span config reconciler test data
  - Test: `./dev test pkg/ccl/spanconfigccl/spanconfigreconcilerccl -v --rewrite`

### Previous-release bootstrap data (only if changing an existing release's schema)

- **`pkg/sql/catalog/bootstrap/data/{version}_system.keys`**
- **`pkg/sql/catalog/bootstrap/data/{version}_system.sha256`**
- **`pkg/sql/catalog/bootstrap/data/{version}_tenant.keys`**
- **`pkg/sql/catalog/bootstrap/data/{version}_tenant.sha256`**
  - These are for hardcoded previous release versions, NOT the current `Latest`
  - Only need updating if you're modifying a released schema (rare)

## 6. Documentation

Adding a version constant changes the `version` cluster setting's default value, which is reflected in generated settings docs. Regenerate with:

```bash
./dev gen docs
```

## 7. Runtime Version Gating

If the schema change adds a column/index used at runtime, gate usage on the version:

```go
if settings.Version.IsActive(ctx, clusterversion.V26_2_MyChange) {
    // Use the new column/index
}
```

This ensures mixed-version clusters work during rolling upgrades.

## 8. Verification

Re-run the tests from Section 5 without `--rewrite` to confirm everything passes. Also run your migration test:

```bash
./dev test pkg/upgrade/upgrades -f TestMyMigration -v
```

## 9. Rebase Conflicts

Golden files frequently conflict during rebases because multiple PRs change system tables concurrently. The resolution pattern:

1. Accept either side of the conflict (content doesn't matter)
2. Regenerate by running the tests with `--rewrite`
3. Update hashes in `bootstrap/testdata/testdata` (run test, grep "Unexpected hash", update, re-run with `--rewrite`)

Never try to manually merge golden file content — always regenerate.
