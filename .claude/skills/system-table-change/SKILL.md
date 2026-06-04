---
name: system-table-change
description: Use when adding, removing, or modifying columns/indexes on system tables. Provides a checklist covering schema definitions, migrations, version gates, golden files, and test hashes.
---

# System Table Schema Change Checklist

When adding, removing, or modifying columns or indexes on a system table, multiple files and golden test artifacts must be updated in lockstep. Missing any of these causes test failures that can be confusing to debug.

## Quick Rebase Conflict Checklist

**Use this when rebasing a system table PR onto a master branch that includes other system table changes.**

System table PRs frequently conflict during rebase because:
- Version constants use sequential Internal numbers
- Golden files serialize the entire schema state
- Bootstrap hashes change when any system table changes

After resolving git conflicts in code files, follow this checklist:

1. **Fix version conflicts in `pkg/clusterversion/cockroach_versions.go`**:
   - Update your version constant's `Internal` number to the next available even number
   - Keep both version constants (yours and the one from master)
   - Example: If master added a version at Internal: 30, yours should be 32

2. **Update `SystemDatabaseSchemaBootstrapVersion` in `pkg/sql/catalog/systemschema/system.go`**:
   - Set it to your migration's version constant (not the one from master)

3. **For golden file conflicts, accept either side** (content doesn't matter - you'll regenerate):
   - `pkg/sql/catalog/bootstrap/testdata/testdata`
   - `pkg/sql/catalog/systemschema_test/testdata/bootstrap_system`
   - `pkg/sql/catalog/systemschema_test/testdata/bootstrap_tenant`
   - `pkg/sql/logictest/testdata/logic_test/crdb_internal_catalog`
   - `pkg/sql/logictest/testdata/logic_test/pg_catalog`

4. **Run `./dev generate`** to regenerate all code and docs:
   ```bash
   ./dev generate
   ```

5. **Update bootstrap test hashes** (critical step - must be done before --rewrite):
   ```bash
   # Run test to see hash mismatch errors
   ./dev test pkg/sql/catalog/bootstrap -f TestInitialValuesToString -v

   # Look for lines like:
   #   Unexpected hash value abc123... for system.
   #   Unexpected hash value def456... for tenant.

   # Update BOTH hashes in pkg/sql/catalog/bootstrap/testdata/testdata:
   #   system hash=abc123...
   #   tenant hash=def456...

   # Now run with --rewrite to regenerate the KV data
   ./dev test pkg/sql/catalog/bootstrap -f TestInitialValuesToString --rewrite
   ```

6. **Regenerate all golden files** (see Section 5 for complete list):
   ```bash
   # Schema golden files
   ./dev test pkg/sql/catalog/systemschema_test --rewrite

   # Logic test files (requires COCKROACH_WORKSPACE)
   COCKROACH_WORKSPACE=$PWD ./dev test pkg/sql/logictest/tests/local -f TestLogic_crdb_internal_catalog --rewrite
   COCKROACH_WORKSPACE=$PWD ./dev test pkg/sql/logictest/tests/local -f TestLogic_pg_catalog --rewrite
   ```

7. **Verify all tests pass** (run without --rewrite):
   ```bash
   ./dev test pkg/sql/catalog/bootstrap -f TestInitialValuesToString
   ./dev test pkg/sql/catalog/systemschema_test
   ./dev test pkg/upgrade/upgrades -f TestYourMigration
   ```

8. **Squash fixup commits**: If you created separate commits for golden file updates during the rebase, squash them into your migration commit where they belong.

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

These files contain serialized representations of the schema and must be regenerated after schema changes.

**CRITICAL**: For bootstrap testdata, you MUST update hashes before running `--rewrite`. The hash is in the test input, not the output, so `--rewrite` won't update it.

### Bootstrap test data

- **`pkg/sql/catalog/bootstrap/testdata/testdata`**
  - Contains `system hash=<sha256>` and `tenant hash=<sha256>` followed by KV data
  - **Hash-then-rewrite pattern** (MUST be done in this order):
    ```bash
    # Step 1: Run test WITHOUT --rewrite to get new hashes
    ./dev test pkg/sql/catalog/bootstrap -f TestInitialValuesToString -v

    # Step 2: Look for "Unexpected hash value" errors in output
    #   Example: Unexpected hash value abc123... for system.
    #            Unexpected hash value def456... for tenant.

    # Step 3: Manually update BOTH hashes in the file:
    #   system hash=abc123...
    #   tenant hash=def456...

    # Step 4: NOW run with --rewrite to regenerate KV data
    ./dev test pkg/sql/catalog/bootstrap -f TestInitialValuesToString --rewrite

    # Step 5: Verify it passes
    ./dev test pkg/sql/catalog/bootstrap -f TestInitialValuesToString
    ```

### Bootstrap schema golden files

- **`pkg/sql/catalog/systemschema_test/testdata/bootstrap_system`**
- **`pkg/sql/catalog/systemschema_test/testdata/bootstrap_tenant`**
  - Contain SQL schema output and JSON descriptor blobs
  - Test: `./dev test pkg/sql/catalog/systemschema_test -f TestValidateSystemSchemaAfterBootStrap -v --rewrite`

### Logic test golden files

- **`pkg/sql/logictest/testdata/logic_test/pg_catalog`** — pg_catalog column metadata
- **`pkg/sql/logictest/testdata/logic_test/crdb_internal_catalog`** — internal catalog metadata
  - These require `COCKROACH_WORKSPACE` environment variable to enable `--rewrite`:
    ```bash
    COCKROACH_WORKSPACE=$PWD ./dev test pkg/sql/logictest/tests/local -f TestLogic_pg_catalog --rewrite
    COCKROACH_WORKSPACE=$PWD ./dev test pkg/sql/logictest/tests/local -f TestLogic_crdb_internal_catalog --rewrite
    ```

### Initial bootstrap keys and catalog cache test data

- **`pkg/sql/tests/testdata/initial_keys`** — initial bootstrap keys
  - Test: `./dev test pkg/sql/tests -f TestInitialKeys -v --rewrite`

- **`pkg/sql/catalog/internal/catkv/testdata/testdata_app`** — catalog cache test data for app tenant
- **`pkg/sql/catalog/internal/catkv/testdata/testdata_system`** — catalog cache test data for system tenant
  - Test: `./dev test pkg/sql/catalog/internal/catkv -v --rewrite`

### Additional golden files for new tables

When adding a new system table, these additional golden files may need regeneration:

- **`pkg/sql/logictest/testdata/logic_test/information_schema`** — information_schema metadata
- **`pkg/sql/logictest/testdata/logic_test/system`** — system table tests
  - These also require `COCKROACH_WORKSPACE`:
    ```bash
    COCKROACH_WORKSPACE=$PWD ./dev test pkg/sql/logictest/tests/local -f TestLogic_information_schema --rewrite
    COCKROACH_WORKSPACE=$PWD ./dev test pkg/sql/logictest/tests/local -f TestLogic_system --rewrite
    ```

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

**See the "Quick Rebase Checklist" section at the top of this document for the complete rebase workflow.**

Golden files frequently conflict during rebases because multiple PRs change system tables concurrently.

**Key principles**:
- Accept either side of golden file conflicts (content doesn't matter - you'll regenerate)
- Never try to manually merge golden file content
- Always update hashes BEFORE running `--rewrite` (hash-then-rewrite pattern)
- Regenerate all golden files, not just the ones with conflicts
- Squash golden file fixup commits into your migration commit
