# CockroachDB Release Preparation Guide

This document provides step-by-step instructions for preparing the CockroachDB source tree for beta releases, specifically following the R.1: Prepare for beta checklist from the clusterversion README.md.

## Beta Release Preparation (R.1 Checklist)

When preparing for a beta release (e.g., 25.4 beta1), the following files must be updated:

### 1. Core Version Changes

**Set Development Branch Flag:**
- File: `pkg/clusterversion/cockroach_versions.go`
- Change: Set `const DevelopmentBranch = false` (line ~334)

**Update Version String:**
- File: `pkg/build/version.txt`
- Change: Update from alpha to beta version (e.g., `v25.4.0-alpha.2` → `v25.4.0-beta.1`)

### 2. Regenerate Documentation

Run the following command to update generated documentation:
```bash
./dev gen docs
```

This updates:
- `docs/generated/settings/settings-for-tenants.txt`
- `docs/generated/settings/settings.html`

### 3. Update Test Data

**System Schema Tests:**
Run this command to regenerate bootstrap test data:
```bash
./dev test pkg/sql/catalog/systemschema_test --rewrite
```

This updates:
- `pkg/sql/catalog/systemschema_test/testdata/bootstrap_system`
- `pkg/sql/catalog/systemschema_test/testdata/bootstrap_tenant`

**Bootstrap Hash Test:**
Run this command and manually update hash values if the test fails:
```bash
./dev test pkg/sql/catalog/bootstrap --rewrite -f TestInitialValuesToString
```

If the rewrite fails, manually update the hash values in:
- `pkg/sql/catalog/bootstrap/testdata/testdata`
  - Update `system hash=` value
  - Update `tenant hash=` value
  - Update binary data values as shown in test output

### 4. CLI Test Data Updates

**Declarative Rules Tests:**
- File: `pkg/cli/testdata/declarative-rules/deprules`
  - Change: Update version reference to current release branch (e.g., `debug declarative-print-rules 25.2 dep` → `debug declarative-print-rules 25.3 dep`)

- File: `pkg/cli/testdata/declarative-rules/invalid_version`
  - Change: Update supported versions list to reflect current releases (use short format like `25.2`, `25.3` not `1000025.x`)

### 5. Logic Test Updates

**Internal Catalog Test:**
- File: `pkg/sql/logictest/testdata/logic_test/crdb_internal_catalog`
- Change: Update `majorVal` from `1000025` to `25` in systemDatabaseSchemaVersion entries

## Example PR Reference

For reference, see PR #148382 which demonstrates the complete set of changes needed for beta release preparation.

## Expected File Count

A typical beta release preparation should modify approximately 10-12 files:
1. `pkg/clusterversion/cockroach_versions.go`
2. `pkg/build/version.txt`
3. `docs/generated/settings/settings-for-tenants.txt`
4. `docs/generated/settings/settings.html`
5. `pkg/sql/catalog/systemschema_test/testdata/bootstrap_system`
6. `pkg/sql/catalog/systemschema_test/testdata/bootstrap_tenant`
7. `pkg/sql/catalog/bootstrap/testdata/testdata`
8. `pkg/cli/testdata/declarative-rules/deprules`
9. `pkg/cli/testdata/declarative-rules/invalid_version`
10. `pkg/sql/logictest/testdata/logic_test/crdb_internal_catalog`

## Verification

After making all changes:
1. Run the bootstrap test to ensure it passes: `./dev test pkg/sql/catalog/bootstrap -f TestInitialValuesToString`
2. Check git status to verify expected number of modified files
3. All tests should pass before proceeding with the release

## Notes

- Some test data files contain binary encoded data that may change when version numbers are updated
- Hash values in bootstrap test data are expected to change when the development branch flag is modified
- The logic test updates are necessary to reflect the new major version in system database schema version metadata
- CLI declarative rules tests use short version format (e.g., `25.3`) not the long format (e.g., `1000025.3`)
- Always run the CLI tests after updating declarative rules files to ensure correct version format

---

## R.2: Mint Release (Creating Final Version)

This change finalizes the cluster version for the release. It should be done when you are absolutely sure that no additional version gates are needed - right before cutting the first RC (typically rc.1).

**Important timing note:** The minting happens before the final v25.X.0 release. It's typically done when preparing rc.1, which is shipped before the final release.

### Critical Step: Update SystemDatabaseSchemaBootstrapVersion

**IMPORTANT:** This is the most commonly missed step and will cause test failures if forgotten.

**File:** `pkg/sql/catalog/systemschema/system.go` (line ~1445)

**Change:** Update from the last internal version to the final minted version:

```go
// Before (last internal version - e.g., V25_4_AddSystemStatementHintsTable)
var SystemDatabaseSchemaBootstrapVersion = clusterversion.V25_4_AddSystemStatementHintsTable.Version()

// After (final minted version - e.g., V25_4)
var SystemDatabaseSchemaBootstrapVersion = clusterversion.V25_4.Version()
```

### Full Checklist

1. **Update cockroach_versions.go:**
   - Add the final version key (e.g., `V25_4`) with `Internal: 0`
   - Set `finalVersion` constant to this key (e.g., `const finalVersion Key = V25_4`)

2. **Update SystemDatabaseSchemaBootstrapVersion** (see above - critical!)

3. **Update version.txt:**
   ```bash
   # Update pkg/build/version.txt to RC version (e.g., v25.4.0-rc.1)
   # Note: Minting happens before the final release, typically when cutting rc.1
   ```

4. **Regenerate documentation:**
   ```bash
   ./dev gen docs
   ```

5. **Regenerate bootstrap test data:**
   ```bash
   ./dev test pkg/sql/catalog/systemschema_test --rewrite
   ```
   This updates:
   - `pkg/sql/catalog/systemschema_test/testdata/bootstrap_system`
   - `pkg/sql/catalog/systemschema_test/testdata/bootstrap_tenant`

6. **Update bootstrap hash test data:**

   Run the test to see what values need updating:
   ```bash
   ./dev test pkg/sql/catalog/bootstrap -f TestInitialValuesToString
   ```

   The test will fail showing the expected hash values. Update in `pkg/sql/catalog/bootstrap/testdata/testdata`:
   - Line 1: `system hash=<new_hash_from_test_output>`
   - Line ~228: `tenant hash=<new_hash_from_test_output>`
   - Update the binary data on the line following each hash (the `{"key":"8b89898a89","value":"..."}` entry)

7. **Update logic test data:**

   After updating `SystemDatabaseSchemaBootstrapVersion`, you need to update the expected test output:

   - File: `pkg/sql/logictest/testdata/logic_test/crdb_internal_catalog`
   - Change: Update the `systemDatabaseSchemaVersion` in the test data to match the new minted version

   **Before (with internal version):**
   ```
   1           {"database": {"id": 1, "name": "system", ... "systemDatabaseSchemaVersion": {"internal": 14, "majorVal": 25, "minorVal": 3}, ...}}
   ```

   **After (final minted version):**
   ```
   1           {"database": {"id": 1, "name": "system", ... "systemDatabaseSchemaVersion": {"majorVal": 25, "minorVal": 4}, ...}}
   ```

   Note: The `internal` field should be removed when minting the final version.

8. **Verify all tests pass:**
   ```bash
   ./dev test pkg/sql/catalog/bootstrap -f TestInitialValuesToString
   ./dev test pkg/sql/catalog/systemschema_test
   ./dev test pkg/sql/logictest -f crdb_internal_catalog
   ```

### Common Errors and Solutions

**Error: "Unexpected hash value for system"**
- **Cause:** Forgot to update `SystemDatabaseSchemaBootstrapVersion` in step 2
- **Fix:** Complete step 2, then re-run steps 5-7

**Error: "output didn't match expected" with binary data diff**
- **Cause:** The binary-encoded system database descriptor needs updating
- **Fix:** Copy the exact binary value from the test output diff (shown after the `+` sign) and paste it into the testdata file

### Expected Files Modified

A typical R.2 mint should modify:
1. `pkg/clusterversion/cockroach_versions.go` (add final version, set finalVersion)
2. `pkg/sql/catalog/systemschema/system.go` (update SystemDatabaseSchemaBootstrapVersion)
3. `pkg/build/version.txt` (update to final version)
4. `docs/generated/settings/settings-for-tenants.txt`
5. `docs/generated/settings/settings.html`
6. `pkg/sql/catalog/systemschema_test/testdata/bootstrap_system`
7. `pkg/sql/catalog/systemschema_test/testdata/bootstrap_tenant`
8. `pkg/sql/catalog/bootstrap/testdata/testdata` (hashes and binary data)
9. `pkg/sql/logictest/testdata/logic_test/crdb_internal_catalog` (update systemDatabaseSchemaVersion)

### Example PRs

- For reference: [#112347](https://github.com/cockroachdb/cockroach/pull/112347)
- 25.3 mint: [#150211](https://github.com/cockroachdb/cockroach/pull/150211)