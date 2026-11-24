# R.1: Prepare for beta (Release Branch)

When preparing for a beta release (e.g., 25.4 beta1), the following files must be updated:

## 1. Core Version Changes

**Set Development Branch Flag:**
- File: `pkg/clusterversion/cockroach_versions.go`
- Change: Set `const DevelopmentBranch = false` (line ~334)

**Update Version String:**
- File: `pkg/build/version.txt`
- Change: Update from alpha to beta version (e.g., `v25.4.0-alpha.2` → `v25.4.0-beta.1`)

## 2. Regenerate Documentation

Run the following command to update generated documentation:
```bash
./dev gen docs
```

This updates:
- `docs/generated/settings/settings-for-tenants.txt`
- `docs/generated/settings/settings.html`

## 3. Update Test Data

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

## 4. CLI Test Data Updates

**Declarative Rules Tests:**
- File: `pkg/cli/testdata/declarative-rules/deprules`
  - Change: Update version reference to current release branch (e.g., `debug declarative-print-rules 25.2 dep` → `debug declarative-print-rules 25.3 dep`)

- File: `pkg/cli/testdata/declarative-rules/invalid_version`
  - Change: Update supported versions list to reflect current releases (use short format like `25.2`, `25.3` not `1000025.x`)

## 5. Logic Test Updates

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
