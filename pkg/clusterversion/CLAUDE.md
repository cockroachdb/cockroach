# CockroachDB Release Preparation Guide

**IMPORTANT FOR FUTURE CLAUDE SESSIONS:**
This file (`pkg/clusterversion/CLAUDE.md`) is the central location for detailed Claude-friendly runbooks for release tasks. When adding new runbooks:
1. **Add the detailed runbook to this file (CLAUDE.md)** - not as a separate file
2. **Insert in the same order as README.md** - The sections should appear in order: R.1, R.2, M.1, M.2, M.3, M.4, M.5. This makes it easy for humans to find the runbook where they expect it.
3. **Also update `pkg/clusterversion/README.md`** with a "Claude Prompt" section that references this file
4. **Follow the existing pattern:** R.1, R.2, M.1, M.2 are already documented as examples
5. **Use consistent header levels:** Main sections use `##`, subsections use `###`, individual steps use `####`
6. **Include these subsections:** Overview, Prerequisites, Step-by-Step Checklist, Expected Files Modified, Validation/Verification, Common Errors, Quick Reference Commands

---

This document provides detailed, step-by-step instructions for release preparation tasks, following the checklists from the clusterversion README.md.

## R.1: Prepare for beta (Release Branch)

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

### Notes

- Some test data files contain binary encoded data that may change when version numbers are updated
- Hash values in bootstrap test data are expected to change when the development branch flag is modified
- The logic test updates are necessary to reflect the new major version in system database schema version metadata
- CLI declarative rules tests use short version format (e.g., `25.3`) not the long format (e.g., `1000025.3`)
- Always run the CLI tests after updating declarative rules files to ensure correct version format

---

## R.2: Mint release (Release Branch)

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

---

## M.1: Bump Current Version (Master Branch)

This change advances the current release series version on master after forking a release branch, allowing the addition of new upgrade gates for the next version. It does NOT yet enable mixed-cluster or upgrade tests with the forked release.

**When**: Around the time the first beta is being cut on the release branch. Technically this can happen right after forking, but if there are changes to gates or upgrades in the forked release it might cause issues with master-to-master upgrades.

**Example**: After cutting release-25.4, bump master from 25.4 development to 26.1 development.

### Prerequisites

Before starting, ensure:
1. The release branch has been cut (e.g., `release-25.4`)
2. You're working on the `master` branch
3. You know the previous and new release numbers (e.g., 25.4 → 26.1)

### Step-by-Step Checklist

#### 1. Update pkg/clusterversion/cockroach_versions.go

This is the main file where version keys are defined.

**Add the new start version constant** (around line 238):
```go
// V25_4 is CockroachDB v25.4. It's used for all v25.4.x patch releases.
V25_4

V26_1_Start  // Add this line

// *************************************************
// Step (1) Add new versions above this comment.
```

**Add the version to the versionTable** (around line 303):
```go
V25_4: {Major: 25, Minor: 4, Internal: 0},

// v26.1 versions. Internal versions must be even.
V26_1_Start: {Major: 25, Minor: 4, Internal: 2},  // Add these lines

// *************************************************
// Step (2): Add new versions above this comment.
```

**Add the placeholder constant** (around line 323):
```go
// PreviousRelease is the logical cluster version of the previous release (which must
// have at least an RC build published).
const PreviousRelease Key = V25_3

// V26_1 is a placeholder that will eventually be replaced by the actual 26.1
// version Key, but in the meantime it points to the latest Key. The placeholder
// is defined so that it can be referenced in code that simply wants to check if
// a cluster is running 26.1 and has completed all associated migrations; most
// version gates can use this instead of defining their own version key if they
// only need to check that the cluster has upgraded to 26.1.
const V26_1 = Latest

// DevelopmentBranch must be true on the main development branch but should be
```

**Note:** Do NOT update `PreviousRelease` - that only happens in M.3 after an RC is published.

#### 2. Update pkg/roachpb/version.go

**Add the successor mapping** (around line 237):
```go
{25, 2}: {25, 3},
{25, 3}: {25, 4},
{25, 4}: {26, 1},  // Add this line
}
```

#### 3. Update pkg/roachpb/version_test.go

**Update the expected release series** (around line 96):
```go
expected := "20.1, 20.2, 21.1, 21.2, 22.1, 22.2, 23.1, 23.2, 24.1, 24.2, 24.3, 25.1, 25.2, 25.3, 25.4, 26.1"
```

#### 4. Update pkg/sql/catalog/systemschema/system.go

**Update the bootstrap version** (around line 1445):
```go
// Before
var SystemDatabaseSchemaBootstrapVersion = clusterversion.V25_4.Version()

// After
var SystemDatabaseSchemaBootstrapVersion = clusterversion.V26_1_Start.Version()
```

This ensures new clusters bootstrap at the start of the new version.

#### 5. Update pkg/upgrade/upgrades/upgrades.go

**Add the first upgrade for the new version** (at the end of the upgrades array, around line 124):
```go
	upgrade.NewTenantUpgrade(
		"create statement_hints table",
		clusterversion.V25_4_AddSystemStatementHintsTable.Version(),
		upgrade.NoPrecondition,
		createStatementHintsTable,
		upgrade.RestoreActionNotRequired(
			"restore for a cluster predating this table can leave it empty",
		),
	),

	newFirstUpgrade(clusterversion.V26_1_Start.Version()),  // Add this line

	// Note: when starting a new release version, the first upgrade (for
	// Vxy_zStart) must be a newFirstUpgrade. Keep this comment at the bottom.
}
```

#### 6. Update pkg/build/version.txt

Update the version string:
```bash
# Before
v25.4.1

# After
v26.1.0-alpha.00000000
```

#### 7. Update Schema Changer Rules

This is the most complex step involving multiple files.

**a) Copy current rules to a new release directory:**
```bash
cp -r pkg/sql/schemachanger/scplan/internal/rules/current \
      pkg/sql/schemachanger/scplan/internal/rules/release_25_4
```

**b) Update the package name in all files:**
```bash
find pkg/sql/schemachanger/scplan/internal/rules/release_25_4 -name "*.go" \
     -exec sed -i '' 's/^package current$/package release_25_4/' {} \;
```

**c) Update BUILD.bazel in release_25_4 directory:**

Change the library name and import path:
```bazel
# Before
go_library(
    name = "current",
    # ...
    importpath = "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules/current",

# After
go_library(
    name = "release_25_4",
    # ...
    importpath = "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules/release_25_4",
```

Change the test target name:
```bazel
# Before
go_test(
    name = "current_test",
    # ...
    embed = [":current"],

# After
go_test(
    name = "release_25_4_test",
    # ...
    embed = [":release_25_4"],
```

**d) Update pkg/sql/schemachanger/scplan/internal/rules/current/helpers.go:**

Update the version references:
```go
// Before
const (
	// rulesVersion version of elements that can be appended to rel rule names.
	rulesVersion = "-25.4"
)

// rulesVersionKey version of elements used by this rule set.
var rulesVersionKey = clusterversion.V25_4

// After
const (
	// rulesVersion version of elements that can be appended to rel rule names.
	rulesVersion = "-26.1"
)

// rulesVersionKey version of elements used by this rule set.
var rulesVersionKey = clusterversion.V26_1
```

**e) Update pkg/sql/schemachanger/scplan/plan.go:**

Add import for the new release:
```go
import (
	// ...
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules/current"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules/release_25_2"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules/release_25_3"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules/release_25_4"  // Add this
	// ...
)
```

Add to rulesForReleases array (around line 158):
```go
var rulesForReleases = []rulesForRelease{
	// NB: sort versions in descending order, i.e. newest supported version first.
	{activeVersion: clusterversion.Latest, rulesRegistry: current.GetRegistry()},
	{activeVersion: clusterversion.V25_4, rulesRegistry: release_25_4.GetRegistry()},  // Add this
	{activeVersion: clusterversion.V25_3, rulesRegistry: release_25_3.GetRegistry()},
	{activeVersion: clusterversion.V25_2, rulesRegistry: release_25_2.GetRegistry()},
}

```

#### 8. Regenerate Files

**a) Update Bazel build files:**
```bash
./dev gen bazel
```

This updates various BUILD.bazel files across the codebase.

**b) Update releases file:**
```bash
bazel build //pkg/cmd/release:release
_bazel/bin/pkg/cmd/release/release_/release update-releases-file
```

This updates `pkg/testutils/release/cockroach_releases.yaml`. Note that the forked release version will disappear from the releases file. This is expected and desired - if it's there, upgrade tests will attempt to run against it.

**IMPORTANT - Predecessor Version Edge Case:**

The `release update-releases-file` command assumes M.1 is performed shortly after the branch cut, before the RC is published. If you're performing M.1 **after the RC is already published** (e.g., 25.4.0-rc.1 exists when bumping to 26.1), the tool will incorrectly set the predecessor.

**Example:** When bumping to 26.1 after 25.4.0-rc.1 is published:
- **Incorrect (auto-generated):** `"26.1": predecessor: "25.4"`
- **Correct (manual fix needed):** `"26.1": predecessor: "25.3"`

**Why:** The new major version (26.1) should upgrade from the previous major series (25.3), not the just-forked series (25.4).

**Fix:** After running `update-releases-file`, check the generated entry for the new version and manually correct the predecessor if needed:
```bash
# Check what was generated
grep -A 1 '"26.1":' pkg/testutils/release/cockroach_releases.yaml

# If predecessor is "25.4", manually edit to "25.3"
```

**c) Regenerate scplan test outputs:**
```bash
./dev test pkg/sql/schemachanger/scplan/internal/rules/... --rewrite
```

This updates test data in:
- `pkg/sql/schemachanger/scplan/internal/rules/current/testdata/deprules`
- `pkg/sql/schemachanger/scplan/internal/rules/release_25_4/testdata/deprules`

**d) Regenerate CLI test outputs:**
```bash
./dev test pkg/cli -f DeclarativeRules --rewrite
```

This updates:
- `pkg/cli/testdata/declarative-rules/invalid_version`

**e) Regenerate documentation and other generated files:**
```bash
./dev generate
```

This updates:
- `docs/generated/settings/settings-for-tenants.txt`
- `docs/generated/settings/settings.html`

#### 9. Verify Changes

Run tests to ensure everything is working:

```bash
# Test version packages
./dev test pkg/clusterversion pkg/roachpb

# Test schema changer
./dev test pkg/sql/schemachanger/scplan/internal/rules/...

# Test CLI
./dev test pkg/cli -f DeclarativeRules
```

### Expected Files Modified

A typical M.1 bump should modify approximately 15-20 files:

**Core version files:**
1. `pkg/clusterversion/cockroach_versions.go`
2. `pkg/roachpb/version.go`
3. `pkg/roachpb/version_test.go`
4. `pkg/sql/catalog/systemschema/system.go`
5. `pkg/upgrade/upgrades/upgrades.go`
6. `pkg/build/version.txt`

**Generated/updated files:**
7. `pkg/BUILD.bazel`
8. `pkg/testutils/release/cockroach_releases.yaml`
9. `pkg/sql/logictest/REPOSITORIES.bzl`
10. `pkg/cli/testdata/declarative-rules/invalid_version`
11. `docs/generated/settings/settings-for-tenants.txt`
12. `docs/generated/settings/settings.html`

**Schema changer files:**
13. `pkg/sql/schemachanger/scplan/plan.go`
14. `pkg/sql/schemachanger/scplan/BUILD.bazel`
15. `pkg/sql/schemachanger/scplan/internal/rules/current/helpers.go`
16. `pkg/sql/schemachanger/scplan/internal/rules/current/testdata/deprules`
17. `pkg/sql/schemachanger/scplan/internal/rules/release_25_4/` (entire new directory)

### Common Errors and Solutions

#### Error: "package release_25_4 not found"

**Cause:** The new release directory wasn't created or the package name wasn't updated correctly.

**Fix:**
1. Verify the directory exists: `ls pkg/sql/schemachanger/scplan/internal/rules/release_25_4`
2. Check package names: `grep "^package " pkg/sql/schemachanger/scplan/internal/rules/release_25_4/*.go`
3. Re-run the sed command from step 7b if needed

#### Error: "undefined: clusterversion.V26_1"

**Cause:** The placeholder constant wasn't added to `cockroach_versions.go`.

**Fix:** Add the `const V26_1 = Latest` line as shown in step 1.

#### Error: "test output didn't match expected" in scplan tests

**Cause:** Test outputs need to be regenerated after changing version constants.

**Fix:** Run the rewrite commands:
```bash
./dev test pkg/sql/schemachanger/scplan/internal/rules/... --rewrite
./dev test pkg/cli -f DeclarativeRules --rewrite
```

#### Error: Build failures in Bazel

**Cause:** BUILD.bazel files weren't regenerated or are out of sync.

**Fix:**
```bash
./dev gen bazel
```

#### Error: Release file contains old release

**Cause:** The releases file wasn't updated or cached data is stale.

**Fix:**
1. Clean build: `bazel clean`
2. Rebuild and update:
   ```bash
   bazel build //pkg/cmd/release:release
   _bazel/bin/pkg/cmd/release/release_/release update-releases-file
   ```

#### Error: check_generated_code failure in CI

**Cause:** Generated documentation files weren't updated.

**Fix:**
```bash
./dev generate
```

### Important Notes

- **Do NOT update `PreviousRelease`** - This doesn't happen until M.3
- **Do NOT update `pkg/testutils/release/cockroach_releases.yaml`** - This doesn't happen until M.3
- **The forked release disappearing from releases file is expected** - This prevents premature upgrade testing
- **Version numbers use the previous release's minor for internal versions** - V26_1_Start has version `25.4-2`, not `26.1-2`
- **All internal version numbers must be even** - This convention must be maintained
- **Schema changer rules must be versioned** - Each release gets its own frozen copy of the rules

### Verification Checklist

Before committing, verify:

- [ ] All version constants follow naming conventions (V{MAJOR}_{MINOR}_Start)
- [ ] Internal version number is even (e.g., 2, 4, 6)
- [ ] Successor map includes new version
- [ ] SystemDatabaseSchemaBootstrapVersion points to new start version
- [ ] First upgrade added for new version
- [ ] version.txt updated to new alpha version
- [ ] Schema changer rules copied and updated
- [ ] All tests pass: `./dev test pkg/clusterversion pkg/roachpb pkg/sql/schemachanger/scplan/internal/rules/... pkg/cli -f DeclarativeRules`
- [ ] Git status shows expected number of modified files (~15-20)
- [ ] Releases file no longer contains the forked release version

### Example PRs

- 25.4 bump (master → 26.1 equivalent): [#149494](https://github.com/cockroachdb/cockroach/pull/149494)
- 25.2 bump: [#139387](https://github.com/cockroachdb/cockroach/pull/139387)

### Timeline Context

In the release cycle:
- **Now (M.1)**: Master bumped to 26.1, release-25.4 exists but no upgrade tests yet
- **M.2 (later)**: After first 25.4 RC, enable mixed-cluster logic tests
- **M.3 (later)**: After first 25.4 RC published, enable upgrade tests, update PreviousRelease
- **M.4 (later)**: Bump MinSupported version
- **M.5 (later)**: Finalize gates and bootstrap data when 25.4.0 final is released

### Common Test Failures After M.1 PR

After the main M.1 PR is merged, CI tests will typically reveal two types of failures that need fixing:

#### Type 1: Version Gate Failures
These are logic errors in code that references version gates. Review test output carefully as each case is unique and requires understanding the specific version gate logic.

**Example locations:**
- Version-dependent feature checks
- Migration logic
- Upgrade compatibility code

#### Type 2: Test Expectation Updates (Most Common)

These are straightforward test output updates that happen because version numbers and URLs changed.

##### A. Documentation URL Updates (v25.4 → dev)

**Pattern:** After bumping from X.Y to next major version, all doc URLs referencing `vX.Y` must change to `dev`.

**Files typically affected:**
- `pkg/sql/logictest/testdata/logic_test/*`
- `pkg/sql/pgwire/testdata/pgtest/*`
- `pkg/ccl/logictestccl/testdata/logic_test/*`

**Changes needed:**
```bash
# Documentation URLs
www.cockroachlabs.com/docs/v25.4/* → www.cockroachlabs.com/docs/dev/*

# Issue tracker URLs
go.crdb.dev/issue-v/*/v25.4 → go.crdb.dev/issue-v/*/dev
```

**Quick fix:**
```bash
# Find all files with v25.4 references
grep -r "v25\.4" pkg/sql pkg/ccl --include="*.md" --include="logic_test*" --include="pgtest*"

# Replace with sed (example for URLs)
sed -i '' 's|www.cockroachlabs.com/docs/v25.4|www.cockroachlabs.com/docs/dev|g' <file>
sed -i '' 's|go.crdb.dev/issue-v/\([0-9]*\)/v25.4|go.crdb.dev/issue-v/\1/dev|g' <file>
```

##### B. Version Number Updates in Test Outputs

**Pattern:** Tests that query version information expect the new version number.

**Common files:**
- `pkg/sql/logictest/testdata/logic_test/upgrade`
- `pkg/sql/logictest/testdata/logic_test/crdb_internal`
- `pkg/ccl/logictestccl/testdata/logic_test/crdb_internal_tenant`

**Changes needed:**
```diff
 query T
 SELECT crdb_internal.release_series(crdb_internal.node_executable_version())
 ----
-25.4
+26.1
```

**Quick fix:**
```bash
# Use replace_all=true for files with multiple occurrences
sed -i '' 's/^25\.4$/26.1/' <file>
```

##### C. systemDatabaseSchemaVersion Update

**Pattern:** After bumping to version X.Y, the systemDatabaseSchemaVersion must reflect `VX_Y_Start`.

**File:** `pkg/sql/logictest/testdata/logic_test/crdb_internal_catalog`

**How to determine new values:**

1. Find `VX_Y_Start` in `pkg/clusterversion/cockroach_versions.go`:
   ```go
   V26_1_Start: {Major: 25, Minor: 4, Internal: 2}
   ```

2. Convert to JSON format:
   ```json
   {
     "majorVal": 1000025,    // Major * 1000000 + MinorSeries
     "minorVal": 4,          // Minor from VX_Y_Start
     "internal": 2           // Internal from VX_Y_Start
   }
   ```

**Example change:**
```diff
-"systemDatabaseSchemaVersion": {"internal": 14, "majorVal": 1000025, "minorVal": 3}
+"systemDatabaseSchemaVersion": {"internal": 2, "majorVal": 1000025, "minorVal": 4}
```

**Test to verify:**
```bash
./dev test pkg/sql/logictest --filter='TestReadCommittedLogic/crdb_internal_catalog'
```

### Post-Fix Verification Checklist

After fixing test failures:

```bash
# Run the tests that previously failed
./dev test pkg/sql/logictest --filter='<test_name>'

# Verify file count is similar to previous M.1 PRs
# Expected: ~60-65 files changed total (original PR + test fixes)
git diff --stat <base_branch>

# Run broader tests to ensure nothing broke
./dev test pkg/sql/logictest --filter='TestLogic/local/cluster_settings'
./dev test pkg/sql/catalog/bootstrap
```

### Historical Context

- **PR 149494** (25.3 → 25.4): 60 files changed
- **PR 156225** (25.4 → 26.1): 63 files changed (50 original + 14 test fixes)

This consistency helps validate that the changes are complete and correct.

### CRITICAL: Validate Changes Before Creating PR

**This task is performed every quarter.** Before creating the M.1 PR or fixing any test failures, you MUST validate that the changes follow the same pattern as previous quarterly M.1 PRs.

#### Step 1: Find the Previous M.1 PR

The most recent M.1 PRs (for reference):
- **PR #149494** - clusterversion: move to 25.4 version (July 2025)
- **PR #139387** - clusterversion: move to 25.2 version (January 2025)

Find the PR for the previous quarter:
```bash
# Find recent M.1 version bump PRs
gh pr list --search "clusterversion: move to 25" --state merged --limit 10 \
  --json number,title,mergedAt,url | \
  jq -r '.[] | select(.title | test("move to [0-9]+\\.[0-9]+ version")) |
  "\(.number) | \(.title) | \(.mergedAt) | \(.url)"'
```

#### Step 2: Compare Files Changed

**BEFORE creating the PR**, compare your changes against the previous M.1 PR:

```bash
# Get files changed in your current work
git diff --name-only <base_branch> | sort > /tmp/current_files.txt

# Get files changed in previous M.1 PR (example: PR #149494)
gh pr view 149494 --json files --jq '.files[].path' | sort > /tmp/previous_files.txt

# Compare the two lists
echo "=== Files ONLY in current PR (investigate these!) ==="
comm -13 /tmp/previous_files.txt /tmp/current_files.txt

echo "=== Files ONLY in previous PR (you might be missing these!) ==="
comm -23 /tmp/previous_files.txt /tmp/current_files.txt

echo "=== Common files (expected) ==="
comm -12 /tmp/previous_files.txt /tmp/current_files.txt
```

#### Step 3: Justify Each Unexpected Change

For **every file** that appears in your PR but NOT in the previous M.1 PR, you must:

1. **Understand why it changed** - Read the actual diff
2. **Verify it's intentional** - Check if it matches a pattern from the runbook
3. **If it's from `release update-releases-file`** - Compare with previous PR to see if the tool modified this file before
4. **Document or revert** - Either justify why it's needed now, or revert the change

**Example investigation:**
```bash
# Check if REPOSITORIES.bzl was modified in previous M.1 PR
gh pr view 149494 --json files --jq '.files[] | select(.path == "pkg/sql/logictest/REPOSITORIES.bzl") | .path'
# If this returns nothing, the file should NOT change in M.1!

# Check what changed in your version
git diff <base_branch> -- pkg/sql/logictest/REPOSITORIES.bzl
```

#### Step 4: File-by-File Pattern Validation

For files that appear in BOTH PRs, verify the changes follow the same pattern:

**Expected file changes (from runbook and previous PRs):**

| File | Expected Change | Verify |
|------|----------------|--------|
| `pkg/clusterversion/cockroach_versions.go` | Add new version constants, update table | ✓ Always changes |
| `pkg/build/version.txt` | Bump to new alpha version | ✓ Always changes |
| `pkg/roachpb/version.go` | Update successor series map | ✓ Always changes |
| `pkg/sql/catalog/systemschema/system.go` | Update bootstrap version | ✓ Always changes |
| `pkg/upgrade/upgrades/upgrades.go` | Add first upgrade | ✓ Always changes |
| `pkg/sql/schemachanger/scplan/internal/rules/release_X_Y/*` | New directory with copied rules | ✓ Always changes |
| `pkg/sql/schemachanger/scplan/plan.go` | Add new release to rules map | ✓ Always changes |
| `pkg/testutils/release/cockroach_releases.yaml` | Update releases (via tool) | ✓ Always changes |
| `docs/generated/settings/*` | Regenerated docs | ✓ Always changes |
| `pkg/sql/catalog/bootstrap/testdata/*` | Updated bootstrap data | ✓ Always changes |
| `pkg/BUILD.bazel` | Updated build rules | ✓ Always changes |
| **`pkg/sql/logictest/REPOSITORIES.bzl`** | **Should NOT change in M.1** | ⚠️ Only changes in M.2 |

#### Step 5: Red Flags - Files That Should NOT Change in M.1

Based on comparing with previous M.1 PRs, these files typically **should NOT** change:

1. **`pkg/sql/logictest/REPOSITORIES.bzl`**
   - Only changes in **M.2** (after RC is published)
   - If `release update-releases-file` modified it, revert the change
   - The 25.4.0-rc.1 config should appear in M.2, not M.1

2. **Test expectation files (before running tests)**
   - Test fixes come AFTER the M.1 PR is created and CI runs
   - Don't include test fixes in the initial M.1 commit

3. **Files unrelated to version bumping**
   - Check git blame to see if changes are from uncommitted local work

#### Step 6: Validation Checklist

Before creating the M.1 PR, verify:

- [ ] Compared files changed with previous M.1 PR (#149494 or similar)
- [ ] Investigated every file that's different from the pattern
- [ ] Verified REPOSITORIES.bzl was NOT modified (or reverted if it was)
- [ ] Justified or reverted any unexpected changes
- [ ] File count is similar to previous M.1 PR (~50-60 files for base PR)
- [ ] No test expectation updates in the base PR (those come after CI runs)

#### Why This Matters

**Real example from PR #156225:**
- `REPOSITORIES.bzl` was incorrectly modified by `release update-releases-file`
- It removed 25.2.7 config and added 25.4.0-rc.1 config
- Previous PR #149494 did NOT modify this file
- This caused test failures and wasted time fixing tests based on incorrect baseline
- Had to revert the file to pre-M.1 state

**Time saved:** If we had validated against PR #149494 before creating the PR, we would have caught this immediately and avoided fixing test failures from a broken baseline.

### Quick Reference Commands

```bash
# Step 7: Schema changer setup
cp -r pkg/sql/schemachanger/scplan/internal/rules/current \
      pkg/sql/schemachanger/scplan/internal/rules/release_25_4
find pkg/sql/schemachanger/scplan/internal/rules/release_25_4 -name "*.go" \
     -exec sed -i '' 's/^package current$/package release_25_4/' {} \;

# Step 8: Regeneration
./dev gen bazel
bazel build //pkg/cmd/release:release
_bazel/bin/pkg/cmd/release/release_/release update-releases-file
./dev test pkg/sql/schemachanger/scplan/internal/rules/... --rewrite
./dev test pkg/cli -f DeclarativeRules --rewrite
./dev generate

# Verification
./dev test pkg/clusterversion pkg/roachpb
./dev test pkg/sql/schemachanger/scplan/internal/rules/...
./dev test pkg/cli -f DeclarativeRules
```

---

## M.2: Enable Mixed-Cluster Logic Tests

This section provides step-by-step instructions for the M.2 task: "Enable mixed-cluster logic tests" on the master branch after M.1 has been completed. This task enables testing with the newly forked release version in mixed-cluster configurations.

### Overview

**When to perform:** After M.1 is complete and the release branch exists. This is typically done shortly after the first beta is cut on the release branch.

**What it does:** Adds bootstrap data from the release branch and configures logic tests to run in mixed-cluster mode with the forked release version. This enables testing code changes against the previous release to catch compatibility issues early.

**Dependencies:**
- M.1 must be completed
- Release branch must exist (e.g., `release-25.4`)

### Prerequisites

Before starting, ensure:
1. The release branch exists and is accessible (e.g., `release-25.4`)
2. M.1 PR exists or is merged (contains V25_4 constant)
3. You know the release version number (e.g., 25.4)
4. You have the M.1 branch checked out or can create a new branch based on it

### Step-by-Step Checklist

#### Step 1: Generate Bootstrap Data from Release Branch

Bootstrap data is a snapshot of the system catalog from the release version, used to simulate a cluster running that version.

**a) Checkout the release branch:**
```bash
git fetch origin release-25.4
git checkout origin/release-25.4
```

**b) Build and run the sql-bootstrap-data tool:**
```bash
./dev build sql-bootstrap-data && bin/sql-bootstrap-data
```

This command will:
- Build the bootstrap data generator
- Generate 4 files in `pkg/sql/catalog/bootstrap/data/`:
  - `25_4_system.keys` (~160KB)
  - `25_4_system.sha256` (64 bytes)
  - `25_4_tenant.keys` (~155KB)
  - `25_4_tenant.sha256` (64 bytes)

**c) Copy the generated files to a temporary location:**
```bash
cp pkg/sql/catalog/bootstrap/data/25_4_* /tmp/
```

**d) Return to your working branch:**
```bash
git checkout <your-m2-branch-name>
cp /tmp/25_4_* pkg/sql/catalog/bootstrap/data/
```

#### Step 2: Update initial_values.go

Add the bootstrap data mapping for the new version.

**File:** `pkg/sql/catalog/bootstrap/initial_values.go`

**a) Add the bootstrap data mapping to `initialValuesFactoryByKey`** (around line 66):
```go
var initialValuesFactoryByKey = map[clusterversion.Key]initialValuesFactoryFn{
	clusterversion.Latest: buildLatestInitialValues,

	clusterversion.V25_2: hardCodedInitialValues{
		system:        v25_2_system_keys,
		systemHash:    v25_2_system_sha256,
		nonSystem:     v25_2_tenant_keys,
		nonSystemHash: v25_2_tenant_sha256,
	}.build,

	clusterversion.V25_3: hardCodedInitialValues{
		system:        v25_3_system_keys,
		systemHash:    v25_3_system_sha256,
		nonSystem:     v25_3_tenant_keys,
		nonSystemHash: v25_3_tenant_sha256,
	}.build,

	clusterversion.V25_4: hardCodedInitialValues{  // Add this entry
		system:        v25_4_system_keys,
		systemHash:    v25_4_system_sha256,
		nonSystem:     v25_4_tenant_keys,
		nonSystemHash: v25_4_tenant_sha256,
	}.build,
}
```

**b) Add go:embed variables for the new files** (at the end of the file, around line 162):
```go
//go:embed data/25_4_system.keys
var v25_4_system_keys string

//go:embed data/25_4_system.sha256
var v25_4_system_sha256 string

//go:embed data/25_4_tenant.keys
var v25_4_tenant_keys string

//go:embed data/25_4_tenant.sha256
var v25_4_tenant_sha256 string
```

#### Step 3: Update BUILD.bazel

Add the new bootstrap data files to the build configuration.

**File:** `pkg/sql/catalog/bootstrap/BUILD.bazel`

**Update embedsrcs** (around line 10):
```bazel
embedsrcs = [
    "data/25_2_system.keys",
    "data/25_2_system.sha256",
    "data/25_2_tenant.keys",
    "data/25_2_tenant.sha256",
    "data/25_3_system.keys",
    "data/25_3_system.sha256",
    "data/25_3_tenant.keys",
    "data/25_3_tenant.sha256",
    "data/25_4_system.keys",      # Add these 4 lines
    "data/25_4_system.sha256",
    "data/25_4_tenant.keys",
    "data/25_4_tenant.sha256",
],
```

#### Step 4: Configure Logictest

Add the mixed-cluster test configuration for the new version.

**File:** `pkg/sql/logictest/logictestbase/logictestbase.go`

**a) Add the local-mixed-25.4 configuration** (after the previous mixed config, around line 522):
```go
{
	// This config runs tests using 25.4 cluster version, simulating a node that
	// is operating in a mixed-version cluster.
	Name:                        "local-mixed-25.4",
	NumNodes:                    1,
	OverrideDistSQLMode:         "off",
	BootstrapVersion:            clusterversion.V25_4,
	DisableUpgrade:              true,
	DeclarativeCorpusCollection: true,
},
```

**b) Update default-configs set** (around line 625):
```go
"default-configs": makeConfigSet(
	"local",
	// ... other configs ...
	"local-mixed-25.2",
	"local-mixed-25.3",
	"local-mixed-25.4",  // Add this line
),
```

**⚠️ IMPORTANT:** Do NOT add `cockroach-go-testserver-25.4` configuration in M.2. This configuration requires a predecessor binary (v25.4.0-rc.1) that is only added in M.3 after the first RC is published. See the "Common Errors" section for details.

#### Step 5: Generate Test Files

Run bazel generation to create test files for the new configuration.

```bash
./dev gen bazel
```

This will generate test files in:
- `pkg/ccl/logictestccl/tests/local-mixed-25.4/`
- `pkg/sql/logictest/tests/local-mixed-25.4/`
- `pkg/sql/sqlitelogictest/tests/local-mixed-25.4/`

**Add the generated directories to git:**
```bash
git add pkg/ccl/logictestccl/tests/local-mixed-25.4/
git add pkg/sql/logictest/tests/local-mixed-25.4/
git add pkg/sql/sqlitelogictest/tests/local-mixed-25.4/
```

**Note:** The `cockroach-go-testserver-25.4` directory should NOT be generated in M.2 (it will be added in M.3).

#### Step 6: Update Logictest Skipif Directives

Some logic tests need to skip or adjust their expectations for mixed-cluster configurations.

**Common file to update:**
- `pkg/sql/logictest/testdata/logic_test/crdb_internal_catalog`

**Pattern:**
Look for skipif directives that include previous mixed versions and add the new one:

```
# Before:
skipif config schema-locked-disabled local-mixed-25.3

# After:
skipif config schema-locked-disabled local-mixed-25.3 local-mixed-25.4
```

**How to find files that need updating:**
```bash
# Search for skipif directives with previous mixed versions
grep -r "skipif.*local-mixed-25.3" pkg/sql/logictest/testdata/ --include="logic_test"
```

For each file found, add `local-mixed-25.4` to the skipif list.

#### Step 7: Verify Changes

**a) Run bootstrap tests:**
```bash
./dev test pkg/sql/catalog/bootstrap -f TestInitialKeys -v
```

Expected: Test should pass with no errors.

**b) Run a quick logictest with the new config:**
```bash
./dev testlogic base --config=local-mixed-25.4 --files=crdb_internal_catalog -v
```

Expected: Tests should pass or skip appropriately.

**c) Check file count:**
```bash
git status --short | wc -l
```

Expected: Approximately 15-20 files changed (varies based on how many logictest files need skipif updates).

#### Step 8: Commit Changes

**Add all changes and commit:**
```bash
git add -A
git commit -m "bootstrap: enable 25.4 mixed-cluster logic tests

This change enables mixed-cluster logic tests for version 25.4 by adding
bootstrap data from release-25.4 and configuring the local-mixed-25.4 test
configuration.

The bootstrap data was obtained by running (on release-25.4 branch):
  ./dev build sql-bootstrap-data && bin/sql-bootstrap-data

Changes include:
- Added bootstrap data files (25_4_system.keys/sha256, 25_4_tenant.keys/sha256)
- Updated initial_values.go with V25_4 mapping
- Updated BUILD.bazel files with new bootstrap data embedsrcs
- Added local-mixed-25.4 logictest configuration
- Generated test files for local-mixed-25.4 config
- Updated logictests with skipif directives for local-mixed-25.4

Note: cockroach-go-testserver-25.4 configuration is NOT included in M.2.
It will be added in M.3 after the first RC binary is published and added
to REPOSITORIES.bzl.

Part of M.2 \"Enable mixed-cluster logic tests\" checklist.

Release note: None
Epic: None

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>
"
```

### Expected Files Modified

A typical M.2 change should modify approximately 15-20 files:

**Core changes (always modified):**
1. `pkg/sql/catalog/bootstrap/BUILD.bazel` - Add bootstrap files to embedsrcs
2. `pkg/sql/catalog/bootstrap/initial_values.go` - Add V25_4 mapping
3. `pkg/sql/catalog/bootstrap/data/25_4_system.keys` - New file
4. `pkg/sql/catalog/bootstrap/data/25_4_system.sha256` - New file
5. `pkg/sql/catalog/bootstrap/data/25_4_tenant.keys` - New file
6. `pkg/sql/catalog/bootstrap/data/25_4_tenant.sha256` - New file
7. `pkg/sql/logictest/logictestbase/logictestbase.go` - Add test configurations
8. `pkg/BUILD.bazel` - Updated by `./dev gen bazel` (binary file)

**Generated test files (always created):**
9. `pkg/ccl/logictestccl/tests/local-mixed-25.4/BUILD.bazel`
10. `pkg/ccl/logictestccl/tests/local-mixed-25.4/generated_test.go`
11. `pkg/sql/logictest/tests/local-mixed-25.4/BUILD.bazel`
12. `pkg/sql/logictest/tests/local-mixed-25.4/generated_test.go`
13. `pkg/sql/logictest/tests/cockroach-go-testserver-25.4/BUILD.bazel`
14. `pkg/sql/logictest/tests/cockroach-go-testserver-25.4/generated_test.go`
15. `pkg/sql/sqlitelogictest/tests/local-mixed-25.4/BUILD.bazel`
16. `pkg/sql/sqlitelogictest/tests/local-mixed-25.4/generated_test.go`

**Test expectation updates (may vary):**
17. `pkg/sql/logictest/testdata/logic_test/crdb_internal_catalog` - Update skipif
18. `pkg/sql/logictest/tests/cockroach-go-testserver-25.2/generated_test.go` - Regenerated
19. `pkg/sql/logictest/tests/cockroach-go-testserver-25.3/generated_test.go` - Regenerated

**Note:** The testserver generated_test.go files for 25.2 and 25.3 get regenerated because we added 25.4 to the testserver configs list.

### Validation and Verification

### CRITICAL: Validate Before Creating PR

**This task is performed every quarter.** Before creating the M.2 PR, validate that changes follow the expected pattern.

#### Step 1: Compare with Previous M.2 PR

Find the most recent M.2 PR for reference. Note that some previous M.2 PRs may have been combined with M.1 changes.

**Example standalone M.2 PRs:**
- #156183 (included both M.1 and M.2, so filter accordingly)

**Compare file lists:**
```bash
# Get files changed in your current work
git diff --name-only <base-branch> | sort > /tmp/current_m2_files.txt

# Get files from a reference M.2 (adjust PR number)
gh pr view 156183 --json files --jq '.files[].path' | \
  grep -E "(bootstrap/data|initial_values|BUILD.bazel|logictestbase|local-mixed|cockroach-go-testserver)" | \
  sort > /tmp/previous_m2_files.txt

# Compare
echo "=== Files ONLY in current M.2 (investigate!) ==="
comm -13 /tmp/previous_m2_files.txt /tmp/current_m2_files.txt

echo "=== Files ONLY in previous M.2 (might be missing!) ==="
comm -23 /tmp/previous_m2_files.txt /tmp/current_m2_files.txt
```

**Expected differences:**
- cockroach-go-testserver-25.4 files (new in this version)
- Version-specific files (25.4 vs 25.3)
- All other files should follow the same pattern

#### Step 2: Verify File Count

```bash
git diff --stat <base-branch> | tail -1
```

Expected: Approximately 15-20 files changed, 5000-5500 insertions.

#### Step 3: Verify Bootstrap Data Integrity

```bash
# Check file sizes
ls -lh pkg/sql/catalog/bootstrap/data/25_4_*
```

Expected:
- `25_4_system.keys`: ~160KB
- `25_4_system.sha256`: 64 bytes
- `25_4_tenant.keys`: ~155KB
- `25_4_tenant.sha256`: 64 bytes

```bash
# Verify sha256 hashes match
cd pkg/sql/catalog/bootstrap/data
sha256sum 25_4_system.keys | awk '{print $1}' | diff - 25_4_system.sha256
sha256sum 25_4_tenant.keys | awk '{print $1}' | diff - 25_4_tenant.sha256
```

Expected: No output (hashes match).

#### Step 4: Run Tests

```bash
# Bootstrap tests
./dev test pkg/sql/catalog/bootstrap -f TestInitialKeys -v

# Quick logictest
./dev testlogic base --config=local-mixed-25.4 --files=crdb_internal_catalog -v
```

Expected: All tests pass.

### Verification Checklist

Before creating the PR, verify:

- [ ] Bootstrap data generated from correct release branch (release-25.4)
- [ ] All 4 bootstrap files present and correct size
- [ ] SHA256 hashes verified
- [ ] `initial_values.go` has V25_4 mapping and go:embed variables
- [ ] `BUILD.bazel` includes all 4 new bootstrap files
- [ ] `logictestbase.go` has local-mixed-25.4 config (NOT cockroach-go-testserver-25.4)
- [ ] local-mixed-25.4 added to default-configs set
- [ ] `./dev gen bazel` ran successfully
- [ ] All generated test directories added to git (local-mixed-25.4 only)
- [ ] NO cockroach-go-testserver-25.4 directories or configs (that's M.3!)
- [ ] Skipif directives updated in crdb_internal_catalog
- [ ] Bootstrap tests pass
- [ ] Quick logictest passes
- [ ] File count is reasonable (~12-15 files, fewer than if you mistakenly added testserver config)
- [ ] No unexpected files changed
- [ ] Git status shows only M.2-related changes

### Common Errors and Solutions

### Error: "Bootstrap data files not found"

**Cause:** Forgot to copy bootstrap files from release branch.

**Fix:**
1. Checkout release-25.4 branch
2. Run `./dev build sql-bootstrap-data && bin/sql-bootstrap-data`
3. Copy the 4 generated files to your working branch

### Error: "undefined: v25_4_system_keys"

**Cause:** go:embed variables not added to `initial_values.go`.

**Fix:** Add the 4 go:embed variable declarations at the end of `initial_values.go`.

### Error: "clusterversion.V25_4 undefined"

**Cause:** M.1 not completed or working on wrong base branch.

**Fix:** Ensure your branch is based on the M.1 PR branch that has V25_4 defined.

### Error: "Test files not generated"

**Cause:** `./dev gen bazel` not run or failed.

**Fix:**
1. Run `./dev gen bazel` again
2. Check for errors in the output
3. Ensure logictestbase.go changes are saved

### Error: "SHA256 hash mismatch"

**Cause:** Bootstrap files corrupted or from wrong version.

**Fix:**
1. Re-generate bootstrap data from clean release-25.4 branch
2. Verify you're on the correct release branch
3. Don't manually edit the bootstrap files

### Error: Logic test fails with "bootstrap version not found"

**Cause:** Bootstrap data not embedded correctly.

**Fix:**
1. Verify BUILD.bazel has embedsrcs entries
2. Verify initial_values.go has go:embed variables and map entry
3. Run `./dev gen bazel` to update build files
4. Rebuild the test

### Error: TestLogic_cross_version_tenant_backup fails with "could not locate file cockroach-v25.4.0-rc.1"

**Cause:** `cockroach-go-testserver-25.4` configuration was added in M.2, but it should only be added in M.3.

**Why:** The `cockroach-go-testserver-25.4` configuration requires a predecessor version binary (v25.4.0-rc.1 or similar) to be available in `pkg/sql/logictest/REPOSITORIES.bzl`. These binaries are only added during M.3, after the first RC is published.

**What to add in M.2:**
- `local-mixed-25.4` configuration - ✅ This is correct for M.2

**What NOT to add in M.2:**
- `cockroach-go-testserver-25.4` configuration - ❌ Save this for M.3
- Entries in `cockroach-go-testserver-configs` set - ❌ Save this for M.3
- Visibility entries for `cockroach-go-testserver-25.4` in BUILD.bazel - ❌ Save this for M.3

**Fix:**
1. Remove `cockroach-go-testserver-25.4` config from `logictestbase.go`
2. Remove it from the `cockroach-go-testserver-configs` set
3. Delete the `pkg/sql/logictest/tests/cockroach-go-testserver-25.4/` directory
4. Remove `cockroach-go-testserver-25.4` visibility entries from `pkg/sql/logictest/BUILD.bazel`
5. Run `./dev gen bazel` to regenerate files
6. Update your commit message to remove references to cockroach-go-testserver-25.4

**Reference:** The `cockroach-go-testserver-25.3` configuration was added in M.3 (commit b4a7d05d8e8), not M.2.
### Timeline Context

In the release cycle:
- **M.1 (just completed)**: Master bumped to 26.1, release-25.4 exists
- **Now (M.2)**: Enable mixed-cluster logic tests with 25.4 bootstrap data
- **M.3 (later)**: After first 25.4 RC published, enable upgrade tests, update PreviousRelease
- **M.4 (later)**: Bump MinSupported version
- **M.5 (later)**: Finalize gates and bootstrap data when 25.4.0 final is released

### Notes

- **Bootstrap data is immutable:** Once generated from the release branch, these files should not change
- **Binary files:** Bootstrap .keys files are base64-encoded binary data, don't try to edit them
- **Test generation:** The generated_test.go files are auto-generated, don't edit manually
- **Skipif updates:** May need to update more logictest files as CI reveals compatibility issues
- **M.1 dependency:** This PR must be based on top of the M.1 PR, not master

### Quick Reference Commands

```bash
# Step 1: Generate bootstrap data
git checkout origin/release-25.4
./dev build sql-bootstrap-data && bin/sql-bootstrap-data
cp pkg/sql/catalog/bootstrap/data/25_4_* /tmp/
git checkout <your-m2-branch>
cp /tmp/25_4_* pkg/sql/catalog/bootstrap/data/

# Step 5: Generate test files
./dev gen bazel
git add pkg/ccl/logictestccl/tests/local-mixed-25.4/ \
        pkg/sql/logictest/tests/local-mixed-25.4/ \
        pkg/sql/logictest/tests/cockroach-go-testserver-25.4/ \
        pkg/sql/sqlitelogictest/tests/local-mixed-25.4/

# Step 7: Verification
./dev test pkg/sql/catalog/bootstrap -f TestInitialKeys -v
./dev testlogic base --config=local-mixed-25.4 --files=crdb_internal_catalog -v

# Validate hashes
cd pkg/sql/catalog/bootstrap/data
sha256sum 25_4_system.keys | awk '{print $1}' | diff - 25_4_system.sha256
sha256sum 25_4_tenant.keys | awk '{print $1}' | diff - 25_4_tenant.sha256
```

---

## M.3: Enable Upgrade Tests

This section provides step-by-step instructions for the M.3 task: "Enable upgrade tests" on the master branch after the first RC is published. This task enables roachtest upgrade tests with the newly released version.

### Overview

**When to perform:** After the first RC (e.g., v25.4.0-rc.1) is published and available on the releases page.

**What it does:**
- Updates `PreviousRelease` constant to enable upgrade testing from the forked release
- Generates roachtest fixtures for the RC version
- Adds `cockroach-go-testserver-25.4` logictest configuration
- Updates the releases file with RC binary information

**Critical requirement:** Fixture generation **MUST** be done on a gceworker (amd64 required, cannot use Mac).

**Dependencies:**
- First RC must be published (e.g., v25.4.0-rc.1)
- M.1 and M.2 should be completed
- Access to a gceworker

### Prerequisites

Before starting, ensure:
1. The first RC is published and available (verify at cockroachlabs.com/docs/releases)
2. You have access to a gceworker (see gceworker setup instructions)
3. M.1 and M.2 are complete (V25_4 constant exists, bootstrap data added)
4. You know the exact RC version (e.g., v25.4.0-rc.1)

### Two Approaches

Based on recent PRs, there are two valid approaches:

#### Approach A: Two separate PRs (recommended - most recent pattern)
1. **PR 1 (Fixtures)**: Update releases file + generate fixtures (~6 files)
2. **PR 2 (Code)**: Update PreviousRelease + add testserver config (~9 files)

**Recent examples:**
- Fixtures PR: [#150712](https://github.com/cockroachdb/cockroach/pull/150712)
- Code PR: [#152080](https://github.com/cockroachdb/cockroach/pull/152080)

#### Approach B: Single combined PR (older pattern)
1. **One PR**: Everything together (~15 files)

**Example:** [#141765](https://github.com/cockroachdb/cockroach/pull/141765)

**Recommendation:** Use Approach A (two PRs) as it's the most recent pattern and makes review easier.

---

### Approach A: Two-PR Method (Recommended)

## Phase 1: Fixtures PR

This PR adds the roachtest fixtures and updates the releases file.

### Step 1: Update Releases File (on Mac)

**a) Create a new branch:**
```bash
git checkout master  # or your M.2 branch if M.2 isn't merged yet
git pull origin master
git checkout -b enable-upgrade-tests-25.4-fixtures
```

**b) Build and run the release tool:**
```bash
bazel build //pkg/cmd/release:release
_bazel/bin/pkg/cmd/release/release_/release update-releases-file
```

**What this updates:**
- `pkg/testutils/release/cockroach_releases.yaml` - Adds latest RC version under "25.4"
- `pkg/sql/logictest/REPOSITORIES.bzl` - Adds RC binaries with checksums for all platforms

**c) Verify the changes:**
```bash
# Check that 25.4.0-rc.1 was added
grep -A 2 '"25.4":' pkg/testutils/release/cockroach_releases.yaml

# Check that the RC binaries were added to REPOSITORIES.bzl
grep "25.4.0-rc.1" pkg/sql/logictest/REPOSITORIES.bzl
```

**⚠️ CRITICAL: Manually Verify REPOSITORIES.bzl**

The `release update-releases-file` tool may **incorrectly remove** older version configs that are still needed by active testserver configurations.

**Check which testserver configs are active:**
```bash
grep "cockroach-go-testserver-" pkg/sql/logictest/logictestbase/logictestbase.go | grep "Name:"
```

**Verify REPOSITORIES.bzl has binaries for ALL active testserver versions:**
```bash
# Example: If you see 25.2, 25.3, 25.4 configs, verify all are in REPOSITORIES.bzl
grep -E "^\s+\(\"25\.[0-9]" pkg/sql/logictest/REPOSITORIES.bzl
```

**If a needed version is missing** (e.g., tool removed 25.2.7 but cockroach-go-testserver-25.2 still exists):
1. Get the old config from before M.3:
   ```bash
   git show HEAD^:pkg/sql/logictest/REPOSITORIES.bzl | grep -A 4 "25.2.7"
   ```
2. Manually restore it to REPOSITORIES.bzl (add it back to the _CONFIGS list)
3. Verify all active testserver versions are present

**Example from PR #156535:**
- Tool incorrectly removed 25.2.7
- But cockroach-go-testserver-25.2 still exists and needs those binaries
- Had to manually restore 25.2.7 entries

**Pattern:** Keep N-2, N-1, and N (current) release binaries until their testserver configs are removed.

**d) Commit the releases file update:**
```bash
git add pkg/testutils/release/cockroach_releases.yaml pkg/sql/logictest/REPOSITORIES.bzl
git commit -m "master: Update pkg/testutils/release/cockroach_releases.yaml

This updates the releases file with the v25.4.0-rc.1 release candidate and
adds the RC binaries to REPOSITORIES.bzl for use in upgrade tests.

Part of M.3 fixtures preparation.

Release note: None
Epic: None"
```

### Step 2: Generate Fixtures (MUST use gceworker)

**⚠️ CRITICAL:** This **CANNOT** be done on Mac. Must use gceworker (amd64 required).

The roachtest fixtures README states: "the roachtest needs to be run on `amd64` (if you are using a Mac it's recommended to use a gceworker)."

#### Step 2.1: Setup gceworker

**IMPORTANT:** According to gceworker setup notes, you **MUST** run `update-firewall` before `create`, otherwise you'll get stuck trying to connect and end up with a gceworker that doesn't have anything pre-installed.

**a) Set your zone (optional but recommended):**
```bash
# Add to ~/.zshrc to avoid specifying zone every time
echo 'export CLOUDSDK_COMPUTE_ZONE=us-west1-a' >> ~/.zshrc
source ~/.zshrc
```

Choose a zone close to you:
- `us-west1-a` or `us-west1-b` (Oregon - for SF/West Coast)
- `us-east1-b` (South Carolina - for East Coast)

**b) Update firewall rules (REQUIRED before create):**
```bash
# From your Mac, in the cockroach repo:
./scripts/gceworker.sh update-firewall
```

**c) Create the gceworker (if it doesn't exist):**
```bash
./scripts/gceworker.sh create
```

This will take 5-10 minutes and will auto-install dependencies (bazel, go, etc.).

**d) SSH to the gceworker:**
```bash
./scripts/gceworker.sh start
```

**Common SSH connection errors:**

If you see `ERROR: (gcloud.compute.ssh) [/usr/bin/ssh] exited with return code [255]`:
- The gceworker might already exist - run: `gcloud compute instances list | grep gceworker`
- If it exists but won't connect, try: `./scripts/gceworker.sh start` (it will retry)
- If connection still fails after retries, you may need to destroy and recreate:
  ```bash
  ./scripts/gceworker.sh destroy
  ./scripts/gceworker.sh update-firewall
  ./scripts/gceworker.sh create
  ```

#### Step 2.2: Clone and setup on gceworker

**On the gceworker terminal**, run:

```bash
# Create directory structure
mkdir -p ~/go/src/github.com/cockroachdb

# Clone the repo (use your fork so you can push later)
cd ~/go/src/github.com/cockroachdb
git clone git@github.com:<your-github-username>/cockroach.git
cd cockroach

# If SSH doesn't work, use HTTPS:
# git clone https://github.com/<your-github-username>/cockroach.git

# Add upstream remote and fetch tags
git remote add upstream https://github.com/cockroachdb/cockroach.git
git fetch upstream --tags

# Checkout the RC tag
git checkout v25.4.0-rc.1

# Set environment variables
export FIXTURE_VERSION=v25.4.0-rc.1
export COCKROACH_DEV_LICENSE="<your-license-key>"

# Verify license is set
echo $COCKROACH_DEV_LICENSE
```

**Expected:** Should show the license key starting with `crl-0-`.

**⚠️ Session Disconnection Note:**

If you close your laptop or lose SSH connection, you'll need to:
1. Reconnect: `./scripts/gceworker.sh start`
2. Navigate back: `cd ~/go/src/github.com/cockroachdb/cockroach`
3. Re-export environment variables:
   ```bash
   export FIXTURE_VERSION=v25.4.0-rc.1
   export COCKROACH_DEV_LICENSE="<your-license-key>"
   ```

#### Step 2.3: Build binaries on gceworker

**On the gceworker terminal**, run:

```bash
# First time only: run dev doctor to configure build settings
./dev doctor
```

When prompted:
- **"Which config you want to use (dev,crosslinux)?"** → Press **Enter** (accept default "dev")
- **"Do you want to use the lintonbuild configuration?"** → Type **n** then **Enter** (linters slow down builds)

```bash
# Build required binaries
./dev build cockroach short //c-deps:libgeos roachprod workload roachtest

# Clean up any previous local clusters (error "cluster local does not exist" is normal/OK)
./bin/roachprod destroy local
```

**Expected:**
- Build should complete successfully (may take 10-20 minutes on first build)
- You should see: `Successfully built binary for target //pkg/cmd/cockroach:cockroach`
- The `roachprod destroy` error "cluster local does not exist" is normal - it just means no previous cluster to clean up

**Common build errors:**

**Error: "please run `dev doctor` to refresh dev status"**
- Run `./dev doctor` and answer the prompts as shown above

**Error: "COCKROACH_DEV_LICENSE not set"**
- Re-export the license: `export COCKROACH_DEV_LICENSE="<license>"`

**Error: "bazel: command not found"**
- The gceworker should have bazel pre-installed. If not, run `./dev doctor` to install.

**Error: "./bin/roachprod: No such file or directory"**
- The initial build command might not have built all required binaries. Run:
  ```bash
  ./dev build roachprod workload roachtest
  ```

#### Step 2.4: Generate fixtures

**On the gceworker terminal**, run:

```bash
./bin/roachtest run generate-fixtures --local --debug \
  --cockroach ./cockroach --suite fixtures
```

**Expected:** The test will FAIL intentionally and print instructions like:

```
--- FAIL: generate-fixtures (XXs)
    fixtures.go:123:
        To complete the test, run the following:

        for i in 1 2 3 4; do
          mkdir -p pkg/cmd/roachtest/fixtures/${i} && \
          mv artifacts/generate-fixtures/run_1/logs/${i}.unredacted/checkpoint-*.tgz \
            pkg/cmd/roachtest/fixtures/${i}/
        done
```

**Common errors:**

**Error: "license required"**
- Verify COCKROACH_DEV_LICENSE is set: `echo $COCKROACH_DEV_LICENSE`

**Error: "roachprod cluster exists"**
- Clean up: `./bin/roachprod destroy local`

#### Step 2.5: Move fixtures to correct directories

**On the gceworker terminal**, copy the command from the test output and run it:

```bash
# Example command (use the exact command from your test output):
for i in 1 2 3 4; do
  mkdir -p pkg/cmd/roachtest/fixtures/${i} && \
  mv artifacts/generate-fixtures/run_1/logs/${i}.unredacted/checkpoint-*.tgz \
    pkg/cmd/roachtest/fixtures/${i}/
done
```

**Verify the fixtures were created:**
```bash
ls -lh pkg/cmd/roachtest/fixtures/*/checkpoint-v25.4.tgz
```

**Expected:** Should show 4 files (one in each directory 1, 2, 3, 4), each around 3-5 MB.

Example output:
```
-rw-rw-r-- 1 user user 4.4M Oct 30 12:26 pkg/cmd/roachtest/fixtures/1/checkpoint-v25.4.tgz
-rw-rw-r-- 1 user user 4.2M Oct 30 12:26 pkg/cmd/roachtest/fixtures/2/checkpoint-v25.4.tgz
-rw-rw-r-- 1 user user 3.7M Oct 30 12:26 pkg/cmd/roachtest/fixtures/3/checkpoint-v25.4.tgz
-rw-rw-r-- 1 user user 2.8M Oct 30 12:26 pkg/cmd/roachtest/fixtures/4/checkpoint-v25.4.tgz
```

**Note:** If your fixture sizes are significantly different (e.g., 0 bytes or >10MB), compare against existing fixtures from a previous version to verify they're correct:
```bash
# On your Mac, check existing fixture sizes
ls -lh pkg/cmd/roachtest/fixtures/*/checkpoint-v25.3.tgz
```

#### Step 2.6: Copy fixtures back to Mac

**From your Mac terminal** (NOT the gceworker), run:

```bash
# Create tmp directory for fixtures
mkdir -p /tmp/fixtures-25.4

# Copy the fixtures from gceworker to local tmp directory
# Replace <yourname> with your username and adjust zone if needed
scp -r gceworker-<yourname>.us-west1-a.cockroach-workers:~/go/src/github.com/cockroachdb/cockroach/pkg/cmd/roachtest/fixtures /tmp/fixtures-25.4/

# Navigate to your local cockroach repo
cd ~/go/src/github.com/cockroachdb/cockroach

# Switch to your fixtures branch
git checkout enable-upgrade-tests-25.4-fixtures

# Copy the fixtures to the correct locations
for i in 1 2 3 4; do
  cp /tmp/fixtures-25.4/fixtures/${i}/checkpoint-v25.4.tgz pkg/cmd/roachtest/fixtures/${i}/
done

# Verify they're in place
ls -lh pkg/cmd/roachtest/fixtures/*/checkpoint-v25.4.tgz
```

**Expected:** All 4 fixtures copied successfully, each ~3-5 MB.

**Alternative using roachprod:**
```bash
# If you created the gceworker with roachprod:
roachprod get <gceworker-name>:1 cockroach/pkg/cmd/roachtest/fixtures/ /tmp/fixtures-25.4/
```

### Step 3: Commit and Push Fixtures PR

**On your Mac:**

```bash
# Add the fixtures
git add pkg/cmd/roachtest/fixtures/*/checkpoint-v25.4.tgz

# Verify what's being committed
git status

# Create commit
git commit -m "roachtest: add 25.4 fixtures

This adds roachtest fixtures for v25.4.0-rc.1 to enable upgrade testing.

Fixtures were generated on gceworker by running:
  ./bin/roachtest run generate-fixtures --local --debug \\
    --cockroach ./cockroach --suite fixtures

Part of M.3 \"Enable upgrade tests\" checklist.

Release note: None
Epic: None

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"

# Push to your fork
git push -u origin enable-upgrade-tests-25.4-fixtures

# Create PR
gh pr create --repo cockroachdb/cockroach \
  --title "master: Update releases file and add 25.4 fixtures" \
  --body "Part of M.3: Enable upgrade tests for 25.4.

This PR:
- Updates releases file with v25.4.0-rc.1
- Updates REPOSITORIES.bzl with RC binaries
- Adds roachtest fixtures for v25.4.0-rc.1

Fixtures were generated on gceworker using amd64 architecture as required.

Part of the quarterly release runbook.

Epic: None"
```

### Step 4: Wait for Fixtures PR to Merge

Wait for the fixtures PR to be reviewed and merged before proceeding with the code PR.

---

## Phase 2: Code Changes PR

This PR updates the code to enable upgrade tests.

### Step 1: Create Branch (after fixtures PR merges)

```bash
# Pull the merged fixtures PR
git checkout master
git pull origin master

# Create new branch for code changes
git checkout -b enable-upgrade-tests-25.4-code
```

### Step 2: Update PreviousRelease Constant

**File:** `pkg/clusterversion/cockroach_versions.go`

**Change:** (around line 332)
```go
// Before
const PreviousRelease Key = V25_3

// After
const PreviousRelease Key = V25_4
```

### Step 3: Add cockroach-go-testserver-25.4 Configuration

**File:** `pkg/sql/logictest/logictestbase/logictestbase.go`

**a) Add the testserver configuration** (after the 25.3 config, around line 560):

```go
{
	// This config runs tests using a 25.4 predecessor binary, testing upgrade
	// compatibility.
	Name:                        "cockroach-go-testserver-25.4",
	NumNodes:                    1,
	OverrideDistSQLMode:         "off",
	UseCockroachGoTestserver:    true,
	CockroachGoTestserverVersion: "v25.4.0",
	DeclarativeCorpusCollection: true,
},
```

**b) Add to cockroach-go-testserver-configs set** (around line 615):

```go
"cockroach-go-testserver-configs": makeConfigSet(
	"cockroach-go-testserver-25.2",
	"cockroach-go-testserver-25.3",
	"cockroach-go-testserver-25.4",  // Add this line
),
```

### Step 4: Update Logictest BUILD.bazel Visibility

**File:** `pkg/sql/logictest/BUILD.bazel`

**Update cockroach_predecessor_version visibility** (around line 160):

```bazel
cockroach_predecessor_version(
    name = "cockroach_predecessor_version",
    visibility = [
        "//pkg/ccl/logictestccl:__subpackages__",
        "//pkg/sql/logictest/tests/cockroach-go-testserver-25.2:__pkg__",
        "//pkg/sql/logictest/tests/cockroach-go-testserver-25.3:__pkg__",
        "//pkg/sql/logictest/tests/cockroach-go-testserver-25.4:__pkg__",  # Add this line
        "//pkg/sql/sqlitelogictest:__subpackages__",
    ],
)
```

### Step 5: Verify supportsSkipUpgradeTo Logic

**File:** `pkg/cmd/roachtest/roachtestutil/mixedversion/mixedversion.go`

Check the `supportsSkipUpgradeTo` function (around line 850):

```go
func supportsSkipUpgradeTo(v *version.Version) bool {
	return v.Major() == 25 && v.Minor() == 4 && v.Patch() == 0 && v.PreRelease() == ""
}
```

The logic checks `v.Minor() == 4` which should already cover 25.4. Verify this is correct - if the Ordinal is 4 for 25.4, no changes needed.

**If changes are needed:** Update the condition to include the new version.

### Step 6: Generate Bazel Files

```bash
./dev gen bazel
```

This generates:
- `pkg/sql/logictest/tests/cockroach-go-testserver-25.4/BUILD.bazel`
- `pkg/sql/logictest/tests/cockroach-go-testserver-25.4/generated_test.go`
- Updates to various other BUILD.bazel files

### Step 7: Commit and Push Code PR

```bash
# Add all changes
git add -A

# Verify what's being committed
git status

# Commit
git commit -m "clusterversion: bump PreviousRelease to V25_4

This change updates the PreviousRelease constant from V25_3 to V25_4
and adds the cockroach-go-testserver-25.4 logictest configuration to
enable upgrade tests for version 25.4.

Changes include:
- Updated PreviousRelease constant in pkg/clusterversion/cockroach_versions.go
- Added cockroach-go-testserver-25.4 test configuration
- Generated test files for cockroach-go-testserver-25.4 config
- Updated BUILD.bazel files via ./dev gen bazel

The supportsSkipUpgradeTo logic already correctly handles 25.4 (Ordinal == 4)
so no changes were needed there.

Part of M.3 \"Enable upgrade tests\" checklist.

Release note: None
Epic: None

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"

# Push to your fork
git push -u origin enable-upgrade-tests-25.4-code

# Create PR
gh pr create --repo cockroachdb/cockroach \
  --title "clusterversion: bump PreviousRelease to V25_4" \
  --body "Part of M.3: Enable upgrade tests for 25.4.

This PR:
- Updates PreviousRelease constant to V25_4
- Adds cockroach-go-testserver-25.4 logictest configuration
- Generates test files for the new configuration

Depends on the fixtures PR being merged first.

Part of the quarterly release runbook.

Epic: None"
```

---

### Approach B: Single Combined PR (Alternative)

If you prefer to do everything in one PR:

**Step 1:** Follow all steps from Approach A Phase 1 and Phase 2, but do them on a single branch.

**Step 2:** Commit everything together with a combined commit message.

**Expected files:** ~15 files total (6 from fixtures + 9 from code changes).

---

### Expected Files Modified

#### Approach A - Fixtures PR (~6 files):
1. `pkg/testutils/release/cockroach_releases.yaml` - Updated by release tool
2. `pkg/sql/logictest/REPOSITORIES.bzl` - Updated by release tool with RC binaries
3. `pkg/cmd/roachtest/fixtures/1/checkpoint-v25.4.tgz` - Generated fixture
4. `pkg/cmd/roachtest/fixtures/2/checkpoint-v25.4.tgz` - Generated fixture
5. `pkg/cmd/roachtest/fixtures/3/checkpoint-v25.4.tgz` - Generated fixture
6. `pkg/cmd/roachtest/fixtures/4/checkpoint-v25.4.tgz` - Generated fixture

#### Approach A - Code PR (~9 files):
1. `pkg/clusterversion/cockroach_versions.go` - PreviousRelease constant
2. `pkg/sql/logictest/logictestbase/logictestbase.go` - Add testserver config
3. `pkg/sql/logictest/BUILD.bazel` - Visibility update
4. `pkg/BUILD.bazel` - Updated by `./dev gen bazel`
5. `pkg/cli/testdata/declarative-rules/deprules` - May be regenerated
6. `pkg/sql/logictest/tests/cockroach-go-testserver-25.4/BUILD.bazel` - Generated
7. `pkg/sql/logictest/tests/cockroach-go-testserver-25.4/generated_test.go` - Generated
8. `pkg/ccl/logictestccl/tests/cockroach-go-testserver-25.4/BUILD.bazel` - Generated
9. `pkg/ccl/logictestccl/tests/cockroach-go-testserver-25.4/generated_test.go` - Generated

**Note:** Additional test expectation files may need updates based on CI test results.

#### Approach B - Combined PR:
All of the above files in one PR (~15 files).

---

### Validation and Verification

### CRITICAL: Validate Before Creating PR

**This task is performed every quarter.** Before creating the M.3 PR(s), validate that changes follow the expected pattern.

#### Step 1: Compare with Previous M.3 PRs

Find the most recent M.3 PRs for reference:

**Recent M.3 PRs (Two-PR approach):**
- **Fixtures PR #150712** - 6 files (releases.yaml, REPOSITORIES.bzl, 4 fixtures)
- **Code PR #152080** - 9 files (cockroach_versions.go, logictestbase.go, BUILD.bazel, etc.)

**Compare file lists:**

**For Fixtures PR:**
```bash
# Get files from reference PR
gh pr view 150712 --json files --jq '.files[].path' | sort > /tmp/ref_fixtures.txt

# Get your current files
git diff --name-only master | sort > /tmp/my_fixtures.txt

# Compare
echo "=== Files ONLY in your PR (investigate!) ==="
comm -13 /tmp/ref_fixtures.txt /tmp/my_fixtures.txt

echo "=== Files ONLY in reference PR (might be missing!) ==="
comm -23 /tmp/ref_fixtures.txt /tmp/my_fixtures.txt
```

**Expected differences:**
- Version numbers (25.3 → 25.4)
- RC version (rc.1 vs rc.2, etc.)

**For Code PR:**
```bash
# Get files from reference PR
gh pr view 152080 --json files --jq '.files[].path' | sort > /tmp/ref_code.txt

# Get your current files
git diff --name-only master | sort > /tmp/my_code.txt

# Compare
echo "=== Files ONLY in your PR (investigate!) ==="
comm -13 /tmp/ref_code.txt /tmp/my_code.txt

echo "=== Files ONLY in reference PR (might be missing!) ==="
comm -23 /tmp/ref_code.txt /tmp/my_code.txt
```

#### Step 2: Verify Fixture Integrity

```bash
# Check file sizes (should be 3-5 MB each)
ls -lh pkg/cmd/roachtest/fixtures/*/checkpoint-v25.4.tgz

# Verify all 4 fixtures exist
for i in 1 2 3 4; do
  if [ ! -f "pkg/cmd/roachtest/fixtures/${i}/checkpoint-v25.4.tgz" ]; then
    echo "Missing fixture $i"
  fi
done

# Compare with previous version fixtures to verify sizes are reasonable
ls -lh pkg/cmd/roachtest/fixtures/*/checkpoint-v25.3.tgz
```

#### Step 3: Verify Releases File

```bash
# Check that the RC was added
grep -A 5 '"25.4":' pkg/testutils/release/cockroach_releases.yaml

# Check that REPOSITORIES.bzl has the binaries
grep -c "25.4.0-rc.1" pkg/sql/logictest/REPOSITORIES.bzl
```

Expected: Should show multiple entries (one for each platform).

#### Step 4: Run Tests

**For Fixtures PR:**
```bash
# No specific tests - the fixtures are binary artifacts
# Just verify they exist and have reasonable sizes
```

**For Code PR:**
```bash
# Test that the new config is recognized
./dev testlogic base --config=cockroach-go-testserver-25.4 --files=cluster_settings -v

# Bootstrap tests
./dev test pkg/sql/catalog/bootstrap -f TestInitialKeys -v

# Version tests
./dev test pkg/clusterversion pkg/roachpb -v
```

Expected: All tests should pass.

### Verification Checklist

**Before creating Fixtures PR:**
- [ ] Releases file updated with v25.4.0-rc.1
- [ ] REPOSITORIES.bzl has RC binaries for all platforms
- [ ] All 4 fixture files generated (1, 2, 3, 4)
- [ ] Fixture files are reasonable size (3-5 MB each, compare with v25.3 fixtures)
- [ ] Fixtures generated on gceworker (amd64), not Mac
- [ ] File count is ~6 files
- [ ] Compared against previous fixtures PR (#150712)
- [ ] gceworker firewall rules updated before creating worker

**Before creating Code PR:**
- [ ] PreviousRelease updated to V25_4
- [ ] cockroach-go-testserver-25.4 config added to logictestbase.go
- [ ] Config added to cockroach-go-testserver-configs set
- [ ] BUILD.bazel visibility updated
- [ ] supportsSkipUpgradeTo logic verified (should already support 25.4)
- [ ] `./dev gen bazel` run successfully
- [ ] Generated test directories created
- [ ] File count is ~9 files (may vary with additional test updates)
- [ ] Compared against previous code PR (#152080)
- [ ] Logic tests pass with new config
- [ ] Bootstrap tests pass

### Common Errors and Solutions

#### Error: "could not locate file cockroach-v25.4.0-rc.1"

**Cause:** REPOSITORIES.bzl not updated or RC binaries not available yet.

**Fix:**
1. Verify RC is published: check cockroachlabs.com/docs/releases
2. Re-run `release update-releases-file`
3. Verify REPOSITORIES.bzl contains the RC binaries

#### Error: "fixture generation failed with license error"

**Cause:** COCKROACH_DEV_LICENSE not set on gceworker.

**Fix:**
```bash
# On gceworker:
export COCKROACH_DEV_LICENSE="<your-license-key>"
echo $COCKROACH_DEV_LICENSE  # Verify it's set
```

#### Error: "no such file: checkpoint-v25.4.tgz"

**Cause:** Fixture generation didn't complete or files weren't moved correctly.

**Fix:**
1. Check if artifacts directory exists: `ls artifacts/generate-fixtures/`
2. Look for the checkpoint files: `find artifacts -name "checkpoint-*.tgz"`
3. Re-run the move command from the test output

#### Error: "scp: No such file or directory" when copying from gceworker

**Cause:** Path to fixtures incorrect or gceworker hostname wrong.

**Fix:**
```bash
# Verify the fixtures exist on gceworker first
ssh gceworker-<yourname>.us-west1-b.cockroach-workers \
  "ls -lh ~/go/src/github.com/cockroachdb/cockroach/pkg/cmd/roachtest/fixtures/*/checkpoint-v25.4.tgz"

# Use the correct full path in scp
```

#### Error: "fixture size is 0 bytes or very small"

**Cause:** Fixture generation failed but created empty files.

**Fix:**
1. Check roachtest logs: `cat artifacts/generate-fixtures/run_1/logs/*/roachtest.log`
2. Look for errors in the logs
3. Re-run fixture generation after fixing the issue

#### Error: "undefined: clusterversion.V25_4"

**Cause:** M.1 and M.2 not completed or working on wrong branch.

**Fix:**
1. Verify V25_4 constant exists in `pkg/clusterversion/cockroach_versions.go`
2. Ensure your branch is based on master with M.1 merged

#### Error: "supportsSkipUpgradeTo test failures"

**Cause:** The version skip logic doesn't recognize 25.4.

**Fix:**
1. Check the `supportsSkipUpgradeTo` function in `mixedversion.go`
2. Verify the Ordinal for 25.4 is 4
3. Update the logic if needed to include 25.4

#### Error: "cannot find cockroach binary" during fixture generation

**Cause:** Build didn't complete or binary not in expected location.

**Fix:**
```bash
# On gceworker, verify the binary exists:
ls -lh ./cockroach

# If missing, rebuild:
./dev build cockroach short
```

#### Error: "gceworker clone is taking forever"

**Cause:** CockroachDB repo is large (~10GB).

**Solution:**
1. Use shallow clone to speed up:
   ```bash
   git clone --depth 1 --branch v25.4.0-rc.1 \
     https://github.com/cockroachdb/cockroach.git
   ```
2. Or wait for full clone (can take 15-30 minutes depending on connection)
3. Consider keeping the gceworker running between tasks

### Post-M.3 Test Failures: Investigation Strategy

After creating the M.3 PR, CI will typically reveal test failures. These are **expected** and follow patterns from previous quarters. Use this strategy to triage and fix them:

**Step 1: Compare with previous M.3 PRs to understand the pattern**

```bash
# Find which test files were modified in previous M.3 code PR
gh pr view 152080 --json files --jq '.files[] | select(.path | contains("test")) | .path'

# Example output shows:
# pkg/crosscluster/logical/logical_replication_job_test.go  ← V25_2 → V25_3 fix
# pkg/sql/logictest/testdata/logic_test/vector_index_mixed  ← Config restriction
```

**Step 2: Examine the actual fixes from previous PRs**

```bash
# See what changed in a specific test file
gh api repos/cockroachdb/cockroach/pulls/152080/files | \
  jq -r '.[] | select(.filename == "pkg/crosscluster/logical/logical_replication_job_test.go") | .patch'

# This shows the V25_2 → V25_3 pattern, which you'll replicate as V25_3 → V25_4
```

**Step 3: Apply the same patterns to your failures**

Common test failure patterns documented below (in order of frequency):
1. Tests using `MakeTestingClusterSettingsWithVersions` with old version + PreviousRelease
2. Version-gated logictests running on new testserver config when they test pre-release behavior
3. CLI tests expecting old version numbers in output

**For each failed test:**
1. Check if previous M.3 PR modified a similar test
2. Understand the fix pattern
3. Apply the same pattern to the current version

#### Error: "TestDeclarativeRules/declarative_corpus_validation_standalone_command" fails with version mismatch

**Cause:** After updating `PreviousRelease` to V25_4, the CLI test expects declarative rules test data to reference version 25.4 instead of 25.3.

**Symptoms:**
```
output didn't match expected:
  @@ -1,6 +1,6 @@
  -debug declarative-print-rules 1000025.3 dep
  +debug declarative-print-rules 1000025.4 dep
```

**Fix:**
Regenerate the test data using the `--rewrite` flag:
```bash
./dev test pkg/cli -f TestDeclarativeRules --rewrite
```

This updates `pkg/cli/testdata/declarative-rules/deprules` with the correct version references and new schema changer rules for 25.4.

#### Error: Test failures with "minimum supported version X cannot be greater than binary version Y"

**Cause:** After updating `PreviousRelease` from V25_3 to V25_4, tests that use `PreviousRelease` in combination with hardcoded older versions (like V25_3) may create invalid cluster configurations where the minimum supported version is higher than the binary version.

**Example symptom:**
```
F251030 16:52:35.743665 61030 settings/cluster/cluster_settings.go:183
Fatal error: minimum supported version 25.4 cannot be greater than binary version 25.3
```

**Real-world example from PR #156535:**
```go
// This test FAILED after changing PreviousRelease from V25_3 to V25_4:
st := cluster.MakeTestingClusterSettingsWithVersions(
    clusterversion.V25_3.Version(),          // Binary version (25.3)
    clusterversion.PreviousRelease.Version(), // Min supported (now 25.4!)
    true,
)
// ERROR: Min supported (25.4) > binary version (25.3) is invalid
```

**CRITICAL: Context-Aware Fix Required**

⚠️ **DO NOT blindly replace all V25_3 with V25_4** - you must understand the test's purpose first.

**When TO update V25_3 → V25_4:**
1. **Binary/cluster version in relation to PreviousRelease:**
   ```go
   // CORRECT FIX: Binary version must be >= min supported version
   st := cluster.MakeTestingClusterSettingsWithVersions(
       clusterversion.V25_4.Version(),          // Updated to match new PreviousRelease
       clusterversion.PreviousRelease.Version(), // V25_4
       true,
   )
   ```

2. **Testing current release functionality:**
   - Tests that validate features in the "current stable release" (now 25.4)
   - Tests checking upgrade compatibility FROM the new previous release

3. **Testing version gates that should be active at V25_4:**
   - When the test validates that a feature is enabled at V25_4 or higher

**When NOT to update V25_3 → V25_4:**
1. **Testing upgrade paths FROM older versions:**
   ```go
   // KEEP V25_3: Testing upgrade from 25.3 → 26.1
   testUpgrade(
       from: clusterversion.V25_3.Version(),  // Don't change!
       to: clusterversion.Latest.Version(),
   )
   ```

2. **Testing backward compatibility with specific old versions:**
   ```go
   // KEEP V25_3: Testing that 25.3 clusters can read new data
   testBackwardCompat(clusterversion.V25_3)  // Don't change!
   ```

3. **Testing mixed-version behavior for specific versions:**
   ```go
   // KEEP V25_3: Testing 25.3 + 26.1 mixed cluster
   testMixedVersion(
       old: clusterversion.V25_3,  // Don't change!
       new: clusterversion.Latest,
   )
   ```

**How to analyze and fix:**

1. **Read the test name and context:**
   ```bash
   # Understand what the test is doing
   # Example: "TestGetWriterType/immediate-mode" with cluster settings
   ```

2. **Check if test uses PreviousRelease:**
   ```bash
   grep -A 5 "PreviousRelease" <test_file.go>
   ```

3. **Determine the relationship:**
   - Is V25_3 meant to be the "binary/current version"? → Update to V25_4
   - Is V25_3 meant to be an "old version to upgrade from"? → Keep V25_3

4. **Look for these patterns that need fixing:**
   ```go
   // PATTERN 1: Binary version as first argument to MakeTestingClusterSettingsWithVersions
   cluster.MakeTestingClusterSettingsWithVersions(
       clusterversion.V25_3.Version(),          // ← FIX: Update to V25_4
       clusterversion.PreviousRelease.Version(), // V25_4
       true,
   )

   // PATTERN 2: MaxVersion in version range checks
   if version >= V25_3 && version < PreviousRelease {  // ← FIX: Update to V25_4

   // PATTERN 3: Feature availability checks
   featureAvailableAt := V25_3  // ← Maybe update to V25_4 if it's "current release"
   ```

5. **Validate your fix:**
   ```bash
   # Run the specific test
   ./dev test <package> -f <TestName> -v
   ```

**Search for affected tests:**
```bash
# Find tests that might have this issue (binary version < PreviousRelease)
grep -r "V25_3.*PreviousRelease\|PreviousRelease.*V25_3" --include="*_test.go" pkg/

# Review each match carefully - don't automatically change all!
```

**Example fix from PR #156535:**
```go
// File: pkg/crosscluster/logical/logical_replication_job_test.go
// Test: TestGetWriterType/immediate-mode

// BEFORE (BROKEN after PreviousRelease → V25_4):
st := cluster.MakeTestingClusterSettingsWithVersions(
    clusterversion.V25_3.Version(),          // Binary: 25.3
    clusterversion.PreviousRelease.Version(), // Min: 25.4 ❌
    true,
)

// AFTER (FIXED - binary version must be >= min supported):
st := cluster.MakeTestingClusterSettingsWithVersions(
    clusterversion.V25_4.Version(),          // Binary: 25.4 ✓
    clusterversion.PreviousRelease.Version(), // Min: 25.4 ✓
    true,
)

// RATIONALE: This test is checking immediate-mode writer behavior on a
// current-version cluster, not testing upgrade from 25.3. The binary
// version should match the new PreviousRelease.
```

#### Error: Version-gated logictest failures on cockroach-go-testserver-25.4

**Cause:** After adding the `cockroach-go-testserver-25.4` configuration, tests that verify "feature X is NOT supported before version Y" will fail when running on the 25.4 testserver because the feature is now available.

**Example symptom:**
```
--- FAIL: TestLogic_mixed_version_partial_stats (24.32s)
    logic.go:4491: expected "pq: creating partial statistics with a WHERE clause is not yet supported",
                   but no error occurred
```

**Real-world example from PR #156535:**
```
# File: pkg/sql/logictest/testdata/logic_test/mixed_version_partial_stats
# LogicTest: cockroach-go-testserver-configs  ← Runs on ALL testserver configs

# Test expects feature to NOT be available initially
statement error pq: creating partial statistics with a WHERE clause is not yet supported
CREATE STATISTICS pstat ON a FROM t WHERE a > 2;

# When this test runs on testserver-25.4, the feature IS available,
# so the expected error doesn't occur → test fails
```

**Fix:** Restrict the test to run only on the previous testserver config (not the new one):

```diff
-# LogicTest: cockroach-go-testserver-configs
+# LogicTest: cockroach-go-testserver-25.3
```

**IMPORTANT: Multi-version testing pattern for upgrade tests**

For tests with `LogicTest: cockroach-go-testserver-configs` that have comments mentioning they test upgrade behavior to a specific series, update them to use the **two previous series** starting from `MinSupported`:

**Rule:**
```
LogicTest: cockroach-go-testserver-configs
→
LogicTest: cockroach-go-testserver-{MinSupported} cockroach-go-testserver-{MinSupported+0.1}
```

**How to find MinSupported:**
Look in `pkg/clusterversion/cockroach_versions.go`:
```go
const (
    V25_2 // MinSupported
    V25_3
    V26_1 // Latest
)
```

**Example from PR #156535 (M.3 for 25.4):**

Given `MinSupported = V25_2`, update tests like:

```diff
 # LogicTest: cockroach-go-testserver-configs
+# LogicTest: cockroach-go-testserver-25.2 cockroach-go-testserver-25.3

-# Sanity check that partial statistics with WHERE clause is only allowed to
-# be used once the cluster is upgraded to 25.4.
+# Sanity check that partial statistics with WHERE clause is only allowed to
+# be used once the cluster is upgraded to 25.4.
```

**Why two versions?** This ensures the test validates upgrade behavior from:
- The minimum supported version (25.2)
- The previous release (25.3)
- But NOT the current release (25.4) where the feature is already available

**Tests that follow this pattern:**
- `mixed_version_partial_stats` - Comment mentions "upgraded to 25.4"
- `mixed_version_ltree` - Comment mentions "not supported until version 25.4"
- Any test with comments like "only allowed after upgrade to X.Y" or "not supported until version X.Y"

**Pattern from PR #152080:**
```bash
# In previous M.3 for 25.3, vector_index_mixed was changed from:
# LogicTest: cockroach-go-testserver-configs
# To:
# LogicTest: cockroach-go-testserver-25.2
```

**How to identify tests that need this fix:**

1. **Test failure shows**: "expected error but no error occurred" on testserver-25.4
2. **Test validates pre-release behavior**: Tests with names like `mixed_version_*` that verify features are gated
3. **Test uses `upgrade` commands**: Tests that upgrade from an old version and verify behavior changes

**When TO restrict tests:**
- Test expects an error/unsupported message that's no longer true on 25.4
- Test validates version-gated behavior that's checking "before 25.4"
- Test name suggests it's testing upgrade compatibility with a specific version

**When NOT to restrict tests:**
- Test validates generic upgrade behavior (e.g., login works during upgrade)
- Test doesn't have version-specific assertions
- Test name is like `upgrade` (generic) vs `mixed_version_feature_x` (specific feature)

**Search for potentially affected tests:**
```bash
# Find tests running on all testserver configs
grep -r "# LogicTest: cockroach-go-testserver-configs" pkg/sql/logictest/testdata/logic_test/

# Check each for version-specific behavior
# Examples of tests that likely need restriction:
# - mixed_version_<feature_name> - if testing feature gated at 25.4
# - Tests with "statement error" expecting version-gated errors

# Examples of tests that are fine on all configs:
# - mixed_version_can_login - generic upgrade behavior
# - cross_version_tenant_backup - generic backup behavior during upgrade
```

**Example from PR #156535:**
- ✅ **Fixed**: `mixed_version_partial_stats` → Changed to `cockroach-go-testserver-25.3`
  - Reason: Tests that partial stats with WHERE is NOT supported before 25.4
- ✅ **Left alone**: `mixed_version_can_login` → Still uses `cockroach-go-testserver-configs`
  - Reason: Tests generic login behavior during upgrade, not version-specific

#### Error: mixed_version_bootstrap_tenant test failures due to schema evolution

**Cause:** The `mixed_version_bootstrap_tenant` test compares bootstrap data between old and new executables to ensure identical system catalog initialization. When system tables evolve between versions (adding columns, changing descriptors), the test fails because table descriptors at keys `/Table/3/1/<tableID>/2/1` differ between versions.

**Example symptom from PR #156535:**
```
--- FAIL: TestLogic_mixed_version_bootstrap_tenant
    logic.go:4491: expected 0 rows, but found differences at:
    /Table/3/1/12/2/1  old_executable  <descriptor_hash_1>
    /Table/3/1/12/2/1  new_executable  <descriptor_hash_2>
    /Table/3/1/15/2/1  old_executable  <descriptor_hash_3>
    /Table/3/1/15/2/1  new_executable  <descriptor_hash_4>
```

**Root cause:** System tables changed between versions. For example:
- Table 12 (system.eventlog) gained a `payload` column in PR #146130
- Table 15 (system.jobs) descriptor may have changed due to schema migrations
- Other system tables may evolve with new columns, constraints, or metadata

**Before choosing a fix, consult code owners:**

This test has complex trade-offs between test coverage and maintenance burden. Before implementing a fix, consult:
- **Test author**: Rafi Shamim <rafi@cockroachlabs.com> (created in commit 11d257111aa)
- **Frequent contributor**: Radu Berinde <radu@cockroachlabs.com> (4 commits including adding cockroach-go-testserver-configs)
- **Team owner**: @cockroachdb/sql-foundations-noreview (from .github/CODEOWNERS)

You can find current contributors using:
```bash
git log --format='%an <%ae>' --follow pkg/sql/logictest/testdata/logic_test/mixed_version_bootstrap_tenant | sort | uniq -c | sort -rn
```

**Two possible approaches:**

**Approach A: Restrict test to older testserver config**

Limit the test to run only on previous version configs where schema hasn't evolved yet:

```diff
-# LogicTest: cockroach-go-testserver-configs
+# LogicTest: cockroach-go-testserver-25.2
```

✅ **Pros:**
- Simple, straightforward fix
- No changes to test logic
- Follows pattern used for other version-gated tests

❌ **Cons:**
- Reduces test coverage for newer versions
- Test won't validate that 25.3→25.4 bootstrap remains consistent
- May need to be updated again next quarter

**Use this when:** Schema evolution is expected and intentional, and validating older version consistency is more important than newer version coverage.

**Approach B: Update test to exclude table descriptors**

Modify the test's WHERE clause to exclude table descriptor keys that are allowed to differ:

```diff
 SELECT k, is_present_in, v FROM bootstrapped_tenant_data
 WHERE is_present_in <> 'both'
   AND k <> '/Table/6/1/"version"/0'
+  AND k NOT LIKE '/Table/3/1/%/2/1'
 ORDER BY 1, 2
```

Also update the duplicate assertion:
```diff
 SELECT 1/count(*) FROM bootstrapped_tenant_data
 WHERE is_present_in <> 'both'
   AND k <> '/Table/6/1/"version"/0'
+  AND k NOT LIKE '/Table/3/1/%/2/1'
```

✅ **Pros:**
- Maintains test coverage for all testserver configs
- Acknowledges that system table evolution is expected
- Test continues to validate non-descriptor bootstrap data consistency

❌ **Cons:**
- Changes test intent (originally validated ALL bootstrap data equality)
- May mask legitimate bootstrap bugs in table descriptors
- Pattern `/Table/3/1/%/2/1` is broad (excludes all table descriptors, not just evolved ones)

**Use this when:** You want to maintain broad coverage but accept that system table schemas evolve between versions as part of normal development.

**Historical context:**

This test has evolved over time:
- Originally validated complete bootstrap data equality
- In commit a0ba7326a22 (Aug 2025): Restricted from `cockroach-go-testserver-configs` to `cockroach-go-testserver-24.1` due to schema evolution
- In commit 11d257111aa (Oct 2025): Changed back to `cockroach-go-testserver-configs` with comment: "Check that the bootstrapped data for tenants remains the same... Only the 'version' value in system.settings and table descriptors in system.descriptor are allowed to differ"

The comment in commit 11d257111aa suggests that **Approach B** (excluding descriptors) may align with the original test author's intent, but consult with the code owners to confirm.

**Decision matrix:**

| Scenario | Recommended Approach | Rationale |
|----------|---------------------|-----------|
| System table changes are intentional for 25.4 | Approach B | Test comment already acknowledges descriptors can differ |
| You're unsure if schema changes are intentional | Consult code owners first | Don't mask potential bugs |
| Test keeps breaking every quarter | Approach B | Reduce maintenance burden |
| Coverage of all configs is critical | Approach B | Maintains broader test execution |

### CRITICAL: Validate Changes Before Creating PR

This is a **quarterly task** - validate your changes against previous M.3 PRs to ensure consistency and catch any missing or unexpected files.

#### Reference PRs

Compare your changes against these previous M.3 PRs:
- **25.3 M.3 Combined PR**: #141765 (~12 files - fixtures + code in one PR)
- **25.3 M.3 Fixtures PR**: #150712 (~6 files - fixtures only)
- **25.3 M.3 Code PR**: #152080 (~9 files - code only)

Use the combined PR as the primary reference if you're doing a single-PR approach, or the separate PRs if you're doing the two-PR approach.

#### Expected File Counts

**Single combined PR approach (~14 files):**
- 4 fixture files: `pkg/cmd/roachtest/fixtures/{1,2,3,4}/checkpoint-v25.4.tgz`
- 1 releases file: `pkg/testutils/release/cockroach_releases.yaml`
- 1 repositories file: `pkg/sql/logictest/REPOSITORIES.bzl`
- 1 version file: `pkg/clusterversion/cockroach_versions.go`
- 1 deprules file: `pkg/cli/testdata/declarative-rules/deprules`
- 1 BUILD.bazel: `pkg/BUILD.bazel` (visibility update)
- 1 logictestbase: `pkg/sql/logictest/logictestbase/logictestbase.go`
- 1 logictest BUILD: `pkg/sql/logictest/BUILD.bazel`
- 2 generated test files: `pkg/sql/logictest/tests/cockroach-go-testserver-25.4/{BUILD.bazel,generated_test.go}`
- Plus any test fixes (e.g., tests using V25_3 with PreviousRelease)

**Two-PR approach:**
- Fixtures PR: ~6 files (4 fixtures + releases.yaml + REPOSITORIES.bzl)
- Code PR: ~9 files (versions.go, deprules, BUILD files, test configs, test fixes)

#### How to Validate

**1. Get file lists:**
```bash
# Your current PR files
gh pr view <YOUR_PR_NUMBER> --json files --jq '.files[].path' | sort > /tmp/current_m3_files.txt

# Reference PR files (use #141765 for combined, or #152080 for code-only)
gh pr view 141765 --json files --jq '.files[].path' | sort > /tmp/ref_m3_combined.txt

# Compare
comm -3 /tmp/current_m3_files.txt /tmp/ref_m3_combined.txt
```

**2. Expected differences (version-specific files):**
- Your PR will have `checkpoint-v25.4.tgz` files, reference has `checkpoint-v25.1.tgz` (or v25.3 for #141765)
- Your PR will have `cockroach-go-testserver-25.4/` directory, reference has `cockroach-go-testserver-25.1/` (or 25.3)
- Your PR may have `CLAUDE.md` updates with new documentation
- Your PR may have additional test fixes (files with V25_3 → V25_4 updates)
- Reference PR may have files specific to that quarter (e.g., mixed_version test files for bootstrap data updates)

**3. Investigate if:**
- File count differs by more than 3-4 files from reference
- You're missing core files (fixtures, releases.yaml, REPOSITORIES.bzl, cockroach_versions.go)
- You have unexpected package changes beyond test fixes

**4. Review each unexpected file:**
```bash
# For each file only in your PR, understand why:
gh pr diff <YOUR_PR_NUMBER> -- path/to/unexpected/file

# Is it:
# - A necessary test fix due to PreviousRelease bump? ✓ OK
# - Documentation update? ✓ OK
# - An unrelated change that should be in a separate PR? ❌ Remove
```

#### Validation Checklist

Before creating/updating the M.3 PR:
- [ ] Compared file list against reference PR (#141765 for combined, or #150712/#152080 for two-PR)
- [ ] File count is within expected range (~14 for combined, ~6+9 for two-PR approach)
- [ ] All core files are present (fixtures, releases, versions, test configs)
- [ ] Version-specific differences explained (25.4 vs 25.1/25.3 files)
- [ ] Any unexpected files justified and documented
- [ ] No unrelated changes included
- [ ] Test fixes follow context-aware guidelines (see "Error: Test failures with minimum supported version" above)

**Example validation from PR #156535:**
```bash
$ comm -3 /tmp/current_m3_files.txt /tmp/ref_m3_combined.txt
# Only in current (14 files):
CLAUDE.md                                    # ← OK: New documentation
checkpoint-v25.4.tgz (×4)                   # ← OK: Version-specific
cockroach-go-testserver-25.4/ (×2)          # ← OK: Version-specific

# Only in reference (12 files):
README.md                                    # ← OK: Not needed in this PR
mixed_version_stats, mixed_version_ttl      # ← OK: Bootstrap updates from M.2
cockroach-go-testserver-25.1/ (×2)          # ← OK: Version-specific to that quarter

# Conclusion: All differences explained and expected ✓
```

---

### Timeline Context

In the release cycle:
- **M.1 (completed)**: Master bumped to 26.1
- **M.2 (completed)**: Mixed-cluster logic tests enabled with 25.4 bootstrap data
- **Now (M.3)**: Enable upgrade tests after first RC published
- **M.4 (later)**: Bump MinSupported version
- **M.5 (later)**: Finalize gates when 25.4.0 final is released

### Notes

- **Fixtures are platform-specific:** Must be generated on amd64, hence the gceworker requirement
- **Fixture sizes:** Each checkpoint file is typically 3-5 MB, totaling ~15-20 MB for all 4 fixtures
- **Firewall rules required:** Must run `./scripts/gceworker.sh update-firewall` BEFORE creating gceworker
- **RC must be published first:** Cannot do M.3 until the first RC (e.g., v25.4.0-rc.1) is available
- **Two-PR approach is cleaner:** Separating fixtures and code changes makes review easier
- **Fixtures are immutable:** Once generated for an RC, they shouldn't change
- **License is required:** Fixture generation requires COCKROACH_DEV_LICENSE environment variable
- **Session management:** If SSH connection drops, remember to re-export environment variables when reconnecting

### Quick Reference Commands

**Setup gceworker - On Mac (one-time):**
```bash
# Optional: Set default zone
echo 'export CLOUDSDK_COMPUTE_ZONE=us-west1-a' >> ~/.zshrc
source ~/.zshrc

# REQUIRED: Update firewall before creating
./scripts/gceworker.sh update-firewall

# Create gceworker
./scripts/gceworker.sh create

# Connect to gceworker
./scripts/gceworker.sh start
```

**Fixtures PR - On Mac:**
```bash
# Update releases file
bazel build //pkg/cmd/release:release
_bazel/bin/pkg/cmd/release/release_/release update-releases-file

# Verify changes
grep -A 2 '"25.4":' pkg/testutils/release/cockroach_releases.yaml
```

**Fixtures Generation - On gceworker:**
```bash
# Setup (after SSH'ing in)
cd ~/go/src/github.com/cockroachdb/cockroach
git checkout v25.4.0-rc.1
export FIXTURE_VERSION=v25.4.0-rc.1
export COCKROACH_DEV_LICENSE="<license>"

# Run dev doctor (first time only)
./dev doctor
# Press Enter for "dev", type "n" for lintonbuild

# Build and generate
./dev build cockroach short //c-deps:libgeos roachprod workload roachtest
./bin/roachprod destroy local  # Error "cluster local does not exist" is OK
./bin/roachtest run generate-fixtures --local --debug \
  --cockroach ./cockroach --suite fixtures

# Move fixtures (use command from test output)
for i in 1 2 3 4; do
  mkdir -p pkg/cmd/roachtest/fixtures/${i} && \
  mv artifacts/generate-fixtures/run_1/logs/${i}.unredacted/checkpoint-*.tgz \
    pkg/cmd/roachtest/fixtures/${i}/
done

# Verify (should be 3-5 MB each)
ls -lh pkg/cmd/roachtest/fixtures/*/checkpoint-v25.4.tgz
```

**Copy Fixtures - On Mac:**
```bash
# Create tmp directory
mkdir -p /tmp/fixtures-25.4

# Copy from gceworker (adjust username and zone)
scp -r gceworker-<name>.us-west1-a.cockroach-workers:~/go/src/github.com/cockroachdb/cockroach/pkg/cmd/roachtest/fixtures /tmp/fixtures-25.4/

# Copy to local repo
for i in 1 2 3 4; do
  cp /tmp/fixtures-25.4/fixtures/${i}/checkpoint-v25.4.tgz pkg/cmd/roachtest/fixtures/${i}/
done
```

**Code PR - On Mac:**
```bash
# After fixtures PR merges
git checkout master && git pull origin master
git checkout -b enable-upgrade-tests-25.4-code

# Make code changes (PreviousRelease, testserver config, BUILD.bazel visibility)
# Then:
./dev gen bazel

# Verify
./dev testlogic base --config=cockroach-go-testserver-25.4 --files=cluster_settings
```

---

### Next Steps

**For future Claude sessions working on M.4 or M.5:**

The remaining master branch tasks (M.4, M.5) have checklists in `pkg/clusterversion/README.md` but do not yet have detailed runbooks in this file. When implementing these tasks:

1. **Add a detailed runbook section to this file** following the pattern established by R.1, R.2, M.1, and M.2
2. **Ensure the README.md has a "Claude Prompt"** that references this file (already added for M.3, M.4, M.5)
3. **Follow the established structure:** Overview, Prerequisites, Step-by-Step Checklist, Expected Files Modified, Validation/Verification, Common Errors, Quick Reference Commands
4. **Base your runbook on:** Previous PRs listed in README.md, the checklist, and lessons learned during implementation

This pattern ensures consistency and makes the quarterly release process smoother for future teams.
