# M.1: Bump Current Version (Master Branch)

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

