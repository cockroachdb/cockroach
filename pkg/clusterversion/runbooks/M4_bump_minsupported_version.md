# M.4: Bump MinSupported Version

This section provides step-by-step instructions for the M.4 task: "Bump MinSupported version" on the master branch. This task removes support for the oldest version in the rolling upgrade window.

**📋 Quick Reference:** For a streamlined checklist-style guide, see [`M4_bump_minsupported_version_QUICK.md`](M4_bump_minsupported_version_QUICK.md). This document provides detailed explanations and troubleshooting.

---

### Overview

**When to perform:** After the final release is published (e.g., after v25.2.0 is released, bump MinSupported from v25.2 to v25.4).

**What it does:**
- Updates the MinSupported constant to the next version(s), narrowing the upgrade window
- Removes obsolete version gates and compatibility code for the old MinSupported version
- Deletes bootstrap data, schema changer rules, and test configurations for the old version
- Cleans up test files that reference the old version

**Why:** CockroachDB maintains a rolling upgrade window of N-2 versions. When a new version is released (e.g., 25.4), the oldest version in the window (e.g., 25.2) is no longer supported for direct upgrades. This allows removal of compatibility code and reduces maintenance burden.

**Dependencies:**
- The final release must be published (e.g., v25.2.0)
- Previous master branch tasks (M.1, M.2, M.3) should be complete

**This should be done in 3 PRs:**
- **PR1**: Bump MinSupported from ORIG_VERSION to ORIG_VERSION+1 (e.g., v25.2 to v25.3)
- **PR2**: Bump MinSupported from ORIG_VERSION+1 to ORIG_VERSION+2 (e.g., v25.3 to v25.4)
- **PR3**: Prefix version keys below MinSupported with TODO_Delete_

**Note:** We often advance MinSupported by two versions (e.g., from v25.2 to v25.4). This is done in two separate PRs to make review easier and reduce the risk of test failures.

---

# First PRs: Bump MinSupported from VERSION to VERSION+1

Note: We often advance MinSupported up two versions. So this should be done in two PRs:
- **PR1**: Bump MinSupported from ORIG_VERSION to ORIG_VERSION+1 (e.g., v25.2 to v25.3)
- **PR2**: Bump MinSupported from ORIG_VERSION+1 to ORIG_VERSION+2 (e.g., v25.3 to v25.4)

Each PR follows the same pattern described below.

### Prerequisites

Before starting, ensure:
1. The final release is published and documented (e.g., v25.2.0)
2. You know which version to bump from and to (e.g., from v25.2 to v25.3)
3. You have the reference PR for the pattern (e.g., #147634 for v25.1 → v25.2, #157767 for v25.2 → v25.3)
4. You're working on the `master` branch

### Commit Organization Strategy

This task should be organized into **5 logical commits** based on the reference PR #157767:

1. **Commit 1**: Bump MinSupported constant
2. **Commit 2**: Remove schema changer rules
3. **Commit 3**: Remove bootstrap data
4. **Commit 4**: Update mixed_version tests
5. **Commit 5**: Remove local-mixed test configuration

**Note:** Some commits from older reference PRs may not apply to your codebase and should be skipped.

### Step-by-Step Checklist

#### Commit 1: Bump MinSupported Constant

Update the MinSupported constant and all code that references it.

**File:** `pkg/clusterversion/cockroach_versions.go` (line ~362)

```go
// Before:
const MinSupported Key = V25_2

// After:
const MinSupported Key = V25_3
```

**Find all files that reference MinSupported:**
```bash
# Search for files that need updates
git grep -l "MinSupported" pkg/ | grep -v "_test.go" | grep -v "CLAUDE.md"
```

**Common files to update** (grep for old version constant like V25_2):
- `pkg/crosscluster/logical/logical_replication_writer_processor.go`
- `pkg/crosscluster/physical/alter_replication_job.go`
- `pkg/kv/kvserver/closedts/policyrefresher/policy_refresher.go`
- `pkg/kv/kvserver/closedts/sidetransport/sender.go`
- `pkg/kv/kvserver/obsolete_code_test.go`
- `pkg/sql/backfill/mvcc_index_merger.go`
- `pkg/sql/catalog/funcdesc/helpers.go`
- `pkg/sql/conn_executor.go`
- `pkg/sql/create_index.go`
- `pkg/sql/create_table.go`
- `pkg/sql/distsql_running.go`
- **`pkg/storage/pebble.go`** - Update `MinimumSupportedFormatVersion` constant

**⚠️ IMPORTANT: DO NOT modify `pkg/sql/execversion/version.go`**

This file uses a separate versioning mechanism and is owned by the **SQL Queries team**. It should NOT be modified as part of the M.4 MinSupported bump task. If you see version references in this file during your search, leave them unchanged.

**Update each file** by changing references from the old MinSupported version to the new one.

**CRITICAL: Update pkg/storage/pebble.go (around line 2459):**

When bumping MinSupported, you must also update the `MinimumSupportedFormatVersion` constant to match the Pebble format version for the new MinSupported version.

**How to determine the correct value:**
1. Look at `pebbleFormatVersionMap` in the same file (around line 2450)
2. Find the entry for the new MinSupported version (e.g., `clusterversion.V25_3`)
3. Use that Pebble format version for `MinimumSupportedFormatVersion`

**Example for bumping MinSupported from V25_2 to V25_3:**
```go
// pebbleFormatVersionMap shows:
// V25_3: pebble.FormatValueSeparation
// V25_2: pebble.FormatTableFormatV6  (being removed)

// Therefore, update MinimumSupportedFormatVersion:
-const MinimumSupportedFormatVersion = pebble.FormatTableFormatV6
+const MinimumSupportedFormatVersion = pebble.FormatValueSeparation
```

**Why this is important:**
- The test `TestMinimumSupportedFormatVersion` validates: `MinimumSupportedFormatVersion == pebbleFormatVersionMap[MinSupported]`
- If you forget this change, the test will fail showing: `expected: 0xNN, actual: 0xMM`
- This ensures new stores are created with the correct minimum Pebble format version

**Example pattern to search for:**
```bash
# Find files with V25_2 that aren't in testdata
git grep "V25_2" pkg/ | grep -v testdata | grep -v CLAUDE.md | grep -v "_test.go"
```

**Create commit:**
```bash
git add pkg/clusterversion/cockroach_versions.go pkg/storage/pebble.go <other-modified-files>
git commit -m "clusterversion, storage: bump MinSupported from v25.2 to v25.3

Part of the quarterly M.4 \"Bump MinSupported\" task as outlined in
\`pkg/clusterversion/README.md\`.

This commit updates the MinSupported constant from V25_2 to V25_3,
along with all code that references the MinSupported version.

After this change, clusters running v25.2 can no longer connect to
clusters running master, and direct upgrades from v25.2 to master are
no longer supported.

Changes include updates to:
- Cross-cluster logical and physical replication
- Closed timestamp policy handling
- KV server obsolete code tracking
- SQL backfill and index merging
- Catalog function descriptors
- Connection executor and DDL operations
- Distributed SQL execution versioning
- Storage layer: MinimumSupportedFormatVersion updated to match new MinSupported

Part of #147634 (reference PR for this quarterly task).

Release note: None"
```

#### Commit 2: Remove Schema Changer Rules

Remove the frozen schema changer rules for the old MinSupported version.

**Directory to delete:** `pkg/sql/schemachanger/scplan/internal/rules/release_25_2/`

```bash
# Delete the entire directory
rm -rf pkg/sql/schemachanger/scplan/internal/rules/release_25_2/
```

**File:** `pkg/sql/schemachanger/scplan/plan.go`

Remove the import:
```go
// Remove this line:
"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules/release_25_2"
```

Remove from rulesForReleases array (around line 158):
```go
// Remove this line:
{activeVersion: clusterversion.TODO_Delete_V25_2, rulesRegistry: release_25_2.GetRegistry()},
```

**File:** `pkg/sql/schemachanger/scplan/BUILD.bazel`

Remove the dependency:
```bazel
# Remove from deps list:
"//pkg/sql/schemachanger/scplan/internal/rules/release_25_2",
```

**Create commit:**
```bash
git add -A  # Captures deletions
git commit -m "schemachanger: remove release_25_2 schema changer rules

Part of the quarterly M.4 \"Bump MinSupported\" task as outlined in
\`pkg/clusterversion/README.md\`.

After bumping MinSupported from v25.2 to v25.3, the frozen schema
changer rules for release 25.2 are no longer needed. These rules were
used to ensure schema changes work correctly in mixed-version clusters
with v25.2 nodes, which are no longer supported.

This commit removes:
- The entire release_25_2 rules directory
- Import and registry entry from plan.go
- Bazel dependency

Part of #147634 (reference PR for this quarterly task).

Release note: None"
```

#### Commit 3: Remove Bootstrap Data

Delete the bootstrap data files for the old MinSupported version.

**Files to delete:**
```bash
rm pkg/sql/catalog/bootstrap/data/25_2_system.keys
rm pkg/sql/catalog/bootstrap/data/25_2_system.sha256
rm pkg/sql/catalog/bootstrap/data/25_2_tenant.keys
rm pkg/sql/catalog/bootstrap/data/25_2_tenant.sha256
```

**File:** `pkg/sql/catalog/bootstrap/initial_values.go`

Remove the V25_2 entry from initialValuesFactoryByKey map (around line 66):
```go
// REMOVE this entire block:
clusterversion.TODO_Delete_V25_2: hardCodedInitialValues{
    system:        v25_2_system_keys,
    systemHash:    v25_2_system_sha256,
    nonSystem:     v25_2_tenant_keys,
    nonSystemHash: v25_2_tenant_sha256,
}.build,
```

Remove the go:embed variables (around lines 147-157):
```go
// REMOVE these lines:
//go:embed data/25_2_system.keys
var v25_2_system_keys string

//go:embed data/25_2_system.sha256
var v25_2_system_sha256 string

//go:embed data/25_2_tenant.keys
var v25_2_tenant_keys string

//go:embed data/25_2_tenant.sha256
var v25_2_tenant_sha256 string
```

**File:** `pkg/sql/catalog/bootstrap/BUILD.bazel`

Remove embedsrcs entries (around line 10):
```bazel
# REMOVE these 4 lines:
"data/25_2_system.keys",
"data/25_2_system.sha256",
"data/25_2_tenant.keys",
"data/25_2_tenant.sha256",
```

**Create commit:**
```bash
git add -A
git commit -m "bootstrap: remove 25.2 bootstrap data

Part of the quarterly M.4 \"Bump MinSupported\" task as outlined in
\`pkg/clusterversion/README.md\`.

This commit removes the bootstrap data for v25.2, which is now below the
minimum supported version after bumping MinSupported from v25.2 to v25.3.

The bootstrap data files are used to initialize clusters at specific
versions. Since clusters can no longer start at v25.2 (it's below
MinSupported), these files are no longer needed.

Changes:
- Removed 25_2_system.keys and 25_2_system.sha256
- Removed 25_2_tenant.keys and 25_2_tenant.sha256
- Removed V25_2 entry from initialValuesFactoryByKey map in initial_values.go
- Removed go:embed variables for v25.2 bootstrap data
- Updated BUILD.bazel to remove embedsrcs for deleted files
- Updated first_upgrade_test.go error message expectations (descriptor
  validation errors now use simpler format after bootstrap data removal)

Part of #147634 (reference PR for this quarterly task).

Release note: None"
```

#### Commit 4: Update Mixed Version Tests

Update tests that use the cockroach-go-testserver configuration to reference the new MinSupported version.

**Files to update:**
```bash
# Find mixed_version test files
ls pkg/sql/logictest/testdata/logic_test/mixed_version_*
```

Common files:
- `pkg/sql/logictest/testdata/logic_test/mixed_version_char`
- `pkg/sql/logictest/testdata/logic_test/mixed_version_citext`
- `pkg/sql/logictest/testdata/logic_test/mixed_version_ltree`
- `pkg/sql/logictest/testdata/logic_test/mixed_version_partial_stats`

**Update LogicTest headers:**

For files with single config (char, citext):
```diff
-# LogicTest: cockroach-go-testserver-25.2
+# LogicTest: cockroach-go-testserver-25.3
```

For files with multiple configs (ltree, partial_stats):
```diff
-# LogicTest: cockroach-go-testserver-25.2 cockroach-go-testserver-25.3
+# LogicTest: cockroach-go-testserver-25.3
```

**Create commit:**
```bash
git add pkg/sql/logictest/testdata/logic_test/mixed_version_*
git commit -m "logictest: update mixed_version tests to use 25.3 testserver

Part of the quarterly M.4 \"Bump MinSupported\" task as outlined in
\`pkg/clusterversion/README.md\`.

After bumping MinSupported from v25.2 to v25.3, tests that use the
cockroach-go-testserver predecessor binary configuration need to be
updated to test against v25.3 instead of v25.2.

This commit updates the LogicTest headers for mixed-version tests that
validate feature compatibility across version boundaries:
- mixed_version_char: Tests CHAR type upgrades
- mixed_version_citext: Tests case-insensitive text type upgrades
- mixed_version_ltree: Tests ltree type availability after upgrade
- mixed_version_partial_stats: Tests partial statistics with WHERE clause

For ltree and partial_stats, which previously tested with both 25.2 and
25.3 testservers, the headers now only reference 25.3 since 25.2 is
below MinSupported.

Part of #147634 (reference PR for this quarterly task).

Release note: None"
```

#### Commit 5: Remove Local-Mixed Test Configuration

Remove the local-mixed-X.Y test configuration for the old MinSupported version and all references to it.

**File:** `pkg/sql/logictest/logictestbase/logictestbase.go`

Remove the config definition (around lines 501-515):
```go
// REMOVE this entire block:
{
    Name:                        "local-mixed-25.2",
    NumNodes:                    1,
    OverrideDistSQLMode:         "off",
    BootstrapVersion:            clusterversion.TODO_Delete_V25_2,
    DisableUpgrade:              true,
    DeclarativeCorpusCollection: true,
    DisableSchemaLockedByDefault: true,
},
```

Remove from default-configs set (around line 660):
```go
// REMOVE "local-mixed-25.2" from this list:
"default-configs": makeConfigSet(
    "local",
    // ...
    "local-mixed-25.2",  // REMOVE THIS LINE
    "local-mixed-25.3",
    "local-mixed-25.4",
),
```

Remove from schema-locked-disabled set (around lines 684-686):
```go
// REMOVE "local-mixed-25.2" from this set:
"schema-locked-disabled": makeConfigSet(
    "local-legacy-schema-changer",
    "local-mixed-25.2",  // REMOVE THIS LINE
),
```

**Delete test directories:**
```bash
rm -rf pkg/ccl/logictestccl/tests/local-mixed-25.2/
rm -rf pkg/sql/logictest/tests/local-mixed-25.2/
rm -rf pkg/sql/sqlitelogictest/tests/local-mixed-25.2/
```

**Update nightly build script:**

**⚠️ CRITICAL:** This file is easy to miss but will break the nightly builds if not updated.

**File:** `build/teamcity/cockroach/nightlies/sqllogic_corpus_nightly_impl.sh`

This script runs nightly corpus generation for all mixed version configurations. Update the loop to remove the old version:

```bash
# Find the line (around line 84):
for config in local-mixed-25.2 local-mixed-25.3; do

# Update to remove local-mixed-25.2:
for config in local-mixed-25.3; do
```

**Example for bumping from 25.2 to 25.3:**
```bash
# Before:
for config in local-mixed-25.2 local-mixed-25.3; do

# After:
for config in local-mixed-25.3; do
```

**Example for bumping from 25.3 to 25.4:**
```bash
# Before:
for config in local-mixed-25.3 local-mixed-25.4; do

# After:
for config in local-mixed-25.4; do
```

**Why this matters:**
- If not updated, the nightly build will fail trying to reference a deleted `local-mixed-X.Y` directory
- This broke the nightly in PR #158225, requiring follow-up fix in commit 4bc710b4667
- The error will be: "references nonexistent local-mixed-X.Y logictest directory"

**Remove references from test files:**

**⚠️ CRITICAL: Understanding `onlyif` and `skipif` Directives**

The `onlyif config` and `skipif config` directives in logic tests **guard the next statement(s)**:
- `onlyif config local-mixed-25.2` → Run the next statement ONLY on local-mixed-25.2
- `skipif config local-mixed-25.2` → Skip the next statement on local-mixed-25.2

**IMPORTANT: These two directives require DIFFERENT handling when removing a config:**

**Rule 1: For `onlyif config local-mixed-25.2` directives:**
- Remove BOTH the directive AND the guarded statement(s)
- The guarded statement is version-specific and won't be needed after the config is removed

**Rule 2: For `skipif config local-mixed-25.2` directives:**
- Remove ONLY the directive itself
- KEEP the guarded statement(s)
- The guarded statement should run in all configs now that local-mixed-25.2 is removed

**Examples:**

```bash
# Example 1: onlyif directive (remove both directive AND statement)

# BEFORE:
onlyif config local-mixed-25.2
statement error pgcode 0A000 pq: unimplemented: usage of user-defined function
CREATE TABLE t1(a INT PRIMARY KEY, b INT DEFAULT f1());

statement ok
CREATE TABLE t1(a INT PRIMARY KEY, b INT DEFAULT f1());

# AFTER (CORRECT - removed both directive and guarded statement):
statement ok
CREATE TABLE t1(a INT PRIMARY KEY, b INT DEFAULT f1());

# Example 2: skipif directive (remove ONLY directive, keep statement)

# BEFORE:
skipif config local-mixed-25.2
statement error pgcode 0A000 unimplemented: cannot evaluate function in this context
ALTER TABLE test_tbl_t ADD COLUMN c int AS (test_tbl_f()) stored;

# AFTER (CORRECT - removed only directive, kept statement):
statement error pgcode 0A000 unimplemented: cannot evaluate function in this context
ALTER TABLE test_tbl_t ADD COLUMN c int AS (test_tbl_f()) stored;
```

**Why the difference?**
- `onlyif`: The guarded statement was ONLY for that specific config. Removing the config means we don't need that statement anymore (there's usually an unguarded version later)
- `skipif`: The guarded statement was for ALL configs EXCEPT the one being removed. Now that we're removing that config, the statement should run everywhere

**Pattern to follow for `onlyif`:**
1. Find `onlyif config local-mixed-25.2` line
2. Identify what statement it guards (usually the next 3-10 lines until blank line or next directive)
3. Remove BOTH the directive AND the guarded statement
4. If this leaves an empty subtest or empty user block, remove those too

**Pattern to follow for `skipif`:**
1. Find `skipif config local-mixed-25.2` line
2. Remove ONLY the directive line
3. Keep all following statements unchanged

Find all files with local-mixed-25.2 references:
```bash
grep -r "local-mixed-25\.2" pkg/sql/logictest/testdata/logic_test/ \
     pkg/ccl/logictestccl/testdata/logic_test/ | cut -d: -f1 | sort -u
```

For each file, remove local-mixed-25.2 using targeted sed commands:
```bash
# For files with LogicTest headers, skipif, or onlyif on dedicated lines:
sed -i '' \
  -e '/^# LogicTest:/s/ !*local-mixed-25\.2//g' \
  -e '/^skipif config local-mixed-25\.2$/d' \
  -e '/^onlyif config local-mixed-25\.2$/d' \
  "$file"

# For files with multi-config onlyif/skipif lines:
sed -i '' \
  -e '/^onlyif config/s/ local-mixed-25\.2//g' \
  -e '/^skipif config/s/ local-mixed-25\.2//g' \
  "$file"
```

**⚠️ IMPORTANT: Manual review required after sed**

The sed commands above only remove the *directives*, not the statements they guard. You MUST:
1. Manually review each file that had `onlyif config local-mixed-25.2`
2. Identify and remove guarded statements that would cause duplicates
3. For `skipif config local-mixed-25.2`, the sed commands are sufficient - no manual work needed
4. Run tests to catch "relation already exists" or similar errors

**How to find files needing manual review (onlyif only):**
```bash
# Find files that had onlyif with local-mixed-25.2 (these need manual review)
git diff pkg/sql/logictest/testdata/logic_test/ pkg/ccl/logictestccl/testdata/logic_test/ | \
  grep -B 2 "^-onlyif config.*local-mixed-25\.2" | \
  grep "^diff --git" | sed 's/.*b\///' | sort -u

# Files with skipif don't need manual review - the directive removal is sufficient
```

**Remove empty LogicTest directive lines:**

If any files have headers like `# LogicTest: !local-mixed-25.2` that become empty `# LogicTest:`, remove them:
```bash
# Find files with empty LogicTest directives
grep -l "^# LogicTest:$" pkg/sql/logictest/testdata/logic_test/* \
     pkg/ccl/logictestccl/testdata/logic_test/*

# Remove the empty lines
sed -i '' '/^# LogicTest:$/d' <file>
```

**Regenerate Bazel files:**
```bash
./dev gen bazel
```

**Verify the changes:**
```bash
# Should succeed
./dev build short
```

**Create commit:**
```bash
git add -A
git commit -m "logictest: remove local-mixed-25.2 test configuration

Part of the quarterly M.4 \"Bump MinSupported\" task as outlined in
\`pkg/clusterversion/README.md\`.

After bumping MinSupported from v25.2 to v25.3, the local-mixed-25.2
test configuration is no longer needed since it simulates a mixed-version
cluster with v25.2 nodes, which can no longer connect to the cluster.

This commit:
- Removes the local-mixed-25.2 config from logictestbase.go
- Removes it from the default-configs and schema-locked-disabled sets
- Deletes the generated test directories for local-mixed-25.2
- Removes all references from logic test files (skipif/onlyif directives)
- Removes empty LogicTest directive lines that resulted from deletions
- Regenerates Bazel BUILD files via \`./dev gen bazel\`

Changes affect 34 test files that had skipif or onlyif directives
referencing local-mixed-25.2, plus the generated test files and BUILD
files that were auto-generated based on the removed configuration.

Part of #147634 (reference PR for this quarterly task).

Release note: None"
```

### Expected Files Modified

A typical M.4 bump should modify approximately 70-80 files across the 6 commits:

**Commit 1 (1 file):**
1. `pkg/clusterversion/cockroach_versions.go` - Prefix version keys

**Commit 2 (~13 files):**
1. `pkg/clusterversion/cockroach_versions.go` - MinSupported constant
2. `pkg/crosscluster/logical/logical_replication_writer_processor.go`
3. `pkg/crosscluster/physical/alter_replication_job.go`
4. `pkg/kv/kvserver/closedts/policyrefresher/policy_refresher.go`
5. `pkg/kv/kvserver/closedts/sidetransport/sender.go`
6. `pkg/kv/kvserver/obsolete_code_test.go`
7. `pkg/sql/backfill/mvcc_index_merger.go`
8. `pkg/sql/catalog/funcdesc/helpers.go`
9. `pkg/sql/conn_executor.go`
10. `pkg/sql/create_index.go`
11. `pkg/sql/create_table.go`
12. `pkg/sql/distsql_running.go`
13. `pkg/sql/execversion/version.go`
14. **`pkg/storage/pebble.go`** - MinimumSupportedFormatVersion constant

**Commit 4 (~30 files):**
1. `pkg/sql/schemachanger/scplan/internal/rules/release_25_2/` - Entire directory deleted (~25 files)
2. `pkg/sql/schemachanger/scplan/plan.go`
3. `pkg/sql/schemachanger/scplan/BUILD.bazel`
4. Various testdata files in the deleted directory

**Commit 5 (6 files):**
1. `pkg/sql/catalog/bootstrap/data/25_2_system.keys` - Deleted
2. `pkg/sql/catalog/bootstrap/data/25_2_system.sha256` - Deleted
3. `pkg/sql/catalog/bootstrap/data/25_2_tenant.keys` - Deleted
4. `pkg/sql/catalog/bootstrap/data/25_2_tenant.sha256` - Deleted
5. `pkg/sql/catalog/bootstrap/initial_values.go`
6. `pkg/sql/catalog/bootstrap/BUILD.bazel`

**Commit 6 (4 files):**
1. `pkg/sql/logictest/testdata/logic_test/mixed_version_char`
2. `pkg/sql/logictest/testdata/logic_test/mixed_version_citext`
3. `pkg/sql/logictest/testdata/logic_test/mixed_version_ltree`
4. `pkg/sql/logictest/testdata/logic_test/mixed_version_partial_stats`

**Commit 7 (~65 files):**
1. `pkg/sql/logictest/logictestbase/logictestbase.go` - Config removal
2. `build/teamcity/cockroach/nightlies/sqllogic_corpus_nightly_impl.sh` - **CRITICAL: Update nightly build loop**
3. `pkg/ccl/logictestccl/tests/local-mixed-25.2/BUILD.bazel` - Deleted
4. `pkg/ccl/logictestccl/tests/local-mixed-25.2/generated_test.go` - Deleted
5. `pkg/sql/logictest/tests/local-mixed-25.2/BUILD.bazel` - Deleted
6. `pkg/sql/logictest/tests/local-mixed-25.2/generated_test.go` - Deleted
7. `pkg/sql/sqlitelogictest/tests/local-mixed-25.2/BUILD.bazel` - Deleted
8. `pkg/sql/sqlitelogictest/tests/local-mixed-25.2/generated_test.go` - Deleted
9. ~34 logic test files with skipif/onlyif updates
10. ~20 generated test files updated by `./dev gen bazel`
11. `pkg/BUILD.bazel` - Binary file updated
12. Various other files with version-gated logic

### Validation and Verification

#### Before Creating PR

**1. Run core tests:**
```bash
./dev test pkg/clusterversion pkg/storage
```

Expected: All tests should pass.

**2. Verify build:**
```bash
./dev build short
```

Expected: Build completes successfully.

**3. Check commit structure:**
```bash
git log --oneline HEAD~6..HEAD
```

Expected: Should show 6 commits in the correct order.

**4. Verify file counts:**
```bash
git diff --stat <base-branch>
```

Expected: Approximately 70-80 files changed, with significant deletions (~5,000-6,000 lines).

#### CRITICAL: Validate Against Previous PR

**This task is performed every quarter.** Before creating the PR, validate that changes follow the same pattern as the reference PR.

**Step 1: Compare file lists**
```bash
# Get files from reference PR #147634
gh pr view 147634 --json files --jq '.files[].path' | sort > /tmp/ref_m4_files.txt

# Get your current files
git diff --name-only <base-branch> | sort > /tmp/current_m4_files.txt

# Compare
echo "=== Files ONLY in current PR (investigate!) ==="
comm -13 /tmp/ref_m4_files.txt /tmp/current_m4_files.txt

echo "=== Files ONLY in reference PR (might be missing!) ==="
comm -23 /tmp/ref_m4_files.txt /tmp/current_m4_files.txt
```

**Step 2: Justify differences**

For each file that appears in your PR but NOT in the reference PR:
1. Understand why it changed
2. Verify it matches a pattern from the runbook
3. Document or revert if unexpected

**Step 3: Verify commit messages match pattern**
```bash
# Check that commit messages follow the established pattern
git log --format="%s" HEAD~6..HEAD
```

Expected format:
- `clusterversion: prefix version keys below 25.3 with TODO_Delete_`
- `clusterversion: bump MinSupported from v25.2 to v25.3`
- `schemachanger: remove release_25_2 schema changer rules`
- `bootstrap: remove 25.2 bootstrap data`
- `logictest: update mixed_version tests to use 25.3 testserver`
- `logictest: remove local-mixed-25.2 test configuration`

### Common Errors and Solutions

#### Error 1: Bazel generation failure after removing config

**Error:** `panic: unknown config name local-mixed-25.2`

**Cause:** Test files still reference local-mixed-25.2 after the config was removed from logictestbase.go.

**Fix:**
```bash
# Find all references
grep -r "local-mixed-25\.2" pkg/sql/logictest/testdata/logic_test/ \
     pkg/ccl/logictestccl/testdata/logic_test/

# Remove them with sed (see Commit 7 instructions above)
```

#### Error 2: Accidentally removed newlines with aggressive regex

**Error:** File corruption where lines are concatenated (e.g., `# LogicTest: !local-legacy-schema-changer# A basic sanity check...`)

**Cause:** Using overly aggressive perl or sed commands like `perl -pi -e 's/\s*local-mixed-25\.2\s*/ /g; s/\s+$//; s/\s+/ /g'`

**Fix:** Don't use global whitespace replacement. Use targeted sed:
```bash
# WRONG (removes newlines):
perl -pi -e 's/\s*local-mixed-25\.2\s*/ /g; s/\s+$//; s/\s+/ /g'

# RIGHT (preserves structure):
sed -i '' \
  -e '/^# LogicTest:/s/ !*local-mixed-25\.2//g' \
  -e '/^skipif config local-mixed-25\.2$/d' \
  -e '/^onlyif config local-mixed-25\.2$/d'
```

#### Error 3: Empty LogicTest directive

**Error:** `empty LogicTest directive` during bazel generation

**Cause:** Files had headers like `# LogicTest: !local-mixed-25.2` which became `# LogicTest:` after removal.

**Fix:**
```bash
# Find files with empty directives
grep -l "^# LogicTest:$" pkg/sql/logictest/testdata/logic_test/* \
     pkg/ccl/logictestccl/testdata/logic_test/*

# Remove the empty lines
sed -i '' '/^# LogicTest:$/d' <files>
```

#### Error 4: Nightly build failure - nonexistent local-mixed directory

**Error:** Nightly build fails with: `references nonexistent local-mixed-X.Y logictest directory`

**Cause:** Forgot to update `build/teamcity/cockroach/nightlies/sqllogic_corpus_nightly_impl.sh` to remove the old config from the loop.

**Real-world occurrence:** This broke the nightly in PR #158225, requiring follow-up fix in commit 4bc710b4667 and issue #158741.

**Fix:**
```bash
# Edit build/teamcity/cockroach/nightlies/sqllogic_corpus_nightly_impl.sh
# Find the line (around line 84):
for config in local-mixed-25.2 local-mixed-25.3; do

# Update to:
for config in local-mixed-25.3; do
```

**Prevention:**
- Always check this file when removing a local-mixed config
- Add it to your commit checklist
- This file is in the runbook's "Expected Files Modified" section

#### Error 5: Committed local files accidentally

**Error:** Local files (`.claude/`, `PLAN_*.md`, etc.) appear in commit.

**Cause:** Used `git add -A` without excluding local files.

**Fix:**
```bash
# Reset the commit
git reset HEAD~ --soft

# Unstage local files
git reset HEAD .claude/settings.local.json PLAN_*.md *.local.md

# Re-commit without local files
git commit -m "..."
```

#### Error 6: Missing file deletions in commit

**Error:** Deleted directories don't appear in commit.

**Cause:** Forgot to use `git add -A` or `git add -u` which capture deletions.

**Fix:**
```bash
# Always use git add -A for commits with deletions
git add -A  # Adds modifications AND deletions
```

#### Error 7: "relation already exists" or duplicate statement errors in logic tests

**Error:** Test failures like:
```
expected success, but found
(42P07) relation "test.public.t1" already exists
```

**Cause:** Removed `onlyif config local-mixed-25.2` directives but forgot to remove the statements they guarded, resulting in duplicate statements that both execute.

**Example from udf_in_table:**
```bash
# File had two CREATE TABLE statements:
# 1. Guarded by "onlyif config local-mixed-25.2" (lines 88-98)
# 2. Unguarded default version (lines 533-547)

# After removing ONLY the directive (WRONG):
statement ok
CREATE TABLE t1(...)   # ← Was guarded, now runs on all configs!

# Later:
statement ok
CREATE TABLE t1(...)   # ← Unguarded, always runs → DUPLICATE!
```

**How to identify:**
```bash
# After removing directives with sed, search git diff for removed onlyif lines
git diff pkg/sql/logictest/testdata/logic_test/ | grep "^-onlyif config.*local-mixed-25\.2"

# For each file, manually check if guarded statements should also be removed
```

**Fix:**
1. Read the test file and identify what the removed `onlyif` directive was guarding
2. Determine if the guarded statement is a duplicate of an unguarded statement elsewhere
3. If duplicate: remove the guarded statement (usually 3-10 lines after the removed directive)
4. If the removal leaves an empty subtest or user block, remove those too

**Example fix for udf_in_table:**
```bash
# Remove the entire guarded statement block (lines 88-98)
# Keep only the unguarded version (lines 533-547)
```

**Prevention:**
- When running sed to remove directives, immediately grep for removed `onlyif` lines
- Manually review each file that had `onlyif` to check for guarded statements
- Run tests early to catch duplicate statement errors: `./dev testlogic`

#### Error 8: Incorrect pebble format version change during rebase

**Error:** After rebasing against master, `pkg/storage/pebble.go` has the wrong pebble format version for V25_3.

**Symptom:**
```go
// WRONG - V25_3 value changed when it shouldn't have:
var pebbleFormatVersionMap = map[clusterversion.Key]pebble.FormatMajorVersion{
	clusterversion.V25_4_PebbleFormatV2BlobFiles: pebble.FormatV2BlobFiles,
	clusterversion.V25_3:                         pebble.FormatTableFormatV6,  // ← WRONG!
}

// CORRECT - Only removed V25_2, kept V25_3 unchanged:
var pebbleFormatVersionMap = map[clusterversion.Key]pebble.FormatMajorVersion{
	clusterversion.V25_4_PebbleFormatV2BlobFiles: pebble.FormatV2BlobFiles,
	clusterversion.V25_3:                         pebble.FormatValueSeparation,  // ← CORRECT
}
```

**Cause:** During rebase, git may auto-merge changes to `pebbleFormatVersionMap` incorrectly. The commit that removes `local-mixed-25.2` should only remove the V25_2 entry from this map, not change the V25_3 value.

**How to identify:**
```bash
# Check the pebble format version in the logictest removal commit
git show <commit-sha>:pkg/storage/pebble.go | grep -A 4 "pebbleFormatVersionMap ="

# Compare with master to see what the correct V25_3 value should be
git show master:pkg/storage/pebble.go | grep -A 4 "pebbleFormatVersionMap ="
```

**Fix:**
```bash
# Start interactive rebase to edit the problematic commit
git rebase -i <base-commit>

# Mark the commit with pebble.go for 'edit'
# Then fix the file:
# Edit pkg/storage/pebble.go to restore correct V25_3 value
git add pkg/storage/pebble.go
git commit --amend --no-edit
git rebase --continue
```

**Prevention:**
- After rebase, always check `pkg/storage/pebble.go` changes carefully
- The change should only be a deletion of the old version's line (V25_2)
- Never change the values for existing newer versions (V25_3, V25_4, etc.)
- Reference PR #147634 to verify the expected pattern

### Quick Reference Commands

**Commit 1 - Bump MinSupported:**
```bash
# Find files to update
git grep -l "MinSupported" pkg/ | grep -v "_test.go"
git grep "V25_2" pkg/ | grep -v testdata

# Update files manually (including pkg/storage/pebble.go!)
# In pkg/storage/pebble.go, update MinimumSupportedFormatVersion to match
# the pebbleFormatVersionMap entry for the new MinSupported version

git add pkg/clusterversion/cockroach_versions.go pkg/storage/pebble.go <other-files>
git commit -m "clusterversion, storage: bump MinSupported from v25.2 to v25.3..."

# Verify with test
./dev test pkg/storage -f TestMinimumSupportedFormatVersion -v
```

**Commit 2 - Remove schema changer rules:**
```bash
rm -rf pkg/sql/schemachanger/scplan/internal/rules/release_25_2/
# Edit plan.go and BUILD.bazel
git add -A
git commit -m "schemachanger: remove release_25_2 schema changer rules..."
```

**Commit 3 - Remove bootstrap data:**
```bash
rm pkg/sql/catalog/bootstrap/data/25_2_*
# Edit initial_values.go and BUILD.bazel
git add -A
git commit -m "bootstrap: remove 25.2 bootstrap data..."
```

**Commit 4 - Update mixed_version tests:**
```bash
# Edit 4 mixed_version test files
git add pkg/sql/logictest/testdata/logic_test/mixed_version_*
git commit -m "logictest: update mixed_version tests to use 25.3 testserver..."
```

**Commit 5 - Remove local-mixed config:**
```bash
# Edit logictestbase.go to remove the config definition and references

# CRITICAL: Update nightly build script
# Edit build/teamcity/cockroach/nightlies/sqllogic_corpus_nightly_impl.sh
# Change: for config in local-mixed-25.2 local-mixed-25.3; do
# To:     for config in local-mixed-25.3; do

rm -rf pkg/ccl/logictestccl/tests/local-mixed-25.2/
rm -rf pkg/sql/logictest/tests/local-mixed-25.2/
rm -rf pkg/sql/sqlitelogictest/tests/local-mixed-25.2/

# Remove references from test files
find pkg/sql/logictest/testdata/logic_test/ \
     pkg/ccl/logictestccl/testdata/logic_test/ \
     -type f -exec grep -l "local-mixed-25\.2" {} \; | while read file; do
  sed -i '' \
    -e '/^# LogicTest:/s/ !*local-mixed-25\.2//g' \
    -e '/^skipif config local-mixed-25\.2$/d' \
    -e '/^onlyif config local-mixed-25\.2$/d' \
    "$file"
done

# Handle multi-config lines
find pkg/sql/logictest/testdata/logic_test/ \
     pkg/ccl/logictestccl/testdata/logic_test/ \
     -type f -exec grep -l "local-mixed-25\.2" {} \; | while read file; do
  sed -i '' \
    -e '/^onlyif config/s/ local-mixed-25\.2//g' \
    -e '/^skipif config/s/ local-mixed-25\.2//g' \
    "$file"
done

# Remove empty LogicTest directives
sed -i '' '/^# LogicTest:$/d' pkg/sql/logictest/testdata/logic_test/udf_in_index \
          pkg/ccl/logictestccl/testdata/logic_test/provisioning

./dev gen bazel
git add -A
git commit -m "logictest: remove local-mixed-25.2 test configuration..."
```

**Verification:**
```bash
# Test
./dev test pkg/clusterversion pkg/storage

# Build
./dev build short

# View commits
git log --oneline HEAD~5..HEAD
```

### Timeline Context

In the release cycle:
- **M.1 (complete)**: Master bumped to next major version
- **M.2 (complete)**: Mixed-cluster logic tests enabled
- **M.3 (complete)**: Upgrade tests enabled with RC binaries
- **Now (M.4)**: Bump MinSupported to narrow upgrade window
- **M.5 (later)**: Finalize version gates when final release is published

### Notes

- **Timing:** M.4 should be performed shortly after the final release is published (e.g., v25.2.0)
- **Upgrade window:** After M.4, the upgrade window narrows from N-2 to the new MinSupported
- **Testing:** Some test failures related to DNS lookup or network issues are unrelated and can be ignored
- **File count:** Total file changes should be around 70-80 files per PR with ~5,000-6,000 deletions
- **Commit organization:** Following the 5-commit structure makes review easier and matches the established pattern

### Example PRs

- 25.2 → 25.3 bump: [#157767](https://github.com/cockroachdb/cockroach/pull/157767)
- 25.1 → 25.2 bump: [#147634](https://github.com/cockroachdb/cockroach/pull/147634)

---

# Last PR for MinSupported Bump Task: Prefix Version Keys with TODO_Delete_

This is a follow-up task after the main MinSupported bump PRs (e.g., after bumping from v25.2 → v25.3 → v25.4). It marks obsolete version keys for future removal by prefixing them with `TODO_Delete_`.

### Overview

**When to perform:** After the MinSupported bump PRs are merged (e.g., after bumping from V25_2 to V25_4 via two PRs).

**What it does:**
- Prefixes internal version keys below the new MinSupported with `TODO_Delete_`
- Signals to developers that these version gates are obsolete and can be removed
- Prepares for future cleanup of compatibility code

**Why separate PR:** This change can be large (touching many files) and is purely cosmetic/organizational, so it's cleaner to separate it from the functional MinSupported bump.

### Prerequisites

Before starting, ensure:
1. The main MinSupported bump PRs have been merged
2. You know which versions are now below MinSupported (e.g., V25_2 and V25_3 after bumping to V25_4)
3. You're working on the `master` branch

### Step-by-Step Instructions

#### Step 1: Prefix Version Keys

**File:** `pkg/clusterversion/cockroach_versions.go`

**Find all INTERNAL version keys below the new MinSupported** (e.g., all V25_2_* and V25_3_* keys when MinSupported is now V25_4):

```bash
# Example for MinSupported bumped from V25_2 to V25_4 (via V25_3)
# Find all V25_2_* and V25_3_* version keys (NOT V25_2 or V25_3 themselves):
grep -n "^\s*V25_2_\|^\s*V25_3_" pkg/clusterversion/cockroach_versions.go
```

**Add TODO_Delete_ prefix:**
```go
// Before:
V25_2_Start
V25_2_AddSystemStatementHintsTable
V25_2  // ← DO NOT PREFIX THIS ONE!

V25_3_Start
V25_3_AddEventLogColumnAndIndex
V25_3_AddEstimatedLastLoginTime
V25_3_AddHotRangeLoggerJob
V25_3  // ← DO NOT PREFIX THIS ONE!

// After:
TODO_Delete_V25_2_Start
TODO_Delete_V25_2_AddSystemStatementHintsTable
V25_2  // ← Keep as-is, no TODO_Delete_ prefix

TODO_Delete_V25_3_Start
TODO_Delete_V25_3_AddEventLogColumnAndIndex
TODO_Delete_V25_3_AddEstimatedLastLoginTime
TODO_Delete_V25_3_AddHotRangeLoggerJob
V25_3  // ← Keep as-is, no TODO_Delete_ prefix
```

**⚠️ IMPORTANT:** Do NOT prefix the final release keys (e.g., V25_2, V25_3). Only prefix the internal version keys (e.g., V25_2_Start, V25_3_FeatureName).

**Why:** The final release keys are referenced in other parts of the codebase and tooling, and they serve as anchors for release tracking.

#### Step 2: Update All References

**Find all files that reference the old internal version keys:**

```bash
# Example for V25_2_* and V25_3_* keys
git grep "V25_2_Start\|V25_2_AddSystemStatementHintsTable" pkg/
git grep "V25_3_Start\|V25_3_AddEventLogColumnAndIndex\|V25_3_AddEstimatedLastLoginTime\|V25_3_AddHotRangeLoggerJob" pkg/
```

**For each file found, update the references:**
```go
// Before:
if version.IsActive(clusterversion.V25_3_Start) {

// After:
if version.IsActive(clusterversion.TODO_Delete_V25_3_Start) {
```

**Common files to check:**
- Version gates in package code
- Upgrade migration code in `pkg/upgrade/upgrades/`
- Test files that reference specific version gates
- Any conditional logic checking `IsActive()` for these versions

#### Step 3: Create Commit

```bash
git add -A
git commit -m "clusterversion: prefix version keys below 25.4 with TODO_Delete_

Part of the quarterly M.4 \"Bump MinSupported\" task (part 3) as outlined
in \`pkg/clusterversion/README.md\`.

This commit marks version keys below v25.4 with the TODO_Delete_ prefix
to indicate they will be removed in the next major release.

After bumping MinSupported from v25.2 to v25.4, all version gates for
v25.2 and v25.3 features are always active and can be simplified. This
prefix helps track which version checks are obsolete.

Part of the cleanup following the MinSupported bump.

Release note: None"
```

#### Step 4: Verify Build

```bash
# Ensure everything still compiles
./dev build short

# Run version tests
./dev test pkg/clusterversion -v
```

### Expected Files Modified

A typical TODO_Delete_ prefix PR should modify:
- `pkg/clusterversion/cockroach_versions.go` (version key definitions)
- Various files in `pkg/upgrade/upgrades/` (migration references)
- Other files with version gate checks (variable count based on features)

Typically 5-20 files total, depending on how many internal version keys existed for those releases.

### Validation Checklist

Before creating the PR:
- [ ] All internal version keys for old MinSupported versions are prefixed with TODO_Delete_
- [ ] Final release keys (e.g., V25_2, V25_3) are NOT prefixed
- [ ] All references to prefixed keys are updated throughout codebase
- [ ] Build succeeds: `./dev build short`
- [ ] Version tests pass: `./dev test pkg/clusterversion`
- [ ] Git grep shows no remaining unprefixed references to old internal keys

### Post-Commit: File Cleanup Issues

After this PR is merged, create GitHub issues for cleaning up TODO_Delete_ version gates:

**Tasks:**
1. Search for all uses of TODO_Delete_ version keys
2. Create issues assigned to relevant teams (based on package ownership)
3. Document that these version checks can be removed since the old versions are now always active

### Notes

- **This is optional:** Some teams prefer to skip this step and directly remove obsolete code
- **Timing:** Can be done immediately after MinSupported bump PRs or later
- **Follow-up:** Create issues for teams to remove the TODO_Delete_ code and simplify related logic

### Example PRs

- TBD (this is a new pattern introduced in the runbook)

---

