# M.4: Bump MinSupported - Quick Reference

**Full details:** See `M4_bump_minsupported_version.md`

## Overview

Two PRs to bump MinSupported (e.g., from V25_2 → V25_3 → V25_4), each following this 5-commit pattern.

**Reference PRs:**
- 25.2→25.3: #157767
- 25.3→25.4: #158225

---

## Prerequisites Checklist

- [ ] Final release published (e.g., v25.2.0)
- [ ] Working on `master` branch
- [ ] Know versions: FROM (e.g., 25.2) and TO (e.g., 25.3)

---

## Commit 1: Bump MinSupported Constant

### Critical Files (⚠️ BUILD BREAKS IF MISSED)

| File | Action | Line |
|------|--------|------|
| `pkg/clusterversion/cockroach_versions.go` | `const MinSupported Key = V25_3` | ~362 |
| `pkg/storage/pebble.go` | Update `MinimumSupportedFormatVersion` | ~2518 |

**For pebble.go:** Check `pebbleFormatVersionMap` at line ~2510, use the pebble format for new MinSupported version.

### Find Files to Update

```bash
git grep -l "MinSupported" pkg/ | grep -v "_test.go"
git grep "V25_2" pkg/ | grep -v testdata
```

### Common Files

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

**⚠️ DO NOT modify:** `pkg/sql/execversion/version.go` (owned by SQL Queries team)

### Commit

```bash
git add pkg/clusterversion/cockroach_versions.go pkg/storage/pebble.go <other-files>
git commit -m "clusterversion, storage: bump MinSupported from v25.2 to v25.3

Part of the quarterly M.4 \"Bump MinSupported\" task.

This commit updates the MinSupported constant from V25_2 to V25_3.
After this change, clusters running v25.2 can no longer connect to master.

Release note: None"
```

### Verify

```bash
./dev test pkg/storage -f TestMinimumSupportedFormatVersion -v
```

---

## Commit 2: Remove Schema Changer Rules

### Actions

```bash
rm -rf pkg/sql/schemachanger/scplan/internal/rules/release_25_2/
```

Edit `pkg/sql/schemachanger/scplan/plan.go`:
- Remove import for `release_25_2`
- Remove from `rulesForReleases` array (line ~158)

Edit `pkg/sql/schemachanger/scplan/BUILD.bazel`:
- Remove dependency

### Commit

```bash
git add -A
git commit -m "schemachanger: remove release_25_2 schema changer rules

Part of the quarterly M.4 \"Bump MinSupported\" task.

After bumping MinSupported from v25.2 to v25.3, the frozen schema
changer rules for release 25.2 are no longer needed.

Release note: None"
```

---

## Commit 3: Remove Bootstrap Data

### Actions

```bash
rm pkg/sql/catalog/bootstrap/data/25_2_system.keys
rm pkg/sql/catalog/bootstrap/data/25_2_system.sha256
rm pkg/sql/catalog/bootstrap/data/25_2_tenant.keys
rm pkg/sql/catalog/bootstrap/data/25_2_tenant.sha256
```

Edit `pkg/sql/catalog/bootstrap/initial_values.go`:
- Remove V25_2 entry from `initialValuesFactoryByKey` map (line ~66)
- Remove `go:embed` variables (lines ~147-157)

Edit `pkg/sql/catalog/bootstrap/BUILD.bazel`:
- Remove embedsrcs entries

### Commit

```bash
git add -A
git commit -m "bootstrap: remove 25.2 bootstrap data

Part of the quarterly M.4 \"Bump MinSupported\" task.

This commit removes the bootstrap data for v25.2, which is now below
the minimum supported version.

Release note: None"
```

---

## Commit 4: Update Mixed Version Tests

### Files to Update

```bash
ls pkg/sql/logictest/testdata/logic_test/mixed_version_*
```

Typical files:
- `mixed_version_char`
- `mixed_version_citext`
- `mixed_version_ltree`
- `mixed_version_partial_stats`

**Note:** Some of these files may not exist in newer versions. Only update files that exist.

### Action

Update LogicTest headers:

```diff
-# LogicTest: cockroach-go-testserver-25.2
+# LogicTest: cockroach-go-testserver-25.3
```

Or for multi-config files:

```diff
-# LogicTest: cockroach-go-testserver-25.2 cockroach-go-testserver-25.3
+# LogicTest: cockroach-go-testserver-25.3
```

### Commit

```bash
git add pkg/sql/logictest/testdata/logic_test/mixed_version_*
git commit -m "logictest: update mixed_version tests to use 25.3 testserver

Part of the quarterly M.4 \"Bump MinSupported\" task.

Updates LogicTest headers for mixed-version tests to test against v25.3
instead of v25.2.

Release note: None"
```

---

## Commit 5: Remove Local-Mixed Test Configuration

### Critical Files (⚠️ BREAKS NIGHTLY IF MISSED)

| File | Action |
|------|--------|
| `pkg/sql/logictest/logictestbase/logictestbase.go` | Remove config definition and set entries |
| `build/teamcity/cockroach/nightlies/sqllogic_corpus_nightly_impl.sh` | ⚠️ Update `for config in` loop |
| Test directories | Delete `local-mixed-25.2/` |
| Logic test files | Remove skipif/onlyif refs |

### Step 1: Update logictestbase.go

Remove config definition (lines ~501-515):
```go
// REMOVE:
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

Remove from sets (lines ~660, ~684):
```go
"default-configs": makeConfigSet(
    "local-mixed-25.2",  // REMOVE THIS LINE
)

"schema-locked-disabled": makeConfigSet(
    "local-mixed-25.2",  // REMOVE THIS LINE
)
```

### Step 2: Update Nightly Build Script (⚠️ CRITICAL)

Edit `build/teamcity/cockroach/nightlies/sqllogic_corpus_nightly_impl.sh` (line ~84):

```diff
-for config in local-mixed-25.2 local-mixed-25.3; do
+for config in local-mixed-25.3; do
```

**This broke the nightly in PR #158225!** Don't forget this file.

### Step 3: Delete Test Directories

```bash
rm -rf pkg/ccl/logictestccl/tests/local-mixed-25.2/
rm -rf pkg/sql/logictest/tests/local-mixed-25.2/
rm -rf pkg/sql/sqlitelogictest/tests/local-mixed-25.2/
```

### Step 4: Update Logic Test Files

```bash
# Remove from dedicated directive lines
find pkg/sql/logictest/testdata/logic_test/ \
     pkg/ccl/logictestccl/testdata/logic_test/ \
     -type f -exec grep -l "local-mixed-25\.2" {} \; | while read file; do
  sed -i '' \
    -e '/^# LogicTest:/s/ !*local-mixed-25\.2//g' \
    -e '/^skipif config local-mixed-25\.2$/d' \
    -e '/^onlyif config local-mixed-25\.2$/d' \
    "$file"
done

# Remove from multi-config lines
find pkg/sql/logictest/testdata/logic_test/ \
     pkg/ccl/logictestccl/testdata/logic_test/ \
     -type f -exec grep -l "local-mixed-25\.2" {} \; | while read file; do
  sed -i '' \
    -e '/^onlyif config/s/ local-mixed-25\.2//g' \
    -e '/^skipif config/s/ local-mixed-25\.2//g' \
    "$file"
done

# Remove empty LogicTest directives (if any)
sed -i '' '/^# LogicTest:$/d' pkg/sql/logictest/testdata/logic_test/* \
          pkg/ccl/logictestccl/testdata/logic_test/* 2>/dev/null || true
```

**⚠️ IMPORTANT:** After sed, manually check files with `onlyif` directives - you may need to remove guarded statements. See full runbook for details.

### Step 5: Regenerate Bazel

```bash
./dev gen bazel
```

### Commit

```bash
git add -A
git commit -m "logictest: remove local-mixed-25.2 test configuration

Part of the quarterly M.4 \"Bump MinSupported\" task.

After bumping MinSupported from v25.2 to v25.3, the local-mixed-25.2
test configuration is no longer needed.

Changes:
- Removed local-mixed-25.2 config from logictestbase.go
- Updated nightly build script (sqllogic_corpus_nightly_impl.sh)
- Deleted test directories
- Removed skipif/onlyif references from test files
- Regenerated Bazel BUILD files

Release note: None"
```

---

## Validation

### Run Tests

```bash
./dev test pkg/clusterversion pkg/storage
```

### Build

```bash
./dev build short
```

### Check Commits

```bash
git log --oneline HEAD~5..HEAD
```

Expected:
1. Bump MinSupported from v25.2 to v25.3
2. Remove release_25_2 schema changer rules
3. Remove 25.2 bootstrap data
4. Update mixed_version tests to use 25.3 testserver
5. Remove local-mixed-25.2 test configuration

### Compare with Reference PR

```bash
# Get files from reference PR
gh pr view 157767 --json files --jq '.files[].path' | sort > /tmp/ref_files.txt

# Get your files
git diff --name-only master | sort > /tmp/current_files.txt

# Compare
echo "=== Only in current ==="
comm -13 /tmp/ref_files.txt /tmp/current_files.txt

echo "=== Only in reference ==="
comm -23 /tmp/ref_files.txt /tmp/current_files.txt
```

Justify differences (version-specific files are expected to differ).

---

## Critical Files Checklist

Before creating PR, verify these files were updated:

- [ ] `pkg/storage/pebble.go` - MinimumSupportedFormatVersion
- [ ] `build/teamcity/cockroach/nightlies/sqllogic_corpus_nightly_impl.sh` - nightly loop
- [ ] All three test directory types deleted (ccl, logictest, sqlitelogictest)
- [ ] `./dev gen bazel` ran successfully

---

## Expected File Count

~70-80 files changed across 5 commits with ~5,000-6,000 deletions.

---

## Quick Troubleshooting

| Error | Quick Fix |
|-------|-----------|
| `TestMinimumSupportedFormatVersion` fails | Check `pkg/storage/pebble.go` MinimumSupportedFormatVersion matches pebbleFormatVersionMap |
| Nightly build fails "nonexistent local-mixed" | Update `sqllogic_corpus_nightly_impl.sh` |
| `panic: unknown config name` | Logic test files still reference old config - run sed commands again |
| `empty LogicTest directive` | Remove with `sed -i '' '/^# LogicTest:$/d' <file>` |
| Duplicate statement errors | Manually review files with `onlyif` - see full runbook |

**Full troubleshooting:** See main runbook section "Common Errors and Solutions"

---

## Third PR: Prefix Old Version Gates with TODO_Delete_

After both MinSupported bump PRs are merged, create a third PR to mark obsolete version gates.

### What to Do

Prefix all non-permanent version keys below MinSupported with `TODO_Delete_`:

**Example:**
```go
// Before:
V25_2_Start
V25_3_AddEventLogColumnAndIndex

// After:
TODO_Delete_V25_2_Start
TODO_Delete_V25_3_AddEventLogColumnAndIndex
```

**Rules:**
- Prefix if: `Internal != 0` AND version value `< MinSupported`
- Do NOT prefix: Final release keys (e.g., `V25_2`, `V25_3`)
- Do NOT prefix: Bootstrap keys (`Major: 0, Minor: 0`)

### Files to Update

1. `pkg/clusterversion/cockroach_versions.go` - Add prefix to key names and versionTable
2. Update all references: `git grep V25_2_Start` and replace with `TODO_Delete_V25_2_Start`
3. Update test files that use these keys

**Example PR:** #160852

### After Merging: Create Cleanup Issues

Create GitHub issues for teams to remove the TODO_Delete_ gates:

1. Find original PRs: `git log -S "V25_3_GateName"`
2. Determine team ownership from PR approvals (use GraphQL API)
3. Create one issue per team with their gates listed

**Example issues:** #160856, #160857, #160858, #160859, #160860, #160861

**Full details:** See main runbook "Post-Commit: Create Cleanup Issues for Teams"
