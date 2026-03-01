# M.1 CI Failure Reference

This file covers failures that appear after the M.1 (Bump Current Version) PR.
Run `validate-m1.sh` first — it catches most Type 2 failures before CI. Consult
this file when CI still fails after that.

---

## Quick Errors (Pre-PR Build Failures)

#### Error: "package release_25_4 not found"

**Cause:** The new release directory wasn't created or the package name wasn't updated correctly.

**Fix:**
1. Verify the directory exists: `ls pkg/sql/schemachanger/scplan/internal/rules/release_25_4`
2. Check package names: `grep "^package " pkg/sql/schemachanger/scplan/internal/rules/release_25_4/*.go`
3. Re-run the sed command from step 7b if needed

#### Error: "undefined: clusterversion.V26_1"

**Cause:** The placeholder constant wasn't added to `cockroach_versions.go`.

**Fix:** Add the `const V26_1 = Latest` line as shown in step 1 of the runbook.

#### Error: "test output didn't match expected" in scplan tests

**Cause:** Test outputs need to be regenerated after changing version constants.

**Fix:**
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

---

## CI Test Failures (Post-PR-Push)

After the M.1 PR is pushed, CI tests may reveal failures. Use this decision tree
before modifying any code.

### Decision Tree

```
Test fails after M.1 changes
    │
    ├─ FIRST: Did you modify production code beyond the runbook steps?
    │  └─ YES → STOP! Revert changes and consult previous M.1 PRs
    │  └─ NO → Continue below
    │
    ├─ Is it a logic test expecting old version number?
    │  └─ YES → Update test expectations in testdata file (Type 2A, 2B, 2C below)
    │
    ├─ Is it a bootstrap hash mismatch?
    │  └─ YES → Regenerate with -rewrite flag (See Type 2C below)
    │
    ├─ Is it a mixedversion test failure?
    │  └─ Did you modify mixedversion production code?
    │      ├─ YES → REVERT immediately! Check previous M.1 PRs first
    │      └─ NO → Check test history (Type 2D below)
    │
    └─ Is it failing in production code logic?
       └─ Check test history: When was this test added?
           ├─ BEFORE last M.1 → Likely test expectation issue
           └─ AFTER last M.1 → May be new regression test requiring code change (Type 1 below)
```

### ⚠️ When NOT to Modify Production Code

**RED FLAGS — stop and reconsider:**
- ❌ Adding imports to files that didn't have them (unless new regression test)
- ❌ Adding conditional logic based on version counts
- ❌ "Fixing" one test breaks multiple others (ALWAYS WRONG)
- ❌ Previous M.1 PRs didn't make similar changes
- ❌ Adding BUILD.bazel dependencies to mixedversion package

**GREEN FLAGS — production code change may be needed:**
- ✅ Failing test was added AFTER the previous M.1
- ✅ Test explicitly checks version-bump edge cases with clear documentation
- ✅ Test commit message references a specific issue number
- ✅ Multiple related tests fail expecting the same new behavior

**Before modifying production code, ALWAYS:**
```bash
# Check: Did previous M.1 PRs modify this file?
gh pr view 149494 --json files --jq '.files[] | select(.path == "path/to/file") | .path'

# Check: Was the failing test added AFTER the last M.1?
git log -S "TestName" --oneline --all
git show <commit-hash>  # Review the test's purpose
```

---

### Type 1: Version Gate Failures (Rare — Requires Code Changes)

These are logic errors in code that references version gates, OR new regression
tests added after the previous M.1.

**How to identify:**
- Test was added AFTER the previous M.1 PR
- Test explicitly validates version-bump edge cases
- Test documents a specific issue number it's fixing

**Action:**
```bash
# Find when the test was added
git log -S "TestSupportsSkipCurrentVersion" --oneline --all
```
Compare date with last M.1 (e.g., PR #149494 was July 2025). If test is newer,
read it carefully, make minimal production code changes, then run the full suite:
```bash
./dev test pkg/cmd/roachtest/roachtestutil/mixedversion  # If mixedversion code changed
./dev test pkg/sql/logictest -f='TestLogic'              # After logic test changes
```

---

### Type 2: Test Expectation Updates (Most Common — No Code Changes)

Straightforward test output updates because version numbers changed.
**Do NOT modify production code for these.**

#### A. Version Number Updates in Test Outputs

**Pattern:** Tests querying version info expect the new version number.

**Common files:**
- `pkg/sql/logictest/testdata/logic_test/upgrade`
- `pkg/sql/logictest/testdata/logic_test/crdb_internal`
- `pkg/ccl/logictestccl/testdata/logic_test/crdb_internal_tenant`

**Fix:**
```bash
sed -i '' 's/^25\.4$/26.1/' <file>
```

#### B. systemDatabaseSchemaVersion Update

**File:** `pkg/sql/logictest/testdata/logic_test/crdb_internal_catalog`

Find `VX_Y_Start` in `cockroach_versions.go`:
```go
V26_1_Start: {Major: 25, Minor: 4, Internal: 2}
```
Convert to JSON and update the test file:
```diff
-"systemDatabaseSchemaVersion": {"internal": 14, "majorVal": 1000025, "minorVal": 3}
+"systemDatabaseSchemaVersion": {"internal": 2, "majorVal": 1000025, "minorVal": 4}
```
Verify: `./dev test pkg/sql/logictest --filter='TestReadCommittedLogic/crdb_internal_catalog'`

#### C. Bootstrap Schema Hash Mismatches

**Symptoms:** `TestInitialValuesToString` hash mismatch, `TestValidateSystemSchemaAfterBootStrap` failures.

**Fix:**
```bash
bazel test //pkg/sql/catalog/bootstrap:bootstrap_test \
  --test_arg=-rewrite \
  --sandbox_writable_path=$(pwd)/pkg/sql/catalog/bootstrap

# Verify
./dev test pkg/sql/catalog/bootstrap -f='TestInitialValuesToString'
```

#### D. Mixedversion Test Failures (TRICKY!)

**Common symptoms:**
- `TestTestPlanner/step_stages` — extra version in upgrade path
- `Test_choosePreviousReleases/skip-version_upgrades` — wrong version list
- `TestSupportsSkipUpgradeTo` — unexpected true/false value

**⚠️ These are almost NEVER caused by the version bump itself.** Usually caused
by incorrect modifications to mixedversion production code.

**BEFORE making any changes:**
```bash
# Check if you modified mixedversion.go
gh pr view 149494 --json files \
  --jq '.files[] | select(.path | contains("mixedversion/mixedversion.go"))'
# If that returns nothing, previous M.1 PRs did NOT modify it — revert yours.
```

**If testdata regeneration is needed (rare):**
```bash
bazel test //pkg/cmd/roachtest/roachtestutil/mixedversion:mixedversion_test \
  --test_filter=TestTestPlanner \
  --test_arg=-rewrite \
  --sandbox_writable_path=$(pwd)/pkg/cmd/roachtest/roachtestutil/mixedversion

# Always run the FULL suite after, not just filtered
./dev test pkg/cmd/roachtest/roachtestutil/mixedversion
```

If unsure: do NOT modify mixedversion code. Ask on #test-eng or file an issue.

---

## Post-Fix Verification

After fixing test failures:

```bash
# Run the tests that previously failed
./dev test pkg/sql/logictest --filter='<test_name>'

# Run FULL test suites for packages with modified production code
./dev test pkg/cmd/roachtest/roachtestutil/mixedversion  # If mixedversion changed
./dev test pkg/sql/pgwire -f='TestPGTest'                # If pgwire tests failed
./dev test pkg/sql/logictest -f='TestLogic'              # After logic test changes

# Verify file count is similar to previous M.1 PRs (~60-65 files total)
git diff --stat <base_branch>
```

---

## Files That Should/Shouldn't Change

**SHOULD change:**
- ✅ `pkg/clusterversion/cockroach_versions.go`
- ✅ `pkg/sql/logictest/testdata/logic_test/*` (version expectations)
- ✅ `pkg/sql/catalog/bootstrap/testdata/*`
- ✅ `pkg/sql/catalog/systemschema_test/testdata/*`
- ✅ `pkg/sql/schemachanger/scplan/internal/rules/release_X_Y/*`
- ✅ `pkg/testutils/release/cockroach_releases.yaml`
- ✅ `docs/generated/settings/*`

**Should NOT change (unless test is new):**
- ❌ `pkg/cmd/roachtest/roachtestutil/mixedversion/mixedversion.go`
- ❌ `pkg/clusterversion/clusterversion.go`
- ❌ `pkg/sql/logictest/REPOSITORIES.bzl` (only changes in M.2 after RC)

**Historical file counts (for sanity check):**
- PR 149494 (25.3 → 25.4): 60 files changed
- PR 156225 (25.4 → 26.1): 63 files changed (50 original + 14 test fixes)

---

## Manual Reference PR Comparison

The `compare-with-reference-pr.sh` script handles this automatically. These are
the manual steps if you need finer control:

```bash
# Get files in current work
git diff --name-only <base_branch> | sort > /tmp/current_files.txt

# Get files from reference M.1 PR
gh pr view 149494 --json files --jq '.files[].path' | sort > /tmp/previous_files.txt

# Find unexpected additions
comm -13 /tmp/previous_files.txt /tmp/current_files.txt

# Find potentially missing files
comm -23 /tmp/previous_files.txt /tmp/current_files.txt
```

**Real example of why this matters (PR #156225):**
`REPOSITORIES.bzl` was incorrectly modified by `release update-releases-file`,
removing 25.2.7 config and adding 25.4.0-rc.1 config. PR #149494 did NOT modify
this file. Caused test failures from a broken baseline — would have been caught
immediately by a file comparison.
