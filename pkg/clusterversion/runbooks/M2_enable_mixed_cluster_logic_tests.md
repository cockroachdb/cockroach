# M.2: Enable Mixed-Cluster Logic Tests

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

