# M.3: Enable Upgrade Tests

This section provides step-by-step instructions for the M.3 task: "Enable upgrade tests" on the master branch after the first RC is published. This task enables roachtest upgrade tests with the newly released version.

**📋 Quick Reference:** For a streamlined checklist-style guide, see [`M3_enable_upgrade_tests_QUICK.md`](M3_enable_upgrade_tests_QUICK.md). This document provides detailed explanations and troubleshooting.

---

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

# Phase 1: Fixtures PR

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

# Phase 2: Code Changes PR

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

