# M.3: Enable Upgrade Tests - Quick Reference

**Full details:** See `M3_enable_upgrade_tests.md`

## Overview

Enable upgrade tests after the first RC is published using a **2-PR approach** (recommended).

**Critical:** Fixtures MUST be generated on **gceworker** (amd64), NOT Mac.

**Reference PRs:**
- Fixtures: #150712
- Code: #152080

---

## Prerequisites Checklist

- [ ] First RC published (e.g., v25.4.0-rc.1) - verify at cockroachlabs.com/docs/releases
- [ ] M.1 and M.2 completed (V25_4 constant exists, bootstrap data added)
- [ ] Access to gceworker (or can create one)
- [ ] Know exact RC version number

---

## PR 1: Fixtures (~6 files)

### Step 1: Update Releases File (on Mac)

```bash
git checkout master
git pull origin master
git checkout -b enable-upgrade-tests-25.4-fixtures

# Build and run release tool
bazel build //pkg/cmd/release:release
_bazel/bin/pkg/cmd/release/release_/release update-releases-file
```

**Files updated:**
- `pkg/testutils/release/cockroach_releases.yaml` - Adds RC version
- `pkg/sql/logictest/REPOSITORIES.bzl` - Adds RC binaries with checksums

### ⚠️ CRITICAL: Verify REPOSITORIES.bzl

The tool may **incorrectly remove** older version binaries still needed by active testserver configs.

```bash
# Check active testserver configs
grep "cockroach-go-testserver-" pkg/sql/logictest/logictestbase/logictestbase.go | grep "Name:"

# Verify REPOSITORIES.bzl has binaries for ALL active versions
grep -E "^\s+\(\"25\.[0-9]" pkg/sql/logictest/REPOSITORIES.bzl
```

**If a version is missing:**
```bash
# Get old config
git show HEAD^:pkg/sql/logictest/REPOSITORIES.bzl | grep -A 4 "25.X.Y"

# Manually restore to REPOSITORIES.bzl
```

**Pattern:** Keep N-2, N-1, and N release binaries until their testserver configs are removed.

### Commit

```bash
git add pkg/testutils/release/cockroach_releases.yaml pkg/sql/logictest/REPOSITORIES.bzl
git commit -m "master: Update pkg/testutils/release/cockroach_releases.yaml

Updates releases file with v25.4.0-rc.1 RC and adds RC binaries to REPOSITORIES.bzl.

Part of M.3 fixtures preparation.

Release note: None
Epic: None"
```

---

### Step 2: Generate Fixtures on gceworker

#### 2.1: Setup gceworker (First Time)

**⚠️ MUST run update-firewall BEFORE create, or connection will fail!**

```bash
# On Mac - set zone (optional)
export CLOUDSDK_COMPUTE_ZONE=us-west1-a  # Or us-east1-b

# Update firewall FIRST
./scripts/gceworker.sh update-firewall

# Create gceworker (takes 5-10 min)
./scripts/gceworker.sh create

# SSH to gceworker
./scripts/gceworker.sh start
```

**Zones:**
- `us-west1-a` (Oregon - West Coast)
- `us-east1-b` (South Carolina - East Coast)

#### 2.2: Clone Repo on gceworker

**On gceworker terminal:**

```bash
mkdir -p ~/go/src/github.com/cockroachdb
cd ~/go/src/github.com/cockroachdb
git clone git@github.com:<your-username>/cockroach.git
cd cockroach

# Add upstream and fetch tags
git remote add upstream https://github.com/cockroachdb/cockroach.git
git fetch upstream --tags

# Checkout RC tag
git checkout v25.4.0-rc.1

# Set environment
export FIXTURE_VERSION=v25.4.0-rc.1
export COCKROACH_DEV_LICENSE="<your-license>"

# Verify
echo $COCKROACH_DEV_LICENSE
```

#### 2.3: Build Binaries

```bash
# Configure (first time only)
./dev doctor
# Press Enter for "dev" config
# Type "n" for lintonbuild

# Build
./dev build cockroach short //c-deps:libgeos roachprod workload roachtest

# Clean up previous clusters (error "does not exist" is OK)
./bin/roachprod destroy local
```

#### 2.4: Generate Fixtures

```bash
./bin/roachtest run generate-fixtures --local --debug \
  --cockroach ./cockroach --suite fixtures
```

**Expected:** Test FAILS intentionally and prints move commands.

#### 2.5: Move Fixtures

```bash
# Copy command from test output and run it:
for i in 1 2 3 4; do
  mkdir -p pkg/cmd/roachtest/fixtures/${i} && \
  mv artifacts/generate-fixtures/run_1/logs/${i}.unredacted/checkpoint-*.tgz \
    pkg/cmd/roachtest/fixtures/${i}/
done

# Verify
ls -lh pkg/cmd/roachtest/fixtures/*/checkpoint-v25.4.tgz
```

**Expected:** 4 files, each ~3-5 MB.

---

### Step 3: Copy Fixtures to Mac

**On Mac terminal:**

```bash
# Create tmp directory
mkdir -p /tmp/fixtures-25.4

# Copy from gceworker (adjust username/zone)
scp -r gceworker-<yourname>.us-west1-a.cockroach-workers:~/go/src/github.com/cockroachdb/cockroach/pkg/cmd/roachtest/fixtures /tmp/fixtures-25.4/

# Navigate to local repo
cd ~/go/src/github.com/cockroachdb/cockroach
git checkout enable-upgrade-tests-25.4-fixtures

# Copy fixtures
for i in 1 2 3 4; do
  cp /tmp/fixtures-25.4/fixtures/${i}/checkpoint-v25.4.tgz pkg/cmd/roachtest/fixtures/${i}/
done

# Verify
ls -lh pkg/cmd/roachtest/fixtures/*/checkpoint-v25.4.tgz
```

---

### Step 4: Commit and Push Fixtures PR

```bash
git add pkg/cmd/roachtest/fixtures/*/checkpoint-v25.4.tgz
git status  # Verify

git commit -m "roachtest: add 25.4 fixtures

Adds roachtest fixtures for v25.4.0-rc.1 to enable upgrade testing.

Fixtures generated on gceworker by running:
  ./bin/roachtest run generate-fixtures --local --debug \\
    --cockroach ./cockroach --suite fixtures

Part of M.3 \"Enable upgrade tests\" checklist.

Release note: None
Epic: None

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"

# Push to fork
git push -u celiala enable-upgrade-tests-25.4-fixtures

# Create PR
gh pr create --repo cockroachdb/cockroach \
  --title "master: Update releases file and add 25.4 fixtures" \
  --body "Part of M.3: Enable upgrade tests for 25.4.

This PR:
- Updates releases file with v25.4.0-rc.1
- Updates REPOSITORIES.bzl with RC binaries
- Adds roachtest fixtures for v25.4.0-rc.1

Fixtures generated on gceworker using amd64 architecture.

Epic: None"
```

**Wait for this PR to merge before proceeding to PR 2.**

---

## PR 2: Code Changes (~9 files)

### Step 1: Create Branch

```bash
git checkout master
git pull origin master
git checkout -b enable-upgrade-tests-25.4-code
```

---

### Step 2: Update PreviousRelease Constant

**File:** `pkg/clusterversion/cockroach_versions.go` (line ~332)

```go
// Before
const PreviousRelease Key = V25_3

// After
const PreviousRelease Key = V25_4
```

---

### Step 3: Add cockroach-go-testserver-25.4 Config

**File:** `pkg/sql/logictest/logictestbase/logictestbase.go`

**Add config** (after 25.3 config, line ~560):

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

**Add to set** (line ~615):

```go
"cockroach-go-testserver-configs": makeConfigSet(
	"cockroach-go-testserver-25.2",
	"cockroach-go-testserver-25.3",
	"cockroach-go-testserver-25.4",  // Add this
),
```

---

### Step 4: Update BUILD.bazel Visibility

**File:** `pkg/sql/logictest/BUILD.bazel` (line ~160)

```bazel
cockroach_predecessor_version(
    name = "cockroach_predecessor_version",
    visibility = [
        "//pkg/ccl/logictestccl:__subpackages__",
        "//pkg/sql/logictest/tests/cockroach-go-testserver-25.2:__pkg__",
        "//pkg/sql/logictest/tests/cockroach-go-testserver-25.3:__pkg__",
        "//pkg/sql/logictest/tests/cockroach-go-testserver-25.4:__pkg__",  # Add
        "//pkg/sql/sqlitelogictest:__subpackages__",
    ],
)
```

---

### Step 5: Verify supportsSkipUpgradeTo

**File:** `pkg/cmd/roachtest/roachtestutil/mixedversion/mixedversion.go` (line ~850)

```go
func supportsSkipUpgradeTo(v *version.Version) bool {
	return v.Major() == 25 && v.Minor() == 4 && v.Patch() == 0 && v.PreRelease() == ""
}
```

**Check:** If Minor() == 4 covers 25.4, no changes needed. Otherwise, update condition.

---

### Step 6: Generate Bazel Files

```bash
./dev gen bazel
```

**Generates:**
- `pkg/sql/logictest/tests/cockroach-go-testserver-25.4/BUILD.bazel`
- `pkg/sql/logictest/tests/cockroach-go-testserver-25.4/generated_test.go`
- Updates to various BUILD.bazel files

---

### Step 6.5: Regenerate Declarative Rules Corpus

Bumping `PreviousRelease` changes which schema changer rules are active, so the
declarative rules test corpus must be regenerated:

```bash
./dev test pkg/cli -f=TestDeclarativeRules --rewrite
```

**Updates:** `pkg/cli/testdata/declarative-rules/deprules`

---

### Step 7: Commit and Push Code PR

```bash
git add -A
git status  # Verify

git commit -m "clusterversion: bump PreviousRelease to V25_4

Updates PreviousRelease constant from V25_3 to V25_4 and adds the
cockroach-go-testserver-25.4 logictest configuration to enable
upgrade tests for version 25.4.

Changes:
- Updated PreviousRelease constant
- Added cockroach-go-testserver-25.4 test configuration
- Generated test files for new config
- Updated BUILD.bazel files via ./dev gen bazel

The supportsSkipUpgradeTo logic already handles 25.4 correctly.

Part of M.3 \"Enable upgrade tests\" checklist.

Release note: None
Epic: None

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"

# Push to fork
git push -u celiala enable-upgrade-tests-25.4-code

# Create PR
gh pr create --repo cockroachdb/cockroach \
  --title "clusterversion: bump PreviousRelease to V25_4" \
  --body "Part of M.3: Enable upgrade tests for 25.4.

This PR:
- Updates PreviousRelease constant to V25_4
- Adds cockroach-go-testserver-25.4 logictest configuration
- Generates test files for the new configuration

Depends on fixtures PR being merged first.

Epic: None"
```

---

## Expected Files

### PR 1 (Fixtures): ~6 files
1. `pkg/testutils/release/cockroach_releases.yaml`
2. `pkg/sql/logictest/REPOSITORIES.bzl`
3-6. `pkg/cmd/roachtest/fixtures/{1,2,3,4}/checkpoint-v25.4.tgz`

### PR 2 (Code): ~10 files
1. `pkg/clusterversion/cockroach_versions.go`
2. `pkg/sql/logictest/logictestbase/logictestbase.go`
3. `pkg/sql/logictest/BUILD.bazel`
4. `pkg/cmd/roachtest/roachtestutil/mixedversion/mixedversion.go` (verify only)
5. `pkg/cli/testdata/declarative-rules/deprules` (regenerated via `--rewrite`)
6. `pkg/sql/logictest/tests/cockroach-go-testserver-25.4/BUILD.bazel` (generated)
7. `pkg/sql/logictest/tests/cockroach-go-testserver-25.4/generated_test.go` (generated)
8-10. Various BUILD.bazel updates from `./dev gen bazel`

---

## Validation

### Fixtures PR

```bash
# Verify fixture sizes
ls -lh pkg/cmd/roachtest/fixtures/*/checkpoint-v25.4.tgz

# Verify releases file updated
grep -A 2 '"25.4":' pkg/testutils/release/cockroach_releases.yaml

# Verify REPOSITORIES.bzl has RC binaries
grep "25.4.0-rc.1" pkg/sql/logictest/REPOSITORIES.bzl
```

### Code PR

```bash
# Verify PreviousRelease updated
grep "const PreviousRelease" pkg/clusterversion/cockroach_versions.go

# Verify testserver config added
grep "cockroach-go-testserver-25.4" pkg/sql/logictest/logictestbase/logictestbase.go

# Run unit tests
./dev test pkg/clusterversion -v
./dev test pkg/roachpb -v
./dev test pkg/sql/logictest/logictestbase -v
./dev test pkg/cli -f=TestDeclarativeRules -v

# Build succeeds
./dev build short
```

---

## Critical Warnings

| Warning | Impact | Prevention |
|---------|--------|------------|
| ⚠️ **Fixtures on Mac** | Wrong architecture, tests fail | MUST use gceworker (amd64) |
| ⚠️ **REPOSITORIES.bzl verification** | Nightly builds break | Manually verify ALL active testserver versions present |
| ⚠️ **gceworker firewall** | Can't connect to gceworker | MUST run `update-firewall` BEFORE `create` |
| ⚠️ **Session disconnect** | Lose environment vars | Re-export FIXTURE_VERSION and COCKROACH_DEV_LICENSE |

---

## Quick Troubleshooting

### gceworker Issues

| Error | Fix |
|-------|-----|
| SSH connection fails (exit 255) | `./scripts/gceworker.sh start` (retry) or destroy & recreate |
| "bazel: command not found" | Run `./dev doctor` to install |
| "COCKROACH_DEV_LICENSE not set" | `export COCKROACH_DEV_LICENSE="<license>"` |

### Fixture Generation

| Error | Fix |
|-------|-----|
| "license required" | Verify: `echo $COCKROACH_DEV_LICENSE` |
| "roachprod cluster exists" | `./bin/roachprod destroy local` |
| Fixtures 0 bytes or >10MB | Compare with previous version sizes, regenerate if needed |

### CI Failures After PR 2

| Error | Fix |
|-------|-----|
| `TestLogic_mixed_version_bootstrap_tenant` fails with descriptor diffs | System table descriptors evolve between versions. Exclude differing keys in the test's WHERE clause (see full runbook for details) |
| `TestDeclarativeRules` fails with version mismatch | Forgot Step 6.5: run `./dev test pkg/cli -f=TestDeclarativeRules --rewrite` |

### REPOSITORIES.bzl

| Error | Fix |
|-------|-----|
| Tool removed needed version | `git show HEAD^:pkg/sql/logictest/REPOSITORIES.bzl | grep -A 4 "X.Y.Z"` then restore manually |
| Nightly build fails "missing binary" | Verify all active testserver versions in REPOSITORIES.bzl |

---

## Alternative: Single PR Approach

If you prefer one combined PR (not recommended):

1. Do all steps from PR1 and PR2 on a single branch
2. Commit everything together (~15 files)
3. Example: #141765

**Recommendation:** Stick with 2-PR approach for easier review.
