# M.3 Plan: Enable upgrade tests for 25.4

## Prerequisites
- **25.4.0-rc.1 is published** ✓ (confirmed in releases file)
- **M.2 branch is complete** ✓ (`enable-mixed-cluster-25.4` branch)
- **Access to a gceworker** - **REQUIRED** for fixture generation

---

## IMPORTANT: Two Approaches

Based on recent PRs, there are two valid approaches:

### Approach A: Two separate PRs (most recent pattern - PR #150712 + #152080)
1. **PR 1 (Fixtures)**: Fixtures + run `release update-releases-file`
2. **PR 2 (Code)**: Update PreviousRelease + add testserver config

### Approach B: Single combined PR (older pattern - PR #141765)
1. **One PR**: Everything together

**Recommendation**: Use Approach A (two PRs) as it's the most recent pattern and makes review easier.

---

## Phase 1: Fixtures PR (if using Approach A)

### 1.1 Update releases file (on Mac)
```bash
git checkout enable-mixed-cluster-25.4
git checkout -b enable-upgrade-tests-25.4-fixtures

bazel build //pkg/cmd/release:release
_bazel/bin/pkg/cmd/release/release_/release update-releases-file
```

**What this updates:**
- `pkg/testutils/release/cockroach_releases.yaml` - Adds 25.4.0-rc.1
- `pkg/sql/logictest/REPOSITORIES.bzl` - Adds 25.4.0-rc.1 binaries with checksums

**Commit this first:**
```bash
git add pkg/testutils/release/cockroach_releases.yaml pkg/sql/logictest/REPOSITORIES.bzl
git commit -m "master: Update pkg/testutils/release/cockroach_releases.yaml"
```

### 1.2 Generate fixtures (MUST use gceworker)

**⚠️ CRITICAL: This CANNOT be done on Mac. Must use gceworker (amd64 required).**

Fixtures README states: "the roachtest needs to be run on `amd64` (if you are using a Mac it's recommended to use a gceworker)."

**a) SSH to gceworker
```bash
# Create or use existing gceworker
roachprod create <your-gceworker-name> -n 1 --gce-machine-type n2-standard-4
roachprod ssh <your-gceworker-name>:1
```

**b) On gceworker - Clone and checkout
```bash
git clone git@github.com:celiala/cockroach.git
cd cockroach
git checkout v25.4.0-rc.1
export FIXTURE_VERSION=v25.4.0-rc.1
```

**c) On gceworker - Set license
```bash
export COCKROACH_DEV_LICENSE="<your-license-key>"
```

**d) On gceworker - Build binaries
```bash
./dev build cockroach short //c-deps:libgeos roachprod workload roachtest
./bin/roachprod destroy local  # Clean up any remnants
```

**e) On gceworker - Generate fixtures
```bash
./bin/roachtest run generate-fixtures --local --debug \
  --cockroach ./cockroach --suite fixtures
```

**Expected:** This will FAIL intentionally with instructions like:
```
for i in 1 2 3 4; do
  mkdir -p pkg/cmd/roachtest/fixtures/${i} && \
  mv artifacts/generate-fixtures/run_1/logs/${i}.unredacted/checkpoint-*.tgz \
    pkg/cmd/roachtest/fixtures/${i}/
done
```

**f) On gceworker - Move fixtures
```bash
# Run the command from the test output
# Verify you get checkpoint-v25.4.tgz in each directory
ls -la pkg/cmd/roachtest/fixtures/*/checkpoint-v25.4.tgz
```

**g) Copy fixtures back to Mac
```bash
# On your Mac:
roachprod get <gceworker>:1 cockroach/pkg/cmd/roachtest/fixtures/ /tmp/fixtures-25.4/

# Copy to your local repo:
cp /tmp/fixtures-25.4/*/checkpoint-v25.4.tgz pkg/cmd/roachtest/fixtures/
```

**h) Add fixtures and push PR**
```bash
git add pkg/cmd/roachtest/fixtures/*/checkpoint-v25.4.tgz
git commit -m "roachtest: add 25.4 fixtures"
git push -u celiala enable-upgrade-tests-25.4-fixtures
gh pr create --repo cockroachdb/cockroach --title "master: Update releases file and add 25.4 fixtures"
```

---

## Phase 2: Code Changes PR (if using Approach A)

### 2.1 Create new branch (wait for fixtures PR to merge first)
```bash
git checkout master
git pull origin master  # Get the merged fixtures PR
git checkout -b enable-upgrade-tests-25.4-code
```

### 2.2 Update PreviousRelease constant
- **File:** `pkg/clusterversion/cockroach_versions.go`
- **Change:** `const PreviousRelease Key = V25_3` → `const PreviousRelease Key = V25_4`

### 2.3 Add cockroach-go-testserver-25.4 config
- **File:** `pkg/sql/logictest/logictestbase/logictestbase.go`
- Add config similar to `cockroach-go-testserver-25.3`
- Add to `cockroach-go-testserver-configs` set

### 2.4 Update logictest BUILD.bazel visibility
- **File:** `pkg/sql/logictest/BUILD.bazel`
- Update `cockroach_predecessor_version` visibility to include 25.4

### 2.5 Verify supportsSkipUpgradeTo logic
- **File:** `pkg/cmd/roachtest/roachtestutil/mixedversion/mixedversion.go`
- Check if any special handling is needed for 25.4

### 2.6 Generate bazel files
```bash
./dev gen bazel
```

### 2.7 Commit and push
```bash
git add -A
git commit -m "clusterversion: bump PreviousVersion to 25.4"
git push -u celiala enable-upgrade-tests-25.4-code
gh pr create --repo cockroachdb/cockroach --title "clusterversion: bump PreviousVersion"
```

---

## Phase 3: Verification

### 3.1 Check version gate alignment
```bash
# Compare gates on master vs release-25.4
git diff origin/release-25.4 -- pkg/clusterversion/cockroach_versions.go
```

Ensure all V25_4_* gates are identical.

### 3.2 Run tests
```bash
# Test with new config
./dev testlogic base --config=cockroach-go-testserver-25.4 --files=<some-test>

# Bootstrap tests
./dev test pkg/sql/catalog/bootstrap
```

---

## Key Differences: M.2 vs M.3

| Aspect | M.2 | M.3 |
|--------|-----|-----|
| **Timing** | After beta cut | After RC.1 published |
| **PreviousRelease** | Not updated | Updated to V25_4 |
| **Bootstrap data** | Generated from release branch | Already done in M.2 |
| **Fixtures** | Not needed | **Required - gceworker only** |
| **REPOSITORIES.bzl** | Not updated | Updated with RC binaries |
| **testserver config** | local-mixed-25.4 only | Add cockroach-go-testserver-25.4 |

---

## Expected Files Modified

### Approach A (Two PRs):

**Fixtures PR (~6 files):**
1. `pkg/testutils/release/cockroach_releases.yaml` - Updated by release tool
2. `pkg/sql/logictest/REPOSITORIES.bzl` - Updated by release tool with binaries
3. `pkg/cmd/roachtest/fixtures/1/checkpoint-v25.4.tgz` - Generated on gceworker
4. `pkg/cmd/roachtest/fixtures/2/checkpoint-v25.4.tgz` - Generated on gceworker
5. `pkg/cmd/roachtest/fixtures/3/checkpoint-v25.4.tgz` - Generated on gceworker
6. `pkg/cmd/roachtest/fixtures/4/checkpoint-v25.4.tgz` - Generated on gceworker

**Code PR (~9 files):**
1. `pkg/clusterversion/cockroach_versions.go` - PreviousRelease constant
2. `pkg/sql/logictest/logictestbase/logictestbase.go` - Add testserver config
3. `pkg/sql/logictest/BUILD.bazel` - Visibility update
4. `pkg/BUILD.bazel` - Updated by `./dev gen bazel`
5. `pkg/cli/testdata/declarative-rules/deprules` - Generated
6. `pkg/sql/logictest/tests/cockroach-go-testserver-25.4/BUILD.bazel` - Generated
7. `pkg/sql/logictest/tests/cockroach-go-testserver-25.4/generated_test.go` - Generated
8. Possibly: `pkg/cmd/roachtest/roachtestutil/mixedversion/mixedversion.go` - If logic needs update
9. Possibly: Test expectation files - Various logic test files

### Approach B (One PR):
- All of the above files in a single PR (~15 files total)

---

## Common Pitfalls to Avoid

1. **DON'T generate fixtures on Mac** - Will not work, must use gceworker
2. **DON'T forget to export COCKROACH_DEV_LICENSE** - Fixture generation will fail
3. **DON'T use alpha releases** - Must use rc.1 or later (minted version)
4. **DON'T skip checking version gate alignment** - Can cause upgrade issues

---

## Validation Against Previous PRs

Before creating your PRs, compare against these recent M.3 PRs:

### Recent M.3 PRs (Two-PR approach):
- **PR #150712** (Fixtures): 6 files - releases.yaml, REPOSITORIES.bzl, + 4 fixtures
- **PR #152080** (Code): 9 files - cockroach_versions.go, logictestbase.go, BUILD.bazel, etc.

### Validation commands:
```bash
# Compare your fixtures PR files:
gh pr view 150712 --json files --jq '.files[].path' | sort > /tmp/ref_fixtures.txt
git diff --name-only <base> | sort > /tmp/my_fixtures.txt
comm -3 /tmp/ref_fixtures.txt /tmp/my_fixtures.txt

# Compare your code PR files:
gh pr view 152080 --json files --jq '.files[].path' | sort > /tmp/ref_code.txt
git diff --name-only <base> | sort > /tmp/my_code.txt
comm -3 /tmp/ref_code.txt /tmp/my_code.txt
```

**Expected differences:**
- Version numbers (25.3 vs 25.4)
- Additional test expectation files (depends on what tests need updates)

---

## Reference

- **Runbook:** `pkg/clusterversion/README.md` - M.3 checklist
- **Fixtures README:** `pkg/cmd/roachtest/fixtures/README.md`
- **Example PRs:**
  - Fixtures: [#150712](https://github.com/cockroachdb/cockroach/pull/150712)
  - Code: [#152080](https://github.com/cockroachdb/cockroach/pull/152080)
  - Combined: [#141765](https://github.com/cockroachdb/cockroach/pull/141765)
