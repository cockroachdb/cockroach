# M.3 Plan: Enable upgrade tests for 25.4

## Prerequisites
- **25.4.0-rc.1 is published** ✓ (confirmed in releases file)
- **M.2 branch is complete** ✓ (`enable-mixed-cluster-25.4` branch)
- **Access to a gceworker** - **REQUIRED** for fixture generation

---

## Phase 1: Local Setup (can do on Mac)

### 1.1 Create new branch based on M.2
```bash
git checkout enable-mixed-cluster-25.4
git checkout -b enable-upgrade-tests-25.4
```

### 1.2 Update PreviousRelease constant
- **File:** `pkg/clusterversion/cockroach_versions.go`
- **Change:** `const PreviousRelease Key = V25_3` → `const PreviousRelease Key = V25_4`

### 1.3 Add cockroach-go-testserver-25.4 config
- **File:** `pkg/sql/logictest/logictestbase/logictestbase.go`
- Add config similar to `cockroach-go-testserver-25.3`
- Add to `cockroach-go-testserver-configs` set

### 1.4 Update logictest BUILD.bazel visibility
- **File:** `pkg/sql/logictest/BUILD.bazel`
- Update `cockroach_predecessor_version` visibility to include 25.4

### 1.5 Verify supportsSkipUpgradeTo logic
- **File:** `pkg/cmd/roachtest/roachtestutil/mixedversion/mixedversion.go`
- Check if any special handling is needed for 25.4

### 1.6 Update releases file
```bash
bazel build //pkg/cmd/release:release
_bazel/bin/pkg/cmd/release/release_/release update-releases-file
```

This should:
- Update `pkg/testutils/release/cockroach_releases.yaml` (25.4 now has rc.1)
- Update `pkg/sql/logictest/REPOSITORIES.bzl` (add 25.4.0-rc.1 binaries)

### 1.7 Generate bazel files
```bash
./dev gen bazel
```

### 1.8 Commit local changes
Commit everything EXCEPT the fixture files (which you'll add from gceworker).

---

## Phase 2: Fixture Generation (MUST use gceworker)

**⚠️ CRITICAL: This CANNOT be done on Mac. Must use gceworker (amd64 required).**

Fixtures README states: "the roachtest needs to be run on `amd64` (if you are using a Mac it's recommended to use a gceworker)."

### 2.1 SSH to gceworker
```bash
# Create or use existing gceworker
roachprod create <your-gceworker-name> -n 1 --gce-machine-type n2-standard-4
roachprod ssh <your-gceworker-name>:1
```

### 2.2 On gceworker - Clone and checkout
```bash
git clone git@github.com:celiala/cockroach.git
cd cockroach
git checkout v25.4.0-rc.1
export FIXTURE_VERSION=v25.4.0-rc.1
```

### 2.3 On gceworker - Set license
```bash
export COCKROACH_DEV_LICENSE="<your-license-key>"
```

### 2.4 On gceworker - Build binaries
```bash
./dev build cockroach short //c-deps:libgeos roachprod workload roachtest
./bin/roachprod destroy local  # Clean up any remnants
```

### 2.5 On gceworker - Generate fixtures
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

### 2.6 On gceworker - Move fixtures
```bash
# Run the command from the test output
# Verify you get checkpoint-v25.4.tgz in each directory
ls -la pkg/cmd/roachtest/fixtures/*/checkpoint-v25.4.tgz
```

### 2.7 Copy fixtures back to Mac
```bash
# On your Mac:
roachprod get <gceworker>:1 cockroach/pkg/cmd/roachtest/fixtures/ /tmp/fixtures-25.4/

# Copy to your local repo:
cp /tmp/fixtures-25.4/*/checkpoint-v25.4.tgz pkg/cmd/roachtest/fixtures/
```

### 2.8 Add fixtures to git
```bash
git add pkg/cmd/roachtest/fixtures/*/checkpoint-v25.4.tgz
git commit --amend  # Add to your existing M.3 commit
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

### Code changes (~5 files)
1. `pkg/clusterversion/cockroach_versions.go` - PreviousRelease
2. `pkg/sql/logictest/logictestbase/logictestbase.go` - Add testserver config
3. `pkg/sql/logictest/BUILD.bazel` - Visibility update
4. `pkg/cmd/roachtest/roachtestutil/mixedversion/mixedversion.go` - Verify logic

### Generated files
5. `pkg/testutils/release/cockroach_releases.yaml` - Updated by release tool
6. `pkg/sql/logictest/REPOSITORIES.bzl` - Updated by release tool
7. Various `BUILD.bazel` files - Updated by `./dev gen bazel`
8. `pkg/sql/logictest/tests/cockroach-go-testserver-25.4/` - New directory

### Fixture files (4 large binary files)
9. `pkg/cmd/roachtest/fixtures/1/checkpoint-v25.4.tgz`
10. `pkg/cmd/roachtest/fixtures/2/checkpoint-v25.4.tgz`
11. `pkg/cmd/roachtest/fixtures/3/checkpoint-v25.4.tgz`
12. `pkg/cmd/roachtest/fixtures/4/checkpoint-v25.4.tgz`

---

## Common Pitfalls to Avoid

1. **DON'T generate fixtures on Mac** - Will not work, must use gceworker
2. **DON'T forget to export COCKROACH_DEV_LICENSE** - Fixture generation will fail
3. **DON'T use alpha releases** - Must use rc.1 or later (minted version)
4. **DON'T skip checking version gate alignment** - Can cause upgrade issues

---

## Reference

- **Runbook:** `pkg/clusterversion/README.md` - M.3 checklist
- **Fixtures README:** `pkg/cmd/roachtest/fixtures/README.md`
- **Example PR:** [#141765](https://github.com/cockroachdb/cockroach/pull/141765)
