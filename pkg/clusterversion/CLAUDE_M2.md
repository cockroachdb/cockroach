# CockroachDB M.2: Enable Mixed-Cluster Logic Tests Guide

This document provides step-by-step instructions for the M.2 task: "Enable mixed-cluster logic tests" on the master branch after the M.1 version bump. This follows the checklist from `pkg/clusterversion/README.md`.

## Overview

**When to perform:** After M.1 has been completed (version bump on master).

**What it does:** Adds bootstrap data from the forked release branch and configures mixed-cluster logic test support. This enables testing of mixed-version clusters (e.g., testing 26.1 code with a 25.4 bootstrap schema).

**Example:** After bumping master to 26.1 (M.1), enable mixed-cluster tests for 25.4.

## Prerequisites

Before starting, ensure:
1. M.1 has been completed on master
2. The release branch exists (e.g., `release-25.4`)
3. You know the release version to enable mixed-cluster tests for (e.g., 25.4)

## Step-by-Step Checklist

### 1. Generate Bootstrap Data from Release Branch

**Switch to the release branch:**
```bash
git checkout release-25.4
```

**Build and run the bootstrap data generator:**
```bash
./dev build sql-bootstrap-data
bin/sql-bootstrap-data
```

This will create 4 files in `pkg/sql/catalog/bootstrap/data/`:
- `25_4_system.keys` - Bootstrap data for system tenant
- `25_4_system.sha256` - SHA-256 hash of system data
- `25_4_tenant.keys` - Bootstrap data for non-system tenants
- `25_4_tenant.sha256` - SHA-256 hash of tenant data

**Copy the generated files (note the location):**
The files are now in your working directory. You'll add them to master in the next step.

### 2. Switch Back to Master Branch

**Return to your M.2 working branch:**
```bash
git checkout enable-mixed-cluster-25.4  # or whatever branch name you're using
```

The bootstrap data files from step 1 should still be in `pkg/sql/catalog/bootstrap/data/`.

### 3. Update pkg/sql/catalog/bootstrap/initial_values.go

**Add the new bootstrap data mapping** (around the `hardCodedMap` section):

```go
var hardCodedMap = map[clusterversion.Key]func() ([]kvpb.KeyValue, []kvpb.KeyValue){
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

**Add the go:embed variables** (at the end of the file):

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

### 4. Update pkg/sql/catalog/bootstrap/BUILD.bazel

**Add the bootstrap data files to embedsrcs:**

```starlark
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

### 5. Add Mixed-Cluster Logictest Configuration

**Edit pkg/sql/logictest/logictestbase/logictestbase.go:**

Add the new configuration to the `logicTestConfigs` array (around line 200-300):

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

**Add to DefaultConfigNames** (around line 450):

```go
"local-mixed-25.2",
"local-mixed-25.3",
"local-mixed-25.4",  // Add this line
```

### 6. Generate Test Files

**Run the code generator:**
```bash
./dev gen bazel
```

This will automatically create:
- `pkg/ccl/logictestccl/tests/local-mixed-25.4/` (BUILD.bazel and generated_test.go)
- `pkg/sql/logictest/tests/local-mixed-25.4/` (BUILD.bazel and generated_test.go)
- `pkg/sql/sqlitelogictest/tests/local-mixed-25.4/` (BUILD.bazel and generated_test.go)

**Add the generated directories to git:**
```bash
git add pkg/ccl/logictestccl/tests/local-mixed-25.4/
git add pkg/sql/logictest/tests/local-mixed-25.4/
git add pkg/sql/sqlitelogictest/tests/local-mixed-25.4/
```

### 7. Update Logictests with Skipif Directives

Some logic tests need to be skipped for the new mixed-version config. Look at the previous M.2 PR for the release to see which files were updated.

**Common files to update:**

1. **pkg/sql/logictest/testdata/logic_test/crdb_internal_catalog**

Add `local-mixed-25.4` to existing skipif directives:
```
skipif config schema-locked-disabled local-mixed-25.3 local-mixed-25.4
```

2. **pkg/sql/logictest/testdata/logic_test/row_level_security**

Add `!local-mixed-25.4` to the LogicTest header at the top of the file:
```
# LogicTest: !local-legacy-schema-changer !local-mixed-25.2 !local-mixed-25.3 !local-mixed-25.4
```

**How to find other files:**
```bash
# Look at the previous M.2 PR to see which test files were updated
gh pr diff <previous-m2-pr-number> | grep -A5 -B5 skipif
```

### 8. Test Your Changes

**Test the bootstrap package:**
```bash
./dev test pkg/sql/catalog/bootstrap -f TestInitialKeys -v
```

This verifies that the bootstrap data is valid and can be loaded correctly.

**Optionally run a few mixed-cluster logic tests:**
```bash
./dev testlogic base --config=local-mixed-25.4 --files='prepare'
```

### 9. Create Commit and PR

**Add all changes:**
```bash
git add -A
```

**Create commit:**
```bash
git commit -m "bootstrap: add 25.4 bootstrap data

This change enables mixed-cluster logic tests for version 25.4 by adding
bootstrap data from release-25.4 and configuring the local-mixed-25.4 test
configuration.

The bootstrap data was obtained by running (on release-25.4 branch):
  ./dev build sql-bootstrap-data && bin/sql-bootstrap-data

Changes include:
- Added bootstrap data files (25_4_system.keys/sha256, 25_4_tenant.keys/sha256)
- Updated initial_values.go with V25_4 mapping
- Added local-mixed-25.4 logictest configuration
- Generated test files for local-mixed-25.4 config
- Updated logictests with skipif directives for local-mixed-25.4

Part of M.2 \"Enable mixed-cluster logic tests\" checklist.

Release note: None"
```

**Push and create PR:**
```bash
git push -u <your-fork> enable-mixed-cluster-25.4
gh pr create --draft --title "bootstrap: add 25.4 bootstrap data" \
  --body "See commit message" --base master
```

## Common Errors and Solutions

### Error: "no such file or directory" for bootstrap data files

**Problem:** The bootstrap data files weren't generated or are in the wrong location.

**Solution:** Make sure you ran the generation on the release branch:
```bash
git checkout release-25.4
./dev build sql-bootstrap-data && bin/sql-bootstrap-data
```

### Error: Build failure in initial_values.go

**Problem:** Missing or incorrect go:embed directives.

**Solution:** Ensure all 4 go:embed variables are added at the end of the file and the variable names match exactly (e.g., `v25_4_system_keys`, not `v254_system_keys`).

### Error: Test failures in generated_test.go files

**Problem:** The test configuration might be incorrect or skipif directives are missing.

**Solution:**
1. Check that the logictest config has `DisableUpgrade: true`
2. Review the previous M.2 PR to see if additional skipif directives are needed

### Warning: "no tests to run" when running tests

**Problem:** The test filter doesn't match any tests.

**Solution:** This is usually fine if you're just verifying the bootstrap package compiles. Add `-v` flag to see the warning is just informational.

## File Count Check

Typically, M.2 changes around 20 files:
- 4 bootstrap data files (new)
- 2 Go files (initial_values.go, logictestbase.go)
- 1 BUILD.bazel file (bootstrap)
- 6 generated test files (3 directories × 2 files each)
- 2-4 logictest data files (skipif directives)
- 3-4 generated test files (updates from ./dev gen)
- Some generated settings documentation files

If your count is significantly different, double-check the steps above.

## References

- Main README: `pkg/clusterversion/README.md`
- Previous M.2 PRs: Search GitHub for "bootstrap: add 25.3 bootstrap data"
- Bootstrap package: `pkg/sql/catalog/bootstrap/`
- Logictest configs: `pkg/sql/logictest/logictestbase/logictestbase.go`

## Notes

- M.2 should be based on the M.1 PR branch, not directly on master
- The bootstrap data files are binary and will be large (~160KB each for the .keys files)
- Always verify the SHA-256 files contain exactly one line with a 64-character hex string
- This task is much more mechanical than M.1, but still requires attention to detail
