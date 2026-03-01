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

**IMPORTANT:** The `update-releases-file` tool does NOT work for this M.1 task. You must **manually** update `pkg/testutils/release/cockroach_releases.yaml`.

**Manual Update Steps:**

When bumping from version X.Y to X.Z (e.g., 26.1 → 26.2):

1. **Do NOT change any existing mappings** - Keep all previous entries as-is
2. **Find the latest mapping** - For example: `"26.1": predecessor: "25.4"`
3. **Add a new entry** for the new version with the same predecessor:
```yaml
"26.1":
  predecessor: "25.4"
"26.2":
  predecessor: "25.4"   # Use same predecessor as 26.1
```

**Why:** Both 26.1 and 26.2 are development versions that haven't been released yet. They both should upgrade from the last stable release (25.4). This will be corrected later in M.3 when we add actual release data.

**Example for 26.1 → 26.2 bump:**
```bash
# Before (what exists):
"25.4":
  latest: 25.4.2
  predecessor: "25.3"
"26.1":
  predecessor: "25.4"

# After (what you should have):
"25.4":
  latest: 25.4.2
  predecessor: "25.3"
"26.1":
  predecessor: "25.4"    # KEEP this entry
"26.2":
  predecessor: "25.4"    # ADD this entry

# Verify your changes
tail -10 pkg/testutils/release/cockroach_releases.yaml
```

**Note:** In M.3, when the RC is published, the predecessor relationships will be updated to reflect the actual release hierarchy.

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

See [failures/m1_failures.md](failures/m1_failures.md) for quick errors, the CI
failure decision tree, Type 1/Type 2 classifications, and the files-that-should/
shouldn't-change reference.

### Important Notes

- **Do NOT update `PreviousRelease`** - This doesn't happen until M.3
- **MUST manually update `pkg/testutils/release/cockroach_releases.yaml`** - Add new version entry, keep previous entries (see step 8b for details)
- **Version numbers use the previous release's minor for internal versions** - V26_1_Start has version `25.4-2`, not `26.1-2`
- **All internal version numbers must be even** - This convention must be maintained
- **Schema changer rules must be versioned** - Each release gets its own frozen copy of the rules

### Verification Checklist

Before creating or re-pushing the PR, run the pre-push validation script:
```bash
./pkg/clusterversion/runbooks/scripts/validate-m1.sh
```
This rewrites test data and runs the unit tests most likely to fail after an M.1 change.

Also compare changed files against the reference PR to catch unexpected scope:
```bash
./pkg/clusterversion/runbooks/scripts/compare-with-reference-pr.sh 149494
```

Before committing, verify:

- [ ] All version constants follow naming conventions (V{MAJOR}_{MINOR}_Start)
- [ ] Internal version number is even (e.g., 2, 4, 6)
- [ ] Successor map includes new version
- [ ] SystemDatabaseSchemaBootstrapVersion points to new start version
- [ ] First upgrade added for new version
- [ ] version.txt updated to new alpha version
- [ ] Schema changer rules copied and updated
- [ ] validate-m1.sh passes cleanly
- [ ] compare-with-reference-pr.sh shows no unexpected file changes
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

### CI Test Failures After M.1 PR

See [failures/m1_failures.md](failures/m1_failures.md) for the full decision tree,
Type 1/Type 2 failure guides, post-fix verification, and the manual reference PR
comparison steps.

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

