# M.1 Bump Current Version — QUICK

Advances master from vX.Y dev → vX.Z dev after forking the release-X.Y branch.
For full context and explanations, see `M1_bump_current_version.md`.

## Variables (auto-detect from cockroach_versions.go)

- `OLD`: current placeholder alias (e.g. `V26_2`) — find `const V<X>_<Y> = Latest`
- `NEW`: new placeholder alias (e.g. `V26_3`)
- `NEW_START`: new start key (e.g. `V26_3_Start`)
- `OLD_DIR`: frozen rules dir to create (e.g. `release_26_2`)
- `OLD_VER`: version string being frozen (e.g. `26.2`)
- `NEW_VER`: new alpha version string (e.g. `26.3`)

## Step 1 — cockroach_versions.go

In the `Key` iota, just before `numKeys`:
```go
// V<OLD> is CockroachDB v<OLD_VER>. It's used for all v<OLD_VER>.x patch releases.
<OLD>
<NEW_START>
```

In `versionTable`:
```go
<OLD>:       {Major: X, Minor: Y, Internal: 0},
// v<NEW_VER> versions. Internal versions must be even.
<NEW_START>: {Major: X, Minor: Y, Internal: 2},
```

Replace the placeholder alias at the bottom:
```go
// Before:  const <OLD> = Latest
// After:   const <NEW> = Latest
```

Do NOT touch `PreviousRelease` (that's M.3).

## Step 2 — pkg/roachpb/version.go

Add to successor map:
```go
{X, Y}: {X, Z},   // e.g. {26, 2}: {26, 3}
```

## Step 3 — pkg/roachpb/version_test.go

Append `<OLD_VER>, <NEW_VER>` to the expected series string in `TestReleaseSeriesSuccessor`.

## Step 4 — pkg/sql/catalog/systemschema/system.go

```go
// Before:
var SystemDatabaseSchemaBootstrapVersion = clusterversion.<OLD>.Version()
// After:
var SystemDatabaseSchemaBootstrapVersion = clusterversion.<NEW_START>.Version()
```

## Step 5 — pkg/upgrade/upgrades/upgrades.go

Append before the closing comment:
```go
newFirstUpgrade(clusterversion.<NEW_START>.Version()),
```

## Step 6 — pkg/build/version.txt

```
v<NEW_VER>.0-alpha.00000000
```

## Step 7 — Schema changer rules

```bash
# a) Freeze current rules
cp -r pkg/sql/schemachanger/scplan/internal/rules/current \
      pkg/sql/schemachanger/scplan/internal/rules/<OLD_DIR>

# b) Rename package in all Go files
find pkg/sql/schemachanger/scplan/internal/rules/<OLD_DIR> -name "*.go" \
     -exec sed -i '' 's/^package current$/package <OLD_DIR>/' {} \;
```

c) Update `<OLD_DIR>/BUILD.bazel`: change `name`, `importpath`, test `name` and `embed`
   from `current` / `current_test` → `<OLD_DIR>` / `<OLD_DIR>_test`.

d) Update `current/helpers.go`:
```go
rulesVersion    = "-<NEW_VER>"
rulesVersionKey = clusterversion.<NEW>
```

e) Update `scplan/plan.go` — add import and entry to `rulesForReleases`:
```go
{activeVersion: clusterversion.<OLD>, rulesRegistry: <OLD_DIR>.GetRegistry()},
```
(Insert between `Latest` and the previous release entry, keeping descending order.)

## Step 8 — Regenerate

```bash
./dev gen bazel

./dev test pkg/sql/schemachanger/scplan/internal/rules/... --rewrite
./dev test pkg/cli -f DeclarativeRules --rewrite
./dev generate
```

Manually update `pkg/testutils/release/cockroach_releases.yaml` — add an entry for `<NEW_VER>`:

```yaml
"<NEW_VER>":
  predecessor: "<PREV_STABLE>"
```

**Why `<PREV_STABLE>` and not `<OLD_VER>`?** The `predecessor` field tracks the last
*released* stable version before this one. At M.1 time, `<OLD_VER>` has not yet shipped
as a stable release — it is still an in-development version on `release-<OLD_VER>`. So
both `<OLD_VER>` and `<NEW_VER>` share the same last stable predecessor: `<PREV_STABLE>`.
This is intentional and expected; it will look like N → N-2 rather than N → N-1, which
can surprise reviewers.

**IMPORTANT:** After `./dev gen bazel`, remove any `upgradeinterlockccl_test` lines
from `pkg/BUILD.bazel` before committing. See MEMORY.md.

## Step 9 — Verify

```bash
./dev test pkg/clusterversion pkg/roachpb
./dev test pkg/sql/schemachanger/scplan/internal/rules/...
./dev test pkg/cli -f DeclarativeRules
./dev build short
```

## Expected files changed (~55-60)

- `pkg/clusterversion/cockroach_versions.go`
- `pkg/roachpb/version.go`, `version_test.go`
- `pkg/sql/catalog/systemschema/system.go`
- `pkg/upgrade/upgrades/upgrades.go`
- `pkg/build/version.txt`
- `pkg/testutils/release/cockroach_releases.yaml`
- `pkg/sql/schemachanger/scplan/plan.go`
- `pkg/sql/schemachanger/scplan/internal/rules/current/helpers.go`
- `pkg/sql/schemachanger/scplan/internal/rules/current/testdata/deprules`
- `pkg/sql/schemachanger/scplan/internal/rules/<OLD_DIR>/` (new directory, ~35 files)
- `pkg/cli/testdata/declarative-rules/invalid_version`
- Various generated docs and BUILD.bazel files

## Commit and PR

**Title:** `clusterversion: move to <NEW_VER> version`

**Body template:**
```
This change advances the current release series version on master from
<OLD_VER> to <NEW_VER>, following the standard M.1 process after forking the
release-<OLD_VER> branch. This allows the addition of new upgrade gates for
version <NEW_VER> and prepares master for the next development cycle.
```

> **Common mistake:** The commit message should say "from `<OLD_VER>` to `<NEW_VER>`"
> (i.e., from the *frozen* version to the *new* dev version). Do NOT say "from
> `<PREV>` to `<OLD_VER>`" — that would be off by one. The frozen key `<OLD>` means
> `<OLD_VER>` is done; the new development target is `<NEW_VER>`.

## CI failures

All M.1 CI failures are Type 2 (testdata updates, no code changes).
See `failures/m1_failures.md` for fix commands.
