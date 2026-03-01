# M.2 CI Failure Reference

This file covers failures that appear during or after the M.2 (Enable Mixed-Cluster
Logic Tests) PR. Run `validate-m2.sh <version>` first — it catches bootstrap
integrity and config wiring issues before CI. Consult this file when CI still fails.

---

## Common Errors

#### Error: "Bootstrap data files not found"

**Cause:** Forgot to copy bootstrap files from release branch.

**Fix:**
1. Checkout the release branch (e.g., `release-25.4`)
2. Run `./dev build sql-bootstrap-data && bin/sql-bootstrap-data`
3. Copy the 4 generated files to your working branch

#### Error: "undefined: v25_4_system_keys"

**Cause:** go:embed variables not added to `initial_values.go`.

**Fix:** Add the 4 go:embed variable declarations at the end of `initial_values.go`.

#### Error: "clusterversion.V25_4 undefined"

**Cause:** M.1 not completed or working on wrong base branch.

**Fix:** Ensure your branch is based on the M.1 PR branch that has V25_4 defined.

#### Error: "Test files not generated"

**Cause:** `./dev gen bazel` not run or failed.

**Fix:**
1. Run `./dev gen bazel` again
2. Check for errors in the output
3. Ensure `logictestbase.go` changes are saved

#### Error: "SHA256 hash mismatch"

**Cause:** Bootstrap files corrupted or from wrong version.

**Fix:**
1. Re-generate bootstrap data from a clean checkout of the release branch
2. Verify you're on the correct release branch
3. Do not manually edit the bootstrap `.keys` files

#### Error: Logic test fails with "bootstrap version not found"

**Cause:** Bootstrap data not embedded correctly.

**Fix:**
1. Verify `BUILD.bazel` has `embedsrcs` entries for all 4 new files
2. Verify `initial_values.go` has go:embed variables and map entry
3. Run `./dev gen bazel` to update build files

#### Error: TestLogic_cross_version_tenant_backup fails — "could not locate file cockroach-v25.4.0-rc.1"

**Cause:** `cockroach-go-testserver-25.4` config was added in M.2, but it belongs in M.3.

The `cockroach-go-testserver-25.4` config requires a predecessor binary
(e.g., `v25.4.0-rc.1`) in `REPOSITORIES.bzl`. Those binaries are only added
in M.3, after the first RC is published.

**Fix:**
1. Remove `cockroach-go-testserver-25.4` config from `logictestbase.go`
2. Remove it from the `cockroach-go-testserver-configs` set
3. Delete `pkg/sql/logictest/tests/cockroach-go-testserver-25.4/`
4. Remove its visibility entry from `pkg/sql/logictest/BUILD.bazel`
5. Run `./dev gen bazel`

**What to add in M.2:** `local-mixed-25.4` ✅
**What NOT to add in M.2:** `cockroach-go-testserver-25.4` ❌ (save for M.3)

**Reference:** `cockroach-go-testserver-25.3` was added in M.3 (commit b4a7d05d8e8), not M.2.

---

## Verifying Which skipif Directives Are Necessary (Optional)

The runbook says to add `skipif config local-mixed-25.4` only when tests actually
fail. If you added them proactively, verify they're all necessary:

```bash
# On a throwaway branch, remove the new skipif directives
git checkout -b verify-skipif-necessity

# Revert skipif changes in the affected files
git show master:pkg/sql/logictest/testdata/logic_test/crdb_internal_catalog > \
  pkg/sql/logictest/testdata/logic_test/crdb_internal_catalog

# Run the config and note what fails
./dev testlogic --config=local-mixed-25.4

# Add back only the directives for tests that actually failed
```

**Example:** PR #161136 (M.2 26.1) proactively added skipif to 6 files.
Verification PR #161473 showed only `crdb_internal_catalog` needed it — the other
5 were unnecessary and would reduce test coverage.

---

## Manual Reference PR Comparison

The `compare-with-reference-pr.sh` script handles this automatically. If you need
finer control (PR #156183 combined M.1 and M.2, so you need to filter):

```bash
git diff --name-only <base-branch> | sort > /tmp/current_m2_files.txt

gh pr view 156183 --json files --jq '.files[].path' | \
  grep -E "(bootstrap/data|initial_values|BUILD.bazel|logictestbase|local-mixed|cockroach-go-testserver)" | \
  sort > /tmp/previous_m2_files.txt

echo "=== Only in current (investigate) ==="
comm -13 /tmp/previous_m2_files.txt /tmp/current_m2_files.txt

echo "=== Only in reference (might be missing) ==="
comm -23 /tmp/previous_m2_files.txt /tmp/current_m2_files.txt
```

**Expected differences:** cockroach-go-testserver-25.4 files (new this version),
and version-specific file names (25.4 vs 25.3). Everything else should follow
the same pattern.
