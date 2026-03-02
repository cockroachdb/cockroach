# M.3 CI Failure Reference

This file covers failures that appear during the M.3 (Enable Upgrade Tests) task.
Run `validate-m3.sh` for the code PR (PR 2) before pushing. Consult this file
when issues arise during fixture generation or CI.

---

## gceworker Issues

| Error | Fix |
|-------|-----|
| SSH connection fails (exit 255) | `./scripts/gceworker.sh start` (retry), or destroy & recreate |
| "bazel: command not found" | Run `./dev doctor` to install |
| "COCKROACH_DEV_LICENSE not set" | `export COCKROACH_DEV_LICENSE="<license>"` |
| Session disconnects mid-run | Re-export `FIXTURE_VERSION` and `COCKROACH_DEV_LICENSE`, re-run |

**Reminder:** Always run `./scripts/gceworker.sh update-firewall` BEFORE
`./scripts/gceworker.sh create`, or the SSH connection will fail.

---

## Fixture Generation Issues

| Error | Fix |
|-------|-----|
| "license required" | Verify: `echo $COCKROACH_DEV_LICENSE` |
| "roachprod cluster exists" | `./bin/roachprod destroy local` |
| Fixtures 0 bytes or >10MB | Compare sizes with previous version's fixtures; regenerate |

**Expected fixture sizes:** Each `checkpoint-vX.Y.tgz` should be ~3-5 MB.

---

## `./dev gen bazel` Fails Silently

Symptom: the command exits with `ERROR: exit status 1` but prints no useful
error message, and `pkg/sql/logictest/tests/cockroach-go-testserver-X.Y/`
is not created.

Create the two files manually:

**1. Compute `configIdx`** — count 0-indexed position of the new config in
the `LogicTestConfigs` slice in `logictestbase.go`:

```bash
grep -n "Name:" pkg/sql/logictest/logictestbase/logictestbase.go | grep -n ""
# Find "cockroach-go-testserver-X.Y" in the output — its left-hand number
# minus 1 is the configIdx (the grep -n "" adds 1-based line counts).
```

For example, if it appears as line 24 in the grep output, configIdx = 23.

**2. Create the directory and files:**

```bash
mkdir -p pkg/sql/logictest/tests/cockroach-go-testserver-X.Y
```

Copy `pkg/sql/logictest/tests/cockroach-go-testserver-25.4/BUILD.bazel`
and make two substitutions:

- `cockroach-go-testserver-25_4_test` → `cockroach-go-testserver-X_Y_test`
  (dots become underscores in the Bazel target name)

Copy `pkg/sql/logictest/tests/cockroach-go-testserver-25.4/generated_test.go`
and make two substitutions:

- `package testcockroach_go_testserver_254` → `package testcockroach_go_testserver_XY`
  (dots and underscores dropped; e.g. 26.1 → `261`)
- `const configIdx = 22` → `const configIdx = <your computed value>`

The list of `TestLogic_*` functions should match the 25.4 file exactly —
they reflect which logic test files have `cockroach-go-testserver` in their
header, which rarely changes between M.3 cycles.

**3. Verify:**

```bash
ls pkg/sql/logictest/tests/cockroach-go-testserver-X.Y/
# Should show: BUILD.bazel  generated_test.go
```

---

## CI Failures After PR 2 (Code Changes)

| Error | Fix |
|-------|-----|
| `TestLogic_mixed_version_bootstrap_tenant` fails with descriptor diffs | System table descriptors evolve between versions. Exclude differing keys in the test's `WHERE` clause (see full runbook for details) |
| `TestDeclarativeRules` fails with version mismatch | Forgot to run `validate-m3.sh` — run `./dev test pkg/cli -f=TestDeclarativeRules --rewrite` |

---

## REPOSITORIES.bzl Issues

| Error | Fix |
|-------|-----|
| Tool removed a version still needed | `git show HEAD^:pkg/sql/logictest/REPOSITORIES.bzl \| grep -A 4 "X.Y.Z"` then restore manually |
| Nightly build fails "missing binary" | Verify all active testserver versions are present in `REPOSITORIES.bzl` |

**Pattern to maintain:** Keep N-2, N-1, and N release binaries in `REPOSITORIES.bzl`
until their testserver configs are removed in a future M.4.

**Check active testserver configs:**
```bash
grep "cockroach-go-testserver-" pkg/sql/logictest/logictestbase/logictestbase.go | grep "Name:"
grep -E "^\s+\(\"25\.[0-9]" pkg/sql/logictest/REPOSITORIES.bzl
```
