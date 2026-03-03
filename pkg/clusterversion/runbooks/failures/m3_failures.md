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

`./dev gen bazel` updates **four things** when a new testserver config is added.
The manual fallback must replicate all four, not just the first.

---

### Step 1: Compute `configIdx`

Count the 0-indexed position of the new config in the `LogicTestConfigs` slice
in `logictestbase.go`:

```bash
grep -n "Name:" pkg/sql/logictest/logictestbase/logictestbase.go | grep -n ""
# Find "cockroach-go-testserver-X.Y" in the output.
# Its left-hand line number minus 1 is configIdx.
# (grep -n "" adds 1-based counts to the inner grep's output)
```

For example, if it appears as line 24 in the grep output, `configIdx = 23`.

---

### Step 2: Create the new testserver directory

```bash
mkdir -p pkg/sql/logictest/tests/cockroach-go-testserver-X.Y
```

Copy `pkg/sql/logictest/tests/cockroach-go-testserver-25.4/BUILD.bazel`
and substitute:

- `cockroach-go-testserver-25_4_test` → `cockroach-go-testserver-X_Y_test`
  (dots become underscores in the Bazel target name)

Copy `pkg/sql/logictest/tests/cockroach-go-testserver-25.4/generated_test.go`
and substitute:

- `package testcockroach_go_testserver_254` → `package testcockroach_go_testserver_XY`
  (dots and underscores dropped; e.g. 26.1 → `261`)
- `const configIdx = 22` → `const configIdx = <your computed value>`

The list of `TestLogic_*` functions should match the 25.4 file exactly —
they reflect which logic test files have `cockroach-go-testserver` in their
header, which rarely changes between M.3 cycles.

---

### Step 3: Bump configIdx in all configs that come after the new one

Inserting a new config shifts every subsequent config's index up by 1.
Find and increment all `generated_test.go` files with `configIdx` >= your new
config's index (excluding the new directory you just created):

```bash
NEW_IDX=<your computed configIdx>

grep -rl "const configIdx" pkg/sql/logictest/tests/ \
  | grep -v "cockroach-go-testserver-X.Y" \
  | while read f; do
      idx=$(grep "const configIdx" "$f" | grep -o '[0-9]*')
      if [ "$idx" -ge "$NEW_IDX" ]; then
        new_idx=$((idx + 1))
        sed -i '' "s/const configIdx = $idx/const configIdx = $new_idx/" "$f"
        echo "Updated $f: $idx -> $new_idx"
      fi
    done
```

**Verify:** `grep -r "const configIdx" pkg/sql/logictest/tests/ | sort -t= -k2 -n`
— no two files should share the same value.

---

### Step 4: Add the new target to `pkg/BUILD.bazel`

`pkg/BUILD.bazel` has two lists that need the new target: `ALL_TESTS` and
`GO_TARGETS`. Both are alphabetically sorted. Add the new line after the
previous testserver version in each:

```bash
# Find the insertion point in both lists
grep -n "cockroach-go-testserver" pkg/BUILD.bazel
```

Add (in both `ALL_TESTS` and `GO_TARGETS`, after the previous testserver line):

```
    "//pkg/sql/logictest/tests/cockroach-go-testserver-X.Y:cockroach-go-testserver-X_Y_test",
```

Note: dots become underscores in the target name suffix (e.g. `26.1` → `26_1`).

---

### Step 5: Verify all four changes

```bash
# New directory created
ls pkg/sql/logictest/tests/cockroach-go-testserver-X.Y/
# → BUILD.bazel  generated_test.go

# No duplicate configIdx values
grep -r "const configIdx" pkg/sql/logictest/tests/ | sort -t= -k2 -n

# pkg/BUILD.bazel has the new target in both lists
grep "cockroach-go-testserver-X.Y" pkg/BUILD.bazel | wc -l
# → 2
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
grep -E "^\s+\(\"[0-9]" pkg/sql/logictest/REPOSITORIES.bzl
```
