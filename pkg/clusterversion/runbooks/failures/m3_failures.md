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
