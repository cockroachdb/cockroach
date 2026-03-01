# M.4 CI Failure Reference

This file covers failures that appear during the M.4 (Bump MinSupported) PR.
Run `validate-m4.sh` before pushing. Consult this file when CI still fails.

---

## Quick Reference

| Error | Quick Fix |
|-------|-----------|
| `TestMinimumSupportedFormatVersion` fails | Check `pkg/storage/pebble.go` — `MinimumSupportedFormatVersion` must match `pebbleFormatVersionMap` for new MinSupported |
| Nightly build fails "nonexistent local-mixed" | Update `build/teamcity/cockroach/nightlies/sqllogic_corpus_nightly_impl.sh` |
| `panic: unknown config name` | Logic test files still reference old config — re-run the sed commands from the runbook |
| `empty LogicTest directive` | `sed -i '' '/^# LogicTest:$/d' <file>` |
| Duplicate statement errors | Manually review files that had `onlyif` — see Error 7 below |

---

## Detailed Errors

#### Error 1: Bazel generation failure after removing config

**Error:** `panic: unknown config name local-mixed-25.2`

**Cause:** Test files still reference `local-mixed-25.2` after the config was removed from `logictestbase.go`.

**Fix:**
```bash
grep -r "local-mixed-25\.2" pkg/sql/logictest/testdata/logic_test/ \
     pkg/ccl/logictestccl/testdata/logic_test/
# Remove with the targeted sed commands from the runbook (Commit 5)
```

#### Error 2: Accidentally removed newlines with aggressive regex

**Error:** File corruption — lines concatenated (e.g., `# LogicTest: !local-legacy-schema-changer# A basic sanity check...`)

**Cause:** Using global whitespace replacement like `perl -pi -e 's/\s*local-mixed-25\.2\s*/ /g; s/\s+$//; s/\s+/ /g'`

**Fix:** Use targeted sed that preserves structure:
```bash
# WRONG (removes newlines):
perl -pi -e 's/\s*local-mixed-25\.2\s*/ /g; s/\s+$//; s/\s+/ /g'

# RIGHT:
sed -i '' \
  -e '/^# LogicTest:/s/ !*local-mixed-25\.2//g' \
  -e '/^skipif config local-mixed-25\.2$/d' \
  -e '/^onlyif config local-mixed-25\.2$/d'
```

#### Error 3: Empty LogicTest directive

**Error:** `empty LogicTest directive` during bazel generation.

**Cause:** Files had headers like `# LogicTest: !local-mixed-25.2` which became `# LogicTest:` after removal.

**Fix:**
```bash
grep -l "^# LogicTest:$" pkg/sql/logictest/testdata/logic_test/* \
     pkg/ccl/logictestccl/testdata/logic_test/*
sed -i '' '/^# LogicTest:$/d' <files>
```

#### Error 4: Nightly build failure — nonexistent local-mixed directory

**Error:** `references nonexistent local-mixed-X.Y logictest directory`

**Cause:** Forgot to update `build/teamcity/cockroach/nightlies/sqllogic_corpus_nightly_impl.sh`.

**Real-world occurrence:** Broke the nightly in PR #158225, required follow-up fix in commit 4bc710b4667 and issue #158741.

**Fix:**
```bash
# Around line 84 of sqllogic_corpus_nightly_impl.sh:
# Before:
for config in local-mixed-25.2 local-mixed-25.3; do
# After:
for config in local-mixed-25.3; do
```

This file is in the runbook's "Expected Files Modified" section — always check it.

#### Error 5: Committed local files accidentally

**Error:** `.claude/`, `PLAN_*.md`, or other local files appear in the commit.

**Fix:**
```bash
git reset HEAD~ --soft
git reset HEAD .claude/settings.local.json PLAN_*.md *.local.md
git commit -m "..."
```

#### Error 6: Missing file deletions in commit

**Error:** Deleted directories don't appear in the commit.

**Cause:** Used `git add` without `-u` or `-A`, which don't capture deletions.

**Fix:**
```bash
git add -A  # Captures modifications AND deletions
```

#### Error 7: "relation already exists" / duplicate statement errors

**Error:**
```
expected success, but found
(42P07) relation "test.public.t1" already exists
```

**Cause:** Removed `onlyif config local-mixed-25.2` directive but didn't remove
the statement it was guarding. That statement now runs unconditionally AND there's
an unguarded duplicate later in the file.

**How to identify:**
```bash
# Find removed onlyif lines in the diff
git diff pkg/sql/logictest/testdata/logic_test/ | grep "^-onlyif config.*local-mixed-25\.2"

# For each file, manually check if guarded statements should also be removed
```

**Fix:**
1. Read the test file — find what the removed `onlyif` was guarding (usually 3-10 lines after it)
2. If it's a duplicate of an unguarded statement elsewhere in the file: remove the guarded block
3. If removing it leaves an empty subtest or user block, remove those too

**Prevention:** When running sed to remove directives, immediately grep the diff
for removed `onlyif` lines and manually review each file.

#### Error 8: Incorrect pebble format version after rebase

**Symptom:**
```go
// WRONG — V25_3 value changed during rebase:
clusterversion.V25_3: pebble.FormatTableFormatV6  // wrong value

// CORRECT — only the old version's entry was removed:
clusterversion.V25_3: pebble.FormatValueSeparation  // unchanged
```

**Cause:** During rebase, git auto-merged changes to `pebbleFormatVersionMap`
incorrectly. The commit removing `local-mixed-25.2` should only delete the
V25_2 entry, not change V25_3's value.

**How to identify:**
```bash
git show <commit-sha>:pkg/storage/pebble.go | grep -A 4 "pebbleFormatVersionMap ="
git show master:pkg/storage/pebble.go | grep -A 4 "pebbleFormatVersionMap ="
```

**Fix:**
```bash
git rebase -i <base-commit>
# Mark the problematic commit for 'edit', fix the file, then:
git add pkg/storage/pebble.go
git commit --amend --no-edit
git rebase --continue
```

**Prevention:** After any rebase, check `pkg/storage/pebble.go` carefully.
The only change should be a deletion of the old version's line.
Reference PR #147634 for the expected pattern.
