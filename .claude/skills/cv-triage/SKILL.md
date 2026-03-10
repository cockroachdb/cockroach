# CV Triage

Diagnose a CockroachDB CI test failure from a clusterversion release task (M.1, M.2, M.3, M.4, R.1, R.2) and output the exact fix command.

## Trigger

Invoked as `/cv-triage` or when the user pastes CI error output and asks "what does this mean" or "how do I fix this".

## Input

The user provides CI error output — either:
- Raw log lines pasted into the chat
- A path to a log file
- An EngFlow/TeamCity URL

Read any provided log file or URL. Then diagnose.

## Steps

### 1. Identify the task context

Ask the user which release task they were doing if not clear from context:
- M.1: Bump Current Version (master branch)
- M.2: Enable Mixed-Cluster Logic Tests (master branch)
- M.3: Enable Upgrade Tests (master branch)
- M.4: Bump MinSupported Version (master branch)
- R.1: Prepare for Beta (release branch)
- R.2: Mint Release (release branch)

### 2. Classify the failure

**Type 2 (Most common — test expectation update only, NO code change):**

Look for these patterns in the error output:

| Pattern | Type | Fix |
|---------|------|-----|
| `"output didn't match expected"` + version number diff | 2A | sed or manual update in testdata file |
| `systemDatabaseSchemaVersion` shows wrong major/minor/internal | 2B | Update crdb_internal_catalog testdata |
| `"Unexpected hash value for system"` or `"Unexpected hash value for tenant"` | 2C | `./dev test pkg/sql/catalog/bootstrap -f TestInitialValuesToString --rewrite` |
| `"bootstrap_system"` or `"bootstrap_tenant"` diff | 2C | `./dev test pkg/sql/catalog/systemschema_test --rewrite` |
| `declarative-rules` or `deprules` mismatch | 2D | `./dev test pkg/cli -f DeclarativeRules --rewrite` |
| `"no such config"` or `"unknown config name"` | 2E | Logic test file still references removed config |
| `"empty LogicTest directive"` | 2F | `sed -i '' '/^# LogicTest:$/d' <file>` |
| `"nonexistent" local-mixed-XX.X` config | 2G | File still references removed local-mixed config |

**Type 1 (Rare — may require production code change):**

Look for these patterns:
- Test was added AFTER the previous M.1/M.4 PR (check with `git log -S "TestName"`)
- Logic error in version gate logic (not just output mismatch)
- Multiple different tests failing in a way that suggests a missing constant

**Ambiguous — needs investigation:**
- Mixedversion test failure: Check if production code was accidentally modified
- Panic or nil pointer in unrelated package: Likely pre-existing or unrelated

### 3. Output diagnosis

For **Type 2** failures, output:
1. Failure type label (e.g., "Type 2C — bootstrap hash mismatch")
2. Exact fix command (copy-pasteable)
3. Files to check afterward

For **Type 1** failures, output:
1. Failure type label (e.g., "Type 1 — possible code change needed")
2. Investigation commands to confirm
3. Reference: which previous task PR to compare against
4. Warning: Do NOT modify production code until confirmed

For **ambiguous** failures, output:
1. What was observed
2. How to narrow it down (is it pre-existing? run `git bisect`? check PR history?)

### 4. Task-specific failure references

For deeper context, read the appropriate failures file:

- M.1 failures: `pkg/clusterversion/runbooks/failures/m1_failures.md`
- M.2 failures: `pkg/clusterversion/runbooks/failures/m2_failures.md`
- M.3 failures: `pkg/clusterversion/runbooks/failures/m3_failures.md`
- M.4 failures: `pkg/clusterversion/runbooks/failures/m4_failures.md`

These are in `$GOPATH/src/github.com/cockroachdb/cockroach/pkg/clusterversion/runbooks/failures/`.

## Output Format

Keep the output short and actionable. Structure:

```
**Diagnosis:** [Type 2C — bootstrap hash mismatch]

**Root cause:** The system bootstrap hash changed because [reason from logs].

**Fix:**
./dev test pkg/sql/catalog/bootstrap -f TestInitialValuesToString --rewrite

**After fixing, verify:**
./dev test pkg/sql/catalog/bootstrap -f TestInitialValuesToString
```

Do NOT output long explanations. The user needs to act, not read.

## Common Misclassifications to Avoid

- A test that references a version number in its *name* (e.g., `TestUpgradeFrom25_3`) failing is NOT necessarily about version numbers — check if it's actually a test data mismatch.
- A "missing binary" error in logictest is almost always a REPOSITORIES.bzl issue (M.3 fixture PR not complete), not a code bug.
- A panic in `mixedversion` is a RED FLAG — check whether production code was accidentally changed.
