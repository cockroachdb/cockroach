---
name: reduce-unoptimized-query-oracle
description: Reduce an unoptimized-query-oracle test failure log to the simplest possible reproduction case. Use when you have unoptimized-query-oracle*.log files from a failed roachtest and need to find the minimal SQL to reproduce the bug.
---

# Reduce Unoptimized Query Oracle Test Failure

Reduce an unoptimized-query-oracle test failure log to the simplest possible reproduction case.

The unoptimized-query-oracle test runs a series of random SQL statements to
create a random dataset, and then executes a random "Query of Interest" twice,
with different optimization settings. If the two executions return different
results, it indicates a bug in CockroachDB.

## When to Use

Use this skill when:
- You have a test failure with SQL statement logs (e.g., `unoptimized-query-oracle*.log`)
- You need to find the minimal SQL to reproduce a bug
- The test compares optimized vs unoptimized query results, or produces an internal error

## Step 1: Locate the Failure Artifacts

Ask the user where the artifacts directory is.

Find the relevant files in the artifacts directory:
- **Full log**: `unoptimized-query-oracle*.log` (the SQL statements that led to failure)
- **Failure log**: `unoptimized-query-oracle*.failure.log` (the specific failure details)
- **Cockroach log**: `logs/1.unredacted/cockroach.log` (contains the git commit)

Extract the git commit from cockroach.log:
```bash
grep "binary: CockroachDB" <artifacts>/logs/1.unredacted/cockroach.log
```
Look for the commit hash in the version string (e.g., `cb94db961b8f55e3473f279d98ae90f0eeb0adcb`).
Also check whether this was a test build.

Determine if this is a multi-region test by checking:
- The test name (e.g., `seed-multi-region` indicates multi-region)
- The presence of `\connect` lines in the log file (indicates multi-region)

## Step 2: Check Out and Build

```bash
git checkout <commit-hash>
./dev build short
```

Use `./dev build short -- --crdb_test` if this was a test build.

## Step 3: Initial Reproduction

Determine the correct demo command based on test type:
- **Multi-region test**: Use `--nodes=9`
- **Single-region test**: Omit `--nodes` flag

```bash
./cockroach demo --multitenant=false --nodes=9 --insecure --set=errexit=false --no-example-database --format=tsv -f <log-file>
```

**Check the output for:**
1. `internal error` or assertion failure - Note the error message for the reduce step
2. Or, different results between the two executions of the "Query of Interest"
   (which is the randomly generated query repeated twice near the end of the
   log, wrapped in various SET and RESET staements). These different results
   could take the form of different result sets, or could also be an error in
   one case and no error in the other case. This is a "oracle" failure.
3. Or, a panic.

**IMPORTANT:** Many failures are nondeterministic. If no error appears on the
first run, try up to 10 times before concluding it doesn't reproduce.

It can be helpful at this point to compare the output with the
`unoptimized-query-oracle*.failure.log` which should show the failure from the
original test run. (Though sometimes this file is missing information when there
is a panic or internal error.)

If it fails to reproduce after 10 times, pause here and report to the user that
the failure cannot be reproduced, and show the command that was tried. The user
might have additional instructions.

If it looks like it reproduces, it's time to move on to the next step.

## Step 4: Use the Reduce Tool

Build the reduce tool:
```bash
./dev build reduce
```

### Prepare the Log File

For multi-region tests, remove `\connect` lines (they cause syntax errors):
```bash
grep -v '^\\connect' <original-log> > <cleaned-log>
```

### Run Reduce

**For internal errors/assertion failures:**
```bash
bin/reduce -contains "<error-regex>" -multi-region -chunk 25 -v -file <cleaned-log> 2>&1 | tee reduce-output.log
```
Use a distinctive part of the error message as the regex (e.g., `"nil LeafTxnInputState"`).

**For oracle failures (different results):**
```bash
bin/reduce -unoptimized-query-oracle -multi-region -chunk 25 -v -file <cleaned-log> 2>&1 | tee reduce-output.log
```

The reduce tool might take up to an hour to run.

### Extract the Reduced SQL

The reduce tool outputs progress lines followed by the final SQL. Extract just the SQL:
```bash
grep -A1000 "^reduction: " reduce-output.log | tail -n +2 > reduced.sql
```

**IMPORTANT:** Immediately save a backup of the reduce output before manual simplification:
```bash
cp reduced.sql reduced_original.sql
```
This provides a recovery point if the working file gets corrupted during simplification.

If the reduce tool fails to reproduce, pause here and report this to the
user. They might have additional instructions.

## Step 5: Create Test Script and Determine Reproduction Rate

**IMPORTANT:** Many bugs are nondeterministic. Before manual simplification,
create a reusable test script and determine the reproduction rate.

Create a test script (adjust `--nodes` and error pattern as needed):
```bash
cat > test_repro.sh << 'EOF'
#!/bin/bash
# Test if reduced_v2.sql reproduces the error (exits on first success, up to 10 attempts)
for i in {1..10}; do
  if ./cockroach demo --multitenant=false --nodes=9 --insecure \
     --set=errexit=false --no-example-database --format=tsv \
     -f reduced_v2.sql 2>&1 | grep -q "<error-pattern>"; then
    echo "Run $i: REPRODUCED"
    exit 0
  else
    echo "Run $i: no error"
  fi
done
echo "FAILED"
EOF
chmod +x test_repro.sh
```

Run the test 10 times to determine the reproduction rate:
```bash
count=0; success=0
for i in {1..10}; do
  if ./cockroach demo --multitenant=false --nodes=9 --insecure --set=errexit=false --no-example-database --format=tsv -f reduced.sql 2>&1 | grep -q "<error-pattern>"; then
    echo "Run $i: REPRODUCED"
    ((success++))
  else
    echo "Run $i: no error"
  fi
  ((count++))
done
echo "Repro rate: $success/$count"
```

This rate determines how many attempts you need when testing simplifications:
- 100% rate: Single attempt sufficient
- 50% rate: 2-3 attempts usually sufficient
- 10% rate: Need ~10 attempts to be confident
- <5% rate: May need 20+ attempts

If the reduced SQL fails to reproduce after 10 attempts, pause here and report
this to the user. They might have additional instructions.

## Step 6: Manual Simplification

Now iteratively simplify the SQL while maintaining reproduction.

**CRITICAL:** For nondeterministic failures, you MUST test each simplification
with enough attempts based on the repro rate from Step 5. A single failed
attempt does NOT mean the simplification broke the repro - it may just be
nondeterminism.

### Workflow for Each Simplification

1. Copy `reduced.sql` to `reduced_v2.sql`
2. Make ONE small change to `reduced_v2.sql`
3. Run `./test_repro.sh` (which tests `reduced_v2.sql`)
4. If it reproduces: Copy `reduced_v2.sql` to `reduced.sql`, continue simplifying
5. If it doesn't reproduce after enough attempts: Discard `reduced_v2.sql`, try a different change

This workflow avoids needing to restore files - you always keep the last working
version in `reduced.sql`.

**IMPORTANT:** Run copy, edit, and test as separate bash commands (not chained with `&&`).
This reduces the number of permission checks.

### What to Try Removing (in rough order)

1. **Query projections and aggregations** - Simplify SELECT list to just essential columns
2. **Query predicates** - Simplify WHERE clause
3. **Indexes** - Try removing secondary indexes
4. **Query joins** - Simplify WHERE clause
5. **Columns from CREATE TABLE** - Remove columns not referenced in the failing query
6. other SQL simplifications

For "oracle" failures, when editing the Query of Interest, be sure to edit
*BOTH* copies of the Query of Interest so that they are identical. Otherwise it
won't be an apples-to-apples comparison when diffing the result sets.

### Common Required Elements

These often cannot be removed:
- Specific RESET/SET sequences for optimizer settings, such as distsql and vectorize
- `SET testing_optimizer_disable_rule_probability` (affects query plan selection)
- Certain indexes (affect query plans)
- Multi-node setup (`--nodes=9`) for distributed query bugs
- `CREATE STATISTICS` statements (affect query planning)

### Backtracking

If a change breaks reproduction:
1. Discard `reduced_v2.sql` (don't copy it to `reduced.sql`)
2. Verify `reduced.sql` still reproduces. If it doesn't, this means the repro is
   nondeterministic. (It might have started out nondeterministic, or might have
   become nondeterministic over the course of simplification.) Try reproducing
   it 10 times and note the new repro rate. Use the new repro rate to adjust the
   number of repro attempts during each simplification step going forward.
3. Try a DIFFERENT simplification

Never continue simplifying from a broken state.

If you get stuck (i.e. cannot reproduce again after backtracking), stop and
report to the user with the exact command you were trying.

## Step 7: Final Verification

After about 20 minutes of simplification, or if there are no more
simplifications, it's time to stop.

1. Run reproduction 10+ times to confirm stability and determine final repro rate
2. Document the minimal reproduction steps
3. Note which elements were required vs optional

## Output

The final `reduced.sql` should be a minimal SQL script that:
- Reproduces the bug when run with the appropriate demo command
- Contains only the essential schema, data, and queries
- Is small enough to include in a bug report

## Example Bug Report Format

```
**Minimal Reproduction:**

Command:
./cockroach demo --multitenant=false --nodes=9 --insecure --set=errexit=false --no-example-database --format=tsv -f repro.sql

repro.sql:
[paste minimal SQL here]

Error:
[paste error message and relevant stack trace]

Repro rate: ~X% (may need multiple attempts)
```
