---
name: reduce-unoptimized-query-oracle
description: Reduce an unoptimized-query-oracle test failure log to the simplest possible reproduction case. Use when you have unoptimized-query-oracle*.log files from a failed roachtest and need to find the minimal SQL to reproduce the bug.
disable-model-invocation: true
---

# Reduce Unoptimized Query Oracle Test Failure

Reduce an unoptimized-query-oracle test failure log to the simplest possible
reproduction case.

The unoptimized-query-oracle roachtest runs a series of random SQL statements to
create a random dataset, and then executes a random "Query of Interest" twice,
with different optimization settings. If the two executions return different
results, it indicates a bug in CockroachDB.

## When to Use

Use this skill when:
- You have a test failure from the unoptimized-query-oracle roachtest.
- You need to find the minimal SQL to reproduce the test failure.

## Step 1: Locate artifacts

**Ask the user where the artifacts directory is.**

Find the relevant files in the artifacts directory:
- **Test parameters**: `params.log` (the parameters from the roachtest)
- **Test log**: `test.log` (the log from the roachtest)
- **Failure log**: `failure*.log` (the failure log from the roachtest)
- **Full SQL log**: `unoptimized-query-oracle*.log` (the SQL statements that led to failure)
- **Query of interest log**: `unoptimized-query-oracle*.failure.log` (containing
  the query of interest and possibly more information about the failure)
- **Cockroach log**: `logs/1.unredacted/cockroach.log` or
  `logs/unredacted/cockroach.log` (contains the git commit)

## Step 2: Determine test configuration

Determine the git commit from `cockroach.log`:
```bash
grep "binary: CockroachDB" cockroach.log
```
Look for the commit hash in the version string (e.g., `cb94db961b8f55e3473f279d98ae90f0eeb0adcb`).

Determine if runtime assertions are enabled by checking for:
- `"runtimeAssertionsBuild": "true"` in `params.log`
- or `Runtime assertions enabled` in `test.log`

Determine if metamorphic settings apply by looking for:
- lines like these in `params.log`:
  ```
  "metamorphicBufferedSender": "true",
  "metamorphicWriteBuffering": "true",
  ```
- or lines like these in `test.log`:
  ```
  metamorphically setting "kv.rangefeed.buffered_sender.enabled" to 'true'
  metamorphically setting "kv.transaction.write_buffering.enabled" to 'true'
  ```

Determine environment variables from the beginning of `cockroach.log`:
```bash
grep -A10 "using local environment variables:" cockroach.log
```

Important environment variables include:
- `COCKROACH_INTERNAL_CHECK_CONSISTENCY_FATAL`
- `COCKROACH_INTERNAL_DISABLE_METAMORPHIC_TESTING`
- `COCKROACH_RANDOM_SEED`
- `COCKROACH_TESTING_FORCE_RELEASE_BRANCH`
But there might be more important environment variables, so best to get all of
them.

Determine if this is a multi-region test or single-region test by checking:
- the test name (e.g., `seed-multi-region` in `test.log` indicates multi-region)
- or the presence of `\connect` lines in the full SQL log
If both of these are missing, it's a single-region test.

## Step 3: Check Out and Build

For a normal build use:
```bash
git checkout <commit-hash>
./dev build short
```

If runtime assertions were enabled, use a test build instead:
```bash
git checkout <commit-hash>
./dev build short -- --crdb_test
```

**Note:** Only build libgeos if the reproduction uses geospatial functions (BOX2D,
geometry, geography, etc.):
```bash
./dev build libgeos
```

## Step 4: Prepare the Full SQL Log File

First, check that the following statements are at the top of the full SQL log
file. If they are not, add them:
```sql
SET statement_timeout='1m0s';
SET sql_safe_updates = false;
```

If metamorphic settings were used, also add them to the top of the full SQL log
file:
```sql
SET CLUSTER SETTING kv.rangefeed.buffered_sender.enabled = true;
SET CLUSTER SETTING kv.transaction.write_buffering.enabled = true;
```

Create an appropriate directory either in the artifacts directory or in the
repository root for holding temp files.

## Step 5: Initial Reproduction

Determine the correct demo command based on test type:
- **Multi-region test**: Use `--nodes=9`
- **Single-region test**: Omit `--nodes` option

Use a command like this to try reproducing the test failure from the full SQL
log file. This command could take up to 20 minutes to finish.

```bash
<env vars> ./cockroach demo --multitenant=false --nodes=9 --insecure --set=errexit=false --no-example-database --format=tsv -f <full-sql-log-file>
```

**Check that the output reproduces the test failure described in the failure
log.** There are many possible failure modes. Look for one of the following,
which should match the failure log:

1. **Different results** between the two executions of the "Query of Interest"
   (which is the randomly generated SELECT statement repeated twice near the end
   of the log, wrapped in various SET and RESET staements). These different
   results could take the form of different result sets, or could also be an
   error in one case and no error in the other case. This is an **"oracle"
   failure**.
2. Or, `internal error` or **assertion failure**. Note the error message for the
   reduce step.
3. Or, a **panic**. Note the error message for the reduce step.
4. Or, a **timeout**. Note the statement that timed out.

### Troubleshooting

**IMPORTANT:** Many failures are nondeterministic, especially for multi-region
tests. If no failure happens on the first run, try up to 10 times before
concluding it doesn't reproduce.

It can be helpful at this point to compare the output with the `failure*.log`
which should show the failure from the original test run.

**If the initial run fails to reproduce after 10 times, pause here and report to
the user that the failure cannot be reproduced, and show the command that was
tried.**  The user might have additional instructions.

If it looks like it reproduces, it's time to move on to the next step.

## Step 6: Use the Reduce Tool

Build the reduce tool:
```bash
./dev build reduce
```

### Prepare the Full SQL Log File again

For multi-region tests, remove `\connect` lines (they cause syntax errors in the
`reduce` tool):
```bash
grep -v '^\\connect' <full-sql-log-file> > <cleaned-log>
```

### Run Reduce

**IMPORTANT:** The reduce tool must be run from the cockroach repository root
directory, because it looks for `./cockroach` in the current directory.

Use the `-multi-region` option for multi-region tests, or omit it for
single-region tests.

**For "oracle" failures (different results):**
```bash
./bin/reduce -unoptimized-query-oracle -multi-region -chunk 25 -v -file <cleaned-log> 2>&1 | tee reduce-output.log
```
The `-unoptimized-query-oracle` option checks whether the two executions of the
"Query of Interest" produce the same results.

**For internal errors/assertion failures/panics:**
```bash
./bin/reduce -contains "<error-regex>" -multi-region -chunk 25 -v -file <cleaned-log> 2>&1 | tee reduce-output.log
```
Use a distinctive part of the error message as the `-contains` regex (e.g.,
`"nil LeafTxnInputState"`).

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

**If the reduce tool fails to reproduce, pause here and report this to the
user. They might have additional instructions.** Occasionally we have to modify
the reduce tool itself, if the test failure is not reproducing.

## Step 7: Create Test Script and Determine Reproduction Rate

**IMPORTANT:** Many bugs are nondeterministic. Before manual simplification,
create a reusable test script and determine the reproduction rate.

Create a small test script (adjust as needed):
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
For "oracle" failures, instead of checking for an error pattern, the test script
probably needs to isolate and diff the results of the two executions of the
"Query of Interest".

Run the test script to determine the reproduction rate. It's not always 100%.

This rate determines how many attempts you need when testing simplifications:
- 100% rate: Single attempt sufficient
- 50% rate: 2-3 attempts usually sufficient
- 10% rate: Need ~10 attempts to be confident
- <5% rate: May need 20+ attempts

Note that in some cases, the following settings might need to be added back to
the reduced file to get a repro:
```sql
SET statement_timeout='1m0s';
SET sql_safe_updates = false;
```

**If the reduced SQL fails to reproduce after 10 attempts, pause here and report
this to the user. They might have additional instructions.**

## Step 8: Manual Simplification

Now iteratively simplify the SQL while maintaining reproduction.

**CRITICAL:** For nondeterministic failures, you MUST test each simplification
with enough attempts based on the repro rate. A single failed attempt does NOT
mean the simplification broke the repro - it may just be nondeterminism.

### Workflow for Each Simplification

1. Copy `reduced.sql` to `reduced_v2.sql`
2. Make ONE small change to `reduced_v2.sql`
3. Run `./test_repro.sh` (which tests `reduced_v2.sql`)
4. If it reproduces: Copy `reduced_v2.sql` to `reduced.sql`, continue simplifying
5. If it doesn't reproduce after enough attempts: Discard `reduced_v2.sql`, try
   a different change (i.e. backtrack).

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
6. **Weird characters** - Remove or replace non-ASCII characters from names and data
7. other SQL simplifications

**For "oracle" failures, when editing the Query of Interest, be sure to edit
_BOTH_ copies of the Query of Interest so that they are identical.** Otherwise
it won't be an apples-to-apples comparison when diffing the result sets.

### Common Required Elements

These often cannot be removed:
- **Optimizer random seed**: `SET testing_optimizer_random_seed = <value>` - this
  specific value often cannot be changed, as it determines which optimizer rules
  are disabled
- **Optimizer rule probability**: `SET testing_optimizer_disable_rule_probability`
  - affects query plan selection
- Specific RESET/SET sequences for optimizer settings, such as distsql and vectorize
- Certain indexes (affect query plans)
- Multi-node setup (`--nodes=9`) for distributed query bugs (though try
  single-node first - it may work and is simpler)
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

**If you get stuck (i.e. cannot reproduce again after backtracking), stop and
report to the user with the exact command you were trying.**

## Step 9: Final Verification and Output

**After about 20 minutes of simplification, or if there are no more
simplifications after backtracking a few times, it's time to stop.**

1. Run reproduction 10+ times to confirm stability and determine final repro rate
2. Document the minimal reproduction steps
3. Note which elements were required vs optional

### Output

The final output should include two files that can be shown to the user:

1. **reduced.sql** - The minimal SQL script that reproduces the bug
2. **bisect_run.sh** - A script for use with `git bisect run`

Write the output in such a way that it could be copied and pasted into a
terminal.

### Example Output Format

(The commands in this output should be edited to match what was necessary to
reproduce.)

```bash
# Minimal Reproduction

# reduced.sql
cat > reduced.sql << 'EOF'
CREATE TABLE t ();

SET testing_optimizer_random_seed = 1234567890;
SET testing_optimizer_disable_rule_probability = 0.5;

SELECT ...;
EOF

# bisect_run.sh
cat > bisect_run.sh << 'EOF'
#!/bin/bash
# Git bisect run script
# Exit codes: 0=good (bug not present), 1=bad (bug present), 125=skip (build failed)

REPO_DIR="/path/to/cockroach"
REPRO_SQL="/path/to/reduced.sql"

cd "$REPO_DIR" || exit 125

echo "=== Testing commit $(git rev-parse --short HEAD) ==="

# Build (use --crdb_test if runtime assertions were enabled in the original test)
if ! ./dev build short -- --crdb_test 2>&1 | grep -q "Successfully built"; then
    echo "BUILD FAILED - skipping"
    exit 125
fi

# Test for bug (try 3 times for flaky bugs)
for i in {1..3}; do
    if ./cockroach demo --multitenant=false --insecure \
        --set=errexit=false --no-example-database --format=tsv \
        -f "$REPRO_SQL" 2>&1 | grep -q "<error-pattern>"; then
        echo "BUG PRESENT - marking as BAD"
        exit 1
    fi
done

echo "Bug not present - marking as GOOD"
exit 0
EOF
chmod +x bisect_run.sh

# Command to reproduce
git checkout <commit-hash>
./bisect_run.sh

# Command to bisect
git bisect start ...
git bisect run bisect_run.sh

# Failure
# <paste stacktrace or relevant failure details here>

# Repro rate: ~X% (may need multiple attempts)
```

**After showing this output, ask the user if they want to try reproducing the
bug on master branch.**

## Optional Step 10: Check if Bug is Fixed on Master

Before bisecting, check whether the bug has already been fixed on master.

```bash
git stash  # if needed
git checkout master
./dev build short -- --crdb_test
./cockroach demo --multitenant=false --insecure --set=errexit=false --no-example-database --format=tsv -f reduced.sql
```

Run this a few times to account for flakiness. Note whether the bug reproduces
on master or not.

## Optional Step 11: Bisect

If the user wants to find the commit that introduced or fixed the bug, use
`git bisect`.

### If the Bug is Already Fixed on Master

Bisect to find the **fix commit** (the first commit where the bug no longer
reproduces). Use custom terms since the "good" commit (master) is newer than the
"bad" commit:

```bash
git bisect start --first-parent --term-old=broken --term-new=fixed
git bisect broken <commit-where-bug-exists>   # e.g., the original failing commit
git bisect fixed master                        # master is fixed

git bisect run ./bisect_run.sh

# When done
git bisect reset
```

**Note:** The `--first-parent` option follows only merge commits on the main
branch, avoiding detours into feature branches. The bisect script must return 0
when the bug is NOT present (fixed) and 1 when the bug IS present (broken).

### If the Bug Still Exists on Master

Bisect to find the **regression commit** (the first commit where the bug was
introduced):

```bash
git bisect start --first-parent
git bisect good <known-good-commit>   # e.g., a previous release tag
git bisect bad master                  # master has the bug

git bisect run ./bisect_run.sh

# When done
git bisect reset
```

The bisect will identify the commit that introduced or fixed the bug.

### Finding a Good Commit

If you don't know a good commit (where the bug doesn't exist), you can jump back
in time to find one.

```bash
# Find a commit from ~6 months ago on the main branch
git rev-list --first-parent -1 --before="6 months ago" HEAD
```

Test whether the bug exists at that commit. If not, use it as the good commit
for bisect. If the bug still exists, try going back further in time, but don't
go back further than 1 year.

**If a known good commit can't be found within 1 year, stop and report this to
the user.**
