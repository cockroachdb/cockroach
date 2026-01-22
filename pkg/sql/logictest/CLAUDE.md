# Logic Tests

This package executes SQL logic tests which are run as end-to-end tests against CockroachDB's SQL layer.

## Overview

TestLogic implements the infrastructure that runs end-to-end tests against CockroachDB's SQL layer. It is typically used to run CockroachDB's own tests (stored in the `testdata` directory) during development and CI, and a subset of SQLite's "Sqllogictest" during nightly CI runs.

TestLogic reads one or more test input files containing sequences of SQL statements and queries. Each input file is meant to test a feature group. Each test input file gets its own fresh, empty database.

The test input is expressed using a domain-specific language called Test-Script, defined by SQLite's "Sqllogictest". The official home of Sqllogictest and Test-Script is https://www.sqlite.org/sqllogictest/

## Running Tests

```bash
# Run all logic tests
./dev testlogic

# Run only specific test files (using a pattern match)
./dev testlogic base --files='rename|zone'

# Run CCL/enterprise logic tests
./dev testlogic ccl

# Run specific test files under a specific configuration
./dev testlogic base --config=local --files='prepare|fk'

# Run with verbose output
./dev testlogic base --files='mytest' -- -v

# Run with SQL statement visibility (useful for debugging)
./dev testlogic base --files='mytest' -- -show-sql

# Rewrite test expectations with actual results
./dev testlogic base --files='mytest' -- -rewrite
```

## Test File Structure

### Configuration Directives

Logic tests can start with a configuration directive that lists which configurations to run:

```
# LogicTest: local fakedist
```

This runs the test in both `local` and `fakedist` configurations (in separate subtests). If missing, the test runs in the default configuration.

**Blocklists** allow running all configs except specific ones:

```
# LogicTest: enterprise-configs !3node-tenant
```

You can optionally specify an issue number for the blocklist:

```
# LogicTest: !3node-tenant(<issue number>)
```

### Cluster Option Directives

These directives affect cluster settings across all configurations:

```
# cluster-opt: opt1 opt2
```

Options include:
- `tracing-off`: Disable tracing by default

### Tenant Cluster Setting Overrides

Configure tenant cluster settings (useful for secondary tenant configurations):

```
# tenant-cluster-setting-override-opt: setting_name1=setting_value1 setting_name2=setting_value2
```

### Tenant Capability Overrides

Configure tenant capabilities (useful for secondary tenant configurations):

```
# tenant-capability-override-opt: capability_id1=capability_value1 capability_id2=capability_value2
```

## Test-Script Language

### Statement Directives

**statement ok** - Runs a statement expecting success:
```
statement ok
CREATE TABLE kv (k INT PRIMARY KEY, v INT)
```

**statement count N** - Expects N rows affected (preferred over `ok` for DML):
```
statement count 2
INSERT INTO kv VALUES (1,2), (2,3)
```

**statement error \<regexp\>** - Expects an error matching the regexp:
```
statement error duplicate key
INSERT INTO kv VALUES (1,2)
```

**statement notice \<regexp\>** - Expects a notice matching the regexp:
```
statement notice NOTICE: something happened
SELECT some_function()
```

**statement async \<name\> \<options\>** - Runs statement asynchronously (useful for blocking operations):
```
statement async blocking_stmt ok
SELECT pg_sleep(10)
```

**awaitstatement \<name\>** - Completes a pending async statement:
```
awaitstatement blocking_stmt
```

### Query Directives

**query \<typestring\> \<options\> \<label\>** - Runs a query and verifies results:

```
query I
SELECT 1, 2
----
1 2
```

**Type strings** specify column count and types:
- `T` - text (also for arrays, timestamps, etc.)
- `I` - integer
- `F` - floating point (15 significant decimal digits)
- `R` - decimal
- `B` - boolean
- `O` - oid
- `_` - include column header but ignore results (useful for non-deterministic columns)

**Query options** (comma-separated):
- `nosort` - Don't sort results
- `rowsort` - Sort both expected and actual rows
- `valuesort` - Sort all values as one big set
- `partialsort(x,y,...)` - Partial sort preserving order on specified columns (1-indexed)
- `colnames` - Verify column names (first line of expected results)
- `retry` - Retry on mismatch with exponential backoff
- `async` - Run asynchronously (use with label as unique name)
- `kvtrace` - Compare against KV operation traces
- `noticetrace` - Compare only notices
- `nodeidx=N` - Run on node N of the cluster
- `allowunsafe` - Allow access to unsafe internals
- `scrub-row-counts` - Remove "row count" lines from results
- `strip-oids` - Replace virtual table OIDs with `__OID__` placeholders
- `match(<regexp>)` - Exclude rows not matching regexp from output

**query error \<regexp\>** - Expects query to fail:
```
query error division by zero
SELECT 1/0
```

**query empty** - Expects no rows returned:
```
query empty
SELECT * FROM kv WHERE k > 100
```

**awaitquery \<name\>** - Completes a pending async query:
```
awaitquery async_query_name
```

### Other Directives

**repeat \<number\>** - Repeats the following statement/query:
```
repeat 50
statement ok
INSERT INTO T VALUES ((SELECT max(k+1) FROM T))
```

**let $varname** - Stores query result for later use:
```
let $foo
SELECT max(v) FROM kv

statement ok
SELECT * FROM kv WHERE v = $foo
```

**sleep \<duration\>** - Introduces a sleep period:
```
sleep 2s
```

**user \<username\> [nodeidx=N] [newsession]** - Changes user for subsequent operations:
```
user testuser nodeidx=1 newsession
```

Prefix with `host-cluster-` to force connection to host cluster in multi-tenant configs.

**skip #ISSUE [args...]** - Skips entire logic test:
```
skip #12345 reason for skipping
```

**skip under \<deadlock/race/stress/metamorphic/duress\> [#ISSUE]** - Conditionally skip test:
```
skip under race #12345
```

**skipif \<condition\>** - Skips following statement/query if condition matches:
```
skipif cockroachdb
statement ok
-- This runs on other databases but not CockroachDB
```

Conditions: `mysql`, `mssql`, `postgresql`, `cockroachdb`, `config CONFIG`, `bigendian`, `littleendian`

**onlyif \<condition\>** - Only runs if condition matches:
```
onlyif cockroachdb
statement ok
-- This only runs on CockroachDB
```

**subtest \<testname\>** - Defines a subtest:
```
subtest my_feature
statement ok
-- Test statements here
```

**retry** - Retries next statement/query until success or timeout:
```
retry
query I
SELECT count(*) FROM eventually_consistent_table
----
10
```

**retry_duration \<duration\>** - Sets retry timeout (default 45s):
```
retry_duration 10s
```

**traceon \<file\>** / **traceoff** - Controls tracing:
```
traceon trace.txt
query I
SELECT expensive_operation()
----
result
traceoff
```

## Command-Line Flags

### Input Selection

- `-d <glob>` - Select files matching glob pattern
- `-bigtest` - Enable long-running SQLite logic tests

### Configuration

- `-config name[,name2,...]` - Customize test cluster configuration (must be one of `LogicTestConfigs`)

### Error Handling

- `-max-errors N` - Stop after N errors (default 1, 0 for unlimited)
- `-allow-prepare-fail` - Tolerate errors during query preparation
- `-flex-types` - Tolerate mismatched numeric types

### Output Control

- `-v` - Verbose output (Go testing flag)
- `-show-sql` - Show SQL statements before execution (useful for debugging)
- `-show-diff` - Generate diff for expectation mismatches
- `-error-summary` - Print per-error summary of failing queries
- `-full-messages` - Don't shorten errors/SQL in summaries

### Test Modification

- `-rewrite` - Rewrite test files with actual results (USE WITH CAUTION)
- `-rewrite-sql` - Reformat SQL queries (USE SPARINGLY)
- `-line-length N` - Target line length for `-rewrite-sql`

### Advanced Options

- `-disable-opt-rule-probability P` - Randomly disable optimizer rules with probability P
- `-optimizer-cost-perturbation P` - Randomly perturb optimizer costs by fraction P
- `-default-workmem` - Disable randomization of `sql.distsql.temp_storage.workmem`

## Adding a Test File

After adding a new test file:

```bash
./dev generate bazel
```

This generates the test code required for Bazel to recognize the new test.

## Debugging Tips

1. **See what's being executed**: Use `-show-sql` to see each statement before execution
2. **Understand failures**: Use `-v` for verbose output and `-show-diff` for diffs
3. **Update expectations**: Use `-rewrite` after verifying the new behavior is correct
4. **Isolate issues**: Use `-max-errors 0` to see all failures, not just the first one
5. **Check specific configs**: Use `-config` to test only specific configurations

## Common Patterns

**Testing error conditions:**
```
statement error pgcode 23505 duplicate key
INSERT INTO t VALUES (1)
```

**Testing with retries for eventually consistent operations:**
```
retry
query I
SELECT count(*) FROM table_being_populated
----
100
```

**Using variables for dynamic values:**
```
let $id
SELECT max(id) FROM users

query T
SELECT name FROM users WHERE id = $id
----
expected_name
```

**Async operations for concurrent testing:**
```
statement async lock_holder ok
BEGIN; SELECT * FROM t WHERE k=1 FOR UPDATE; SELECT pg_sleep(5)

statement async lock_waiter ok
SELECT * FROM t WHERE k=1 FOR UPDATE

awaitstatement lock_holder
awaitstatement lock_waiter
```