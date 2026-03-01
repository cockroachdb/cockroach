# LogicTest Framework

This directory contains CockroachDB's primary end-to-end SQL testing infrastructure.
Test files are in `testdata/logic_test/`. CCL-specific tests are in
`pkg/ccl/logictestccl/testdata/logic_test/`.

## Running Tests

Running with `--config=local` is usually the quickest way to run a test.

```bash
# Run all tests with default configs
./dev testlogic base

# Run specific test file(s)
./dev testlogic base --config=local --files='fk'      # Files matching 'fk'
./dev testlogic base --config=local --files='_tmp'    # The _tmp file (for iteration)
./dev testlogic base --files='(fk|grant)'             # Multiple patterns, all default configs

# Run with specific config
./dev testlogic base --config=local
./dev testlogic base --config=fakedist

# Run CCL tests (enterprise features)
./dev testlogic ccl --files='_tmp'

# Useful flags
./dev testlogic base --files='_tmp' -v              # Verbose output
./dev testlogic base --files='_tmp' --show-sql      # Print SQL as it runs
./dev testlogic base --files='_tmp' --rewrite       # Update expected results
```

## Adding a Test File

Use an existing test file if there already is a good place to add a test.

If you do add a new test file, then regenerate Bazel build files:

```bash
./dev generate bazel
```

## Test File Syntax

### File Header

Optionally specify which configs to run:

```sql
# LogicTest: local fakedist

# LogicTest: default-configs

# LogicTest: !3node-tenant(#123456)   # Blocklist config with issue link
```

### Statements

```sql
statement ok
CREATE TABLE t (id INT PRIMARY KEY)

statement count 2
INSERT INTO t VALUES (1), (2)

statement error duplicate key
INSERT INTO t VALUES (1)

statement error pgcode 23505
INSERT INTO t VALUES (1)
```

### Queries

```sql
query IT
SELECT id, name FROM t ORDER BY id
----
1  alice
2  bob

query I rowsort
SELECT id FROM t
----
1
2

query T colnames
SELECT name FROM t LIMIT 1
----
name
alice

query error relation "foo" does not exist
SELECT * FROM foo
```

**Type characters:** `T` (text), `I` (integer), `F` (float), `R` (decimal), `B` (bool), `O` (oid)

**Query options:**
- `rowsort` - Sort rows before comparing
- `colnames` - Include column headers in output
- `retry` - Retry with backoff (~45s) for eventually-consistent results
- `nosort` - Expect exact order
- `partialsort(1,2)` - Sort by specific columns only
- More options are defined in the comments of logic.go

Note, if a query returns multiple rows then one of the following is required:
- ORDER BY clause in the query
- rowsort option
- nosort option

### Variables

```sql
let $table_id
SELECT 'users'::regclass::oid::int

query I
SELECT * FROM crdb_internal.tables WHERE table_id = $table_id
----
...
```

### Conditional Execution

```sql
skipif config 3node-tenant
statement ok
SET CLUSTER SETTING ...

onlyif config local-legacy-schema-changer
statement ok
...

skip under race
statement ok
...
```

### Other Directives

```sql
user testuser
statement ok
SELECT * FROM t

user root

subtest my_subtest_name
```

**Note on `user` directive:** Only `root` and `testuser` have pre-generated TLS
certificates for the logic test framework. Do not create custom users and switch
to them with the `user` directive. Instead, use `SET ROLE` to assume a different
role's permissions while connected as `testuser`.

```sql

repeat 10
statement ok
INSERT INTO t VALUES (...)
```

## Test Configurations

### Common Configs

| Config | Description |
|--------|-------------|
| `local` | Single node, DistSQL off, fastest |
| `local-legacy-schema-changer` | Declarative schema changer disabled |
| `local-vec-off` | Vectorization disabled |
| `fakedist` | 3 nodes with fake span resolver |
| `5node` | 5-node cluster |
| `3node-tenant` | Runs as SQL tenant (CCL only) |
| `local-read-committed` | READ COMMITTED isolation (CCL) |
| `local-mixed-25.4` | Mixed-version testing |

### Config Sets

Use these instead of listing individual configs:

- `default-configs` - Standard set of configs
- `5node-default-configs` - 5-node configs
- `3node-tenant-default-configs` - Tenant configs
- `enterprise-configs` - CCL-only configs

## CCL Tests

Tests requiring CCL features (multi-region, tenants, enterprise isolation levels) go in:

```
pkg/ccl/logictestccl/testdata/logic_test/
```

Run with:

```bash
./dev testlogic ccl --files='my_test'
```

CCL-only configs include: `3node-tenant`, `3node-tenant-multiregion`,
`local-read-committed`, `local-repeatable-read`, `multiregion-9node-3region-3azs`.

## Mixed-Version Testing with cockroach-go-testserver

The `cockroach-go-testserver-*` configs run actual separate cockroach processes
using the cockroach-go/testserver package. They bootstrap with a predecessor
version and upgrade to the current commit, enabling true mixed-version testing.

Key differences from `local-mixed-*` configs:
- Runs separate binary processes (not in-memory)
- Downloads and uses actual release binaries
- Only works with bazel (not `go test` or `./dev testlogic`)
- Skipped under race/stress builds

Run these tests via bazel:

```bash
bazel test //pkg/sql/logictest/tests/cockroach-go-testserver-25.4:cockroach-go-testserver-25_4_test \
  --test_filter='TestLogic/...'
```

The `upgrade` directive is available only in these configs:

```sql
# Upgrade a specific node (0-indexed)
upgrade 0

# Upgrade all nodes
upgrade all

# Then finalize the upgrade
statement ok
SET CLUSTER SETTING version = crdb_internal.node_executable_version()
```

## Iterative Development

Use the `_tmp` file for rapid iteration:

```bash
# Edit the temp file
vim pkg/sql/logictest/testdata/logic_test/_tmp

# Run it
./dev testlogic base --config=local --files='_tmp'

# For CCL features
vim pkg/ccl/logictestccl/testdata/logic_test/_tmp
./dev testlogic ccl --config=local --files='_tmp'
```

## Key Files

- `logic.go` - Core test engine and directive parser
- `logictestbase/logictestbase.go` - Configuration definitions
- `parsing.go` - Directive parsing helpers
- `testdata/logic_test/` - Test files
