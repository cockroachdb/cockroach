---
name: integration-test
description: Guidelines for writing integration tests with CockroachDB test servers, including when to use them and how to use sqlutils.
---

# Integration Test Guidelines

Integration tests use `testserver`, a fully functional CockroachDB server with
all components in-memory and in the same process. They're **SLOW** to spin up
(can add seconds per test case), so use them only when necessary.

## When to Use Integration Tests

**Use for:**
- SQL execution and query validation
- Transaction behavior
- Schema operations
- Distributed systems behavior

**Avoid for:**
- Pure logic and data transformations
- Parsing and validation
- Anything that can be unit tested

## Test Server Types

**Single Server** (`serverutils.StartServerOnly`):
- Use for: Single-node behavior, SQL queries, schema operations
- Faster than clusters
- Sufficient for most integration tests

**Test Cluster** (`testcluster.StartTestCluster`):
- Use for: Distributed behavior, replication, multi-node coordination
- Much slower than single server
- Only use when you specifically need multiple nodes

## Basic Setup

```go
func TestDatabaseOperation(t *testing.T) {
    ctx := context.Background()
    s := serverutils.StartServerOnly(t, base.TestServerArgs{})
    defer s.Stopper().Stop(ctx)

    sqlDB := sqlutils.MakeSQLRunner(s.ApplicationLayer().SQLConn(t))

    // Use sqlDB for queries
}
```

**With configuration:**
```go
s := serverutils.StartServerOnly(t, base.TestServerArgs{
    // Custom configuration
    Knobs: base.TestingKnobs{
        // Testing knobs to control behavior
    },
})
```

## Using sqlutils.MakeSQLRunner

**Prefer** `sqlutils.MakeSQLRunner` over raw database connections in most cases.
It provides automatic error handling (fails the test on error), cleaner syntax,
and helper methods for common operations.

**Common methods:**
```go
sqlDB := sqlutils.MakeSQLRunner(s.ApplicationLayer().SQLConn(t))

// Execute statements (fails test on error)
sqlDB.Exec(t, "CREATE TABLE foo (id INT PRIMARY KEY)")
sqlDB.Exec(t, "INSERT INTO foo VALUES (1), (2), (3)")

// Query single value to use in test logic - PREFER QueryRow
var jobID jobspb.JobID
sqlDB.QueryRow(t, "SELECT job_id FROM system.jobs ORDER BY created DESC LIMIT 1").Scan(&jobID)
// Now use jobID in test logic

var count int
sqlDB.QueryRow(t, "SELECT count(*) FROM foo").Scan(&count)
require.Greater(t, count, 0)

// Query results as strings - use for validation
results := sqlDB.QueryStr(t, "SELECT * FROM foo ORDER BY id")
// returns [][]string

// Check query results - best for exact result verification
sqlDB.CheckQueryResults(t,
    "SELECT id FROM foo ORDER BY id",
    [][]string{{"1"}, {"2"}, {"3"}},
)
```

**When to use the raw `sqlDB.DB` handle instead:**

Use `sqlDB.DB` when you need direct error handling rather than automatic test
failure:
- **Retry loops** (e.g., `testutils.SucceedsSoon`) - need to return errors for retry logic
- **Custom error handling** - when you need to inspect or handle specific error types

```go
// Raw DB access in retry loop
testutils.SucceedsSoon(t, func() error {
    _, err := sqlDB.DB.ExecContext(context.Background(), query)
    return err  // return error for retry logic
})

// Raw DB access for custom error handling
_, err := sqlDB.DB.ExecContext(ctx, query)
if err != nil {
    if strings.Contains(err.Error(), "specific condition") {
        // handle specific error
    }
}
```

**Choosing the right query method:**
- `sqlDB.QueryRow` - **Preferred** when reading data to use later in test logic (IDs, counts, values). Preserves datatypes and enables clean scanning into typed variables.
- `sqlDB.QueryStr` - Use when you need all results as strings for manipulation or comparison.
- `sqlDB.CheckQueryResults` - Use for validating exact query output matches expected strings.
- `sqlDB.DB` - Use when you need the raw `*gosql.DB` handle for direct error handling, such as retry loops or custom error inspection (see above).

## Performance: Reuse Test Servers

**CRITICAL:** Spin up the server ONCE before the test loop, not inside each iteration.

**Good:**
```go
func TestMultipleCases(t *testing.T) {
    s := serverutils.StartServerOnly(t, base.TestServerArgs{})
    defer s.Stopper().Stop(context.Background())
    sqlDB := sqlutils.MakeSQLRunner(s.ApplicationLayer().SQLConn(t))

    tests := []struct {
        name     string
        query    string
        expected [][]string
    }{
        {name: "select 1", query: "SELECT 1", expected: [][]string{{"1"}}},
        {name: "select 2", query: "SELECT 2", expected: [][]string{{"2"}}},
    }

    for _, tc := range tests {
        t.Run(tc.name, func(t *testing.T) {
            result := sqlDB.QueryStr(t, tc.query)
            require.Equal(t, tc.expected, result)
        })
    }
}
```

## Database and Table Management

Balance performance with test isolation. Choose based on your test needs:

**Pattern 1: Reuse table, clear data (faster)**
```go
sqlDB.Exec(t, "CREATE TABLE test_table (id INT PRIMARY KEY, data TEXT)")

for _, tc := range tests {
    t.Run(tc.name, func(t *testing.T) {
        sqlDB.Exec(t, "DELETE FROM test_table")
        // Run test case
    })
}
```

**Pattern 2: Unique table per case**
```go
for _, tc := range tests {
    t.Run(tc.name, func(t *testing.T) {
        tableName := fmt.Sprintf("test_%s", strings.ReplaceAll(tc.name, " ", "_"))
        sqlDB.Exec(t, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", tableName))
        // Run test case
    })
}
```

**Pattern 3: Schema changes require per-case tables**

When test cases modify schema (ALTER TABLE, ADD COLUMN), each needs its own table to avoid conflicts.

## Combining with Table-Driven Tests

Use the `/table-driven-test` skill for structuring test cases. The key pattern: set up the server once, then use table-driven structure for the test cases.

```go
func TestDatabaseOperations(t *testing.T) {
    // Setup server ONCE
    ctx := context.Background()
    s := serverutils.StartServerOnly(t, base.TestServerArgs{})
    defer s.Stopper().Stop(ctx)
    sqlDB := sqlutils.MakeSQLRunner(s.ApplicationLayer().SQLConn(t))

    // Table-driven test cases
    tests := []struct {
        name     string
        setup    string
        query    string
        expected [][]string
    }{
        {
            name:     "basic select",
            setup:    "CREATE TABLE t1 (id INT); INSERT INTO t1 VALUES (1)",
            query:    "SELECT * FROM t1",
            expected: [][]string{{"1"}},
        },
        {
            name:     "empty table",
            setup:    "CREATE TABLE t2 (id INT)",
            query:    "SELECT * FROM t2",
            expected: [][]string{},
        },
    }

    for _, tc := range tests {
        t.Run(tc.name, func(t *testing.T) {
            sqlDB.Exec(t, tc.setup)
            result := sqlDB.QueryStr(t, tc.query)
            require.Equal(t, tc.expected, result)
        })
    }
}
```