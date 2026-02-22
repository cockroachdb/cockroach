---
name: table-driven-test
description: Guidelines for creating clean, well-structured table-driven tests in Go following CockroachDB conventions.
---

# Table-Driven Test Guidelines

Table-driven tests define multiple test cases in a slice of structs, then iterate over them executing the same test logic. This makes it easy to add cases, improves readability, and reduces duplication.

**When to use table-driven tests:**
- You have 3+ similar test cases that vary by inputs/outputs
- Tests follow the same logic pattern with different data
- Most unit and integration tests benefit from this structure

**When to skip:**
- Only 1-2 simple test cases (overhead not worth it)
- Each test requires completely different logic
- Test setup/teardown varies significantly between cases

**Not the same as:** Datadriven tests (different library with testdata files)

## Basic Structure

```go
func TestMyFunction(t *testing.T) {
    tests := []struct {
        name        string
        input       string
        expectedLen int
    }{
        {name: "basic case", input: "hello", expectedLen: 5},
        {name: "empty input", input: "", expectedLen: 0},
    }

    for _, tc := range tests {
        t.Run(tc.name, func(t *testing.T) {
            result, err := MyFunction(tc.input)
            require.NoError(t, err)
            require.Equal(t, tc.expectedLen, result)
        })
    }
}
```

## Core Principles

### 1. Only Specify What's Necessary

**Bad:**
```go
{
    name: "remap table",
    tableID: 100, tableName: "users", schemaID: 1,
    schemaName: "public", databaseID: 50, databaseName: "mydb",
    expectedID: 51,  // only this is actually tested!
}
```

**Good:**
```go
{name: "remap table", tableID: 100, expectedID: 51}
```

### 2. Struct Field Ordering: Inputs First, Then Expected

Order struct fields with input fields at the top and verification fields at the bottom. Prefix all verification fields with `expected` so readers can immediately distinguish inputs from outputs.

**Bad:**
```go
tests := []struct {
    name      string
    wantErr   bool       // verification mixed with inputs
    input     string
    output    int        // unclear if this is input or verification
}
```

**Good:**
```go
tests := []struct {
    name          string
    input         string
    expectedCount int
    expectedErr   string
}
```

### 3. One Concern Per Test Case

**Bad:**
```go
{
    name: "multiple behaviors",
    input: map[int]int{
        10: 60,  // normal remapping
        49: 99,  // edge case
        50: 50,  // system table preservation
    },
}
```

**Good:**
```go
{name: "normal remapping", input: map[int]int{10: 60}},
{name: "preserve system table IDs under 50", input: map[int]int{49: 49}},
{name: "remap IDs at or above 50", input: map[int]int{50: 100}},
```

### 4. Independent Test Cases

Each case should be self-contained. Don't build dependent state across cases.

### 5. Names Describe Intent, Not Inputs

Test case names should hint at the **intention** or **scenario**, not duplicate the input data. A reader should understand what the case is testing from the name alone.

- Good: `"two matched regions"`, `"error on negative input"`
- Bad: `"match region x and y"`, `"test1"`, `"input_abc"`

The name should answer "what scenario is this?" not "what data does this use?"

## Assertions

Use `require.*` (stops on failure) for most checks. Use `assert.*` (continues on failure) only when you want to see multiple failures.

**Common patterns:**
```go
// Errors
require.NoError(t, err)
require.Error(t, err)
require.ErrorContains(t, err, "not found")

// Equality
require.Equal(t, expected, actual)  // shows both values on failure
require.True(t, result == expected) // don't do this - hides values

// Collections
require.Len(t, slice, expectedLen)
require.Contains(t, slice, element)
```

**Avoid redundant nil checks:** Don't use `require.NotNil` before an assertion that will already fail on nil (like `require.Equal` or `require.Contains`). The subsequent assertion provides a clearer failure message anyway.

```go
// Bad: redundant nil check
require.NotNil(t, result)
require.Equal(t, expectedVal, result.Field)

// Good: Equal already fails clearly if result is nil
require.Equal(t, expectedVal, result.Field)
```

**Error handling in test cases:**
```go
tests := []struct {
    name        string
    input       string
    expectedErr string
}{
    {name: "valid input", input: "hello"},
    {name: "empty input rejected", input: "", expectedErr: "must not be empty"},
}

for _, tc := range tests {
    t.Run(tc.name, func(t *testing.T) {
        err := Validate(tc.input)
        if tc.expectedErr != "" {
            require.ErrorContains(t, err, tc.expectedErr)
        } else {
            require.NoError(t, err)
        }
    })
}
```

## Variadic Helper Functions

Use helpers to reduce boilerplate and make test data readable.

**Example from CockroachDB** (`pkg/backup/compaction_dist_test.go`):

```go
// Helper types
type mockEntry struct {
    span     roachpb.Span
    locality string
}

// Variadic helpers
func entry(start, end string, locality string) mockEntry {
    return mockEntry{
        span:     mockSpan(start, end),
        locality: locality,
    }
}

func entries(specs ...mockEntry) []execinfrapb.RestoreSpanEntry {
    var entries []execinfrapb.RestoreSpanEntry
    for _, s := range specs {
        var dir cloudpb.ExternalStorage
        if s.locality != "" {
            dir = cloudpb.ExternalStorage{
                URI: "nodelocal://1/test?COCKROACH_LOCALITY=" + s.locality,
            }
        }
        entries = append(entries, execinfrapb.RestoreSpanEntry{
            Span:  s.span,
            Files: []execinfrapb.RestoreFileSpec{{Dir: dir}},
        })
    }
    return entries
}

// Usage - reads like a specification
entries := entries(
    entry("a", "b", "dc=dc1"),
    entry("c", "d", "dc=dc2"),
    entry("e", "f", "dc=dc3"),
)
```

**When to create helpers:**
- Complex struct initialization obscures test intent
- Patterns repeat across test cases
- Building composite data structures

**When NOT to use:**
- Simple values that don't need transformation
- One-off test cases
- Helpers add more complexity than they remove

## Integration Tests

For tests requiring a database server, see the `/integration-test` skill. The table-driven patterns here apply to both unit and integration tests.