---
name: go-test
description: Common test guidance for all Go tests in CockroachDB, including assertion patterns and debugging test failures.
---

# General Test Guidelines

This skill covers common testing advice that applies to all Go tests in CockroachDB, whether unit tests, table-driven tests, or integration tests.

## Assertions

Use `require.*` (stops on failure) for most checks. Use `assert.*` (continues on failure) only when you want to see multiple failures at once.

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

## Debugging Test Failures

When a `./dev test` run fails:

1. **Don't weaken assertions to make tests pass.** If removing or loosening an assertion makes a test pass, that may mean the **source code under test has a bug**, not the test.

2. **Read the source code being tested.** Before changing any assertion, read the function or component the test exercises. Verify whether the test uncovered a real bug in the production code.

3. **Decide what to fix:**
   - If the source code is wrong: **fix the source code**, keep the assertion.
   - If the test logic was wrong (e.g., wrong expected value, outdated assumption): **fix the test**.
   - If you're uncertain: **ask the user** before changing either.

## Related Skills

- `/table-driven-test` — Structuring test cases as table-driven tests with struct slices.
- `/integration-test` — Writing tests that use a CockroachDB test server.
