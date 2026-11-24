---
name: skip-test-with-issue
description: Skip a flaky or broken test with proper issue tracking. Use when asked to skip a test, disable a test, or mark a test as flaky.
---

# Skip a Flaky Test

Use `skip.WithIssue` from `"github.com/cockroachdb/cockroach/pkg/testutils/skip"`
to skip a test with a tracking issue number.

## Usage

Ask the user for the GitHub issue number, then add the skip after the defer
statements:

```go
func TestFlakyTest(t *testing.T) {
    defer leaktest.AfterTest(t)()
    defer log.Scope(t).Close(t)
    skip.WithIssue(t, 167182, "flaky due to timeout")

    // test body...
}
```

## Other Common Skip Variants

The `skip` package provides several other incantations for conditional skips:

- `skip.UnderRace(t, issueNum, "reason")` - skip only when running with race detector
- `skip.UnderDeadlock(t, issueNum, "reason")` - skip only when running with deadlock detector
- `skip.UnderDuress(t, issueNum, "reason")` - skip only when running under stress/duress

Use these when a test is flaky only under specific conditions rather than universally broken.

## Notes

- Issue number is an integer, not a string
- Place skip call after defer statements, before test body
- Be specific in reason: "flaky due to timeout" not just "flaky"
- Multiple tests with same root cause can share an issue number
- Ensure the `skip` package is imported
- Run `crlfmt -w -tab 2 <file>` after editing
