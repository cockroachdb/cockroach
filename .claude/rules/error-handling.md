---
globs: ["**/*.go"]
---

# CockroachDB Error Handling

CockroachDB uses [cockroachdb/errors](https://github.com/cockroachdb/errors),
a superset of Go's standard `errors` package. It interoperates with
`cockroachdb/redact` - ensure error constructor arguments have proper redaction.

## Creating Errors

```go
errors.New("connection failed")                        // static string
errors.Newf("invalid value: %d", val)                  // formatted
errors.AssertionFailedf("expected non-nil pointer")    // assertion failure (louder alerts)
```

## Wrapping and Adding Context

```go
return errors.Wrap(err, "opening file")                // preferred over fmt.Errorf with %w
return errors.Wrapf(err, "connecting to %s", addr)     // formatted context
```

Keep context succinct; avoid "failed to" which piles up:
```go
// Bad: "failed to x: failed to y: failed to create store: the error"
return errors.Newf("failed to create new store: %s", err)

// Good: "x: y: new store: the error"
return errors.Wrap(err, "new store")
```

## User-Facing Information

```go
errors.WithHint(err, "check your network connection")  // hints for end-users
errors.WithDetail(err, "request payload was 2.5MB")    // details for developers
```

## Detecting and Handling Errors

```go
// Sentinel errors.
var ErrNotFound = errors.New("not found")
if errors.Is(err, ErrNotFound) { /* ... */ }           // works across network boundaries

// Custom error types.
var nfErr *NotFoundError
if errors.As(err, &nfErr) { /* ... */ }
```

## Error Propagation

| Scenario | Approach |
|----------|----------|
| No additional context needed | Return original error |
| Adding context | `errors.Wrap` or `errors.Wrapf` |
| Passing through goroutine channel | `errors.WithStack` on both ends |
| Callers don't need to detect this error | `errors.Newf` |
| Hide original cause | `errors.Handled` or `errors.Opaque` |

## Safe Details for PII Protection

Error messages are redacted by default in Sentry reports:
```go
errors.WithSafeDetails(err, "node_id=%d", nodeID)     // mark specific values as safe
errors.Newf("processing %s", errors.Safe(operationName))
```

## Key Files

- `/docs/RFCS/20190318_error_handling.md` - Error handling RFC
- [cockroachdb/errors README](https://raw.githubusercontent.com/cockroachdb/errors/refs/heads/master/README.md) - Full API documentation
