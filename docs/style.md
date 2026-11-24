# Go Coding Guidelines

Our style is derived from the
[Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md).
This document covers CockroachDB-specific conventions only; readers are expected
to know standard Go and the Uber guide. Use `crlfmt -w -tab 2 <file>.go` (not
`gofmt`) to format code: 100-column code lines, 80-column comments, tab width 2.

## Wrapping Function Signatures

We use `crlfmt`'s rules for wrapping long function signatures.

While it is fairly perscriptive, it does not add or remove repeated identical
argument types, e.g. `start int, end int` vs `start, end int`; the latter form
with the omitted specifier should only be used on a single line no argument
should appear by itself on a line with no type (confusing and brittle to edits).

## Go Conventions

- **Enums**: start at `iota + 1` unless the zero value is meaningful.
- **Error flow**: reduce nesting — handle errors early and return, keeping the
  happy path at the lowest indentation level. Reduce variable scope by using
  `if err := f(); err != nil` where possible.
- **Mutexes**: embed `sync.Mutex` directly in private types; use a named `mu`
  field for exported types.
- **Channels**: size should be one or unbuffered (zero). Any other size requires
  high scrutiny and justification.
- **Slice/map ownership**: document whether a function captures or copies its
  slice/map arguments.
- **Empty slices**: prefer `var` declaration (not `[]int{}`). `nil` is a valid
  empty slice. Check emptiness with `len(s) == 0`, not `s == nil`.
- **Defer**: always use `defer` for releasing locks, closing files, and similar
  cleanup.
- **Struct initialization**: always specify field names. Use `&T{}` instead of
  `new(T)`.
- **String performance**: prefer `strconv` over `fmt` for primitive conversions.
  Avoid repeated `[]byte("...")` conversions in loops.
- **Type assertions**: always use the "comma ok" idiom.
- **Functional options**: use the `Option` interface pattern at API boundaries;
  use option structs for internal functions.

---

## Code Comments

- **Data structure comments** belong at the declaration. Explain purpose,
  lifecycle, field interactions, and what code initializes/accesses each field.
- **Function declaration comments** focus on inputs, outputs, and contract --
  not implementation.
- **Algorithmic comments** belong inside function bodies. Separate processing
  phases and explain *why*, not *what*.
- In struct, API, and protobuf definitions, there should be more comments than
  code.
- Do not narrate code ("this increments x", "now we iterate over strings").
- **Block comments** (standalone line) use full sentences with capitalization
  and punctuation. **Inline comments** (end of code line) are lowercase
  without terminal punctuation.
- **Overview and design comments** must be understandable with zero knowledge
  of the code. Minimize new terminology; define new terms immediately.
- Factually incorrect comments are bugs -- fix immediately or file an issue.
- In reviews, prefix grammar suggestions with "nit:".

### Naked bool and other simple literal parameters

For `bool` literals passed to functions, and in some cases other simple literals
like `nil`, `0`, or `1`, it can be helpful to add a C-style comment with the
parameter name, e.g. `printInfo(ctx, txn, 1 /* depth */, false /* verbose */)`.
When doing so the comment always uses the parameter name verbatim -- do not
negate it and do not substitute a different word. Another good option in such
cases is to define a named const, e.g. `const verbose, notVerbose = true, false`
to pass, or even have the function take a typed arg e.g. `type Verbosity bool`.

### Example: protobuf enum with architectural context

From `roachpb/metadata.proto` -- the enum-level comment provides a birds-eye
view of joint configs; each value explains its role in replication changes.

```proto
// ReplicaType identifies which raft activities a replica participates in. In
// normal operation, VOTER_FULL and LEARNER are the only used states. However,
// atomic replication changes require a transition through a "joint config"; in
// this joint config, the VOTER_DEMOTING and VOTER_INCOMING types are used as
// well to denote voters which are being downgraded to learners and newly added
// by the change, respectively.
//
// All voter types indicate a replica that participates in all raft activities,
// including voting for leadership and committing entries. In a joint config, two
// separate majorities are required: one from the set of replicas that have
// either type VOTER or VOTER_OUTGOING or VOTER_DEMOTING, as well as that of the
// set of types VOTER and VOTER_INCOMING.
enum ReplicaType {
  VOTER_FULL = 0;
  VOTER_INCOMING = 2;
  VOTER_OUTGOING = 3;
}
```

### Example: function body phase comments with domain depth

From `replica_command.go` -- comments explain lease semantics and range-merge
edge cases, not just control flow.

```go
func (r *Replica) executeAdminCommandWithDescriptor(
  ctx context.Context, updateDesc func(*roachpb.RangeDescriptor) error,
) *roachpb.Error {
  // Retry forever as long as we see errors we know will resolve.
  retryOpts := base.DefaultRetryOptions()
  // Randomize quite a lot just in case someone else also interferes with us
  // in a retry loop. Note that this is speculative; there wasn't an incident
  // that suggested this.
  retryOpts.RandomizationFactor = 0.5
  var lastErr error
  for retryable := retry.StartWithCtx(ctx, retryOpts); retryable.Next(); {
    // The replica may have been destroyed since the start of the retry loop.
    // We need to explicitly check this condition. Having a valid lease, as we
    // verify below, does not imply that the range still exists: even after a
    // range has been merged into its left-hand neighbor, its final lease
    // (i.e., the lease we have in r.mu.state.Lease) can remain valid
    // indefinitely.
    if _, err := r.IsDestroyed(); err != nil {
      return roachpb.NewError(err)
    }

    // Admin commands always require the range lease to begin (see
    // executeAdminBatch), but we may have lost it while in this retry loop.
    // Without the lease, a replica's local descriptor can be arbitrarily
    // stale, which will result in a ConditionFailedError. To avoid this, we
    // make sure that we still have the lease before each attempt.
    if _, pErr := r.redirectOnOrAcquireLease(ctx); pErr != nil {
      return pErr
    }
```

---

## Code Organization

### Function ordering

Order by receiver, then rough call order; constructors after type definition.

### Package naming

- **Names must be unique across the entire project**, because otherwise
  `goimports` may auto-import the wrong package.
- Use the parent name plus a suffix to achieve uniqueness: `server/serverpb`,
  `kv/kvserver`, `util/contextutil`, `util/testutil`.
- Protobuf definitions go in a `xxxxpb` sub-directory of the parent package
  (e.g. `pkg/server/serverpb`, `pkg/sql/sessiondatapb`).

### Breaking cyclical dependencies

**Interface extraction:** Extract an interface into a leaf sub-package that
both sides can import.

```go
// Package bapi defines the interface.
package bapi
type B interface { Bar() }

// Package a imports only the interface.
package a
import "b/bapi"
func Foo(b bapi.B) { b.Bar() }

// Package b imports a and implements the interface.
package b
import "a"
type bImpl struct{}
func (x bImpl) Bar() { a.Foo(x) }
```

Ensure package `a` has a mock implementation for its own unit tests.

**Dependency injection:** Use when the interface approach is impractical.

1. In package `a`: `var BarFn = func() { /* placeholder */ }`
2. In package `b`: `func init() { a.BarFn = Bar }`

The package exposing the injection slot must pass its unit tests independently,
with valid default behavior when the slot is not populated.

**`X_test` package trick:** Test files in package `X` can declare
`package X_test`, which is allowed to import packages that transitively depend
on `X`. For example, `testserver` depends on `pkg/server` which depends on
`pkg/sql`, so tests needing a test server use `package sql_test` or
`package server_test`.

---

## Data-Driven Tests and Scope of Tested Behavior

We use the [datadriven](https://github.com/cockroachdb/datadriven) package as an
alternative to table-driven tests. One of the most used implementations is
"logic tests" which execute SQL and compare results to golden files.

A well-written test should observe and assert *only* behavior directly relevant
to the functionality under test. Overly broad assertions (e.g. selecting all
system tables instead of only tables the test created) make tests brittle and
train engineers to blindly update them, defeating their purpose. A logic test
should choose its `SELECT` and `WHERE` clause so that the output is fully
determined by the test's own constructed conditions -- not by unrelated factors
like total system table count, OS version, table IDs, or range IDs.

This is particularly important to consider intentionally when working with tests
that have a `--rewrite` option as this can easily capture more output in what is
being tested than intended.

---

## Error Handling

We use [cockroachdb/errors](https://github.com/cockroachdb/errors), a superset
of Go's standard `errors` package that integrates with `cockroachdb/redact`.

### Creating and wrapping errors

```go
errors.New("connection failed")                      // static string
errors.Newf("invalid value: %d", val)                // formatted
errors.AssertionFailedf("expected non-nil pointer")  // assertion failure
errors.Wrap(err, "opening file")                     // add context
errors.Wrapf(err, "connecting to %s", addr)          // formatted context
```

Keep context succinct; avoid "failed to" which piles up:
```go
// Bad: "failed to x: failed to y: failed to create store: the error"
return errors.Newf("failed to create new store: %s", err)

// Good: "x: y: new store: the error"
return errors.Wrap(err, "new store")
```

### User-facing information

```go
errors.WithHint(err, "check your network connection")
errors.WithDetail(err, "request payload was 2.5MB")
errors.WithSafeDetails(err, "node_id=%d", nodeID)   // safe for Sentry reports
```

### Detecting errors

```go
if errors.Is(err, ErrNotFound) { /* ... */ }    // sentinel errors
var nfErr *NotFoundError
if errors.As(err, &nfErr) { /* ... */ }         // typed errors
```

### Error propagation

| Scenario | Approach |
|----------|----------|
| No additional context needed | Return original error |
| Adding context | `errors.Wrap` or `errors.Wrapf` |
| Passing through goroutine channel | `errors.WithStack` on both ends |
| Callers don't need to detect this error | `errors.Newf` |
| Hide original cause | `errors.Handled` or `errors.Opaque` |

### `%v` for potentially-nil errors

Prefer `%v` over `%s` when formatting errors that might be nil. A nil error
formatted with `%s` renders as `%!s(<nil>)`, while `%v` renders as `<nil>`.

### Error stability

How errors should affect process and session lifecycle:

- **User input / query errors** (e.g. `SELECT invalid`, `SELECT 1/0`):
  Return a regular error with an appropriate SQLSTATE code. Do not kill
  the session or the process.

- **Unexpected condition scoped to one query** (unreachable code,
  precondition failure): Return `errors.AssertionFailedf(...)`. Crash
  reports are sent automatically. Do not kill the session or the process.

- **Candidate future feature** (unsupported parameter combination, unexpected
  but valid input hitting a `default`/`else` clause): Use
  `pgerror.UnimplementedWithIssue(...)` or `pgerror.Unimplemented(...)`.
  See "Unimplemented features" below.

- **Invalid session-scoped state** (unreachable code operating on session
  state): Propagate an assertion error to the client. Kill or make the
  session read-only. Do not kill the process.

- **Invalid state on a read-only or non-persisting path** (unexpected disk
  data, bad subsystem return): Propagate an assertion error, also call
  `log.Errorf`. Ensure no data is persisted. Do not kill the process.

- **Invalid state on a data-persisting path** (post-condition failure during
  write, corruption in KV storage): Call `log.Fatalf`. This kills the
  process and sends a crash report automatically.

### SQLSTATE codes

SQLSTATE is the stable, documented error API -- not message text. Use the
same code PostgreSQL would use in an equivalent situation. Only invent new
codes when PostgreSQL has no equivalent; respect the two-character category
prefix. Verify codes with SQL logic tests.

CockroachDB-specific codes:
- **XX000** -- internal error; auto-derived for assertion failures, triggers
  a crash report when the error flows back to the client.
- **XXUUU** -- auto-chosen when no SQLSTATE is set. Reduce these over time.
- **XXA00** -- txn committed but a schema change op failed; manual
  intervention likely needed.
- **40001** -- serialization error. Txn did **not** commit; **can** be retried.
- **40003** -- statement completion unknown. Txn **may or may not** have
  committed; manual intervention likely needed.

See `pkg/sql/pgwire/pgcode/codes.go` for the full list.

### Errors as API

- **Messages, hints, and details** can be changed freely without release notes.
- **SQLSTATE values** are the stable contract. New codes or splitting one code
  into multiple alternatives requires a release note.

### Message formatting

- **Message**: lowercase start, no trailing period, no newlines. Keep it
  short and descriptive.
- **Hint / Detail**: full sentences (capital start, period at end). May be
  multi-line. Hints tell the user what to *do*; details elaborate on what
  *happened*.

### Large strings

Do not include arbitrarily large strings in error payloads -- they cause
excessive memory use and truncated crash reports. When in doubt, truncate
to a prefix and append a unicode ellipsis (`…`).

### Unimplemented features

Mark unsupported-but-plausible functionality with
`pgerror.UnimplementedWithIssue(issueNum, ...)` or
`pgerror.Unimplemented(...)`. Label the tracking issue with `docs-todo`,
`known-limitation`, and `X-anchored-telemetry`. Unimplemented errors get
their own non-crash telemetry automatically.

### Incomplete implementations

Merging incomplete implementations is acceptable as long as every
unimplemented code path uses `unimplemented.New(...)` (or the variants
above). Track each gap with a GitHub issue and a `// TODO(#NNNNN)`
comment at the call site.

---

## Log and Error Redactability

Data in logs and errors is **unsafe (PII-laden) by default** and will be
redacted before being sent to Cockroach Labs. Mark information as safe to
preserve observability. Always use `cockroachdb/errors` (not `fmt.Errorf`) so
that error messages are automatically redactable.

### What is automatically safe

- Format string literals in `errors.New`/`Newf`/`Wrap`/`log.Infof`/etc.
- Booleans, integers, floats, `time.Time`, `time.Duration`.
- The redactable contents of existing `error` values when wrapped.
- Everything else is unsafe by default.

### Implementing redactability on your types

| Scenario | Approach |
|----------|----------|
| Type contains only IDs, counts, or other non-PII (e.g. `NodeID`) | Implement `SafeValue` and update the allowlist in `pkg/testutils/lint/passes/redactcheck/redactcheck.go`. |
| Type has a mix of safe and unsafe fields | Implement `SafeFormatter` (preferred). |

**Prefer `SafeFormatter` over `SafeValue`** -- it is more precise, does not
require linter allowlist changes, and is resistant to someone later adding
unsafe fields to the type.

#### Example: migrating `String()` to `SafeFormat()`

```go
// Before: entire output is considered unsafe.
func (m MetricSnap) String() string {
        suffix := ""
        if m.ConnsRefused > 0 {
                suffix = fmt.Sprintf(", refused %d conns", m.ConnsRefused)
        }
        return fmt.Sprintf("infos %d/%d sent/received, bytes %dB/%dB sent/received%s",
                m.InfosSent, m.InfosReceived,
                m.BytesSent, m.BytesReceived,
                suffix)
}

// After: output is safe (all fields are integers, format strings are literals).
func (m MetricSnap) SafeFormat(w redact.SafePrinter, _ rune) {
        w.Printf("infos %d/%d sent/received, bytes %dB/%dB sent/received",
                m.InfosSent, m.InfosReceived,
                m.BytesSent, m.BytesReceived)
        if m.ConnsRefused > 0 {
                w.Printf(", refused %d conns", m.ConnsRefused)
        }
}

func (m MetricSnap) String() string { return redact.StringWithoutMarkers(m) }
```

### Composing redactable strings

- `redact.Sprint()` / `redact.Sprintf()` -- create a `RedactableString` from
  mixed safe/unsafe values.
- `redact.StringBuilder` -- build a `RedactableString` programmatically.
- Store `redact.RedactableString` in struct fields that will appear in logs,
  instead of plain `string`.

### Don'ts

- **Don't** use `redact.Safe()` / `errors.Safe()` / `log.Safe()` -- these are
  fragile promises that break when someone changes the argument's type.
- **Don't** cast a `string` to `RedactableString` manually -- use
  `redact.Sprintf` instead.
- **Don't** use `SafeValue` on complex types -- someone may later add an unsafe
  field without noticing.

### Testing redactability

Use `echotest` to lock down `SafeFormat` output:

```go
func TestMyType_SafeFormat(t *testing.T) {
    v := MyType{ID: 1, Name: "sensitive"}
    redacted := string(redact.Sprint(v))
    echotest.Require(t, redacted, datapathutils.TestDataPath(t, t.Name()))
}
```

Run with `-rewrite` to generate the initial testdata file, then verify markers
appear around unsafe fields.

---

## Working with Protobufs

CockroachDB uses a [gogoproto fork](https://github.com/cockroachdb/gogoproto).
Proto definitions live in `.proto` files; generated `.pb.go` files are
regenerated by `./dev gen protobuf` or `./dev build short`.

* Don't use `required` -- it breaks backward/forward compatibility between
  nodes running different binaries.
* Use `gogoproto.nullable` so the compiled field type is the value itself
  (not a pointer). Absence is represented as the zero value. Exceptions:
  when you need to distinguish zero from absent, or when the message type
  is very large and pointer copying is cheaper.
* Don't use `optional` -- use `gogoproto.nullable` instead if you need
  presence semantics.
* Avoid `types.Any` and `json` types -- they are very hard to decode when
  inspecting debug zips.

**Gotcha:** Empty proto fields can have non-zero marshalling overhead on
hot paths. Use the
[omitEmpty field](https://github.com/cockroachdb/gogoproto/pull/2) to
ensure truly empty encoding.

---

## SQL Column Labels

When defining result column labels for statements or virtual tables:

* Use consistent casing; use underscores to separate words (consistent with
  `information_schema`), e.g. `start_key` not `startkey`.
* The same concept across different statements should use the same label, e.g.
  `variable` and `value` match between `SHOW ALL CLUSTER SETTINGS` and
  `SHOW SESSION ALL`.
* Labels must be usable without quoting. Avoid SQL keywords: `table_name` not
  `"table"`, `index_name` not `"index"`. Never name a column `"user"` -- use
  `user_name`.
* Avoid abbreviations unless universally used in spoken English (`id` is fine,
  `loc` is not).
* For "primary key" columns, disambiguate overloaded words: `zone_id`, `job_id`,
  `table_id` instead of just `id`.
* For objects with multiple handle types (ID, name), disambiguate in the label:
  `table_name` not `table`, so `table_id` can be added later.
* Reuse labels from `information_schema` or existing `SHOW` statements when they
  match the principles above.
