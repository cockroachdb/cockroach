- Feature Name: Error handling
- Status: draft
- Start Date: 2019-03-18
- Authors: knz
- RFC PR: [#36987](https://github.com/cockroachdb/cockroach/pull/36987)
- Cockroach Issue: [#35854](https://github.com/cockroachdb/cockroach/issue/35854)
  [#36918](https://github.com/cockroachdb/cockroach/issue/36918)
  [#24108](https://github.com/cockroachdb/cockroach/issue/24108)

# Summary

This RFC explains how our requirements for error handling have grown
over time and how the various code patterns currently in use in
CockroachDB are inadequate.

It then proposes a new library of error types. This library is
compatible with the `error` interface, including the upcoming [Go 2
semantics](Go-error-handling-v2-). Additionally:

- it provides `Wrap` primitives akin to those found in
  `github.com/pkg/errors`.
- it is compatible with both the `causer` interface (`Cause() error`) from
  `github.com/pkg/errors` and the `Wrapper` interface (`Unwrap() error`) from Go 2.
- it preserves the entire structure of errors across the wire (using
  protobuf encoding).
- it enables fast, reliable and secure determination of whether
  a particular cause is present (not relying on the presence of a substring in the error message).
- it preserves reportable details void of PII using the same
  infrastructure as the `log` package (`log.Safe`).
- it provides "sane" handling of assertion errors, in a way
  that properly masks the presence of particular causes.

The library is also upward-compatible from current uses of `roachpb`
errors and `github.com/pkg/errors`: errors of these types can be
converted "after the fact" to the new structured error types and their
details are preserved. This makes it possible to introduce the new
library gradually without having to rewrite all the code at once.

The expected benefits include:

- better learnability for new contributors;
- easier maintainability;
- less vulnerability to string injection (better security);
- richer details reported to telemetry for "serious errors";
- more details available for troubleshooting unexpected errors in tests.

Note: an [early prototype has been implemented in PR
#36023](https://github.com/cockroachdb/cockroach/pull/36023) however
it does not fully reflect (as of 2019-04-22) the design presented in
this RFC.

Table of contents:

- [Motivation](#Motivation)
- [Guide-level explanation](#Guide-level-explanation)
- [Reference-level explanation](#Reference-level-explanation)
  - [Detailed design](#Detailed-design)
  - [Drawbacks](#Drawbacks)
  - [Rationale and Alternatives](#Rationale-and-Alternatives)
  - [Unresolved questions](#Unresolved-questions)
- [Appendices](#Appendices)
  - [Problematic error use cases](#Problematic-error-use-cases)
  - [Error handling outside of CockroachDB](#Error-handling-outside-of-CockroachDB)

# Motivation

- [Too much diversity](#Too-much-diversity)
- [Unreliable "standard" ways to inspect errors](#Unreliable-standard-ways-to-inspect-errors)
  - [Vulnerability to embedded application strings](#Vulnerability-to-embedded-application-strings)
  - [Improper/unsafe testing of intermediate causes](#Improper-unsafe-testing-of-intermediate-causes)
  - [Excessively loose coupling](#Excessively-loose-coupling)
  - [Mismatched audiences: code vs humans](#Mismatched-audiences-code-vs-humans)
- [Unreliable reliance on the pg error code](#Unreliable-reliance-on-the-pg-error-code)
- [Blindness to error causes in telemetry](#Blindness-to-error-causes-in-telemetry)
- [Barrier errors with debugging details](#Barrier-errors-with-debugging-details)
- [Stack traces for troubleshooting](#Stack-traces-for-troubleshooting)
- [Errors subsumed by other errors](#Errors-subsumed-by-other-errors)
- [Errors subsumed by non-errors](#Errors-subsumed-by-non-errors)
- [Motivation for a new error type: summary](#Motivation-for-a-new-error-type-summary)

## Too much diversity

There are currently 5 different error handling "protocols" inside
CockroachDB, including a mix of CockroachDB-specific error types and
multiple 3rd party error packages.

This diversity makes the code difficult to approach for newcomers, and
difficult to maintain. One has to always remember "which errors should
I use in which context?"

## Unreliable "standard" ways to inspect errors

Go provides 4 "idiomatic" ways to inspect errors:

1. reference comparison to global objects, e.g. `err == io.EOF`
2. type assertions to known error types, e.g. `err.(*os.PathError)`
3. predicate provided by library, e.g. `os.IsNotExists(err)`
4. string comparison on the result of `err.Error()`

Method 1 breaks down when using wrapped errors, or when transferring
errors over the network. See instances in section [Suspicious
comparisons of the error
object](#Suspicious-comparisons-of-the-error-object).

Method 2 breaks down if the error object is converted to a different
type, as happens currently in CockroachDB when a non-`roachpb` error
is transferred through the network. When wire representations *are*
available, the method is generally reliable; however, if errors are
implemented as a chain of causes, care should be taken to perform the
test on all the intermediate levels. See instances in section
[Suspicious assertions on the error
type](#Suspicious-assertions-on-the-error-type).

Method 3 is generally reliable although the predicates in the standard
library obviously do not know about any additional custom types. Also,
the implementation of the predicate method can be cumbersome if one
must test errors from multiple packages (dependency cycles). Also, the
method loses its reliability if the predicate itself relies on one of
the other methods in a way that's unreliable. For example, the current
predicates in CockroachDB's `sqlbase` package are defective in this
way.

Method 4 is the most problematic and unfortunately has been used
pervasively inside CockroachDB. It has several sub-problems, detailed
in the following sub-sections. See also the section [Suspicious
comparisons of the error
message](#Suspicious-comparisons-of-the-error-message) at the end for
the list of suspicious cases in the code.

### Vulnerability to embedded application strings

The main problem with comparing an error message to some reference
string is that the reference can appear in one of the application
values embedded inside the error. For example, consider the error
produced thus:

```
root@127.0.0.1:34312/defaultdb> select 'connection reset by peer'::int;
pq: could not parse "connection reset by peer" as type int: strconv.ParseInt: parsing "connection reset by peer": invalid syntax
```

And the test in `pgwire/pgerror/errors.go` function
`IsSQLRetryableError()` which merely checks whether the error contains
the string `"connection reset by peer"`. This method would return
`true` in that case, whereas the error is not retriable.

This problem is in fact a form of *bad value injection* and creates a
vector by which a remote user can misuse the internals of CockroachDB
(a security vulnerability).

**In general, string comparisons on error messages are vulnerable to
injection and can even cause security problems.**

### Improper/unsafe testing of intermediate causes

When, say, a retry error happens while handling a duplicate insertion error,
we want a quick way to determine the error is retryable.

Testing whether the entire error message contains some reference
string can work but is neither fast nor reliable (see previous section).

In the (relatively uncommon case) of a constant string prefix, one can
rely on exact matches to the first argument provided to
`errors.Wrap`. In that case, the prefix provided as 2nd
argument to `errors.Wrap()` can be extracted as follows:

1. `s1 := err.Error()`
2. `s2 := err.Cause().Error()`
3. `prefix_message := s1[:len(s2)-len(s1)]`

This provides precise extraction of intermediate layers, at the
expense of performance.

Moreover, this technique is currently not used anywhere in
CockroachDB.

### Excessively loose coupling

Consider the following code sample:

```
pkg/sql/schema_changer.go:      if strings.Contains(err.Error(), "must be after replica GC threshold") {
```

This implicitly refers to this error:

```
pkg/roachpb/errors.go:func (e *BatchTimestampBeforeGCError) message(_ *Error) string {
pkg/roachpb/errors.go:  return fmt.Sprintf("batch timestamp %v must be after replica GC threshold %v", e.Timestamp, e.Threshold)
```

The problem here is that if a programmer modifies the text of the
error message in `roachpb`, there is no direct feedback to tell them
they should also update the code in the SQL schema changer.

In the lucky case, there might be a unit test that trips up,
but what this really needs is some way for the schema changer code to
ascertain the error was originally a
`roachpb.BatchTimestampBeforeGCError` object.

### Mismatched audiences: code vs humans

The choice to convey precise information via an error message, for subsequent
testing in code, may prevent further tuning of that message to become more helpful
to human users.

For example, consider the code in `replica_command.go` which does
different things depending on whether the error message indicates that
the store is "amost out of disk space" or "busy applying snapshots".

If (hypothetically) a product management study found out that users
find the distinction confusing and would be better satisfied by
merging the two errors into one message "store is too busy", the code
would need some heavy lifting to preserve the distinction in behavior.

**In general, error messages should be the domain of humans, and
precise information for internal use should be conveyed using
structural data — either separate types or dedicated structure
fields.**

## Unreliable reliance on the pg error code

PostgreSQL clients expect and use a 5-character "SQL state" also
called "pg error code".  These codes are grouped in broad categories
identified by the first two characters. The SQL wire protocol separates
the 5-character code from the rest of the error message.

Today CockroachDB's source code provides and uses pg codes
inconsistently:

- from the perspective of PostgreSQL clients, these codes are
  *informational* and (with the exception of `40001`) generally not required
  to uniquely and specifically describe particular situations.

  For example, `CodeUniqueViolationError` (`23505`) is meant to be
  produced when introducing a duplicate row in a unique index, however:
  - it is not guaranteed to be produced in every case (e.g. it can be
    "taken over" by `40001`),
  - or it can be produced by multiple situations that are only vaguely
    related (e.g. both a direct INSERT/UPSERT or an index backfill).

- some internal components inside CockroachDB have grown to require
  *precise* codes that uniquely identify particular situations.

  This happened because of the problem outlined in the previous
  section: the lack of reliable mechanism to test/identify
  intermediate error situations.

  For example, the code `cockroach user` expects the creation of
  existing users to fail with `CodeDuplicateObjectError` (this is a
  bug, incidentally, since a duplicate user insert would fail with
  `CodeUniqueViolationError`, a different code).

  Another example is the code `row_container.go` that expects
  "out of memory" errors from the `util/mon` package to appear
  as pg errors with code `CodeOutOfMemoryError`. It does not consider
  that a separate part of the source code could produce the same
  code *informationally* (towards SQL clients).

**In general, with the exception of certain "critical" codes (`40001`
and the `XX...` codes) the specific values of the pg code should never be
assumed to be precise nor used to determine further behavior inside
CockroachDB.**

See section [Suspicious reliance on the pg error
code](#suspicious-reliance-on-the-pg-error-code) for examples of
use of pg codes with mistaken assumptions.

## Blindness to error causes in telemetry

We want to report important errors to telemetry (Sentry) for further
research. However the report must be stripped of PII. We want error
objects that preserve the "safe" part of details available when the
error was produced or wrapped.

A mechanism to achieve this is already available in the `log` packages
for the sentry reports produced upon `log.Fatal`. The calling code can
enclose arguments to the call with `log.Safe` to indicate the argument
is suitable for shipping in a telemetry report. The format string (the
first argument) itself is also considered safe. This mechanism is
opt-in: we consider that any string is un-safe for reporting by
default.

Currently, all error objects in CockroachDB except for `pgerror.Error`
are unable to distinguish safe sub-strings and must thus be stripped
of all details when shipped to telemetry.

## Stack traces for troubleshooting

When an error becomes serious enough, we find it useful to find out
where in the code it was generated, and with which call stack.

For this purpose, the package `github.com/pkg/errors` helpfully embeds
the caller stack strace every time a leaf error is instantiated,
and every time an error is wrapped.

This stack trace is printed out upon formatting the error with `%+v`,
or, only for wrapped errors, can be extracted via the `StackTrace()` method.

The `pgerror.Error` object also captures:
- the first caller (file, line, function) in the `Source` field, always;
- stack traces when instantiated via the `AssertionFailedf` constructor
  or when wrapping another assertion error.

Unfortunately, stack traces are not collected for the other error types,
and the stack traces collected by `github.com/pkg/errors` are not
reported to telemetry.

## Barrier errors with debugging details

When an assertion failure is encountered while handling an error, we want
to mark the assertion failure to be an "unrecoverable error" and let it flow
to the top level (and telemetry) without any form of further handling.

This type of error must inhibit any other mechanism that inspects causes.

*At the same time* we want to preserve the context of the causes for troubleshooting.
Therefore something like `fmt.Errorf("unrecoverable: %s", err)` is inadequate
because it would drop all the additional details contained in `err`.

(In fact, embedding the original error in the message for the new error is unsafe
because it does not inhibit methods that inspect the message.)

**In general, we need an error wrapping type which preserves all the details
of its cause(s) for troubleshooting, but eliminates all its semantic value.**

We will cause such a type an "error barrier" in the rest of the RFC.

Currently `pgerror.AssertionFailedf` acts as an error barrier and
attempts to preserve many details of its cause, but some details are
lost. No other error mechanism in CockroachDB successfully achieves
the role of barrier.

## Errors subsumed by other errors

Consider the following code sample:

```go
// Try an INSERT.
if err := txn.Exec(txn1); err != nil {
  if sqlbase.IsDuplicateInsertError(err) {
     // Try with an UPDATE instead.
     if err := txn.Exec(txn2); err != nil {
        return errors.Wrap(err, "while updating")
     }
	 return nil
  }
  return errors.Wrap(err, "while inserting")
}
```

This code is defective, because if `txn2` fails, the original `err` object is lost.

One can try to "fix" as follows:

```go
if origErr := txn.Exec(txn1); origErr != nil {
  if sqlbase.IsDuplicateInsertError(origErr) {
     if newErr := txn.Exec(txn2); err != nil {
        return errors.Wrap(origErr, "while updating")
...
```

But then it's `newErr` that gets lost when `txn2` fails.

We can try to "fix" as follows:

```go
if origErr := txn.Exec(txn1); origErr != nil {
  if sqlbase.IsDuplicateInsertError(err) {
     if newErr := txn.Exec(txn2); err != nil {
        return errors.Wrapf(newErr, "while updating after insert error: %v", origErr)
...
```

This is slightly better, however if the `origErr` was structured, all
its structure is lost by string-ification into a message.  See also
the section [Suspicious flattening of
errors](#Suspicious-flattening-of-errors) for a list of potential
information loss in the current source code.

**In general, patterns of code like if-error-do-something-else need
structure that's richer than a simple linked list to preserve all the
error details.**

(This suggest some form of tree instead.)

## Errors subsumed by non-errors

Moreover, in the code above another question happens: what if the
substitute handling succeeds, i.e. there should be "no error" in the
end? This occurs e.g. if `txn1` fails but `txn2` succeeds.

Today in CockroachDB the original error object gets dropped on the
floor. This is a problem because if the new behavior then causes a
_later_ error, the chain of events leading to that later error is
lost.

The general problem is that
"if-error-do-something-else-that-is-not-an-error" loses information.
There are currently only a few such patterns in CockroachDB but they
are all defective in the same way.

**In general, if-error-do-something-else-that-is-not-an-error is code smell
that should be avoided. It suggests that the original error object
was actually something else than error, for example it should have
been part of the function protocol.**

For example, "duplicate error" should not be a thing in CockroachDB,
instead methods that "insert" values should return a separate boolean
value that indicates whether a duplicate was detected, independently
from the error return. Ditto for "retry errors" and the other places
where this pattern is used.

Of course, independently from error handling we should also consider
detailing these choices for an alternate path in logs and event
traces.

These situations are largely out of scope for this RFC, except for the
following: **we should aim to audit the code and ensure that no
barrier errors is never dropped** and instead let to flow where it can
be collected for telemetry and/or sent to a client where it can be
perceived by a human.

## Motivation for a new error type: summary

The requirements on error objects have grown over time.

- **Structured error causes.**
  When an error is raised in the context of handling another error, we want to remember the context.
  So we need a "decorator" object with a link to the original error.
  Moreover, to support the "if-error-do-something-else" pattern we need
  to be able to store more than one cause at a given level.

- **Wire format.**
  CockroachDB is a distributed system and errors can flow over the network. We want error objects
  that have a wire representation that preserves all the error details.

- **Safe telemetry details.**
  We want to report important errors to telemetry (Sentry) for further inspection. However
  the report must be stripped of PII. We want error objects that preserve the "safe" part
  of details available when the error was produced or wrapped.

- **pg error codes.**
  PostgreSQL clients expect and use a 5-character "SQL state" also called "pg error code".
  These codes are grouped in broad categories identified by the first two characters.
  An error object that ultimately flows to a SQL client must provide a meaningful,
  relevant pg code.

- **Stack traces for troubleshooting.**
  The point where an error is handled and becomes worthy of debugging
  attention can be far away from the point it was generated. It is thus
  useful/desirable to embed the caller stack trace in the generated
  error object.

- **[Barrier error type](#Barrier-errors-with-debugging-details) with preservation of debugging details.**
  In certain cases we want to preserve the cause for troubleshooting
  but prevent the rest of the code from observing its semantic value.

Several error packages and struct types are currently in use in CockroachDB.

**None of them satisfy all the requirements:**

| Error package/struct                           | Used in CockroachDB? | Structure         | Wire format | Safe telemetry details | pg code | Stack traces | Barrier with details |
|------------------------------------------------|----------------------|-------------------|-------------|------------------------|---------|--------------|----------------------|
| `golang.org/pkg/errors`, `errorString`         | Yes                  | (leaf)            | No          | No                     | No      | No           | No                   |
| `github.com/pkg/errors`, `fundamental`         | Yes                  | (leaf)            | No          | No                     | No      | Yes          | No                   |
| `github.com/pkg/errors`, `withMessage`         | Yes                  | linked list       | No          | No                     | No      | No           | No                   |
| `github.com/pkg/errors`, `withStack`           | Yes                  | linked list       | No          | No                     | No      | Yes          | No                   |
| `github.com/hashicorp/errwrap`, `wrappedError` | No                   | binary tree       | No          | No                     | No      | No           | Yes                  |
| `upspin.io/errors`, `Error`                    | No                   | linked list       | Yes         | Yes                    | No      | No           | No                   |
| Go 2 (presumably new types)                    | No                   | linked list       | No          | ?                      | No      | ?            | No                   |
| (CRDB) `roachpb.Error`                         | Yes                  | single leaf cause | Yes         | No                     | No      | No           | No                   |
| (CRDB) `distsqlpb.Error`                       | Yes                  | single leaf cause | Yes         | No                     | Yes     | No           | No                   |
| (CRDB) `pgerror.Error` (2.1/previous)          | Yes                  | (leaf)            | Yes         | Yes                    | Yes     | Yes          | Yes                  |
| (CRDB) proposed new `Error` object             | Not yet              | tree              | Yes         | Yes                    | Yes     | Yes          | Yes                  |

The table above can be further simplified as follows:

| Error package/struct                           | Structure | Wire format | Safe telemetry details | pg code | Stack traces | Barrier with details |
|------------------------------------------------|-----------|-------------|------------------------|---------|--------------|----------------------|
| `golang.org/pkg/errors`, `errorString`         | BAD       | BAD         | BAD                    | BAD     | BAD          | BAD                  |
| `github.com/pkg/errors`, `fundamental`         | BAD       | BAD         | BAD                    | BAD     | good         | BAD                  |
| `github.com/pkg/errors`, `withMessage`         | BAD       | BAD         | BAD                    | BAD     | BAD          | BAD                  |
| `github.com/pkg/errors`, `withStack`           | BAD       | BAD         | BAD                    | BAD     | good         | BAD                  |
| `github.com/hashicorp/errwrap`, `wrappedError` | good      | BAD         | BAD                    | BAD     | BAD          | BAD                  |
| `upspin.io/errors`, `Error`                    | BAD       | good        | good                   | BAD     | BAD          | BAD                  |
| Go 2 (presumably new types)                    | BAD       | BAD         | ?                      | BAD     | ?            | BAD                  |
| (CRDB) `roachpb.Error`                         | BAD       | good        | BAD                    | BAD     | BAD          | BAD                  |
| (CRDB) `distsqlpb.Error`                       | BAD       | good        | BAD                    | good    | BAD          | BAD                  |
| (CRDB) `pgerror.Error` (2.1/previous)          | BAD       | good        | good                   | good    | good         | good                 |
| (CRDB) proposed new error objects              | good      | good        | good                   | good    | good         | good                 |

This failure by the current code to meet all our requirements is the main motivation for this work.

# Guide-level explanation

The package is `github.com/cockroachdb/cockroach/pkg/errors`.

Table of contents:

- [Instantiating new errors](#Instantiating-new-errors)
- [Decorating existing errors](#Decorating-existing-errors)
- [Utility features](#Utility-features)
  - [Adding `context`](#Adding-context)
  - [Formatting variants](#Formatting-variants)
  - [Safe details](#Safe-details)
  - [Additional PostgreSQL error fields](#Additional-PostgreSQL-error-fields)
  - [Telemetry keys](#Telemetry-keys)
  - [Depth variants](#Depth-variants)
- [Handling chains of error causes](#Handling-chains-of-error-causes)
  - [Accessing the cause](#Accessing-the-cause)
  - [Preservation of causes across the wire](#Preservation-of-causes-across-the-wire)
  - [Barrier errors](#Barrier-errors)
  - [Multiple causes](#Multiple-causes)
  - [Identification of causes](#Identification-of-causes)
  - [Error equivalence and markers](#Error-equivalence-and-markers)
- [Promotion and preservation during formatting](#Promotion-and-preservation-during-formatting)
- [What comes out of an error?](#What-comes-out-of-an-error)

## Instantiating new errors

Instantiating a new error can be as simple as `errors.New("hello")` or
`errors.Errorf("hello %s", "world")`.

To equip a code useful to PostgreSQL clients when constructing a new
error, the following form is preferred:
`errors.New("hello").WithDefaultCode(pgerror.CodeSyntaxError)`

The library is compatible with existing protobuf error objects, so
instantiating, for example, with `err :=
&roachpb.RangeRetryError{Reason: "hello"}` is also valid.

## Decorating existing errors

Adding some words of context can be as simple as `errors.Wrap(err,
"hello")`, although the form `Wrapx()` is preferred (see next section).

To add a code useful to PostgreSQL clients, one can use
e.g. `errors.WithDefaultCode(err, pgerror.CodeSyntaxError)`.
The new code is only used if the original error did not provide a code already.

## Utility features

The following features are opt-in and can be used to enhance the
quality of error details included in telemetry or available for
troubleshooting.

### Adding `context`

When wrapping an error with `errors.WithCtx(err, ctx)`, the logging
tags embedded in `ctx` (if any) are made available in the error object
and will be used when printing error details.

The following additional convenience forms are available:

- `errors.New("hello").WithCtx(ctx)` during initial construction
- `errors.WithCtx(err, ctx, "hello")`
- `errors.Wrapx(err, ctx, pgerror.CodeSyntaxError, "hello")`

  (combines `WithCtx`, `Wrap` and `WithDefaultCode`; preferred)

### Formatting variants

The library contains `XXXf()` variants for the decorating functions
that accept a format string and a variable argument list.

- `New` -> `Newf`
- `Wrap` -> `Wrapf`
- `Wrapx` -> `Wrapxf`
- etc.

A linter enforces that the formatting variants are used properly.

### Safe details

In some cases errors are packaged and shipped to telemetry (Sentry)
for further investigation.  To ensure that no personally identifiable
information (PII) is leaked, most of the details of an error are
masked.

Only the pg code (if any) and stack trace(s) (if any) are shipped by default.

When using the formatting variants (`Newf`, `Wrapf` etc) from the
library, additionally the format string is shipped to telemetry,
together with the value of any subsequent positional argument
constructed using `log.Safe` from
`github.com/cockroachdb/cockroach/pkg/util/log` (aliased to
`errors.Safe` for convenience).

For example: `errors.Newf("hello %s", log.Safe("world"))` will
cause both the strings `hello %s` and `world` to become available
in telemetry details.

### Additional PostgreSQL error fields

PostgreSQL error objects contain additional fields that are displayed
in a special way by PostgreSQL clients and are generally useful to
human users. These include:

- the "details" field. This is used e.g. for syntax errors to print
  where in the input SQL string the error was found using ASCII art.

- the "hint" field. This is used to suggest a course of action to the
  user. For example we use this to tell the user to search on Github
  or open an issue if they encounter an internal error or an error due
  to a feature in PostgreSQL that is not supported in CockroachDB.

- the "source" field. This is the file, line, function information
  where the original error was found.

In the proposed library, the postgres details can be added:

- during construction, using `WithDetail()` or `WithDetailf()`,
  e.g. `errors.New("hello).WithDetail("world")`
- when wrapping, using `errors.WrapWithDetail(err, "some detail")`.

When multiple errors contain details, the detail strings are concatenated
to produce the final error packet sent to the SQL client.

The detail strings are not considered "safe" for reporting.

Similarly, hints can be added during construction using `WithHint()`
and when wrapping using `WrapWithHint`. Hints are not considered safe
for reporting either.

The source field is further automatically populated by the library.

### Telemetry keys

Throughout the SQL package (and presumably over time throughout
CockroachDB) errors can be annotated with "telemetry keys" to be
increment when the error flows out of a server towards a client.

This is used to e.g. link errors to existing issues on Github.

The following APIs are provided (upward-compatible from existing `pgerror` client code):

```go
func UnimplementedWithIssue(issue int, format string, args ...interface{})
func UnimplementedWithIssueDetail(issue int, detail, msg string)
func UnimplementedWithIssueHint(issue int, msg, hint string)
...
```

The telemetry keys are stored in the error chain and can be retrieved
via the accessor `TelemetryKeys() []string`.

### Depth variants

The library often embeds a stack trace in error objects. This stack trace
starts at the level above the error constructor (or wrapping function) by default.
This can be customized using the `Depth` variants:

- `New("hello")` vs `NewDepth(1, "hello")`
- `Newf("hello %s", "world")` vs `NewDepthf(1, "hello %s", "world")`
- etc.

## Handling chains of error causes

### Accessing the cause

The error types in the library implement the `causer` interface and Go
2's `Wrapper` interface. It is thus possible to retrieve the layers of
cause via the `Cause()` or `Unwrap()` methods.

This comes with caveats; see the remaining sections for details.


### Preservation of causes across the wire

The library's types are protobuf-encodable and thus naturally their entire
structure is preserved when transferred across the network.

To ensure maximum convenience, some additional magic is provided
to cover the following use case:

1. a crdb/encodable error is constructed;
2. it passes through some package which uses `errors.Wrap` (from `github.com/pkg/errors`, not the new library);
3. the resulting error is sent across the wire.

When this occurs, the library makes extra effort to convert the
wrapper object from `github.com/pkg/errors` into a form that's
encodable, so as to preserve all the chain of causes and the
intermediate message prefixes added via `github.com/pkg/errors.Wrap()`.

### Barrier errors

When an assertion failure is encountered while handling an error, we want
to mark the assertion failure to be an "unrecoverable error" and let it flow
to the top level (and telemetry) without any form of further handling.

This type of error must inhibit any other mechanism that inspects causes.

For this purpose the library provides a special error type:
`BarrierError`.  This contains an optional "internal cause" however
*this cause is not visible to the `Cause()` and `Unwrap()` methods*
(nor the other mechanisms provided below). This original cause is only
visible:

- when inspecting the error object, e.g. via `%+v` formatting;
- when the error flows out of the system (e.g. towards a SQL client);
- when reported to telemetry.

The following functions create barrier errors:

- `errors.AssertionFailed()`
- `errors.NewAssertionErrorWithWrappedErr()`

Additionally, a barrier error interacts with the pg error code as follows:

- if the original pg error code was absent or was for a "normal" error,
  the barrier error will mask the original code and replace it
  with `XX000/CodeInternalError`.

- if the original pg error code was present with the `XX` prefix, then
  the original code is preserved. This covers the following situation:

  1. a low level storage failure is encountered and generates
     `CodeDataCorruptedError` (`XX0001`)
  2. the storage failure trips an unexpected path in the code (an
     assertion failure.)

  In that case we wish that the assertion failure preserves the
  serious original code.

### Promotion of `XX` codes to barrier errors

When a `XX` code is provided via `Wrapx`, `WithDefaultCode`, etc, the
result becomes a barrier and the original error, if any, is masked as an
"internal cause" invisible via `causer`/`Wrapper` interfaces.

This makes it possible to e.g. wrap with `CodeCCLRequired` (`XXC01`)
on a CCL-only path and ensure that the original error is prevented
from triggering further processing (e.g. retries).

### Multiple causes

CockroachDB contains multiple code patterns that try something, then
if that first something results in an error try something else.

If the second action itself results in error, there are then *two* error objects.

Prior to this RFC, one of the errors would be "dropped on the floor"
or, at best, flattened into a text message with
e.g. `errors.Wrapf(err1, "while handling %v", err2)`.

The proposed library extends this behavior and makes it possible to
preserve multiple causes using `WithOtherCause()`, for example:

```go
// Try an INSERT.
if origErr := txn.Exec(txn1); origErr != nil {
  if sqlbase.IsDuplicateInsertError(origErr) {
     // Try with an UPDATE instead.
     if newErr := txn.Exec(txn2); err != nil {
        return errors.Wrap(newErr, "while updating").WithOtherCause(origErr)
     }
	 return nil
  }
  return errors.Wrap(origErr, "while inserting")
}
```

The "other" error causes annotated in this way are invisible to the
`Cause()` and `Unwrap()` methods, however they are used
for telemetry reports and can be inspected for troubleshooting with `%+v`.

If an "other" cause annotated in this way is a barrier error, and the
wrapped error is not a barrier error yet, then the result is
transmuted into a new barrier error.

Also, see the next section.

### Identification of causes

The preferred ways to determine whether an error has a particular cause are:

- the `errors.Is()` function, modeled after the [proposed function of the
  same name in Go 2](#Go-error-handling-v2-).
- the `errors.If()` function, provided until Go 2's generics become available
  and we can start to implement the `errors.As()` function.

The prototypes are:

```go
// Is returns true iff the error contains `reference` in any of its
// cause(s). Causes behind a barrier error and "other" causes added
// using WithOtherCause() are invisible to Is().
func Is(err error, reference error) bool

// If applies the predicate function to all the causes and returns
// what the predicate returns the
// first time the predicate returns `true` in its the second return value.
// If the predicate never returns `true`, the function returns `(nil, false)`.
// Causes behind a barrier error and "other" cuases added using
// using WithOtherCause() are invisible to Is()
func If(err error, predicate func(error) (interface{}, bool)) (interface{}, bool)
```

Example uses:

```go
  // Was:
  //
  //    if err == io.EOF { ...
  //
  if errors.Is(err, io.EOF) { ...
```

```go
  // Was:
  //
  //   if r, ok := errors.Cause(err).(*roachpb.RangeFeedRetryError); ok
  //
  if ri, ok := errors.If(err, func(err error) (interface{}, bool) {
    return err.(*roachpb.RangeFeedRetryError)
  }); ok {
     r := ri.(*roachpb.RangeFeedRetryError)
	 ...
```

For convenience, `IsAny()` able to detect multiple types at once:

```go
// IsAny is like Is() but supports multiple reference errors.
func IsAny(err error, references ...error) bool
```

There is no need for `IfAny()` since the predicate passed to `If()` can
test for multiple types.

(Further work can consider auto-generating predicate functions like
`roachpb.IsRangeFeedRetryError()` to simplify the code further.)

In the (expected rare) case where code needs to test the presence of a
secondary "other" cause, the following functions can be used instead:

```go
// IsOther is like Is() but includes "other" causes. It does not
// peek beyond barrier errors however.
func IsOther(err error, reference error) bool

// IfOther is like If() but includes "other" causes. It does not
// peek beyond barrier errors however.
func IfOther(err error, predicate func(error) (interface{}, bool)) (interface{}, bool)
```

### Error equivalence and markers

The library provides a "marker" facility to help with cases when an
error object is not protobuf-encodable and it is transmitted across
the wire.

For example, `io.EOF` is not protobuf-encodable, so the
predicate `if err == io.EOF` will not work properly if `err` was
transmitted across the wire.

To help with this, the library provides *marker errors* that implement
the following interface:

```
type ErrorMarker interface {
  ErrorMark() string
}
```

Additionally, a global `errors.ErrorMark(err error)` is provided that
extracts the marker if `err` implements `ErrorMarker`, or
constructs a new marker on-the-fly otherwise. When a non-encodable error
is transmitted across the wire, it is converted to a new error type
but preserving the original error mark.

Error markers are used automatically by the `errors.Is()` function:
`Is(err, ref)` will return `true` if *either* `err == ref` *or*
`ErrorMark(err) == ErrorMark(ref)` at the leaf level of cause.

Markers created automatically with `errors.ErrorMark` contain the
package path, type name and error message of the provided error, and
thus automatically support most of the standard errors
(e.g. `io.EOF`).

Therefore, `errors.If(err, io.EOF)` is properly able to detect a
`io.EOF` originating across the network.

#### Message-independent error markers

In some cases it is desirable to create two or more error objects with
different messages but that are considered equivalent via `If()`.

For example, in `pkg/sql/schema_changer.go` we see the type
`errTableVersionMismatch` which can be instantiated with a diversity
of arguments. However the code that tests for this error needs to
detect it regardless of the generated message text.

In this case, the library provides the function `errors.Mark(err error, mark error)`:

```go
// Mark wraps the provided error with the same mark as refmark,
// so that ErrorMark() applied on the result will use that mark
// instead of a new mark derived from err.
func Mark(err error, refmark err)
```

With this facility, the code in `schema_changer.go` can be modified as follows:

```go
// refTableVersionMismatch can be used as sentinel to detect any instance
// of errTableVersionMismatch in error handling.
var refTableVersionMismatch = errTableVersionMismatch{}

func makeErrTableVersionMismatch(version, expected sqlbase.DescriptorVersion) error {
    return errors.Mark(errTableVersionMismatch{
        version:  version,
        expected: expected,
    }, refTableVersionMismatch)
}

// in the detection code, isPermanentSchemaChangeError():
    ...
    if errors.IsAny(err,
        ...
        refTableVersionMismatch
        ...) {
      ...
    }
    ...
```

## Promotion and preservation during formatting

If an error object is passed as a formatting argument to one of the
constructors (`Newf`, `Wrapf`, etc.), for example `errors.Wrapf(err,
"while handling: %v", otherErr)`, this situation is detected and the
library makes efforts to preserve the argument error object:

- if any of the error arguments was a barrier error, the resulting
  error becomes a barrier too (with the same pg code).

- the embedded `context` tags, stack traces, safe details, pg details,
  etc., if any, are preserved in the resulting error object.

However, errors captured in this way are invisible to the `Cause()`
and `Unwrap()` methods and `Is()` / `If()`.
They are also not preserved as "other" error and thus remain invisible
to `IsOther()` and `IfOther()`.

Prefer the `Wrap()` variants when preserving the cause is important,
or in case of doubt.

A linter detects uses of `panic(fmt.Sprintf(...))` and suggests using
`panic(errors.Newf(...))` instead, so that uses like
`panic(fmt.Sprintf("unexpected: %s", err))` get the original error
details preserved in the panic object.

## What comes out of an error?

### Error message

The *message* of an error is the value returned by its `Error()` method.

This contains the initial string composed via `fmt.Errorf()`,
`errors.New()`, `errors.Newf()` etc, prefixes by the additional
strings given via `errors.Wrap()` or `errors.Wrapf()`.

The message does not contain information from ["other" causes](#Multiple-causes) nor
the ["internal" causes of barriers](#Barrier-errors).

This is also the string used to populate the "message" field in error
packets on the PostgreSQL wire protocol.

Note that the full message is never included in telemetry report (it
may contain PII), however any original formatting string and
additional arguments passed via `log.Safe()` will be preserved and
reported. See [Safe details for
telemetry](#Safe-details-for-telemetry) below.

### Details for troubleshooting

The full details of what composes the error can be obtained by
formatting the error using `%+v`.

(The "simple" `%v` formatter merely includes the error message, for
compatibility with existing code.)

### PostgreSQL error code

The *code* of an error is the value returned by the function
`errors.GetPGCode(err)`.

This is composed as follows:

- the direct chain of causes is traversed. If any barrier error is
  encountered in the causes, the recursion stops and that barrier's
  code is returned immediately.

- if a leaf error is reached during the recursion and it does not provide
  a code, then a "default code" is computed in the following cases:

  - if the error implements `roachpb.ClientVisibleRetryError`, then the
    default code becomes `40001/CodeSerializationFailureError`.

  - if the error implements `roachpb.ClientVisibleAmbiguousError`, then
    the default code becomes
    `40003/CodeStatementCompletionUnknownError`.

- on the return path of the recursion, the first available code is
  picked up and used as final result (i.e. the "innermost" code
  prevails).

- if no code was found, the code `XXUUU/CodeUncategorizedError` is
  produced instead.

### PostgreSQL error details

The PostgreSQL "detail" field is retrieved via `errors.Detail(err)`.

The return value is obtained by concatenating the collection of
[PostgreSQL "detail" fields](#Additional-PostgreSQL-error-fields) is
concatenated across both [direct](#Decorating-existing-errors) and
["other"](#Multiple-causes) causes, using a depth-first traversal.

### PostgreSQL error hints

The PostgreSQL "hint" field is retrieved via `errors.Hint(err)`.

The return value is obtained by collecting all the [PostgreSQL "hint"
fields](#Additional-PostgreSQL-error-fields) across both
[direct](#Decorating-existing-errors) and ["other"](#Multiple-causes)
causes, using a depth-first traversal, then *de-duplicating* the hints
before concatenating the result.

The de-duplication ensures that the hint to open an issue on Github,
if present, is only included once.

### PostgreSQL source field

The PostgreSQL "source" field (file, lineno, function) is
collected from the innermost cause that has this information available.

### Telemetry keys to increment

The collection of telemetry keys to increment when an error flows out
is collected through both [direct](#Decorating-existing-errors) and
["other"](#Multiple-causes) causes.

### Safe details for telemetry

A "telemetry packet" is assembled by composing the following:

- the error type name and safe message (format + safe arguments) at
  every level of [direct](#Decorating-existing-errors) or
  ["other"](#Multiple-causes) cause;
- the pg error code(s) at every level where available;
- all embedded stack traces using "additional" fields in the packet.

# Reference-level explanation

Table of contents:

- [Detailed design](#Detailed-design)
  - [Elementary types](#Elementary-types)
  - [Special casing for barrier errors](#Special-casing-for-barrier-errors)
  - [Error constructors](#Error-constructors)
    - [Leaf instances](#Leaf-instances)
    - [Wrapper errors](#Wrapper-errors)
  - [Wire encoding](#Wire-encoding)
  - [Compatibility APIs](#Compatibility-APIs)
- [Drawbacks](#Drawbacks)
- [Rationale and Alternatives](#Rationale-and-Alternatives)
- [Unresolved questions](#Unresolved-questions)

Note: an [early prototype has been implemented in PR
#36023](https://github.com/cockroachdb/cockroach/pull/36023) however
it does not fully reflect (as of 2019-04-22) the design presented in
this RFC.

## Detailed design

The library follows the design principle used in
`github.com/pkg/errors`: separate *elementary types* are provided and
can be composed to form an arbitrary complex error detail tree.

Each of the `Wrap` or `With` functions decorates the error given to it with
one or more of the elementary types.

For example:

- `errors.WithMessage(err, msg)` returns `&withMessage{cause: err, message: msg}`
- `errors.WithDetail(err, detail)` returns `&withDetail{cause: err, detail: detail}`
- `errors.Wrap(err, msg)` returns `&withMessage{cause: &withStack{cause: err, stack: callers()}, message: msg}`

We use multiple elementary type instead of a single "god type" with
all possible fields (like is [used in Upspin](#upspinioerrors)) so
that the various algorithms (`Cause()`, `GetPGCode()`, etc.) become
easier to write and reason about.

### Elementary types

| Type               | Description                                                               | Produced by (example)        |
|--------------------|---------------------------------------------------------------------------|------------------------------|
| `fundamental`      | simple error with a message                                               | `New`                        |
| `barrier`          | [barrier error](#Barrier-errors) with an internal cause                   | `AssertionFailed`            |
| `withMessage`      | wrapper with a simple [message prefix](#Decorating-existing-errors)       | `Wrap`                       |
| `withTags`         | wrapper with [logging tags](#Adding-context)                              | `WrapCtx`                    |
| `withSafeDetail`   | wrapper with additional [safe details](#Safe-details)                     | `Errorf`, `AssertionFailedf` |
| `withDefaultCode`  | wrapper with a default [pg code](#PostgreSQL-error-code)                  | `WithDefaultCode`            |
| `withDetail`       | wrapper with a [pg detail field](#PostgreSQL-error-details)               | `WithDetail`                 |
| `withHint`         | wrapper with a [pg hint field](#PostgreSQL-error-hints)                   | `WithHint`                   |
| `withOtherCause`   | wrapper with an ["other" cause](#Multiple-causes)                         | `WithOtherCause`             |
| `withMark`         | wrapper with an [error mark](#Message-independent-error-markers)          | `Mark`                       |
| `withTelemetryKey` | wrapper with a [telemetry key](#Telemetry-keys)                           | `WithTelemetryKey`           |
| `unknownWrapper`   | wrapper with a [preserved mark from an non-encodable error wrapper](#xxx) |                              |

### Special casing for barrier errors

All the error types except `barrier` and `fundamental` implement the
methods `Cause()` and `Unwrap()` that return their `cause` field.

`barrier` does have an `internalCause` field for the purpose of
printing details on `%+v` and providing safe details for telemetry,
but does not implement `Cause()` so as to inhibit use of the origin cause.

### Error constructors

#### Leaf instances

| Constructor                                                          | Type produced                               | Type after wire conversion |
|----------------------------------------------------------------------|---------------------------------------------|----------------------------|
| `New`                                                                | `fundamental` + `withStack`                 | preserved                  |
| `AssertionFailed`                                                    | `fundamental` + `barrier` + `withStack`     | preserved                  |
| formatting variant with `xxx-f(format string, args ...interface{})`  | type of base constructor + `withSafeDetail` | preserved                  |
| `&roachpb.NodeUnavailableError{}`, etc                               | the given type                              | preserved                  |
| `errors.New` from `golang.org/pkg/errors` or `github.com/pkg/errors` | type from original package                  | `fundamental` + `withMark` |
| other leaf error without known wire encoding                         | type from original package                  | `fundamental` + `withMark` |

#### Wrapper errors

| Constructor                                        | Type produced                                                | Type after wire conversion |
|----------------------------------------------------|--------------------------------------------------------------|----------------------------|
| `WithMessage`                                      | `withMessage`                                                | preserved                  |
| `WithStack`                                        | `withStack`                                                  | preserved                  |
| `WithDefaultCode`                                  | `withDefaultCode`                                            | preserved                  |
| `WithCtx`                                          | `withTags`                                                   | preserved                  |
| `WithTelemetryKey`, `UnimplementedWithIssue`       | `withTelemetryKey`                                           | preserved                  |
| `WithHint`                                         | `withHint`                                                   | preserved                  |
| `WithDetail`                                       | `withDetail`                                                 | preserved                  |
| `WithOtherCause`                                   | `withOtherCause`                                             | preserved                  |
| `Mark`                                             | `withMark`                                                   | preserved                  |
| `Wrap`                                             | `withMessage` + `withStack`                                  | preserved                  |
| `WrapCtx`                                          | `withTags` + `withStack`                                     | preserved                  |
| `Wrapx`                                            | `withMessage` + `withDefaultCode` + `withTags` + `withStack` | preserved                  |
| formatting variant with `xxx-f()`                  | wrapper type + `withSafeDetail`                              | preserved                  |
| `errors.Wrap` from `github.com/pkg/errors`         | type from original package                                   | `unknownWrapper`           |
| other wrapper implementing `Cause()` or `Unwrap()` | type from original package                                   | `unknownWrapper`           |

### Compatibility APIs

The following functions are provided to ensure the new `errors`
package can be used as drop-in replacement to `golang.org/pkg/errors`
and `github.com/pkg/errors`:

```go
// Compatibility with golang.org/pkg/errors.
func New(msg string) Error

// Compatibility with github.com/pkg/errors.
func Errorf(format string, args ...interface{}) Error
func WithStack(err error) error
func Wrap(err error, msg string) error
func Wrapf(err error, format string, args ...interface{}) error
func WithMessage(err error, msg string) error
func WithMessagef(err error, format string, args ...interface{}) error
func Cause(err error) error

// Forward compatibility with Go 2.
func Unwrap(err error) error
func Is(err error, ref error) error
```

### Wire encoding

The library provides encoding via an intermediate protobuf-compatible
representation. This is provided by the following facilities:

```go
// EncodeError converts a Go error to an encodable error.
func EncodeError(err error) AnyErrorContainer

// GetError converts an AnyErrorContainer to a Go error.
func (e *AnyErrorContainer) GetError() error
```

`EncodeError` is the function that is responsible for transforming any
non-encodable error object in the chain into a protobuf-encodable
equivalent error:

- non-encodable leafs are transformed to `fundamental` + `withMark`.
- non-encodable wrappers are transformed to `unknownWrapper`.

When an unknown error type is _received_ on the wire (as would happen,
for example, when receiving a new error type from version X+1 on a
node running version X), it is also converted to either
`fundamental` + `withMark` or `unknownWrapper`, so as to preserve the
overall structure/cause of the error.

The advantage of this approach is that it enables an error to flow
from a node running version X+1 to another node running version X+1,
via a node running version X, *while preserving the error structure*
and the ability to test the cause via `errors.Is` / `errors.If`.

## Drawbacks

![mandatory xkcd comic](https://imgs.xkcd.com/comics/standards.png)

This introduces yet another error handling library.

This additional complexity is mitigated by making API drop-in
compatible with those already in use throughout CockroachDB. This
avoids a steep learning curve and facilitates "upgrading" existing
code without large rewrites. Care was also taken to make it
forward-compatible with the [announced Go 2 error value
semantics](#Error-value-semantics).

## Rationale and Alternatives

Alternatives:

- **Keep the status quo:** error string comparisons are unsafe (to the
  point they may cause [security
  vulnerabilities](#Vulnerability-to-embedded-application-strings))
  and generally [hard to reason
  about](#Unreliable-standard-ways-to-inspect-errors). It also does
  not [satisfy the other
  requirements](#Motivation-for-a-new-error-type-summary) that have
  grown over time.

- **Use a single error type (presumably `roachpb.Error`) everywhere:**
  this creates even more complexity as any error generated by a 3rd party
  library needs to be converted into the specific error type. This also
  prevents preserving (and reasoning about) chains of causes.

- **Use a single "god type" for wrapping causes:** this makes
  the implementation of ancillary services (compute a pg error code,
  collect the hints, etc) more difficult and harder to reason about.

## Unresolved questions

- Are the ctx tags also [Safe details](#Safe-details)?

# Appendices

## Problematic error use cases

### Suspicious comparisons of the error object

Comparison of the error object are vulnerable to:

- conversions of the error object
- error wraps
- communication over the network

```
pkg/storage/node_liveness.go:           if err == errNodeDrainingSet {
pkg/storage/node_liveness.go:                                                   if err == ErrEpochIncremented {
pkg/storage/node_liveness.go:           if err == errNodeAlreadyLive {
pkg/storage/node_liveness.go:   if err == ErrNoLivenessRecord {
pkg/storage/replica.go: if err == stop.ErrUnavailable {
pkg/storage/replica_gossip.go:          if err == errSystemConfigIntent {
pkg/storage/replica_raft.go:    if err := r.submitProposalLocked(proposal); err == raft.ErrProposalDropped {
pkg/storage/replica_raft.go:            if err == raft.ErrProposalDropped {
pkg/storage/replica_raft.go:            if err := r.submitProposalLocked(p); err == raft.ErrProposalDropped {
pkg/storage/replica_raftstorage.go:     if err == raft.ErrCompacted {
pkg/storage/store.go:           if err == errRetry {

pkg/storage/intentresolver/intent_resolver.go:          if err == stop.ErrThrottled {

pkg/storage/tscache/interval_skl.go:            if err == arenaskl.ErrArenaFull {
pkg/storage/tscache/interval_skl.go:    if err == arenaskl.ErrArenaFull {

pkg/kv/dist_sender_rangefeed.go:                        if err == io.EOF {

pkg/rpc/snappy.go:      if err == io.EOF {

pkg/server/status.go:                   if err == io.EOF {

pkg/jobs/jobs.go:               if execDone := execErrCh == nil; err == gosql.ErrNoRows && !execDone {

pkg/sql/sqlbase/structured.go:                  if err := tree.Insert(pi, false /* fast */); err == interval.ErrEmptyRange {
pkg/sql/sqlbase/structured.go:                  } else if err == interval.ErrInvertedRange {

pkg/sql/distsqlrun/outbox.go:                   if err == io.EOF {
pkg/sql/distsqlrun/server.go:           if err == io.EOF {
pkg/sql/opt/optgen/lang/scanner.go:     if err == io.EOF {
pkg/sql/row/fk_existence_delete.go:                     if err == errSkipUnusedFK {
pkg/sql/row/fk_existence_insert.go:                     if err == errSkipUnusedFK {

pkg/sql/conn_executor.go:                       if err == io.EOF || err == errDrainingComplete {
pkg/sql/crdb_internal.go:                                               if err == sqlbase.ErrIndexGCMutationsList {
pkg/sql/exec_util.go:                           if err == sqlbase.ErrDescriptorNotFound || err == ctx.Err() {
pkg/sql/opt_catalog.go:         if err == sqlbase.ErrDescriptorNotFound || tableLookup.IsAdding {
pkg/sql/planner.go:             if err == errTableAdding {
pkg/sql/set_zone_config.go:     if err == errNoZoneConfigApplies {
pkg/sql/show_zone_config.go:    if err == errNoZoneConfigApplies {
pkg/sql/table.go:               if err == errTableDropped || err == sqlbase.ErrDescriptorNotFound {
pkg/sql/table.go:               if err == sqlbase.ErrDescriptorNotFound {
pkg/sql/zone_config.go: if err == errNoZoneConfigApplies {
pkg/sql/zone_config.go:         if err == errMissingKey {
pkg/sql/schema_changer.go:                                              if err == sqlbase.ErrDescriptorNotFound {
pkg/sql/schema_changer.go:  switch err {
    case
        context.Canceled,
        context.DeadlineExceeded,
        ...

pkg/util/binfetcher/extract.go:         if err == io.EOF {

pkg/util/encoding/csv/reader.go:                if err == io.EOF {
pkg/util/encoding/csv/reader.go:        if err == bufio.ErrBufferFull {
pkg/util/encoding/csv/reader.go:                for err == bufio.ErrBufferFull {
pkg/util/encoding/csv/reader.go:        if len(line) > 0 && err == io.EOF {

pkg/util/grpcutil/grpc_util.go: if err == ErrCannotReuseClientConn {
pkg/util/grpcutil/grpc_util.go: if err == context.Canceled ||

pkg/util/log/file.go:                   if err == io.EOF {

pkg/util/netutil/net.go:        return err == cmux.ErrListenerClosed ||
pkg/util/netutil/net.go:                err == grpc.ErrServerStopped ||
pkg/util/netutil/net.go:                err == io.EOF ||

pkg/workload/cli/run.go:                                if err == ctx.Err() {
pkg/workload/histogram/histogram.go:            if err := dec.Decode(&tick); err == io.EOF {
pkg/workload/tpcc/new_order.go: if err == errSimulated {

pkg/acceptance/cluster/docker.go:               if err := binary.Read(rc, binary.BigEndian, &header); err == io.EOF {

pkg/ccl/importccl/load.go:              if err == io.EOF {
pkg/ccl/importccl/read_import_csv.go:           finished := err == io.EOF
pkg/ccl/importccl/read_import_mysql.go:         if err == io.EOF {
pkg/ccl/importccl/read_import_mysql.go:         if err == mysql.ErrEmpty {
pkg/ccl/importccl/read_import_mysql.go:         if err == io.EOF {
pkg/ccl/importccl/read_import_mysql.go:         if err == mysql.ErrEmpty {
pkg/ccl/importccl/read_import_mysqlout.go:              finished := err == io.EOF
pkg/ccl/importccl/read_import_pgcopy.go:                if err == bufio.ErrTooLong {
pkg/ccl/importccl/read_import_pgcopy.go:                if err == io.EOF {
pkg/ccl/importccl/read_import_pgcopy.go:                if err == io.EOF {
pkg/ccl/importccl/read_import_pgdump.go:                if err == errCopyDone {
pkg/ccl/importccl/read_import_pgdump.go:                if err == bufio.ErrTooLong {
pkg/ccl/importccl/read_import_pgdump.go:                if err == io.EOF {
pkg/ccl/importccl/read_import_pgdump.go:                if err == io.EOF {
pkg/ccl/importccl/read_import_pgdump.go:                                if err == io.EOF {

pkg/ccl/workloadccl/fixture.go:                 if err == iterator.Done {
pkg/ccl/workloadccl/fixture.go:                         if err == iterator.Done {
pkg/ccl/workloadccl/fixture.go:         if err == iterator.Done {
pkg/ccl/workloadccl/fixture.go:                 if err == iterator.Done {

pkg/cmd/docgen/extract/xhtml.go:                        if err == io.EOF {

pkg/cmd/roachprod/install/cluster_synced.go:                                    if err == io.EOF {
pkg/cmd/roachprod/vm/gce/utils.go:              if err == io.EOF {

pkg/cmd/roachtest/cluster.go:           if l.stderr == l.stdout {
pkg/cmd/roachtest/cluster.go:                   // If l.stderr == l.stdout, we use only one pipe to avoid

pkg/testutils/net.go:           } else if err == errEAgain {
```

### Suspicious assertions on the error type

Assertions on the error type breaks down if the error object is
converted to a different type (in particular when the error does not
have a wire representation). Care must also be taken to perform the test
at every level of a chain of causes, until barrier errors if any.

```
pkg/storage/bulk/sst_batcher.go:                                if _, ok := err.(*roachpb.AmbiguousResultError); ok {
pkg/storage/engine/mvcc.go:                     switch tErr := err.(type) {
pkg/storage/merge_queue.go:     switch err := pErr.GoError(); err.(type) {
pkg/storage/node_liveness.go:                   if _, ok := err.(*errRetryLiveness); ok {
pkg/storage/queue.go:           _, ok := err.(*benignError)
pkg/storage/queue.go:           purgErr, ok = err.(purgatoryError)
pkg/storage/replica_command.go:                 switch err.(type) {
pkg/storage/replica_command.go: if detail, ok := err.(*roachpb.ConditionFailedError); ok {
pkg/storage/store.go:                           if _, ok := err.(*roachpb.AmbiguousResultError); !ok {
pkg/storage/store_bootstrap.go: if _, ok := err.(*NotBootstrappedError); !ok {
pkg/storage/stores.go:          switch err.(type) {

pkg/roachpb/errors.go:  if intErr, ok := err.(*internalError); ok {
pkg/roachpb/errors.go:          if sErr, ok := err.(ErrorDetailInterface); ok {
pkg/roachpb/errors.go:          if r, ok := err.(transactionRestartError); ok {
pkg/roachpb/errors.go:                  if _, isInternalError := err.(*internalError); !isInternalError && isTxnError {

pkg/server/server.go:           if _, notBootstrapped := err.(*storage.NotBootstrappedError); notBootstrapped {
pkg/server/status.go:                                           if _, skip := err.(*roachpb.RangeNotFoundError); skip {
pkg/server/status.go:                                   if _, skip := err.(*roachpb.RangeNotFoundError); skip {
pkg/server/status/runtime.go:           if _, ok := err.(gosigar.ErrNotImplemented); ok {

pkg/base/config.go:     if _, ok := err.(*security.Error); !ok {

pkg/ccl/changefeedccl/errors.go:                if _, ok := err.(*retryableError); ok {
pkg/ccl/changefeedccl/errors.go:                if e, ok := err.(interface{ Unwrap() error }); ok {
pkg/ccl/changefeedccl/errors.go:        if e, ok := err.(*retryableError); ok {

pkg/ccl/importccl/read_import_proc.go:                          if _, ok := err.(storagebase.DuplicateKeyError); ok {
pkg/ccl/importccl/read_import_proc.go:          if err, ok := err.(storagebase.DuplicateKeyError); ok {

pkg/ccl/storageccl/export_storage.go:           if s3err, ok := err.(s3.RequestFailure); ok {

pkg/cli/debug.go:               if wiErr, ok := err.(*roachpb.WriteIntentError); ok {
pkg/cli/flags.go:               if aerr, ok := err.(*net.AddrError); ok {
pkg/cli/start.go:                               if le, ok := err.(server.ListenError); ok {
pkg/cli/start.go:                       if _, ok := err.(errTryHardShutdown); ok {

pkg/cmd/roachprod/ssh/ssh.go:   switch t := err.(type) {
pkg/cmd/roachprod/vm/aws/support.go:            if exitErr, ok := err.(*exec.ExitError); ok {
pkg/cmd/roachprod/vm/gce/gcloud.go:             if exitErr, ok := err.(*exec.ExitError); ok {
pkg/cmd/roachtest/tpcc.go:      } else if pqErr, ok := err.(*pq.Error); !ok ||
pkg/cmd/roachtest/tpchbench.go:                 if pqErr, ok := err.(*pq.Error); !(ok && pqErr.Code == pgerror.CodeUndefinedTableError) {
pkg/cmd/roachtest/tpchbench.go: } else if pqErr, ok := err.(*pq.Error); !ok ||

pkg/cmd/urlcheck/lib/urlcheck/urlcheck.go:              if err, ok := err.(net.Error); ok && err.Timeout() {

pkg/internal/client/db.go:      if _, ok := err.(*roachpb.TransactionRetryWithProtoRefreshError); ok {
pkg/internal/client/db.go:              switch err.(type) {
pkg/internal/client/lease.go:           if _, ok := err.(*roachpb.ConditionFailedError); ok {
pkg/internal/client/txn.go:                                     if _, retryable := err.(*roachpb.TransactionRetryWithProtoRefreshError); !retryable {
pkg/internal/client/txn.go:     retryErr, ok := err.(*roachpb.TransactionRetryWithProtoRefreshError)

pkg/jobs/jobs.go:       ierr, ok := err.(*InvalidStatusError)

pkg/sql/sem/tree/type_check.go: if _, ok := err.(placeholderTypeAmbiguityError); ok {

pkg/sql/conn_executor.go:       _, retriable := err.(*roachpb.TransactionRetryWithProtoRefreshError)
pkg/sql/conn_executor.go:                               switch t := err.(type) {
pkg/sql/conn_executor.go:               if _, ok := err.(fsm.TransitionNotFoundError); ok {
pkg/sql/conn_executor.go:                       err.(errorutil.UnexpectedWithIssueErr).SendReport(ex.Ctx(), &ex.server.cfg.Settings.SV)
pkg/sql/database.go:            if _, ok := err.(*roachpb.ConditionFailedError); ok {
pkg/sql/distsql_running.go:     if retryErr, ok := err.(*roachpb.UnhandledRetryableError); ok {
pkg/sql/distsql_running.go:     if retryErr, ok := err.(*roachpb.TransactionRetryWithProtoRefreshError); ok {
pkg/sql/rename_table.go:                if _, ok := err.(*roachpb.ConditionFailedError); ok {
pkg/sql/schema_changer.go:      switch err := err.(type) {
pkg/sql/sequence.go:                    switch err.(type) {

pkg/sql/scrub/errors.go:        _, ok := err.(*Error)
pkg/sql/scrub/errors.go:                return err.(*Error).underlying

pkg/sql/distsqlrun/processors.go:                               if ure, ok := err.(*roachpb.UnhandledRetryableError); ok {
pkg/sql/distsqlrun/scrub_tablereader.go:                if v, ok := err.(*scrub.Error); ok {

pkg/sql/exec/error.go:                                  if e, ok := err.(error); ok {

pkg/sql/logictest/logic.go:             pqErr, ok := err.(*pq.Error)
pkg/sql/logictest/logic.go:                     pqErr, ok := err.(*pq.Error)
pkg/sql/logictest/logic.go:     if pqErr, ok := err.(*pq.Error); ok {

pkg/sql/pgwire/conn.go:         return err.(error)
pkg/sql/pgwire/conn.go:         if err, ok := err.(net.Error); ok && err.Timeout() {

pkg/sql/pgwire/pgerror/errors.go:       if pqErr, ok := err.(*pq.Error); ok {
pkg/sql/pgwire/pgerror/wrap.go: pgErr, ok := err.(*Error)
pkg/sql/pgwire/pgerror/wrap.go: if cause, ok := err.(causer); ok {
pkg/sql/pgwire/pgerror/wrap.go:         switch err.(type) {
pkg/sql/pgwire/pgerror/wrap.go: if e, ok := err.(stackTracer); ok {

pkg/sqlmigrations/migrations.go:                if _, ok := err.(*roachpb.ConditionFailedError); ok {

pkg/util/grpcutil/grpc_util.go: if streamErr, ok := err.(transport.StreamError); ok && streamErr.Code == codes.Canceled {
pkg/util/grpcutil/grpc_util.go: if _, ok := err.(connectionNotReadyError); ok {
pkg/util/grpcutil/grpc_util.go: if _, ok := err.(netutil.InitialHeartbeatFailedE
rror); ok {

pkg/util/timeutil/pgdate/parsing.go:    if err, ok := err.(*pgerror.Error); ok {
```

### Suspicious comparisons of the error message

Comparisons of the error string are vulnerable to the presence of the
reference string in app-level data.

```
pkg/storage/replica_command.go:                 if strings.Contains(err.Error(), substr) {
pkg/storage/syncing_write.go:           if strings.Contains(err.Error(), "No such file or directory") {
pkg/storage/engine/rocksdb.go:  if strings.Contains(errStr, "No such file or directory") ||
pkg/storage/engine/rocksdb.go:          strings.Contains(errStr, "File not found") ||
pkg/storage/engine/rocksdb.go:          strings.Contains(errStr, "The system cannot find the path specified") {

pkg/server/admin.go:    return err != nil && strings.HasSuffix(err.Error(), "does not exist")
pkg/server/grpc_server.go:      return ok && s.Code() == codes.Unavailable && strings.Contains(err.Error(), "node waiting for init")

pkg/security/securitytest/securitytest.go:              if strings.HasSuffix(err.Error(), "not found") {
pkg/security/securitytest/securitytest.go:      if err != nil && strings.HasSuffix(err.Error(), "not found") {

pkg/sql/schema_changer.go:	if pgerror.IsSQLRetryableError(err) {
pkg/sql/schema_changer.go:      if strings.Contains(err.Error(), "must be after replica GC threshold") {

pkg/ccl/changefeedccl/errors.go:                if strings.Contains(errStr, retryableErrorString) {
pkg/ccl/changefeedccl/errors.go:                if strings.Contains(errStr, `rpc error`) {
pkg/ccl/changefeedccl/cdctest/nemeses.go:       if err := txn.Commit(); err != nil && !strings.Contains(err.Error(), `restart transaction`) {

pkg/ccl/storageccl/export_storage.go:           if strings.Contains(err.Error(), "net/http: timeout awaiting response headers") {

pkg/util/grpcutil/grpc_util.go:         strings.Contains(err.Error(), "is closing") ||
pkg/util/grpcutil/grpc_util.go:         strings.Contains(err.Error(), "node unavailable") {
pkg/util/grpcutil/grpc_util.go:         strings.Contains(err.Error(), "tls: use of closed connection") ||
pkg/util/grpcutil/grpc_util.go:         strings.Contains(err.Error(), "use of closed network connection") ||
pkg/util/grpcutil/grpc_util.go:         strings.Contains(err.Error(), io.EOF.Error()) ||
pkg/util/grpcutil/grpc_util.go:         strings.Contains(err.Error(), io.ErrClosedPipe.Error()) ||

pkg/util/netutil/net.go:                strings.Contains(err.Error(), "use of closed network connection")

pkg/util/timeutil/zoneinfo.go:  if err != nil && strings.Contains(err.Error(), "zoneinfo.zip") {

pkg/cli/dump.go:                if strings.Contains(err.Error(), "column \"crdb_sql_type\" does not exist") {
pkg/cli/dump.go:                if strings.Contains(err.Error(), "column \"is_hidden\" does not exist") {
pkg/cli/zone.go:        if err != nil && strings.Contains(err.Error(), "syntax error") {

pkg/acceptance/localcluster/cluster.go: return strings.Contains(err.Error(), "grpc: the connection is unavailable")

pkg/acceptance/cluster/docker.go:       if err != nil && strings.Contains(err.Error(), "already in use") {
pkg/acceptance/cluster/docker.go:       if err := c.cluster.client.ContainerKill(ctx, c.id, "9"); err != nil && !strings.Contains(err.Error(), "is not running") {

pkg/cmd/roachprod/ssh/ssh.go:           if strings.Contains(err.Error(), "cannot decode encrypted private key") {
pkg/cmd/roachprod/vm/aws/keys.go:       if err == nil || strings.Contains(err.Error(), "InvalidKeyPair.Duplicate") {

pkg/cmd/roachtest/bank.go:				if err != nil && !(pgerror.IsSQLRetryableError(err) || isExpectedRelocateError(err)) {
pkg/cmd/roachtest/bank.go:				if err != nil && !(pgerror.IsSQLRetryableError(err) || isExpectedRelocateError(err)) {
pkg/cmd/roachtest/bank.go:			if !pgerror.IsSQLRetryableError(err) {
pkg/cmd/roachtest/bank.go:		if err != nil && !pgerror.IsSQLRetryableError(err) {
pkg/cmd/roachtest/cdc.go:       ); err != nil && !strings.Contains(err.Error(), "unknown cluster setting") {
pkg/cmd/roachtest/cdc.go:       ); err != nil && !strings.Contains(err.Error(), "unknown cluster setting") {
pkg/cmd/roachtest/cluster.go:                   if err != context.Canceled && !strings.Contains(err.Error(), "killed") {
pkg/cmd/roachtest/disk_full.go:                                 } else if strings.Contains(err.Error(), "a panic has occurred") {
pkg/cmd/roachtest/split.go:             if !strings.Contains(err.Error(), "unknown cluster setting") {

pkg/cmd/zerosum/main.go:        if localcluster.IsUnavailableError(err) || strings.Contains(err.Error(), "range is frozen") {

pkg/workload/tpcc/partition.go: if err != nil && strings.Contains(err.Error(), "syntax error") {
pkg/workload/tpcc/tpcc.go:                                              if !strings.Contains(err.Error(), duplFKErr) {
```

### Suspicious reliance on the pg error code

```
pkg/cli/error.go:                       if wErr.Code == pgerror.CodeProtocolViolationError {
pkg/cli/user.go:                        if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == pgerror.CodeDuplicateObjectError {
pkg/cmd/roachtest/tpchbench.go:                 if pqErr, ok := err.(*pq.Error); !(ok && pqErr.Code == pgerror.CodeUndefinedTableError) {
pkg/sql/conn_executor_exec.go:          if pgErr.Code == pgerror.CodeUndefinedColumnError ||
pkg/sql/conn_executor_exec.go:                  pgErr.Code == pgerror.CodeUndefinedTableError {
pkg/sql/create_stats.go:                if ok && pgerr.Code == pgerror.CodeLockNotAvailableError {
pkg/sql/opt/optbuilder/util.go:         if pgerr, ok := pgerror.GetPGCause(err); ok && pgerr.Code == pgerror.CodeInvalidSchemaNameError {
pkg/sql/rowcontainer/row_container.go:  if pgErr, ok := pgerror.GetPGCause(err); !(ok && pgErr.Code == pgerror.CodeOutOfMemoryError) {
pkg/sql/stats/automatic_stats.go:               if ok && pgerr.Code == pgerror.CodeLockNotAvailableError {
```

### Suspicious flattening of errors

```
pkg/base/addr_validation.go:            panic(fmt.Sprintf("programming error: %s address not normalized: %v", msg, err))
pkg/base/store_spec.go:                 return SizeSpec{}, fmt.Errorf("could not parse store size (%s) %s", value, err)
pkg/base/store_spec.go:                 return SizeSpec{}, fmt.Errorf("could not parse store size (%s) %s", value, err)

pkg/gossip/gossip.go:           return errors.Errorf("n%d: couldn't gossip descriptor: %v", desc.NodeID, err)

pkg/internal/client/db.go:                      return fmt.Sprintf("%v", err)
pkg/internal/client/db.go:                      return fmt.Sprintf("%v", err)
pkg/internal/client/db.go:                      return fmt.Sprintf("%v", err)
pkg/internal/client/db.go:                      return fmt.Sprintf("%v", err)

pkg/keys/printer.go:            return fmt.Sprintf("<invalid: %s>", err)
pkg/keys/printer.go:                                            return fmt.Sprintf("/%q/err:%v", key, err)
pkg/keys/printer.go:            return fmt.Sprintf("/%q/err:%v", key, err)
pkg/keys/printer.go:            return fmt.Sprintf("/%q/err:%v", key, err)

pkg/kv/dist_sender.go:                          fmt.Sprintf("sending to all %d replicas failed; last error: %v %v", len(replicas), br, err),

pkg/roachpb/data.go:            return fmt.Sprintf("/<err: %s>", err)
pkg/roachpb/errors.go:                          panic(fmt.Sprintf("transactionRestartError %T must be an ErrorDetail", err))
pkg/roachpb/metadata.go:                        return errors.Errorf("replica %d is invalid: %s", i, err)
pkg/roachpb/version.go:                 return c, errors.Errorf("invalid version %s: %s", s, err)

pkg/storage/raft_log_queue.go:          return truncateDecision{}, errors.Errorf("error retrieving first index for r%d: %s", rangeID, err)
pkg/storage/replica_command.go:                         return reply, errors.Errorf("unable to determine split key: %s", err)
pkg/storage/replica_command.go:         return reply, errors.Errorf("unable to allocate right hand side range descriptor: %s", err)
pkg/storage/replica_raftstorage.go:             return OutgoingSnapshot{}, errors.Errorf("failed to get desc: %s", err)
pkg/storage/replica_raftstorage.go:             return OutgoingSnapshot{}, errors.Errorf("failed to fetch term of %d: %s", appliedIndex, err)
pkg/storage/replica_raftstorage.go:             return errors.Errorf("%s: failed to lookup zone config: %s", r, err)
pkg/storage/replica_range_lease.go:                             Message:   fmt.Sprintf("couldn't request lease for %+v: %v", nextLeaseHolder, err),
pkg/storage/replica_write.go:                                   return batch, ms, br, res, roachpb.NewErrorf("failed to run commit trigger: %s", err)
pkg/storage/store.go:           return errors.Errorf("unable to add replica %v: %s", rightRepl, err)
pkg/storage/store.go:           return errors.Errorf("cannot remove range: %s", err)
pkg/storage/store_snapshot.go:          return errors.Errorf("%s: expected EOF, got resp=%v err=%v", to, unexpectedResp, err)

pkg/storage/batcheval/cmd_subsume.go:           return result.Result{}, fmt.Errorf("fetching local range descriptor: %s", err)
pkg/storage/batcheval/cmd_subsume.go:           return result.Result{}, fmt.Errorf("fetching local range descriptor as txn: %s", err)

pkg/storage/engine/version.go:          return 0, fmt.Errorf("version file %s is not formatted correctly; %s", filename, err)

pkg/storage/idalloc/id_alloc.go:                                panic(fmt.Sprintf("unexpectedly exited id allocation retry loop: %s", err))

pkg/storage/tscache/interval_skl.go:                    panic(fmt.Sprintf("unexpected error: %v", err))
pkg/storage/tscache/interval_skl.go:                    panic(fmt.Sprintf("unexpected error: %v", err))
pkg/storage/tscache/interval_skl.go:                                            panic(fmt.Sprintf("SetMeta with larger meta should not return %v", err))
pkg/storage/tscache/interval_skl.go:                            panic(fmt.Sprintf("unexpected error: %v", err))
pkg/storage/tscache/interval_skl.go:                            panic(fmt.Sprintf("SetMeta with larger meta should not return %v", err))
pkg/storage/tscache/interval_skl.go:                            panic(fmt.Sprintf("unexpected error: %v", err))

pkg/security/certificate_loader.go:             return errors.Errorf("could not stat key file %s: %v", fullKeyPath, err)
pkg/security/certificate_loader.go:             return errors.Errorf("could not read key file %s: %v", fullKeyPath, err)
pkg/security/certs.go:          return nil, nil, errors.Errorf("error parsing CA certificate %s: %s", sslCA, err)
pkg/security/certs.go:                  return errors.Errorf("could not stat CA key file %s: %v", caKeyPath, err)
pkg/security/certs.go:                  return errors.Errorf("could not generate new CA key: %v", err)
pkg/security/certs.go:                  return errors.Errorf("could not write CA key to file %s: %v", caKeyPath, err)
pkg/security/certs.go:                  return errors.Errorf("could not read CA key file %s: %v", caKeyPath, err)
pkg/security/certs.go:                  return errors.Errorf("could not parse CA key file %s: %v", caKeyPath, err)
pkg/security/certs.go:          return errors.Errorf("could not generate CA certificate: %v", err)
pkg/security/certs.go:                  return errors.Errorf("could not read existing CA cert file %s: %v", certPath, err)
pkg/security/certs.go:                  return errors.Errorf("could not parse existing CA cert file %s: %v", certPath, err)
pkg/security/certs.go:          return errors.Errorf("could not stat CA cert file %s: %v", certPath, err)
pkg/security/certs.go:          return errors.Errorf("could not write CA certificate file %s: %v", certPath, err)
pkg/security/certs.go:          return errors.Errorf("could not generate new node key: %v", err)
pkg/security/certs.go:          return errors.Errorf("error creating node server certificate and key: %s", err)
pkg/security/certs.go:          return errors.Errorf("error writing node server certificate to %s: %v", certPath, err)
pkg/security/certs.go:          return errors.Errorf("error writing node server key to %s: %v", keyPath, err)
pkg/security/certs.go:          return errors.Errorf("could not generate new UI key: %v", err)
pkg/security/certs.go:          return errors.Errorf("error creating UI server certificate and key: %s", err)
pkg/security/certs.go:          return errors.Errorf("error writing UI server certificate to %s: %v", certPath, err)
pkg/security/certs.go:          return errors.Errorf("error writing UI server key to %s: %v", keyPath, err)
pkg/security/certs.go:          return errors.Errorf("could not generate new client key: %v", err)
pkg/security/certs.go:          return errors.Errorf("error creating client certificate and key: %s", err)
pkg/security/certs.go:          return errors.Errorf("error writing client certificate to %s: %v", certPath, err)
pkg/security/certs.go:          return errors.Errorf("error writing client key to %s: %v", keyPath, err)
pkg/security/certs.go:                  return errors.Errorf("error writing client PKCS8 key to %s: %v", pkcs8KeyPath, err)
pkg/security/pem.go:                    return errors.Errorf("could not encode PEM block: %v", err)
pkg/security/pem.go:                    return nil, errors.Errorf("error marshaling ECDSA key: %s", err)

pkg/server/admin.go:            return nil, status.Errorf(codes.NotFound, "%s", err)
pkg/server/admin.go:            return nil, status.Errorf(codes.NotFound, "%s", err)
pkg/server/admin.go:            return nil, status.Errorf(codes.NotFound, "%s", err)
pkg/server/admin.go:            return nil, status.Errorf(codes.NotFound, "%s", err)
pkg/server/admin.go:            return nil, status.Errorf(codes.NotFound, "%s", err)
pkg/server/admin.go:            return nil, status.Errorf(codes.NotFound, "%s", err)
pkg/server/admin.go:            return nil, s.serverErrorf("error constructing query: %v", err)
pkg/server/node.go:             return errors.Errorf("couldn't gossip descriptor for node %d: %s", n.Descriptor.NodeID, err)
pkg/server/node.go:                     return errors.Errorf("failed to start store: %s", err)
pkg/server/node.go:                     return errors.Errorf("could not query store capacity: %s", err)
pkg/server/node.go:             return fmt.Errorf("failed to initialize the gossip interface: %s", err)
pkg/server/node.go:             return errors.Errorf("error retrieving cluster version for bootstrap: %s", err)
pkg/server/node.go:                     return errors.Errorf("error allocating store ids: %s", err)
pkg/server/server.go:           panic(fmt.Sprintf("error returned to Undrain: %s", err))
pkg/server/status.go:                   fmt.Fprintf(&buf, "n%d: %s", nodeID, err)
pkg/server/status.go:           return nil, fmt.Errorf("log file %s could not be opened: %s", req.File, err)
pkg/server/status.go:           return nil, grpcstatus.Errorf(codes.InvalidArgument, "StartTime could not be parsed: %s", err)
pkg/server/status.go:           return nil, grpcstatus.Errorf(codes.InvalidArgument, "EndTime could not be parsed: %s", err)
pkg/server/status.go:           return nil, grpcstatus.Errorf(codes.InvalidArgument, "Max could not be parsed: %s", err)
pkg/server/status.go:                   return nil, grpcstatus.Errorf(codes.InvalidArgument, "regex pattern could not be compiled: %s", err)
pkg/server/status.go:           err = errors.Errorf("could not unmarshal NodeStatus from %s: %s", key, err)
pkg/server/status.go:           return nil, errors.Errorf("unable to marshal %+v to json: %s", value, err)

pkg/sql/crdb_internal.go:                               errorStr = tree.NewDString(fmt.Sprintf("error decoding payload: %v", err))
pkg/sql/crdb_internal.go:                                       errorStr = tree.NewDString(fmt.Sprintf("%serror decoding progress: %v", baseErr, err))
pkg/sql/distsql_running.go:                             r.resultWriter.SetError(errors.Errorf("error ingesting remote spans: %s", err))
pkg/sql/drop_table.go:                  return errors.Errorf("error resolving referenced table ID %d: %v", idx.ForeignKey.Table, err)
pkg/sql/drop_table.go:                  return errors.Errorf("error resolving referenced table ID %d: %v", ancestor.TableID, err)
pkg/sql/drop_view.go:                           errors.Errorf("error resolving dependency relation ID %d: %v", depID, err)
pkg/sql/exec_util.go:           return false, fmt.Errorf("query ID %s malformed: %s", queryID, err)
pkg/sql/group.go:                                                       v.err = pgerror.AssertionFailedf("can't evaluate %s - %v", t.Exprs[i].String(), err)
pkg/sql/show_cluster_setting.go:                                                return errors.Errorf("unable to read existing value: %s", err)
pkg/sql/show_cluster_setting.go:                                                gossipObj = fmt.Sprintf("<error: %s>", err)
pkg/sql/show_syntax.go:                 return pgerror.AssertionFailedf("unknown parser error: %v", err)

pkg/sql/row/fetcher.go:                 fmt.Fprintf(&buf, "error decoding: %v", err)

pkg/sql/sem/builtins/builtins.go:                                       return nil, pgerror.Newf(pgerror.CodeInvalidParameterValueError, "message: %s", err)

pkg/sql/sem/tree/datum.go:              suffix = fmt.Sprintf(": %v", err)
pkg/sql/sem/tree/type_check.go:                 sigWithErr := fmt.Sprintf(compExprsWithSubOpFmt, left, subOp, op, right, err)
pkg/sql/sem/tree/type_check.go:                 sigWithErr := fmt.Sprintf(compExprsFmt, left, op, right, err)
pkg/sql/sem/tree/type_check.go:                 return nil, nil, pgerror.Newf(pgerror.CodeDatatypeMismatchError, "tuples %s are not the same type: %v", Exprs(exprs), err)

pkg/sql/sqlbase/encoded_datum.go:                       return fmt.Sprintf("<error: %v>", err)
pkg/sql/sqlbase/errors.go:      return pgerror.Newf(pgerror.CodeStatementCompletionUnknownError, "%+v", err)
pkg/sql/sqlbase/structured.go:                                  return fmt.Errorf("PARTITION %s: %v", p.Name, err)
pkg/sql/sqlbase/structured.go:                          return fmt.Errorf("PARTITION %s: %v", p.Name, err)
pkg/sql/sqlbase/structured.go:                          return fmt.Errorf("PARTITION %s: %v", p.Name, err)
pkg/sql/sqlbase/system.go:              panic(fmt.Sprintf("could not marshal ZoneConfig for ID: %d: %s", keyID, err))

pkg/sql/types/types.go:         panic(pgerror.AssertionFailedf("error during Size call: %v", err))

pkg/sql/exec/error.go:                                          retErr = fmt.Errorf(fmt.Sprintf("%v", err))

pkg/sql/distsqlpb/data.go:                              panic(fmt.Sprintf("failed to serialize placeholder: %s", err))

pkg/sql/distsqlrun/hashjoiner.go:                                       err = pgerror.Wrapf(addErr, pgerror.CodeOutOfMemoryError, "while spilling: %v", err)
pkg/sql/distsqlrun/inbound.go:                                  err = pgerror.Newf(pgerror.CodeConnectionFailureError, "communication error: %s", err)

pkg/sql/pgwire/command_result.go:               panic(fmt.Sprintf("can't overwrite err: %s with err: %s", r.err, err))
pkg/sql/pgwire/conn.go:         panic(fmt.Sprintf("unexpected err from buffer: %s", err))
pkg/sql/pgwire/conn.go:         panic(fmt.Sprintf("unexpected err from buffer: %s", err))
pkg/sql/pgwire/conn.go:         panic(fmt.Sprintf("unexpected err from buffer: %s", err))
pkg/sql/pgwire/conn.go:         panic(fmt.Sprintf("unexpected err from buffer: %s", err))
pkg/sql/pgwire/conn.go:         panic(fmt.Sprintf("unexpected err from buffer: %s", err))
pkg/sql/pgwire/conn.go:         panic(fmt.Sprintf("unexpected err from buffer: %s", err))
pkg/sql/pgwire/conn.go:         panic(fmt.Sprintf("unexpected err from buffer: %s", err))
pkg/sql/pgwire/conn.go:         panic(fmt.Sprintf("unexpected err from buffer: %s", err))
pkg/sql/pgwire/conn.go:         panic(fmt.Sprintf("unexpected err from buffer: %s", err))
pkg/sql/pgwire/conn.go:         panic(fmt.Sprintf("unexpected err from buffer: %s", err))

pkg/server/debug/pprofui/server.go:                     msg := fmt.Sprintf("profile for id %s not found: %s", id, err)

pkg/ccl/changefeedccl/sink.go:                          return nil, errors.Errorf(`param %s must be a bool: %s`, sinkParamTLSEnabled, err)
pkg/ccl/changefeedccl/sink.go:                          return nil, errors.Errorf(`param %s must be base 64 encoded: %s`, sinkParamCACert, err)

pkg/util/envutil/env.go:                        panic(fmt.Sprintf("error parsing %s: %s", name, err))
pkg/util/envutil/env.go:                        panic(fmt.Sprintf("error parsing %s: %s", name, err))
pkg/util/envutil/env.go:                        panic(fmt.Sprintf("error parsing %s: %s", name, err))
pkg/util/envutil/env.go:                        panic(fmt.Sprintf("error parsing %s: %s", name, err))
pkg/util/envutil/env.go:                        panic(fmt.Sprintf("error parsing %s: %s", name, err))
pkg/util/envutil/env.go:                        panic(fmt.Sprintf("error parsing %s: %s", name, err))

pkg/util/ipaddr/ipaddr.go:              return pgerror.AssertionFailedf("unable to write to buffer: %v", err)

pkg/util/log/file.go:                   fmt.Fprintf(OrigStderr, "log: failed to remove symlink %s: %s", symlink, err)
pkg/util/log/file.go:                           fmt.Fprintf(OrigStderr, "log: failed to create symlink %s: %s", symlink, err)
pkg/util/log/reportables.go:            Errorf(context.Background(), "unable to encode stack trace: %+v", err)
pkg/util/log/reportables.go:            Errorf(context.Background(), "unable to decode stack trace: %+v", err)

pkg/util/randutil/rand.go:              panic(fmt.Sprintf("could not read from crypto/rand: %s", err))
pkg/util/version/version.go:            panic(fmt.Sprintf("invalid version '%s' passed the regex: %s", str, err))

pkg/acceptance/localcluster/cluster.go:         panic(fmt.Sprintf("must run from within the cockroach repository: %s", err))
pkg/acceptance/util_cluster.go:                                         t.Fatalf("unable to scan for length of replicas array: %s", err)

pkg/ccl/cliccl/debug.go:                                fmt.Fprintf(os.Stderr, "could not unmarshal encryption settings for file %s: %v", name, err)
pkg/ccl/cliccl/debug.go:                return "", "", fmt.Errorf("could not unmarshal encryption settings for %s: %v", keyRegistryFilename, err)

pkg/ccl/cmdccl/enc_utils/main.go:               return nil, errors.Errorf("could not read %s: %v", absPath, err)
pkg/ccl/cmdccl/enc_utils/main.go:               return nil, errors.Errorf("could not build AES cipher for file %s: %v", absPath, err)

pkg/ccl/importccl/read_import_mysql.go:                         return nil, pgerror.Unimplementedf("import.mysql.default", "unsupported default expression %q for column %q: %v", exprString, name, err)

pkg/cli/debug_synctest.go:                              fmt.Fprintf(stderr, "error after seq %d (trying %d additional writes): %v\n", lastSeq, n, err)
pkg/cli/debug_synctest.go:                      fmt.Fprintf(stderr, "error after seq %d: %v\n", lastSeq, err)

pkg/cli/error.go:                       return errors.Errorf(format, err)
pkg/cli/error.go:                       return errors.Errorf(format, extraInsecureHint(), err)
pkg/cli/error.go:                       return errors.Errorf("operation timed out.\n\n%v", err)
pkg/cli/error.go:                       return errors.Errorf("connection lost.\n\n%v", err)
pkg/cli/node.go:                        return nil, errors.Errorf("unable to parse %s: %s", str, err)
pkg/cli/sql.go:         fmt.Fprintf(stderr, "\\set %s: %v\n", strings.Join(args, " "), err)
pkg/cli/sql.go:         fmt.Fprintf(stderr, "\\unset %s: %v\n", args[0], err)
pkg/cli/sql.go:         return "", fmt.Errorf("error in external command: %s", err)
pkg/cli/sql.go:         fmt.Fprintf(stderr, "command failed: %s\n", err)
pkg/cli/sql.go:         fmt.Fprintf(stderr, "command failed: %s\n", err)
pkg/cli/sql.go:         fmt.Fprintf(stderr, "input error: %s\n", err)
pkg/cli/sql.go:         fmt.Fprintf(stderr, "warning: cannot enable safe updates: %v\n", err)
pkg/cli/sql.go:         fmt.Fprintf(stderr, "warning: cannot enable check_syntax: %v\n", err)
pkg/cli/sql_util.go:                            fmt.Fprintf(stderr, "warning: unable to restore current database: %v\n", err)
pkg/cli/sql_util.go:            fmt.Fprintf(stderr, "warning: unable to retrieve the server's version: %s\n", err)
pkg/cli/sql_util.go:            fmt.Fprintf(stderr, "warning: error retrieving the %s: %v\n", what, err)
pkg/cli/sql_util.go:            fmt.Fprintf(stderr, "warning: invalid %s: %v\n", what, err)
pkg/cli/sql_util.go:                    err = errors.Wrapf(rowsErr, "error after row-wise error: %v", err)

pkg/cmd/uptodate/uptodate.go:   fmt.Fprintf(os.Stderr, "%s: %s\n", os.Args[0], err)
pkg/cmd/urlcheck/lib/urlcheck/urlcheck.go:                              fmt.Fprintf(&buf, "%s : %s\n", url, err)

pkg/cmd/internal/issues/issues.go:              message += fmt.Sprintf("\n\nFailed to find issue assignee: \n%s", err)

pkg/cmd/prereqs/prereqs.go:             fmt.Fprintf(os.Stderr, "%s: %s\n", os.Args[0], err)

pkg/cmd/roachprod-stress/main.go:                       return fmt.Errorf("bad failure regexp: %s", err)
pkg/cmd/roachprod-stress/main.go:                       return fmt.Errorf("bad ignore regexp: %s", err)
pkg/cmd/roachprod-stress/main.go:                                               error(fmt.Sprintf("%s", err))
pkg/cmd/roachprod-stress/main.go:                                               error(fmt.Sprintf("%s", err))
pkg/cmd/roachprod-stress/main.go:                               return fmt.Errorf("unexpected context error: %v", err)

pkg/cmd/roachprod/cloud/gc.go:  _, _, err = client.PostMessage(channel, fmt.Sprintf("`%s`", err), params)
pkg/cmd/roachprod/install/cluster_synced.go:                    msg += fmt.Sprintf("\n%v", err)
pkg/cmd/roachprod/install/cluster_synced.go:                    fmt.Printf("  %2d: %v\n", c.Nodes[i], err)
pkg/cmd/roachprod/install/cluster_synced.go:                    s = fmt.Sprintf("%s: %v", out, err)
pkg/cmd/roachprod/install/cluster_synced.go:            return errors.Errorf("failed to create destination directory: %v", err)
pkg/cmd/roachprod/install/cluster_synced.go:                    return errors.Errorf("failed to sync logs: %v", err)
pkg/cmd/roachprod/install/cockroach.go:                                 msg = fmt.Sprintf("%s: %v", out, err)
pkg/cmd/roachprod/main.go:                              fmt.Fprintf(os.Stderr, "Error while cleaning up partially-created cluster: %s\n", err)
pkg/cmd/roachprod/main.go:                      fmt.Fprintf(os.Stderr, "failed to update %s DNS: %v", gce.Subdomain, err)
pkg/cmd/roachprod/main.go:                      fmt.Fprintf(os.Stderr, "%s\n", err)
pkg/cmd/roachprod/main.go:              fmt.Fprintf(os.Stderr, "unable to lookup current user: %s\n", err)
pkg/cmd/roachprod/main.go:              fmt.Fprintf(os.Stderr, "%s\n", err)
pkg/cmd/roachprod/main.go:              fmt.Printf("problem loading clusters: %s\n", err)
pkg/cmd/roachprod/tests.go:                             fmt.Printf("%s\n", err)
pkg/cmd/roachprod/tests.go:                             fmt.Printf("%s\n", err)
pkg/cmd/roachprod/tests.go:                             fmt.Printf("%s\n", err)

pkg/cmd/roachprod/vm/aws/terraformgen/terraformgen.go:  fmt.Fprintf(os.Stderr, "%v\n", err)
pkg/cmd/roachprod/vm/gce/utils.go:                      fmt.Fprintf(os.Stderr, "removing %s failed: %v", f.Name(), err)
```

## Error handling outside of CockroachDB

### Go error handling pre-v2

- https://golang.org/ref/spec#Errors
- https://github.com/golang/go/wiki/Errors
- https://golangbot.com/error-handling/
- https://gobyexample.com/errors

Summary:

- `error` is an interface
- opaque error message with `Error() string`
- how to obtain more details:
  - type assertion on underlying struct, e.g. `err.(*os.PathError)`
  - comparison of reference with singleton object, e.g. `err == io.EOF`
  - some predicate in library like `os.IsNotExists()`
  - string comparison on the result of `err.Error()`

Standard packages:

- `https://golang.org/pkg/errors/`
  - `errors.New`
  - `fmt.Errorf`
  - internally: `errors.errorString` containing a simple message

### `github.com/pkg/errors`

(NB: this is different from the standard `golang.org/pkg/errors`!)

- chains errors as a linked list
- `errors.Wrap()` / `Wrapf()`
- "next" level with `Cause() error` (non-exported `causer` interface)
- `errors.Cause()` recurses to find the first error that does not implement `causer`

- internally:
  - `errors.fundamental` "end of chain" with message + callstack
  - `errors.withStack` wrapper with stack but no message
  - `errors.withMessage` wrapper with message but no stack

- `withStack` stack trace exposed via public method `StackTrace()`, however
- `errors.fundamental` stack trace is not exposed on its own (embedded via `%+v` formatting)
- messages not directly exposed, `Error()` and formats will always embed the rest of the chain in the result string
  - it's possible to "extract" the message by rendering the wrapper and its cause separately,
    and "substracting" one from the other.

### `github.com/hashicorp/errwrap`

https://godoc.org/github.com/hashicorp/errwrap

- chains errors as a general tree
- `errwrap.Walk` to walk through all the errors
- various `Get` method to extract intermediate levels

### `upspin.io/errors`

- https://godoc.org/upspin.io/errors
- https://commandcenter.blogspot.com/2017/12/error-handling-in-upspin.html

- chains errors as a linked list
- structured and public metadata at each level of decoration
- errors have a wire representation

### Go error handling v2+

#### Error handling

https://go.googlesource.com/proposal/+/master/design/go2draft-error-handling-overview.md

- new language keywords `check` and `handle`
- `check f()` implicitly expands to `if err := f(); err != nil { ...handle... }`
- no further relevance in this RFC

#### Error value semantics

- https://go.googlesource.com/proposal/+/master/design/go2draft-error-values-overview.md

- observes that the 4 ways to obtain more details (as listed above) do
  not work well in the presence of error wrapping.
- new interface `Wrapper` that does the same as the `causer` interface
  except its method is called `Unwrap()` instead of `Cause()`
- new primitive `Is()` to check any intermediate error for equality with some reference
- new primitve `As()` to check castability of any error in the chain
- new `Formatter` interface that makes it easier to determine whether to display details
