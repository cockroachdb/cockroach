- Feature Name: Error Handling
- Status: draft
- Start Date: 2017-05-12
- Authors: knz, dt
- RFC PR:
- Cockroach Issue:  [#5452](https://github.com/cockroachdb/cockroach/issues/5452)

# Summary

Establish patterns and helper utilities to ensure consistent handling and
presentation of errors.

# Background

Much of our code existing uses the ubiquitous `if err != nil { return err }`
pattern, sometimes with the addition of context via an `errors.Wrapf` call.
These errors can bubble up to our SQL connection handler or to a callsite that
simply passes them to `panic`. Many callsites to `panic` or `log.Fatal` construct
a string or error object containing both a statement of what went wrong as well
the value(s) in question, e.g. `missing value in column %s` or
`unexpected value: raw_bytes:"..."`.

# Motivation

Bubbling the original error back up, adding as much context as possible along
the way, makes it easier to see what went wrong and thus figure out how to fix
it. Unfortunately it also means that when our connection handler (pgwire) gets
an error, or our panic handler recovers a crash, we can make very limited
assumptions about what is actually in the error, and thus whether it is suitable
for presentation to the user, inclusion in a diagnostic crash report, etc.

Presenting users (SQL clients) with the details of internal (e.g. KV) errors
over which they may have no control is poor UX: we should strive to provide
useful, understandable, actionable errors that the user can do something about.

On the other hand, an administrator will want *detailed* information about an
internal error, to identify and fix the root cause.

Error messages that contain potentially sensitive user data cannot be included
in crash reports, and at some point in the future, if/when we add support for
encrypting stores, we may want an option to avoid inclusion in (unencrypted) log
files as well.

# Detailed Design

## Presentation to Users (i.e. errors to SQL clients)
TL;DR: The sql executor will only return `pgerror.Error`-derived errors.

While implementing SQL execution, we must consciously choose when constructing
an error whether it is intended for presentation to a user, and use `pgerror`
helpers when it is to correctly indicate that, along with its matching error
code.

Errors *not* intended for user consumption, i.e. those that have bubbled up from
some lower level over which the user may have no control should be labeled as
internal errors by wrapping with with some `InternalError` type.

When an `InternalError` reaches the connection handler (`pqwire`), we should log
the wrapped error in detail to node's logs along with a unique identifier. We
should then include that unique itenditier (and the node id) in the message
returned to the user, such that a DBA with log access, presented with the
message shown to the user, could then consult the logs to troubleshoot. The
message shown to the user should also clearly state that the error is an
"internal error", in case they require assitence from a DBA/SRE.

In the future, we may wish to completely omit the wrapped error's message from
the message presented to a user for an internal error, but that seems premature
at this time.

Initially any non-`pgerror` `error` arriving at the executor can simply wrapped
with an internal error.

If we wanted to leverage the compiler to help us ensure we were returning the
correct error types before they got to the executor, we could change the
signature of `planNode` to return a `pgerror`.

See #5452.

## Crash Reporting (and potentially Non-sensitive Log Files)

### Tag Values with `log.Safe{}` Wrapper
Add a `type Safe struct {v interface{}}` for tagging a value as non-sensitive.

If a value wrapped in `Safe` is recovered by a panic handler, it is known to be
non-sensitive and thus safe to unwrap and include in a crash report.

e.g. `panic(log.Safe{fmt.Sprintf("unknown cmd type: %s", cmd.Type})`

### Unwrap `Safe` in `log.{Info, Error, Fatal}f`
When Sprintf'ing args, unwrap any `Safe` args. In the future, we can then add
optional allow redacting of non-`Safe` args to produce non-sensitive logs.

Optional: consider adding a `log.Panicf` to change above example
`log.Panicf("unknown cmd type: %d", log.Safe{cmd.Type})`

### Add a Dual-message `error` Type
Add `log.Err(fmt, args...) error` that returns an error which contains two
sprintf'ed messages, one with all args interpolated as usual, the other with
only the args wrapped in `Safe` interpolated and the others blank. In both
cases, args wrapped in `Safe` would be unwrapped before interpolation.

Note: eager interpolation is important since args may be allocated in a buffer.

#### `type SafeError error`
As an optimization, when all args to an error are "safe", a `SafeErrorf` can
replace a dual-message error and only needs to sprintf a single message.
This would essentially just be a error tagged as known-safe.

### Wrapping/Unwrapping
When crash reporting, an error is unwrapped via `Cause()` until:
  - a Dual-Message error is found.
  - a SafeError is found.
  - no `Cause()` is available.
If no reportable error is found, we can fallback to the current behavior of
reporting the error's type and where it came from (file and line).

If we find that we frequently want to add "safe" contextual information to
errors, we can add a variant of error.Wrap which does the above dual-message
formatting.
