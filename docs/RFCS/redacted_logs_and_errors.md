- Feature Name: Handling Sensitive Data in Logs and Crash Reports
- Status: draft
- Start Date: 2017-05-15
- Authors: knz, dt
- RFC PR:
- Cockroach Issue:

# Summary

Establish patterns and helper utilities for handling potentially sensitive
content in logged errors and crash reports.

# Background

Many of our logged messages and errors, including those handled by our crash
reporting, contain potentially sensitive information in them, like column names
or values, unexpected byte strings, etc. A `log.Fatal` call construct a string
or error object containing both a statement of what went wrong as well the
value(s) in question, e.g. `missing value in column %s` or `unexpected value:
raw_bytes:"..."`.

This means our current log files have to be treated with the same care and
diligence as our raw data, and our crash reports are very limited in the useful
information they can report (just the type and location of an error) without
risking leaking sensitive data.

# Motivation

Users may want send send log files to an aggregator or external service, or may
even just want to write them to a different disk with different access controls.
At some point in the future, we may also add support for encrypting stores.

In all of these cases, including potentially sensitive, raw user data in logged
messages could be a problem.

Our crash reports could also be much more useful if they could include the non-
sensitive portions of errors -- the statement of what went wrong and values that
are not sensitive, like a command type or flag.

# Detailed Design

## Tag Values with `log.Safe{}` Wrapper
Add a `type Safe struct {v interface{}}` to the `log` package, for tagging a
value as non-sensitive.

## Unwrap `Safe`-Tagged Args To `log.{Info, Error, Fatal}f`
When `Sprintf`ing args in log messages, unwrap any `Safe` args.

## Optionally Redact non-`Safe` Fields in Logged Messages
When running with scrubbed logs, while `Sprintf`ing log messages, any args *not*
wrapped in `Safe` are redacted, to produce non-sensitive logs.

Note: Some types (ints?, bools?) might be safe to assume are non-sensitive even if not
tagged.


## Add Dual-message `error` Type and Helpers
Add helpers to construct `error`s that contain both a detailed, potentially
sensitive message as well as a "safe" message that does not contain sensitive
data.

```
type LoggableError struct {
  message string
  redacted string
  cause error
}
```

`Errorf(fmt string, args ...interface{})` and `Wrapf(err, fmt, args)` helpers to
construct these (mirroring their counterparts in `pkg/errors`) which `sprintf`
args both directly, for message, and then as described above, for the redacted
string.

These helpers will likely need to reside in a new package, not `log`, as the
`Errorf` name is already in use in the `log` package.
Perhaps `loggable`? In action, this would end up more or less like:
```
return loggable.Errorf("cannot use %s value in for column %s, log.Safe{c.Typ}, c.Name)
```

Note: eager interpolation is important since args may be allocated in a buffer.

## Crash Reporting
When reporting a crash, if the recovered arg to `panic` is either `log.Safe` or
a `LoggableError`, we can include it (or its `.redacted` string), rather than
the default of only its type and callsite (e.g. `errors.wrappedError somefile.go:431`).

Optional: we can add `log.Panicf` shorthand if we find an excess of
`panic(loggable.Errorf(...))` calls cumbersome, though unification with
log.Fatal may be preferable?
