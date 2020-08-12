- Feature Name: Log file redaction
- Status: completed
- Start Date: 2020-04-28
- Authors: knz
- RFC PR: [#48076](https://github.com/cockroachdb/cockroach/pull/48076)
- Cockroach Issue: [#46031](https://github.com/cockroachdb/cockroach/issues/46031)

# Summary

This RFC proposes a way to automate the redaction of PII and other
confidential information from CockroachDB log messages.

**Why:** This makes it possible for customers to willingly share log files with
CRL when CRL does not otherwise have access to a running cluster due
to the confidentiality of the information stored therein, or
confidentiality of the network environment. It also opens the path to
an “anonymous” mode for `cockroach zip`.

**How:**
The proposed approach is to annotate the the parts of messages that
are potentially PII-laden or confidential inside logs, and
automatically erase the annotated parts when extracting the data.

These annotations are only active when a particular command line flag
is set. Whether it is set by default is discussed below.

The solution presented here is suitable for a backport to 19.2 and
20.1, to support important large customer deployments throughout 2020.

An advantage of this approach is that it does not require CRL
engineers to change their workflow and thus does not incur
productivity overheads. It also does not require changes to most
existing infrastructure around log file parsing and collection.

**Note: in the rest of this RFC, we use the word “sensitive” or
“unsafe” to designate information that's potentially PII-laden or
confidential, and “safe” for information that's certainly known to not
be unsafe.**

Table of contents:

- [Guide-level explanation](#guide-level-explanation)
  - [Understanding logging formats](#understanding-logging-formats)
  - [Example result of redaction by `debug zip --anonymize-logs` and similar](#example-result-of-redaction-by-debug-zip---anonymize-logs-and-similar)
  - [Behavior of `cockroach debug merge-logs`](#behavior-of-cockroach-debug-merge-logs)
  - [Impact on troubleshootability](#impact-on-troubleshootability)
  - [Default configuration](#default-configuration)
- [Reference-level explanation](#reference-level-explanation)
  - [Detailed design](#detailed-design)
    - [Staged implementation](#staged-implementation)
    - [The `SafeFormatter` API](#the-safeformatter-api)
    - [Known areas in need of finer-grained structured logging](#known-areas-in-need-of-finer-grained-structured-logging)
    - [Later stage: API argument types](#later-stage-api-argument-types)
  - [Drawbacks](#drawbacks)
  - [Rationale and Alternatives](#rationale-and-alternatives)
    - [Status quo](#status-quo)
    - [New structured logging format](#new-structured-logging-format)
    - [After-log Restructuring](#after-log-restructuring)
    - [Separate always-safe logging API](#separate-always-safe-logging-api)
    - [Alternate ways to mark unsafe values](#alternate-ways-to-mark-unsafe-values)



# Guide-level explanation

From a user's perspective, the feature would work as follows:

- when enabled, the log files on disk at the running cluster would
  still contain all the details, sensitive and all.

- however, the parts of the error messages that contain sensitve
  information would be annotated with special markers, for example:
  `advertising CockroachDB node at ‹kenax:26258›` the
  sensitive string `kenax:26258` is enclosed.

- `cockroach debug merge-logs --redact` and `cockroach debug zip
  --redact-logs` automatically remove the marked parts when
  extracting logs.

  Note: within the scope of this RFC, only the logs are anonymized in
  `debug zip`. The `zip` command will also collect sensitive data in
  other data items, for example the copy of the jobs table. The user
  would still be responsible for separating the (now-stripped) logs
  from the rest of the zip contents and communicating just that to
  support. A discussion about how to streamline the anonymization of
  more `zip` output is deferred to a later stage.

- we document the special annotations, so that customers can also
  perform this redaction with their own tools and compare their
  results with the output of `debug merge-logs` and `debug zip`, to
  gain confidence that our tool does the right job (= establish
  trust).

- the admin UI would also redact, in the case where the logged-in user
  is not an admin. This way we can also relax the permission
  requirements on the log details in the admin UI since now the logs
  don't contain confidential information any more.

- the feature can be disabled with the hidden flag
  `--redactable-logs=false` in v20.2. This option may be removed in
  21.1 or later.

- in the backport version of the feature (v20.1 and prior, if
  backported), the feature is disabled by default and can opted in by
  passing the `--redactable-logs=true` flag on the server command
  line.

## Understanding logging formats

With `--redactable-log=false`, a log entry is the same as
previously:

```
I200524 10:11:00.760333 1899 kv/kvserver/replica_command.go:392  [r53/1:/{Table/65-Max}] initiating a split of this range at key /Table/65/1/hidd3n›key [r54]
```

Note: two spaces after the filename/lineno and before the logging tags. These two spaces have
always been present in CockroachDB since version v1.0. These two spaces indicate:

- marking was not used to identify potentially-sensitive information.
- the message may contain marker characters (for example in
  `hidd3n›key`); however, when it does, it is because the raw data
  being logged already contained these characters.

Contrast with the output with `--redactable-logs=true`:

```
I200524 10:11:00.760333 1899 kv/kvserver/replica_command.go:392  [r53/1:/‹{Table/65-Max}›] initiating a split of this range at key ‹/Table/65/1/hidd3n?key› [r54]
```

Note:

- marking was used to delimit the sensitive datums `{Table/65-Max}` and `/Table/65/1/hidd3n?key`
- occurrences of the markers inside the datums have been escaped: `"hidd3n›key"` has become `"hidd3n?key"`.
- there is now a vertical ellipsis "⋮" between the logging message prefix and the message itself. This
  ellipsis is introduced to indicate that the message is *redactable*. This is explained
  further in the next sub-section.


The rationale for placing the special "⋮" character on every line,
instead of a mark in the file header: we often get grepped subsets of
the file without the file header or the entire file, so something that
can work on a line-by-line basis is best.


## Example result of redaction by `debug zip --redact-logs` and similar


| Raw log string on disk server-side                                            | Output of redaction                                                  |
|-------------------------------------------------------------------------------|----------------------------------------------------------------------|
| `server/server.go:1423 ⋮ password of user ‹admin› was set to ‹"s3cr34?!@x_"›` | `server/server.go:1423 ⋮ password of user ‹×› was set to ‹×›`        |
| `server/server.go:1423  password of user admin was set to "s3cr34›!@x_"`      | `server/server.go:1423 ⋮ ‹×›`  (the entire message is stripped away) |

What is going on here?

- if the flag `--redactable-logs=true` was used server-side (recognized by
  the presence of "⋮"), then `debug zip --redact-logs` knows that
  there are marker boundaries and can request their redaction.

- if the flag `--redactable-logs=false` was used server-side, or a
  version of CockroachDB without this feature implemented (no "⋮"
  indicator), `debug zip --redact-logs` cannot determine what is
  sensitive and what isn't, at all.  Therefore, it must take the most
  conservative approach and strip the log message entirely. The
  filename/line number in the source code is preserved however, and
  can be used to manually reconstruct some information from the log
  file.

Note that the behavior of `--redact-logs` in the client is enforced
*even for server nodes that don't know how to perform redaction
themselves*; in that case, the `zip` command takes responsibility for
performing the most conservative redaction.

## Behavior of `cockroach debug merge-logs`

The `merge-log` command, like `debug zip`, becomes configurable to either:

- via `--redact`, to redact sensitive information, or not;
- via `--redactable-output`, to keep the special "⋮" and "‹..›" markers, or not.

For example, assuming the input log line:

```
server/server.go:1423 ⋮ safe ‹unsafe›
server/server.go:1424  unknownsafe
```

Then we have the following combinations:

- `merge-logs --redact=false --redactable-output=false`

  ```
  server/server.go:1423  safe unsafe
  server/server.go:1424  unknownsafe
  ```

- `merge-logs --redact=false --redactable-output=true`

  ```
  server/server.go:1423 ⋮ safe ‹unsafe›
  server/server.go:1424 ⋮ ‹unknownsafe›
  ```

- `merge-logs --redact=true --redactable-output=false`

  ```
  server/server.go:1423  safe ‹×›
  server/server.go:1424  ‹×›
  ```

- `merge-logs --redact=true --redactable-output=true`

  ```
  server/server.go:1423 ⋮ safe ‹×›
  server/server.go:1424 ⋮ ‹×›
  ```

The defaults are `--redact=false --redactable-output=true`.

## Impact on Troubleshootability

Note that the client-side `--redact-logs` still "works" and does a
somewhat useful job even if `--redactable-logs` was not enabled
server-side.

The quality / usefulness of the logs transmitted via the feature can
be summarized as follows:

|                       | `--redactable-logs=true`  | `--redactable-logs=false` |
|-----------------------|---------------------------|---------------------------|
| `--redact-logs=false` | Good (no confidentiality) | Good (no confidentiality) |
| `--redact-logs=true`  | Good (confidentiality OK) | Poor (confidentiality OK) |

## Default configuration

Server-side, default for `--redactable-logs`:

- in v20.2: TBD, probably `true` — new feature, enabled by default.
- in tests: TBD, probably `false` — we may want test logs to be marker-free to reduce their size? (unsure)
- in v20.1, v19.2 (backports): `false` — we can't break
  compatibility with 3rd party log monitoring tools in a patch release.

The default remaining to `false` for v20.1/v19.2 means that users who
deploy these versions and care about confidentiality must be
proactively informed that they should enable the feature manually,
so that logs can be readily shared should a problem ever arise.

A discussion exists about whether `--redact-logs` should be enabled
by default or not for `cockroach debug zip`.

In favor of enabling by default:

- having more information is also a liability for CRL; we don't want
  customer information except as a last resort.
- based on preliminary analysis, it appears that log files, even
  redacted, already contain sufficient information for many types of
  problems that need log-based troubleshooting.
- redacting by default grows trust in and reputation of our brand.

Against:

- redaction will make support and L2 engineers doubt whether they have
  enough information to troubleshoot, even if they do.
- if redacted logs are not actually sufficient in practice, there will
  be an additional burden on support to enquire for unredacted logs
  with users/customers.
- additional support iterations reduce productivity and increase costs
  overall.

The discussion on the RFC saw mild consensus emerge in favor of
redacting by default.

# Reference-level explanation

Table of contents:

- [Detailed design](#detailed-design)
- [Staged implementation](#staged-implementation)
- [The `SafeFormatter` API](#the-safeformatter-api)
- [Known areas in need of finer-grained structured logging](#known-areas-in-need-of-finer-grained-structured-logging)
- [Later stage: API argument types](#later-stage-api-argument-types)

## Detailed design

The proposed approach is prototyped in this PR:
https://github.com/cockroachdb/cockroach/pull/48051

It works as follows:

- every argument passed to `log.Infof` etc that is *not* enclosed in
  `redact.Safe()` / `errors.Safe()` and does not implement the
  [`SafeFormatter` interface](#the-safeformatter-api) (ex
  `SafeMessager`, see discussion below) is then enclosed by unicode
  markers "‹" and "›" the logging message.

  (Remember: the `SafeMessager` interface and `log.Safe()` has existed
  for a long time and declares that a piece of data is safe for the
  purpose of Sentry/telemetry reporting. We are reusing+enhancing this
  contract here.)

  If the item to be printed already contained the markers, they are
  replaced by an unrelated "escape" character (current implementation
  uses `?`). As the unicode characters "‹" and "›" are presumably
  extremely rare in log outputs (if at all present), this escaping
  will be equally uncommon. It needs to be present for security,
  though, to completely block the risk of unwanted data disclosure or
  other abuses of log entry parsers.

- we extend the "context tags" mechanism to also support `redact.Safe`
  (previously `log.Safe`), so that non-safe bits of data in context
  tags also gets marked.

- we change the `log.EntryDecoder` to accept a flag that indicates what to do
  with data enclosed between the special markers; either:

  - don't do anything (pass-through) - this will be used in some tests,
  - remove the markers ("flatten") - this will be used in other tests,
    for admin users in the admin UI,
    and for `cockroach debug zip` when selecting the "no
    confidentiality" mode.
  - edit the sensitive data out (redact) - this will be used for
    `cockroach debug zip` in "confidentiality" mode,  in the admin
    UI for non-admin users.

- we plumb that flag through the status RPCs that retrieve log entries.

- we make the "confidentiality mode" configurable in `cockroach debug zip` via
  a new flag.

Once this is in place, we also enhance the basic functionality as follows:

- add a whitelist of data types that can always be considered safe
  even without `redact.Safe()`, for example `roachpb.NodeID`  or `RangeID`.

- enhance the `cockroachdb/errors` library to also perform this marking
  when an error object is logged, so that we get insight into errors
  even in "confidentiality" mode.

### The `SafeFormatter` API

The need here is to mark variables (non-constant literal strings) as
"safe" for reporting.

Historically, CockroachDB has provided the `log.Safe()` wrapper which
declares safety at the point a log function is called, e.g.

```go
log.Infof(ctx, "hello %s", log.Safe(myVar))
```

A critical problem we've found is that nothing prevents the definition
of the value passed as argument to the log call from being
changed far from the logging site to start including sensitive
information, without anyone noticing.

For example:

```go
var myData = safeFunc()
// ... 100 lines of code later ...
log.Infof("hello %s", log.Safe(myData))
```

Later, it's possible to modifify the initial assignment to `var myData
= unsafeFunc()` and neither the author of that change nor the reviewer
will be reminded to switch the `log.Safe` call 100 lines later.

One step forward from this was to introduce
promote a `SafeMessager` interface, defined as follows:

```go
type SafeMessager interface {
  SafeMessage() string
}
```

With this, each data type passed to a `log` call can opt into safe
reporting by implementing that interface. Any change to an
implementation of the `SafeMessage()` then "pops out" during reviews.

Experience revealed two more problems with this interface however:

- it forces a string allocation and linear copying inside the method,
  and then again a copy in the caller. The “common idiom” in Go is
  instead to pass a streaming writer as argument and have the method
  operate on that.

- it does not enable data types that have different formattings
  depending on context. For example, an integer value could be printed
  as hex value (`Infof(ctx, "%x", myInt)`, or zero-padded `Infof(ctx,
  "%08d" myInt)` and `SafeMessager` does not support this.

The proposed evolution to this sytem is inspired by Go's standard
`fmt.Formatter` interface:

```go
// SafeFormatter is implemented by object types that want to separate
// sensitive and non-sensitive information.
type SafeFormatter interface {
   SafeFormat(s SafePrinter, verb rune)
}

// SafePrinter is provided by the caller of SafeFormat.
// There's an implementation of it in logging, and one in errors.
type SafePrinter interface {
   // inherits fmt.State to access format flags,
   // however calls to fmt.State's underlying Write method
   // considers all bytes written as unsafe.
   fmt.State

   // Spell out safe bits
   SafeString(string)  // forced by linter to only take literals
   Safe(SafeFormatter)

   // The following methods dynamicall check for the SafeFormatter
   // interface and either use that, or mark the argument
   // payload as unsafe.
   Print(args ...interface{})
   Println(args ...interface{})
   // For printf, a linter checks that the format string is
   // a constant literal, so the implementation can assume it's always
   // safe.
   Printf(format string, arg ...interface{})
}
```

With this mechanism in place, we can then implement `SafeFormatter` in various
types, for example for `RangeDescriptor`:

```go
func (r RangeDescriptor) SafeFormat(s SafePrinter, _ rune) {
    s.Printf("r%d:", r.RangeID)
    if !r.IsInitialized() {
        s.SafeString("{-}")
    } else {
        w.Print(r.RSpan())
    }
    s.SafeString(" [")

    if allReplicas := r.Replicas().All(); len(allReplicas) > 0 {
        for i, rep := range allReplicas {
            if i > 0 {
                s.SafeString(", ")
            }
            s.Safe(rep) // assumes `rep` implements SafeFormatter too
            // or alternatively: rep.SafeFormat(s, 's') to avoid an allocation.
        }
    } else {
        s.Safestring("<no replicas>")
    }
    s.Printf(", next=%d, gen=%d", r.NextReplicaID, r.Generation)
    if s := r.GetStickyBit(); !s.IsEmpty() {
        s.Printf(", sticky=%s", s)
    }
    s.SafeString("]")
}
```

This API is most useful for “complex” types with a mix of safe and
unsafe bits, or whose representation can mix safe and unsafe bits.

However, it does not help with the “simple” types; taking the example
above, how does `r.RangeID` make its way into the range descriptor
representation? Clearly, we cannot implement `(RangeID) SafeFormat()`,
because the `SafePrinter` interface forbids turning a variable value
(the current value of the RangeID) into a safe string.

For these, we introduce the following:

```go
// SafeValue is a marker interface to be implemented by types
// that alias base Go types and whose natural representation
// via Printf is always safe for reporting.
//
// This is recognized by the SafePrinter interface as
// an alternative to SafeFormatter.
//
// It is provided to decorate "leaf" Go types, such
// as aliases to int (NodeID, RangeID etc).
// A linter enforces that a type can only implement this
// interface if it aliases a base go type. More complex
// types should implement SafeFormatter instead.
//
// An automatic process during builds collects all the types
// that implement this interface, as well as all uses
// of this type, and produces a report.
//
// Changes to this report receive maximal amount of scrutiny.
type SafeValue interface {
    SafeValue()
}
```

In the provided implementation the report is automatically generated
in `docs/generated/redact_safe.md`.

### Staged implementation

The feature will be introduced in the following order:

1. the initial implementation (e.g. that of [this
   pr](https://github.com/cockroachdb/cockroach/pull/48051)) will add
   the infrastructure for log redaction, but keep the feature disabled
   by default: `--redactable-logs=false` server-side, and
   `--redact-logs=false` for `cockroach debug zip` and `cockroach
   debug merge-logs`.
   
   Meanwhile, `cockroach debug merge-logs` will see a new
   flag `--redactable-output` defaulting to `true`, which
   means "preserve markers" even though they are not enabled yet.

2. initial small-scale experimentation and iterations will work to
   increase the amount of meaningful redactable markers and create
   some education materials for the rest of the eng team.

   Some of the work identified in [Known areas in need of
   finer-grained structured
   logging](#known-areas-in-need-of-finer-grained-structured-logging)
   may be performed at this point.

3. The `--redactable-logs` starts to default to `true` server-side,
   and `--redact-logs=true` client-side.

   [The linter discussed below](#later-stage-api-argument-types) is
   introduced to create an incentive to increase the amount of
   markers by the eng team.

4. Concurrently with (3) or afterwards, the feature is backported to
   v20.1 and possibly prior, with `--redactable-logs` and
   `--redact-logs` both defaulting to `false` for backward
   compatibility.

5. Concurrently with (4), the printing code for `error` objects is
   enhanced to extract safe details from errors in a format compatible
   with this RFC. This may be either implemented as a general
   feature of `cockroachdb/errors` or specifically in CockroachDB's
   `log` package (TBD).

### Known areas in need of finer-grained structured logging

The following areas in CockroachDB need to be worked on soon
after the basic infrastructure is in place:

- Pebble and RocksDB logging: this code is interfacing with the rest
  of CockroachDB via Go's basic `log` interaace and pre-formats log
  entries, so that CockroachDB's `log` package considers all
  Pebble/RocksDB log entries as "unsafe"; for example:

  ```
  pebble/compaction.go:1287 ⋮ [n1] ‹[JOB 12] compacting: sstable created 000580›
  ```

  We need to enhance the logging interface with at least Pebble
  with discrete format/argument positional arguments.

- SQL job states need to be annotated as safe; possibly job IDs too:

  ```
  jobs/registry.go:810 ⋮ [n1] job ‹557975186965299201›: stepping through state ‹succeeded› with error ‹<nil>›
  ```

- gossip events should be annotated

  ```
  gossip/gossip.go:577 ⋮ [n1] gossip status (‹ok›, ‹1› node‹›)
  ‹gossip client (0/3 cur/max conns)›‹gossip server (0/3 cur/max conns, infos 0/0 sent/received, bytes 0B/0B sent/received)›‹›
  ```

  should be:

  ```
  gossip/gossip.go:577 ⋮ [n1] gossip status (ok, 1 node)
  gossip client (0/3 cur/max conns)
  gossip server (0/3 cur/max conns, infos 0/0 sent/received, bytes 0B/0B sent/received)
  ```

- range descriptor tags need to be split between a safe and unsafe parts:

  ```
  [n1,split,s1,r‹74/1:/{Table/65/1/5…-Max}›]
  ```

  should be:

  ```
  [n1,split,s1,r74/1:/‹{Table/65/1/5…-Max}›]
  ```

etc.

### Later stage: API argument types

A concern raised during the RFC period is that the average CRDB
engineer will not make good redacted log messages by default unless
nudged to do so.

The proposal above is *conservative*: logged data is considered unsafe
by default, and so new log calls without an incentive to mark data as
safe will be redacted out in production environments.

How to work against this?

Here the proposal is to introduce a new linter that inspects the
values passed to the logging API (`log.Infof` etc) and rejects
any argument that neither:

- implements `SafeFormatter` (nb: `log.Safe` also implements
  `SafeFormatter`, so that's implicitly included here)
- implements a new `Redactable` interface, which would
  be implemented by a new wrapper function `log.Unsafe()`.

This way, an engineer who writes:

```go
log.Infof(ctx, "some data %s does %s", unsafeVar, safeVar)
```

will receive a linter error:

```
thatfile.go:123:  unsafeVar does not implement SafeFormatter, use log.Unsafe(unsafeVar) instead
```

(No lint error is generated for `safeVar` which, for the purpose of
this example, already implements `SafeMessager`.

This way, the engineer can decide whether they implement
`SafeFormatter` for their type (once and for all, which is more
convenient); or change all their log calls using that type to add
`log.Unsafe` calls.

## Drawbacks

The proposed approach "pollutes" the log files with special characters.

FWIW, this drawback can be compensated as follows:

- all the `TestServer` / `TestCluster` instances could run with the flag
  disabled, so that test logs are always clean.

  (Note: the authors and reviewers of the RFC dislike this idea, because
  it is useful for engineers writing tests to see the redaction markers and
  get educated about their presence and purpose.)

- it's trivial to filter them out, e.g. via `sed -e 's/‹//g;s/›//g'`.

## Rationale and Alternatives

Table of contents:

- [Status quo](#status-quo)
- [New structured logging format](#new-structured-logging-format)
- [After-log Restructuring](#after-log-restructuring)
- [Separate always-safe logging API](#separate-always-safe-logging-api)
- [Alternate ways to mark unsafe values](#alternate-ways-to-mark-unsafe-values)

### Status quo

What if we don't do anything of this?

In this case, customers with sensitive data in their cluster will not
be willing to share their logs with us. This is because we can't
guarantee that some customer-specific information is not present in
logs (SQL statements, range keys, etc).

### New "structured" logging format

What if we used a different logging format, for example something
"structured" like JSON? Would we still need to do something?

(NB/Reminder: structured logging is a roadmapped feature already.)

A structured format would, at minimum, provide discrete (separate) fields for
timestamp, severity, origin (file/lineno), goroutine ID, etc.

What type of structure would we provide for the main "payload" of a log event?
Remember a log message is composed of:

- context tags which are key=value pairs enclosed in `[...]`
- the main logging message. Since our logging API is printf-based,
  the *input* for this is optional format string and then argument values.

How to capture this in a structured format? There are two design points:

1) either provide all the input as-is without preprocessing. For example,

   `log.Infof(ctx, "my format %d / %d", 123, 456)`

   would result in a payload like:

   `{ts: '2020-04-28', severity: 'info', args:["my format %d / %d", 123, 456]}`

2) or provide the data post-formatting:

   `{ts: '2020-04-28', severity: 'info', msg: "my format 123 / 456"}`

With option (1) we have to face complicated design decisions *even
without considering unsafe data redaction*:

- how do we teach consumers of log entries to re-do the formatting? We
  need to build consumer tools that re-implement printf-like
  formatting again every time the messages are to be displayed.

- certain data types display differently depending on the `%` verb
  being used.  Is there always a "raw" format we can put in the log
  file, so that the `%`-formatting is always possible later? For
  example, we can't format `%+v` if only the output of an object's
  `.String()` is included in the structured entry.

- what schema to use? Which format? There are many design points as to
  whether to use JSON, Avro or other things, which JSON (which field
  names), etc.

Then, with both options (1) and (2), we need to devise a way to
annotate sensitive data:

- with option (1), it seems like we can do this with a bitmap next to
  the argument array.

  However, what about `error` objects? These can contain a mix of
  safe and unsafe fields. The `cockroachdb/errors` library has
  introduced this distinction (for Sentry reporting). What would be a
  good way to spell out an `error` object in the structured format, so
  as to properly identify *parts* of it that are unsafe?

- with option (2), we must identify parts of the message string that are unsafe.
  It is an identical problem to solve as the one of marking log entries
  in a flat text format, so using a structured format did not buy us
  anything as alternative.

Conclusion: while a structured format *is* in the cards for a later
version of CockroachDB, it will require more design iterations and
probably thinking time.

Moreover, if/when we introduce such structured logging, we also must
build the tools to display (flatten) the structured format in tests,
while troubleshooting `debug zip` outputs, etc. This means additional
engineering effort not otherwise required by the main proposal in the
RFC.

This design overhead carries the risk of failing to deliver a good
solution for the important customer deployments planned for summer
2020.

### After-log Restructuring

The proposal in this RFC is to create structure in log messages
*upfront* (with markers), so that redaction can use that structure afterwards.

It's possible to consider an alternative approach which does not use
structure upfront.

This alternative is closest to what a “naive” solution to log
redaction looks like: “parse” the log messages and redact sensitive
bits out. The desirable property of this solution is that it works
even for log files created in historical versions of CockroachDB.

From a end-user perspective, this alternative would be able to process
a log line that looks like this:

```
kv/kvserver/replica_command.go:392  [r53] initiating a split of this range at key "/Table/65/1/hidd3n›key" [r54]
```

into this:

```
kv/kvserver/replica_command.go:392  [r53] initiating a split of this range at key "REDACTED" [r54]
```

How would this even work?

Here's the approach, which can be automated:

1. take the CockroachDB git revision number, which is spelled out at the start of the log file.
   Get a git checkout of that exact revision.

2. take the filename/lineno prefix in the log line.
   Look at the source code at that line from the copy at (1). In this case we find:

   ```go
   log.Infof(ctx, "initiating a split of this range at key %q [r%d]",
		splitKey.StringWithDirs(nil /* valDirs */, 50 /* maxLen */), rightRangeID)
   ```

3. extract the format string, `initiating a split of this range at key %q [r%d]`

4. translate the format string into a *parser* for the log line. This is possible
   because the output of `%q`-formatting is parsable, and so is that of `%d`.
   The parser would look like this:

   1. read the constant string "`initiating a split of this range at key `".
   2. read a Go escaped string between double quotes.
   3. read the constant string "` [r`".
   4. read an integer.
   5. read the constant string "`]`".

5. copy the constant strings to the redacted log output.

6. inspect the Go type of the positional arguments in the source code extracted at (2).
   This is possible thanks to the Go `analysis` package.

7. for every positional argument that implements the `SafeType` interface, or is part
   of a whitelist (e.g. `RangeID`), or is a literal constant, copy that value
   to the redacted log output.

   In the example above, the key is not whitelisted by the range ID is, so
   we get `[r54]` in the output but not the key.

For correctness, if any of these steps fails, we must assume that the
string is in fact sensitive information and must be redacted out.

To make this translation efficient, we can also *pre-build the
parsers* upon every build of CockroachDB (and possibly embed it into
the executable, so it's available for use by the log retrieval
RPC). Pre-building is possible again by using the Go `analysis`
package an identifying all the `log` calls in the source code.

Here are the reasons why this solution is unpractical, and in fact
delivers much less value than it looks at first:

- the parser at revision Y is not able to safely redact log messages
  produced at revision X. However, a CockroachDB node typically
  accumulates log files across upgrades. So the parser at revision Y
  must learn to refuse to analyze/redact files produced from
  revision X (from any revision different from Y, really).

- the data structures for this parser logic, if embedded, are going to
  bloat the CockroachDB executable even more than it is today.  If not
  embedded, we'll need to distribute a separate "redaction binary" to
  customers who want to perform redaction themselves. This will
  complicate the support story.

- most of the log calls use `%s` and `%v` formats which are not
  parsable (unless at the very end of the format, but that is not a
  majority of cases).

- many log calls use `"%s"` instead of `%q`, which again is not
  parsable.

- all the `-Depth` variants (`log.InfofDepth` etc) break the mapping
  between the filename/lineno information and the format string.

- this approach has nothing to say about the context tags printed at
  the start of the log line, and these will need to be redacted
  out conservatively.

  An additional complexity is that context tag values can contain
  occurrences of the closing bracket `]`, which would prevent safe
  parsing of the rest of the string. (This could be worked around with
  a heuristic: if there is only one `]` in the rest of the entire log
  line, then that is *necessarily* the end of the context tag
  string. However that heuristic is relatively expensive to apply.)


### Separate always-safe logging API

The alternative here is to use the [strangler
pattern](https://docs.microsoft.com/en-us/azure/architecture/patterns/strangler):

1. introduce a new logging API, e.g. `log.SafeLog(...)`, with the
   contract that *everything* logged through this API can be considered
   safe and suitable for reporting. The contract would
   be enforced largely by linting and code reviews.

   Rename the previous API endpoints to `log.DeprecatedXXX()` to
   signal the change.

   Make the logging output from this API go to different files.
   Those files would be collected by `debug zip` instead of the main
   log files.

2. teach engineers how to design their logging code so that it's
   always safe (i.e. use `log.Redact()` calls upfront) and start
   using `log.SafeLog()` in new code.

3. convert some existing "essential" logging calls in the code to use
   `log.SafeLog()` so that the functionality provides some immediate
   value: node start/join events, range log, and a few others already
   known to be widely useful for troubleshooting.

   This first minimal implementation can be backported to 20.1/19.2 and
   enable some troubleshootability for current users who are
   about confidentiality.

4. This initial seeding in (3) then serves both as an example and
   incentive for the engineering team to gradually migrate the
   remainder of our current logging calls to the new API.
   This is the "strangling" part of the strangler pattern.

5. Eventually, we deprecate and remove the non-safe API.

Pros:

- no marker, no complexity in the processing of files: the new API log
  files are safe by construction.

- it gives us an opportunity to review and improve logging messages
  as we work through the code to convert existing log calls.

Cons:

- No matter how good we are with linting and code reviews, we'll make
  mistakes, and with this new interface those mistakes would mean
  leakage of unsafe data instead of less-useful log messages.

- It's unclear whether we can effectively identify a set of
  "important" logging events to convert to the new API within the
  given time frame (summer 2020 deployments).

- There will be a long period of time with two APIs side by side,
  which will make maintenance and teaching the code more
  difficult. This will impair productivity.

- While the `log.SafeLog` API gets introduced (over the course of one
  or two release cycles), there will be more calls to the old API
  introduced in the code base. We will need to design a "catch up" process.

  Something that could aid in "strangling" is CI tooling that does not
  allow changing/adding lines following the deprecated format.

- In day-to-day programming, the ability to log potentially
  unsafe information *is important* - it helps troubleshoot
  problems during development. It is possible that we will not be able
  to remove the old API entirely for this reason. But then, we'd have
  two APIs side-by-side with the associated productivity overheads
  forever ("which one to use & when?" answered again and again)

### Alternate ways to mark unsafe values

The main proposal above is to *enclose* sensitive values using visible unicode markers.

Each of these two parts ("enclose", "visible markers") is a separate design decision:

- "enclose": we can mark the fields in other ways than adding bytes inside the message.
- "visible markers": we can use other delimiters.

#### Using range markers instead of enclosing

We can add a *new field* in the logging line with a list of range
of characters in the message payload, which indicate where the unsafe
information starts and ends.
For example,

```
log.Infof(ctx, "foo %d+%d", 123, log.Safe(456))
```

could result in the following entry:

```
I200426 20:06:45.578422 75 test.go:102 (4-6)  foo 123+456
                                       ^^^^^
```

The annotation `4-6` in this example say that the bytes between positions 4 and 6
in the string `foo 123+456` (= the substring `123`) are unsafe
and can be redacted out for confidentiality.

If there are multiple unsafe bits, we'd have multiple ranges
e.g. separated by commas.

Pros:

- the "main message" part of the log line is unaltered
- there are no special characters in the log

Cons:

- the redaction post-processing becomes more complicated and error prone.
- the byte size of log messages is larger than with the main proposal
  in the RFC.
- the collection of these annotation ranges costs more Go heap
  allocations and makes the rendering of log messages significantly
  slower than it already is (in contrast, the main proposal is
  lightweight in computational overhead).
- if a user runs their own preprocessing on the log entries and modify
  the message in any way, the ranges will be off and the redaction
  will produce incorrect results.
- it complicates the processing of *tracing* events. For these we
  would need to update the protobuf APIs to add a new field to contain
  these annotation ranges. In contrast, the main proposal embeds the
  markers in the existing string payload and do not need to add new
  API fields.

### Using invisible markers

A previous version of the RFC was proposing to use the commonly-unused
ASCII bytes SI and SO (https://en.wikipedia.org/wiki/Shift_Out_and_Shift_In_characters).

Pros:

- they appear invisible when displayed on a terminal, so that `--logtostderr`
  always hides them
- copy/paste of "flat" log messages from a terminal is facilitated.
- the markers are single-byte so it's computationally cheaper to scan
  through logged strings to escape them if present in the payload already.

Cons:

- insivible characters may cause surprise
- copy-paste with external tools could mis-behave

### Using other markers

We can use either separate "begin" and "end" marker or a single
"special quote" character. We can also use different characters than
"‹" and "›".

(In both cases, the formatting escapes/substitutes the markers
from the PII-laden datum so it's guaranteed not to be present in the
payload.)

Discussion pair vs single quote:

- pairs make it visually easier to recognize what's going on.
- pairs are easier to recover from if log messages are partially
  truncated or corrupted.

Discussion "‹" and "›" vs other markers:

- HTML markers `<sensitive>...</sensitive>`

  - pros: easy to understand

  - cons: make the payloads much larger.
  - cons: make the payloads much harder to read by the human observer.

- Invisible unicode markers e.g. INVISIBLE SEPARATOR' (U+2063) / ZERO WIDTH JOINER (U+200D)

  Pros/cons: same as in [Using invisible markers](#using-invisble-markers) above,
  except that the markers are multibyte.

- Other rarely-used ASCII characters: for example, using the `~`
  (tilde) and/or `^` (caret)

  - pros: certain characters like `~`/`^` in particular are very rarely
    used in logging so escaping will also be rare.
  - pros: single-byte and thus potentially cheaper.

  - cons: does not appear invisible when displayed in a terminal or `less -r`.
  - cons: does not "pop out" as much as "‹" and "›" when viewed via `less` or text
    editors so it's less easy to recognize visually what's going on.

