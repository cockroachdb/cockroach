- Feature Name: PII erasure in logs (and maybe traces)
- Status: draft
- Start Date: 2020-04-28
- Authors: knz
- RFC PR: [#48076](https://github.com/cockroachdb/cockroach/pull/48076)
- Cockroach Issue: [#46031](https://github.com/cockroachdb/cockroach/issues/46031)

# Summary

This RFC proposes a way to automate the redaction of PII or other
customer-sensitive information from CockroachDB log and trace
messages.

This makes it possible for customers to willingly share `cockroach
debug zip` output and other trace-related debugging data with CRL when
CRL does not otherwise have access to a running cluster due to the
confidentiality of the information stored therein, or confidentiality
of the network environment.

It also makes it possible for non-admin users to access logs and
traces from the web UI without risking breaching the confidentiality
of data that would not otherwise be visible to them.

The solution presented here is suitable for a backport to 19.2 and
20.1, to support important large customer deployments throughout 2020.

The proposed approach is to annotate the the parts of messages that
are potentially PII-laden inside logs (and possibly traces), and automatically
erase the annotated parts when extracting the data.

These annotations are only active when a particular command line flag
is set. Whether it is set by default is discussed below.

An advantage of this approach is that it does not require CRL
engineers to change their workflow and thus does not incur
productivity overheads. It also does not require changes to most
existing infrastructure around log file parsing and collection.

# Guide-level explanation

From a user's perspective, the feature would work as follows:

- the feature is enabled by passing the `--redactable-logs` flag on
  the server command line.

- when enabled, the log files on disk at the running cluster would
  still contain all the details.

- however, the parts of the error messages that contain PII or
  potentially sensitive information would be annotated with special
  markers, for example: `advertising CockroachDB node at ‹kenax:26258›`
  the potentially-sensitive string `kenax:26258` is enclosed.

- `cockroach debug zip --anonymize-logs` automatically removes the
  marked parts when extracting logs.
  
  Note: within the scope of this RFC, only the logs are
  anonymized. `debug zip` will also collect confidential data in other
  data items, for example the copy of the jobs table. The user would
  still be responsible for separating the (now-stripped) logs from the
  rest of the zip contents and communicating just that to support. A
  discussion about how to streamline the anonymization of more `zip`
  output is deferred to a later stage.

- we document the special annotations, so that customers can also
  perform this redaction with their own tools and compare their
  results with the output of `debug zip`, to gain confidence that our
  tool does the right job (= establish trust).

- the admin UI would also redact, in the case where the logged-in user
  is not an admin. This way we can also relax the permission
  requirements on the log details in the admin UI since now the logs
  don't contain confidential information any more.

## Understanding logging formats

Without `--redactable-logs` enabled, a log entry is the same as previously:

```
I200508 12:34:36.229124 56 server/server.go:1423  password of user admin was set to "s3cr34›!@x_"
```

Note: two spaces after the filename/lineno and before the logging tags. These two spaces have
always been present in CockroachDB since version v1.0. These two spaces indicate:

- marking was not used to identify potentially-sensitive information.
- the message may contain marker characters (for example in
  `s3cr34›!@x_`); however, when it does, it is because the raw data
  being logged already contained these characters.

Contrast with the output when `--redactable-logs` is enabled:

```
I200508 12:34:36.229124 56 server/server.go:1423 ⋮ password of user ‹admin› was set to ‹"s3cr34?!@x_"›
```

Note:

- marking was used to delimit the sensitive datums `admin` and `"s3cr34?!@x_"`
- occurrences of the markers inside the datums have been escaped: `"s3cr34‹!@x_"` has become `"s3cr34?!@x_"`.
- there is now a vertical ellipsis "⋮" between the logging message prefix and the message itself. This
  ellipsis is introduced to indicate that the message is *redactable*. This is explained
  further in the next sub-section.

## Example result of redaction by `debug zip --anonymize-logs` and similar


| Raw log string on disk server-side                                            | Output of redaction                                                         |
|-------------------------------------------------------------------------------|-----------------------------------------------------------------------------|
| `server/server.go:1423 ⋮ password of user ‹admin› was set to ‹"s3cr34?!@x_"›` | `server/server.go:1423 ⋮ password of user ‹REDACTED› was set to ‹REDACTED›` |
| `server/server.go:1423  password of user admin was set to "s3cr34›!@x_"`      | `server/server.go:1423`  (the entire message is stripped away)              |

What is going on here?

- if the flag `--redactable-logs` was used server-side (recognized by
  the presence of "⋮"), then `debug zip --anonymize-logs` knows that
  there are marker boundaries and can request their redaction.

- if the flag `--redactable-logs` was not used server-side (no "⋮"
  indicator), `debug zip --anonymize-logs` cannot determine what is
  sensitive and what isn't, at all.  Therefore, it must take the most
  conservative approach and strip the log message entirely. The
  filename/line number in the source code is preserved however, and
  can be used to manually reconstruct some information from the log
  file.

## Impact on Troubleshootability

Note that the client-side `--anonymize-logs` still "works" and does a
somewhat useful job even if `--redactable-logs` was not enabled
server-side.

The quality / usefulness of the logs transmitted via the feature can
be summarized as follows:

|                          | `--redactable-logs=true`  | `--redactable-logs=false` |
|--------------------------|---------------------------|---------------------------|
| `--anonymize-logs=false` | Good (no confidentiality) | Good (no confidentiality) |
| `--anonymize-logs=true`  | Good (confidentiality OK) | Poor (confidentiality OK) |

## Default configuration

Server-side, default for `--redactable-logs`:

- in v20.2: TBD, probably `true` — new feature, enabled by default.
- in tests: `false` — test logs are always marker-free.
- in v20.1, v19.2 (backports): `false` — we can't break
  compatibility with 3rd party log monitoring tools in a patch release.

The default remaining to `false` for v20.1/v19.2 means that users who
deploy these versions and care about confidentiality must be
proactively informed that they should enable the feature manually,
so that logs can be readily shared should a problem ever arise.

Client-s1ize, `cockroach debug zip` always defaults to
`--anonymize-logs=false`. The log entries are retrieved in full by
default.

Technical support should inform users *who already have expressed
concern about confidentiality* that the flags exist and they can use
them. We do not want to promote the use of the flags unless a concern
was already voiced by the user, because "more information" always
helps with troubleshooting problems faster.

# Reference-level explanation

## Detailed design

The proposed approach is prototyped in this PR:
https://github.com/cockroachdb/cockroach/pull/48051

It works as follows:

- every argument passed to `log.Infof` etc that is *not* enclosed in
  `log.Safe()` is then enclosed by unicode markers "‹" and "›" the
  logging/tracing message.

  (Remember: `log.Safe()` has existed for a long time and declares
  that a piece of data is PII- or confidential- free for the purpose
  of Sentry/telemetry reporting. We are reusing this contract here.)

  If the item to be printed already contained the markers, they are
  replaced by an unrelated "escape" character (current implementation
  uses `?`). As the unicode characters "‹" and "›" are presumably
  extremely rare in log outputs (if at all present), this escaping
  will be equally uncommon. It needs to be present for security,
  though, to completely block the risk of unwanted data disclosure or
  other abuses of log entry parsers.

- we extend the "context tags" mechanism to also support `log.Safe`,
  so that non-safe bits of data in context tags also gets marked.

- we change the `log.EntryDecoder` to accept a flag that indicates what to do
  with data enclosed between the special markers: either

  - don't do anything (pass-through) - this will be used in some tests,
  - remove the markers ("flatten") - this will be used in other tests,
    in SQL trace outputs by default, for admin users in the admin UI,
    and for `cockroach debug zip` when selecting the "no
    confidentiality" mode.
  - edit the sensitive data out (redact) - this will be used for
    `cockroach debug zip` in "confidentiality" mode,  in the admin
    UI for non-admin users, in SQL trace outputs with a flag.

- we plumb that flag through the status RPCs that retrieve log entries.

- we make the "confidentiality mode" configurable in `cockroach debug zip` via
  a new flag.

Once this is in place, we can enhance the basic functionality as follows:

- add a whitelist of data types that can always be considered PII-free
  even without `log.Safe()`, for example `roachpb.NodeID`  or `RangeID`.

- enhance the `cockroachdb/errors` library to also perform this marking
  when an error object is logged, so that we get insight into errors
  even in "confidentiality" mode.

## Drawbacks

The proposed approach "pollutes" the log files with special characters.

FWIW, this drawback is compensated as follows:

- all the `TestServer` / `TestCluster` instances run with the flag
  disabled, so that test logs are always clean.
- it's trivial to filter them out, e.g. via `sed -e 's/‹//g;s/›//g'`.

## Rationale and Alternatives

### Status quo

What if we don't do anything of this?

In this case, customers with sensitive data in their cluster will not
be willing to share their logs with us. This is because we can't
guarantee that some customer-specific information is not present in
logs (SQL statements, range keys, etc).

### New "structured" logging format

What if we used a different logging format, for example something
"structure" like JSON? Would we still need to do something?

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
without considering PII redaction*:

- how do we teach consumer of log entries to re-do the formatting? We
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
annotate PII-laden data:

- with option (1), it seems like we can do this with a bitmap next to
  the argument array.

  However, what about `error` objects? These can contain a mix of
  PII-laden and PII-free fields. The `cockroachdb/errors` library has
  introduced this distinction (for Sentry reporting). What would be a
  good way to spell out an `error` object in the structured format, so
  as to properly identify *parts* of it that are PII-laden?

- with option (2), we must identify parts of the message string that are PII-laden.
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

### Separate always-safe logging API

The alternative here is to use the [strangler
pattern](https://docs.microsoft.com/en-us/azure/architecture/patterns/strangler):

1. introduce a new logging API, e.g. `log.SafeLog(...)`, with the
   contract that *everything* logged through this API can be considered
   PII-safe and suitable for reporting. The contract would
   be enforced largely by linting and code reviews.
   
   Rename the previous API endpoints to `log.DeprecatedXXX()` to
   signal the change.

   Make the logging output from this API go to different files.
   Those files would be collected by `debug zip` instead of the main
   log files.

2. teach engineers how to design their logging code so that it's
   always PII-safe (i.e. use `log.Redact()` calls upfront) and start
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
  files are PII-free by construction.

- it gives us an opportunity to review and improve logging messages
  as we work through the code to convert existing log calls.

Cons:

- No matter how good we are with linting and code reviews, we'll make
  mistakes, and with this new interface those mistakes would mean
  leakage of PII instead of less-useful log messages.

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

- In day-to-day programming, the ability to log/trace potentially
  PII-laden information *is important* - it helps troubleshoot
  problems during development. It is possible that we will not be able
  to remove the old API entirely for this reason. But then, we'd have
  two APIs side-by-side with the associated productivity overheads
  forever ("which one to use & when?" answered again and again)

### Alternate ways to mark PII-laden values

The main proposal above is to *enclose* sensitive values using visible unicode markers.

Each of these two parts ("enclose", "visible markers") is a separate design decision:

- "enclose": we can mark the fields in other ways than adding bytes inside the message.
- "visible markers": we can use other delimiters.

#### Using range markers instead of enclosing

We can add a *new field* in the logging line with a list of range
of characters in the message payload, which indicate where the PII-laden
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
in the string `foo 123+456` (= the substring `123`) are potentially PII-laden
and can be redacted out for confidentiality.

If there are multiple PII-laden bits, we'd have multiple ranges
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

