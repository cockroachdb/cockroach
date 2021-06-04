
The supported log output formats are documented below.


- [`crdb-v1`](#format-crdb-v1)

- [`crdb-v1-count`](#format-crdb-v1-count)

- [`crdb-v1-tty`](#format-crdb-v1-tty)

- [`crdb-v1-tty-count`](#format-crdb-v1-tty-count)

- [`crdb-v2`](#format-crdb-v2)

- [`crdb-v2-tty`](#format-crdb-v2-tty)

- [`json`](#format-json)

- [`json-compact`](#format-json-compact)

- [`json-fluent`](#format-json-fluent)

- [`json-fluent-compact`](#format-json-fluent-compact)



## Format `crdb-v1`

This is the legacy file format used from CockroachDB v1.0.

Each log entry is emitted using a common prefix, described below,
followed by:

- The logging context tags enclosed between `[` and `]`, if any. It is possible
  for this to be omitted if there were no context tags.
- the text of the log entry.

Beware that the text of the log entry can span multiple lines. The following caveats apply:


- The text of the log entry can start with text enclosed between `[` and `]`.
  If there were no logging tags to start with, it is not possible to distinguish between
  logging context tag information and a `[...]` string in the main text of the
  log entry. This means that this format is ambiguous. For an unambiguous alternative,
  consider `crdb-v1-count`.

- The text of the log entry can embed arbitrary application-level strings,
  including strings that represent log entries. In particular, an accident
  of implementation can cause the common entry prefix (described below)
  to also appear on a line of its own, as part of the payload of a previous
  log entry. There is no automated way to recognize when this occurs.
  Care must be taken by a human observer to recognize these situations.

- The log entry parser provided by CockroachDB to read log files is faulty
  and is unable to recognize the aforementioned pitfall; nor can it read
  entries larger than 64KiB successfully. Generally, use of this internal
  log entry parser is discouraged for entries written with this format.

See the newer format `crdb-v2` for an alternative
without these limitations.

### Header lines

At the beginning of each file, a header is printed using a similar format as
regular log entries. This header reports when the file was created,
which parameters were used to start the server, the server identifiers
if known, and other metadata about the running process.

- This header appears to be logged at severity `INFO` (with an `I` prefix
  at the start of the line) even though it does not really have a severity.
- The header is printed unconditionally even when a filter is configured to
  omit entries at the `INFO` level.

### Common log entry prefix

Each line of output starts with the following prefix:

     Lyymmdd hh:mm:ss.uuuuuu goid [chan@]file:line marker

| Field           | Description                                                                                                                          |
|-----------------|--------------------------------------------------------------------------------------------------------------------------------------|
| L               | A single character, representing the [log level](logging.html#logging-levels-severities) (e.g., `I` for `INFO`). |
| yy              | The year (zero padded; i.e., 2016 is `16`).                                                                                |
| mm              | The month (zero padded; i.e., May is `05`).                                                                                |
| dd              | The day (zero padded).                                                                                                               |
| hh:mm:ss.uuuuuu | Time in hours, minutes and fractional seconds. Timezone is UTC.                                                                      |
| goid            | The goroutine id (omitted if zero for use by tests).                                                                                 |
| chan            | The channel number (omitted if zero for backward compatibility).                                                                     |
| file            | The file name where the entry originated.                                                                                            |
| line            | The line number where the entry originated.                                                                                          |
| marker          | Redactability marker ` + redactableIndicator + ` (see below for details).                                                  |

The redactability marker can be empty; in this case, its position in the common prefix is
a double ASCII space character which can be used to reliably identify this situation.

If the marker ` + redactableIndicator + ` is present, the remainder of the log entry
contains delimiters (‹...›) around
fields that are considered sensitive. These markers are automatically recognized
by [`cockroach debug zip`](cockroach-debug-zip.html) and [`cockroach debug merge-logs`](cockroach-debug-merge-logs.html) when log redaction is requested.


## Format `crdb-v1-count`

This is an alternative, backward-compatible legacy file format used from CockroachDB v2.0.

Each log entry is emitted using a common prefix, described below,
followed by the text of the log entry.

Beware that the text of the log entry can span multiple lines. The following caveats apply:


- The text of the log entry can embed arbitrary application-level strings,
  including strings that represent log entries. In particular, an accident
  of implementation can cause the common entry prefix (described below)
  to also appear on a line of its own, as part of the payload of a previous
  log entry. There is no automated way to recognize when this occurs.
  Care must be taken by a human observer to recognize these situations.

- The log entry parser provided by CockroachDB to read log files is faulty
  and is unable to recognize the aforementioned pitfall; nor can it read
  entries larger than 64KiB successfully. Generally, use of this internal
  log entry parser is discouraged for entries written with this format.

See the newer format `crdb-v2` for an alternative
without these limitations.

### Header lines

At the beginning of each file, a header is printed using a similar format as
regular log entries. This header reports when the file was created,
which parameters were used to start the server, the server identifiers
if known, and other metadata about the running process.

- This header appears to be logged at severity `INFO` (with an `I` prefix
  at the start of the line) even though it does not really have a severity.
- The header is printed unconditionally even when a filter is configured to
  omit entries at the `INFO` level.

### Common log entry prefix

Each line of output starts with the following prefix:

     Lyymmdd hh:mm:ss.uuuuuu goid [chan@]file:line marker tags counter

| Field           | Description                                                                                                                          |
|-----------------|--------------------------------------------------------------------------------------------------------------------------------------|
| L               | A single character, representing the [log level](logging.html#logging-levels-severities) (e.g., `I` for `INFO`). |
| yy              | The year (zero padded; i.e., 2016 is `16`).                                                                                |
| mm              | The month (zero padded; i.e., May is `05`).                                                                                |
| dd              | The day (zero padded).                                                                                                               |
| hh:mm:ss.uuuuuu | Time in hours, minutes and fractional seconds. Timezone is UTC.                                                                      |
| goid            | The goroutine id (omitted if zero for use by tests).                                                                                 |
| chan            | The channel number (omitted if zero for backward compatibility).                                                                     |
| file            | The file name where the entry originated.                                                                                            |
| line            | The line number where the entry originated.                                                                                          |
| marker          | Redactability marker ` + redactableIndicator + ` (see below for details).                                                  |
| tags    | The logging tags, enclosed between `[` and `]`. May be absent. |
| counter | The entry counter. Always present.                                                 |

The redactability marker can be empty; in this case, its position in the common prefix is
a double ASCII space character which can be used to reliably identify this situation.

If the marker ` + redactableIndicator + ` is present, the remainder of the log entry
contains delimiters (‹...›) around
fields that are considered sensitive. These markers are automatically recognized
by [`cockroach debug zip`](cockroach-debug-zip.html) and [`cockroach debug merge-logs`](cockroach-debug-merge-logs.html) when log redaction is requested.


## Format `crdb-v1-tty`

Same textual format as `crdb-v1`.

In addition, if the output stream happens to be a VT-compatible terminal,
and the flag `no-color` was *not* set in the configuration, the entries
are decorated using ANSI color codes.

## Format `crdb-v1-tty-count`

Same textual format as `crdb-v1-count`.

In addition, if the output stream happens to be a VT-compatible terminal,
and the flag `no-color` was *not* set in the configuration, the entries
are decorated using ANSI color codes.

## Format `crdb-v2`

This is the main file format used from CockroachDB v21.1.

Each log entry is emitted using a common prefix, described below,
followed by the text of the log entry.

### Entry format

Each line of output starts with the following prefix:

     Lyymmdd hh:mm:ss.uuuuuu goid [chan@]file:line marker [tags...] counter cont

| Field           | Description                                                                                                                          |
|-----------------|--------------------------------------------------------------------------------------------------------------------------------------|
| L               | A single character, representing the [log level](logging.html#logging-levels-severities) (e.g., `I` for `INFO`). |
| yy              | The year (zero padded; i.e., 2016 is `16`).                                                                                |
| mm              | The month (zero padded; i.e., May is `05`).                                                                                |
| dd              | The day (zero padded).                                                                                                               |
| hh:mm:ss.uuuuuu | Time in hours, minutes and fractional seconds. Timezone is UTC.                                                                      |
| goid            | The goroutine id (zero when cannot be determined).                                                                                   |
| chan            | The channel number (omitted if zero for backward compatibility).                                                                     |
| file            | The file name where the entry originated. Also see below.                                                                            |
| line            | The line number where the entry originated.                                                                                          |
| marker          | Redactability marker "⋮" (see below for details).                                                          |
| tags            | The logging tags, enclosed between `[` and `]`. See below.                                                       |
| counter         | The optional entry counter (see below for details).                                                                                  |
| cont            | Continuation mark for structured and multi-line entries. See below.                                                                  |

The `chan@` prefix before the file name indicates the logging channel,
and is omitted if the channel is `DEV`.

The file name may be prefixed by the string `(gostd) ` to indicate
that the log entry was produced inside the Go standard library, instead
of a CockroachDB component. Entry parsers must be configured to ignore this prefix
when present.

`marker` can be empty; in this case, its position in the common prefix is
a double ASCII space character which can be used to reliably identify this situation.
If the marker "⋮" is present, the remainder of the log entry
contains delimiters (‹...›)
around fields that are considered sensitive. These markers are automatically recognized
by [`cockroach debug zip`](cockroach-debug-zip.html) and [`cockroach debug merge-logs`](cockroach-debug-merge-logs.html)
when log redaction is requested.

The logging `tags` are enclosed between square brackets `[...]`,
and the syntax `[-]` is used when there are no logging tags
associated with the log entry.

`counter` is numeric, and is incremented for every
log entry emitted to this sink. (There is thus one counter sequence per
sink.) For entries that do not have a counter value
associated (e.g., header entries in file sinks), the counter position
in the common prefix is empty: `tags` is then
followed by two ASCII space characters, instead of one space; the `counter`,
and another space. The presence of the two ASCII spaces indicates
reliably that no counter was present.

`cont` is a format/continuation indicator:

| Continuation indicator | ASCII | Description |
|------------------------|-------|--|
| space                  | 0x32  | Start of an unstructured entry. |
| equal sign, "="        | 0x3d  | Start of a structured entry. |
| exclamation mark, "!"  | 0x21  | Start of an embedded stack trace. |
| plus sign, "+"         | 0x2b  | Continuation of a multi-line entry. The payload contains a newline character at this position. |
| vertical bar           | 0x7c  | Continuation of a large entry. |

### Examples

Example single-line unstructured entry:

     I210116 21:49:17.073282 14 server/node.go:464 ⋮ [] 23  started with engine type ‹2›

Example multi-line unstructured entry:

     I210116 21:49:17.083093 14 1@cli/start.go:690 ⋮ [-] 40  node startup completed:
     I210116 21:49:17.083093 14 1@cli/start.go:690 ⋮ [-] 40 +CockroachDB node starting at 2021-01-16 21:49 (took 0.0s)

Example structured entry:

     I210116 21:49:17.080713 14 1@util/log/event_log.go:32 ⋮ [] 32 ={"Timestamp":1610833757080706620,"EventType":"node_restart"}

Example long entries broken up into multiple lines:

     I210116 21:49:17.073282 14 server/node.go:464 ⋮ [] 23  aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa....
     I210116 21:49:17.073282 14 server/node.go:464 ⋮ [] 23 |aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa

     I210116 21:49:17.080713 14 1@util/log/event_log.go:32 ⋮ [] 32 ={"Timestamp":1610833757080706620,"EventTy...
     I210116 21:49:17.080713 14 1@util/log/event_log.go:32 ⋮ [] 32 |pe":"node_restart"}

### Backward-compatibility notes

Entries in this format can be read by most `crdb-v1` log parsers,
in particular the one included in the DB console and
also the [`cockroach debug merge-logs`](cockroach-debug-merge-logs.html)
facility.

However, implementers of previous version parsers must
understand that the logging tags field is now always
included, and the lack of logging tags is included
by a tag string set to `[-]`.

Likewise, the entry counter is now also always included,
and there is a special character after `counter`
to indicate whether the remainder of the line is a
structured entry, or a continuation of a previous entry.

Finally, in the previous format, structured entries
were prefixed with the string `Structured entry: `. In
the new format, they are prefixed by the `=` continuation
indicator.


## Format `crdb-v2-tty`

Same textual format as `crdb-v2`.

In addition, if the output stream happens to be a VT-compatible terminal,
and the flag `no-color` was *not* set in the configuration, the entries
are decorated using ANSI color codes.

## Format `json`

This format emits log entries as a JSON payload.

The JSON object is guaranteed to not contain unescaped newlines
or other special characters, and the entry as a whole is followed
by a newline character. This makes the format suitable for
processing over a stream unambiguously.

Each entry contains at least the following fields:

| Field | Description |
|-------|-------------|
| `file` | The name of the source file where the event was emitted. |
| `goroutine` | The identifier of the goroutine where the event was emitted. |
| `line` | The line number where the event was emitted in the source. |
| `redactable` | Whether the payload is redactable (see below for details). |
| `timestamp` | The timestamp at which the event was emitted on the logging channel. |


After a couple of *header* entries written at the beginning of each log sink,
all subsequent log entries also contain the following fields:

| Field               | Description |
|---------------------|-------------|
| `channel` | The name of the logging channel where the event was sent. |
| `severity` | The severity of the event. |
| `channel_numeric` | The numeric identifier for the logging channel where the event was sent. |
| `entry_counter` | The entry number on this logging sink, relative to the last process restart. |
| `severity_numeric` | The numeric value of the severity of the event. |


Additionally, the following fields are conditionally present:

| Field               | Description |
|---------------------|-------------|
| `node_id` | The node ID where the event was generated, once known. Only reported for single-tenant or KV servers. |
| `cluster_id` | The cluster ID where the event was generated, once known. Only reported for single-tenant of KV servers. |
| `instance_id` | The SQL instance ID where the event was generated, once known. Only reported for multi-tenant SQL servers. |
| `tenant_id` | The SQL tenant ID where the event was generated, once known. Only reported for multi-tenant SQL servers. |
| `tags`    | The logging context tags for the entry, if there were context tags. |
| `message` | For unstructured events, the flat text payload. |
| `event`   | The logging event, if structured (see below for details). |
| `stacks`  | Goroutine stacks, for fatal events. |

When an entry is structured, the `event` field maps to a dictionary
whose structure is one of the documented structured events. See the [reference documentation](eventlog.html)
for structured events for a list of possible payloads.

When the entry is marked as `redactable`, the `tags`, `message`, and/or `event` payloads
contain delimiters (‹...›) around
fields that are considered sensitive. These markers are automatically recognized
by [`cockroach debug zip`](cockroach-debug-zip.html) and [`cockroach debug merge-logs`](cockroach-debug-merge-logs.html) when log redaction is requested.




## Format `json-compact`

This format emits log entries as a JSON payload.

The JSON object is guaranteed to not contain unescaped newlines
or other special characters, and the entry as a whole is followed
by a newline character. This makes the format suitable for
processing over a stream unambiguously.

Each entry contains at least the following fields:

| Field | Description |
|-------|-------------|
| `f` | The name of the source file where the event was emitted. |
| `g` | The identifier of the goroutine where the event was emitted. |
| `l` | The line number where the event was emitted in the source. |
| `r` | Whether the payload is redactable (see below for details). |
| `t` | The timestamp at which the event was emitted on the logging channel. |


After a couple of *header* entries written at the beginning of each log sink,
all subsequent log entries also contain the following fields:

| Field               | Description |
|---------------------|-------------|
| `C` | The name of the logging channel where the event was sent. |
| `sev` | The severity of the event. |
| `c` | The numeric identifier for the logging channel where the event was sent. |
| `n` | The entry number on this logging sink, relative to the last process restart. |
| `s` | The numeric value of the severity of the event. |


Additionally, the following fields are conditionally present:

| Field               | Description |
|---------------------|-------------|
| `N` | The node ID where the event was generated, once known. Only reported for single-tenant or KV servers. |
| `x` | The cluster ID where the event was generated, once known. Only reported for single-tenant of KV servers. |
| `q` | The SQL instance ID where the event was generated, once known. Only reported for multi-tenant SQL servers. |
| `T` | The SQL tenant ID where the event was generated, once known. Only reported for multi-tenant SQL servers. |
| `tags`    | The logging context tags for the entry, if there were context tags. |
| `message` | For unstructured events, the flat text payload. |
| `event`   | The logging event, if structured (see below for details). |
| `stacks`  | Goroutine stacks, for fatal events. |

When an entry is structured, the `event` field maps to a dictionary
whose structure is one of the documented structured events. See the [reference documentation](eventlog.html)
for structured events for a list of possible payloads.

When the entry is marked as `redactable`, the `tags`, `message`, and/or `event` payloads
contain delimiters (‹...›) around
fields that are considered sensitive. These markers are automatically recognized
by [`cockroach debug zip`](cockroach-debug-zip.html) and [`cockroach debug merge-logs`](cockroach-debug-merge-logs.html) when log redaction is requested.




## Format `json-fluent`

This format emits log entries as a JSON payload.

The JSON object is guaranteed to not contain unescaped newlines
or other special characters, and the entry as a whole is followed
by a newline character. This makes the format suitable for
processing over a stream unambiguously.

Each entry contains at least the following fields:

| Field | Description |
|-------|-------------|
| `tag` | A Fluent tag for the event, formed by the process name and the logging channel. |
| `file` | The name of the source file where the event was emitted. |
| `goroutine` | The identifier of the goroutine where the event was emitted. |
| `line` | The line number where the event was emitted in the source. |
| `redactable` | Whether the payload is redactable (see below for details). |
| `timestamp` | The timestamp at which the event was emitted on the logging channel. |


After a couple of *header* entries written at the beginning of each log sink,
all subsequent log entries also contain the following fields:

| Field               | Description |
|---------------------|-------------|
| `channel` | The name of the logging channel where the event was sent. |
| `severity` | The severity of the event. |
| `channel_numeric` | The numeric identifier for the logging channel where the event was sent. |
| `entry_counter` | The entry number on this logging sink, relative to the last process restart. |
| `severity_numeric` | The numeric value of the severity of the event. |


Additionally, the following fields are conditionally present:

| Field               | Description |
|---------------------|-------------|
| `node_id` | The node ID where the event was generated, once known. Only reported for single-tenant or KV servers. |
| `cluster_id` | The cluster ID where the event was generated, once known. Only reported for single-tenant of KV servers. |
| `instance_id` | The SQL instance ID where the event was generated, once known. Only reported for multi-tenant SQL servers. |
| `tenant_id` | The SQL tenant ID where the event was generated, once known. Only reported for multi-tenant SQL servers. |
| `tags`    | The logging context tags for the entry, if there were context tags. |
| `message` | For unstructured events, the flat text payload. |
| `event`   | The logging event, if structured (see below for details). |
| `stacks`  | Goroutine stacks, for fatal events. |

When an entry is structured, the `event` field maps to a dictionary
whose structure is one of the documented structured events. See the [reference documentation](eventlog.html)
for structured events for a list of possible payloads.

When the entry is marked as `redactable`, the `tags`, `message`, and/or `event` payloads
contain delimiters (‹...›) around
fields that are considered sensitive. These markers are automatically recognized
by [`cockroach debug zip`](cockroach-debug-zip.html) and [`cockroach debug merge-logs`](cockroach-debug-merge-logs.html) when log redaction is requested.




## Format `json-fluent-compact`

This format emits log entries as a JSON payload.

The JSON object is guaranteed to not contain unescaped newlines
or other special characters, and the entry as a whole is followed
by a newline character. This makes the format suitable for
processing over a stream unambiguously.

Each entry contains at least the following fields:

| Field | Description |
|-------|-------------|
| `tag` | A Fluent tag for the event, formed by the process name and the logging channel. |
| `f` | The name of the source file where the event was emitted. |
| `g` | The identifier of the goroutine where the event was emitted. |
| `l` | The line number where the event was emitted in the source. |
| `r` | Whether the payload is redactable (see below for details). |
| `t` | The timestamp at which the event was emitted on the logging channel. |


After a couple of *header* entries written at the beginning of each log sink,
all subsequent log entries also contain the following fields:

| Field               | Description |
|---------------------|-------------|
| `C` | The name of the logging channel where the event was sent. |
| `sev` | The severity of the event. |
| `c` | The numeric identifier for the logging channel where the event was sent. |
| `n` | The entry number on this logging sink, relative to the last process restart. |
| `s` | The numeric value of the severity of the event. |


Additionally, the following fields are conditionally present:

| Field               | Description |
|---------------------|-------------|
| `N` | The node ID where the event was generated, once known. Only reported for single-tenant or KV servers. |
| `x` | The cluster ID where the event was generated, once known. Only reported for single-tenant of KV servers. |
| `q` | The SQL instance ID where the event was generated, once known. Only reported for multi-tenant SQL servers. |
| `T` | The SQL tenant ID where the event was generated, once known. Only reported for multi-tenant SQL servers. |
| `tags`    | The logging context tags for the entry, if there were context tags. |
| `message` | For unstructured events, the flat text payload. |
| `event`   | The logging event, if structured (see below for details). |
| `stacks`  | Goroutine stacks, for fatal events. |

When an entry is structured, the `event` field maps to a dictionary
whose structure is one of the documented structured events. See the [reference documentation](eventlog.html)
for structured events for a list of possible payloads.

When the entry is marked as `redactable`, the `tags`, `message`, and/or `event` payloads
contain delimiters (‹...›) around
fields that are considered sensitive. These markers are automatically recognized
by [`cockroach debug zip`](cockroach-debug-zip.html) and [`cockroach debug merge-logs`](cockroach-debug-merge-logs.html) when log redaction is requested.




