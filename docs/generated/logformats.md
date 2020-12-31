# Documentation for logging formats

The supported log output formats are documented below.


- [`crdb-v1`](#format-crdb-v1)

- [`crdb-v1-count`](#format-crdb-v1-count)

- [`crdb-v1-tty`](#format-crdb-v1-tty)

- [`crdb-v1-tty-count`](#format-crdb-v1-tty-count)

- [`json`](#format-json)

- [`json-compact`](#format-json-compact)

- [`json-fluent`](#format-json-fluent)

- [`json-fluent-compact`](#format-json-fluent-compact)



## Format `crdb-v1`

This is the legacy file format used from CockroachDB v1.0.

Each log entry is emitted using a common prefix, described below,
followed by:

- The logging context tags enclosed between "[" and "]", if any. It is possible
  for this to be omitted if there were no context tags.
- the text of the log entry.

Beware that the text of the log entry can span multiple lines. In particular,
the following caveats apply:


- the text of the log entry can start with text enclosed between "[" and "]".
  It is not possible to distinguish between logging context tag information
  and a "[...]" string in the main text of the log entry, if there were
  no logging tags to start with. This means that this format is ambiguous.
  Consider `crdb-v1-count` for an unambiguous alternative.

- the text of the log entry can embed arbitrary application-level strings,
  including strings that represent log entries. In particular, an accident
  of implementation can cause the common entry prefix (described below)
  to also appear on a line of its own, as part of the payload of a previous
  log entry. There is no automated way to recognize when this occurs.
  Care must be taken by a human observer to recognize these situations.

- The log entry parser provided by CockroachDB to read log files is faulty
  and is unable to recognize the aforementioned pitfall; nor can it read
  entries larger than 64KiB successfully. Generally, use of this internal
  log entry parser is discouraged.

### Common log entry prefix

Each line of output starts with the following prefix:

     Lyymmdd hh:mm:ss.uuuuuu goid [chan@]file:line marker

where the fields are defined as follows:

| Field           | Description                                                       |
|-----------------|------------------------------------------------------------------ |
| L               | A single character, representing the log level (eg 'I' for INFO). |
| yy              | The year (zero padded; ie 2016 is '16').                          |
| mm              | The month (zero padded; ie May is '05').                          |
| dd              | The day (zero padded).                                            |
| hh:mm:ss.uuuuuu | Time in hours, minutes and fractional seconds. Timezone is UTC.   |
| goid            | The goroutine id (omitted if zero for use by tests).              |
| chan            | The channel number (omitted if zero for backward-compatibility).  |
| file            | The file name where the entry originated.                         |
| line            | The line number where the entry originated.                       |
| marker          | Redactability marker (see below for details).                     |

The redactability marker can be empty; in this case, its position in the common prefix is
a double ASCII space character which can be used to reliably identify this situation.

If the marker "⋮" is present, the remainder of the log entry
contains delimiters (‹...›) around
fields that are considered sensitive. These markers are automatically recognized
by `debug zip` and `debug merge-logs` when log redaction is requested.


## Format `crdb-v1-count`

This is an alternative, backward-compatible legacy file format used from CockroachDB v2.0.

Each log entry is emitted using a common prefix, described below,
followed by the text of the log entry.

Beware that the text of the log entry can span multiple lines. In particular,
the following caveats apply:


- the text of the log entry can embed arbitrary application-level strings,
  including strings that represent log entries. In particular, an accident
  of implementation can cause the common entry prefix (described below)
  to also appear on a line of its own, as part of the payload of a previous
  log entry. There is no automated way to recognize when this occurs.
  Care must be taken by a human observer to recognize these situations.

- The log entry parser provided by CockroachDB to read log files is faulty
  and is unable to recognize the aforementioned pitfall; nor can it read
  entries larger than 64KiB successfully. Generally, use of this internal
  log entry parser is discouraged.

### Common log entry prefix

Each line of output starts with the following prefix:

     Lyymmdd hh:mm:ss.uuuuuu goid [chan@]file:line markertags counter

where the fields are defined as follows:

| Field           | Description                                                       |
|-----------------|------------------------------------------------------------------ |
| L               | A single character, representing the log level (eg 'I' for INFO). |
| yy              | The year (zero padded; ie 2016 is '16').                          |
| mm              | The month (zero padded; ie May is '05').                          |
| dd              | The day (zero padded).                                            |
| hh:mm:ss.uuuuuu | Time in hours, minutes and fractional seconds. Timezone is UTC.   |
| goid            | The goroutine id (omitted if zero for use by tests).              |
| chan            | The channel number (omitted if zero for backward-compatibility).  |
| file            | The file name where the entry originated.                         |
| line            | The line number where the entry originated.                       |
| marker          | Redactability marker (see below for details).                     |
| tags            | The logging tags, enclosed between "[" and "]". May be absent.    |
| counter         | The entry counter. Always present.                                |

The redactability marker can be empty; in this case, its position in the common prefix is
a double ASCII space character which can be used to reliably identify this situation.

If the marker "⋮" is present, the remainder of the log entry
contains delimiters (‹...›) around
fields that are considered sensitive. These markers are automatically recognized
by `debug zip` and `debug merge-logs` when log redaction is requested.


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

## Format `json`

This format emits log entries as a JSON payload.

The JSON object is guaranteed to not contain unescaped newlines
or other special characters, and the entry as a whole is followed
by a newline character. This makes the format suitable for
processing over a stream unambiguously.

Each entry contains at least the following fields:

| Field | Description |
|-------|-------------|
| `channel` | The name of the logging channel where the event was sent. |
| `severity` | The severity of the event. |
| `channel_numeric` | The numeric identifier for the logging channel where the event was sent. |
| `file` | The name of the source file where the event was emitted. |
| `goroutine` | The identifier of the goroutine where the event was emitted. |
| `line` | The line number where the event was emitted in the source. |
| `entry_counter` | The entry number on this logging sink, relative to the last process restart. |
| `redactable` | Whether the payload is redactable (see below for details). |
| `severity_numeric` | The numeric value of the severity of the event. |
| `timestamp` | The timestamp at which the event was emitted on the logging channel. |


Additionally, the following fields are conditionally present:

| Field               | Description |
|---------------------|-------------|
| `tags`    | The logging context tags for the entry, if there were context tags. |
| `message` | For unstructured events, the flat text payload. |
| `event`   | The logging event, if structured (see below for details). |
| `stacks`  | Goroutine stacks, for fatal events. |

When an entry is structured, the `event` field maps to a dictionary
whose structure is one of the documented structured events. See the reference
documentation for structured events for a list of possible payloads.

Then the entry is marked as "redactable", the `tags`, `message` and/or `event` payloads
contain delimiters (‹...›) around
fields that are considered sensitive. These markers are automatically recognized
by `debug zip` and `debug merge-logs` when log redaction is requested.




## Format `json-compact`

This format emits log entries as a JSON payload.

The JSON object is guaranteed to not contain unescaped newlines
or other special characters, and the entry as a whole is followed
by a newline character. This makes the format suitable for
processing over a stream unambiguously.

Each entry contains at least the following fields:

| Field | Description |
|-------|-------------|
| `C` | The name of the logging channel where the event was sent. |
| `sev` | The severity of the event. |
| `c` | The numeric identifier for the logging channel where the event was sent. |
| `f` | The name of the source file where the event was emitted. |
| `g` | The identifier of the goroutine where the event was emitted. |
| `l` | The line number where the event was emitted in the source. |
| `n` | The entry number on this logging sink, relative to the last process restart. |
| `r` | Whether the payload is redactable (see below for details). |
| `s` | The numeric value of the severity of the event. |
| `t` | The timestamp at which the event was emitted on the logging channel. |


Additionally, the following fields are conditionally present:

| Field               | Description |
|---------------------|-------------|
| `tags`    | The logging context tags for the entry, if there were context tags. |
| `message` | For unstructured events, the flat text payload. |
| `event`   | The logging event, if structured (see below for details). |
| `stacks`  | Goroutine stacks, for fatal events. |

When an entry is structured, the `event` field maps to a dictionary
whose structure is one of the documented structured events. See the reference
documentation for structured events for a list of possible payloads.

Then the entry is marked as "redactable", the `tags`, `message` and/or `event` payloads
contain delimiters (‹...›) around
fields that are considered sensitive. These markers are automatically recognized
by `debug zip` and `debug merge-logs` when log redaction is requested.




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
| `channel` | The name of the logging channel where the event was sent. |
| `severity` | The severity of the event. |
| `channel_numeric` | The numeric identifier for the logging channel where the event was sent. |
| `file` | The name of the source file where the event was emitted. |
| `goroutine` | The identifier of the goroutine where the event was emitted. |
| `line` | The line number where the event was emitted in the source. |
| `entry_counter` | The entry number on this logging sink, relative to the last process restart. |
| `redactable` | Whether the payload is redactable (see below for details). |
| `severity_numeric` | The numeric value of the severity of the event. |
| `timestamp` | The timestamp at which the event was emitted on the logging channel. |


Additionally, the following fields are conditionally present:

| Field               | Description |
|---------------------|-------------|
| `tags`    | The logging context tags for the entry, if there were context tags. |
| `message` | For unstructured events, the flat text payload. |
| `event`   | The logging event, if structured (see below for details). |
| `stacks`  | Goroutine stacks, for fatal events. |

When an entry is structured, the `event` field maps to a dictionary
whose structure is one of the documented structured events. See the reference
documentation for structured events for a list of possible payloads.

Then the entry is marked as "redactable", the `tags`, `message` and/or `event` payloads
contain delimiters (‹...›) around
fields that are considered sensitive. These markers are automatically recognized
by `debug zip` and `debug merge-logs` when log redaction is requested.




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
| `C` | The name of the logging channel where the event was sent. |
| `sev` | The severity of the event. |
| `c` | The numeric identifier for the logging channel where the event was sent. |
| `f` | The name of the source file where the event was emitted. |
| `g` | The identifier of the goroutine where the event was emitted. |
| `l` | The line number where the event was emitted in the source. |
| `n` | The entry number on this logging sink, relative to the last process restart. |
| `r` | Whether the payload is redactable (see below for details). |
| `s` | The numeric value of the severity of the event. |
| `t` | The timestamp at which the event was emitted on the logging channel. |


Additionally, the following fields are conditionally present:

| Field               | Description |
|---------------------|-------------|
| `tags`    | The logging context tags for the entry, if there were context tags. |
| `message` | For unstructured events, the flat text payload. |
| `event`   | The logging event, if structured (see below for details). |
| `stacks`  | Goroutine stacks, for fatal events. |

When an entry is structured, the `event` field maps to a dictionary
whose structure is one of the documented structured events. See the reference
documentation for structured events for a list of possible payloads.

Then the entry is marked as "redactable", the `tags`, `message` and/or `event` payloads
contain delimiters (‹...›) around
fields that are considered sensitive. These markers are automatically recognized
by `debug zip` and `debug merge-logs` when log redaction is requested.




