# Logging levels (severities)

## INFO

The INFO severity is used for informational messages, when no action
is required as a result.

## WARNING

The WARNING severity is used for situations which may require special handling,
while normal operation is expected to resume automatically.

## ERROR

The ERROR severity is used for situations that require special handling,
when normal operation could not proceed as expected.
Other operations can continue mostly unaffected.

## FATAL

The FATAL severity is used for situations that require an immedate, hard
server shutdown. A report is also sent to telemetry if telemetry
is enabled.

# Logging channels

## DEV

The DEV channel is the channel used during development, to collect log
details useful for troubleshooting when it is unclear which other
channel to use. It is also the default logging channel in
CockroachDB, when the caller does not indicate a channel.

This channel is special in that there are no constraints as to
what may or may not be logged on it. Conversely, users in
production deployments are invited to not collect The DEV channel logs in
centralized logging facilities, because they likely contain
sensitive operational data.

## STORAGE

The STORAGE channel is the channel used to report low-level storage
layer events (RocksDB/Pebble).

## SESSIONS

The SESSIONS channel is the channel used to report client network activity:

- connections opened/closed.
- authentication events: logins, failed attempts.
- session and query cancellation.

This is typically configured in "audit" mode, with event
numbering and synchronous writes.

## SENSITIVE_ACCESS

The SENSITIVE_ACCESS channel is the channel used to report SQL
data access to sensitive data (when enabled):

- data access audit events (when table audit is enabled).
- SQL statements executed by users with the ADMIN bit.
- operations that write to `system` tables.

This is typically configured in "audit" mode, with event
numbering and synchronous writes.

## SQL_EXEC

The SQL_EXEC channel is the channel used to report SQL execution on
behalf of client connections:

- logical SQL statement executions (if enabled)
- pgwire events (if enabled)

## SQL_PERF

The SQL_PERF channel is the channel used to report SQL executions
that are marked to be highlighted as "out of the ordinary"
to facilitate performance investigations.
This includes the "SQL slow query log".

Arguably, this channel overlaps with SQL_EXEC defined above.
However, we keep them separate for backward-compatibility
with previous versions, where the corresponding events
were redirected to separate files.

## SQL_INTERNAL_PERF

The SQL_INTERNAL_PERF channel is like the SQL perf channel above but aimed at
helping developers of CockroachDB itself. It exists as a separate
channel so as to not pollute the SQL perf logging output with
internal troubleshooting details.

