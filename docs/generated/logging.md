## Logging levels (severities)

### INFO

The `INFO` severity is used for informational messages that do not
require action.

### WARNING

The `WARNING` severity is used for situations which may require special handling,
where normal operation is expected to resume automatically.

### ERROR

The `ERROR` severity is used for situations that require special handling,
where normal operation could not proceed as expected.
Other operations can continue mostly unaffected.

### FATAL

The `FATAL` severity is used for situations that require an immedate, hard
server shutdown. A report is also sent to telemetry if telemetry
is enabled.


## Logging channels

### `DEV`

The `DEV` channel is used during development to collect log
details useful for troubleshooting that fall outside the
scope of other channels. It is also the default logging
channel for events not associated with a channel.

This channel is special in that there are no constraints as to
what may or may not be logged on it. Conversely, users in
production deployments are invited to not collect `DEV` logs in
centralized logging facilities, because they likely contain
sensitive operational data.
See [Configure logs](configure-logs.html#dev-channel).

### `OPS`

The `OPS` channel is used to report "point" operational events,
initiated by user operators or automation:

- Operator or system actions on server processes: process starts,
  stops, shutdowns, crashes (if they can be logged),
  including each time: command-line parameters, current version being run
- Actions that impact the topology of a cluster: node additions,
  removals, decommissions, etc.
- Job-related initiation or termination
- [Cluster setting](cluster-settings.html) changes
- [Zone configuration](configure-replication-zones.html) changes

### `HEALTH`

The `HEALTH` channel is used to report "background" operational
events, initiated by CockroachDB or reporting on automatic processes:

- Current resource usage, including critical resource usage
- Node-node connection events, including connection errors and
  gossip details
- Range and table leasing events
- Up- and down-replication, range unavailability

### `STORAGE`

The `STORAGE` channel is used to report low-level storage
layer events (RocksDB/Pebble).

### `SESSIONS`

The `SESSIONS` channel is used to report client network activity when enabled via
the `server.auth_log.sql_connections.enabled` and/or
`server.auth_log.sql_sessions.enabled` [cluster setting](cluster-settings.html):

- Connections opened/closed
- Authentication events: logins, failed attempts
- Session and query cancellation

This is typically configured in "audit" mode, with event
numbering and synchronous writes.

### `SQL_SCHEMA`

The `SQL_SCHEMA` channel is used to report changes to the
SQL logical schema, excluding privilege and ownership changes
(which are reported separately on the `PRIVILEGES` channel) and
zone configuration changes (which go to the `OPS` channel).

This includes:

- Database/schema/table/sequence/view/type creation
- Adding/removing/changing table columns
- Changing sequence parameters

`SQL_SCHEMA` events generally comprise changes to the schema that affect the
functional behavior of client apps using stored objects.

### `USER_ADMIN`

The `USER_ADMIN` channel is used to report changes
in users and roles, including:

- Users added/dropped
- Changes to authentication credentials (e.g., passwords, validity, etc.)
- Role grants/revocations
- Role option grants/revocations

This is typically configured in "audit" mode, with event
numbering and synchronous writes.

### `PRIVILEGES`

The `PRIVILEGES` channel is used to report data
authorization changes, including:

- Privilege grants/revocations on database, objects, etc.
- Object ownership changes

This is typically configured in "audit" mode, with event
numbering and synchronous writes.

### `SENSITIVE_ACCESS`

The `SENSITIVE_ACCESS` channel is used to report SQL
data access to sensitive data:

- Data access audit events (when table audit is enabled via
  [EXPERIMENTAL_AUDIT](experimental-audit.html))
- SQL statements executed by users with the admin role
- Operations that write to system tables

This is typically configured in "audit" mode, with event
numbering and synchronous writes.

### `SQL_EXEC`

The `SQL_EXEC` channel is used to report SQL execution on
behalf of client connections:

- Logical SQL statement executions (when enabled via the
  `sql.trace.log_statement_execute` [cluster setting](cluster-settings.html))
- uncaught Go panic errors during the execution of a SQL statement.

### `SQL_PERF`

The `SQL_PERF` channel is used to report SQL executions
that are marked as "out of the ordinary"
to facilitate performance investigations.
This includes the SQL "slow query log".

Arguably, this channel overlaps with `SQL_EXEC`.
However, we keep both channels separate for backward compatibility
with versions prior to v21.1, where the corresponding events
were redirected to separate files.

### `SQL_INTERNAL_PERF`

The `SQL_INTERNAL_PERF` channel is like the `SQL_PERF` channel, but is aimed at
helping developers of CockroachDB itself. It exists as a separate
channel so as to not pollute the `SQL_PERF` logging output with
internal troubleshooting details.

