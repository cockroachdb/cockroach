- Feature Name: Logging modernization
- Status: draft
- Start Date: 2020-07-28
- Authors: knz
- RFC PR: [#51990](https://github.com/cockroachdb/cockroach/pull/51990)
- Cockroach Issue:
  [#51007](https://github.com/cockroachdb/cockroach/issues/51007)
  [#44755](https://github.com/cockroachdb/cockroach/issues/44755)
  [#50905](https://github.com/cockroachdb/cockroach/issues/50905)
  [#40306](https://github.com/cockroachdb/cockroach/issues/40306)
  [#32102](https://github.com/cockroachdb/cockroach/issues/32102)

# Summary

This RFC proposes to modernize CockroachDB's logging infrastructure in
several ways:

**Goal A.** by redirecting several existing logging events to dedicated
channels, where they can become more readily discoverable.  For
example, we want to send node start/stop/restart events to a
dedicated "operations" channel, distinct from the general "debug"
channel, to ease the monitoring of important cluster events.

**Goal B.** by duplicating the event log (currently only written to
`system.event_log`) to a logging channel, where it can be extracted
from the cluster even when the stored event log becomes unavailable.

**Goal C.** by replacing "secondary loggers" by logging *channels*, to offer
a simpler and more uniform configuration mechanism than previously.

**Goal D.** by automatically deriving documentation about logging events sent to
non-debug channels.

This work is motivated by a combination of factors:

- we have at least one strong customer commitment to separate
  various operational and security-related logging events on their own
  logging channels, away from the "main log", so they can be collected
  separately. The customers in particular are interested to reduce
  the noise of the data subject to their 3rd party monitoring systems.

- we have several security objectives (a combination of user requests
  and compliance requirements) which demand separate logging for
  security-related events: user and role management, granting
  privileges, etc.

- the current logging system has been built by organic addition
  of bits and pieces each serving a particular purpose. We want
  to reduce this complexity to a simple and uniform system before
  we add any more complexity to satisfy the goals above.

To achieve this, the RFC proposes to perform the following changes:

- replace the previous "secondary logger" mechanism by a
  newly-introduced "logging channel" abstraction.

- by extending the logging API to simplify and promote the use of
  channels throughout the source tree. A part of this new API is
  auto-generated, to simplify future additions of new channels or
  severity levels.

- by changing the logging configuration to operate on channels.

- by introducing a doc generator.

A prototype implementation has been produced here: https://github.com/cockroachdb/cockroach/pull/51987

Table of contents:

- [Motivation](#motivation)
  - [Log collection for health monitoring](#log-collection-for-health-monitoring)
  - [Log collection for security](#log-collection-for-security)
  - [Future: network and structured logging](#future-network-and-structured-logging)
  - [Logging configuration](#logging-configuration)
- [Guide-level explanation](#guide-level-explanation)
  - [From the perspective of DBAs and operators](#from-the-perspective-of-dbas-and-operators)
    - [What is the same](#what-is-the-same)
    - [What is different](#what-is-different)
    - [List of logging channels](#list-of-logging-channels)
    - [Command-line configuration](#command-line-configuration)
  - [From the perspective of CockroachDB developers](#from-the-perspective-of-cockroachdb-developers)
  - [From the perspective of documentation writers and readers](#from-the-perspective-of-documentation-writers-and-readers)
- [Reference-level implementation](#reference-level-implementation)
  - [Detailed design](#detailed-design)
    - [Changes to the logging package](#changes-to-the-logging-package)
    - [Changes to the build rules](#changes-to-the-build-rules)
    - [Changes to the logging calls](#changes-to-the-logging-calls)
  - [Drawbacks](#drawbacks)
  - [Rationales and Alternatives](#rationales-and-alternatives)
  - [Unresolved questions](#unresolved-questions)

# Motivation

## Log collection for health monitoring

Automated health monitoring is usually performed by scanning logs for
abnormal events. This is what is done e.g. automatically in Datadog.
Some of our customers also export their logs into ElasticSearch and run
their own monitoring tools and heuristics on top.

In this use case, CockroachDB's logs currently contain too much noise:
many debugging and informational messages are logged, including some
messages that only occur on edge cases without impact on overall
cluster health. These messages appear as “irregular behavior” from the
perspective of monitoring tools, even though they are innocuous and do
not require action in the common case.

Users have requested in the past that we “identify which log messages
are important” or enable automation of “extraction of logging events
that are important to understand cluster health”. These requests stem
from the current difficulty to sort the ‘chaff’ from the ‘wheat’ when
viewing logs.

## Log collection for security

Automated Intrusion Detection Systems (IDS) and automatic advance
detection of DoS attacks operate like health monitoring described
above: security-related events in logs are scanned to recognize
patterns of abnormal behavior. For the same reasons as above, users
have requested to extract security-sensitive events automatically to
reduce the amount of noise fed into IDS and DoS detection tools.

Additionally, certain security regulations (compliance reqs) mandate
*audit logs* which record sensitive actions performed by operators:
configuration, user management, etc; these audit logs must provide
moderate
[non-repudiation](https://en.wikipedia.org/wiki/Non-repudiation)
guarantees so that they can be later analyzed to determine “who did
what and when”.

Non-repudiation impacts performance, because it requires synchronous
writes (to protect against repudiation by crashing a server) and, if
possible, synchronization to a network service (to protect against
repudiation by deleting log files). Therefore, it is important that
non-repudiation parameters are only applied to the subset of the
logging events where it is needed. If we were to apply the parameters
to all logging, this would impact overall cluster performance and
health under contention, when nodes emit frequent health and
troubleshooting details in their logs.

## Future: network and structured logging

CockroachDB currently logs to files.  Users who wish to export their
logs must set up 3rd party automation to scrape log files and send
them to a separate service, e.g. Datadog S3 or ElasticSearch.

This automation is particularly cumbersome when running CockroachDB
inside containers, where the logging directory is (in default
configurations) hard to observe from a process running out of the
container. Configuration of scraping from files thus always requires
extra effort and ad-hoc configuration parameters.

Users have requested in the past to simplify CockroachDB deployments
by removing the need for scraping log files. The request is that
CockroachDB should proactively send its logs over the network
to a log collector.

Separately, log collectors are better able to parse and index log
events when they can readily separate the different parts of a log
message.  Currently CockroachDB's logs are text-based with a single
text line per log message. To simplify integrations with log
collectors, users have requested that CockroachDB directly emits log
entries in a structured format, to avoid a lossy conversion to the
text-only format.

Answering these needs directly remains out of scope for this RFC, but
this motivation will guide the proposal and pave the road for this
future work.

## Logging configuration

CockroachDB currently has a wealth of (arguably under-documented)
logging parameters:

- It supports logging to files and/or to the `cockroach` process'
  standard error stream. Later, we will envision network sinks. Which
  outputs are enabled is individually configurable.

- Addititionally, it also supports filtering logging event by *severity*
  (a numeric level for logging events) when writing to files.

- It also supports a *separate* severity filter when writing to the
  standard error stream.

- Additionally, it also already supports redirecting log files to
  different directories than data storage, for example to a different
  disk storage than that used for the main data files (to protect IOPS
  quota burnout from logging activity).

- Additionally, it also already supports using different logging
  directories for different logs, for example it is possible to separate
  the main logs from that used for [SQL audit
  logging](https://www.cockroachlabs.com/docs/v20.1/sql-audit-logging.html)
  (an experimental feature introduced in v2.1).

- Additionally, it also supports the introduction of [redaction
  markers](20200427_log_file_redaction.md) to annotate potentially
  sensitive information.

Each of these configuration options has been implemented using ad-hoc mechanisms
and their own command-line flags, grown organically on top of each other.

Before new solutions can be added to answer the needs expressed above,
we thus wish to reduce the complexity of the configuration mechanisms
by making the configuration more uniform and easier to explain.

# Guide-level explanation

## From the perspective of DBAs and operators

Each logging event is emitted on exactly one *logical channel*, which
broadly defines a logging category.

Logging channels are analogous to [logging *facilities* in
Syslog](https://en.wikipedia.org/wiki/Syslog), or [logging *services*
in
Datadog](https://docs.datadoghq.com/logs/log_collection/?tab=http#reserved-attributes).

The destination of logging messages (where they are written to) can be configured
separately for each channel:

- whether to use disk or, later, network;
- whether the writes are synchronous (useful for audit logs);
- whether the events are numbered (useful for audit logs);
- what parameters to use for the writes, for example which directory to use for files;
- whether to copy entries to the process' standard error output.

On each channel, events have (like previously) a *severity level*. These indicate
the “importance” of the logging event.

Severities are analogous to [logging severities in
Syslog](https://en.wikipedia.org/wiki/Syslog) and [logging *status* in
Datadog](https://docs.datadoghq.com/logs/log_collection/?tab=http#reserved-attributes).

### What is the same

The severities are unchanged from previous versions: INFO, WARNING,
ERROR, FATAL. The introduction of finer-grain severities is out of
scope of this RFC (but not incompatible with it).

By default, CockroachDB writes events of different channels to
different log files in the `logs` sub-directory of the first
disk-based store directory. This is unchanged.

The `--logtostderr` flag is recognized for backward-compatibility as an
override for the stderr event filter for all the channels
simultaneously.

The `--log-dir` flag is recognized for backward-compatibility as an
override for the logging directory for all the channels
simultaneously. However, this flag is now hidden and deprecated.

The `--logfile-verbosity` flag is recognized for
backward-compatibility as a filter for events written to files for
all the channels simultaneously. However, this flag is now hidden and
deprecated.

The `--log-file-max-size` and `--log-group-max-size` parameters are
recognized for backward-compatibility as maximum size/number
parameters for files written by all the channels. However, these flags
are now hidden and deprecated.

### What is different

There were already event channels previously in CockroachDB, albeit
not documented: “general” (debug) logs, the RocksDB/Pebble log, SQL
audit logging, SQL statement logs and the SQL slow query log.

These channels are preserved but they are now renamed:

| Previous name      | Previous generated file names | New name                            | Generated file names                 |
|--------------------|-------------------------------|-------------------------------------|--------------------------------------|
| “general” log      | `cockroach.log`               | DEV channel                         | `cockroach.log` (unchanged)          |
| RocksDB log        | `cockroach-rocksdb.log`       | STORAGE channel                     | `cockroach-storage.log`              |
| Pebble log         | `cockroach-pebble.log`        | STORAGE channel                     | `cockroach-storage.log`              |
| SQL audit log      | `cockroach-sql-audit.log`     | SENSITIVE_ACCESS channel            | `cockroach-sensitive-access.log`     |
| SQL exec log       | `cockroach-sql-exec.log`      | SQL_EXEC channel                    | `cockroach-sql-exec.log` (unchanged) |
| SQL slow query log | `cockroach-sql-slow.log`      | SQL_PERF channel (see note 1 below) | `cockroach-sql-perf.log`             |
| SQL auth log       | `cockroach-sql-auth.log`      | SESSIONS channel                    | `cockroach-sessions.log`             |

Additionally the following **new channels** are added: OPS, HEALTH,
USER_ADMIN, PRIVILEGES.

This achieves goal A of the [summary](#summary).

A new command-line configuration interface is also defined, see below.

### List of logging channels

Here is a summary of all supported channels:

- **DEV** - Development

  DEV is the channel used during CockroachDB development, to collect
  log details useful for troubleshooting when it is unclear which
  other channel to use. It is also the default logging channel in
  CockroachDB, when the code does not otherwise indicate a channel.

  This channel is special in that there are no constraints as to
  what may or may not be logged on it. Conversely, users in
  production deployments are invited to not collect DEV logs in
  centralized logging facilities, because they likely contain
  sensitive operational data.

  DEV log events are also not documented.

- **OPS** - Operations

  OPS is the channel used to report "point" operational events,
  *initiated by user operators or external automation*:

  - operator or system actions on server processes: process starts,
    stops, shutdowns, crashes (if they can be logged),
    including each time: command-line parameters, current version being run.
  - actions that impact the topology of a cluster: node additions,
    removals, decommissions, etc.
  - job-related initiation or termination.
  - cluster setting changes.
  - zone configuration changes.
  - cluster-wide event (previously "event log")

  By making this channel replicate the event log, we achieve goal B of the [summary](#summary).

- **HEALTH** - Node and cluster health

  HEALTH is the channel used to report "background" operational
  events, initiated by CockroachDB or reporting on automatic processes:

  - current resource usage, including critical resource usage.
  - node-node connection events, including connection errors and
    gossip details.
  - range and table leasing events.
  - up-, down-replication; range unavailability.

- **STORAGE** - Low-level storage activity

  STORAGE is the channel used to report low-level storage
  layer events (RocksDB/Pebble). This also includes
  details about encryption-at-rest.

- **SESSIONS** - Client connections and sessions

  SESSIONS is the channel used to report client network activity:

  - connections opened/closed.
  - authentication events: logins, failed attempts.
  - session and query cancellation.

  This is configured by default in "audit" mode, with event
  numbering and synchronous writes.

- **USER_ADMIN** - User and role administration

  USER_ADMIN is the channel used to report changes
  in users and roles, including:

  - users added/dropped.
  - changes to authentication credentials, incl passwords, validity etc.
  - role grants/revocations.
  - role option grants/revocations.

  This is typically configured in "audit" mode, with event
  numbering and synchronous writes.

- **PRIVILEGES** - Data access rules

  PRIVILEGES is the channel used to report data
  authorization changes, including:

  - privilege grants/revocations on database, objects etc.
  - object ownership changes.

  This is typically configured in "audit" mode, with event
  numbering and synchronous writes.

- **SENSITIVE_ACCESS** - Data and system access by privileged clients or operations

  SENSITIVE_ACCESS is the channel used to report
  data access to sensitive data via SQL or APIs:

  - data access audit events (when table audit is enabled).
  - SQL statements executed by users with the ADMIN bit.
  - operations that write to `system` tables.

  This is typically configured in "audit" mode, with event
  numbering and synchronous writes.

- **SQL_EXEC** - SQL execution

  SQL_EXEC is the channel used to report SQL execution on
  behalf of client connections:

  - logical SQL statement executions (if enabled)
  - pgwire events (if enabled)

- **SQL_PERF** - SQL performance

  SQL_PERF is the channel used to report SQL executions
  that are marked to be highlighted as "out of the ordinary"
  to facilitate performance investigations.
  This includes the "SQL slow query log".

  Note 1 from the table above: Arguably, this channel overlaps with
  SQL_EXEC defined above.  However, we keep them separate for
  backward-compatibility with previous versions, where the
  corresponding events were redirected to separate files.

### Command-line configuration

A single command-line flag is defined to apply configuration to logging channel: `--log`

Its command-line syntax is uniform yet flexible:

   `--log=<channel(s)>:<sink>:<parameters...>`

This applies the provided `sink` and `parameters...` to the channel(s) specified before the colon.
There can be zero or more parameters separated by commas.

For example,

   `--log=DEV:file:dir=/tmp,max-file-size=10MB`

says that the DEV channel should write to files, in the directory `/tmp`, with
a max of 10MB per file.

It is possible to apply the same configuration to multiple channels
with a comma-separated list of channel names; or to all channels with
the asterisk `*`. For example:

- `--log=DEV,STORAGE:file:dir=/tmp,max-file-size=10MB`

  Applies the same configuration as above to both DEV and STORAGE.

- `--log=*:file:dir=/tmp,max-file-size=10MB`

  Applies the same configuration as above to all logging channels.

The parameters can be further omitted; only the channel target and
method are mandatory. The simpler parameter to force file output is
thus `--log=*:file`, which applies file output to all channels.

To apply different parameters to different channels, both the following options are possible:

- one `--log` flag per channel, for example

  `--log=DEV:file:dir=/logs/dev  --log=STORAGE:file:dir=/logs/storage`

- multiple channel configurations, separated by spaces; for example:

  `--log="DEV:file:dir=/logs/dev  STORAGE:file:dir=/logs/storage"`

This uniform configuration mechanism achieves goal C of the [summary](#summary).


#### Sinks and parameters

The following sinks are supported:

- `stderr`: write to the CockroachDB process' standard error

- `file`: write to files

- later, more sinks will be added here (e.g. syslog, json-over-http etc)

All sinks uniformly support the following parameters:

  - `filter`: severity at or above which messages are to be included
    on this sink. `INFO` for all messages, `NONE` for none.

  - `redactable`: whether to include redaction markers.

  - `redacted`: whether to pre-redact the event output. Implies `redactable=true`.

  - `disabled`: force-disable the sink (if it was enabled by default);
    equivalent to `filter=NONE`.

  - `audit`: number the events inside the log, to make it harder to rewrite logs to erase traces of access.

The `file` sink also supports the following parameters:

  - `dir`: use the given destination directory. Incompatible with `firststore`.

  - `firststore`: use the `logs` sub-directory in the first disk-based store directory. Incompatible with `dir`.

  - `audit`: perform writes synchronously.

  - `max-file-size`: approximate max size per file before rotating to a new file.

  - `max-group-size`: approximate max combined size of all files
    generated for the channel before old files are automatically
    deleted.

  - `prefix`: file name prefix to append to the executable name.

**Possible extension**

The sinks may later support a `format` parameter which specifies the format of log messages.

This might be added to specify e.g. JSON output, or using a textual
format with a different timestamp layout, or other combinations.

(This particular feature is kept out of scope for the RFC; it is only
mentioned here as an illustration of how the syntax can be extended
later.)


#### Default configuration

The default logging configuration is backward-compatible with previous CockroachDB versions:

- By default, `--log=*:stderr:filter=NONE`

  If `--logtostderr` is provided, proceed as per `--log=*:stderr:filter=INFO` `--log=STORAGE:stderr:filter=WARNING`
  (i.e. log all INFO events or above severity to stderr, except for STORAGE which only logs WARNING or above severity.)

  This special case for STORAGE already existed in previous CockroachDB versions.

  If `--logtostderr=<LEVEL>` is provided, proceed as per `--log=*:stderr:filter=<LEVEL>`

- By default, `--log=*:file:filter=INFO` - log all events to files by default, combined
  with `--log=STORAGE:file:filter=WARNING` - log all STORAGE events to files at
  severity WARNING or above (i.e. do not log STORAGE INFO events to files by default).

  The special case for STORAGE already existed in previous CockroachDB versions.

- By default, `--log=*:file:firststore` - use first store directory as logging output.

  If `--sql-audit-dir=<DIR>` is provided, proceed as per `--log=SENSITIVE_ACCESS:file:dir=<DIR>`

- By default, `--log=*:file:max-file-size=10MB`

  If `--log-file-max-file=NNN` is provided, proceed as per `--log=*:file:max-file-size=NNN`

- By default, `--logs*:file:max-group-size=100MB`

  If `--log-group-max-size=NNN` is provided, proceed as per `--log=*:file:max-group-size=NNN`

- By default, `--log=<CHANNEL>:file:prefix=<channel>`

  i.e. the file name prefix for each channel file output is named after the channel's name.

  For example, files for the OPS channel are named after
  `cockroach-ops.log`. The `prefix` option makes this configurable.

- By default, `--log=*:stderr:redactable=false`.

- By default, `--log=*:file:redactable=false`.

  If `--redactable-logs` is provided, proceed as per `--log=*:file:redactable=true`

## From the perspective of CockroachDB developers

The base logging API remains unchanged: `log.Infof`, `log.Warningf` etc continue to exist.
However, the base API is now documented to send events to the DEV channel.

(This was already true, but is now made explicit in comments.)

Additionally, each channel is also available as its own sub-API:

- `log.Ops.Infof`, `logs.Ops.Warningf`, etc, send events to the OPS channel.
- `log.Health.Infof`, `logs.Health.Warningf()` etc. send events to the HEALTH channel.
- and so on for all the channels: `log.Dev`, `log.Privileges`, `log.SensitiveAccess`, etc are all defined.
- the documentation strings on each of these new APIs now provides a reminder
  of the purpose of the channel and severity level.

All the per-channel APIs are now auto-generated by a script in the
`pkg/util/log` directory. The code is auto-refreshed whenever the
list or *documentation* of channels and severities is modified in
`log.proto`.

## From the perspective of documentation writers and readers

The list of supported channels and severities is now auto-generated in
the source tree at location `docs/generated/logging.md`, ready to be
embeded in the reference documentation.

In a later phase, we intend to automatically extract all logging
events reported on non-DEV channels into a table, with guidance
automatically extracted from source code comments next to the logging
code. This will help automate the documentation of CockroachDb's
logging output.

This achieves goal D in the [summary](#summary).

# Reference-level explanation

A prototype implementation has been produced here: https://github.com/cockroachdb/cockroach/pull/51987

## Detailed design


### Changes to command-line parsing

The handling of logging configuration flags is to be changed
to achieve [the semantics described above](#command-line-configuration).

### Changes to the logging package

- the `SecondaryLogger` object is un-exported -> `secondaryLogger`

- a new `Channel` protobuf enum type is introduced.

- a Go map provides separate secondary loggers for each channel.

- log writes are redirected to the appropriate logger based on the provided channel.

- the [API explained
  above](#from-the-perspective-of-cockroachdb-develoeprs) is
  auto-generated via a script, to provide the severity and channel as
  separate arguments to the (internal, non-exported) logging code.

### Changes to the build rules

- the API auto-generation is invoked via a `go:generate` rule.

- the documented list of channels and severities is generated
  via a `Makefile` rule.

### Changes to the logging calls

Given that the API change retains the existing log functions, all the
existing log call points automatically go to the DEV channel, unless
specified otherwise.

The RFC proposes to immediately perform the following changes:

- convert the existing uses of secondary loggers to redirect events to
  channels STORAGE (Pebble/RocksDB), SESSIONS (SQL auth) SENSITIVE_ACCESS
  (SQL audit), SQL_EXEC (SQL execution) and SQL_PERF (SQL slow
  queries).

- send server start-up and shutdown events, including reports of used
  configuration parameters, to the OPS channel.

- send server Status and Gossip log events to the HEALTH channel.

- send byte monitor growth notification events to the HEALTH channel.

- duplicate event log events to the OPS channel.

- log all SQL statements executed by `admin` users to the
  SENSITIVE_ACCESS channel.

- log all SQL privilege updates to the PRIVILEGES channel.

- log all SQL user/role additions, role option changes and role
  membership changes to the USER_ADMIN channel.

- log all HTTP API calls to the SESSIONS channel.

## Drawbacks

TBD

## Rationale and Alternatives

TBD

## Unresolved questions

- Whether we should keep `log.Info` `log.Warning` etc in the API.

  Pros of keeping them: backward-compat. No habit changes.

  Cons: makes it harder for engineers to think about optiong into other channels than DEV.

  If we were to deprecate the existing API the devs would need to make a call
  about explicitly using the DEV channel or one of the other channels.

- Whether the Pebble and RocksDB logs can be combined to a single STORAGE channel.

  (Currently they are on distinct channels/files.)

  This RFC proposes to merge them but this was further not discussed so far.

- Whether the special case for STORAGE (default config
  `--log=STORAGE:file:filter=WARNING`
  `--log=STORAGE:stderr:filter=WARNING`) should be preserved.

  Pros of keeping it: backward-compatibility, less noise in logs.

  Cons: makes it slightly harder to document and explain.
