- Feature Name: Logging modernization
- Status: completed
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
three ways:

- by organizing logging events throughout CockroachDB in broad
  categories called “channels”, so that events that belong to
  different channels can be redirected to different outputs.

- by introducing network logging, using a
  [Fluentd](https://www.fluentd.org) integration.

- by streamlining, simplifying and refactoring the logging code and
  its configuration options, so that the code becomes more readily
  maintainable and auditable in the future.

This work is motivated by a combination of factors:

- we have multiple customer commitment to separate various operational
  and security-related logging events on their own logging channels,
  away from the "main log", so they can be collected separately. The
  customers in particular are interested to reduce the noise of the
  data subject to their 3rd party monitoring systems.

- we have several security objectives (a combination of user requests
  and compliance requirements) which demand separate logging for
  security-related events: user and role management, granting
  privileges, etc.

- the current logging system has been built by organic addition
  of bits and pieces each serving a particular purpose. We want
  to reduce this complexity to a simple and uniform system before
  we add any more complexity to satisfy the goals above.

Table of contents:

- [Motivation](#motivation)
  - [CockroachCloud short-term requirements](#cockroachcloud-short-term-requirements)
  - [Log collection for health monitoring](#log-collection-for-health-monitoring)
  - [Log collection for security](#log-collection-for-security)
  - [Network and structured logging](#network-and-structured-logging)
  - [Logging configuration](#logging-configuration)
- [Guide-level explanation](#guide-level-explanation)
  - [From the perspective of DBAs and operators](#from-the-perspective-of-dbas-and-operators)
    - [What is the same](#what-is-the-same)
    - [What is different](#what-is-different)
    - [List of logging channels](#list-of-logging-channels)
    - [Configuration - general concepts](#configuration---general-concepts)
    - [Configuration - sink parameters](#configuration---sink-parameters)
    - [Configuration - command-line and YAML format](#configuration---command-line-and-yaml-format)
    - [Configuration - checking and visualization](#configuration---checking-and-visualization)
    - [Suggested best practices](#suggested-best-practices)
	  - [General best practics](#general-best-practices)
	  - [CockroachCloud logging use cases](#cockroachcloud-logging-use-cases)
  - [From the perspective of CockroachDB developers](#from-the-perspective-of-cockroachdb-developers)
  - [From the perspective of documentation writers and readers](#from-the-perspective-of-documentation-writers-and-readers)
- [Reference-level implementation](#reference-level-implementation)
  - [Detailed design](#detailed-design)
    - [YAML configuration format](#yaml-configuration-format)
    - [Default configuration](#default-configuration)
    - [Changes to command-line parsing](#changes-to-command-line-parsing)
    - [Changes to the logging package](#changes-to-the-logging-package)
    - [Changes to the build rules](#changes-to-the-build-rules)
    - [Changes to the logging calls](#changes-to-the-logging-calls)
  - [Drawbacks](#drawbacks)
  - [Rationales and Alternatives](#rationales-and-alternatives)
  - [Unresolved questions](#unresolved-questions)

# Motivation

## CockroachCloud short-term requirements

To mitigate the risk of large-scale attacks on the CC infrastructure
(DoS), the CC team deems it urgent to integrate an Intrusion Detection
Systems (IDS) that will monitor security-adjacent events in CC
clusters to detect anomalies.

Today, it is not possible to extract all the security-adjacent events
(certain events are simply missing from logs), nor is it possible to
collect *only* security-adjacent events (too much noise).

The next two sub-sections explain these two concerns in more
detail. Even though the text refers to users “in general”, the reader
is invited to consider that CC is a primary consumer for the proposed
enhancements.

## Log collection for health monitoring

Automated health monitoring is usually performed by scanning logs for
abnormal events. This is what is done e.g. automatically in Datadog.
Some of our customers also export their logs into ElasticSearch and run
their own monitoring tools and heuristics on top.

In this use case, CockroachDB's logs currently contain too much noise:
debugging and tracing messages are logged, including some messages
that only occur on edge cases without impact on overall cluster
health. These messages appear as “irregular behavior” from the
perspective of monitoring tools, even though they are innocuous and do
not require action in the common case.

Users have requested in the past that we “identify which log messages
are important” or enable automation of “extraction of logging events
that are important to understand cluster health”. These requests stem
from the current difficulty to sort the ‘chaff’ from the ‘wheat’ when
viewing logs.

## Log collection for security

Automated IDS  and automatic advance
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

## Network and structured logging

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

There are many log collection services available; each with its own
protocol, authentication handshake, push or pull model, etc. To ensure
the solution satisfies a broad range of deployments, it is thus useful
to select a common standard in the industry. As of this writing,
(Fluentd)[https://www.fluentd.org] matches this requirement as it
provides integrations with most common technology stacks.

## Logging configuration

CockroachDB currently has a wealth of (arguably under-documented)
logging parameters:

- It supports logging to files and/or to the `cockroach` process'
  standard error stream. We also aim to introduce network sinks. Which
  outputs are enabled is already and ought to remain individually configurable.

- Additionally, it also supports filtering logging event by *severity*
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

Each of these configuration options has been implemented using ad-hoc
mechanisms and their own command-line flags, grown organically on top
of each other. Their current interactions are extremely hard to
understand from reading the code, and maintainance of the logging
system has become ossified over time.

Before new solutions can be added to answer the needs expressed above,
we thus wish to reduce the complexity of the configuration mechanisms
by making the configuration more uniform and easier to explain.

# Guide-level explanation

## From the perspective of DBAs and operators

Each logging event is emitted on a *channel*, which categories the
“type” or “purpose” of the event.

Logging channels are analogous to [logging *facilities* in
Syslog](https://en.wikipedia.org/wiki/Syslog), or [logging *services*
in
Datadog](https://docs.datadoghq.com/logs/log_collection/?tab=http#reserved-attributes).

The destination of logging messages (where they are written to) can be configured
separately for each channel:

- whether to use disk, stderr stream or network;
- which format to use for log entries;
- whether the writes are synchronous (useful for audit logs);
- whether the events are numbered (useful for audit logs);
- what parameters to use for the writes, for example which directory
  to use for files;
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

By default, CockroachDB v20.2 writes events of its channels to
different log files in the `logs` sub-directory of the first
disk-based store directory. This default is unchanged in v21.1.

This default configuration includes:

| File                                   | Events collected                                                      | Channel in new model |
|----------------------------------------|-----------------------------------------------------------------------|----------------------|
| `cockroach.log`                        | Uncategorized and debug messages                                      | `DEV`                |
| `cockroach-pebble.log`                 | Low-level storage logs                                                | `STORAGE`            |
| `cockroach-sql-audit.log`              | Output from the experimental/alpha "SQL audit logging" feature        | `SENSITIVE_ACCESS`   |
| `cockroach-sql-exec.log`               | SQL statements when enabled via cluster setting                       | `SQL_EXEC`           |
| `cockroach-auth.log`                   | SQL authentication and session logs, when enabled via cluster setting | `SESSIONS`           |
| `cockroach-sql-slow.log`               | Slow query log when enabled via cluster setting                       | `SQL_PERF`           |
| `cockroach-sql-slow-internal-only.log` | Slow query log for 'internal' queries                                 | `SQL_INTERNAL_PERF`  |

(This particular mapping of channels to files is preserved in v21.1
but is also deprecated as a default configuration. In v21.2 and
forward, a user would need to make this configuration explicit if they
wish to preserve it.)

Additionally, a file `cockroach-stderr.log` also exists. This
mechanism is called the “stray error capture”, or technically inside
the source code “capture of fd2 internal writes”. This is an oddity
that emerges from our use of the Go language: the Go runtime and
certain CockroachDB software dependencies bypass the CockroachDB
logging system entirely, and so cannot be subject to its
configuration. For example, uncaught software exceptions (panics) are
written in this way. To prevent this output from clobbering the
`cockroach` stderr output, or from becoming lost entirely when stderr
is not monitored, it is redirected to its own log file. This behavior
becomes configurable in v21.1.

Finally, the following flags continue to be recognized for
backward-compatibility, but are now hidden from the `--help` text and
report a deprecation notice:

- The `--logtostderr` overrides the stderr event filter for all
  the channels simultaneously.

- The `--log-dir` flag overrides the logging directory for all the
  channels simultaneously.

- The `--log-file-verbosity` flag defines a severity filter for events
  written to files for all the channels simultaneously.

- The `--log-file-max-size` and `--log-group-max-size` parameters set
  the maximum size/number parameters for files written by all the
  channels.

These flags will be removed in v21.2.

### What is different

There were already event channels previously in CockroachDB, albeit
not documented: “general” (debug) logs, the RocksDB/Pebble log, SQL
audit logging, SQL statement logs and the SQL slow query log.

These channels are preserved but they are now renamed:

| Previous name                           | New name            |
|-----------------------------------------|---------------------|
| “general” log                           | `DEV`               |
| RocksDB log                             | `STORAGE`           |
| Pebble log                              | `STORAGE`           |
| SQL audit log                           | `SENSITIVE_ACCESS`  |
| SQL exec log                            | `SQL_EXEC`          |
| SQL slow query log                      | `SQL_PERF`          |
| SQL slow query log for internal queries | `SQL_INTERNAL_PERF` |
| SQL auth log                            | `SESSIONS`          |

Additionally the following **new channels** are added: OPS, HEALTH,
SQL_SCHEMA, USER_ADMIN, PRIVILEGES.

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
  - restores and imports (see [this issue](https://github.com/cockroachdb/cockroach/issues/57575))
  - cluster setting changes.
  - zone configuration changes.

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

- **SQL_SCHEMA** - Logical schema changes

  The SQL_SCHEMA channel is used to report changes to the
  SQL logical schema, excluding privilege and ownership changes
  (which are reported on the separate channel PRIVILEGES) and
  zone config changes (which go to OPS).

  This includes:

  - database/schema/table/sequence/view/type creation
  - adding/removing/changing table columns
  - changing sequence parameters

  etc., more generally changes to the schema that affect the
  functional behavior of client apps using stored objects.


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

- **SQL_INTERNAL_PERF** - same as SQL_PERF but for "internal" queries.


### Configuration - general concepts

To effectively make use of the new logging options, it is useful to
properly understand the main components of the processing of log events:

- a logging event emanates from a component inside CockroachDB with
  a *severity*, and *payload*.

- it enters the logging system via its *channel*.

- eventually it exits the logging system (into files, network etc) via
  zero or more *sinks*.

- the *mapping of channels to sinks* is configurable. For example, the
  operator can say "All health messages go to a file named
  `health.log`, and all security messages go to a file named
  `security.log` and also a network logger.".

- there are sink-specific options that tailor how the log events are
  written to each sink. For example, the text format and redaction
  options can be configured per sink.

### Configuration - sink parameters

Every sink has at least the following parameters:

- `channels`: the list of the channels that are redirected to this
  sink. This makes it possible to e.g. redirect multiple channels to
  the same file, the same network collector, etc.

  The channel selection is case-insensitive; possible values:

  -  `ALL` for all channels;
  -  a comma-delimited list of specific channels;
  -  `ALL EXCEPT <list>` for all channels except the provided
     channel names.

- `filter`: the minimum severity of entries to send to this sink. This
  makes it possible to e.g. only collect severe errors to a particular
  file.

- `format`: how to format the log entries before they are written to
  the sink. For example, `crdb-v1` represents CockroachDB's legacy
  logging format, `json-fluent-compact` represents a
  Fluentd-compatible JSON format.

- `redact`: whether to perform redaction of sensitive details
  upfront. This makes it possible to e.g. ensure that sensitive data
  never escapes the nodes when connecting to a network log collector.

- `redactable`: whether to keep redaction markers in the sink
  output. This option is provided for compatibility when integrating
  with log processors which may be confused by the redaction markers.

- `exit-on-error`: whether to stop the CockroachDB node if an error is
  encountered while writing to the sink. This option should be defined
  for at least one sink, as otherwise a risk would exist to entirely
  lose certain log entries.

- `auditable`: whether to make the sink more robust to
  repudiation. The precise effects of this flag depends on the
  sink. For example, it implies `exit-on-error` on all sinks, and also
  disables buffering for `file` sinks.

The following sinks are supported:

- `stderr`: the standard error stream for the `cockroach`
  process. Sink-specific options:

  - `no-color` removes terminal color escape codes from the output.

- `file-group`: write the log events to files. Sink-specific options:

  - `dir`: logging directory where output files are created.
  - `max-file-size`: approximate max size per file before rotating to
    a new file. Can be set to zero for no maximum.
  - `max-group-size`: approximate max combined size of all files
    generated for the channel before old files are automatically
    deleted. Can be set to zero to disable removal of old files.
  - `sync-writes`: disable buffering and synchronzie writes. This flag
    is useful to define audit logs but can incur a performance overhead
	and higher disk IOPS consumption.

- `fluent-server`: write log events through a network connection to a
  Fluentd-compatible server. Sink-specific options.

  - `address`: the network address and port of the Fluentd collector.
  - `net`: the network protocol to use, defaults `tcp` (possible
    values `tcp` `tcp4` `tcp6` `udp` `udp4` `udp6` `unix`).

Naturally, for convenience, nearly all these parameters have
customizable defaults. This makes it possible to customize the
configuration surgically, without having to spell out or repeat
parameters across sinks.

### Configuration - command-line and YAML format

The configuration is passed via a new command-line parameter
`--log=<yaml>`. Its value is a
[YAML](https://en.wikipedia.org/wiki/YAML) payload. YAML was chosen as
it is the native configuration language for K8s services.

The YAML payload has the following general structure:

```
file-defaults: ...       # defaults inherited by file sinks
fluent-defaults: ...     # defaults inherited by fluent sinks
sinks:
   stderr: ...           # parameters for the stderr sink
   file-groups: ...      # file sink definitions
   fluent-servers: ...   # fluent server definitions
stray-error-capture: ... # parameters for the stray error capture system
```

(The full configuration format is described in [the reference-level
section](#yaml-configuration-format) below, as well as the [default
configuration](#default-configuration).)

As usual with YAML-compatible systems, the YAML configuration can be
passed either in “block format”, with different parameters on different
lines, or in “flow” format which resembles JSON. For example,
`--log=sinks: {stderr: { ... }}` can customize the `stderr` sink
parameters on just 1 line.

### Configuration - checking and visualization

To help verify configuration parameters, and also to assist learning
and teaching the logging options, a new command is introduced:
`cockroach debug check-log-config`.

This accepts the same command-line flags as `cockroach start` but
merely validates the logging options and *prints out the resulting
configuration*.

It also prints out a URL to a diagram that represents the topology of
the configuration visually. For example, the [default
configuration](#default-configuration) is visualized as follows:

![default config](www.plantuml.com/plantuml/png/FCkn3e8m60JWdQVeg6zXKqve0WNxL-9635Ob0MfAcNnvKnId--wIEypXwiCo3ibWxu45ntp_U0PswxtjGk-cf7FZoqrs2cnsY-_ipDx9RizID36gccBau8N9eyXKccTO2I9PALfzAbeIEKeUsvcWMvwMKiLMQzd80Xde-r8aZMpGSY50_pSk49jhwEDcgGoquh_nqqSPx20A0s7uFa_HSAy8ACGzRFP1DZ6bBKeihRPbTptuxajRImD1Bxyp_Ry10_n3XHePCbQJ9o86PRxwBRdMIfiCNl0O-SWdW4r7G8bcw7u-5vxjb1w8_K4rQ_6V8flTyhDhdf8UlEsgD59jZ62gHcdJ86qs1WyV1nDp_Fflwwzki-aIRVkEK8kwNlWNZ3eb6DSeoncn9B-gWUStBabkDCqlAlpqzZgsEwg6d2fwquhGAsjJdIhiBoBKnMJkslukg0iFVOPsav_DPhaqyPsthU1BfbenvY4m6LblbrYzYTdL3KyR5F_7wP8gzXkO4Uhl2Yi-OBYNbcj915PIF1T3uPDqvMes66_s1I-OeeSm_ka_N9W2c9eGmzIddAMpbCHqbXAOpfpuhYlW7Ufebk0ac2Mi0nkf2fouBiG4l8Qf2k9oYFG1dCnq0ru3NKDS094-WfFWdY2kXEZMu2IunHvMMo9n3JXvSmM4hU6wz2xW5vTi7CH_)

### Suggested best practices

#### General best practices

- The default for the `dir` parameter for file sinks is the `logs`
  subdirectory of the first on-disk store. **This is undesirable in
  Cloud deployments** where the main data store is subject to IOPS
  budgets: adding logging activity to the data store directory,
  especially audit logs, is going to excessively consume the IOPS
  budget.

  For cloud deployments it is better to redirect logging to a separate
  directory with less/different IOPS restriction:
  `--log=file-defaults: { dir: /your/own/log/dir/here }`.

- Some deployments **automate the scraping and collection of logs to
  an external service**.

  Because the DEV channel is noisy and may contain sensitive details,
  it is advised to separate the sinks for the DEV channel from that of
  the other channels, and only collect data over the network for the
  non-DEV channels.

  For example: `--log=file-defaults: {dir: /custom}, sinks:
  {file-groups: {default: {dir: cockroach-data/logs}}}`

  In this example, the `/custom` directory is applied first to all
  file sinks, then an exception is defined for the DEV channel in the
  `default` sink.

- **Stripping sensitive information ahead of scraping logs**. When
  log collection is done for data mining purposes, and non-admin (or
  generally non-privileged users) have access to collected logs, it is
  important to ensure that sensitive information is removed from
  collected logs.

  For this the best practice is as follows:

  - do **not** centrally collect logs from the DEV channel.
  - enable the `redact` parameter on log channels that are centrally collected.
  - avoid exposing data from the USER_ADMIN and SENSITIVE_ACCESS
    channels, even when `redact` is enabled: the patterns of
    behavior may themselves be sensitive. It is best to restric data
    mining to e.g. the `SQL_PERF`, `OPS` and `HEALTH` channels.

- To make `cockroach` more robust to its standard error stream getting
  closed, which can happen e.g. when using the `--background` flag
  from an interactive terminal and then closing the terminal, it is
  now possible to mark the stderr stream as non-critical.

  This can be done with `--log=sinks: {stderr: {exit-on-error:
  false}}`

#### CockroachCloud logging use cases

For CockroachCloud we really have three fundamental use cases:

1. we (CRL support) need logging output when troubleshooting cluster
   misbehavior or outages.

2. we need security logs to detect malicious access attempts and other
   suspicious security-adjacent behavior, by means of an IDS with
   automatic behavior modeling. These security logs need to monitor at
   least user/role administration, SQL privilege changes and
   authentication attempts.

3. CC end-users want the ability to access "their own logs" to
   troubleshoot SQL query performance, audit their DDL, etc.

To achieve all 3 goals at acceptable cost, we propose the following architecture:

- CockroachDB nodes inside CC clusters are configured to route _all_
  their logging to a Fluentd-compatible collector. The collector needs
  to run locally in each region (preferably inside the same k8s
  cluster) to ensure that the latency of network logging remains low,
  and to prevent potential confidentiality/tampering attacks on the
  logging stream in-transit.

- the cluster setting `server.auth_log.sql_sessions.enabled` must be
  set explicitly on every CC cluster, to force the emission of authn
  events to the log.

- At the network collector, we'd then configure the logging to be
  *routed* as follows:

  - a copy of the SESSIONS, USER_ADMIN, PRIVILEGES to be sent to the
    IDS as a stream.

  - a copy of SESSIONS, USER_ADMIN, PRIVILEGES, SQL_SCHEMA,
    SENSITIVE_ACCESS, SQL_EXEC, SQL_PERF, SQL_INTERNAL_PERF to be sent
    to storage buckets on which the CC end-user has read-only
    permission, so they can "download their own logs". Each channel
    stored to a different file.

    (Note that we need to care that CC-sensitive data does not
	get copied to user logs, see [this issue](https://github.com/cockroachdb/cockroach/issues/57902))

  - a copy of the OPS and HEALTH channel to be sent to the CC
    monitoring platform to detect correlated cluster outages.

- Additionally, for the time being we will also preserve the ability
  of every CC node to copy their logging events to a file stored
  locally. This is needed for backward-compatibility with `cockroach
  debug zip` until this issue is addressed:
  https://github.com/cockroachdb/cockroach/issues/57710

  However, we need to be careful that the SESSIONS channel is only
  logged to the network and not to disk, because it produces a lot of
  events and this would consume disk IOPS excessively.

## From the perspective of CockroachDB developers

The base logging API remains unchanged: `log.Infof`, `log.Warningf`
etc continue to exist. However, the base API is now documented to
send events to the DEV channel.

(This was already true, but is now made explicit in comments.)

Additionally, each channel is also available as its own sub-API:

- `log.Ops.Infof`, `logs.Ops.Warningf`, etc, send events to the OPS
  channel.
- `log.Health.Infof`, `logs.Health.Warningf()` etc. send events to the
  HEALTH channel.
- and so on for all the channels: `log.Dev`, `log.Privileges`,
  `log.SensitiveAccess`, etc are all defined.
- the documentation strings on each of these new APIs now provides a reminder
  of the purpose of the channel and severity level.

All the per-channel APIs are now auto-generated by a script in the
`pkg/util/log` directory. The code is auto-refreshed whenever the
list or *documentation* of channels and severities is modified in
`logpb/log.proto`.

## From the perspective of documentation writers and readers

The list of supported channels and severities is now auto-generated in
the source tree at location `docs/generated/logging.md`, ready to be
embeded in the reference documentation.

It is also advised to use `cockroach debug check-log-config` with the
`--only-channels` parameter to simplify diagrams for inclusion in
documentation.

In a later phase, we intend to automatically extract all logging
events reported on non-DEV channels into a table, with guidance
automatically extracted from source code comments next to the logging
code. This will help automate the documentation of CockroachDb's
logging output.

# Reference-level explanation

The prototype implementation has been produced as follows:

- Major refactors:
  - [util/log: various simplifications towards logging channels #56336](https://github.com/cockroachdb/cockroach/pull//56336)
  - [util/log: misc fixes #56897](https://github.com/cockroachdb/cockroach/pull/56897)
  - [util/log: more misc cleanups #57000](https://github.com/cockroachdb/cockroach/pull/57000)
- New configuration system:
  - [util/log,cli: channel abstraction; new configuration system #57134](https://github.com/cockroachdb/cockroach/pull/57134)
  - [util/log: new experimental integration with Fluentd #57170](https://github.com/cockroachdb/cockroach/pull/57170)
- Usage of logging channels:
  - [*: new logging channels OPS and HEALTH #57171](https://github.com/cockroachdb/cockroach/pull/57171)
  - [util/log: new logging channels USER_ADMIN and PRIVILEGES #51987](https://github.com/cockroachdb/cockroach/pull/51987)

## Detailed design


### YAML configuration format

This is to be documented in the new `logconfig` package:

```yaml
file-defaults: # optional
  dir: <path>           # output directory, defaults to first store dir
  max-file-size: <sz>   # max log file size, default 10MB
  max-group-size: <sz>  # max log file group size, default 100MB
  sync-writes: <bool>   # whether to sync each write, default false
  <common sink parameters>

fluent-defaults: # optional
  <common sink parameters>

sinks: # optional
 stderr: # optional
  channels: <chans>        # channel selection for stderr output, default ALL
  no-color: <bool>         # disable colors for format crdb-v1-tty
  <common sink parameters> # if not specified, inherit from file-defaults

 file-groups: #optional
   <filename>:
     channels: <chans>        # channel selection for this file output, mandatory
     max-file-size: <sz>      # defaults to file-defaults.max-file-size
     max-group-size: <sz>     # defaults to file-defaults.max-group-size
     sync-writes: <bool>      # defaults to file-defaults.sync-writes
     <common sink parameters> # if not specified, inherit from file-defaults

   ... repeat ...

 fluent-servers: #optional
   <servername>:
     channels: <chans>        # channel selection for this file output, mandatory
     address: <addr:port>     # network address, mandatory
     net: <protocol>          # network protocol, default tcp
     <common sink parameters> # if not specified, inherit from fluent-defaults

   ... repeat ...

capture-stray-errors: # optional
  enable: <bool>       # whether to enable internal fd2 capture
  dir: <optional>      # output directory, defaults to file-defaults.dir
  max-group-size: <sz> # defaults to file-defaults.max-group-size

<common sink parameters>
  filter: <severity>    # min severity level for file output, default INFO
  redact: <bool>        # whether to remove sensitive info, default false
  redactable: <bool>    # whether to strip redaction markers, default false
  format: <fmt>         # format to use for log enries, default
                        # crdb-v1 for files, crdb-v1-tty for stderr
                        # and json-fluent-compact for fluent sinks
  exit-on-error: <bool> # whether to terminate upon a write error
                        # default true for file+stderr sinks, false for
                        # fluent sinks
  auditable: <bool>     # if true, activates sink-specific features
                        # that enhance non-repudiability.
                        # also implies exit-on-error: true.
```

### Default configuration

Here is the v20.2-compatible configuration, which is set by default
unless `--log` is used to override it:

```yaml
file-defaults:
  # dir is not specified - inferred from the first on-disk --store
  filter: INFO
  redact: false
  redactable: true
  format: crdb-v1
  exit-on-error: true
  max-file-size: 10MiB
  max-group-size: 100MiB
  sync-writes: false
fluent-defaults:
  filter: INFO
  exit-on-error: false
  format: json-fluent-compact
sinks:
  stderr:
    filter: NONE   # this disables stderr by default
    channels: ALL
    redact: false
    redactable: true
    format: crdb-v1-tty
    exit-on-error: true
    no-color: false
  file-groups:
    default:       # cockroach.log
      channels: ALL EXCEPT STORAGE,SQL_EXEC,SENSITIVE_ACCESS,SESSIONS,SQL_PERF,SQL_INTERNAL_PERF
    pebble:
      channels: STORAGE
    sql-exec:
      channels: SQL_EXEC
    sql-audit:
      channels: SENSITIVE_ACCESS
      auditable: true
    sql-auth:
      channels: SESSIONS
      auditable: true
    sql-perf:
      channels: SQL_PERF
    sql-perf-internal-only:
      channeks: SQL_INTERNAL_PERF
stray-error-capture:
  # dir inherited from file-defaults
  # max-group-size inherited from file-defaults
  enable: true
```

Note: these `file-groups` defaults listed here are deprecated in v21.1
and will be removed in v21.2.

### Changes to command-line parsing

The handling of logging configuration flags is to be changed
to achieve the semantics described above.


### Changes to the logging package

- the `SecondaryLogger` object is removed.

- a new `Channel` protobuf enum type is introduced.

- a Go map provides separate a `*loggerT` for each channel.

- log writes are redirected to the appropriate logger based on the provided channel.

- each `*loggerT` is associated with zero or more `logSinks`. Each
  logging event is writen to all sinks associated with its channel
  logger.

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

- duplicate certain `system.eventlog` events to the OPS channel,
  others to `USER_ADMIN`.

- log all SQL statements executed by `admin` users to the
  SENSITIVE_ACCESS channel.

- log all SQL privilege updates to the PRIVILEGES channel.

- log all SQL user/role additions, role option changes and role
  membership changes to the USER_ADMIN channel.

- log all HTTP API calls to the SESSIONS channel.

## Drawbacks

None found.

## Rationale and Alternatives

None found.

## Unresolved questions

- Whether we should keep `log.Info` `log.Warning` etc in the API.

  Pros of keeping them: backward-compat. No habit changes.

  Cons: makes it harder for engineers to think about optiong into other channels than DEV.

  If we were to deprecate the existing API the devs would need to make a call
  about explicitly using the DEV channel or one of the other channels.

  Resolution: these are kept for now.
