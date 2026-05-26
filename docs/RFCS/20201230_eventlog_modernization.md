- Feature Name: Event log modernization
- Status: completed
- Start Date: 2020-12-01
- Authors: knz
- RFC PR: [#58374](https://github.com/cockroachdb/cockroach/pull/58374)
- Cockroach Issue: [#57629](https://github.com/cockroachdb/cockroach/issues/57629)

# Summary

The venerable `system.eventlog` mechanism is evolving in v21.1 as follows:

- the event types and their definition are stabilized and documented.
- their output in logs is becoming more reliable.
- their output in logs becomes parsable.

The reason why this change is performed is that more and more users
are consuming the events externally.

# Motivation

CockroachDB needs to report structured events for "important"
(notable) events on the logical structure of a cluster, including
changes to the SQL logical schema, node activity, privilege changes
etc.

Previously, these events were reported mainly into the
table `system.eventlog`, with a partial copy of the payload into the
debugging external log file (`cockroach.log`).

This mechanism was originally designed to be tightly coupled with the
DB console (previously named “admin UI”), and thus not meant for
external consumption.

It was incomplete and unsatisfactory in many ways:

- the event payloads were not documented.

- the event payloads were not centrally defined, which was preventing
  the generation of automatic documentation.

- the payload types were declared "inline" in the log calls, which
  made it easy for team members to inadvertently change the structure of
  the payload and make them backward-incompatible for users consuming
  this data externally.

- the payload fields were inconsistently named across event types.

- the metadata fields on the payloads were incompletely and
  inconsistently populated:
      the SQL instance ID was missing in some cases.
      the descriptor ID of affected descriptor was missing in some
      cases.

- the same event type was mistakenly used for different events (e.g.
  "rename_database" for both RENAME DATABASE and CONVERT TO SCHEMA)

- the same event type was abusingly over-used for multiple separate
  operations, e.g. a single event would be generated for a multi-table,
  multi-user GRANT or REVOKE operation.

- events could be omitted entirely from external logs, e.g.
  if the system.eventlog table was not available during a node
  restart.

- the copy in the external log was not parseable. Generally, the
  logging package was unaware of the internal structure of events
  and would “flatten” them.

- no provision was available to partially redact events. From the
  logging system's perspective, the entire payload is sensitive.


The modernization project brings solutions to these shortcomings as follows:

- it centralizes the payload definitions and standardizes them into a
  new package `eventspb`, where changes can be consistently linted and reviewed.
- it groups events into categories and associates logging
  channels (new in v21.1, see the log modernization RFC) to each event category.
- it enables automatic generation of documentation for events.
- it ensures that field names are consistent across event payloads.
- it ensures that event metadata is consistently populated.
- it decomposes complex GRANT/REVOKE operations into individual
  events.
- it automates the generation of a reference documentation for all
  event types.
- it ensures that events are sent to external logs unconditionally.

# Guide-level explanation

Starting in v21.1, CockroachDB now produces standardized, documented,
structured and stable *notable events* for “important” changes to a
cluster.

An undocumented, unstable prototype of this feature was previously
available in the shape of the `system.eventlog` table. This prototype
is now maturing so that notable events can be reliably exploited as
part of CockroachDB's external API.

The salient aspects of the new functionality are as follows:

1. Notable events are now produced primarily through the logging subsystem.

2. In particular, the `system.eventlog` table is now subservient /
   secondary and merely retains a *copy* of the events and is now
   opt-out via a new cluster setting `server.eventlog.enable`. Notable
   events remain logged even if the system.eventlog table is disabled
   or unavailable due to a system range dysfunction.

3. Each notable event type is emitted to one of the logging channels
   (new concept in v21.1, defined in a separate RFC.) For example,
   DDL-related events are emitted on the SQL_SCHEMA channel,
   privilege-related events on the PRIVILEGES channel, and so on.
   This makes it possible to redirect the events to different logging
   sinks (e.g. different files) using the standard logging
   configuration system.

4. The list of all notable events, the structured payload and fields
   that are emitted for each, as well as their destination logging
   channel, are now documented as part of the stable API of
   CockroachDB. A copy of this documentation can be found in the
   source code repository in `docs/generated`.

5. Since the events are now part of a stabilized API, the CockroachDB
   team commits to cross-version stability of event payloads.  Changes
   will be reported in release notes; new fields may be added in new
   versions but existing fields will go through the regular
   deprecation cycle. This makes it possible to more reliably build
   3rd-party tooling on top of notable events.

# Reference-level explanation

## Detailed design

The meta-issue (epic) that tracks the changes is [this
one](https://github.com/cockroachdb/cockroach/pull/58374). The main
implementation steps include:

- [sql,log: productionize the event logging #57737](https://github.com/cockroachdb/cockroach/pull/57737)
- [sql: make writes to `system.eventlog` conditional #57879](https://github.com/cockroachdb/cockroach/pull/57879)
- [util/log: report the multi-tenant identifiers in log files, simplify event logging #57890](https://github.com/cockroachdb/cockroach/pull/57890)
- [*: new logging channels OPS and HEALTH #57171](https://github.com/cockroachdb/cockroach/pull/57171)
- [util/log: new logging channels SQL_SCHEMA, USER_ADMIN and PRIVILEGES #51987](https://github.com/cockroachdb/cockroach/pull/51987)
- [eventpb: new JSON serialization with redaction markers #57990](https://github.com/cockroachdb/cockroach/pull/57990)
- [sql,util/log: include the application name in SQL structured events #58130](https://github.com/cockroachdb/cockroach/pull/58130)
- [util/log: new JSON output formats #58126](https://github.com/cockroachdb/cockroach/pull/58126)
- [util/log: report the server identifiers in JSON payloads #58128](https://github.com/cockroachdb/cockroach/pull/58128)
- [pgwire: migrate auth/conn logs to notable events #57839](https://github.com/cockroachdb/cockroach/pull/57839)

### Event types

The event payloads were defined as inline anonymous `struct` definitions in many
files through the `server` and `sql` packages.

They are now defined as *named* protobuf definitions in the new `eventspb` package.

Each event type implements the following interface:

```go
// EventPayload is implemented by CommonEventDetails.
type EventPayload interface {
	// LoggingChannel indicates which logging channel to send this event to.
	// This is defined by the event category, at the top of each .proto file.
	LoggingChannel() logpb.Channel

	// AppendJSONFields appends the JSON representation of the event's
	// fields to the given redactable byte slice. Note that the
	// representation is missing the outside '{' and '}'
	// delimiters. This is intended so that the outside printer can
	// decide how to embed the event in a larger payload.
	//
	// The printComma, if true, indicates whether to print a comma
	// before the first field. The returned bool value indicates whether
	// to print a comma when appending more fields afterwards.
	AppendJSONFields(printComma bool, b redact.RedactableBytes) (bool, redact.RedactableBytes)
}
```

When two or more events are intended to share fields, these fields are
now defined using a common embedded struct. For example:

```protobuf
// CommonEventDetails contains the fields common to all events.
message CommonEventDetails {
  // The timestamp of the event. Expressed as nanoseconds since
  // the Unix epoch.
  int64 timestamp = 1;
  // The type of the event.
  string event_type = 2;
}

// CommonSQLEventDetails contains the fields common to all
// SQL events.
message CommonSQLEventDetails {
  // A normalized copy of the SQL statement that triggered the event.
  string statement = 1;

  // The user account that triggered the event.
  string user = 2;

  // The primary object descriptor affected by the operation. Set to zero for operations
  // that don't affect descriptors.
  uint32 descriptor_id = 3;

  // The application name for the session where the event was emitted.
  // This is included in the event to ease filtering of logging output
  // by application.
  string application_name = 4;
}
```

### Internal event logging API

A new API `log.StructuredEvent(context.Context,
eventspb.EventPayload)` sends a structured event to the channel
reported by its `LoggingChannel()` method.

The repetitive, error-prone event log reporting logic spread around
the `sql` package is now centralized into the following functions (in
`sql/event_log.go`):

```go
// logEvent emits a cluster event in the context of a regular SQL
// statement.
func (p *planner) logEvent(
	ctx context.Context, descID descpb.ID, event eventpb.EventPayload,
) error

// logEventInternalForSchemaChange emits a cluster event in the
// context of a schema changer.
func logEventInternalForSchemaChanges(
	ctx context.Context,
	execCfg *ExecutorConfig,
	txn *kv.Txn,
	sqlInstanceID base.SQLInstanceID,
	descID descpb.ID,
	mutationID descpb.MutationID,
	event eventpb.EventPayload,
) error

// logEventInternalForSQLStatements emits a cluster event on behalf of
// a SQL statement, when the point where the event is emitted does not
// have access to a (*planner) and the current statement metadata.
//
// Note: usage of this interface should be minimized.
func logEventInternalForSQLStatements(
	ctx context.Context,
	execCfg *ExecutorConfig,
	txn *kv.Txn,
	descID descpb.ID,
	user security.SQLUsername,
	appName string,
	stmt string,
	event eventpb.EventPayload,
) error
```

Example change/use enabled by these new APIs:

```diff
@@ -157,18 +158,11 @@ func doCreateSequence(

        // Log Create Sequence event. This is an auditable log event and is
        // recorded in the same transaction as the table descriptor update.
-       return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
-               params.ctx,
-               params.p.txn,
-               EventLogCreateSequence,
-               int32(desc.ID),
-               int32(params.extendedEvalCtx.NodeID.SQLInstanceID()),
-               struct {
-                       SequenceName string
-                       Statement    string
-                       User         string
-               }{name.FQString(), context, params.p.User().Normalized()},
-       )
+       return params.p.logEvent(params.ctx,
+               desc.ID,
+               &eventpb.CreateSequence{
+                       SequenceName: name.FQString(),
+               })
 }
```


The previous `EventLogger` struct is deleted; its method
`InsertEventRecord` is promoted to a new exported function, but direct
use of it is restricted to the few points in the `server` package
which are not performing SQL operations.

### Code and Doc generation

The mapping of event types to event channels, i.e. the implementation
of the `LoggingChannels()` methods, is now auto-generated from a
comment at the start of the `.proto` file.

The inventory of all event types and the documentation for their payload
is now auto-documented  from comments in the `.proto` files.

### Logging changes

The payload of events sent via `log.StructuredEvent` are now packaged
in the logging output using valid JSON syntax, instead of being
rendered into a non-parsable format via `%+v` as previously.

Specifically:

- in the `crdb-v1-*` logging formats, payloads are emitted as a
  single-line string starting with the prefix `Structured entry:`,
  followed by a valid JSON payload.

- in the `json-*` logging formats, payloads are emitted under a new
  `event` field of the overall logging entry. Their JSON structure is
  preserved.

## Drawbacks

None identified.

## Rationale and Alternatives

None identified.

## Unresolved questions

None identified.
