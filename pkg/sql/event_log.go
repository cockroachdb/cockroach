// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

// The logging functions in this file are the different stages of a
// pipeline that add more and more information to logging events until
// they are ready to be sent to either external sinks or to a system
// table.
//
// The overall structure of this pipeline is as follows:
//
//  regular statement execution
//  for "special" statements that
//  have structured logging, e.g. CREATE, DROP etc
//    |
//  (produces pair(s) of descID (optional) and logpb.EventPayload)
//    |
//    v
//  logEvent(descID, payload) / logEvents(eventEntries...)
//    |
//    |           ,------- query logging in exec_log.go
//    |          /         optionally via logEventOnlyExternally()
//    |         /
//    v        v
// logEventsWithOptions()
//    |
//  (extracts SQL exec details
//   from execution context - see sqlEventCommonExecPayload)
//    |
//    |                ,----------- async CREATE STATS
//    |               /             goroutine
//    |              /              on behalf of CREATE STATS stmt
//    v             v
// logEventInternalForSQLStatements()
//    |          (SQL exec details struct
//    |           and main event struct provided
//    |           separately as arguments)
//    |
//  (writes the exec details
//   inside the event struct)
//    |
//    |                      ,----- job execution, at end
//    |                      |
//    |                LogEventForJobs()
//    |                      |
//    |                (add job ID,
//    |                 + fields from job metadata
//    |                 timestamp initialized at job txn read ts)
//    |                      |
//    |    ,-----------------'
//    |   /
//    |  /                   ,------- async schema change
//    |  |                   |        execution, at end
//    |  |                   v
//    |  |            logEventInternalForSchemaChanges()
//    |  |                   |
//    |  |             (add mutation ID,
//    |  |              + fields from sc.change metadata
//    |  |              timestamp initialized to txn read ts)
//    |  |                   |
//    |  |     ,-------------'
//    |  |    /
//    |  |   /
//    |  |   |
//    |  |   |      ,-------- node-level events outside of SQL
//    |  |   |     /          (e.g. cluster membership)
//    |  |   |    /
//    v  v   v   v
//  (expectation: per-type event structs
//   fully populated at this point.
//   Timestamp field must be set too.)
//     |
//     v
// InsertEventRecords() / insertEventRecords()
//     |
//  (finalize field EventType from struct type)
//     |
//   (route)
//     |
//     +--> system.eventlog if not disabled by setting
//     |                   └ also the Obs Service, if connected
//     |
//     +--> DEV channel if requested by log.V
//     |
//     `--> external sinks (via logging package)
//
//

// logEvent emits a cluster event in the context of a regular SQL
// statement.
func (p *planner) logEvent(ctx context.Context, descID descpb.ID, event logpb.EventPayload) error {
	if sqlCommon, ok := event.(eventpb.EventWithCommonSQLPayload); ok {
		sqlCommon.CommonSQLDetails().DescriptorID = uint32(descID)
	}
	return p.logEventsWithOptions(ctx,
		2, /* depth: use caller location */
		eventLogOptions{dst: LogEverywhere},
		event)
}

// logEvents is like logEvent, except that it can write multiple
// events simultaneously. This is advantageous for SQL statements
// that produce multiple events, e.g. GRANT, as they will
// processed using only one write batch (and thus lower latency).
func (p *planner) logEvents(ctx context.Context, entries ...logpb.EventPayload) error {
	return p.logEventsWithOptions(ctx,
		2, /* depth: use caller location */
		eventLogOptions{dst: LogEverywhere},
		entries...)
}

// eventLogOptions
type eventLogOptions struct {
	// Where to emit the log event to.
	dst LogEventDestination

	// By default, a copy of each structured event is sent to the DEV
	// channel (in addition to its default, nominal channel) if the
	// vmodule filter is set to 2 or higher for the source file where
	// the event call originates.
	//
	// If verboseTraceLevel is non-zero, its value is used as value for
	// the vmodule filter. See exec_log for an example use.
	verboseTraceLevel log.Level
}

func (p *planner) getCommonSQLEventDetails() eventpb.CommonSQLEventDetails {
	redactableStmt := p.FormatAstAsRedactableString(p.stmt.AST, p.extendedEvalCtx.Context.Annotations)
	commonSQLEventDetails := eventpb.CommonSQLEventDetails{
		Statement:       redactableStmt,
		Tag:             p.stmt.AST.StatementTag(),
		User:            p.SessionData().SessionUser().Normalized(),
		ApplicationName: p.SessionData().ApplicationName,
	}
	if pls := p.extendedEvalCtx.Context.Placeholders.Values; len(pls) > 0 {
		commonSQLEventDetails.PlaceholderValues = make([]string, len(pls))
		for idx, val := range pls {
			commonSQLEventDetails.PlaceholderValues[idx] = val.String()
		}
	}
	return commonSQLEventDetails
}

// logEventsWithOptions is like logEvent() but it gives control to the
// caller as to where the event is written to.
//
// If opts.dst does not include LogToSystemTable, this function is
// guaranteed to not return an error.
func (p *planner) logEventsWithOptions(
	ctx context.Context, depth int, opts eventLogOptions, entries ...logpb.EventPayload,
) error {
	return logEventInternalForSQLStatements(ctx,
		p.extendedEvalCtx.ExecCfg, p.InternalSQLTxn(),
		1+depth,
		opts,
		p.getCommonSQLEventDetails(),
		entries...)
}

// logEventInternalForSchemaChange emits a cluster event in the
// context of a schema changer.
func logEventInternalForSchemaChanges(
	ctx context.Context,
	execCfg *ExecutorConfig,
	txn isql.Txn,
	sqlInstanceID base.SQLInstanceID,
	descID descpb.ID,
	mutationID descpb.MutationID,
	event logpb.EventPayload,
) error {
	event.CommonDetails().Timestamp = txn.KV().ReadTimestamp().WallTime
	scCommon, ok := event.(eventpb.EventWithCommonSchemaChangePayload)
	if !ok {
		return errors.AssertionFailedf("unknown event type: %T", event)
	}
	m := scCommon.CommonSchemaChangeDetails()
	m.InstanceID = int32(sqlInstanceID)
	m.DescriptorID = uint32(descID)
	m.MutationID = uint32(mutationID)

	// Delegate the storing of the event to the regular event logic.
	//
	// We use depth=1 because the caller of this function typically
	// wraps the call in a db.Txn() callback, which confuses the vmodule
	// filtering. Easiest is to pretend the event is sourced here.
	return insertEventRecords(
		ctx, execCfg,
		txn,
		1, /* depth: use this function as origin */
		eventLogOptions{dst: LogEverywhere},
		event,
	)
}

// logEventInternalForSQLStatements emits a cluster event on behalf of
// a SQL statement, when the point where the event is emitted does not
// have access to a (*planner) and the current statement metadata.
//
// In each event, if the DescriptorID field (in the
// CommonSQLEventDetails) is already populated, it is preserved.
//
// Note: usage of this interface should be minimized.
//
// If writeToEventLog is false, this function guarantees that it
// returns no error.
func logEventInternalForSQLStatements(
	ctx context.Context,
	execCfg *ExecutorConfig,
	txn isql.Txn,
	depth int,
	opts eventLogOptions,
	commonSQLEventDetails eventpb.CommonSQLEventDetails,
	entries ...logpb.EventPayload,
) error {
	// Inject the common fields into the payload provided by the caller.
	injectCommonFields := func(event logpb.EventPayload) error {
		if txn == nil {
			// No txn is set (e.g. for COPY or BEGIN), so use now instead.
			event.CommonDetails().Timestamp = timeutil.Now().UnixNano()
		} else {
			event.CommonDetails().Timestamp = txn.KV().ReadTimestamp().WallTime
		}
		sqlCommon, ok := event.(eventpb.EventWithCommonSQLPayload)
		if !ok {
			return errors.AssertionFailedf("unknown event type: %T", event)
		}
		m := sqlCommon.CommonSQLDetails()

		// We are going to inject the shared CommonSQLEventDetails into the event.
		// However, before we do this, we must take notice that the caller
		// may have populated the DescriptorID already. Here we have two cases:
		// - either the caller has provided a value in *both*
		//      commonSQLEventDetails.DescriptorID,
		//   and also
		//      the event .DescriptorID field
		//   in which case, we will keep the former.
		//
		// - or, only the event .DescriptorID is set, and
		//   commonSQLEventDetails.DescriptorID is zero.
		//   In this case, we keep the event's value.

		// First, save whatever value was there in the event first.
		prevDescID := m.DescriptorID

		// Overwrite with the common details.
		*m = commonSQLEventDetails

		// If the common details didn't have a descriptor ID, keep the
		// one that was in the event already.
		if m.DescriptorID == 0 {
			m.DescriptorID = prevDescID
		}
		return nil
	}

	for i := range entries {
		if err := injectCommonFields(entries[i]); err != nil {
			return err
		}
	}

	return insertEventRecords(
		ctx,
		execCfg,
		txn,
		1+depth, /* depth */
		opts,    /* eventLogOptions */
		entries...,
	)
}

type schemaChangerEventLogger struct {
	txn     isql.Txn
	execCfg *ExecutorConfig
	depth   int
}

var _ scrun.EventLogger = (*schemaChangerEventLogger)(nil)
var _ scbuild.EventLogger = (*schemaChangerEventLogger)(nil)

// NewSchemaChangerBuildEventLogger returns a scbuild.EventLogger implementation.
func NewSchemaChangerBuildEventLogger(txn isql.Txn, execCfg *ExecutorConfig) scbuild.EventLogger {
	return &schemaChangerEventLogger{
		txn:     txn,
		execCfg: execCfg,
		depth:   1,
	}
}

// NewSchemaChangerRunEventLogger returns a scrun.EventLogger implementation.
func NewSchemaChangerRunEventLogger(txn isql.Txn, execCfg *ExecutorConfig) scrun.EventLogger {
	return &schemaChangerEventLogger{
		txn:     txn,
		execCfg: execCfg,
		depth:   0,
	}
}

// LogEvent implements the scbuild.EventLogger interface.
func (l *schemaChangerEventLogger) LogEvent(
	ctx context.Context, details eventpb.CommonSQLEventDetails, event logpb.EventPayload,
) error {
	return logEventInternalForSQLStatements(ctx,
		l.execCfg,
		l.txn,
		l.depth,
		eventLogOptions{dst: LogEverywhere},
		details,
		event)
}

// LogEventForSchemaChange implements the scrun.EventLogger interface.
func (l *schemaChangerEventLogger) LogEventForSchemaChange(
	ctx context.Context, event logpb.EventPayload,
) error {
	event.CommonDetails().Timestamp = l.txn.KV().ReadTimestamp().WallTime
	scCommon, ok := event.(eventpb.EventWithCommonSchemaChangePayload)
	if !ok {
		return errors.AssertionFailedf("unknown event type: %T", event)
	}
	scCommon.CommonSchemaChangeDetails().InstanceID = int32(l.execCfg.NodeInfo.NodeID.SQLInstanceID())
	return insertEventRecords(
		ctx, l.execCfg,
		l.txn,
		1, /* depth: use this function as origin */
		eventLogOptions{dst: LogEverywhere},
		event,
	)
}

// LogEventForJobs emits a cluster event in the context of a job.
func LogEventForJobs(
	ctx context.Context,
	execCfg *ExecutorConfig,
	txn isql.Txn,
	event logpb.EventPayload,
	jobID int64,
	payload jobspb.Payload,
	user username.SQLUsername,
	status jobs.State,
) error {
	event.CommonDetails().Timestamp = txn.KV().ReadTimestamp().WallTime
	jobCommon, ok := event.(eventpb.EventWithCommonJobPayload)
	if !ok {
		return errors.AssertionFailedf("unknown event type: %T", event)
	}
	m := jobCommon.CommonJobDetails()
	m.JobID = jobID
	m.JobType = payload.Type().String()
	m.User = user.Normalized()
	m.Status = string(status)
	for _, id := range payload.DescriptorIDs {
		m.DescriptorIDs = append(m.DescriptorIDs, uint32(id))
	}
	m.Description = payload.Description

	// Delegate the storing of the event to the regular event logic.
	//
	// We use depth=1 because the caller of this function typically
	// wraps the call in a db.Txn() callback, which confuses the vmodule
	// filtering. Easiest is to pretend the event is sourced here.
	return insertEventRecords(
		ctx, execCfg,
		txn,
		1, /* depth: use this function for vmodule filtering */
		eventLogOptions{dst: LogEverywhere},
		event,
	)
}

var eventLogSystemTableEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"server.eventlog.enabled",
	"if set, logged notable events are also stored in the table system.eventlog",
	true,
	settings.WithPublic)

// EventLogTestingKnobs provides hooks and knobs for event logging.
type EventLogTestingKnobs struct {
	// SyncWrites causes events to be written on the same txn as
	// the SQL statement that causes them.
	SyncWrites bool
}

// ModuleTestingKnobs implements base.ModuleTestingKnobs interface.
func (*EventLogTestingKnobs) ModuleTestingKnobs() {}

// LogEventDestination indicates for InsertEventRecords where the
// event should be directed to.
type LogEventDestination int

// hasFlag returns true if the receiver has all of the given flags.
func (d LogEventDestination) hasFlag(f LogEventDestination) bool {
	return d&f == f
}

const (
	// LogToSystemTable makes InsertEventRecords write one or more
	// entries to the system eventlog table. (This behavior may be
	// removed in a later version.)
	LogToSystemTable LogEventDestination = 1 << iota
	// LogExternally makes InsertEventRecords write the event(s) to the
	// external logs.
	LogExternally
	// LogToDevChannelIfVerbose makes InsertEventRecords copy
	// the structured event to the DEV logging channel
	// if the vmodule filter for the log call is set high enough.
	LogToDevChannelIfVerbose

	// LogEverywhere logs to all the possible outputs.
	LogEverywhere LogEventDestination = LogExternally | LogToSystemTable | LogToDevChannelIfVerbose
)

// InsertEventRecords inserts events into the event log as part
// of the provided transaction, using the provided internal executor.
//
// This converts to a call to insertEventRecords() with just 1 entry.
//
// Note: it is not safe to pass the same entry references to multiple
// subsequent calls (it causes a race condition).
func InsertEventRecords(
	ctx context.Context, execCfg *ExecutorConfig, dst LogEventDestination, info ...logpb.EventPayload,
) {
	if len(info) == 0 {
		return
	}
	// We use depth=1 because the caller of this function typically
	// wraps the call in a db.Txn() callback, which confuses the vmodule
	// filtering. Easiest is to pretend the event is sourced here.
	err := insertEventRecords(ctx, execCfg, nil, /* txn */
		1, /* depth: use this function */
		eventLogOptions{dst: dst},
		info...)
	if err != nil {
		// By spec, it should not return an error when passed a nil txn.
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "insertEventRecords returned unexpected error"))
	}
}

// insertEventRecords inserts one or more event into the event log as
// part of the provided txn, using the provided internal executor.
//
// The caller is responsible for populating the timestamp field in the
// event payload and all the other per-payload specific fields. This
// function only takes care of populating the EventType field based on
// the run-time type of the event payload.
//
// If the txn field is non-nil and EventLogTestingKnobs.SyncWrites is
// set, the write is performed on the given txn object. We need this
// for tests.
//
// Otherwise, an asynchronous task is spawned to do the write:
//   - if there's at txn, after the txn commit time (i.e. we don't log
//     if the txn ends up aborting), using a txn commit trigger.
//   - otherwise (no txn), immediately.
//
// Note: it is not safe to pass the same entry references to multiple
// subsequent calls (it causes a race condition).
func insertEventRecords(
	ctx context.Context,
	execCfg *ExecutorConfig,
	txn isql.Txn,
	depth int,
	opts eventLogOptions,
	entries ...logpb.EventPayload,
) error {
	// Finish populating the entries.
	for i := range entries {
		// Ensure the type field is populated.
		event := entries[i]
		eventType := logpb.GetEventTypeName(event)
		event.CommonDetails().EventType = eventType

		// The caller is responsible for the timestamp field.
		if event.CommonDetails().Timestamp == 0 {
			return errors.AssertionFailedf("programming error: timestamp field in event %d not populated: %T", i, event)
		}
	}

	if opts.dst.hasFlag(LogToDevChannelIfVerbose) {
		// Emit a copy of the structured to the DEV channel when the
		// vmodule setting matches.
		level := log.Level(2)
		if opts.verboseTraceLevel != 0 {
			// Caller has overridden the level at which which log to the
			// trace.
			level = opts.verboseTraceLevel
		}
		if log.VDepth(level, depth) {
			// The VDepth() call ensures that we are matching the vmodule
			// setting to where the depth is equal to 1 in the caller stack.
			for i := range entries {
				log.InfofDepth(ctx, depth, "SQL event: payload %+v", entries[i])
			}
		}
	}

	// If we only want to log externally and not write to the events table, early exit.
	loggingToSystemTable := opts.dst.hasFlag(LogToSystemTable) && eventLogSystemTableEnabled.Get(&execCfg.Settings.SV)
	if !loggingToSystemTable {
		// Simply emit the events to their respective channels and call it a day.
		if opts.dst.hasFlag(LogExternally) {
			for i := range entries {
				log.StructuredEvent(ctx, severity.INFO, entries[i])
			}
		}
		// Not writing to system table: shortcut.
		return nil
	}

	// When logging to the system table and there is a txn open already,
	// ensure that the external logging only sees the event when the
	// transaction commits.
	if txn != nil && opts.dst.hasFlag(LogExternally) {
		txn.KV().AddCommitTrigger(func(ctx context.Context) {
			for i := range entries {
				log.StructuredEvent(ctx, severity.INFO, entries[i])
			}
		})
	}

	// Are we doing synchronous writes?
	syncWrites := execCfg.EventLogTestingKnobs != nil && execCfg.EventLogTestingKnobs.SyncWrites
	if txn != nil && syncWrites {
		// Yes, do it now.
		query, args := prepareEventWrite(ctx, execCfg, entries)
		return writeToSystemEventsTable(ctx, txn, len(entries), query, args)
	}
	// No: do them async.
	// With txn: trigger async write at end of txn (no event logged if txn aborts).
	// Without txn: schedule it now.
	if txn == nil {
		asyncWriteToOtelAndSystemEventsTable(ctx, execCfg, entries)
	} else {
		txn.KV().AddCommitTrigger(func(ctx context.Context) {
			asyncWriteToOtelAndSystemEventsTable(ctx, execCfg, entries)
		})
	}
	return nil
}

func asyncWriteToOtelAndSystemEventsTable(
	ctx context.Context, execCfg *ExecutorConfig, entries []logpb.EventPayload,
) {
	// perAttemptTimeout is the maximum amount of time to wait on each
	// eventlog write attempt.
	const perAttemptTimeout time.Duration = 5 * time.Second
	// maxAttempts is the maximum number of attempts to write an
	// eventlog entry.
	const maxAttempts = 10

	stopper := execCfg.RPCContext.Stopper
	origCtx := ctx
	if err := stopper.RunAsyncTask(
		// Note: we don't want to inherit the cancellation of the parent
		// context. The SQL statement that causes the eventlog entry may
		// terminate (and its context cancelled) before the eventlog entry
		// gets written.
		context.Background(), "record-events", func(ctx context.Context) {
			ctx, span := execCfg.AmbientCtx.AnnotateCtxWithSpan(ctx, "record-events")
			defer span.Finish()

			// Copy the tags from the original query (which will contain
			// things like username, current internal executor context, etc).
			ctx = logtags.AddTags(ctx, logtags.FromContext(origCtx))

			// Stop writing the event when the server shuts down.
			ctx, stopCancel := stopper.WithCancelOnQuiesce(ctx)
			defer stopCancel()

			// Prepare the data to send.
			query, args := prepareEventWrite(ctx, execCfg, entries)

			// We use a retry loop in case there are transient
			// non-retriable errors on the cluster during the table write.
			// (retriable errors are already processed automatically
			// by db.Txn)
			retryOpts := base.DefaultRetryOptions()
			retryOpts.Closer = ctx.Done()
			retryOpts.MaxRetries = int(maxAttempts)
			for r := retry.Start(retryOpts); r.Next(); {
				// Don't try too long to write if the system table is unavailable.
				if err := timeutil.RunWithTimeout(ctx, "record-events", perAttemptTimeout, func(ctx context.Context) error {
					return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
						return writeToSystemEventsTable(ctx, txn, len(entries), query, args)
					})
				}); err != nil {
					log.Ops.Warningf(ctx, "unable to save %d entries to system.eventlog: %v", len(entries), err)
				} else {
					break
				}
			}
		}); err != nil {
		expectedStopperError := errors.Is(err, stop.ErrThrottled) || errors.Is(err, stop.ErrUnavailable)
		if !expectedStopperError {
			// RunAsyncTask only returns an error not listed above
			// if its context was canceled, and we're using the
			// background context here.
			err = errors.NewAssertionErrorWithWrappedErrf(err, "unexpected stopper error")
		}
		log.Warningf(ctx, "failed to start task to save %d events in eventlog: %v", len(entries), err)
	}
}

func prepareEventWrite(
	ctx context.Context, execCfg *ExecutorConfig, entries []logpb.EventPayload,
) (query string, args []interface{}) {
	reportingID := execCfg.NodeInfo.NodeID.SQLInstanceID()
	const colsPerEvent = 4
	// Note: we insert the value zero as targetID because sadly this
	// now-deprecated column has a NOT NULL constraint.
	// TODO(knz): Add a migration to remove the column altogether.
	const baseQuery = `
INSERT INTO system.eventlog (
  timestamp, "eventType", "reportingID", info, "targetID"
)
VALUES($1, $2, $3, $4, 0)`
	args = make([]interface{}, 0, len(entries)*colsPerEvent)

	sp := tracing.SpanFromContext(ctx)
	var traceID [16]byte
	var spanID [8]byte
	if sp != nil {
		// Our trace IDs are 8 bytes, but OTLP insists on 16. We'll use only the
		// most significant bytes and leave the rest zero.
		//
		// NOTE(andrei): The BigEndian is an arbitrary decision; I don't know how
		// others serialize their UUIDs, but I went with the network byte order.
		binary.BigEndian.PutUint64(traceID[:], uint64(sp.TraceID()))
		binary.BigEndian.PutUint64(spanID[:], uint64(sp.SpanID()))
	}
	for i := 0; i < len(entries); i++ {
		event := entries[i]

		infoBytes := redact.RedactableBytes("{")
		_, infoBytes = event.AppendJSONFields(false /* printComma */, infoBytes)
		infoBytes = append(infoBytes, '}')
		// In the system.eventlog table, we do not use redaction markers.
		// (compatibility with previous versions of CockroachDB.)
		infoBytes = infoBytes.StripMarkers()
		eventType := event.CommonDetails().EventType
		args = append(
			args,
			timeutil.Unix(0, event.CommonDetails().Timestamp),
			eventType,
			reportingID,
			string(infoBytes),
		)
	}

	// In the common case where we have just 1 event, we want to skeep
	// the extra heap allocation and buffer operations of the loop
	// below. This is an optimization.
	query = baseQuery
	if len(entries) > 1 {
		// Extend the query with additional VALUES clauses for all the
		// events after the first one.
		var completeQuery strings.Builder
		completeQuery.WriteString(baseQuery)

		for i := range entries[1:] {
			placeholderNum := 1 + colsPerEvent*(i+1)
			fmt.Fprintf(&completeQuery, ", ($%d, $%d, $%d, $%d, 0)",
				placeholderNum, placeholderNum+1, placeholderNum+2, placeholderNum+3)
		}
		query = completeQuery.String()
	}

	return query, args
}

func writeToSystemEventsTable(
	ctx context.Context, txn isql.Txn, numEntries int, query string, args []interface{},
) error {
	rows, err := txn.ExecEx(
		ctx, "log-event", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		query, args...,
	)
	if err != nil {
		return err
	}
	if rows != numEntries {
		return errors.AssertionFailedf("%d rows affected by log insertion; expected %d rows affected", rows, numEntries)
	}
	return nil
}
