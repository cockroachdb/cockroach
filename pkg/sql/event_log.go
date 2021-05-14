// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// eventLogEntry represents a SQL-level event to be sent to logging
// outputs(s).
type eventLogEntry struct {
	// targetID is the main object affected by this event.
	// For DDL statements, this is typically the ID of
	// the affected descriptor.
	targetID int32

	// event is the main event payload.
	event eventpb.EventPayload
}

// logEvent emits a cluster event in the context of a regular SQL
// statement.
func (p *planner) logEvent(
	ctx context.Context, descID descpb.ID, event eventpb.EventPayload,
) error {
	return p.logEventsWithSystemEventLogOption(ctx, true, /* writeToEventLog */
		eventLogEntry{targetID: int32(descID), event: event})
}

// logEvents is like logEvent, except that it can write multiple
// events simultaneously. This is advantageous for SQL statements
// that produce multiple events, e.g. GRANT, as they will
// processed using only one write batch (and thus lower latency).
func (p *planner) logEvents(ctx context.Context, entries ...eventLogEntry) error {
	return p.logEventsWithSystemEventLogOption(ctx, true /* writeToEventLog */, entries...)
}

func (p *planner) logEventsOnlyExternally(ctx context.Context, entries ...eventLogEntry) {
	// The API contract for logEventWithSystemEventLogOption() is that it returns
	// no error when system.eventlog is not written to.
	_ = p.logEventsWithSystemEventLogOption(ctx, false /* writeToEventLog */, entries...)
}

// logEventsWithSystemEventLogOption is like logEvent() but it gives
// control to the caller as to whether the entry is written into
// system.eventlog.
//
// If writeToEventLog is false, this function guarantees that it
// returns no error.
func (p *planner) logEventsWithSystemEventLogOption(
	ctx context.Context, writeToEventLog bool, entries ...eventLogEntry,
) error {
	user := p.User()
	stmt := tree.AsStringWithFQNames(p.stmt.AST, p.extendedEvalCtx.EvalContext.Annotations)
	stmtTag := p.stmt.AST.StatementTag()
	pl := p.extendedEvalCtx.EvalContext.Placeholders.Values
	appName := p.SessionData().ApplicationName
	return logEventInternalForSQLStatements(ctx, p.extendedEvalCtx.ExecCfg, p.txn, user, appName, stmt, stmtTag, pl, writeToEventLog, entries...)
}

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
) error {
	event.CommonDetails().Timestamp = txn.ReadTimestamp().WallTime
	scCommon, ok := event.(eventpb.EventWithCommonSchemaChangePayload)
	if !ok {
		return errors.AssertionFailedf("unknown event type: %T", event)
	}
	m := scCommon.CommonSchemaChangeDetails()
	m.InstanceID = int32(sqlInstanceID)
	m.DescriptorID = uint32(descID)
	m.MutationID = uint32(mutationID)

	// Delegate the storing of the event to the regular event logic.
	return InsertEventRecord(
		ctx, execCfg.InternalExecutor,
		txn,
		int32(descID),
		int32(execCfg.NodeID.SQLInstanceID()),
		false, /* skipExternalLog */
		event,
		false, /* onlyLog */
	)
}

// logEventInternalForSQLStatements emits a cluster event on behalf of
// a SQL statement, when the point where the event is emitted does not
// have access to a (*planner) and the current statement metadata.
//
// Note: usage of this interface should be minimized.
//
// If writeToEventLog is false, this function guarantees that it
// returns no error.
func logEventInternalForSQLStatements(
	ctx context.Context,
	execCfg *ExecutorConfig,
	txn *kv.Txn,
	user security.SQLUsername,
	appName string,
	stmt string,
	stmtTag string,
	placeholders tree.QueryArguments,
	writeToEventLog bool,
	entries ...eventLogEntry,
) error {
	// Inject the common fields into the payload provided by the caller.
	for i := range entries {
		if err := injectCommonFields(
			txn, entries[i].targetID, user, appName, stmt, stmtTag, placeholders, entries[i].event,
		); err != nil {
			return err
		}
	}

	// Delegate the storing of the event to the regular event logic.
	if !writeToEventLog || !eventLogEnabled.Get(&execCfg.InternalExecutor.s.cfg.Settings.SV) {
		return skipWritePath(ctx, txn, entries, !writeToEventLog)
	}

	return batchInsertEventRecords(ctx, execCfg.InternalExecutor,
		txn,
		int32(execCfg.NodeID.SQLInstanceID()),
		entries...,
	)
}

// injectCommonFields injects the common fields into the event payload provided by the caller.
func injectCommonFields(
	txn *kv.Txn,
	descID int32,
	user security.SQLUsername,
	appName string,
	stmt string,
	stmtTag string,
	placeholders tree.QueryArguments,
	event eventpb.EventPayload,
) error {
	event.CommonDetails().Timestamp = txn.ReadTimestamp().WallTime
	sqlCommon, ok := event.(eventpb.EventWithCommonSQLPayload)
	if !ok {
		return errors.AssertionFailedf("unknown event type: %T", event)
	}
	m := sqlCommon.CommonSQLDetails()
	m.Statement = stmt
	m.Tag = stmtTag
	m.ApplicationName = appName
	m.User = user.Normalized()
	m.DescriptorID = uint32(descID)
	if len(placeholders) > 0 {
		m.PlaceholderValues = make([]string, len(placeholders))
		for idx, val := range placeholders {
			m.PlaceholderValues[idx] = val.String()
		}
	}
	return nil
}

// LogEventForJobs emits a cluster event in the context of a job.
func LogEventForJobs(
	ctx context.Context,
	execCfg *ExecutorConfig,
	txn *kv.Txn,
	event eventpb.EventPayload,
	jobID int64,
	payload jobspb.Payload,
	user security.SQLUsername,
	status jobs.Status,
) error {
	event.CommonDetails().Timestamp = txn.ReadTimestamp().WallTime
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
	return InsertEventRecord(
		ctx, execCfg.InternalExecutor,
		txn,
		0, /* targetID */
		int32(execCfg.NodeID.SQLInstanceID()),
		false, /* skipExternalLog */
		event,
		false, /* onlyLog */
	)
}

var eventLogEnabled = settings.RegisterBoolSetting(
	"server.eventlog.enabled",
	"if set, logged notable events are also stored in the table system.eventlog",
	true,
).WithPublic()

// InsertEventRecord inserts a single event into the event log as part
// of the provided transaction, using the provided internal executor.
//
// The caller is responsible for populating the timestamp field
// in the event payload.
//
// If the skipExternalLog bool is set, this function does not call
// log.StructuredEvent(). In that case, the caller is responsible for
// calling log.StructuredEvent() directly.
//
// Note: the targetID and reportingID columns are deprecated and
// should be removed after v21.1 is released.
//
// If onlyLog is set, this function guarantees that it returns no
// error.
func InsertEventRecord(
	ctx context.Context,
	ex *InternalExecutor,
	txn *kv.Txn,
	targetID, reportingID int32,
	skipExternalLog bool,
	info eventpb.EventPayload,
	onlyLog bool,
) error {
	if onlyLog || !eventLogEnabled.Get(&ex.s.cfg.Settings.SV) {
		return skipWritePath(ctx, txn, []eventLogEntry{{event: info}}, onlyLog)
	}
	return batchInsertEventRecords(
		ctx, ex, txn,
		reportingID,
		eventLogEntry{
			targetID: targetID,
			event:    info,
		})
}

// batchInsertEventRecords is like InsertEventRecord except it takes
// a slice of events to batch write. Any insert that calls this function
// will always write to the event table (i.e. it won't only log them, and writing
// to the event table will not be disabled).
func batchInsertEventRecords(
	ctx context.Context,
	ex *InternalExecutor,
	txn *kv.Txn,
	reportingID int32,
	entries ...eventLogEntry,
) error {
	const colsPerEvent = 5
	const baseQuery = `
INSERT INTO system.eventlog (
  timestamp, "eventType", "targetID", "reportingID", info
)
VALUES($1, $2, $3, $4, $5)`
	args := make([]interface{}, 0, len(entries)*colsPerEvent)

	// Prepare first row so we can take the fast path if we're only inserting one event log.
	if err := prepareRow(
		ctx, txn, &args, entries[0].event, entries[0].targetID, reportingID,
	); err != nil {
		return err
	}
	if len(entries) == 1 {
		return execEventLogInsert(ctx, ex, txn, baseQuery, args, len(entries))
	}

	var additionalRows strings.Builder
	for i := 1; i < len(entries); i++ {
		var placeholderNum = 1 + (i * colsPerEvent)
		if err := prepareRow(ctx, txn, &args, entries[i].event, entries[i].targetID, reportingID); err != nil {
			return err
		}
		additionalRows.WriteString(fmt.Sprintf(", ($%d, $%d, $%d, $%d, $%d)",
			placeholderNum, placeholderNum+1, placeholderNum+2, placeholderNum+3, placeholderNum+4))
	}

	rows, err := ex.Exec(ctx, "log-event", txn, baseQuery+additionalRows.String(), args...)
	if err != nil {
		return err
	}
	if rows != len(entries) {
		return errors.Errorf("%d rows affected by log insertion; expected %d rows affected.", rows, len(entries))
	}
	return nil
}

// skipWritePath is used when either onlyLog is true, or writes to the event log
// table are disabled. In these cases, we do not write to the event log table.
func skipWritePath(ctx context.Context, txn *kv.Txn, entries []eventLogEntry, onlyLog bool) error {
	for i := range entries {
		if err := setupEventAndMaybeLog(
			ctx, txn, entries[i].event, onlyLog,
		); err != nil {
			return err
		}
	}
	return nil
}

// setupEventAndMaybeLog prepares the event log to be written. Also,
// if onlyLog is true, it will log the event.
func setupEventAndMaybeLog(
	ctx context.Context, txn *kv.Txn, info eventpb.EventPayload, onlyLog bool,
) error {
	eventType := eventpb.GetEventTypeName(info)

	// Ensure the type field is populated.
	info.CommonDetails().EventType = eventType

	// The caller is responsible for the timestamp field.
	if info.CommonDetails().Timestamp == 0 {
		return errors.AssertionFailedf("programming error: timestamp field in event not populated: %T", info)
	}

	// If we only want to log and not write to the events table, early exit.
	if onlyLog {
		log.StructuredEvent(ctx, info)
		return nil
	}

	// Ensure that the external logging sees the event when the
	// transaction commits.
	txn.AddCommitTrigger(func(ctx context.Context) {
		log.StructuredEvent(ctx, info)
	})

	return nil
}

// constructArgs constructs the values for a single event-log row insert.
func constructArgs(
	args *[]interface{},
	event eventpb.EventPayload,
	eventType string,
	targetID int32,
	reportingID int32,
) error {
	*args = append(
		*args,
		timeutil.Unix(0, event.CommonDetails().Timestamp),
		eventType, targetID,
		reportingID,
	)
	var info interface{}
	if event != nil {
		infoBytes, err := json.Marshal(event)
		if err != nil {
			return err
		}
		info = string(infoBytes)
	}
	*args = append(*args, info)
	return nil
}

// execEventLogInsert executes the insert query to insert the new events
// into the event log table.
func execEventLogInsert(
	ctx context.Context,
	ex *InternalExecutor,
	txn *kv.Txn,
	query string,
	args []interface{},
	numEvents int,
) error {
	rows, err := ex.Exec(ctx, "log-event", txn, query, args...)
	if err != nil {
		return err
	}
	if rows != numEvents {
		return errors.Errorf("%d rows affected by log insertion; expected %d rows affected.", rows, numEvents)
	}
	return nil
}

// prepareRow creates the values of an insert for a row. It populates the
// event payload with additional info, and then adds the values of the row to args.
func prepareRow(
	ctx context.Context,
	txn *kv.Txn,
	args *[]interface{},
	event eventpb.EventPayload,
	targetID int32,
	reportingID int32,
) error {
	// Setup event log.
	eventType := eventpb.GetEventTypeName(event)
	if err := setupEventAndMaybeLog(
		ctx, txn, event, false, /* onlyLog */
	); err != nil {
		return err
	}

	// Construct the args for this row.
	if err := constructArgs(args, event, eventType, targetID, reportingID); err != nil {
		return err
	}
	return nil
}
