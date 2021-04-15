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

// logEvent emits a cluster event in the context of a regular SQL
// statement.
func (p *planner) logEvent(
	ctx context.Context, descID descpb.ID, event eventpb.EventPayload,
) error {
	return p.logEventWithSystemEventLogOption(ctx, descID, event, true /* writeToEventLog */)
}

// batchLogEvents is like logEvent, except it takes in slice of events
// to batch write.
func (p *planner) batchLogEvents(
	ctx context.Context, descIDs descpb.IDs, events []eventpb.EventPayload,
) error {
	return p.batchLogEventsWithSystemEventLogOption(ctx, descIDs, events, true /* writeToEventLog */)
}

func (p *planner) logEventOnlyExternally(
	ctx context.Context, descID descpb.ID, event eventpb.EventPayload,
) {
	// The API contract for logEventWithSystemEventLogOption() is that it returns
	// no error when system.eventlog is not written to.
	_ = p.logEventWithSystemEventLogOption(ctx, descID, event, false /* writeToEventLog */)
}

// logEventWithSystemEventLogOption is like logEvent() but it gives
// control to the caller as to whether the entry is written into
// system.eventlog.
//
// If writeToEventLog is false, this function guarantees that it
// returns no error.
func (p *planner) logEventWithSystemEventLogOption(
	ctx context.Context, descID descpb.ID, event eventpb.EventPayload, writeToEventLog bool,
) error {
	user, stmt, pl, appName := computeCommonFields(p)
	return logEventInternalForSQLStatements(ctx, p.extendedEvalCtx.ExecCfg, p.txn, descID, user, appName, stmt, pl, event, writeToEventLog)
}

// batchLogEventsWithSystemEventLogOption is like logEventWithSystemEventLogOption
// except it takes a slice of events to batch write.
func (p *planner) batchLogEventsWithSystemEventLogOption(
	ctx context.Context, descIDs descpb.IDs, events []eventpb.EventPayload, writeToEventLog bool,
) error {
	user, stmt, pl, appName := computeCommonFields(p)
	return batchLogEventInternalForSQLStatements(ctx, p.extendedEvalCtx.ExecCfg, p.txn, descIDs, user, appName, stmt, pl, events, writeToEventLog)
}

// computeCommonFields computes the common fields from data already known to the planner.
func computeCommonFields(p *planner) (security.SQLUsername, string, tree.QueryArguments, string) {
	user := p.User()
	stmt := tree.AsStringWithFQNames(p.stmt.AST, p.extendedEvalCtx.EvalContext.Annotations)
	pl := p.extendedEvalCtx.EvalContext.Placeholders.Values
	appName := p.SessionData().ApplicationName
	return user, stmt, pl, appName
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
	descID descpb.ID,
	user security.SQLUsername,
	appName string,
	stmt string,
	placeholders tree.QueryArguments,
	event eventpb.EventPayload,
	writeToEventLog bool,
) error {
	if err := injectCommonFields(
		txn, descID, user, appName, stmt, placeholders, event,
	); err != nil {
		return err
	}

	// Delegate the storing of the event to the regular event logic.
	return InsertEventRecord(ctx, execCfg.InternalExecutor,
		txn,
		int32(descID),
		int32(execCfg.NodeID.SQLInstanceID()),
		false, /* skipExternalLog */
		event,
		!writeToEventLog,
	)
}

// batchLogEventInternalForSQLStatements is like logEventInternalForSQLStatements
// except it takes a slice of events to batch write.
func batchLogEventInternalForSQLStatements(
	ctx context.Context,
	execCfg *ExecutorConfig,
	txn *kv.Txn,
	descIDs descpb.IDs,
	user security.SQLUsername,
	appName string,
	stmt string,
	placeholders tree.QueryArguments,
	events []eventpb.EventPayload,
	writeToEventLog bool,
) error {
	// Inject the common fields into the payload provided by the caller.
	for i := range events {
		if err := injectCommonFields(
			txn, descIDs[i], user, appName, stmt, placeholders, events[i],
		); err != nil {
			return err
		}
	}

	// Delegate the storing of the event to the regular event logic.
	if !writeToEventLog || !eventLogEnabled.Get(&execCfg.InternalExecutor.s.cfg.Settings.SV) {
		return skipWritePath(ctx, txn, events, !writeToEventLog)
	}

	return batchInsertEventRecords(ctx, execCfg.InternalExecutor,
		txn,
		descIDs,
		int32(execCfg.NodeID.SQLInstanceID()),
		events,
	)
}

// injectCommonFields injects the common fields into the event payload provided by the caller.
func injectCommonFields(
	txn *kv.Txn,
	descID descpb.ID,
	user security.SQLUsername,
	appName string,
	stmt string,
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
		return skipWritePath(ctx, txn, []eventpb.EventPayload{info}, onlyLog)
	}

	return batchInsertEventRecords(
		ctx, ex, txn,
		descpb.IDs{descpb.ID(targetID)},
		reportingID,
		[]eventpb.EventPayload{info},
	)
}

// batchInsertEventRecords is like InsertEventRecord except it takes
// a slice of events to batch write. Any insert that calls this function
// will always write to the event table (i.e. it won't only log them, and writing
// to the event table will not be disabled).
func batchInsertEventRecords(
	ctx context.Context,
	ex *InternalExecutor,
	txn *kv.Txn,
	descIDs descpb.IDs,
	reportingID int32,
	events []eventpb.EventPayload,
) error {
	const colsPerEvent = 5
	const baseQuery = `
INSERT INTO system.eventlog (
  timestamp, "eventType", "targetID", "reportingID", info
)
VALUES($1, $2, $3, $4, $5)`
	args := make([]interface{}, 0, len(events)*colsPerEvent)

	// Prepare first row so we can take the fast path if we're only inserting one event log.
	if err := prepareRow(
		ctx, txn, &args, events[0], descIDs[0], reportingID,
	); err != nil {
		return err
	}
	if len(events) == 1 {
		return execEventLogInsert(ctx, ex, txn, baseQuery, args, len(events))
	}

	var additionalRows strings.Builder
	for i := 1; i < len(events); i++ {
		var placeholderNum = 1 + (i * colsPerEvent)
		if err := prepareRow(ctx, txn, &args, events[i], descIDs[i], reportingID); err != nil {
			return err
		}
		additionalRows.WriteString(fmt.Sprintf(", ($%d, $%d, $%d, $%d, $%d)",
			placeholderNum, placeholderNum+1, placeholderNum+2, placeholderNum+3, placeholderNum+4))
	}

	rows, err := ex.Exec(ctx, "log-event", txn, baseQuery+additionalRows.String(), args...)
	if err != nil {
		return err
	}
	if rows != len(events) {
		return errors.Errorf("%d rows affected by log insertion; expected %d rows affected.", rows, len(events))
	}
	return nil
}

// skipWritePath is used when either onlyLog is true, or writes to the event log
// table are disabled. In these cases, we do not write to the event log table.
func skipWritePath(
	ctx context.Context, txn *kv.Txn, events []eventpb.EventPayload, onlyLog bool,
) error {
	for i := 1; i < len(events); i++ {
		if err := setupEventAndMaybeLog(
			ctx, txn, events[i], onlyLog,
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
	descID descpb.ID,
	reportingID int32,
) error {
	*args = append(
		*args,
		timeutil.Unix(0, event.CommonDetails().Timestamp),
		eventType, int32(descID),
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
	descID descpb.ID,
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
	if err := constructArgs(args, event, eventType, descID, reportingID); err != nil {
		return err
	}
	return nil
}
