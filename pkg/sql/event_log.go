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
	return insertEventRecords(
		ctx, execCfg.InternalExecutor,
		txn,
		int32(execCfg.NodeID.SQLInstanceID()), /* reporter ID */
		false,                                 /* skipExternalLog */
		false,                                 /* onlyLog */
		eventLogEntry{
			targetID: int32(descID),
			event:    event,
		},
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

	return insertEventRecords(ctx, execCfg.InternalExecutor,
		txn,
		int32(execCfg.NodeID.SQLInstanceID()),
		false,            /* skipExternalLog */
		!writeToEventLog, /* onlyLog */
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
	return insertEventRecords(
		ctx, execCfg.InternalExecutor,
		txn,
		int32(execCfg.NodeID.SQLInstanceID()), /* reporter ID */
		false,                                 /* skipExternalLog */
		false,                                 /* onlyLog */
		eventLogEntry{event: event},
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
// This converts to a call to insertEventRecords() with just 1 entry.
func InsertEventRecord(
	ctx context.Context,
	ex *InternalExecutor,
	txn *kv.Txn,
	targetID, reportingID int32,
	skipExternalLog bool,
	info eventpb.EventPayload,
	onlyLog bool,
) error {
	return insertEventRecords(ctx, ex, txn, reportingID,
		skipExternalLog, onlyLog,
		eventLogEntry{targetID: targetID, event: info})
}

// insertEventRecords inserts one or more event into the event log as
// part of the provided txn, using the provided internal executor.
//
// The caller is responsible for populating the timestamp field in the
// event payload and all the other per-payload specific fields. This
// function only takes care of populating the EventType field based on
// the run-time type of the event payload.
//
// Note: the targetID and reportingID columns are deprecated and
// should be removed after v21.1 is released.
func insertEventRecords(
	ctx context.Context,
	ex *InternalExecutor,
	txn *kv.Txn,
	reportingID int32,
	skipExternalLog bool,
	onlyLog bool,
	entries ...eventLogEntry,
) error {
	// Finish populating the entries.
	for i := range entries {
		// Ensure the type field is populated.
		event := entries[i].event
		eventType := eventpb.GetEventTypeName(event)
		event.CommonDetails().EventType = eventType

		// The caller is responsible for the timestamp field.
		if event.CommonDetails().Timestamp == 0 {
			return errors.AssertionFailedf("programming error: timestamp field in event %d not populated: %T", i, event)
		}
	}

	// If we only want to log externally and not write to the events table, early exit.
	loggingToSystemTable := !onlyLog && eventLogEnabled.Get(&ex.s.cfg.Settings.SV)
	if !loggingToSystemTable {
		// Simply emit the events to their respective channels and call it a day.
		if !skipExternalLog {
			for i := range entries {
				log.StructuredEvent(ctx, entries[i].event)
			}
		}
		// Not writing to system table: shortcut.
		return nil
	}

	// When logging to the system table, ensure that the external
	// logging only sees the event when the transaction commits.
	if !skipExternalLog {
		txn.AddCommitTrigger(func(ctx context.Context) {
			for i := range entries {
				log.StructuredEvent(ctx, entries[i].event)
			}
		})
	}

	// The function below this point is specialized to write to the
	// system table.

	const colsPerEvent = 5
	const baseQuery = `
INSERT INTO system.eventlog (
  timestamp, "eventType", "targetID", "reportingID", info
)
VALUES($1, $2, $3, $4, $5)`
	args := make([]interface{}, 0, len(entries)*colsPerEvent)
	constructArgs := func(reportingID int32, entry eventLogEntry) error {
		event := entry.event
		infoBytes, err := json.Marshal(event)
		if err != nil {
			return err
		}
		eventType := eventpb.GetEventTypeName(event)
		args = append(
			args,
			timeutil.Unix(0, event.CommonDetails().Timestamp),
			eventType,
			entry.targetID,
			reportingID,
			string(infoBytes),
		)
		return nil
	}

	// In the common case where we have just 1 event, we want to skeep
	// the extra heap allocation and buffer operations of the loop
	// below. This is an optimization.
	query := baseQuery
	if err := constructArgs(reportingID, entries[0]); err != nil {
		return err
	}
	if len(entries) > 1 {
		// Extend the query with additional VALUES clauses for all the
		// events after the first one.
		var completeQuery strings.Builder
		completeQuery.WriteString(baseQuery)

		for _, extraEntry := range entries[1:] {
			placeholderNum := 1 + len(args)
			if err := constructArgs(reportingID, extraEntry); err != nil {
				return err
			}
			fmt.Fprintf(&completeQuery, ", ($%d, $%d, $%d, $%d, $%d)",
				placeholderNum, placeholderNum+1, placeholderNum+2, placeholderNum+3, placeholderNum+4)
		}
		query = completeQuery.String()
	}

	rows, err := ex.Exec(ctx, "log-event", txn, query, args...)
	if err != nil {
		return err
	}
	if rows != len(entries) {
		return errors.Errorf("%d rows affected by log insertion; expected %d rows affected.", rows, len(entries))
	}
	return nil
}
