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
	return p.logEventsWithOptions(ctx,
		eventLogOptions{dst: LogEverywhere},
		eventLogEntry{targetID: int32(descID), event: event})
}

// logEvents is like logEvent, except that it can write multiple
// events simultaneously. This is advantageous for SQL statements
// that produce multiple events, e.g. GRANT, as they will
// processed using only one write batch (and thus lower latency).
func (p *planner) logEvents(ctx context.Context, entries ...eventLogEntry) error {
	return p.logEventsWithOptions(ctx,
		eventLogOptions{dst: LogEverywhere},
		entries...)
}

// eventLogOptions
type eventLogOptions struct {
	// Where to emit the log event to.
	dst LogEventDestination
}

// logEventsWithOptions is like logEvent() but it gives control to the
// caller as to where the event is written to.
//
// If opts.dst does not include LogToSystemTable, this function is
// guaranteed to not return an error.
func (p *planner) logEventsWithOptions(
	ctx context.Context, opts eventLogOptions, entries ...eventLogEntry,
) error {
	commonPayload := sqlEventCommonExecPayload{
		user:         p.User(),
		stmt:         tree.AsStringWithFQNames(p.stmt.AST, p.extendedEvalCtx.EvalContext.Annotations),
		stmtTag:      p.stmt.AST.StatementTag(),
		placeholders: p.extendedEvalCtx.EvalContext.Placeholders.Values,
		appName:      p.SessionData().ApplicationName,
	}
	return logEventInternalForSQLStatements(ctx,
		p.extendedEvalCtx.ExecCfg, p.txn,
		opts,
		commonPayload,
		entries...)
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
		eventLogOptions{dst: LogEverywhere},
		eventLogEntry{
			targetID: int32(descID),
			event:    event,
		},
	)
}

// sqlEventExecPayload contains the statement and session details
// necessary to populate an eventpb.CommonSQLExecDetails.
type sqlEventCommonExecPayload struct {
	user         security.SQLUsername
	stmt         string
	stmtTag      string
	placeholders tree.QueryArguments
	appName      string
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
	opts eventLogOptions,
	commonPayload sqlEventCommonExecPayload,
	entries ...eventLogEntry,
) error {
	// Inject the common fields into the payload provided by the caller.
	injectCommonFields := func(entry eventLogEntry) error {
		event := entry.event
		event.CommonDetails().Timestamp = txn.ReadTimestamp().WallTime
		sqlCommon, ok := event.(eventpb.EventWithCommonSQLPayload)
		if !ok {
			return errors.AssertionFailedf("unknown event type: %T", event)
		}
		m := sqlCommon.CommonSQLDetails()
		m.Statement = commonPayload.stmt
		m.Tag = commonPayload.stmtTag
		m.ApplicationName = commonPayload.appName
		m.User = commonPayload.user.Normalized()
		m.DescriptorID = uint32(entry.targetID)
		if pls := commonPayload.placeholders; len(pls) > 0 {
			m.PlaceholderValues = make([]string, len(pls))
			for idx, val := range pls {
				m.PlaceholderValues[idx] = val.String()
			}
		}
		return nil
	}

	for i := range entries {
		if err := injectCommonFields(entries[i]); err != nil {
			return err
		}
	}

	return insertEventRecords(ctx,
		execCfg.InternalExecutor, txn,
		int32(execCfg.NodeID.SQLInstanceID()), /* reporter ID */
		opts,                                  /* eventLogOptions */
		entries...,                            /* ...eventLogEntry */
	)
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
		eventLogOptions{dst: LogEverywhere},
		eventLogEntry{event: event},
	)
}

var eventLogSystemTableEnabled = settings.RegisterBoolSetting(
	"server.eventlog.enabled",
	"if set, logged notable events are also stored in the table system.eventlog",
	true,
).WithPublic()

// LogEventDestination indicates for InsertEventRecord where the
// event should be directed to.
type LogEventDestination int

func (d LogEventDestination) hasFlag(f LogEventDestination) bool {
	return d&f != 0
}

const (
	// LogToSystemTable makes InsertEventRecord write one or more
	// entries to the system eventlog table. (This behavior may be
	// removed in a later version.)
	LogToSystemTable LogEventDestination = 1 << iota
	// LogExternally makes InsertEventRecord write the event(s) to the
	// external logs.
	LogExternally

	// LogEverywhere logs to all the possible outputs.
	LogEverywhere LogEventDestination = LogExternally | LogToSystemTable
)

// InsertEventRecord inserts a single event into the event log as part
// of the provided transaction, using the provided internal executor.
//
// This converts to a call to insertEventRecords() with just 1 entry.
func InsertEventRecord(
	ctx context.Context,
	ex *InternalExecutor,
	txn *kv.Txn,
	reportingID int32,
	dst LogEventDestination,
	targetID int32,
	info eventpb.EventPayload,
) error {
	return insertEventRecords(ctx, ex, txn, reportingID,
		eventLogOptions{dst: dst},
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
	opts eventLogOptions,
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
	loggingToSystemTable := opts.dst.hasFlag(LogToSystemTable) && eventLogSystemTableEnabled.Get(&ex.s.cfg.Settings.SV)
	if !loggingToSystemTable {
		// Simply emit the events to their respective channels and call it a day.
		if opts.dst.hasFlag(LogExternally) {
			for i := range entries {
				log.StructuredEvent(ctx, entries[i].event)
			}
		}
		// Not writing to system table: shortcut.
		return nil
	}

	// When logging to the system table, ensure that the external
	// logging only sees the event when the transaction commits.
	if opts.dst.hasFlag(LogExternally) {
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
