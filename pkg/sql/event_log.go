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
	// Compute the common fields from data already known to the planner.
	user := p.User()
	stmt := tree.AsStringWithFQNames(p.stmt.AST, p.extendedEvalCtx.EvalContext.Annotations)
	pl := p.extendedEvalCtx.EvalContext.Placeholders.Values
	appName := p.SessionData().ApplicationName

	return logEventInternalForSQLStatements(ctx, p.extendedEvalCtx.ExecCfg, p.txn, descID, user, appName, stmt, pl, event, writeToEventLog)
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
	// Inject the common fields into the payload provided by the caller.
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
	eventType := eventpb.GetEventTypeName(info)

	// Ensure the type field is populated.
	info.CommonDetails().EventType = eventType

	// The caller is responsible for the timestamp field.
	if info.CommonDetails().Timestamp == 0 {
		return errors.AssertionFailedf("programming error: timestamp field in event not populated: %T", info)
	}

	if onlyLog {
		log.StructuredEvent(ctx, info)
		return nil
	}

	// Ensure that the external logging sees the event when the
	// transaction commits.
	txn.AddCommitTrigger(func(ctx context.Context) {
		log.StructuredEvent(ctx, info)
	})

	// If writes to the event log table are disabled, take a shortcut.
	if !eventLogEnabled.Get(&ex.s.cfg.Settings.SV) {
		return nil
	}

	const insertEventTableStmt = `
INSERT INTO system.eventlog (
  timestamp, "eventType", "targetID", "reportingID", info
)
VALUES(
  $1, $2, $3, $4, $5
)
`
	args := []interface{}{
		timeutil.Unix(0, info.CommonDetails().Timestamp),
		eventType,
		targetID,
		reportingID,
		nil, // info
	}
	if info != nil {
		infoBytes, err := json.Marshal(info)
		if err != nil {
			return err
		}
		args[4] = string(infoBytes)
	}
	rows, err := ex.Exec(ctx, "log-event", txn, insertEventTableStmt, args...)
	if err != nil {
		return err
	}
	if rows != 1 {
		return errors.Errorf("%d rows affected by log insertion; expected exactly one row affected.", rows)
	}
	return nil
}
