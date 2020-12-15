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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

// logEvent emits a cluster event in the context of a regular SQL
// statement.
func (p *planner) logEvent(
	ctx context.Context, descID descpb.ID, event eventpb.EventPayload,
) error {
	// Compute the common fields from data already known to the planner.
	user := p.User()
	stmt := tree.AsStringWithFQNames(p.stmt.AST, p.extendedEvalCtx.EvalContext.Annotations)

	return logEventInternalForSQLStatements(ctx, p.extendedEvalCtx.ExecCfg, p.txn, descID, user, stmt, event)
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
	event.CommonDetails().Timestamp = txn.ReadTimestamp().GoTime()
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
		event)
}

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
	stmt string,
	event eventpb.EventPayload,
) error {
	// Inject the common fields into the payload provided by the caller.
	event.CommonDetails().Timestamp = txn.ReadTimestamp().GoTime()
	sqlCommon, ok := event.(eventpb.EventWithCommonSQLPayload)
	if !ok {
		return errors.AssertionFailedf("unknown event type: %T", event)
	}
	m := sqlCommon.CommonSQLDetails()
	m.Statement = stmt
	m.User = user.Normalized()
	m.DescriptorID = uint32(descID)

	// Delegate the storing of the event to the regular event logic.
	return InsertEventRecord(ctx, execCfg.InternalExecutor,
		txn,
		int32(descID),
		int32(execCfg.NodeID.SQLInstanceID()),
		false, /* skipExternalLog */
		event)
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
func InsertEventRecord(
	ctx context.Context,
	ex *InternalExecutor,
	txn *kv.Txn,
	targetID, reportingID int32,
	skipExternalLog bool,
	info eventpb.EventPayload,
) error {
	eventType := eventpb.GetEventTypeName(info)

	// Ensure the type field is populated.
	info.CommonDetails().EventType = eventType

	// The caller is responsible for the timestamp field.
	var zeroTime time.Time
	if info.CommonDetails().Timestamp == zeroTime {
		return errors.AssertionFailedf("programming error: timestamp field in event not populated: %T", info)
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
		info.CommonDetails().Timestamp,
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
