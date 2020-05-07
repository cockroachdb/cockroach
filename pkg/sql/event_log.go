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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// EventLogType represents an event type that can be recorded in the event log.
type EventLogType string

// NOTE: When you add a new event type here. Please manually add it to
// pkg/ui/src/util/eventTypes.ts so that it will be recognized in the UI.
const (
	// EventLogCreateDatabase is recorded when a database is created.
	EventLogCreateDatabase EventLogType = "create_database"
	// EventLogDropDatabase is recorded when a database is dropped.
	EventLogDropDatabase EventLogType = "drop_database"

	// EventLogCreateTable is recorded when a table is created.
	EventLogCreateTable EventLogType = "create_table"
	// EventLogDropTable is recorded when a table is dropped.
	EventLogDropTable EventLogType = "drop_table"
	// EventLogTruncateTable is recorded when a table is truncated.
	EventLogTruncateTable EventLogType = "truncate_table"
	// EventLogAlterTable is recorded when a table is altered.
	EventLogAlterTable EventLogType = "alter_table"
	// EventLogCommentOnColumn is recorded when a column is commented.
	EventLogCommentOnColumn EventLogType = "comment_on_column"
	// EventLogCommentOnTable is recorded when a table is commented.
	EventLogCommentOnDatabase EventLogType = "comment_on_database"
	// EventLogCommentOnTable is recorded when a table is commented.
	EventLogCommentOnTable EventLogType = "comment_on_table"
	// EventLogCommentOnIndex is recorded when a index is commented.
	EventLogCommentOnIndex EventLogType = "comment_on_index"

	// EventLogCreateIndex is recorded when an index is created.
	EventLogCreateIndex EventLogType = "create_index"
	// EventLogDropIndex is recorded when an index is dropped.
	EventLogDropIndex EventLogType = "drop_index"
	// EventLogAlterIndex is recorded when an index is altered.
	EventLogAlterIndex EventLogType = "alter_index"

	// EventLogCreateView is recorded when a view is created.
	EventLogCreateView EventLogType = "create_view"
	// EventLogDropView is recorded when a view is dropped.
	EventLogDropView EventLogType = "drop_view"

	// EventLogCreateSequence is recorded when a sequence is created.
	EventLogCreateSequence EventLogType = "create_sequence"
	// EventLogDropSequence is recorded when a sequence is dropped.
	EventLogDropSequence EventLogType = "drop_sequence"
	// EventLogAlterSequence is recorded when a sequence is altered.
	EventLogAlterSequence EventLogType = "alter_sequence"

	// EventLogReverseSchemaChange is recorded when an in-progress schema change
	// encounters a problem and is reversed.
	EventLogReverseSchemaChange EventLogType = "reverse_schema_change"
	// EventLogFinishSchemaChange is recorded when a previously initiated schema
	// change has completed.
	EventLogFinishSchemaChange EventLogType = "finish_schema_change"
	// EventLogFinishSchemaRollback is recorded when a previously
	// initiated schema change rollback has completed.
	EventLogFinishSchemaRollback EventLogType = "finish_schema_change_rollback"

	// EventLogNodeJoin is recorded when a node joins the cluster.
	EventLogNodeJoin EventLogType = "node_join"
	// EventLogNodeRestart is recorded when an existing node rejoins the cluster
	// after being offline.
	EventLogNodeRestart EventLogType = "node_restart"
	// EventLogNodeDecommissioned is recorded when a node is marked as
	// decommissioning.
	EventLogNodeDecommissioned EventLogType = "node_decommissioned"
	// EventLogNodeRecommissioned is recorded when a decommissioned node is
	// recommissioned.
	EventLogNodeRecommissioned EventLogType = "node_recommissioned"

	// EventLogSetClusterSetting is recorded when a cluster setting is changed.
	EventLogSetClusterSetting EventLogType = "set_cluster_setting"

	// EventLogSetZoneConfig is recorded when a zone config is changed.
	EventLogSetZoneConfig EventLogType = "set_zone_config"
	// EventLogRemoveZoneConfig is recorded when a zone config is removed.
	EventLogRemoveZoneConfig EventLogType = "remove_zone_config"

	// EventLogCreateStatistics is recorded when statistics are collected for a
	// table.
	EventLogCreateStatistics EventLogType = "create_statistics"
)

// EventLogSetClusterSettingDetail is the json details for a settings change.
type EventLogSetClusterSettingDetail struct {
	SettingName string
	Value       string
	User        string
}

// An EventLogger exposes methods used to record events to the event table.
type EventLogger struct {
	*InternalExecutor
}

// MakeEventLogger constructs a new EventLogger.
func MakeEventLogger(execCfg *ExecutorConfig) EventLogger {
	return EventLogger{InternalExecutor: execCfg.InternalExecutor}
}

// InsertEventRecord inserts a single event into the event log as part of the
// provided transaction.
func (ev EventLogger) InsertEventRecord(
	ctx context.Context,
	txn *kv.Txn,
	eventType EventLogType,
	targetID, reportingID int32,
	info interface{},
) error {
	// Record event record insertion in local log output.
	txn.AddCommitTrigger(func(ctx context.Context) {
		log.Infof(
			ctx, "Event: %q, target: %d, info: %+v",
			eventType,
			targetID,
			info,
		)
	})

	const insertEventTableStmt = `
INSERT INTO system.eventlog (
  timestamp, "eventType", "targetID", "reportingID", info
)
VALUES(
  now(), $1, $2, $3, $4
)
`
	args := []interface{}{
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
		args[3] = string(infoBytes)
	}
	rows, err := ev.Exec(ctx, "log-event", txn, insertEventTableStmt, args...)
	if err != nil {
		return err
	}
	if rows != 1 {
		return errors.Errorf("%d rows affected by log insertion; expected exactly one row affected.", rows)
	}
	return nil
}
