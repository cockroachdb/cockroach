// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Tracy (matt@cockroachlabs.com)

package sql

import (
	"encoding/json"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// EventLogType represents an event type that can be recorded in the event log.
type EventLogType string

// NOTE: When you add a new event type here. Please manually add it to
// ui/app/util/eventTypes.ts so that it will be recognized in the UI.
const (
	// EventLogCreateDatabase is recorded when a database is created.
	EventLogCreateDatabase EventLogType = "create_database"
	// EventLogDropDatabase is recorded when a database is dropped.
	EventLogDropDatabase EventLogType = "drop_database"

	// EventLogCreateTable is recorded when a table is created.
	EventLogCreateTable EventLogType = "create_table"
	// EventLogDropTable is recorded when a table is dropped.
	EventLogDropTable EventLogType = "drop_table"
	// EventLogAlterTable is recorded when a table is altered.
	EventLogAlterTable EventLogType = "alter_table"

	// EventLogCreateIndex is recorded when an index is created.
	EventLogCreateIndex EventLogType = "create_index"
	// EventLogDropIndex is recorded when an index is dropped.
	EventLogDropIndex EventLogType = "drop_index"

	// EventLogCreateView is recorded when a view is created.
	EventLogCreateView EventLogType = "create_view"
	// EventLogDropView is recorded when a view is dropped.
	EventLogDropView EventLogType = "drop_view"

	// EventLogReverseSchemaChange is recorded when an in-progress schema change
	// encounters a problem and is reversed.
	EventLogReverseSchemaChange EventLogType = "reverse_schema_change"
	// EventLogFinishSchemaChange is recorded when a previously initiated schema
	// change has completed.
	EventLogFinishSchemaChange EventLogType = "finish_schema_change"

	// EventLogNodeJoin is recorded when a node joins the cluster.
	EventLogNodeJoin EventLogType = "node_join"
	// EventLogNodeRestart is recorded when an existing node rejoins the cluster
	// after being offline.
	EventLogNodeRestart EventLogType = "node_restart"
)

// An EventLogger exposes methods used to record events to the event table.
type EventLogger struct {
	InternalExecutor
}

// MakeEventLogger constructs a new EventLogger. A LeaseManager is required in
// order to correctly execute SQL statements.
func MakeEventLogger(leaseMgr *LeaseManager) EventLogger {
	return EventLogger{InternalExecutor{
		LeaseManager: leaseMgr,
	}}
}

// InsertEventRecord inserts a single event into the event log as part of the
// provided transaction.
func (ev EventLogger) InsertEventRecord(
	ctx context.Context,
	txn *client.Txn,
	eventType EventLogType,
	targetID, reportingID int32,
	info interface{},
) error {
	// Record event record insertion in local log output.
	txn.AddCommitTrigger(func() {
		log.Infof(
			ctx, "Event: %q, target: %d, info: %+v",
			eventType,
			targetID,
			info,
		)
	})

	const insertEventTableStmt = `
INSERT INTO system.eventlog (
  timestamp, eventType, targetID, reportingID, info
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

	rows, err := ev.ExecuteStatementInTransaction(
		ctx, "log-event", txn, insertEventTableStmt, args...)
	if err != nil {
		return err
	}
	if rows != 1 {
		return errors.Errorf("%d rows affected by log insertion; expected exactly one row affected.", rows)
	}
	return nil
}
