// Copyright 2015 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/privilege"
)

// EventLogType represents an event type that can be recorded in the event log.
type EventLogType string

const (
	// EventLogCreateTable is recorded when a table is created.
	EventLogCreateTable EventLogType = "create_table"
)

// eventTableSchema describes the schema of the event log table.
const eventTableSchema = `
CREATE TABLE system.eventlog (
  timestamp    TIMESTAMP  NOT NULL,
  eventType    STRING     NOT NULL,
  effectingID  INT        NOT NULL,
  reportingID  INT        NOT NULL,
  info         STRING,
  uniqueID     INT        DEFAULT experimental_unique_int(),
  PRIMARY KEY (timestamp, uniqueID)
);`

// AddEventLogToMetadataSchema adds the event log table to the supplied
// MetadataSchema.
func AddEventLogToMetadataSchema(schema *MetadataSchema) {
	schema.AddTable(keys.EventLogTableID, eventTableSchema, privilege.List{privilege.ALL})
}

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

// insertEventRecord inserts a single event into the event log as part of the
// provided transaction.
func (ev EventLogger) insertEventRecord(txn *client.Txn, eventType EventLogType, effectingID, reportingID int32, info interface{}) *roachpb.Error {
	const insertEventTableStmt = `
INSERT INTO system.eventlog (
  timestamp, eventType, effectingID, reportingID, info
)
VALUES(
  $1, $2, $3, $4, $5
)
`
	args := []interface{}{
		ev.selectEventTimestamp(txn.Proto.Timestamp),
		eventType,
		effectingID,
		reportingID,
		nil, // info
	}
	if info != nil {
		infoBytes, err := json.Marshal(info)
		if err != nil {
			return roachpb.NewError(err)
		}
		args[4] = string(infoBytes)
	}

	rows, err := ev.ExecuteStatementInTransaction(txn, insertEventTableStmt, args...)
	if err != nil {
		return err
	}
	if rows != 1 {
		return roachpb.NewErrorf("%d rows affected by log insertion; expected exactly one row affected.", rows)
	}
	return nil
}

// selectEventTimestamp selects a timestamp for this log message. If the
// transaction this event is being written in has a non-zero timestamp, then that
// timestamp should be used; otherwise, the store's physical clock is used.
// This helps with testing; in normal usage, the logging of an event will never
// be the first action in the transaction, and thus the transaction will have an
// assigned database timestamp. However, in the case of our tests log events
// *are* the first action in a transaction, and we must elect to use the store's
// physical time instead.
func (ev EventLogger) selectEventTimestamp(input roachpb.Timestamp) time.Time {
	if input == roachpb.ZeroTimestamp {
		return ev.LeaseManager.clock.PhysicalTime()
	}
	return input.GoTime()
}
