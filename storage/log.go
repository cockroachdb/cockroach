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

package storage

import (
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util"
)

// RangeEventLogType describes a specific event type recorded in the range log
// table.
type RangeEventLogType string

const (
	// RangeEventLogSplit is the event type recorded when a range splits.
	RangeEventLogSplit RangeEventLogType = "split"
)

// rangeEventTableSchema describes the schema of the event log table.  It is
// currently envisioned as a wide table; many different event types can be
// recorded to the table.
const rangeEventTableSchema = `
CREATE TABLE rangelog.event (
  timestamp     TIMESTAMP  NOT NULL,
  rangeID       INT        NOT NULL,
  eventType     STRING     NOT NULL,
  storeID       INT        NOT NULL,
  otherRangeID  INT,
  PRIMARY KEY (timestamp, rangeID)
);`

type rangeLogEvent struct {
	timestamp    time.Time
	rangeID      roachpb.RangeID
	eventType    RangeEventLogType
	storeID      roachpb.StoreID
	otherRangeID *roachpb.RangeID
	info         string
}

func (s *Store) insertRangeLogEvent(txn *client.Txn, event rangeLogEvent) error {
	const insertEventTableStmt = `
INSERT INTO rangelog.event (
  timestamp, rangeID, eventType, storeID, otherRangeID
)
VALUES(
  $1, $2, $3, $4, $5
)
`
	args := make([]interface{}, 5)
	args[0] = event.timestamp // timestamp
	args[1] = event.rangeID   // rangeID
	args[2] = event.eventType // eventType
	args[3] = event.storeID   // storeID
	if event.otherRangeID != nil {
		args[4] = *event.otherRangeID // otherRangeID
	}

	rows, err := s.ctx.SQLExecutor.ExecuteStatementInTransaction(txn, insertEventTableStmt, args...)
	if err != nil {
		return err
	}
	if rows != 1 {
		return util.Errorf("failed to insert range event log; execute returned zero rows effected.")
	}
	return nil
}

// AddEventLogToMetadataSchema adds the range event log table to the supplied
// MetadataSchema.
func AddEventLogToMetadataSchema(schema *sql.MetadataSchema) {
	allPrivileges := sql.NewPrivilegeDescriptor(security.RootUser, privilege.List{privilege.ALL})
	db := sql.MakeMetadataDatabase("rangelog", allPrivileges)
	db.AddTable(rangeEventTableSchema, allPrivileges)
	schema.AddDatabase(db)
}

// logSplit logs a range split event into the event table. The affected range is
// the range which previously existed and is being split in half; the "other"
// range is the new range which is being created.
func (s *Store) logSplit(txn *client.Txn, updated, new roachpb.RangeDescriptor) error {
	if !s.ctx.LogRangeEvents {
		return nil
	}
	return s.insertRangeLogEvent(txn, rangeLogEvent{
		timestamp:    txn.Proto.Timestamp.GoTime(),
		rangeID:      updated.RangeID,
		eventType:    RangeEventLogSplit,
		storeID:      s.StoreID(),
		otherRangeID: &new.RangeID,
	})
}
