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
	"encoding/json"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/privilege"
)

// RangeEventLogType describes a specific event type recorded in the range log
// table.
type RangeEventLogType string

const (
	// RangeEventLogSplit is the event type recorded when a range splits.
	RangeEventLogSplit RangeEventLogType = "split"
)

// rangeEventTableSchema defines the schema of the event log table. It is
// currently envisioned as a wide table; many different event types can be
// recorded to the table.
const rangeEventTableSchema = `
CREATE TABLE system.rangelog (
  timestamp     TIMESTAMP  NOT NULL,
  rangeID       INT        NOT NULL,
  storeID       INT        NOT NULL,
  eventType     STRING     NOT NULL,
  otherRangeID  INT,
  info          STRING,
  PRIMARY KEY (timestamp, rangeID)
);`

type rangeLogEvent struct {
	timestamp    time.Time
	rangeID      roachpb.RangeID
	storeID      roachpb.StoreID
	eventType    RangeEventLogType
	otherRangeID *roachpb.RangeID
	info         *string
}

func (s *Store) insertRangeLogEvent(txn *client.Txn, event rangeLogEvent) *roachpb.Error {
	const insertEventTableStmt = `
INSERT INTO system.rangelog (
  timestamp, rangeID, eventType, storeID, otherRangeID, info
)
VALUES(
  $1, $2, $3, $4, $5, $6
)
`
	args := []interface{}{
		event.timestamp,
		event.rangeID,
		event.storeID,
		event.eventType,
		nil, // otherRangeID
		nil, // info
	}
	if event.otherRangeID != nil {
		args[4] = *event.otherRangeID
	}
	if event.info != nil {
		args[5] = *event.info
	}

	rows, err := s.ctx.SQLExecutor.ExecuteStatementInTransaction(txn, insertEventTableStmt, args...)
	if err != nil {
		return err
	}
	if rows != 1 {
		return roachpb.NewErrorf("%d rows affected by log insertion; expected exactly one row affected.", rows)
	}
	return nil
}

// AddEventLogToMetadataSchema adds the range event log table to the supplied
// MetadataSchema.
func AddEventLogToMetadataSchema(schema *sql.MetadataSchema) {
	schema.AddTable(keys.RangeEventTableID, rangeEventTableSchema, privilege.List{privilege.ALL})
}

// logSplit logs a range split event into the event table. The affected range is
// the range which previously existed and is being split in half; the "other"
// range is the new range which is being created.
func (s *Store) logSplit(txn *client.Txn, updatedDesc, newDesc roachpb.RangeDescriptor) *roachpb.Error {
	if !s.ctx.LogRangeEvents {
		return nil
	}
	info := struct {
		UpdatedDesc roachpb.RangeDescriptor
		NewDesc     roachpb.RangeDescriptor
	}{updatedDesc, newDesc}
	infoBytes, err := json.Marshal(info)
	if err != nil {
		return roachpb.NewError(err)
	}
	infoStr := string(infoBytes)
	return s.insertRangeLogEvent(txn, rangeLogEvent{
		timestamp:    txn.Proto.Timestamp.GoTime(),
		rangeID:      updatedDesc.RangeID,
		eventType:    RangeEventLogSplit,
		storeID:      s.StoreID(),
		otherRangeID: &newDesc.RangeID,
		info:         &infoStr,
	})
}
