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

// TODO(mrtracy): All of this logic should probably be moved into the SQL
// package; there are going to be additional event log tables which will not be
// strongly associated with a store, and it would be better to keep event log
// tables close together in the code.

// RangeEventLogType describes a specific event type recorded in the range log
// table.
type RangeEventLogType string

const (
	// RangeEventLogSplit is the event type recorded when a range splits.
	RangeEventLogSplit RangeEventLogType = "split"
	// RangeEventLogAdd is the event type recorded when a range adds a
	// new replica.
	RangeEventLogAdd RangeEventLogType = "add"
	// RangeEventLogRemove is the event type recorded when a range removes a
	// replica.
	RangeEventLogRemove RangeEventLogType = "remove"
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
  uniqueID      INT        DEFAULT unique_rowid(),
  PRIMARY KEY (timestamp, uniqueID)
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
  timestamp, rangeID, storeID, eventType, otherRangeID, info
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

	// Update range event metrics. We do this close to the insertion of the
	// corresponding range log entry to reduce potential skew between metrics and
	// range log.
	switch event.eventType {
	case RangeEventLogSplit:
		s.metrics.rangeSplits.Inc(1)
	case RangeEventLogAdd:
		s.metrics.rangeAdds.Inc(1)
	case RangeEventLogRemove:
		s.metrics.rangeRemoves.Inc(1)
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
// TODO(mrtracy): There are several different reasons that a replica split
// could occur, and that information should be logged.
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
		timestamp:    selectEventTimestamp(s, txn.Proto.Timestamp),
		rangeID:      updatedDesc.RangeID,
		eventType:    RangeEventLogSplit,
		storeID:      s.StoreID(),
		otherRangeID: &newDesc.RangeID,
		info:         &infoStr,
	})
}

// logChange logs a replica change event, which represents a replica being added
// to or removed from a range.
// TODO(mrtracy): There are several different reasons that a replica change
// could occur, and that information should be logged.
func (s *Store) logChange(txn *client.Txn, changeType roachpb.ReplicaChangeType, replica roachpb.ReplicaDescriptor,
	desc roachpb.RangeDescriptor) *roachpb.Error {
	if !s.ctx.LogRangeEvents {
		return nil
	}

	var logType RangeEventLogType
	var infoStruct interface{}
	switch changeType {
	case roachpb.ADD_REPLICA:
		logType = RangeEventLogAdd
		infoStruct = struct {
			AddReplica  roachpb.ReplicaDescriptor
			UpdatedDesc roachpb.RangeDescriptor
		}{replica, desc}
	case roachpb.REMOVE_REPLICA:
		logType = RangeEventLogRemove
		infoStruct = struct {
			RemovedReplica roachpb.ReplicaDescriptor
			UpdatedDesc    roachpb.RangeDescriptor
		}{replica, desc}
	default:
		return roachpb.NewErrorf("unknown replica change type %s", changeType)
	}

	infoBytes, err := json.Marshal(infoStruct)
	if err != nil {
		return roachpb.NewError(err)
	}
	infoStr := string(infoBytes)
	return s.insertRangeLogEvent(txn, rangeLogEvent{
		timestamp: selectEventTimestamp(s, txn.Proto.Timestamp),
		rangeID:   desc.RangeID,
		eventType: logType,
		storeID:   s.StoreID(),
		info:      &infoStr,
	})
}

// selectEventTimestamp selects a timestamp for this log message. If the
// transaction this event is being written in has a non-zero timestamp, then that
// timestamp should be used; otherwise, the store's physical clock is used.
// This helps with testing; in normal usage, the logging of an event will never
// be the first action in the transaction, and thus the transaction will have an
// assigned database timestamp. However, in the case of our tests log events
// *are* the first action in a transaction, and we must elect to use the store's
// physical time instead.
func selectEventTimestamp(s *Store, input roachpb.Timestamp) time.Time {
	if input == roachpb.ZeroTimestamp {
		return s.Clock().PhysicalTime()
	}
	return input.GoTime()
}
