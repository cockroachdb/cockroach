// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package logical

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TODO(azhu): Define WriteType as built-in database enum.
type WriteType int

const (
	Insert WriteType = iota
	Delete
)

func (t WriteType) String() string {
	switch t {
	case Insert:
		return "Insert"
	case Delete:
		return "Delete"
	default:
		return fmt.Sprintf("Unrecognized WriteType(%d)", int(t))
	}
}

type ConflictResolutionType int

const (
	Unresolved ConflictResolutionType = iota
	ManuallyResolved
	Applied
	Ignored
)

type DeadLetterQueueClient interface {
	Create() error

	Log(
		ctx context.Context,
		ingestionJobID int64,
		KVBatch []streampb.StreamEvent_KV,
		rp RowProcessor,
		conflictReason string,
	) error
}

type TableDeadLetterQueueClient struct {
}

// TODO(azhu): Implement after V0.
// In future iterations Create() should create a DLQ table in the database.
func (dlq *TableDeadLetterQueueClient) Create() error {
	return nil
}

// TODO(azhu): Append logs to DLQ table.
func (dlq *TableDeadLetterQueueClient) Log(
	ctx context.Context,
	ingestionJobID int64,
	KVBatch []streampb.StreamEvent_KV,
	rp RowProcessor,
	conflictReason string,
) error {
	decoder := rp.GetDecoder()

	for _, batch := range KVBatch {
		kv := batch.KeyValue
		row, err := decoder.DecodeKV(ctx, kv, cdcevent.CurrentRow, kv.Value.Timestamp, false)
		if err != nil {
			return err
		}

		var writeType string
		if row.IsDeleted() {
			writeType = Delete.String()
		} else {
			writeType = Insert.String()
		}

		tableID := row.TableID
		writeTimestamp := kv.Value.Timestamp
		isInitialScan := batch.PrevValue.RawBytes == nil
		sqlStmt := rp.GetSQLStatement(row, isInitialScan)

		dlqRowBase := `
ingestion_job_id: %d, 
table_id: %d, 
write_timestamp: %d, 
write_type: %s, 
write: %s, 
conflict_reason: %s`
		dlqRow := fmt.Sprintf(dlqRowBase, ingestionJobID, tableID, writeTimestamp, writeType, sqlStmt, conflictReason)
		log.Infof(ctx, dlqRow)
	}
	return nil
}
