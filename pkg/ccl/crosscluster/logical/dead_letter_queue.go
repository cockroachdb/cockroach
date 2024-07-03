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
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	dlqBaseTableName = "crdb_replication_conflict_dlq_%d"
	writeEnumStmt    = "CREATE TYPE write AS ENUM ('insert', 'update', 'delete')"

	createTableStmt = `CREATE TABLE IF NOT EXISTS "%s" (
			id                  INT8 DEFAULT unique_rowid(),
			ingestion_job_id    INT8 NOT NULL,
  		table_id    				INT8 NOT NULL,
			dlq_timestamp     	TIMESTAMP NOT NULL,
  		dlq_reason					STRING NOT NULL,
			write_type					write,       
  		key_value_bytes			BYTES NOT NULL,
			incoming_row     		STRING NOT NULL,
			PRIMARY KEY (ingestion_job_id, dlq_timestamp, id) USING HASH
		)`

	insertRowStmt = `INSERT INTO "%s" (
			ingestion_job_id, 
			table_id, 
			dlq_timestamp,
			dlq_reason,
			write_type,
			key_value_bytes,
			incoming_row
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
)

type WriteType int

const (
	Insert WriteType = iota
	Delete
)

func (t WriteType) String() string {
	switch t {
	case Insert:
		return "insert"
	case Delete:
		return "delete"
	default:
		return fmt.Sprintf("Unrecognized WriteType(%d)", int(t))
	}
}

type DeadLetterQueueClient interface {
	Create(
		ctx context.Context, jobExecCtx sql.JobExecContext, tableIDs []int32,
	) error

	Log(
		ctx context.Context,
		ie isql.Executor,
		ingestionJobID int64,
		kv streampb.StreamEvent_KV,
		cdcEventRow cdcevent.Row,
		dlqReason retryEligibility,
	) error
}

type deadLetterQueueClient struct {
}

func (dlq *deadLetterQueueClient) Create(
	ctx context.Context, jobExecCtx sql.JobExecContext, tableIDs []int32,
) error {
	db := jobExecCtx.ExecCfg().InternalDB.Executor()

	if _, err := db.Exec(ctx, "create-write-type-enum", nil, writeEnumStmt); err != nil {
		return errors.Wrap(err, "failed to create write enum")
	}

	// Create a dlq table for each given table.
	for _, tableID := range tableIDs {
		tableName := fmt.Sprintf(dlqBaseTableName, tableID)
		if _, err := db.Exec(ctx, "create-dlq-table", nil, fmt.Sprintf(createTableStmt, tableName)); err != nil {
			return errors.Wrapf(err, "failed to create dlq for table %s", tableName)
		}
	}
	return nil
}

func (dlq *deadLetterQueueClient) Log(
	ctx context.Context,
	ie isql.Executor,
	ingestionJobID int64,
	kv streampb.StreamEvent_KV,
	cdcEventRow cdcevent.Row,
	dlqReason retryEligibility,
) error {
	if !cdcEventRow.IsInitialized() {
		return errors.New("cdc event row not initialized")
	}

	tableID := cdcEventRow.TableID
	tableName := fmt.Sprintf(dlqBaseTableName, tableID)
	currentTime := timeutil.Now()
	bytes := kv.KeyValue.Value.RawBytes

	var writeType WriteType
	if cdcEventRow.IsDeleted() {
		writeType = Delete
	} else {
		writeType = Insert
	}

	if _, err := ie.Exec(
		ctx,
		"insert-row-into-dlq-table",
		nil, /* txn */
		fmt.Sprintf(insertRowStmt, tableName),
		ingestionJobID,
		tableID,
		currentTime,
		dlqReason.String(),
		writeType,
		bytes,
		cdcEventRow.DebugString(),
	); err != nil {
		return errors.Wrapf(err, "failed to insert row for table %d", tableID)
	}
	return nil
}

func InitDeadLetterQueueClient() DeadLetterQueueClient {
	return &deadLetterQueueClient{}
}
