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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const (
	dlqBaseTableName   = "%s.crdb_replication_conflict_dlq_%d"
	createEnumBaseStmt = `CREATE TYPE IF NOT EXISTS %s.crdb_replication_mutation_type AS ENUM (
			'insert', 'update', 'delete'
	)`

	createTableBaseStmt = `CREATE TABLE IF NOT EXISTS %s (
			id                  INT8 DEFAULT unique_rowid(),
			ingestion_job_id    INT8 NOT NULL,
  		table_id    				INT8 NOT NULL,
			dlq_timestamp     	TIMESTAMPTZ NOT NULL DEFAULT now():::TIMESTAMPTZ,
  		dlq_reason					STRING NOT NULL,
			mutation_type				%s.crdb_replication_mutation_type,       
  		key_value_bytes			BYTES NOT NULL,
			incoming_row     		STRING NOT NULL,
  		-- PK should be unique based on the ID, job ID and timestamp at which the 
  		-- row was written to the table.
  		-- For any table being replicated in an LDR job, there should not be rows 
  		-- where they have identical ID and were written to the table at the same 
  		-- time.
			PRIMARY KEY (ingestion_job_id, dlq_timestamp, id) USING HASH
		)`

	insertBaseStmt = `INSERT INTO %s (
			ingestion_job_id, 
			table_id, 
			dlq_reason,
			mutation_type,
			key_value_bytes,
			incoming_row
		) VALUES ($1, $2, $3, $4, $5, $6)`
)

type ReplicationMutationType int

const (
	Insert ReplicationMutationType = iota
	Delete
	Update
)

func (t ReplicationMutationType) String() string {
	switch t {
	case Insert:
		return "insert"
	case Delete:
		return "delete"
	case Update:
		return "update"
	default:
		return fmt.Sprintf("Unrecognized ReplicationMutationType(%d)", int(t))
	}
}

type DeadLetterQueueClient interface {
	Create(ctx context.Context) error

	Log(
		ctx context.Context,
		ingestionJobID int64,
		kv streampb.StreamEvent_KV,
		cdcEventRow cdcevent.Row,
		dlqReason retryEligibility,
	) error
}

type loggingDeadLetterQueueClient struct {
}

func (dlq *loggingDeadLetterQueueClient) Create(ctx context.Context) error {
	return nil
}

func (dlq *loggingDeadLetterQueueClient) Log(
	ctx context.Context,
	ingestionJobID int64,
	kv streampb.StreamEvent_KV,
	cdcEventRow cdcevent.Row,
	dlqReason retryEligibility,
) error {
	if !cdcEventRow.IsInitialized() {
		return errors.New("cdc event row not initialized")
	}

	tableID := cdcEventRow.TableID
	var mutationType ReplicationMutationType
	if cdcEventRow.IsDeleted() {
		mutationType = Delete
	} else {
		mutationType = Insert
	}

	bytes, err := protoutil.Marshal(&kv)
	if err != nil {
		return errors.Wrap(err, "failed to marshal kv event")
	}

	log.Infof(ctx, `ingestion_job_id: %d,  
		table_id: %d, 
		dlq_reason: %s, 
		mutation_type: %s,  
		key_value_bytes: %v, 
		incoming_row: %s`,
		ingestionJobID, tableID, dlqReason.String(), mutationType.String(), bytes, cdcEventRow.DebugString())
	return nil
}

type deadLetterQueueClient struct {
	ie              isql.Executor
	tableIDToPrefix map[int32]string
}

func (dlq *deadLetterQueueClient) Create(ctx context.Context) error {
	// Store unique db names to avoid executing identical enum statements.
	uniqueDbNames := make(map[string]bool)

	// Create a dlq table for each table to be replicated.
	for tableID, prefix := range dlq.tableIDToPrefix {
		tableName := fmt.Sprintf(dlqBaseTableName, prefix, tableID)
		prefixFields := strings.Split(prefix, ".")
		if len(prefixFields) != 2 {
			return errors.New("database and schema prefixes are required for dlq table creation")
		}
		dbName := prefixFields[0]
		_, ok := uniqueDbNames[dbName]
		// Run create enum statement if the current db name has not been seen before.
		if !ok {
			uniqueDbNames[dbName] = true
			createEnumStmt := fmt.Sprintf(createEnumBaseStmt, dbName)
			if _, err := dlq.ie.Exec(ctx, "create-enum", nil, createEnumStmt); err != nil {
				return errors.Wrapf(err, "failed to create crdb_replication_mutation_type enum in database %s", dbName)
			}
		}

		createTableStmt := fmt.Sprintf(createTableBaseStmt, tableName, dbName)
		if _, err := dlq.ie.Exec(ctx, "create-dlq-table", nil, createTableStmt); err != nil {
			return errors.Wrapf(err, "failed to create dlq for table %d", tableID)
		}
	}
	return nil
}

func (dlq *deadLetterQueueClient) Log(
	ctx context.Context,
	ingestionJobID int64,
	kv streampb.StreamEvent_KV,
	cdcEventRow cdcevent.Row,
	dlqReason retryEligibility,
) error {
	if !cdcEventRow.IsInitialized() {
		return errors.New("cdc event row not initialized")
	}

	bytes, err := protoutil.Marshal(&kv)
	if err != nil {
		return errors.Wrap(err, "failed to marshal kv event")
	}

	// TODO(azhu): include update type
	var mutationType ReplicationMutationType
	if cdcEventRow.IsDeleted() {
		mutationType = Delete
	} else {
		mutationType = Insert
	}

	tableID := int32(cdcEventRow.TableID)
	prefix, ok := dlq.tableIDToPrefix[tableID]
	if !ok {
		return errors.Newf("failed to look up database and schema prefixes for table %d", tableID)
	}

	dlqTableName := fmt.Sprintf(dlqBaseTableName, prefix, tableID)
	insertStmt := fmt.Sprintf(insertBaseStmt, dlqTableName)

	if _, err := dlq.ie.Exec(
		ctx,
		"insert-row-into-dlq-table",
		nil, /* txn */
		insertStmt,
		ingestionJobID,
		tableID,
		dlqReason.String(),
		mutationType.String(),
		bytes,
		cdcEventRow.DebugString(),
	); err != nil {
		return errors.Wrapf(err, "failed to insert row for table %s", prefix)
	}
	return nil
}

func InitDeadLetterQueueClient(
	ie isql.Executor, tableIDToPrefix map[int32]string,
) DeadLetterQueueClient {
	return &deadLetterQueueClient{
		ie:              ie,
		tableIDToPrefix: tableIDToPrefix,
	}
}

func InitLoggingDeadLetterQueueClient() DeadLetterQueueClient {
	return &loggingDeadLetterQueueClient{}
}
