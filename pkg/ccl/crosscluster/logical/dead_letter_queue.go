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
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const (
	dlqSchemaName = "crdb_replication"
	// dlqBaseTableName is defined with the following naming convention: "db.dlqSchema.schema_table"
	dlqBaseTableName   = "%s.%s.%s_%s"
	createEnumBaseStmt = `CREATE TYPE IF NOT EXISTS %s.%s.mutation_type AS ENUM (
			'insert', 'update', 'delete'
	)`
	createSchemaBaseStmt = `CREATE SCHEMA IF NOT EXISTS %s.%s`
	createTableBaseStmt  = `CREATE TABLE IF NOT EXISTS %s (
			id                  INT8 DEFAULT unique_rowid(),
			ingestion_job_id    INT8 NOT NULL,
  		table_id    				INT8 NOT NULL,
			dlq_timestamp     	TIMESTAMPTZ NOT NULL DEFAULT now():::TIMESTAMPTZ,
  		dlq_reason					STRING NOT NULL,
			mutation_type				%s.%s.mutation_type,       
  		key_value_bytes			BYTES NOT NULL,
			incoming_row     		JSONB,
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
	insertRowStmtFallBack = `INSERT INTO %s (
			ingestion_job_id,
			table_id,
			dlq_reason,
			mutation_type,
			key_value_bytes
		) VALUES ($1, $2, $3, $4, $5)`
)

type fullyQualifiedTableName struct {
	database string
	schema   string
	table    string
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
	var mutationType replicationMutationType
	if cdcEventRow.IsDeleted() {
		mutationType = deleteMutation
	} else {
		mutationType = insertMutation
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
	ie            isql.Executor
	tableIDToName map[int32]fullyQualifiedTableName
}

func (dlq *deadLetterQueueClient) Create(ctx context.Context) error {
	// Create a dlq table for each table to be replicated.
	for tableID, name := range dlq.tableIDToName {
		dlqTableName := fmt.Sprintf(dlqBaseTableName, name.database, dlqSchemaName, name.schema, name.table)
		createSchemaStmt := fmt.Sprintf(createSchemaBaseStmt, name.database, dlqSchemaName)
		if _, err := dlq.ie.Exec(ctx, "create-dlq-schema", nil, createSchemaStmt); err != nil {
			return errors.Wrapf(err, "failed to create crdb_replication schema in database %s", name.database)
		}

		createEnumStmt := fmt.Sprintf(createEnumBaseStmt, name.database, dlqSchemaName)
		if _, err := dlq.ie.Exec(ctx, "create-dlq-enum", nil, createEnumStmt); err != nil {
			return errors.Wrapf(err, "failed to create mutation_type enum in database %s", name.database)
		}

		createTableStmt := fmt.Sprintf(createTableBaseStmt, dlqTableName, name.database, dlqSchemaName)
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

	tableID := int32(cdcEventRow.TableID)
	qualifiedName, ok := dlq.tableIDToName[tableID]
	if !ok {
		return errors.Newf("failed to look up fully qualified name for table %d", tableID)
	}
	dlqTableName := fmt.Sprintf(dlqBaseTableName, qualifiedName.database, dlqSchemaName, qualifiedName.schema, qualifiedName.table)

	bytes, err := protoutil.Marshal(&kv)
	if err != nil {
		return errors.Wrap(err, "failed to marshal kv event")
	}

	// TODO(azhu): include update type
	var mutationType replicationMutationType
	if cdcEventRow.IsDeleted() {
		mutationType = deleteMutation
	} else {
		mutationType = insertMutation
	}

	jsonRow, err := cdcEventRow.ToJSON()
	if err != nil {
		log.Warningf(ctx, "failed to convert cdc event row to json: %v", err)
		if _, err := dlq.ie.Exec(
			ctx,
			"insert-row-into-dlq-table-fallback",
			nil, /* txn */
			fmt.Sprintf(insertRowStmtFallBack, dlqTableName),
			ingestionJobID,
			tableID,
			dlqReason.String(),
			mutationType.String(),
			bytes,
		); err != nil {
			return errors.Wrapf(err, "failed to insert row for table %s without json", dlqTableName)
		}
		return nil
	}

	if _, err := dlq.ie.Exec(
		ctx,
		"insert-row-into-dlq-table",
		nil, /* txn */
		fmt.Sprintf(insertBaseStmt, dlqTableName),
		ingestionJobID,
		tableID,
		dlqReason.String(),
		mutationType.String(),
		bytes,
		jsonRow,
	); err != nil {
		return errors.Wrapf(err, "failed to insert row for table %s", dlqTableName)
	}
	return nil
}

func InitDeadLetterQueueClient(
	ie isql.Executor, tableIDToName map[int32]fullyQualifiedTableName,
) DeadLetterQueueClient {
	return &deadLetterQueueClient{
		ie:            ie,
		tableIDToName: tableIDToName,
	}
}

func InitLoggingDeadLetterQueueClient() DeadLetterQueueClient {
	return &loggingDeadLetterQueueClient{}
}
