// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const (
	dlqSchemaName = "crdb_replication"
	// dlqBaseTableName is defined as: "<dbName>.<dlqSchemaName>.dlq_<tableID>_<schemaName>_<tableName>"
	dlqBaseTableName     = "%s.%s.%s"
	createSchemaBaseStmt = `CREATE SCHEMA IF NOT EXISTS %s.%s`
	createTableBaseStmt  = `CREATE TABLE IF NOT EXISTS %s (
			id                  INT8 DEFAULT unique_rowid(),
			ingestion_job_id    INT8 NOT NULL,
  		table_id    				INT8 NOT NULL,
			dlq_timestamp     	TIMESTAMPTZ NOT NULL DEFAULT now():::TIMESTAMPTZ,
  		dlq_reason					STRING NOT NULL,
			mutation_type				STRING NOT NULL,       
  		key_value_bytes			BYTES NOT NULL NOT VISIBLE,
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

type dstTableMetadata struct {
	database string
	schema   string
	table    string
	tableID  descpb.ID
}

func (f dstTableMetadata) getDatabaseName() string {
	return lexbase.EscapeSQLIdent(f.database)
}

func (f dstTableMetadata) toDLQTableName() string {
	return fmt.Sprintf(dlqBaseTableName,
		f.getDatabaseName(),
		dlqSchemaName,
		lexbase.EscapeSQLIdent(fmt.Sprintf("dlq_%d_%s_%s", f.tableID, f.schema, f.table)))
}

type DeadLetterQueueClient interface {
	Create(ctx context.Context) error

	Log(
		ctx context.Context,
		ingestionJobID int64,
		kv streampb.StreamEvent_KV,
		cdcEventRow cdcevent.Row,
		reason error,
		stoppedRetryReason retryEligibility,
	) error
}

type noopDeadLetterQueueClient struct {
}

func (dlq *noopDeadLetterQueueClient) Create(_ context.Context) error {
	return nil
}

func (dlq *noopDeadLetterQueueClient) Log(
	ctx context.Context,
	ingestionJobID int64,
	kv streampb.StreamEvent_KV,
	cdcEventRow cdcevent.Row,
	reason error,
	stoppedRetryReason retryEligibility,
) error {
	if !cdcEventRow.IsInitialized() {
		return errors.New("cdc event row not initialized")
	}

	tableID := cdcEventRow.TableID
	var mutationType string
	if cdcEventRow.IsDeleted() {
		mutationType = deleteMutation.String()
	} else {
		mutationType = insertMutation.String()
	}

	bytes, err := protoutil.Marshal(&kv)
	if err != nil {
		return errors.Wrap(err, "failed to marshal kv event")
	}

	log.Infof(ctx, `ingestion_job_id: %d,  
		table_id: %d, 
		dlq_reason: (%s) %s,
		mutation_type: %s,  
		key_value_bytes: %v, 
		incoming_row: %s`,
		ingestionJobID, tableID, reason.Error(), stoppedRetryReason.String(), mutationType, bytes, cdcEventRow.DebugString())
	return nil
}

type deadLetterQueueClient struct {
	db               descs.DB
	ie               isql.Executor
	destTableBySrcID map[descpb.ID]dstTableMetadata
}

func (dlq *deadLetterQueueClient) Create(ctx context.Context) error {
	// Create a dlq table for each table to be replicated.
	for _, dstTableMeta := range dlq.destTableBySrcID {
		dlqTableName := dstTableMeta.toDLQTableName()
		createSchemaStmt := fmt.Sprintf(createSchemaBaseStmt, dstTableMeta.getDatabaseName(), dlqSchemaName)
		if _, err := dlq.ie.Exec(ctx, "create-dlq-schema", nil, createSchemaStmt); err != nil {
			return errors.Wrapf(err, "failed to create crdb_replication schema in database %s", dstTableMeta.getDatabaseName())
		}

		createTableStmt := fmt.Sprintf(createTableBaseStmt, dlqTableName)
		if _, err := dlq.ie.Exec(ctx, "create-dlq-table", nil, createTableStmt); err != nil {
			return errors.Wrapf(err, "failed to create dlq for table %d", dstTableMeta.tableID)
		}

		// CREATE TABLE IF NOT EXISTS silently accepts a pre-existing
		// table, and the DLQ table name is predictable from the
		// destination ID. We just asked CREATE to produce the table
		// as the node user, so anything we end up with that isn't
		// node-owned is something we didn't create -- refuse to use
		// it rather than evaluate any column DEFAULTs, CHECK
		// constraints, or triggers a non-node owner might have
		// installed.
		if err := dlq.validateDLQOwnership(ctx, dstTableMeta); err != nil {
			return errors.Wrapf(err,
				"dead letter queue table %s was not created by this node; refuse to use it",
				dlqTableName)
		}
	}
	return nil
}

// validateDLQOwnership refuses to use a DLQ table that is not owned
// by the node user. Create() runs `CREATE TABLE IF NOT EXISTS` via
// the internal executor (whose default identity is node), so a
// freshly created DLQ table is always node-owned. A pre-existing
// table with the same predictable name but a different owner is
// not the one we declared and may carry column DEFAULTs, CHECK
// constraints, or triggers we did not author -- refuse it.
//
// This check is intentionally narrow: a user who has somehow been
// granted CREATE on the node-owned crdb_replication.dlq_* table (an
// explicit misconfiguration; CREATE on that schema is not granted by
// default) could ALTER the table without changing ownership, which
// this check would not catch. Bounding that further is left to the
// preceding LDR commit's identity flip: DLQ Log() now runs as the
// job owner rather than as node.
func (dlq *deadLetterQueueClient) validateDLQOwnership(
	ctx context.Context, meta dstTableMetadata,
) error {
	return dlq.db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		byName := txn.Descriptors().ByName(txn.KV()).Get()
		dbDesc, err := byName.Database(ctx, meta.database)
		if err != nil {
			return err
		}
		scDesc, err := byName.Schema(ctx, dbDesc, dlqSchemaName)
		if err != nil {
			return err
		}
		tableName := fmt.Sprintf("dlq_%d_%s_%s", meta.tableID, meta.schema, meta.table)
		tableDesc, err := byName.Table(ctx, dbDesc, scDesc, tableName)
		if err != nil {
			return err
		}
		owner := tableDesc.GetPrivileges().Owner()
		if !owner.IsNodeUser() {
			return errors.Newf("owner is %q, expected node user", owner)
		}
		return nil
	})
}

func (dlq *deadLetterQueueClient) Log(
	ctx context.Context,
	ingestionJobID int64,
	kv streampb.StreamEvent_KV,
	cdcEventRow cdcevent.Row,
	reason error,
	stoppedRetryingReason retryEligibility,
) error {
	if !cdcEventRow.IsInitialized() {
		return errors.New("cdc event row not initialized")
	}

	// TableID in cdcEventRow is the source table ID.
	srcTableID := cdcEventRow.TableID
	dstTableMeta, ok := dlq.destTableBySrcID[srcTableID]
	if !ok {
		return errors.Newf("failed to look up fully qualified name for src table id %d", srcTableID)
	}
	dlqTableName := dstTableMeta.toDLQTableName()

	bytes, err := protoutil.Marshal(&kv)
	if err != nil {
		return errors.Wrap(err, "failed to marshal kv event")
	}

	// TODO(azhu): include update type
	var mutationType string
	if cdcEventRow.IsDeleted() {
		mutationType = deleteMutation.String()
	} else {
		mutationType = insertMutation.String()
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
			dstTableMeta.tableID,
			fmt.Sprintf("%s (%s)", reason, stoppedRetryingReason),
			mutationType,
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
		dstTableMeta.tableID,
		fmt.Sprintf("%s (%s)", reason, stoppedRetryingReason),
		mutationType,
		bytes,
		jsonRow,
	); err != nil {
		return errors.Wrapf(err, "failed to insert row for table %s", dlqTableName)
	}
	return nil
}

func InitDeadLetterQueueClient(
	db descs.DB, ie isql.Executor, destTableBySrcID map[descpb.ID]dstTableMetadata,
) DeadLetterQueueClient {
	if testingDLQ != nil {
		return testingDLQ
	}
	return &deadLetterQueueClient{
		db:               db,
		ie:               ie,
		destTableBySrcID: destTableBySrcID,
	}
}

var testingDLQ DeadLetterQueueClient

// TestingSetDLQ sets the DLQ to the passed implementation, globally, until the
// returned reversion function is called.
func TestingSetDLQ(d DeadLetterQueueClient) func() {
	v := testingDLQ
	testingDLQ = d
	return func() { testingDLQ = v }
}

func InitNoopDeadLetterQueueClient() DeadLetterQueueClient {
	return &noopDeadLetterQueueClient{}
}
