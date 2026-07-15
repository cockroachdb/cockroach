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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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

// DeadLetterQueueClient writes rows that could not be applied to their
// destination table into a per-table dead letter queue. Obtain one via
// LoadDeadLetterQueueClient, which reuses the DLQ tables that an earlier
// CreateDeadLetterQueue call made.
type DeadLetterQueueClient interface {
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

	log.Dev.Infof(ctx, `ingestion_job_id: %d,  
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

	// overrideBySrcID maps a source table ID to the per-op executor override that
	// authorizes a DLQ insert for that table (see buildDLQOverride). Populated by
	// resolveOverrides during construction and read-only afterward, which is what
	// lets concurrent Log calls read it without synchronization.
	overrideBySrcID map[descpb.ID]sessiondata.InternalExecutorOverride
}

// createDLQTables creates the crdb_replication schema and dlq_* table for every
// replicated table as the node user. It is idempotent (CREATE ... IF NOT
// EXISTS) so a resumed job reuses tables an earlier run created.
func (dlq *deadLetterQueueClient) createDLQTables(ctx context.Context) error {
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
	}
	return nil
}

// resolveOverrides validates that every DLQ table is node-owned (see
// validateDLQOwnership) and populates overrideBySrcID with the per-op executor
// override for each. It runs on both the create and load paths, so the writer
// processor (which only loads) still gets the ownership check.
func (dlq *deadLetterQueueClient) resolveOverrides(ctx context.Context) error {
	for srcID, dstTableMeta := range dlq.destTableBySrcID {
		schemaID, tableID, err := dlq.validateDLQOwnership(ctx, dstTableMeta)
		if err != nil {
			return errors.Wrapf(err, "resolving dead letter queue table %s",
				dstTableMeta.toDLQTableName())
		}
		dlq.overrideBySrcID[srcID] = buildDLQOverride(schemaID, tableID)
	}
	return nil
}

// validateDLQOwnership refuses to use a DLQ table that is not node-owned,
// returning the validated schema and table IDs so the caller builds the executor
// override against the same descriptors that passed the check.
//
// createDLQTables produces the table as node, but CREATE ... IF NOT EXISTS
// silently accepts a pre-existing one, and the DLQ table name is predictable. An
// attacker (the VULM-456 threat model) who pre-creates it can plant a UDF
// DEFAULT / CHECK / trigger that later Log() INSERTs would evaluate under the job
// owner's identity; that attacker owns the table, so an owner mismatch reliably
// flags it as not ours.
//
// The check is intentionally narrow: an attacker granted CREATE on the
// node-owned dlq_* table (a misconfiguration; not granted by default) could
// ALTER in poisoned surface without changing ownership. That residual risk is
// bounded because Log() runs as the job owner, not node -- worst case is
// "attacker -> job owner", not "attacker -> node".
func (dlq *deadLetterQueueClient) validateDLQOwnership(
	ctx context.Context, meta dstTableMetadata,
) (schemaID, tableID descpb.ID, err error) {
	err = dlq.db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		scDesc, tableDesc, err := lookupDLQTable(ctx, txn, meta)
		if err != nil {
			return err
		}
		owner := tableDesc.GetPrivileges().Owner()
		if !owner.IsNodeUser() {
			return errors.Newf("table is not owned by the node user (owner is %q)", owner)
		}
		schemaID, tableID = scDesc.GetID(), tableDesc.GetID()
		return nil
	})
	return schemaID, tableID, err
}

// lookupDLQTable resolves the DLQ table descriptor for the given destination
// table by its predictable name (dlq_<tableID>_<schema>_<table>), returning the
// enclosing schema descriptor as well.
func lookupDLQTable(
	ctx context.Context, txn descs.Txn, meta dstTableMetadata,
) (catalog.SchemaDescriptor, catalog.TableDescriptor, error) {
	byName := txn.Descriptors().ByName(txn.KV()).Get()
	dbDesc, err := byName.Database(ctx, meta.database)
	if err != nil {
		return nil, nil, err
	}
	scDesc, err := byName.Schema(ctx, dbDesc, dlqSchemaName)
	if err != nil {
		return nil, nil, err
	}
	tableName := fmt.Sprintf("dlq_%d_%s_%s", meta.tableID, meta.schema, meta.table)
	tableDesc, err := byName.Table(ctx, dbDesc, scDesc, tableName)
	if err != nil {
		return nil, nil, err
	}
	return scDesc, tableDesc, nil
}

// buildDLQOverride returns the executor override that lets the LDR job owner --
// who holds no grants on the node-owned DLQ table or crdb_replication schema --
// insert a DLQ row. It grants only INSERT on the table and USAGE on the schema,
// the minimum a Log() insert needs; it does not bypass RLS, as the DLQ table has
// no row-level security policies.
func buildDLQOverride(schemaID, tableID descpb.ID) sessiondata.InternalExecutorOverride {
	overrides := map[uint32]sessiondata.DescriptorOverride{
		uint32(tableID): {
			Privileges: privilege.List{privilege.INSERT}.ToBitField(),
		},
		uint32(schemaID): {
			Privileges: privilege.List{privilege.USAGE}.ToBitField(),
		},
	}
	// Leaving User unset preserves the executor's job-owner identity.
	return sessiondata.InternalExecutorOverride{DescriptorOverrides: overrides}
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

	ovr, ok := dlq.overrideBySrcID[srcTableID]
	if !ok {
		return errors.Newf("no dlq executor override for src table id %d", srcTableID)
	}

	jsonRow, err := cdcEventRow.ToJSON()
	if err != nil {
		log.Dev.Warningf(ctx, "failed to convert cdc event row to json: %v", err)
		if _, err := dlq.ie.ExecEx(
			ctx,
			"insert-row-into-dlq-table-fallback",
			nil, /* txn */
			ovr,
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

	if _, err := dlq.ie.ExecEx(
		ctx,
		"insert-row-into-dlq-table",
		nil, /* txn */
		ovr,
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

// CreateDeadLetterQueue creates the DLQ tables for every replicated table and
// validates that they are node-owned. Use it from the job coordinator; writer
// processors that Log() into the already-created tables should use
// LoadDeadLetterQueueClient instead.
func CreateDeadLetterQueue(
	ctx context.Context,
	db descs.DB,
	ie isql.Executor,
	destTableBySrcID map[descpb.ID]dstTableMetadata,
) error {
	if testingDLQ != nil {
		return nil
	}
	c := newDeadLetterQueueClient(db, ie, destTableBySrcID)
	if err := c.createDLQTables(ctx); err != nil {
		return err
	}
	// resolveOverrides also validates node ownership; the overrides it builds are
	// discarded here since only Log() (via LoadDeadLetterQueueClient) uses them.
	return c.resolveOverrides(ctx)
}

// LoadDeadLetterQueueClient returns a client for DLQ tables created by an
// earlier CreateDeadLetterQueue call. It validates that the tables exist
// and are node-owned but does not create them, so it is safe to call from every
// writer processor.
func LoadDeadLetterQueueClient(
	ctx context.Context,
	db descs.DB,
	ie isql.Executor,
	destTableBySrcID map[descpb.ID]dstTableMetadata,
) (DeadLetterQueueClient, error) {
	if testingDLQ != nil {
		return testingDLQ, nil
	}
	c := newDeadLetterQueueClient(db, ie, destTableBySrcID)
	if err := c.resolveOverrides(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

func newDeadLetterQueueClient(
	db descs.DB, ie isql.Executor, destTableBySrcID map[descpb.ID]dstTableMetadata,
) *deadLetterQueueClient {
	return &deadLetterQueueClient{
		db:               db,
		ie:               ie,
		destTableBySrcID: destTableBySrcID,
		overrideBySrcID:  make(map[descpb.ID]sessiondata.InternalExecutorOverride),
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
