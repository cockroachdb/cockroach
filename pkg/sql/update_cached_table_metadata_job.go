// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type tableMetadataUpsertQuery struct {
	stmt     bytes.Buffer
	argCount int
	args     []interface{}
}

// newTableMetadataUpsertQuery creates a new tableMetadataUpsertQuery,
// which expects the given number of rows to be added.
// TODO (xinhaoz): can probably do the entire query setup in this method instead
// of using addRow. We don't really need to edit the query after it's created since
// we have the entire batch.
func newTableMetadataUpsertQuery(it *tableMetadataBatchIterator) *tableMetadataUpsertQuery {
	q := &tableMetadataUpsertQuery{}
	q.args = make([]interface{}, 0, it.batchLen*iterCols)
	q.stmt.WriteString(`
UPSERT INTO system.table_metadata (
	db_id,
	db_name,
	table_id,
	table_name,
	replication_size_bytes, 
	total_ranges,
	total_live_data_bytes,
	total_data_bytes, 
	total_columns,
	total_indexes,
	regions,
	perc_live_data,
  last_updated
) VALUES
`)

	return q
}

func (q *tableMetadataUpsertQuery) getArgs() []interface{} {
	return q.args
}

func (q *tableMetadataUpsertQuery) addRow(row *tableMetadataBatchRow) error {
	// Convert the replicas (int array) to a JSON array string
	replicas, err := dArrayIntToDJSON(tree.MustBeDArray(row.getUniqueReplicas()))
	if err != nil {
		log.Errorf(context.Background(), "failed to convert replicas to JSON array: %v", err)
		replicas, err = tree.MakeDJSON([]interface{}{})
		if err != nil {
			// This should never happen.
			log.Error(context.Background(), err.Error())
		}
	}

	args := []interface{}{
		row.getParentID(),             // db_id
		row.getDatabaseName(),         // db_name
		row.getID(),                   // table_id
		row.getTableName(),            // table_name
		row.getApproximateDiskBytes(), // replication_size_bytes
		row.getRangeCount(),           // total_ranges
		row.getLiveBytes(),            // total_live_data_bytes
		row.getTotalBytes(),           // total_data_bytes
		row.getColumnCount(),          // total_columns
		row.getIndexCount(),           // total_indexes
		replicas,                      // replicas
		row.getLivePercentage(),       // perc_live_data
	}

	if q.argCount > 0 {
		q.stmt.WriteString(", ")
	}

	q.stmt.WriteString("(")
	for i, a := range args {
		q.argCount++
		if i > 0 {
			q.stmt.WriteString(", ")
		}
		q.stmt.WriteString(fmt.Sprintf("$%d", len(q.args)+1))
		q.args = append(q.args, a)
	}
	// Add now() as the last_updated column.
	q.stmt.WriteString(", now()")
	q.stmt.WriteString(")")

	return nil
}

func (q *tableMetadataUpsertQuery) getQuery() string {
	return q.stmt.String()
}

type tableMetadataUpdateJobResumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*tableMetadataUpdateJobResumer)(nil)

// Resume is part of the jobs.Resumer interface.
func (j *tableMetadataUpdateJobResumer) Resume(ctx context.Context, execCtxI interface{}) error {
	log.Infof(ctx, "starting table metadata update job")

	execCtx := execCtxI.(JobExecContext)
	it := newMetadataIterator(execCtx.ExecCfg())

	for {
		if ok, err := it.fetchNextBatch(ctx, 0); !ok || err != nil {
			return err
		}
		if it.batchLen == 0 {
			break
		}
		q := newTableMetadataUpsertQuery(it)

		for i := range it.batchLen {
			log.Infof(ctx, "table metadata: %v", it.batchRows[i])
			err := q.addRow(&it.batchRows[i])
			if err != nil {
				log.Errorf(ctx, "failed to add row to batch: %v", err)
			}
		}

		_, err := execCtx.ExecCfg().InternalDB.Executor().ExecEx(
			ctx,
			"batch-upsert-table-metadata",
			nil, // txn
			sessiondata.NodeUserSessionDataOverride,
			q.getQuery(),
			q.getArgs()...,
		)
		if err != nil {
			log.Errorf(ctx, "failed to upsert batch of table metadata: %v", err)
			return err
		}
	}

	return nil
}

// OnFailOrCancel implements jobs.Resumer.
func (j *tableMetadataUpdateJobResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	// TODO (xinhaoz): Implement this.
	return nil
}

// CollectProfile implements jobs.Resumer.
func (j *tableMetadataUpdateJobResumer) CollectProfile(
	ctx context.Context, execCtx interface{},
) error {
	// TODO (xinhaoz): Implement this.
	return nil
}

func dArrayIntToDJSON(dArray *tree.DArray) (tree.Datum, error) {
	arr := make([]interface{}, len(dArray.Array))
	for i := range len(dArray.Array) {
		arr[i] = int64(tree.MustBeDInt(dArray.Array[i]))
	}
	return tree.MakeDJSON(arr)
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeUpdateCachedTableMetadata, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &tableMetadataUpdateJobResumer{
			job: job,
		}
	}, jobs.DisablesTenantCostControl)
}
