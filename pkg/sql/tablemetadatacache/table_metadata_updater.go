// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tablemetadatacache

import (
	"bytes"
	"context"
	gojson "encoding/json"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const pruneBatchSize = 512

// tableMetadataUpdater encapsulates the logic for updating the table metadata cache.
type tableMetadataUpdater struct {
	ie isql.Executor
	// upsertQuery is the query used to upsert table metadata rows,
	// it is reused for each batch to avoid allocations between batches.
	upsertQuery *tableMetadataBatchUpsertQuery
}

// newTableMetadataUpdater creates a new tableMetadataUpdater.
func newTableMetadataUpdater(ie isql.Executor) *tableMetadataUpdater {
	return &tableMetadataUpdater{ie: ie, upsertQuery: newTableMetadataBatchUpsertQuery(tableBatchSize)}
}

// updateCache performs a full update of the system.table_metadata, returning
// the number of rows updated and the last error encountered.
func (u *tableMetadataUpdater) updateCache(ctx context.Context) (updated int, err error) {
	it := newTableMetadataBatchIterator(u.ie)

	for {
		more, err := it.fetchNextBatch(ctx, tableBatchSize)
		if err != nil {
			// TODO (xinhaoz): add error handling for a batch failure in
			// https://github.com/cockroachdb/cockroach/issues/130040. For now,
			// we can't continue because the page key is in an invalid state.
			log.Errorf(ctx, "failed to fetch next batch: %s", err.Error())
			return updated, err
		}

		if !more {
			// No more results.
			break
		}

		count, err := u.upsertBatch(ctx, it.batchRows)
		if err != nil {
			// If an upsert fails, let's just continue to the next batch for now.
			log.Errorf(ctx, "failed to upsert batch of size: %d,  err: %s", len(it.batchRows), err.Error())
			continue
		}
		updated += count
	}

	return updated, err
}

// pruneCache deletes entries in the system.table_metadata that are not
// present in system.namespace, using batched deletions with a batch size
// of pruneBatchSize.
func (u *tableMetadataUpdater) pruneCache(ctx context.Context) (removed int, err error) {
	for {
		rowsAffected, err := u.ie.ExecEx(
			ctx,
			"prune-table-metadata",
			nil, // txn
			sessiondata.NodeUserWithLowUserPrioritySessionDataOverride, `
DELETE FROM system.table_metadata
WHERE table_id IN (
  SELECT table_id
  FROM system.table_metadata
  WHERE table_id NOT IN (
    SELECT id FROM system.namespace
  )
  LIMIT $1
)
RETURNING table_id`, pruneBatchSize)

		if err != nil {
			return 0, err
		}

		if rowsAffected == 0 {
			// No more rows to delete
			break
		}

		removed += rowsAffected
	}

	return removed, nil
}

// upsertBatch upserts the given batch of table metadata rows returning
// the number of rows upserted and any error that occurred.
func (u *tableMetadataUpdater) upsertBatch(
	ctx context.Context, batch []tableMetadataIterRow,
) (int, error) {
	u.upsertQuery.resetForBatch()

	upsertBatchSize := 0
	for _, row := range batch {
		if err := u.upsertQuery.addRow(&row); err != nil {
			log.Errorf(ctx, "failed to add row to upsert batch: %s", err.Error())
			continue
		}
		upsertBatchSize++
	}

	if upsertBatchSize == 0 {
		return 0, nil
	}

	return u.ie.ExecEx(
		ctx,
		"batch-upsert-table-metadata",
		nil, // txn
		sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
		u.upsertQuery.getQuery(),
		u.upsertQuery.args...,
	)
}

type tableMetadataBatchUpsertQuery struct {
	stmt bytes.Buffer
	args []interface{}
}

// newTableMetadataBatchUpsertQuery creates a new tableMetadataBatchUpsertQuery,
// which expects the given number of rows to be added.
func newTableMetadataBatchUpsertQuery(batchLen int) *tableMetadataBatchUpsertQuery {
	q := &tableMetadataBatchUpsertQuery{
		args: make([]interface{}, 0, batchLen*iterCols),
	}
	q.resetForBatch()
	return q
}

// resetForBatch resets the query to its initial state to be reused for
// another batch.
func (q *tableMetadataBatchUpsertQuery) resetForBatch() {
	q.stmt.Reset()
	q.stmt.WriteString(`
UPSERT INTO system.table_metadata (
	db_id,
	table_id,
	db_name,
  schema_name,
	table_name,
	total_columns,
	total_indexes,
	store_ids,
	replication_size_bytes,
	total_ranges,
	total_live_data_bytes,
	total_data_bytes,
	perc_live_data,
  last_updated
) VALUES
`)
	q.args = q.args[:0]
}

// addRow adds a tableMetadataIterRow to the batch upsert query.
func (q *tableMetadataBatchUpsertQuery) addRow(row *tableMetadataIterRow) error {
	var stats roachpb.SpanStats
	if err := gojson.Unmarshal(row.spanStatsJSON, &stats); err != nil {
		log.Errorf(context.Background(), "failed to decode span stats: %v", err)
	}

	livePercentage := float64(0)
	total := stats.TotalStats.Total()
	if total > 0 {
		livePercentage = float64(stats.ApproximateTotalStats.LiveBytes) / float64(total)
	}

	// TODO (xinhaoz): Get store ids from span stats after
	// https://github.com/cockroachdb/cockroach/issues/129060 is complete.
	storeIds := make([]int, 0)

	args := []interface{}{
		row.dbID,                              // db_id
		row.tableID,                           // table_id
		row.dbName,                            // db_name
		row.schemaName,                        // schema_name,
		row.tableName,                         // table_name
		row.columnCount,                       // total_columns
		row.indexCount,                        // total_indexes
		storeIds,                              // storeIds
		stats.ApproximateDiskBytes,            // replication_size_bytes
		stats.RangeCount,                      // total_ranges
		stats.ApproximateTotalStats.LiveBytes, // total_live_data_bytes
		total,                                 // total_data_bytes
		livePercentage,                        // perc_live_data
	}

	if len(q.args) > 0 {
		q.stmt.WriteString(", ")
	}

	q.stmt.WriteString("(")
	for i, a := range args {
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

func (q *tableMetadataBatchUpsertQuery) getQuery() string {
	return q.stmt.String()
}
