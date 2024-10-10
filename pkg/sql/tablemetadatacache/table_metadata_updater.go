// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tablemetadatacache

import (
	"bytes"
	"context"
	gojson "encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	tablemetadatacacheutil "github.com/cockroachdb/cockroach/pkg/sql/tablemetadatacache/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const pruneBatchSize = 512

// tableMetadataUpdater encapsulates the logic for updating the table metadata cache.
type tableMetadataUpdater struct {
	ie             isql.Executor
	metrics        *TableMetadataUpdateJobMetrics
	updateProgress func(ctx context.Context, progress float32)
	testKnobs      *tablemetadatacacheutil.TestingKnobs
}

// tableMetadataDetails contains additional details for a table_metadata row that doesn't
// map to other columns in the system.table_metadata schema.
type tableMetadataDetails struct {
	// Whether auto stats is enabled at the table level. This
	// can be nil if not explicitly set for the table
	AutoStatsEnabled *bool `json:"auto_stats_enabled"`
	// The last time table statistics has been updated for
	// the table. This can be nil if table stats haven't been
	// updated yet.
	StatsLastUpdated *time.Time `json:"stats_last_updated"`
}

var _ tablemetadatacacheutil.ITableMetadataUpdater = &tableMetadataUpdater{}

// newTableMetadataUpdater creates a new tableMetadataUpdater.
func newTableMetadataUpdater(
	onProgressUpdated func(ctx context.Context, progress float32),
	metrics *TableMetadataUpdateJobMetrics,
	ie isql.Executor,
	testKnobs *tablemetadatacacheutil.TestingKnobs,
) *tableMetadataUpdater {
	return &tableMetadataUpdater{
		ie:             ie,
		metrics:        metrics,
		updateProgress: onProgressUpdated,
		testKnobs:      testKnobs,
	}
}

// RunUpdater implements tablemetadatacacheutil.ITableMetadataUpdater
func (u *tableMetadataUpdater) RunUpdater(ctx context.Context) error {
	u.metrics.NumRuns.Inc(1)
	sw := timeutil.NewStopWatch()
	sw.Start()
	if _, err := u.pruneCache(ctx); err != nil {
		log.Errorf(ctx, "failed to prune table metadata cache: %s", err.Error())
	}
	rowsUpdated, err := u.updateCache(ctx)
	sw.Stop()
	u.metrics.Duration.RecordValue(sw.Elapsed().Nanoseconds())
	u.metrics.UpdatedTables.Inc(int64(rowsUpdated))
	return err
}

// updateCache performs a full update of the system.table_metadata, returning
// the number of rows updated and the last error encountered.
func (u *tableMetadataUpdater) updateCache(ctx context.Context) (updated int, err error) {
	// upsertQuery is the query used to upsert table metadata rows,
	// it is reused for each batch to avoid allocations between batches.
	upsert := newTableMetadataBatchUpsertQuery(tableBatchSize)
	it := newTableMetadataBatchIterator(u.ie, u.testKnobs.GetAOSTClause())
	estimatedRowsToUpdate, err := u.getRowsToUpdateCount(ctx)
	if err != nil {
		log.Errorf(ctx, "failed to get estimated row count. err=%s", err.Error())
	}
	estimatedBatches := int(math.Ceil(float64(estimatedRowsToUpdate) / float64(tableBatchSize)))
	batchNum := 0
	for {
		more, err := it.fetchNextBatch(ctx, tableBatchSize)
		if err != nil {
			// TODO (xinhaoz): add error handling for a batch failure in
			// https://github.com/cockroachdb/cockroach/issues/130040. For now,
			// we can't continue because the page key is in an invalid state.
			log.Errorf(ctx, "failed to fetch next batch: %s", err.Error())
			u.metrics.Errors.Inc(1)
			return updated, err
		}

		if !more {
			// No more results.
			break
		}
		batchNum++
		count, err := u.upsertBatch(ctx, it.batchRows, upsert)
		if err != nil {
			// If an upsert fails, let's just continue to the next batch for now.
			log.Errorf(ctx, "failed to upsert batch of size: %d,  err: %s", len(it.batchRows), err.Error())
			u.metrics.Errors.Inc(1)
			continue
		}

		updated += count
		if batchNum == estimatedBatches {
			u.updateProgress(ctx, .99)
		} else if batchNum%batchesPerProgressUpdate == 0 && estimatedBatches > 0 {
			progress := float32(updated) / float32(estimatedRowsToUpdate)
			u.updateProgress(ctx, progress)
		}
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
			sessiondata.NodeUserWithBulkLowPriSessionDataOverride, `
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
	ctx context.Context, batch []tableMetadataIterRow, upsertQuery *tableMetadataBatchUpsertQuery,
) (int, error) {
	upsertQuery.resetForBatch()
	upsertBatchSize := 0
	for _, row := range batch {
		if err := upsertQuery.addRow(ctx, &row); err != nil {
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
		sessiondata.NodeUserWithBulkLowPriSessionDataOverride,
		upsertQuery.getQuery(),
		upsertQuery.args...,
	)
}

func (u *tableMetadataUpdater) getRowsToUpdateCount(ctx context.Context) (int, error) {
	query := fmt.Sprintf(`
SELECT count(*)::INT 
FROM system.namespace t
JOIN system.namespace d ON t."parentID" = d.id
%s
WHERE d."parentID" = 0 and t."parentSchemaID" != 0
`, u.testKnobs.GetAOSTClause())
	row, err := u.ie.QueryRow(ctx, "get-table-metadata-row-count", nil, query)
	if err != nil {
		return 0, err
	}

	return int(tree.MustBeDInt(row[0])), nil
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
	table_type,
	store_ids,
	replication_size_bytes,
	total_ranges,
	total_live_data_bytes,
	total_data_bytes,
	perc_live_data,
	details,
  last_updated
) VALUES
`)
	q.args = q.args[:0]
}

// addRow adds a tableMetadataIterRow to the batch upsert query.
func (q *tableMetadataBatchUpsertQuery) addRow(
	ctx context.Context, row *tableMetadataIterRow,
) error {
	var stats roachpb.SpanStats
	if err := gojson.Unmarshal(row.spanStatsJSON, &stats); err != nil {
		log.Errorf(ctx, "failed to decode span stats: %v", err)
	}

	livePercentage := float64(0)
	total := stats.TotalStats.Total()
	liveBytes := stats.TotalStats.LiveBytes
	if total > 0 {
		livePercentage = float64(liveBytes) / float64(total)
	}

	storeIds := make([]int, len(stats.StoreIDs))
	for i, id := range stats.StoreIDs {
		storeIds[i] = int(id)
	}

	details := tableMetadataDetails{AutoStatsEnabled: row.autoStatsEnabled, StatsLastUpdated: row.statsLastUpdated}
	detailsStr, err := gojson.Marshal(details)
	if err != nil {
		log.Errorf(ctx, "failed to encode details: %v", err)
	}
	args := []interface{}{
		row.dbID,                   // db_id
		row.tableID,                // table_id
		row.dbName,                 // db_name
		row.schemaName,             // schema_name,
		row.tableName,              // table_name
		row.columnCount,            // total_columns
		row.indexCount,             // total_indexes
		row.tableType,              // table_type
		storeIds,                   // storeIds
		stats.ApproximateDiskBytes, // replication_size_bytes
		stats.RangeCount,           // total_ranges
		liveBytes,                  // total_live_data_bytes
		total,                      // total_data_bytes
		livePercentage,             // perc_live_data
		string(detailsStr),
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
