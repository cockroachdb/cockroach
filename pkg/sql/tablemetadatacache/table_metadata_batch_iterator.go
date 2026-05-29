// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tablemetadatacache

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

const (
	idIdx = iota
	tableNameIdx
	parentIDIdx
	databaseNameIdx
	schemaIDIdx
	schemaNameIdx
	columnCountIdx
	secondaryIndexCountIdx
	tableTypeIdx
	autoStatsEnabledIdx
	statsLastUpdatedIdx
	spanIdx
	primaryIndexIDIdx
	secondaryIndexIDsIdx

	// iterCols is the number of columns returned by the batch iterator.
	iterCols
)

type paginationKey struct {
	parentID int64
	schemaID int64
	name     string
}

// tableMetadataIterRow is the structured row returned by
// the batch iterator.
type tableMetadataIterRow struct {
	tableID          int
	tableName        string
	dbID             int
	dbName           string
	schemaID         int
	schemaName       string
	columnCount      int
	indexCount       int
	spanStats        roachpb.SpanStats
	tableType        string
	autoStatsEnabled *bool
	statsLastUpdated *time.Time
	// primaryIndexID is the ID of the table's primary index, or 0 if the
	// relation has no primary index (e.g., views).
	primaryIndexID descpb.IndexID
	// indexSizes maps each live index ID to its replication size in bytes,
	// derived from per-index spans in the same SpanStats batch as the
	// table-level span. Empty for relations with no indexes.
	indexSizes map[descpb.IndexID]int64
}

type tableMetadataBatchIterator struct {
	ie isql.Executor
	// codec is used to compute per-index span prefixes.
	codec keys.SQLCodec
	// The last ID that was read from the iterator.
	lastID paginationKey
	// The current batch of rows.
	batchRows []tableMetadataIterRow
	// query statement to use for retrieving batches
	queryStatement   string
	spanStatsFetcher spanStatsFetcher
	batchSize        int64
}

func newTableMetadataBatchIterator(
	ie isql.Executor,
	codec keys.SQLCodec,
	spanStatsFetcher spanStatsFetcher,
	aostClause string,
	batchSize int64,
) *tableMetadataBatchIterator {
	return &tableMetadataBatchIterator{
		batchSize:        batchSize,
		ie:               ie,
		codec:            codec,
		spanStatsFetcher: spanStatsFetcher,
		batchRows:        make([]tableMetadataIterRow, 0, batchSize),
		lastID: paginationKey{
			parentID: 1,
			schemaID: 1,
			name:     "",
		},
		queryStatement: newBatchQueryStatement(aostClause),
	}
}

// fetchNextBatch fetches the next batch of tables by joining
// information from:
// - system.namespace
// - system.descriptor
// - crdb_internal.tenant_span_stats
//
// It will return true if any tables were fetched.
func (batchIter *tableMetadataBatchIterator) fetchNextBatch(
	ctx context.Context,
) (more bool, retErr error) {
	batchSize := batchIter.batchSize
	if batchSize == 0 {
		return false, nil
	}

	batchIter.batchRows = batchIter.batchRows[:0]

	// fetch-table-metadata-batch is a query that fetches a batch of rows
	// from the system.namespace table according to the pagination key.
	// Rows are then joined with the system.descirptor table to get the
	// table descriptor metadata.
	//
	// We collect the table spans (one per row) and per-index spans (zero
	// or more per row) and issue a single SpanStats rpc via the
	// spanStatsFetcher. The response is keyed by span.String(), so the
	// table and per-index sizes are demultiplexed back to each row after
	// the RPC returns. tableSpans is kept parallel to batchRows;
	// indexSpanRefs records the (row, index) attribution for each
	// element of indexSpans.
	var tableSpans roachpb.Spans
	var indexSpans roachpb.Spans
	type indexSpanRef struct {
		rowIdx  int
		indexID descpb.IndexID
	}
	var indexSpanRefs []indexSpanRef
	var itErr error
	var exhausted bool
	for {
		// The logic in this for-loop is wrapped in an IIFE so that the iterator
		// can be closed with defer.
		func() {
			it, err := batchIter.ie.QueryIteratorEx(
				ctx,
				"fetch-table-metadata-batch",
				nil, /* txn */
				sessiondata.NodeUserWithBulkLowPriSessionDataOverride,
				batchIter.queryStatement,
				batchIter.lastID.parentID, batchIter.lastID.schemaID, batchIter.lastID.name,
				batchSize,
			)
			if err != nil {
				itErr = err
				return
			}
			defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

			// If there are no more results we've reached the end of the namespace table
			// and are done.
			ok, err := it.Next(ctx)
			if !ok || err != nil {
				itErr = err
				exhausted = !ok
				return
			}

			for ; ok; ok, err = it.Next(ctx) {
				if err != nil {
					itErr = err
					more = len(batchIter.batchRows) > 0
					return
				}

				row := it.Cur()
				if row.Len() != iterCols {
					itErr = errors.New("unexpected number of columns returned")
					return
				}

				batchIter.lastID = paginationKey{
					parentID: int64(tree.MustBeDInt(row[parentIDIdx])),
					schemaID: int64(tree.MustBeDInt(row[schemaIDIdx])),
					name:     string(tree.MustBeDString(row[tableNameIdx])),
				}

				// If the column count row is NULL, this is not a table.
				if row[columnCountIdx] == tree.DNull {
					continue
				}

				iterRow := tableMetadataIterRow{
					tableID:     int(tree.MustBeDInt(row[idIdx])),
					tableName:   string(tree.MustBeDString(row[tableNameIdx])),
					dbID:        int(tree.MustBeDInt(row[parentIDIdx])),
					dbName:      string(tree.MustBeDString(row[databaseNameIdx])),
					schemaID:    int(tree.MustBeDInt(row[schemaIDIdx])),
					schemaName:  string(tree.MustBeDString(row[schemaNameIdx])),
					columnCount: int(tree.MustBeDInt(row[columnCountIdx])),
					// Add 1 to the index count to account for the primary index.
					indexCount: int(tree.MustBeDInt(row[secondaryIndexCountIdx])) + 1,
					tableType:  string(tree.MustBeDString(row[tableTypeIdx])),
				}

				if row[autoStatsEnabledIdx] != tree.DNull {
					b := bool(tree.MustBeDBool(row[autoStatsEnabledIdx]))
					iterRow.autoStatsEnabled = &b
				}

				if row[statsLastUpdatedIdx] != tree.DNull {
					t := tree.MustBeDTimestamp(row[statsLastUpdatedIdx])
					iterRow.statsLastUpdated = &t.Time
				}

				dSpan := tree.MustBeDArray(row[spanIdx]).Array
				tableSpans = append(tableSpans, roachpb.Span{
					Key:    []byte(tree.MustBeDBytes(dSpan[0])),
					EndKey: []byte(tree.MustBeDBytes(dSpan[1])),
				})

				// Collect per-index spans for this row. Each becomes its own
				// entry in the SpanStats request, paired with an indexSpanRef
				// so the per-index size can be matched back to the right
				// (row, index) after the RPC returns. Views have no primary
				// index and no secondary indexes; sequences have a primary
				// only; tables and matviews have both.
				rowIdx := len(batchIter.batchRows)
				if row[primaryIndexIDIdx] != tree.DNull {
					iterRow.primaryIndexID = descpb.IndexID(tree.MustBeDInt(row[primaryIndexIDIdx]))
					indexSpans = append(indexSpans, indexSpan(batchIter.codec, descpb.ID(iterRow.tableID), iterRow.primaryIndexID))
					indexSpanRefs = append(indexSpanRefs, indexSpanRef{rowIdx: rowIdx, indexID: iterRow.primaryIndexID})
				}
				if row[secondaryIndexIDsIdx] != tree.DNull {
					for _, d := range tree.MustBeDArray(row[secondaryIndexIDsIdx]).Array {
						idxID := descpb.IndexID(tree.MustBeDInt(d))
						indexSpans = append(indexSpans, indexSpan(batchIter.codec, descpb.ID(iterRow.tableID), idxID))
						indexSpanRefs = append(indexSpanRefs, indexSpanRef{rowIdx: rowIdx, indexID: idxID})
					}
				}

				batchIter.batchRows = append(batchIter.batchRows, iterRow)
			}
		}()

		if itErr != nil {
			return more, itErr
		}
		if exhausted {
			return false, nil
		}
		// The namespace table contains non-table entries like types. It's possible we did not
		// encounter any table entries in this batch - in that case, let's get the next one.
		if len(batchIter.batchRows) > 0 {
			break
		}
	}

	// Collect the span stats for the tables and indexes in the batch in
	// one round trip. Table-level spans come first, then per-index spans;
	// the response is keyed by span.String() so order doesn't matter for
	// demultiplexing.
	allSpans := make(roachpb.Spans, 0, len(tableSpans)+len(indexSpans))
	allSpans = append(allSpans, tableSpans...)
	allSpans = append(allSpans, indexSpans...)
	res, err := batchIter.spanStatsFetcher.SpanStats(ctx, &roachpb.SpanStatsRequest{
		Spans:  allSpans,
		NodeID: "0", // Fan out.
		// Tablemetadata does not use the ApproximateTotalStats field, so we can skip the
		// nodes making additional RangeStats RPCs to collect the MVCC stats across all replicas.
		SkipApproxTotalStats: true,
	})

	if err != nil {
		return true, err
	}

	if len(res.Errors) > 0 {
		log.Dev.Errorf(ctx, "SpanStats request completed with %d errors. errors=%q",
			len(res.Errors), res.Errors)
		// For now, we won't write partial results to the cache.
		return true, errors.New("An error has occurred while fetching span stats.")
	}

	if res.SpanToStats != nil {
		for i, row := range batchIter.batchRows {
			spanStats := res.SpanToStats[tableSpans[i].String()]
			if spanStats == nil {
				continue
			}
			row.spanStats = *spanStats
			batchIter.batchRows[i] = row
		}
		for i, ref := range indexSpanRefs {
			spanStats := res.SpanToStats[indexSpans[i].String()]
			if spanStats == nil {
				continue
			}
			row := &batchIter.batchRows[ref.rowIdx]
			if row.indexSizes == nil {
				row.indexSizes = make(map[descpb.IndexID]int64)
			}
			row.indexSizes[ref.indexID] = int64(spanStats.ApproximateDiskBytes)
		}
	}

	return true, nil
}

// indexSpan returns the span containing the keys of the given index.
func indexSpan(codec keys.SQLCodec, tableID descpb.ID, indexID descpb.IndexID) roachpb.Span {
	start := roachpb.Key(rowenc.MakeIndexKeyPrefix(codec, tableID, indexID))
	return roachpb.Span{Key: start, EndKey: start.PrefixEnd()}
}

// newBatchQueryStatement creates a query statement to fetch batches of table metadata to insert into
// system.table_metadata.
func newBatchQueryStatement(aostClause string) string {
	return fmt.Sprintf(`
WITH cte (n_id, n_name, "n_parentID", db_name, "n_parentSchemaID", schema_name, d) AS (
   SELECT
       n.id,
       n.name,
       n."parentID",
       db_name.name,
       n."parentSchemaID",
       schema_name.name,
       crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', enc_desc.descriptor)
   FROM system.namespace n
   JOIN system.descriptor enc_desc ON n.id = enc_desc.id
   JOIN system.namespace db_name ON n."parentID" = db_name.id AND db_name."parentID" = 0
   JOIN system.namespace schema_name ON n."parentSchemaID" = schema_name.id AND schema_name."parentID" = n."parentID"
   WHERE (n."parentID", n."parentSchemaID", n.name) > ($1, $2, $3)
     AND n."parentSchemaID" != 0
   ORDER BY n."parentID", n."parentSchemaID", n.name
   LIMIT $4
)
SELECT
    n_id AS id,
    n_name AS name,
    "n_parentID" AS "parentID",
    db_name,
    "n_parentSchemaID" AS "parentSchemaID",
    schema_name,
    json_array_length(d->'table' -> 'columns') as columns,
    COALESCE(json_array_length(d->'table' -> 'indexes'), 0) as indexes,
    CASE
        WHEN d->'table'->>'isMaterializedView' = 'true' THEN 'MATERIALIZED_VIEW'
        WHEN d->'table'->>'viewQuery' IS NOT NULL THEN 'VIEW'
        WHEN d->'table'->'sequenceOpts' IS NOT NULL THEN 'SEQUENCE'
        ELSE 'TABLE'
    END as table_type,
    (d->'table'->'autoStatsSettings'->>'enabled')::BOOL as auto_stats_enabled,
    (
        SELECT max("createdAt") as stats_last_updated
        FROM system.table_statistics
        WHERE "tableID" = n_id
        GROUP BY "tableID"
    ),
    crdb_internal.table_span(n_id) as span,
    (d->'table'->'primaryIndex'->>'id')::INT as primary_index_id,
    (
        SELECT array_agg((idx->>'id')::INT)
        FROM jsonb_array_elements(d->'table'->'indexes') idx
    ) as secondary_index_ids
FROM
    cte
%[1]s
`, aostClause)
}
