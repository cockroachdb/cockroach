// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tablemetadatacache

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

const (
	// tableBatchSize is the number of tables to fetch in a
	// single batch from the system tables.
	tableBatchSize = 20
)

const (
	idIdx = iota
	tableNameIdx
	parentIDIdx
	databaseNameIdx
	schemaIDIdx
	schemaNameIdx
	columnCountIdx
	indexCountIdx
	spanStatsIdx
	tableTypeIdx
	autoStatsEnabledIdx
	statsLastUpdatedIdx

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
	spanStatsJSON    []byte
	tableType        string
	autoStatsEnabled *bool
	statsLastUpdated *time.Time
}

type tableMetadataBatchIterator struct {
	ie isql.Executor
	// The last ID that was read from the iterator.
	lastID paginationKey
	// The current batch of rows.
	batchRows []tableMetadataIterRow
	// query statement to use for retrieving batches
	queryStatement string
}

func newTableMetadataBatchIterator(
	ie isql.Executor, aostClause string,
) *tableMetadataBatchIterator {
	return &tableMetadataBatchIterator{
		ie:        ie,
		batchRows: make([]tableMetadataIterRow, 0, tableBatchSize),
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
	ctx context.Context, batchSize int,
) (bool, error) {
	if batchSize == 0 {
		return false, nil
	}

	batchIter.batchRows = batchIter.batchRows[:0]

	// fetch-table-metadata-batch is a query that fetches a batch of rows
	// from the system.namespace table according to the pagination key.
	// We collect the table spans for the tables in the batch and issue
	// a single SpanStats rpc via crdb_internal.tenant_span_stats to get the
	// span stats for all the tables in the batch.
	// Rows are then joined with the system.descirptor table to get the
	// table descriptor metadata.
	for {
		it, err := batchIter.ie.QueryIteratorEx(
			ctx,
			"fetch-table-metadata-batch",
			nil, /* txn */
			sessiondata.NodeUserWithBulkLowPriSessionDataOverride, batchIter.queryStatement,
			batchIter.lastID.parentID, batchIter.lastID.schemaID, batchIter.lastID.name, batchSize,
		)
		if err != nil {
			return false, err
		}

		// If there are no more results we've reached the end of the namespace table
		// and are done.
		ok, err := it.Next(ctx)
		if !ok || err != nil {
			return false, err
		}

		for ; ok; ok, err = it.Next(ctx) {
			if err != nil {
				return len(batchIter.batchRows) > 0, err
			}

			row := it.Cur()
			if row.Len() != iterCols {
				return false, errors.New("unexpected number of columns returned")
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
				tableID:       int(tree.MustBeDInt(row[idIdx])),
				tableName:     string(tree.MustBeDString(row[tableNameIdx])),
				dbID:          int(tree.MustBeDInt(row[parentIDIdx])),
				dbName:        string(tree.MustBeDString(row[databaseNameIdx])),
				schemaID:      int(tree.MustBeDInt(row[schemaIDIdx])),
				schemaName:    string(tree.MustBeDString(row[schemaNameIdx])),
				columnCount:   int(tree.MustBeDInt(row[columnCountIdx])),
				indexCount:    int(tree.MustBeDInt(row[indexCountIdx])),
				spanStatsJSON: []byte(tree.MustBeDJSON(row[spanStatsIdx]).JSON.String()),
				tableType:     string(tree.MustBeDString(row[tableTypeIdx])),
			}

			if row[autoStatsEnabledIdx] != tree.DNull {
				b := bool(tree.MustBeDBool(row[autoStatsEnabledIdx]))
				iterRow.autoStatsEnabled = &b
			}

			if row[statsLastUpdatedIdx] != tree.DNull {
				t := tree.MustBeDTimestamp(row[statsLastUpdatedIdx])
				iterRow.statsLastUpdated = &t.Time
			}

			batchIter.batchRows = append(batchIter.batchRows, iterRow)
		}

		// The namespace table contains non-table entries like types. It's possible we did not
		// encounter any table entries in this batch - in that case, let's get the next one.
		if len(batchIter.batchRows) > 0 {
			break
		}
	}

	return len(batchIter.batchRows) > 0, nil
}

// newBatchQueryStatement creates a query statement to fetch batches of table metadata to insert into
// system.table_metadata.
func newBatchQueryStatement(aostClause string) string {
	return fmt.Sprintf(`
WITH tables AS (SELECT n.id,
                       n.name,
                       n."parentID",
                       n."parentSchemaID",
                       d.descriptor,
                       crdb_internal.table_span(n.id) as span
                FROM system.namespace n
                JOIN system.descriptor d ON n.id = d.id
								%[1]s
                WHERE (n."parentID", n."parentSchemaID", n.name) > ($1, $2, $3) AND n."parentSchemaID" != 0
                ORDER BY (n."parentID", n."parentSchemaID", n.name)
                LIMIT $4),
span_array AS (SELECT array_agg((span[1], span[2])) as all_spans FROM tables),
span_stats AS (SELECT stats, t.id FROM crdb_internal.tenant_span_stats((SELECT all_spans FROM span_array)) s
               JOIN tables t on t.span[1] = s.start_key and t.span[2] = s.end_key)
SELECT t.id,
       t.name,
       t."parentID",
       db_name.name,
       t."parentSchemaID",
       schema_name.name,
       json_array_length(d -> 'table' -> 'columns') as columns,
       COALESCE(json_array_length(d -> 'table' -> 'indexes'), 0),
       s.stats,
			 CASE
			 		WHEN d->'table'->>'isMaterializedView' = 'true' THEN 'MATERIALIZED_VIEW'
			 		WHEN d->'table'->>'viewQuery' IS NOT NULL THEN 'VIEW'
			 		WHEN d->'table'->'sequenceOpts' IS NOT NULL THEN 'SEQUENCE'
			 		ELSE 'TABLE'
			 END as table_type,
			(d->'table'->'autoStatsSettings'->>'enabled')::BOOL as auto_stats_enabled,
			ts.last_updated as stats_last_updated
FROM tables t
LEFT JOIN span_stats s ON t.id = s.id
LEFT JOIN (select "tableID", max("createdAt") as last_updated from system.table_statistics group by "tableID") ts ON ts."tableID" = t.id
LEFT JOIN system.namespace db_name ON t."parentID" = db_name.id
LEFT JOIN system.namespace schema_name ON t."parentSchemaID" = schema_name.id,
crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', t.descriptor) AS d
%[1]s
ORDER BY (t."parentID", t."parentSchemaID", t.name)
`, aostClause)
}
