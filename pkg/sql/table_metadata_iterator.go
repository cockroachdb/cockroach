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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

const (
	// tableBatchSize is the number of tables to fetch in a single batch from
	// the system tables.
	tableBatchSize = 10
	iterCols       = 12
)

type paginationKey struct {
	parentID int64
	schemaID int64
	name     string
}

// This is just a wrapper around a batch row to make
// it easier to read. Keeping the row as a tree.Datums
// makes it easier to insert into the table.
type tableMetadataBatchRow struct {
	row tree.Datums
}

func (r tableMetadataBatchRow) getID() tree.Datum {
	return r.row[0]
}

func (r tableMetadataBatchRow) getTableName() tree.Datum {
	return r.row[1]
}

func (r tableMetadataBatchRow) getParentID() tree.Datum {
	return r.row[2]
}

func (r tableMetadataBatchRow) getDatabaseName() tree.Datum {
	return r.row[3]
}

func (r tableMetadataBatchRow) getSchemaID() tree.Datum {
	return r.row[4]
}

func (r tableMetadataBatchRow) getColumnCount() tree.Datum {
	return r.row[5]
}

func (r tableMetadataBatchRow) getIndexCount() tree.Datum {
	return r.row[6]
}

func (r tableMetadataBatchRow) getRangeCount() tree.Datum {
	return r.row[7]
}

func (r tableMetadataBatchRow) getApproximateDiskBytes() tree.Datum {
	return r.row[8]
}

func (r tableMetadataBatchRow) getLiveBytes() tree.Datum {
	return r.row[9]
}

func (r tableMetadataBatchRow) getTotalBytes() tree.Datum {
	return r.row[10]
}

func (r tableMetadataBatchRow) getLivePercentage() tree.Datum {
	return r.row[11]
}

func (r tableMetadataBatchRow) getUniqueReplicas() tree.Datum {
	return r.row[12]
}

type tableMetadataBatchIterator struct {
	execCfg *ExecutorConfig
	// The last ID that was read from the iterator.
	lastID paginationKey
	// The current batch of rows.
	batchRows [tableBatchSize]tableMetadataBatchRow
	// The length of the current batch.
	batchLen int
}

func newMetadataIterator(execCfg *ExecutorConfig) *tableMetadataBatchIterator {
	return &tableMetadataBatchIterator{
		execCfg: execCfg,
		lastID: paginationKey{
			parentID: 0,
			schemaID: 0,
			name:     "",
		},
	}
}

// fetchNextBatch fetches the next batch of tables by joining
// information from:
// - system.namespace
// - system.descriptor
// - crdb_internal.tenant_span_stats
//
// Notes: some rows may not be tables and will not be useful. how to avoid this?
// It will return true if any tables were fetched, and an error if one occurred.
func (ti *tableMetadataBatchIterator) fetchNextBatch(
	ctx context.Context, batchSize int,
) (bool, error) {
	if batchSize == 0 {
		batchSize = tableBatchSize
	}

	// TODO (xinhaoz): For this prototype we include the span stats call in this
	// query because it's easy to make batched requests like this. This approach is
	// not currently tolerant to query errors.
	for {
		it, err := ti.execCfg.InternalDB.Executor().QueryIteratorEx(
			ctx,
			"fetch-table-metadata-batch",
			nil, /* txn */
			sessiondata.NodeUserSessionDataOverride, `
WITH range_info AS (
  SELECT 
    table_id,
    array_agg(DISTINCT replicas) AS unique_replicas
  FROM (
    SELECT 
      table_id::int,
      range_id,
      unnest(replicas) AS replicas
    FROM [SHOW CLUSTER RANGES WITH TABLES]
    WHERE table_id IS NOT NULL
  )
  GROUP BY table_id
),
tables AS (SELECT n.id,
                       n.name,
                       n."parentID",
                       n."parentSchemaID",
                       d.descriptor
                FROM system.namespace n
                LEFT JOIN system.descriptor d ON n.id = d.id
                WHERE n."parentID" > $1 AND
                      n."parentSchemaID" > $2 AND
                      n.name > $3
                LIMIT $4)
SELECT t.id,
       t.name,
       t."parentID",
       parent_n.name,
       t."parentSchemaID",
       json_array_length(d -> 'table' -> 'columns') as columns,
       COALESCE(json_array_length(d -> 'table' -> 'indexes'), 0) as indexes,
       ts.range_count,
       ts.approximate_disk_bytes,
       ts.live_bytes,
       ts.total_bytes,
       ts.live_percentage,
       ri.unique_replicas
FROM tables t
LEFT JOIN crdb_internal.tenant_span_stats(t."parentID", t.id) AS ts ON TRUE
LEFT JOIN range_info ri ON t.id = ri.table_id
LEFT JOIN system.namespace parent_n ON t."parentID" = parent_n.id,
crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', t.descriptor) AS d
`,
			ti.lastID.parentID, ti.lastID.schemaID, ti.lastID.name, batchSize,
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

		ti.batchLen = 0
		for ; ok; ok, err = it.Next(ctx) {
			if err != nil {
				// (xinhaoz) If an error occurs here with the iterator, should we try
				// to get the next batch?
				return ti.batchLen > 0, err
			}

			row := it.Cur()

			ti.lastID = paginationKey{
				parentID: int64(tree.MustBeDInt(row[2])),
				schemaID: int64(tree.MustBeDInt(row[4])),
				name:     string(tree.MustBeDString(row[1])),
			}

			// If the column row is NULL, this is not a table.
			if row[4] == tree.DNull {
				continue
			}

			ti.batchRows[ti.batchLen] = tableMetadataBatchRow{row: it.Cur()}
			ti.batchLen++
		}

		// We will try again if we didn't get any tables.
		if ti.batchLen > 0 {
			break
		}
	}

	return true, nil
}
