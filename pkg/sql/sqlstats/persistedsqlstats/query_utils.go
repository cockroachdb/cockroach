// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package persistedsqlstats

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

var (
	// N.B.: statement and transaction statistics system tables has hash sharded
	//       primary index with number of buckets. A "wide scan" is a scan node
	//       that scans over the entire bucket (until the latest aggregation
	//       interval) of the hash sharded index.
	//       In the EXPLAIN output, a wide scan looks like the following:
	// 				```
	// 				Spans: /0 - /1/{some TimestampTZ indicating latest agg interval}
	// 				```
	//       Running large number of wide scans is extremely risky as its
	//       performance can degrade overtime as MVCC garbage accumulates. Hence,
	//       the StatsCompactor only issues a limited number of "wide scan"
	//       queries.
	//       In contrast, a constraint scan have a much narrower scanning span.

	deleteFromStmtStatsWithWideScan = `
		DELETE FROM system.statement_statistics
		WHERE (aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash, app_name, node_id) IN (
		  SELECT aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash, app_name, node_id
		  FROM system.statement_statistics
		  WHERE crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_plan_hash_transaction_fingerprint_id_shard_8 = $1
				AND aggregated_ts < $3
		  ORDER BY aggregated_ts ASC
		  LIMIT $2
		) RETURNING aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash, app_name, node_id`

	deleteFromTxnStatsWithWideScan = `
		DELETE FROM system.transaction_statistics
		WHERE (aggregated_ts, fingerprint_id, app_name, node_id) IN (
		  SELECT aggregated_ts, fingerprint_id, app_name, node_id
		  FROM system.transaction_statistics
		  WHERE crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_shard_8 = $1
				AND aggregated_ts < $3
		  ORDER BY aggregated_ts ASC
		  LIMIT $2
		) RETURNING aggregated_ts, fingerprint_id, app_name, node_id`

	deleteFromStmtStatsWithConstrainedScan = `
		DELETE FROM system.statement_statistics
		WHERE (aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash, app_name, node_id) IN (
		  SELECT aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash, app_name, node_id
		  FROM system.statement_statistics
		  WHERE crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_plan_hash_transaction_fingerprint_id_shard_8 = $1
		  AND (
				(
		      aggregated_ts,
					fingerprint_id,
					transaction_fingerprint_id,
		      plan_hash,
		      app_name,
		      node_id                   
				) >= ($4, $5, $6, $7, $8, $9)
		  )
				AND aggregated_ts < $3
		  ORDER BY aggregated_ts ASC
		  LIMIT $2
		) RETURNING aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash, app_name, node_id`

	deleteFromTxnStatsWithConstrainedScan = `
		DELETE FROM system.transaction_statistics
		WHERE (aggregated_ts, fingerprint_id, app_name, node_id) IN (
		  SELECT aggregated_ts, fingerprint_id, app_name, node_id
		  FROM system.transaction_statistics
		  WHERE crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_shard_8 = $1
		  AND (
				(
		      aggregated_ts,
					fingerprint_id,
		      app_name,
		      node_id                   
				) >= ($4, $5, $6, $7)
		  )
				AND aggregated_ts < $3
		  ORDER BY aggregated_ts ASC
		  LIMIT $2
		) RETURNING aggregated_ts, fingerprint_id, app_name, node_id`
)

type cleanupOperations struct {
	initialScanStmt      string
	initialDeleteStmt    string
	subsequentDeleteStmt string
}

func (c *StatsCompactor) cleanupOpForTable(tableName string) *cleanupOperations {
	switch tableName {
	case "system.statement_statistics":
		return &cleanupOperations{
			initialScanStmt: c.getQueryForCheckingTableRowCounts(
				tableName,
				systemschema.StmtStatsHashColumnName,
			),
			initialDeleteStmt:    deleteFromStmtStatsWithWideScan,
			subsequentDeleteStmt: deleteFromStmtStatsWithConstrainedScan,
		}
	case "system.transaction_statistics":
		return &cleanupOperations{
			initialScanStmt: c.getQueryForCheckingTableRowCounts(
				tableName,
				systemschema.TxnStatsHashColumnName,
			),
			initialDeleteStmt:    deleteFromTxnStatsWithWideScan,
			subsequentDeleteStmt: deleteFromTxnStatsWithConstrainedScan,
		}
	default:
		panic(errors.AssertionFailedf("unsupported table %s", tableName))
	}
}

func (c *StatsCompactor) executeDeleteStmt(
	ctx context.Context, delStmt string, qargs []interface{},
) (lastRow tree.Datums, rowsDeleted int64, err error) {
	it, err := c.ie.QueryIteratorEx(ctx,
		"delete-old-sql-stats",
		nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		delStmt,
		qargs...,
	)
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		err = errors.CombineErrors(err, it.Close())
	}()

	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		lastRow = it.Cur()
		rowsDeleted++
	}

	return lastRow, rowsDeleted, err
}
