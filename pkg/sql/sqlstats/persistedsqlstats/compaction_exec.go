// Copyright 2021 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
)

// StatsCompactor is responsible for compacting older SQL Stats. It is
// executed by sql.sqlStatsCompactionResumer.
type StatsCompactor struct {
	st *cluster.Settings
	db *kv.DB
	ie sqlutil.InternalExecutor

	rowsRemovedCounter *metric.Counter

	knobs *TestingKnobs
}

// NewStatsCompactor returns a new instance of StatsCompactor.
func NewStatsCompactor(
	setting *cluster.Settings,
	internalEx sqlutil.InternalExecutor,
	db *kv.DB,
	rowsRemovedCounter *metric.Counter,
	knobs *TestingKnobs,
) *StatsCompactor {
	return &StatsCompactor{
		st:                 setting,
		db:                 db,
		ie:                 internalEx,
		rowsRemovedCounter: rowsRemovedCounter,
		knobs:              knobs,
	}
}

// DeleteOldestEntries removes the oldest statement and transaction statistics
// that exceeded the limit defined by `sql.stats.persisted_rows.max`
// (persistedsqlstats.SQLStatsMaxPersistedRows).
func (c *StatsCompactor) DeleteOldestEntries(ctx context.Context) error {
	stmtStatsEntryCount, txnStatsEntryCount, err := c.getExistingStmtAndTxnStatsCount(ctx)
	if err != nil {
		return err
	}

	err = c.deleteOldestEntries(ctx, stmtStatsEntryCount, txnStatsEntryCount)
	if err != nil {
		return err
	}

	return nil
}

func (c *StatsCompactor) getRowCountForShard(
	ctx context.Context, opName string, stmt string, shardIdx int64,
) (int64, error) {
	row, err := c.ie.QueryRowEx(ctx,
		opName,
		nil,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		stmt,
		shardIdx,
	)
	if err != nil {
		return 0, err
	}

	if row.Len() != 1 {
		return 0, errors.AssertionFailedf("unexpected number of column returned")
	}
	rowCount := int64(tree.MustBeDInt(row[0]))

	return rowCount, nil
}

func (c *StatsCompactor) getRowCountPerShard(
	ctx context.Context, opName, tableName, hashColumnName string,
) ([]int64, error) {
	rowCountPerShard := make([]int64, systemschema.SQLStatsHashShardBucketCount)

	// [1]: table name
	// [2]: hash column name
	stmt := `
SELECT count(*)
FROM %[1]s
WHERE %[2]s = $1
AS OF SYSTEM TIME follower_read_timestamp()`

	if c.knobs.DisableFollowerRead {
		stmt = `
SELECT count(*)
FROM %[1]s
WHERE %[2]s = $1`
	}

	stmt = fmt.Sprintf(stmt, tableName, hashColumnName)

	for shardIdx := int64(0); shardIdx < systemschema.SQLStatsHashShardBucketCount; shardIdx++ {
		rowCount, err := c.getRowCountForShard(ctx, opName, stmt, shardIdx)
		if err != nil {
			return nil, err
		}
		rowCountPerShard[shardIdx] = rowCount
	}

	return rowCountPerShard, nil
}

// getExistingStmtAndTxnStatsCount returns the counts of statement and
// transaction statistics in the system table for each hash bucket. This
// function use follower read so the result returned here would be slightly
// stale. This is to avoid contentions on the system tables that stores the
// statistics.
func (c *StatsCompactor) getExistingStmtAndTxnStatsCount(
	ctx context.Context,
) ([]int64, []int64, error) {
	var err error

	var stmtStatsRowCountPerShard []int64
	{
		stmtStatsRowCountPerShard, err = c.getRowCountPerShard(
			ctx,
			"scan-sql-stmt-stats-entries",
			"system.statement_statistics",
			systemschema.StmtStatsHashColumnName,
		)
		if err != nil {
			return nil /* stmtStatsRowCountPerShard */, nil /* txnStatsRowCountPerShard */, err
		}
	}

	var txnStatsRowCountPerShard []int64
	{
		txnStatsRowCountPerShard, err = c.getRowCountPerShard(
			ctx,
			"scan-txn-stmt-stats-entries",
			"system.transaction_statistics",
			systemschema.TxnStatsHashColumnName,
		)
		if err != nil {
			return nil /* stmtStatsRowCountPerShard */, nil /* txnStatsRowCountPerShard */, err
		}
	}

	return stmtStatsRowCountPerShard, txnStatsRowCountPerShard, nil
}

// getRowLimitPerShard calculates the max number of rows we can keep per hash
// bucket. It calculates using the cluster setting sql.stats.persisted_rows.max.
//
// It calculates as follows:
// * quotient, remainder = sql.stats.persisted_rows.max / bucket count
// * limitPerShard[0:remainder] = quotient + 1
// * limitPerShard[remainder:] = quotient
func (c *StatsCompactor) getRowLimitPerShard() []int64 {
	limitPerShard := make([]int64, systemschema.SQLStatsHashShardBucketCount)

	maxPersistedRows := SQLStatsMaxPersistedRows.Get(&c.st.SV)

	maxStatsEntryPerShard := maxPersistedRows / systemschema.SQLStatsHashShardBucketCount
	remaining := maxPersistedRows % systemschema.SQLStatsHashShardBucketCount

	for shardIdx := range limitPerShard {
		if int64(shardIdx) < remaining {
			limitPerShard[shardIdx] = maxStatsEntryPerShard + 1
		} else {
			limitPerShard[shardIdx] = maxStatsEntryPerShard
		}
	}

	return limitPerShard
}

func (c *StatsCompactor) deleteOldestEntries(
	ctx context.Context, stmtStatsRowCountPerShard, txnStatsRowCountPerShard []int64,
) error {
	maxRowPerShard := c.getRowLimitPerShard()

	// [1]: table name
	// [2]: primary key
	// [3]: hash column name
	const stmt = `
DELETE FROM %[1]s
WHERE (%[2]s) IN (
  SELECT %[2]s
  FROM %[1]s
  WHERE %[3]s = $1
  ORDER BY aggregated_ts ASC
  LIMIT $2
)
`

	removeRows := func(
		ctx context.Context,
		opName string,
		txn *kv.Txn,
		stmt string,
		rowCountPerShard []int64,
	) error {

		for shardIdx := 0; shardIdx < systemschema.SQLStatsHashShardBucketCount; shardIdx++ {
			if entriesToRemove := rowCountPerShard[shardIdx] - maxRowPerShard[shardIdx]; entriesToRemove > 0 {
				_, err := c.ie.ExecEx(ctx,
					opName,
					txn,
					sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
					stmt,
					shardIdx,
					entriesToRemove,
				)

				if err != nil {
					return err
				}

				c.rowsRemovedCounter.Inc(entriesToRemove)
			}
		}
		return nil
	}

	return c.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		removeStmtStatsStmt := fmt.Sprintf(stmt,
			"system.statement_statistics",
			"aggregated_ts, fingerprint_id, plan_hash, app_name, node_id",
			systemschema.StmtStatsHashColumnName,
		)
		if err := removeRows(ctx,
			"delete-old-stmt-stats",
			txn,
			removeStmtStatsStmt,
			stmtStatsRowCountPerShard); err != nil {
			return err
		}

		removeTxnStatsStmt := fmt.Sprintf(stmt,
			"system.transaction_statistics",
			"aggregated_ts, fingerprint_id, app_name, node_id",
			systemschema.TxnStatsHashColumnName,
		)

		return removeRows(ctx,
			"delete-old-txn-stats",
			txn,
			removeTxnStatsStmt,
			txnStatsRowCountPerShard)
	})
}
