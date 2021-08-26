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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
)

// maxDeleteRowsPerTxn limits max number of rows StatsCompactor deletes
// per transaction. This is to avoid having large transaction which can
// have negative impact on the overall system performance.
const maxDeleteRowsPerTxn = 128

// StatsCompactor is responsible for compacting older SQL Stats. It is
// executed by sql.sqlStatsCompactionResumer.
type StatsCompactor struct {
	st *cluster.Settings
	db *kv.DB
	ie sqlutil.InternalExecutor

	rowsRemovedCounter *metric.Counter

	knobs *sqlstats.TestingKnobs
}

// NewStatsCompactor returns a new instance of StatsCompactor.
func NewStatsCompactor(
	setting *cluster.Settings,
	internalEx sqlutil.InternalExecutor,
	db *kv.DB,
	rowsRemovedCounter *metric.Counter,
	knobs *sqlstats.TestingKnobs,
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
	if err := c.removeStaleRowsPerShard(ctx,
		"system.statement_statistics",
		systemschema.StmtStatsHashColumnName,
		"aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash, app_name, node_id",
	); err != nil {
		return err
	}

	return c.removeStaleRowsPerShard(ctx,
		"system.transaction_statistics",
		systemschema.TxnStatsHashColumnName,
		"aggregated_ts, fingerprint_id, app_name, node_id",
	)
}

func (c *StatsCompactor) removeStaleRowsPerShard(
	ctx context.Context, tableName, hashColumnName, pkColumnNames string,
) error {
	rowLimitPerShard := c.getRowLimitPerShard()

	existingRowCountQuery := c.getQueryForCheckingTableRowCounts(tableName, hashColumnName)
	deleteOldStatsStmt := c.getStatementForDeletingStaleRows(tableName, pkColumnNames, hashColumnName)

	for shardIdx, rowLimit := range rowLimitPerShard {
		var existingRowCount int64
		if err := c.getRowCountForShard(
			ctx,
			existingRowCountQuery,
			shardIdx,
			&existingRowCount,
		); err != nil {
			return err
		}

		if err := c.removeStaleRowsForShard(
			ctx,
			deleteOldStatsStmt,
			shardIdx,
			existingRowCount,
			rowLimit,
		); err != nil {
			return err
		}
	}

	return nil
}

func (c *StatsCompactor) getRowCountForShard(
	ctx context.Context, stmt string, shardIdx int, count *int64,
) error {
	row, err := c.ie.QueryRowEx(ctx,
		"scan-row-count",
		nil,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		stmt,
		shardIdx,
	)
	if err != nil {
		return err
	}

	if row.Len() != 1 {
		return errors.AssertionFailedf("unexpected number of column returned")
	}
	*count = int64(tree.MustBeDInt(row[0]))

	return nil
}

// getRowLimitPerShard calculates the max number of rows we can keep per hash
// bucket. It calculates using the cluster setting sql.stats.persisted_rows.max.
//
// It calculates as follows:
// * quotient, remainder = sql.stats.persisted_rows.max / bucket count
// * limitPerShard[0:remainder] = quotient
// * limitPerShard[remainder:] = quotient + 1
func (c *StatsCompactor) getRowLimitPerShard() []int64 {
	limitPerShard := make([]int64, systemschema.SQLStatsHashShardBucketCount)
	maxPersistedRows := SQLStatsMaxPersistedRows.Get(&c.st.SV)

	for shardIdx := int64(0); shardIdx < systemschema.SQLStatsHashShardBucketCount; shardIdx++ {
		limitPerShard[shardIdx] = maxPersistedRows / (systemschema.SQLStatsHashShardBucketCount - shardIdx)
		maxPersistedRows -= limitPerShard[shardIdx]
	}

	return limitPerShard
}

// removeStaleRowsForShard deletes the oldest rows in the given hash bucket.
// It breaks the removal operation into multiple smaller transactions where
// each transaction will delete up to maxDeleteRowsPerTxn rows. This is to
// avoid having one large transaction.
func (c *StatsCompactor) removeStaleRowsForShard(
	ctx context.Context,
	stmt string,
	shardIdx int,
	existingRowCountPerShard, maxRowLimitPerShard int64,
) error {
	if rowsToRemove := existingRowCountPerShard - maxRowLimitPerShard; rowsToRemove > 0 {
		for remainToBeRemoved := rowsToRemove; remainToBeRemoved > 0; {
			rowsToRemovePerTxn := remainToBeRemoved
			if remainToBeRemoved > maxDeleteRowsPerTxn {
				rowsToRemovePerTxn = maxDeleteRowsPerTxn
			}
			if _, err := c.ie.ExecEx(ctx,
				"delete-old-sql-stats",
				nil, /* txn */
				sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
				stmt,
				shardIdx,
				rowsToRemovePerTxn,
			); err != nil {
				return err
			}

			c.rowsRemovedCounter.Inc(rowsToRemovePerTxn)
			remainToBeRemoved -= rowsToRemovePerTxn
		}
	}

	return nil
}

func (c *StatsCompactor) getQueryForCheckingTableRowCounts(
	tableName, hashColumnName string,
) string {
	// [1]: table name
	// [2]: follower read clause
	// [3]: hash column name
	existingRowCountQuery := `
SELECT count(*)
FROM %[1]s
%[2]s
WHERE %[3]s = $1
`
	followerReadClause := "AS OF SYSTEM TIME follower_read_timestamp()"

	if c.knobs != nil {
		followerReadClause = c.knobs.AOSTClause
	}

	return fmt.Sprintf(existingRowCountQuery, tableName, followerReadClause, hashColumnName)
}

func (c *StatsCompactor) getStatementForDeletingStaleRows(
	tableName, pkColumnNames, hashColumnName string,
) string {
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
	return fmt.Sprintf(stmt,
		tableName,
		pkColumnNames,
		hashColumnName,
	)
}
