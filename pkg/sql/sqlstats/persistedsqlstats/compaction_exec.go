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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

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
	if err := c.removeStaleRowsPerShard(
		ctx,
		c.cleanupOpForTable("system.statement_statistics"),
	); err != nil {
		return err
	}

	return c.removeStaleRowsPerShard(
		ctx,
		c.cleanupOpForTable("system.transaction_statistics"),
	)
}

func (c *StatsCompactor) removeStaleRowsPerShard(
	ctx context.Context, ops *cleanupOperations,
) error {
	rowLimitPerShard := c.getRowLimitPerShard()
	for shardIdx, rowLimit := range rowLimitPerShard {
		var existingRowCount int64

		if err := c.getRowCountForShard(
			ctx,
			ops.initialScanStmt,
			shardIdx,
			&existingRowCount,
		); err != nil {
			return err
		}

		if c.knobs != nil && c.knobs.OnCleanupStartForShard != nil {
			c.knobs.OnCleanupStartForShard(shardIdx, existingRowCount, rowLimit)
		}

		if err := c.removeStaleRowsForShard(
			ctx,
			ops,
			int64(shardIdx),
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
	ops *cleanupOperations,
	shardIdx int64,
	existingRowCountPerShard, maxRowLimitPerShard int64,
) error {
	var err error
	var qargs []interface{}

	lastDeletedRow := tree.Datums{}
	stmt := ops.initialDeleteStmt
	maxDeleteRowsPerTxn := CompactionJobRowsToDeletePerTxn.Get(&c.st.SV)

	if rowsToRemove := existingRowCountPerShard - maxRowLimitPerShard; rowsToRemove > 0 {
		for remainToBeRemoved := rowsToRemove; remainToBeRemoved > 0; {
			if len(lastDeletedRow) > 0 {
				stmt = ops.subsequentDeleteStmt
			}
			rowsToRemovePerTxn := remainToBeRemoved
			if remainToBeRemoved > maxDeleteRowsPerTxn {
				rowsToRemovePerTxn = maxDeleteRowsPerTxn
			}

			qargs, err = c.getQargs(shardIdx, rowsToRemovePerTxn, lastDeletedRow)
			if err != nil {
				return err
			}

			var rowsRemoved int64

			lastDeletedRow, rowsRemoved, err = c.executeDeleteStmt(
				ctx,
				stmt,
				qargs,
			)
			if err != nil {
				return err
			}
			c.rowsRemovedCounter.Inc(rowsToRemovePerTxn)

			// If we removed less rows compared to what we intended, it means something
			// else is interfering with the cleanup job, likely a human operator.
			// This can happen when the operator forgot to cancel the job when manual
			// intervention is happening.
			if rowsRemoved < rowsToRemovePerTxn {
				break
			}

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

	followerReadClause := c.knobs.GetAOSTClause()

	return fmt.Sprintf(existingRowCountQuery, tableName, followerReadClause, hashColumnName)
}

func (c *StatsCompactor) getQargs(
	shardIdx, limit int64, lastDeletedRow tree.Datums,
) ([]interface{}, error) {
	qargs := make([]interface{}, 0, len(lastDeletedRow)+3)

	qargs = append(qargs, tree.NewDInt(tree.DInt(shardIdx)))
	qargs = append(qargs, tree.NewDInt(tree.DInt(limit)))

	now := timeutil.Now()
	if c.knobs != nil && c.knobs.StubTimeNow != nil {
		now = c.knobs.StubTimeNow()
	}
	aggInterval := SQLStatsAggregationInterval.Get(&c.st.SV)
	aggTs := now.Truncate(aggInterval)
	datum, err := tree.MakeDTimestampTZ(aggTs, time.Microsecond)
	if err != nil {
		return nil, err
	}
	qargs = append(qargs, datum)

	for _, value := range lastDeletedRow {
		qargs = append(qargs, value)
	}

	return qargs, nil
}
