// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package persistedsqlstats

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// StatsCompactor is responsible for compacting older SQL Stats. It is
// executed by sql.sqlStatsCompactionResumer.
type StatsCompactor struct {
	st *cluster.Settings
	db isql.DB

	rowsRemovedCounter *metric.Counter

	knobs *sqlstats.TestingKnobs

	scratch struct {
		qargs []interface{}
	}
}

// NewStatsCompactor returns a new instance of StatsCompactor.
func NewStatsCompactor(
	setting *cluster.Settings,
	db isql.DB,
	rowsRemovedCounter *metric.Counter,
	knobs *sqlstats.TestingKnobs,
) *StatsCompactor {
	return &StatsCompactor{
		st:                 setting,
		db:                 db,
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
		stmtStatsCleanupOps,
	); err != nil {
		return err
	}

	return c.removeStaleRowsPerShard(
		ctx,
		txnStatsCleanupOps,
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
			ops.getScanStmt(c.knobs),
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
	row, err := c.db.Executor().QueryRowEx(ctx,
		"scan-row-count",
		nil,
		sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
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
	var lastDeletedRow tree.Datums
	var qargs []interface{}
	maxDeleteRowsPerTxn := CompactionJobRowsToDeletePerTxn.Get(&c.st.SV)

	if rowsToRemove := existingRowCountPerShard - maxRowLimitPerShard; rowsToRemove > 0 {
		for remainToBeRemoved := rowsToRemove; remainToBeRemoved > 0; {
			rowsToRemovePerTxn := remainToBeRemoved
			if remainToBeRemoved > maxDeleteRowsPerTxn {
				rowsToRemovePerTxn = maxDeleteRowsPerTxn
			}

			stmt := ops.getDeleteStmt(lastDeletedRow)
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

func (c *StatsCompactor) executeDeleteStmt(
	ctx context.Context, delStmt string, qargs []interface{},
) (lastRow tree.Datums, rowsDeleted int64, err error) {
	it, err := c.db.Executor().QueryIteratorEx(ctx,
		"delete-old-sql-stats",
		nil, /* txn */
		sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
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

func (c *StatsCompactor) getQargs(
	shardIdx, limit int64, lastDeletedRow tree.Datums,
) ([]interface{}, error) {
	size := len(lastDeletedRow) + 2
	if cap(c.scratch.qargs) < size {
		c.scratch.qargs = make([]interface{}, 0, size)
	}
	c.scratch.qargs = c.scratch.qargs[:0]

	c.scratch.qargs = append(c.scratch.qargs, tree.NewDInt(tree.DInt(shardIdx)))
	c.scratch.qargs = append(c.scratch.qargs, tree.NewDInt(tree.DInt(limit)))

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
	c.scratch.qargs = append(c.scratch.qargs, datum)

	for _, value := range lastDeletedRow {
		c.scratch.qargs = append(c.scratch.qargs, value)
	}

	return c.scratch.qargs, nil
}

type cleanupOperations struct {
	initialScanStmtTemplate string
	unconstrainedDeleteStmt string
	constrainedDeleteStmt   string
}

// TODO(#91600): Add deterministic execbuilder tests for these queries at
// pkg/sql/opt/exec/execbuilder/testdata/sql_activity_stats_compaction
// When changing the constraint queries below, make sure to also change the queries in those tests.
var (
	stmtStatsCleanupOps = &cleanupOperations{
		initialScanStmtTemplate: `
      SELECT count(*)
      FROM system.statement_statistics
      %s
      WHERE crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_plan_hash_transaction_fingerprint_id_shard_8 = $1`,
		unconstrainedDeleteStmt: `
      DELETE FROM system.statement_statistics
        WHERE crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_plan_hash_transaction_fingerprint_id_shard_8 = $1
          AND aggregated_ts < $3
        ORDER BY aggregated_ts ASC
        LIMIT $2
       RETURNING aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash, app_name, node_id`,
		constrainedDeleteStmt: `
    DELETE FROM system.statement_statistics
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
     RETURNING aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash, app_name, node_id`,
	}
	txnStatsCleanupOps = &cleanupOperations{
		initialScanStmtTemplate: `
      SELECT count(*)
      FROM system.transaction_statistics
      %s
      WHERE crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_shard_8 = $1`,
		unconstrainedDeleteStmt: `
    DELETE FROM system.transaction_statistics
      WHERE crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_shard_8 = $1
        AND aggregated_ts < $3
      ORDER BY aggregated_ts ASC
      LIMIT $2
     RETURNING aggregated_ts, fingerprint_id, app_name, node_id`,
		constrainedDeleteStmt: `
    DELETE FROM system.transaction_statistics
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
     RETURNING aggregated_ts, fingerprint_id, app_name, node_id`,
	}
)

func (c *cleanupOperations) getScanStmt(knobs *sqlstats.TestingKnobs) string {
	return fmt.Sprintf(c.initialScanStmtTemplate, knobs.GetAOSTClause())
}

func (c *cleanupOperations) getDeleteStmt(lastDeletedRow tree.Datums) string {
	if len(lastDeletedRow) == 0 {
		return c.unconstrainedDeleteStmt
	}

	return c.constrainedDeleteStmt
}
