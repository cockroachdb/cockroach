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

// getExistingStmtAndTxnStatsCount returns the counts of statement and
// transaction statistics in the system table. This function use follower read
// so the result returned here would be slightly stale. This is to avoid
// contentions on the system tables that stores the statistics.
func (c *StatsCompactor) getExistingStmtAndTxnStatsCount(
	ctx context.Context,
) (stmtStatsEntryCount, txnStatsEntryCount int64, err error) {
	getRowCount := func(ctx context.Context, opName string, stmt string) (int64, error) {
		row, err := c.ie.QueryRowEx(ctx,
			opName,
			nil,
			sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
			stmt)
		if err != nil {
			return 0, err
		}

		if row.Len() != 1 {
			return 0, errors.AssertionFailedf("unexpected number of column returned")
		}
		rowCount := int64(tree.MustBeDInt(row[0]))

		return rowCount, nil
	}

	stmt := "SELECT count(*) FROM system.statement_statistics AS OF SYSTEM TIME follower_read_timestamp()"
	if c.knobs.DisableFollowerRead {
		stmt = "SELECT count(*) FROM system.statement_statistics"
	}

	stmtStatsEntryCount, err = getRowCount(ctx, "scan-sql-stmt-stats-entries", stmt)
	if err != nil {
		return 0 /* stmtStatsEntryCount */, 0 /* txnStatsEntryCount */, err
	}

	stmt = "SELECT count(*) FROM system.transaction_statistics AS OF SYSTEM TIME follower_read_timestamp()"
	if c.knobs.DisableFollowerRead {
		stmt = "SELECT count(*) FROM system.transaction_statistics"
	}

	txnStatsEntryCount, err = getRowCount(ctx, "scan-sql-txn-stats-entries", stmt)

	return stmtStatsEntryCount, txnStatsEntryCount, nil
}

func (c *StatsCompactor) deleteOldestEntries(
	ctx context.Context, curStmtStatsEntries, curTxnStatsEntries int64,
) error {
	maxStatsEntry := SQLStatsMaxPersistedRows.Get(&c.st.SV)

	// [1]: table name
	// [2]: primary key
	const stmt = `
DELETE FROM %[1]s
WHERE (%[2]s) IN (
  SELECT %[2]s
  FROM %[1]s
  ORDER BY aggregated_ts ASC
  LIMIT $1
)
`

	removeRows := func(
		ctx context.Context,
		opName string,
		txn *kv.Txn,
		stmt string,
		curRowsCount int64) error {
		if entriesToRemove := curRowsCount - maxStatsEntry; entriesToRemove > 0 {
			_, err := c.ie.ExecEx(ctx,
				opName,
				txn,
				sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
				stmt,
				entriesToRemove,
			)

			if err != nil {
				return err
			}

			c.rowsRemovedCounter.Inc(entriesToRemove)
		}
		return nil
	}

	return c.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		removeStmtStatsStmt := fmt.Sprintf(stmt,
			"system.statement_statistics",
			"aggregated_ts, fingerprint_id, plan_hash, app_name, node_id",
		)
		if err := removeRows(ctx,
			"delete-old-stmt-stats",
			txn,
			removeStmtStatsStmt,
			curStmtStatsEntries); err != nil {
			return err
		}

		removeTxnStatsStmt := fmt.Sprintf(stmt,
			"system.transaction_statistics",
			"aggregated_ts, fingerprint_id, app_name, node_id",
		)

		return removeRows(ctx,
			"delete-old-txn-stats",
			txn,
			removeTxnStatsStmt,
			curTxnStatsEntries)
	})
}
