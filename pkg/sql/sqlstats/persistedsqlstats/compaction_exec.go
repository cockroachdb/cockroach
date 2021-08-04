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
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/errors"
)

// StatsCompactor is responsible for compacting older SQL Stats. It is
// executed by sql.sqlStatsCompactionResumer.
type StatsCompactor struct {
	Settings *cluster.Settings

	initialized bool
	db          *kv.DB
	ie          sqlutil.InternalExecutor

	TestingKnobs *StatsCompactorTestingKnobs
}

// StatsCompactorTestingKnobs can be used to tune the behavior of
// StatsCompactor in unit tests.
type StatsCompactorTestingKnobs struct {
	// DisableFollowerRead disables the use of follower read used in
	// StatsCompactor. This is used because follower can cause problems in
	// in unit tests where we immediately start running follower read SQL queries
	// on system tables that are yet to be created. This is not a scenario that is
	// possible in real life.
	DisableFollowerRead bool

	// JobSchedulerEnv overrides the environment to use for scheduled jobs.
	JobsSchedulerEnv scheduledjobs.JobSchedulerEnv
}

// Init initializes the StatsCompactor. This method needs to be called before
// the DeleteOldestEntries method.
func (c *StatsCompactor) Init(internalEx sqlutil.InternalExecutor, db *kv.DB) {
	c.db = db
	c.ie = internalEx
	c.initialized = true
}

// DeleteOldestEntries removes the oldest statement and transaction statistics
// that exceeded the limit defined by `sql.stats.persisted_rows.max`
// (persistedsqlstats.SQLStatsMaxPersistedRows).
func (c *StatsCompactor) DeleteOldestEntries(ctx context.Context) error {
	if !c.initialized {
		return errors.AssertionFailedf("CompactExecutor has not being initialized")
	}

	stmtStatsEntryCount, txnStatsEntryCount, err := c.getExistingStmtAndTxnStatsEntries(ctx)
	if err != nil {
		return err
	}

	err = c.deleteOldestEntries(ctx, stmtStatsEntryCount, txnStatsEntryCount)
	if err != nil {
		return err
	}

	return nil
}

func (c *StatsCompactor) getExistingStmtAndTxnStatsEntries(
	ctx context.Context,
) (stmtStatsEntryCount, txnStatsEntryCount int64, err error) {
	stmt := "SELECT count(*) FROM system.statement_statistics AS OF SYSTEM TIME follower_read_timestamp()"
	if c.TestingKnobs != nil && c.TestingKnobs.DisableFollowerRead {
		stmt = "SELECT count(*) FROM system.statement_statistics"
	}
	row, err := c.ie.QueryRowEx(ctx,
		"scan-sql-stmt-stats-entries",
		nil,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		stmt)
	if err != nil {
		return 0 /* stmtStatsEntryCount */, 0 /* txnStatsEntryCount */, err
	}

	if row.Len() != 1 {
		return 0 /* stmtStatsEntryCount */, 0 /* txnStatsEntryCount */, errors.AssertionFailedf("unexpected number of column returned")
	}
	stmtStatsEntryCount = int64(tree.MustBeDInt(row[0]))

	stmt = "SELECT count(*) FROM system.transaction_statistics AS OF SYSTEM TIME follower_read_timestamp()"
	if c.TestingKnobs != nil && c.TestingKnobs.DisableFollowerRead {
		stmt = "SELECT count(*) FROM system.transaction_statistics"
	}
	row, err = c.ie.QueryRowEx(ctx,
		"scan-sql-txn-stats-entries",
		nil,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		stmt)
	if err != nil {
		return 0 /* stmtStatsEntryCount */, 0 /* txnStatsEntryCount */, err
	}

	if row.Len() != 1 {
		return 0 /* stmtStatsEntryCount */, 0 /* txnStatsEntryCount */, errors.AssertionFailedf("unexpected number of column returned")
	}
	txnStatsEntryCount = int64(tree.MustBeDInt(row[0]))

	return stmtStatsEntryCount, txnStatsEntryCount, nil
}

func (c *StatsCompactor) deleteOldestEntries(
	ctx context.Context, curStmtStatsEntries, curTxnStatsEntries int64,
) error {
	maxStatsEntry := SQLStatsMaxPersistedRows.Get(&c.Settings.SV)

	// [1]: table name
	// [2]: primary key
	// [3]: number of entries to remove
	stmt := `
DELETE FROM %[1]s
WHERE (%[2]s) IN (
  SELECT %[2]s
  FROM %[1]s
  ORDER BY aggregated_ts ASC
  LIMIT %[3]d
)
`

	err := c.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if entriesToRemove := curStmtStatsEntries - maxStatsEntry; entriesToRemove > 0 {
			_, err := c.ie.ExecEx(ctx,
				"delete-old-stmt-stats",
				txn,
				sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
				fmt.Sprintf(stmt,
					"system.statement_statistics",
					"aggregated_ts, fingerprint_id, plan_hash, app_name, node_id",
					entriesToRemove))

			if err != nil {
				return err
			}
		}

		if entriesToRemove := curTxnStatsEntries - maxStatsEntry; entriesToRemove > 0 {
			_, err := c.ie.ExecEx(ctx,
				"delete-old-txn-stats",
				txn,
				sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
				fmt.Sprintf(stmt,
					"system.transaction_statistics",
					"aggregated_ts, fingerprint_id, app_name, node_id",
					entriesToRemove))

			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}
