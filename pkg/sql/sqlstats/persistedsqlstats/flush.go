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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Flush causes the PersistedSQLStats to flush all in-memory SQL stats into
// system table. This function runs asynchronously and returns immediately.
// If an error is encountered during the flush process, the error is sent back
// to the main flush loop through channel. Flush becomes a no-op if a flush
// operation is already in progress.
func (s *PersistedSQLStats) Flush(ctx context.Context, stopper *stop.Stopper) {
	s.mu.Lock()

	switch s.mu.state {
	case idle:
		s.mu.state = flushing
		s.mu.Unlock()
		err := stopper.RunAsyncTask(ctx, "sql-stats-flush-worker", func(ctx context.Context) {
			var err error
			flushBegin := s.getTimeNow()
			defer func() {
				s.mu.Lock()
				s.mu.state = idle
				s.cfg.FlushDuration.RecordValue(s.getTimeNow().Sub(flushBegin).Nanoseconds())
				s.cfg.FlushCounter.Inc(1)
				if err == nil {
					s.mu.lastFlushed = s.getTimeNow()
				} else {
					s.cfg.FailureCounter.Inc(1)
				}
				s.mu.Unlock()
				if s.cfg.Knobs != nil && s.cfg.Knobs.OnStatsFlushFinished != nil {
					s.cfg.Knobs.OnStatsFlushFinished(err)
				}
			}()

			// Currently, we write statement and transaction statistics sequentially.
			// It might be worth it to run them in separately async tasks concurrently
			// as a performance optimization if that ever becomes a problem.
			err = s.cfg.KvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				err := s.doFlushStmtStats(ctx, txn)
				if err != nil {
					return err
				}

				err = s.doFlushTxnStats(ctx, txn)
				if err != nil {
					return err
				}

				return nil
			})

			if err != nil {
				s.errChan <- errors.Errorf("failed to flush sql stats: %s", err)
				// We report the error and exit early if we have encountered an error
				// during flush. This ensures that we don't accidentally clears the
				// in-memory stats.
				return
			}

			err = s.SQLStats.Reset(ctx)
			if err != nil {
				s.errChan <- errors.Errorf("failed to reset in-memory sql stats: %s", err)
			}
		})

		if err != nil {
			s.errChan <- errors.Errorf("failed to start flush async task: %s", err)
		}
	case flushing:
		s.mu.Unlock()
		// We do nothing here if we are already flushing.
		return
	}
}

func (s *PersistedSQLStats) doFlushTxnStats(ctx context.Context, txn *kv.Txn) error {
	err := s.SQLStats.IterateTransactionStats(ctx, &sqlstats.IteratorOptions{}, func(key roachpb.TransactionFingerprintID, stats *roachpb.CollectedTransactionStatistics) error {
		aggregatedTs := s.computeAggregatedTs()
		serializedFingerprintID := sqlstatsutil.EncodeUint64ToBytes(uint64(key))

		insertFn := func(ctx context.Context, txn *kv.Txn) (alreadyExists bool, err error) {
			rowsAffected, err := s.insertTransactionStats(ctx, txn, aggregatedTs, serializedFingerprintID, stats)

			if err != nil {
				return false /* alreadyExists */, err
			}

			if rowsAffected == 0 {
				return true /* alreadyExists */, nil /* err */
			}

			return false /* alreadyExists */, nil /* err */
		}

		readFn := func(ctx context.Context, txn *kv.Txn) error {
			persistedData := roachpb.TransactionStatistics{}
			err := s.fetchPersistedTransactionStats(ctx, txn, aggregatedTs, serializedFingerprintID, stats.App, &persistedData)
			if err != nil {
				return err
			}

			stats.Stats.Add(&persistedData)
			return nil
		}

		updateFn := func(ctx context.Context, txn *kv.Txn) error {
			return s.updateTransactionStats(ctx, txn, aggregatedTs, serializedFingerprintID, stats)
		}

		err := s.doInsertElseDoUpdate(ctx, txn, insertFn, readFn, updateFn)
		if err != nil {
			return errors.Errorf("failed to insert statement %d's statistics: %s", key, err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil

}

func (s *PersistedSQLStats) doFlushStmtStats(ctx context.Context, txn *kv.Txn) error {
	err := s.SQLStats.IterateStatementStats(ctx, &sqlstats.IteratorOptions{}, func(stats *roachpb.CollectedStatementStatistics) error {
		aggregatedTs := s.computeAggregatedTs()
		serializedFingerprintID := sqlstatsutil.EncodeUint64ToBytes(uint64(stats.ID))

		insertFn := func(ctx context.Context, txn *kv.Txn) (alreadyExists bool, err error) {
			rowsAffected, err := s.insertStatementStats(ctx, txn, aggregatedTs, serializedFingerprintID, stats)

			if err != nil {
				return false /* alreadyExists */, err
			}

			if rowsAffected == 0 {
				return true /* alreadyExists */, nil /* err */
			}

			return false /* alreadyExists */, nil /* err */
		}

		readFn := func(ctx context.Context, txn *kv.Txn) error {
			persistedData := roachpb.StatementStatistics{}
			err := s.fetchPersistedStatementStats(ctx, txn, aggregatedTs, serializedFingerprintID, &stats.Key, &persistedData)
			if err != nil {
				return err
			}

			stats.Stats.Add(&persistedData)
			return nil
		}

		updateFn := func(ctx context.Context, txn *kv.Txn) error {
			return s.updateStatementStats(ctx, txn, aggregatedTs, serializedFingerprintID, stats)
		}

		err := s.doInsertElseDoUpdate(ctx, txn, insertFn, readFn, updateFn)
		if err != nil {
			return errors.Errorf("failed to insert statement %d's statistics: %s", stats.ID, err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (s *PersistedSQLStats) doInsertElseDoUpdate(
	ctx context.Context,
	txn *kv.Txn,
	insertFn func(context.Context, *kv.Txn) (alreadyExists bool, err error),
	readFn func(context.Context, *kv.Txn) error,
	updateFn func(context.Context, *kv.Txn) error,
) error {
	alreadyExists, err := insertFn(ctx, txn)
	if err != nil {
		return err
	}

	if alreadyExists {
		err = readFn(ctx, txn)
		if err != nil {
			return err
		}

		err = updateFn(ctx, txn)
		if err != nil {
			return errors.Errorf("failed to insert stats: %s", err)
		}
	}

	return nil
}

func (s *PersistedSQLStats) computeAggregatedTs() time.Time {
	interval := SQLStatsFlushInterval.Get(&s.cfg.Settings.SV)
	now := s.getTimeNow()

	aggTs := now.Truncate(interval)

	return aggTs
}

func (s *PersistedSQLStats) getTimeNow() time.Time {
	if s.cfg.Knobs != nil && s.cfg.Knobs.StubTimeNow != nil {
		return s.cfg.Knobs.StubTimeNow()
	}

	return timeutil.Now()
}

func (s *PersistedSQLStats) insertTransactionStats(
	ctx context.Context,
	txn *kv.Txn,
	aggregatedTs time.Time,
	serializedFingerprintID []byte,
	stats *roachpb.CollectedTransactionStatistics,
) (rowsAffected int, err error) {
	insertStmt := `
INSERT INTO system.transaction_statistics
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_shard_8, aggregated_ts, fingerprint_id, app_name, node_id)
DO NOTHING
`

	aggInterval := SQLStatsFlushInterval.Get(&s.cfg.Settings.SV)

	metadata, statistics, err := s.prepareTransactionStatsInsertValues(stats)
	if err != nil {
		return 0 /* rowsAffected */, err
	}

	rowsAffected, err = s.cfg.InternalExecutor.ExecEx(
		ctx,
		"insert-txn-stats",
		txn, /* txn */
		sessiondata.InternalExecutorOverride{
			User: security.NodeUserName(),
		},
		insertStmt,
		aggregatedTs,                         // aggregated_ts
		serializedFingerprintID,              // fingerprint_id
		stats.App,                            // app_name
		s.cfg.SQLIDContainer.SQLInstanceID(), // node_id
		stats.Stats.Count,                    // count
		aggInterval,                          // agg_interval
		metadata,                             // metadata
		statistics,                           // statistics
	)

	return rowsAffected, err
}
func (s *PersistedSQLStats) updateTransactionStats(
	ctx context.Context,
	txn *kv.Txn,
	aggregatedTs time.Time,
	serializedFingerprintID []byte,
	stats *roachpb.CollectedTransactionStatistics,
) error {
	updateStmt := `
UPDATE system.transaction_statistics
SET statistics = $1, count = $2
WHERE fingerprint_id = $3
	AND aggregated_ts = $4
  AND app_name = $5
  AND node_id = $6
`

	statisticsJSON, err := sqlstatsutil.BuildTxnStatisticsJSON(stats)
	if err != nil {
		return err
	}
	statistics := tree.NewDJSON(statisticsJSON)

	rowsAffected, err := s.cfg.InternalExecutor.ExecEx(
		ctx,
		"update-stmt-stats",
		txn, /* txn */
		sessiondata.InternalExecutorOverride{
			User: security.NodeUserName(),
		},
		updateStmt,
		statistics,                           // statistics
		stats.Stats.Count,                    // count
		serializedFingerprintID,              // fingerprint_id
		aggregatedTs,                         // aggregated_ts
		stats.App,                            // app_name
		s.cfg.SQLIDContainer.SQLInstanceID(), // node_id
	)

	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return errors.Errorf("failed to update transaction statistics")
	}

	return nil
}

func (s *PersistedSQLStats) updateStatementStats(
	ctx context.Context,
	txn *kv.Txn,
	aggregatedTs time.Time,
	serializedFingerprintID []byte,
	stats *roachpb.CollectedStatementStatistics,
) error {
	updateStmt := `
UPDATE system.statement_statistics
SET statistics = $1, count = $2
WHERE fingerprint_id = $3
	AND aggregated_ts = $4
  AND app_name = $5
  AND plan_hash = $6
  AND node_id = $7
`

	statisticsJSON, err := sqlstatsutil.BuildStmtStatisticsJSON(stats)
	if err != nil {
		return err
	}
	statistics := tree.NewDJSON(statisticsJSON)

	rowsAffected, err := s.cfg.InternalExecutor.ExecEx(
		ctx,
		"update-stmt-stats",
		txn, /* txn */
		sessiondata.InternalExecutorOverride{
			User: security.NodeUserName(),
		},
		updateStmt,
		statistics,                           // statistics
		stats.Stats.Count,                    // count
		serializedFingerprintID,              // fingerprint_id
		aggregatedTs,                         // aggregated_ts
		stats.Key.App,                        // app_name
		dummyPlanHash,                        // plan_id
		s.cfg.SQLIDContainer.SQLInstanceID(), // node_id
	)

	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return errors.Errorf("failed to update statement statistics")
	}

	return nil
}

func (s *PersistedSQLStats) insertStatementStats(
	ctx context.Context,
	txn *kv.Txn,
	aggregatedTs time.Time,
	serializedFingerprintID []byte,
	stats *roachpb.CollectedStatementStatistics,
) (rowsAffected int, err error) {
	insertStmt := `
INSERT INTO system.statement_statistics
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_plan_hash_shard_8, aggregated_ts, fingerprint_id, app_name, plan_hash, node_id)
DO NOTHING
`
	aggInterval := SQLStatsFlushInterval.Get(&s.cfg.Settings.SV)

	metadata, statistics, err := s.prepareStatementStatsInsertValues(stats)
	if err != nil {
		return 0 /* rowsAffected */, err
	}

	plan := tree.NewDJSON(sqlstatsutil.ExplainTreePlanNodeToJSON(&stats.Stats.SensitiveInfo.MostRecentPlanDescription))

	rowsAffected, err = s.cfg.InternalExecutor.ExecEx(
		ctx,
		"insert-stmt-stats",
		txn, /* txn */
		sessiondata.InternalExecutorOverride{
			User: security.NodeUserName(),
		},
		insertStmt,
		aggregatedTs,                         // aggregated_ts
		serializedFingerprintID,              // fingerprint_id
		dummyPlanHash,                        // plan_id
		stats.Key.App,                        // app_name
		s.cfg.SQLIDContainer.SQLInstanceID(), // node_id
		stats.Stats.Count,                    // count
		aggInterval,                          // agg_internal
		metadata,                             // metadata
		statistics,                           // statistics
		plan,                                 // plan
	)

	return rowsAffected, err
}

func (s *PersistedSQLStats) fetchPersistedTransactionStats(
	ctx context.Context,
	txn *kv.Txn,
	aggregatedTs time.Time,
	serializedFingerprintID []byte,
	appName string,
	result *roachpb.TransactionStatistics,
) error {
	readStmt := `
SELECT
    count,
    statistics
FROM
    system.transaction_statistics
WHERE fingerprint_id = $1
    AND app_name = $2
	  AND aggregated_ts = $3
    AND node_id = $4
FOR UPDATE
`

	row, err := s.cfg.InternalExecutor.QueryRowEx(
		ctx,
		"fetch-txn-stats",
		txn, /* txn */
		sessiondata.InternalExecutorOverride{
			User: security.NodeUserName(),
		},
		readStmt,                             // stmt
		serializedFingerprintID,              // fingerprint_id
		appName,                              // app_name
		aggregatedTs,                         // aggregated_ts
		s.cfg.SQLIDContainer.SQLInstanceID(), // node_id
	)

	if err != nil {
		return err
	}

	if row == nil {
		return errors.Errorf("transaction statistics not found")
	}

	if len(row) != 2 {
		return errors.AssertionFailedf("unexpected number of returning columns")
	}

	count := int64(*row[0].(*tree.DInt))
	result.Count = count

	statistics, ok := row[1].(*tree.DJSON)
	if !ok {
		return errors.Errorf("corrupted data")
	}

	err = sqlstatsutil.DecodeTxnStatsStatisticsJSON(statistics.JSON, result)
	if err != nil {
		return err
	}

	return nil
}

func (s *PersistedSQLStats) fetchPersistedStatementStats(
	ctx context.Context,
	txn *kv.Txn,
	aggregatedTs time.Time,
	serializedFingerprintID []byte,
	key *roachpb.StatementStatisticsKey,
	result *roachpb.StatementStatistics,
) error {
	readStmt := `
SELECT
    count,
    statistics
FROM
    system.statement_statistics
WHERE fingerprint_id = $1
    AND app_name = $2
	  AND aggregated_ts = $3
    AND plan_hash = $4
    AND node_id = $5
FOR UPDATE
`
	row, err := s.cfg.InternalExecutor.QueryRowEx(
		ctx,
		"fetch-stmt-stats",
		txn, /* txn */
		sessiondata.InternalExecutorOverride{
			User: security.NodeUserName(),
		},
		readStmt,                             // stmt
		serializedFingerprintID,              // fingerprint_id
		key.App,                              // app_name
		aggregatedTs,                         // aggregated_ts
		dummyPlanHash,                        // plan_hash
		s.cfg.SQLIDContainer.SQLInstanceID(), // node_id
	)

	if err != nil {
		return err
	}

	if row == nil {
		return errors.Errorf("statement statistics not found")
	}

	if len(row) != 2 {
		return errors.AssertionFailedf("unexpected number of returning columns")
	}

	count := int64(*row[0].(*tree.DInt))
	result.Count = count

	statistics, ok := row[1].(*tree.DJSON)
	if !ok {
		return errors.Errorf("corrupted data")
	}

	err = sqlstatsutil.DecodeStmtStatsStatisticsJSON(statistics.JSON, result)
	if err != nil {
		return err
	}

	return nil
}

func (s *PersistedSQLStats) prepareTransactionStatsInsertValues(
	stats *roachpb.CollectedTransactionStatistics,
) (metadata *tree.DJSON, statistics *tree.DJSON, err error) {
	// Prepare data for insertion.
	metadataJSON := sqlstatsutil.BuildTxnMetadataJSON(stats)
	metadata = tree.NewDJSON(metadataJSON)

	statisticsJSON, err := sqlstatsutil.BuildTxnStatisticsJSON(stats)
	if err != nil {
		return nil /* metadata */, nil /* statistics */, err
	}
	statistics = tree.NewDJSON(statisticsJSON)

	return metadata, statistics, nil
}

func (s *PersistedSQLStats) prepareStatementStatsInsertValues(
	stats *roachpb.CollectedStatementStatistics,
) (metadata *tree.DJSON, statistics *tree.DJSON, err error) {
	// Prepare data for insertion.
	metadataJSON, err := sqlstatsutil.BuildStmtMetadataJSON(stats)
	if err != nil {
		return nil /* metadata */, nil /* statistics */, err
	}
	metadata = tree.NewDJSON(metadataJSON)

	statisticsJSON, err := sqlstatsutil.BuildStmtStatisticsJSON(stats)
	if err != nil {
		return nil /* metadata */, nil /* statistics */, err
	}
	statistics = tree.NewDJSON(statisticsJSON)

	return metadata, statistics, nil
}
