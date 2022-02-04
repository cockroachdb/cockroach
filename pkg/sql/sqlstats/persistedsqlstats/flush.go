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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Flush flushes in-memory sql stats into system table. Any errors encountered
// during the flush will be logged as warning.
func (s *PersistedSQLStats) Flush(ctx context.Context) {
	log.Infof(ctx, "flushing %d stmt/txn fingerprints (%d bytes) after %s",
		s.SQLStats.GetTotalFingerprintCount(), s.SQLStats.GetTotalFingerprintBytes(), timeutil.Since(s.lastFlushStarted))

	aggregatedTs := s.ComputeAggregatedTs()
	s.lastFlushStarted = s.getTimeNow()

	s.flushStmtStats(ctx, aggregatedTs)
	s.flushTxnStats(ctx, aggregatedTs)

	if err := s.SQLStats.Reset(ctx); err != nil {
		log.Warningf(ctx, "fail to reset in-memory SQL Stats: %s", err)
	}
}

func (s *PersistedSQLStats) flushStmtStats(ctx context.Context, aggregatedTs time.Time) {
	// s.doFlush directly logs errors if they are encountered. Therefore,
	// no error is returned here.
	_ = s.SQLStats.IterateStatementStats(ctx, &sqlstats.IteratorOptions{},
		func(ctx context.Context, statistics *roachpb.CollectedStatementStatistics) error {
			s.doFlush(ctx, func() error {
				return s.doFlushSingleStmtStats(ctx, statistics, aggregatedTs)
			}, "failed to flush statement statistics" /* errMsg */)

			return nil
		})

	if s.cfg.Knobs != nil && s.cfg.Knobs.OnStmtStatsFlushFinished != nil {
		s.cfg.Knobs.OnStmtStatsFlushFinished()
	}
}

func (s *PersistedSQLStats) flushTxnStats(ctx context.Context, aggregatedTs time.Time) {
	_ = s.SQLStats.IterateTransactionStats(ctx, &sqlstats.IteratorOptions{},
		func(ctx context.Context, statistics *roachpb.CollectedTransactionStatistics) error {
			s.doFlush(ctx, func() error {
				return s.doFlushSingleTxnStats(ctx, statistics, aggregatedTs)
			}, "failed to flush transaction statistics" /* errMsg */)

			return nil
		})

	if s.cfg.Knobs != nil && s.cfg.Knobs.OnTxnStatsFlushFinished != nil {
		s.cfg.Knobs.OnTxnStatsFlushFinished()
	}
}

func (s *PersistedSQLStats) doFlush(ctx context.Context, workFn func() error, errMsg string) {
	var err error
	flushBegin := s.getTimeNow()

	defer func() {
		if err != nil {
			s.cfg.FailureCounter.Inc(1)
			log.Warningf(ctx, "%s: %s", errMsg, err)
		}
		flushDuration := s.getTimeNow().Sub(flushBegin)
		s.cfg.FlushDuration.RecordValue(flushDuration.Nanoseconds())
		s.cfg.FlushCounter.Inc(1)
	}()

	err = workFn()
}

func (s *PersistedSQLStats) doFlushSingleTxnStats(
	ctx context.Context, stats *roachpb.CollectedTransactionStatistics, aggregatedTs time.Time,
) error {
	return s.cfg.KvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Explicitly copy the stats variable so the txn closure is retryable.
		scopedStats := *stats

		serializedFingerprintID := sqlstatsutil.EncodeUint64ToBytes(uint64(stats.TransactionFingerprintID))

		insertFn := func(ctx context.Context, txn *kv.Txn) (alreadyExists bool, err error) {
			rowsAffected, err := s.insertTransactionStats(ctx, txn, aggregatedTs, serializedFingerprintID, &scopedStats)

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
			err := s.fetchPersistedTransactionStats(ctx, txn, aggregatedTs, serializedFingerprintID, scopedStats.App, &persistedData)
			if err != nil {
				return err
			}

			scopedStats.Stats.Add(&persistedData)
			return nil
		}

		updateFn := func(ctx context.Context, txn *kv.Txn) error {
			return s.updateTransactionStats(ctx, txn, aggregatedTs, serializedFingerprintID, &scopedStats)
		}

		err := s.doInsertElseDoUpdate(ctx, txn, insertFn, readFn, updateFn)
		if err != nil {
			return errors.Wrapf(err, "flushing transaction %d's statistics", stats.TransactionFingerprintID)
		}
		return nil
	})
}

func (s *PersistedSQLStats) doFlushSingleStmtStats(
	ctx context.Context, stats *roachpb.CollectedStatementStatistics, aggregatedTs time.Time,
) error {
	return s.cfg.KvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Explicitly copy the stats so that this closure is retryable.
		scopedStats := *stats

		serializedFingerprintID := sqlstatsutil.EncodeUint64ToBytes(uint64(scopedStats.ID))
		serializedTransactionFingerprintID := sqlstatsutil.EncodeUint64ToBytes(uint64(scopedStats.Key.TransactionFingerprintID))
		serializedPlanHash := sqlstatsutil.EncodeUint64ToBytes(scopedStats.Key.PlanHash)

		insertFn := func(ctx context.Context, txn *kv.Txn) (alreadyExists bool, err error) {
			rowsAffected, err := s.insertStatementStats(
				ctx,
				txn,
				aggregatedTs,
				serializedFingerprintID,
				serializedTransactionFingerprintID,
				serializedPlanHash,
				&scopedStats,
			)

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
			err := s.fetchPersistedStatementStats(
				ctx,
				txn,
				aggregatedTs,
				serializedFingerprintID,
				serializedTransactionFingerprintID,
				serializedPlanHash,
				&scopedStats.Key,
				&persistedData,
			)
			if err != nil {
				return err
			}

			scopedStats.Stats.Add(&persistedData)
			return nil
		}

		updateFn := func(ctx context.Context, txn *kv.Txn) error {
			return s.updateStatementStats(
				ctx,
				txn,
				aggregatedTs,
				serializedFingerprintID,
				serializedTransactionFingerprintID,
				serializedPlanHash,
				&scopedStats,
			)
		}

		err := s.doInsertElseDoUpdate(ctx, txn, insertFn, readFn, updateFn)
		if err != nil {
			return errors.Wrapf(err, "flush statement %d's statistics", scopedStats.ID)
		}
		return nil
	})
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
			return err
		}
	}

	return nil
}

// ComputeAggregatedTs returns the aggregation timestamp to assign
// in-memory SQL stats during storage or aggregation.
func (s *PersistedSQLStats) ComputeAggregatedTs() time.Time {
	interval := SQLStatsAggregationInterval.Get(&s.cfg.Settings.SV)
	now := s.getTimeNow()

	aggTs := now.Truncate(interval)

	return aggTs
}

// GetAggregationInterval returns the current aggregation interval
// used by PersistedSQLStats.
func (s *PersistedSQLStats) GetAggregationInterval() time.Duration {
	return SQLStatsAggregationInterval.Get(&s.cfg.Settings.SV)
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
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_shard_8, aggregated_ts, fingerprint_id, app_name, node_id)
DO NOTHING
`

	aggInterval := s.GetAggregationInterval()

	// Prepare data for insertion.
	metadataJSON, err := sqlstatsutil.BuildTxnMetadataJSON(stats)
	if err != nil {
		return 0 /* rowsAffected */, err
	}
	metadata := tree.NewDJSON(metadataJSON)

	statisticsJSON, err := sqlstatsutil.BuildTxnStatisticsJSON(stats)
	if err != nil {
		return 0 /* rowsAffected */, err
	}
	statistics := tree.NewDJSON(statisticsJSON)

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
SET statistics = $1
WHERE fingerprint_id = $2
	AND aggregated_ts = $3
  AND app_name = $4
  AND node_id = $5
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
		serializedFingerprintID,              // fingerprint_id
		aggregatedTs,                         // aggregated_ts
		stats.App,                            // app_name
		s.cfg.SQLIDContainer.SQLInstanceID(), // node_id
	)

	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return errors.AssertionFailedf("failed to update transaction statistics for  fingerprint_id: %s, app: %s, aggregated_ts: %s, node_id: %d",
			serializedFingerprintID, stats.App, aggregatedTs,
			s.cfg.SQLIDContainer.SQLInstanceID())
	}

	return nil
}

func (s *PersistedSQLStats) updateStatementStats(
	ctx context.Context,
	txn *kv.Txn,
	aggregatedTs time.Time,
	serializedFingerprintID []byte,
	serializedTransactionFingerprintID []byte,
	serializedPlanHash []byte,
	stats *roachpb.CollectedStatementStatistics,
) error {
	updateStmt := `
UPDATE system.statement_statistics
SET statistics = $1
WHERE fingerprint_id = $2
  AND transaction_fingerprint_id = $3
	AND aggregated_ts = $4
  AND app_name = $5
  AND plan_hash = $6
  AND node_id = $7
`

	statisticsJSON, err := sqlstatsutil.BuildStmtStatisticsJSON(&stats.Stats)
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
		serializedFingerprintID,              // fingerprint_id
		serializedTransactionFingerprintID,   // transaction_fingerprint_id
		aggregatedTs,                         // aggregated_ts
		stats.Key.App,                        // app_name
		serializedPlanHash,                   // plan_hash
		s.cfg.SQLIDContainer.SQLInstanceID(), // node_id
	)

	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return errors.AssertionFailedf("failed to update statement statistics "+
			"for fingerprint_id: %s, "+
			"transaction_fingerprint_id: %s, "+
			"app: %s, "+
			"aggregated_ts: %s, "+
			"plan_hash: %d, "+
			"node_id: %d",
			serializedFingerprintID, serializedTransactionFingerprintID, stats.Key.App,
			aggregatedTs, serializedPlanHash, s.cfg.SQLIDContainer.SQLInstanceID())
	}

	return nil
}

func (s *PersistedSQLStats) insertStatementStats(
	ctx context.Context,
	txn *kv.Txn,
	aggregatedTs time.Time,
	serializedFingerprintID []byte,
	serializedTransactionFingerprintID []byte,
	serializedPlanHash []byte,
	stats *roachpb.CollectedStatementStatistics,
) (rowsAffected int, err error) {
	insertStmt := `
INSERT INTO system.statement_statistics
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_plan_hash_transaction_fingerprint_id_shard_8,
             aggregated_ts, fingerprint_id, transaction_fingerprint_id, app_name, plan_hash, node_id)
DO NOTHING
`
	aggInterval := s.GetAggregationInterval()

	// Prepare data for insertion.
	metadataJSON, err := sqlstatsutil.BuildStmtMetadataJSON(stats)
	if err != nil {
		return 0 /* rowsAffected */, err
	}
	metadata := tree.NewDJSON(metadataJSON)

	statisticsJSON, err := sqlstatsutil.BuildStmtStatisticsJSON(&stats.Stats)
	if err != nil {
		return 0 /* rowsAffected */, err
	}
	statistics := tree.NewDJSON(statisticsJSON)

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
		serializedTransactionFingerprintID,   // transaction_fingerprint_id
		serializedPlanHash,                   // plan_hash
		stats.Key.App,                        // app_name
		s.cfg.SQLIDContainer.SQLInstanceID(), // node_id
		aggInterval,                          // agg_interval
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
	// We use `SELECT ... FOR UPDATE` statement because we are going to perform
	// and `UPDATE` on the stats for the given fingerprint later.
	readStmt := `
SELECT
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
		return errors.AssertionFailedf("transaction statistics not found for fingerprint_id: %s, app: %s, aggregated_ts: %s, node_id: %d",
			serializedFingerprintID, appName, aggregatedTs,
			s.cfg.SQLIDContainer.SQLInstanceID())
	}

	if len(row) != 1 {
		return errors.AssertionFailedf("unexpectedly found %d returning columns for fingerprint_id: %s, app: %s, aggregated_ts: %s, node_id: %d",
			len(row), serializedFingerprintID, appName, aggregatedTs,
			s.cfg.SQLIDContainer.SQLInstanceID())
	}

	statistics := tree.MustBeDJSON(row[0])
	return sqlstatsutil.DecodeTxnStatsStatisticsJSON(statistics.JSON, result)
}

func (s *PersistedSQLStats) fetchPersistedStatementStats(
	ctx context.Context,
	txn *kv.Txn,
	aggregatedTs time.Time,
	serializedFingerprintID []byte,
	serializedTransactionFingerprintID []byte,
	serializedPlanHash []byte,
	key *roachpb.StatementStatisticsKey,
	result *roachpb.StatementStatistics,
) error {
	readStmt := `
SELECT
    statistics
FROM
    system.statement_statistics
WHERE fingerprint_id = $1
    AND transaction_fingerprint_id = $2
    AND app_name = $3
	  AND aggregated_ts = $4
    AND plan_hash = $5
    AND node_id = $6
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
		serializedTransactionFingerprintID,   // transaction_fingerprint_id
		key.App,                              // app_name
		aggregatedTs,                         // aggregated_ts
		serializedPlanHash,                   // plan_hash
		s.cfg.SQLIDContainer.SQLInstanceID(), // node_id
	)

	if err != nil {
		return err
	}

	if row == nil {
		return errors.AssertionFailedf(
			"statement statistics not found fingerprint_id: %s, app: %s, aggregated_ts: %s, plan_hash: %d, node_id: %d",
			serializedFingerprintID, key.App, aggregatedTs, serializedPlanHash, s.cfg.SQLIDContainer.SQLInstanceID())
	}

	if len(row) != 1 {
		return errors.AssertionFailedf("unexpectedly found %d returning columns for fingerprint_id: %s, app: %s, aggregated_ts: %s, plan_hash %d, node_id: %d",
			len(row), serializedFingerprintID, key.App, aggregatedTs, serializedPlanHash,
			s.cfg.SQLIDContainer.SQLInstanceID())
	}

	statistics := tree.MustBeDJSON(row[0])

	return sqlstatsutil.DecodeStmtStatsStatisticsJSON(statistics.JSON, result)
}
