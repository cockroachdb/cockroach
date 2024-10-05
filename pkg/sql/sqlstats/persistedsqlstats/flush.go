// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package persistedsqlstats

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Flush flushes in-memory sql stats into a system table. Any errors encountered
// during the flush will be logged as warning.
func (s *PersistedSQLStats) Flush(ctx context.Context) {
	now := s.getTimeNow()

	allowDiscardWhenDisabled := DiscardInMemoryStatsWhenFlushDisabled.Get(&s.cfg.Settings.SV)
	minimumFlushInterval := MinimumInterval.Get(&s.cfg.Settings.SV)

	enabled := SQLStatsFlushEnabled.Get(&s.cfg.Settings.SV)
	flushingTooSoon := now.Before(s.lastFlushStarted.Add(minimumFlushInterval))

	// Handle wiping in-memory stats here, we only wipe in-memory stats under 2
	// circumstances:
	// 1. flush is enabled, and we are not early aborting the flush due to flushing
	//    too frequently.
	// 2. flush is disabled, but we allow discard in-memory stats when disabled.
	shouldWipeInMemoryStats := enabled && !flushingTooSoon
	shouldWipeInMemoryStats = shouldWipeInMemoryStats || (!enabled && allowDiscardWhenDisabled)

	if shouldWipeInMemoryStats {
		defer func() {
			if err := s.SQLStats.Reset(ctx); err != nil {
				log.Warningf(ctx, "fail to reset in-memory SQL Stats: %s", err)
			}
		}()
	}

	// Handle early abortion of the flush.
	if !enabled {
		return
	}

	if flushingTooSoon {
		log.Infof(ctx, "flush aborted due to high flush frequency. "+
			"The minimum interval between flushes is %s", minimumFlushInterval.String())
		return
	}

	fingerprintCount := s.SQLStats.GetTotalFingerprintCount()
	s.cfg.FlushedFingerprintCount.Inc(fingerprintCount)
	log.Infof(ctx, "flushing %d stmt/txn fingerprints (%d bytes) after %s",
		fingerprintCount, s.SQLStats.GetTotalFingerprintBytes(), timeutil.Since(s.lastFlushStarted))
	s.lastFlushStarted = now

	aggregatedTs := s.ComputeAggregatedTs()

	// We only check the statement count as there should always be at least as many statements as transactions.
	limitReached := false

	var err error
	if sqlStatsLimitTableSizeEnabled.Get(&s.SQLStats.GetClusterSettings().SV) {
		limitReached, err = s.StmtsLimitSizeReached(ctx)
	}

	if err != nil {
		log.Errorf(ctx, "encountered an error at flush, checking for statement statistics size limit: %v", err)
	}
	if limitReached {
		log.Infof(ctx, "unable to flush fingerprints because table limit was reached.")
	} else {
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			s.flushStmtStats(ctx, aggregatedTs)
		}()

		go func() {
			defer wg.Done()
			s.flushTxnStats(ctx, aggregatedTs)
		}()

		wg.Wait()
	}
}

func (s *PersistedSQLStats) StmtsLimitSizeReached(ctx context.Context) (bool, error) {
	// Doing a count check on every flush for every node adds a lot of overhead.
	// To reduce the overhead only do the check once an hour by default.
	intervalToCheck := SQLStatsLimitTableCheckInterval.Get(&s.cfg.Settings.SV)
	if !s.lastSizeCheck.IsZero() && s.lastSizeCheck.Add(intervalToCheck).After(timeutil.Now()) {
		log.Infof(ctx, "PersistedSQLStats.StmtsLimitSizeReached skipped with last check at: %s and check interval: %s", s.lastSizeCheck, intervalToCheck)
		return false, nil
	}

	maxPersistedRows := float64(SQLStatsMaxPersistedRows.Get(&s.cfg.Settings.SV))

	// The statistics table is split into 8 shards. Instead of counting all the
	// rows across all the shards the count can be limited to a single shard.
	// Then check the size off that one shard. This reduces the risk of causing
	// contention or serialization issues. The cleanup is done by the shard, so
	// it should prevent the data from being skewed to a single shard.
	randomShard := rand.Intn(systemschema.SQLStatsHashShardBucketCount)
	readStmt := fmt.Sprintf(`SELECT count(*)
      FROM system.statement_statistics
      %s
      WHERE crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_plan_hash_transaction_fingerprint_id_shard_8 = $1
`, s.cfg.Knobs.GetAOSTClause())

	row, err := s.cfg.DB.Executor().QueryRowEx(
		ctx,
		"fetch-stmt-count",
		nil,
		sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
		readStmt,
		randomShard,
	)

	if err != nil {
		return false, err
	}
	actualSize := float64(tree.MustBeDInt(row[0]))
	maxPersistedRowsByShard := maxPersistedRows / systemschema.SQLStatsHashShardBucketCount
	isSizeLimitReached := actualSize > (maxPersistedRowsByShard * 1.5)
	// If the table is over the limit do the check for every flush. This allows
	// the flush to start again as soon as the data is within limits instead of
	// needing to wait an hour.
	if !isSizeLimitReached {
		s.lastSizeCheck = timeutil.Now()
	}

	return isSizeLimitReached, nil
}

func (s *PersistedSQLStats) flushStmtStats(ctx context.Context, aggregatedTs time.Time) {
	// s.doFlush directly logs errors if they are encountered. Therefore,
	// no error is returned here.
	_ = s.SQLStats.IterateStatementStats(ctx, sqlstats.IteratorOptions{},
		func(ctx context.Context, statistics *appstatspb.CollectedStatementStatistics) error {
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
	_ = s.SQLStats.IterateTransactionStats(ctx, sqlstats.IteratorOptions{},
		func(ctx context.Context, statistics *appstatspb.CollectedTransactionStatistics) error {
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
	ctx context.Context, stats *appstatspb.CollectedTransactionStatistics, aggregatedTs time.Time,
) error {
	return s.cfg.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// Explicitly copy the stats variable so the txn closure is retryable.
		scopedStats := *stats

		serializedFingerprintID := sqlstatsutil.EncodeUint64ToBytes(uint64(stats.TransactionFingerprintID))

		insertFn := func(ctx context.Context, txn isql.Txn) (alreadyExists bool, err error) {
			rowsAffected, err := s.insertTransactionStats(ctx, txn, aggregatedTs, serializedFingerprintID, &scopedStats)

			if err != nil {
				return false /* alreadyExists */, err
			}

			if rowsAffected == 0 {
				return true /* alreadyExists */, nil /* err */
			}

			return false /* alreadyExists */, nil /* err */
		}

		readFn := func(ctx context.Context, txn isql.Txn) error {
			persistedData := appstatspb.TransactionStatistics{}
			err := s.fetchPersistedTransactionStats(ctx, txn, aggregatedTs, serializedFingerprintID, scopedStats.App, &persistedData)
			if err != nil {
				return err
			}

			scopedStats.Stats.Add(&persistedData)
			return nil
		}

		updateFn := func(ctx context.Context, txn isql.Txn) error {
			return s.updateTransactionStats(ctx, txn, aggregatedTs, serializedFingerprintID, &scopedStats)
		}

		err := s.doInsertElseDoUpdate(ctx, txn, insertFn, readFn, updateFn)
		if err != nil {
			return errors.Wrapf(err, "flushing transaction %d's statistics", stats.TransactionFingerprintID)
		}
		return nil
	}, isql.WithPriority(admissionpb.UserLowPri))
}

func (s *PersistedSQLStats) doFlushSingleStmtStats(
	ctx context.Context, stats *appstatspb.CollectedStatementStatistics, aggregatedTs time.Time,
) error {
	return s.cfg.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// Explicitly copy the stats so that this closure is retryable.
		scopedStats := *stats

		serializedFingerprintID := sqlstatsutil.EncodeUint64ToBytes(uint64(scopedStats.ID))
		serializedTransactionFingerprintID := sqlstatsutil.EncodeUint64ToBytes(uint64(scopedStats.Key.TransactionFingerprintID))
		serializedPlanHash := sqlstatsutil.EncodeUint64ToBytes(scopedStats.Key.PlanHash)

		insertFn := func(ctx context.Context, txn isql.Txn) (alreadyExists bool, err error) {
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

		readFn := func(ctx context.Context, txn isql.Txn) error {
			persistedData := appstatspb.StatementStatistics{}
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

		updateFn := func(ctx context.Context, txn isql.Txn) error {
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
	}, isql.WithPriority(admissionpb.UserLowPri))
}

func (s *PersistedSQLStats) doInsertElseDoUpdate(
	ctx context.Context,
	txn isql.Txn,
	insertFn func(context.Context, isql.Txn) (alreadyExists bool, err error),
	readFn func(context.Context, isql.Txn) error,
	updateFn func(context.Context, isql.Txn) error,
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
	txn isql.Txn,
	aggregatedTs time.Time,
	serializedFingerprintID []byte,
	stats *appstatspb.CollectedTransactionStatistics,
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

	nodeID := s.GetEnabledSQLInstanceID()
	rowsAffected, err = txn.ExecEx(
		ctx,
		"insert-txn-stats",
		txn.KV(),
		sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
		insertStmt,
		aggregatedTs,            // aggregated_ts
		serializedFingerprintID, // fingerprint_id
		stats.App,               // app_name
		nodeID,                  // node_id
		aggInterval,             // agg_interval
		metadata,                // metadata
		statistics,              // statistics
	)

	return rowsAffected, err
}
func (s *PersistedSQLStats) updateTransactionStats(
	ctx context.Context,
	txn isql.Txn,
	aggregatedTs time.Time,
	serializedFingerprintID []byte,
	stats *appstatspb.CollectedTransactionStatistics,
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

	nodeID := s.GetEnabledSQLInstanceID()
	rowsAffected, err := txn.ExecEx(
		ctx,
		"update-stmt-stats",
		txn.KV(), /* txn */
		sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
		updateStmt,
		statistics,              // statistics
		serializedFingerprintID, // fingerprint_id
		aggregatedTs,            // aggregated_ts
		stats.App,               // app_name
		nodeID,                  // node_id
	)

	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return errors.AssertionFailedf("failed to update transaction statistics for  fingerprint_id: %s, app: %s, aggregated_ts: %s, node_id: %d",
			serializedFingerprintID, stats.App, aggregatedTs, nodeID)
	}

	return nil
}

func (s *PersistedSQLStats) updateStatementStats(
	ctx context.Context,
	txn isql.Txn,
	aggregatedTs time.Time,
	serializedFingerprintID []byte,
	serializedTransactionFingerprintID []byte,
	serializedPlanHash []byte,
	stats *appstatspb.CollectedStatementStatistics,
) error {
	updateStmt := `
UPDATE system.statement_statistics
SET statistics = $1,
index_recommendations = $2
WHERE fingerprint_id = $3
  AND transaction_fingerprint_id = $4
	AND aggregated_ts = $5
  AND app_name = $6
  AND plan_hash = $7
  AND node_id = $8
`
	statisticsJSON, err := sqlstatsutil.BuildStmtStatisticsJSON(&stats.Stats)
	if err != nil {
		return err
	}
	statistics := tree.NewDJSON(statisticsJSON)
	indexRecommendations := tree.NewDArray(types.String)
	for _, recommendation := range stats.Stats.IndexRecommendations {
		if err := indexRecommendations.Append(tree.NewDString(recommendation)); err != nil {
			return err
		}
	}

	nodeID := s.GetEnabledSQLInstanceID()
	rowsAffected, err := txn.ExecEx(
		ctx,
		"update-stmt-stats",
		txn.KV(), /* txn */
		sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
		updateStmt,
		statistics,                         // statistics
		indexRecommendations,               // index_recommendations
		serializedFingerprintID,            // fingerprint_id
		serializedTransactionFingerprintID, // transaction_fingerprint_id
		aggregatedTs,                       // aggregated_ts
		stats.Key.App,                      // app_name
		serializedPlanHash,                 // plan_hash
		nodeID,                             // node_id
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
			aggregatedTs, serializedPlanHash, nodeID)
	}

	return nil
}

func (s *PersistedSQLStats) insertStatementStats(
	ctx context.Context,
	txn isql.Txn,
	aggregatedTs time.Time,
	serializedFingerprintID []byte,
	serializedTransactionFingerprintID []byte,
	serializedPlanHash []byte,
	stats *appstatspb.CollectedStatementStatistics,
) (rowsAffected int, err error) {

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
	nodeID := s.GetEnabledSQLInstanceID()

	indexRecommendations := tree.NewDArray(types.String)
	for _, recommendation := range stats.Stats.IndexRecommendations {
		if err := indexRecommendations.Append(tree.NewDString(recommendation)); err != nil {
			return 0, err
		}
	}

	values := "$1 ,$2, $3, $4, $5, $6, $7, $8, $9, $10, $11"
	args := append(make([]interface{}, 0, 11),
		aggregatedTs,                       // aggregated_ts
		serializedFingerprintID,            // fingerprint_id
		serializedTransactionFingerprintID, // transaction_fingerprint_id
		serializedPlanHash,                 // plan_hash
		stats.Key.App,                      // app_name
		nodeID,                             // node_id
		aggInterval,                        // agg_interval
		metadata,                           // metadata
		statistics,                         // statistics
		plan,                               // plan
		indexRecommendations,               // index_recommendations
	)

	insertStmt := fmt.Sprintf(`
INSERT INTO system.statement_statistics
VALUES (%s)
ON CONFLICT (crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_plan_hash_transaction_fingerprint_id_shard_8,
             aggregated_ts, fingerprint_id, transaction_fingerprint_id, app_name, plan_hash, node_id)
DO NOTHING
`, values)
	rowsAffected, err = txn.ExecEx(
		ctx,
		"insert-stmt-stats",
		txn.KV(), /* txn */
		sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
		insertStmt,
		args...,
	)

	return rowsAffected, err
}

func (s *PersistedSQLStats) fetchPersistedTransactionStats(
	ctx context.Context,
	txn isql.Txn,
	aggregatedTs time.Time,
	serializedFingerprintID []byte,
	appName string,
	result *appstatspb.TransactionStatistics,
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

	nodeID := s.GetEnabledSQLInstanceID()
	row, err := txn.QueryRowEx(
		ctx,
		"fetch-txn-stats",
		txn.KV(), /* txn */
		sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
		readStmt,                // stmt
		serializedFingerprintID, // fingerprint_id
		appName,                 // app_name
		aggregatedTs,            // aggregated_ts
		nodeID,                  // node_id
	)

	if err != nil {
		return err
	}

	if row == nil {
		return errors.AssertionFailedf("transaction statistics not found for fingerprint_id: %s, app: %s, aggregated_ts: %s, node_id: %d",
			serializedFingerprintID, appName, aggregatedTs,
			nodeID)
	}

	if len(row) != 1 {
		return errors.AssertionFailedf("unexpectedly found %d returning columns for fingerprint_id: %s, app: %s, aggregated_ts: %s, node_id: %d",
			len(row), serializedFingerprintID, appName, aggregatedTs,
			nodeID)
	}

	statistics := tree.MustBeDJSON(row[0])
	return sqlstatsutil.DecodeTxnStatsStatisticsJSON(statistics.JSON, result)
}

func (s *PersistedSQLStats) fetchPersistedStatementStats(
	ctx context.Context,
	txn isql.Txn,
	aggregatedTs time.Time,
	serializedFingerprintID []byte,
	serializedTransactionFingerprintID []byte,
	serializedPlanHash []byte,
	key *appstatspb.StatementStatisticsKey,
	result *appstatspb.StatementStatistics,
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
	nodeID := s.GetEnabledSQLInstanceID()
	row, err := txn.QueryRowEx(
		ctx,
		"fetch-stmt-stats",
		txn.KV(), /* txn */
		sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
		readStmt,                           // stmt
		serializedFingerprintID,            // fingerprint_id
		serializedTransactionFingerprintID, // transaction_fingerprint_id
		key.App,                            // app_name
		aggregatedTs,                       // aggregated_ts
		serializedPlanHash,                 // plan_hash
		nodeID,                             // node_id
	)

	if err != nil {
		return err
	}

	if row == nil {
		return errors.AssertionFailedf(
			"statement statistics not found fingerprint_id: %s, app: %s, aggregated_ts: %s, plan_hash: %d, node_id: %d",
			serializedFingerprintID, key.App, aggregatedTs, serializedPlanHash, nodeID)
	}

	if len(row) != 1 {
		return errors.AssertionFailedf("unexpectedly found %d returning columns for fingerprint_id: %s, app: %s, aggregated_ts: %s, plan_hash %d, node_id: %d",
			len(row), serializedFingerprintID, key.App, aggregatedTs, serializedPlanHash, nodeID)
	}

	statistics := tree.MustBeDJSON(row[0])

	return sqlstatsutil.DecodeStmtStatsStatisticsJSON(statistics.JSON, result)
}
