// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package persistedsqlstats

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type flushBucket struct {
	aggInterval  time.Duration
	aggregatedTs time.Time
	nodeID       base.SQLInstanceID
}

func (s *PersistedSQLStats) getBucket() flushBucket {
	return flushBucket{
		aggInterval:  s.GetAggregationInterval(),
		aggregatedTs: s.ComputeAggregatedTs(),
		nodeID:       s.GetEnabledSQLInstanceID(),
	}
}

// MaybeFlush flushes in-memory sql stats into a system table, returning true if the flush
// was attempted. Any errors encountered will be logged as warning. We may return
// without attempting to flush any sql stats if any of the following are true:
// 1. The flush is disabled by the cluster setting `sql.stats.flush.enabled`.
// 2. The flush is called too soon after the last flush (`sql.stats.flush.minimum_interval`).
// 3. We have reached the limit of the number of rows in the system table.
func (s *PersistedSQLStats) MaybeFlush(ctx context.Context, stopper *stop.Stopper) bool {
	return s.MaybeFlushWithDrainer(ctx, stopper, s.SQLStats)
}

func (s *PersistedSQLStats) MaybeFlushWithDrainer(
	ctx context.Context, stopper *stop.Stopper, ssDrainer sqlstats.SSDrainer,
) bool {
	now := s.getTimeNow()
	allowDiscardWhenDisabled := DiscardInMemoryStatsWhenFlushDisabled.Get(&s.cfg.Settings.SV)
	minimumFlushInterval := MinimumInterval.Get(&s.cfg.Settings.SV)

	enabled := SQLStatsFlushEnabled.Get(&s.cfg.Settings.SV)
	flushingTooSoon := now.Before(s.lastFlushStarted.Add(minimumFlushInterval))

	// Reset stats is performed individually for statement and transaction stats
	// within SSDrainer.DrainStats function. Here, we reset stats only when
	// sql stats flush is disabled.
	if !enabled && allowDiscardWhenDisabled {
		defer func() {
			if err := ssDrainer.Reset(ctx); err != nil {
				log.Warningf(ctx, "fail to reset SQL Stats: %s", err)
			}
		}()
	}

	// Handle early abortion of the flush.
	if !enabled {
		return false
	}

	if flushingTooSoon {
		log.Infof(ctx, "flush aborted due to high flush frequency. "+
			"The minimum interval between flushes is %s", minimumFlushInterval.String())
		return false
	}

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
		return false
	}

	lastFlush := s.lastFlushStarted
	s.lastFlushStarted = now

	flushBegin := s.getTimeNow()
	stmtStats, txnStats, fingerprintCount := ssDrainer.DrainStats(ctx)
	s.cfg.FlushedFingerprintCount.Inc(fingerprintCount)
	if log.V(1) {
		log.Infof(ctx, "flushing %d stmt/txn fingerprints (%d bytes) after %s",
			fingerprintCount, s.SQLStats.GetTotalFingerprintBytes(), timeutil.Since(lastFlush))
	}

	bucket := s.getBucket()
	s.flush(ctx, stopper, &bucket, stmtStats, txnStats)
	s.cfg.FlushLatency.RecordValue(s.getTimeNow().Sub(flushBegin).Nanoseconds())

	if s.cfg.Knobs != nil && s.cfg.Knobs.OnStmtStatsFlushFinished != nil {
		s.cfg.Knobs.OnStmtStatsFlushFinished()
	}

	if s.cfg.Knobs != nil && s.cfg.Knobs.OnTxnStatsFlushFinished != nil {
		s.cfg.Knobs.OnTxnStatsFlushFinished()
	}

	return true
}

func (s *PersistedSQLStats) StmtsLimitSizeReached(ctx context.Context) (bool, error) {
	// Doing a count check on every flush for every node adds a lot of overhead.
	// To reduce the overhead only do the check once an hour by default.
	intervalToCheck := SQLStatsLimitTableCheckInterval.Get(&s.cfg.Settings.SV)
	if !s.lastSizeCheck.IsZero() && s.lastSizeCheck.Add(intervalToCheck).After(timeutil.Now()) {
		if log.V(1) {
			log.Infof(ctx, "PersistedSQLStats.StmtsLimitSizeReached skipped with last check at: %s and check interval: %s", s.lastSizeCheck, intervalToCheck)
		}
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

func (s *PersistedSQLStats) flush(
	ctx context.Context,
	stopper *stop.Stopper,
	flushBucket *flushBucket,
	stmtStats []*appstatspb.CollectedStatementStatistics,
	txnStats []*appstatspb.CollectedTransactionStatistics,
) {
	if s.cfg.Knobs != nil && s.cfg.Knobs.FlushInterceptor != nil {
		s.cfg.Knobs.FlushInterceptor(ctx, stopper, flushBucket.aggregatedTs, stmtStats, txnStats)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)

	err := stopper.RunAsyncTask(ctx, "sql-stmt-stats-flush", func(ctx context.Context) {
		defer wg.Done()

		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()
		s.flushStmtStatsInBatches(ctx, stmtStats, flushBucket)
	})
	if err != nil {
		log.Warningf(ctx, "failed to execute sql-stmt-stats-flush task, %s", err.Error())
	}

	err = stopper.RunAsyncTask(ctx, "sql-txn-stats-flush", func(ctx context.Context) {
		defer wg.Done()

		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()
		s.flushTxnStatsInBatches(ctx, txnStats, flushBucket)
	})
	if err != nil {
		log.Warningf(ctx, "failed to execute sql-txn-stats-flush task, %s", err.Error())
	}

	wg.Wait()
}

func (s *PersistedSQLStats) flushTxnStatsInBatches(
	ctx context.Context, stats []*appstatspb.CollectedTransactionStatistics, flushBucket *flushBucket,
) {
	batchSize := int(SQLStatsFlushBatchSize.Get(&s.cfg.Settings.SV))
	for i := 0; i < len(stats); i += batchSize {
		end := i + batchSize
		if end > len(stats) {
			end = len(stats)
		}
		batch := stats[i:end]
		if err := doFlushTxnStats(ctx, batch, flushBucket, s.cfg.DB); err != nil {
			s.cfg.FlushesFailed.Inc(1)
			log.Warningf(ctx, "failed to flush transaction statistics: %s", err)
		} else {
			s.cfg.FlushesSuccessful.Inc(1)
		}
	}
}

func (s *PersistedSQLStats) flushStmtStatsInBatches(
	ctx context.Context, stats []*appstatspb.CollectedStatementStatistics, flushBucket *flushBucket,
) {
	batchSize := int(SQLStatsFlushBatchSize.Get(&s.cfg.Settings.SV))
	for i := 0; i < len(stats); i += batchSize {
		end := i + batchSize
		if end > len(stats) {
			end = len(stats)
		}
		batch := stats[i:end]
		if err := doFlushStmtStats(ctx, batch, flushBucket, s.cfg.DB); err != nil {
			s.cfg.FlushesFailed.Inc(1)
			log.Warningf(ctx, "failed to flush statement statistics: %s", err)
		} else {
			s.cfg.FlushesSuccessful.Inc(1)
		}
	}
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

const transactionStatisticUpsertQuery = `
INSERT INTO system.transaction_statistics as t
VALUES %s
ON CONFLICT (crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_shard_8, aggregated_ts, fingerprint_id, app_name, node_id)
DO UPDATE
SET
  statistics = crdb_internal.merge_transaction_stats(ARRAY(t.statistics, EXCLUDED.statistics))
`

func doFlushTxnStats(
	ctx context.Context,
	stats []*appstatspb.CollectedTransactionStatistics,
	bucket *flushBucket,
	db isql.DB,
) error {
	var args []interface{}
	placeholders := make([]string, 0, len(stats))
	for i, stat := range stats {
		serializedFingerprintID := sqlstatsutil.EncodeUint64ToBytes(uint64(stat.TransactionFingerprintID))

		// Prepare data for insertion.
		metadataJSON, err := sqlstatsutil.BuildTxnMetadataJSON(stat)
		if err != nil {
			return err
		}
		metadata := tree.NewDJSON(metadataJSON)

		statisticsJSON, err := sqlstatsutil.BuildTxnStatisticsJSON(stat)
		if err != nil {
			return err
		}
		statistics := tree.NewDJSON(statisticsJSON)

		placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			i*7+1, i*7+2, i*7+3, i*7+4, i*7+5, i*7+6, i*7+7))
		args = append(args,
			bucket.aggregatedTs,     // aggregated_ts
			serializedFingerprintID, // fingerprint_id
			stat.App,                // app_name
			bucket.nodeID,           // node_id
			bucket.aggInterval,      // agg_interval
			metadata,                // metadata
			statistics,              // statistics
		)
	}

	query := fmt.Sprintf(transactionStatisticUpsertQuery, strings.Join(placeholders, ", "))
	return db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := txn.ExecEx(ctx,
			"upsert-txn-stats",
			txn.KV(), /* txn */
			sessiondata.NodeUserWithLowUserPrioritySessionDataOverride, query, args...)
		return err
	}, isql.WithPriority(admissionpb.UserLowPri))
}

const statementStatisticUpsertQuery = `
INSERT INTO system.statement_statistics as s
VALUES %s
ON CONFLICT (crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_plan_hash_transaction_fingerprint_id_shard_8,
						 aggregated_ts, fingerprint_id, transaction_fingerprint_id, app_name, plan_hash, node_id)
DO UPDATE
SET
	statistics = crdb_internal.merge_statement_stats(ARRAY(s.statistics, EXCLUDED.statistics)),
	index_recommendations = EXCLUDED.index_recommendations
`

func doFlushStmtStats(
	ctx context.Context,
	stats []*appstatspb.CollectedStatementStatistics,
	flushBucket *flushBucket,
	db isql.DB,
) error {
	var args []interface{}
	placeholders := make([]string, 0, len(stats))
	for i, stat := range stats {

		serializedFingerprintID := sqlstatsutil.EncodeUint64ToBytes(uint64(stat.ID))
		serializedTransactionFingerprintID := sqlstatsutil.EncodeUint64ToBytes(uint64(stat.Key.TransactionFingerprintID))
		serializedPlanHash := sqlstatsutil.EncodeUint64ToBytes(stat.Key.PlanHash)

		metadataJSON, err := sqlstatsutil.BuildStmtMetadataJSON(stat)
		if err != nil {
			return err
		}
		metadata := tree.NewDJSON(metadataJSON)

		statisticsJSON, err := sqlstatsutil.BuildStmtStatisticsJSON(&stat.Stats)
		if err != nil {
			return err
		}
		statistics := tree.NewDJSON(statisticsJSON)

		plan := tree.NewDJSON(sqlstatsutil.ExplainTreePlanNodeToJSON(&stat.Stats.SensitiveInfo.MostRecentPlanDescription))

		indexRecommendations := tree.NewDArray(types.String)
		for _, recommendation := range stat.Stats.IndexRecommendations {
			if err := indexRecommendations.Append(tree.NewDString(recommendation)); err != nil {
				return err
			}
		}

		placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			i*11+1, i*11+2, i*11+3, i*11+4, i*11+5, i*11+6, i*11+7, i*11+8, i*11+9, i*11+10, i*11+11))

		args = append(args,
			flushBucket.aggregatedTs,           // aggregated_ts
			serializedFingerprintID,            // fingerprint_id
			serializedTransactionFingerprintID, // transaction_fingerprint_id
			serializedPlanHash,                 // plan_hash
			stat.Key.App,                       // app_name
			flushBucket.nodeID,                 // node_id
			flushBucket.aggInterval,            // agg_interval
			metadata,                           // metadata
			statistics,                         // statistics
			plan,                               // plan
			indexRecommendations,               // index_recommendations
		)
	}

	query := fmt.Sprintf(statementStatisticUpsertQuery, strings.Join(placeholders, ", "))
	return db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := txn.ExecEx(ctx,
			"upsert-stmt-stats",
			txn.KV(), /* txn */
			sessiondata.NodeUserWithLowUserPrioritySessionDataOverride, query, args...)
		return err
	}, isql.WithPriority(admissionpb.UserLowPri))
}
