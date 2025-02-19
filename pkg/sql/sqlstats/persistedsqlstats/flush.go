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
	"github.com/cockroachdb/errors"
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

		for _, statistics := range stmtStats {
			if err := doFlushSingleStmtStats(ctx, statistics, flushBucket, s.cfg.DB); err != nil {
				s.cfg.FlushesFailed.Inc(1)
				log.Warningf(ctx, "failed to flush statement statistics: %s", err)
			} else {
				s.cfg.FlushesSuccessful.Inc(1)
			}
		}
	})
	if err != nil {
		log.Warningf(ctx, "failed to execute sql-stmt-stats-flush task, %s", err.Error())
	}

	err = stopper.RunAsyncTask(ctx, "sql-txn-stats-flush", func(ctx context.Context) {
		defer wg.Done()

		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		for _, statistics := range txnStats {
			if err := doFlushSingleTxnStats(ctx, statistics, flushBucket, s.cfg.DB); err != nil {
				s.cfg.FlushesFailed.Inc(1)
				log.Warningf(ctx, "failed to flush transaction statistics: %s", err)
			} else {
				s.cfg.FlushesSuccessful.Inc(1)
			}
		}
	})
	if err != nil {
		log.Warningf(ctx, "failed to execute sql-txn-stats-flush task, %s", err.Error())
	}

	wg.Wait()
}

func doFlushSingleTxnStats(
	ctx context.Context,
	stats *appstatspb.CollectedTransactionStatistics,
	bucket *flushBucket,
	db isql.DB,
) error {
	return db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		serializedFingerprintID := sqlstatsutil.EncodeUint64ToBytes(uint64(stats.TransactionFingerprintID))

		err := upsertTransactionStats(ctx, txn, bucket, serializedFingerprintID, stats)
		if err != nil {
			return errors.Wrapf(err, "flushing transaction %d's statistics", stats.TransactionFingerprintID)
		}
		return nil
	}, isql.WithPriority(admissionpb.UserLowPri))
}

func doFlushSingleStmtStats(
	ctx context.Context,
	stats *appstatspb.CollectedStatementStatistics,
	flushBucket *flushBucket,
	db isql.DB,
) error {
	return db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		serializedFingerprintID := sqlstatsutil.EncodeUint64ToBytes(uint64(stats.ID))
		serializedTransactionFingerprintID := sqlstatsutil.EncodeUint64ToBytes(uint64(stats.Key.TransactionFingerprintID))
		serializedPlanHash := sqlstatsutil.EncodeUint64ToBytes(stats.Key.PlanHash)

		err := upsertStatementStats(
			ctx,
			txn,
			flushBucket,
			serializedFingerprintID,
			serializedTransactionFingerprintID,
			serializedPlanHash,
			stats,
		)
		if err != nil {
			return errors.Wrapf(err, "flush statement %d's statistics", stats.ID)
		}
		return nil
	}, isql.WithPriority(admissionpb.UserLowPri))
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

func upsertTransactionStats(
	ctx context.Context,
	txn isql.Txn,
	flushBucket *flushBucket,
	serializedFingerprintID []byte,
	stats *appstatspb.CollectedTransactionStatistics,
) error {

	// Prepare data for insertion.
	metadataJSON, err := sqlstatsutil.BuildTxnMetadataJSON(stats)
	if err != nil {
		return err
	}
	metadata := tree.NewDJSON(metadataJSON)

	statisticsJSON, err := sqlstatsutil.BuildTxnStatisticsJSON(stats)
	if err != nil {
		return err
	}
	statistics := tree.NewDJSON(statisticsJSON)

	_, err = txn.ExecParsed(
		ctx,
		"upsert-txn-stats",
		txn.KV(),
		sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
		upsertTxnStatsStmt,
		flushBucket.aggregatedTs, // aggregated_ts
		serializedFingerprintID,  // fingerprint_id
		stats.App,                // app_name
		flushBucket.nodeID,       // node_id
		flushBucket.aggInterval,  // agg_interval
		metadata,                 // metadata
		statistics,               // statistics
	)

	return err
}

func upsertStatementStats(
	ctx context.Context,
	txn isql.Txn,
	flushBucket *flushBucket,
	serializedFingerprintID []byte,
	serializedTransactionFingerprintID []byte,
	serializedPlanHash []byte,
	stats *appstatspb.CollectedStatementStatistics,
) error {

	// Prepare data for insertion.
	metadataJSON, err := sqlstatsutil.BuildStmtMetadataJSON(stats)
	if err != nil {
		return err
	}
	metadata := tree.NewDJSON(metadataJSON)

	statisticsJSON, err := sqlstatsutil.BuildStmtStatisticsJSON(&stats.Stats)
	if err != nil {
		return err
	}
	statistics := tree.NewDJSON(statisticsJSON)

	plan := tree.NewDJSON(sqlstatsutil.ExplainTreePlanNodeToJSON(&stats.Stats.SensitiveInfo.MostRecentPlanDescription))

	indexRecommendations := tree.NewDArray(types.String)
	for _, recommendation := range stats.Stats.IndexRecommendations {
		if err := indexRecommendations.Append(tree.NewDString(recommendation)); err != nil {
			return err
		}
	}

	args := append(make([]interface{}, 0, 11),
		flushBucket.aggregatedTs,           // aggregated_ts
		serializedFingerprintID,            // fingerprint_id
		serializedTransactionFingerprintID, // transaction_fingerprint_id
		serializedPlanHash,                 // plan_hash
		stats.Key.App,                      // app_name
		flushBucket.nodeID,                 // node_id
		flushBucket.aggInterval,            // agg_interval
		metadata,                           // metadata
		statistics,                         // statistics
		plan,                               // plan
		indexRecommendations,               // index_recommendations
	)

	_, err = txn.ExecParsed(
		ctx,
		"upsert-stmt-stats",
		txn.KV(), /* txn */
		sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
		upsertStmtStatsStmt,
		args...,
	)

	return err
}
