// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlstatstestutil

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
)

// GetRandomizedCollectedStatementStatisticsForTest returns a
// appstatspb.CollectedStatementStatistics with its fields randomly filled.
func GetRandomizedCollectedStatementStatisticsForTest(
	t *testing.T,
) (result appstatspb.CollectedStatementStatistics) {
	data := sqlstatsutil.GenRandomData()
	sqlstatsutil.FillObject(t, reflect.ValueOf(&result), &data)

	return result
}

// GetRandomizedCollectedTransactionStatisticsForTest returns a
// appstatspb.CollectedTransactionStatistics with its fields randomly filled.
func GetRandomizedCollectedTransactionStatisticsForTest(
	t *testing.T,
) (result appstatspb.CollectedTransactionStatistics) {
	data := sqlstatsutil.GenRandomData()
	sqlstatsutil.FillObject(t, reflect.ValueOf(&result), &data)

	return result
}

func InsertMockedIntoSystemStmtStats(
	ctx context.Context,
	ie *sql.InternalExecutor,
	stmtStats *appstatspb.CollectedStatementStatistics,
	nodeID base.SQLInstanceID,
	aggInterval *time.Duration,
) error {
	if stmtStats == nil {
		return nil
	}

	aggIntervalVal := time.Hour
	if aggInterval != nil {
		aggIntervalVal = *aggInterval
	}

	stmtFingerprint := sqlstatsutil.EncodeUint64ToBytes(uint64(stmtStats.ID))
	txnFingerprint := sqlstatsutil.EncodeUint64ToBytes(uint64(stmtStats.Key.TransactionFingerprintID))
	planHash := sqlstatsutil.EncodeUint64ToBytes(stmtStats.Key.PlanHash)

	metadataJSON, err := sqlstatsutil.BuildStmtMetadataJSON(stmtStats)
	if err != nil {
		return err
	}

	statisticsJSON, err := sqlstatsutil.BuildStmtStatisticsJSON(&stmtStats.Stats)
	if err != nil {
		return err
	}
	statistics := tree.NewDJSON(statisticsJSON)

	plan := tree.NewDJSON(sqlstatsutil.ExplainTreePlanNodeToJSON(&stmtStats.Stats.SensitiveInfo.MostRecentPlanDescription))

	metadata := tree.NewDJSON(metadataJSON)

	_, err = ie.ExecEx(ctx, "insert-mock-stmt-stats", nil, sessiondata.NodeUserSessionDataOverride,
		`UPSERT INTO system.statement_statistics
VALUES ($1 ,$2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		stmtStats.AggregatedTs, // aggregated_ts
		stmtFingerprint,        // fingerprint_id
		txnFingerprint,         // transaction_fingerprint_id
		planHash,               // plan_hash
		stmtStats.Key.App,      // app_name
		nodeID,                 // node_id
		aggIntervalVal,         // agg_interval
		metadata,               // metadata
		statistics,             // statistics
		plan,                   // plan
	)

	return err
}

func InsertMockedIntoSystemTxnStats(
	ctx context.Context,
	ie *sql.InternalExecutor,
	stats *appstatspb.CollectedTransactionStatistics,
	nodeID base.SQLInstanceID,
	aggInterval *time.Duration,
) error {
	if stats == nil {
		return nil
	}

	aggIntervalVal := time.Hour
	if aggInterval != nil {
		aggIntervalVal = *aggInterval
	}

	txnFingerprint := sqlstatsutil.EncodeUint64ToBytes(uint64(stats.TransactionFingerprintID))

	statisticsJSON, err := sqlstatsutil.BuildTxnStatisticsJSON(stats)
	if err != nil {
		return err
	}
	statistics := tree.NewDJSON(statisticsJSON)

	metadataJSON, err := sqlstatsutil.BuildTxnMetadataJSON(stats)
	if err != nil {
		return err
	}
	metadata := tree.NewDJSON(metadataJSON)
	aggregatedTs := stats.AggregatedTs

	_, err = ie.ExecEx(ctx, "insert-mock-txn-stats", nil, sessiondata.NodeUserSessionDataOverride,
		` UPSERT INTO system.transaction_statistics
VALUES ($1 ,$2, $3, $4, $5, $6, $7)`,
		aggregatedTs,   // aggregated_ts
		txnFingerprint, // fingerprint_id
		stats.App,      // app_name
		nodeID,         // node_id
		aggIntervalVal, // agg_interval
		metadata,       // metadata
		statistics,     // statistics
	)

	return err
}

func InsertMockedIntoSystemStmtActivity(
	ctx context.Context,
	ie *sql.InternalExecutor,
	stmtStats *appstatspb.CollectedStatementStatistics,
	aggInterval *time.Duration,
) error {
	if stmtStats == nil {
		return nil
	}

	aggIntervalVal := time.Hour
	if aggInterval != nil {
		aggIntervalVal = *aggInterval
	}

	stmtFingerprint := sqlstatsutil.EncodeUint64ToBytes(uint64(stmtStats.ID))
	txnFingerprint := sqlstatsutil.EncodeUint64ToBytes(uint64(stmtStats.Key.TransactionFingerprintID))
	planHash := sqlstatsutil.EncodeUint64ToBytes(stmtStats.Key.PlanHash)

	statisticsJSON, err := sqlstatsutil.BuildStmtStatisticsJSON(&stmtStats.Stats)
	if err != nil {
		return err
	}
	statistics := tree.NewDJSON(statisticsJSON)

	plan := tree.NewDJSON(sqlstatsutil.ExplainTreePlanNodeToJSON(&stmtStats.Stats.SensitiveInfo.MostRecentPlanDescription))

	metadataJSON, err := sqlstatsutil.BuildStmtDetailsMetadataJSON(
		&appstatspb.AggregatedStatementMetadata{
			Query:          stmtStats.Key.Query,
			FormattedQuery: "",
			QuerySummary:   "",
			StmtType:       "",
			AppNames:       []string{stmtStats.Key.App},
			Databases:      []string{stmtStats.Key.Database},
			ImplicitTxn:    false,
			DistSQLCount:   0,
			FailedCount:    0,
			FullScanCount:  0,
			VecCount:       0,
			TotalCount:     0,
		})
	if err != nil {
		return err
	}
	metadata := tree.NewDJSON(metadataJSON)

	_, err = ie.ExecEx(ctx, "insert-mock-stmt-activity", nil, sessiondata.NodeUserSessionDataOverride,
		`UPSERT INTO system.statement_activity
VALUES ($1 ,$2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`,
		stmtStats.AggregatedTs, // aggregated_ts
		stmtFingerprint,        // fingerprint_id
		txnFingerprint,         // transaction_fingerprint_id
		planHash,               // plan_hash
		stmtStats.Key.App,      // app_name
		aggIntervalVal,         // agg_interval
		metadata,               // metadata
		statistics,             // statistics
		plan,                   // plan
		// TODO allow these values to be mocked. No need for them right now.
		[]string{}, // index_recommendations
		1,          // execution_count
		1,          // execution_total_seconds
		1,          // execution_total_cluster_seconds
		1,          // contention_time_avg_seconds
		1,          // cpu_sql_avg_nanos
		1,          //  service_latency_avg_seconds
		1,          // service_latency_p99_seconds
	)

	return err
}

func InsertMockedIntoSystemTxnActivity(
	ctx context.Context,
	ie *sql.InternalExecutor,
	stats *appstatspb.CollectedTransactionStatistics,
	aggInterval *time.Duration,
) error {
	if stats == nil {
		return nil
	}

	aggIntervalVal := time.Hour
	if aggInterval != nil {
		aggIntervalVal = *aggInterval
	}

	txnFingerprint := sqlstatsutil.EncodeUint64ToBytes(uint64(stats.TransactionFingerprintID))

	statisticsJSON, err := sqlstatsutil.BuildTxnStatisticsJSON(stats)
	if err != nil {
		return err
	}
	statistics := tree.NewDJSON(statisticsJSON)

	metadataJSON, err := sqlstatsutil.BuildTxnMetadataJSON(stats)
	if err != nil {
		return err
	}
	metadata := tree.NewDJSON(metadataJSON)
	aggregatedTs := stats.AggregatedTs

	_, err = ie.ExecEx(ctx, "insert-mock-txn-activity", nil, sessiondata.NodeUserSessionDataOverride,
		` UPSERT INTO system.transaction_activity
VALUES ($1 ,$2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
		aggregatedTs,   // aggregated_ts
		txnFingerprint, // fingerprint_id
		stats.App,      // app_name
		aggIntervalVal, // agg_interval
		metadata,       // metadata
		statistics,     // statistics
		// TODO (xinhaoz) allow mocking of these fields. Not necessary at the moment.
		"", // query
		1,  // execution_count
		1,  // execution_total_seconds
		1,  // execution_total_cluster_seconds
		1,  // contention_time_avg_seconds
		1,  // cpu_sql_avg_nanos
		1,  // service_latency_avg_seconds
		1,  // service_latency_p99_seconds
	)

	return err
}
