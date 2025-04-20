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
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/safesql"
	"github.com/pkg/errors"
)

// GetRandomizedCollectedStatementStatisticsForTest returns a
// appstatspb.CollectedStatementStatistics with its fields randomly filled.
func GetRandomizedCollectedStatementStatisticsForTest(
	t testing.TB,
) (result appstatspb.CollectedStatementStatistics) {
	data := sqlstatsutil.GenRandomData()
	sqlstatsutil.FillObject(t, reflect.ValueOf(&result), &data)

	return result
}

// GetRandomizedCollectedTransactionStatisticsForTest returns a
// appstatspb.CollectedTransactionStatistics with its fields randomly filled.
func GetRandomizedCollectedTransactionStatisticsForTest(
	t testing.TB,
) (result appstatspb.CollectedTransactionStatistics) {
	data := sqlstatsutil.GenRandomData()
	sqlstatsutil.FillObject(t, reflect.ValueOf(&result), &data)

	return result
}

func InsertMockedIntoSystemStmtStats(
	ctx context.Context,
	ie isql.Executor,
	stmtStatsList []appstatspb.CollectedStatementStatistics,
	nodeID base.SQLInstanceID,
) error {
	if len(stmtStatsList) == 0 {
		return nil
	}

	aggIntervalVal := time.Hour

	// Initialize the query builder
	query := safesql.NewQuery()
	query.Append("UPSERT INTO system.statement_statistics ")
	query.Append("(aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash, app_name, node_id, agg_interval, metadata, statistics, plan) VALUES ")

	for i, stmtStats := range stmtStatsList {
		if i > 0 {
			query.Append(", ")
		}

		stmtFingerprint := sqlstatsutil.EncodeUint64ToBytes(uint64(stmtStats.ID))
		txnFingerprint := sqlstatsutil.EncodeUint64ToBytes(uint64(stmtStats.Key.TransactionFingerprintID))
		planHash := sqlstatsutil.EncodeUint64ToBytes(stmtStats.Key.PlanHash)

		metadataJSON, err := sqlstatsutil.BuildStmtMetadataJSON(&stmtStats)
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

		query.Append("($, $, $, $, $, $, $, $, $, $)",
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
	}

	// Execute the query
	_, err := ie.ExecEx(
		ctx,
		"insert-mock-stmt-stats-batch",
		nil,
		sessiondata.NodeUserSessionDataOverride,
		query.String(),
		query.QueryArguments()...,
	)

	return err
}

func InsertMockedIntoSystemTxnStats(
	ctx context.Context,
	ie isql.Executor,
	statsList []appstatspb.CollectedTransactionStatistics,
	nodeID base.SQLInstanceID,
) error {
	if len(statsList) == 0 {
		return nil
	}
	aggIntervalVal := time.Hour

	// Initialize the query builder
	query := safesql.NewQuery()
	query.Append("UPSERT INTO system.transaction_statistics ")
	query.Append("(aggregated_ts, fingerprint_id, app_name, node_id, agg_interval, metadata, statistics) VALUES ")

	for i, stats := range statsList {
		if i > 0 {
			query.Append(", ")
		}

		txnFingerprint := sqlstatsutil.EncodeUint64ToBytes(uint64(stats.TransactionFingerprintID))

		statisticsJSON, err := sqlstatsutil.BuildTxnStatisticsJSON(&stats)
		if err != nil {
			return err
		}
		statistics := tree.NewDJSON(statisticsJSON)

		metadataJSON, err := sqlstatsutil.BuildTxnMetadataJSON(&stats)
		if err != nil {
			return err
		}
		metadata := tree.NewDJSON(metadataJSON)

		query.Append("($, $, $, $, $, $, $)",
			stats.AggregatedTs, // aggregated_ts
			txnFingerprint,     // fingerprint_id
			stats.App,          // app_name
			nodeID,             // node_id
			aggIntervalVal,     // agg_interval
			metadata,           // metadata
			statistics,         // statistics
		)
	}

	// Execute the query
	_, err := ie.ExecEx(
		ctx,
		"insert-mock-txn-stats-batch",
		nil,
		sessiondata.NodeUserSessionDataOverride,
		query.String(),
		query.QueryArguments()...,
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

type comparisonFunc func(actual, expected int) (bool, error)

type StatementFilter struct {
	// Application the statement ran in.
	App string
	// The representative query.
	Query         string
	ExecCount     int
	AllowInternal bool
}

// waitForStatementStatsCount waits for statement statistics to match a specific condition
func waitForStatementStatsCount(
	t *testing.T,
	conn *sqlutils.SQLRunner,
	expectedCount int,
	comparisonFn comparisonFunc,
	filters ...StatementFilter,
) {
	t.Helper()

	var filter StatementFilter
	if len(filters) > 0 {
		filter = filters[0]
	}

	// Build the query.
	var query safesql.Query
	query.Append("SELECT count(*) ")

	if filter.ExecCount > 0 {
		query.Append(", COALESCE(sum((statistics -> 'statistics' ->> 'cnt')::INT), 0) AS exec_count")
	}

	query.Append(" FROM crdb_internal.cluster_statement_statistics WHERE 1=1")

	if filter.App == "" && !filter.AllowInternal {
		query.Append(" AND app_name NOT LIKE '%internal-%' ")
	} else if filter.App != "" {
		query.Append(" AND app_name = $", filter.App)
	}

	if filter.Query != "" {
		query.Append(" AND metadata ->> 'query' = $", filter.Query)
	}

	q := query.String()
	args := query.QueryArguments()
	testutils.SucceedsWithin(t, func() error {
		var count, execCount int
		row := conn.QueryRow(t, q, args...)
		if filter.ExecCount > 0 {
			row.Scan(&count, &execCount)
		} else {
			// We only need to scan the first column into count.
			row.Scan(&count)
		}
		matches, err := comparisonFn(count, expectedCount)
		if !matches {
			return err
		}

		if filter.ExecCount > 0 && execCount != filters[0].ExecCount {
			return errors.Errorf("expected exec count %d, got %d", filters[0].ExecCount, execCount)
		}

		return nil
	}, 5*time.Second)
}

// WaitForStatementStatsCountAtLeast waits for statement statistics count to be >= expectedCount
func WaitForStatementStatsCountAtLeast(
	t *testing.T, conn *sqlutils.SQLRunner, expectedCount int, filters ...StatementFilter,
) {
	t.Helper()
	waitForStatementStatsCount(t, conn, expectedCount,
		func(actual, expected int) (bool, error) {
			return actual >= expected, errors.Errorf("expected at least %d statement(s), got %d", expected, actual)
		},
		filters...)
}

// WaitForStatementStatsCountEqual waits for statement statistics count to be == expectedCount
func WaitForStatementStatsCountEqual(
	t *testing.T, conn *sqlutils.SQLRunner, expectedCount int, filters ...StatementFilter,
) {
	t.Helper()
	waitForStatementStatsCount(t, conn, expectedCount,
		func(actual, expected int) (bool, error) {
			return actual == expected, errors.Errorf("expected %d statement(s), got %d", expected, actual)
		},
		filters...)
}

type TransactionFilter struct {
	// Application the transaction ran in.
	App           string
	ExecCount     int
	AllowInternal bool
}

func waitForTransactionStatsCount(
	t *testing.T,
	conn *sqlutils.SQLRunner,
	expectedCount int,
	comparisonFn comparisonFunc,
	filters ...TransactionFilter,
) {
	t.Helper()

	var filter TransactionFilter
	if len(filters) > 0 {
		filter = filters[0]
	}
	verifyExecCount := filter.ExecCount > 0

	// Build the query.
	var query safesql.Query
	query.Append("SELECT count(*)")

	if verifyExecCount {
		query.Append(", COALESCE(sum((statistics -> 'statistics' ->> 'cnt')::INT), 0) AS exec_count")
	}

	query.Append(" FROM crdb_internal.cluster_transaction_statistics WHERE 1=1")

	if filter.App == "" && !filter.AllowInternal {
		query.Append(" AND app_name NOT LIKE '%internal-%' ")
	} else if filter.App != "" {
		query.Append(" AND app_name = $", filter.App)

	}

	q := query.String()
	args := query.QueryArguments()
	testutils.SucceedsWithin(t, func() error {
		var count, execCount int
		row := conn.QueryRow(t, q, args...)

		if verifyExecCount {
			row := conn.QueryRow(t, q, args...)
			row.Scan(&count, &execCount)
		} else {
			// We only need to scan the first column into count.
			row.Scan(&count)
		}

		matches, err := comparisonFn(count, expectedCount)
		if !matches {
			return err
		}
		if verifyExecCount && execCount != filters[0].ExecCount {
			return errors.Errorf("expected exec count %d, got %d", filters[0].ExecCount, execCount)
		}
		return nil
	}, 5*time.Second)
}

// WaitForTransactionStatsCountAtLeast waits for transaction statistics count to be >= expectedCount
func WaitForTransactionStatsCountAtLeast(
	t *testing.T, conn *sqlutils.SQLRunner, expectedCount int, filters ...TransactionFilter,
) {
	t.Helper()
	waitForTransactionStatsCount(t, conn, expectedCount,
		func(actual, expected int) (bool, error) {
			return actual >= expected, errors.Errorf("expected at least %d transaction(s), got %d", expected, actual)
		},
		filters...)
}

// WaitForTransactionStatsCountEqual waits for transaction statistics count to be == expectedCount
func WaitForTransactionStatsCountEqual(
	t *testing.T, conn *sqlutils.SQLRunner, expectedCount int, filters ...TransactionFilter,
) {
	t.Helper()
	waitForTransactionStatsCount(t, conn, expectedCount,
		func(actual, expected int) (bool, error) {
			return actual == expected, errors.Errorf("expected %d transaction(s), got %d", expected, actual)
		},
		filters...)
}
