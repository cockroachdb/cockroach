// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlstatstestutil

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/safesql"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// GetRandomizedCollectedStatementStatisticsForTest returns a
// appstatspb.CollectedStatementStatistics with its fields randomly filled.
func GetRandomizedCollectedStatementStatisticsForTest(
	t testing.TB,
) (result appstatspb.CollectedStatementStatistics) {
	data := sqlstatsutil.GenRandomData()
	sqlstatsutil.FillObject(t, reflect.ValueOf(&result), &data)
	result.Stats.Count = 1

	return result
}

// GetRandomizedCollectedTransactionStatisticsForTest returns a
// appstatspb.CollectedTransactionStatistics with its fields randomly filled.
func GetRandomizedCollectedTransactionStatisticsForTest(
	t testing.TB,
) (result appstatspb.CollectedTransactionStatistics) {
	data := sqlstatsutil.GenRandomData()
	sqlstatsutil.FillObject(t, reflect.ValueOf(&result), &data)
	result.Stats.Count = 1

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
	ie isql.Executor,
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
	ie isql.Executor,
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

// The below utilities are used to assist tests that want to verify the state
// of in-memory statement and transaciton statistics. The recording of sql
// execution statistics will be moved to be asynchronous from its execution,
// so existing and future tests will need to wait for their expected query to
// appear. In the future we'll also be introducing a testing knob to allow tests
// to record stats synchronously for testing purposes.

type countAssertionFn func(count int) (bool, string)

// StatementFilter allows callers to narrow down the results to fingerprints
// matching specific properties.
type StatementFilter struct {
	// Application the statement ran in.
	App string
	// The representative query.
	Query string
	// Only match queries that have been executed at least ExecCount times.
	ExecCount int
	// AllowInternal queries with internal app names to be included in the results.
	AllowInternal bool
}

func (f StatementFilter) String() string {
	var filters []string
	if f.App != "" {
		filters = append(filters, "app="+f.App)
	}
	if f.Query != "" {
		filters = append(filters, "query="+f.Query)
	}
	if f.ExecCount > 0 {
		filters = append(filters, fmt.Sprintf("exec_count=%d", f.ExecCount))
	}
	if f.AllowInternal {
		filters = append(filters, "allow_internal")
	}
	return strings.Join(filters, ",")
}

// waitForStatementStatsRows queries crdb_internal.cluster_statement_statistics
// which is a virtual table representing the in-memory statement statistics. The function
// will wait until the provided comparison function of the expected fingerprint count
// and actual count returns true. StatementFilter can be optionally provided to narrow
// down the results to fingerprints matching specific properties.
func waitForStatementStatsRows(
	t *testing.T,
	conn *sqlutils.SQLRunner,
	rowCountAssertion countAssertionFn,
	filters ...StatementFilter,
) {
	t.Helper()

	if len(filters) > 1 {
		t.Fatalf("only one filter is supported, got %d", len(filters))
	}
	var filter StatementFilter
	if len(filters) > 0 {
		filter = filters[0]
	}

	// Build the query.
	var query safesql.Query
	query.Append("SELECT count(*) ")

	if filter.ExecCount > 0 {
		query.Append(", COALESCE(sum((statistics -> 'statistics' ->> 'cnt')::INT), 0)")
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
		matches, err := rowCountAssertion(count)
		if !matches {
			return errors.Errorf("%s filters=%s", err, filter.String())
		}

		if filter.ExecCount > 0 && execCount < filter.ExecCount {
			return errors.Errorf("expected exec count of at least %d, got %d. filters=%s",
				filters[0].ExecCount, execCount, filter.String())
		}

		return nil
	}, 5*time.Second)
}

// WaitForStatementEntriesAtLeast waits for the count of statement stats entries to be >= expectedCount.
func WaitForStatementEntriesAtLeast(
	t *testing.T, conn *sqlutils.SQLRunner, expectedCount int, filters ...StatementFilter,
) {
	t.Helper()
	waitForStatementStatsRows(t, conn,
		func(actual int) (bool, string) {
			return actual >= expectedCount, fmt.Sprintf("expected at least %d statement(s), got %d", expectedCount, actual)
		},
		filters...)
}

// WaitForStatementEntriesEqual waits for the count of statement stats entries to be == expectedCount.
func WaitForStatementEntriesEqual(
	t *testing.T, conn *sqlutils.SQLRunner, expectedCount int, filters ...StatementFilter,
) {
	t.Helper()
	waitForStatementStatsRows(t, conn,
		func(actual int) (bool, string) {
			return actual == expectedCount, fmt.Sprintf("expected %d statement(s), got %d", expectedCount, actual)
		},
		filters...)
}

type TransactionFilter struct {
	// Application the transaction ran in.
	App           string
	ExecCount     int
	AllowInternal bool
}

func (f TransactionFilter) String() string {
	var filters []string
	if f.App != "" {
		filters = append(filters, "app="+f.App)
	}
	if f.ExecCount > 0 {
		filters = append(filters, fmt.Sprintf("exec_count=%d", f.ExecCount))
	}
	if f.AllowInternal {
		filters = append(filters, "allow_internal")
	}
	return strings.Join(filters, ",")
}

// waitForTransactionStatsRows queries crdb_internal.cluster_transaction_statistics
// which is a virtual table representing the in-memory transaction statistics. The function
// will wait until the comparison function of the expected fingerprint count and actual count
// returns true. TransactionFilter can be optionally provided to narrow down the results
// to fingerprints matching specific properties.
// provided to narrow down the results to fingerprints matching specific properties.
func waitForTransactionStatsRows(
	t *testing.T,
	conn *sqlutils.SQLRunner,
	rowCountAssertion countAssertionFn,
	filters ...TransactionFilter,
) {
	t.Helper()

	if len(filters) > 1 {
		t.Fatalf("only one filter is supported, got %d", len(filters))
	}
	var filter TransactionFilter
	if len(filters) > 0 {
		filter = filters[0]
	}
	verifyExecCount := filter.ExecCount > 0

	// Build the query.
	var query safesql.Query
	query.Append("SELECT count(*)")

	if verifyExecCount {
		query.Append(", COALESCE(sum((statistics -> 'statistics' ->> 'cnt')::INT), 0)")
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
			row.Scan(&count, &execCount)
		} else {
			// We only need to scan the first column into count.
			row.Scan(&count)
		}

		matches, err := rowCountAssertion(count)
		if !matches {
			return errors.Errorf("%s filters=%s", err, filter.String())
		}
		if verifyExecCount && execCount < filter.ExecCount {
			return errors.Errorf("expected exec count of at least %d, got %d", filters[0].ExecCount, execCount)
		}
		return nil
	}, 5*time.Second)
}

// WaitForTransactionEntriesAtLeast waits for transaction fingerprint to be >= expectedCount.
func WaitForTransactionEntriesAtLeast(
	t *testing.T, conn *sqlutils.SQLRunner, expectedCount int, filters ...TransactionFilter,
) {
	t.Helper()
	waitForTransactionStatsRows(t, conn,
		func(actual int) (bool, string) {
			return actual >= expectedCount, fmt.Sprintf("expected at least %d transaction(s), got %d", expectedCount, actual)
		},
		filters...)
}

// WaitForTransactionEntriesEqual waits for transaction fingerprint count to be == expectedCount.
func WaitForTransactionEntriesEqual(
	t *testing.T, conn *sqlutils.SQLRunner, expectedCount int, filters ...TransactionFilter,
) {
	t.Helper()
	waitForTransactionStatsRows(t, conn,
		func(actual int) (bool, string) {
			return actual == expectedCount, fmt.Sprintf("expected %d transaction(s), got %d", expectedCount, actual)
		},
		filters...)
}

// MakeObserverConnection creates a connection with the given pgURL, adding
// the application name '$ internal-stats-obs' to ensure queries run in this
// connection are given internal application names. This makes it easier to
// filter these queries out for testing.
func MakeObserverConnection(
	t *testing.T, pgURL url.URL,
) (runner *sqlutils.SQLRunner, cleanupFn func()) {
	q := pgURL.Query()
	q.Add("application_name", catconstants.InternalAppNamePrefix+"-stats-obs")
	pgURL.RawQuery = q.Encode()
	db, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	runner = sqlutils.MakeSQLRunner(db)
	cleanupFn = func() {
		db.Close()
	}
	return runner, cleanupFn
}
