// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package persistedsqlstats_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatstestutil"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestInformationSchemaSQLStatsViews exercises the behavioral contracts of the
// information_schema.crdb_statement_statistics and
// information_schema.crdb_transaction_statistics views that logictest cannot
// cover, since the persisted system tables are empty in logictest (no flush).
//
// Rather than drive a real workload and flush, the test seeds the persisted
// system tables directly with mocked rows. This gives precise, deterministic
// control over app_name, fingerprint IDs, and the metadata JSONB, which is what
// the view bodies key off of. Every assertion is scoped to a unique synthetic
// app_name so the subtests are independent of each other and of any background
// flush activity.
func TestInformationSchemaSQLStatsViews(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(s.SQLConn(t))
	ie := s.InternalExecutor().(isql.Executor)

	// Disable the periodic flush so the persisted tables only ever contain the
	// rows this test inserts. The test never flushes; it writes directly to the
	// system tables via the mocked-insert helpers.
	sqlDB.Exec(t, "SET CLUSTER SETTING sql.stats.flush.enabled = false")

	// aggTs is a fixed aggregation timestamp shared by all seeded rows. It is
	// part of the persisted tables' primary key but is otherwise irrelevant to
	// the contracts under test.
	aggTs := timeutil.Now().Truncate(time.Hour)

	// makeStmt returns a mocked statement-statistics row with the fields the
	// statement view reads set explicitly; all other fields are randomized.
	makeStmt := func(
		id int, app, query, summary, db string, txnFP int,
	) appstatspb.CollectedStatementStatistics {
		stmt := sqlstatstestutil.GetRandomizedCollectedStatementStatisticsForTest(t)
		stmt.ID = appstatspb.StmtFingerprintID(id)
		stmt.AggregatedTs = aggTs
		stmt.Key.App = app
		stmt.Key.TransactionFingerprintID = appstatspb.TransactionFingerprintID(txnFP)
		// query/querySummary/db land in the metadata JSONB, which is the
		// COALESCE fallback the statement view reads when system.statements has
		// no matching row.
		stmt.Key.Query = query
		stmt.Key.QuerySummary = summary
		stmt.Key.Database = db
		return stmt
	}

	// makeTxn returns a mocked transaction-statistics row. stmtFPs is encoded
	// into the metadata JSONB as stmtFingerprintIDs, which the transaction view
	// decodes into the stmt_fingerprint_ids BYTES[] column.
	makeTxn := func(
		txnFP int, app string, stmtFPs ...appstatspb.StmtFingerprintID,
	) appstatspb.CollectedTransactionStatistics {
		txn := sqlstatstestutil.GetRandomizedCollectedTransactionStatisticsForTest(t)
		txn.AggregatedTs = aggTs
		txn.App = app
		txn.TransactionFingerprintID = appstatspb.TransactionFingerprintID(txnFP)
		txn.StatementFingerprintIDs = stmtFPs
		return txn
	}

	insertStmts := func(stmts ...appstatspb.CollectedStatementStatistics) {
		require.NoError(t, sqlstatstestutil.InsertMockedIntoSystemStmtStats(ctx, ie, stmts, 1))
	}
	insertTxns := func(txns ...appstatspb.CollectedTransactionStatistics) {
		require.NoError(t, sqlstatstestutil.InsertMockedIntoSystemTxnStats(ctx, ie, txns, 1))
	}

	// The internal-app filter (WHERE app_name NOT LIKE '$ internal%') must drop
	// internal-workload rows from both views while keeping user-workload rows.
	t.Run("internal app filtered", func(t *testing.T) {
		const userApp = "test_app_filter"
		const internalApp = "$ internal-filter-test"

		insertStmts(
			makeStmt(101, userApp, "SELECT 1", "SELECT _", "defaultdb", 201),
			makeStmt(102, internalApp, "SELECT 2", "SELECT _", "defaultdb", 202),
		)
		insertTxns(
			makeTxn(201, userApp),
			makeTxn(202, internalApp),
		)

		// Both rows reach the base tables; the filter is what the view applies on
		// top, so checking the base tables first proves the view-level filtering
		// (rather than a missing insert) is what drops the internal-app row.
		sqlDB.CheckQueryResults(t,
			`SELECT app_name FROM system.statement_statistics
			 WHERE app_name IN ('test_app_filter', '$ internal-filter-test')
			 ORDER BY app_name`,
			[][]string{{internalApp}, {userApp}},
		)
		sqlDB.CheckQueryResults(t,
			`SELECT app_name FROM system.transaction_statistics
			 WHERE app_name IN ('test_app_filter', '$ internal-filter-test')
			 ORDER BY app_name`,
			[][]string{{internalApp}, {userApp}},
		)

		// Both apps are requested, but the view only returns the user app.
		sqlDB.CheckQueryResults(t,
			`SELECT app_name FROM information_schema.crdb_statement_statistics
			 WHERE app_name IN ('test_app_filter', '$ internal-filter-test')`,
			[][]string{{userApp}},
		)
		sqlDB.CheckQueryResults(t,
			`SELECT app_name FROM information_schema.crdb_transaction_statistics
			 WHERE app_name IN ('test_app_filter', '$ internal-filter-test')`,
			[][]string{{userApp}},
		)
	})

	// When system.statements has no row for a fingerprint, query/query_summary/
	// database fall back to the values stored in the statement_statistics
	// metadata JSONB.
	t.Run("coalesce falls back to metadata", func(t *testing.T) {
		const app = "test_app_coalesce"
		insertStmts(makeStmt(111, app, "SELECT meta_query", "SELECT meta_summary", "meta_db", 211))

		sqlDB.CheckQueryResults(t,
			`SELECT query, query_summary, database
			 FROM information_schema.crdb_statement_statistics
			 WHERE app_name = 'test_app_coalesce'`,
			[][]string{{"SELECT meta_query", "SELECT meta_summary", "meta_db"}},
		)
	})

	// When system.statements does have a matching row, its columns win over the
	// metadata fallback. The metadata query is set to a sentinel that must not
	// appear in the result.
	t.Run("join hit prefers statements table", func(t *testing.T) {
		const app = "test_app_join_hit"
		insertStmts(makeStmt(121, app, "metadata_query_not_used", "metadata_summary_not_used", "metadata_db_not_used", 221))

		_, err := ie.ExecEx(
			ctx,
			"insert-mock-statements",
			nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			`UPSERT INTO system.statements (fingerprint_id, fingerprint, summary, db, metadata)
			 VALUES ($1, $2, $3, $4, '{}'::JSONB)`,
			sqlstatsutil.EncodeUint64ToBytes(121),
			"SELECT stmt_table_query",
			"SELECT stmt_summary",
			"stmt_db",
		)
		require.NoError(t, err)

		sqlDB.CheckQueryResults(t,
			`SELECT query, query_summary, database
			 FROM information_schema.crdb_statement_statistics
			 WHERE app_name = 'test_app_join_hit'`,
			[][]string{{"SELECT stmt_table_query", "SELECT stmt_summary", "stmt_db"}},
		)
	})

	// A transaction's stmt_fingerprint_ids elements are byte-identical to the
	// statement view's fingerprint_id, so unnesting the array joins the two
	// views. This documents the cross-view join recipe.
	t.Run("cross view join on fingerprint id", func(t *testing.T) {
		const app = "test_app_xjoin"
		insertStmts(makeStmt(131, app, "SELECT joined", "SELECT _", "defaultdb", 231))
		insertTxns(makeTxn(231, app, appstatspb.StmtFingerprintID(131)))

		sqlDB.CheckQueryResults(t,
			`SELECT count(*)
			 FROM information_schema.crdb_statement_statistics AS s
			 WHERE s.app_name = 'test_app_xjoin'
			   AND s.fingerprint_id IN (
			     SELECT unnest(stmt_fingerprint_ids)
			     FROM information_schema.crdb_transaction_statistics
			     WHERE app_name = 'test_app_xjoin'
			   )`,
			[][]string{{"1"}},
		)
	})
}
