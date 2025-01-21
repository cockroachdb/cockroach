// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package application_api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatstestutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// additionalTimeoutUnderDuress is the additional timeout to use for the http
// client if under stress.
const additionalTimeoutUnderDuress = 30 * time.Second

func TestStatusAPICombinedTransactions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Increase the timeout for the http client if under stress.
	additionalTimeout := 0 * time.Second
	if skip.Duress() {
		additionalTimeout = additionalTimeoutUnderDuress
	}

	var params base.TestServerArgs
	params.Knobs.SpanConfig = &spanconfig.TestingKnobs{ManagerDisableJobCreation: true} // TODO(irfansharif): #74919.
	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: params,
	})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	thirdServer := testCluster.Server(2)
	firstServerProto := testCluster.Server(0)

	type testCase struct {
		query         string
		fingerprinted string
		count         int
		shouldRetry   bool
		numRows       int
	}

	testCases := []testCase{
		{query: `CREATE DATABASE roachblog`, count: 1, numRows: 0},
		{query: `SET database = roachblog`, count: 1, numRows: 0},
		{query: `CREATE TABLE posts (id INT8 PRIMARY KEY, body STRING)`, count: 1, numRows: 0},
		{
			query:         `INSERT INTO posts VALUES (1, 'foo')`,
			fingerprinted: `INSERT INTO posts VALUES (_, __more__)`,
			count:         1,
			numRows:       1,
		},
		{query: `SELECT * FROM posts`, count: 2, numRows: 1},
		{query: `BEGIN; SELECT * FROM posts; SELECT * FROM posts; COMMIT`, count: 3, numRows: 2},
		{
			query:         `BEGIN; SELECT crdb_internal.force_retry('2s'); SELECT * FROM posts; COMMIT;`,
			fingerprinted: `BEGIN; SELECT crdb_internal.force_retry(_); SELECT * FROM posts; COMMIT;`,
			shouldRetry:   true,
			count:         1,
			numRows:       2,
		},
		{
			query:         `BEGIN; SELECT crdb_internal.force_retry('5s'); SELECT * FROM posts; COMMIT;`,
			fingerprinted: `BEGIN; SELECT crdb_internal.force_retry(_); SELECT * FROM posts; COMMIT;`,
			shouldRetry:   true,
			count:         1,
			numRows:       2,
		},
	}

	appNameToTestCase := make(map[string]testCase)

	for i, tc := range testCases {
		appName := fmt.Sprintf("app%d", i)
		appNameToTestCase[appName] = tc

		// Create a brand new connection for each app, so that we don't pollute
		// transaction stats collection with `SET application_name` queries.
		sqlDB, err := thirdServer.ApplicationLayer().SQLConnE()
		if err != nil {
			t.Fatal(err)
		}

		if _, err := sqlDB.Exec(fmt.Sprintf(`SET application_name = "%s"`, appName)); err != nil {
			t.Fatal(err)
		}
		for c := 0; c < tc.count; c++ {
			if _, err := sqlDB.Exec(tc.query); err != nil {
				t.Fatal(err)
			}
		}
		if err := sqlDB.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// Hit query endpoint.
	var resp serverpb.StatementsResponse
	if err := srvtestutils.GetStatusJSONProtoWithAdminAndTimeoutOption(firstServerProto, "combinedstmts", &resp, true, additionalTimeout); err != nil {
		t.Fatal(err)
	}

	// Construct a map of all the statement fingerprint IDs.
	statementFingerprintIDs := make(map[appstatspb.StmtFingerprintID]bool, len(resp.Statements))
	for _, respStatement := range resp.Statements {
		statementFingerprintIDs[respStatement.ID] = true
	}

	respAppNames := make(map[string]bool)
	for _, respTransaction := range resp.Transactions {
		appName := respTransaction.StatsData.App
		tc, found := appNameToTestCase[appName]
		if !found {
			// Ignore internal queries, they aren't relevant to this test.
			continue
		}
		respAppNames[appName] = true
		// Ensure all statementFingerprintIDs comprised by the Transaction Response can be
		// linked to StatementFingerprintIDs for statements in the response.
		for _, stmtFingerprintID := range respTransaction.StatsData.StatementFingerprintIDs {
			if _, found := statementFingerprintIDs[stmtFingerprintID]; !found {
				t.Fatalf("app: %s, expected stmtFingerprintID: %d not found in StatementResponse.", appName, stmtFingerprintID)
			}
		}
		stats := respTransaction.StatsData.Stats
		if tc.count != int(stats.Count) {
			t.Fatalf("app: %s, expected count %d, got %d", appName, tc.count, stats.Count)
		}
		if tc.shouldRetry && respTransaction.StatsData.Stats.MaxRetries == 0 {
			t.Fatalf("app: %s, expected retries, got none\n", appName)
		}

		// Sanity check numeric stat values
		if respTransaction.StatsData.Stats.CommitLat.Mean <= 0 {
			t.Fatalf("app: %s, unexpected mean for commit latency\n", appName)
		}
		if respTransaction.StatsData.Stats.RetryLat.Mean <= 0 && tc.shouldRetry {
			t.Fatalf("app: %s, expected retry latency mean to be non-zero as retries were involved\n", appName)
		}
		if respTransaction.StatsData.Stats.ServiceLat.Mean <= 0 {
			t.Fatalf("app: %s, unexpected mean for service latency\n", appName)
		}
		if respTransaction.StatsData.Stats.NumRows.Mean != float64(tc.numRows) {
			t.Fatalf("app: %s, unexpected number of rows observed. expected: %d, got %d\n",
				appName, tc.numRows, int(respTransaction.StatsData.Stats.NumRows.Mean))
		}
	}

	// Ensure we got transaction statistics for all the queries we sent.
	for appName := range appNameToTestCase {
		if _, found := respAppNames[appName]; !found {
			t.Fatalf("app: %s did not appear in the response\n", appName)
		}
	}
}

func TestStatusAPITransactions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDeadlock(t, "test is very slow under deadlock")
	skip.UnderRace(t, "test is too slow to run under race")

	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	thirdServer := testCluster.Server(2)
	firstServerProto := testCluster.Server(0)

	type testCase struct {
		query         string
		fingerprinted string
		count         int
		shouldRetry   bool
		numRows       int
	}

	testCases := []testCase{
		{query: `CREATE DATABASE roachblog`, count: 1, numRows: 0},
		{query: `SET database = roachblog`, count: 1, numRows: 0},
		{query: `CREATE TABLE posts (id INT8 PRIMARY KEY, body STRING)`, count: 1, numRows: 0},
		{
			query:         `INSERT INTO posts VALUES (1, 'foo')`,
			fingerprinted: `INSERT INTO posts VALUES (_, __more__)`,
			count:         1,
			numRows:       1,
		},
		{query: `SELECT * FROM posts`, count: 2, numRows: 1},
		{query: `BEGIN; SELECT * FROM posts; SELECT * FROM posts; COMMIT`, count: 3, numRows: 2},
		{
			query:         `BEGIN; SELECT crdb_internal.force_retry('2s'); SELECT * FROM posts; COMMIT;`,
			fingerprinted: `BEGIN; SELECT crdb_internal.force_retry(_); SELECT * FROM posts; COMMIT;`,
			shouldRetry:   true,
			count:         1,
			numRows:       2,
		},
		{
			query:         `BEGIN; SELECT crdb_internal.force_retry('5s'); SELECT * FROM posts; COMMIT;`,
			fingerprinted: `BEGIN; SELECT crdb_internal.force_retry(_); SELECT * FROM posts; COMMIT;`,
			shouldRetry:   true,
			count:         1,
			numRows:       2,
		},
	}

	appNameToTestCase := make(map[string]testCase)

	for i, tc := range testCases {
		appName := fmt.Sprintf("app%d", i)
		appNameToTestCase[appName] = tc

		// Create a brand-new connection for each app, so that we don't pollute
		// transaction stats collection with `SET application_name` queries.
		sqlDB, err := thirdServer.ApplicationLayer().SQLConnE()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sqlDB.Exec(fmt.Sprintf(`SET application_name = "%s"`, appName)); err != nil {
			t.Fatal(err)
		}
		for c := 0; c < tc.count; c++ {
			if _, err := sqlDB.Exec(tc.query); err != nil {
				t.Fatal(err)
			}
		}
		if err := sqlDB.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// Hit query endpoint.
	var resp serverpb.StatementsResponse
	if err := srvtestutils.GetStatusJSONProto(firstServerProto, "statements", &resp); err != nil {
		t.Fatal(err)
	}

	// Construct a map of all the statement fingerprint IDs.
	statementFingerprintIDs := make(map[appstatspb.StmtFingerprintID]bool, len(resp.Statements))
	for _, respStatement := range resp.Statements {
		statementFingerprintIDs[respStatement.ID] = true
	}

	respAppNames := make(map[string]bool)
	for _, respTransaction := range resp.Transactions {
		appName := respTransaction.StatsData.App
		tc, found := appNameToTestCase[appName]
		if !found {
			// Ignore internal queries, they aren't relevant to this test.
			continue
		}
		respAppNames[appName] = true
		// Ensure all statementFingerprintIDs comprised by the Transaction Response can be
		// linked to StatementFingerprintIDs for statements in the response.
		for _, stmtFingerprintID := range respTransaction.StatsData.StatementFingerprintIDs {
			if _, found := statementFingerprintIDs[stmtFingerprintID]; !found {
				t.Fatalf("app: %s, expected stmtFingerprintID: %d not found in StatementResponse.", appName, stmtFingerprintID)
			}
		}
		stats := respTransaction.StatsData.Stats
		if tc.count != int(stats.Count) {
			t.Fatalf("app: %s, expected count %d, got %d", appName, tc.count, stats.Count)
		}
		if tc.shouldRetry && respTransaction.StatsData.Stats.MaxRetries == 0 {
			t.Fatalf("app: %s, expected retries, got none\n", appName)
		}

		// Sanity check numeric stat values
		if respTransaction.StatsData.Stats.CommitLat.Mean <= 0 {
			t.Fatalf("app: %s, unexpected mean for commit latency\n", appName)
		}
		if respTransaction.StatsData.Stats.RetryLat.Mean <= 0 && tc.shouldRetry {
			t.Fatalf("app: %s, expected retry latency mean to be non-zero as retries were involved\n", appName)
		}
		if respTransaction.StatsData.Stats.ServiceLat.Mean <= 0 {
			t.Fatalf("app: %s, unexpected mean for service latency\n", appName)
		}
		if respTransaction.StatsData.Stats.NumRows.Mean != float64(tc.numRows) {
			t.Fatalf("app: %s, unexpected number of rows observed. expected: %d, got %d\n",
				appName, tc.numRows, int(respTransaction.StatsData.Stats.NumRows.Mean))
		}
	}

	// Ensure we got transaction statistics for all the queries we sent.
	for appName := range appNameToTestCase {
		if _, found := respAppNames[appName]; !found {
			t.Fatalf("app: %s did not appear in the response\n", appName)
		}
	}
}

func TestStatusAPITransactionStatementFingerprintIDsTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.Background())

	firstServerProto := testCluster.Server(0).ApplicationLayer()
	thirdServerSQL := sqlutils.MakeSQLRunner(testCluster.ServerConn(2))
	testingApp := "testing"

	thirdServerSQL.Exec(t, `CREATE DATABASE db; CREATE TABLE db.t();`)
	thirdServerSQL.Exec(t, fmt.Sprintf(`SET application_name = "%s"`, testingApp))

	maxStmtFingerprintIDsLen := int(sqlstats.TxnStatsNumStmtFingerprintIDsToRecord.Get(
		&firstServerProto.ExecutorConfig().(sql.ExecutorConfig).Settings.SV))

	// Construct 2 transaction queries that include an absurd number of statements.
	// These two queries have the same first 1000 statements, but should still have
	// different fingerprints, as fingerprints take into account all
	// statementFingerprintIDs (unlike the statementFingerprintIDs stored on the
	// proto response, which are capped).
	testQuery1 := "BEGIN;"
	for i := 0; i < maxStmtFingerprintIDsLen+1; i++ {
		testQuery1 += "SELECT * FROM db.t;"
	}
	testQuery2 := testQuery1 + "SELECT * FROM db.t; COMMIT;"
	testQuery1 += "COMMIT;"

	thirdServerSQL.Exec(t, testQuery1)
	thirdServerSQL.Exec(t, testQuery2)

	// Hit query endpoint.
	var resp serverpb.StatementsResponse
	if err := srvtestutils.GetStatusJSONProto(firstServerProto, "statements", &resp); err != nil {
		t.Fatal(err)
	}

	txnsFound := 0
	for _, respTransaction := range resp.Transactions {
		appName := respTransaction.StatsData.App
		if appName != testingApp {
			// Only testQuery1 and testQuery2 are relevant to this test.
			continue
		}

		txnsFound++
		if len(respTransaction.StatsData.StatementFingerprintIDs) != maxStmtFingerprintIDsLen {
			t.Fatalf("unexpected length of StatementFingerprintIDs. expected:%d, got:%d",
				maxStmtFingerprintIDsLen, len(respTransaction.StatsData.StatementFingerprintIDs))
		}
	}
	if txnsFound != 2 {
		t.Fatalf("transactions were not disambiguated as expected. expected %d txns, got: %d",
			2, txnsFound)
	}
}

func TestStatusAPIStatements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Increase the timeout for the http client if under stress.
	additionalTimeout := 0 * time.Second
	if skip.Duress() {
		additionalTimeout = additionalTimeoutUnderDuress
	}

	// Aug 30 2021 19:50:00 GMT+0000
	aggregatedTs := int64(1630353000)
	statsKnobs := sqlstats.CreateTestingKnobs()
	statsKnobs.StubTimeNow = func() time.Time { return timeutil.Unix(aggregatedTs, 0) }
	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: statsKnobs,
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true, // TODO(irfansharif): #74919.
				},
			},
		},
	})
	defer testCluster.Stopper().Stop(context.Background())

	firstServerProto := testCluster.Server(0).ApplicationLayer()
	thirdServerSQL := sqlutils.MakeSQLRunner(testCluster.ServerConn(2))

	statements := []struct {
		stmt          string
		fingerprinted string
	}{
		{stmt: `CREATE DATABASE roachblog`},
		{stmt: `SET database = roachblog`},
		{stmt: `CREATE TABLE posts (id INT8 PRIMARY KEY, body STRING)`},
		{
			stmt:          `INSERT INTO posts VALUES (1, 'foo')`,
			fingerprinted: `INSERT INTO posts VALUES (_, __more__)`,
		},
		{stmt: `SELECT * FROM posts`},
	}

	for _, stmt := range statements {
		thirdServerSQL.Exec(t, stmt.stmt)
	}

	// Test that non-admin without VIEWACTIVITY privileges cannot access.
	var resp serverpb.StatementsResponse
	err := srvtestutils.GetStatusJSONProtoWithAdminAndTimeoutOption(firstServerProto, "statements", &resp, false, additionalTimeout)
	if !testutils.IsError(err, "status: 403") {
		t.Fatalf("expected privilege error, got %v", err)
	}

	testPath := func(path string, expectedStmts []string) {
		// Hit query endpoint.
		if err := srvtestutils.GetStatusJSONProtoWithAdminAndTimeoutOption(firstServerProto, path, &resp, false, additionalTimeout); err != nil {
			t.Fatal(err)
		}

		// See if the statements returned are what we executed.
		var statementsInResponse []string
		for _, respStatement := range resp.Statements {
			if respStatement.Stats.FailureCount > 0 {
				// We ignore failed statements here as the INSERT statement can fail and
				// be automatically retried, confusing the test success check.
				continue
			}
			if strings.HasPrefix(respStatement.Key.KeyData.App, catconstants.InternalAppNamePrefix) {
				// We ignore internal queries, these are not relevant for the
				// validity of this test.
				continue
			}
			if strings.HasPrefix(respStatement.Key.KeyData.Query, "ALTER USER") {
				// Ignore the ALTER USER ... VIEWACTIVITY statement.
				continue
			}
			statementsInResponse = append(statementsInResponse, respStatement.Key.KeyData.Query)
		}

		sort.Strings(expectedStmts)
		sort.Strings(statementsInResponse)

		if !reflect.DeepEqual(expectedStmts, statementsInResponse) {
			t.Fatalf("expected queries\n\n%v\n\ngot queries\n\n%v\n%s",
				expectedStmts, statementsInResponse, pretty.Sprint(resp))
		}
	}

	var expectedStatements []string
	for _, stmt := range statements {
		var expectedStmt = stmt.stmt
		if stmt.fingerprinted != "" {
			expectedStmt = stmt.fingerprinted
		}
		expectedStatements = append(expectedStatements, expectedStmt)
	}

	// Grant VIEWACTIVITY.
	thirdServerSQL.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITY", apiconstants.TestingUserNameNoAdmin().Normalized()))

	// Test no params.
	testPath("statements", expectedStatements)
	// Test combined=true forwards to CombinedStatements
	testPath(fmt.Sprintf("statements?combined=true&start=%d", aggregatedTs+60), nil)

	// Remove VIEWACTIVITY so we can test with just the VIEWACTIVITYREDACTED role.
	thirdServerSQL.Exec(t, fmt.Sprintf("ALTER USER %s NOVIEWACTIVITY", apiconstants.TestingUserNameNoAdmin().Normalized()))
	// Grant VIEWACTIVITYREDACTED.
	thirdServerSQL.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITYREDACTED", apiconstants.TestingUserNameNoAdmin().Normalized()))

	// Test no params.
	testPath("statements", expectedStatements)
	// Test combined=true forwards to CombinedStatements
	testPath(fmt.Sprintf("statements?combined=true&start=%d", aggregatedTs+60), nil)
}

type testStatement struct {
	query                  string
	fingerprintID          appstatspb.StmtFingerprintID
	aggregatedTs           time.Time
	count                  int64
	serviceLatency         appstatspb.NumericStat
	addStatementStatistics bool
	addStatementActivity   bool
}

func TestStatusAPICombinedStatementsTotalLatency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "test is too slow to run under race")
	// Increase the timeout for the http client if under stress.
	additionalTimeout := 0 * time.Second
	if skip.Duress() {
		additionalTimeout = additionalTimeoutUnderDuress
	}

	sqlStatsKnobs := sqlstats.CreateTestingKnobs()
	// Start the cluster.
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Insecure: true,
		Knobs: base.TestingKnobs{
			SQLStatsKnobs: sqlStatsKnobs,
		},
	})

	defer srv.Stopper().Stop(context.Background())
	defer sqlDB.Close()
	ts := srv.ApplicationLayer()
	db := sqlutils.MakeSQLRunner(sqlDB)

	// Disabled flush so no extra data is inserted to the system table.
	db.Exec(t, "SET CLUSTER SETTING sql.stats.activity.flush.enabled = 'f'")
	// Give permission to write to system tables.
	db.Exec(t, "INSERT INTO system.users VALUES ('node', NULL, true, 3)")
	db.Exec(t, "GRANT node TO root")

	stmts := []testStatement{
		{
			query:                  "SELECT * FROM foo",
			fingerprintID:          appstatspb.StmtFingerprintID(1088747230083123385),
			aggregatedTs:           timeutil.FromUnixMicros(1704103200000000), // January 1, 2024 10:00:00
			count:                  1,
			serviceLatency:         appstatspb.NumericStat{Mean: 3, SquaredDiffs: 0},
			addStatementStatistics: true,
			addStatementActivity:   false,
		},
		{
			query:                  "SELECT * FROM bar",
			fingerprintID:          appstatspb.StmtFingerprintID(2422963415274537466),
			aggregatedTs:           timeutil.FromUnixMicros(1704106800000000), // January 1, 2024 11:00:00
			count:                  5,
			serviceLatency:         appstatspb.NumericStat{Mean: 7, SquaredDiffs: 0},
			addStatementStatistics: true,
			addStatementActivity:   false,
		},
		{
			query:                  "SELECT * FROM abc",
			fingerprintID:          appstatspb.StmtFingerprintID(8810317408726404693),
			aggregatedTs:           timeutil.FromUnixMicros(1704110400000000), // January 1, 2024 12:00:00
			count:                  11,
			serviceLatency:         appstatspb.NumericStat{Mean: 13, SquaredDiffs: 0},
			addStatementStatistics: true,
			addStatementActivity:   true,
		},
		{
			query:                  "SELECT * FROM xyz",
			fingerprintID:          appstatspb.StmtFingerprintID(2759320105256699956),
			aggregatedTs:           timeutil.FromUnixMicros(1704114000000000), // January 1, 2024 13:00:00
			count:                  17,
			serviceLatency:         appstatspb.NumericStat{Mean: 19, SquaredDiffs: 0},
			addStatementStatistics: true,
			addStatementActivity:   true,
		},
	}

	for _, stmt := range stmts {
		s := generateStatement()
		s.AggregatedTs = stmt.aggregatedTs
		s.ID = stmt.fingerprintID
		s.Key.Query = stmt.query
		s.Key.QuerySummary = stmt.query
		s.Stats.Count = stmt.count
		s.Stats.ServiceLat = stmt.serviceLatency

		if stmt.addStatementStatistics {
			insertStatementIntoSystemStmtStatsTable(t, db, s)
		}
		if stmt.addStatementActivity {
			insertStatementIntoSystemStmtActivityTable(t, db, s)
		}
	}

	testCases := []struct {
		testName     string
		start        string
		end          string
		count        int
		totalLatency float32
		sourceTable  string
	}{
		{
			testName:     "period with one statement on statement_statistics and none on statement_activity",
			start:        "1704103200", // January 1, 2024 10:00:00
			end:          "1704104400", // January 1, 2024 10:20:00
			count:        1,
			totalLatency: getTotalLatencyForStatements(stmts[0]),
			sourceTable:  "crdb_internal.statement_statistics_persisted",
		},
		{
			testName:     "period with 2 statements on statement_statistics and none on statement_activity",
			start:        "1704103200", // January 1, 2024 10:00:00
			end:          "1704109200", // January 1, 2024 11:40:00
			count:        2,
			totalLatency: getTotalLatencyForStatements(stmts[0], stmts[1]),
			sourceTable:  "crdb_internal.statement_statistics_persisted",
		},
		{
			testName:     "period with one statement on statement_activity",
			start:        "1704110400", // January 1, 2024 12:00:00
			end:          "1704112800", // January 1, 2024 12:40:00
			count:        1,
			totalLatency: getTotalLatencyForStatements(stmts[2]),
			sourceTable:  "crdb_internal.statement_activity",
		},
		{
			testName:     "period with 2 statements on statement_activity",
			start:        "1704110400", // January 1, 2024 12:00:00
			end:          "1704116400", // January 1, 2024 13:40:00
			count:        2,
			totalLatency: getTotalLatencyForStatements(stmts[2], stmts[3]),
			sourceTable:  "crdb_internal.statement_activity",
		},
		{
			testName:     "period with 2 statements on statement_activity and 4 statements on statement_statistics",
			start:        "1704103200", // January 1, 2024 10:00:00
			end:          "1704116400", // January 1, 2024 13:40:00
			count:        4,
			totalLatency: getTotalLatencyForStatements(stmts[0], stmts[1], stmts[2], stmts[3]),
			sourceTable:  "crdb_internal.statement_statistics_persisted",
		},
	}

	for _, tc := range testCases {
		var resp serverpb.StatementsResponse
		endpoint := fmt.Sprintf("combinedstmts?start=%s&end=%s", tc.start, tc.end)
		err := srvtestutils.GetStatusJSONProtoWithAdminAndTimeoutOption(ts, endpoint, &resp, true, additionalTimeout)
		require.NoError(t, err, fmt.Sprintf("server error on %s", tc.testName))
		require.Equal(t, tc.sourceTable, resp.StmtsSourceTable, fmt.Sprintf("source table error on %s", tc.testName))
		require.Equal(t, tc.count, len(resp.Statements), fmt.Sprintf("stmts length error on %s", tc.testName))
		require.Equal(t, tc.totalLatency, resp.StmtsTotalRuntimeSecs, fmt.Sprintf("total latency on %s", tc.testName))
	}

}

func getTotalLatencyForStatements(statements ...testStatement) float32 {
	total := float32(0)
	for _, statement := range statements {
		total = total + float32(statement.count)*float32(statement.serviceLatency.Mean)
	}
	return total
}

func TestStatusAPICombinedStatementsWithFullScans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Increase the timeout for the http client if under stress.
	additionalTimeout := 0 * time.Second
	if skip.Duress() {
		additionalTimeout = additionalTimeoutUnderDuress
	}
	skip.UnderRace(t, "test is too slow to run under race")

	// Aug 30 2021 19:50:00 GMT+0000
	aggregatedTs := int64(1630353000)
	oneMinAfterAggregatedTs := aggregatedTs + 60
	statsKnobs := sqlstats.CreateTestingKnobs()
	statsKnobs.StubTimeNow = func() time.Time { return timeutil.Unix(aggregatedTs, 0) }
	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: statsKnobs,
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true,
				},
			},
		},
	})
	defer testCluster.Stopper().Stop(context.Background())

	endpoint := fmt.Sprintf("combinedstmts?start=%d&end=%d", aggregatedTs-3600, oneMinAfterAggregatedTs)
	findJobQuery := "SELECT status FROM crdb_internal.jobs WHERE statement = 'CREATE INDEX idx_age ON football.public.players (age) STORING (name)';"
	testAppName := "TestCombinedStatementsWithFullScans"

	firstServerProto := testCluster.Server(0).ApplicationLayer()
	sqlSB := testCluster.ServerConn(0)
	thirdServerSQL := sqlutils.MakeSQLRunner(testCluster.ServerConn(2))

	var resp serverpb.StatementsResponse
	// Test that non-admin without VIEWACTIVITY privileges cannot access.
	err := srvtestutils.GetStatusJSONProtoWithAdminOption(firstServerProto, endpoint, &resp, false)
	if !testutils.IsError(err, "status: 403") {
		t.Fatalf("expected privilege error, got %v", err)
	}

	thirdServerSQL.Exec(t, fmt.Sprintf("GRANT SYSTEM VIEWACTIVITY TO %s", apiconstants.TestingUserNameNoAdmin().Normalized()))
	thirdServerSQL.Exec(t, fmt.Sprintf(`SET application_name = '%s'`, testAppName))

	type TestCases struct {
		stmt      string
		respQuery string
		fullScan  bool
		distSQL   bool
		failed    bool
		count     int
	}

	// These test statements are executed before the index is introduced.
	statementsBeforeIndex := []TestCases{
		{stmt: `CREATE DATABASE football`, respQuery: `CREATE DATABASE football`, fullScan: false, distSQL: false, failed: false, count: 1},
		{stmt: `SET database = football`, respQuery: `SET database = football`, fullScan: false, distSQL: false, failed: false, count: 1},
		{stmt: `CREATE TABLE players (id INT PRIMARY KEY, name TEXT, position TEXT, age INT,goals INT)`, respQuery: `CREATE TABLE players (id INT8 PRIMARY KEY, name STRING, "position" STRING, age INT8, goals INT8)`, fullScan: false, distSQL: false, failed: false, count: 1},
		{stmt: `INSERT INTO players (id, name, position, age, goals) VALUES (1, 'Lionel Messi', 'Forward', 34, 672), (2, 'Cristiano Ronaldo', 'Forward', 36, 674)`, respQuery: `INSERT INTO players(id, name, "position", age, goals) VALUES (_, __more__), (__more__)`, fullScan: false, distSQL: false, failed: false, count: 1},
		{stmt: `SELECT avg(goals) FROM players`, respQuery: `SELECT avg(goals) FROM players`, fullScan: true, distSQL: true, failed: false, count: 1},
		{stmt: `SELECT name FROM players WHERE age >= 32`, respQuery: `SELECT name FROM players WHERE age >= _`, fullScan: true, distSQL: true, failed: false, count: 1},
	}

	statementsCreateIndex := []TestCases{
		// Drop the index, if it exists. Then, create the index.
		{stmt: `DROP INDEX IF EXISTS idx_age`, respQuery: `DROP INDEX IF EXISTS idx_age`, fullScan: false, distSQL: false, failed: false, count: 1},
		{stmt: `CREATE INDEX idx_age ON players (age) STORING (name)`, respQuery: `CREATE INDEX idx_age ON players (age) STORING (name)`, fullScan: false, distSQL: false, failed: false, count: 1},
	}

	// These test statements are executed after an index is created on the players table.
	statementsAfterIndex := []TestCases{
		// Since the index is created, the fullScan value should be false.
		{stmt: `SELECT name FROM players WHERE age < 32`, respQuery: `SELECT name FROM players WHERE age < _`, fullScan: false, distSQL: false, failed: false, count: 1},
		// Although the index is created, the fullScan value should be true because the previous query was not using the index. Its count should also be 2.
		{stmt: `SELECT name FROM players WHERE age >= 32`, respQuery: `SELECT name FROM players WHERE age >= _`, fullScan: true, distSQL: true, failed: false, count: 2},
	}

	type ExpectedStatementData struct {
		count    int
		fullScan bool
		distSQL  bool
	}

	// expectedStatementStatsMap maps the query response format to the associated
	// expected statement statistics for verification.
	expectedStatementStatsMap := make(map[string]ExpectedStatementData)

	// Helper function to execute the statements and store the expected statement
	executeStatements := func(statements []TestCases) {
		// Clear the map at the start of each execution batch.
		expectedStatementStatsMap = make(map[string]ExpectedStatementData)
		for _, stmt := range statements {
			thirdServerSQL.Exec(t, stmt.stmt)
			expectedStatementStatsMap[stmt.respQuery] = ExpectedStatementData{
				fullScan: stmt.fullScan,
				distSQL:  stmt.distSQL,
				count:    stmt.count,
			}
		}
	}

	// Helper function to convert a response into a JSON string representation.
	responseToJSON := func(resp interface{}) string {
		bytes, err := json.Marshal(resp)
		if err != nil {
			t.Fatal(err)
		}
		return string(bytes)
	}

	// Helper function to verify the combined statement statistics response.
	verifyCombinedStmtStats := func() {
		err := srvtestutils.GetStatusJSONProtoWithAdminAndTimeoutOption(firstServerProto, endpoint, &resp, false, additionalTimeout)
		require.NoError(t, err)

		// actualResponseStatsMap maps the query response format to the actual
		// statement statistics received from the server response.
		actualResponseStatsMap := make(map[string]serverpb.StatementsResponse_CollectedStatementStatistics)
		for _, respStatement := range resp.Statements {
			// Skip failed statements: The test app may encounter transient 40001
			// errors that are automatically retried. Thus, we only consider
			// statements that were that were successfully executed by the test app
			// to avoid counting such failures. If a statement that we expect to be
			// successful is not found in the response, the test will fail later.
			if respStatement.Key.KeyData.App == testAppName && respStatement.Stats.FailureCount == 0 {
				actualResponseStatsMap[respStatement.Key.KeyData.Query] = respStatement
			}
		}

		for respQuery, expectedData := range expectedStatementStatsMap {
			respStatement, exists := actualResponseStatsMap[respQuery]
			require.True(t, exists, "Expected statement '%s' not found in response: %v", respQuery, responseToJSON(resp))

			actualCount := respStatement.Stats.FirstAttemptCount
			actualFullScan := respStatement.Key.KeyData.FullScan
			actualDistSQL := respStatement.Key.KeyData.DistSQL

			stmtJSONString := responseToJSON(respStatement)

			require.Equal(t, expectedData.fullScan, actualFullScan, "failed for respStatement: %v", stmtJSONString)
			require.Equal(t, expectedData.distSQL, actualDistSQL, "failed for respStatement: %v", stmtJSONString)
			require.Equal(t, expectedData.count, int(actualCount), "failed for respStatement: %v", stmtJSONString)
		}
	}

	// Execute and verify the queries that will be executed before the index is created.
	executeStatements(statementsBeforeIndex)
	verifyCombinedStmtStats()

	// Execute the queries that will create the index.
	executeStatements(statementsCreateIndex)

	// Wait for the job which creates the index to complete.
	testutils.SucceedsWithin(t, func() error {
		var status string
		for {
			row := sqlSB.QueryRow(findJobQuery)
			err = row.Scan(&status)
			if err != nil {
				return err
			}
			if status == "succeeded" {
				break
			}
			// sleep for a fraction of a second
			time.Sleep(100 * time.Millisecond)
		}
		return nil
	}, 3*time.Second)

	// Execute and verify the queries that will be executed after the index is created.
	executeStatements(statementsAfterIndex)
	verifyCombinedStmtStats()
}

func TestStatusAPICombinedStatements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Resource-intensive test, times out under stress.
	skip.UnderRace(t, "expensive tests")

	// Aug 30 2021 19:50:00 GMT+0000
	aggregatedTs := int64(1630353000)
	statsKnobs := sqlstats.CreateTestingKnobs()
	statsKnobs.StubTimeNow = func() time.Time { return timeutil.Unix(aggregatedTs, 0) }
	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: statsKnobs,
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true, // TODO(irfansharif): #74919.
				},
			},
		},
	})
	defer testCluster.Stopper().Stop(context.Background())

	firstServerProto := testCluster.Server(0).ApplicationLayer()
	thirdServerSQL := sqlutils.MakeSQLRunner(testCluster.ServerConn(2))

	statements := []struct {
		stmt          string
		fingerprinted string
	}{
		{stmt: `CREATE DATABASE roachblog`},
		{stmt: `SET database = roachblog`},
		{stmt: `CREATE TABLE posts (id INT8 PRIMARY KEY, body STRING)`},
		{
			stmt:          `INSERT INTO posts VALUES (1, 'foo')`,
			fingerprinted: `INSERT INTO posts VALUES (_, __more__)`,
		},
		{stmt: `SELECT * FROM posts`},
	}

	for _, stmt := range statements {
		thirdServerSQL.Exec(t, stmt.stmt)
	}

	var resp serverpb.StatementsResponse
	// Test that non-admin without VIEWACTIVITY privileges cannot access.
	err := srvtestutils.GetStatusJSONProtoWithAdminOption(firstServerProto, "combinedstmts", &resp, false)
	if !testutils.IsError(err, "status: 403") {
		t.Fatalf("expected privilege error, got %v", err)
	}

	verifyStmts := func(path string, expectedStmts []string, hasTxns bool, t *testing.T) {
		// Hit query endpoint.
		if err := srvtestutils.GetStatusJSONProtoWithAdminOption(firstServerProto, path, &resp, false); err != nil {
			t.Fatal(err)
		}

		// See if the statements returned are what we executed.
		var statementsInResponse []string
		expectedTxnFingerprints := map[appstatspb.TransactionFingerprintID]struct{}{}
		for _, respStatement := range resp.Statements {
			if respStatement.Stats.FailureCount > 0 {
				// We ignore failed statements here as the INSERT statement can fail and
				// be automatically retried, confusing the test success check.
				continue
			}
			if strings.HasPrefix(respStatement.Key.KeyData.App, catconstants.InternalAppNamePrefix) {
				// CombinedStatementStats should filter out internal queries.
				t.Fatalf("unexpected internal query: %s", respStatement.Key.KeyData.Query)
			}
			if strings.HasPrefix(respStatement.Key.KeyData.Query, "ALTER USER") {
				// Ignore the ALTER USER ... VIEWACTIVITY statement.
				continue
			}

			statementsInResponse = append(statementsInResponse, respStatement.Key.KeyData.Query)
			for _, txnFingerprintID := range respStatement.TxnFingerprintIDs {
				expectedTxnFingerprints[txnFingerprintID] = struct{}{}
			}
		}

		for _, respTxn := range resp.Transactions {
			delete(expectedTxnFingerprints, respTxn.StatsData.TransactionFingerprintID)
		}
		// We set the default value of Transaction Fingerprint as 0 for some cases,
		// so this also needs to be removed from the list.
		delete(expectedTxnFingerprints, 0)

		sort.Strings(expectedStmts)
		sort.Strings(statementsInResponse)

		if !reflect.DeepEqual(expectedStmts, statementsInResponse) {
			t.Fatalf("expected queries\n\n%v\n\ngot queries\n\n%v\n%s\n path: %s",
				expectedStmts, statementsInResponse, pretty.Sprint(resp), path)
		}
		if hasTxns {
			// We expect that expectedTxnFingerprints is now empty since
			// we should have removed them all.
			assert.Empty(t, expectedTxnFingerprints)
		} else {
			assert.Empty(t, resp.Transactions)
		}
	}

	var expectedStatements []string
	for _, stmt := range statements {
		var expectedStmt = stmt.stmt
		if stmt.fingerprinted != "" {
			expectedStmt = stmt.fingerprinted
		}
		expectedStatements = append(expectedStatements, expectedStmt)
	}

	oneMinAfterAggregatedTs := aggregatedTs + 60

	t.Run("fetch_mode=combined, VIEWACTIVITY", func(t *testing.T) {
		// Grant VIEWACTIVITY.
		thirdServerSQL.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITY", apiconstants.TestingUserNameNoAdmin().Normalized()))

		// Test with no query params.
		verifyStmts("combinedstmts", expectedStatements, true, t)
		// Test with end = 1 min after aggregatedTs; should give the same results as get all.
		verifyStmts(fmt.Sprintf("combinedstmts?end=%d", oneMinAfterAggregatedTs), expectedStatements, true, t)
		// Test with start = 1 hour before aggregatedTs  end = 1 min after aggregatedTs; should give same results as get all.
		verifyStmts(fmt.Sprintf("combinedstmts?start=%d&end=%d", aggregatedTs-3600, oneMinAfterAggregatedTs),
			expectedStatements, true, t)
		// Test with start = 1 min after aggregatedTs; should give no results
		verifyStmts(fmt.Sprintf("combinedstmts?start=%d", oneMinAfterAggregatedTs), nil, true, t)
	})

	t.Run("fetch_mode=combined, VIEWACTIVITYREDACTED", func(t *testing.T) {
		// Remove VIEWACTIVITY so we can test with just the VIEWACTIVITYREDACTED role.
		thirdServerSQL.Exec(t, fmt.Sprintf("ALTER USER %s NOVIEWACTIVITY", apiconstants.TestingUserNameNoAdmin().Normalized()))
		// Grant VIEWACTIVITYREDACTED.
		thirdServerSQL.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITYREDACTED", apiconstants.TestingUserNameNoAdmin().Normalized()))

		// Test with no query params.
		verifyStmts("combinedstmts", expectedStatements, true, t)
		// Test with end = 1 min after aggregatedTs; should give the same results as get all.
		verifyStmts(fmt.Sprintf("combinedstmts?end=%d", oneMinAfterAggregatedTs), expectedStatements, true, t)
		// Test with start = 1 hour before aggregatedTs  end = 1 min after aggregatedTs; should give same results as get all.
		verifyStmts(fmt.Sprintf("combinedstmts?start=%d&end=%d", aggregatedTs-3600, oneMinAfterAggregatedTs), expectedStatements, true, t)
		// Test with start = 1 min after aggregatedTs; should give no results
		verifyStmts(fmt.Sprintf("combinedstmts?start=%d", oneMinAfterAggregatedTs), nil, true, t)
	})

	t.Run("fetch_mode=StmtsOnly", func(t *testing.T) {
		verifyStmts("combinedstmts?fetch_mode.stats_type=0", expectedStatements, false, t)
	})

	t.Run("fetch_mode=TxnsOnly with limit", func(t *testing.T) {
		// Verify that we only return stmts for the txns in the response.
		// We'll add a limit in a later commit to help verify this behaviour.
		if err := srvtestutils.GetStatusJSONProtoWithAdminOption(firstServerProto, "combinedstmts?fetch_mode.stats_type=1&limit=2",
			&resp, false); err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 2, len(resp.Transactions))
		stmtFingerprintIDs := map[appstatspb.StmtFingerprintID]struct{}{}
		for _, txn := range resp.Transactions {
			for _, stmtFingerprint := range txn.StatsData.StatementFingerprintIDs {
				stmtFingerprintIDs[stmtFingerprint] = struct{}{}
			}
		}

		for _, stmt := range resp.Statements {
			if _, ok := stmtFingerprintIDs[stmt.ID]; !ok {
				t.Fatalf("unexpected stmt; stmt unrelated to a txn int he response: %s", stmt.Key.KeyData.Query)
			}
		}
	})
}

func TestStatusAPIStatementDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// The liveness session might expire before the stress race can finish.
	skip.UnderRace(t, "expensive tests")

	// Aug 30 2021 19:50:00 GMT+0000
	aggregatedTs := int64(1630353000)
	statsKnobs := sqlstats.CreateTestingKnobs()
	statsKnobs.StubTimeNow = func() time.Time { return timeutil.Unix(aggregatedTs, 0) }
	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: statsKnobs,
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true,
				},
			},
		},
	})
	defer testCluster.Stopper().Stop(context.Background())

	firstServerProto := testCluster.Server(0).ApplicationLayer()
	thirdServerSQL := sqlutils.MakeSQLRunner(testCluster.ServerConn(2))

	statements := []string{
		`set application_name = 'first-app'`,
		`CREATE DATABASE roachblog`,
		`SET database = roachblog`,
		`CREATE TABLE posts (id INT8 PRIMARY KEY, body STRING)`,
		`INSERT INTO posts VALUES (1, 'foo')`,
		`INSERT INTO posts VALUES (2, 'foo')`,
		`INSERT INTO posts VALUES (3, 'foo')`,
		`SELECT * FROM posts`,
	}

	for _, stmt := range statements {
		thirdServerSQL.Exec(t, stmt)
	}

	query := `INSERT INTO posts VALUES (_, __more__)`
	fingerprintID := appstatspb.ConstructStatementFingerprintID(query, true, `roachblog`)
	path := fmt.Sprintf(`stmtdetails/%v`, fingerprintID)

	var resp serverpb.StatementDetailsResponse
	// Test that non-admin without VIEWACTIVITY or VIEWACTIVITYREDACTED privileges cannot access.
	err := srvtestutils.GetStatusJSONProtoWithAdminOption(firstServerProto, path, &resp, false)
	if !testutils.IsError(err, "status: 403") {
		t.Fatalf("expected privilege error, got %v", err)
	}

	type resultValues struct {
		query             string
		totalCount        int
		aggregatedTsCount int
		planHashCount     int
		fullScanCount     int
		appNames          []string
		databases         []string
	}

	testPath := func(path string, expected resultValues) {
		err := srvtestutils.GetStatusJSONProtoWithAdminOption(firstServerProto, path, &resp, false)
		require.NoError(t, err)
		require.Equal(t, int64(expected.totalCount), resp.Statement.Stats.Count)
		require.Equal(t, expected.aggregatedTsCount, len(resp.StatementStatisticsPerAggregatedTs))
		require.Equal(t, expected.planHashCount, len(resp.StatementStatisticsPerPlanHash))
		require.Equal(t, expected.query, resp.Statement.Metadata.Query)
		require.Equal(t, expected.appNames, resp.Statement.Metadata.AppNames)
		require.Equal(t, int64(expected.totalCount), resp.Statement.Metadata.TotalCount)
		require.Equal(t, expected.databases, resp.Statement.Metadata.Databases)
		require.Equal(t, int64(expected.fullScanCount), resp.Statement.Metadata.FullScanCount)
	}

	// Grant VIEWACTIVITY.
	thirdServerSQL.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITY", apiconstants.TestingUserNameNoAdmin().Normalized()))

	// Test with no query params.
	testPath(
		path,
		resultValues{
			query:             query,
			totalCount:        3,
			aggregatedTsCount: 1,
			planHashCount:     1,
			appNames:          []string{"first-app"},
			fullScanCount:     0,
			databases:         []string{"roachblog"},
		})

	// Execute same fingerprint id statement on a different application
	statements = []string{
		`set application_name = 'second-app'`,
		`INSERT INTO posts VALUES (4, 'foo')`,
		`INSERT INTO posts VALUES (5, 'foo')`,
	}
	for _, stmt := range statements {
		thirdServerSQL.Exec(t, stmt)
	}

	oneMinAfterAggregatedTs := aggregatedTs + 60

	testData := []struct {
		path           string
		expectedResult resultValues
	}{
		{ // Test with no query params.
			path: path,
			expectedResult: resultValues{
				query:             query,
				totalCount:        5,
				aggregatedTsCount: 1,
				planHashCount:     1,
				appNames:          []string{"first-app", "second-app"},
				fullScanCount:     0,
				databases:         []string{"roachblog"}},
		},
		{ // Test with end = 1 min after aggregatedTs; should give the same results as get all.
			path: fmt.Sprintf("%v?end=%d", path, oneMinAfterAggregatedTs),
			expectedResult: resultValues{
				query:             query,
				totalCount:        5,
				aggregatedTsCount: 1,
				planHashCount:     1,
				appNames:          []string{"first-app", "second-app"},
				fullScanCount:     0,
				databases:         []string{"roachblog"}},
		},
		{ // Test with start = 1 hour before aggregatedTs  end = 1 min after aggregatedTs; should give same results as get all.
			path: fmt.Sprintf("%v?start=%d&end=%d", path, aggregatedTs-3600, oneMinAfterAggregatedTs),
			expectedResult: resultValues{
				query:             query,
				totalCount:        5,
				aggregatedTsCount: 1,
				planHashCount:     1,
				appNames:          []string{"first-app", "second-app"},
				fullScanCount:     0,
				databases:         []string{"roachblog"}},
		},
		{ // Test with start = 1 min after aggregatedTs; should give no results.
			path: fmt.Sprintf("%v?start=%d", path, oneMinAfterAggregatedTs),
			expectedResult: resultValues{
				query:             "",
				totalCount:        0,
				aggregatedTsCount: 0,
				planHashCount:     0,
				appNames:          []string{},
				fullScanCount:     0,
				databases:         []string{}},
		},
		{ // Test with one app_name.
			path: fmt.Sprintf("%v?app_names=first-app", path),
			expectedResult: resultValues{
				query:             query,
				totalCount:        3,
				aggregatedTsCount: 1,
				planHashCount:     1,
				appNames:          []string{"first-app"},
				fullScanCount:     0,
				databases:         []string{"roachblog"}},
		},
		{ // Test with another app_name.
			path: fmt.Sprintf("%v?app_names=second-app", path),
			expectedResult: resultValues{
				query:             query,
				totalCount:        2,
				aggregatedTsCount: 1,
				planHashCount:     1,
				appNames:          []string{"second-app"},
				fullScanCount:     0,
				databases:         []string{"roachblog"}},
		},
		{ // Test with both app_names.
			path: fmt.Sprintf("%v?app_names=first-app&app_names=second-app", path),
			expectedResult: resultValues{
				query:             query,
				totalCount:        5,
				aggregatedTsCount: 1,
				planHashCount:     1,
				appNames:          []string{"first-app", "second-app"},
				fullScanCount:     0,
				databases:         []string{"roachblog"}},
		},
		{ // Test with non-existing app_name.
			path: fmt.Sprintf("%v?app_names=non-existing", path),
			expectedResult: resultValues{
				query:             "",
				totalCount:        0,
				aggregatedTsCount: 0,
				planHashCount:     0,
				appNames:          []string{},
				fullScanCount:     0,
				databases:         []string{}},
		},
		{ // Test with app_name, start and end time.
			path: fmt.Sprintf("%v?start=%d&end=%d&app_names=first-app&app_names=second-app", path, aggregatedTs-3600, oneMinAfterAggregatedTs),
			expectedResult: resultValues{
				query:             query,
				totalCount:        5,
				aggregatedTsCount: 1,
				planHashCount:     1,
				appNames:          []string{"first-app", "second-app"},
				fullScanCount:     0,
				databases:         []string{"roachblog"}},
		},
	}

	for _, test := range testData {
		testPath(test.path, test.expectedResult)
	}

	// Remove VIEWACTIVITY so we can test with just the VIEWACTIVITYREDACTED role.
	thirdServerSQL.Exec(t, fmt.Sprintf("ALTER USER %s NOVIEWACTIVITY", apiconstants.TestingUserNameNoAdmin().Normalized()))
	// Grant VIEWACTIVITYREDACTED.
	thirdServerSQL.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITYREDACTED", apiconstants.TestingUserNameNoAdmin().Normalized()))

	for _, test := range testData {
		testPath(test.path, test.expectedResult)
	}

	// Test fix for #83608. The stmt being below requested has a fingerprint id
	// that is 15 chars in hexadecimal. We should be able to find this stmt now
	// that we construct the filter using a bytes comparison instead of string.

	statements = []string{
		`set application_name = 'fix_83608'`,
		`set database = defaultdb`,
		`SELECT 1, 2, 3, 4`,
	}
	for _, stmt := range statements {
		thirdServerSQL.Exec(t, stmt)
	}

	selectQuery := "SELECT _, _, _, _"
	fingerprintID = appstatspb.ConstructStatementFingerprintID(selectQuery, true, "defaultdb")

	testPath(
		fmt.Sprintf(`stmtdetails/%v`, fingerprintID),
		resultValues{
			query:             selectQuery,
			totalCount:        1,
			aggregatedTsCount: 1,
			planHashCount:     1,
			appNames:          []string{"fix_83608"},
			fullScanCount:     0,
			databases:         []string{"defaultdb"},
		})
}

func TestUnprivilegedUserResetIndexUsageStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlConn := sqlutils.MakeSQLRunner(conn)
	sqlConn.Exec(t, "CREATE USER non_admin_user")

	ie := s.InternalExecutor().(*sql.InternalExecutor)

	_, err := ie.ExecEx(
		ctx,
		"test-reset-index-usage-stats-as-non-admin-user",
		nil, /* txn */
		sessiondata.InternalExecutorOverride{
			User: username.MakeSQLUsernameFromPreNormalizedString("non_admin_user"),
		},
		"SELECT crdb_internal.reset_index_usage_stats()",
	)

	require.Contains(t, err.Error(), "user non_admin_user does not have REPAIRCLUSTER system privilege")
}

// TestCombinedStatementUsesCorrectSourceTable tests that requests read from
// the expected crdb_internal table given the table states. We have a lot of
// different tables that requests could potentially read from (in-memory, cached,
// system tables etc.), so we should sanity check that we are using the expected ones.
// given some simple table states.
func TestCombinedStatementUsesCorrectSourceTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Disable flushing sql stats so we can manually set the table states
	// without worrying about unexpected stats appearing.
	settings := cluster.MakeTestingClusterSettings()
	statsKnobs := sqlstats.CreateTestingKnobs()
	defaultMockInsertedAggTs := timeutil.Unix(1696906800, 0)
	statsKnobs.StubTimeNow = func() time.Time { return defaultMockInsertedAggTs }
	persistedsqlstats.SQLStatsFlushEnabled.Override(ctx, &settings.SV, false)
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Settings: settings,
		Knobs: base.TestingKnobs{
			SQLStatsKnobs: statsKnobs,
		},
	})
	defer srv.Stopper().Stop(ctx)

	conn := sqlutils.MakeSQLRunner(srv.ApplicationLayer().SQLConn(t))
	conn.Exec(t, "SET application_name = $1", server.CrdbInternalStmtStatsCombined)
	conn.Exec(t, "SET CLUSTER SETTING sql.stats.activity.flush.enabled = 'f'")
	// Clear the in-memory stats so we only have the above app name.
	// Then populate it with 1 query.
	conn.Exec(t, "SELECT crdb_internal.reset_sql_stats()")
	conn.Exec(t, "SELECT 1")

	type testCase struct {
		name               string
		tableSetupFn       func() error
		expectedStmtsTable string
		expectedTxnsTable  string
		reqs               []serverpb.CombinedStatementsStatsRequest
		isEmpty            bool
		returnInternal     bool
	}

	ie := srv.InternalExecutor().(*sql.InternalExecutor)

	defaultMockOneEach := func() error {
		startTs := defaultMockInsertedAggTs
		stmt := sqlstatstestutil.GetRandomizedCollectedStatementStatisticsForTest(t)
		stmt.ID = 1
		stmt.AggregatedTs = startTs
		stmt.Key.App = server.CrdbInternalStmtStatsPersisted
		stmt.Key.TransactionFingerprintID = 1
		require.NoError(t, sqlstatstestutil.InsertMockedIntoSystemStmtStats(ctx, ie, []appstatspb.CollectedStatementStatistics{stmt}, 1))

		stmt.Key.App = server.CrdbInternalStmtStatsCached
		require.NoError(t, sqlstatstestutil.InsertMockedIntoSystemStmtActivity(ctx, ie, &stmt, nil))

		txn := sqlstatstestutil.GetRandomizedCollectedTransactionStatisticsForTest(t)
		txn.StatementFingerprintIDs = []appstatspb.StmtFingerprintID{1}
		txn.TransactionFingerprintID = 1
		txn.AggregatedTs = startTs
		txn.App = server.CrdbInternalTxnStatsPersisted
		require.NoError(t, sqlstatstestutil.InsertMockedIntoSystemTxnStats(ctx, ie, []appstatspb.CollectedTransactionStatistics{txn}, 1))
		txn.App = server.CrdbInternalTxnStatsCached
		require.NoError(t, sqlstatstestutil.InsertMockedIntoSystemTxnActivity(ctx, ie, &txn, nil))

		return nil
	}
	testCases := []testCase{
		{
			name:         "activity and persisted tables empty",
			tableSetupFn: func() error { return nil },
			// We should attempt to read from the in-memory tables, since
			// they are the last resort.
			expectedStmtsTable: server.CrdbInternalStmtStatsCombined,
			expectedTxnsTable:  server.CrdbInternalTxnStatsCombined,
			reqs: []serverpb.CombinedStatementsStatsRequest{
				{FetchMode: createTxnFetchMode(0)},
			},
			returnInternal: false,
		},
		{
			name:               "all tables have data in selected range",
			tableSetupFn:       defaultMockOneEach,
			expectedStmtsTable: server.CrdbInternalStmtStatsCached,
			expectedTxnsTable:  server.CrdbInternalTxnStatsCached,
			reqs: []serverpb.CombinedStatementsStatsRequest{
				{Start: defaultMockInsertedAggTs.Unix()},
				{
					Start: defaultMockInsertedAggTs.Unix(),
					End:   defaultMockInsertedAggTs.Unix(),
				},
			},
			returnInternal: false,
		},
		{
			name:         "all tables have data but no start range is provided",
			tableSetupFn: defaultMockOneEach,
			// When no date range is provided, we should default to reading from
			// persisted or in-memory (whichever has data first). In this case the
			// persisted table has data.
			expectedStmtsTable: server.CrdbInternalStmtStatsPersisted,
			expectedTxnsTable:  server.CrdbInternalTxnStatsPersisted,
			reqs: []serverpb.CombinedStatementsStatsRequest{
				{},
				{End: defaultMockInsertedAggTs.Unix()},
			},
			returnInternal: false,
		},
		{
			name:               "all tables have data but not in the selected range",
			tableSetupFn:       defaultMockOneEach,
			expectedStmtsTable: server.CrdbInternalStmtStatsCombined,
			expectedTxnsTable:  server.CrdbInternalTxnStatsCombined,
			reqs: []serverpb.CombinedStatementsStatsRequest{
				{Start: defaultMockInsertedAggTs.Add(time.Hour).Unix()},
				{End: defaultMockInsertedAggTs.Truncate(time.Hour * 2).Unix()},
			},
			isEmpty:        true,
			returnInternal: false,
		},
		{
			name:         "activity table has data in range with specified sort, fetchmode=txns",
			tableSetupFn: defaultMockOneEach,
			// For txn mode, we should not use the activity table for stmts.
			expectedStmtsTable: server.CrdbInternalStmtStatsPersisted,
			expectedTxnsTable:  server.CrdbInternalTxnStatsCached,
			// These sort options do exist on the activity table.
			reqs: []serverpb.CombinedStatementsStatsRequest{
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createTxnFetchMode(0)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createTxnFetchMode(1)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createTxnFetchMode(2)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createTxnFetchMode(4)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createTxnFetchMode(5)},
			},
			returnInternal: false,
		},
		{
			name:               "activity table has data in range with specified sort, fetchmode=stmts",
			tableSetupFn:       defaultMockOneEach,
			expectedStmtsTable: server.CrdbInternalStmtStatsCached,
			expectedTxnsTable:  "",
			// These sort options do exist on the activity table.
			reqs: []serverpb.CombinedStatementsStatsRequest{
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createStmtFetchMode(0)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createStmtFetchMode(1)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createStmtFetchMode(2)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createStmtFetchMode(4)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createStmtFetchMode(5)},
			},
			returnInternal: false,
		},
		{
			name:               "activity table has data in range, but selected sort is not on it, fetchmode=txns",
			tableSetupFn:       defaultMockOneEach,
			expectedStmtsTable: server.CrdbInternalStmtStatsPersisted,
			expectedTxnsTable:  server.CrdbInternalTxnStatsPersisted,
			// These sort options do not exist on the activity table.
			reqs: []serverpb.CombinedStatementsStatsRequest{
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createTxnFetchMode(8)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createTxnFetchMode(9)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createTxnFetchMode(10)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createTxnFetchMode(11)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createTxnFetchMode(12)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createTxnFetchMode(13)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createTxnFetchMode(14)},
			},
			returnInternal: false,
		},
		{
			name:               "activity table has data in range, but selected sort is not on it, fetchmode=stmts",
			tableSetupFn:       defaultMockOneEach,
			expectedStmtsTable: server.CrdbInternalStmtStatsPersisted,
			expectedTxnsTable:  "",
			// These sort options do not exist on the activity table.
			reqs: []serverpb.CombinedStatementsStatsRequest{
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createStmtFetchMode(6)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createStmtFetchMode(7)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createStmtFetchMode(8)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createStmtFetchMode(9)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createStmtFetchMode(10)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createStmtFetchMode(11)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createStmtFetchMode(12)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createStmtFetchMode(13)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createStmtFetchMode(14)},
			},
			returnInternal: false,
		},
		{
			name:               "activity table has data in range with specified sort, fetchmode=stmts",
			tableSetupFn:       defaultMockOneEach,
			expectedStmtsTable: server.CrdbInternalStmtStatsPersisted,
			expectedTxnsTable:  "",
			// These sort options do exist on the activity table.
			reqs: []serverpb.CombinedStatementsStatsRequest{
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createStmtFetchMode(0)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createStmtFetchMode(1)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createStmtFetchMode(2)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createStmtFetchMode(4)},
				{Start: defaultMockInsertedAggTs.Unix(), FetchMode: createStmtFetchMode(5)},
			},
			returnInternal: true,
		},
	}

	client := srv.ApplicationLayer().GetStatusClient(t)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, tc.tableSetupFn())

			defer func() {
				// Reset tables.
				conn.Exec(t, "SELECT crdb_internal.reset_sql_stats()")
				conn.Exec(t, "SELECT crdb_internal.reset_activity_tables()")
				conn.Exec(t, "SELECT 1")
			}()

			for _, r := range tc.reqs {
				conn.Exec(t, fmt.Sprintf("SET CLUSTER SETTING sql.stats.response.show_internal.enabled=%v", tc.returnInternal))
				resp, err := client.CombinedStatementStats(ctx, &r)
				require.NoError(t, err)

				require.Equal(t, tc.expectedStmtsTable, resp.StmtsSourceTable, "req: %v", r)
				require.Equal(t, tc.expectedTxnsTable, resp.TxnsSourceTable, "req: %v", r)

				if tc.isEmpty {
					continue
				}

				require.NotZero(t, len(resp.Statements), "req: %v", r)
				// Verify we used the correct queries to return data.
				require.Equal(t, tc.expectedStmtsTable, resp.Statements[0].Key.KeyData.App, "req: %v", r)
				if tc.expectedTxnsTable == server.CrdbInternalTxnStatsCombined {
					// For the combined query, we're using in-mem data and we set the
					// app name there to the in-memory stmts table.
					require.Equal(t, server.CrdbInternalStmtStatsCombined, resp.Transactions[0].StatsData.App, "req: %v", r)
				} else if tc.expectedTxnsTable != "" {
					require.NotZero(t, len(resp.Transactions))
					require.Equal(t, tc.expectedTxnsTable, resp.Transactions[0].StatsData.App, "req: %v", r)
				}
			}

		})
	}
}

func createStmtFetchMode(
	sort serverpb.StatsSortOptions,
) *serverpb.CombinedStatementsStatsRequest_FetchMode {
	return &serverpb.CombinedStatementsStatsRequest_FetchMode{
		StatsType: serverpb.CombinedStatementsStatsRequest_StmtStatsOnly,
		Sort:      sort,
	}
}
func createTxnFetchMode(
	sort serverpb.StatsSortOptions,
) *serverpb.CombinedStatementsStatsRequest_FetchMode {
	return &serverpb.CombinedStatementsStatsRequest_FetchMode{
		StatsType: serverpb.CombinedStatementsStatsRequest_TxnStatsOnly,
		Sort:      sort,
	}
}

func generateStatement() appstatspb.CollectedStatementStatistics {
	return appstatspb.CollectedStatementStatistics{
		AggregatedTs:        timeutil.FromUnixNanos(1704103200), // January 1, 2024 10:00:00
		ID:                  appstatspb.StmtFingerprintID(1088747230083123385),
		AggregationInterval: time.Hour,
		Key: appstatspb.StatementStatisticsKey{
			App:                      "test_app",
			Database:                 "test_database",
			DistSQL:                  true,
			FullScan:                 true,
			ImplicitTxn:              true,
			PlanHash:                 uint64(200),
			Query:                    "SELECT * FROM foo",
			QuerySummary:             "SELECT * FROM foo",
			TransactionFingerprintID: appstatspb.TransactionFingerprintID(50),
			Vec:                      true,
		},

		Stats: appstatspb.StatementStatistics{
			Count:         10,
			Indexes:       []string{"15@4"},
			LastErrorCode: "",
			MaxRetries:    0,
			Nodes:         []int64{1},
			PlanGists:     []string{"AgEeCADnAwIAAAMHEgUUIR4AAA=="},
			Regions:       []string{"us-east1"},
			SQLType:       "TypeDDL",
		},
	}
}
func generateStatisticsColumn(
	t *testing.T, statement appstatspb.CollectedStatementStatistics,
) string {
	// Create execution Statistics JSON
	executionStats := struct {
		Cnt            int                    `json:"cnt"`
		ContentionTime appstatspb.NumericStat `json:"contentionTime"`
		CpuSQLNanos    appstatspb.NumericStat `json:"cpuSQLNanos"`
		MaxDiskUsage   appstatspb.NumericStat `json:"maxDiskUsage"`
		MaxMemUsage    appstatspb.NumericStat `json:"maxMemUsage"`
		NetworkBytes   appstatspb.NumericStat `json:"networkBytes"`
		NetworkMsgs    appstatspb.NumericStat `json:"networkMsgs"`
	}{
		Cnt: 3,
		ContentionTime: appstatspb.NumericStat{
			Mean:         0,
			SquaredDiffs: 0,
		},
		CpuSQLNanos: appstatspb.NumericStat{
			Mean:         5,
			SquaredDiffs: 0,
		},
		MaxDiskUsage: appstatspb.NumericStat{
			Mean:         10,
			SquaredDiffs: 0,
		},
		MaxMemUsage: appstatspb.NumericStat{
			Mean:         10,
			SquaredDiffs: 0,
		},
		NetworkBytes: appstatspb.NumericStat{
			Mean:         0,
			SquaredDiffs: 0,
		},
		NetworkMsgs: appstatspb.NumericStat{
			Mean:         0,
			SquaredDiffs: 0,
		},
	}
	executionStatsBytes, err := json.Marshal(executionStats)
	require.NoError(t, err)

	// Create stats JSON
	stats := struct {
		BytesRead        appstatspb.NumericStat `json:"bytesRead"`
		Cnt              int64                  `json:"cnt"`
		FirstAttemptCnt  int64                  `json:"firstAttemptCnt"`
		IdleLat          appstatspb.NumericStat `json:"idleLat"`
		Indexes          []string               `json:"indexes"`
		LastErrorCode    string                 `json:"lastErrorCode"`
		LastExecAt       time.Time              `json:"lastExecAt"`
		MaxRetries       int                    `json:"maxRetries"`
		Nodes            []int64                `json:"nodes"`
		KVNodeIDs        []int32                `json:"kvNodeIds"`
		NumRows          appstatspb.NumericStat `json:"numRows"`
		OvhLat           appstatspb.NumericStat `json:"ovhLat"`
		ParseLat         appstatspb.NumericStat `json:"parseLat"`
		PlanGists        []string               `json:"planGists"`
		PlanLat          appstatspb.NumericStat `json:"planLat"`
		Regions          []string               `json:"regions"`
		UsedFollowerRead bool                   `json:"usedFollowerRead"`
		RowsRead         appstatspb.NumericStat `json:"rowsRead"`
		RowsWritten      appstatspb.NumericStat `json:"rowsWritten"`
		RunLat           appstatspb.NumericStat `json:"runLat"`
		SvcLat           appstatspb.NumericStat `json:"svcLat"`
	}{
		BytesRead: appstatspb.NumericStat{
			Mean:         0,
			SquaredDiffs: 0,
		},
		Cnt:             statement.Stats.Count,
		FirstAttemptCnt: statement.Stats.Count,
		IdleLat: appstatspb.NumericStat{
			Mean:         0,
			SquaredDiffs: 0,
		},
		Indexes:       statement.Stats.Indexes,
		LastErrorCode: statement.Stats.LastErrorCode,
		LastExecAt:    statement.AggregatedTs.Add(time.Minute * 10),
		MaxRetries:    0,
		Nodes:         statement.Stats.Nodes,
		KVNodeIDs:     statement.Stats.KVNodeIDs,
		NumRows: appstatspb.NumericStat{
			Mean:         0,
			SquaredDiffs: 0,
		},
		OvhLat: appstatspb.NumericStat{
			Mean:         0,
			SquaredDiffs: 0,
		},
		ParseLat: appstatspb.NumericStat{
			Mean:         0,
			SquaredDiffs: 0,
		},
		PlanGists: statement.Stats.PlanGists,
		PlanLat: appstatspb.NumericStat{
			Mean:         0,
			SquaredDiffs: 0,
		},
		Regions:          statement.Stats.Regions,
		UsedFollowerRead: statement.Stats.UsedFollowerRead,
		RowsRead: appstatspb.NumericStat{
			Mean:         0,
			SquaredDiffs: 0,
		},
		RowsWritten: appstatspb.NumericStat{
			Mean:         0,
			SquaredDiffs: 0,
		},
		RunLat: appstatspb.NumericStat{
			Mean:         0,
			SquaredDiffs: 0,
		},
		SvcLat: statement.Stats.ServiceLat,
	}
	statsBytes, err := json.Marshal(stats)
	require.NoError(t, err)

	return fmt.Sprintf("{\"execution_statistics\": %s, \"statistics\": %s"+
		", \"index_recommendations\": []} ", string(executionStatsBytes), string(statsBytes))
}

func insertStatementIntoSystemStmtStatsTable(
	t *testing.T, db *sqlutils.SQLRunner, statement appstatspb.CollectedStatementStatistics,
) {
	// Create metadata JSON
	metadata := struct {
		Database     string `json:"db"`
		DistSQL      bool   `json:"distsql"`
		FullScan     bool   `json:"fullScan"`
		ImplicitTxn  bool   `json:"implicitTxn"`
		Query        string `json:"query"`
		QuerySummary string `json:"querySummary"`
		StmtType     string `json:"stmtType"`
		Vec          bool   `json:"vec"`
	}{
		Database:     statement.Key.Database,
		DistSQL:      statement.Key.DistSQL,
		FullScan:     statement.Key.FullScan,
		ImplicitTxn:  statement.Key.ImplicitTxn,
		Query:        statement.Key.Query,
		QuerySummary: statement.Key.QuerySummary,
		StmtType:     statement.Stats.SQLType,
		Vec:          statement.Key.Vec,
	}
	metadataBytes, err := json.Marshal(metadata)
	require.NoError(t, err)
	statistics := generateStatisticsColumn(t, statement)

	query := `INSERT INTO system.statement_statistics (
  aggregated_ts,
  fingerprint_id,
  transaction_fingerprint_id,
  plan_hash,
  app_name,
  node_id,
  agg_interval,
  metadata,
  statistics,
  plan,
  index_recommendations
)
VALUES (
  $1,
  $2,
  $3,
  $4,
  $5,
  1,
  $6,
  $7,
  $8::JSONB,
  '{"Children": [], "Name": ""}',
  ARRAY['creation : CREATE INDEX t3_k_i_f ON t3(k, i, f)']
)`
	args := []interface{}{
		statement.AggregatedTs,
		statement.ID,
		statement.Key.TransactionFingerprintID,
		statement.Key.PlanHash,
		statement.Key.App,
		statement.AggregationInterval,
		string(metadataBytes),
		statistics,
	}

	db.Exec(t, query, args...)
}

func insertStatementIntoSystemStmtActivityTable(
	t *testing.T, db *sqlutils.SQLRunner, statement appstatspb.CollectedStatementStatistics,
) {
	// Create metadata JSON
	metadata := struct {
		AppNames      []string `json:"appNames"`
		Database      []string `json:"db"`
		DistSQLCount  int      `json:"distSQLCount"`
		FailedCount   int      `json:"failedCount"`
		FullScanCount int      `json:"fullScanCount"`
		ImplicitTxn   bool     `json:"implicitTxn"`
		Query         string   `json:"query"`
		QuerySummary  string   `json:"querySummary"`
		StmtType      string   `json:"stmtType"`
		VecCount      int      `json:"vecCount"`
	}{
		AppNames:      []string{statement.Key.App},
		Database:      []string{statement.Key.Database},
		DistSQLCount:  1,
		FailedCount:   0,
		FullScanCount: 1,
		ImplicitTxn:   statement.Key.ImplicitTxn,
		Query:         statement.Key.Query,
		QuerySummary:  statement.Key.QuerySummary,
		StmtType:      statement.Stats.SQLType,
		VecCount:      1,
	}
	metadataBytes, err := json.Marshal(metadata)
	require.NoError(t, err)
	statistics := generateStatisticsColumn(t, statement)

	query := `INSERT INTO system.statement_activity (
  aggregated_ts,
  fingerprint_id,
  transaction_fingerprint_id,
  plan_hash,
  app_name,
  agg_interval,
  metadata,
  statistics,
  plan,
  index_recommendations,
  execution_count,
	execution_total_seconds,
	execution_total_cluster_seconds,
	contention_time_avg_seconds,
	cpu_sql_avg_nanos,
	service_latency_avg_seconds,
	service_latency_p99_seconds
)
VALUES (
  $1,
  $2,
  $3,
  $4,
  $5,
  $6,
  $7,
  $8::JSONB,
  '{"Children": [], "Name": ""}',
  ARRAY['creation : CREATE INDEX t3_k_i_f ON t3(k, i, f)'],
	$9,
	$10,
	$10,
	0,
	5,
	10,
	7
)`

	args := []interface{}{
		statement.AggregatedTs,
		statement.ID,
		statement.Key.TransactionFingerprintID,
		statement.Key.PlanHash,
		statement.Key.App,
		statement.AggregationInterval,
		string(metadataBytes),
		statistics,
		statement.Stats.Count,
		float64(statement.Stats.Count) * statement.Stats.ServiceLat.Mean,
	}
	db.Exec(t, query, args...)
}
