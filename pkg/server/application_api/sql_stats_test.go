// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package application_api_test

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
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

// additionalTimeoutUnderStress is the additional timeout to use for the http
// client if under stress.
const additionalTimeoutUnderStress = 30 * time.Second

func TestStatusAPICombinedTransactions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Increase the timeout for the http client if under stress.
	additionalTimeout := 0 * time.Second
	if skip.Stress() {
		additionalTimeout = additionalTimeoutUnderStress
	}

	var params base.TestServerArgs
	params.Knobs.SpanConfig = &spanconfig.TestingKnobs{ManagerDisableJobCreation: true} // TODO(irfansharif): #74919.
	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: params,
	})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	thirdServer := testCluster.Server(2)
	pgURL, cleanupGoDB := sqlutils.PGUrl(
		t, thirdServer.AdvSQLAddr(), "CreateConnections" /* prefix */, url.User(username.RootUser))
	defer cleanupGoDB()
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
			fingerprinted: `INSERT INTO posts VALUES (_, '_')`,
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
		sqlDB, err := gosql.Open("postgres", pgURL.String())
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

	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	thirdServer := testCluster.Server(2)
	pgURL, cleanupGoDB := sqlutils.PGUrl(
		t, thirdServer.AdvSQLAddr(), "CreateConnections" /* prefix */, url.User(username.RootUser))
	defer cleanupGoDB()
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
			fingerprinted: `INSERT INTO posts VALUES (_, _)`,
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
		sqlDB, err := gosql.Open("postgres", pgURL.String())
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
	if skip.Stress() {
		additionalTimeout = additionalTimeoutUnderStress
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
			fingerprinted: `INSERT INTO posts VALUES (_, '_')`,
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
			if respStatement.Key.KeyData.Failed {
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

func TestStatusAPICombinedStatementsWithFullScans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Increase the timeout for the http client if under stress.
	additionalTimeout := 0 * time.Second
	if skip.Stress() {
		additionalTimeout = additionalTimeoutUnderStress
	}

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
	findJobQuery := "SELECT status FROM [SHOW JOBS] WHERE statement = 'CREATE INDEX idx_age ON football.public.players (age) STORING (name)';"
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
		{stmt: `INSERT INTO players (id, name, position, age, goals) VALUES (1, 'Lionel Messi', 'Forward', 34, 672), (2, 'Cristiano Ronaldo', 'Forward', 36, 674)`, respQuery: `INSERT INTO players(id, name, "position", age, goals) VALUES (_, '_', __more1_10__), (__more1_10__)`, fullScan: false, distSQL: false, failed: false, count: 1},
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
		failed   bool
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
				failed:   stmt.failed,
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
			if respStatement.Key.KeyData.App == testAppName && !respStatement.Key.KeyData.Failed {
				actualResponseStatsMap[respStatement.Key.KeyData.Query] = respStatement
			}
		}

		for respQuery, expectedData := range expectedStatementStatsMap {
			respStatement, exists := actualResponseStatsMap[respQuery]
			require.True(t, exists, "Expected statement '%s' not found in response: %v", respQuery, responseToJSON(resp))

			actualCount := respStatement.Stats.Count
			actualFullScan := respStatement.Key.KeyData.FullScan
			actualDistSQL := respStatement.Key.KeyData.DistSQL
			actualFailed := respStatement.Key.KeyData.Failed

			stmtJSONString := responseToJSON(respStatement)

			require.Equal(t, expectedData.fullScan, actualFullScan, "failed for respStatement: %v", stmtJSONString)
			require.Equal(t, expectedData.distSQL, actualDistSQL, "failed for respStatement: %v", stmtJSONString)
			require.Equal(t, expectedData.failed, actualFailed, "failed for respStatement: %v", stmtJSONString)
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
	skip.UnderStressRace(t, "expensive tests")

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
			fingerprinted: `INSERT INTO posts VALUES (_, '_')`,
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
			if respStatement.Key.KeyData.Failed {
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
	skip.UnderStressRace(t, "expensive tests")

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

	query := `INSERT INTO posts VALUES (_, '_')`
	fingerprintID := appstatspb.ConstructStatementFingerprintID(query,
		false, true, `roachblog`)
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
	fingerprintID = appstatspb.ConstructStatementFingerprintID(selectQuery, false,
		true, "defaultdb")

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
	sqlConn.Exec(t, "CREATE USER nonAdminUser")

	ie := s.InternalExecutor().(*sql.InternalExecutor)

	_, err := ie.ExecEx(
		ctx,
		"test-reset-index-usage-stats-as-non-admin-user",
		nil, /* txn */
		sessiondata.InternalExecutorOverride{
			User: username.MakeSQLUsernameFromPreNormalizedString("nonAdminUser"),
		},
		"SELECT crdb_internal.reset_index_usage_stats()",
	)

	require.Contains(t, err.Error(), "requires admin privilege")
}
