// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package application_api_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestAdminAPIStatementDiagnosticsBundle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.ApplicationLayer()
	conn := sqlutils.MakeSQLRunner(ts.SQLConn(t))

	query := "EXPLAIN ANALYZE (DEBUG) SELECT 'secret'"
	conn.Exec(t, query)

	query = "SELECT id FROM system.statement_diagnostics LIMIT 1"
	idRow := conn.Query(t, query)
	var diagnosticRow string
	if idRow.Next() {
		err := idRow.Scan(&diagnosticRow)
		require.NoError(t, err)
	} else {
		t.Fatal("no results")
	}

	reqBundle := func(isAdmin bool, expectedStatusCode int) {
		client, err := ts.GetAuthenticatedHTTPClient(isAdmin, serverutils.SingleTenantSession)
		require.NoError(t, err)
		resp, err := client.Get(ts.AdminURL().WithPath("/_admin/v1/stmtbundle/" + diagnosticRow).String())
		require.NoError(t, err)
		defer func() {
			err := resp.Body.Close()
			require.NoError(t, err)
		}()
		require.Equal(t, expectedStatusCode, resp.StatusCode)
	}

	t.Run("with admin role", func(t *testing.T) {
		reqBundle(true, http.StatusOK)
	})

	t.Run("no permissions", func(t *testing.T) {
		reqBundle(false, http.StatusForbidden)
	})

	t.Run("with VIEWACTIVITYREDACTED role", func(t *testing.T) {
		// VIEWACTIVITYREDACTED cannot download bundles due to PII.
		// This will be revisited once we allow requesting and downloading redacted bundles.
		conn.Exec(t, fmt.Sprintf("GRANT SYSTEM VIEWACTIVITYREDACTED TO %s", apiconstants.TestingUserNameNoAdmin().Normalized()))
		reqBundle(false, http.StatusForbidden)
		conn.Exec(t, fmt.Sprintf("REVOKE SYSTEM VIEWACTIVITYREDACTED FROM %s", apiconstants.TestingUserNameNoAdmin().Normalized()))
	})

	t.Run("with VIEWACTIVITY role", func(t *testing.T) {
		// VIEWACTIVITY users can download bundles.
		conn.Exec(t, fmt.Sprintf("GRANT SYSTEM VIEWACTIVITY TO %s", apiconstants.TestingUserNameNoAdmin().Normalized()))
		reqBundle(false, http.StatusOK)
		conn.Exec(t, fmt.Sprintf("REVOKE SYSTEM VIEWACTIVITY FROM %s", apiconstants.TestingUserNameNoAdmin().Normalized()))
	})
}

func TestCreateStatementDiagnosticsReport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	ts := s.ApplicationLayer()

	req := &serverpb.CreateStatementDiagnosticsReportRequest{
		StatementFingerprint: "INSERT INTO test VALUES (_)",
	}
	var resp serverpb.CreateStatementDiagnosticsReportResponse
	if err := srvtestutils.PostStatusJSONProto(ts, "stmtdiagreports", req, &resp); err != nil {
		t.Fatal(err)
	}

	var respGet serverpb.StatementDiagnosticsReportsResponse
	if err := srvtestutils.GetStatusJSONProto(ts, "stmtdiagreports", &respGet); err != nil {
		t.Fatal(err)
	}

	if respGet.Reports[0].StatementFingerprint != req.StatementFingerprint {
		t.Fatal("statement diagnostics request was not persisted")
	}
}

func TestCreateStatementDiagnosticsReportWithViewActivityOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	db := sqlutils.MakeSQLRunner(sqlDB)

	ts := s.ApplicationLayer()

	ctx := context.Background()
	ie := ts.InternalExecutor().(*sql.InternalExecutor)

	if err := srvtestutils.GetStatusJSONProtoWithAdminOption(ts, "stmtdiagreports", &serverpb.CreateStatementDiagnosticsReportRequest{}, false); err != nil {
		if !testutils.IsError(err, "status: 403") {
			t.Fatalf("expected privilege error, got %v", err)
		}
	}
	_, err := ie.ExecEx(
		ctx,
		"inserting-stmt-bundle-req",
		nil, /* txn */
		sessiondata.InternalExecutorOverride{
			User: apiconstants.TestingUserNameNoAdmin(),
		},
		"SELECT crdb_internal.request_statement_bundle('SELECT _', 0::FLOAT, 0::INTERVAL, 0::INTERVAL)",
	)
	require.Contains(t, err.Error(), "requesting statement bundle requires VIEWACTIVITY privilege")

	// Grant VIEWACTIVITY and all test should work.
	db.Exec(t, fmt.Sprintf("GRANT SYSTEM VIEWACTIVITY TO %s", apiconstants.TestingUserNameNoAdmin().Normalized()))
	req := &serverpb.CreateStatementDiagnosticsReportRequest{
		StatementFingerprint: "INSERT INTO test VALUES (_)",
	}
	var resp serverpb.CreateStatementDiagnosticsReportResponse
	if err := srvtestutils.PostStatusJSONProtoWithAdminOption(ts, "stmtdiagreports", req, &resp, false); err != nil {
		t.Fatal(err)
	}
	var respGet serverpb.StatementDiagnosticsReportsResponse
	if err := srvtestutils.GetStatusJSONProtoWithAdminOption(ts, "stmtdiagreports", &respGet, false); err != nil {
		t.Fatal(err)
	}
	if respGet.Reports[0].StatementFingerprint != req.StatementFingerprint {
		t.Fatal("statement diagnostics request was not persisted")
	}
	_, err = ie.ExecEx(
		ctx,
		"inserting-stmt-bundle-req",
		nil, /* txn */
		sessiondata.InternalExecutorOverride{
			User: apiconstants.TestingUserNameNoAdmin(),
		},
		"SELECT crdb_internal.request_statement_bundle('SELECT _', 0::FLOAT, 0::INTERVAL, 0::INTERVAL)",
	)
	require.NoError(t, err)

	db.CheckQueryResults(t, `
      SELECT count(*)
      FROM system.statement_diagnostics_requests
      WHERE statement_fingerprint = 'SELECT _'
`, [][]string{{"1"}})

	// Grant VIEWACTIVITYREDACTED and all test should get permission errors.
	db.Exec(t, fmt.Sprintf("GRANT SYSTEM VIEWACTIVITYREDACTED TO %s", apiconstants.TestingUserNameNoAdmin().Normalized()))

	if err := srvtestutils.PostStatusJSONProtoWithAdminOption(ts, "stmtdiagreports", req, &resp, false); err != nil {
		if !testutils.IsError(err, "status: 403") {
			t.Fatalf("expected privilege error, got %v", err)
		}
	}
	if err := srvtestutils.GetStatusJSONProtoWithAdminOption(ts, "stmtdiagreports", &respGet, false); err != nil {
		if !testutils.IsError(err, "status: 403") {
			t.Fatalf("expected privilege error, got %v", err)
		}
	}

	_, err = ie.ExecEx(
		ctx,
		"inserting-stmt-bundle-req",
		nil, /* txn */
		sessiondata.InternalExecutorOverride{
			User: apiconstants.TestingUserNameNoAdmin(),
		},
		"SELECT crdb_internal.request_statement_bundle('SELECT _', 0::FLOAT, 0::INTERVAL, 0::INTERVAL)",
	)
	require.Contains(t, err.Error(), "users with VIEWACTIVITYREDACTED privilege can only request redacted statement bundles")
}

func TestStatementDiagnosticsCompleted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	ts := s.ApplicationLayer()

	_, err := db.Exec("CREATE TABLE test (x int PRIMARY KEY)")
	if err != nil {
		t.Fatal(err)
	}

	req := &serverpb.CreateStatementDiagnosticsReportRequest{
		StatementFingerprint: "INSERT INTO test VALUES (_)",
	}
	var resp serverpb.CreateStatementDiagnosticsReportResponse
	if err := srvtestutils.PostStatusJSONProto(ts, "stmtdiagreports", req, &resp); err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("INSERT INTO test VALUES (1)")
	if err != nil {
		t.Fatal(err)
	}

	var respGet serverpb.StatementDiagnosticsReportsResponse
	if err := srvtestutils.GetStatusJSONProto(ts, "stmtdiagreports", &respGet); err != nil {
		t.Fatal(err)
	}

	if respGet.Reports[0].Completed != true {
		t.Fatal("statement diagnostics was not captured")
	}

	var diagRespGet serverpb.StatementDiagnosticsResponse
	diagPath := fmt.Sprintf("stmtdiag/%d", respGet.Reports[0].StatementDiagnosticsId)
	if err := srvtestutils.GetStatusJSONProto(ts, diagPath, &diagRespGet); err != nil {
		t.Fatal(err)
	}
}

func TestStatementDiagnosticsDoesNotReturnExpiredRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	db := sqlutils.MakeSQLRunner(sqlDB)

	ts := s.ApplicationLayer()

	statementFingerprint := "INSERT INTO test VALUES (_)"
	expiresAfter := 5 * time.Millisecond

	// Create statement diagnostics request with defined expiry time.
	req := &serverpb.CreateStatementDiagnosticsReportRequest{
		StatementFingerprint: statementFingerprint,
		MinExecutionLatency:  500 * time.Millisecond,
		ExpiresAfter:         expiresAfter,
	}
	var resp serverpb.CreateStatementDiagnosticsReportResponse
	if err := srvtestutils.PostStatusJSONProto(ts, "stmtdiagreports", req, &resp); err != nil {
		t.Fatal(err)
	}

	// Wait for request to expire.
	time.Sleep(expiresAfter)

	// Check that created statement diagnostics report is incomplete.
	report := db.QueryStr(t, `
SELECT completed
FROM system.statement_diagnostics_requests
WHERE statement_fingerprint = $1`, statementFingerprint)

	require.Equal(t, report[0][0], "false")

	// Check that expired report is not returned in API response.
	var respGet serverpb.StatementDiagnosticsReportsResponse
	if err := srvtestutils.GetStatusJSONProto(ts, "stmtdiagreports", &respGet); err != nil {
		t.Fatal(err)
	}

	for _, report := range respGet.Reports {
		require.NotEqual(t, report.StatementFingerprint, statementFingerprint)
	}
}
