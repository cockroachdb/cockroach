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
	"fmt"
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

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	ts := s.ApplicationLayer()

	query := "EXPLAIN ANALYZE (DEBUG) SELECT 'secret'"
	_, err := db.Exec(query)
	require.NoError(t, err)

	query = "SELECT id FROM system.statement_diagnostics LIMIT 1"
	idRow, err := db.Query(query)
	require.NoError(t, err)
	var diagnosticRow string
	if idRow.Next() {
		err = idRow.Scan(&diagnosticRow)
		require.NoError(t, err)
	} else {
		t.Fatal("no results")
	}

	client, err := ts.GetAuthenticatedHTTPClient(false, serverutils.SingleTenantSession)
	require.NoError(t, err)
	resp, err := client.Get(ts.AdminURL().WithPath("/_admin/v1/stmtbundle/" + diagnosticRow).String())
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 500, resp.StatusCode)

	adminClient, err := ts.GetAuthenticatedHTTPClient(true, serverutils.SingleTenantSession)
	require.NoError(t, err)
	adminResp, err := adminClient.Get(ts.AdminURL().WithPath("/_admin/v1/stmtbundle/" + diagnosticRow).String())
	require.NoError(t, err)
	defer adminResp.Body.Close()
	require.Equal(t, 200, adminResp.StatusCode)
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
	require.Contains(t, err.Error(), "requesting statement bundle requires VIEWACTIVITY or ADMIN role option")

	// Grant VIEWACTIVITY and all test should work.
	db.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITY", apiconstants.TestingUserNameNoAdmin().Normalized()))
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
	db.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITYREDACTED", apiconstants.TestingUserNameNoAdmin().Normalized()))

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
	require.Contains(t, err.Error(), "VIEWACTIVITYREDACTED role option cannot request statement bundle")
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
