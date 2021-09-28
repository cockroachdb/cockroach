// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package serverccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTenantCannotSeeNonTenantStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	serverParams, _ := tests.CreateTestServerParams()
	testCluster := serverutils.StartNewTestCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: serverParams,
	})
	defer testCluster.Stopper().Stop(ctx)

	server := testCluster.Server(0 /* idx */)

	tenant, sqlDB := serverutils.StartTenant(t, server, base.TestTenantArgs{
		TenantID: roachpb.MakeTenantID(10 /* id */),
		TestingKnobs: base.TestingKnobs{
			SQLStatsKnobs: &sqlstats.TestingKnobs{
				AOSTClause: "AS OF SYSTEM TIME '-1us'",
			},
		},
	})

	nonTenant := testCluster.Server(1 /* idx */)

	tenantStatusServer := tenant.StatusServer().(serverpb.SQLStatusServer)

	type testCase struct {
		stmt        string
		fingerprint string
	}

	testCaseTenant := []testCase{
		{stmt: `CREATE DATABASE roachblog_t`},
		{stmt: `SET database = roachblog_t`},
		{stmt: `CREATE TABLE posts_t (id INT8 PRIMARY KEY, body STRING)`},
		{
			stmt:        `INSERT INTO posts_t VALUES (1, 'foo')`,
			fingerprint: `INSERT INTO posts_t VALUES (_, '_')`,
		},
		{stmt: `SELECT * FROM posts_t`},
	}

	for _, stmt := range testCaseTenant {
		_, err := sqlDB.Exec(stmt.stmt)
		require.NoError(t, err)
	}

	err := sqlDB.Close()
	require.NoError(t, err)

	testCaseNonTenant := []testCase{
		{stmt: `CREATE DATABASE roachblog_nt`},
		{stmt: `SET database = roachblog_nt`},
		{stmt: `CREATE TABLE posts_nt (id INT8 PRIMARY KEY, body STRING)`},
		{
			stmt:        `INSERT INTO posts_nt VALUES (1, 'foo')`,
			fingerprint: `INSERT INTO posts_nt VALUES (_, '_')`,
		},
		{stmt: `SELECT * FROM posts_nt`},
	}

	pgURL, cleanupGoDB := sqlutils.PGUrl(
		t, nonTenant.ServingSQLAddr(), "CreateConnections" /* prefix */, url.User(security.RootUser))
	defer cleanupGoDB()
	sqlDB, err = gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)

	for _, stmt := range testCaseNonTenant {
		_, err = sqlDB.Exec(stmt.stmt)
		require.NoError(t, err)
	}
	err = sqlDB.Close()
	require.NoError(t, err)

	request := &serverpb.StatementsRequest{}

	tenantStats, err := tenantStatusServer.Statements(ctx, request)
	require.NoError(t, err)

	combinedStatsRequest := &serverpb.CombinedStatementsStatsRequest{}
	tenantCombinedStats, err := tenantStatusServer.CombinedStatementStats(ctx, combinedStatsRequest)
	require.NoError(t, err)

	path := "/_status/statements"
	var nonTenantStats serverpb.StatementsResponse
	err = serverutils.GetJSONProto(nonTenant, path, &nonTenantStats)
	require.NoError(t, err)

	path = "/_status/combinedstmts"
	var nonTenantCombinedStats serverpb.StatementsResponse
	err = serverutils.GetJSONProto(nonTenant, path, &nonTenantCombinedStats)
	require.NoError(t, err)

	checkStatements := func(tc []testCase, actual *serverpb.StatementsResponse) {
		var expectedStatements []string
		for _, stmt := range tc {
			var expectedStmt = stmt.stmt
			if stmt.fingerprint != "" {
				expectedStmt = stmt.fingerprint
			}
			expectedStatements = append(expectedStatements, expectedStmt)
		}

		var actualStatements []string
		for _, respStatement := range actual.Statements {
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
			actualStatements = append(actualStatements, respStatement.Key.KeyData.Query)
		}

		sort.Strings(expectedStatements)
		sort.Strings(actualStatements)

		require.Equal(t, expectedStatements, actualStatements)
	}

	// First we verify that we have expected stats from tenants
	checkStatements(testCaseTenant, tenantStats)
	checkStatements(testCaseTenant, tenantCombinedStats)

	// Now we verify the non tenant stats are what we expected.
	checkStatements(testCaseNonTenant, &nonTenantStats)
	checkStatements(testCaseNonTenant, &nonTenantCombinedStats)

	// Now we verify that tenant and non-tenant have no visibility into each other's stats.
	for _, tenantStmt := range tenantStats.Statements {
		for _, nonTenantStmt := range nonTenantStats.Statements {
			require.NotEqual(t, tenantStmt, nonTenantStmt, "expected tenant to have no visibility to non-tenant's statement stats, but found:", nonTenantStmt)
		}
	}

	for _, tenantTxn := range tenantStats.Transactions {
		for _, nonTenantTxn := range nonTenantStats.Transactions {
			require.NotEqual(t, tenantTxn, nonTenantTxn, "expected tenant to have no visibility to non-tenant's transaction stats, but found:", nonTenantTxn)
		}
	}

	for _, tenantStmt := range tenantCombinedStats.Statements {
		for _, nonTenantStmt := range nonTenantCombinedStats.Statements {
			require.NotEqual(t, tenantStmt, nonTenantStmt, "expected tenant to have no visibility to non-tenant's statement stats, but found:", nonTenantStmt)
		}
	}

	for _, tenantTxn := range tenantCombinedStats.Transactions {
		for _, nonTenantTxn := range nonTenantCombinedStats.Transactions {
			require.NotEqual(t, tenantTxn, nonTenantTxn, "expected tenant to have no visibility to non-tenant's transaction stats, but found:", nonTenantTxn)
		}
	}
}

func TestResetSQLStatsRPCForTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStressRace(t, "expensive tests")

	ctx := context.Background()

	stmts := []string{
		"SELECT 1",
		"SELECT 1, 1",
		"SELECT 1, 1, 1",
	}

	testHelper := newTestTenantHelper(t, 3 /* tenantClusterSize */)
	defer testHelper.cleanup(ctx, t)

	testCluster := testHelper.testCluster()
	controlCluster := testHelper.controlCluster()

	// Disable automatic flush to ensure tests are deterministic.
	testCluster.tenantConn(0 /* idx */).
		Exec(t, "SET CLUSTER SETTING sql.stats.flush.enabled = false")
	controlCluster.tenantConn(0 /* idx */).
		Exec(t, "SET CLUSTER SETTING sql.stats.flush.enabled = false")

	for _, flushed := range []bool{false, true} {
		t.Run(fmt.Sprintf("flushed=%t", flushed), func(t *testing.T) {
			// Clears the SQL Stats at the end of each test via builtin.
			defer func() {
				testCluster.tenantConn(0 /* idx */).Exec(t, "SELECT crdb_internal.reset_sql_stats()")
				controlCluster.tenantConn(0 /* idx */).Exec(t, "SELECT crdb_internal.reset_sql_stats()")
			}()

			for _, stmt := range stmts {
				testCluster.tenantConn(0 /* idx */).Exec(t, stmt)
				controlCluster.tenantConn(0 /* idx */).Exec(t, stmt)
			}

			if flushed {
				testCluster.tenantSQLStats(0 /* idx */).Flush(ctx)
				controlCluster.tenantSQLStats(0 /* idx */).Flush(ctx)
			}

			status := testCluster.tenantStatusSrv(1 /* idx */)

			statsPreReset, err := status.Statements(ctx, &serverpb.StatementsRequest{
				Combined: true,
			})
			require.NoError(t, err)

			require.NotEqual(t, 0, len(statsPreReset.Statements),
				"expected to find stats for at least one statement, but found: %d", len(statsPreReset.Statements))
			ensureExpectedStmtFingerprintExistsInRPCResponse(t, stmts, statsPreReset, "test")

			_, err = status.ResetSQLStats(ctx, &serverpb.ResetSQLStatsRequest{
				ResetPersistedStats: true,
			})
			require.NoError(t, err)

			statsPostReset, err := status.Statements(ctx, &serverpb.StatementsRequest{
				Combined: true,
			})
			require.NoError(t, err)

			if !statsPostReset.LastReset.After(statsPreReset.LastReset) {
				t.Fatal("expected to find stats last reset value changed, but didn't")
			}

			for _, txnStatsPostReset := range statsPostReset.Transactions {
				for _, txnStatsPreReset := range statsPreReset.Transactions {
					require.NotEqual(t, txnStatsPostReset, txnStatsPreReset,
						"expected to have reset SQL stats, but still found transaction %+v", txnStatsPostReset)
				}
			}

			for _, stmtStatsPostReset := range statsPostReset.Statements {
				for _, stmtStatsPreReset := range statsPreReset.Statements {
					require.NotEqual(t, stmtStatsPostReset, stmtStatsPreReset,
						"expected to have reset SQL stats, but still found statement %+v", stmtStatsPostReset)
				}
			}

			// Ensures that sql stats reset is isolated by tenant boundary.
			statsFromControlCluster, err :=
				controlCluster.tenantStatusSrv(1 /* idx */).Statements(ctx, &serverpb.StatementsRequest{
					Combined: true,
				})
			require.NoError(t, err)

			ensureExpectedStmtFingerprintExistsInRPCResponse(t, stmts, statsFromControlCluster, "control")
		})
	}
}

func ensureExpectedStmtFingerprintExistsInRPCResponse(
	t *testing.T, expectedStmts []string, resp *serverpb.StatementsResponse, clusterType string,
) {
	t.Helper()

	for _, stmt := range expectedStmts {
		fingerprint := strings.Replace(stmt, "1", "_", -1)
		found := false
		for _, foundStmt := range resp.Statements {
			if !strings.Contains(foundStmt.Key.KeyData.App, resp.InternalAppNamePrefix) {
				if fingerprint == foundStmt.Key.KeyData.Query {
					found = true
					break
				}
			}
		}
		require.True(t, found, "expected %s to be found in "+
			"%s tenant cluster, but it was not found", fingerprint, clusterType)
	}
}

func TestContentionEventsForTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStressRace(t, "expensive tests")

	ctx := context.Background()

	testHelper := newTestTenantHelper(t, 3 /* tenantClusterSize */)
	defer testHelper.cleanup(ctx, t)

	testingCluster := testHelper.testCluster()
	controlledCluster := testHelper.controlCluster()

	sqlutils.CreateTable(
		t,
		testingCluster[0].tenantConn,
		"test",
		"x INT PRIMARY KEY",
		1, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)

	testTableID, err :=
		strconv.Atoi(testingCluster.tenantConn(0).QueryStr(t, "SELECT 'test.test'::regclass::oid")[0][0])
	require.NoError(t, err)

	testingCluster.tenantConn(0).Exec(t, "USE test")
	testingCluster.tenantConn(1).Exec(t, "USE test")

	testingCluster.tenantConn(0).Exec(t, `
BEGIN;
UPDATE test SET x = 100 WHERE x = 1;
`)
	testingCluster.tenantConn(1).Exec(t, `
SET TRACING=on;
BEGIN PRIORITY HIGH;
UPDATE test SET x = 1000 WHERE x = 1;
COMMIT;
SET TRACING=off;
`)
	testingCluster.tenantConn(0).ExpectErr(
		t,
		"^pq: restart transaction.+",
		"COMMIT;",
	)

	resp, err :=
		testingCluster.tenantStatusSrv(2).ListContentionEvents(ctx, &serverpb.ListContentionEventsRequest{})
	require.NoError(t, err)

	require.GreaterOrEqualf(t, len(resp.Events.IndexContentionEvents), 1,
		"expecting at least 1 contention event, but found none")

	found := false
	for _, event := range resp.Events.IndexContentionEvents {
		if event.TableID == descpb.ID(testTableID) && event.IndexID == descpb.IndexID(1) {
			found = true
			break
		}
	}

	require.True(t, found,
		"expect to find contention event for table %d, but found %+v", testTableID, resp)

	resp, err = controlledCluster.tenantStatusSrv(0).ListContentionEvents(ctx, &serverpb.ListContentionEventsRequest{})
	require.NoError(t, err)
	for _, event := range resp.Events.IndexContentionEvents {
		if event.TableID == descpb.ID(testTableID) && event.IndexID == descpb.IndexID(1) {
			t.Errorf("did not expect contention event in controlled cluster, but it was found")
		}
	}
}
