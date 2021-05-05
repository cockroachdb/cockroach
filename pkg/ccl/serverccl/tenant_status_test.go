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
	"net/url"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTenantCannotSeeNonTenantStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	testCluster := serverutils.StartNewTestCluster(t, 3 /* numNodes */, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(ctx)

	server := testCluster.Server(0 /* idx */)

	tenant, sqlDB := serverutils.StartTenant(t, server, base.TestTenantArgs{
		TenantID: roachpb.MakeTenantID(10 /* id */),
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
			fingerprint: `INSERT INTO posts_t VALUES (_, _)`,
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
			fingerprint: `INSERT INTO posts_nt VALUES (_, _)`,
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

	path := "/_status/statements"
	var nonTenantStats serverpb.StatementsResponse
	err = serverutils.GetJSONProto(nonTenant, path, &nonTenantStats)
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

	// Now we verify the non tenant stats are what we expected.
	checkStatements(testCaseNonTenant, &nonTenantStats)

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
}
