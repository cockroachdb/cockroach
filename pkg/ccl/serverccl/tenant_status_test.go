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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
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

	testCaseTenant := []struct {
		stmt          string
		fingerprinted string
	}{
		{stmt: `CREATE DATABASE roachblog_t`},
		{stmt: `SET database = roachblog_t`},
		{stmt: `CREATE TABLE posts_t (id INT8 PRIMARY KEY, body STRING)`},
		{
			stmt:          `INSERT INTO posts_t VALUES (1, 'foo')`,
			fingerprinted: `INSERT INTO posts_t VALUES (_, _)`,
		},
		{stmt: `SELECT * FROM posts_t`},
	}

	for _, stmt := range testCaseTenant {
		_, err := sqlDB.Exec(stmt.stmt)
		require.NoError(t, err)
	}

	err := sqlDB.Close()
	require.NoError(t, err)

	testCaseNonTenant := []struct {
		stmt          string
		fingerprinted string
	}{
		{stmt: `CREATE DATABASE roachblog_nt`},
		{stmt: `SET database = roachblog_nt`},
		{stmt: `CREATE TABLE posts_nt (id INT8 PRIMARY KEY, body STRING)`},
		{
			stmt:          `INSERT INTO posts_nt VALUES (1, 'foo')`,
			fingerprinted: `INSERT INTO posts_nt VALUES (_, _)`,
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

	for _, tenantStmt := range tenantStats.Statements {
		for _, nonTenantStmt := range nonTenantStats.Statements {
			if reflect.DeepEqual(tenantStmt, nonTenantStmt) {
				t.Fatal("expected tenant to have no visibility to non-tenant's statement stats, but found:", nonTenantStmt)
			}
		}
	}

	for _, tenantTxn := range tenantStats.Transactions {
		for _, nonTenantTxn := range nonTenantStats.Transactions {
			if reflect.DeepEqual(tenantTxn, nonTenantTxn) {
				t.Fatal("expected tenant to have no visibility to non-tenant's transaction stats, but found:", nonTenantTxn)
			}
		}
	}
}
