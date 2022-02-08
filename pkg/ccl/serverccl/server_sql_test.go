// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package serverccl

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/licenseccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/systemconfigwatcher/systemconfigwatchertest"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// TestSQLServer starts up a semi-dedicated SQL server and runs some smoke test
// queries. The SQL server shares some components, notably Gossip, with a test
// server serving as a KV backend.
//
// TODO(tbg): start narrowing down and enumerating the unwanted dependencies. In
// the end, the SQL server in this test should not depend on a Gossip instance
// and must not rely on having a NodeID/NodeDescriptor/NodeLiveness/...
//
// In short, it should not rely on the test server through anything other than a
// `*kv.DB` and a small number of allowlisted RPCs.
func TestSQLServer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	_, db := serverutils.StartTenant(
		t,
		tc.Server(0),
		base.TestTenantArgs{TenantID: serverutils.TestTenantID()},
	)
	defer db.Close()
	r := sqlutils.MakeSQLRunner(db)
	r.QueryStr(t, `SELECT 1`)
	r.Exec(t, `CREATE DATABASE foo`)
	r.Exec(t, `CREATE TABLE foo.kv (k STRING PRIMARY KEY, v STRING)`)
	r.Exec(t, `INSERT INTO foo.kv VALUES('foo', 'bar')`)
	// Cause an index backfill operation.
	r.Exec(t, `CREATE INDEX ON foo.kv (v)`)
	t.Log(sqlutils.MatrixToStr(r.QueryStr(t, `SET distsql=off; SELECT * FROM foo.kv`)))
	t.Log(sqlutils.MatrixToStr(r.QueryStr(t, `SET distsql=auto; SELECT * FROM foo.kv`)))
}

func TestTenantCannotSetClusterSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	// StartTenant with the default permissions to
	_, db := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{TenantID: serverutils.TestTenantID(), AllowSettingClusterSettings: false})
	defer db.Close()
	_, err := db.Exec(`SET CLUSTER SETTING sql.defaults.vectorize=off`)
	require.NoError(t, err)
	_, err = db.Exec(`SET CLUSTER SETTING kv.snapshot_rebalance.max_rate = '2MiB';`)
	var pqErr *pq.Error
	ok := errors.As(err, &pqErr)
	require.True(t, ok, "expected err to be a *pq.Error but is of type %T. error is: %v", err)
	if !strings.Contains(pqErr.Message, "unknown cluster setting") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestTenantCanUseEnterpriseFeatures(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	license, _ := (&licenseccl.License{
		Type: licenseccl.License_Enterprise,
	}).Encode()

	defer utilccl.TestingDisableEnterprise()()
	defer envutil.TestSetEnv(t, "COCKROACH_TENANT_LICENSE", license)()

	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())

	_, db := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{TenantID: serverutils.TestTenantID(), AllowSettingClusterSettings: false})
	defer db.Close()

	_, err := db.Exec(`BACKUP INTO 'userfile:///backup'`)
	require.NoError(t, err)
	_, err = db.Exec(`BACKUP INTO LATEST IN 'userfile:///backup'`)
	require.NoError(t, err)
}

func TestTenantUnauthenticatedAccess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	_, err := tc.Server(0).StartTenant(ctx,
		base.TestTenantArgs{
			TenantID: roachpb.MakeTenantID(security.EmbeddedTenantIDs()[0]),
			TestingKnobs: base.TestingKnobs{
				TenantTestingKnobs: &sql.TenantTestingKnobs{
					// Configure the SQL server to access the wrong tenant keyspace.
					TenantIDCodecOverride: roachpb.MakeTenantID(security.EmbeddedTenantIDs()[1]),
				},
			},
		})
	require.Error(t, err)
	require.Regexp(t, `Unauthenticated desc = requested key .* not fully contained in tenant keyspace /Tenant/1{0-1}`, err)
}

// TestTenantHTTP verifies that SQL tenant servers expose metrics and debugging endpoints.
func TestTenantHTTP(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	tenant, err := tc.Server(0).StartTenant(ctx,
		base.TestTenantArgs{
			TenantID: serverutils.TestTenantID(),
		})
	require.NoError(t, err)

	t.Run("prometheus", func(t *testing.T) {
		httpClient, err := tenant.GetHTTPClient()
		require.NoError(t, err)
		defer httpClient.CloseIdleConnections()
		resp, err := httpClient.Get(tenant.AdminURL() + "/_status/vars")
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), "sql_ddl_started_count_internal")
	})
	t.Run("pprof", func(t *testing.T) {
		httpClient, err := tenant.GetAdminAuthenticatedHTTPClient()
		require.NoError(t, err)
		defer httpClient.CloseIdleConnections()
		resp, err := httpClient.Get(tenant.AdminURL() + "/debug/pprof/goroutine?debug=2")
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), "goroutine")
	})

}

func TestNonExistentTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	_, err := tc.Server(0).StartTenant(ctx,
		base.TestTenantArgs{
			TenantID:        serverutils.TestTenantID(),
			Existing:        true,
			SkipTenantCheck: true,
		})
	require.Error(t, err)
	require.Equal(t, "system DB uninitialized, check if tenant is non existent", err.Error())
}

// TestTenantRowIDs confirms `unique_rowid()` works as expected in a
// multi-tenant setup.
func TestTenantRowIDs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	const numRows = 10
	tenant, db := serverutils.StartTenant(
		t,
		tc.Server(0),
		base.TestTenantArgs{TenantID: serverutils.TestTenantID()},
	)
	defer db.Close()
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TABLE foo(key INT PRIMARY KEY DEFAULT unique_rowid(), val INT)`)
	sqlDB.Exec(t, fmt.Sprintf("INSERT INTO foo (val) SELECT * FROM generate_series(1, %d)", numRows))

	// Verify that the rows are inserted successfully and that the row ids
	// are based on the SQL instance ID.
	rows := sqlDB.Query(t, "SELECT key FROM foo")
	defer rows.Close()
	rowCount := 0
	instanceID := int(tenant.SQLInstanceID())
	for rows.Next() {
		var key int
		if err := rows.Scan(&key); err != nil {
			t.Fatal(err)
		}
		require.Equal(t, instanceID, key&instanceID)
		rowCount++
	}
	require.Equal(t, numRows, rowCount)
}

// TestNoInflightTracesVirtualTableOnTenant verifies that internal inflight traces table
// is correctly handled by tenants (which don't provide this functionality as of now).
func TestNoInflightTracesVirtualTableOnTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	args := base.TestClusterArgs{}
	tc := testcluster.StartTestCluster(t, 2 /* nodes */, args)
	defer tc.Stopper().Stop(ctx)

	tenn, err := tc.Server(0).StartTenant(ctx, base.TestTenantArgs{TenantID: serverutils.TestTenantID()})
	require.NoError(t, err, "Failed to start tenant node")
	ex := tenn.DistSQLServer().(*distsql.ServerImpl).ServerConfig.Executor
	_, err = ex.Exec(ctx, "get table", nil, /* txn */
		"select * from crdb_internal.cluster_inflight_traces WHERE trace_id = 4;")
	require.Error(t, err, "cluster_inflight_traces should be unsupported")
	require.Contains(t, err.Error(), "table crdb_internal.cluster_inflight_traces is not implemented on tenants")
}

func TestSystemConfigWatcherCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	systemconfigwatchertest.TestSystemConfigWatcher(t, false /* skipSecondary */)
}
