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
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
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
	require.Equal(t, pq.ErrorCode(pgcode.InsufficientPrivilege.String()), pqErr.Code, "err %v has unexpected code", err)
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
		resp, err := httputil.Get(ctx, "http://"+tenant.HTTPAddr()+"/_status/vars")
		defer http.DefaultClient.CloseIdleConnections()
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), "sql_ddl_started_count_internal")
	})
	t.Run("pprof", func(t *testing.T) {
		resp, err := httputil.Get(ctx, "http://"+tenant.HTTPAddr()+"/debug/pprof/goroutine?debug=2")
		defer http.DefaultClient.CloseIdleConnections()
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), "goroutine")
	})

}

func TestIdleExit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	warmupDuration := 500 * time.Millisecond
	countdownDuration := 4000 * time.Millisecond
	tenant, err := tc.Server(0).StartTenant(ctx,
		base.TestTenantArgs{
			TenantID:      serverutils.TestTenantID(),
			IdleExitAfter: warmupDuration,
			TestingKnobs: base.TestingKnobs{
				TenantTestingKnobs: &sql.TenantTestingKnobs{
					IdleExitCountdownDuration: countdownDuration,
				},
			},
			Stopper: tc.Stopper(),
		})

	require.NoError(t, err)

	time.Sleep(warmupDuration / 2)
	log.Infof(context.Background(), "Opening first con")
	db := serverutils.OpenDBConn(
		t, tenant.SQLAddr(), "", false, tc.Stopper(),
	)
	r := sqlutils.MakeSQLRunner(db)
	r.QueryStr(t, `SELECT 1`)
	require.NoError(t, db.Close())

	time.Sleep(warmupDuration/2 + countdownDuration/2)

	// Opening a connection in the middle of the countdown should stop the
	// countdown timer. Closing the connection will restart the countdown.
	log.Infof(context.Background(), "Opening second con")
	db = serverutils.OpenDBConn(
		t, tenant.SQLAddr(), "", false, tc.Stopper(),
	)
	r = sqlutils.MakeSQLRunner(db)
	r.QueryStr(t, `SELECT 1`)
	require.NoError(t, db.Close())

	time.Sleep(countdownDuration / 2)

	// If the tenant is stopped, that most likely means that the second connection
	// didn't stop the countdown
	select {
	case <-tc.Stopper().IsStopped():
		t.Error("stop on idle triggered too early")
	default:
	}

	time.Sleep(countdownDuration * 3 / 2)

	select {
	case <-tc.Stopper().IsStopped():
	default:
		t.Error("stop on idle didn't trigger")
	}
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
