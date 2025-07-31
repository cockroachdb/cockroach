// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverccl

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/licenseccl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// TestSQLServer starts up a semi-dedicated SQL server and runs some smoke test
// queries.
func TestSQLServer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	ts, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		// This test is specific to secondary tenants; no need to run it
		// using the system tenant.
		DefaultTestTenant: base.TestTenantAlwaysEnabled,
	})
	defer ts.Stopper().Stop(ctx)

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

	tc := serverutils.StartCluster(t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	}})
	defer tc.Stopper().Stop(ctx)

	// StartTenant with the default permissions to
	_, db := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{TenantID: serverutils.TestTenantID()})
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

// TestTenantCanUseEnterpriseFeaturesWithEnvVar verifies that tenants
// can get a license from the env variable.
func TestTenantCanUseEnterpriseFeaturesWithEnvVar(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	license, _ := (&licenseccl.License{
		Type: licenseccl.License_Enterprise,
	}).Encode()

	defer ccl.TestingDisableEnterprise()()
	defer envutil.TestSetEnv(t, "COCKROACH_TENANT_LICENSE", license)()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(context.Background())

	_, db := serverutils.StartTenant(t, s, base.TestTenantArgs{TenantID: serverutils.TestTenantID()})
	defer db.Close()

	_, err := db.Exec(`BACKUP INTO 'userfile:///backup'`)
	require.NoError(t, err)
	_, err = db.Exec(`BACKUP INTO LATEST IN 'userfile:///backup'`)
	require.NoError(t, err)
}

// TestTenantCanUseEnterpriseFeatures verifies that tenants can get a license
// from the cluster setting.
func TestTenantCanUseEnterpriseFeaturesWithSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	license, _ := (&licenseccl.License{
		Type:             licenseccl.License_Enterprise,
		OrganizationName: "mytest",
	}).Encode()

	defer ccl.TestingDisableEnterprise()()

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	ie := s.SystemLayer().InternalExecutor().(isql.Executor)
	_, err := ie.Exec(ctx, "set-license", nil, "SET CLUSTER SETTING cluster.organization = 'mytest'")
	require.NoError(t, err)
	_, err = ie.Exec(ctx, "set-license", nil, "SET CLUSTER SETTING enterprise.license = $1", license)
	require.NoError(t, err)

	_, db := serverutils.StartTenant(t, s, base.TestTenantArgs{TenantID: serverutils.TestTenantID()})
	defer db.Close()

	_, err = db.Exec(`BACKUP INTO 'userfile:///backup'`)
	require.NoError(t, err)
	_, err = db.Exec(`BACKUP INTO LATEST IN 'userfile:///backup'`)
	require.NoError(t, err)
}

func TestTenantUnauthenticatedAccess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	_, err := s.TenantController().StartTenant(ctx,
		base.TestTenantArgs{
			TenantID: roachpb.MustMakeTenantID(securitytest.EmbeddedTenantIDs()[0]),
			TestingKnobs: base.TestingKnobs{
				TenantTestingKnobs: &sql.TenantTestingKnobs{
					// Configure the SQL server to access the wrong tenant keyspace.
					TenantIDCodecOverride: roachpb.MustMakeTenantID(securitytest.EmbeddedTenantIDs()[1]),
				},
			},
		})
	require.Error(t, err)
	require.Regexp(t, `requested key .* not fully contained in tenant keyspace /Tenant/1{0-1}.*Unauthenticated`, err)
}

// TestTenantHTTP verifies that SQL tenant servers expose metrics and debugging endpoints.
func TestTenantHTTP(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		// This test is specific to secondary tenants; no need to run it
		// using the system tenant.
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSharedProcessModeButDoesntYet(
			base.TestTenantAlwaysEnabled, 113187,
		),
	})
	defer s.Stopper().Stop(ctx)

	ts := s.ApplicationLayer()

	t.Run("prometheus", func(t *testing.T) {
		httpClient, err := ts.GetUnauthenticatedHTTPClient()
		require.NoError(t, err)
		defer httpClient.CloseIdleConnections()
		resp, err := httpClient.Get(ts.AdminURL().WithPath("/_status/vars").String())
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), "sql_ddl_started_count_internal")
	})
	t.Run("pprof", func(t *testing.T) {
		httpClient, err := ts.GetAdminHTTPClient()
		require.NoError(t, err)
		defer httpClient.CloseIdleConnections()
		u := ts.AdminURL().WithPath("/debug/pprof/goroutine")
		q := u.Query()
		q.Set("debug", "2")
		u.RawQuery = q.Encode()
		resp, err := httpClient.Get(u.String())
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), "goroutine")
	})
}

// TestTenantProcessDebugging verifies that in-process SQL tenant servers gate
// process debugging behind capabilities.
func TestTenantProcessDebugging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// External service tenants are allowed to debug their own processes without
	// capabilities and shared service tenants implicitly have all capabilities,
	// so we currently never expect a tenant hitting their admin server -- whether
	// that is their own process or the system one.
	const expectDebugToRequireCap = false

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	tenant, _, err := s.TenantController().StartSharedProcessTenant(ctx,
		base.TestSharedProcessTenantArgs{
			TenantID:   serverutils.TestTenantID(),
			TenantName: "processdebug",
		})
	require.NoError(t, err)
	defer tenant.AppStopper().Stop(ctx)

	t.Run("system tenant pprof", func(t *testing.T) {
		httpClient, err := s.GetAdminHTTPClient()
		require.NoError(t, err)
		defer httpClient.CloseIdleConnections()

		url := s.AdminURL().WithPath("/debug/pprof/goroutine")
		q := url.Query()
		q.Add("debug", "2")
		url.RawQuery = q.Encode()

		resp, err := httpClient.Get(url.String())
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Contains(t, string(body), "goroutine")
	})

	t.Run("pprof", func(t *testing.T) {
		httpClient, err := tenant.GetAdminHTTPClient()
		require.NoError(t, err)
		defer httpClient.CloseIdleConnections()

		url := tenant.AdminURL().WithPath("/debug/pprof/")
		q := url.Query()
		q.Add("debug", "2")
		url.RawQuery = q.Encode()

		if expectDebugToRequireCap {
			resp, err := httpClient.Get(url.String())
			require.NoError(t, err)
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, http.StatusForbidden, resp.StatusCode)
			require.Contains(t, string(body), "tenant does not have capability to debug the running process")

			_, err = db.Exec(`ALTER TENANT processdebug GRANT CAPABILITY can_debug_process=true`)
			require.NoError(t, err)

			serverutils.WaitForTenantCapabilities(t, s, serverutils.TestTenantID(), map[tenantcapabilities.ID]string{
				tenantcapabilities.CanDebugProcess: "true",
			}, "")
		}
		resp, err := httpClient.Get(url.String())
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Contains(t, string(body), "goroutine")

		_, err = db.Exec(`ALTER TENANT processdebug REVOKE CAPABILITY can_debug_process`)
		require.NoError(t, err)

		serverutils.WaitForTenantCapabilities(t, s, serverutils.TestTenantID(), map[tenantcapabilities.ID]string{
			tenantcapabilities.CanDebugProcess: "false",
		}, "")
	})

	t.Run("vmodule", func(t *testing.T) {
		httpClient, err := tenant.GetAdminHTTPClient()
		require.NoError(t, err)
		defer httpClient.CloseIdleConnections()

		url := tenant.AdminURL().WithPath("/debug/vmodule")
		q := url.Query()
		q.Add("duration", "-1s")
		q.Add("vmodule", "exec_log=3")
		url.RawQuery = q.Encode()

		if expectDebugToRequireCap {
			resp, err := httpClient.Get(url.String())
			require.NoError(t, err)
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, http.StatusForbidden, resp.StatusCode)
			require.Contains(t, string(body), "tenant does not have capability to debug the running process")

			_, err = db.Exec(`ALTER TENANT processdebug GRANT CAPABILITY can_debug_process=true`)
			require.NoError(t, err)

			serverutils.WaitForTenantCapabilities(t, s, serverutils.TestTenantID(), map[tenantcapabilities.ID]string{
				tenantcapabilities.CanDebugProcess: "true",
			}, "")
		}
		resp, err := httpClient.Get(url.String())
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Contains(t, string(body), "previous vmodule configuration: \nnew vmodule configuration: exec_log=3\n")

		_, err = db.Exec(`ALTER TENANT processdebug REVOKE CAPABILITY can_debug_process`)
		require.NoError(t, err)

		serverutils.WaitForTenantCapabilities(t, s, serverutils.TestTenantID(), map[tenantcapabilities.ID]string{
			tenantcapabilities.CanDebugProcess: "false",
		}, "")
	})
}

func TestNonExistentTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	_, err := s.TenantController().StartTenant(ctx,
		base.TestTenantArgs{
			TenantID:            serverutils.TestTenantID(),
			DisableCreateTenant: true,
			SkipTenantCheck:     true,

			SkipWaitForTenantCache: true,

			TestingKnobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					ShutdownTenantConnectorEarlyIfNoRecordPresent: true,
				},
			},
		})
	require.True(t, errors.Is(err, &kvpb.MissingRecordError{}))
}

// TestTenantRowIDs confirms `unique_rowid()` works as expected in a
// multi-tenant setup.
func TestTenantRowIDs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestTenantAlwaysEnabled,
	})
	defer s.Stopper().Stop(ctx)
	const numRows = 10
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TABLE foo(key INT PRIMARY KEY DEFAULT unique_rowid(), val INT)`)
	sqlDB.Exec(t, fmt.Sprintf("INSERT INTO foo (val) SELECT * FROM generate_series(1, %d)", numRows))

	// Verify that the rows are inserted successfully and that the row ids
	// are based on the SQL instance ID.
	rows := sqlDB.Query(t, "SELECT key FROM foo")
	defer rows.Close()
	rowCount := 0
	instanceID := int(s.ApplicationLayer().SQLInstanceID())
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

// TestTenantInstanceIDReclaimLoop confirms that the sql_instances reclaim loop
// has been started.
func TestTenantInstanceIDReclaimLoop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	clusterSettings := func() *cluster.Settings {
		cs := cluster.MakeTestingClusterSettings()
		instancestorage.ReclaimLoopInterval.Override(ctx, &cs.SV, 250*time.Millisecond)
		instancestorage.PreallocatedCount.Override(ctx, &cs.SV, 5)
		return cs
	}

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Settings:          clusterSettings(),
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	_, db := serverutils.StartTenant(
		t, s, base.TestTenantArgs{
			TenantID: serverutils.TestTenantID(),
			Settings: clusterSettings(),
		},
	)
	defer db.Close()
	sqlDB := sqlutils.MakeSQLRunner(db)

	var rowCount int64
	testutils.SucceedsSoon(t, func() error {
		sqlDB.QueryRow(t, `SELECT count(*) FROM system.sql_instances WHERE addr IS NULL`).Scan(&rowCount)
		// We set PreallocatedCount to 5. When the tenant gets started, it drops
		// to 4. Eventually this will be 5 if the reclaim loop runs.
		if rowCount == 5 {
			return nil
		}
		return fmt.Errorf("waiting for preallocated rows")
	})
}

// TestStartTenantWithStaleInstance covers the following scenario:
// - a sql server starts up and is assigned port 'a'
// - the sql server shuts down and releases port 'a'
// - something else starts up and claims port 'a'. In the test that is the
// listener. This is important because the listener causes connections to 'a' to
// hang instead of responding with a RESET packet.
// - a different server with stale instance information schedules a distsql
// flow and attempts to dial port 'a'.
func TestStartTenantWithStaleInstance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	var listener net.Listener
	// In rare cases under stress net.Listen call can result in an error that
	// the address is already in use (because the stopped tenant hasn't released
	// the socket); thus, we allow for some retries to go around that issue.
	testutils.SucceedsSoon(t, func() error {
		rpcAddr := func() string {
			tenantStopper := stop.NewStopper()
			defer tenantStopper.Stop(ctx)
			server, db := serverutils.StartTenant(t, s, base.TestTenantArgs{
				Stopper:  tenantStopper,
				TenantID: serverutils.TestTenantID(),
			},
			)
			defer db.Close()
			return server.RPCAddr()
		}()

		var err error
		listener, err = net.Listen("tcp", rpcAddr)
		return err
	})
	defer func() {
		_ = listener.Close()
	}()

	_, db := serverutils.StartTenant(t, s, base.TestTenantArgs{
		TenantID: serverutils.TestTenantID(),
	})
	defer func() {
		_ = db.Close()
	}()

	// Query a table to make sure the tenant is healthy, doesn't really matter
	// which table.
	_, err := db.Exec("SELECT count(*) FROM system.sqlliveness")
	require.NoError(t, err)
}
