// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerController(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	sc := s.TenantController().ServerController().(*serverController)
	ts := s.SystemLayer().(*testServer)

	d, _, err := sc.getServer(ctx, "system")
	require.NoError(t, err)
	if d.(*systemServerWrapper).server != ts.topLevelServer {
		t.Fatal("expected wrapped system server")
	}

	d, _, err = sc.getServer(ctx, "somename")
	require.Nil(t, d)
	require.Error(t, err, `no tenant found with name "somename"`)

	_, err = db.Exec("CREATE TENANT hello")
	require.NoError(t, err)
	_, err = db.Exec("ALTER TENANT hello START SERVICE SHARED")
	require.NoError(t, err)

	_, _, err = sc.getServer(ctx, "hello")
	// TODO(knz): We're not really expecting an error here.
	// The actual error seen will exist as long as in-memory
	// servers use the standard KV connector.
	//
	// To make this error go away, we need either to place
	// this test in a separate CCL package, or to make these servers
	// use a new non-CCL connector.
	//
	// However, none of this is necessary to test the
	// controller itself: it's sufficient to see that the
	// tenant constructor was called.
	require.Error(t, err, "tenant connector requires a CCL binary")
	// TODO(knz): test something about d.
}

// TestServerControllerStopStart is, when run under stress, a
// regression test for #112077, a bug in which we would fail to
// respond to a service start request that occured while a server was
// shutting down.
func TestServerControllerStopStart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	sqlRunner := sqlutils.MakeSQLRunner(db)
	// Speed up the tenant capabilities watcher to increase chance of hitting race.
	sqlRunner.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'")
	sqlRunner.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval ='100ms'")
	sqlRunner.Exec(t, "SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval ='100ms'")

	tryConnect := func() error {
		conn, err := s.SystemLayer().SQLConnE(serverutils.DBName("cluster:hello"))
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		return conn.Ping()
	}

	shouldConnectSoon := func() {
		testutils.SucceedsSoon(t, tryConnect)
	}

	shouldFailToConnectSoon := func() {
		testutils.SucceedsSoon(t, func() error {
			if err := tryConnect(); err == nil {
				return errors.Newf("still accepting connections")
			}
			return nil
		})
	}

	sqlRunner.Exec(t, "CREATE TENANT hello")
	sqlRunner.Exec(t, "ALTER VIRTUAL CLUSTER hello START SERVICE SHARED")
	shouldConnectSoon()
	sqlRunner.Exec(t, "ALTER VIRTUAL CLUSTER hello STOP SERVICE")
	shouldFailToConnectSoon()
	sqlRunner.Exec(t, "ALTER VIRTUAL CLUSTER hello START SERVICE SHARED")
	shouldConnectSoon()
}

// TestServerControllerWaitForDefaultTenant tests that the server SQL
// controller knows how to wait for the default cluster.
func TestServerControllerWaitForDefaultCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	sqlRunner := sqlutils.MakeSQLRunner(db)
	if skip.Duress() {
		sqlRunner.Exec(t, "SET CLUSTER SETTING server.controller.mux_virtual_cluster_wait.timeout = '1m'")
	}

	tryConnect := func() error {
		conn, err := s.SystemLayer().SQLConnE(serverutils.DBName("cluster:hello"))
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		return conn.Ping()
	}

	sqlRunner.Exec(t, "CREATE TENANT hello")
	sqlRunner.Exec(t, "ALTER VIRTUAL CLUSTER hello START SERVICE SHARED")
	sqlRunner.Exec(t, "SET CLUSTER SETTING server.controller.default_target_cluster = 'hello'")
	require.NoError(t, tryConnect())
}

func TestSQLErrorUponInvalidTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	db, err := s.SystemLayer().SQLConnE(serverutils.DBName("cluster:nonexistent"))
	// Expect no error yet: the connection is opened lazily; an
	// error here means the parameters were incorrect.
	require.NoError(t, err)

	err = db.Ping()
	require.NotNil(t, err)
	require.Regexp(t, `service unavailable for target tenant \(nonexistent\)`, err.Error())

	// Regression test for CRDB-40449; make sure pre-conn memory is freed.
	testutils.SucceedsSoon(t, func() error {
		var usedPreConnMemory int
		err = sqlDB.QueryRow("select used from crdb_internal.node_memory_monitors where name='pre-conn'").Scan(&usedPreConnMemory)
		if err != nil {
			return err
		}
		if usedPreConnMemory != 0 {
			return errors.Errorf("expected 0 bytes used, got %d", usedPreConnMemory)
		}
		return nil
	})
}

func TestSharedProcessServerInheritsTempStorageLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const specialSize = 123123123

	// Start a server with a custom temp storage limit.
	ctx := context.Background()
	st := cluster.MakeClusterSettings()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Settings:          st,
		TempStorageConfig: base.DefaultTestTempStorageConfigWithSize(st, specialSize),
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	// Start a shared process tenant server.
	ts, _, err := s.TenantController().StartSharedProcessTenant(ctx, base.TestSharedProcessTenantArgs{
		TenantName: "hello",
	})
	require.NoError(t, err)

	tss := ts.(*testTenant)
	require.Equal(t, int64(specialSize), tss.SQLCfg.TempStorageConfig.Mon.Limit())
}

// TestServerSQLConn checks that the SQLConn() method on the
// SystemLayer() of TestServerInterface works even when non-specific
// SQL connection requests are redirected to a secondary tenant.
func TestServerSQLConn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)
	systemTenant := s.SystemLayer()

	// Start some secondary tenant servers.
	secondaryTenantExtNoName, err := s.TenantController().StartTenant(ctx, base.TestTenantArgs{
		TenantID: roachpb.MustMakeTenantID(2),
	})
	require.NoError(t, err)

	secondaryTenantExtNamed, err := s.TenantController().StartTenant(ctx, base.TestTenantArgs{
		TenantName: "hello",
		TenantID:   roachpb.MustMakeTenantID(10),
	})
	require.NoError(t, err)

	secondaryTenantSh, _, err := s.TenantController().StartSharedProcessTenant(ctx, base.TestSharedProcessTenantArgs{
		TenantName: "world",
	})
	require.NoError(t, err)
	multitenant.DefaultTenantSelect.Override(ctx, &systemTenant.ClusterSettings().SV, "world")

	for _, tc := range []struct {
		testName     string
		tbName       string
		sqlInterface serverutils.ApplicationLayerInterface
	}{
		{"system", "foo", systemTenant},
		{"secondary-external-noname", "bar", secondaryTenantExtNoName},
		{"secondary-external-named", "baz", secondaryTenantExtNamed},
		{"secondary-shared", "qux", secondaryTenantSh},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			_, err = tc.sqlInterface.InternalExecutor().(isql.Executor).
				Exec(ctx, "create-table", nil, "CREATE TABLE defaultdb."+tc.tbName+" (i INT)")
			require.NoError(t, err)

			conn := tc.sqlInterface.SQLConn(t, serverutils.DBName("defaultdb"))
			var unused int
			assert.NoError(t, conn.QueryRowContext(ctx, "SELECT count(*) FROM "+tc.tbName).Scan(&unused))
		})
	}
}
