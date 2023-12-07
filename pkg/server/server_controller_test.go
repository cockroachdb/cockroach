// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

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

	_, err = db.Exec("CREATE TENANT hello; ALTER TENANT hello START SERVICE SHARED")
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

// TestServerControllerStopReallyStops starts and stops a
// share-process test tenant and tries to detect if any goroutines
// related to the tenant server are still running after the server has
// been removed from the server controller.
func TestServerControllerStopReallyStops(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	waitForNoServiceForTenant := func() {
		// We don't use succeeds soon here because we want to
		// return as soon as we can after our server has been
		// removed from the list.
		for {
			_, _, err := s.TenantController().ServerController().(*serverController).getServer(ctx, "hello")
			if errors.Is(err, errNoTenantServerRunning) {
				return
			}
			time.Sleep(time.Duration(1))
		}
	}
	waitForServiceForTenant := func() {
		testutils.SucceedsSoon(t, func() error {
			_, _, err := s.TenantController().ServerController().(*serverController).getServer(ctx, "hello")
			return err
		})
	}

	failIfTaggedGoRoutines := func(dump string) {
		// We expect a profile that looks like:
		// goroutine profile: total 445
		// 20 @ 0x102108788 0x1020d3dd4 0x1020d39a4 0x102e80d70 0x10228ce70 0x102e80ccc 0x10213d5f4
		// # labels: {"pebble":"table-cache"}
		// #      0x102e80d6f     github.com/cockroachdb/pebble.(*tableCacheShard).releaseLoop....
		// #      ...
		// #      0x102e80ccb     github.com/cockroachdb/pebble.(*tableCacheShard).releas...
		//
		// 13 @ 0x102108788 0x102119aa8 0x1050a2e50 0x1050a3044 0x1050a7b14 0x102d3a508 0x10213d5f4
		// #      0x1050a2e4f     github.com/cockroachdb/cockroach/pkg/kv/kvserver/...
		// #      ...
		// #      0x102d3a507     github.com/cockroachdb/cockroach/pkg/util/stop.(*Sto...
		goroutines := strings.Split(dump, "\n\n")
		labelWeCareAbout := `"cluster":"hello"`
		exceptions := []string{
			// When using a shared-process server, we may end up
			// using the internalClientAdapter. As a result, async
			// processes started on the kvserver-side of some
			// request may actually be started from a goroutine with
			// the cluster tags.
			//
			// TODO(ssd): Understand what the abvoe implies for when
			// we can actually be gauaranteed no more
			// (user-observable) writes to the tenant keyspace will
			// happen.
			"(*IntentResolver).cleanupFinishedTxnIntents",
			// TODO(ssd): I suspect we see this with our label for a
			// reason similar to the above. But, I've yet to trace
			// it in the same way.
			"github.com/cockroachdb/pebble/vfs.(*diskHealthCheckingFS).startTickerLocked",
			"github.com/cockroachdb/pebble/vfs.(*diskHealthCheckingFile).startTicker",
		}

		var errBuf strings.Builder
		for _, goRoutine := range goroutines {
			if strings.Contains(goRoutine, labelWeCareAbout) {
				var ignore bool
				for _, e := range exceptions {
					if strings.Contains(goRoutine, e) {
						t.Logf("ignoring goroutine that matches %s", e)
						ignore = true
						break
					}
				}
				if !ignore {
					errBuf.WriteString(goRoutine)
					errBuf.WriteString("\n")
				}
			}
		}
		errStr := errBuf.String()
		if errStr != "" {
			t.Fatalf("unexpected goroutines from tenant server still running after stop:\n%s", errStr)
		}
	}

	sqlRunner := sqlutils.MakeSQLRunner(db)
	sqlRunner.Exec(t, "CREATE TENANT hello")
	sqlRunner.Exec(t, "ALTER VIRTUAL CLUSTER hello START SERVICE SHARED")
	waitForServiceForTenant()
	sqlRunner.Exec(t, "ALTER VIRTUAL CLUSTER hello STOP SERVICE")
	waitForNoServiceForTenant()
	// Every method of "waiting" for a goroutine technically
	// returns just before the goroutine actually completes. As a
	// aresult, in some race/stress builds this test ends up
	// catching goroutines that are properly accounted for.
	//
	// Unfortunately this defeates the point of the test somewhat
	// so it is useful to remove when investigating a shutdown
	// problem.
	time.Sleep(10 * time.Millisecond)
	var buf strings.Builder
	require.NoError(t, pprof.Lookup("goroutine").WriteTo(&buf, 1))
	failIfTaggedGoRoutines(buf.String())
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

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
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
