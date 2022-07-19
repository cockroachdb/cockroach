// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"context"
	"crypto/tls"
	gosql "database/sql"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/balancer"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/denylist"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenantdirsvr"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/throttler"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	pgproto3 "github.com/jackc/pgproto3/v2"
	pgx "github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// To ensure tenant startup code is included.
var _ = kvtenantccl.Connector{}

const frontendError = "Frontend error!"
const backendError = "Backend error!"

// notFoundTenantID is used to trigger a NotFound error when it is requested in
// the test directory server.
const notFoundTenantID = 99

func TestLongDBName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	defer testutils.TestingHook(&BackendDial, func(
		_ *pgproto3.StartupMessage, outgoingAddr string, _ *tls.Config,
	) (net.Conn, error) {
		require.Equal(t, outgoingAddr, "127.0.0.1:26257")
		return nil, newErrorf(codeParamsRoutingFailed, "boom")
	})()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	s, addr := newSecureProxyServer(
		ctx, t, stopper, &ProxyOptions{RoutingRule: "127.0.0.1:26257"})

	longDB := strings.Repeat("x", 70) // 63 is limit
	pgurl := fmt.Sprintf("postgres://unused:unused@%s/%s?options=--cluster=tenant-cluster-28&sslmode=require", addr, longDB)
	te.TestConnectErr(ctx, t, pgurl, codeParamsRoutingFailed, "boom")
	require.Equal(t, int64(1), s.metrics.RoutingErrCount.Count())
}

// TestBackendDownRetry tries to connect to a unavailable backend. After 3
// failed attempts, a "tenant not found" error simulates the tenant being
// deleted.
func TestBackendDownRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	opts := &ProxyOptions{RoutingRule: "undialable%$!@$:1234"}
	// Set RefreshDelay to -1 so that we could simulate a ListPod call under
	// the hood, which then triggers an EnsurePod again.
	opts.testingKnobs.dirOpts = []tenant.DirOption{tenant.RefreshDelay(-1)}
	server, addr := newSecureProxyServer(ctx, t, stopper, opts)
	directoryServer := mustGetTestSimpleDirectoryServer(t, server.handler)

	callCount := 0
	defer testutils.TestingHook(&BackendDial, func(
		_ *pgproto3.StartupMessage, outgoingAddr string, _ *tls.Config,
	) (net.Conn, error) {
		callCount++
		// After 3 dials, we delete the tenant.
		if callCount >= 3 {
			directoryServer.DeleteTenant(roachpb.MakeTenantID(28))
		}
		return nil, newErrorf(codeBackendDown, "SQL pod is down")
	})()

	// Valid connection, but no backend server running.
	pgurl := fmt.Sprintf("postgres://unused:unused@%s/db?options=--cluster=tenant-cluster-28&sslmode=require", addr)
	te.TestConnectErr(ctx, t, pgurl, codeParamsRoutingFailed, "cluster tenant-cluster-28 not found")
	require.Equal(t, 3, callCount)
}

func TestFailedConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	s, proxyAddr := newSecureProxyServer(ctx, t, stopper, &ProxyOptions{RoutingRule: "undialable%$!@$"})

	// TODO(asubiotto): consider using datadriven for these, especially if the
	// proxy becomes more complex.

	_, p, err := addr.SplitHostPort(proxyAddr, "")
	require.NoError(t, err)
	u := fmt.Sprintf("postgres://unused:unused@localhost:%s/", p)

	// Unencrypted connections bounce.
	te.TestConnectErr(
		ctx, t, u+"?options=--cluster=tenant-cluster-28&sslmode=disable",
		codeUnexpectedInsecureStartupMessage, "server requires encryption",
	)
	require.Equal(t, int64(0), s.metrics.RoutingErrCount.Count())

	sslModesUsingTLS := []string{"require", "allow"}
	for i, sslmode := range sslModesUsingTLS {
		// TenantID rejected as malformed.
		te.TestConnectErr(
			ctx, t, u+"?options=--cluster=dimdog&sslmode="+sslmode,
			codeParamsRoutingFailed, "invalid cluster identifier 'dimdog'",
		)
		require.Equal(t, int64(1+(i*3)), s.metrics.RoutingErrCount.Count())

		// No cluster name and TenantID.
		te.TestConnectErr(
			ctx, t, u+"?sslmode="+sslmode,
			codeParamsRoutingFailed, "missing cluster identifier",
		)
		require.Equal(t, int64(2+(i*3)), s.metrics.RoutingErrCount.Count())

		// Bad TenantID. Ensure that we don't leak any parsing errors.
		te.TestConnectErr(
			ctx, t, u+"?options=--cluster=tenant-cluster-foo3&sslmode="+sslmode,
			codeParamsRoutingFailed, "invalid cluster identifier 'tenant-cluster-foo3'",
		)
		require.Equal(t, int64(3+(i*3)), s.metrics.RoutingErrCount.Count())
	}
}

func TestUnexpectedError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	// Set up a Server whose FrontendAdmitter function always errors with a
	// non-codeError error.
	defer testutils.TestingHook(&FrontendAdmit, func(
		conn net.Conn, incomingTLSConfig *tls.Config,
	) *FrontendAdmitInfo {
		log.Infof(context.Background(), "frontend admitter returning unexpected error")
		return &FrontendAdmitInfo{Conn: conn, Err: errors.New("unexpected error")}
	})()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	_, addr := newProxyServer(ctx, t, stopper, &ProxyOptions{})

	u := fmt.Sprintf("postgres://root:admin@%s/?sslmode=disable&connect_timeout=5", addr)

	// Time how long it takes for pgx.Connect to return. If the proxy handles
	// errors appropriately, pgx.Connect should return near immediately
	// because the server should close the connection. If not, it may take up
	// to the 5s connect_timeout for pgx.Connect to give up.
	start := timeutil.Now()
	_, err := pgx.Connect(ctx, u)
	require.Contains(t, err.Error(), "internal server error")
	t.Log(err)
	elapsed := timeutil.Since(start)
	if elapsed >= 5*time.Second {
		t.Errorf("pgx.Connect took %s to error out", elapsed)
	}
}

func TestProxyAgainstSecureCRDB(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	sql, db, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	pgs := sql.(*server.TestServer).PGServer().(*pgwire.Server)
	pgs.TestingSetTrustClientProvidedRemoteAddr(true)
	pgs.TestingEnableAuthLogging()
	defer sql.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE USER bob WITH PASSWORD 'builder'`)

	s, addr := newSecureProxyServer(
		ctx, t, sql.Stopper(), &ProxyOptions{RoutingRule: sql.ServingSQLAddr(), SkipVerify: true},
	)
	_, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)

	url := fmt.Sprintf("postgres://bob:wrong@%s/tenant-cluster-28.defaultdb?sslmode=require", addr)
	te.TestConnectErr(ctx, t, url, 0, "failed SASL auth|password authentication failed")

	url = fmt.Sprintf("postgres://bob@%s/tenant-cluster-28.defaultdb?sslmode=require", addr)
	te.TestConnectErr(ctx, t, url, 0, "failed SASL auth|password authentication failed")

	url = fmt.Sprintf("postgres://bob:builder@toothless-28.blah:%s/defaultdb?sslmode=require", port)
	te.TestConnectErr(ctx, t, url, codeParamsRoutingFailed, "server error")

	url = fmt.Sprintf("postgres://bob:builder@tenant-cluster-28.blah:%s/defaultdb?sslmode=require", port)
	te.TestConnectErr(ctx, t, url, codeParamsRoutingFailed, "server error")

	url = fmt.Sprintf("postgres://bob:builder@%s/tenant-cluster-28.defaultdb?sslmode=require", addr)
	te.TestConnect(ctx, t, url, func(conn *pgx.Conn) {
		require.Equal(t, int64(1), s.metrics.CurConnCount.Value())
		require.NoError(t, runTestQuery(ctx, conn))
	})

	// SNI provides tenant ID.
	url = fmt.Sprintf("postgres://bob:builder@serverless-28.blah:%s/defaultdb?sslmode=require", port)
	te.TestConnect(ctx, t, url, func(conn *pgx.Conn) {
		require.Equal(t, int64(1), s.metrics.CurConnCount.Value())
		require.NoError(t, runTestQuery(ctx, conn))
	})

	// SNI and database provide tenant IDs that match.
	url = fmt.Sprintf(
		"postgres://bob:builder@serverless-28.blah:%s/tenant-cluster-28.defaultdb?sslmode=require", port,
	)
	te.TestConnect(ctx, t, url, func(conn *pgx.Conn) {
		require.Equal(t, int64(1), s.metrics.CurConnCount.Value())
		require.NoError(t, runTestQuery(ctx, conn))
	})

	// SNI and database provide tenant IDs that don't match.
	url = fmt.Sprintf(
		"postgres://bob:builder@serverless-28.blah:%s/tenant-cluster-29.defaultdb?sslmode=require", port,
	)
	te.TestConnectErr(ctx, t, url, codeParamsRoutingFailed, "server error")

	require.Equal(t, int64(3), s.metrics.SuccessfulConnCount.Count())
	require.Equal(t, int64(2), s.metrics.AuthFailedCount.Count())
	require.Equal(t, int64(3), s.metrics.RoutingErrCount.Count())
}

func TestProxyTLSConf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("insecure", func(t *testing.T) {
		ctx := context.Background()
		te := newTester()
		defer te.Close()

		defer testutils.TestingHook(&BackendDial, func(
			_ *pgproto3.StartupMessage, _ string, tlsConf *tls.Config,
		) (net.Conn, error) {
			require.Nil(t, tlsConf)
			return nil, newErrorf(codeParamsRoutingFailed, "boom")
		})()

		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		_, addr := newSecureProxyServer(ctx, t, stopper, &ProxyOptions{
			Insecure:    true,
			RoutingRule: "127.0.0.1:26257",
		})

		pgurl := fmt.Sprintf("postgres://unused:unused@%s/%s?options=--cluster=tenant-cluster-28&sslmode=require", addr, "defaultdb")
		te.TestConnectErr(ctx, t, pgurl, codeParamsRoutingFailed, "boom")
	})

	t.Run("skip-verify", func(t *testing.T) {
		ctx := context.Background()
		te := newTester()
		defer te.Close()

		defer testutils.TestingHook(&BackendDial, func(
			_ *pgproto3.StartupMessage, _ string, tlsConf *tls.Config,
		) (net.Conn, error) {
			require.True(t, tlsConf.InsecureSkipVerify)
			return nil, newErrorf(codeParamsRoutingFailed, "boom")
		})()

		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		_, addr := newSecureProxyServer(ctx, t, stopper, &ProxyOptions{
			Insecure:    false,
			SkipVerify:  true,
			RoutingRule: "127.0.0.1:26257",
		})

		pgurl := fmt.Sprintf("postgres://unused:unused@%s/%s?options=--cluster=tenant-cluster-28&sslmode=require", addr, "defaultdb")
		te.TestConnectErr(ctx, t, pgurl, codeParamsRoutingFailed, "boom")
	})

	t.Run("no-skip-verify", func(t *testing.T) {
		ctx := context.Background()
		te := newTester()
		defer te.Close()

		defer testutils.TestingHook(&BackendDial, func(
			_ *pgproto3.StartupMessage, outgoingAddress string, tlsConf *tls.Config,
		) (net.Conn, error) {
			outgoingHost, _, err := addr.SplitHostPort(outgoingAddress, "")
			require.NoError(t, err)

			require.False(t, tlsConf.InsecureSkipVerify)
			require.Equal(t, tlsConf.ServerName, outgoingHost)
			return nil, newErrorf(codeParamsRoutingFailed, "boom")
		})()

		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		_, addr := newSecureProxyServer(ctx, t, stopper, &ProxyOptions{
			Insecure:    false,
			SkipVerify:  false,
			RoutingRule: "127.0.0.1:26257",
		})

		pgurl := fmt.Sprintf("postgres://unused:unused@%s/%s?options=--cluster=tenant-cluster-28&sslmode=require", addr, "defaultdb")
		te.TestConnectErr(ctx, t, pgurl, codeParamsRoutingFailed, "boom")
	})

}

func TestProxyTLSClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// NB: The leaktest call is an important part of this test. We're
	// verifying that no goroutines are leaked, despite calling Close an
	// underlying TCP connection (rather than the TLSConn that wraps it).

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	sql, db, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	pgs := sql.(*server.TestServer).PGServer().(*pgwire.Server)
	pgs.TestingSetTrustClientProvidedRemoteAddr(true)
	pgs.TestingEnableAuthLogging()
	defer sql.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE USER bob WITH PASSWORD 'builder'`)

	var proxyIncomingConn atomic.Value // *conn
	originalFrontendAdmit := FrontendAdmit
	defer testutils.TestingHook(&FrontendAdmit, func(
		conn net.Conn, incomingTLSConfig *tls.Config,
	) *FrontendAdmitInfo {
		proxyIncomingConn.Store(conn)
		return originalFrontendAdmit(conn, incomingTLSConfig)
	})()

	s, addr := newSecureProxyServer(
		ctx, t, sql.Stopper(), &ProxyOptions{RoutingRule: sql.ServingSQLAddr(), SkipVerify: true},
	)

	url := fmt.Sprintf("postgres://bob:builder@%s/tenant-cluster-28.defaultdb?sslmode=require", addr)

	conn, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	require.Equal(t, int64(1), s.metrics.CurConnCount.Value())
	require.NoError(t, runTestQuery(ctx, conn))

	// Cut the connection.
	incomingConn, ok := proxyIncomingConn.Load().(*proxyConn)
	require.True(t, ok)
	require.NoError(t, incomingConn.Close())
	<-incomingConn.done() // should immediately proceed
	_ = conn.Close(ctx)

	require.Equal(t, int64(1), s.metrics.SuccessfulConnCount.Count())
	require.Equal(t, int64(0), s.metrics.AuthFailedCount.Count())
}

func TestProxyModifyRequestParams(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	sql, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	pgs := sql.(*server.TestServer).PGServer().(*pgwire.Server)
	pgs.TestingSetTrustClientProvidedRemoteAddr(true)
	pgs.TestingEnableAuthLogging()
	defer sql.Stopper().Stop(ctx)

	// Create some user with password authn.
	_, err := sqlDB.Exec("CREATE USER testuser WITH PASSWORD foo123")
	require.NoError(t, err)

	outgoingTLSConfig, err := sql.RPCContext().GetClientTLSConfig()
	require.NoError(t, err)
	proxyOutgoingTLSConfig := outgoingTLSConfig.Clone()
	proxyOutgoingTLSConfig.InsecureSkipVerify = true

	// We wish the proxy to work even without providing a valid TLS client cert to the SQL server.
	proxyOutgoingTLSConfig.Certificates = nil

	originalBackendDial := BackendDial
	defer testutils.TestingHook(&BackendDial, func(
		msg *pgproto3.StartupMessage, outgoingAddress string, tlsConfig *tls.Config,
	) (net.Conn, error) {
		params := msg.Parameters
		authToken, ok := params["authToken"]
		require.True(t, ok)
		require.Equal(t, "abc123", authToken)
		user, ok := params["user"]
		require.True(t, ok)
		require.Equal(t, "bogususer", user)
		require.Contains(t, params, "user")

		// NB: This test will fail unless the user used between the proxy
		// and the backend is changed to a user that actually exists.
		delete(params, "authToken")
		params["user"] = "testuser"

		return originalBackendDial(msg, sql.ServingSQLAddr(), proxyOutgoingTLSConfig)
	})()

	s, proxyAddr := newSecureProxyServer(ctx, t, sql.Stopper(), &ProxyOptions{})

	u := fmt.Sprintf("postgres://bogususer:foo123@%s/?sslmode=require&authToken=abc123&options=--cluster=tenant-cluster-28&sslmode=require", proxyAddr)
	te.TestConnect(ctx, t, u, func(conn *pgx.Conn) {
		require.Equal(t, int64(1), s.metrics.CurConnCount.Value())
		require.NoError(t, runTestQuery(ctx, conn))
	})
}

func TestInsecureProxy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	sql, db, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	pgs := sql.(*server.TestServer).PGServer().(*pgwire.Server)
	pgs.TestingSetTrustClientProvidedRemoteAddr(true)
	pgs.TestingEnableAuthLogging()
	defer sql.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE USER bob WITH PASSWORD 'builder'`)

	s, addr := newProxyServer(
		ctx, t, sql.Stopper(), &ProxyOptions{RoutingRule: sql.ServingSQLAddr(), SkipVerify: true},
	)

	url := fmt.Sprintf("postgres://bob:wrong@%s?sslmode=disable&options=--cluster=tenant-cluster-28&sslmode=require", addr)
	te.TestConnectErr(ctx, t, url, 0, "failed SASL auth|password authentication failed")

	url = fmt.Sprintf("postgres://bob:builder@%s/?sslmode=disable&options=--cluster=tenant-cluster-28&sslmode=require", addr)
	te.TestConnect(ctx, t, url, func(conn *pgx.Conn) {
		require.NoError(t, runTestQuery(ctx, conn))
	})
	require.Equal(t, int64(1), s.metrics.AuthFailedCount.Count())
	require.Equal(t, int64(1), s.metrics.SuccessfulConnCount.Count())
}

func TestErroneousFrontend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	defer testutils.TestingHook(&FrontendAdmit, func(
		conn net.Conn, incomingTLSConfig *tls.Config,
	) *FrontendAdmitInfo {
		return &FrontendAdmitInfo{Conn: conn, Err: errors.New(frontendError)}
	})()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	_, addr := newProxyServer(ctx, t, stopper, &ProxyOptions{})

	url := fmt.Sprintf("postgres://bob:builder@%s/?sslmode=disable&options=--cluster=tenant-cluster-28&sslmode=require", addr)

	// Generic message here as the Frontend's error is not codeError and
	// by default we don't pass back error's text. The startup message doesn't
	// get processed in this case.
	te.TestConnectErr(ctx, t, url, 0, "internal server error")
}

func TestErroneousBackend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	defer testutils.TestingHook(&BackendDial, func(
		msg *pgproto3.StartupMessage, outgoingAddress string, tlsConfig *tls.Config,
	) (net.Conn, error) {
		return nil, errors.New(backendError)
	})()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	_, addr := newProxyServer(ctx, t, stopper, &ProxyOptions{})

	url := fmt.Sprintf("postgres://bob:builder@%s/?sslmode=disable&options=--cluster=tenant-cluster-28&sslmode=require", addr)

	// Generic message here as the Backend's error is not codeError and
	// by default we don't pass back error's text. The startup message has
	// already been processed.
	te.TestConnectErr(ctx, t, url, 0, "internal server error")
}

func TestProxyRefuseConn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	defer testutils.TestingHook(&BackendDial, func(
		msg *pgproto3.StartupMessage, outgoingAddress string, tlsConfig *tls.Config,
	) (net.Conn, error) {
		return nil, newErrorf(codeProxyRefusedConnection, "too many attempts")
	})()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	s, addr := newSecureProxyServer(ctx, t, stopper, &ProxyOptions{})

	url := fmt.Sprintf("postgres://root:admin@%s?sslmode=require&options=--cluster=tenant-cluster-28&sslmode=require", addr)
	te.TestConnectErr(ctx, t, url, codeProxyRefusedConnection, "too many attempts")
	require.Equal(t, int64(1), s.metrics.RefusedConnCount.Count())
	require.Equal(t, int64(0), s.metrics.SuccessfulConnCount.Count())
	require.Equal(t, int64(0), s.metrics.AuthFailedCount.Count())
}

func TestDenylistUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	denyList, err := ioutil.TempFile("", "*_denylist.yml")
	require.NoError(t, err)

	sql, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	sql.(*server.TestServer).PGServer().(*pgwire.Server).TestingSetTrustClientProvidedRemoteAddr(true)
	defer sql.Stopper().Stop(ctx)

	// Create some user with password authn.
	_, err = sqlDB.Exec("CREATE USER testuser WITH PASSWORD foo123")
	require.NoError(t, err)

	outgoingTLSConfig, err := sql.RPCContext().GetClientTLSConfig()
	require.NoError(t, err)
	proxyOutgoingTLSConfig := outgoingTLSConfig.Clone()
	proxyOutgoingTLSConfig.InsecureSkipVerify = true

	// We wish the proxy to work even without providing a valid TLS client cert to the SQL server.
	proxyOutgoingTLSConfig.Certificates = nil

	originalBackendDial := BackendDial
	defer testutils.TestingHook(&BackendDial, func(
		msg *pgproto3.StartupMessage, outgoingAddress string, tlsConfig *tls.Config,
	) (net.Conn, error) {
		time.AfterFunc(100*time.Millisecond, func() {
			dlf := denylist.File{
				Denylist: []*denylist.DenyEntry{
					{
						Entity:     denylist.DenyEntity{Type: denylist.IPAddrType, Item: "127.0.0.1"},
						Expiration: timeutil.Now().Add(time.Minute),
						Reason:     "test-denied",
					},
				},
			}

			bytes, err := dlf.Serialize()
			require.NoError(t, err)
			_, err = denyList.Write(bytes)
			require.NoError(t, err)
		})
		return originalBackendDial(msg, sql.ServingSQLAddr(), proxyOutgoingTLSConfig)
	})()

	s, addr := newSecureProxyServer(ctx, t, sql.Stopper(), &ProxyOptions{
		Denylist:           denyList.Name(),
		PollConfigInterval: 10 * time.Millisecond,
	})
	defer func() { _ = os.Remove(denyList.Name()) }()

	url := fmt.Sprintf("postgres://testuser:foo123@%s/defaultdb_29?sslmode=require&options=--cluster=tenant-cluster-28&sslmode=require", addr)
	te.TestConnect(ctx, t, url, func(conn *pgx.Conn) {
		require.Eventuallyf(
			t,
			func() bool {
				_, err = conn.Exec(context.Background(), "SELECT 1")
				return err != nil
			},
			time.Second, 5*time.Millisecond,
			"Expected the connection to eventually fail",
		)
		require.Regexp(t, "unexpected EOF|connection reset by peer", err.Error())
		require.Equal(t, int64(1), s.metrics.ExpiredClientConnCount.Count())
	})
}

func TestDirectoryConnect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// TODO(jaylim-crl): This is a potential port reuse issue, so skip this
	// under stress. See linked GitHub issue.
	skip.UnderStress(t, "https://github.com/cockroachdb/cockroach/issues/76839")
	skip.UnderDeadlockWithIssue(t, 71365)
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	srv, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	srv.(*server.TestServer).PGServer().(*pgwire.Server).TestingSetTrustClientProvidedRemoteAddr(true)
	defer srv.Stopper().Stop(ctx)

	// Create tenant 28.
	sqlConn := srv.InternalExecutor().(*sql.InternalExecutor)
	_, err := sqlConn.Exec(ctx, "", nil, "SELECT crdb_internal.create_tenant(28)")
	require.NoError(t, err)

	// New test directory server.
	tds1, tdsAddr := newDirectoryServer(ctx, t, srv, &net.TCPAddr{})

	// New proxy server using the directory. Define both the directory and the
	// routing rule so that fallback to the routing rule can be tested.
	opts := &ProxyOptions{
		RoutingRule:   srv.ServingSQLAddr(),
		DirectoryAddr: tdsAddr.String(),
		Insecure:      true,
	}
	_, addr := newProxyServer(ctx, t, srv.Stopper(), opts)

	t.Run("fallback when tenant not found", func(t *testing.T) {
		url := fmt.Sprintf(
			"postgres://root:admin@%s/?sslmode=disable&options=--cluster=tenant-cluster-%d",
			addr, notFoundTenantID)
		te.TestConnectErr(ctx, t, url, codeParamsRoutingFailed, "cluster tenant-cluster-99 not found")
	})

	t.Run("fail to connect to backend", func(t *testing.T) {
		// Retry the backend connection 3 times before permanent failure.
		countFailures := 0
		defer testutils.TestingHook(&BackendDial, func(
			*pgproto3.StartupMessage, string, *tls.Config,
		) (net.Conn, error) {
			countFailures++
			if countFailures >= 3 {
				return nil, newErrorf(codeBackendDisconnected, "backend disconnected")
			}
			return nil, newErrorf(codeBackendDown, "backend down")
		})()

		// Ensure that Directory.ReportFailure is being called correctly.
		countReports := 0
		defer testutils.TestingHook(&reportFailureToDirectoryCache, func(
			ctx context.Context, tenantID roachpb.TenantID, addr string, directoryCache tenant.DirectoryCache,
		) error {
			require.Equal(t, roachpb.MakeTenantID(28), tenantID)
			pods, err := directoryCache.TryLookupTenantPods(ctx, tenantID)
			require.NoError(t, err)
			require.Len(t, pods, 1)
			require.Equal(t, pods[0].Addr, addr)

			countReports++
			err = directoryCache.ReportFailure(ctx, tenantID, addr)
			require.NoError(t, err)
			return err
		})()

		url := fmt.Sprintf("postgres://root:admin@%s/?sslmode=disable&options=--cluster=tenant-cluster-28", addr)
		te.TestConnectErr(ctx, t, url, codeBackendDisconnected, "backend disconnected")
		require.Equal(t, 3, countFailures)
		require.Equal(t, 2, countReports)
	})

	t.Run("successful connection", func(t *testing.T) {
		url := fmt.Sprintf("postgres://root:admin@%s/?sslmode=disable&options=--cluster=tenant-cluster-28", addr)
		te.TestConnect(ctx, t, url, func(conn *pgx.Conn) {
			require.NoError(t, runTestQuery(ctx, conn))
		})
	})

	// Stop the directory server and the tenant SQL process started earlier. This
	// tests whether the proxy can recover when the directory server and a SQL
	// tenant pod restart.
	tds1.Stopper().Stop(ctx)

	// Pass the same tdsAddr used to start up the directory server previously,
	// since it's not allowed to jump to a different address.
	tds2, _ := newDirectoryServer(ctx, t, srv, tdsAddr)
	defer tds2.Stopper().Stop(ctx)

	t.Run("successful connection after restart", func(t *testing.T) {
		// Try to connect through the proxy again. This may take several tries
		// in order to clear the proxy directory of the old SQL tenant process
		// address and replace with the new.
		require.Eventually(t, func() bool {
			url := fmt.Sprintf("postgres://root:admin@%s/?sslmode=disable&options=--cluster=tenant-cluster-28", addr)
			conn, err := pgx.Connect(ctx, url)
			if err != nil {
				return false
			}
			defer func() { _ = conn.Close(ctx) }()
			require.NoError(t, runTestQuery(ctx, conn))
			return true
		}, 30*time.Second, 100*time.Millisecond)
	})
}

func TestConnectionRebalancingDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	defer log.Scope(t).Close(t)

	// Start KV server, and enable session migration.
	params, _ := tests.CreateTestServerParams()
	s, mainDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	_, err := mainDB.Exec("ALTER TENANT ALL SET CLUSTER SETTING server.user_login.session_revival_token.enabled = true")
	require.NoError(t, err)

	// Start two SQL pods for the test tenant.
	const podCount = 2
	tenantID := serverutils.TestTenantID()
	tenants := startTestTenantPods(ctx, t, s, tenantID, podCount)
	defer func() {
		for _, tenant := range tenants {
			tenant.Stopper().Stop(ctx)
		}
	}()

	// Register one SQL pod in the directory server.
	tds := tenantdirsvr.NewTestStaticDirectoryServer(s.Stopper(), nil /* timeSource */)
	tds.CreateTenant(tenantID, "tenant-cluster")
	tds.AddPod(tenantID, &tenant.Pod{
		TenantID:       tenantID.ToUint64(),
		Addr:           tenants[0].SQLAddr(),
		State:          tenant.RUNNING,
		StateTimestamp: timeutil.Now(),
	})
	require.NoError(t, tds.Start(ctx))

	opts := &ProxyOptions{SkipVerify: true, DisableConnectionRebalancing: true}
	opts.testingKnobs.directoryServer = tds
	proxy, addr := newSecureProxyServer(ctx, t, s.Stopper(), opts)
	connectionString := fmt.Sprintf("postgres://testuser:hunter2@%s/?sslmode=require&options=--cluster=tenant-cluster-%s", addr, tenantID)

	// Open 12 connections to the first pod.
	dist := map[string]int{}
	var conns []*gosql.DB
	for i := 0; i < 12; i++ {
		db, err := gosql.Open("postgres", connectionString)
		db.SetMaxOpenConns(1)
		defer db.Close()
		require.NoError(t, err)
		addr := queryAddr(ctx, t, db)
		dist[addr]++
		conns = append(conns, db)
	}
	require.Len(t, dist, 1)

	// Add a second SQL pod.
	tds.AddPod(tenantID, &tenant.Pod{
		TenantID:       tenantID.ToUint64(),
		Addr:           tenants[1].SQLAddr(),
		State:          tenant.RUNNING,
		StateTimestamp: timeutil.Now(),
	})

	// Wait until the update gets propagated to the directory cache.
	testutils.SucceedsSoon(t, func() error {
		pods, err := proxy.handler.directoryCache.TryLookupTenantPods(ctx, tenantID)
		if err != nil {
			return err
		}
		if len(pods) != 2 {
			return errors.Newf("expected 2 pods, but got %d", len(pods))
		}
		return nil
	})

	// The update above should trigger the pod watcher. Regardless, we'll invoke
	// rebalancing directly as well. There should be no rebalancing attempts.
	proxy.handler.balancer.RebalanceTenant(ctx, tenantID)
	time.Sleep(2 * time.Second)

	require.Equal(t, int64(0), proxy.metrics.ConnMigrationAttemptedCount.Count())

	// Reset distribution and count again.
	dist = map[string]int{}
	for _, c := range conns {
		addr := queryAddr(ctx, t, c)
		dist[addr]++
	}
	require.Len(t, dist, 1)
}

func TestPodWatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	defer log.Scope(t).Close(t)

	// Start KV server, and enable session migration.
	params, _ := tests.CreateTestServerParams()
	s, mainDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	_, err := mainDB.Exec("ALTER TENANT ALL SET CLUSTER SETTING server.user_login.session_revival_token.enabled = true")
	require.NoError(t, err)

	// Start four SQL pods for the test tenant.
	const podCount = 4
	tenantID := serverutils.TestTenantID()
	tenants := startTestTenantPods(ctx, t, s, tenantID, podCount)
	defer func() {
		for _, tenant := range tenants {
			tenant.Stopper().Stop(ctx)
		}
	}()

	// Register only 3 SQL pods in the directory server. We will add the 4th
	// once the watcher has been established.
	tds := tenantdirsvr.NewTestStaticDirectoryServer(s.Stopper(), nil /* timeSource */)
	tds.CreateTenant(tenantID, "tenant-cluster")
	for i := 0; i < 3; i++ {
		tds.AddPod(tenantID, &tenant.Pod{
			TenantID:       tenantID.ToUint64(),
			Addr:           tenants[i].SQLAddr(),
			State:          tenant.RUNNING,
			StateTimestamp: timeutil.Now(),
		})
	}
	require.NoError(t, tds.Start(ctx))

	opts := &ProxyOptions{SkipVerify: true}
	opts.testingKnobs.directoryServer = tds
	opts.testingKnobs.balancerOpts = []balancer.Option{
		balancer.NoRebalanceLoop(),
		balancer.RebalanceRate(1.0),
	}
	proxy, addr := newSecureProxyServer(ctx, t, s.Stopper(), opts)
	connectionString := fmt.Sprintf("postgres://testuser:hunter2@%s/?sslmode=require&options=--cluster=tenant-cluster-%s", addr, tenantID)

	// Open 12 connections to it. The balancer should distribute the connections
	// evenly across 3 SQL pods (i.e. 4 connections each).
	dist := map[string]int{}
	var conns []*gosql.DB
	for i := 0; i < 12; i++ {
		db, err := gosql.Open("postgres", connectionString)
		db.SetMaxOpenConns(1)
		defer db.Close()
		require.NoError(t, err)
		addr := queryAddr(ctx, t, db)
		dist[addr]++
		conns = append(conns, db)
	}

	// Validate that connections are balanced evenly (i.e. 12/3 = 4).
	for _, d := range dist {
		require.Equal(t, 4, d)
	}

	// Register the 4th pod. This should emit an event to the pod watcher, which
	// triggers rebalancing. Based on the balancer's algorithm, balanced is
	// defined as [2, 4] connections. As a result, 2 connections will be moved
	// to the new pod. Note that for testing, we set customRebalanceRate to 1.0.
	tds.AddPod(tenantID, &tenant.Pod{
		TenantID:       tenantID.ToUint64(),
		Addr:           tenants[3].SQLAddr(),
		State:          tenant.RUNNING,
		StateTimestamp: timeutil.Now(),
	})

	// Wait until two connections have been migrated.
	testutils.SucceedsSoon(t, func() error {
		if proxy.metrics.ConnMigrationSuccessCount.Count() >= 2 {
			return nil
		}
		return errors.New("waiting for connection migration")
	})

	// Reset distribution and count again.
	dist = map[string]int{}
	for _, c := range conns {
		addr := queryAddr(ctx, t, c)
		dist[addr]++
	}

	// Validate distribution.
	var counts []int
	for _, d := range dist {
		counts = append(counts, d)
	}
	sort.Ints(counts)
	require.Equal(t, []int{2, 3, 3, 4}, counts)
}

func TestConnectionMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	s, mainDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	tenantID := serverutils.TestTenantID()

	_, err := mainDB.Exec("ALTER TENANT ALL SET CLUSTER SETTING server.user_login.session_revival_token.enabled = true")
	require.NoError(t, err)

	// Start first SQL pod.
	tenant1, tenantDB1 := serverutils.StartTenant(t, s, tests.CreateTestTenantParams(tenantID))
	tenant1.PGServer().(*pgwire.Server).TestingSetTrustClientProvidedRemoteAddr(true)
	defer tenant1.Stopper().Stop(ctx)
	defer tenantDB1.Close()

	// Start second SQL pod.
	params2 := tests.CreateTestTenantParams(tenantID)
	params2.Existing = true
	tenant2, tenantDB2 := serverutils.StartTenant(t, s, params2)
	tenant2.PGServer().(*pgwire.Server).TestingSetTrustClientProvidedRemoteAddr(true)
	defer tenant2.Stopper().Stop(ctx)
	defer tenantDB2.Close()

	_, err = tenantDB1.Exec("CREATE USER testuser WITH PASSWORD 'hunter2'")
	require.NoError(t, err)
	_, err = tenantDB1.Exec("GRANT admin TO testuser")
	require.NoError(t, err)

	// Create a proxy server without using a directory. The directory is very
	// difficult to work with, and there isn't a way to easily stub out fake
	// loads. For this test, we will stub out lookupAddr in the connector. We
	// will alternate between tenant1 and tenant2, starting with tenant1.
	opts := &ProxyOptions{SkipVerify: true, RoutingRule: tenant1.SQLAddr()}
	proxy, addr := newSecureProxyServer(ctx, t, s.Stopper(), opts)

	connectionString := fmt.Sprintf("postgres://testuser:hunter2@%s/?sslmode=require&options=--cluster=tenant-cluster-%s", addr, tenantID)

	// validateMiscMetrics ensures that our invariant of
	// attempts = success + error_recoverable + error_fatal is valid, and all
	// other transfer related metrics were incremented as well.
	validateMiscMetrics := func(t *testing.T) {
		t.Helper()
		totalAttempts := proxy.metrics.ConnMigrationSuccessCount.Count() +
			proxy.metrics.ConnMigrationErrorRecoverableCount.Count() +
			proxy.metrics.ConnMigrationErrorFatalCount.Count()
		require.Equal(t, totalAttempts, proxy.metrics.ConnMigrationAttemptedCount.Count())
		require.Equal(t, totalAttempts,
			proxy.metrics.ConnMigrationAttemptedLatency.TotalCount())
		require.Equal(t, totalAttempts,
			proxy.metrics.ConnMigrationTransferResponseMessageSize.TotalCount())
	}

	// Test that connection transfers are successful. Note that if one sub-test
	// fails, the remaining will fail as well since they all use the same
	// forwarder instance.
	t.Run("successful", func(t *testing.T) {
		tCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		db, err := gosql.Open("postgres", connectionString)
		db.SetMaxOpenConns(1)
		defer db.Close()
		require.NoError(t, err)

		// Spin up a goroutine to trigger the initial connection.
		go func() {
			_ = db.PingContext(tCtx)
		}()

		var f *forwarder
		require.Eventually(t, func() bool {
			connsMap := proxy.handler.balancer.GetTracker().GetConnsMap(tenantID)
			for _, conns := range connsMap {
				if len(conns) != 0 {
					f = conns[0].(*forwarder)
					return true
				}
			}
			return false
		}, 10*time.Second, 100*time.Millisecond)

		// Set up forwarder hooks.
		prevTenant1 := true
		var lookupAddrDelayDuration time.Duration
		f.connector.testingKnobs.lookupAddr = func(ctx context.Context) (string, error) {
			if lookupAddrDelayDuration != 0 {
				select {
				case <-ctx.Done():
					return "", errors.Wrap(ctx.Err(), "injected delays")
				case <-time.After(lookupAddrDelayDuration):
				}
			}
			if prevTenant1 {
				prevTenant1 = false
				return tenant2.SQLAddr(), nil
			}
			prevTenant1 = true
			return tenant1.SQLAddr(), nil
		}

		t.Run("normal_transfer", func(t *testing.T) {
			require.Equal(t, tenant1.SQLAddr(), queryAddr(tCtx, t, db))

			_, err = db.Exec("SET application_name = 'foo'")
			require.NoError(t, err)

			// Show that we get alternating SQL pods when we transfer.
			require.NoError(t, f.TransferConnection())
			require.Equal(t, int64(1), f.metrics.ConnMigrationSuccessCount.Count())
			require.Equal(t, tenant2.SQLAddr(), queryAddr(tCtx, t, db))

			var name string
			require.NoError(t, db.QueryRow("SHOW application_name").Scan(&name))
			require.Equal(t, "foo", name)

			_, err = db.Exec("SET application_name = 'bar'")
			require.NoError(t, err)

			require.NoError(t, f.TransferConnection())
			require.Equal(t, int64(2), f.metrics.ConnMigrationSuccessCount.Count())
			require.Equal(t, tenant1.SQLAddr(), queryAddr(tCtx, t, db))

			require.NoError(t, db.QueryRow("SHOW application_name").Scan(&name))
			require.Equal(t, "bar", name)

			// Now attempt a transfer concurrently with requests.
			initSuccessCount := f.metrics.ConnMigrationSuccessCount.Count()
			subCtx, cancel := context.WithCancel(tCtx)
			defer cancel()

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for subCtx.Err() == nil {
					_ = f.TransferConnection()
					time.Sleep(100 * time.Millisecond)
				}
			}()

			// This loop will run approximately 5 seconds.
			var tenant1Addr, tenant2Addr int
			for i := 0; i < 100; i++ {
				addr := queryAddr(tCtx, t, db)
				if addr == tenant1.SQLAddr() {
					tenant1Addr++
				} else {
					require.Equal(t, tenant2.SQLAddr(), addr)
					tenant2Addr++
				}
				time.Sleep(50 * time.Millisecond)
			}

			// Ensure that the goroutine terminates so other subtests are not
			// affected.
			cancel()
			wg.Wait()

			// Ensure that some transfers were performed, and the forwarder isn't
			// closed.
			require.True(t, tenant1Addr >= 2)
			require.True(t, tenant2Addr >= 2)
			require.Nil(t, f.ctx.Err())

			// Check metrics.
			require.True(t, f.metrics.ConnMigrationSuccessCount.Count() > initSuccessCount+4)
			require.Equal(t, int64(0), f.metrics.ConnMigrationErrorRecoverableCount.Count())
			require.Equal(t, int64(0), f.metrics.ConnMigrationErrorFatalCount.Count())

			validateMiscMetrics(t)
		})

		// Transfers should fail if there is an open transaction. These failed
		// transfers should not close the connection.
		t.Run("failed_transfers_with_tx", func(t *testing.T) {
			initSuccessCount := f.metrics.ConnMigrationSuccessCount.Count()
			initAddr := queryAddr(tCtx, t, db)

			err = crdb.ExecuteTx(tCtx, db, nil /* txopts */, func(tx *gosql.Tx) error {
				// Run multiple times to ensure that connection isn't closed.
				for i := 0; i < 5; i++ {
					err := f.TransferConnection()
					if err == nil {
						return errors.New("no error")
					}
					if !assert.Regexp(t, "cannot serialize", err.Error()) {
						return errors.Wrap(err, "non-serialization error")
					}
					addr := queryAddr(tCtx, t, tx)
					if initAddr != addr {
						return errors.Newf(
							"address does not match, expected %s, found %s",
							initAddr,
							addr,
						)
					}
				}
				return nil
			})
			require.NoError(t, err)

			// None of the migrations should succeed, and the forwarder should
			// still be active.
			require.Nil(t, f.ctx.Err())
			require.Equal(t, initSuccessCount, f.metrics.ConnMigrationSuccessCount.Count())
			require.Equal(t, int64(5), f.metrics.ConnMigrationErrorRecoverableCount.Count())
			require.Equal(t, int64(0), f.metrics.ConnMigrationErrorFatalCount.Count())

			// Once the transaction is closed, transfers should work.
			require.NoError(t, f.TransferConnection())
			require.NotEqual(t, initAddr, queryAddr(tCtx, t, db))
			require.Nil(t, f.ctx.Err())
			require.Equal(t, initSuccessCount+1, f.metrics.ConnMigrationSuccessCount.Count())
			require.Equal(t, int64(5), f.metrics.ConnMigrationErrorRecoverableCount.Count())
			require.Equal(t, int64(0), f.metrics.ConnMigrationErrorFatalCount.Count())

			validateMiscMetrics(t)
		})

		// Transfer timeout caused by dial issues should not close the session.
		// We will test this by introducing delays when connecting to the SQL
		// pod.
		t.Run("failed_transfers_with_dial_issues", func(t *testing.T) {
			initSuccessCount := f.metrics.ConnMigrationSuccessCount.Count()
			initErrorRecoverableCount := f.metrics.ConnMigrationErrorRecoverableCount.Count()
			initAddr := queryAddr(tCtx, t, db)

			// Set the delay longer than the timeout.
			lookupAddrDelayDuration = 10 * time.Second
			defer testutils.TestingHook(&defaultTransferTimeout, 3*time.Second)()

			err := f.TransferConnection()
			require.Error(t, err)
			require.Regexp(t, "injected delays", err.Error())
			require.Equal(t, initAddr, queryAddr(tCtx, t, db))
			require.Nil(t, f.ctx.Err())

			require.Equal(t, initSuccessCount, f.metrics.ConnMigrationSuccessCount.Count())
			require.Equal(t, initErrorRecoverableCount+1,
				f.metrics.ConnMigrationErrorRecoverableCount.Count())
			require.Equal(t, int64(0), f.metrics.ConnMigrationErrorFatalCount.Count())

			validateMiscMetrics(t)
		})
	})

	// Test transfer timeouts caused by waiting for a transfer state response.
	// In reality, this can only be caused by pipelined queries. Consider the
	// folllowing:
	//   1. short-running simple query
	//   2. long-running simple query
	//   3. SHOW TRANSFER STATE
	// When (1) returns a response, the forwarder will see that we're in a
	// safe transfer point, and initiate (3). But (2) may block until we hit
	// a timeout.
	//
	// There's no easy way to simulate pipelined queries. pgtest (that allows
	// us to send individual pgwire messages) does not support authentication,
	// which is what the proxy needs, so we will stub isSafeTransferPointLocked
	// instead.
	t.Run("transfer_timeout_in_response", func(t *testing.T) {
		tCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		db, err := gosql.Open("postgres", connectionString)
		db.SetMaxOpenConns(1)
		defer db.Close()
		require.NoError(t, err)

		// Use a single connection so that we don't reopen when the connection
		// is closed.
		conn, err := db.Conn(tCtx)
		require.NoError(t, err)

		// Trigger the initial connection.
		require.NoError(t, conn.PingContext(tCtx))

		var f *forwarder
		require.Eventually(t, func() bool {
			connsMap := proxy.handler.balancer.GetTracker().GetConnsMap(tenantID)
			for _, conns := range connsMap {
				if len(conns) != 0 {
					f = conns[0].(*forwarder)
					return true
				}
			}
			return false
		}, 10*time.Second, 100*time.Millisecond)

		initSuccessCount := f.metrics.ConnMigrationSuccessCount.Count()
		initErrorRecoverableCount := f.metrics.ConnMigrationErrorRecoverableCount.Count()

		// Set up forwarder hooks.
		prevTenant1 := true
		f.connector.testingKnobs.lookupAddr = func(ctx context.Context) (string, error) {
			if prevTenant1 {
				prevTenant1 = false
				return tenant2.SQLAddr(), nil
			}
			prevTenant1 = true
			return tenant1.SQLAddr(), nil
		}
		defer testutils.TestingHook(&isSafeTransferPointLocked, func(req *processor, res *processor) bool {
			return true
		})()
		// Transfer timeout is 3s, and we'll run pg_sleep for 10s.
		defer testutils.TestingHook(&defaultTransferTimeout, 3*time.Second)()

		goCh := make(chan struct{}, 1)
		errCh := make(chan error, 1)
		go func() {
			goCh <- struct{}{}
			_, err := conn.ExecContext(tCtx, "SELECT pg_sleep(10)")
			errCh <- err
		}()

		// Block until goroutine is started. We want to make sure we run the
		// transfer request *after* sending the query. This doesn't guarantee,
		// but is the best that we can do. We also added a sleep call here.
		//
		// Alternatively, we could open another connection, and query the server
		// to make sure pg_sleep is running, but that seems unnecessary for just
		// one test.
		<-goCh
		time.Sleep(2 * time.Second)
		// This should be an error because the transfer timed out. Connection
		// should automatically be closed.
		require.Error(t, f.TransferConnection())

		select {
		case <-time.After(10 * time.Second):
			t.Fatalf("require that pg_sleep query terminates")
		case err = <-errCh:
			require.Error(t, err)
			require.Regexp(t, "(closed|bad connection)", err.Error())
		}

		require.EqualError(t, f.ctx.Err(), context.Canceled.Error())
		require.Equal(t, initSuccessCount, f.metrics.ConnMigrationSuccessCount.Count())
		require.Equal(t, initErrorRecoverableCount, f.metrics.ConnMigrationErrorRecoverableCount.Count())
		require.Equal(t, int64(1), f.metrics.ConnMigrationErrorFatalCount.Count())

		totalAttempts := f.metrics.ConnMigrationSuccessCount.Count() +
			f.metrics.ConnMigrationErrorRecoverableCount.Count() +
			f.metrics.ConnMigrationErrorFatalCount.Count()
		require.Equal(t, totalAttempts, f.metrics.ConnMigrationAttemptedCount.Count())
		require.Equal(t, totalAttempts,
			f.metrics.ConnMigrationAttemptedLatency.TotalCount())
		// Here, we get a transfer timeout in response, so the message size
		// should not be recorded.
		require.Equal(t, totalAttempts-1,
			f.metrics.ConnMigrationTransferResponseMessageSize.TotalCount())
	})

	// All connections should eventually be terminated.
	require.Eventually(t, func() bool {
		connsMap := proxy.handler.balancer.GetTracker().GetConnsMap(tenantID)
		return len(connsMap) == 0
	}, 10*time.Second, 100*time.Millisecond)
}

// TestCurConnCountMetric ensures that the CurConnCount metric is accurate.
// Previously, there was a regression where the CurConnCount metric wasn't
// decremented whenever the connections were closed due to a goroutine leak.
func TestCurConnCountMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Start KV server.
	params, _ := tests.CreateTestServerParams()
	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Start a single SQL pod.
	tenantID := serverutils.TestTenantID()
	tenants := startTestTenantPods(ctx, t, s, tenantID, 1)
	defer func() {
		for _, tenant := range tenants {
			tenant.Stopper().Stop(ctx)
		}
	}()

	// Register the SQL pod in the directory server.
	tds := tenantdirsvr.NewTestStaticDirectoryServer(s.Stopper(), nil /* timeSource */)
	tds.CreateTenant(tenantID, "tenant-cluster")
	tds.AddPod(tenantID, &tenant.Pod{
		TenantID:       tenantID.ToUint64(),
		Addr:           tenants[0].SQLAddr(),
		State:          tenant.RUNNING,
		StateTimestamp: timeutil.Now(),
	})
	require.NoError(t, tds.Start(ctx))

	opts := &ProxyOptions{SkipVerify: true, DisableConnectionRebalancing: true}
	opts.testingKnobs.directoryServer = tds
	proxy, addr := newSecureProxyServer(ctx, t, s.Stopper(), opts)
	connectionString := fmt.Sprintf("postgres://testuser:hunter2@%s/?sslmode=require&options=--cluster=tenant-cluster-%s", addr, tenantID)

	// Open 500 connections to the SQL pod.
	const numConns = 500
	var wg sync.WaitGroup
	wg.Add(numConns)
	for i := 0; i < numConns; i++ {
		go func() {
			defer wg.Done()

			// Opens a new connection, runs SELECT 1, and closes it right away.
			// Ignore all connection errors.
			conn, err := pgx.Connect(ctx, connectionString)
			if err != nil {
				return
			}
			_ = conn.Ping(ctx)
			_ = conn.Close(ctx)
		}()
	}
	wg.Wait()

	// Ensure that the CurConnCount metric gets decremented to 0 whenever all
	// the connections are closed.
	testutils.SucceedsSoon(t, func() error {
		val := proxy.metrics.CurConnCount.Value()
		if val == 0 {
			return nil
		}
		return errors.Newf("expected CurConnCount=0, but got %d", val)
	})
}

func TestClusterNameAndTenantFromParams(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	testCases := []struct {
		name                string
		params              map[string]string
		expectedClusterName string
		expectedTenantID    uint64
		expectedParams      map[string]string
		expectedError       string
		expectedHint        string
	}{
		{
			name:          "empty params",
			params:        map[string]string{},
			expectedError: "missing cluster identifier",
			expectedHint:  clusterIdentifierHint,
		},
		{
			name: "cluster identifier is not provided",
			params: map[string]string{
				"database": "defaultdb",
				"options":  "--foo=bar",
			},
			expectedError: "missing cluster identifier",
			expectedHint:  clusterIdentifierHint,
		},
		{
			name: "multiple different cluster identifiers",
			params: map[string]string{
				"database": "happy-koala-7.defaultdb",
				"options":  "--cluster=happy-tiger",
			},
			expectedError: "multiple different cluster identifiers provided",
			expectedHint: "Is 'happy-koala-7' or 'happy-tiger' the identifier for the cluster that you're connecting to?\n--\n" +
				clusterIdentifierHint,
		},
		{
			name: "invalid cluster identifier in database param",
			params: map[string]string{
				// Cluster names need to be between 6 to 100 alphanumeric characters.
				"database": "short-0.defaultdb",
			},
			expectedError: "invalid cluster identifier 'short-0'",
			expectedHint:  "Is 'short' a valid cluster name?\n--\n" + clusterNameFormHint,
		},
		{
			name: "invalid cluster identifier in options param",
			params: map[string]string{
				// Cluster names need to be between 6 to 100 alphanumeric characters.
				"options": fmt.Sprintf("--cluster=%s-0", strings.Repeat("a", 101)),
			},
			expectedError: fmt.Sprintf(
				"invalid cluster identifier '%s-0'",
				strings.Repeat("a", 101),
			),
			expectedHint: fmt.Sprintf(
				"Is '%s' a valid cluster name?\n--\n%s",
				strings.Repeat("a", 101),
				clusterNameFormHint,
			),
		},
		{
			name:          "invalid database param (1)",
			params:        map[string]string{"database": "."},
			expectedError: "invalid database param",
		},
		{
			name:          "invalid database param (2)",
			params:        map[string]string{"database": "a."},
			expectedError: "invalid database param",
		},
		{
			name:          "invalid database param (3)",
			params:        map[string]string{"database": ".b"},
			expectedError: "invalid database param",
		},
		{
			name:          "invalid database param (4)",
			params:        map[string]string{"database": "a.b.c"},
			expectedError: "invalid database param",
		},
		{
			name: "multiple cluster flags",
			params: map[string]string{
				"database": "hello-world.defaultdb",
				"options":  "--cluster=foobar --cluster=barbaz --cluster=testbaz",
			},
			expectedError: "multiple cluster flags provided",
		},
		{
			name: "invalid cluster flag",
			params: map[string]string{
				"database": "hello-world.defaultdb",
				"options":  "--cluster=",
			},
			expectedError: "invalid cluster flag",
		},
		{
			name:          "no tenant id",
			params:        map[string]string{"database": "happy2koala.defaultdb"},
			expectedError: "invalid cluster identifier 'happy2koala'",
			expectedHint:  missingTenantIDHint + "\n--\n" + clusterNameFormHint,
		},
		{
			name:          "missing tenant id",
			params:        map[string]string{"database": "happy2koala-.defaultdb"},
			expectedError: "invalid cluster identifier 'happy2koala-'",
			expectedHint:  missingTenantIDHint + "\n--\n" + clusterNameFormHint,
		},
		{
			name:          "missing cluster name",
			params:        map[string]string{"database": "-7.defaultdb"},
			expectedError: "invalid cluster identifier '-7'",
			expectedHint:  "Is '' a valid cluster name?\n--\n" + clusterNameFormHint,
		},
		{
			name:          "bad tenant id",
			params:        map[string]string{"database": "happy-koala-0-7a.defaultdb"},
			expectedError: "invalid cluster identifier 'happy-koala-0-7a'",
			expectedHint:  "Is '7a' a valid tenant ID?\n--\n" + clusterNameFormHint,
		},
		{
			name:          "zero tenant id",
			params:        map[string]string{"database": "happy-koala-0.defaultdb"},
			expectedError: "invalid cluster identifier 'happy-koala-0'",
			expectedHint:  "Tenant ID 0 is invalid.",
		},
		{
			name: "multiple similar cluster identifiers",
			params: map[string]string{
				"database": "happy-koala-7.defaultdb",
				"options":  "--cluster=happy-koala-7",
			},
			expectedClusterName: "happy-koala",
			expectedTenantID:    7,
			expectedParams:      map[string]string{"database": "defaultdb"},
		},
		{
			name: "cluster identifier in database param",
			params: map[string]string{
				"database": fmt.Sprintf("%s-7.defaultdb", strings.Repeat("a", 100)),
				"foo":      "bar",
			},
			expectedClusterName: strings.Repeat("a", 100),
			expectedTenantID:    7,
			expectedParams:      map[string]string{"database": "defaultdb", "foo": "bar"},
		},
		{
			name: "valid cluster identifier with invalid arrangements",
			params: map[string]string{
				"database": "defaultdb",
				"options":  "-c --cluster=happy-koala-7 -c -c -c",
			},
			expectedClusterName: "happy-koala",
			expectedTenantID:    7,
			expectedParams: map[string]string{
				"database": "defaultdb",
				"options":  "-c  -c -c -c",
			},
		},
		{
			name: "short option: cluster identifier in options param",
			params: map[string]string{
				"database": "defaultdb",
				"options":  "-ccluster=happy-koala-7",
			},
			expectedClusterName: "happy-koala",
			expectedTenantID:    7,
			expectedParams:      map[string]string{"database": "defaultdb"},
		},
		{
			name: "short option with spaces: cluster identifier in options param",
			params: map[string]string{
				"database": "defaultdb",
				"options":  "-c   cluster=happy-koala-7",
			},
			expectedClusterName: "happy-koala",
			expectedTenantID:    7,
			expectedParams:      map[string]string{"database": "defaultdb"},
		},
		{
			name: "long option: cluster identifier in options param",
			params: map[string]string{
				"database": "defaultdb",
				"options":  "--cluster=happy-koala-7\t--foo=test",
			},
			expectedClusterName: "happy-koala",
			expectedTenantID:    7,
			expectedParams: map[string]string{
				"database": "defaultdb",
				"options":  "--foo=test",
			},
		},
		{
			name: "long option: cluster identifier in options param with other options",
			params: map[string]string{
				"database": "defaultdb",
				"options":  "-csearch_path=public --cluster=happy-koala-7\t--foo=test",
			},
			expectedClusterName: "happy-koala",
			expectedTenantID:    7,
			expectedParams: map[string]string{
				"database": "defaultdb",
				"options":  "-csearch_path=public \t--foo=test",
			},
		},
		{
			name:                "leading 0s are ok",
			params:              map[string]string{"database": "happy-koala-0-07.defaultdb"},
			expectedClusterName: "happy-koala-0",
			expectedTenantID:    7,
			expectedParams:      map[string]string{"database": "defaultdb"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			msg := &pgproto3.StartupMessage{Parameters: tc.params}

			originalParams := make(map[string]string)
			for k, v := range msg.Parameters {
				originalParams[k] = v
			}

			fe := &FrontendAdmitInfo{Msg: msg}
			outMsg, clusterName, tenantID, err := clusterNameAndTenantFromParams(ctx, fe)
			if tc.expectedError == "" {
				require.NoErrorf(t, err, "failed test case\n%+v", tc)

				// When expectedError is specified, we always have a valid expectedTenantID.
				require.Equal(t, roachpb.MakeTenantID(tc.expectedTenantID), tenantID)

				require.Equal(t, tc.expectedClusterName, clusterName)
				require.Equal(t, tc.expectedParams, outMsg.Parameters)
			} else {
				require.EqualErrorf(t, err, tc.expectedError, "failed test case\n%+v", tc)

				pgerr := pgerror.Flatten(err)
				require.Equal(t, tc.expectedHint, pgerr.Hint)
			}

			// Check that the original parameters were not modified.
			require.Equal(t, originalParams, msg.Parameters)
		})
	}
}

type tester struct {
	// mu synchronizes the authenticated and errToClient fields, since they
	// need to be set on background goroutines, and will cause race builds to
	// fail if not synchronized.
	mu struct {
		syncutil.Mutex
		authenticated bool
		errToClient   *codeError
	}

	restoreAuthenticate    func()
	restoreSendErrToClient func()
}

func newTester() *tester {
	te := &tester{}

	// Record successful connection and authentication.
	originalAuthenticate := authenticate
	te.restoreAuthenticate =
		testutils.TestingHook(&authenticate, func(clientConn, crdbConn net.Conn, throttleHook func(status throttler.AttemptStatus) error) error {
			err := originalAuthenticate(clientConn, crdbConn, throttleHook)
			te.setAuthenticated(err == nil)
			return err
		})

	// Capture any error sent to the client.
	originalSendErrToClient := SendErrToClient
	te.restoreSendErrToClient =
		testutils.TestingHook(&SendErrToClient, func(conn net.Conn, err error) {
			if codeErr := (*codeError)(nil); errors.As(err, &codeErr) {
				te.setErrToClient(codeErr)
			}
			originalSendErrToClient(conn, err)
		})

	return te
}

func (te *tester) Close() {
	te.restoreAuthenticate()
	te.restoreSendErrToClient()
}

// Authenticated returns true if the connection was successfully established and
// authenticated.
func (te *tester) Authenticated() bool {
	te.mu.Lock()
	defer te.mu.Unlock()
	return te.mu.authenticated
}

func (te *tester) setAuthenticated(auth bool) {
	te.mu.Lock()
	defer te.mu.Unlock()
	te.mu.authenticated = auth
}

// ErrToClient returns any error sent by the proxy to the client.
func (te *tester) ErrToClient() *codeError {
	te.mu.Lock()
	defer te.mu.Unlock()
	return te.mu.errToClient
}

func (te *tester) setErrToClient(codeErr *codeError) {
	te.mu.Lock()
	defer te.mu.Unlock()
	te.mu.errToClient = codeErr
}

// TestConnect connects to the given URL and invokes the given callback with the
// established connection. Use TestConnectErr if connection establishment isn't
// expected to succeed.
func (te *tester) TestConnect(ctx context.Context, t *testing.T, url string, fn func(*pgx.Conn)) {
	t.Helper()
	te.setAuthenticated(false)
	te.setErrToClient(nil)
	connConfig, err := pgx.ParseConfig(url)
	require.NoError(t, err)
	if !strings.EqualFold(connConfig.Host, "127.0.0.1") {
		connConfig.TLSConfig.ServerName = connConfig.Host
		connConfig.Host = "127.0.0.1"
	}
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	require.NoError(t, err)
	fn(conn)
	require.NoError(t, conn.Close(ctx))
	require.True(t, te.Authenticated())
	require.Nil(t, te.ErrToClient())
}

// TestConnectErr tries to establish a connection to the given URL. It expects
// an error to occur and validates the error matches the provided information.
func (te *tester) TestConnectErr(
	ctx context.Context, t *testing.T, url string, expCode errorCode, expErr string,
) {
	t.Helper()
	te.setAuthenticated(false)
	te.setErrToClient(nil)
	cfg, err := pgx.ParseConfig(url)
	require.NoError(t, err)

	// Prevent pgx from tying to connect to the `::1` ipv6 address for localhost.
	cfg.LookupFunc = func(ctx context.Context, s string) ([]string, error) { return []string{s}, nil }
	if !strings.EqualFold(cfg.Host, "127.0.0.1") && cfg.TLSConfig != nil {
		cfg.TLSConfig.ServerName = cfg.Host
		cfg.Host = "127.0.0.1"
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err == nil {
		_ = conn.Close(ctx)
	}
	require.Regexp(t, expErr, err.Error())
	require.False(t, te.Authenticated())
	if expCode != 0 {
		require.NotNil(t, te.ErrToClient())
		require.Equal(t, expCode, te.ErrToClient().code)
	}
}

func newSecureProxyServer(
	ctx context.Context, t *testing.T, stopper *stop.Stopper, opts *ProxyOptions,
) (server *Server, addr string) {
	// Created via:
	const _ = `
openssl genrsa -out testdata/testserver.key 2048
openssl req -new -x509 -sha256 -key testdata/testserver.key -out testdata/testserver.crt \
  -days 3650 -config testdata/testserver_config.cnf
`
	opts.ListenKey = testutils.TestDataPath(t, "testserver.key")
	opts.ListenCert = testutils.TestDataPath(t, "testserver.crt")

	return newProxyServer(ctx, t, stopper, opts)
}

func newProxyServer(
	ctx context.Context, t *testing.T, stopper *stop.Stopper, opts *ProxyOptions,
) (server *Server, addr string) {
	const listenAddress = "127.0.0.1:0"
	ln, err := net.Listen("tcp", listenAddress)
	require.NoError(t, err)

	server, err = NewServer(ctx, stopper, *opts)
	require.NoError(t, err)

	err = server.Stopper.RunAsyncTask(ctx, "proxy-server-serve", func(ctx context.Context) {
		_ = server.Serve(ctx, ln)
	})
	require.NoError(t, err)

	return server, ln.Addr().String()
}

func runTestQuery(ctx context.Context, conn *pgx.Conn) error {
	var n int
	if err := conn.QueryRow(ctx, "SELECT $1::int", 1).Scan(&n); err != nil {
		return err
	}
	if n != 1 {
		return errors.Errorf("expected 1 got %d", n)
	}
	return nil
}

func newDirectoryServer(
	ctx context.Context, t *testing.T, srv serverutils.TestServerInterface, addr *net.TCPAddr,
) (*tenantdirsvr.TestDirectoryServer, *net.TCPAddr) {
	// Start listening on port that the tenant directory server will use.
	var listener *net.TCPListener
	require.Eventually(t, func() bool {
		var err error
		listener, err = net.ListenTCP("tcp", addr)
		return err == nil
	}, 30*time.Second, time.Second)

	// Create the tenant directory server.
	tdsStopper := stop.NewStopper()
	tds, err := tenantdirsvr.New(tdsStopper)
	require.NoError(t, err)

	// Override the tenant starter function to start a new tenant process using
	// the TestServerInterface.
	tds.TenantStarterFunc = func(ctx context.Context, tenantID uint64) (*tenantdirsvr.Process, error) {
		// Recognize special tenant ID that triggers an error.
		if tenantID == notFoundTenantID {
			return nil, status.Error(codes.NotFound, "tenant not found")
		}

		tenantStopper := tenantdirsvr.NewSubStopper(tdsStopper)
		ten, err := srv.StartTenant(ctx, base.TestTenantArgs{
			Existing:      true,
			TenantID:      roachpb.MakeTenantID(tenantID),
			ForceInsecure: true,
			Stopper:       tenantStopper,
		})
		require.NoError(t, err)
		sqlAddr, err := net.ResolveTCPAddr("tcp", ten.SQLAddr())
		require.NoError(t, err)
		ten.PGServer().(*pgwire.Server).TestingSetTrustClientProvidedRemoteAddr(true)
		return &tenantdirsvr.Process{SQL: sqlAddr, Stopper: tenantStopper}, nil
	}

	// Start serving on a background goroutine.
	go func() { require.NoError(t, tds.Serve(listener)) }()

	return tds, listener.Addr().(*net.TCPAddr)
}

// mustGetTestSimpleDirectoryServer returns the underlying simple directory
// server. This can only be used with a routing rule.
func mustGetTestSimpleDirectoryServer(
	t *testing.T, handler *proxyHandler,
) *tenantdirsvr.TestSimpleDirectoryServer {
	t.Helper()
	require.NotNil(t, handler.testingKnobs.directoryServer, "routing rule was not used")
	svr, ok := handler.testingKnobs.directoryServer.(*tenantdirsvr.TestSimpleDirectoryServer)
	require.True(t, ok)
	return svr
}

type queryer interface {
	QueryRowContext(context.Context, string, ...interface{}) *gosql.Row
}

// queryAddr queries the SQL node that `db` is connected to for its address.
func queryAddr(ctx context.Context, t *testing.T, db queryer) string {
	t.Helper()
	var host, port string
	require.NoError(t, db.QueryRowContext(ctx, `
			SELECT
				a.value AS "host", b.value AS "port"
			FROM crdb_internal.node_runtime_info a, crdb_internal.node_runtime_info b
			WHERE a.component = 'DB' AND a.field = 'Host'
				AND b.component = 'DB' AND b.field = 'Port'
		`).Scan(&host, &port))
	return fmt.Sprintf("%s:%s", host, port)
}

// startTestTenantPods starts count SQL pods for the given tenant, and returns
// a list of tenant servers. Note that a default admin testuser with the
// password hunter2 will be created.
func startTestTenantPods(
	ctx context.Context,
	t *testing.T,
	ts serverutils.TestServerInterface,
	tenantID roachpb.TenantID,
	count int,
) []serverutils.TestTenantInterface {
	t.Helper()

	var tenants []serverutils.TestTenantInterface
	for i := 0; i < count; i++ {
		params := tests.CreateTestTenantParams(tenantID)
		// The first SQL pod will create the tenant keyspace in the host.
		if i != 0 {
			params.Existing = true
		}
		tenant, tenantDB := serverutils.StartTenant(t, ts, params)
		tenant.PGServer().(*pgwire.Server).TestingSetTrustClientProvidedRemoteAddr(true)

		// Create a test user. We only need to do it once.
		if i == 0 {
			_, err := tenantDB.Exec("CREATE USER testuser WITH PASSWORD 'hunter2'")
			require.NoError(t, err)
			_, err = tenantDB.Exec("GRANT admin TO testuser")
			require.NoError(t, err)
		}
		tenantDB.Close()

		tenants = append(tenants, tenant)
	}
	return tenants
}
