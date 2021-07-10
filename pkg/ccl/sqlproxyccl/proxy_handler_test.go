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
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/denylist"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenantdirsvr"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
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

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	defer testutils.TestingHook(&BackendDial, func(
		_ *pgproto3.StartupMessage, outgoingAddr string, _ *tls.Config,
	) (net.Conn, error) {
		require.Equal(t, outgoingAddr, "dim-dog-28-0.cockroachdb:26257")
		return nil, newErrorf(codeParamsRoutingFailed, "boom")
	})()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	s, addr := newSecureProxyServer(ctx, t, stopper, &ProxyOptions{RoutingRule: "{{clusterName}}-0.cockroachdb:26257"})

	longDB := strings.Repeat("x", 70) // 63 is limit
	pgurl := fmt.Sprintf("postgres://unused:unused@%s/%s?options=--cluster=dim-dog-28", addr, longDB)
	te.TestConnectErr(ctx, t, pgurl, codeParamsRoutingFailed, "boom")
	require.Equal(t, int64(1), s.metrics.RoutingErrCount.Count())
}

// TestBackendDownRetry tries to connect to a unavailable backend. After 3
// failed attempts, a "tenant not found" error simulates the tenant being
// deleted.
func TestBackendDownRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	callCount := 0
	defer testutils.TestingHook(&resolveTCPAddr,
		func(network, addr string) (*net.TCPAddr, error) {
			callCount++
			if callCount >= 3 {
				return nil, errors.New("tenant not found")
			}
			return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 26257}, nil
		})()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	_, addr := newSecureProxyServer(ctx, t, stopper, &ProxyOptions{RoutingRule: "undialable%$!@$"})

	// Valid connection, but no backend server running.
	pgurl := fmt.Sprintf("postgres://unused:unused@%s/db?options=--cluster=dim-dog-28", addr)
	te.TestConnectErr(ctx, t, pgurl, codeParamsRoutingFailed, "cluster dim-dog-28 not found")
	require.Equal(t, 3, callCount)
}

func TestFailedConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	s, addr := newSecureProxyServer(ctx, t, stopper, &ProxyOptions{RoutingRule: "undialable%$!@$"})

	// TODO(asubiotto): consider using datadriven for these, especially if the
	// proxy becomes more complex.

	_, p, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	u := fmt.Sprintf("postgres://unused:unused@localhost:%s/", p)

	// Unencrypted connections bounce.
	for _, sslmode := range []string{"disable", "allow"} {
		te.TestConnectErr(
			ctx, t, u+"?options=--cluster=dim-dog-28&sslmode="+sslmode,
			codeUnexpectedInsecureStartupMessage, "server requires encryption",
		)
	}
	require.Equal(t, int64(0), s.metrics.RoutingErrCount.Count())

	// TenantID rejected as malformed.
	te.TestConnectErr(
		ctx, t, u+"?options=--cluster=dimdog&sslmode=require",
		codeParamsRoutingFailed, "invalid cluster name 'dimdog'",
	)
	require.Equal(t, int64(1), s.metrics.RoutingErrCount.Count())

	// No cluster name and TenantID.
	te.TestConnectErr(
		ctx, t, u+"?sslmode=require",
		codeParamsRoutingFailed, "missing cluster name in connection string",
	)
	require.Equal(t, int64(2), s.metrics.RoutingErrCount.Count())

	// Bad TenantID. Ensure that we don't leak any parsing errors.
	te.TestConnectErr(
		ctx, t, u+"?options=--cluster=dim-dog-foo3&sslmode=require",
		codeParamsRoutingFailed, "invalid cluster name 'dim-dog-foo3'",
	)
	require.Equal(t, int64(3), s.metrics.RoutingErrCount.Count())
}

func TestUnexpectedError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	// Set up a Server whose FrontendAdmitter function always errors with a
	// non-codeError error.
	defer testutils.TestingHook(&FrontendAdmit, func(
		conn net.Conn, incomingTLSConfig *tls.Config,
	) (net.Conn, *pgproto3.StartupMessage, error) {
		log.Infof(context.Background(), "frontend admitter returning unexpected error")
		return conn, nil, errors.New("unexpected error")
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

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	sql, db, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	sql.(*server.TestServer).PGServer().TestingSetTrustClientProvidedRemoteAddr(true)
	defer sql.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE USER bob WITH PASSWORD 'builder'`)

	s, addr := newSecureProxyServer(
		ctx, t, sql.Stopper(), &ProxyOptions{RoutingRule: sql.ServingSQLAddr(), SkipVerify: true},
	)

	url := fmt.Sprintf("postgres://bob:wrong@%s/dim-dog-28.defaultdb?sslmode=require", addr)
	te.TestConnectErr(ctx, t, url, 0, "ERROR: password authentication failed for user bob")

	url = fmt.Sprintf("postgres://bob@%s/dim-dog-28.defaultdb?sslmode=require", addr)
	te.TestConnectErr(ctx, t, url, 0, "ERROR: password authentication failed for user bob")

	url = fmt.Sprintf("postgres://bob:builder@%s/dim-dog-28.defaultdb?sslmode=require", addr)
	te.TestConnect(ctx, t, url, func(conn *pgx.Conn) {
		require.Equal(t, int64(1), s.metrics.CurConnCount.Value())
		require.NoError(t, runTestQuery(ctx, conn))
	})
	require.Equal(t, int64(1), s.metrics.SuccessfulConnCount.Count())
	require.Equal(t, int64(2), s.metrics.AuthFailedCount.Count())
}

func TestProxyTLSClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// NB: The leaktest call is an important part of this test. We're
	// verifying that no goroutines are leaked, despite calling Close an
	// underlying TCP connection (rather than the TLSConn that wraps it).

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	sql, db, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	sql.(*server.TestServer).PGServer().TestingSetTrustClientProvidedRemoteAddr(true)
	defer sql.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE USER bob WITH PASSWORD 'builder'`)

	var proxyIncomingConn atomic.Value // *conn
	originalFrontendAdmit := FrontendAdmit
	defer testutils.TestingHook(&FrontendAdmit, func(
		conn net.Conn, incomingTLSConfig *tls.Config,
	) (net.Conn, *pgproto3.StartupMessage, error) {
		proxyIncomingConn.Store(conn)
		return originalFrontendAdmit(conn, incomingTLSConfig)
	})()

	s, addr := newSecureProxyServer(
		ctx, t, sql.Stopper(), &ProxyOptions{RoutingRule: sql.ServingSQLAddr(), SkipVerify: true},
	)

	url := fmt.Sprintf("postgres://bob:builder@%s/dim-dog-28.defaultdb?sslmode=require", addr)

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

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	sql, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	sql.(*server.TestServer).PGServer().TestingSetTrustClientProvidedRemoteAddr(true)
	defer sql.Stopper().Stop(ctx)

	outgoingTLSConfig, err := sql.RPCContext().GetClientTLSConfig()
	require.NoError(t, err)
	proxyOutgoingTLSConfig := outgoingTLSConfig.Clone()
	proxyOutgoingTLSConfig.InsecureSkipVerify = true

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
		params["user"] = "root"

		return originalBackendDial(msg, sql.ServingSQLAddr(), proxyOutgoingTLSConfig)
	})()

	s, proxyAddr := newSecureProxyServer(ctx, t, sql.Stopper(), &ProxyOptions{})

	u := fmt.Sprintf("postgres://bogususer@%s/?sslmode=require&authToken=abc123&options=--cluster=dim-dog-28", proxyAddr)
	te.TestConnect(ctx, t, u, func(conn *pgx.Conn) {
		require.Equal(t, int64(1), s.metrics.CurConnCount.Value())
		require.NoError(t, runTestQuery(ctx, conn))
	})
}

func TestInsecureProxy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	sql, db, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	sql.(*server.TestServer).PGServer().TestingSetTrustClientProvidedRemoteAddr(true)
	defer sql.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE USER bob WITH PASSWORD 'builder'`)

	s, addr := newProxyServer(
		ctx, t, sql.Stopper(), &ProxyOptions{RoutingRule: sql.ServingSQLAddr(), SkipVerify: true},
	)

	url := fmt.Sprintf("postgres://bob:wrong@%s?sslmode=disable&options=--cluster=dim-dog-28", addr)
	te.TestConnectErr(ctx, t, url, 0, "ERROR: password authentication failed for user bob")

	url = fmt.Sprintf("postgres://bob:builder@%s/?sslmode=disable&options=--cluster=dim-dog-28", addr)
	te.TestConnect(ctx, t, url, func(conn *pgx.Conn) {
		require.NoError(t, runTestQuery(ctx, conn))
	})
	require.Equal(t, int64(1), s.metrics.AuthFailedCount.Count())
	require.Equal(t, int64(1), s.metrics.SuccessfulConnCount.Count())
}

func TestErroneousFrontend(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	defer testutils.TestingHook(&FrontendAdmit, func(
		conn net.Conn, incomingTLSConfig *tls.Config,
	) (net.Conn, *pgproto3.StartupMessage, error) {
		return conn, nil, errors.New(frontendError)
	})()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	_, addr := newProxyServer(ctx, t, stopper, &ProxyOptions{})

	url := fmt.Sprintf("postgres://bob:builder@%s/?sslmode=disable&options=--cluster=dim-dog-28", addr)

	// Generic message here as the Frontend's error is not codeError and
	// by default we don't pass back error's text. The startup message doesn't
	// get processed in this case.
	te.TestConnectErr(ctx, t, url, 0, "internal server error")
}

func TestErroneousBackend(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	url := fmt.Sprintf("postgres://bob:builder@%s/?sslmode=disable&options=--cluster=dim-dog-28", addr)

	// Generic message here as the Backend's error is not codeError and
	// by default we don't pass back error's text. The startup message has
	// already been processed.
	te.TestConnectErr(ctx, t, url, 0, "internal server error")
}

func TestProxyRefuseConn(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	url := fmt.Sprintf("postgres://root:admin@%s?sslmode=require&options=--cluster=dim-dog-28", addr)
	te.TestConnectErr(ctx, t, url, codeProxyRefusedConnection, "too many attempts")
	require.Equal(t, int64(1), s.metrics.RefusedConnCount.Count())
	require.Equal(t, int64(0), s.metrics.SuccessfulConnCount.Count())
	require.Equal(t, int64(0), s.metrics.AuthFailedCount.Count())
}

func TestDenylistUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	denyList, err := ioutil.TempFile("", "*_denylist.yml")
	require.NoError(t, err)

	sql, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	sql.(*server.TestServer).PGServer().TestingSetTrustClientProvidedRemoteAddr(true)
	defer sql.Stopper().Stop(ctx)

	outgoingTLSConfig, err := sql.RPCContext().GetClientTLSConfig()
	require.NoError(t, err)
	proxyOutgoingTLSConfig := outgoingTLSConfig.Clone()
	proxyOutgoingTLSConfig.InsecureSkipVerify = true

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

	url := fmt.Sprintf("postgres://root:admin@%s/defaultdb_29?sslmode=require&options=--cluster=dim-dog-28", addr)
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

	ctx := context.Background()
	te := newTester()
	defer te.Close()

	srv, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	srv.(*server.TestServer).PGServer().TestingSetTrustClientProvidedRemoteAddr(true)
	defer srv.Stopper().Stop(ctx)

	// Create tenant 28.
	sqlConn := srv.InternalExecutor().(*sql.InternalExecutor)
	_, err := sqlConn.Exec(ctx, "", nil, "SELECT crdb_internal.create_tenant(28)")
	require.NoError(t, err)

	// New test directory server.
	tds1, tdsAddr := newDirectoryServer(ctx, t, srv, &net.TCPAddr{})

	// New proxy server using the directory. Define both the directory and the
	// routing rule so that fallback to the routing rule can be tested.
	const drainTimeout = 200 * time.Millisecond
	opts := &ProxyOptions{
		RoutingRule:   srv.ServingSQLAddr(),
		DirectoryAddr: tdsAddr.String(),
		Insecure:      true,
		DrainTimeout:  drainTimeout,
	}
	proxy, addr := newProxyServer(ctx, t, srv.Stopper(), opts)

	t.Run("fallback when tenant not found", func(t *testing.T) {
		defer testutils.TestingHook(&resolveTCPAddr,
			func(network, addr string) (*net.TCPAddr, error) {
				// Expect fallback.
				require.Equal(t, srv.ServingSQLAddr(), addr)
				return net.ResolveTCPAddr(network, addr)
			})()

		url := fmt.Sprintf(
			"postgres://root:admin@%s/?sslmode=disable&options=--cluster=tenant-cluster-%d",
			addr, notFoundTenantID)
		te.TestConnect(ctx, t, url, func(*pgx.Conn) {})
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
		defer testutils.TestingHook(&reportFailureToDirectory, func(
			ctx context.Context, tenantID roachpb.TenantID, addr string, directory *tenant.Directory,
		) error {
			require.Equal(t, roachpb.MakeTenantID(28), tenantID)
			addrs, err := directory.LookupTenantAddrs(ctx, tenantID)
			require.NoError(t, err)
			require.Len(t, addrs, 1)
			require.Equal(t, addrs[0], addr)

			countReports++
			err = directory.ReportFailure(ctx, tenantID, addr)
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

	t.Run("drain connection", func(t *testing.T) {
		url := fmt.Sprintf("postgres://root:admin@%s/?sslmode=disable&options=--cluster=tenant-cluster-28", addr)
		te.TestConnect(ctx, t, url, func(conn *pgx.Conn) {
			require.Equal(t, int64(1), proxy.metrics.CurConnCount.Value())

			// Connection should be terminated after draining.
			require.Eventually(t, func() bool {
				var n int
				err = conn.QueryRow(ctx, "SELECT $1::int", 1).Scan(&n)
				if err != nil {
					return true
				}
				require.EqualValues(t, 1, n)

				// Trigger drain of connections.
				tds2.Drain()
				return false
			}, 30*time.Second, 5*drainTimeout)

			require.Equal(t, int64(1), proxy.metrics.IdleDisconnectCount.Count())
		})
	})
}

func TestClusterNameAndTenantFromParams(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	testCases := []struct {
		name                string
		params              map[string]string
		expectedClusterName string
		expectedTenantID    uint64
		expectedParams      map[string]string
		expectedError       string
	}{
		{
			name:          "empty params",
			params:        map[string]string{},
			expectedError: "missing cluster name in connection string",
		},
		{
			name: "cluster name is not provided",
			params: map[string]string{
				"database": "defaultdb",
				"options":  "--foo=bar",
			},
			expectedError: "missing cluster name in connection string",
		},
		{
			name: "multiple similar cluster names",
			params: map[string]string{
				"database": "happy-koala-7.defaultdb",
				"options":  "--cluster=happy-koala",
			},
			expectedError: "multiple cluster names provided",
		},
		{
			name: "multiple different cluster names",
			params: map[string]string{
				"database": "happy-koala-7.defaultdb",
				"options":  "--cluster=happy-tiger",
			},
			expectedError: "multiple cluster names provided",
		},
		{
			name: "invalid cluster name in database param",
			params: map[string]string{
				// Cluster names need to be between 6 to 20 alphanumeric characters.
				"database": "short-0.defaultdb",
			},
			expectedError: "invalid cluster name 'short-0'",
		},
		{
			name: "invalid cluster name in options param",
			params: map[string]string{
				// Cluster names need to be between 6 to 20 alphanumeric characters.
				"options": "--cluster=cockroachlabsdotcomfoobarbaz-0",
			},
			expectedError: "invalid cluster name 'cockroachlabsdotcomfoobarbaz-0'",
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
			expectedError: "invalid cluster name 'happy2koala'",
		},
		{
			name:          "missing tenant id",
			params:        map[string]string{"database": "happy2koala-.defaultdb"},
			expectedError: "invalid cluster name 'happy2koala-'",
		},
		{
			name:          "missing cluster name",
			params:        map[string]string{"database": "-7.defaultdb"},
			expectedError: "invalid cluster name '-7'",
		},
		{
			name:          "bad tenant id",
			params:        map[string]string{"database": "happy-koala-0-7a.defaultdb"},
			expectedError: "invalid cluster name 'happy-koala-0-7a'",
		},
		{
			name:          "zero tenant id",
			params:        map[string]string{"database": "happy-koala-0.defaultdb"},
			expectedError: "invalid cluster name 'happy-koala-0'",
		},
		{
			name: "cluster name in database param",
			params: map[string]string{
				"database": "happy-koala-7.defaultdb",
				"foo":      "bar",
			},
			expectedClusterName: "happy-koala",
			expectedTenantID:    7,
			expectedParams:      map[string]string{"database": "defaultdb", "foo": "bar"},
		},
		{
			name: "valid cluster name with invalid arrangements",
			params: map[string]string{
				"database": "defaultdb",
				"options":  "-c --cluster=happy-koala-7 -c -c -c",
			},
			expectedClusterName: "happy-koala",
			expectedTenantID:    7,
			expectedParams:      map[string]string{"database": "defaultdb"},
		},
		{
			name: "short option: cluster name in options param",
			params: map[string]string{
				"database": "defaultdb",
				"options":  "-ccluster=happy-koala-7",
			},
			expectedClusterName: "happy-koala",
			expectedTenantID:    7,
			expectedParams:      map[string]string{"database": "defaultdb"},
		},
		{
			name: "short option with spaces: cluster name in options param",
			params: map[string]string{
				"database": "defaultdb",
				"options":  "-c   cluster=happy-koala-7",
			},
			expectedClusterName: "happy-koala",
			expectedTenantID:    7,
			expectedParams:      map[string]string{"database": "defaultdb"},
		},
		{
			name: "long option: cluster name in options param",
			params: map[string]string{
				"database": "defaultdb",
				"options":  "--cluster=happy-koala-7\t--foo=test",
			},
			expectedClusterName: "happy-koala",
			expectedTenantID:    7,
			expectedParams:      map[string]string{"database": "defaultdb"},
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

			outMsg, clusterName, tenantID, err := clusterNameAndTenantFromParams(ctx, msg)
			if tc.expectedError == "" {
				require.NoErrorf(t, err, "failed test case\n%+v", tc)

				// When expectedError is specified, we always have a valid expectedTenantID.
				require.Equal(t, roachpb.MakeTenantID(tc.expectedTenantID), tenantID)

				require.Equal(t, tc.expectedClusterName, clusterName)
				require.Equal(t, tc.expectedParams, outMsg.Parameters)
			} else {
				require.EqualErrorf(t, err, tc.expectedError, "failed test case\n%+v", tc)
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

	restoreResolveTCPAddr  func()
	restoreAuthenticate    func()
	restoreSendErrToClient func()
}

func newTester() *tester {
	te := &tester{}

	// Override default lookup function so that it does not use net.ResolveTCPAddr.
	te.restoreResolveTCPAddr =
		testutils.TestingHook(&resolveTCPAddr,
			func(network, addr string) (*net.TCPAddr, error) {
				return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 26257}, nil
			})

	// Record successful connection and authentication.
	originalAuthenticate := authenticate
	te.restoreAuthenticate =
		testutils.TestingHook(&authenticate, func(clientConn, crdbConn net.Conn) error {
			err := originalAuthenticate(clientConn, crdbConn)
			te.setAuthenticated(err == nil)
			return err
		})

	// Capture any error sent to the client.
	originalSendErrToClient := SendErrToClient
	te.restoreSendErrToClient =
		testutils.TestingHook(&SendErrToClient, func(conn net.Conn, err error) {
			if codeErr, ok := err.(*codeError); ok {
				te.setErrToClient(codeErr)
			}
			originalSendErrToClient(conn, err)
		})

	return te
}

func (te *tester) Close() {
	te.restoreResolveTCPAddr()
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
	conn, err := pgx.Connect(ctx, url)
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
	conn, err := pgx.Connect(ctx, url)
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
	opts.ListenKey = "testdata/testserver.key"
	opts.ListenCert = "testdata/testserver.crt"

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

		log.TestingClearServerIdentifiers()
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
