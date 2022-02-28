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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/denylist"
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
		require.Equal(t, outgoingAddr, "dim-dog-28-0.cockroachdb:26257")
		return nil, newErrorf(codeParamsRoutingFailed, "boom")
	})()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	s, addr := newSecureProxyServer(ctx, t, stopper, &ProxyOptions{RoutingRule: "{{clusterName}}-0.cockroachdb:26257"})

	longDB := strings.Repeat("x", 70) // 63 is limit
	pgurl := fmt.Sprintf("postgres://unused:unused@%s/%s?options=--cluster=dim-dog-28&sslmode=require", addr, longDB)
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
	_, addr := newSecureProxyServer(ctx, t, stopper, &ProxyOptions{RoutingRule: "undialable%$!@$:1234"})

	// Valid connection, but no backend server running.
	pgurl := fmt.Sprintf("postgres://unused:unused@%s/db?options=--cluster=dim-dog-28&sslmode=require", addr)
	te.TestConnectErr(ctx, t, pgurl, codeParamsRoutingFailed, "cluster dim-dog-28 not found")
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
		ctx, t, u+"?options=--cluster=dim-dog-28&sslmode=disable",
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
			ctx, t, u+"?options=--cluster=dim-dog-foo3&sslmode="+sslmode,
			codeParamsRoutingFailed, "invalid cluster identifier 'dim-dog-foo3'",
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

	url := fmt.Sprintf("postgres://bob:wrong@%s/dim-dog-28.defaultdb?sslmode=require", addr)
	te.TestConnectErr(ctx, t, url, 0, "failed SASL auth")

	url = fmt.Sprintf("postgres://bob@%s/dim-dog-28.defaultdb?sslmode=require", addr)
	te.TestConnectErr(ctx, t, url, 0, "failed SASL auth")

	url = fmt.Sprintf("postgres://bob:builder@%s/dim-dog-28.defaultdb?sslmode=require", addr)
	te.TestConnect(ctx, t, url, func(conn *pgx.Conn) {
		require.Equal(t, int64(1), s.metrics.CurConnCount.Value())
		require.NoError(t, runTestQuery(ctx, conn))
	})
	require.Equal(t, int64(1), s.metrics.SuccessfulConnCount.Count())
	require.Equal(t, int64(2), s.metrics.AuthFailedCount.Count())
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
			RoutingRule: "{{clusterName}}-0.cockroachdb:26257",
		})

		pgurl := fmt.Sprintf("postgres://unused:unused@%s/%s?options=--cluster=dim-dog-28&sslmode=require", addr, "defaultdb")
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
			RoutingRule: "{{clusterName}}-0.cockroachdb:26257",
		})

		pgurl := fmt.Sprintf("postgres://unused:unused@%s/%s?options=--cluster=dim-dog-28&sslmode=require", addr, "defaultdb")
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
			RoutingRule: "{{clusterName}}-0.cockroachdb:26257",
		})

		pgurl := fmt.Sprintf("postgres://unused:unused@%s/%s?options=--cluster=dim-dog-28&sslmode=require", addr, "defaultdb")
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

	u := fmt.Sprintf("postgres://bogususer:foo123@%s/?sslmode=require&authToken=abc123&options=--cluster=dim-dog-28&sslmode=require", proxyAddr)
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

	url := fmt.Sprintf("postgres://bob:wrong@%s?sslmode=disable&options=--cluster=dim-dog-28&sslmode=require", addr)
	te.TestConnectErr(ctx, t, url, 0, "failed SASL auth")

	url = fmt.Sprintf("postgres://bob:builder@%s/?sslmode=disable&options=--cluster=dim-dog-28&sslmode=require", addr)
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
	) (net.Conn, *pgproto3.StartupMessage, error) {
		return conn, nil, errors.New(frontendError)
	})()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	_, addr := newProxyServer(ctx, t, stopper, &ProxyOptions{})

	url := fmt.Sprintf("postgres://bob:builder@%s/?sslmode=disable&options=--cluster=dim-dog-28&sslmode=require", addr)

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

	url := fmt.Sprintf("postgres://bob:builder@%s/?sslmode=disable&options=--cluster=dim-dog-28&sslmode=require", addr)

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

	url := fmt.Sprintf("postgres://root:admin@%s?sslmode=require&options=--cluster=dim-dog-28&sslmode=require", addr)
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

	url := fmt.Sprintf("postgres://testuser:foo123@%s/defaultdb_29?sslmode=require&options=--cluster=dim-dog-28&sslmode=require", addr)
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
			ctx context.Context, tenantID roachpb.TenantID, addr string, directory TenantResolver,
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
			// The current connection count can take a bit of time to drop to 1,
			// since the previous successful connection asynchronously closes.
			// PGX cuts the connection on the client side, but it can take time
			// for the proxy to get the notification and react.
			require.Eventually(t, func() bool {
				return proxy.metrics.CurConnCount.Value() == 1
			}, 10*time.Second, 10*time.Millisecond)

			// Connection should be forcefully terminated after the drain timeout,
			// even though it's being continuously used.
			require.Eventually(t, func() bool {
				// Trigger drain of connections. Do this repeatedly inside the
				// loop in order to avoid race conditions where the proxy is not
				// yet hooked up to the directory server (and thus misses any
				// one-time DRAIN notifications).
				tds2.Drain()

				// Run query until it fails (because connection was closed).
				return runTestQuery(ctx, conn) != nil
			}, 30*time.Second, 5*drainTimeout)

			// Ensure failure was due to forced drain disconnection.
			require.Equal(t, int64(1), proxy.metrics.IdleDisconnectCount.Count())
		})
	})
}

func TestConnectionMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	tenantID := serverutils.TestTenantID()

	// Start first SQL pod.
	tenant1, mainDB1 := serverutils.StartTenant(t, s, tests.CreateTestTenantParams(tenantID))
	tenant1.PGServer().(*pgwire.Server).TestingSetTrustClientProvidedRemoteAddr(true)
	defer tenant1.Stopper().Stop(ctx)
	defer mainDB1.Close()

	// Start second SQL pod.
	params2 := tests.CreateTestTenantParams(tenantID)
	params2.Existing = true
	tenant2, mainDB2 := serverutils.StartTenant(t, s, params2)
	tenant2.PGServer().(*pgwire.Server).TestingSetTrustClientProvidedRemoteAddr(true)
	defer tenant2.Stopper().Stop(ctx)
	defer mainDB2.Close()

	_, err := mainDB1.Exec("CREATE USER testuser WITH PASSWORD 'hunter2'")
	require.NoError(t, err)
	_, err = mainDB1.Exec("GRANT admin TO testuser")
	require.NoError(t, err)
	_, err = mainDB1.Exec("SET CLUSTER SETTING server.user_login.session_revival_token.enabled = true")
	require.NoError(t, err)

	// Create a proxy server without using a directory. The directory is very
	// difficult to work with, and there isn't a way to easily stub out fake
	// loads. For this test, we will stub out lookupAddr in the connector. We
	// will alternate between tenant1 and tenant2, starting with tenant1.
	forwarderCh := make(chan *forwarder)
	opts := &ProxyOptions{SkipVerify: true, RoutingRule: tenant1.SQLAddr()}
	opts.testingKnobs.afterForward = func(f *forwarder) error {
		select {
		case forwarderCh <- f:
		case <-time.After(10 * time.Second):
			return errors.New("no receivers for forwarder")
		}
		return nil
	}
	_, addr := newSecureProxyServer(ctx, t, s.Stopper(), opts)

	// The tenant ID does not matter here since we stubbed RoutingRule.
	connectionString := fmt.Sprintf("postgres://testuser:hunter2@%s/?sslmode=require&options=--cluster=tenant-cluster-28", addr)

	type queryer interface {
		QueryRowContext(context.Context, string, ...interface{}) *gosql.Row
	}
	// queryAddr queries the SQL node that `db` is connected to for its address.
	queryAddr := func(t *testing.T, ctx context.Context, db queryer) string {
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
		select {
		case f = <-forwarderCh:
		case <-time.After(10 * time.Second):
			t.Fatal("no connection")
		}

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
			require.Equal(t, tenant1.SQLAddr(), queryAddr(t, tCtx, db))

			_, err = db.Exec("SET application_name = 'foo'")
			require.NoError(t, err)

			// Show that we get alternating SQL pods when we transfer.
			f.RequestTransfer()
			require.Eventually(t, func() bool {
				return f.metrics.ConnMigrationSuccessCount.Count() == 1
			}, 20*time.Second, 25*time.Millisecond)
			require.Equal(t, tenant2.SQLAddr(), queryAddr(t, tCtx, db))

			var name string
			require.NoError(t, db.QueryRow("SHOW application_name").Scan(&name))
			require.Equal(t, "foo", name)

			_, err = db.Exec("SET application_name = 'bar'")
			require.NoError(t, err)

			f.RequestTransfer()
			require.Eventually(t, func() bool {
				return f.metrics.ConnMigrationSuccessCount.Count() == 1
			}, 20*time.Second, 25*time.Millisecond)
			require.Equal(t, tenant1.SQLAddr(), queryAddr(t, tCtx, db))

			require.NoError(t, db.QueryRow("SHOW application_name").Scan(&name))
			require.Equal(t, "bar", name)

			// Now attempt a transfer concurrently with requests.
			closerCh := make(chan struct{})
			go func() {
				for i := 0; i < 10 && tCtx.Err() == nil; i++ {
					f.RequestTransfer()
					time.Sleep(500 * time.Millisecond)
				}
				closerCh <- struct{}{}
			}()

			// This test runs for 5 seconds.
			var tenant1Addr, tenant2Addr int
			for i := 0; i < 100; i++ {
				addr := queryAddr(t, tCtx, db)
				if addr == tenant1.SQLAddr() {
					tenant1Addr++
				} else {
					require.Equal(t, tenant2.SQLAddr(), addr)
					tenant2Addr++
				}
				time.Sleep(50 * time.Millisecond)
			}

			// In 5s, we should have at least 10 successful transfers. Just do
			// an approximation here.
			require.Eventually(t, func() bool {
				return f.metrics.ConnMigrationSuccessCount.Count() >= 5
			}, 20*time.Second, 25*time.Millisecond)
			require.True(t, tenant1Addr > 2)
			require.True(t, tenant2Addr > 2)
			require.Equal(t, int64(0), f.metrics.ConnMigrationErrorFatalCount.Count())

			// Ensure that the goroutine terminates so other subtests are not
			// affected.
			<-closerCh

			// There's a chance that we still have an in-progress transfer, so
			// attempt to wait.
			require.Eventually(t, func() bool {
				f.mu.Lock()
				defer f.mu.Unlock()
				return stateReady == f.mu.state
			}, 10*time.Second, 25*time.Millisecond)

			require.Equal(t, f.metrics.ConnMigrationAttemptedCount.Count(),
				f.metrics.ConnMigrationRequestedCount.Count())
			require.Equal(t, int64(0), f.metrics.ConnMigrationProtocolErrorCount.Count())
		})

		// Transfers should fail if there is an open transaction. These failed
		// transfers should not close the connection.
		t.Run("failed_transfers_with_tx", func(t *testing.T) {
			initSuccessCount := f.metrics.ConnMigrationSuccessCount.Count()
			initAddr := queryAddr(t, tCtx, db)

			err = crdb.ExecuteTx(tCtx, db, nil /* txopts */, func(tx *gosql.Tx) error {
				for i := 0; i < 10; i++ {
					f.RequestTransfer()
					addr := queryAddr(t, tCtx, tx)
					if initAddr != addr {
						return errors.Newf(
							"address does not match, expected %s, found %s",
							initAddr,
							addr,
						)
					}
					time.Sleep(50 * time.Millisecond)
				}
				return nil
			})
			require.NoError(t, err)

			// Make sure there are no pending transfers.
			func() {
				f.mu.Lock()
				defer f.mu.Unlock()
				require.Equal(t, stateReady, f.mu.state)
			}()

			// Just check that we have half of what we requested since we cannot
			// guarantee that the transfer will run within 50ms.
			require.True(t, f.metrics.ConnMigrationErrorRecoverableCount.Count() >= 5)
			require.Equal(t, int64(0), f.metrics.ConnMigrationErrorFatalCount.Count())
			require.Equal(t, initSuccessCount, f.metrics.ConnMigrationSuccessCount.Count())
			prevErrorRecoverableCount := f.metrics.ConnMigrationErrorRecoverableCount.Count()

			// Once the transaction is closed, transfers should work.
			f.RequestTransfer()
			require.Eventually(t, func() bool {
				return f.metrics.ConnMigrationSuccessCount.Count() == initSuccessCount+1
			}, 20*time.Second, 25*time.Millisecond)
			require.NotEqual(t, initAddr, queryAddr(t, tCtx, db))
			require.Equal(t, prevErrorRecoverableCount, f.metrics.ConnMigrationErrorRecoverableCount.Count())
			require.Equal(t, int64(0), f.metrics.ConnMigrationErrorFatalCount.Count())

			// We have already asserted metrics above, so transfer must have
			// been completed.
			f.mu.Lock()
			defer f.mu.Unlock()
			require.Equal(t, stateReady, f.mu.state)

			require.Equal(t, int64(0), f.metrics.ConnMigrationProtocolErrorCount.Count())
		})

		// Transfer timeout caused by dial issues should not close the session.
		// We will test this by introducing delays when connecting to the SQL
		// pod.
		t.Run("failed_transfers_with_dial_issues", func(t *testing.T) {
			initSuccessCount := f.metrics.ConnMigrationSuccessCount.Count()
			initErrorRecoverableCount := f.metrics.ConnMigrationErrorRecoverableCount.Count()
			initAddr := queryAddr(t, tCtx, db)

			// Set the delay longer than the timeout.
			lookupAddrDelayDuration = 10 * time.Second
			f.testingKnobs.transferTimeoutDuration = func() time.Duration {
				return 3 * time.Second
			}

			f.RequestTransfer()
			require.Eventually(t, func() bool {
				return f.metrics.ConnMigrationErrorRecoverableCount.Count() == initErrorRecoverableCount+1
			}, 20*time.Second, 25*time.Millisecond)
			require.Equal(t, initAddr, queryAddr(t, tCtx, db))
			require.Equal(t, initSuccessCount, f.metrics.ConnMigrationSuccessCount.Count())
			require.Equal(t, int64(0), f.metrics.ConnMigrationErrorFatalCount.Count())

			// We have already asserted metrics above, so transfer must have
			// been completed.
			f.mu.Lock()
			defer f.mu.Unlock()
			require.Equal(t, stateReady, f.mu.state)

			require.Equal(t, int64(0), f.metrics.ConnMigrationProtocolErrorCount.Count())
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
	// which is what the proxy needs, so we will stub isSafeTransferPoint
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

		// Spin up a goroutine to trigger the initial connection.
		go func() {
			_ = conn.PingContext(tCtx)
		}()

		var f *forwarder
		select {
		case f = <-forwarderCh:
		case <-time.After(10 * time.Second):
			t.Fatal("no connection")
		}

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
		f.testingKnobs.isSafeTransferPoint = func() bool {
			return true
		}
		f.testingKnobs.transferTimeoutDuration = func() time.Duration {
			// Transfer timeout is 3s, and we'll run pg_sleep for 10s.
			return 3 * time.Second
		}

		goCh := make(chan struct{}, 1)
		errCh := make(chan error, 1)
		go func() {
			goCh <- struct{}{}
			_, err = conn.ExecContext(tCtx, "SELECT pg_sleep(10)")
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
		time.Sleep(250 * time.Millisecond)
		f.RequestTransfer()

		// Connection should be closed because this is a non-recoverable error,
		// i.e. timeout after sending the request, but before fully receiving
		// its response.
		require.Eventually(t, func() bool {
			err := conn.PingContext(tCtx)
			return err != nil && strings.Contains(err.Error(), "bad connection")
		}, 20*time.Second, 25*time.Millisecond)

		select {
		case <-time.After(10 * time.Second):
			t.Fatalf("require that pg_sleep query terminates")
		case err = <-errCh:
			require.NotNil(t, err)
			require.Regexp(t, "bad connection", err.Error())
		}
		require.Eventually(t, func() bool {
			return f.metrics.ConnMigrationErrorFatalCount.Count() == 1
		}, 30*time.Second, 25*time.Millisecond)
		require.Equal(t, int64(0), f.metrics.ConnMigrationProtocolErrorCount.Count())

		f.mu.Lock()
		defer f.mu.Unlock()
		require.Equal(t, stateTransferSessionSerialization, f.mu.state)
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
				// Cluster names need to be between 6 to 20 alphanumeric characters.
				"database": "short-0.defaultdb",
			},
			expectedError: "invalid cluster identifier 'short-0'",
			expectedHint:  "Is 'short' a valid cluster name?\n--\n" + clusterNameFormHint,
		},
		{
			name: "invalid cluster identifier in options param",
			params: map[string]string{
				// Cluster names need to be between 6 to 20 alphanumeric characters.
				"options": "--cluster=cockroachlabsdotcomfoobarbaz-0",
			},
			expectedError: "invalid cluster identifier 'cockroachlabsdotcomfoobarbaz-0'",
			expectedHint:  "Is 'cockroachlabsdotcomfoobarbaz' a valid cluster name?\n--\n" + clusterNameFormHint,
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
				"database": "happy-koala-7.defaultdb",
				"foo":      "bar",
			},
			expectedClusterName: "happy-koala",
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

			outMsg, clusterName, tenantID, err := clusterNameAndTenantFromParams(ctx, msg)
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
	cfg, err := pgx.ParseConfig(url)
	require.NoError(t, err)

	// Prevent pgx from tying to connect to the `::1` ipv6 address for localhost.
	cfg.LookupFunc = func(ctx context.Context, s string) ([]string, error) { return []string{s}, nil }
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
