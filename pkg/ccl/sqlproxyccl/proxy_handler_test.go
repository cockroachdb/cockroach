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
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

// To ensure tenant startup code is included.
var _ = kvtenantccl.Connector{}

const frontendError = "Frontend error!"
const backendError = "Backend error!"

func hookBackendDial(
	f func(
		msg *pgproto3.StartupMessage, outgoingAddress string, tlsConfig *tls.Config,
	) (net.Conn, error),
) func() {
	return testutils.TestingHook(&backendDial, f)
}
func hookFrontendAdmit(
	f func(conn net.Conn, incomingTLSConfig *tls.Config) (net.Conn, *pgproto3.StartupMessage, error),
) func() {
	return testutils.TestingHook(&frontendAdmit, f)
}
func hookSendErrToClient(f func(conn net.Conn, err error)) func() {
	return testutils.TestingHook(&sendErrToClient, f)
}
func hookAuthenticate(f func(clientConn, crdbConn net.Conn) error) func() {
	return testutils.TestingHook(&authenticate, f)
}

func newSecureProxyServer(
	ctx context.Context, t *testing.T, stopper *stop.Stopper, opts *ProxyOptions,
) (server *Server, addr string) {
	// Created via:
	const _ = `
openssl genrsa -out testserver.key 2048
openssl req -new -x509 -sha256 -key testserver.key -out testserver.crt \
  -days 3650 -config testserver_config.cnf	
`
	opts.ListenKey = "testserver.key"
	opts.ListenCert = "testserver.crt"

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

type assertCtx struct {
	emittedCode *errorCode
}

func makeAssertCtx() assertCtx {
	var emittedCode errorCode = -1
	return assertCtx{
		emittedCode: &emittedCode,
	}
}

func (ac *assertCtx) onSendErrToClient(code errorCode) {
	*ac.emittedCode = code
}

func (ac *assertCtx) assertConnectErr(
	ctx context.Context, t *testing.T, prefix, suffix string, expCode errorCode, expErr string,
) {
	t.Helper()
	*ac.emittedCode = -1
	t.Run(suffix, func(t *testing.T) {
		conn, err := pgx.Connect(ctx, prefix+suffix)
		if err == nil {
			_ = conn.Close(ctx)
		}
		require.Contains(t, err.Error(), expErr)
		require.Equal(t, expCode, *ac.emittedCode)
	})
}

func TestLongDBName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	defer hookBackendDial(func(_ *pgproto3.StartupMessage, outgoingAddr string, _ *tls.Config) (net.Conn, error) {
		require.Equal(t, outgoingAddr, "dim-dog-28-0.cockroachdb:26257")
		return nil, newErrorf(codeParamsRoutingFailed, "boom")
	})()

	ac := makeAssertCtx()
	originalSendErrToClient := sendErrToClient
	defer hookSendErrToClient(func(conn net.Conn, err error) {
		if codeErr, ok := err.(*codeError); ok {
			ac.onSendErrToClient(codeErr.code)
		}
		originalSendErrToClient(conn, err)
	})()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	s, addr := newSecureProxyServer(ctx, t, stopper, &ProxyOptions{RoutingRule: "{{clusterName}}-0.cockroachdb:26257"})

	longDB := strings.Repeat("x", 70) // 63 is limit
	pgurl := fmt.Sprintf("postgres://unused:unused@%s/%s?options=--cluster=dim-dog-28", addr, longDB)
	ac.assertConnectErr(ctx, t, pgurl, "" /* suffix */, codeParamsRoutingFailed, "boom")
	require.Equal(t, int64(1), s.metrics.RoutingErrCount.Count())
}

func TestFailedConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	// TODO(asubiotto): consider using datadriven for these, especially if the
	// proxy becomes more complex.

	var originalSendErrToClient = sendErrToClient
	ac := makeAssertCtx()
	defer hookSendErrToClient(func(conn net.Conn, err error) {
		if codeErr, ok := err.(*codeError); ok {
			ac.onSendErrToClient(codeErr.code)
		}
		originalSendErrToClient(conn, err)
	})()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	s, addr := newSecureProxyServer(ctx, t, stopper, &ProxyOptions{RoutingRule: "undialable%$!@$"})

	_, p, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	u := fmt.Sprintf("postgres://unused:unused@localhost:%s/", p)
	// Valid connections, but no backend server running.
	for _, sslmode := range []string{"require", "prefer"} {
		ac.assertConnectErr(
			ctx, t, u, "?options=--cluster=dim-dog-28&sslmode="+sslmode,
			codeBackendDown, "unable to reach backend SQL server",
		)
	}

	ac.assertConnectErr(
		ctx, t, u, "?options=--cluster=dim-dog-28&sslmode=verify-ca&sslrootcert=testserver.crt",
		codeBackendDown, "unable to reach backend SQL server",
	)
	ac.assertConnectErr(
		ctx, t, u, "?options=--cluster=dim-dog-28&sslmode=verify-full&sslrootcert=testserver.crt",
		codeBackendDown, "unable to reach backend SQL server",
	)
	require.Equal(t, int64(4), s.metrics.BackendDownCount.Count())

	// Unencrypted connections bounce.
	for _, sslmode := range []string{"disable", "allow"} {
		ac.assertConnectErr(
			ctx, t, u, "?options=--cluster=dim-dog-28&sslmode="+sslmode,
			codeUnexpectedInsecureStartupMessage, "server requires encryption",
		)
	}
	require.Equal(t, int64(0), s.metrics.RoutingErrCount.Count())
	// TenantID rejected as malformed.
	ac.assertConnectErr(
		ctx, t, u, "?options=--cluster=dim&sslmode=require",
		codeParamsRoutingFailed, "invalid cluster name",
	)
	require.Equal(t, int64(1), s.metrics.RoutingErrCount.Count())
	// No TenantID.
	ac.assertConnectErr(
		ctx, t, u, "?sslmode=require",
		codeParamsRoutingFailed, "missing cluster name",
	)
	require.Equal(t, int64(2), s.metrics.RoutingErrCount.Count())
}

func TestUnexpectedError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	// Set up a Server whose FrontendAdmitter function always errors with a
	// non-codeError error.
	defer hookFrontendAdmit(
		func(conn net.Conn, incomingTLSConfig *tls.Config) (net.Conn, *pgproto3.StartupMessage, error) {
			log.Infof(context.Background(), "frontendAdmit returning unexpected error")
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
	require.Error(t, err)
	t.Log(err)
	elapsed := timeutil.Since(start)
	if elapsed >= 5*time.Second {
		t.Errorf("pgx.Connect took %s to error out", elapsed)
	}
}

func TestProxyAgainstSecureCRDB(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	sql, db, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	sql.(*server.TestServer).PGServer().TestingSetTrustClientProvidedRemoteAddr(true)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE USER bob WITH PASSWORD 'builder'`)

	var connSuccess bool
	originalAuthenticate := authenticate
	defer hookAuthenticate(func(clientConn, crdbConn net.Conn) error {
		err := originalAuthenticate(clientConn, crdbConn)
		connSuccess = err == nil
		return err
	})()

	defer sql.Stopper().Stop(ctx)

	s, addr := newSecureProxyServer(
		ctx, t, sql.Stopper(), &ProxyOptions{RoutingRule: sql.ServingSQLAddr(), SkipVerify: true},
	)

	url := fmt.Sprintf("postgres://bob:wrong@%s/dim-dog-28.defaultdb?sslmode=require", addr)
	_, err := pgx.Connect(ctx, url)
	require.Regexp(t, "ERROR: password authentication failed for user bob", err)

	url = fmt.Sprintf("postgres://bob@%s/dim-dog-28.defaultdb?sslmode=require", addr)
	_, err = pgx.Connect(ctx, url)
	require.Regexp(t, "ERROR: password authentication failed for user bob", err)

	url = fmt.Sprintf("postgres://bob:builder@%s/dim-dog-28.defaultdb?sslmode=require", addr)
	conn, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close(ctx))
		require.True(t, connSuccess)
		require.Equal(t, int64(1), s.metrics.SuccessfulConnCount.Count())
		require.Equal(t, int64(2), s.metrics.AuthFailedCount.Count())
	}()

	require.Equal(t, int64(1), s.metrics.CurConnCount.Value())
	require.NoError(t, runTestQuery(ctx, conn))
}

func TestProxyTLSClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// NB: The leaktest call is an important part of this test. We're
	// verifying that no goroutines are leaked, despite calling Close an
	// underlying TCP connection (rather than the TLSConn that wraps it).

	ctx := context.Background()

	sql, db, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	sql.(*server.TestServer).PGServer().TestingSetTrustClientProvidedRemoteAddr(true)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE USER bob WITH PASSWORD 'builder'`)

	var proxyIncomingConn atomic.Value // *conn
	var connSuccess bool
	frontendAdmit := frontendAdmit
	defer hookFrontendAdmit(func(conn net.Conn, incomingTLSConfig *tls.Config) (net.Conn, *pgproto3.StartupMessage, error) {
		proxyIncomingConn.Store(conn)
		return frontendAdmit(conn, incomingTLSConfig)
	})()
	originalAuthenticate := authenticate
	defer hookAuthenticate(func(clientConn, crdbConn net.Conn) error {
		err := originalAuthenticate(clientConn, crdbConn)
		connSuccess = err == nil
		return err
	})()

	defer sql.Stopper().Stop(ctx)

	s, addr := newSecureProxyServer(
		ctx, t, sql.Stopper(), &ProxyOptions{RoutingRule: sql.ServingSQLAddr(), SkipVerify: true},
	)

	url := fmt.Sprintf("postgres://bob:builder@%s/dim-dog-28.defaultdb?sslmode=require", addr)
	c, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	require.Equal(t, int64(1), s.metrics.CurConnCount.Value())
	defer func() {
		incomingConn, ok := proxyIncomingConn.Load().(*conn)
		require.True(t, ok)
		require.NoError(t, incomingConn.Close())
		<-incomingConn.done() // should immediately proceed

		require.True(t, connSuccess)
		require.Equal(t, int64(1), s.metrics.SuccessfulConnCount.Count())
		require.Equal(t, int64(0), s.metrics.AuthFailedCount.Count())
	}()

	require.NoError(t, runTestQuery(ctx, c))
}

func TestProxyModifyRequestParams(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	sql, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	sql.(*server.TestServer).PGServer().TestingSetTrustClientProvidedRemoteAddr(true)

	outgoingTLSConfig, err := sql.RPCContext().GetClientTLSConfig()
	require.NoError(t, err)
	proxyOutgoingTLSConfig := outgoingTLSConfig.Clone()
	proxyOutgoingTLSConfig.InsecureSkipVerify = true

	backendDial := backendDial
	defer hookBackendDial(func(msg *pgproto3.StartupMessage, outgoingAddress string, tlsConfig *tls.Config) (net.Conn, error) {
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

		return backendDial(msg, sql.ServingSQLAddr(), proxyOutgoingTLSConfig)
	})()

	defer sql.Stopper().Stop(ctx)

	s, proxyAddr := newSecureProxyServer(ctx, t, sql.Stopper(), &ProxyOptions{})

	u := fmt.Sprintf("postgres://bogususer@%s/?sslmode=require&authToken=abc123&options=--cluster=dim-dog-28", proxyAddr)
	conn, err := pgx.Connect(ctx, u)
	require.NoError(t, err)
	require.Equal(t, int64(1), s.metrics.CurConnCount.Value())
	defer func() {
		require.NoError(t, conn.Close(ctx))
	}()

	require.NoError(t, runTestQuery(ctx, conn))
}

func TestInsecureProxy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	sql, db, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	defer sql.Stopper().Stop(ctx)
	sql.(*server.TestServer).PGServer().TestingSetTrustClientProvidedRemoteAddr(true)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE USER bob WITH PASSWORD 'builder'`)

	s, addr := newProxyServer(
		ctx, t, sql.Stopper(), &ProxyOptions{RoutingRule: sql.ServingSQLAddr(), SkipVerify: true},
	)

	u := fmt.Sprintf("postgres://bob:wrong@%s?sslmode=disable&options=--cluster=dim-dog-28", addr)
	_, err := pgx.Connect(ctx, u)
	require.Error(t, err)
	require.Regexp(t, "ERROR: password authentication failed for user bob", err)

	u = fmt.Sprintf("postgres://bob:builder@%s/?sslmode=disable&options=--cluster=dim-dog-28", addr)
	conn, err := pgx.Connect(ctx, u)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, conn.Close(ctx))
		require.Equal(t, int64(1), s.metrics.AuthFailedCount.Count())
		require.Equal(t, int64(1), s.metrics.SuccessfulConnCount.Count())
	}()

	require.NoError(t, runTestQuery(ctx, conn))
}

func TestInsecureDoubleProxy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	sql, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	defer sql.Stopper().Stop(ctx)
	sql.(*server.TestServer).PGServer().TestingSetTrustClientProvidedRemoteAddr(true)

	// Test multiple proxies:  proxyB -> proxyA -> tc
	_, proxyA := newProxyServer(ctx, t, sql.Stopper(),
		&ProxyOptions{RoutingRule: sql.ServingSQLAddr(), Insecure: true},
	)
	_, proxyB := newProxyServer(ctx, t, sql.Stopper(),
		&ProxyOptions{RoutingRule: proxyA, Insecure: true},
	)

	u := fmt.Sprintf("postgres://root:admin@%s/dim-dog-28.dim-dog-29.defaultdb?sslmode=disable", proxyB)
	conn, err := pgx.Connect(ctx, u)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close(ctx))
	}()
	require.NoError(t, runTestQuery(ctx, conn))
}

func TestErroneousFrontend(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	defer hookFrontendAdmit(func(conn net.Conn, incomingTLSConfig *tls.Config) (net.Conn, *pgproto3.StartupMessage, error) {
		return conn, nil, errors.New(frontendError)
	})()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	_, addr := newProxyServer(ctx, t, stopper, &ProxyOptions{})

	u := fmt.Sprintf("postgres://bob:builder@%s/?sslmode=disable&options=--cluster=dim-dog-28", addr)

	_, err := pgx.Connect(ctx, u)
	require.Error(t, err)
	// Generic message here as the Frontend's error is not codeError and
	// by default we don't pass back error's text. The startup message doesn't get
	// processed in this case.
	require.Regexp(t, "connection reset by peer|failed to receive message", err)
}

func TestErroneousBackend(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	defer hookBackendDial(
		func(msg *pgproto3.StartupMessage, outgoingAddress string, tlsConfig *tls.Config) (net.Conn, error) {
			return nil, errors.New(backendError)
		})()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	_, addr := newProxyServer(ctx, t, stopper, &ProxyOptions{})

	u := fmt.Sprintf("postgres://bob:builder@%s/?sslmode=disable&options=--cluster=dim-dog-28", addr)

	_, err := pgx.Connect(ctx, u)
	require.Error(t, err)
	// Generic message here as the Backend's error is not codeError and
	// by default we don't pass back error's text. The startup message has already
	// been processed.
	require.Regexp(t, "failed to receive message", err)
}

func TestProxyRefuseConn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	defer hookBackendDial(func(_ *pgproto3.StartupMessage, _ string, _ *tls.Config) (net.Conn, error) {
		return nil, newErrorf(codeProxyRefusedConnection, "too many attempts")
	})()

	ac := makeAssertCtx()
	originalSendErrToClient := sendErrToClient
	defer hookSendErrToClient(func(conn net.Conn, err error) {
		if codeErr, ok := err.(*codeError); ok {
			ac.onSendErrToClient(codeErr.code)
		}
		originalSendErrToClient(conn, err)
	})()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	s, addr := newSecureProxyServer(ctx, t, stopper, &ProxyOptions{})

	ac.assertConnectErr(
		ctx, t, fmt.Sprintf("postgres://root:admin@%s/", addr),
		"?sslmode=require&options=--cluster=dim-dog-28",
		codeProxyRefusedConnection, "too many attempts",
	)
	require.Equal(t, int64(1), s.metrics.RefusedConnCount.Count())
	require.Equal(t, int64(0), s.metrics.SuccessfulConnCount.Count())
	require.Equal(t, int64(0), s.metrics.AuthFailedCount.Count())
}

func TestDenylistUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	sql, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	sql.(*server.TestServer).PGServer().TestingSetTrustClientProvidedRemoteAddr(true)

	denyList, err := ioutil.TempFile("", "*_denylist.yml")
	require.NoError(t, err)

	outgoingTLSConfig, err := sql.RPCContext().GetClientTLSConfig()
	require.NoError(t, err)
	proxyOutgoingTLSConfig := outgoingTLSConfig.Clone()
	proxyOutgoingTLSConfig.InsecureSkipVerify = true

	backendDial := backendDial
	defer hookBackendDial(func(msg *pgproto3.StartupMessage, outgoingAddress string, tlsConfig *tls.Config) (net.Conn, error) {
		time.AfterFunc(100*time.Millisecond, func() {
			_, err := denyList.WriteString("127.0.0.1: test-denied")
			require.NoError(t, err)
		})
		return backendDial(msg, sql.ServingSQLAddr(), proxyOutgoingTLSConfig)
	})()

	defer sql.Stopper().Stop(ctx)

	s, addr := newSecureProxyServer(ctx, t, sql.Stopper(), &ProxyOptions{Denylist: denyList.Name()})
	defer func() { _ = os.Remove(denyList.Name()) }()

	url := fmt.Sprintf("postgres://root:admin@%s/defaultdb_29?sslmode=require&options=--cluster=dim-dog-28", addr)
	conn, err := pgx.Connect(context.Background(), url)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close(ctx))
		require.Equal(t, int64(1), s.metrics.ExpiredClientConnCount.Count())
	}()

	require.Eventuallyf(
		t,
		func() bool {
			_, err = conn.Exec(context.Background(), "SELECT 1")
			return err != nil && strings.Contains(err.Error(), "expired")
		},
		time.Second, 5*time.Millisecond,
		"unexpected error received: %v", err,
	)
}

func TestProxyAgainstSecureCRDBWithIdleTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	sql, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	sql.(*server.TestServer).PGServer().TestingSetTrustClientProvidedRemoteAddr(true)

	outgoingTLSConfig, err := sql.RPCContext().GetClientTLSConfig()
	require.NoError(t, err)
	proxyOutgoingTLSConfig := outgoingTLSConfig.Clone()
	proxyOutgoingTLSConfig.InsecureSkipVerify = true

	idleTimeout, _ := time.ParseDuration("0.5s")
	var connSuccess bool
	frontendAdmit := frontendAdmit
	defer hookFrontendAdmit(func(conn net.Conn, incomingTLSConfig *tls.Config) (net.Conn, *pgproto3.StartupMessage, error) {
		return frontendAdmit(conn, incomingTLSConfig)
	})()
	backendDial := backendDial
	defer hookBackendDial(func(msg *pgproto3.StartupMessage, outgoingAddress string, tlsConfig *tls.Config) (net.Conn, error) {
		return backendDial(msg, sql.ServingSQLAddr(), proxyOutgoingTLSConfig)
	})()
	originalAuthenticate := authenticate
	defer hookAuthenticate(func(clientConn, crdbConn net.Conn) error {
		err := originalAuthenticate(clientConn, crdbConn)
		connSuccess = err == nil
		return err
	})()

	defer sql.Stopper().Stop(ctx)

	s, addr := newSecureProxyServer(ctx, t, sql.Stopper(), &ProxyOptions{IdleTimeout: idleTimeout})

	url := fmt.Sprintf("postgres://root:admin@%s/?sslmode=require&options=--cluster=dim-dog-28", addr)
	conn, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	require.Equal(t, int64(1), s.metrics.CurConnCount.Value())
	defer func() {
		require.NoError(t, conn.Close(ctx))
		require.True(t, connSuccess)
		require.Equal(t, int64(1), s.metrics.SuccessfulConnCount.Count())
	}()

	var n int
	err = conn.QueryRow(ctx, "SELECT $1::int", 1).Scan(&n)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)

	time.Sleep(idleTimeout * 2)
	err = conn.QueryRow(context.Background(), "SELECT $1::int", 1).Scan(&n)
	require.EqualError(t, err, "FATAL: terminating connection due to idle timeout (SQLSTATE 57P01)")
}

func newDirectoryServer(
	ctx context.Context, t *testing.T, srv serverutils.TestServerInterface, addr *net.TCPAddr,
) (*stop.Stopper, *net.TCPAddr) {
	tdsStopper := stop.NewStopper()
	var listener *net.TCPListener
	var err error
	require.Eventually(t, func() bool {
		listener, err = net.ListenTCP("tcp", addr)
		return err == nil
	}, 30*time.Second, time.Second)
	require.NoError(t, err)
	tds, err := tenant.NewTestDirectoryServer(tdsStopper)
	require.NoError(t, err)
	tds.TenantStarterFunc = func(ctx context.Context, tenantID uint64) (*tenant.Process, error) {
		log.TestingClearServerIdentifiers()
		tenantStopper := tenant.NewSubStopper(tdsStopper)
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
		return &tenant.Process{SQL: sqlAddr, Stopper: tenantStopper}, nil
	}
	go func() { require.NoError(t, tds.Serve(listener)) }()
	return tdsStopper, listener.Addr().(*net.TCPAddr)
}

func TestDirectoryReconnect(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	// New test cluster
	srv, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	defer srv.Stopper().Stop(ctx)

	// Create tenant 28
	sqlConn := srv.InternalExecutor().(*sql.InternalExecutor)
	_, err := sqlConn.Exec(ctx, "", nil, "SELECT crdb_internal.create_tenant(28)")
	require.NoError(t, err)

	// New test directory server
	stopper1, tdsAddr := newDirectoryServer(ctx, t, srv, &net.TCPAddr{})

	// New proxy server using the directory
	_, addr := newProxyServer(
		ctx, t, srv.Stopper(), &ProxyOptions{DirectoryAddr: tdsAddr.String(), Insecure: true},
	)

	// try to connect - should be successful.
	url := fmt.Sprintf("postgres://root:admin@%s/?sslmode=disable&options=--cluster=tenant-cluster-28", addr)
	_, err = pgx.Connect(ctx, url)
	require.NoError(t, err)

	// Stop the directory server and the tenant
	stopper1.Stop(ctx)

	stopper2, _ := newDirectoryServer(ctx, t, srv, tdsAddr)
	defer stopper2.Stop(ctx)

	require.Eventually(t, func() bool {
		// try to connect through the proxy again - should be successful.
		url = fmt.Sprintf("postgres://root:admin@%s/?sslmode=disable&options=--cluster=tenant-cluster-28", addr)
		_, err = pgx.Connect(ctx, url)
		return err == nil
	}, 1000*time.Second, 100*time.Millisecond)
}
