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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

const FrontendError = "Frontend error!"
const BackendError = "Backend error!"

func HookBackendDial(f func(
	msg *pgproto3.StartupMessage, outgoingAddress string, tlsConfig *tls.Config,
) (net.Conn, error)) func() {
	return testutils.HookGlobal(&BackendDial, f)
}
func HookFrontendAdmit(
	f func(conn net.Conn, incomingTLSConfig *tls.Config) (net.Conn, *pgproto3.StartupMessage, error),
) func() {
	return testutils.HookGlobal(&FrontendAdmit, f)
}
func HookSendErrToClient(f func(conn net.Conn, err error)) func() {
	return testutils.HookGlobal(&SendErrToClient, f)
}
func HookAuthenticate(f func(clientConn, crdbConn net.Conn) error) func() {
	return testutils.HookGlobal(&Authenticate, f)
}

func newSecureProxyServer(t *testing.T, ctx context.Context, opts *ProxyOptions) (server *Server, addr string, cleanup func()) {
	// Created via:
	const _ = `
openssl genrsa -out testserver.key 2048
openssl req -new -x509 -sha256 -key testserver.key -out testserver.crt \
  -days 3650 -config testserver_config.cnf	
`
	opts.ListenKey = "testserver.key"
	opts.ListenCert = "testserver.crt"

	return newProxyServer(t, ctx, opts)
}

func newProxyServer(t *testing.T, ctx context.Context, opts *ProxyOptions) (server *Server, addr string, done func()) {
	handler, err := NewProxyHandler(ctx, *opts)
	require.NoError(t, err)

	const listenAddress = "127.0.0.1:0"
	// NB: ln closes before wg.Wait or we deadlock.
	ln, err := net.Listen("tcp", listenAddress)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)

	done = func() {
		_ = ln.Close()
		wg.Wait()
	}

	server = NewServer(func(ctx context.Context, metrics *Metrics, proxyConn *Conn) error {
		return handler.Handle(ctx, metrics, proxyConn)
	})

	go func() {
		defer wg.Done()
		_ = server.Serve(ctx, ln)
	}()

	return server, ln.Addr().String(), done
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
	emittedCode *ErrorCode
}

func makeAssertCtx() assertCtx {
	var emittedCode ErrorCode = -1
	return assertCtx{
		emittedCode: &emittedCode,
	}
}

func (ac *assertCtx) onSendErrToClient(code ErrorCode) {
	*ac.emittedCode = code
}

func (ac *assertCtx) assertConnectErr(
	t *testing.T, ctx context.Context, prefix, suffix string, expCode ErrorCode, expErr string,
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

	defer HookBackendDial(func(_ *pgproto3.StartupMessage, _ string, _ *tls.Config) (net.Conn, error) {
		return nil, NewErrorf(CodeParamsRoutingFailed, "boom")
	})()

	ac := makeAssertCtx()
	sendErrToClient := SendErrToClient
	defer HookSendErrToClient(func(conn net.Conn, err error) {
		sendErrToClient(conn, err)
		if codeErr, ok := err.(*CodeError); ok {
			ac.onSendErrToClient(codeErr.Code)
		}
	})()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, addr, done := newSecureProxyServer(t, ctx, &ProxyOptions{})
	defer done()

	longDB := strings.Repeat("x", 70) // 63 is limit
	pgurl := fmt.Sprintf("postgres://unused:unused@%s/%s?options=--cluster=dim-dog-28", addr, longDB)
	ac.assertConnectErr(t, ctx, pgurl, "" /* suffix */, CodeParamsRoutingFailed, "boom")
	require.Equal(t, int64(1), s.Metrics.RoutingErrCount.Count())
}

func TestFailedConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// TODO(asubiotto): consider using datadriven for these, especially if the
	// proxy becomes more complex.

	var sendErrToClient = SendErrToClient
	ac := makeAssertCtx()
	defer HookSendErrToClient(func(conn net.Conn, err error) {
		sendErrToClient(conn, err)
		if codeErr, ok := err.(*CodeError); ok {
			ac.onSendErrToClient(codeErr.Code)
		}
	})()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, addr, done := newSecureProxyServer(t, ctx, &ProxyOptions{RoutingRule: "undialable%$!@$"})
	defer done()

	_, p, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	u := fmt.Sprintf("postgres://unused:unused@localhost:%s/", p)
	// Valid connections, but no backend server running.
	for _, sslmode := range []string{"require", "prefer"} {
		ac.assertConnectErr(
			t, ctx, u, "?options=--cluster=dim-dog-28&sslmode="+sslmode,
			CodeBackendDown, "unable to reach backend SQL server",
		)
	}

	ac.assertConnectErr(
		t, ctx, u, "?options=--cluster=dim-dog-28&sslmode=verify-ca&sslrootcert=testserver.crt",
		CodeBackendDown, "unable to reach backend SQL server",
	)
	ac.assertConnectErr(
		t, ctx, u, "?options=--cluster=dim-dog-28&sslmode=verify-full&sslrootcert=testserver.crt",
		CodeBackendDown, "unable to reach backend SQL server",
	)
	require.Equal(t, int64(4), s.Metrics.BackendDownCount.Count())

	// Unencrypted connections bounce.
	for _, sslmode := range []string{"disable", "allow"} {
		ac.assertConnectErr(
			t, ctx, u, "?options=--cluster=dim-dog-28&sslmode="+sslmode,
			CodeUnexpectedInsecureStartupMessage, "server requires encryption",
		)
	}

	// TenantID rejected as malformed.
	ac.assertConnectErr(
		t, ctx, u, "?options=--cluster=dim&sslmode=require",
		CodeParamsRoutingFailed, "invalid cluster name",
	)

	// No TenantID.
	ac.assertConnectErr(
		t, ctx, u, "?sslmode=require",
		CodeParamsRoutingFailed, "missing cluster name",
	)
	require.Equal(t, int64(2), s.Metrics.RoutingErrCount.Count())
}

func TestUnexpectedError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up a Server whose FrontendAdmitter function always errors with a
	// non-CodeError error.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer HookFrontendAdmit(
		func(conn net.Conn, incomingTLSConfig *tls.Config) (net.Conn, *pgproto3.StartupMessage, error) {
			return conn, nil, errors.New("unexpected error")
		})()
	s := NewServer((&ProxyHandler{}).Handle)
	const listenAddress = "127.0.0.1:0"
	ln, err := net.Listen("tcp", listenAddress)
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.Serve(ctx, ln)
	}()
	defer func() {
		_ = ln.Close()
		wg.Wait()
	}()

	u := fmt.Sprintf("postgres://root:admin@%s/?sslmode=disable&connect_timeout=5", ln.Addr().String())

	// Time how long it takes for pgx.Connect to return. If the proxy handles
	// errors appropriately, pgx.Connect should return near immediately
	// because the server should close the connection. If not, it may take up
	// to the 5s connect_timeout for pgx.Connect to give up.
	start := timeutil.Now()
	_, err = pgx.Connect(ctx, u)
	require.Error(t, err)
	t.Log(err)
	elapsed := timeutil.Since(start)
	if elapsed >= 5*time.Second {
		t.Errorf("pgx.Connect took %s to error out", elapsed)
	}
}

func TestProxyAgainstSecureCRDB(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sql, db, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	defer sql.Stopper().Stop(ctx)
	defer sql.(*server.TestServer).PGServer().TestingSetTrustClientProvidedRemoteAddr(true)()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE USER bob WITH PASSWORD 'builder'`)

	var connSuccess bool
	authenticate := Authenticate
	defer HookAuthenticate(func(clientConn, crdbConn net.Conn) error {
		err := authenticate(clientConn, crdbConn)
		connSuccess = err == nil
		return err
	})()

	s, addr, done := newSecureProxyServer(
		t, ctx, &ProxyOptions{RoutingRule: sql.ServingSQLAddr(), SkipVerify: true},
	)
	defer done()

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
		require.Equal(t, int64(1), s.Metrics.SuccessfulConnCount.Count())
		require.Equal(t, int64(2), s.Metrics.AuthFailedCount.Count())
	}()

	require.Equal(t, int64(1), s.Metrics.CurConnCount.Value())
	require.NoError(t, runTestQuery(ctx, conn))
}

func TestProxyTLSClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// NB: The leaktest call is an important part of this test. We're
	// verifying that no goroutines are leaked, despite calling Close an
	// underlying TCP connection (rather than the TLSConn that wraps it).

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sql, db, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	defer sql.Stopper().Stop(ctx)
	defer sql.(*server.TestServer).PGServer().TestingSetTrustClientProvidedRemoteAddr(true)()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE USER bob WITH PASSWORD 'builder'`)

	var proxyIncomingConn atomic.Value // *Conn
	var connSuccess bool
	frontendAdmit := FrontendAdmit
	defer HookFrontendAdmit(func(conn net.Conn, incomingTLSConfig *tls.Config) (net.Conn, *pgproto3.StartupMessage, error) {
		proxyIncomingConn.Store(conn)
		return frontendAdmit(conn, incomingTLSConfig)
	})()
	authenticate := Authenticate
	defer HookAuthenticate(func(clientConn, crdbConn net.Conn) error {
		err := authenticate(clientConn, crdbConn)
		connSuccess = err == nil
		return err
	})()
	s, addr, done := newSecureProxyServer(
		t, ctx, &ProxyOptions{RoutingRule: sql.ServingSQLAddr(), SkipVerify: true},
	)
	defer done()

	url := fmt.Sprintf("postgres://bob:builder@%s/dim-dog-28.defaultdb?sslmode=require", addr)
	conn, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	require.Equal(t, int64(1), s.Metrics.CurConnCount.Value())
	defer func() {
		incomingConn, ok := proxyIncomingConn.Load().(*Conn)
		require.True(t, ok)
		require.NoError(t, incomingConn.Close())
		<-incomingConn.Done() // should immediately proceed

		require.True(t, connSuccess)
		require.Equal(t, int64(1), s.Metrics.SuccessfulConnCount.Count())
		require.Equal(t, int64(0), s.Metrics.AuthFailedCount.Count())
	}()

	require.NoError(t, runTestQuery(ctx, conn))
}

func TestProxyModifyRequestParams(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sql, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	defer sql.Stopper().Stop(ctx)
	defer sql.(*server.TestServer).PGServer().TestingSetTrustClientProvidedRemoteAddr(true)()

	outgoingTLSConfig, err := sql.RPCContext().GetClientTLSConfig()
	require.NoError(t, err)
	outgoingTLSConfig.InsecureSkipVerify = true

	backendDial := BackendDial
	defer HookBackendDial(func(msg *pgproto3.StartupMessage, outgoingAddress string, tlsConfig *tls.Config) (net.Conn, error) {
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

		return backendDial(msg, sql.ServingSQLAddr(), outgoingTLSConfig)
	})()

	s, proxyAddr, done := newSecureProxyServer(t, ctx, &ProxyOptions{})
	defer done()

	u := fmt.Sprintf("postgres://bogususer@%s/?sslmode=require&authToken=abc123&options=--cluster=dim-dog-28", proxyAddr)
	conn, err := pgx.Connect(ctx, u)
	require.NoError(t, err)
	require.Equal(t, int64(1), s.Metrics.CurConnCount.Value())
	defer func() {
		require.NoError(t, conn.Close(ctx))
	}()

	require.NoError(t, runTestQuery(ctx, conn))
}

func TestInsecureProxy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sql, db, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	defer sql.Stopper().Stop(ctx)
	defer sql.(*server.TestServer).PGServer().TestingSetTrustClientProvidedRemoteAddr(true)()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE USER bob WITH PASSWORD 'builder'`)

	s, addr, cleanup := newProxyServer(
		t, ctx, &ProxyOptions{RoutingRule: sql.ServingSQLAddr(), SkipVerify: true},
	)
	defer cleanup()

	u := fmt.Sprintf("postgres://bob:wrong@%s?sslmode=disable&options=--cluster=dim-dog-28", addr)
	_, err := pgx.Connect(ctx, u)
	require.Error(t, err)
	require.Regexp(t, "ERROR: password authentication failed for user bob", err)

	u = fmt.Sprintf("postgres://bob:builder@%s/?sslmode=disable&options=--cluster=dim-dog-28", addr)
	conn, err := pgx.Connect(ctx, u)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, conn.Close(ctx))
		require.Equal(t, int64(1), s.Metrics.AuthFailedCount.Count())
		require.Equal(t, int64(1), s.Metrics.SuccessfulConnCount.Count())
	}()

	require.NoError(t, runTestQuery(ctx, conn))
}

func TestInsecureDoubleProxy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sql, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	defer sql.Stopper().Stop(ctx)
	defer sql.(*server.TestServer).PGServer().TestingSetTrustClientProvidedRemoteAddr(true)()

	// Test multiple proxies:  proxyB -> proxyA -> tc
	_, proxyA, cleanupA := newProxyServer(t, ctx,
		&ProxyOptions{RoutingRule: sql.ServingSQLAddr(), Insecure: true},
	)
	defer cleanupA()
	_, proxyB, cleanupB := newProxyServer(t, ctx,
		&ProxyOptions{RoutingRule: proxyA, Insecure: true},
	)
	defer cleanupB()

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer HookFrontendAdmit(func(conn net.Conn, incomingTLSConfig *tls.Config) (net.Conn, *pgproto3.StartupMessage, error) {
		return conn, nil, errors.New(FrontendError)
	})()

	_, addr, cleanup := newProxyServer(t, ctx, &ProxyOptions{})
	defer cleanup()

	u := fmt.Sprintf("postgres://bob:builder@%s/?sslmode=disable&options=--cluster=dim-dog-28", addr)

	_, err := pgx.Connect(ctx, u)
	require.Error(t, err)
	// Generic message here as the Frontend's error is not CodeError and
	// by default we don't pass back error's text. The startup message doesn't get
	// processed in this case.
	require.Regexp(t, "connection reset by peer", err)
}

func TestErroneousBackend(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, addr, cleanup := newProxyServer(t, ctx, &ProxyOptions{})
	defer cleanup()

	defer HookBackendDial(
		func(msg *pgproto3.StartupMessage, outgoingAddress string, tlsConfig *tls.Config) (net.Conn, error) {
			return nil, errors.New(BackendError)
		})()

	u := fmt.Sprintf("postgres://bob:builder@%s/?sslmode=disable&options=--cluster=dim-dog-28", addr)

	_, err := pgx.Connect(ctx, u)
	require.Error(t, err)
	// Generic message here as the Backend's error is not CodeError and
	// by default we don't pass back error's text. The startup message has already
	// been processed.
	require.Regexp(t, "failed to receive message", err)
}

func TestProxyRefuseConn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer HookBackendDial(func(_ *pgproto3.StartupMessage, _ string, _ *tls.Config) (net.Conn, error) {
		return nil, NewErrorf(CodeProxyRefusedConnection, "too many attempts")
	})()

	ac := makeAssertCtx()
	sendErrToClient := SendErrToClient
	defer HookSendErrToClient(func(conn net.Conn, err error) {
		sendErrToClient(conn, err)
		if codeErr, ok := err.(*CodeError); ok {
			ac.onSendErrToClient(codeErr.Code)
		}
	})()

	s, addr, done := newSecureProxyServer(t, ctx, &ProxyOptions{})
	defer done()

	ac.assertConnectErr(
		t, ctx, fmt.Sprintf("postgres://root:admin@%s/", addr),
		"?sslmode=require&options=--cluster=dim-dog-28",
		CodeProxyRefusedConnection, "too many attempts",
	)
	require.Equal(t, int64(1), s.Metrics.RefusedConnCount.Count())
	require.Equal(t, int64(0), s.Metrics.SuccessfulConnCount.Count())
	require.Equal(t, int64(0), s.Metrics.AuthFailedCount.Count())
}

func TestProxyKeepAlive(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sql, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	defer sql.Stopper().Stop(ctx)
	defer sql.(*server.TestServer).PGServer().TestingSetTrustClientProvidedRemoteAddr(true)()

	denyList, err := ioutil.TempFile("", "*_denylist.yml")
	require.NoError(t, err)

	outgoingTLSConfig, err := sql.RPCContext().GetClientTLSConfig()
	require.NoError(t, err)
	outgoingTLSConfig.InsecureSkipVerify = true

	backendDial := BackendDial
	defer HookBackendDial(func(msg *pgproto3.StartupMessage, outgoingAddress string, tlsConfig *tls.Config) (net.Conn, error) {
		time.AfterFunc(100*time.Millisecond, func() {
			_, err := denyList.WriteString("127.0.0.1: test-denied")
			require.NoError(t, err)
		})
		return backendDial(msg, sql.ServingSQLAddr(), outgoingTLSConfig)
	})()

	s, addr, done := newSecureProxyServer(t, ctx, &ProxyOptions{Denylist: denyList.Name()})
	defer done()
	defer func() { _ = os.Remove(denyList.Name()) }()

	url := fmt.Sprintf("postgres://root:admin@%s/defaultdb_29?sslmode=require&options=--cluster=dim-dog-28", addr)
	conn, err := pgx.Connect(context.Background(), url)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close(ctx))
		require.Equal(t, int64(1), s.Metrics.ExpiredClientConnCount.Count())
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sql, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
	defer sql.Stopper().Stop(ctx)
	defer sql.(*server.TestServer).PGServer().TestingSetTrustClientProvidedRemoteAddr(true)()

	outgoingTLSConfig, err := sql.RPCContext().GetClientTLSConfig()
	require.NoError(t, err)
	outgoingTLSConfig.InsecureSkipVerify = true

	idleTimeout, _ := time.ParseDuration("0.5s")
	var connSuccess bool
	frontendAdmit := FrontendAdmit
	defer HookFrontendAdmit(func(conn net.Conn, incomingTLSConfig *tls.Config) (net.Conn, *pgproto3.StartupMessage, error) {
		return frontendAdmit(conn, incomingTLSConfig)
	})()
	backendDial := BackendDial
	defer HookBackendDial(func(msg *pgproto3.StartupMessage, outgoingAddress string, tlsConfig *tls.Config) (net.Conn, error) {
		return backendDial(msg, sql.ServingSQLAddr(), outgoingTLSConfig)
	})()
	authenticate := Authenticate
	defer HookAuthenticate(func(clientConn, crdbConn net.Conn) error {
		err := authenticate(clientConn, crdbConn)
		connSuccess = err == nil
		return err
	})()
	s, addr, done := newSecureProxyServer(t, ctx, &ProxyOptions{IdleTimeout: idleTimeout})
	defer done()

	url := fmt.Sprintf("postgres://root:admin@%s/?sslmode=require&options=--cluster=dim-dog-28", addr)
	conn, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	require.Equal(t, int64(1), s.Metrics.CurConnCount.Value())
	defer func() {
		require.NoError(t, conn.Close(ctx))
		require.True(t, connSuccess)
		require.Equal(t, int64(1), s.Metrics.SuccessfulConnCount.Count())
	}()

	var n int
	err = conn.QueryRow(ctx, "SELECT $1::int", 1).Scan(&n)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)

	time.Sleep(idleTimeout * 2)
	err = conn.QueryRow(context.Background(), "SELECT $1::int", 1).Scan(&n)
	require.EqualError(t, err, "FATAL: terminating connection due to idle timeout (SQLSTATE 57P01)")
}
