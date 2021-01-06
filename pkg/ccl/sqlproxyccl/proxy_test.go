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
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

const FrontendError = "Frontend error!"
const BackendError = "Backend error!"

// This options are used to simplify the construction of test proxy handlers for
// the test cases below.
type testHandlerOptions struct {
	IncomingTLSConfig *tls.Config // config used for client -> proxy connection

	// TLS settings to use when connecting to OutgoingAddress.
	BackendTLSConfig *tls.Config

	// The address of the backend to connect to.
	OutgoingAddress string

	// If set, will be called immediately after a new incoming connection
	// is accepted. It can optionally negotiate SSL, provide admittance control or
	// other types of frontend connection filtering.
	FrontendAdmitter func(incoming net.Conn) (net.Conn, *pgproto3.StartupMessage, error)

	// If set, will be used to establish and return connection to the backend.
	// If not set, the old logic will be used.
	// The argument is the startup message received from the frontend. It
	// contains the protocol version and params sent by the client.
	BackendDialer func(msg *pgproto3.StartupMessage) (net.Conn, error)

	// If set, consulted to decorate an error message to be sent to the client.
	// The error passed to this method will contain no internal information.
	OnSendErrToClient func(err error)

	// If set, it will be called with the startup message received.
	InspectStartupMessage func(msg *pgproto3.StartupMessage, conn *Conn)

	// If set, consulted to modify the parameters set by the frontend before
	// forwarding them to the backend during startup.
	ModifyRequestParams func(map[string]string)

	// Called after successfully connecting to OutgoingAddr.
	OnConnectionSuccess func()

	// KeepAliveLoop if provided controls the lifetime of the proxy connection.
	// It will be run in its own goroutine when the connection is successfully
	// opened. Returning from `KeepAliveLoop` will close the proxy connection.
	// Note that non-nil error return values will be forwarded to the user and
	// hence should explain the reason for terminating the connection.
	// Most common use of KeepAliveLoop will be as an infinite loop that
	// periodically checks if the connection should still be kept alive. Hence it
	// may block indefinitely so it's prudent to use the provided context and
	// return on context cancellation.
	// See `TestProxyKeepAlive` for example usage.
	KeepAliveLoop func(context.Context) error
}

func testProxyHandler(opts *testHandlerOptions) func(
	metrics *Metrics, proxyConn *Conn,
) error {
	return func(metrics *Metrics, proxyConn *Conn) error {
		frontendAdmitter := opts.FrontendAdmitter
		if frontendAdmitter == nil {
			// Keep this until all clients are switched to provide FrontendAdmitter
			// at what point we can also drop IncomingTLSConfig
			frontendAdmitter = func(incoming net.Conn) (net.Conn, *pgproto3.StartupMessage, error) {
				return FrontendAdmit(incoming, opts.IncomingTLSConfig)
			}
		}
		conn, msg, err := frontendAdmitter(proxyConn)
		if err != nil {
			if opts.OnSendErrToClient != nil {
				opts.OnSendErrToClient(err)
			}
			SendErrToClient(conn, err)
			return err
		}
		// This currently only happens for CancelRequest type of startup messages
		// that we don't support
		if conn == nil {
			return nil
		}

		defer func() { _ = conn.Close() }()

		if opts.InspectStartupMessage != nil {
			opts.InspectStartupMessage(msg, proxyConn)
		}

		backendDialer := opts.BackendDialer
		if backendDialer == nil {
			// This we need to keep until all the clients are switched to provide BackendDialer.
			// It constructs a backend dialer from the information provided via
			// BackendConfigFromParams function.
			backendDialer = func(msg *pgproto3.StartupMessage) (net.Conn, error) {
				// We should be able to remove this when the all clients switch to
				// backend dialer.
				if opts.ModifyRequestParams != nil {
					opts.ModifyRequestParams(msg.Parameters)
				}

				crdbConn, err := BackendDial(msg, opts.OutgoingAddress, opts.BackendTLSConfig)
				if err != nil {
					return nil, err
				}

				return crdbConn, nil
			}
		}

		crdbConn, err := backendDialer(msg)
		if err != nil {
			UpdateMetricsForError(metrics, err)
			if opts.OnSendErrToClient != nil {
				opts.OnSendErrToClient(err)
			}
			SendErrToClient(conn, err)
			return err
		}

		defer func() { _ = crdbConn.Close() }()

		if err := Authenticate(conn, crdbConn); err != nil {
			UpdateMetricsForError(metrics, err)
			SendErrToClient(conn, err)
			return errors.AssertionFailedf("unrecognized auth failure")
		}

		metrics.SuccessfulConnCount.Inc(1)

		if opts.OnConnectionSuccess != nil {
			opts.OnConnectionSuccess()
		}

		errConnectionCopy := make(chan error, 1)
		errExpired := make(chan error, 1)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if opts.KeepAliveLoop != nil {
			go func() {
				errExpired <- opts.KeepAliveLoop(ctx)
			}()
		}

		go func() {
			err := ConnectionCopy(crdbConn, conn)
			errConnectionCopy <- err
		}()

		select {
		case err := <-errConnectionCopy:
			UpdateMetricsForError(metrics, err)
			SendErrToClient(conn, err)
			return err
		case err := <-errExpired:
			if err != nil {
				// The client connection expired.
				codeErr := NewErrorf(
					CodeExpiredClientConnection, "expired client conn: %v", err,
				)
				UpdateMetricsForError(metrics, codeErr)
				SendErrToClient(conn, codeErr)
				return codeErr
			}
			return nil
		}
	}
}

func setupTestProxyWithCerts(
	t *testing.T, opts *testHandlerOptions,
) (server *Server, addr string, done func()) {
	// Created via:
	const _ = `
openssl genrsa -out testserver.key 2048
openssl req -new -x509 -sha256 -key testserver.key -out testserver.crt \
  -days 3650 -config testserver_config.cnf	
`
	cer, err := tls.LoadX509KeyPair("testserver.crt", "testserver.key")
	require.NoError(t, err)
	opts.FrontendAdmitter = func(incoming net.Conn) (net.Conn, *pgproto3.StartupMessage, error) {
		return FrontendAdmit(
			incoming,
			&tls.Config{
				Certificates: []tls.Certificate{cer},
				ServerName:   "localhost",
			},
		)
	}

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

	server = NewServer(testProxyHandler(opts))

	go func() {
		defer wg.Done()
		_ = server.Serve(ln)
	}()

	return server, ln.Addr().String(), done
}

func testingTenantIDFromDatabaseForAddr(
	addr string, validTenant string,
) func(msg *pgproto3.StartupMessage) (net.Conn, error) {
	return func(msg *pgproto3.StartupMessage) (net.Conn, error) {
		const dbKey = "database"
		p := msg.Parameters
		db, ok := p[dbKey]
		if !ok {
			return nil, NewErrorf(
				CodeParamsRoutingFailed, "need to specify database",
			)
		}
		sl := strings.SplitN(db, "_", 2)
		if len(sl) != 2 {
			return nil, NewErrorf(
				CodeParamsRoutingFailed, "malformed database name",
			)
		}
		db, tenantID := sl[0], sl[1]

		if tenantID != validTenant {
			return nil, NewErrorf(CodeParamsRoutingFailed, "invalid tenantID")
		}

		p[dbKey] = db
		return BackendDial(msg, addr, &tls.Config{
			// NB: this would be false in production.
			InsecureSkipVerify: true,
		})
	}
}

func runTestQuery(conn *pgx.Conn) error {
	var n int
	if err := conn.QueryRow(context.Background(), "SELECT $1::int", 1).Scan(&n); err != nil {
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

func (ac *assertCtx) onSendErrToClient(err error) {
	codeErr := (*CodeError)(nil)
	if errors.As(err, &codeErr) {
		*ac.emittedCode = codeErr.Code
	}
}

func (ac *assertCtx) assertConnectErr(
	t *testing.T, prefix, suffix string, expCode ErrorCode, expErr string,
) {
	t.Helper()
	*ac.emittedCode = -1
	t.Run(suffix, func(t *testing.T) {
		ctx := context.Background()
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

	ac := makeAssertCtx()

	opts := testHandlerOptions{
		BackendDialer: func(msg *pgproto3.StartupMessage) (net.Conn, error) {
			return nil, NewErrorf(CodeParamsRoutingFailed, "boom")
		},
		OnSendErrToClient: ac.onSendErrToClient,
	}
	s, addr, done := setupTestProxyWithCerts(t, &opts)
	defer done()

	longDB := strings.Repeat("x", 70) // 63 is limit
	pgurl := fmt.Sprintf("postgres://unused:unused@%s/%s", addr, longDB)
	ac.assertConnectErr(t, pgurl, "" /* suffix */, CodeParamsRoutingFailed, "boom")
	require.Equal(t, int64(1), s.Metrics.RoutingErrCount.Count())
}

func TestFailedConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// TODO(asubiotto): consider using datadriven for these, especially if the
	// proxy becomes more complex.

	ac := makeAssertCtx()
	opts := testHandlerOptions{
		BackendDialer: testingTenantIDFromDatabaseForAddr(
			"undialable%$!@$", "29",
		),
		OnSendErrToClient: ac.onSendErrToClient,
	}
	s, addr, done := setupTestProxyWithCerts(t, &opts)
	defer done()

	_, p, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	u := fmt.Sprintf("postgres://unused:unused@localhost:%s/", p)
	// Valid connections, but no backend server running.
	for _, sslmode := range []string{"require", "prefer"} {
		ac.assertConnectErr(
			t, u, "defaultdb_29?sslmode="+sslmode,
			CodeBackendDown, "unable to reach backend SQL server",
		)
	}
	ac.assertConnectErr(
		t, u, "defaultdb_29?sslmode=verify-ca&sslrootcert=testserver.crt",
		CodeBackendDown, "unable to reach backend SQL server",
	)
	ac.assertConnectErr(
		t, u, "defaultdb_29?sslmode=verify-full&sslrootcert=testserver.crt",
		CodeBackendDown, "unable to reach backend SQL server",
	)
	require.Equal(t, int64(4), s.Metrics.BackendDownCount.Count())

	// Unencrypted connections bounce.
	for _, sslmode := range []string{"disable", "allow"} {
		ac.assertConnectErr(
			t, u, "defaultdb_29?sslmode="+sslmode,
			CodeUnexpectedInsecureStartupMessage, "server requires encryption",
		)
	}

	// TenantID rejected by test hook.
	ac.assertConnectErr(
		t, u, "defaultdb_28?sslmode=require",
		CodeParamsRoutingFailed, "invalid tenantID",
	)

	// No TenantID.
	ac.assertConnectErr(
		t, u, "defaultdb?sslmode=require",
		CodeParamsRoutingFailed, "malformed database name",
	)
	require.Equal(t, int64(2), s.Metrics.RoutingErrCount.Count())
}

func TestUnexpectedError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up a Server whose proxy handler function always errors with a
	// non-CodeError error.
	ctx := context.Background()
	s := NewServer(func(metrics *Metrics, proxyConn *Conn) error {
		return errors.New("unexpected error")
	})
	const listenAddress = "127.0.0.1:0"
	ln, err := net.Listen("tcp", listenAddress)
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.Serve(ln)
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

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	sqlDB.Exec(t, `CREATE USER bob WITH PASSWORD 'builder'`)

	var connSuccess bool
	opts := testHandlerOptions{
		OnConnectionSuccess: func() { connSuccess = true },
		BackendDialer: func(msg *pgproto3.StartupMessage) (net.Conn, error) {
			return BackendDial(msg, tc.Server(0).ServingSQLAddr(), &tls.Config{InsecureSkipVerify: true})
		},
	}
	s, addr, done := setupTestProxyWithCerts(t, &opts)
	defer done()

	url := fmt.Sprintf("postgres://bob:wrong@%s?sslmode=require", addr)
	_, err := pgx.Connect(context.Background(), url)
	require.True(t, testutils.IsError(err, "ERROR: password authentication failed for user bob"))

	url = fmt.Sprintf("postgres://bob@%s?sslmode=require", addr)
	_, err = pgx.Connect(context.Background(), url)
	require.True(t, testutils.IsError(err, "ERROR: password authentication failed for user bob"))

	url = fmt.Sprintf("postgres://bob:builder@%s?sslmode=require", addr)
	conn, err := pgx.Connect(context.Background(), url)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close(ctx))
		require.True(t, connSuccess)
		require.Equal(t, int64(1), s.Metrics.SuccessfulConnCount.Count())
		require.Equal(t, int64(2), s.Metrics.AuthFailedCount.Count())
	}()

	require.Equal(t, int64(1), s.Metrics.CurConnCount.Value())
	require.NoError(t, runTestQuery(conn))
}

func TestProxyTLSClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// NB: The leaktest call is an important part of this test. We're
	// verifying that no goroutines are leaked, despite calling Close an
	// underlying TCP connection (rather than the TLSConn that wraps it).

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	sqlDB.Exec(t, `CREATE USER bob WITH PASSWORD 'builder'`)

	var proxyIncomingConn atomic.Value // *Conn
	var connSuccess bool
	opts := testHandlerOptions{
		InspectStartupMessage: func(msg *pgproto3.StartupMessage, conn *Conn) {
			proxyIncomingConn.Store(conn)
		},
		OnConnectionSuccess: func() { connSuccess = true },
		BackendDialer: func(msg *pgproto3.StartupMessage) (net.Conn, error) {
			return BackendDial(msg, tc.Server(0).ServingSQLAddr(), &tls.Config{InsecureSkipVerify: true})
		},
	}
	s, addr, done := setupTestProxyWithCerts(t, &opts)
	defer done()

	url := fmt.Sprintf("postgres://bob:builder@%s/defaultdb_29?sslmode=require", addr)
	conn, err := pgx.Connect(context.Background(), url)
	require.NoError(t, err)
	require.Equal(t, int64(1), s.Metrics.CurConnCount.Value())
	defer func() {
		incomingConn := proxyIncomingConn.Load().(*Conn)
		require.NoError(t, incomingConn.Close())
		<-incomingConn.Done() // should immediately proceed

		require.True(t, connSuccess)
		require.Equal(t, int64(1), s.Metrics.SuccessfulConnCount.Count())
		require.Equal(t, int64(0), s.Metrics.AuthFailedCount.Count())
	}()

	require.NoError(t, runTestQuery(conn))
}

func TestProxyModifyRequestParams(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	outgoingTLSConfig, err := tc.Server(0).RPCContext().GetClientTLSConfig()
	require.NoError(t, err)
	outgoingTLSConfig.InsecureSkipVerify = true

	opts := testHandlerOptions{
		BackendDialer: func(msg *pgproto3.StartupMessage) (net.Conn, error) {
			params := msg.Parameters
			require.EqualValues(t, map[string]string{
				"authToken": "abc123",
				"user":      "bogususer",
			}, params)

			// NB: This test will fail unless the user used between the proxy
			// and the backend is changed to a user that actually exists.
			delete(params, "authToken")
			params["user"] = "root"

			return BackendDial(msg, tc.Server(0).ServingSQLAddr(), outgoingTLSConfig)
		},
	}
	s, proxyAddr, done := setupTestProxyWithCerts(t, &opts)
	defer done()

	u := fmt.Sprintf("postgres://bogususer@%s/?sslmode=require&authToken=abc123", proxyAddr)
	conn, err := pgx.Connect(ctx, u)
	require.NoError(t, err)
	require.Equal(t, int64(1), s.Metrics.CurConnCount.Value())
	defer func() {
		require.NoError(t, conn.Close(ctx))
	}()

	require.NoError(t, runTestQuery(conn))
}

func newInsecureProxyServer(
	t *testing.T,
	outgoingAddr string,
	outgoingTLSConfig *tls.Config,
	customOptions ...func(*testHandlerOptions),
) (server *Server, addr string, cleanup func()) {
	op := testHandlerOptions{
		BackendDialer: func(message *pgproto3.StartupMessage) (net.Conn, error) {
			return BackendDial(message, outgoingAddr, outgoingTLSConfig)
		}}
	for _, opt := range customOptions {
		opt(&op)
	}
	s := NewServer(testProxyHandler(&op))
	const listenAddress = "127.0.0.1:0"
	ln, err := net.Listen("tcp", listenAddress)
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.Serve(ln)
	}()
	return s, ln.Addr().String(), func() {
		_ = ln.Close()
		wg.Wait()
	}
}

func TestInsecureProxy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	sqlDB.Exec(t, `CREATE USER bob WITH PASSWORD 'builder'`)

	s, addr, cleanup := newInsecureProxyServer(
		t, tc.Server(0).ServingSQLAddr(), &tls.Config{InsecureSkipVerify: true}, func(op *testHandlerOptions) {} /* custom options */)
	defer cleanup()

	u := fmt.Sprintf("postgres://bob:wrong@%s?sslmode=disable", addr)
	_, err := pgx.Connect(context.Background(), u)
	require.Error(t, err)
	require.True(t, testutils.IsError(err, "ERROR: password authentication failed for user bob"))

	u = fmt.Sprintf("postgres://bob:builder@%s/?sslmode=disable", addr)
	conn, err := pgx.Connect(ctx, u)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, conn.Close(ctx))
		require.Equal(t, int64(1), s.Metrics.AuthFailedCount.Count())
		require.Equal(t, int64(1), s.Metrics.SuccessfulConnCount.Count())
	}()

	require.NoError(t, runTestQuery(conn))
}

func TestInsecureDoubleProxy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{Insecure: true},
	})
	defer tc.Stopper().Stop(ctx)

	// Test multiple proxies:  proxyB -> proxyA -> tc
	_, proxyA, cleanupA := newInsecureProxyServer(t, tc.Server(0).ServingSQLAddr(),
		nil /* tls config */, func(op *testHandlerOptions) {} /* custom server options */)
	defer cleanupA()
	_, proxyB, cleanupB := newInsecureProxyServer(t, proxyA, nil /* tls config */, func(op *testHandlerOptions) {} /* custom server options */)
	defer cleanupB()

	u := fmt.Sprintf("postgres://root:admin@%s/?sslmode=disable", proxyB)
	conn, err := pgx.Connect(ctx, u)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close(ctx))
	}()
	require.NoError(t, runTestQuery(conn))
}

func TestErroneousFrontend(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	_, addr, cleanup := newInsecureProxyServer(
		t, tc.Server(0).ServingSQLAddr(), nil, /* tls config */
		func(op *testHandlerOptions) {
			op.FrontendAdmitter = func(incoming net.Conn) (net.Conn, *pgproto3.StartupMessage, error) {
				return incoming, nil, NewErrorf(CodeClientDisconnected, FrontendError)
			}
		})
	defer cleanup()

	u := fmt.Sprintf("postgres://bob:builder@%s/?sslmode=disable", addr)

	_, err := pgx.Connect(ctx, u)
	require.Error(t, err)
	require.Regexp(t, FrontendError, err)
}

func TestErroneousBackend(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	_, addr, cleanup := newInsecureProxyServer(
		t, tc.Server(0).ServingSQLAddr(), nil, /* tls config */
		func(op *testHandlerOptions) {
			op.BackendDialer = func(message *pgproto3.StartupMessage) (net.Conn, error) {
				return nil, NewErrorf(CodeBackendDisconnected, BackendError)
			}
		})
	defer cleanup()

	u := fmt.Sprintf("postgres://bob:builder@%s/?sslmode=disable", addr)

	_, err := pgx.Connect(ctx, u)
	require.Error(t, err)
	require.Regexp(t, BackendError, err)
}

func TestProxyRefuseConn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	outgoingTLSConfig, err := tc.Server(0).RPCContext().GetClientTLSConfig()
	require.NoError(t, err)
	outgoingTLSConfig.InsecureSkipVerify = true

	ac := makeAssertCtx()
	opts := testHandlerOptions{
		BackendDialer: func(msg *pgproto3.StartupMessage) (net.Conn, error) {
			return nil, NewErrorf(CodeProxyRefusedConnection, "too many attempts")
		},
		OnSendErrToClient: ac.onSendErrToClient,
	}
	s, addr, done := setupTestProxyWithCerts(t, &opts)
	defer done()

	ac.assertConnectErr(
		t, fmt.Sprintf("postgres://root:admin@%s/", addr), "defaultdb_29?sslmode=require",
		CodeProxyRefusedConnection, "too many attempts",
	)
	require.Equal(t, int64(1), s.Metrics.RefusedConnCount.Count())
	require.Equal(t, int64(0), s.Metrics.SuccessfulConnCount.Count())
	require.Equal(t, int64(0), s.Metrics.AuthFailedCount.Count())
}

func TestProxyKeepAlive(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	outgoingTLSConfig, err := tc.Server(0).RPCContext().GetClientTLSConfig()
	require.NoError(t, err)
	outgoingTLSConfig.InsecureSkipVerify = true

	opts := testHandlerOptions{
		KeepAliveLoop: func(ctx context.Context) error {
			t := timeutil.NewTimer()
			t.Reset(100 * time.Millisecond)
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-t.C:
					t.Read = true
					return errors.New("expired")
				}
			}
		},
		BackendDialer: func(msg *pgproto3.StartupMessage) (net.Conn, error) {
			return BackendDial(msg, tc.Server(0).ServingSQLAddr(), outgoingTLSConfig)
		},
	}
	s, addr, done := setupTestProxyWithCerts(t, &opts)
	defer done()

	url := fmt.Sprintf("postgres://root:admin@%s/defaultdb_29?sslmode=require", addr)
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

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	outgoingTLSConfig, err := tc.Server(0).RPCContext().GetClientTLSConfig()
	require.NoError(t, err)
	outgoingTLSConfig.InsecureSkipVerify = true

	idleTimeout, _ := time.ParseDuration("0.5s")
	var connSuccess bool
	opts := testHandlerOptions{
		OnConnectionSuccess: func() { connSuccess = true },
		BackendDialer: func(msg *pgproto3.StartupMessage) (net.Conn, error) {
			conn, err := BackendDial(msg, tc.Server(0).ServingSQLAddr(), outgoingTLSConfig)
			if err != nil {
				return nil, err
			}
			return IdleDisconnectOverlay(conn, idleTimeout), nil
		},
	}
	s, addr, done := setupTestProxyWithCerts(t, &opts)
	defer done()

	url := fmt.Sprintf("postgres://root:admin@%s/defaultdb_29?sslmode=require", addr)
	conn, err := pgx.Connect(context.Background(), url)
	require.NoError(t, err)
	require.Equal(t, int64(1), s.Metrics.CurConnCount.Value())
	defer func() {
		require.NoError(t, conn.Close(ctx))
		require.True(t, connSuccess)
		require.Equal(t, int64(1), s.Metrics.SuccessfulConnCount.Count())
	}()

	var n int
	err = conn.QueryRow(context.Background(), "SELECT $1::int", 1).Scan(&n)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	time.Sleep(idleTimeout * 2)
	err = conn.QueryRow(context.Background(), "SELECT $1::int", 1).Scan(&n)
	require.EqualError(t, err, "FATAL: terminating connection due to idle timeout (SQLSTATE 57P01)")
}
