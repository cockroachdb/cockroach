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
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

func setupTestProxyWithCerts(
	t *testing.T, opts *Options,
) (server *Server, addr string, done func()) {
	// Created via:
	const _ = `
openssl genrsa -out testserver.key 2048
openssl req -new -x509 -sha256 -key testserver.key -out testserver.crt \
  -days 3650 -config testserver_config.cnf	
`
	cer, err := tls.LoadX509KeyPair("testserver.crt", "testserver.key")
	require.NoError(t, err)
	opts.IncomingTLSConfig = &tls.Config{
		Certificates: []tls.Certificate{cer},
		ServerName:   "localhost",
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

	server = NewServer(*opts)

	go func() {
		defer wg.Done()
		_ = server.Serve(ln)
	}()

	return server, ln.Addr().String(), done
}

func testingTenantIDFromDatabaseForAddr(
	addr string, validTenant string,
) func(map[string]string, *Conn) (config *BackendConfig, clientErr error) {
	return func(p map[string]string, _ *Conn) (config *BackendConfig, clientErr error) {
		const dbKey = "database"
		db, ok := p[dbKey]
		if !ok {
			return nil, errors.Newf("need to specify database")
		}
		sl := strings.SplitN(db, "_", 2)
		if len(sl) != 2 {
			return nil, errors.Newf("malformed database name")
		}
		db, tenantID := sl[0], sl[1]

		if tenantID != validTenant {
			return nil, errors.Newf("invalid tenantID")
		}

		p[dbKey] = db
		return &BackendConfig{
			OutgoingAddress: addr,
			TLSConf: &tls.Config{
				// NB: this would be false in production.
				InsecureSkipVerify: true,
			},
		}, nil
	}
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

func (ac *assertCtx) onSendErrToClient(code ErrorCode, msg string) string {
	*ac.emittedCode = code
	return msg
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

	var m map[string]string
	opts := Options{
		BackendConfigFromParams: func(
			mm map[string]string, _ *Conn) (config *BackendConfig, clientErr error) {
			m = mm
			return nil, errors.New("boom")
		},
		OnSendErrToClient: ac.onSendErrToClient,
	}
	s, addr, done := setupTestProxyWithCerts(t, &opts)
	defer done()

	longDB := strings.Repeat("x", 70) // 63 is limit
	pgurl := fmt.Sprintf("postgres://unused:unused@%s/%s", addr, longDB)
	ac.assertConnectErr(t, pgurl, "" /* suffix */, CodeParamsRoutingFailed, "boom")
	require.Equal(t, longDB, m["database"])
	require.Equal(t, int64(1), s.metrics.RoutingErrCount.Count())
}

func TestFailedConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// TODO(asubiotto): consider using datadriven for these, especially if the
	// proxy becomes more complex.

	ac := makeAssertCtx()
	opts := Options{
		BackendConfigFromParams: testingTenantIDFromDatabaseForAddr("undialable%$!@$", "29"),
		OnSendErrToClient:       ac.onSendErrToClient,
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
	require.Equal(t, int64(4), s.metrics.BackendDownCount.Count())

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
	require.Equal(t, int64(2), s.metrics.RoutingErrCount.Count())
}

func TestProxyAgainstSecureCRDB(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	// TODO(tbg): if I use the https (!) port of ./cockroach demo, the
	// connection hangs instead of failing. Why? Probably both ends end up waiting
	// for the other side due to protocol mismatch. Should set deadlines on all
	// the read/write ops to avoid this failure mode.

	outgoingTLSConfig, err := tc.Server(0).RPCContext().GetClientTLSConfig()
	require.NoError(t, err)
	outgoingTLSConfig.InsecureSkipVerify = true

	var connSuccess bool
	opts := Options{
		BackendConfigFromParams: func(params map[string]string, _ *Conn) (*BackendConfig, error) {
			return &BackendConfig{
				OutgoingAddress:     tc.Server(0).ServingSQLAddr(),
				TLSConf:             outgoingTLSConfig,
				OnConnectionSuccess: func() { connSuccess = true },
			}, nil
		},
	}
	s, addr, done := setupTestProxyWithCerts(t, &opts)
	defer done()

	url := fmt.Sprintf("postgres://root:admin@%s/defaultdb_29?sslmode=require", addr)
	conn, err := pgx.Connect(context.Background(), url)
	require.NoError(t, err)
	require.Equal(t, int64(1), s.metrics.CurConnCount.Value())
	defer func() {
		require.NoError(t, conn.Close(ctx))
		require.True(t, connSuccess)
		require.Equal(t, int64(1), s.metrics.SuccessfulConnCount.Count())
	}()

	var n int
	err = conn.QueryRow(context.Background(), "SELECT $1::int", 1).Scan(&n)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
}

func TestProxyTLSClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// NB: The leaktest call is an important part of this test. We're
	// verifying that no goroutines are leaked, despite calling Close an
	// underlying TCP connection (rather than the TLSConn that wraps it).

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	outgoingTLSConfig, err := tc.Server(0).RPCContext().GetClientTLSConfig()
	require.NoError(t, err)
	outgoingTLSConfig.InsecureSkipVerify = true

	var proxyIncomingConn atomic.Value // *Conn
	var connSuccess bool
	opts := Options{
		BackendConfigFromParams: func(params map[string]string, conn *Conn) (*BackendConfig, error) {
			proxyIncomingConn.Store(conn)
			return &BackendConfig{
				OutgoingAddress:     tc.Server(0).ServingSQLAddr(),
				TLSConf:             outgoingTLSConfig,
				OnConnectionSuccess: func() { connSuccess = true },
			}, nil
		},
	}
	s, addr, done := setupTestProxyWithCerts(t, &opts)
	defer done()

	url := fmt.Sprintf("postgres://root:admin@%s/defaultdb_29?sslmode=require", addr)
	conn, err := pgx.Connect(context.Background(), url)
	require.NoError(t, err)
	require.Equal(t, int64(1), s.metrics.CurConnCount.Value())
	defer func() {
		incomingConn := proxyIncomingConn.Load().(*Conn)
		require.NoError(t, incomingConn.Close())
		<-incomingConn.Done() // should immediately proceed

		require.True(t, connSuccess)
		require.Equal(t, int64(1), s.metrics.SuccessfulConnCount.Count())
	}()

	var n int
	err = conn.QueryRow(context.Background(), "SELECT $1::int", 1).Scan(&n)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
}

func TestProxyModifyRequestParams(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	outgoingTLSConfig, err := tc.Server(0).RPCContext().GetClientTLSConfig()
	require.NoError(t, err)
	outgoingTLSConfig.InsecureSkipVerify = true

	opts := Options{
		BackendConfigFromParams: func(params map[string]string, _ *Conn) (*BackendConfig, error) {
			return &BackendConfig{
				OutgoingAddress: tc.Server(0).ServingSQLAddr(),
				TLSConf:         outgoingTLSConfig,
			}, nil
		},
		ModifyRequestParams: func(params map[string]string) {
			require.EqualValues(t, map[string]string{
				"authToken": "abc123",
				"user":      "bogususer",
			}, params)

			// NB: This test will fail unless the user used between the proxy
			// and the backend is changed to a user that actually exists.
			delete(params, "authToken")
			params["user"] = "root"
		},
	}
	s, proxyAddr, done := setupTestProxyWithCerts(t, &opts)
	defer done()

	u := fmt.Sprintf("postgres://bogususer@%s/?sslmode=require&authToken=abc123", proxyAddr)
	conn, err := pgx.Connect(ctx, u)
	require.NoError(t, err)
	require.Equal(t, int64(1), s.metrics.CurConnCount.Value())
	defer func() {
		require.NoError(t, conn.Close(ctx))
	}()

	var n int
	err = conn.QueryRow(ctx, "SELECT $1::int", 1).Scan(&n)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
}

func newInsecureProxyServer(
	t *testing.T, outgoingAddr string, outgoingTLSConfig *tls.Config,
) (addr string, cleanup func()) {
	s := NewServer(Options{
		BackendConfigFromParams: func(params map[string]string, _ *Conn) (*BackendConfig, error) {
			return &BackendConfig{
				OutgoingAddress: outgoingAddr,
				TLSConf:         outgoingTLSConfig,
			}, nil
		},
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
	return ln.Addr().String(), func() {
		_ = ln.Close()
		wg.Wait()
	}
}

func TestInsecureProxy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	outgoingTLSConfig, err := tc.Server(0).RPCContext().GetClientTLSConfig()
	require.NoError(t, err)
	outgoingTLSConfig.InsecureSkipVerify = true

	addr, cleanup := newInsecureProxyServer(t, tc.Server(0).ServingSQLAddr(), outgoingTLSConfig)
	defer cleanup()

	u := fmt.Sprintf("postgres://root:admin@%s/?sslmode=disable", addr)
	conn, err := pgx.Connect(ctx, u)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close(ctx))
	}()

	var n int
	err = conn.QueryRow(ctx, "SELECT $1::int", 1).Scan(&n)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
}

func TestInsecureDoubleProxy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{Insecure: true},
	})
	defer tc.Stopper().Stop(ctx)

	// Test multiple proxies:  proxyB -> proxyA -> tc
	proxyA, cleanupA := newInsecureProxyServer(t, tc.Server(0).ServingSQLAddr(), nil /* tls config */)
	defer cleanupA()
	proxyB, cleanupB := newInsecureProxyServer(t, proxyA, nil /* tls config */)
	defer cleanupB()

	u := fmt.Sprintf("postgres://root:admin@%s/?sslmode=disable", proxyB)
	conn, err := pgx.Connect(ctx, u)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close(ctx))
	}()

	var n int
	err = conn.QueryRow(ctx, "SELECT $1::int", 1).Scan(&n)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
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
	opts := Options{
		BackendConfigFromParams: func(params map[string]string, _ *Conn) (*BackendConfig, error) {
			return &BackendConfig{
				OutgoingAddress: tc.Server(0).ServingSQLAddr(),
				TLSConf:         outgoingTLSConfig,
			}, NewErrorf(CodeProxyRefusedConnection, "too many attempts")
		},
		OnSendErrToClient: ac.onSendErrToClient,
	}
	s, addr, done := setupTestProxyWithCerts(t, &opts)
	defer done()

	ac.assertConnectErr(
		t, fmt.Sprintf("postgres://root:admin@%s/", addr), "defaultdb_29?sslmode=require",
		CodeProxyRefusedConnection, "too many attempts",
	)
	require.Equal(t, int64(1), s.metrics.RefusedConnCount.Count())
	require.Equal(t, int64(0), s.metrics.SuccessfulConnCount.Count())
}

func TestProxyKeepAlive(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	outgoingTLSConfig, err := tc.Server(0).RPCContext().GetClientTLSConfig()
	require.NoError(t, err)
	outgoingTLSConfig.InsecureSkipVerify = true

	opts := Options{
		BackendConfigFromParams: func(params map[string]string, _ *Conn) (*BackendConfig, error) {
			return &BackendConfig{
				OutgoingAddress: tc.Server(0).ServingSQLAddr(),
				TLSConf:         outgoingTLSConfig,
				// Don't let connections last more than 100ms.
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
			}, nil
		},
	}
	s, addr, done := setupTestProxyWithCerts(t, &opts)
	defer done()

	url := fmt.Sprintf("postgres://root:admin@%s/defaultdb_29?sslmode=require", addr)
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
