// Copyright 2022 The Cockroach Authors.
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
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/throttler"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConnector_OpenTenantConnWithToken(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const token = "foobarbaz"
	ctx := context.Background()

	t.Run("error during open", func(t *testing.T) {
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: make(map[string]string),
			},
		}
		c.testingKnobs.dialTenantCluster = func(ctx context.Context) (net.Conn, error) {
			return nil, errors.New("foo")
		}

		crdbConn, err := c.OpenTenantConnWithToken(ctx, token)
		require.EqualError(t, err, "foo")
		require.Nil(t, crdbConn)

		// Ensure that token is deleted.
		str, ok := c.StartupMsg.Parameters[sessionRevivalTokenStartupParam]
		require.False(t, ok)
		require.Equal(t, "", str)
	})

	t.Run("error during auth", func(t *testing.T) {
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: make(map[string]string),
			},
		}
		conn, _ := net.Pipe()
		defer conn.Close()

		var openCalled bool
		c.testingKnobs.dialTenantCluster = func(ctx context.Context) (net.Conn, error) {
			openCalled = true

			// Validate that token is set.
			str, ok := c.StartupMsg.Parameters[sessionRevivalTokenStartupParam]
			require.True(t, ok)
			require.Equal(t, token, str)

			return conn, nil
		}

		defer testutils.TestingHook(
			&readTokenAuthResult,
			func(serverConn net.Conn) error {
				return errors.New("bar")
			},
		)()

		crdbConn, err := c.OpenTenantConnWithToken(ctx, token)
		require.True(t, openCalled)
		require.EqualError(t, err, "bar")
		require.Nil(t, crdbConn)

		// Ensure that token is deleted.
		_, ok := c.StartupMsg.Parameters[sessionRevivalTokenStartupParam]
		require.False(t, ok)

		// Connection should be closed.
		_, err = conn.Write([]byte("foo"))
		require.Regexp(t, "closed pipe", err)
	})

	t.Run("successful", func(t *testing.T) {
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: make(map[string]string),
			},
		}
		conn, _ := net.Pipe()
		defer conn.Close()

		var openCalled bool
		c.testingKnobs.dialTenantCluster = func(ctx context.Context) (net.Conn, error) {
			openCalled = true

			// Validate that token is set.
			str, ok := c.StartupMsg.Parameters[sessionRevivalTokenStartupParam]
			require.True(t, ok)
			require.Equal(t, token, str)

			return conn, nil
		}

		var authCalled bool
		defer testutils.TestingHook(
			&readTokenAuthResult,
			func(serverConn net.Conn) error {
				authCalled = true
				require.Equal(t, conn, serverConn)
				return nil
			},
		)()

		crdbConn, err := c.OpenTenantConnWithToken(ctx, token)
		require.True(t, openCalled)
		require.True(t, authCalled)
		require.NoError(t, err)
		require.Equal(t, conn, crdbConn)

		// Ensure that token is deleted.
		_, ok := c.StartupMsg.Parameters[sessionRevivalTokenStartupParam]
		require.False(t, ok)
	})

	t.Run("idle monitor wrapper is called", func(t *testing.T) {
		var wrapperCalled bool
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: make(map[string]string),
			},
			IdleMonitorWrapperFn: func(crdbConn net.Conn) net.Conn {
				wrapperCalled = true
				return crdbConn
			},
		}

		conn, _ := net.Pipe()
		defer conn.Close()

		var openCalled bool
		c.testingKnobs.dialTenantCluster = func(ctx context.Context) (net.Conn, error) {
			openCalled = true

			// Validate that token is set.
			str, ok := c.StartupMsg.Parameters[sessionRevivalTokenStartupParam]
			require.True(t, ok)
			require.Equal(t, token, str)

			return conn, nil
		}

		var authCalled bool
		defer testutils.TestingHook(
			&readTokenAuthResult,
			func(serverConn net.Conn) error {
				authCalled = true
				require.Equal(t, conn, serverConn)
				return nil
			},
		)()

		crdbConn, err := c.OpenTenantConnWithToken(ctx, token)
		require.True(t, wrapperCalled)
		require.True(t, openCalled)
		require.True(t, authCalled)
		require.NoError(t, err)
		require.Equal(t, conn, crdbConn)

		// Ensure that token is deleted.
		_, ok := c.StartupMsg.Parameters[sessionRevivalTokenStartupParam]
		require.False(t, ok)
	})
}

func TestConnector_OpenTenantConnWithAuth(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	dummyHook := func(throttler.AttemptStatus) error {
		return nil
	}

	t.Run("error during open", func(t *testing.T) {
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: make(map[string]string),
			},
		}
		c.testingKnobs.dialTenantCluster = func(ctx context.Context) (net.Conn, error) {
			return nil, errors.New("foo")
		}

		crdbConn, sentToClient, err := c.OpenTenantConnWithAuth(ctx,
			nil /* clientConn */, nil /* throttleHook */)
		require.EqualError(t, err, "foo")
		require.False(t, sentToClient)
		require.Nil(t, crdbConn)
	})

	t.Run("error during auth", func(t *testing.T) {
		conn, _ := net.Pipe()
		defer conn.Close()

		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: make(map[string]string),
			},
		}

		var openCalled bool
		c.testingKnobs.dialTenantCluster = func(ctx context.Context) (net.Conn, error) {
			openCalled = true
			return conn, nil
		}

		defer testutils.TestingHook(
			&authenticate,
			func(
				clientConn net.Conn,
				crdbConn net.Conn,
				throttleHook func(status throttler.AttemptStatus) error,
			) error {
				return errors.New("bar")
			},
		)()

		crdbConn, sentToClient, err := c.OpenTenantConnWithAuth(ctx,
			nil /* clientConn */, nil /* throttleHook */)
		require.True(t, openCalled)
		require.EqualError(t, err, "bar")
		require.True(t, sentToClient)
		require.Nil(t, crdbConn)

		// Connection should be closed.
		_, err = conn.Write([]byte("foo"))
		require.Regexp(t, "closed pipe", err)
	})

	t.Run("successful", func(t *testing.T) {
		clientConn, _ := net.Pipe()
		defer clientConn.Close()

		serverConn, _ := net.Pipe()
		defer serverConn.Close()

		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: map[string]string{
					// Passing in a token should have no effect.
					sessionRevivalTokenStartupParam: "foo",
				},
			},
		}

		var openCalled bool
		c.testingKnobs.dialTenantCluster = func(ctx context.Context) (net.Conn, error) {
			openCalled = true

			// Validate that token is not set.
			_, ok := c.StartupMsg.Parameters[sessionRevivalTokenStartupParam]
			require.False(t, ok)

			return serverConn, nil
		}

		var authCalled bool
		defer testutils.TestingHook(
			&authenticate,
			func(
				client net.Conn,
				server net.Conn,
				throttleHook func(status throttler.AttemptStatus) error,
			) error {
				authCalled = true
				require.Equal(t, clientConn, client)
				require.NotNil(t, server)
				require.Equal(t, reflect.ValueOf(dummyHook).Pointer(),
					reflect.ValueOf(throttleHook).Pointer())
				return nil
			},
		)()

		crdbConn, sentToClient, err := c.OpenTenantConnWithAuth(ctx, clientConn, dummyHook)
		require.True(t, openCalled)
		require.True(t, authCalled)
		require.NoError(t, err)
		require.False(t, sentToClient)
		require.Equal(t, serverConn, crdbConn)
	})

	t.Run("idle monitor wrapper is called", func(t *testing.T) {
		clientConn, _ := net.Pipe()
		defer clientConn.Close()

		serverConn, _ := net.Pipe()
		defer serverConn.Close()

		var wrapperCalled bool
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: map[string]string{
					// Passing in a token should have no effect.
					sessionRevivalTokenStartupParam: "foo",
				},
			},
			IdleMonitorWrapperFn: func(crdbConn net.Conn) net.Conn {
				wrapperCalled = true
				return crdbConn
			},
		}

		var openCalled bool
		c.testingKnobs.dialTenantCluster = func(ctx context.Context) (net.Conn, error) {
			openCalled = true

			// Validate that token is not set.
			_, ok := c.StartupMsg.Parameters[sessionRevivalTokenStartupParam]
			require.False(t, ok)

			return serverConn, nil
		}

		var authCalled bool
		defer testutils.TestingHook(
			&authenticate,
			func(
				client net.Conn,
				server net.Conn,
				throttleHook func(status throttler.AttemptStatus) error,
			) error {
				authCalled = true
				require.Equal(t, clientConn, client)
				require.NotNil(t, server)
				require.Equal(t, reflect.ValueOf(dummyHook).Pointer(),
					reflect.ValueOf(throttleHook).Pointer())
				return nil
			},
		)()

		crdbConn, sentToClient, err := c.OpenTenantConnWithAuth(ctx, clientConn, dummyHook)
		require.True(t, openCalled)
		require.True(t, wrapperCalled)
		require.True(t, authCalled)
		require.NoError(t, err)
		require.False(t, sentToClient)
		require.Equal(t, serverConn, crdbConn)
	})
}

func TestConnector_dialTenantCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()

	bgCtx := context.Background()

	t.Run("context canceled at boundary", func(t *testing.T) {
		// This is a short test, and is expected to finish within ms.
		ctx, cancel := context.WithTimeout(bgCtx, 2*time.Second)
		defer cancel()

		c := &connector{}
		var lookupAddrCount int
		c.testingKnobs.lookupAddr = func(ctx context.Context) (string, error) {
			lookupAddrCount++
			if lookupAddrCount >= 2 {
				// Cancel context to trigger loop exit on next retry.
				cancel()
			}
			return "", markAsRetriableConnectorError(errors.New("baz"))
		}

		conn, err := c.dialTenantCluster(ctx)
		require.EqualError(t, err, "baz")
		require.True(t, errors.Is(err, context.Canceled))
		require.Nil(t, conn)

		require.Equal(t, 2, lookupAddrCount)
	})

	t.Run("context canceled within loop", func(t *testing.T) {
		// This is a short test, and is expected to finish within ms.
		ctx, cancel := context.WithTimeout(bgCtx, 2*time.Second)
		defer cancel()

		c := &connector{}
		c.testingKnobs.lookupAddr = func(ctx context.Context) (string, error) {
			return "", errors.Wrap(context.Canceled, "foobar")
		}

		conn, err := c.dialTenantCluster(ctx)
		require.EqualError(t, err, "foobar: context canceled")
		require.True(t, errors.Is(err, context.Canceled))
		require.Nil(t, conn)
	})

	t.Run("non-transient error", func(t *testing.T) {
		// This is a short test, and is expected to finish within ms.
		ctx, cancel := context.WithTimeout(bgCtx, 2*time.Second)
		defer cancel()

		c := &connector{}
		c.testingKnobs.lookupAddr = func(ctx context.Context) (string, error) {
			return "", errors.New("baz")
		}

		conn, err := c.dialTenantCluster(ctx)
		require.EqualError(t, err, "baz")
		require.Nil(t, conn)
	})

	t.Run("successful", func(t *testing.T) {
		// This should not take more than 5 seconds.
		ctx, cancel := context.WithTimeout(bgCtx, 5*time.Second)
		defer cancel()

		crdbConn, _ := net.Pipe()
		defer crdbConn.Close()

		var reportFailureFnCount int
		c := &connector{
			TenantID: roachpb.MakeTenantID(42),
		}
		c.Directory = &testTenantResolver{
			reportFailureFn: func(fnCtx context.Context, tenantID roachpb.TenantID, addr string) error {
				reportFailureFnCount++
				require.Equal(t, ctx, fnCtx)
				require.Equal(t, c.TenantID, tenantID)
				require.Equal(t, "127.0.0.10:42", addr)
				return nil
			},
		}

		// We will exercise the following events:
		// 1. retriable error on lookupAddr.
		// 2. retriable error on dialSQLServer.
		var addrLookupFnCount, dialSQLServerCount int
		c.testingKnobs.lookupAddr = func(ctx context.Context) (string, error) {
			addrLookupFnCount++
			if addrLookupFnCount == 1 {
				return "", markAsRetriableConnectorError(errors.New("foo"))
			}
			return "127.0.0.10:42", nil
		}
		c.testingKnobs.dialSQLServer = func(serverAddr string) (net.Conn, error) {
			require.Equal(t, serverAddr, "127.0.0.10:42")
			dialSQLServerCount++
			if dialSQLServerCount == 1 {
				return nil, markAsRetriableConnectorError(errors.New("bar"))
			}
			return crdbConn, nil
		}
		conn, err := c.dialTenantCluster(ctx)
		require.NoError(t, err)
		require.Equal(t, crdbConn, conn)

		// Assert existing calls.
		require.Equal(t, 3, addrLookupFnCount)
		require.Equal(t, 2, dialSQLServerCount)
		require.Equal(t, 1, reportFailureFnCount)
	})
}

func TestConnector_lookupAddr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	t.Run("only routing rule", func(t *testing.T) {
		c := &connector{
			ClusterName: "foo-bar-baz",
			TenantID:    roachpb.MakeTenantID(10),
			RoutingRule: `{{clusterName}}.foo`,
		}

		var resolveTCPAddrCalled bool
		defer testutils.TestingHook(
			&resolveTCPAddr,
			func(network, addr string) (*net.TCPAddr, error) {
				resolveTCPAddrCalled = true
				require.Equal(t, "tcp", network)
				require.Equal(t, "foo-bar-baz-10.foo", addr)
				return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 26257}, nil
			},
		)()

		addr, err := c.lookupAddr(ctx)
		require.True(t, resolveTCPAddrCalled)
		require.NoError(t, err)
		require.Equal(t, "foo-bar-baz-10.foo", addr)
	})

	t.Run("error for only routing rule", func(t *testing.T) {
		c := &connector{
			ClusterName: "cluster-name",
			TenantID:    roachpb.MakeTenantID(10),
			RoutingRule: "foo-bar-baz",
		}

		var resolveTCPAddrCalled bool
		defer testutils.TestingHook(
			&resolveTCPAddr,
			func(network, addr string) (*net.TCPAddr, error) {
				resolveTCPAddrCalled = true
				require.Equal(t, "tcp", network)
				require.Equal(t, "foo-bar-baz", addr)
				return nil, errors.New("foo")
			},
		)()

		addr, err := c.lookupAddr(ctx)
		require.True(t, resolveTCPAddrCalled)
		require.EqualError(t, err, "codeParamsRoutingFailed: cluster cluster-name-10 not found")
		require.Equal(t, "", addr)
	})

	t.Run("directory", func(t *testing.T) {
		var ensureTenantAddrFnCount int
		c := &connector{
			ClusterName: "my-foo",
			TenantID:    roachpb.MakeTenantID(10),
		}
		c.Directory = &testTenantResolver{
			ensureTenantAddrFn: func(fnCtx context.Context, tenantID roachpb.TenantID, clusterName string) (string, error) {
				ensureTenantAddrFnCount++
				require.Equal(t, ctx, fnCtx)
				require.Equal(t, c.TenantID, tenantID)
				require.Equal(t, c.ClusterName, clusterName)
				return "127.0.0.10:80", nil
			},
		}

		addr, err := c.lookupAddr(ctx)
		require.NoError(t, err)
		require.Equal(t, "127.0.0.10:80", addr)
		require.Equal(t, 1, ensureTenantAddrFnCount)
	})

	t.Run("routing rule fallback with directory", func(t *testing.T) {
		var ensureTenantAddrFnCount int
		c := &connector{
			ClusterName: "my-foo",
			TenantID:    roachpb.MakeTenantID(10),
			RoutingRule: "foo.bar",
		}
		c.Directory = &testTenantResolver{
			ensureTenantAddrFn: func(fnCtx context.Context, tenantID roachpb.TenantID, clusterName string) (string, error) {
				ensureTenantAddrFnCount++
				require.Equal(t, ctx, fnCtx)
				require.Equal(t, c.TenantID, tenantID)
				require.Equal(t, c.ClusterName, clusterName)
				return "", status.Errorf(codes.NotFound, "foo")
			},
		}

		var resolveTCPAddrCalled bool
		defer testutils.TestingHook(
			&resolveTCPAddr,
			func(network, addr string) (*net.TCPAddr, error) {
				resolveTCPAddrCalled = true
				require.Equal(t, "tcp", network)
				require.Equal(t, "foo.bar", addr)
				return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 26257}, nil
			},
		)()

		addr, err := c.lookupAddr(ctx)
		require.True(t, resolveTCPAddrCalled)
		require.NoError(t, err)
		require.Equal(t, "foo.bar", addr)
		require.Equal(t, 1, ensureTenantAddrFnCount)
	})

	t.Run("directory with retriable error", func(t *testing.T) {
		var ensureTenantAddrFnCount int
		c := &connector{
			ClusterName: "my-foo",
			TenantID:    roachpb.MakeTenantID(10),
			RoutingRule: "foo.bar",
		}
		c.Directory = &testTenantResolver{
			ensureTenantAddrFn: func(fnCtx context.Context, tenantID roachpb.TenantID, clusterName string) (string, error) {
				ensureTenantAddrFnCount++
				require.Equal(t, ctx, fnCtx)
				require.Equal(t, c.TenantID, tenantID)
				require.Equal(t, c.ClusterName, clusterName)
				return "", errors.New("foo")
			},
		}

		addr, err := c.lookupAddr(ctx)
		require.EqualError(t, err, "foo")
		require.Equal(t, "", addr)
		require.Equal(t, 1, ensureTenantAddrFnCount)
	})
}

func TestConnector_dialSQLServer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("with tlsConfig", func(t *testing.T) {
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{},
			TLSConfig:  &tls.Config{InsecureSkipVerify: true},
		}
		crdbConn, _ := net.Pipe()
		defer crdbConn.Close()

		defer testutils.TestingHook(&BackendDial,
			func(msg *pgproto3.StartupMessage, serverAddress string,
				tlsConfig *tls.Config) (net.Conn, error) {
				require.Equal(t, c.StartupMsg, msg)
				require.Equal(t, "10.11.12.13:80", serverAddress)
				require.Equal(t, "10.11.12.13", tlsConfig.ServerName)
				return crdbConn, nil
			},
		)()
		conn, err := c.dialSQLServer("10.11.12.13:80")
		require.NoError(t, err)
		require.Equal(t, crdbConn, conn)
	})

	t.Run("invalid serverAddr with tlsConfig", func(t *testing.T) {
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{},
			TLSConfig:  &tls.Config{InsecureSkipVerify: true},
		}
		conn, err := c.dialSQLServer("!@#$::")
		require.Error(t, err)
		require.Regexp(t, "invalid address format", err)
		require.False(t, isRetriableConnectorError(err))
		require.Nil(t, conn)
	})

	t.Run("without tlsConfig", func(t *testing.T) {
		c := &connector{StartupMsg: &pgproto3.StartupMessage{}}
		crdbConn, _ := net.Pipe()
		defer crdbConn.Close()

		defer testutils.TestingHook(&BackendDial,
			func(msg *pgproto3.StartupMessage, serverAddress string,
				tlsConfig *tls.Config) (net.Conn, error) {
				require.Equal(t, c.StartupMsg, msg)
				require.Equal(t, "10.11.12.13:1234", serverAddress)
				require.Nil(t, tlsConfig)
				return crdbConn, nil
			},
		)()
		conn, err := c.dialSQLServer("10.11.12.13:1234")
		require.NoError(t, err)
		require.Equal(t, crdbConn, conn)
	})

	t.Run("failed to dial with non-transient error", func(t *testing.T) {
		c := &connector{StartupMsg: &pgproto3.StartupMessage{}}
		defer testutils.TestingHook(&BackendDial,
			func(msg *pgproto3.StartupMessage, serverAddress string,
				tlsConfig *tls.Config) (net.Conn, error) {
				require.Equal(t, c.StartupMsg, msg)
				require.Equal(t, "127.0.0.1:1234", serverAddress)
				require.Nil(t, tlsConfig)
				return nil, errors.New("foo")
			},
		)()
		conn, err := c.dialSQLServer("127.0.0.1:1234")
		require.EqualError(t, err, "foo")
		require.False(t, isRetriableConnectorError(err))
		require.Nil(t, conn)
	})

	t.Run("failed to dial with transient error", func(t *testing.T) {
		c := &connector{StartupMsg: &pgproto3.StartupMessage{}}
		defer testutils.TestingHook(&BackendDial,
			func(msg *pgproto3.StartupMessage, serverAddress string,
				tlsConfig *tls.Config) (net.Conn, error) {
				require.Equal(t, c.StartupMsg, msg)
				require.Equal(t, "127.0.0.2:4567", serverAddress)
				require.Nil(t, tlsConfig)
				return nil, newErrorf(codeBackendDown, "bar")
			},
		)()
		conn, err := c.dialSQLServer("127.0.0.2:4567")
		require.EqualError(t, err, "codeBackendDown: bar")
		require.True(t, isRetriableConnectorError(err))
		require.Nil(t, conn)
	})
}

func TestRetriableConnectorError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	err := errors.New("foobar")
	require.False(t, isRetriableConnectorError(err))
	err = markAsRetriableConnectorError(err)
	require.True(t, isRetriableConnectorError(err))
	require.True(t, errors.Is(err, errRetryConnectorSentinel))
}

var _ TenantResolver = &testTenantResolver{}

// testTenantResolver is a test implementation of the tenant resolver.
type testTenantResolver struct {
	ensureTenantAddrFn  func(ctx context.Context, tenantID roachpb.TenantID, clusterName string) (string, error)
	lookupTenantAddrsFn func(ctx context.Context, tenantID roachpb.TenantID) ([]string, error)
	reportFailureFn     func(ctx context.Context, tenantID roachpb.TenantID, addr string) error
}

// EnsureTenantAddr implements the TenantResolver interface.
func (r *testTenantResolver) EnsureTenantAddr(
	ctx context.Context, tenantID roachpb.TenantID, clusterName string,
) (string, error) {
	return r.ensureTenantAddrFn(ctx, tenantID, clusterName)
}

// LookupTenantAddrs implements the TenantResolver interface.
func (r *testTenantResolver) LookupTenantAddrs(
	ctx context.Context, tenantID roachpb.TenantID,
) ([]string, error) {
	return r.lookupTenantAddrsFn(ctx, tenantID)
}

// ReportFailure implements the TenantResolver interface.
func (r *testTenantResolver) ReportFailure(
	ctx context.Context, tenantID roachpb.TenantID, addr string,
) error {
	return r.reportFailureFn(ctx, tenantID, addr)
}
