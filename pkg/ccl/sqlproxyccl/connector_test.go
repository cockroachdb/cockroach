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

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/balancer"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/throttler"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tracker, err := balancer.NewConnTracker(ctx, stopper, nil /* timeSource */)
	require.NoError(t, err)

	t.Run("error during open", func(t *testing.T) {
		f := &forwarder{}
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: make(map[string]string),
			},
			ConnTracker: tracker,
		}
		c.testingKnobs.dialTenantCluster = func(
			ctx context.Context, requester balancer.ConnectionHandle,
		) (net.Conn, error) {
			require.Equal(t, f, requester)
			return nil, errors.New("foo")
		}

		crdbConn, err := c.OpenTenantConnWithToken(ctx, f, token)
		require.EqualError(t, err, "foo")
		require.Nil(t, crdbConn)

		// Ensure that token is deleted.
		str, ok := c.StartupMsg.Parameters[sessionRevivalTokenStartupParam]
		require.False(t, ok)
		require.Equal(t, "", str)
	})

	t.Run("error during auth", func(t *testing.T) {
		f := &forwarder{}
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: make(map[string]string),
			},
			ConnTracker: tracker,
		}
		conn, _ := net.Pipe()
		defer conn.Close()

		var openCalled bool
		c.testingKnobs.dialTenantCluster = func(
			ctx context.Context, requester balancer.ConnectionHandle,
		) (net.Conn, error) {
			require.Equal(t, f, requester)
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

		crdbConn, err := c.OpenTenantConnWithToken(ctx, f, token)
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
		f := &forwarder{}
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: make(map[string]string),
			},
			ConnTracker: tracker,
		}
		conn, _ := net.Pipe()
		defer conn.Close()

		var openCalled bool
		c.testingKnobs.dialTenantCluster = func(
			ctx context.Context, requester balancer.ConnectionHandle,
		) (net.Conn, error) {
			require.Equal(t, f, requester)
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

		crdbConn, err := c.OpenTenantConnWithToken(ctx, f, token)
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
		f := &forwarder{}
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: make(map[string]string),
			},
			ConnTracker: tracker,
			IdleMonitorWrapperFn: func(crdbConn net.Conn) net.Conn {
				wrapperCalled = true
				return crdbConn
			},
		}

		conn, _ := net.Pipe()
		defer conn.Close()

		var openCalled bool
		c.testingKnobs.dialTenantCluster = func(
			ctx context.Context, requester balancer.ConnectionHandle,
		) (net.Conn, error) {
			require.Equal(t, f, requester)
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

		crdbConn, err := c.OpenTenantConnWithToken(ctx, f, token)
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
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tracker, err := balancer.NewConnTracker(ctx, stopper, nil /* timeSource */)
	require.NoError(t, err)

	dummyHook := func(throttler.AttemptStatus) error {
		return nil
	}

	t.Run("error during open", func(t *testing.T) {
		f := &forwarder{}
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: make(map[string]string),
			},
			ConnTracker: tracker,
		}
		c.testingKnobs.dialTenantCluster = func(
			ctx context.Context, requester balancer.ConnectionHandle,
		) (net.Conn, error) {
			require.Equal(t, f, requester)
			return nil, errors.New("foo")
		}

		crdbConn, sentToClient, err := c.OpenTenantConnWithAuth(ctx, f,
			nil /* clientConn */, nil /* throttleHook */)
		require.EqualError(t, err, "foo")
		require.False(t, sentToClient)
		require.Nil(t, crdbConn)
	})

	t.Run("error during auth", func(t *testing.T) {
		conn, _ := net.Pipe()
		defer conn.Close()

		f := &forwarder{}
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: make(map[string]string),
			},
			ConnTracker: tracker,
		}

		var openCalled bool
		c.testingKnobs.dialTenantCluster = func(
			ctx context.Context, requester balancer.ConnectionHandle,
		) (net.Conn, error) {
			require.Equal(t, f, requester)
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

		crdbConn, sentToClient, err := c.OpenTenantConnWithAuth(ctx, f,
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

		f := &forwarder{}
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: map[string]string{
					// Passing in a token should have no effect.
					sessionRevivalTokenStartupParam: "foo",
				},
			},
			ConnTracker: tracker,
		}

		var openCalled bool
		c.testingKnobs.dialTenantCluster = func(
			ctx context.Context, requester balancer.ConnectionHandle,
		) (net.Conn, error) {
			require.Equal(t, f, requester)
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

		crdbConn, sentToClient, err := c.OpenTenantConnWithAuth(ctx, f, clientConn, dummyHook)
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
		f := &forwarder{}
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: map[string]string{
					// Passing in a token should have no effect.
					sessionRevivalTokenStartupParam: "foo",
				},
			},
			ConnTracker: tracker,
			IdleMonitorWrapperFn: func(crdbConn net.Conn) net.Conn {
				wrapperCalled = true
				return crdbConn
			},
		}

		var openCalled bool
		c.testingKnobs.dialTenantCluster = func(
			ctx context.Context, requester balancer.ConnectionHandle,
		) (net.Conn, error) {
			require.Equal(t, f, requester)
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

		crdbConn, sentToClient, err := c.OpenTenantConnWithAuth(ctx, f, clientConn, dummyHook)
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

		conn, err := c.dialTenantCluster(ctx, nil /* requester */)
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

		conn, err := c.dialTenantCluster(ctx, nil /* requester */)
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

		conn, err := c.dialTenantCluster(ctx, nil /* requester */)
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
		c.DirectoryCache = &testTenantDirectoryCache{
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
		c.testingKnobs.dialSQLServer = func(serverAssignment *balancer.ServerAssignment) (net.Conn, error) {
			require.Equal(t, serverAssignment.Addr(), "127.0.0.10:42")
			dialSQLServerCount++
			if dialSQLServerCount == 1 {
				return nil, markAsRetriableConnectorError(errors.New("bar"))
			}
			return crdbConn, nil
		}
		conn, err := c.dialTenantCluster(ctx, nil /* requester */)
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
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	balancer, err := balancer.NewBalancer(
		ctx,
		stopper,
		nil, /* metrics */
		nil, /* directoryCache */
		nil, /* connTracker */
		balancer.NoRebalanceLoop(),
	)
	require.NoError(t, err)

	t.Run("successful", func(t *testing.T) {
		var lookupTenantPodsFnCount int

		c := &connector{
			ClusterName: "my-foo",
			TenantID:    roachpb.MakeTenantID(10),
			Balancer:    balancer,
		}
		c.DirectoryCache = &testTenantDirectoryCache{
			lookupTenantPodsFn: func(
				fnCtx context.Context, tenantID roachpb.TenantID, clusterName string,
			) ([]*tenant.Pod, error) {
				lookupTenantPodsFnCount++
				require.Equal(t, ctx, fnCtx)
				require.Equal(t, c.TenantID, tenantID)
				require.Equal(t, c.ClusterName, clusterName)
				return []*tenant.Pod{
					{Addr: "127.0.0.10:70", State: tenant.DRAINING},
					{Addr: "127.0.0.10:80", State: tenant.RUNNING},
				}, nil
			},
		}

		addr, err := c.lookupAddr(ctx)
		require.NoError(t, err)
		require.Equal(t, "127.0.0.10:80", addr)
		require.Equal(t, 1, lookupTenantPodsFnCount)
	})

	t.Run("load balancing", func(t *testing.T) {
		var mu struct {
			syncutil.Mutex
			pods map[string]float32
		}
		mu.pods = make(map[string]float32)
		addPod := func(addr string, load float32) {
			mu.Lock()
			defer mu.Unlock()
			mu.pods[addr] = load
		}
		removePod := func(addr string) {
			mu.Lock()
			defer mu.Unlock()
			delete(mu.pods, addr)
		}
		c := &connector{
			ClusterName: "my-foo",
			TenantID:    roachpb.MakeTenantID(10),
			Balancer:    balancer,
		}
		c.DirectoryCache = &testTenantDirectoryCache{
			lookupTenantPodsFn: func(
				fnCtx context.Context, tenantID roachpb.TenantID, clusterName string,
			) ([]*tenant.Pod, error) {
				mu.Lock()
				defer mu.Unlock()

				pods := make([]*tenant.Pod, 0, len(mu.pods))
				for addr, load := range mu.pods {
					pods = append(pods, &tenant.Pod{
						Addr:  addr,
						Load:  load,
						State: tenant.RUNNING,
					})
				}
				return pods, nil
			},
		}

		// Create two pods with similar load.
		const (
			addr1 = "127.0.0.10:80"
			addr2 = "127.0.0.20:90"
		)
		addPod(addr1, 0)
		addPod(addr2, 0)

		// lookupAddr should evenly distribute the load.
		responses := map[string]int{}
		for i := 0; i < 100; i++ {
			addr, err := c.lookupAddr(ctx)
			require.NoError(t, err)
			responses[addr]++
		}
		require.InDelta(t, responses[addr1], 50, 25)
		require.InDelta(t, responses[addr2], 50, 25)

		// Adjust load such that the distribution will be a 1/9 split.
		addPod(addr1, 0.1)
		addPod(addr2, 0.9)

		// We should see that the distribution is skewed towards addr1.
		responses = map[string]int{}
		for i := 0; i < 100; i++ {
			addr, err := c.lookupAddr(ctx)
			require.NoError(t, err)
			responses[addr]++
		}
		require.InDelta(t, responses[addr1], 90, 25)
		require.InDelta(t, responses[addr2], 10, 25)

		// Delete the first pod.
		removePod(addr1)

		// Lookup will still work.
		addr, err := c.lookupAddr(ctx)
		require.NoError(t, err)
		require.Equal(t, addr2, addr)
	})

	t.Run("FailedPrecondition error", func(t *testing.T) {
		var lookupTenantPodsFnCount int
		c := &connector{
			ClusterName: "my-foo",
			TenantID:    roachpb.MakeTenantID(10),
			Balancer:    balancer,
		}
		c.DirectoryCache = &testTenantDirectoryCache{
			lookupTenantPodsFn: func(
				fnCtx context.Context, tenantID roachpb.TenantID, clusterName string,
			) ([]*tenant.Pod, error) {
				lookupTenantPodsFnCount++
				require.Equal(t, ctx, fnCtx)
				require.Equal(t, c.TenantID, tenantID)
				require.Equal(t, c.ClusterName, clusterName)
				return nil, status.Errorf(codes.FailedPrecondition, "foo")
			},
		}

		addr, err := c.lookupAddr(ctx)
		require.EqualError(t, err, "codeUnavailable: foo")
		require.Equal(t, "", addr)
		require.Equal(t, 1, lookupTenantPodsFnCount)
	})

	t.Run("NotFound error", func(t *testing.T) {
		var lookupTenantPodsFnCount int
		c := &connector{
			ClusterName: "my-foo",
			TenantID:    roachpb.MakeTenantID(10),
			Balancer:    balancer,
		}
		c.DirectoryCache = &testTenantDirectoryCache{
			lookupTenantPodsFn: func(
				fnCtx context.Context, tenantID roachpb.TenantID, clusterName string,
			) ([]*tenant.Pod, error) {
				lookupTenantPodsFnCount++
				require.Equal(t, ctx, fnCtx)
				require.Equal(t, c.TenantID, tenantID)
				require.Equal(t, c.ClusterName, clusterName)
				return nil, status.Errorf(codes.NotFound, "foo")
			},
		}

		addr, err := c.lookupAddr(ctx)
		require.EqualError(t, err, "codeParamsRoutingFailed: cluster my-foo-10 not found")
		require.Equal(t, "", addr)
		require.Equal(t, 1, lookupTenantPodsFnCount)
	})

	t.Run("retriable error", func(t *testing.T) {
		var lookupTenantPodsFnCount int
		c := &connector{
			ClusterName: "my-foo",
			TenantID:    roachpb.MakeTenantID(10),
			Balancer:    balancer,
		}
		c.DirectoryCache = &testTenantDirectoryCache{
			lookupTenantPodsFn: func(
				fnCtx context.Context, tenantID roachpb.TenantID, clusterName string,
			) ([]*tenant.Pod, error) {
				lookupTenantPodsFnCount++
				require.Equal(t, ctx, fnCtx)
				require.Equal(t, c.TenantID, tenantID)
				require.Equal(t, c.ClusterName, clusterName)
				return nil, errors.New("foo")
			},
		}

		addr, err := c.lookupAddr(ctx)
		require.EqualError(t, err, "foo")
		require.True(t, isRetriableConnectorError(err))
		require.Equal(t, "", addr)
		require.Equal(t, 1, lookupTenantPodsFnCount)
	})
}

func TestConnector_dialSQLServer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tracker, err := balancer.NewConnTracker(ctx, stopper, nil /* timeSource */)
	require.NoError(t, err)

	tenantID := roachpb.MakeTenantID(10)

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

		sa := balancer.NewServerAssignment(tenantID, tracker, nil, "10.11.12.13:80")
		conn, err := c.dialSQLServer(sa)
		require.NoError(t, err)

		wrappedConn, ok := conn.(*onConnectionClose)
		require.True(t, ok)
		require.Equal(t, crdbConn, wrappedConn.Conn)
	})

	t.Run("invalid serverAddr with tlsConfig", func(t *testing.T) {
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{},
			TLSConfig:  &tls.Config{InsecureSkipVerify: true},
		}
		sa := balancer.NewServerAssignment(tenantID, tracker, nil, "!@#$::")
		conn, err := c.dialSQLServer(sa)
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
		sa := balancer.NewServerAssignment(tenantID, tracker, nil, "10.11.12.13:1234")
		conn, err := c.dialSQLServer(sa)
		require.NoError(t, err)
		wrappedConn, ok := conn.(*onConnectionClose)
		require.True(t, ok)
		require.Equal(t, crdbConn, wrappedConn.Conn)
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
		sa := balancer.NewServerAssignment(tenantID, tracker, nil, "127.0.0.1:1234")
		conn, err := c.dialSQLServer(sa)
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
		sa := balancer.NewServerAssignment(tenantID, tracker, nil, "127.0.0.2:4567")
		conn, err := c.dialSQLServer(sa)
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

var _ tenant.DirectoryCache = &testTenantDirectoryCache{}

// testTenantDirectoryCache is a test implementation of the tenant directory
// cache.
type testTenantDirectoryCache struct {
	lookupTenantPodsFn    func(ctx context.Context, tenantID roachpb.TenantID, clusterName string) ([]*tenant.Pod, error)
	trylookupTenantPodsFn func(ctx context.Context, tenantID roachpb.TenantID) ([]*tenant.Pod, error)
	reportFailureFn       func(ctx context.Context, tenantID roachpb.TenantID, addr string) error
}

// LookupTenantPods implements the tenant.DirectoryCache interface.
func (r *testTenantDirectoryCache) LookupTenantPods(
	ctx context.Context, tenantID roachpb.TenantID, clusterName string,
) ([]*tenant.Pod, error) {
	return r.lookupTenantPodsFn(ctx, tenantID, clusterName)
}

// TryLookupTenantPods implements the tenant.DirectoryCache interface.
func (r *testTenantDirectoryCache) TryLookupTenantPods(
	ctx context.Context, tenantID roachpb.TenantID,
) ([]*tenant.Pod, error) {
	return r.trylookupTenantPodsFn(ctx, tenantID)
}

// ReportFailure implements the tenant.DirectoryCache interface.
func (r *testTenantDirectoryCache) ReportFailure(
	ctx context.Context, tenantID roachpb.TenantID, addr string,
) error {
	return r.reportFailureFn(ctx, tenantID, addr)
}
