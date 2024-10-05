// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlproxyccl

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/balancer"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/throttler"
	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
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
	testutilsccl.ServerlessOnly(t)

	const token = "foobarbaz"
	ctx := context.Background()

	t.Run("error during open", func(t *testing.T) {
		f := &forwarder{}
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: make(map[string]string),
			},
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
			func(serverConn net.Conn) (*pgproto3.BackendKeyData, error) {
				return nil, errors.New("bar")
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
			CancelInfo: makeCancelInfo(
				&net.TCPAddr{IP: net.IP{4, 5, 6, 7}},
				&net.TCPAddr{IP: net.IP{11, 22, 33, 44}},
			),
		}
		pipeConn, _ := net.Pipe()
		defer pipeConn.Close()
		conn := &fakeTCPConn{
			Conn:       pipeConn,
			remoteAddr: &net.TCPAddr{IP: net.IP{1, 2, 3, 4}},
			localAddr:  &net.TCPAddr{IP: net.IP{4, 5, 6, 7}},
		}

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
		crdbBackendKeyData := &pgproto3.BackendKeyData{
			ProcessID: 4,
			SecretKey: 5,
		}
		defer testutils.TestingHook(
			&readTokenAuthResult,
			func(serverConn net.Conn) (*pgproto3.BackendKeyData, error) {
				authCalled = true
				require.Equal(t, conn, serverConn)
				return crdbBackendKeyData, nil
			},
		)()

		crdbConn, err := c.OpenTenantConnWithToken(ctx, f, token)
		require.True(t, openCalled)
		require.True(t, authCalled)
		require.NoError(t, err)
		require.Equal(t, conn, crdbConn)
		require.Equal(t, crdbBackendKeyData, c.CancelInfo.mu.origBackendKeyData)

		// Ensure that token is deleted.
		_, ok := c.StartupMsg.Parameters[sessionRevivalTokenStartupParam]
		require.False(t, ok)
	})
}

func TestConnector_OpenTenantConnWithAuth(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	ctx := context.Background()
	dummyHook := func(throttler.AttemptStatus) error {
		return nil
	}

	t.Run("error during open", func(t *testing.T) {
		f := &forwarder{}
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: make(map[string]string),
			},
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
			CancelInfo: makeCancelInfo(
				&net.TCPAddr{IP: net.IP{4, 5, 6, 7}},
				&net.TCPAddr{IP: net.IP{11, 22, 33, 44}},
			),
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
				proxyBackendKeyData *pgproto3.BackendKeyData,
				throttleHook func(status throttler.AttemptStatus) error,
			) (*pgproto3.BackendKeyData, error) {
				return nil, errors.New("bar")
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
		clientPipeConn, _ := net.Pipe()
		defer clientPipeConn.Close()
		clientConn := &fakeTCPConn{
			Conn:       clientPipeConn,
			remoteAddr: &net.TCPAddr{IP: net.IP{11, 22, 33, 44}},
			localAddr:  &net.TCPAddr{IP: net.IP{4, 5, 6, 7}},
		}

		serverPipeConn, _ := net.Pipe()
		defer serverPipeConn.Close()
		serverConn := &fakeTCPConn{
			Conn:       serverPipeConn,
			remoteAddr: &net.TCPAddr{IP: net.IP{1, 2, 3, 4}},
			localAddr:  &net.TCPAddr{IP: net.IP{4, 5, 6, 7}},
		}

		f := &forwarder{}
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: map[string]string{
					// Passing in a token should have no effect.
					sessionRevivalTokenStartupParam: "foo",
				},
			},
			CancelInfo: makeCancelInfo(
				&net.TCPAddr{IP: net.IP{4, 5, 6, 7}},
				&net.TCPAddr{IP: net.IP{11, 22, 33, 44}},
			),
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
		crdbBackendKeyData := &pgproto3.BackendKeyData{
			ProcessID: 4,
			SecretKey: 5,
		}
		defer testutils.TestingHook(
			&authenticate,
			func(
				client net.Conn,
				server net.Conn,
				proxyBackendKeyData *pgproto3.BackendKeyData,
				throttleHook func(status throttler.AttemptStatus) error,
			) (*pgproto3.BackendKeyData, error) {
				authCalled = true
				require.Equal(t, clientConn, client)
				require.NotNil(t, server)
				require.Equal(t, reflect.ValueOf(dummyHook).Pointer(),
					reflect.ValueOf(throttleHook).Pointer())
				require.Equal(t, proxyBackendKeyData, c.CancelInfo.proxyBackendKeyData)
				return crdbBackendKeyData, nil
			},
		)()

		crdbConn, sentToClient, err := c.OpenTenantConnWithAuth(ctx, f, clientConn, dummyHook)
		require.True(t, openCalled)
		require.True(t, authCalled)
		require.NoError(t, err)
		require.False(t, sentToClient)
		require.Equal(t, serverConn, crdbConn)
		require.Equal(t, crdbBackendKeyData, c.CancelInfo.mu.origBackendKeyData)
	})
}

func TestConnector_dialTenantCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

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

	t.Run("context canceled after dial fails", func(t *testing.T) {
		// This is a short test, and is expected to finish within ms.
		ctx, cancel := context.WithTimeout(bgCtx, 2*time.Second)
		defer cancel()

		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)

		c := &connector{
			TenantID: roachpb.MustMakeTenantID(42),
			DialTenantLatency: metric.NewHistogram(metric.HistogramOptions{
				Mode:         metric.HistogramModePrometheus,
				Metadata:     metaDialTenantLatency,
				Duration:     time.Millisecond,
				BucketConfig: metric.IOLatencyBuckets,
			}),
			DialTenantRetries: metric.NewCounter(metaDialTenantRetries),
		}
		dc := &testTenantDirectoryCache{}
		c.DirectoryCache = dc
		b, err := balancer.NewBalancer(
			ctx,
			stopper,
			balancer.NewMetrics(),
			c.DirectoryCache,
			balancer.NoRebalanceLoop(),
		)
		require.NoError(t, err)
		c.Balancer = b

		var dialSQLServerCount int
		c.testingKnobs.lookupAddr = func(ctx context.Context) (string, error) {
			return "127.0.0.10:42", nil
		}
		c.testingKnobs.dialSQLServer = func(serverAssignment *balancer.ServerAssignment) (net.Conn, error) {
			require.Equal(t, serverAssignment.Addr(), "127.0.0.10:42")
			dialSQLServerCount++

			// Cancel context to trigger loop exit on next retry.
			cancel()
			return nil, markAsRetriableConnectorError(errors.New("bar"))
		}

		var reportFailureFnCount int

		// Invoke dial tenant with a success to ReportFailure.
		// ---------------------------------------------------
		dc.reportFailureFn = func(fnCtx context.Context, tenantID roachpb.TenantID, addr string) error {
			reportFailureFnCount++
			require.Equal(t, ctx, fnCtx)
			require.Equal(t, c.TenantID, tenantID)
			require.Equal(t, "127.0.0.10:42", addr)
			return nil
		}
		conn, err := c.dialTenantCluster(ctx, nil /* requester */)
		require.EqualError(t, err, "bar")
		require.True(t, errors.Is(err, context.Canceled))
		require.Nil(t, conn)

		// Assert existing calls.
		require.Equal(t, 1, dialSQLServerCount)
		require.Equal(t, 1, reportFailureFnCount)
		count, _ := c.DialTenantLatency.CumulativeSnapshot().Total()
		require.Equal(t, count, int64(1))
		require.Equal(t, c.DialTenantRetries.Count(), int64(0))

		// Invoke dial tenant with a failure to ReportFailure. Final error
		// should include the secondary failure.
		// ---------------------------------------------------------------
		dc.reportFailureFn = func(fnCtx context.Context, tenantID roachpb.TenantID, addr string) error {
			reportFailureFnCount++
			require.Equal(t, ctx, fnCtx)
			require.Equal(t, c.TenantID, tenantID)
			require.Equal(t, "127.0.0.10:42", addr)
			return errors.New("failure to report")
		}
		conn, err = c.dialTenantCluster(ctx, nil /* requester */)
		require.EqualError(t, err, "reporting failure: failure to report: bar")
		require.True(t, errors.Is(err, context.Canceled))
		require.Nil(t, conn)

		// Assert existing calls.
		require.Equal(t, 2, dialSQLServerCount)
		require.Equal(t, 2, reportFailureFnCount)
		count, _ = c.DialTenantLatency.CumulativeSnapshot().Total()
		require.Equal(t, count, int64(2))
		require.Equal(t, c.DialTenantRetries.Count(), int64(0))
	})

	t.Run("non-transient error", func(t *testing.T) {
		// This is a short test, and is expected to finish within ms.
		ctx, cancel := context.WithTimeout(bgCtx, 2*time.Second)
		defer cancel()

		c := &connector{
			DialTenantLatency: metric.NewHistogram(metric.HistogramOptions{
				Mode:         metric.HistogramModePreferHdrLatency,
				Metadata:     metaDialTenantLatency,
				Duration:     time.Millisecond,
				BucketConfig: metric.IOLatencyBuckets,
			}),
			DialTenantRetries: metric.NewCounter(metaDialTenantRetries),
		}
		c.testingKnobs.lookupAddr = func(ctx context.Context) (string, error) {
			return "", errors.New("baz")
		}

		conn, err := c.dialTenantCluster(ctx, nil /* requester */)
		require.EqualError(t, err, "baz")
		require.Nil(t, conn)
		count, _ := c.DialTenantLatency.CumulativeSnapshot().Total()
		require.Equal(t, count, int64(1))
		require.Equal(t, c.DialTenantRetries.Count(), int64(0))
	})

	t.Run("successful", func(t *testing.T) {
		// This should not take more than 5 seconds.
		ctx, cancel := context.WithTimeout(bgCtx, 5*time.Second)
		defer cancel()

		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)

		crdbConn, _ := net.Pipe()
		defer crdbConn.Close()

		var reportFailureFnCount int
		c := &connector{
			TenantID: roachpb.MustMakeTenantID(42),
			DialTenantLatency: metric.NewHistogram(metric.HistogramOptions{
				Mode:         metric.HistogramModePreferHdrLatency,
				Metadata:     metaDialTenantLatency,
				Duration:     time.Millisecond,
				BucketConfig: metric.IOLatencyBuckets,
			}),
			DialTenantRetries: metric.NewCounter(metaDialTenantRetries),
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
		b, err := balancer.NewBalancer(
			ctx,
			stopper,
			balancer.NewMetrics(),
			c.DirectoryCache,
			balancer.NoRebalanceLoop(),
		)
		require.NoError(t, err)
		c.Balancer = b

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
		count, _ := c.DialTenantLatency.CumulativeSnapshot().Total()
		require.Equal(t, count, int64(1))
		require.Equal(t, c.DialTenantRetries.Count(), int64(2))
	})

	t.Run("load balancing", func(t *testing.T) {
		ctx := context.Background()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)

		crdbConn, _ := net.Pipe()
		defer crdbConn.Close()

		var mu struct {
			syncutil.Mutex
			pods map[string]struct{}
		}
		mu.pods = make(map[string]struct{})
		addPod := func(addr string) {
			mu.Lock()
			defer mu.Unlock()
			mu.pods[addr] = struct{}{}
		}
		removePod := func(addr string) {
			mu.Lock()
			defer mu.Unlock()
			delete(mu.pods, addr)
		}

		tenantID := roachpb.MustMakeTenantID(42)
		directoryCache := &testTenantDirectoryCache{
			lookupTenantPodsFn: func(
				fnCtx context.Context, tenantID roachpb.TenantID,
			) ([]*tenant.Pod, error) {
				mu.Lock()
				defer mu.Unlock()

				pods := make([]*tenant.Pod, 0, len(mu.pods))
				for addr := range mu.pods {
					pods = append(pods, &tenant.Pod{
						TenantID: tenantID.ToUint64(),
						Addr:     addr,
						State:    tenant.RUNNING,
					})
				}
				return pods, nil
			},
		}

		b, err := balancer.NewBalancer(
			ctx,
			stopper,
			nil, /* metrics */
			directoryCache,
			balancer.NoRebalanceLoop(),
		)
		require.NoError(t, err)

		c := &connector{
			ClusterName:    "my-foo",
			TenantID:       tenantID,
			DirectoryCache: directoryCache,
			Balancer:       b,
		}

		// Create two pods with similar number of assignments.
		const (
			addr1 = "127.0.0.10:80"
			addr2 = "127.0.0.20:90"
		)
		addPod(addr1)
		addPod(addr2)

		// Requests should be evenly distributed.
		responses := map[string]int{}
		c.testingKnobs.dialSQLServer = func(serverAssignment *balancer.ServerAssignment) (net.Conn, error) {
			responses[serverAssignment.Addr()]++
			return crdbConn, nil
		}
		for i := 0; i < 100; i++ {
			conn, err := c.dialTenantCluster(ctx, nil /* requester */)
			require.NoError(t, err)
			require.Equal(t, crdbConn, conn)
		}
		require.Equal(t, map[string]int{addr1: 50, addr2: 50}, responses)

		// Delete the first pod.
		removePod(addr1)

		responses = map[string]int{}
		for i := 0; i < 100; i++ {
			conn, err := c.dialTenantCluster(ctx, nil /* requester */)
			require.NoError(t, err)
			require.Equal(t, crdbConn, conn)
		}
		require.Equal(t, map[string]int{addr2: 100}, responses)
	})
}

func TestConnector_lookupAddr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	balancer, err := balancer.NewBalancer(
		ctx,
		stopper,
		nil, /* metrics */
		nil, /* directoryCache */
		balancer.NoRebalanceLoop(),
	)
	require.NoError(t, err)

	t.Run("successful", func(t *testing.T) {
		var lookupTenantPodsFnCount int

		c := &connector{
			ClusterName: "my-foo",
			TenantID:    roachpb.MustMakeTenantID(10),
			Balancer:    balancer,
		}
		c.DirectoryCache = &testTenantDirectoryCache{
			lookupTenantPodsFn: func(
				fnCtx context.Context, tenantID roachpb.TenantID,
			) ([]*tenant.Pod, error) {
				lookupTenantPodsFnCount++
				require.Equal(t, ctx, fnCtx)
				require.Equal(t, c.TenantID, tenantID)
				return []*tenant.Pod{
					{TenantID: c.TenantID.ToUint64(), Addr: "127.0.0.10:70", State: tenant.DRAINING},
					{TenantID: c.TenantID.ToUint64(), Addr: "127.0.0.10:80", State: tenant.RUNNING},
				}, nil
			},
		}

		addr, err := c.lookupAddr(ctx)
		require.NoError(t, err)
		require.Equal(t, "127.0.0.10:80", addr)
		require.Equal(t, 1, lookupTenantPodsFnCount)
	})

	t.Run("FailedPrecondition error", func(t *testing.T) {
		var lookupTenantPodsFnCount int
		c := &connector{
			ClusterName: "my-foo",
			TenantID:    roachpb.MustMakeTenantID(10),
			Balancer:    balancer,
		}
		c.DirectoryCache = &testTenantDirectoryCache{
			lookupTenantPodsFn: func(
				fnCtx context.Context, tenantID roachpb.TenantID,
			) ([]*tenant.Pod, error) {
				lookupTenantPodsFnCount++
				require.Equal(t, ctx, fnCtx)
				require.Equal(t, c.TenantID, tenantID)
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
			TenantID:    roachpb.MustMakeTenantID(10),
			Balancer:    balancer,
		}
		c.DirectoryCache = &testTenantDirectoryCache{
			lookupTenantPodsFn: func(
				fnCtx context.Context, tenantID roachpb.TenantID,
			) ([]*tenant.Pod, error) {
				lookupTenantPodsFnCount++
				require.Equal(t, ctx, fnCtx)
				require.Equal(t, c.TenantID, tenantID)
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
			TenantID:    roachpb.MustMakeTenantID(10),
			Balancer:    balancer,
		}
		c.DirectoryCache = &testTenantDirectoryCache{
			lookupTenantPodsFn: func(
				fnCtx context.Context, tenantID roachpb.TenantID,
			) ([]*tenant.Pod, error) {
				lookupTenantPodsFnCount++
				require.Equal(t, ctx, fnCtx)
				require.Equal(t, c.TenantID, tenantID)
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
	testutilsccl.ServerlessOnly(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tracker, err := balancer.NewConnTracker(ctx, stopper, nil /* timeSource */)
	require.NoError(t, err)

	tenantID := roachpb.MustMakeTenantID(10)

	t.Run("with tlsConfig", func(t *testing.T) {
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{},
			TLSConfig:  &tls.Config{InsecureSkipVerify: true},
		}
		crdbConn, _ := net.Pipe()
		defer crdbConn.Close()

		defer testutils.TestingHook(&BackendDial,
			func(ctx context.Context, msg *pgproto3.StartupMessage, serverAddress string,
				tlsConfig *tls.Config) (net.Conn, error) {
				require.Equal(t, c.StartupMsg, msg)
				require.Equal(t, "10.11.12.13:80", serverAddress)
				require.Equal(t, "10.11.12.13", tlsConfig.ServerName)
				return crdbConn, nil
			},
		)()

		sa := balancer.NewServerAssignment(tenantID, tracker, nil, "10.11.12.13:80")
		conn, err := c.dialSQLServer(ctx, sa)
		require.NoError(t, err)
		defer conn.Close()

		onCloseWrapper, ok := conn.(*onConnectionClose)
		require.True(t, ok)
		onErrorWrapper, ok := onCloseWrapper.Conn.(*errorSourceConn)
		require.True(t, ok)
		require.Equal(t, crdbConn, onErrorWrapper.Conn)

		conn.Close()
		conns := tracker.GetConnsMap(tenantID)
		require.Empty(t, conns)
	})

	t.Run("invalid serverAddr with tlsConfig", func(t *testing.T) {
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{},
			TLSConfig:  &tls.Config{InsecureSkipVerify: true},
		}
		sa := balancer.NewServerAssignment(tenantID, tracker, nil, "!@#$::")
		defer sa.Close()

		conn, err := c.dialSQLServer(ctx, sa)
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
			func(ctx context.Context, msg *pgproto3.StartupMessage, serverAddress string,
				tlsConfig *tls.Config) (net.Conn, error) {
				require.Equal(t, c.StartupMsg, msg)
				require.Equal(t, "10.11.12.13:1234", serverAddress)
				require.Nil(t, tlsConfig)
				return crdbConn, nil
			},
		)()
		sa := balancer.NewServerAssignment(tenantID, tracker, nil, "10.11.12.13:1234")
		conn, err := c.dialSQLServer(ctx, sa)
		require.NoError(t, err)
		defer conn.Close()

		onCloseWrapper, ok := conn.(*onConnectionClose)
		require.True(t, ok)
		onErrorWrapper, ok := onCloseWrapper.Conn.(*errorSourceConn)
		require.True(t, ok)
		require.Equal(t, crdbConn, onErrorWrapper.Conn)

		conn.Close()
		conns := tracker.GetConnsMap(tenantID)
		require.Empty(t, conns)
	})

	t.Run("failed to dial with non-transient error", func(t *testing.T) {
		c := &connector{StartupMsg: &pgproto3.StartupMessage{}}
		defer testutils.TestingHook(&BackendDial,
			func(ctx context.Context, msg *pgproto3.StartupMessage, serverAddress string,
				tlsConfig *tls.Config) (net.Conn, error) {
				require.Equal(t, c.StartupMsg, msg)
				require.Equal(t, "127.0.0.1:1234", serverAddress)
				require.Nil(t, tlsConfig)
				return nil, errors.New("foo")
			},
		)()
		sa := balancer.NewServerAssignment(tenantID, tracker, nil, "127.0.0.1:1234")
		defer sa.Close()

		conn, err := c.dialSQLServer(ctx, sa)
		require.EqualError(t, err, "foo")
		require.False(t, isRetriableConnectorError(err))
		require.Nil(t, conn)
	})

	t.Run("failed to dial with transient error", func(t *testing.T) {
		c := &connector{StartupMsg: &pgproto3.StartupMessage{}}
		defer testutils.TestingHook(&BackendDial,
			func(ctx context.Context, msg *pgproto3.StartupMessage, serverAddress string,
				tlsConfig *tls.Config) (net.Conn, error) {
				require.Equal(t, c.StartupMsg, msg)
				require.Equal(t, "127.0.0.2:4567", serverAddress)
				require.Nil(t, tlsConfig)
				return nil, withCode(errors.New("bar"), codeBackendDialFailed)
			},
		)()
		sa := balancer.NewServerAssignment(tenantID, tracker, nil, "127.0.0.2:4567")
		defer sa.Close()

		conn, err := c.dialSQLServer(ctx, sa)
		require.EqualError(t, err, "codeBackendDialFailed: bar")
		require.True(t, isRetriableConnectorError(err))
		require.Nil(t, conn)
	})
}

func TestRetriableConnectorError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	err := errors.New("foobar")
	require.False(t, isRetriableConnectorError(err))
	err = markAsRetriableConnectorError(err)
	require.True(t, isRetriableConnectorError(err))
	require.True(t, errors.Is(err, errRetryConnectorSentinel))
}

func TestTenantTLSConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	makeChains := func(commonName string, org string) [][]*x509.Certificate {
		cert := &x509.Certificate{}
		cert.Subject.CommonName = commonName
		if org != "" {
			cert.Subject.OrganizationalUnit = []string{org}
		}
		return [][]*x509.Certificate{{
			cert,
		}}
	}

	tests := []struct {
		name       string
		skipVerify bool
		chains     [][]*x509.Certificate
		err        string
	}{{
		name:   "okSecure",
		chains: makeChains("10", "Tenants"),
	}, {
		name:       "okSkipVerify",
		skipVerify: true,
	}, {
		name: "missingChains",
		err:  "VerifyConnection called with no verified chains",
	}, {
		name:   "wrongTenant",
		err:    "expected a cert for tenant {10} found '1337'",
		chains: makeChains("1337", "Tenants"),
	}, {
		name:   "nodeCert",
		err:    "certificate is not a tenant cert",
		chains: makeChains("10", ""),
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			config := &tls.Config{
				InsecureSkipVerify: tc.skipVerify,
			}

			config, err := tlsConfigForTenant(roachpb.MustMakeTenantID(10), "some.dns.address:123", config)
			require.NoError(t, err)
			require.Equal(t, config.ServerName, "some.dns.address")

			err = config.VerifyConnection(tls.ConnectionState{
				VerifiedChains: tc.chains,
			})

			if tc.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.err)
			}
		})
	}
}

var _ tenant.DirectoryCache = &testTenantDirectoryCache{}

// testTenantDirectoryCache is a test implementation of the tenant directory
// cache.
type testTenantDirectoryCache struct {
	lookupTenantFn        func(ctx context.Context, tenantID roachpb.TenantID) (*tenant.Tenant, error)
	lookupTenantPodsFn    func(ctx context.Context, tenantID roachpb.TenantID) ([]*tenant.Pod, error)
	trylookupTenantPodsFn func(ctx context.Context, tenantID roachpb.TenantID) ([]*tenant.Pod, error)
	reportFailureFn       func(ctx context.Context, tenantID roachpb.TenantID, addr string) error
}

// LookupTenant implements the tenant.DirectoryCache interface.
func (r *testTenantDirectoryCache) LookupTenant(
	ctx context.Context, tenantID roachpb.TenantID,
) (*tenant.Tenant, error) {
	return r.lookupTenantFn(ctx, tenantID)
}

// LookupTenantPods implements the tenant.DirectoryCache interface.
func (r *testTenantDirectoryCache) LookupTenantPods(
	ctx context.Context, tenantID roachpb.TenantID,
) ([]*tenant.Pod, error) {
	return r.lookupTenantPodsFn(ctx, tenantID)
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

type fakeTCPConn struct {
	net.Conn
	remoteAddr *net.TCPAddr
	localAddr  *net.TCPAddr
}

func (c *fakeTCPConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func (c *fakeTCPConn) LocalAddr() net.Addr {
	return c.localAddr
}
