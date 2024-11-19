// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlproxyccl

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestHandleHealth(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	proxyServer, err := NewServer(ctx, stopper, ProxyOptions{})
	require.NoError(t, err)

	rw := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/_status/healthz/", nil)

	proxyServer.mux.ServeHTTP(rw, r)

	require.Equal(t, http.StatusOK, rw.Code)
	out, err := io.ReadAll(rw.Body)
	require.NoError(t, err)

	require.Equal(t, []byte("OK"), out)
}

func TestHandleVars(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	proxyServer, err := NewServer(ctx, stopper, ProxyOptions{})
	require.NoError(t, err)

	rw := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/_status/vars/", nil)

	proxyServer.mux.ServeHTTP(rw, r)

	require.Equal(t, http.StatusOK, rw.Code)
	out, err := io.ReadAll(rw.Body)
	require.NoError(t, err)

	require.Contains(t, string(out), "# HELP proxy_sql_conns")
	require.Contains(t, string(out), "# HELP proxy_balancer_rebalance_total")
	require.Contains(t, string(out), "# HELP proxy_conn_migration_attempted")
}

func TestAwaitNoConnections(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	originalInterval := awaitNoConnectionsInterval
	awaitNoConnectionsInterval = time.Millisecond
	defer func() {
		awaitNoConnectionsInterval = originalInterval
	}()

	proxyServer, err := NewServer(ctx, stopper, ProxyOptions{})
	require.NoError(t, err)

	// Simulate a connection coming in.
	proxyServer.metrics.CurConnCount.Inc(1)
	begin := timeutil.Now()

	// Wait a few milliseconds and simulate the connection dropping.
	waitTime := time.Millisecond * 150
	_ = stopper.RunAsyncTask(ctx, "decrement-con-count", func(context.Context) {
		<-time.After(waitTime)
		proxyServer.metrics.CurConnCount.Dec(1)
	})
	// Wait for there to be no connections.
	<-proxyServer.AwaitNoConnections(ctx)
	// Make sure we waited for the connection to be dropped.
	require.GreaterOrEqual(t, timeutil.Since(begin), waitTime)
}

func TestShouldLogError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s, err := NewServer(ctx, stopper, ProxyOptions{})
	require.NoError(t, err)

	t.Run("no error", func(t *testing.T) {
		require.True(t, s.shouldLogError(ctx, nil, nil, nil))
	})

	t.Run("normal error", func(t *testing.T) {
		require.True(t, s.shouldLogError(ctx, errors.New("foo"), nil, nil))
	})

	highFreqErr := errors.Mark(errors.New("test error"), highFreqErrorMarker)
	reqTags := make(map[string]interface{})

	t.Run("no tenant ID", func(t *testing.T) {
		require.True(t, s.shouldLogError(ctx, highFreqErr, nil, reqTags))
	})

	reqTags["tenant"] = 42

	t.Run("tenant ID not string", func(t *testing.T) {
		require.True(t, s.shouldLogError(ctx, highFreqErr, nil, reqTags))
	})

	reqTags["tenant"] = "42"

	t.Run("invalid connection", func(t *testing.T) {
		require.True(t, s.shouldLogError(ctx, highFreqErr, &fakeTCPConn{}, reqTags))
	})

	conn := &fakeTCPConn{remoteAddr: &net.TCPAddr{IP: net.IP{1, 2, 3, 4}}}

	t.Run("successful - tenant 42", func(t *testing.T) {
		require.True(t, s.shouldLogError(ctx, highFreqErr, conn, reqTags))
		require.False(t, s.shouldLogError(ctx, highFreqErr, conn, reqTags))
		require.False(t, s.shouldLogError(ctx, highFreqErr, conn, reqTags))
	})

	// Using a different tenant should work independently.
	reqTags["tenant"] = "100"

	t.Run("successful - tenant 100", func(t *testing.T) {
		require.True(t, s.shouldLogError(ctx, highFreqErr, conn, reqTags))
		require.False(t, s.shouldLogError(ctx, highFreqErr, conn, reqTags))
		require.False(t, s.shouldLogError(ctx, highFreqErr, conn, reqTags))
	})
}
