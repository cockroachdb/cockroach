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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestHandleHealth(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
