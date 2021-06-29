// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

func TestServerTest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
}

func TestSetupIdleMonitor_WithNoWarmupProvided(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	testServer := netutil.MakeServer(stopper, nil, nil)
	oldConnStateHandler := testServer.ConnState
	require.Nil(t, SetupIdleMonitor(ctx, stopper, 0, testServer))
	// The conn state handler doesn't change
	require.Equal(t,
		reflect.ValueOf(oldConnStateHandler),
		reflect.ValueOf(testServer.ConnState),
	)
}

func TestSetupIdleMonitor_WithWarmupProvided(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.WithIssue(t, 66767, "flaky test")
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	ts := netutil.MakeServer(stopper, nil, nil)
	oldConnStateHandler := ts.ConnState
	warmupDuration := 200 * time.Millisecond
	countdownDuration := 200 * time.Millisecond
	monitor := SetupIdleMonitor(ctx, stopper, warmupDuration, ts, countdownDuration)
	require.NotNil(t, monitor)
	// The conn state handler changes
	require.NotEqual(t,
		reflect.ValueOf(oldConnStateHandler),
		reflect.ValueOf(ts.ConnState),
	)

	ln, err := net.Listen("tcp", "")
	require.NoError(t, err)
	require.NoError(t, stopper.RunAsyncTask(ctx, "ln-close", func(ctx context.Context) {
		<-stopper.ShouldQuiesce()
		require.NoError(t, ln.Close())
	}))

	var connectionEstablished syncutil.AtomicBool
	var connectionOver syncutil.AtomicBool

	require.NoError(t, stopper.RunAsyncTask(ctx, "serve-conn", func(ctx context.Context) {
		_ = ts.ServeWith(ctx, stopper, ln, func(conn net.Conn) {
			defer conn.Close()
			connectionEstablished.Set(true)
			time.Sleep(100 * time.Millisecond)
			connectionOver.Set(true)
		})
	}))

	monitor.mu.Lock()
	require.EqualValues(t, 0, monitor.activeConnectionCount)
	monitor.mu.Unlock()

	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NotNil(t, conn)
	require.NoError(t, err)
	defer conn.Close()

	time.Sleep(50 * time.Millisecond)

	require.True(t, connectionEstablished.Get())

	monitor.mu.Lock()
	require.False(t, monitor.activated)
	require.EqualValues(t, 1, monitor.activeConnectionCount)
	require.EqualValues(t, 1, monitor.totalConnectionCount)
	monitor.mu.Unlock()

	time.Sleep(100 * time.Millisecond)

	require.True(t, connectionOver.Get())
	monitor.mu.Lock()
	require.False(t, monitor.activated)
	require.EqualValues(t, 0, monitor.activeConnectionCount)
	require.EqualValues(t, 1, monitor.totalConnectionCount)
	monitor.mu.Unlock()

	time.Sleep(100 * time.Millisecond)

	monitor.mu.Lock()
	require.True(t, monitor.activated)
	monitor.mu.Unlock()

	time.Sleep(200 * time.Millisecond)

	select {
	case <-stopper.IsStopped():
	default:
		t.Error("stop on idle didn't trigger")
	}
}
