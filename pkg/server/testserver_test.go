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
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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

	ctx := context.Background()
	testServer := netutil.MakeServer(stop.NewStopper(), nil, nil)
	oldConnStateHandler := testServer.ConnState
	require.Nil(t, SetupIdleMonitor(ctx, 0, testServer))
	// The conn state handler doesn't change
	require.Equal(t,
		reflect.ValueOf(oldConnStateHandler),
		reflect.ValueOf(testServer.ConnState),
	)
}

func TestSetupIdleMonitor_WithWarmupProvided(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	testServer := netutil.MakeServer(stop.NewStopper(), nil, nil)
	oldConnStateHandler := testServer.ConnState
	warmupDuration := 300 * time.Millisecond
	monitor := SetupIdleMonitor(ctx, warmupDuration, testServer)
	require.NotNil(t, monitor)
	// The conn state handler changes
	require.NotEqual(t,
		reflect.ValueOf(oldConnStateHandler),
		reflect.ValueOf(testServer.ConnState),
	)

	ln, err := net.Listen("tcp", "")
	require.NoError(t, err)
	connectionEstablished := false
	connectionOver := false
	go require.NoError(t,
		testServer.ServeWith(ctx, stop.NewStopper(), ln, func(conn net.Conn) {
			connectionEstablished = true
			time.Sleep(100 * time.Millisecond)
			connectionOver = true
		}),
	)
	require.EqualValues(t, 0, monitor.activeConnectionCount)

	conn, err := net.Dial("tcp", ln.Addr().String())

	time.Sleep(50 * time.Millisecond)

	require.NotNil(t, conn)
	require.NoError(t, err)
	require.True(t, connectionEstablished)
	require.False(t, monitor.activated)
	require.EqualValues(t, 1, monitor.activeConnectionCount)
	require.EqualValues(t, 1, monitor.totalConnectionCount)

	time.Sleep(100 * time.Millisecond)

	require.True(t, connectionOver)
	require.False(t, monitor.activated)
	require.EqualValues(t, 0, monitor.activeConnectionCount)
	require.EqualValues(t, 1, monitor.totalConnectionCount)

	time.Sleep(200 * time.Millisecond)

	require.True(t, monitor.activated)
}
