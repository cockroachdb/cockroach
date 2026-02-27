// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build linux || (arm64 && darwin)

package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/server/tcpkeepalive"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/stretchr/testify/require"
)

func TestKeepAliveManager(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	grp := ctxgroup.WithContext(ctx)
	clusterSettings := cluster.MakeTestingClusterSettings()
	tcpkeepalive.ProbeInterval.Override(ctx, &clusterSettings.SV, time.Second*1)
	tcpkeepalive.ProbeCount.Override(ctx, &clusterSettings.SV, 5)
	tcpkeepalive.UserTimeout.Override(ctx, &clusterSettings.SV, time.Second*128)
	keepAliveMgr := makeTCPKeepAliveManager(clusterSettings)

	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	mux := cmux.New(l)
	mux.HandleError(func(err error) bool {
		return false
	})

	listener := mux.Match(cmux.Any())
	grp.Go(func() error {
		netutil.FatalIfUnexpected(mux.Serve())
		return nil
	})
	connStr := listener.Addr()

	grp.GoCtx(func(ctx context.Context) error {
		conn, err := net.Dial(connStr.Network(), connStr.String())
		if err != nil {
			return err
		}
		reply := make([]byte, 1)
		_, err = conn.Read(reply)
		return err
	})

	conn, err := listener.Accept()
	require.NoError(t, err)

	// Configure this new connection with keep alive settings.
	keepAliveMgr.configure(ctx, conn)
	_, err = conn.Write([]byte("1"))
	require.NoError(t, err)
	// Confirm the settings are set on any TCP connection that we
	// process.
	muxConn, ok := conn.(*cmux.MuxConn)
	if !ok {
		return
	}
	tcpConn, ok := muxConn.Conn.(*net.TCPConn)
	if !ok {
		return
	}
	idleTime, probeInterval, probeCount, userTimeout, err := sysutil.GetKeepAliveSettings(tcpConn)
	require.NoError(t, err)
	require.Equal(t,
		idleTime,
		tcpkeepalive.ProbeInterval.Get(&clusterSettings.SV),
		"keep alive probe frequency not set")
	require.Equal(t,
		probeInterval,
		tcpkeepalive.ProbeInterval.Get(&clusterSettings.SV),
		"keep alive probe frequency not set")
	require.Equal(t,
		probeCount,
		int(tcpkeepalive.ProbeCount.Get(&clusterSettings.SV)),
		"Computed wait time doesn't match our target timeout")
	require.Equal(t, userTimeout, tcpkeepalive.UserTimeout.Get(&clusterSettings.SV))
	// Validate we didn't hit any errors using the sockets.
	require.NoError(t, listener.Close())
	require.NoError(t, grp.Wait())
	require.NoError(t, conn.Close())
}

// makeTCPConnPair creates a pair of connected TCP connections for testing.
// The caller is responsible for closing both connections.
func makeTCPConnPair(t *testing.T) (server, client *net.TCPConn) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()

	connCh := make(chan net.Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			return
		}
		connCh <- c
	}()

	clientConn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	serverConn := <-connCh

	return serverConn.(*net.TCPConn), clientConn.(*net.TCPConn)
}

func TestConfigureConnKeepAlive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	cs := cluster.MakeTestingClusterSettings()
	tcpkeepalive.ProbeInterval.Override(ctx, &cs.SV, 5*time.Second)
	tcpkeepalive.ProbeCount.Override(ctx, &cs.SV, 7)
	tcpkeepalive.UserTimeout.Override(ctx, &cs.SV, 60*time.Second)

	t.Run("session overrides", func(t *testing.T) {
		serverConn, clientConn := makeTCPConnPair(t)
		defer func() { _ = serverConn.Close() }()
		defer func() { _ = clientConn.Close() }()

		// Apply per-session overrides (non-zero values).
		err := tcpkeepalive.ConfigureConnKeepAlive(
			serverConn,
			20*time.Second,  // idle
			3*time.Second,   // interval
			4,               // count
			100*time.Second, // userTimeout
			&cs.SV,
		)
		require.NoError(t, err)

		idleTime, probeInterval, probeCount, userTimeout, err := sysutil.GetKeepAliveSettings(serverConn)
		require.NoError(t, err)
		require.Equal(t, 20*time.Second, idleTime, "idle time should be session override")
		require.Equal(t, 3*time.Second, probeInterval, "interval should be session override")
		require.Equal(t, 4, probeCount, "count should be session override")
		require.Equal(t, 100*time.Second, userTimeout, "user timeout should be session override")
	})

	t.Run("zero falls back to cluster settings", func(t *testing.T) {
		serverConn, clientConn := makeTCPConnPair(t)
		defer func() { _ = serverConn.Close() }()
		defer func() { _ = clientConn.Close() }()

		// All zeros should fall back to cluster settings.
		err := tcpkeepalive.ConfigureConnKeepAlive(
			serverConn, 0, 0, 0, 0, &cs.SV,
		)
		require.NoError(t, err)

		idleTime, probeInterval, probeCount, userTimeout, err := sysutil.GetKeepAliveSettings(serverConn)
		require.NoError(t, err)
		// idle falls back to ProbeInterval since IdleTime defaults to 0.
		require.Equal(t, tcpkeepalive.ProbeInterval.Get(&cs.SV), idleTime,
			"idle should fall back to probe interval cluster setting")
		require.Equal(t, tcpkeepalive.ProbeInterval.Get(&cs.SV), probeInterval,
			"interval should use cluster setting")
		require.Equal(t, int(tcpkeepalive.ProbeCount.Get(&cs.SV)), probeCount,
			"count should use cluster setting")
		require.Equal(t, tcpkeepalive.UserTimeout.Get(&cs.SV), userTimeout,
			"user timeout should use cluster setting")
	})

	t.Run("idle time cluster setting", func(t *testing.T) {
		serverConn, clientConn := makeTCPConnPair(t)
		defer func() { _ = serverConn.Close() }()
		defer func() { _ = clientConn.Close() }()

		// Set the idle time cluster setting to a value different from interval.
		tcpkeepalive.IdleTime.Override(ctx, &cs.SV, 15*time.Second)
		defer tcpkeepalive.IdleTime.Override(ctx, &cs.SV, 0)

		err := tcpkeepalive.ConfigureConnKeepAlive(
			serverConn, 0, 0, 0, 0, &cs.SV,
		)
		require.NoError(t, err)

		idleTime, probeInterval, _, _, err := sysutil.GetKeepAliveSettings(serverConn)
		require.NoError(t, err)
		require.Equal(t, 15*time.Second, idleTime,
			"idle should use dedicated idle cluster setting")
		require.Equal(t, tcpkeepalive.ProbeInterval.Get(&cs.SV), probeInterval,
			"interval should remain unchanged")
	})
}
