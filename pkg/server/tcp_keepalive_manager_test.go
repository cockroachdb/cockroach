// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build linux || (arm64 && darwin)

package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cmux"
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
	KeepAliveProbeFrequency.Override(ctx, &clusterSettings.SV, time.Second*5)
	KeepAliveTimeout.Override(ctx, &clusterSettings.SV, time.Minute*5)
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
	idleTime, probeInterval, probeCount, err := sysutil.GetKeepAliveSettings(tcpConn)
	require.NoError(t, err)

	require.Equal(t,
		idleTime,
		KeepAliveProbeFrequency.Get(&clusterSettings.SV),
		"keep alive probe frequency not set")
	require.Equal(t,
		probeInterval,
		KeepAliveProbeFrequency.Get(&clusterSettings.SV),
		"keep alive probe frequency not set")

	require.Equal(t,
		KeepAliveTimeout.Get(&clusterSettings.SV),
		time.Duration(probeCount+1)*probeInterval,
		"Computed wait time doesn't match our target timeout")

	// Validate we didn't hit any errors using the sockets.
	require.NoError(t, err)
	require.NoError(t, listener.Close())
	require.NoError(t, grp.Wait())
	require.NoError(t, conn.Close())
}
