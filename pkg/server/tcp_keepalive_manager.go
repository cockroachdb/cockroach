// Copyright 2022 The Cockroach Authors.
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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
)

var KeepAliveTimeout = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"server.sql_tcp_keep_alive.timeout",
	"maximum time for which an idle TCP connection to the server will be kept alive",
	time.Minute*2,
	settings.WithPublic,
)

var KeepAliveProbeFrequency = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"server.sql_tcp_keep_alive.interval",
	"time between keep alive probes and idle time before probes are sent out",
	time.Second*15,
	settings.WithPublic,
)

type tcpKeepAliveManager struct {
	// loggedKeepAliveStatus ensures that errors about setting the TCP
	// keepalive status are only reported once.
	loggedKeepAliveStatus int32
	settings              *cluster.Settings
}

func makeTCPKeepAliveManager(settings *cluster.Settings) tcpKeepAliveManager {
	return tcpKeepAliveManager{
		settings: settings,
	}
}

// configure attempts to set TCP keep-alive on
// connection. Does not fail on errors.
func (k *tcpKeepAliveManager) configure(ctx context.Context, conn net.Conn) {
	muxConn, ok := conn.(*cmux.MuxConn)
	if !ok {
		return
	}
	tcpConn, ok := muxConn.Conn.(*net.TCPConn)
	if !ok {
		return
	}

	// Only log success/failure once.
	doLog := atomic.CompareAndSwapInt32(&k.loggedKeepAliveStatus, 0, 1)
	if err := tcpConn.SetKeepAlive(true); err != nil {
		if doLog {
			log.Ops.Warningf(ctx, "failed to enable TCP keep-alive for pgwire: %v", err)
		}
		return

	}
	// Based on the maximum connection life span and probe interval, pick a maximum
	// probe count.
	maximumConnectionTime := KeepAliveTimeout.Get(&k.settings.SV)
	probeFrequency := KeepAliveProbeFrequency.Get(&k.settings.SV)
	// We are going to try to hit our maximum connection time by
	// allowing enough probes of our select frequency. Additionally,
	// subtract one, since probing starts once the connection has been
	// idle that long.
	probeCount := (maximumConnectionTime / (probeFrequency)) - 1
	if probeCount <= 0 {
		probeCount = 1
	}

	if err := sysutil.SetKeepAliveCount(tcpConn, int(probeCount)); err != nil {
		if doLog {
			log.Ops.Warningf(ctx, "failed to set TCP keep-alive probe count for pgwire: %v", err)
		}
	}

	if err := tcpConn.SetKeepAlivePeriod(probeFrequency); err != nil {
		if doLog {
			log.Ops.Warningf(ctx, "failed to set TCP keep-alive duration for pgwire: %v", err)
		}
		return
	}

	if doLog {
		log.VEventf(ctx, 2, "setting TCP keep-alive interval %d and probe count to %d for pgwire", probeFrequency, probeCount)
	}
}
