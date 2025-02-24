// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
)

var KeepAliveProbeCount = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"server.sql_tcp_keep_alive.count",
	"maximum number of probes that will be sent out before a connection is dropped because "+
		"it's unresponsive (Linux and Darwin only)",
	3,
	settings.WithPublic,
)

var KeepAliveProbeFrequency = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"server.sql_tcp_keep_alive.interval",
	"time between keep alive probes and idle time before probes are sent out",
	time.Second*10,
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
	// Based on the maximum connection life span and probe interval, pick a maximum
	// probe count.
	probeCount := KeepAliveProbeCount.Get(&k.settings.SV)
	probeFrequency := KeepAliveProbeFrequency.Get(&k.settings.SV)

	err := tcpConn.SetKeepAliveConfig(net.KeepAliveConfig{
		Enable:   true,
		Idle:     probeFrequency,
		Interval: probeFrequency,
		Count:    int(probeCount),
	})
	if err != nil {
		if doLog {
			log.Ops.Warningf(ctx, "failed to configure TCP keep-alive for pgwire: %v", err)
		}
		return
	}

	if doLog {
		log.VEventf(ctx, 2, "setting TCP keep-alive interval %d and probe count to %d for pgwire", probeFrequency, probeCount)
	}
}
