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
	"github.com/cockroachdb/cockroach/pkg/util/besteffort"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
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

var TcpUserTimeout = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"server.sql_tcp_user.timeout",
	"specifies the maximum amount of time, in milliseconds, that transmitted data "+
		"can remain unacknowledged before the underlying TCP connection is forcefully closed. "+
		"(Linux and Darwin only)",
	time.Second*30,
	settings.WithPublic,
)

// FIXME: Just add a new setting instead.

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
	besteffort.Warning(ctx, "tcp-socket-keepalive", func(ctx context.Context) error {
		err := tcpConn.SetKeepAliveConfig(net.KeepAliveConfig{
			Enable:   true,
			Idle:     probeFrequency,
			Interval: probeFrequency,
			Count:    int(probeCount),
		})
		// Only errors for the first connection.
		if doLog && err != nil {
			return err
		}
		return nil
	})

	userTimeout := TcpUserTimeout.Get(&k.settings.SV)
	besteffort.Error(ctx, "tcp-socket-user-timeout", func(ctx context.Context) error {
		rawConn, err := tcpConn.SyscallConn()
		if err != nil {
			return err
		}
		var userTimeoutErr error
		err = rawConn.Control(func(fd uintptr) {
			userTimeoutErr = sysutil.SetTcpUserTimeout(sysutil.SocketFd(fd), userTimeout)
		})
		err = errors.CombineErrors(err, userTimeoutErr)
		// Only errors for the first connection.
		if doLog && err != nil {
			return err
		}
		return nil
	})

	if doLog {
		log.VEventf(ctx, 2, "setting TCP keep-alive interval %d and probe count to %d for pgwire", probeFrequency, probeCount)
		log.VEventf(ctx, 2, "setting TCP user timeout %s for pgwire", userTimeout)
	}
}
