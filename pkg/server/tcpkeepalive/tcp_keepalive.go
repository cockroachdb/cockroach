// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package tcpkeepalive provides TCP keepalive configuration utilities and
// cluster settings. It is a separate package to avoid import cycles between
// pkg/server and pkg/sql/pgwire.
package tcpkeepalive

import (
	"crypto/tls"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
)

// ProbeCount is the maximum number of keepalive probes before a connection is
// dropped.
var ProbeCount = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"server.sql_tcp_keep_alive.count",
	"maximum number of probes that will be sent out before a connection is dropped because "+
		"it's unresponsive (Linux and Darwin only)",
	3,
	settings.NonNegativeInt,
	settings.WithPublic,
)

// ProbeInterval is the time between keepalive probes and the default idle
// time before probes are sent.
var ProbeInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"server.sql_tcp_keep_alive.interval",
	"time between keep alive probes and idle time before probes are sent out",
	time.Second*10,
	settings.DurationWithMinimum(0),
	settings.WithPublic,
)

// IdleTime is the time with no activity before sending a keepalive probe.
// If 0, the value of ProbeInterval is used.
var IdleTime = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"server.sql_tcp_keep_alive.idle",
	"time with no network activity before sending a TCP keepalive probe "+
		"(Linux and Darwin only). If 0, the value of "+
		"server.sql_tcp_keep_alive.interval is used.",
	0,
	settings.DurationWithMinimum(0),
	settings.WithPublic,
)

// UserTimeout is the maximum time that transmitted data may remain
// unacknowledged before the connection is dropped.
var UserTimeout = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"server.sql_tcp_user.timeout",
	"specifies the maximum amount of time that transmitted data "+
		"can remain unacknowledged before the underlying TCP connection is forcefully closed. "+
		"(Linux and Darwin only)",
	time.Second*30,
	settings.DurationWithMinimum(0),
	settings.WithPublic,
)

// ExtractTCPConn extracts the underlying *net.TCPConn from a net.Conn,
// handling TLS wrapping.
func ExtractTCPConn(conn net.Conn) *net.TCPConn {
	switch c := conn.(type) {
	case *net.TCPConn:
		return c
	case *tls.Conn:
		if under, ok := c.NetConn().(*net.TCPConn); ok {
			return under
		}
	}
	return nil
}

// ConfigureConnKeepAlive applies per-session TCP keepalive settings to the
// given connection. Zero duration/count values fall back to the corresponding
// cluster setting. Returns an error if the socket options could not be applied.
func ConfigureConnKeepAlive(
	conn net.Conn,
	idle, interval time.Duration,
	count int,
	userTimeout time.Duration,
	sv *settings.Values,
) error {
	tcpConn := ExtractTCPConn(conn)
	if tcpConn == nil {
		return nil
	}

	// For each parameter, a non-zero session value takes precedence. A zero
	// value means "use the cluster setting default." For idle time
	// specifically, if both the session value and the dedicated
	// server.sql_tcp_keep_alive.idle cluster setting are zero, fall back to
	// server.sql_tcp_keep_alive.interval.
	if idle == 0 {
		idle = IdleTime.Get(sv)
		if idle == 0 {
			idle = ProbeInterval.Get(sv)
		}
	}
	if interval == 0 {
		interval = ProbeInterval.Get(sv)
	}
	if count == 0 {
		count = int(ProbeCount.Get(sv))
	}
	if userTimeout == 0 {
		userTimeout = UserTimeout.Get(sv)
	}

	if err := tcpConn.SetKeepAliveConfig(net.KeepAliveConfig{
		Enable:   true,
		Idle:     idle,
		Interval: interval,
		Count:    count,
	}); err != nil {
		return err
	}

	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		return err
	}
	var sysErr error
	if err := rawConn.Control(func(fd uintptr) {
		sysErr = sysutil.SetTcpUserTimeout(sysutil.SocketFd(fd), userTimeout)
	}); err != nil {
		return err
	}
	return sysErr
}
