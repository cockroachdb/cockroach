// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//go:build !(linux || (arm64 && darwin))

package sysutil

import (
	"net"
	"time"
)

type SocketFd = int

// GetKeepAliveSettings gets the keep alive socket connections
// set on a TCP connection.
func GetKeepAliveSettings(
	conn *net.TCPConn,
) (
	idleTime time.Duration,
	probeInterval time.Duration,
	probeCount int,
	userTimeout time.Duration,
	err error,
) {
	return 0, 0, 0, 0, nil
}

// SetTcpUserTimeout sets the TCP_USER_TIMEOUT on a socket determining
// the maximum time transmitted data can be unacknowledged before the
// connection is dropped.
func SetTcpUserTimeout(fd SocketFd, timeout time.Duration) error {
	return nil
}

// GetTcpUserTimeout gets the TCP_USER_TIMEOUT on a socket.
func GetTcpUserTimeout(fd SocketFd) (time.Duration, error) {
	return 0, nil
}
