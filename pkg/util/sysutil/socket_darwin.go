// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Note: We only enable this on ARM64 because the go runtime,
// otherwise is missing the syscall constants.
//go:build arm64 && darwin

package sysutil

import (
	"syscall"
	"time"
)

const TCP_KEEPIDLE = syscall.TCP_KEEPALIVE

type SocketFd = int

// SetTcpUserTimeout sets the TCP_USER_TIMEOUT on a socket determining
// the maximum time transmitted data can be unacknowledged before the
// connection is dropped.
func SetTcpUserTimeout(fd SocketFd, timeout time.Duration) error {
	return syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_RXT_CONNDROPTIME, int(timeout.Seconds()))
}

// GetTcpUserTimeout gets the TCP_USER_TIMEOUT on a socket.
func GetTcpUserTimeout(fd SocketFd) (time.Duration, error) {
	timeout, err := syscall.GetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_RXT_CONNDROPTIME)
	if err != nil {
		return 0, err
	}
	return time.Duration(timeout) * time.Second, nil
}
