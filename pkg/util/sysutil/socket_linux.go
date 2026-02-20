// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build linux

package sysutil

import (
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const TCP_KEEPIDLE = syscall.TCP_KEEPIDLE

type SocketFd = int

// SetTcpUserTimeout sets the TCP_USER_TIMEOUT on a socket determining
// the maximum time transmitted data can be unacknowledged before the
// connection is dropped.
func SetTcpUserTimeout(fd SocketFd, timeout time.Duration) error {
	return unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, int(timeout.Milliseconds()))
}

// GetTcpUserTimeout gets the TCP_USER_TIMEOUT on a socket.
func GetTcpUserTimeout(fd SocketFd) (time.Duration, error) {
	var timeout int
	timeout, err := unix.GetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT)
	if err != nil {
		return 0, err
	}
	return time.Duration(timeout) * time.Millisecond, nil
}
