// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//go:build linux || (arm64 && darwin)

package sysutil

import (
	"net"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
)

// SetKeepAliveCount sets the keep alive probe count on a TCP
// connection.
func SetKeepAliveCount(conn *net.TCPConn, probeCount int) (err error) {
	syscallConn, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	outerErr := syscallConn.Control(func(fd uintptr) {
		err = syscall.SetsockoptInt(SocketFd(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, probeCount)
	})
	if err != nil || outerErr != nil {
		return errors.WithSecondaryError(err, outerErr)
	}
	return nil
}

// GetKeepAliveSettings gets the keep alive socket connections
// set on a TCP connection.
func GetKeepAliveSettings(
	conn *net.TCPConn,
) (idleTime time.Duration, probeInterval time.Duration, probeCount int, err error) {
	syscallConn, err := conn.SyscallConn()
	var probeIntervalSec, idleTimeSec int
	innerErr := syscallConn.Control(func(fd uintptr) {
		probeIntervalSec, err = syscall.GetsockoptInt(SocketFd(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL)
		if err != nil {
			return
		}
		idleTimeSec, err = syscall.GetsockoptInt(SocketFd(fd), syscall.IPPROTO_TCP, TCP_KEEPIDLE)
		if err != nil {
			return
		}
		probeCount, err = syscall.GetsockoptInt(SocketFd(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT)
		if err != nil {
			return
		}
	})
	if err != nil || innerErr != nil {
		return idleTime, probeInterval, probeCount, errors.WithSecondaryError(err, innerErr)
	}
	// Convert to durations
	idleTime = time.Second * time.Duration(idleTimeSec)
	probeInterval = time.Second * time.Duration(probeIntervalSec)

	return idleTime, probeInterval, probeCount, nil
}
