// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build linux

package sysutil

import (
	"net"

	"golang.org/x/sys/unix"
)

// GetTCPInfo retrieves TCP info from a TCP connection.
func GetTCPInfo(conn *net.TCPConn) (value *unix.TCPInfo, ok bool) {
	if conn == nil {
		return nil, false
	}
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return nil, false
	}
	var (
		tcpInfo    *unix.TCPInfo
		syscallErr error
	)
	err = rawConn.Control(func(fd uintptr) {
		tcpInfo, syscallErr = unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
	})
	if syscallErr != nil || err != nil {
		return nil, false
	}
	return tcpInfo, true
}
