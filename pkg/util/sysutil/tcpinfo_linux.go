// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build linux

package sysutil

import (
	"net"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func getTCPInfo(conn syscall.RawConn) (*unix.TCPInfo, error) {
	var (
		tcpInfo    *unix.TCPInfo
		syscallErr error
	)
	err := conn.Control(func(fd uintptr) {
		tcpInfo, syscallErr = unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
	})
	if syscallErr != nil {
		return nil, syscallErr
	}
	if err != nil {
		return nil, err
	}
	return tcpInfo, nil
}

// GetRTTInfo retrieves round-trip time information from a TCP connection.
func GetRTTInfo(conn *net.TCPConn) (value *RTTInfo, ok bool) {
	if conn == nil {
		return nil, false
	}
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return nil, false
	}
	tcpInfo, err := getTCPInfo(rawConn)
	if err != nil {
		return nil, false
	}
	return &RTTInfo{
		RTT:    time.Duration(tcpInfo.Rtt) * time.Microsecond,
		RTTVar: time.Duration(tcpInfo.Rttvar) * time.Microsecond,
	}, true
}
