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

// GetKeepAliveSettings gets the keep alive socket connections
// set on a TCP connection.
func GetKeepAliveSettings(
	conn *net.TCPConn,
) (idleTime time.Duration, probeInterval time.Duration, probeCount int, err error) {
	return 0, 0, 0, nil
}
