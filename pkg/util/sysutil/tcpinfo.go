// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sysutil

import "time"

// RTTInfo holds the round-trip time information for a TCP connection.
type RTTInfo struct {
	RTT    time.Duration
	RTTVar time.Duration
}
