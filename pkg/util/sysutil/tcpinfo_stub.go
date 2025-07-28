// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !linux

package sysutil

import (
	"syscall"
)

// GetRTTInfo is a stub implementation for non-Linux platforms returning (nil, false).
func GetRTTInfo(conn syscall.RawConn) (value *RTTInfo, ok bool) {
	return nil, false
}
