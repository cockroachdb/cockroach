// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Note: We only enable this on ARM64 because the go runtime,
// otherwise is missing the syscall constants.
//go:build arm64 && darwin

package sysutil

import "syscall"

const TCP_KEEPIDLE = syscall.TCP_KEEPALIVE

type SocketFd = int
