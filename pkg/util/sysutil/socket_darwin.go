// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Note: We only enable this on ARM64 because the go runtime,
// otherwise is missing the syscall constants.
//go:build arm64 && darwin

package sysutil

import "syscall"

const TCP_KEEPIDLE = syscall.TCP_KEEPALIVE

type SocketFd = int
