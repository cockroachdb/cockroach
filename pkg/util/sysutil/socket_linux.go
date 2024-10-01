// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build linux

package sysutil

import "syscall"

const TCP_KEEPIDLE = syscall.TCP_KEEPIDLE

type SocketFd = int
