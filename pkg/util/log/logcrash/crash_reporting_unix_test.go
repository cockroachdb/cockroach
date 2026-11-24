// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !windows

package logcrash

import (
	"os"

	"golang.org/x/sys/unix"
)

func init() {
	safeErrorTestCases = append(safeErrorTestCases, safeErrorTestCase{
		err: os.NewSyscallError("write", unix.ENOSPC),
		expErr: `write: no space left on device
(1) write
Wraps: (2) no space left on device
Error types: (1) *os.SyscallError (2) syscall.Errno`,
	})
}
