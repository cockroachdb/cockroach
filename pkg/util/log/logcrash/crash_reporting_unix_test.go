// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !windows

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
