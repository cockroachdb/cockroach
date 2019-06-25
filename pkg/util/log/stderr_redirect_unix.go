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

package log

import (
	"os"

	"golang.org/x/sys/unix"
)

func dupFD(fd uintptr) (uintptr, error) {
	// Warning: failing to set FD_CLOEXEC causes the duplicated file descriptor
	// to leak into subprocesses created by exec.Command. If the file descriptor
	// is a pipe, these subprocesses will hold the pipe open (i.e., prevent
	// EOF), potentially beyond the lifetime of this process.
	//
	// This can break go test's timeouts. go test usually spawns a test process
	// with its stdin and stderr streams hooked up to pipes; if the test process
	// times out, it sends a SIGKILL and attempts to read stdin and stderr to
	// completion. If the test process has itself spawned long-lived
	// subprocesses that hold references to the stdin or stderr pipes, go test
	// will hang until the subprocesses exit, rather defeating the purpose of
	// a timeout.
	nfd, _, errno := unix.Syscall(unix.SYS_FCNTL, fd, unix.F_DUPFD_CLOEXEC, 0)
	if errno != 0 {
		return 0, errno
	}
	return nfd, nil
}

func redirectStderr(f *os.File) error {
	return unix.Dup2(int(f.Fd()), unix.Stderr)
}
