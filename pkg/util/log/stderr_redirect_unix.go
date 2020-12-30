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

// dupFD is used to initialize OrigStderr (see stderr_redirect.go).
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
	nfd, err := unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return 0, err
	}
	return uintptr(nfd), nil
}

// redirectStderr is used to redirect internal writes to fd 2 to the
// specified file. This is needed to ensure that harcoded writes to fd
// 2 by e.g. the Go runtime are redirected to a log file of our
// choosing.
//
// We also override os.Stderr for those other parts of Go which use
// that and not fd 2 directly.
func redirectStderr(f *os.File) error {
	osStderrMu.Lock()
	defer osStderrMu.Unlock()
	if err := unix.Dup2(int(f.Fd()), unix.Stderr); err != nil {
		return err
	}
	os.Stderr = f
	return nil
}
