// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"os"

	"golang.org/x/sys/windows"
)

// dupFD is used to initialize OrigStderr (see stderr_redirect.go).
func dupFD(fd uintptr) (uintptr, error) {
	// Adapted from https://github.com/golang/go/blob/go1.8/src/syscall/exec_windows.go#L303.
	p := windows.CurrentProcess()
	var h windows.Handle
	return uintptr(h), windows.DuplicateHandle(p, windows.Handle(fd), p, &h, 0, true, windows.DUPLICATE_SAME_ACCESS)
}

// redirectStderr is used to redirect internal writes to the error
// handle to the specified file. This is needed to ensure that
// harcoded writes to the error handle by e.g. the Go runtime are
// redirected to a log file of our choosing.
//
// We also override os.Stderr for those other parts of Go which use
// that and not fd 2 directly.
func redirectStderr(f *os.File) error {
	osStderrMu.Lock()
	defer osStderrMu.Unlock()
	if err := windows.SetStdHandle(windows.STD_ERROR_HANDLE, windows.Handle(f.Fd())); err != nil {
		return err
	}
	os.Stderr = f
	return nil
}
