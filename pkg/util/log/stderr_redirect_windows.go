// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"os"

	"golang.org/x/sys/windows"
)

func dupFD(fd uintptr) (uintptr, error) {
	// Adapted from https://github.com/golang/go/blob/go1.8/src/syscall/exec_windows.go#L303.
	p, err := windows.GetCurrentProcess()
	if err != nil {
		return 0, err
	}
	var h windows.Handle
	return uintptr(h), windows.DuplicateHandle(p, windows.Handle(fd), p, &h, 0, true, windows.DUPLICATE_SAME_ACCESS)
}

func redirectStderr(f *os.File) error {
	os.Stderr = f
	return windows.SetStdHandle(windows.STD_ERROR_HANDLE, windows.Handle(f.Fd()))
}
