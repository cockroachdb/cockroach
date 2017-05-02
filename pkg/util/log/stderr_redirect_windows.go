// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package log

import (
	"os"

	"golang.org/x/sys/windows"
)

var (
	kernel32         = windows.MustLoadDLL("kernel32.dll")
	procSetStdHandle = kernel32.MustFindProc("SetStdHandle")
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
