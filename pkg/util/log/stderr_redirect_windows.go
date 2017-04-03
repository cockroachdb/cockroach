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
	"syscall"
)

var (
	kernel32         = syscall.MustLoadDLL("kernel32.dll")
	procSetStdHandle = kernel32.MustFindProc("SetStdHandle")
)

func dupFD(fd uintptr) (uintptr, error) {
	// Adapted from https://github.com/golang/go/blob/go1.8/src/syscall/exec_windows.go#L303.
	p, err := syscall.GetCurrentProcess()
	if err != nil {
		return 0, err
	}
	var h syscall.Handle
	return uintptr(h), syscall.DuplicateHandle(p, syscall.Handle(fd), p, &h, 0, true, syscall.DUPLICATE_SAME_ACCESS)
}

func setStdHandle(nStdHandle int32, hHandle syscall.Handle) error {
	// Adapted from https://github.com/ncw/rclone/blob/v1.36/fs/redirect_stderr_windows.go.
	r0, _, err := procSetStdHandle.Call(uintptr(nStdHandle), uintptr(hHandle))
	if r0 == 0 {
		if err != nil {
			return err
		}
		return syscall.EINVAL
	}
	return nil
}

func redirectStderr(f *os.File) error {
	os.Stderr = f
	return setStdHandle(syscall.STD_ERROR_HANDLE, syscall.Handle(f.Fd()))
}
