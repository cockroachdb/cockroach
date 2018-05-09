// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package sysutil is a cross-platform compatibility layer on top of package
// syscall. It exposes APIs for common operations that require package syscall
// and re-exports several symbols from package syscall that are known to be
// safe. Using package syscall directly from other packages is forbidden.

// +build !linux

package sysutil

import (
	"syscall"

	"github.com/pkg/errors"
)

// Fallocate emulates fallocate in non-linux platforms. It doesn't support mode and offset.
func Fallocate(fd int, mode uint32, off int64, len int64) (err error) {
	_ = mode // not supporting modes in fallocate, as there is no use case till now
	_ = off  // not supporting offset in fallocate, as there is no use case till now
	bytesToWrite := make([]byte, len)
	if err != nil {
		return err
	}
	n, err := syscall.Write(fd, bytesToWrite)
	if err != nil {
		return err
	}
	if int64(n) != len {
		return errors.Errorf("Failed to generate write length %d to fd %d", len, fd)
	}
	return nil
}
