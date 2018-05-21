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

// +build linux

package sysutil

import (
	"os"
	"syscall"

	"github.com/pkg/errors"
)

// CreateLargeFile creates a large file at the given path with bytes size.
// It uses fallocate syscall to create file of given size.
func CreateLargeFile(path string, bytes int64) (err error) {
	f, err := os.Create(path)
	if err != nil {
		return errors.Wrapf(err, "failed to create file %s", path)
	}
	defer f.Close()
	if err := syscall.Fallocate(int(f.Fd()), 0, 0, bytes); err != nil {
		return err
	}
	return f.Sync()
}
