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

// +build !windows

//lint:file-ignore Unconvert (redundant conversions are necessary for cross-platform compatibility)

package sysutil

import (
	"fmt"
	"math"

	"golang.org/x/sys/unix"
)

// ProcessIdentity returns a string describing the user and group that this
// process is running as.
func ProcessIdentity() string {
	return fmt.Sprintf("uid %d euid %d gid %d egid %d",
		unix.Getuid(), unix.Geteuid(), unix.Getgid(), unix.Getegid())
}

// StatFS returns an FSInfo describing the named filesystem. It is only
// supported on Unix-like platforms.
func StatFS(path string) (*FSInfo, error) {
	var fs unix.Statfs_t
	if err := unix.Statfs(path, &fs); err != nil {
		return nil, err
	}
	// Statfs_t's fields have different types on different platforms. Our FSInfo
	// type uses int64s for all fields, so make sure the values returned by the OS
	// will fit.
	if uint64(fs.Bfree) > math.MaxInt64 ||
		uint64(fs.Bavail) > math.MaxInt64 ||
		uint64(fs.Blocks) > math.MaxInt64 ||
		uint64(fs.Bsize) > math.MaxInt64 {
		return nil, fmt.Errorf("statfs syscall returned unrepresentable value %#v", fs)
	}
	return &FSInfo{
		FreeBlocks:  int64(fs.Bfree),
		AvailBlocks: int64(fs.Bavail),
		TotalBlocks: int64(fs.Blocks),
		BlockSize:   int64(fs.Bsize),
	}, nil
}
