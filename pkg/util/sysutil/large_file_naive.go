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

// +build !linux

package sysutil

import (
	"os"

	"github.com/pkg/errors"
)

// CreateLargeFile creates a large file at the given path with bytes size. On
// Linux, it uses the fallocate syscall to efficiently create the file. On other
// platforms, it naively writes the specified number of bytes, which can take a
// long time when the number of bytes is large.
func CreateLargeFile(path string, bytes int64) error {
	f, err := os.Create(path)
	if err != nil {
		return errors.Wrapf(err, "failed to create file %s", path)
	}
	defer f.Close()
	sixtyFourMB := make([]byte, 64<<20)
	for bytes > 0 {
		z := sixtyFourMB
		if bytes < int64(len(z)) {
			z = sixtyFourMB[:bytes]
		}
		if _, err := f.Write(z); err != nil {
			return err
		}
		bytes -= int64(len(z))
	}
	return f.Sync()
}
