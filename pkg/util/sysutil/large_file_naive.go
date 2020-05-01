// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !linux

package sysutil

import (
	"os"

	"github.com/cockroachdb/errors"
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
