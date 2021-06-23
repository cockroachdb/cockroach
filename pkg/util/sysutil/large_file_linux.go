// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build linux

package sysutil

import (
	"os"

	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
)

// ResizeLargeFile resizes the file at the given path to be the provided
// length in bytes. If no file exists at path, ResizeLargeFile creates a file.
// All disk blocks within the new file are allocated, and there are no sparse
// regions.
//
// On Linux, it uses the fallocate syscall to efficiently allocate disk space.
// On other platforms, it naively writes the specified number of bytes, which
// can take a long time.
func ResizeLargeFile(path string, bytes int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	// Ensure that at least length bytes of the file are allocated. If the
	// file already existed, the disk blocks may not have been allocated even
	// if the file size is greater than length.
	if bytes > 0 {
		if err := unix.Fallocate(int(f.Fd()), 0, 0, bytes); err != nil {
			return errors.Wrap(err, "fallocate")
		}
	}

	// Truncate down to bytes, in case the file is longer than bytes. This
	// will be a no-op if the file is already at the desired length.
	if err := unix.Ftruncate(int(f.Fd()), bytes); err != nil {
		return errors.Wrap(err, "ftruncate")
	}
	return errors.Wrap(f.Sync(), "fsync")
}
