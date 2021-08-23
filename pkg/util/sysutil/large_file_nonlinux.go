// Copyright 2021 The Cockroach Authors.
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

// ResizeLargeFile resizes the file at the given path to be the provided
// length in bytes. If no file exists at path, ResizeLargeFile creates a file.
// All disk blocks within the new file are allocated, and there are no sparse
// regions.
//
// On Linux, it uses the fallocate syscall to efficiently allocate disk space.
// On other platforms, it naively writes the specified number of bytes, which
// can take a long time.
func ResizeLargeFile(path string, bytes int64) error {
	return resizeLargeFileNaive(path, bytes)
}
