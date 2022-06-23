// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build !linux
// +build !linux

package vfs

// Prefetch signals the OS (on supported platforms) to fetch the next size
// bytes in file (as returned by os.File.Fd()) after offset into cache. Any
// subsequent reads in that range will not issue disk IO.
func Prefetch(file uintptr, offset uint64, size uint64) error {
	// No-op.
	return nil
}
