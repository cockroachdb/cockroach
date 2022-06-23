// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

// fdGetter is an interface for a file with an Fd() method. A lot of
// File related optimizations (eg. Prefetch(), WAL recycling) rely on the
// existence of the Fd method to return a raw file descriptor.
type fdGetter interface {
	Fd() uintptr
}

// fdFileWrapper is a File wrapper that also exposes an Fd() method. Used to
// wrap outer (wrapped) Files that could unintentionally hide the Fd() method
// exposed by the inner (unwrapped) File. It effectively lets the Fd() method
// bypass the outer File and go to the inner File.
type fdFileWrapper struct {
	File

	// All methods usually pass through to File above, except for Fd(), which
	// bypasses it and gets called directly on the inner file.
	inner fdGetter
}

func (f *fdFileWrapper) Fd() uintptr {
	return f.inner.Fd()
}

// WithFd takes an inner (unwrapped) and an outer (wrapped) vfs.File,
// and returns an fdFileWrapper if the inner file has an Fd() method. Use this
// method to fix the hiding of the Fd() method and the subsequent unintentional
// disabling of Fd-related file optimizations.
func WithFd(inner, outer File) File {
	if f, ok := inner.(fdGetter); ok {
		return &fdFileWrapper{
			File:  outer,
			inner: f,
		}
	}
	return outer
}
