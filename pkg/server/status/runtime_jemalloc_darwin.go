// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !stdmalloc && darwin

package status

// extern void je_zone_register();
import "C"

func init() {
	// On macOS, je_zone_register is run at init time to register jemalloc with
	// the system allocator. Unfortunately, all the machinery for this is in a
	// single file, and is not referenced elsewhere, causing the linker to omit
	// the file's symbols. Manually force the presence of these symbols on macOS
	// by explicitly calling this method.
	//
	// Note that the same could be achieved via linker flags, but go >= 1.9.4
	// requires explicit opt-in for the required flag, which we want to avoid
	// having to put up with.
	//
	// See https://github.com/jemalloc/jemalloc/issues/708 and the references
	// within.
	C.je_zone_register()
}
