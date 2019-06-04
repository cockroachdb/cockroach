// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

// +build !stdmalloc,darwin

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
