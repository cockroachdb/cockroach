// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

// +build !stdmalloc

package cli

// #cgo darwin CPPFLAGS: -I../../c-jemalloc/darwin_includes/internal/include
// #cgo freebsd CPPFLAGS: -I../../c-jemalloc/freebsd_includes/internal/include
// #cgo linux CPPFLAGS: -I../../c-jemalloc/linux_includes/internal/include
// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo linux LDFLAGS: -Wl,-unresolved-symbols=ignore-all
//
// #include <jemalloc/jemalloc.h>
// #include <stddef.h>
//
// // Checks whether jemalloc profiling is enabled and active.
// // Returns true if profiling is enabled and active.
// // Returns false on any mallctl errors.
// bool is_profiling_enabled() {
//   bool enabled = false;
//   size_t enabledSize = sizeof(enabled);
//
//   // Check profiling flag.
//   if (mallctl("opt.prof", &enabled, &enabledSize, NULL, 0) != 0) {
//     return false;
//   }
//   if (!enabled) {
//     return false;
//   }
//
//   // Check prof_active flag.
//   if (mallctl("opt.prof_active", &enabled, &enabledSize, NULL, 0) != 0) {
//     return false;
//   }
//   return enabled;
// }
//
// // Write a heap profile to "filename". Returns true on success, false on error.
// int dump_heap_profile(const char *filename) {
//   return mallctl("prof.dump", NULL, NULL, &filename, sizeof(const char *));
// }
import "C"

import (
	// This is explicit because this Go library does not export any Go symbols.
	_ "github.com/cockroachdb/c-jemalloc"

	"fmt"
	"unsafe"
)

func init() {
	if C.is_profiling_enabled() {
		if jemallocHeapDump != nil {
			panic("jemallocHeapDump is already set")
		}
		jemallocHeapDump = writeJemallocProfile
	}
}

// writeJemallocProfile tells jemalloc to write a heap profile to 'filename'.
// It assumes that profiling is enabled and active.
func writeJemallocProfile(filename string) error {
	cpath := C.CString(filename)
	defer C.free(unsafe.Pointer(cpath))

	if errCode := C.dump_heap_profile(cpath); errCode != 0 {
		return fmt.Errorf("error code %d", errCode)
	}
	return nil
}
