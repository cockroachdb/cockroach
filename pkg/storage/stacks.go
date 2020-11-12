// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import "unsafe"

// #cgo CPPFLAGS: -I../../c-deps/libroach/include
// #cgo LDFLAGS: -lroach
//
// #include <stdlib.h>
// #include <libroach.h>
import "C"

func cSliceToUnsafeGoBytes(s C.DBSlice) []byte {
	if s.data == nil {
		return nil
	}
	// Interpret the C pointer as a pointer to a Go array, then slice.
	return (*[MaxArrayLen]byte)(unsafe.Pointer(s.data))[:s.len:s.len]
}

func cStringToGoString(s C.DBString) string {
	if s.data == nil {
		return ""
	}
	// Reinterpret the string as a slice, then cast to string which does a copy.
	result := string(cSliceToUnsafeGoBytes(C.DBSlice(s)))
	C.free(unsafe.Pointer(s.data))
	return result
}

// ThreadStacks returns the stacks for all threads. The stacks are raw
// addresses, and do not contain symbols. Use addr2line (or atos on Darwin) to
// symbolize.
func ThreadStacks() string {
	return cStringToGoString(C.DBDumpThreadStacks())
}
