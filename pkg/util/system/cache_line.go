// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package system

import (
	"unsafe"

	"golang.org/x/sys/cpu"
)

// CacheLineSize is the size of a CPU cache line in bytes, or a non-zero
// estimate if the size is unknown.
const CacheLineSize = max(
	int(unsafe.Sizeof(cpu.CacheLinePad{})),
	// The size of the CacheLinePad struct is 0 on some software-abstracted
	// platforms, like Wasm, where the cache line size of the underlying CPU is
	// not known. In such cases, use a reasonable default value of 64 bytes.
	//
	// NOTE: we use max instead of an init-time comparison so that CacheLineSize
	// can be used in const expressions. The subtraction here drives this term
	// below 0 to be ignored by max in all cases except when the size of the
	// CacheLinePad struct is 0.
	64-65*int(unsafe.Sizeof(cpu.CacheLinePad{})),
)
