// Copyright 2018 The Cockroach Authors.
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

func nonZeroingMakeByteSlice(len int) []byte {
	ptr := mallocgc(uintptr(len), nil, false)
	return (*[MaxArrayLen]byte)(ptr)[:len:len]
}

// Replacement for C.GoBytes which does not zero initialize the returned slice
// before overwriting it.
//
// TODO(peter): Remove when go1.11 is released which has a similar change to
// C.GoBytes.
func gobytes(ptr unsafe.Pointer, len int) []byte {
	if len == 0 {
		return make([]byte, 0)
	}
	x := nonZeroingMakeByteSlice(len)
	src := (*[MaxArrayLen]byte)(ptr)[:len:len]
	copy(x, src)
	return x
}
