// Copyright 2018 The Cockroach Authors.
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

package engine

import "unsafe"

func nonZeroingMakeByteSlice(len int) []byte {
	ptr := mallocgc(uintptr(len), nil, false)
	return (*[maxArrayLen]byte)(ptr)[:len:len]
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
	src := (*[maxArrayLen]byte)(ptr)[:len:len]
	copy(x, src)
	return x
}
