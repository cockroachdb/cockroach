// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)
// Author: Peter Mattis (peter.mattis@gmail.com)

package engine

// #include <stdlib.h>
// #include "roach_c.h"
import "C"
import (
	"unsafe"

	"github.com/cockroachdb/cockroach/util"
)

// goMerge takes existing and update byte slices that are expected to
// be marshalled proto.Values and merges the two values returning a
// marshalled proto.Value or an error.
func goMerge(existing, update []byte) ([]byte, error) {
	var cValLen C.size_t
	var cErr *C.char
	cVal := C.MergeOne(
		bytesPointer(existing), C.size_t(len(existing)),
		bytesPointer(update), C.size_t(len(update)),
		&cValLen, &cErr)
	if cErr != nil {
		return nil, util.ErrorSkipFrames(0, C.GoString(cErr))
	}
	if cVal != nil {
		defer C.free(unsafe.Pointer(cVal))
	}
	return C.GoBytes(unsafe.Pointer(cVal), C.int(cValLen)), nil
}
