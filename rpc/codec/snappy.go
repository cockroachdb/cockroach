// Copyright 2015 The Cockroach Authors.
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
// Author: Peter Mattis (peter.mattis@gmail.com)

package codec

import (
	"strconv"
	"unsafe"

	// Link against the snappy library. This is explicit because this Go
	// library does not export any Go symbols.
	_ "github.com/cockroachdb/c-snappy"
	"github.com/gogo/protobuf/proto"
)

// #cgo CPPFLAGS: -I ../../../c-snappy/internal
// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all
//
// #include <stdlib.h>
// #include <snappy-c.h>
//
// snappy_status snappy_encode(const char* input,
//                             size_t input_length,
//                             void** compressed,
//                             size_t* compressed_length) {
//   *compressed_length = snappy_max_compressed_length(input_length);
//   *compressed = malloc(*compressed_length);
//   snappy_status status = snappy_compress(input, input_length, *compressed, compressed_length);
//   if (status != SNAPPY_OK) {
//     free(*compressed);
//   }
//   return status;
// }
//
// snappy_status snappy_decode(const char* compressed,
//                             size_t compressed_length,
//                             void** uncompressed,
//                             size_t* uncompressed_length) {
//   snappy_status status = snappy_uncompressed_length(compressed, compressed_length, uncompressed_length);
//   if (status != SNAPPY_OK) {
//     return status;
//   }
//   *uncompressed = malloc(*uncompressed_length);
//   status = snappy_uncompress(compressed, compressed_length, *uncompressed, uncompressed_length);
//   if (status != SNAPPY_OK) {
//     free(*uncompressed);
//   }
//   return status;
// }
import "C"

type snappyError int

var errText = map[snappyError]string{
	errOK:             "ok",
	errInvalidInput:   "invalid input",
	errBufferTooSmall: "buffer too small",
}

func (e snappyError) Error() string {
	s := errText[e]
	if s == "" {
		return "snappy errno " + strconv.Itoa(int(e))
	}
	return s
}

var (
	errOK             = snappyError(0)
	errInvalidInput   = snappyError(1)
	errBufferTooSmall = snappyError(2)
)

// snappyEncode compresses the byte array src and sends the compressed
// data to w.
func snappyEncode(src []byte, w func([]byte) error) error {
	if len(src) == 0 {
		return w(nil)
	}

	var dLen C.size_t
	var dst unsafe.Pointer

	cerr := C.snappy_encode((*C.char)(unsafe.Pointer(&src[0])), C.size_t(len(src)),
		&dst, &dLen)
	if cerr != C.SNAPPY_OK {
		return snappyError(cerr)
	}

	err := w(unsafeSlice(dst, dLen))
	C.free(dst)
	return err
}

// snappyDecode uncompresses the byte array src and unmarshals the
// uncompressed data into m.
func snappyDecode(src []byte, uncompressedSize uint32, m proto.Message) error {
	if len(src) == 0 {
		return proto.Unmarshal(nil, m)
	}

	var dLen C.size_t
	var dst unsafe.Pointer

	cerr := C.snappy_decode((*C.char)(unsafe.Pointer(&src[0])),
		C.size_t(len(src)), &dst, &dLen)
	if cerr != C.SNAPPY_OK {
		return snappyError(cerr)
	}

	// We call through directly to proto.Unmarshal so that we don't have
	// to allocate a slice for "dst" or have some awkward interface
	// where the caller has to deallocate "dst".
	err := proto.Unmarshal(unsafeSlice(dst, dLen), m)
	C.free(dst)
	return err
}

func unsafeSlice(p unsafe.Pointer, n C.size_t) []byte {
	const maxLen = 0x7fffffff
	return (*[maxLen]byte)(p)[:n:n]
}
