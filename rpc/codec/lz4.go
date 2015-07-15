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
// Author: Peter Mattis (peter@cockroachlabs.com)

package codec

import (
	"unsafe"

	// Link against the lz4 library. This is explicit because this Go
	// library does not export any Go symbols.
	_ "github.com/cockroachdb/c-lz4"
	"github.com/gogo/protobuf/proto"
)

// #cgo CPPFLAGS: -I ../../../c-lz4/internal/lib
// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all
//
// #include <stdlib.h>
// #include <lz4.h>
//
// int lz4_encode(const char* input,
//                int inputLength,
//                void** compressed) {
//   int compressBound = LZ4_compressBound(inputLength);
//   *compressed = malloc(compressBound);
//   int outputLength = LZ4_compress_limitedOutput(
//       input, *compressed, inputLength, compressBound);
//   if (outputLength == 0 || outputLength == inputLength) {
//     free(*compressed);
//   }
//   return outputLength;
// }
//
// int lz4_decode(const char* compressed,
//                int compressed_length,
//                void** uncompressed,
//                int uncompressed_length) {
//   *uncompressed = malloc(uncompressed_length);
//   int outputLength = LZ4_decompress_safe(
//       compressed, *uncompressed, compressed_length, uncompressed_length);
//   if (outputLength < 0) {
//     free(*uncompressed);
//   }
//   return outputLength;
// }
import "C"

// lz4Encode compresses the byte array src and sends the compressed
// data to w.
func lz4Encode(src []byte, w func([]byte) error) error {
	if len(src) == 0 {
		return w(nil)
	}

	var dst unsafe.Pointer
	dLen := C.lz4_encode((*C.char)(unsafe.Pointer(&src[0])), C.int(len(src)), &dst)
	if dLen == 0 || int(dLen) == len(src) {
		// The input was not compressed or compressed to exactly the same
		// length as the source. Either way we treat the input as not
		// compressible. See lz4Decode() for the decode-side handling.
		return w(src)
	}

	err := w(unsafeSlice(dst, C.size_t(dLen)))
	C.free(dst)
	return err
}

// lz4Decode uncompresses the byte array src and unmarshals the
// uncompressed data into m.
func lz4Decode(src []byte, uncompressedSize uint32, m proto.Message) error {
	if len(src) == 0 || len(src) == int(uncompressedSize) {
		// len(src) == int(uncompressedSize) indicates the data was not
		// compressible.
		return nilSafeUnmarshal(src, m)
	}

	var dst unsafe.Pointer
	dLen := C.lz4_decode((*C.char)(unsafe.Pointer(&src[0])),
		C.int(len(src)), &dst, C.int(uncompressedSize))
	if uint32(dLen) != uncompressedSize {
		return errInvalidInput
	}

	// We call through directly to proto.Unmarshal so that we don't have
	// to allocate a slice for "dst" or have some awkward interface
	// where the caller has to deallocate "dst".
	err := nilSafeUnmarshal(unsafeSlice(dst, C.size_t(dLen)), m)
	C.free(dst)
	return err
}
