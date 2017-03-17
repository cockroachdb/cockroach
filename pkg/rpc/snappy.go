// Copyright 2017 The Cockroach Authors.
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

package rpc

import (
	"encoding/binary"
	"errors"
	"io"
	"strconv"
	"unsafe"

	// This is explicit because this Go library does not export any Go symbols.
	_ "github.com/cockroachdb/c-snappy"
)

// #cgo CPPFLAGS: -I../../vendor/github.com/cockroachdb/c-snappy/internal
// #cgo !strictld,darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo !strictld,!darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all
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

type snappyCompressor struct {
}

func (snappyCompressor) Do(w io.Writer, raw []byte) error {
	var dLen C.size_t
	var dst unsafe.Pointer

	cerr := C.snappy_encode((*C.char)(unsafe.Pointer(&raw[0])), C.size_t(len(raw)),
		&dst, &dLen)
	if cerr != C.SNAPPY_OK {
		return snappyError(cerr)
	}

	_, err := w.Write(unsafeSlice(dst, dLen))
	return err
}

// zeroCopyWriter implements io.Writer, optimized for the case where a single
// call to Write is performed. This is not a safe general purpose io.Writer
// because Write is supposed to make a copy of its input in order to allow the
// caller to mutate the buffer it passes after Write returns.
type zeroCopyWriter struct {
	buf []byte
}

func (w *zeroCopyWriter) Write(p []byte) (int, error) {
	n := len(p)
	if w.buf == nil {
		w.buf = p[:n:n]
	} else {
		w.buf = append(w.buf, p...)
	}
	return n, nil
}

func (snappyCompressor) Type() string {
	return "snappy"
}

type snappyDecompressor struct {
}

func (snappyDecompressor) Do(r io.Reader) ([]byte, error) {
	var w zeroCopyWriter
	if _, err := io.Copy(&w, r); err != nil {
		return nil, err
	}

	v, _, err := decodedLen(w.buf)
	if err != nil {
		return nil, err
	}

	uncompressed := make([]byte, v)
	uncompressedLen := C.size_t(len(uncompressed))
	cerr := C.snappy_uncompress(
		(*C.char)(unsafe.Pointer(&w.buf[0])), C.size_t(len(w.buf)),
		(*C.char)(unsafe.Pointer(&uncompressed[0])), &uncompressedLen)
	if cerr != C.SNAPPY_OK {
		return nil, snappyError(cerr)
	}

	return uncompressed, nil
}

func (snappyDecompressor) Type() string {
	return "snappy"
}

// decodedLen returns the length of the decoded block and the number of bytes
// that the length header occupied. From https://github.com/golang/snappy.
func decodedLen(src []byte) (blockLen, headerLen int, err error) {
	v, n := binary.Uvarint(src)
	if n <= 0 || v > 0xffffffff {
		return 0, 0, errors.New("snappy: corrupt input")
	}

	const wordSize = 32 << (^uint(0) >> 32 & 1)
	if wordSize == 32 && v > 0x7fffffff {
		return 0, 0, errors.New("snappy: decoded block is too large")
	}
	return int(v), n, nil
}

func unsafeSlice(p unsafe.Pointer, n C.size_t) []byte {
	const maxLen = 0x7fffffff
	return (*[maxLen]byte)(p)[:n:n]
}
