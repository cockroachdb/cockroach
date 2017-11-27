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
// implied.  See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Nikhil Benesch (nikhil.benesch@gmail.com)

// libcrc32 implements CRC32, the 32-bit cyclic redundancy check, using the IEEE
// polynomial (0xedb88320 in little-endian). See Wikipedia for details on the
// mathematics: http://en.wikipedia.org/wiki/Cyclic_redundancy_check
//
// For AMD64 platforms with support for both CLMUL and SSE4.1, libcrc32 uses a
// highly-optimized assembly implementation. On other platforms, or on small
// inputs, libcrc32 falls back to a simpler but still fast "slicing by eight"
// implementation. See the comments in the implementation for details.

#ifndef CRC32_H
#define CRC32_H

#include <stddef.h>
#include <stdint.h>

// crc32_fn_t is the function signature of all crc32 implementations.
typedef uint32_t (crc32_fn_t)(uint32_t crc, const unsigned char* buf, size_t len);

// Update the running checksum `crc` with `len` bytes from `buf` using the IEEE
// polynomial and returns the new checksum. To initialize a new running
// checksum, provide a `crc` of 0:
//
//     uint32_t checksum = 0;
//     while ((char* block = next_block())) {
//         checksum = crc32ieee(checksum, block, BLOCKSZ);
//     }
//
// This function is compatible with the `crc32` function provided by zlib. In
// particular, providing a `NULL` buffer will unconditionally return the
// required initial CRC value, 0, regardless of the value of `crc` or `len`.
extern crc32_fn_t *crc32ieee;

#endif
