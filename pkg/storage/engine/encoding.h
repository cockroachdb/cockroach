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
// implied.  See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

#ifndef ROACHLIB_ENCODING_H
#define ROACHLIB_ENCODING_H

#include <stdint.h>

// EncodeUint32 encodes the uint32 value using a big-endian 4 byte
// representation. The bytes are appended to the supplied buffer.
void EncodeUint32(std::string* buf, uint32_t v);

// EncodeUint64 encodes the uint64 value using a big-endian 8 byte
// representation. The encoded bytes are appended to the supplied buffer.
void EncodeUint64(std::string* buf, uint64_t v);

// DecodedUint32 decodes a fixed-length encoded uint32 from a buffer, returning
// true on a successful decode. The decoded value is returned in *value.
bool DecodeUint32(rocksdb::Slice* buf, uint32_t* value);

// DecodedUint64 decodes a fixed-length encoded uint64 from a buffer, returning
// true on a successful decode. The decoded value is returned in *value.
bool DecodeUint64(rocksdb::Slice* buf, uint64_t* value);

#endif // ROACHLIB_ENCODING_H

// local variables:
// mode: c++
// end:
