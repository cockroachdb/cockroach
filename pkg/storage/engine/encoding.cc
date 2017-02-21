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
// implied.  See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Peter Mattis (peter@cockroachlabs.com)

#include "rocksdb/slice.h"
#include "encoding.h"

// TODO(pmattis): These functions are not tested. Doing so is made
// difficult by "go test" because _test.go files cannot 'import "C"'.

void EncodeUint32(std::string* buf, uint32_t v) {
  const uint8_t tmp[sizeof(v)] = {
    uint8_t(v >> 24), uint8_t(v >> 16), uint8_t(v >> 8), uint8_t(v),
  };
  buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
}

void EncodeUint64(std::string* buf, uint64_t v) {
  const uint8_t tmp[sizeof(v)] = {
    uint8_t(v >> 56), uint8_t(v >> 48), uint8_t(v >> 40), uint8_t(v >> 32),
    uint8_t(v >> 24), uint8_t(v >> 16), uint8_t(v >> 8), uint8_t(v),
  };
  buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
}

bool DecodeUint32(rocksdb::Slice* buf, uint32_t* value) {
  const int N = sizeof(*value);
  if (buf->size() < N) {
    return false;
  }
  const uint8_t* b = reinterpret_cast<const uint8_t*>(buf->data());
  *value = (uint32_t(b[0]) << 24) | (uint32_t(b[1]) << 16) |
      (uint32_t(b[2]) << 8) | uint32_t(b[3]);
  buf->remove_prefix(N);
  return true;
}

bool DecodeUint64(rocksdb::Slice* buf, uint64_t* value) {
  const int N = sizeof(*value);
  if (buf->size() < N) {
    return false;
  }
  const uint8_t* b = reinterpret_cast<const uint8_t*>(buf->data());
  *value = (uint64_t(b[0]) << 56) | (uint64_t(b[1]) << 48) |
      (uint64_t(b[2]) << 40) | (uint64_t(b[3]) << 32) |
      (uint64_t(b[4]) << 24) | (uint64_t(b[5]) << 16) |
      (uint64_t(b[6]) << 8) | uint64_t(b[7]);
  buf->remove_prefix(N);
  return true;
}
