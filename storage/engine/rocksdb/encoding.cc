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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Peter Mattis (peter@cockroachlabs.com)

#include "rocksdb/slice.h"

namespace {

const uint8_t kEscape      = 0x00;
const uint8_t kEscapedTerm = 0x01;
const uint8_t kEscapedNul  = 0xff;
const uint8_t kMarker      = 0x31;

template <typename T>
bool DecodeUvarint(rocksdb::Slice* buf, T* value) {
  if (buf->empty()) {
    return false;
  }
  int len = (*buf)[0] - 10;
  if (len < 0) {
    return false;
  }
  if ((len + 1) > buf->size()) {
    return false;
  }
  if (len > sizeof(T)) {
    // Decoded value will overflow.
    return false;
  }

  *value = 0;
  const uint8_t* ptr = reinterpret_cast<const uint8_t*>(buf->data()) + len;
  for (int i = 0; i < len; ++i) {
    *value |= static_cast<T>(*ptr--) << (i * 8);
  }
  buf->remove_prefix(len + 1);

  return true;
}

}  // namespace

// TODO(pmattis): These functions are not tested. Doing so is made
// difficult by "go test" because _test.go files cannot 'import "C"'.
bool DecodeBytes(rocksdb::Slice* buf, std::string* decoded) {
  const uint8_t *data = reinterpret_cast<const uint8_t*>(buf->data());
  if (buf->size() == 0 || data[0] != kMarker) {
    return false;
  }
  int copyStart = 1;
  for (int i = 1, n = int(buf->size()) - 1; i < n; ++i) {
    uint8_t v = data[i];
    if (v == kEscape) {
      decoded->append(buf->data() + copyStart, i-copyStart);
      v = data[++i];
      if (v == kEscapedTerm) {
        buf->remove_prefix(i + 1);
        return true;
      }
      if (v != kEscapedNul) {
        return false;
      }
      decoded->append("\0", 1);
      copyStart = i + 1;
    }
  }
  return false;
}

bool DecodeUvarint64(rocksdb::Slice* buf, uint64_t* value) {
  return DecodeUvarint(buf, value);
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

bool DecodeUint64Decreasing(rocksdb::Slice* buf, uint64_t* value) {
  if (!DecodeUint64(buf, value)) {
    return false;
  }
  *value = ~*value;
  return true;
}
