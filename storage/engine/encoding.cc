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
// Author: Peter Mattis (petermattis@gmail.com)

#include "rocksdb/slice.h"

namespace {

const unsigned char kEscape      = 0x00;
const unsigned char kEscapedTerm = 0x01;
const unsigned char kEscapedNul  = 0xff;

template <typename T>
bool DecodeUvarint(rocksdb::Slice* buf, T* value) {
  if (buf->empty()) {
    return false;
  }
  int len = (*buf)[0] - 8;
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

bool DecodeBytes(rocksdb::Slice* buf, std::string* decoded) {
  int copyStart = 0;
  for (int i = 0, n = int(buf->size()) - 1; i < n; ++i) {
    unsigned char v = (*buf)[i];
    if (v == kEscape) {
      decoded->append(buf->data() + copyStart, i-copyStart);
      v = (*buf)[++i];
      if (v == kEscapedTerm) {
        buf->remove_prefix(i + 1);
        return true;
      }
      if (v == kEscapedNul) {
        decoded->append("\0", 1);
      }
      copyStart = i + 1;
    }
  }
  return false;
}

bool DecodeUvarint64(rocksdb::Slice* buf, uint64_t* value) {
  return DecodeUvarint(buf, value);
}
