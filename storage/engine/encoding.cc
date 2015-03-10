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

}  // namespace

bool DecodeBytes(const rocksdb::Slice& buf, std::string* decoded) {
  int copyStart = 0;
  for (int i = 0, n = int(buf.size()) - 1; i < n; ++i) {
    unsigned char v = buf[i];
    if (v == kEscape) {
      decoded->append(buf.data() + copyStart, i-copyStart);
      v = buf[++i];
      if (v == kEscapedTerm) {
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
