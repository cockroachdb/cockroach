// Copyright 2018 The Cockroach Authors.
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

#pragma once

#include <rocksdb/db.h>
#include "libroach.h"

namespace cockroach {

class chunkedBuffer {
 public:
  chunkedBuffer() { Clear(); }
  ~chunkedBuffer() { Clear(); }

  // Write a key/value pair to this chunkedBuffer.
  void Put(const rocksdb::Slice& key, const rocksdb::Slice& value);

  // Clear this chunkedBuffer.
  void Clear();

  void GetChunks(DBSlice** bufs, int32_t* len) {
    // Cap the last buffer's size to the amount that's been written to it.
    DBSlice& last = bufs_.back();
    last.len = buf_ptr_ - last.data;
    *bufs = &bufs_.front();
    *len = bufs_.size();
  }

  // Get the number of key/value pairs written to this chunkedBuffer.
  int Count() const { return count_; }

 private:
  void put(const char* data, int len, int next_size_hint);

 private:
  std::vector<DBSlice> bufs_;
  int64_t count_;
  char* buf_ptr_;
};

}  // namespace cockroach
