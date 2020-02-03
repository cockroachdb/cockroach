// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
  // Get the number of bytes written to this chunkedBuffer.
  int NumBytes() const { return bytes_; }

 private:
  void put(const char* data, int len, int next_size_hint);

 private:
  std::vector<DBSlice> bufs_;
  int64_t count_;
  int64_t bytes_;
  char* buf_ptr_;
};

}  // namespace cockroach
