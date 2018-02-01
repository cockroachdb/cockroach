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

#include "chunked_buffer.h"
#include <rocksdb/db.h>
#include "encoding.h"

namespace cockroach {

// Write a key/value pair to this chunkedBuffer.
void chunkedBuffer::Put(const rocksdb::Slice& key, const rocksdb::Slice& value) {
  putLengthPrefixedSlice(key);
  putLengthPrefixedSlice(value);
  count_++;
}

void chunkedBuffer::Clear() {
  for (int i = 0; i < bufs_.size(); i++) {
    delete[] bufs_[i].data;
  }
  count_ = 0;
  buf_ptr_ = nullptr;
  bufs_.clear();
}

// put writes len bytes of the input data to this vector of buffers,
// allocating new buffers if necessary. next_size_hint can be passed to
// indicate that the required size of this buffer will soon be
// len+next_size_hint, to prevent excessive resize operations.
void chunkedBuffer::put(const char* data, int len, int next_size_hint) {
  const int avail = bufs_.empty() ? 0 :
      (bufs_.back().len - (buf_ptr_ - bufs_.back().data));
  if (len > avail) {
    // If it's bigger than the last buf's capacity, we fill the last buf,
    // allocate a new one, and write the remainder to the new one.  Our new
    // buf's size will be the next power of two past the size of the last buf
    // that can accomodate the new data, plus a size hint if available.
    memcpy(buf_ptr_, data, avail);
    data += avail;
    len -= avail;

    int new_size = bufs_.empty() ? 16 : bufs_.back().len * 2;
    for (; new_size < len + next_size_hint; new_size *= 2) {
    }

    DBSlice new_buf;
    new_buf.data = new char[new_size];
    new_buf.len = new_size;
    bufs_.push_back(new_buf);

    // Now reset so that we'll write the remainder below.
    buf_ptr_ = new_buf.data;
  }

  memcpy(buf_ptr_, data, len);
  buf_ptr_ += len;
}

// putLengthPrefixedSlice writes the input value to the iterator's output
// slice, prefixed by the value's length encoded as a Varint32. This matches
// the RocksDB WriteBatch format *without* the leading 1-byte value tag.
void chunkedBuffer::putLengthPrefixedSlice(const rocksdb::Slice& value) {
  // Encode the value length into a buffer to determine the size of the length
  // encoding.
  char buf[5];
  char* ptr = EncodeVarint32(buf, value.size());
  const int put_size = ptr - buf;
  put(buf, put_size, value.size());
  put(value.data(), value.size(), 0);
}

}  // namespace cockroach
