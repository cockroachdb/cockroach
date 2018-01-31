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

#include <memory>
#include "encoding.h"
#include <rocksdb/iterator.h>
#include <rocksdb/write_batch.h>

namespace cockroach {
// Our returned batches have a fixed-size 4-byte header containing the count
// of returned keys.
static const int kHeaderSize = 4;

class buffer {
  public:
    buffer()
      : keys_(0) {

      DBSlice initSlice;
      initSlice.data = initBuf_;
      initSlice.len = 16;
      bufs_.push_back(initSlice);
      bufPtr_ = initBuf_ + kHeaderSize;
    }

    void put(const char *data, int len, int nextSizeHint) {
      DBSlice curBuf = bufs_.back();
      int expectedLen = len + bufPtr_ - curBuf.data;
      int remainder = expectedLen - curBuf.len;
      if (remainder > 0) {
        // If it's bigger than the last buf's capacity, we fill the last buf,
        // allocate a new one, and write the remainder to the new one.
        // Our new buf's size will be the next power of two past the size of the
        // last buf that can accomodate the new data, plus a size hint if
        // available.
        memcpy(bufPtr_, data, len - remainder);
        int newSize = curBuf.len << 1;
        for ( ; newSize < len + nextSizeHint; newSize = newSize << 1);

        DBSlice newBuf;
        newBuf.data = new char[newSize];
        newBuf.len = newSize;
        bufs_.push_back(newBuf);

        // Now reset so that we'll write the remainder below.
        bufPtr_ = newBuf.data;
        data += len - remainder;
        len = remainder;
      }
      memcpy(bufPtr_, data, len);
      bufPtr_ += len;
    }

    // putLengthPrefixedSlice writes the input value to the iterator's output
    // slice, prefixed by the value's length encoded as a Varint32. This matches
    // the RocksDB WriteBatch format *without* the leading 1-byte value tag.
    void putLengthPrefixedSlice(const rocksdb::Slice& value) {
      // Encode the value length into a buffer to determine the size of the length
      // encoding.
      char buf[5];
      char* ptr = EncodeVarint32(buf, value.size());
      int putSize = ptr - buf;
      put(buf, putSize, value.size());
      put(value.data(), value.size(), 0);
    }

    int Count() {
      return keys_;
    }

    void Put(const rocksdb::Slice& key, const rocksdb::Slice& value) {
      putLengthPrefixedSlice(key);
      putLengthPrefixedSlice(value);
      keys_++;
      // TODO(jordan) we can probably do this just once at the end.
      memcpy(&initBuf_, &keys_, sizeof(keys_));
    }

  public:
    char initBuf_[16];
    int keys_;
    std::vector<DBSlice> bufs_;
    char *bufPtr_;
};

};


struct DBIterator {
  std::unique_ptr<rocksdb::Iterator> rep;
  std::unique_ptr<cockroach::buffer> kvs;
  std::unique_ptr<rocksdb::WriteBatch> intents;
};
