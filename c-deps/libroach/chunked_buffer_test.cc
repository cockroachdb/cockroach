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

#include <gtest/gtest.h>
#include "chunked_buffer.h"

// Verify we can a chunked buffer can hold more than 2GB of data when
// writing in pieces that are smaller than the maximum chunk size (128
// MB). See #32896.
TEST(ChunkedBuffer, PutSmall) {
  const std::string data(1 << 20, '.'); // 1 MB
  const int64_t total = 3LL << 30;      // 3 GB
  cockroach::chunkedBuffer buf;
  for (int64_t sum = 0; sum < total; sum += data.size()) {
    buf.Put(data, data);
  }
}

// Verify we can a chunked buffer can hold more than 2GB of data when
// writing in pieces that are larger than the maximum chunk size (128
// MB). See #32896.
TEST(ChunkedBuffer, PutLarge) {
  const std::string data(256 << 20, '.'); // 256 MB
  const int64_t total = 3LL << 30;        // 3 GB
  cockroach::chunkedBuffer buf;
  for (int64_t sum = 0; sum < total; sum += data.size()) {
    buf.Put(data, data);
  }
}
