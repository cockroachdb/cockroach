// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gtest/gtest.h>
#include "chunked_buffer.h"

// Verify we can a chunked buffer can hold more than 2GB of data when
// writing in pieces that are smaller than the maximum chunk size (128
// MB). See #32896.
TEST(ChunkedBuffer, PutSmall) {
  const std::string data(1 << 20, '.');  // 1 MB
  const int64_t total = 3LL << 30;       // 3 GB
  cockroach::chunkedBuffer buf;
  for (int64_t sum = 0; sum < total; sum += data.size()) {
    buf.Put(data, data);
  }
}

// Verify we can a chunked buffer can hold more than 2GB of data when
// writing in pieces that are larger than the maximum chunk size (128
// MB). See #32896.
TEST(ChunkedBuffer, PutLarge) {
  const std::string data(256 << 20, '.');  // 256 MB
  const int64_t total = 3LL << 30;         // 3 GB
  cockroach::chunkedBuffer buf;
  for (int64_t sum = 0; sum < total; sum += data.size()) {
    buf.Put(data, data);
  }
}
