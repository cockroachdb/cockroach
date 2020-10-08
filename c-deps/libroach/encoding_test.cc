// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <cinttypes>
#include <cstdint>
#include <gtest/gtest.h>
#include <random>
#include <vector>
#include "encoding.h"

using namespace cockroach;

TEST(Libroach, Encoding) {
  // clang-format off
  std::vector<uint32_t> cases32{
    0, 1, 2, 3,
    1 << 16, (1 << 16) - 1, (1 << 16) + 1,
    1 << 24, (1 << 24) - 1, (1 << 24) + 1,
    UINT32_MAX - 1, UINT32_MAX,
  };

  std::vector<uint64_t> cases64{
    0, 1, 2, 3,
    1 << 16, (1 << 16) - 1, (1 << 16) + 1,
    1 << 24, (1 << 24) - 1, (1 << 24) + 1,
    UINT32_MAX - 1, UINT32_MAX,
    (1ULL << 32) + 1,
    1ULL << 40, (1ULL << 40) - 1, (1ULL << 40) + 1,
    1ULL << 48, (1ULL << 48) - 1, (1ULL << 48) + 1,
    1ULL << 56, (1ULL << 56) - 1, (1ULL << 56) + 1,
    UINT64_MAX - 1, UINT64_MAX
  };
  // clang-format on

  std::mt19937 rng;
  std::uniform_int_distribution<uint32_t> uniform32;
  std::uniform_int_distribution<uint64_t> uniform64;
  for (int i = 0; i < 32; i++) {
    cases32.push_back(uniform32(rng));
    cases64.push_back(uniform64(rng));
  }

  cases64.insert(cases64.end(), cases32.begin(), cases32.end());

  for (auto it = cases32.begin(); it != cases32.end(); it++) {
    std::string buf;
    EncodeUint32(&buf, *it);
    uint32_t out;
    rocksdb::Slice slice(buf);
    DecodeUint32(&slice, &out);
    EXPECT_EQ(*it, out);
  }

  for (auto it = cases64.begin(); it != cases64.end(); it++) {
    std::string buf;
    EncodeUint64(&buf, *it);
    uint64_t out;
    rocksdb::Slice slice(buf);
    DecodeUint64(&slice, &out);
    EXPECT_EQ(*it, out);
  }

  for (auto it = cases64.begin(); it != cases64.end(); it++) {
    std::string buf;
    EncodeUvarint64(&buf, *it);
    uint64_t out;
    rocksdb::Slice slice(buf);
    DecodeUvarint64(&slice, &out);
    EXPECT_EQ(*it, out);
  }
}

TEST(Libroach, DecodeTenantAndTablePrefix) {
  // clang-format off
  std::vector<std::pair<std::string, uint64_t>> cases{
    {{'\x89'}, 1LLU},
    {{'\xf6', '\xff'}, 255LLU},
    {{'\xf7', '\xff', '\xff'}, 65535LLU},
    {{'\xf8', '\xff', '\xff', '\xff'}, 16777215LLU},
    {{'\xf9', '\xff', '\xff', '\xff', '\xff'}, 4294967295LLU},
    {{'\xfa', '\xff', '\xff', '\xff', '\xff', '\xff'}, 1099511627775LLU},
    {{'\xfb', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff'}, 281474976710655LLU},
    {{'\xfc', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff'}, 72057594037927935LLU},
    {{'\xfd', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff'}, 18446744073709551615LLU},
    {{'\xfe', '\x8d', '\x89'}, 1LLU},
    {{'\xfe', '\x8d', '\xf6', '\xff'}, 255LLU},
    {{'\xfe', '\x8d', '\xf7', '\xff', '\xff'}, 65535LLU},
    {{'\xfe', '\x8d', '\xf8', '\xff', '\xff', '\xff'}, 16777215LLU},
    {{'\xfe', '\x8d', '\xf9', '\xff', '\xff', '\xff', '\xff'}, 4294967295LLU},
    {{'\xfe', '\x8d', '\xfa', '\xff', '\xff', '\xff', '\xff', '\xff'}, 1099511627775LLU},
    {{'\xfe', '\x8d', '\xfb', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff'}, 281474976710655LLU},
    {{'\xfe', '\x8d', '\xfc', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff'}, 72057594037927935LLU},
    {{'\xfe', '\x8d', '\xfd', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff'}, 18446744073709551615LLU},
  };
  // clang-format on

  for (auto it = cases.begin(); it != cases.end(); it++) {
    rocksdb::Slice slice(it->first);
    uint64_t tbl = 0;
    EXPECT_TRUE(DecodeTenantAndTablePrefix(&slice, &tbl));
    EXPECT_EQ(it->second, tbl);
  }
}
