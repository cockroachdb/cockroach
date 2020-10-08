// Copyright 2019 The Cockroach Authors.
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
#include <vector>
#include "comparator.h"
#include "encoding.h"
#include "libroach.h"

using namespace cockroach;

namespace {

struct SeparatorTestCase {
  DBKey a;
  DBKey b;
  DBKey expected;
};

struct SuccessorTestCase {
  DBKey a;
  DBKey expected;
};

DBKey makeKey(const char* s, int64_t wall_time = 0, int32_t logical = 0) {
  return {DBSlice{const_cast<char*>(s), strlen(s)}, wall_time, logical};
}

}  // namespace

TEST(Libroach, Comparator) {
  DBComparator comp;
  std::vector<SeparatorTestCase> sepCases = {
      // Many cases here are adapted from a Pebble unit test.

      // Non-empty b values.
      {makeKey("black"), makeKey("blue"), makeKey("blb")},
      {makeKey(""), makeKey("2"), makeKey("")},
      {makeKey("1"), makeKey("2"), makeKey("1")},
      {makeKey("1"), makeKey("29"), makeKey("2")},
      {makeKey("13"), makeKey("19"), makeKey("14")},
      {makeKey("13"), makeKey("99"), makeKey("2")},
      {makeKey("135"), makeKey("19"), makeKey("14")},
      {makeKey("1357"), makeKey("19"), makeKey("14")},
      {makeKey("1357"), makeKey("2"), makeKey("14")},
      {makeKey("13\xff"), makeKey("14"), makeKey("13\xff")},
      {makeKey("13\xff"), makeKey("19"), makeKey("14")},
      {makeKey("1\xff\xff"), makeKey("19"), makeKey("1\xff\xff")},
      {makeKey("1\xff\xff"), makeKey("2"), makeKey("1\xff\xff")},
      {makeKey("1\xff\xff"), makeKey("9"), makeKey("2")},
      {makeKey("1\xfd\xff"), makeKey("1\xff"), makeKey("1\xfe")},
      {makeKey("1\xff\xff", 20, 3), makeKey("9"), makeKey("2")},
      {makeKey("1\xff\xff", 20, 3), makeKey("19"), makeKey("1\xff\xff", 20, 3)},

      // Empty b values
      {makeKey(""), makeKey(""), makeKey("")},
      {makeKey("green"), makeKey(""), makeKey("green")},
      {makeKey("1"), makeKey(""), makeKey("1")},
      {makeKey("11\xff"), makeKey(""), makeKey("11\xff")},
      {makeKey("1\xff"), makeKey(""), makeKey("1\xff")},
      {makeKey("1\xff\xff"), makeKey(""), makeKey("1\xff\xff")},
      {makeKey("\xff"), makeKey(""), makeKey("\xff")},
      {makeKey("\xff\xff"), makeKey(""), makeKey("\xff\xff")},
  };

  for (const auto& c : sepCases) {
    auto a_str = EncodeKey(c.a);
    auto b_str = EncodeKey(c.b);
    std::printf("a_str: %s, b_str: %s\n", a_str.c_str(), b_str.c_str());
    comp.FindShortestSeparator(&a_str, b_str);
    EXPECT_EQ(EncodeKey(c.expected), a_str);
  }

  std::vector<SuccessorTestCase> succCases = {
      {makeKey("black"), makeKey("c")},
      {makeKey("green"), makeKey("h")},
      {makeKey(""), makeKey("")},
      {makeKey("13"), makeKey("2")},
      {makeKey("135"), makeKey("2")},
      {makeKey("13\xff"), makeKey("2")},
      {makeKey("1\xff\xff", 20, 3), makeKey("2")},
      {makeKey("\xff"), makeKey("\xff")},
      {makeKey("\xff\xff"), makeKey("\xff\xff")},
      {makeKey("\xff\xff\xff"), makeKey("\xff\xff\xff")},
      {makeKey("\xfe\xff\xff"), makeKey("\xff")},
      {makeKey("\xff\xff", 20, 3), makeKey("\xff\xff", 20, 3)},
  };

  for (const auto& c : succCases) {
    auto a_str = EncodeKey(c.a);
    std::printf("a_str: %s\n", a_str.c_str());
    comp.FindShortSuccessor(&a_str);
    EXPECT_EQ(EncodeKey(c.expected), a_str);
  }
}
