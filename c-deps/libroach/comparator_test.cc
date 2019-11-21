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

  DBKey makeKey(std::string s, int64_t wall_time = 0, int32_t logical = 0) {
  return {DBSlice{const_cast<char*>(s.data()), static_cast<int>(s.size())}, wall_time, logical};
}

}  // namespace

TEST(Libroach, Comparator) {
  DBComparator comp;
  std::vector<SeparatorTestCase> sepCases = {
    // Many cases here are adapted from a Pebble unit test.

    // Non-empty b values.
    {makeKey(std::string("black")), makeKey(std::string("blue")), makeKey(std::string("blb"))},
    {makeKey(std::string("")), makeKey(std::string("2")), makeKey(std::string(""))},
    {makeKey(std::string("1")), makeKey(std::string("2")), makeKey(std::string("1"))},
    {makeKey(std::string("1")), makeKey(std::string("29")), makeKey(std::string("2"))},
    {makeKey(std::string("13")), makeKey(std::string("19")), makeKey(std::string("14"))},
    {makeKey(std::string("13")), makeKey(std::string("99")), makeKey(std::string("2"))},
    {makeKey(std::string("135")), makeKey(std::string("19")), makeKey(std::string("14"))},
    {makeKey(std::string("1357")), makeKey(std::string("19")), makeKey(std::string("14"))},
    {makeKey(std::string("1357")), makeKey(std::string("2")), makeKey(std::string("14"))},
    {makeKey(std::string("13\xff")), makeKey(std::string("14")), makeKey(std::string("13\xff"))},
    {makeKey(std::string("13\xff")), makeKey(std::string("19")), makeKey(std::string("14"))},
    {makeKey(std::string("1\xff\xff")), makeKey(std::string("19")), makeKey(std::string("1\xff\xff"))},
    {makeKey(std::string("1\xff\xff")), makeKey(std::string("2")), makeKey(std::string("1\xff\xff"))},
    {makeKey(std::string("1\xff\xff")), makeKey(std::string("9")), makeKey(std::string("2"))},
    {makeKey(std::string("1\xfd\xff")), makeKey(std::string("1\xff")), makeKey(std::string("1\xfe"))},
    {makeKey(std::string("1\xff\xff"), 20, 3), makeKey(std::string("9")), makeKey(std::string("2"))},
    {makeKey(std::string("1\xff\xff"), 20, 3), makeKey(std::string("19")), makeKey(std::string("1\xff\xff"), 20, 3)},

    // Empty b values
    {makeKey(std::string("")), makeKey(std::string("")), makeKey(std::string(""))},
    {makeKey(std::string("green")), makeKey(std::string("")), makeKey(std::string("green"))},
    {makeKey(std::string("1")), makeKey(std::string("")), makeKey(std::string("1"))},
    {makeKey(std::string("11\xff")), makeKey(std::string("")), makeKey(std::string("11\xff"))},
    {makeKey(std::string("1\xff")), makeKey(std::string("")), makeKey(std::string("1\xff"))},
    {makeKey(std::string("1\xff\xff")), makeKey(std::string("")), makeKey(std::string("1\xff\xff"))},
    {makeKey(std::string("\xff")), makeKey(std::string("")), makeKey(std::string("\xff"))},
    {makeKey(std::string("\xff\xff")), makeKey(std::string("")), makeKey(std::string("\xff\xff"))},
  };
  
  for (const auto& c: sepCases) {
    auto a_str = EncodeKey(c.a);
    auto b_str = EncodeKey(c.b);
    std::printf("a_str: %s, b_str: %s\n", a_str.c_str(), b_str.c_str());
    comp.FindShortestSeparator(&a_str, b_str);
    EXPECT_EQ(EncodeKey(c.expected), a_str);
  }

  std::vector<SuccessorTestCase> succCases = {
    {makeKey(std::string("black")), makeKey(std::string("c"))},
    {makeKey(std::string("green")), makeKey(std::string("h"))},
    {makeKey(std::string("")), makeKey(std::string(""))},
    {makeKey(std::string("13")), makeKey(std::string("2"))},
    {makeKey(std::string("135")), makeKey(std::string("2"))},
    {makeKey(std::string("13\xff")), makeKey(std::string("2"))},
    {makeKey(std::string("1\xff\xff"), 20, 3), makeKey(std::string("2"))},
    {makeKey(std::string("\xff")), makeKey(std::string("\xff"))},
    {makeKey(std::string("\xff\xff")), makeKey(std::string("\xff\xff"))},
    {makeKey(std::string("\xff\xff\xff")), makeKey(std::string("\xff\xff\xff"))},
    {makeKey(std::string("\xfe\xff\xff")), makeKey(std::string("\xff"))},
    {makeKey(std::string("\xff\xff"), 20, 3), makeKey(std::string("\xff\xff"), 20, 3)},
  };

  for (const auto& c: succCases) {
    auto a_str = EncodeKey(c.a);
    std::printf("a_str: %s\n", a_str.c_str());
    comp.FindShortSuccessor(&a_str);
    EXPECT_EQ(EncodeKey(c.expected), a_str);
  }
}
