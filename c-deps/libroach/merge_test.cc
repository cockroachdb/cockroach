// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gtest/gtest.h>
#include <utility>
#include <vector>
#include "merge.h"
#include "protos/roachpb/internal.pb.h"

using namespace cockroach;

void testAddColumns(roachpb::InternalTimeSeriesData* data, int offset, int value) {
  data->add_offset(offset);
  data->add_count(value + 1);
  data->add_last(value + 2);
  data->add_first(value + 3);
  data->add_min(value + 4);
  data->add_max(value + 5);
  data->add_sum(value + 6);
  data->add_variance(value + 7);
}

void testAddColumnsNoRollup(roachpb::InternalTimeSeriesData* data, int offset, int value) {
  data->add_offset(offset);
  data->add_last(value);
}

void testAddRows(roachpb::InternalTimeSeriesData* data, int offset, int value) {
  auto row = data->add_samples();
  row->set_offset(offset);
  row->set_sum(value);
  row->set_count(1);
  row->set_max(value + 1);
  row->set_min(value + 2);
}

TEST(TimeSeriesMerge, SortAndDeduplicate) {
  struct TestCase {
    using rowData = std::vector<std::pair<int, int>>;
    rowData originalRows;
    int firstUnsorted;
    rowData expectedRows;
    TestCase(rowData orig, int first, rowData expected)
        : originalRows(orig), firstUnsorted(first), expectedRows(expected){};
  };

  std::vector<TestCase> testCases = {
      // Basic sorting and deduplication.
      TestCase(
          {
              {10, 999},
              {8, 1},
              {9, 2},
              {10, 0},
              {4, 0},
              {10, 3},
          },
          0,
          {
              {4, 0},
              {8, 1},
              {9, 2},
              {10, 3},
          }),
      // Partial sorting and deduplication. The first index is intentionally out
      // of order in order strongly demonstrate that only a suffix of the array
      // is being sorted, anything before firstUnsorted is not modified.
      TestCase(
          {
              {10, 999},
              {8, 1},
              {9, 2},
              {10, 0},
              {4, 0},
              {10, 3},
          },
          3,
          {
              {10, 999},
              {8, 1},
              {9, 2},
              {4, 0},
              {10, 3},
          }),
      // Sort only last sample (common case).
      TestCase(
          {
              {1, 1},
              {2, 2},
              {3, 3},
              {4, 4},
              {5, 5},
          },
          4,
          {
              {1, 1},
              {2, 2},
              {3, 3},
              {4, 4},
              {5, 5},
          }),
      // Already sorted.
      TestCase(
          {
              {1, 1},
              {2, 2},
              {3, 3},
              {4, 4},
              {5, 5},
          },
          0,
          {
              {1, 1},
              {2, 2},
              {3, 3},
              {4, 4},
              {5, 5},
          }),
      // Single element shifted forward.
      TestCase(
          {
              {5, 5},
              {1, 1},
              {2, 2},
              {3, 3},
              {4, 4},
          },
          0,
          {
              {1, 1},
              {2, 2},
              {3, 3},
              {4, 4},
              {5, 5},
          }),
      // Reversed.
      TestCase(
          {
              {5, 5},
              {4, 4},
              {3, 3},
              {2, 2},
              {1, 1},
          },
          0,
          {
              {1, 1},
              {2, 2},
              {3, 3},
              {4, 4},
              {5, 5},
          }),
      // Element shift with duplicate.
      TestCase(
          {
              {5, 999},
              {1, 1},
              {2, 2},
              {5, 5},
              {3, 3},
              {4, 4},
          },
          0,
          {
              {1, 1},
              {2, 2},
              {3, 3},
              {4, 4},
              {5, 5},
          }),
      // Shift with firstUnsorted.
      TestCase(
          {
              {99, 999},
              {88, 888},
              {77, 777},
              {5, 5},
              {1, 1},
              {2, 2},
              {3, 3},
              {4, 4},
          },
          3,
          {
              {99, 999},
              {88, 888},
              {77, 777},
              {1, 1},
              {2, 2},
              {3, 3},
              {4, 4},
              {5, 5},
          })};
  for (auto testCase : testCases) {
    roachpb::InternalTimeSeriesData orig;
    for (auto row : testCase.originalRows) {
      testAddColumns(&orig, row.first, row.second);
    }
    roachpb::InternalTimeSeriesData expected;
    for (auto row : testCase.expectedRows) {
      testAddColumns(&expected, row.first, row.second);
    }

    sortAndDeduplicateColumns(&orig, testCase.firstUnsorted);
    EXPECT_EQ(orig.SerializeAsString(), expected.SerializeAsString());
  }
}

TEST(TimeSeriesMerge, ConvertToColumnar) {
  roachpb::InternalTimeSeriesData orig;
  testAddRows(&orig, 1, 2);
  testAddRows(&orig, 2, 4);
  testAddRows(&orig, 3, 4);

  roachpb::InternalTimeSeriesData expected;
  testAddColumnsNoRollup(&expected, 1, 2);
  testAddColumnsNoRollup(&expected, 2, 4);
  testAddColumnsNoRollup(&expected, 3, 4);

  EXPECT_NE(orig.SerializeAsString(), expected.SerializeAsString());
  convertToColumnar(&orig);
  EXPECT_EQ(orig.SerializeAsString(), expected.SerializeAsString());
}
