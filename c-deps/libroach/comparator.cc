// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

#include "comparator.h"
#include "encoding.h"

namespace cockroach {

int DBComparator::Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const {
  rocksdb::Slice key_a, key_b;
  rocksdb::Slice ts_a, ts_b;
  if (!SplitKey(a, &key_a, &ts_a) || !SplitKey(b, &key_b, &ts_b)) {
    // This should never happen unless there is some sort of corruption of
    // the keys.
    return a.compare(b);
  }

  const int c = key_a.compare(key_b);
  if (c != 0) {
    return c;
  }
  if (ts_a.empty()) {
    if (ts_b.empty()) {
      return 0;
    }
    return -1;
  } else if (ts_b.empty()) {
    return +1;
  }
  return ts_b.compare(ts_a);
}

bool DBComparator::Equal(const rocksdb::Slice& a, const rocksdb::Slice& b) const { return a == b; }

// The RocksDB docs say it is safe to leave these two methods unimplemented.
void DBComparator::FindShortestSeparator(std::string* start, const rocksdb::Slice& limit) const {}

void DBComparator::FindShortSuccessor(std::string* key) const {}

}  // namespace cockroach
