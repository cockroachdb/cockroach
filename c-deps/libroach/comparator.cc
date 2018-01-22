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
