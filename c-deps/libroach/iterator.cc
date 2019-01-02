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
#include "encoding.h"
#include "iterator.h"
#include "keys.h"

using namespace cockroach;

DBIterator::DBIterator(std::atomic<int64_t>* iters, DBIterOptions iter_options) : iters_count(iters) {
  read_opts.prefix_same_as_start = iter_options.prefix;
  read_opts.total_order_seek = !iter_options.prefix;

  SetLowerBound(iter_options.lower_bound);
  SetUpperBound(iter_options.upper_bound);
  read_opts.iterate_lower_bound = &lower_bound;
  read_opts.iterate_upper_bound = &upper_bound;

  if (!EmptyTimestamp(iter_options.min_timestamp_hint) ||
      !EmptyTimestamp(iter_options.max_timestamp_hint)) {
    assert(!EmptyTimestamp(iter_options.min_timestamp_hint));
    assert(!EmptyTimestamp(iter_options.max_timestamp_hint));
    const std::string min = EncodeTimestamp(iter_options.min_timestamp_hint);
    const std::string max = EncodeTimestamp(iter_options.max_timestamp_hint);
    read_opts.table_filter = [min, max, this](const rocksdb::TableProperties& props) {
      auto userprops = props.user_collected_properties;
      auto tbl_min = userprops.find("crdb.ts.min");
      if (tbl_min == userprops.end() || tbl_min->second.empty()) {
        if (stats != nullptr) {
          ++stats->timebound_num_ssts;
        }
        return true;
      }
      auto tbl_max = userprops.find("crdb.ts.max");
      if (tbl_max == userprops.end() || tbl_max->second.empty()) {
        if (stats != nullptr) {
          ++stats->timebound_num_ssts;
        }
        return true;
      }
      // If the timestamp range of the table overlaps with the timestamp range we
      // want to iterate, the table might contain timestamps we care about.
      bool used = max.compare(tbl_min->second) >= 0 && min.compare(tbl_max->second) <= 0;
      if (used && stats != nullptr) {
        ++stats->timebound_num_ssts;
      }
      return used;
    };
  }

  if (iter_options.with_stats) {
    stats.reset(new IteratorStats());
  }

  ++(*iters_count);
}

DBIterator::~DBIterator() {
  --(*iters_count);
}

void DBIterator::SetLowerBound(DBKey key) {
  if (key.key.data == NULL) {
    lower_bound_str = kMinKey.data();
  } else {
    lower_bound_str = EncodeKey(key);
  }
  lower_bound = lower_bound_str;
}


void DBIterator::SetUpperBound(DBKey key) {
  if (key.key.data == NULL) {
    upper_bound_str = kMaxKey.data();
  } else {
    upper_bound_str = EncodeKey(key);
  }
  upper_bound = upper_bound_str;
}
