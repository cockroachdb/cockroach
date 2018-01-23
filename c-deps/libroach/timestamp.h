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

#include <libroach.h>
#include "protos/storage/engine/enginepb/mvcc.pb.h"

namespace cockroach {

const DBTimestamp kZeroTimestamp = {0, 0};

DBTimestamp ToDBTimestamp(const cockroach::util::hlc::LegacyTimestamp& timestamp) {
  return DBTimestamp{timestamp.wall_time(), timestamp.logical()};
}

DBTimestamp PrevTimestamp(DBTimestamp ts) {
  if (ts.logical > 0) {
    --ts.logical;
  } else if (ts.wall_time == 0) {
    fprintf(stderr, "no previous time for zero timestamp\n");
    abort();
  } else {
    --ts.wall_time;
    ts.logical = std::numeric_limits<int32_t>::max();
  }
  return ts;
}

inline bool operator==(const DBTimestamp& a, const DBTimestamp& b) {
  return a.wall_time == b.wall_time && a.logical == b.logical;
}

inline bool operator!=(const DBTimestamp& a, const DBTimestamp& b) { return !(a == b); }

inline bool operator<(const DBTimestamp& a, const DBTimestamp& b) {
  return a.wall_time < b.wall_time || (a.wall_time == b.wall_time && a.logical < b.logical);
}

inline bool operator>(const DBTimestamp& a, const DBTimestamp& b) { return b < a; }

inline bool operator<=(const DBTimestamp& a, const DBTimestamp& b) { return !(b < a); }

inline bool operator>=(const DBTimestamp& a, const DBTimestamp& b) { return b <= a; }

}  // namespace cockroach
