// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "timestamp.h"

namespace cockroach {
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
}  // namespace cockroach
