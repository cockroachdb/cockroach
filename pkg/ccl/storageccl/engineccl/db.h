// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

#ifndef ROACHLIBCCL_DB_H
#define ROACHLIBCCL_DB_H

#include "../../../storage/engine/db.h"

#ifdef __cplusplus
extern "C" {
#endif

// DBBatchReprVerify asserts that all keys in a BatchRepr are between the
// specified start and end keys and computes the MVCCStatsResult for it.
DBStatus DBBatchReprVerify(
  DBSlice repr, DBKey start, DBKey end, int64_t now_nanos, MVCCStatsResult* stats);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif // ROACHLIBCCL_DB_H

// local variables:
// mode: c++
// end:
