// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#pragma once

#include <libroach.h>

#ifdef __cplusplus
extern "C" {
#endif

// DBBatchReprVerify asserts that all keys in a BatchRepr are between the
// specified start and end keys and computes the MVCCStatsResult for it.
DBStatus DBBatchReprVerify(DBSlice repr, DBKey start, DBKey end, int64_t now_nanos,
                           MVCCStatsResult* stats);

extern void* DBOpenHookCCL;

#ifdef __cplusplus
}  // extern "C"
#endif
