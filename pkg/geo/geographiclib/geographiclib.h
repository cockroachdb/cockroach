// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "geodesic.h"

#if defined(__cplusplus)
extern "C" {
#endif

// CR_GEOGRAPHICLIB_InverseBatch computes the sum of the length of the lines
// represented by an array of lat/lngs using Inverse from GeographicLib.
// It is batched in C++ to reduce the cgo overheads.
void CR_GEOGRAPHICLIB_InverseBatch(
  struct geod_geodesic* spheroid,
  double lats[],
  double lngs[],
  int len,
  double *result
);

#if defined(__cplusplus)
}
#endif
