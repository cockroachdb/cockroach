// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
