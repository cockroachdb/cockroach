// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

#include "geodesic.h"
#include "geographiclib.h"

void CR_GEOGRAPHICLIB_InverseBatch(
  struct geod_geodesic* spheroid,
  double lats[],
  double lngs[],
  int len,
  double *result
) {
  *result = 0;
  for (int i = 0; i < len - 1; i++) {
    double s12, az1, az2;
    geod_inverse(spheroid, lats[i], lngs[i], lats[i+1], lngs[i+1], &s12, &az1, &az2);
    *result += s12;
  }
}
