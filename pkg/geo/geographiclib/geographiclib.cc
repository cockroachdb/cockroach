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
