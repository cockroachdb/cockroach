// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#if defined(__cplusplus)
extern "C" {
#endif

void CR_GEOGRAPHICLIB_Inverse(
  double radius,
  double flattening,
  double aLat,
  double aLng,
  double bLat,
  double bLng,
  double *s12,
  double *az1,
  double *az2
);

#if defined(__cplusplus)
}
#endif
