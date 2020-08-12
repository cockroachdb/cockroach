// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

// CR_PROJ_Slice contains data that does not need to be freed.
// It can be either a Go or C pointer (which indicates who allocated the
// memory).
typedef struct {
  char* data;
  size_t len;
} CR_PROJ_Slice;

typedef CR_PROJ_Slice CR_PROJ_Status;

// CR_PROJ_Transform converts the given x/y/z coordinates to a new project specification.
// Note points (x[i], y[i], z[i]) are in the range 0 <= i < point_coint.
CR_PROJ_Status CR_PROJ_Transform(char* fromSpec, char* toSpec, long point_count, double* x,
                                 double* y, double* z);

// CR_PROJ_GetProjMetadata gets the metadata for a given spec in relation to
// the spheroid it represents.
CR_PROJ_Status CR_PROJ_GetProjMetadata(char* spec, int* retIsLatLng, double* retMajorAxis,
                                       double* retEccentricitySquared);
#ifdef __cplusplus
}  // extern "C"
#endif
