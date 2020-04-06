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

// Data Types adapted from `capi/geos_c.h.in` in GEOS.
typedef void *CR_GEOS_Geometry;

// CR_GEOS_Slice is a wrapper around a Go slice.
typedef struct {
  char *data;
  size_t len;
} CR_GEOS_Slice;

// CR_GEOS_String is a wrapper around a Go string.
typedef struct {
  char *data;
  size_t len;
} CR_GEOS_String;

// CR_GEOS contains all the functions loaded by GEOS.
typedef struct CR_GEOS CR_GEOS;

// CR_GEOS_Init initializes the provided GEOSLib with GEOS using dlopen/dlsym.
// Returns a string containing an error if an error was found.
// The CR_GEOS object will be stored in lib.
// The error returned does not need to be freed.
char *CR_GEOS_Init(CR_GEOS_String loc, CR_GEOS **lib);

// CR_GEOS_WKTToWKB converts a given WKT into it's WKB form.
CR_GEOS_String CR_GEOS_WKTToWKB(CR_GEOS *lib, CR_GEOS_String wkt);

// CR_GEOS_ClipWKBByRect clips a given WKB by the given rectangle.
CR_GEOS_String CR_GEOS_ClipWKBByRect(
  CR_GEOS *lib, CR_GEOS_Slice wkb, double xmin, double ymin, double xmax, double ymax);

#ifdef __cplusplus
} // extern "C"
#endif
