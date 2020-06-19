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
typedef void* CR_GEOS_Geometry;

// NB: Both CR_GEOS_Slice and CR_GEOS_String can contain non-printable
// data, so neither is necessarily compatible with a NUL character
// terminated C string. Functions that need data that does not contain
// the NUL character, so that they can convert to a NUL terminated C
// string, must document that additional constraint in their interface.

// CR_GEOS_Slice contains data that does not need to be freed. It
// can be either a Go or C pointer (which indicates who allocated the
// memory).
typedef struct {
  char* data;
  size_t len;
} CR_GEOS_Slice;

// CR_GEOS_String contains a C pointer that needs to be freed.
typedef struct {
  char* data;
  size_t len;
} CR_GEOS_String;

// CR_GEOS_BufferParams are parameters that will be passed to buffer.
typedef struct {
  int endCapStyle;
  int joinStyle;
  int singleSided;
  int quadrantSegments;
  double mitreLimit;
} CR_GEOS_BufferParamsInput;

typedef CR_GEOS_String CR_GEOS_Status;

// CR_GEOS contains all the functions loaded by GEOS.
typedef struct CR_GEOS CR_GEOS;

// CR_GEOS_Init initializes the provided GEOSLib with GEOS using dlopen/dlsym.
// Returns a string containing an error if an error was found. The loc slice
// must be convertible to a NUL character terminated C string.
// The CR_GEOS object will be stored in lib.
// The error returned does not need to be freed (see comment for CR_GEOS_Slice).
CR_GEOS_Slice CR_GEOS_Init(CR_GEOS_Slice geoscLoc, CR_GEOS_Slice geosLoc, CR_GEOS** lib);

// CR_GEOS_WKTToWKB converts a given WKT into it's EWKB form. The wkt slice must be
// convertible to a NUL character terminated C string.
CR_GEOS_Status CR_GEOS_WKTToEWKB(CR_GEOS* lib, CR_GEOS_Slice wkt, int srid, CR_GEOS_String* ewkb);

// CR_GEOS_ClipEWKBByRect clips a given EWKB by the given rectangle.
CR_GEOS_Status CR_GEOS_ClipEWKBByRect(CR_GEOS* lib, CR_GEOS_Slice ewkb, double xmin, double ymin,
                                      double xmax, double ymax, CR_GEOS_String* clippedEWKB);
// CR_GEOS_Buffer buffers a given EWKB by the given distance and params.
CR_GEOS_Status CR_GEOS_Buffer(CR_GEOS* lib, CR_GEOS_Slice ewkb, CR_GEOS_BufferParamsInput params,
                              double distance, CR_GEOS_String* ret);

//
// Unary operators.
//

CR_GEOS_Status CR_GEOS_Area(CR_GEOS* lib, CR_GEOS_Slice a, double* ret);
CR_GEOS_Status CR_GEOS_Length(CR_GEOS* lib, CR_GEOS_Slice a, double* ret);

//
// Topology operators.
//

CR_GEOS_Status CR_GEOS_Centroid(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_String* centroidEWKB);
CR_GEOS_Status CR_GEOS_Union(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                             CR_GEOS_String* unionEWKB);
CR_GEOS_Status CR_GEOS_PointOnSurface(CR_GEOS* lib, CR_GEOS_Slice a,
                                      CR_GEOS_String* pointOnSurfaceEWKB);
CR_GEOS_Status CR_GEOS_Intersection(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                    CR_GEOS_String* intersectionEWKB);

//
// Linear reference.
//
CR_GEOS_Status CR_GEOS_Interpolate(CR_GEOS* lib, CR_GEOS_Slice a, double distance,
                                   CR_GEOS_String* interpolatedPoint);

//
// Binary operators.
//

CR_GEOS_Status CR_GEOS_Distance(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, double* ret);

//
// Binary predicates.
//

CR_GEOS_Status CR_GEOS_Covers(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret);
CR_GEOS_Status CR_GEOS_CoveredBy(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret);
CR_GEOS_Status CR_GEOS_Contains(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret);
CR_GEOS_Status CR_GEOS_Crosses(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret);
CR_GEOS_Status CR_GEOS_Equals(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret);
CR_GEOS_Status CR_GEOS_Intersects(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret);
CR_GEOS_Status CR_GEOS_Overlaps(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret);
CR_GEOS_Status CR_GEOS_Touches(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret);
CR_GEOS_Status CR_GEOS_Within(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret);

//
// DE-9IM related
//

CR_GEOS_Status CR_GEOS_Relate(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, CR_GEOS_String* ret);
CR_GEOS_Status CR_GEOS_RelatePattern(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                     CR_GEOS_Slice pattern, char* ret);

#ifdef __cplusplus
}  // extern "C"
#endif
