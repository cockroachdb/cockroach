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
typedef void* CR_GEOS_PreparedInternalGeometry;

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

// CR_GEOS_PreparedGeometry is a wrapper containing GEOS PreparedGeometry and it's source Geometry.
// This allows us to free the memory for both at the same time.
typedef struct {
  CR_GEOS_Geometry g;
  CR_GEOS_PreparedInternalGeometry p;
} CR_GEOS_PreparedGeometry;

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
CR_GEOS_Status CR_GEOS_Init(CR_GEOS_Slice geoscLoc, CR_GEOS_Slice geosLoc, CR_GEOS** lib);

// CR_GEOS_WKTToWKB converts a given WKT into it's EWKB form. The wkt slice must be
// convertible to a NUL character terminated C string.
CR_GEOS_Status CR_GEOS_WKTToEWKB(CR_GEOS* lib, CR_GEOS_Slice wkt, int srid, CR_GEOS_String* ewkb);

// CR_GEOS_ClipByRect clips a given EWKB by the given rectangle.
CR_GEOS_Status CR_GEOS_ClipByRect(CR_GEOS* lib, CR_GEOS_Slice ewkb, double xmin, double ymin,
                                  double xmax, double ymax, CR_GEOS_String* clippedEWKB);
// CR_GEOS_Buffer buffers a given EWKB by the given distance and params.
CR_GEOS_Status CR_GEOS_Buffer(CR_GEOS* lib, CR_GEOS_Slice ewkb, CR_GEOS_BufferParamsInput params,
                              double distance, CR_GEOS_String* ret);

//
// Validity checking.
//

CR_GEOS_Status CR_GEOS_IsValid(CR_GEOS* lib, CR_GEOS_Slice g, char* ret);
CR_GEOS_Status CR_GEOS_IsValidReason(CR_GEOS* lib, CR_GEOS_Slice g, CR_GEOS_String* ret);
CR_GEOS_Status CR_GEOS_IsValidDetail(CR_GEOS* lib, CR_GEOS_Slice g, int flags, char* retIsValid,
                                     CR_GEOS_String* retReason, CR_GEOS_String* retLocationEWKB);
CR_GEOS_Status CR_GEOS_MakeValid(CR_GEOS* lib, CR_GEOS_Slice g, CR_GEOS_String* validEWKB);

//
// Unary operators.
//

CR_GEOS_Status CR_GEOS_Area(CR_GEOS* lib, CR_GEOS_Slice a, double* ret);
CR_GEOS_Status CR_GEOS_Length(CR_GEOS* lib, CR_GEOS_Slice a, double* ret);
CR_GEOS_Status CR_GEOS_Normalize(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_String* normalizedEWKB);
CR_GEOS_Status CR_GEOS_LineMerge(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_String* ewkb);
CR_GEOS_Status CR_GEOS_MinimumClearance(CR_GEOS* lib, CR_GEOS_Slice a, double* ret);
CR_GEOS_Status CR_GEOS_MinimumClearanceLine(CR_GEOS* lib, CR_GEOS_Slice a,
                                            CR_GEOS_String* clearanceEWKB);

//
// Unary predicates.
//

CR_GEOS_Status CR_GEOS_IsSimple(CR_GEOS* lib, CR_GEOS_Slice a, char* ret);

//
// Topology operators.
//

CR_GEOS_Status CR_GEOS_Boundary(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_String* boundaryEWKB);
CR_GEOS_Status CR_GEOS_Centroid(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_String* centroidEWKB);
CR_GEOS_Status CR_GEOS_ConvexHull(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_String* convexHullEWKB);
CR_GEOS_Status CR_GEOS_Difference(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                  CR_GEOS_String* diffEWKB);
CR_GEOS_Status CR_GEOS_Simplify(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_String* simplifyEWKB,
                                double tolerance);
CR_GEOS_Status CR_GEOS_TopologyPreserveSimplify(CR_GEOS* lib, CR_GEOS_Slice a,
                                                CR_GEOS_String* simplifyEWKB, double tolerance);
CR_GEOS_Status CR_GEOS_UnaryUnion(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_String* unionEWKB);
CR_GEOS_Status CR_GEOS_Union(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                             CR_GEOS_String* unionEWKB);
CR_GEOS_Status CR_GEOS_PointOnSurface(CR_GEOS* lib, CR_GEOS_Slice a,
                                      CR_GEOS_String* pointOnSurfaceEWKB);
CR_GEOS_Status CR_GEOS_Intersection(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                    CR_GEOS_String* intersectionEWKB);
CR_GEOS_Status CR_GEOS_SymDifference(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                     CR_GEOS_String* symdifferenceEWKB);
CR_GEOS_Status CR_GEOS_SharedPaths(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                   CR_GEOS_String* ret);
CR_GEOS_Status CR_GEOS_Node(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_String* ret);

CR_GEOS_Status CR_GEOS_MinimumBoundingCircle(CR_GEOS* lib, CR_GEOS_Slice a, double* radius,
                                              CR_GEOS_String* centerEWKB, CR_GEOS_String* polygonEWKB);

CR_GEOS_Status CR_GEOS_MinimumRotatedRectangle(CR_GEOS* lib, CR_GEOS_Slice g, CR_GEOS_String* ret);
//
// Linear reference.
//
CR_GEOS_Status CR_GEOS_Interpolate(CR_GEOS* lib, CR_GEOS_Slice a, double distance,
                                   CR_GEOS_String* interpolatedPoint);

//
// Binary operators.
//

CR_GEOS_Status CR_GEOS_Distance(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, double* ret);
CR_GEOS_Status CR_GEOS_FrechetDistance(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, double* ret);
CR_GEOS_Status CR_GEOS_FrechetDistanceDensify(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                              double densifyFrac, double* ret);
CR_GEOS_Status CR_GEOS_HausdorffDistance(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                         double* ret);
CR_GEOS_Status CR_GEOS_HausdorffDistanceDensify(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                                double densifyFrac, double* ret);
CR_GEOS_Status CR_GEOS_EqualsExact(CR_GEOS* lib, CR_GEOS_Slice lhs, CR_GEOS_Slice rhs,
                                      double tolerance, char* ret);

//
// PreparedGeometry
//

CR_GEOS_Status CR_GEOS_Prepare(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_PreparedGeometry** ret);
CR_GEOS_Status CR_GEOS_PreparedGeometryDestroy(CR_GEOS* lib, CR_GEOS_PreparedGeometry* g);

CR_GEOS_Status CR_GEOS_PreparedIntersects(CR_GEOS* lib, CR_GEOS_PreparedGeometry* a, CR_GEOS_Slice b, char* ret);

//
// Binary predicates.
//

CR_GEOS_Status CR_GEOS_Covers(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret);
CR_GEOS_Status CR_GEOS_CoveredBy(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret);
CR_GEOS_Status CR_GEOS_Contains(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret);
CR_GEOS_Status CR_GEOS_Crosses(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret);
CR_GEOS_Status CR_GEOS_Disjoint(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret);
CR_GEOS_Status CR_GEOS_Equals(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret);
CR_GEOS_Status CR_GEOS_Intersects(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret);
CR_GEOS_Status CR_GEOS_Overlaps(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret);
CR_GEOS_Status CR_GEOS_Touches(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret);
CR_GEOS_Status CR_GEOS_Within(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret);

//
// DE-9IM related
//

CR_GEOS_Status CR_GEOS_Relate(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, CR_GEOS_String* ret);
CR_GEOS_Status CR_GEOS_RelateBoundaryNodeRule(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                              int bnr, CR_GEOS_String* ret);
CR_GEOS_Status CR_GEOS_RelatePattern(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                     CR_GEOS_Slice pattern, char* ret);

CR_GEOS_Status CR_GEOS_VoronoiDiagram(CR_GEOS* lib, CR_GEOS_Slice g, CR_GEOS_Slice env,
                                      double tolerance, int onlyEdges, CR_GEOS_String* ret);

CR_GEOS_Status CR_GEOS_Snap(CR_GEOS* lib, CR_GEOS_Slice input, CR_GEOS_Slice target, double tolerance, CR_GEOS_String* ret);

  // Data Types adapted from `capi/geos_c.h.in` in GEOS.
typedef void* CR_GEOS_Handle;
typedef void (*CR_GEOS_MessageHandler)(const char*, void*);
typedef void* CR_GEOS_WKTReader;
typedef void* CR_GEOS_WKBReader;
typedef void* CR_GEOS_WKBWriter;
typedef void* CR_GEOS_BufferParams;

// Function declarations from `capi/geos_c.h.in` in GEOS.
typedef CR_GEOS_Handle (*CR_GEOS_init_r)();
typedef void (*CR_GEOS_finish_r)(CR_GEOS_Handle);
typedef CR_GEOS_MessageHandler (*CR_GEOS_Context_setErrorMessageHandler_r)(CR_GEOS_Handle,
                                                                           CR_GEOS_MessageHandler,
                                                                           void*);
typedef void (*CR_GEOS_Free_r)(CR_GEOS_Handle, void* buffer);
typedef void (*CR_GEOS_SetSRID_r)(CR_GEOS_Handle, CR_GEOS_Geometry, int);
typedef int (*CR_GEOS_GetSRID_r)(CR_GEOS_Handle, CR_GEOS_Geometry);
typedef void (*CR_GEOS_GeomDestroy_r)(CR_GEOS_Handle, CR_GEOS_Geometry);

typedef char (*CR_GEOS_HasZ_r)(CR_GEOS_Handle, CR_GEOS_Geometry);
typedef char (*CR_GEOS_IsEmpty_r)(CR_GEOS_Handle, CR_GEOS_Geometry);
typedef char (*CR_GEOS_IsSimple_r)(CR_GEOS_Handle, CR_GEOS_Geometry);
typedef int (*CR_GEOS_GeomTypeId_r)(CR_GEOS_Handle, CR_GEOS_Geometry);

typedef CR_GEOS_WKTReader (*CR_GEOS_WKTReader_create_r)(CR_GEOS_Handle);
typedef CR_GEOS_Geometry (*CR_GEOS_WKTReader_read_r)(CR_GEOS_Handle, CR_GEOS_WKTReader,
                                                     const char*);
typedef void (*CR_GEOS_WKTReader_destroy_r)(CR_GEOS_Handle, CR_GEOS_WKTReader);

typedef CR_GEOS_WKBReader (*CR_GEOS_WKBReader_create_r)(CR_GEOS_Handle);
typedef CR_GEOS_Geometry (*CR_GEOS_WKBReader_read_r)(CR_GEOS_Handle, CR_GEOS_WKBReader, const char*,
                                                     size_t);
typedef void (*CR_GEOS_WKBReader_destroy_r)(CR_GEOS_Handle, CR_GEOS_WKBReader);

typedef CR_GEOS_BufferParams (*CR_GEOS_BufferParams_create_r)(CR_GEOS_Handle);
typedef void (*CR_GEOS_GEOSBufferParams_destroy_r)(CR_GEOS_Handle, CR_GEOS_BufferParams);
typedef int (*CR_GEOS_BufferParams_setEndCapStyle_r)(CR_GEOS_Handle, CR_GEOS_BufferParams,
                                                     int endCapStyle);
typedef int (*CR_GEOS_BufferParams_setJoinStyle_r)(CR_GEOS_Handle, CR_GEOS_BufferParams,
                                                   int joinStyle);
typedef int (*CR_GEOS_BufferParams_setMitreLimit_r)(CR_GEOS_Handle, CR_GEOS_BufferParams,
                                                    double mitreLimit);
typedef int (*CR_GEOS_BufferParams_setQuadrantSegments_r)(CR_GEOS_Handle, CR_GEOS_BufferParams,
                                                          int quadrantSegments);
typedef int (*CR_GEOS_BufferParams_setSingleSided_r)(CR_GEOS_Handle, CR_GEOS_BufferParams,
                                                     int singleSided);
typedef CR_GEOS_Geometry (*CR_GEOS_BufferWithParams_r)(CR_GEOS_Handle, CR_GEOS_Geometry,
                                                       CR_GEOS_BufferParams, double width);

typedef char (*CR_GEOS_isValid_r)(CR_GEOS_Handle, CR_GEOS_Geometry);
typedef char* (*CR_GEOS_isValidReason_r)(CR_GEOS_Handle, CR_GEOS_Geometry);
typedef char (*CR_GEOS_isValidDetail_r)(CR_GEOS_Handle, CR_GEOS_Geometry, int flags, char** reason,
                                        CR_GEOS_Geometry* loc);
typedef CR_GEOS_Geometry (*CR_GEOS_MakeValid_r)(CR_GEOS_Handle, CR_GEOS_Geometry);

typedef int (*CR_GEOS_Area_r)(CR_GEOS_Handle, CR_GEOS_Geometry, double*);
typedef int (*CR_GEOS_Length_r)(CR_GEOS_Handle, CR_GEOS_Geometry, double*);
typedef int (*CR_GEOS_MinimumClearance_r)(CR_GEOS_Handle, CR_GEOS_Geometry, double*);
typedef CR_GEOS_Geometry (*CR_GEOS_MinimumClearanceLine_r)( CR_GEOS_Handle, CR_GEOS_Geometry);
typedef int (*CR_GEOS_Normalize_r)(CR_GEOS_Handle, CR_GEOS_Geometry);
typedef CR_GEOS_Geometry (*CR_GEOS_LineMerge_r)(CR_GEOS_Handle, CR_GEOS_Geometry);

typedef CR_GEOS_Geometry (*CR_GEOS_Boundary_r)(CR_GEOS_Handle, CR_GEOS_Geometry);
typedef CR_GEOS_Geometry (*CR_GEOS_Centroid_r)(CR_GEOS_Handle, CR_GEOS_Geometry);
typedef CR_GEOS_Geometry (*CR_GEOS_ConvexHull_r)(CR_GEOS_Handle, CR_GEOS_Geometry);
typedef CR_GEOS_Geometry (*CR_GEOS_Difference_r)(CR_GEOS_Handle, CR_GEOS_Geometry,
                                                 CR_GEOS_Geometry);
typedef CR_GEOS_Geometry (*CR_GEOS_Simplify_r)(CR_GEOS_Handle, CR_GEOS_Geometry, double);
typedef CR_GEOS_Geometry (*CR_GEOS_TopologyPreserveSimplify_r)(CR_GEOS_Handle, CR_GEOS_Geometry,
                                                               double);
typedef CR_GEOS_Geometry (*CR_GEOS_UnaryUnion_r)(CR_GEOS_Handle, CR_GEOS_Geometry);
typedef CR_GEOS_Geometry (*CR_GEOS_Union_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef CR_GEOS_Geometry (*CR_GEOS_Intersection_r)(CR_GEOS_Handle, CR_GEOS_Geometry,
                                                   CR_GEOS_Geometry);
typedef CR_GEOS_Geometry (*CR_GEOS_SymDifference_r)(CR_GEOS_Handle, CR_GEOS_Geometry,
                                                    CR_GEOS_Geometry);
typedef CR_GEOS_Geometry (*CR_GEOS_PointOnSurface_r)(CR_GEOS_Handle, CR_GEOS_Geometry);

typedef CR_GEOS_Geometry (*CR_GEOS_Interpolate_r)(CR_GEOS_Handle, CR_GEOS_Geometry, double);

typedef CR_GEOS_Geometry (*CR_GEOS_MinimumBoundingCircle_r)(CR_GEOS_Handle, CR_GEOS_Geometry, double*, CR_GEOS_Geometry**);

typedef int (*CR_GEOS_Distance_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry, double*);
typedef int (*CR_GEOS_FrechetDistance_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry,
                                         double*);
typedef int (*CR_GEOS_FrechetDistanceDensify_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry,
                                                double, double*);
typedef int (*CR_GEOS_HausdorffDistance_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry,
                                           double*);
typedef int (*CR_GEOS_HausdorffDistanceDensify_r)(CR_GEOS_Handle, CR_GEOS_Geometry,
                                                  CR_GEOS_Geometry, double, double*);

typedef CR_GEOS_PreparedInternalGeometry (*CR_GEOS_Prepare_r)(CR_GEOS_Handle, CR_GEOS_Geometry);
typedef void (*CR_GEOS_PreparedGeom_destroy_r)(CR_GEOS_Handle, CR_GEOS_PreparedInternalGeometry);

typedef char (*CR_GEOS_PreparedIntersects_r)(CR_GEOS_Handle, CR_GEOS_PreparedInternalGeometry, CR_GEOS_Geometry);

typedef char (*CR_GEOS_Covers_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_CoveredBy_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Contains_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Crosses_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Disjoint_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Equals_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Intersects_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Overlaps_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Touches_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Within_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);

typedef char* (*CR_GEOS_Relate_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char* (*CR_GEOS_RelateBoundaryNodeRule_r)(CR_GEOS_Handle, CR_GEOS_Geometry,
                                                  CR_GEOS_Geometry, int);
typedef char (*CR_GEOS_RelatePattern_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry,
                                        const char*);

typedef CR_GEOS_WKBWriter (*CR_GEOS_WKBWriter_create_r)(CR_GEOS_Handle);
typedef char* (*CR_GEOS_WKBWriter_write_r)(CR_GEOS_Handle, CR_GEOS_WKBWriter, CR_GEOS_Geometry,
                                           size_t*);
typedef void (*CR_GEOS_WKBWriter_setByteOrder_r)(CR_GEOS_Handle, CR_GEOS_WKBWriter, int);
typedef void (*CR_GEOS_WKBWriter_setOutputDimension_r)(CR_GEOS_Handle, CR_GEOS_WKBWriter, int);
typedef void (*CR_GEOS_WKBWriter_destroy_r)(CR_GEOS_Handle, CR_GEOS_WKBWriter);
typedef void (*CR_GEOS_WKBWriter_setIncludeSRID_r)(CR_GEOS_Handle, CR_GEOS_WKBWriter, const char);
typedef CR_GEOS_Geometry (*CR_GEOS_ClipByRect_r)(CR_GEOS_Handle, CR_GEOS_Geometry, double, double,
                                                 double, double);

typedef CR_GEOS_Geometry (*CR_GEOS_SharedPaths_r)(CR_GEOS_Handle, CR_GEOS_Geometry,
                                                  CR_GEOS_Geometry);

typedef CR_GEOS_Geometry (*CR_GEOS_Node_r)(CR_GEOS_Handle, CR_GEOS_Geometry);

typedef CR_GEOS_Geometry (*CR_GEOS_VoronoiDiagram_r)(CR_GEOS_Handle, CR_GEOS_Geometry,
                                                  CR_GEOS_Geometry, double, int);

typedef char (*CR_GEOS_EqualsExact_r)(CR_GEOS_Handle, CR_GEOS_Geometry,
                                                  CR_GEOS_Geometry, double);

typedef CR_GEOS_Geometry (*CR_GEOS_MinimumRotatedRectangle_r)(CR_GEOS_Handle, CR_GEOS_Geometry);

typedef CR_GEOS_Geometry (*CR_GEOS_Snap_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry, double);

#ifdef __cplusplus
}  // extern "C"
#endif

CR_GEOS_String toGEOSString(const char* data, size_t len);
void errorHandler(const char* msg, void* buffer);
