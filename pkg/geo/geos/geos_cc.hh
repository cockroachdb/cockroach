// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <string>

#ifdef __cplusplus
extern "C" {
#endif

#if _WIN32
#include <windows.h>
typedef HMODULE dlhandle;
#else
typedef void* dlhandle;
#endif  // #if _WIN32

#ifdef __cplusplus
}  // extern "C"
#endif

struct CR_GEOS {
  dlhandle geoscHandle;
  dlhandle geosHandle;

  CR_GEOS_init_r GEOS_init_r;
  CR_GEOS_finish_r GEOS_finish_r;
  CR_GEOS_Context_setErrorMessageHandler_r GEOSContext_setErrorMessageHandler_r;
  CR_GEOS_Free_r GEOSFree_r;

  CR_GEOS_HasZ_r GEOSHasZ_r;
  CR_GEOS_IsEmpty_r GEOSisEmpty_r;
  CR_GEOS_IsSimple_r GEOSisSimple_r;
  CR_GEOS_GeomTypeId_r GEOSGeomTypeId_r;

  CR_GEOS_BufferParams_create_r GEOSBufferParams_create_r;
  CR_GEOS_GEOSBufferParams_destroy_r GEOSBufferParams_destroy_r;
  CR_GEOS_BufferParams_setEndCapStyle_r GEOSBufferParams_setEndCapStyle_r;
  CR_GEOS_BufferParams_setJoinStyle_r GEOSBufferParams_setJoinStyle_r;
  CR_GEOS_BufferParams_setMitreLimit_r GEOSBufferParams_setMitreLimit_r;
  CR_GEOS_BufferParams_setQuadrantSegments_r GEOSBufferParams_setQuadrantSegments_r;
  CR_GEOS_BufferParams_setSingleSided_r GEOSBufferParams_setSingleSided_r;
  CR_GEOS_BufferWithParams_r GEOSBufferWithParams_r;

  CR_GEOS_SetSRID_r GEOSSetSRID_r;
  CR_GEOS_GetSRID_r GEOSGetSRID_r;
  CR_GEOS_GeomDestroy_r GEOSGeom_destroy_r;

  CR_GEOS_WKTReader_create_r GEOSWKTReader_create_r;
  CR_GEOS_WKTReader_destroy_r GEOSWKTReader_destroy_r;
  CR_GEOS_WKTReader_read_r GEOSWKTReader_read_r;

  CR_GEOS_WKBReader_create_r GEOSWKBReader_create_r;
  CR_GEOS_WKBReader_destroy_r GEOSWKBReader_destroy_r;
  CR_GEOS_WKBReader_read_r GEOSWKBReader_read_r;

  CR_GEOS_isValid_r GEOSisValid_r;
  CR_GEOS_isValidReason_r GEOSisValidReason_r;
  CR_GEOS_isValidDetail_r GEOSisValidDetail_r;
  CR_GEOS_MakeValid_r GEOSMakeValid_r;

  CR_GEOS_Area_r GEOSArea_r;
  CR_GEOS_Length_r GEOSLength_r;
  CR_GEOS_MinimumClearance_r GEOSMinimumClearance_r;
  CR_GEOS_MinimumClearanceLine_r GEOSMinimumClearanceLine_r;
  CR_GEOS_Normalize_r GEOSNormalize_r;
  CR_GEOS_LineMerge_r GEOSLineMerge_r;

  CR_GEOS_Boundary_r GEOSBoundary_r;
  CR_GEOS_Centroid_r GEOSGetCentroid_r;
  CR_GEOS_ConvexHull_r GEOSConvexHull_r;
  CR_GEOS_Difference_r GEOSDifference_r;
  CR_GEOS_Simplify_r GEOSSimplify_r;
  CR_GEOS_TopologyPreserveSimplify_r GEOSTopologyPreserveSimplify_r;
  CR_GEOS_UnaryUnion_r GEOSUnaryUnion_r;
  CR_GEOS_Union_r GEOSUnion_r;
  CR_GEOS_PointOnSurface_r GEOSPointOnSurface_r;
  CR_GEOS_Intersection_r GEOSIntersection_r;
  CR_GEOS_SymDifference_r GEOSSymDifference_r;

  CR_GEOS_MinimumBoundingCircle_r GEOSMinimumBoundingCircle_r;

  CR_GEOS_Interpolate_r GEOSInterpolate_r;

  CR_GEOS_Distance_r GEOSDistance_r;
  CR_GEOS_FrechetDistance_r GEOSFrechetDistance_r;
  CR_GEOS_FrechetDistanceDensify_r GEOSFrechetDistanceDensify_r;
  CR_GEOS_HausdorffDistance_r GEOSHausdorffDistance_r;
  CR_GEOS_HausdorffDistanceDensify_r GEOSHausdorffDistanceDensify_r;

  CR_GEOS_Prepare_r GEOSPrepare_r;
  CR_GEOS_PreparedGeom_destroy_r GEOSPreparedGeom_destroy_r;

  CR_GEOS_PreparedIntersects_r GEOSPreparedIntersects_r;

  CR_GEOS_Covers_r GEOSCovers_r;
  CR_GEOS_CoveredBy_r GEOSCoveredBy_r;
  CR_GEOS_Contains_r GEOSContains_r;
  CR_GEOS_Crosses_r GEOSCrosses_r;
  CR_GEOS_Disjoint_r GEOSDisjoint_r;
  CR_GEOS_Equals_r GEOSEquals_r;
  CR_GEOS_Intersects_r GEOSIntersects_r;
  CR_GEOS_Overlaps_r GEOSOverlaps_r;
  CR_GEOS_Touches_r GEOSTouches_r;
  CR_GEOS_Within_r GEOSWithin_r;

  CR_GEOS_Relate_r GEOSRelate_r;
  CR_GEOS_RelateBoundaryNodeRule_r GEOSRelateBoundaryNodeRule_r;
  CR_GEOS_RelatePattern_r GEOSRelatePattern_r;

  CR_GEOS_WKBWriter_create_r GEOSWKBWriter_create_r;
  CR_GEOS_WKBWriter_destroy_r GEOSWKBWriter_destroy_r;
  CR_GEOS_WKBWriter_setByteOrder_r GEOSWKBWriter_setByteOrder_r;
  CR_GEOS_WKBWriter_setOutputDimension_r GEOSWKBWriter_setOutputDimension_r;
  CR_GEOS_WKBWriter_setIncludeSRID_r GEOSWKBWriter_setIncludeSRID_r;
  CR_GEOS_WKBWriter_write_r GEOSWKBWriter_write_r;

  CR_GEOS_ClipByRect_r GEOSClipByRect_r;

  CR_GEOS_SharedPaths_r GEOSSharedPaths_r;
  CR_GEOS_VoronoiDiagram_r GEOSVoronoiDiagram_r;
  CR_GEOS_EqualsExact_r GEOSEqualsExact_r;
  CR_GEOS_MinimumRotatedRectangle_r GEOSMinimumRotatedRectangle_r;

  CR_GEOS_Node_r GEOSNode_r;

  CR_GEOS_Snap_r GEOSSnap_r;

  CR_GEOS(dlhandle geoscHandle, dlhandle geosHandle)
      : geoscHandle(geoscHandle), geosHandle(geosHandle) {}

  ~CR_GEOS();
  char *Init();
};

