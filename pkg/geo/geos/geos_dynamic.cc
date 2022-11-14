// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build libgeos_dynamic
// +build libgeos_dynamic

#if _WIN32
#include <windows.h>
#define dlopen(x, y) LoadLibrary(x)
#define dlsym GetProcAddress
#define dlclose FreeLibrary
#define dlerror() ((char*)"failed to execute dlsym")
#else
#include <dlfcn.h>
#endif  // #if _WIN32

#include "geos.h"
#include "geos_cc.hh"

namespace {
    std::string ToString(CR_GEOS_Slice slice); { return std::string(slice.data, slice.len); }
  
    template <typename T> char* InitSym(T* ptr, const char* symbol) {
    *ptr = reinterpret_cast<T>(dlsym(geoscHandle, symbol));
    if (ptr == nullptr) {
      return dlerror();
    }
    return nullptr;
  }
}  // namespace

CR_GEOS::~CR_GEOS() {
  if (geoscHandle != NULL) {
      dlclose(geoscHandle);
  }
  if (geosHandle != NULL) {
    dlclose(geosHandle);
  }
}

char *CR_GEOS::Init() {
  #define INIT(x)                                                                                    \
  do {                                                                                             \
    auto error = InitSym(&x, #x);                                                                  \
    if (error != nullptr) {                                                                        \
      return error;                                                                                \
    }                                                                                              \
  } while (0)

    INIT(GEOS_init_r);
    INIT(GEOS_finish_r);
    INIT(GEOSFree_r);
    INIT(GEOSContext_setErrorMessageHandler_r);
    INIT(GEOSGeom_destroy_r);
    INIT(GEOSBufferParams_create_r);
    INIT(GEOSBufferParams_destroy_r);
    INIT(GEOSBufferParams_setEndCapStyle_r);
    INIT(GEOSBufferParams_setJoinStyle_r);
    INIT(GEOSBufferParams_setMitreLimit_r);
    INIT(GEOSBufferParams_setQuadrantSegments_r);
    INIT(GEOSBufferParams_setSingleSided_r);
    INIT(GEOSBufferWithParams_r);
    INIT(GEOSHasZ_r);
    INIT(GEOSisEmpty_r);
    INIT(GEOSGeomTypeId_r);
    INIT(GEOSSetSRID_r);
    INIT(GEOSGetSRID_r);
    INIT(GEOSisValid_r);
    INIT(GEOSisValidReason_r);
    INIT(GEOSisValidDetail_r);
    INIT(GEOSMakeValid_r);
    INIT(GEOSArea_r);
    INIT(GEOSLength_r);
    INIT(GEOSMinimumClearance_r);
    INIT(GEOSMinimumClearanceLine_r);
    INIT(GEOSNormalize_r);
    INIT(GEOSLineMerge_r);
    INIT(GEOSisSimple_r);
    INIT(GEOSBoundary_r);
    INIT(GEOSDifference_r);
    INIT(GEOSGetCentroid_r);
    INIT(GEOSMinimumBoundingCircle_r);
    INIT(GEOSConvexHull_r);
    INIT(GEOSSimplify_r);
    INIT(GEOSTopologyPreserveSimplify_r);
    INIT(GEOSUnaryUnion_r);
    INIT(GEOSUnion_r);
    INIT(GEOSPointOnSurface_r);
    INIT(GEOSIntersection_r);
    INIT(GEOSSymDifference_r);
    INIT(GEOSInterpolate_r);
    INIT(GEOSDistance_r);
    INIT(GEOSFrechetDistance_r);
    INIT(GEOSFrechetDistanceDensify_r);
    INIT(GEOSHausdorffDistance_r);
    INIT(GEOSHausdorffDistanceDensify_r);
    INIT(GEOSPrepare_r);
    INIT(GEOSPreparedGeom_destroy_r);
    INIT(GEOSPreparedIntersects_r);
    INIT(GEOSCovers_r);
    INIT(GEOSCoveredBy_r);
    INIT(GEOSContains_r);
    INIT(GEOSCrosses_r);
    INIT(GEOSDisjoint_r);
    INIT(GEOSEquals_r);
    INIT(GEOSIntersects_r);
    INIT(GEOSOverlaps_r);
    INIT(GEOSTouches_r);
    INIT(GEOSWithin_r);
    INIT(GEOSRelate_r);
    INIT(GEOSVoronoiDiagram_r);
    INIT(GEOSEqualsExact_r);
    INIT(GEOSMinimumRotatedRectangle_r);
    INIT(GEOSRelateBoundaryNodeRule_r);
    INIT(GEOSRelatePattern_r);
    INIT(GEOSSharedPaths_r);
    INIT(GEOSWKTReader_create_r);
    INIT(GEOSWKTReader_destroy_r);
    INIT(GEOSWKTReader_read_r);
    INIT(GEOSWKBReader_create_r);
    INIT(GEOSWKBReader_destroy_r);
    INIT(GEOSWKBReader_read_r);
    INIT(GEOSWKBWriter_create_r);
    INIT(GEOSWKBWriter_destroy_r);
    INIT(GEOSWKBWriter_setByteOrder_r);
    INIT(GEOSWKBWriter_setOutputDimension_r);
    INIT(GEOSWKBWriter_setIncludeSRID_r);
    INIT(GEOSWKBWriter_write_r);
    INIT(GEOSClipByRect_r);
    INIT(GEOSNode_r);
    INIT(GEOSSnap_r);
    return nullptr;

#undef INIT
}

CR_GEOS_Status CR_GEOS_Init(CR_GEOS_Slice geoscLoc, CR_GEOS_Slice geosLoc, CR_GEOS** lib) {
  // Open the libgeos.$(EXT) first, so that libgeos_c.$(EXT) can read it.
  std::string error;
  auto geosLocStr = ToString(geosLoc);
  dlhandle geosHandle = dlopen(geosLocStr.c_str(), RTLD_LAZY);
  if (!geosHandle) {
    errorHandler(dlerror(), &error);
    return toGEOSString(error.data(), error.length());
  }

  auto geoscLocStr = ToString(geoscLoc);
  dlhandle geoscHandle = dlopen(geoscLocStr.c_str(), RTLD_LAZY);
  if (!geoscHandle) {
    errorHandler(dlerror(), &error);
    dlclose(geosHandle);
    return toGEOSString(error.data(), error.length());
  }

  std::unique_ptr<CR_GEOS> ret(new CR_GEOS(geoscHandle, geosHandle));
  auto initError = ret->Init();
  if (initError != nullptr) {
    errorHandler(initError, &error);
    dlclose(geosHandle);
    dlclose(geoscHandle);
    return toGEOSString(error.data(), error.length());
  }

  *lib = ret.release();
  return toGEOSString(error.data(), error.length());
}
