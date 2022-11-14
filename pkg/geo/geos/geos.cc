// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <cstdarg>
#include <cstring>
#include <memory>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

#include "geos.h"
#include "geos_cc.hh"

#define CR_GEOS_NO_ERROR_DEFINED_MESSAGE "geos: returned invalid result but error not populated"

namespace {
  std::string ToString(CR_GEOS_Slice slice) { return std::string(slice.data, slice.len); }
}

CR_GEOS_Slice cStringToSlice(char* cstr) {
  CR_GEOS_Slice slice = {.data = cstr, .len = strlen(cstr)};
  return slice;
}

// Given data that will be deallocated on return, with length
// equal to len, returns a CR_GEOS_String to return to Go.
CR_GEOS_String toGEOSString(const char* data, size_t len) {
  CR_GEOS_String result = {.data = nullptr, .len = len};
  if (len == 0) {
    return result;
  }
  result.data = static_cast<char*>(malloc(len));
  memcpy(result.data, data, len);
  return result;
}

void errorHandler(const char* msg, void* buffer) {
  std::string* str = static_cast<std::string*>(buffer);
  *str = std::string("geos error: ") + msg;
}

CR_GEOS_Handle initHandleWithErrorBuffer(CR_GEOS* lib, std::string* buffer) {
  auto handle = lib->GEOS_init_r();
  lib->GEOSContext_setErrorMessageHandler_r(handle, errorHandler, buffer);
  return handle;
}

CR_GEOS_Geometry CR_GEOS_GeometryFromSlice(CR_GEOS* lib, CR_GEOS_Handle handle,
                                           CR_GEOS_Slice slice) {
  auto wkbReader = lib->GEOSWKBReader_create_r(handle);
  auto geom = lib->GEOSWKBReader_read_r(handle, wkbReader, slice.data, slice.len);
  lib->GEOSWKBReader_destroy_r(handle, wkbReader);
  return geom;
}

void CR_GEOS_writeGeomToEWKB(CR_GEOS* lib, CR_GEOS_Handle handle, CR_GEOS_Geometry geom,
                             CR_GEOS_String* ewkb, int srid) {
  auto hasZ = lib->GEOSHasZ_r(handle, geom);
  auto wkbWriter = lib->GEOSWKBWriter_create_r(handle);
  lib->GEOSWKBWriter_setByteOrder_r(handle, wkbWriter, 1);
  if (hasZ) {
    lib->GEOSWKBWriter_setOutputDimension_r(handle, wkbWriter, 3);
  }
  if (srid != 0) {
    lib->GEOSSetSRID_r(handle, geom, srid);
  }
  lib->GEOSWKBWriter_setIncludeSRID_r(handle, wkbWriter, true);
  ewkb->data = lib->GEOSWKBWriter_write_r(handle, wkbWriter, geom, &ewkb->len);
  lib->GEOSWKBWriter_destroy_r(handle, wkbWriter);
}

CR_GEOS_Status CR_GEOS_WKTToEWKB(CR_GEOS* lib, CR_GEOS_Slice wkt, int srid, CR_GEOS_String* ewkb) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);

  auto wktReader = lib->GEOSWKTReader_create_r(handle);
  auto wktStr = ToString(wkt);
  auto geom = lib->GEOSWKTReader_read_r(handle, wktReader, wktStr.c_str());
  lib->GEOSWKTReader_destroy_r(handle, wktReader);

  if (geom != NULL) {
    CR_GEOS_writeGeomToEWKB(lib, handle, geom, ewkb, srid);
    lib->GEOSGeom_destroy_r(handle, geom);
  }

  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_ClipByRect(CR_GEOS* lib, CR_GEOS_Slice ewkb, double xmin, double ymin,
                                  double xmax, double ymax, CR_GEOS_String* clippedEWKB) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  *clippedEWKB = {.data = NULL, .len = 0};

  auto geom = CR_GEOS_GeometryFromSlice(lib, handle, ewkb);
  if (geom != nullptr) {
    auto clippedGeom = lib->GEOSClipByRect_r(handle, geom, xmin, ymin, xmax, ymax);
    if (clippedGeom != nullptr) {
      auto srid = lib->GEOSGetSRID_r(handle, geom);
      CR_GEOS_writeGeomToEWKB(lib, handle, clippedGeom, clippedEWKB, srid);
      lib->GEOSGeom_destroy_r(handle, clippedGeom);
    }
    lib->GEOSGeom_destroy_r(handle, geom);
  }

  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_Buffer(CR_GEOS* lib, CR_GEOS_Slice ewkb, CR_GEOS_BufferParamsInput params,
                              double distance, CR_GEOS_String* ret) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  *ret = {.data = NULL, .len = 0};

  auto geom = CR_GEOS_GeometryFromSlice(lib, handle, ewkb);
  if (geom != nullptr) {
    auto gParams = lib->GEOSBufferParams_create_r(handle);
    if (gParams != nullptr) {
      if (lib->GEOSBufferParams_setEndCapStyle_r(handle, gParams, params.endCapStyle) &&
          lib->GEOSBufferParams_setJoinStyle_r(handle, gParams, params.joinStyle) &&
          lib->GEOSBufferParams_setMitreLimit_r(handle, gParams, params.mitreLimit) &&
          lib->GEOSBufferParams_setQuadrantSegments_r(handle, gParams, params.quadrantSegments) &&
          lib->GEOSBufferParams_setSingleSided_r(handle, gParams, params.singleSided)) {
        auto bufferedGeom = lib->GEOSBufferWithParams_r(handle, geom, gParams, distance);
        if (bufferedGeom != nullptr) {
          auto srid = lib->GEOSGetSRID_r(handle, geom);
          CR_GEOS_writeGeomToEWKB(lib, handle, bufferedGeom, ret, srid);
          lib->GEOSGeom_destroy_r(handle, bufferedGeom);
        }
      }
      lib->GEOSBufferParams_destroy_r(handle, gParams);
    }
    lib->GEOSGeom_destroy_r(handle, geom);
  }

  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

//
// Unary operators
//

template <typename T, typename R>
CR_GEOS_Status CR_GEOS_UnaryOperator(CR_GEOS* lib, T fn, CR_GEOS_Slice a, R* ret) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  auto geom = CR_GEOS_GeometryFromSlice(lib, handle, a);
  if (geom != nullptr) {
    auto r = fn(handle, geom, ret);
    // ret == 0 indicates an exception.
    if (r == 0) {
      if (error.length() == 0) {
        error.assign(CR_GEOS_NO_ERROR_DEFINED_MESSAGE);
      }
    }
    lib->GEOSGeom_destroy_r(handle, geom);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

template <typename T, typename R>
CR_GEOS_Status CR_GEOS_BinaryOperator(CR_GEOS* lib, T fn, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                      R* ret) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  auto wkbReader = lib->GEOSWKBReader_create_r(handle);
  auto geomA = lib->GEOSWKBReader_read_r(handle, wkbReader, a.data, a.len);
  auto geomB = lib->GEOSWKBReader_read_r(handle, wkbReader, b.data, b.len);
  lib->GEOSWKBReader_destroy_r(handle, wkbReader);
  if (geomA != nullptr && geomB != nullptr) {
    auto r = fn(handle, geomA, geomB, ret);
    // ret == 0 indicates an exception.
    if (r == 0) {
      if (error.length() == 0) {
        error.assign(CR_GEOS_NO_ERROR_DEFINED_MESSAGE);
      }
    }
  }
  if (geomA != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomA);
  }
  if (geomB != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomB);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_Area(CR_GEOS* lib, CR_GEOS_Slice a, double* ret) {
  return CR_GEOS_UnaryOperator(lib, lib->GEOSArea_r, a, ret);
}

CR_GEOS_Status CR_GEOS_Length(CR_GEOS* lib, CR_GEOS_Slice a, double* ret) {
  return CR_GEOS_UnaryOperator(lib, lib->GEOSLength_r, a, ret);
}

CR_GEOS_Status CR_GEOS_MinimumClearance(CR_GEOS* lib, CR_GEOS_Slice g, double* ret) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  auto geom = CR_GEOS_GeometryFromSlice(lib, handle, g);
  *ret = 0;
  if (geom != nullptr) {
    auto r = lib->GEOSMinimumClearance_r(handle, geom, ret);
    if (r == 2) {
      if (error.length() == 0) {
        error.assign(CR_GEOS_NO_ERROR_DEFINED_MESSAGE);
      }
    }
    lib->GEOSGeom_destroy_r(handle, geom);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_MinimumClearanceLine(CR_GEOS* lib, CR_GEOS_Slice g,
                                            CR_GEOS_String* clearanceEWKB) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  *clearanceEWKB = {.data = NULL, .len = 0};

  auto geom = CR_GEOS_GeometryFromSlice(lib, handle, g);
  if (geom != nullptr) {
    auto clearance = lib->GEOSMinimumClearanceLine_r(handle, geom);
    auto srid = lib->GEOSGetSRID_r(handle, clearance);
    CR_GEOS_writeGeomToEWKB(lib, handle, clearance, clearanceEWKB, srid);
    lib->GEOSGeom_destroy_r(handle, geom);
  }

  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_Normalize(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_String* normalizedEWKB) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  *normalizedEWKB = {.data = NULL, .len = 0};

  auto geomA = CR_GEOS_GeometryFromSlice(lib, handle, a);
  if (geomA != nullptr) {
    auto exceptionStatus = lib->GEOSNormalize_r(handle, geomA);
    if (exceptionStatus != -1) {
      auto srid = lib->GEOSGetSRID_r(handle, geomA);
      CR_GEOS_writeGeomToEWKB(lib, handle, geomA, normalizedEWKB, srid);
    } else {
      error.assign(CR_GEOS_NO_ERROR_DEFINED_MESSAGE);
    }
    lib->GEOSGeom_destroy_r(handle, geomA);
  }

  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_LineMerge(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_String* ewkb) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  *ewkb = {.data = NULL, .len = 0};

  auto geomA = CR_GEOS_GeometryFromSlice(lib, handle, a);
  if (geomA != nullptr) {
    auto merged = lib->GEOSLineMerge_r(handle, geomA);
    auto srid = lib->GEOSGetSRID_r(handle, merged);
    CR_GEOS_writeGeomToEWKB(lib, handle, merged, ewkb, srid);
    lib->GEOSGeom_destroy_r(handle, geomA);
  }

  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

//
// Unary predicates.
//

template <typename T>
CR_GEOS_Status CR_GEOS_UnaryPredicate(CR_GEOS* lib, T fn, CR_GEOS_Slice a, char* ret) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  auto geom = CR_GEOS_GeometryFromSlice(lib, handle, a);
  if (geom != nullptr) {
    auto r = fn(handle, geom);
    // r == 2 indicates an exception.
    if (r == 2) {
      if (error.length() == 0) {
        error.assign(CR_GEOS_NO_ERROR_DEFINED_MESSAGE);
      }
    } else {
      *ret = r;
    }
    lib->GEOSGeom_destroy_r(handle, geom);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_IsSimple(CR_GEOS* lib, CR_GEOS_Slice a, char* ret) {
  return CR_GEOS_UnaryPredicate(lib, lib->GEOSisSimple_r, a, ret);
}

//
// Validity checking.
//

CR_GEOS_Status CR_GEOS_IsValid(CR_GEOS* lib, CR_GEOS_Slice g, char* ret) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  auto geom = CR_GEOS_GeometryFromSlice(lib, handle, g);
  *ret = 0;
  if (geom != nullptr) {
    auto r = lib->GEOSisValid_r(handle, geom);
    if (r == 2) {
      if (error.length() == 0) {
        error.assign(CR_GEOS_NO_ERROR_DEFINED_MESSAGE);
      }
    } else {
      *ret = r;
    }
    lib->GEOSGeom_destroy_r(handle, geom);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_IsValidReason(CR_GEOS* lib, CR_GEOS_Slice g, CR_GEOS_String* ret) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  auto geom = CR_GEOS_GeometryFromSlice(lib, handle, g);
  *ret = {.data = NULL, .len = 0};
  if (geom != nullptr) {
    auto r = lib->GEOSisValidReason_r(handle, geom);
    if (r != NULL) {
      *ret = toGEOSString(r, strlen(r));
      lib->GEOSFree_r(handle, r);
    }
    lib->GEOSGeom_destroy_r(handle, geom);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_IsValidDetail(CR_GEOS* lib, CR_GEOS_Slice g, int flags, char* retIsValid,
                                     CR_GEOS_String* retReason, CR_GEOS_String* retLocationEWKB) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  auto geom = CR_GEOS_GeometryFromSlice(lib, handle, g);
  *retReason = {.data = NULL, .len = 0};
  *retLocationEWKB = {.data = NULL, .len = 0};
  if (geom != nullptr) {
    char* reason = NULL;
    CR_GEOS_Geometry loc = NULL;
    auto r = lib->GEOSisValidDetail_r(handle, geom, flags, &reason, &loc);

    if (r == 2) {
      if (error.length() == 0) {
        error.assign(CR_GEOS_NO_ERROR_DEFINED_MESSAGE);
      }
    } else {
      *retIsValid = r;
    }

    if (reason != NULL) {
      *retReason = toGEOSString(reason, strlen(reason));
      lib->GEOSFree_r(handle, reason);
    }

    if (loc != NULL) {
      auto srid = lib->GEOSGetSRID_r(handle, geom);
      CR_GEOS_writeGeomToEWKB(lib, handle, loc, retLocationEWKB, srid);
      lib->GEOSGeom_destroy_r(handle, loc);
    }

    lib->GEOSGeom_destroy_r(handle, geom);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_MakeValid(CR_GEOS* lib, CR_GEOS_Slice g, CR_GEOS_String* validEWKB) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  auto geom = CR_GEOS_GeometryFromSlice(lib, handle, g);
  *validEWKB = {.data = NULL, .len = 0};
  if (geom != nullptr) {
    auto validGeom = lib->GEOSMakeValid_r(handle, geom);
    if (validGeom != nullptr) {
      auto srid = lib->GEOSGetSRID_r(handle, geom);
      CR_GEOS_writeGeomToEWKB(lib, handle, validGeom, validEWKB, srid);
      lib->GEOSGeom_destroy_r(handle, validGeom);
    }
    lib->GEOSGeom_destroy_r(handle, geom);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

//
// Topology operators.
//

CR_GEOS_Status CR_GEOS_Boundary(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_String* boundaryEWKB) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  auto geom = CR_GEOS_GeometryFromSlice(lib, handle, a);
  *boundaryEWKB = {.data = NULL, .len = 0};
  if (geom != nullptr) {
    auto boundaryGeom = lib->GEOSBoundary_r(handle, geom);
    if (boundaryGeom != nullptr) {
      auto srid = lib->GEOSGetSRID_r(handle, geom);
      CR_GEOS_writeGeomToEWKB(lib, handle, boundaryGeom, boundaryEWKB, srid);
      lib->GEOSGeom_destroy_r(handle, boundaryGeom);
    }
    lib->GEOSGeom_destroy_r(handle, geom);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_Centroid(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_String* centroidEWKB) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  auto geom = CR_GEOS_GeometryFromSlice(lib, handle, a);
  *centroidEWKB = {.data = NULL, .len = 0};
  if (geom != nullptr) {
    auto centroidGeom = lib->GEOSGetCentroid_r(handle, geom);
    if (centroidGeom != nullptr) {
      auto srid = lib->GEOSGetSRID_r(handle, geom);
      CR_GEOS_writeGeomToEWKB(lib, handle, centroidGeom, centroidEWKB, srid);
      lib->GEOSGeom_destroy_r(handle, centroidGeom);
    }
    lib->GEOSGeom_destroy_r(handle, geom);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_MinimumBoundingCircle(CR_GEOS* lib, CR_GEOS_Slice a, double* radius,
                                              CR_GEOS_String* centerEWKB, CR_GEOS_String* polygonEWKB) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  auto geom = CR_GEOS_GeometryFromSlice(lib, handle, a);
  CR_GEOS_Geometry* centerGeom;
  *polygonEWKB = {.data = NULL, .len = 0};
  if (geom != nullptr) {
    auto circlePolygonGeom = lib->GEOSMinimumBoundingCircle_r(handle, geom, radius, &centerGeom);
    if (circlePolygonGeom != nullptr) {
      auto srid = lib->GEOSGetSRID_r(handle, geom);
      CR_GEOS_writeGeomToEWKB(lib, handle, circlePolygonGeom, polygonEWKB, srid);
      lib->GEOSGeom_destroy_r(handle, circlePolygonGeom);
    }
    if (centerGeom != nullptr) {
      auto srid = lib->GEOSGetSRID_r(handle, geom);
      CR_GEOS_writeGeomToEWKB(lib, handle, centerGeom, centerEWKB, srid);
      lib->GEOSGeom_destroy_r(handle, centerGeom);
    }
    lib->GEOSGeom_destroy_r(handle, geom);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_ConvexHull(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_String* convexHullEWKB) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  auto geom = CR_GEOS_GeometryFromSlice(lib, handle, a);
  *convexHullEWKB = {.data = NULL, .len = 0};
  if (geom != nullptr) {
    auto convexHullGeom = lib->GEOSConvexHull_r(handle, geom);
    if (convexHullGeom != nullptr) {
      auto srid = lib->GEOSGetSRID_r(handle, geom);
      CR_GEOS_writeGeomToEWKB(lib, handle, convexHullGeom, convexHullEWKB, srid);
      lib->GEOSGeom_destroy_r(handle, convexHullGeom);
    }
    lib->GEOSGeom_destroy_r(handle, geom);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_Difference(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                  CR_GEOS_String* diffEWKB) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  auto geomA = CR_GEOS_GeometryFromSlice(lib, handle, a);
  auto geomB = CR_GEOS_GeometryFromSlice(lib, handle, b);
  *diffEWKB = {.data = NULL, .len = 0};
  if (geomA != nullptr && geomB != nullptr) {
    auto diffGeom = lib->GEOSDifference_r(handle, geomA, geomB);
    if (diffGeom != nullptr) {
      auto srid = lib->GEOSGetSRID_r(handle, geomA);
      CR_GEOS_writeGeomToEWKB(lib, handle, diffGeom, diffEWKB, srid);
      lib->GEOSGeom_destroy_r(handle, diffGeom);
    }
  }
  if (geomA != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomA);
  }
  if (geomB != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomB);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_Simplify(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_String* simplifyEWKB,
                                double tolerance) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  auto geom = CR_GEOS_GeometryFromSlice(lib, handle, a);
  *simplifyEWKB = {.data = NULL, .len = 0};
  if (geom != nullptr) {
    auto simplifyGeom = lib->GEOSSimplify_r(handle, geom, tolerance);
    if (simplifyGeom != nullptr) {
      auto srid = lib->GEOSGetSRID_r(handle, geom);
      CR_GEOS_writeGeomToEWKB(lib, handle, simplifyGeom, simplifyEWKB, srid);
      lib->GEOSGeom_destroy_r(handle, simplifyGeom);
    }
    lib->GEOSGeom_destroy_r(handle, geom);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_TopologyPreserveSimplify(CR_GEOS* lib, CR_GEOS_Slice a,
                                                CR_GEOS_String* simplifyEWKB, double tolerance) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  auto geom = CR_GEOS_GeometryFromSlice(lib, handle, a);
  *simplifyEWKB = {.data = NULL, .len = 0};
  if (geom != nullptr) {
    auto simplifyGeom = lib->GEOSTopologyPreserveSimplify_r(handle, geom, tolerance);
    if (simplifyGeom != nullptr) {
      auto srid = lib->GEOSGetSRID_r(handle, geom);
      CR_GEOS_writeGeomToEWKB(lib, handle, simplifyGeom, simplifyEWKB, srid);
      lib->GEOSGeom_destroy_r(handle, simplifyGeom);
    }
    lib->GEOSGeom_destroy_r(handle, geom);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_UnaryUnion(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_String* unionEWKB) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);

  auto geomA = CR_GEOS_GeometryFromSlice(lib, handle, a);
  *unionEWKB = {.data = NULL, .len = 0};
  if (geomA != nullptr) {
    auto r = lib->GEOSUnaryUnion_r(handle, geomA);
    if (r != NULL) {
      auto srid = lib->GEOSGetSRID_r(handle, r);
      CR_GEOS_writeGeomToEWKB(lib, handle, r, unionEWKB, srid);
      lib->GEOSGeom_destroy_r(handle, r);
    }
    lib->GEOSGeom_destroy_r(handle, geomA);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_Union(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                             CR_GEOS_String* unionEWKB) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  *unionEWKB = {.data = NULL, .len = 0};

  auto geomA = CR_GEOS_GeometryFromSlice(lib, handle, a);
  auto geomB = CR_GEOS_GeometryFromSlice(lib, handle, b);
  if (geomA != nullptr && geomB != nullptr) {
    auto unionGeom = lib->GEOSUnion_r(handle, geomA, geomB);
    if (unionGeom != nullptr) {
      auto srid = lib->GEOSGetSRID_r(handle, geomA);
      CR_GEOS_writeGeomToEWKB(lib, handle, unionGeom, unionEWKB, srid);
      lib->GEOSGeom_destroy_r(handle, unionGeom);
    }
  }
  if (geomA != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomA);
  }
  if (geomB != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomB);
  }

  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_PointOnSurface(CR_GEOS* lib, CR_GEOS_Slice a,
                                      CR_GEOS_String* pointOnSurfaceEWKB) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  auto geom = CR_GEOS_GeometryFromSlice(lib, handle, a);
  *pointOnSurfaceEWKB = {.data = NULL, .len = 0};
  if (geom != nullptr) {
    auto pointOnSurfaceGeom = lib->GEOSPointOnSurface_r(handle, geom);
    if (pointOnSurfaceGeom != nullptr) {
      auto srid = lib->GEOSGetSRID_r(handle, geom);
      CR_GEOS_writeGeomToEWKB(lib, handle, pointOnSurfaceGeom, pointOnSurfaceEWKB, srid);
      lib->GEOSGeom_destroy_r(handle, pointOnSurfaceGeom);
    }
    lib->GEOSGeom_destroy_r(handle, geom);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_Intersection(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                    CR_GEOS_String* intersectionEWKB) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  *intersectionEWKB = {.data = NULL, .len = 0};

  auto geomA = CR_GEOS_GeometryFromSlice(lib, handle, a);
  auto geomB = CR_GEOS_GeometryFromSlice(lib, handle, b);
  if (geomA != nullptr && geomB != nullptr) {
    auto intersectionGeom = lib->GEOSIntersection_r(handle, geomA, geomB);
    if (intersectionGeom != nullptr) {
      auto srid = lib->GEOSGetSRID_r(handle, geomA);
      CR_GEOS_writeGeomToEWKB(lib, handle, intersectionGeom, intersectionEWKB, srid);
      lib->GEOSGeom_destroy_r(handle, intersectionGeom);
    }
  }
  if (geomA != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomA);
  }
  if (geomB != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomB);
  }

  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_SymDifference(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                     CR_GEOS_String* symdifferenceEWKB) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  *symdifferenceEWKB = {.data = NULL, .len = 0};

  auto geomA = CR_GEOS_GeometryFromSlice(lib, handle, a);
  auto geomB = CR_GEOS_GeometryFromSlice(lib, handle, b);
  if (geomA != nullptr && geomB != nullptr) {
    auto symdifferenceGeom = lib->GEOSSymDifference_r(handle, geomA, geomB);
    if (symdifferenceGeom != nullptr) {
      auto srid = lib->GEOSGetSRID_r(handle, geomA);
      CR_GEOS_writeGeomToEWKB(lib, handle, symdifferenceGeom, symdifferenceEWKB, srid);
      lib->GEOSGeom_destroy_r(handle, symdifferenceGeom);
    }
  }
  if (geomA != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomA);
  }
  if (geomB != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomB);
  }

  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

//
// Linear Reference
//

CR_GEOS_Status CR_GEOS_Interpolate(CR_GEOS* lib, CR_GEOS_Slice a, double distance,
                                   CR_GEOS_String* interpolatedPointEWKB) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  auto geom = CR_GEOS_GeometryFromSlice(lib, handle, a);
  *interpolatedPointEWKB = {.data = NULL, .len = 0};
  if (geom != nullptr) {
    auto interpolatedPoint = lib->GEOSInterpolate_r(handle, geom, distance);
    if (interpolatedPoint != nullptr) {
      auto srid = lib->GEOSGetSRID_r(handle, geom);
      CR_GEOS_writeGeomToEWKB(lib, handle, interpolatedPoint, interpolatedPointEWKB, srid);
      lib->GEOSGeom_destroy_r(handle, interpolatedPoint);
    }
    lib->GEOSGeom_destroy_r(handle, geom);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

//
// Binary operators
//

CR_GEOS_Status CR_GEOS_Distance(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, double* ret) {
  return CR_GEOS_BinaryOperator(lib, lib->GEOSDistance_r, a, b, ret);
}
CR_GEOS_Status CR_GEOS_FrechetDistance(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                       double* ret) {
  return CR_GEOS_BinaryOperator(lib, lib->GEOSFrechetDistance_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_FrechetDistanceDensify(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                              double densifyFrac, double* ret) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);

  auto wkbReader = lib->GEOSWKBReader_create_r(handle);
  auto geomA = lib->GEOSWKBReader_read_r(handle, wkbReader, a.data, a.len);
  auto geomB = lib->GEOSWKBReader_read_r(handle, wkbReader, b.data, b.len);
  lib->GEOSWKBReader_destroy_r(handle, wkbReader);

  if (geomA != nullptr && geomB != nullptr) {
    auto r = lib->GEOSFrechetDistanceDensify_r(handle, geomA, geomB, densifyFrac, ret);
    // ret == 0 indicates an exception.
    if (r == 0) {
      if (error.length() == 0) {
        error.assign(CR_GEOS_NO_ERROR_DEFINED_MESSAGE);
      }
    }
  }
  if (geomA != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomA);
  }
  if (geomB != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomB);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_HausdorffDistance(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                         double* ret) {
  return CR_GEOS_BinaryOperator(lib, lib->GEOSHausdorffDistance_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_HausdorffDistanceDensify(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                                double densifyFrac, double* ret) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);

  auto wkbReader = lib->GEOSWKBReader_create_r(handle);
  auto geomA = lib->GEOSWKBReader_read_r(handle, wkbReader, a.data, a.len);
  auto geomB = lib->GEOSWKBReader_read_r(handle, wkbReader, b.data, b.len);
  lib->GEOSWKBReader_destroy_r(handle, wkbReader);

  if (geomA != nullptr && geomB != nullptr) {
    auto r = lib->GEOSHausdorffDistanceDensify_r(handle, geomA, geomB, densifyFrac, ret);
    // ret == 0 indicates an exception.
    if (r == 0) {
      if (error.length() == 0) {
        error.assign(CR_GEOS_NO_ERROR_DEFINED_MESSAGE);
      }
    }
  }
  if (geomA != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomA);
  }
  if (geomB != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomB);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

//
// PreparedGeometry
//

CR_GEOS_Status CR_GEOS_Prepare(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_PreparedGeometry** ret) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);

  auto wkbReader = lib->GEOSWKBReader_create_r(handle);
  auto geom = lib->GEOSWKBReader_read_r(handle, wkbReader, a.data, a.len);
  lib->GEOSWKBReader_destroy_r(handle, wkbReader);
  if (geom != nullptr) {
    auto preparedGeom = lib->GEOSPrepare_r(handle, geom);
    auto tmp = new CR_GEOS_PreparedGeometry();
    tmp->g = geom;
    tmp->p = preparedGeom;
    *ret = tmp;
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_PreparedGeometryDestroy(CR_GEOS* lib, CR_GEOS_PreparedGeometry* a) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  lib->GEOSPreparedGeom_destroy_r(handle, a->p);
  lib->GEOSGeom_destroy_r(handle, a->g);
  lib->GEOS_finish_r(handle);
  delete a;
  return toGEOSString(error.data(), error.length());
}

template <typename T>
CR_GEOS_Status CR_GEOS_PreparedBinaryPredicate(CR_GEOS* lib, T fn, CR_GEOS_PreparedInternalGeometry a, CR_GEOS_Slice b,
                                       char* ret) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);

  auto wkbReader = lib->GEOSWKBReader_create_r(handle);
  auto geomB = lib->GEOSWKBReader_read_r(handle, wkbReader, b.data, b.len);
  lib->GEOSWKBReader_destroy_r(handle, wkbReader);

  if (geomB != nullptr) {
    auto r = fn(handle, a, geomB);
    // ret == 2 indicates an exception.
    if (r == 2) {
      if (error.length() == 0) {
        error.assign(CR_GEOS_NO_ERROR_DEFINED_MESSAGE);
      }
    } else {
      *ret = r;
    }
  }
  if (geomB != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomB);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_PreparedIntersects(CR_GEOS* lib, CR_GEOS_PreparedGeometry* a, CR_GEOS_Slice b, char* ret) {
  return CR_GEOS_PreparedBinaryPredicate(lib, lib->GEOSPreparedIntersects_r, a->p, b, ret);
}

//
// Binary predicates
//

template <typename T>
CR_GEOS_Status CR_GEOS_BinaryPredicate(CR_GEOS* lib, T fn, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                       char* ret) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);

  auto wkbReader = lib->GEOSWKBReader_create_r(handle);
  auto geomA = lib->GEOSWKBReader_read_r(handle, wkbReader, a.data, a.len);
  auto geomB = lib->GEOSWKBReader_read_r(handle, wkbReader, b.data, b.len);
  lib->GEOSWKBReader_destroy_r(handle, wkbReader);

  if (geomA != nullptr && geomB != nullptr) {
    auto r = fn(handle, geomA, geomB);
    // ret == 2 indicates an exception.
    if (r == 2) {
      if (error.length() == 0) {
        error.assign(CR_GEOS_NO_ERROR_DEFINED_MESSAGE);
      }
    } else {
      *ret = r;
    }
  }
  if (geomA != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomA);
  }
  if (geomB != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomB);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_Covers(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSCovers_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_CoveredBy(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSCoveredBy_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_Contains(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSContains_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_Crosses(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSCrosses_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_Disjoint(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSDisjoint_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_Equals(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSEquals_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_Intersects(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSIntersects_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_Overlaps(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSOverlaps_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_PreparedIntersects(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSPreparedIntersects_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_Touches(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSTouches_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_Within(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSWithin_r, a, b, ret);
}

//
// DE-9IM related
// See: https://en.wikipedia.org/wiki/DE-9IM.
//

CR_GEOS_Status CR_GEOS_Relate(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, CR_GEOS_String* ret) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);

  auto wkbReader = lib->GEOSWKBReader_create_r(handle);
  auto geomA = lib->GEOSWKBReader_read_r(handle, wkbReader, a.data, a.len);
  auto geomB = lib->GEOSWKBReader_read_r(handle, wkbReader, b.data, b.len);
  lib->GEOSWKBReader_destroy_r(handle, wkbReader);

  if (geomA != nullptr && geomB != nullptr) {
    auto r = lib->GEOSRelate_r(handle, geomA, geomB);
    if (r != NULL) {
      *ret = toGEOSString(r, strlen(r));
      lib->GEOSFree_r(handle, r);
    }
  }
  if (geomA != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomA);
  }
  if (geomB != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomB);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_RelateBoundaryNodeRule(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                              int bnr, CR_GEOS_String* ret) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);

  auto wkbReader = lib->GEOSWKBReader_create_r(handle);
  auto geomA = lib->GEOSWKBReader_read_r(handle, wkbReader, a.data, a.len);
  auto geomB = lib->GEOSWKBReader_read_r(handle, wkbReader, b.data, b.len);
  lib->GEOSWKBReader_destroy_r(handle, wkbReader);

  if (geomA != nullptr && geomB != nullptr) {
    auto r = lib->GEOSRelateBoundaryNodeRule_r(handle, geomA, geomB, bnr);
    if (r != NULL) {
      *ret = toGEOSString(r, strlen(r));
      lib->GEOSFree_r(handle, r);
    }
  }
  if (geomA != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomA);
  }
  if (geomB != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomB);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_RelatePattern(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                     CR_GEOS_Slice pattern, char* ret) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);

  auto wkbReader = lib->GEOSWKBReader_create_r(handle);
  auto geomA = lib->GEOSWKBReader_read_r(handle, wkbReader, a.data, a.len);
  auto geomB = lib->GEOSWKBReader_read_r(handle, wkbReader, b.data, b.len);
  auto p = std::string(pattern.data, pattern.len);
  lib->GEOSWKBReader_destroy_r(handle, wkbReader);

  if (geomA != nullptr && geomB != nullptr) {
    auto r = lib->GEOSRelatePattern_r(handle, geomA, geomB, p.c_str());
    // ret == 2 indicates an exception.
    if (r == 2) {
      if (error.length() == 0) {
        error.assign(CR_GEOS_NO_ERROR_DEFINED_MESSAGE);
      }
    } else {
      *ret = r;
    }
  }
  if (geomA != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomA);
  }
  if (geomB != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomB);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_SharedPaths(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b,
                                   CR_GEOS_String* ret) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);

  auto wkbReader = lib->GEOSWKBReader_create_r(handle);
  auto geomA = lib->GEOSWKBReader_read_r(handle, wkbReader, a.data, a.len);
  auto geomB = lib->GEOSWKBReader_read_r(handle, wkbReader, b.data, b.len);
  lib->GEOSWKBReader_destroy_r(handle, wkbReader);
  *ret = {.data = NULL, .len = 0};
  if (geomA != nullptr && geomB != nullptr) {
    auto r = lib->GEOSSharedPaths_r(handle, geomA, geomB);
    if (r != NULL) {
      auto srid = lib->GEOSGetSRID_r(handle, r);
      CR_GEOS_writeGeomToEWKB(lib, handle, r, ret, srid);
      lib->GEOSGeom_destroy_r(handle, r);
    }
  }
  if (geomA != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomA);
  }
  if (geomB != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomB);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_Node(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_String* nodeEWKB) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  *nodeEWKB = {.data = NULL, .len = 0};

  auto geom = CR_GEOS_GeometryFromSlice(lib, handle, a);
  if (geom != nullptr) {
    auto r = lib->GEOSNode_r(handle, geom);
    if (r != NULL) {
      auto srid = lib->GEOSGetSRID_r(handle, r);
      CR_GEOS_writeGeomToEWKB(lib,handle,r,nodeEWKB, srid);
      lib->GEOSGeom_destroy_r(handle, r);
    }
    lib->GEOSGeom_destroy_r(handle, geom);
  }

  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_VoronoiDiagram(CR_GEOS* lib, CR_GEOS_Slice g, CR_GEOS_Slice env,
                                      double tolerance, int onlyEdges, CR_GEOS_String* ret) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);

  auto geomG = CR_GEOS_GeometryFromSlice(lib, handle, g);
  CR_GEOS_Geometry geomEnv = nullptr;
  if (env.data != nullptr) {
   geomEnv = CR_GEOS_GeometryFromSlice(lib, handle, env);
  }
  *ret = {.data = NULL, .len = 0};

  if (geomG != nullptr) {
    auto r = lib->GEOSVoronoiDiagram_r(handle, geomG, geomEnv, tolerance, onlyEdges);
    if (r != NULL) {
      auto srid = lib->GEOSGetSRID_r(handle, r);
      CR_GEOS_writeGeomToEWKB(lib, handle, r, ret, srid);
      lib->GEOSGeom_destroy_r(handle, r);
    }
    lib->GEOSGeom_destroy_r(handle, geomG);
  }
  if (geomEnv != nullptr) {
    lib->GEOSGeom_destroy_r(handle, geomEnv);
  }

  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_EqualsExact(CR_GEOS* lib, CR_GEOS_Slice lhs, CR_GEOS_Slice rhs,
                                      double tolerance, char* ret) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  auto lhsGeom = CR_GEOS_GeometryFromSlice(lib, handle, lhs);
  auto rhsGeom = CR_GEOS_GeometryFromSlice(lib, handle, rhs);
  *ret = 0;
  if (lhsGeom != nullptr && rhsGeom != nullptr) {
    auto r = lib->GEOSEqualsExact_r(handle, lhsGeom, rhsGeom, tolerance);
    *ret = r;
  }
  if (lhsGeom != nullptr) {
    lib->GEOSGeom_destroy_r(handle, lhsGeom);
  }
  if (rhsGeom != nullptr) {
    lib->GEOSGeom_destroy_r(handle, rhsGeom);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_MinimumRotatedRectangle(CR_GEOS* lib, CR_GEOS_Slice g, CR_GEOS_String* ret) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  auto gGeom = CR_GEOS_GeometryFromSlice(lib, handle, g);
  *ret = {.data = NULL, .len = 0};
  if (gGeom != nullptr) {
    auto r = lib->GEOSMinimumRotatedRectangle_r(handle, gGeom);
    if (r != NULL) {
      auto srid = lib->GEOSGetSRID_r(handle, r);
      CR_GEOS_writeGeomToEWKB(lib, handle, r, ret, srid);
      lib->GEOSGeom_destroy_r(handle, r);
    }
    lib->GEOSGeom_destroy_r(handle, gGeom);
  }

  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_Snap(CR_GEOS* lib, CR_GEOS_Slice input, CR_GEOS_Slice target, double tolerance, CR_GEOS_String* snappedEWKB) {
  std::string error;
  auto handle = initHandleWithErrorBuffer(lib, &error);
  auto gGeomInput = CR_GEOS_GeometryFromSlice(lib, handle, input);
  auto gGeomTarget = CR_GEOS_GeometryFromSlice(lib, handle, target);
  *snappedEWKB = {.data = NULL, .len = 0};
  if (gGeomInput != nullptr && gGeomTarget != nullptr) {
    auto r = lib->GEOSSnap_r(handle, gGeomInput, gGeomTarget, tolerance);
    if (r != NULL) {
      auto srid = lib->GEOSGetSRID_r(handle, r);
      CR_GEOS_writeGeomToEWKB(lib, handle, r, snappedEWKB, srid);
      lib->GEOSGeom_destroy_r(handle, r);
    }
  }
  if (gGeomInput != nullptr) {
    lib->GEOSGeom_destroy_r(handle, gGeomInput);
  }
  if (gGeomTarget != nullptr) {
    lib->GEOSGeom_destroy_r(handle, gGeomTarget);
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}
