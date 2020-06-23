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
#if _WIN32
#include <windows.h>
#else
#include <dlfcn.h>
#endif  // #if _WIN32
#include <memory>
#include <stdlib.h>
#include <string>
#include <vector>

#include "geos.h"

#if _WIN32
#define dlopen(x, y) LoadLibrary(x)
#define dlsym GetProcAddress
#define dlclose FreeLibrary
#define dlerror() ((char*)"failed to execute dlsym")
typedef HMODULE dlhandle;
#else
typedef void* dlhandle;
#endif  // #if _WIN32

#define CR_GEOS_NO_ERROR_DEFINED_MESSAGE "geos: returned invalid result but error not populated"

namespace {

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

typedef char (*CR_GEOS_IsEmpty_r)(CR_GEOS_Handle, CR_GEOS_Geometry);
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

typedef int (*CR_GEOS_Area_r)(CR_GEOS_Handle, CR_GEOS_Geometry, double*);
typedef int (*CR_GEOS_Length_r)(CR_GEOS_Handle, CR_GEOS_Geometry, double*);

typedef CR_GEOS_Geometry (*CR_GEOS_Centroid_r)(CR_GEOS_Handle, CR_GEOS_Geometry);
typedef CR_GEOS_Geometry (*CR_GEOS_Union_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef CR_GEOS_Geometry (*CR_GEOS_Intersection_r)(CR_GEOS_Handle, CR_GEOS_Geometry,
                                                   CR_GEOS_Geometry);
typedef CR_GEOS_Geometry (*CR_GEOS_PointOnSurface_r)(CR_GEOS_Handle, CR_GEOS_Geometry);

typedef CR_GEOS_Geometry (*CR_GEOS_Interpolate_r)(CR_GEOS_Handle, CR_GEOS_Geometry, double);

typedef int (*CR_GEOS_Distance_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry, double*);

typedef char (*CR_GEOS_Covers_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_CoveredBy_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Contains_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Crosses_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Equals_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Intersects_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Overlaps_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Touches_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Within_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);

typedef char* (*CR_GEOS_Relate_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_RelatePattern_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry,
                                        const char*);

typedef CR_GEOS_WKBWriter (*CR_GEOS_WKBWriter_create_r)(CR_GEOS_Handle);
typedef char* (*CR_GEOS_WKBWriter_write_r)(CR_GEOS_Handle, CR_GEOS_WKBWriter, CR_GEOS_Geometry,
                                           size_t*);
typedef void (*CR_GEOS_WKBWriter_setByteOrder_r)(CR_GEOS_Handle, CR_GEOS_WKBWriter, int);
typedef void (*CR_GEOS_WKBWriter_destroy_r)(CR_GEOS_Handle, CR_GEOS_WKBWriter);
typedef void (*CR_GEOS_WKBWriter_setIncludeSRID_r)(CR_GEOS_Handle, CR_GEOS_WKBWriter, const char);
typedef CR_GEOS_Geometry (*CR_GEOS_ClipByRect_r)(CR_GEOS_Handle, CR_GEOS_Geometry, double, double,
                                                 double, double);

std::string ToString(CR_GEOS_Slice slice) { return std::string(slice.data, slice.len); }

const char* dlopenFailError = "failed to execute dlopen";

}  // namespace

struct CR_GEOS {
  dlhandle geoscHandle;
  dlhandle geosHandle;

  CR_GEOS_init_r GEOS_init_r;
  CR_GEOS_finish_r GEOS_finish_r;
  CR_GEOS_Context_setErrorMessageHandler_r GEOSContext_setErrorMessageHandler_r;
  CR_GEOS_Free_r GEOSFree_r;

  CR_GEOS_IsEmpty_r GEOSisEmpty_r;
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

  CR_GEOS_Area_r GEOSArea_r;
  CR_GEOS_Length_r GEOSLength_r;

  CR_GEOS_Centroid_r GEOSGetCentroid_r;
  CR_GEOS_Union_r GEOSUnion_r;
  CR_GEOS_PointOnSurface_r GEOSPointOnSurface_r;
  CR_GEOS_Intersection_r GEOSIntersection_r;

  CR_GEOS_Interpolate_r GEOSInterpolate_r;

  CR_GEOS_Distance_r GEOSDistance_r;

  CR_GEOS_Covers_r GEOSCovers_r;
  CR_GEOS_CoveredBy_r GEOSCoveredBy_r;
  CR_GEOS_Contains_r GEOSContains_r;
  CR_GEOS_Crosses_r GEOSCrosses_r;
  CR_GEOS_Equals_r GEOSEquals_r;
  CR_GEOS_Intersects_r GEOSIntersects_r;
  CR_GEOS_Overlaps_r GEOSOverlaps_r;
  CR_GEOS_Touches_r GEOSTouches_r;
  CR_GEOS_Within_r GEOSWithin_r;

  CR_GEOS_Relate_r GEOSRelate_r;
  CR_GEOS_RelatePattern_r GEOSRelatePattern_r;

  CR_GEOS_WKBWriter_create_r GEOSWKBWriter_create_r;
  CR_GEOS_WKBWriter_destroy_r GEOSWKBWriter_destroy_r;
  CR_GEOS_WKBWriter_setByteOrder_r GEOSWKBWriter_setByteOrder_r;
  CR_GEOS_WKBWriter_setIncludeSRID_r GEOSWKBWriter_setIncludeSRID_r;
  CR_GEOS_WKBWriter_write_r GEOSWKBWriter_write_r;

  CR_GEOS_ClipByRect_r GEOSClipByRect_r;

  CR_GEOS(dlhandle geoscHandle, dlhandle geosHandle)
      : geoscHandle(geoscHandle), geosHandle(geosHandle) {}

  ~CR_GEOS() {
    if (geoscHandle != NULL) {
      dlclose(geoscHandle);
    }
    if (geosHandle != NULL) {
      dlclose(geosHandle);
    }
  }

  char* Init() {
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
    INIT(GEOSisEmpty_r);
    INIT(GEOSGeomTypeId_r);
    INIT(GEOSSetSRID_r);
    INIT(GEOSGetSRID_r);
    INIT(GEOSArea_r);
    INIT(GEOSLength_r);
    INIT(GEOSGetCentroid_r);
    INIT(GEOSUnion_r);
    INIT(GEOSPointOnSurface_r);
    INIT(GEOSIntersection_r);
    INIT(GEOSInterpolate_r);
    INIT(GEOSDistance_r);
    INIT(GEOSCovers_r);
    INIT(GEOSCoveredBy_r);
    INIT(GEOSContains_r);
    INIT(GEOSCrosses_r);
    INIT(GEOSEquals_r);
    INIT(GEOSIntersects_r);
    INIT(GEOSOverlaps_r);
    INIT(GEOSTouches_r);
    INIT(GEOSWithin_r);
    INIT(GEOSRelate_r);
    INIT(GEOSRelatePattern_r);
    INIT(GEOSWKTReader_create_r);
    INIT(GEOSWKTReader_destroy_r);
    INIT(GEOSWKTReader_read_r);
    INIT(GEOSWKBReader_create_r);
    INIT(GEOSWKBReader_destroy_r);
    INIT(GEOSWKBReader_read_r);
    INIT(GEOSWKBWriter_create_r);
    INIT(GEOSWKBWriter_destroy_r);
    INIT(GEOSWKBWriter_setByteOrder_r);
    INIT(GEOSWKBWriter_setIncludeSRID_r);
    INIT(GEOSWKBWriter_write_r);
    INIT(GEOSClipByRect_r);
    return nullptr;

#undef INIT
  }

  template <typename T> char* InitSym(T* ptr, const char* symbol) {
    *ptr = reinterpret_cast<T>(dlsym(geoscHandle, symbol));
    if (ptr == nullptr) {
      return dlerror();
    }
    return nullptr;
  }
};

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

CR_GEOS_Slice CR_GEOS_Init(CR_GEOS_Slice geoscLoc, CR_GEOS_Slice geosLoc, CR_GEOS** lib) {
  // Open the libgeos.$(EXT) first, so that libgeos_c.$(EXT) can read it.
  auto geosLocStr = ToString(geosLoc);
  dlhandle geosHandle = dlopen(geosLocStr.c_str(), RTLD_LAZY);
  if (!geosHandle) {
    return cStringToSlice((char*)dlopenFailError);
  }

  auto geoscLocStr = ToString(geoscLoc);
  dlhandle geoscHandle = dlopen(geoscLocStr.c_str(), RTLD_LAZY);
  if (!geoscHandle) {
    dlclose(geosHandle);
    return cStringToSlice((char*)dlopenFailError);
  }

  std::unique_ptr<CR_GEOS> ret(new CR_GEOS(geoscHandle, geosHandle));
  auto error = ret->Init();
  if (error != nullptr) {
    return cStringToSlice(error);
  }

  *lib = ret.release();
  return CR_GEOS_Slice{.data = NULL, .len = 0};
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

void CR_GEOS_PushLittleEndianUint32(std::vector<unsigned char>& buf, uint32_t value) {
  for (auto i = 0; i < sizeof(value); i++) {
    buf.push_back(static_cast<unsigned char>((value >> (8 * i)) & 0xff));
  }
}

void CR_GEOS_writeGeomToEWKB(CR_GEOS* lib, CR_GEOS_Handle handle, CR_GEOS_Geometry geom,
                             CR_GEOS_String* ewkb, int srid) {
  // Empty points error in GEOS EWKB encoding. As such, we have to encode ourselves.
  // This is still broken for GEOMETRYCOLLECTIONs/MULTIPOINT containing empty points,
  // but there's not much we can do there for now.
  if (lib->GEOSisEmpty_r(handle, geom) && lib->GEOSGeomTypeId_r(handle, geom) == 0) {
    std::vector<unsigned char> buf;
    buf.push_back('\x01');  // little endian

    uint32_t metadataInfo = 1;  // 1 means it is a point.
    if (srid != 0) {
      metadataInfo |= 0x20000000;  // add SRID bit.
    }
    CR_GEOS_PushLittleEndianUint32(buf, metadataInfo);
    if (srid != 0) {
      CR_GEOS_PushLittleEndianUint32(buf, uint32_t(srid));
    }
    // Push back two NaN float values in little endian order.
    for (auto i = 0; i < 2; i++) {
      buf.push_back('\x00');
      buf.push_back('\x00');
      buf.push_back('\x00');
      buf.push_back('\x00');
      buf.push_back('\x00');
      buf.push_back('\x00');
      buf.push_back('\xF8');
      buf.push_back('\x7F');
    }
    ewkb->data = static_cast<char*>(malloc(buf.size()));
    ewkb->len = buf.size();
    memcpy(ewkb->data, buf.data(), buf.size());
    return;
  }
  auto wkbWriter = lib->GEOSWKBWriter_create_r(handle);
  lib->GEOSWKBWriter_setByteOrder_r(handle, wkbWriter, 1);
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

CR_GEOS_Status CR_GEOS_ClipEWKBByRect(CR_GEOS* lib, CR_GEOS_Slice ewkb, double xmin, double ymin,
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

//
// Topology operators.
//

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

CR_GEOS_Status CR_GEOS_Equals(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSEquals_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_Intersects(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSIntersects_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_Overlaps(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char* ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSOverlaps_r, a, b, ret);
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
