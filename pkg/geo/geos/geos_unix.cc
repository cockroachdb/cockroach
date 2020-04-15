// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !windows

#include <cstdarg>
#include <cstring>
#include <dlfcn.h>
#include <memory>
#include <stdlib.h>
#include <string>

#include "geos_unix.h"

namespace {

// Data Types adapted from `capi/geos_c.h.in` in GEOS.
typedef void* CR_GEOS_Handle;
typedef void (*CR_GEOS_MessageHandler)(const char*, void*);
typedef void* CR_GEOS_WKTReader;
typedef void* CR_GEOS_WKBReader;
typedef void* CR_GEOS_WKBWriter;

// Function declarations from `capi/geos_c.h.in` in GEOS.
typedef CR_GEOS_Handle (*CR_GEOS_init_r)();
typedef void (*CR_GEOS_finish_r)(CR_GEOS_Handle);
typedef CR_GEOS_MessageHandler (*CR_GEOS_Context_setErrorMessageHandler_r)(CR_GEOS_Handle,
                                                                           CR_GEOS_MessageHandler,
                                                                           void*);

typedef void (*CR_GEOS_SetSRID_r)(CR_GEOS_Handle, CR_GEOS_Geometry, int);
typedef int (*CR_GEOS_GetSRID_r)(CR_GEOS_Handle, CR_GEOS_Geometry);
typedef void (*CR_GEOS_GeomDestroy_r)(CR_GEOS_Handle, CR_GEOS_Geometry);

typedef CR_GEOS_WKTReader (*CR_GEOS_WKTReader_create_r)(CR_GEOS_Handle);
typedef CR_GEOS_Geometry (*CR_GEOS_WKTReader_read_r)(CR_GEOS_Handle, CR_GEOS_WKTReader,
                                                     const char*);
typedef void (*CR_GEOS_WKTReader_destroy_r)(CR_GEOS_Handle, CR_GEOS_WKTReader);

typedef CR_GEOS_WKBReader (*CR_GEOS_WKBReader_create_r)(CR_GEOS_Handle);
typedef CR_GEOS_Geometry (*CR_GEOS_WKBReader_read_r)(CR_GEOS_Handle, CR_GEOS_WKBReader, const char*,
                                                     size_t);
typedef void (*CR_GEOS_WKBReader_destroy_r)(CR_GEOS_Handle, CR_GEOS_WKBReader);

typedef char (*CR_GEOS_Covers_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_CoveredBy_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Contains_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Crosses_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Equals_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Intersects_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Overlaps_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Touches_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);
typedef char (*CR_GEOS_Within_r)(CR_GEOS_Handle, CR_GEOS_Geometry, CR_GEOS_Geometry);

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
  void* dlHandle;

  CR_GEOS_init_r GEOS_init_r;
  CR_GEOS_finish_r GEOS_finish_r;
  CR_GEOS_Context_setErrorMessageHandler_r GEOSContext_setErrorMessageHandler_r;

  CR_GEOS_SetSRID_r GEOSSetSRID_r;
  CR_GEOS_GetSRID_r GEOSGetSRID_r;
  CR_GEOS_GeomDestroy_r GEOSGeom_destroy_r;

  CR_GEOS_WKTReader_create_r GEOSWKTReader_create_r;
  CR_GEOS_WKTReader_destroy_r GEOSWKTReader_destroy_r;
  CR_GEOS_WKTReader_read_r GEOSWKTReader_read_r;

  CR_GEOS_WKBReader_create_r GEOSWKBReader_create_r;
  CR_GEOS_WKBReader_destroy_r GEOSWKBReader_destroy_r;
  CR_GEOS_WKBReader_read_r GEOSWKBReader_read_r;

  CR_GEOS_Covers_r GEOSCovers_r;
  CR_GEOS_CoveredBy_r GEOSCoveredBy_r;
  CR_GEOS_Contains_r GEOSContains_r;
  CR_GEOS_Crosses_r GEOSCrosses_r;
  CR_GEOS_Equals_r GEOSEquals_r;
  CR_GEOS_Intersects_r GEOSIntersects_r;
  CR_GEOS_Overlaps_r GEOSOverlaps_r;
  CR_GEOS_Touches_r GEOSTouches_r;
  CR_GEOS_Within_r GEOSWithin_r;

  CR_GEOS_WKBWriter_create_r GEOSWKBWriter_create_r;
  CR_GEOS_WKBWriter_destroy_r GEOSWKBWriter_destroy_r;
  CR_GEOS_WKBWriter_setByteOrder_r GEOSWKBWriter_setByteOrder_r;
  CR_GEOS_WKBWriter_setIncludeSRID_r GEOSWKBWriter_setIncludeSRID_r;
  CR_GEOS_WKBWriter_write_r GEOSWKBWriter_write_r;

  CR_GEOS_ClipByRect_r GEOSClipByRect_r;

  CR_GEOS(void* h) : dlHandle(h) {}

  ~CR_GEOS() {
    if (dlHandle != NULL) {
      dlclose(dlHandle);
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
    INIT(GEOSContext_setErrorMessageHandler_r);
    INIT(GEOSGeom_destroy_r);
    INIT(GEOSSetSRID_r);
    INIT(GEOSGetSRID_r);
    INIT(GEOSCovers_r);
    INIT(GEOSCoveredBy_r);
    INIT(GEOSContains_r);
    INIT(GEOSCrosses_r);
    INIT(GEOSEquals_r);
    INIT(GEOSIntersects_r);
    INIT(GEOSOverlaps_r);
    INIT(GEOSTouches_r);
    INIT(GEOSWithin_r);
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
    *ptr = reinterpret_cast<T>(dlsym(dlHandle, symbol));
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

CR_GEOS_Slice CR_GEOS_Init(CR_GEOS_Slice loc, CR_GEOS** lib) {
  auto locStr = ToString(loc);
  void* dlHandle = dlopen(locStr.c_str(), RTLD_LAZY);
  if (!dlHandle) {
    return cStringToSlice((char*)dlopenFailError);
  }

  std::unique_ptr<CR_GEOS> ret(new CR_GEOS(dlHandle));
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

void CR_GEOS_writeGeomToEWKB(CR_GEOS* lib, CR_GEOS_Handle handle, CR_GEOS_Geometry geom,
                             CR_GEOS_String* ewkb, int srid) {
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

  auto wkbReader = lib->GEOSWKBReader_create_r(handle);
  auto geom = lib->GEOSWKBReader_read_r(handle, wkbReader, ewkb.data, ewkb.len);
  lib->GEOSWKBReader_destroy_r(handle, wkbReader);
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

//
// Binary predicates
//

template <typename T>
CR_GEOS_Status CR_GEOS_BinaryPredicate(CR_GEOS* lib, T fn, CR_GEOS_Slice a, CR_GEOS_Slice b, char *ret) {
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
        error.assign("geos: returned invalid result but error not populated");
      }
    } else {
      *ret = r;
    }
  }
  lib->GEOS_finish_r(handle);
  return toGEOSString(error.data(), error.length());
}

CR_GEOS_Status CR_GEOS_Covers(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char *ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSCovers_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_CoveredBy(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char *ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSCoveredBy_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_Contains(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char *ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSContains_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_Crosses(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char *ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSCrosses_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_Equals(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char *ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSEquals_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_Intersects(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char *ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSIntersects_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_Overlaps(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char *ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSOverlaps_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_Touches(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char *ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSTouches_r, a, b, ret);
}

CR_GEOS_Status CR_GEOS_Within(CR_GEOS* lib, CR_GEOS_Slice a, CR_GEOS_Slice b, char *ret) {
  return CR_GEOS_BinaryPredicate(lib, lib->GEOSWithin_r, a, b, ret);
}
