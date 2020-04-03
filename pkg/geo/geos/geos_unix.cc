// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <dlfcn.h>
#include <memory>
#include <stdlib.h>
#include <string>

#include "geos_unix.h"

namespace {

// Data Types adapted from `capi/geos_c.h.in` in GEOS.
typedef void* CR_GEOS_Handle;
typedef void* CR_GEOS_WKTReader;
typedef void* CR_GEOS_WKBWriter;

// Function declarations from `capi/geos_c.h.in` in GEOS.
typedef CR_GEOS_Handle (*CR_GEOS_init_r)();
typedef void (*CR_GEOS_finish_r)(CR_GEOS_Handle);

typedef void (*CR_GEOS_Geom_destroy_r)(CR_GEOS_Handle, CR_GEOS_Geometry);

typedef CR_GEOS_WKTReader (*CR_GEOS_WKTReader_create_r)(CR_GEOS_Handle);
typedef CR_GEOS_Geometry (*CR_GEOS_WKTReader_read_r)(CR_GEOS_Handle, CR_GEOS_WKTReader,
                                                     const char*);
typedef void (*CR_GEOS_WKTReader_destroy_r)(CR_GEOS_Handle, CR_GEOS_WKTReader);

typedef CR_GEOS_WKBWriter (*CR_GEOS_WKBWriter_create_r)(CR_GEOS_Handle);
typedef char* (*CR_GEOS_WKBWriter_write_r)(CR_GEOS_Handle, CR_GEOS_WKBWriter, CR_GEOS_Geometry,
                                           size_t*);
typedef void (*CR_GEOS_WKBWriter_setByteOrder_r)(CR_GEOS_Handle, CR_GEOS_WKBWriter, int);
typedef void (*CR_GEOS_WKBWriter_destroy_r)(CR_GEOS_Handle, CR_GEOS_WKBWriter);

std::string ToString(CR_GEOS_String goStr) { return std::string(goStr.data, goStr.len); }

const char* dlopenFailError = "failed to execute dlopen";

}  // namespace

struct CR_GEOS {
  void* dlHandle;

  CR_GEOS_init_r GEOS_init_r;
  CR_GEOS_finish_r GEOS_finish_r;

  CR_GEOS_Geom_destroy_r GEOSGeom_destroy_r;

  CR_GEOS_WKTReader_create_r GEOSWKTReader_create_r;
  CR_GEOS_WKTReader_destroy_r GEOSWKTReader_destroy_r;
  CR_GEOS_WKTReader_read_r GEOSWKTReader_read_r;

  CR_GEOS_WKBWriter_create_r GEOSWKBWriter_create_r;
  CR_GEOS_WKBWriter_destroy_r GEOSWKBWriter_destroy_r;
  CR_GEOS_WKBWriter_setByteOrder_r GEOSWKBWriter_setByteOrder_r;
  CR_GEOS_WKBWriter_write_r GEOSWKBWriter_write_r;

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
    INIT(GEOSGeom_destroy_r);
    INIT(GEOSWKTReader_create_r);
    INIT(GEOSWKTReader_destroy_r);
    INIT(GEOSWKTReader_read_r);
    INIT(GEOSWKBWriter_create_r);
    INIT(GEOSWKBWriter_destroy_r);
    INIT(GEOSWKBWriter_setByteOrder_r);
    INIT(GEOSWKBWriter_write_r);
    return nullptr;

#undef INIT
  }

  template <typename T> char* InitSym(T* ptr, const char* symbol) {
    *ptr = reinterpret_cast<T>(dlsym(dlHandle, symbol));
    return dlerror();
  }
};

char* CR_GEOS_Init(CR_GEOS_String loc, CR_GEOS** lib) {
  auto locStr = ToString(loc);
  void* dlHandle = dlopen(locStr.c_str(), RTLD_LAZY);
  if (!dlHandle) {
    return (char*)dlopenFailError;
  }

  std::unique_ptr<CR_GEOS> ret(new CR_GEOS(dlHandle));
  auto error = ret->Init();
  if (error != nullptr) {
    return error;
  }

  *lib = ret.release();
  return NULL;
}

CR_GEOS_String CR_GEOS_WKTToWKB(CR_GEOS* lib, CR_GEOS_String wktString) {
  CR_GEOS_String result = {.data = NULL, .len = 0};

  auto handle = lib->GEOS_init_r();
  auto wktReader = lib->GEOSWKTReader_create_r(handle);
  auto wktStr = ToString(wktString);
  auto geom = lib->GEOSWKTReader_read_r(handle, wktReader, wktStr.c_str());
  lib->GEOSWKTReader_destroy_r(handle, wktReader);

  if (geom != NULL) {
    auto wkbWriter = lib->GEOSWKBWriter_create_r(handle);
    lib->GEOSWKBWriter_setByteOrder_r(handle, wkbWriter, 1);
    result.data = lib->GEOSWKBWriter_write_r(handle, wkbWriter, geom, &result.len);
    lib->GEOSWKBWriter_destroy_r(handle, wkbWriter);
    lib->GEOSGeom_destroy_r(handle, geom);
  }

  lib->GEOS_finish_r(handle);
  return result;
}
