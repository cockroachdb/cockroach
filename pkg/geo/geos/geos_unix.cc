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

// Data Types adapted from `capi/geos_c.h.in` in GEOS.
typedef void *CR_GEOS_Handle;
typedef void *CR_GEOS_WKTReader;
typedef void *CR_GEOS_WKBWriter;

// Function declarations from `capi/geos_c.h.in` in GEOS.
typedef CR_GEOS_Handle (*CR_GEOS_init_r)();
typedef void (*CR_GEOS_finish_r)(CR_GEOS_Handle);

typedef void (*CR_GEOS_Geom_destroy_r)(CR_GEOS_Handle, CR_GEOS_Geometry);

typedef CR_GEOS_WKTReader (*CR_GEOS_WKTReader_create_r)(CR_GEOS_Handle);
typedef CR_GEOS_Geometry (*CR_GEOS_WKTReader_read_r)(CR_GEOS_Handle,
                                                     CR_GEOS_WKTReader,
                                                     const char *);
typedef void (*CR_GEOS_WKTReader_destroy_r)(CR_GEOS_Handle, CR_GEOS_WKTReader);

typedef CR_GEOS_WKBWriter (*CR_GEOS_WKBWriter_create_r)(CR_GEOS_Handle);
typedef char *(*CR_GEOS_WKBWriter_write_r)(CR_GEOS_Handle, CR_GEOS_WKBWriter,
                                           CR_GEOS_Geometry, size_t *);
typedef void (*CR_GEOS_WKBWriter_setByteOrder_r)(CR_GEOS_Handle,
                                                 CR_GEOS_WKBWriter, int);
typedef void (*CR_GEOS_WKBWriter_destroy_r)(CR_GEOS_Handle, CR_GEOS_WKBWriter);

struct CR_GEOS {
  void *dlHandle;

  CR_GEOS_init_r CR_GEOS_init_r;
  CR_GEOS_finish_r CR_GEOS_finish_r;

  CR_GEOS_Geom_destroy_r CR_GEOS_Geom_destroy_r;

  CR_GEOS_WKTReader_create_r CR_GEOS_WKTReader_create_r;
  CR_GEOS_WKTReader_destroy_r CR_GEOS_WKTReader_destroy_r;
  CR_GEOS_WKTReader_read_r CR_GEOS_WKTReader_read_r;

  CR_GEOS_WKBWriter_create_r CR_GEOS_WKBWriter_create_r;
  CR_GEOS_WKBWriter_destroy_r CR_GEOS_WKBWriter_destroy_r;
  CR_GEOS_WKBWriter_setByteOrder_r CR_GEOS_WKBWriter_setByteOrder_r;
  CR_GEOS_WKBWriter_write_r CR_GEOS_WKBWriter_write_r;

  ~CR_GEOS() {
    if (dlHandle != NULL) {
      dlclose(dlHandle);
    }
  };
};

inline std::string CR_GEOS_StringToString(CR_GEOS_String goStr) {
  return std::string(goStr.data, goStr.len);
}

const char *dlopenFailError = "failed to execute dlopen";

char *CR_GEOS_Init(CR_GEOS_String loc, CR_GEOS **lib) {
  char *error;

  auto locStr = CR_GEOS_StringToString(loc);
  void *dlHandle = dlopen(locStr.c_str(), RTLD_LAZY);
  if (!dlHandle) {
    return (char *)dlopenFailError;
  }

  std::unique_ptr<CR_GEOS> ret(new CR_GEOS());
  ret->dlHandle = dlHandle;

  // TODO(otan): autogenerate all of this.
  ret->CR_GEOS_init_r = (CR_GEOS_init_r)dlsym(dlHandle, "GEOS_init_r");
  if ((error = dlerror()) != NULL) {
    return error;
  }
  ret->CR_GEOS_finish_r = (CR_GEOS_finish_r)dlsym(dlHandle, "GEOS_finish_r");
  if ((error = dlerror()) != NULL) {
    return error;
  }

  ret->CR_GEOS_Geom_destroy_r =
      (CR_GEOS_Geom_destroy_r)dlsym(dlHandle, "GEOSGeom_destroy_r");
  if ((error = dlerror()) != NULL) {
    return error;
  }

  ret->CR_GEOS_WKTReader_create_r =
      (CR_GEOS_WKTReader_create_r)dlsym(dlHandle, "GEOSWKTReader_create_r");
  if ((error = dlerror()) != NULL) {
    return error;
  }
  ret->CR_GEOS_WKTReader_destroy_r =
      (CR_GEOS_WKTReader_destroy_r)dlsym(dlHandle, "GEOSWKTReader_destroy_r");
  if ((error = dlerror()) != NULL) {
    return error;
  }
  ret->CR_GEOS_WKTReader_read_r =
      (CR_GEOS_WKTReader_read_r)dlsym(dlHandle, "GEOSWKTReader_read_r");
  if ((error = dlerror()) != NULL) {
    return error;
  }

  ret->CR_GEOS_WKBWriter_create_r =
      (CR_GEOS_WKBWriter_create_r)dlsym(dlHandle, "GEOSWKBWriter_create_r");
  if ((error = dlerror()) != NULL) {
    return error;
  }
  ret->CR_GEOS_WKBWriter_destroy_r =
      (CR_GEOS_WKBWriter_destroy_r)dlsym(dlHandle, "GEOSWKBWriter_destroy_r");
  if ((error = dlerror()) != NULL) {
    return error;
  }
  ret->CR_GEOS_WKBWriter_setByteOrder_r =
      (CR_GEOS_WKBWriter_setByteOrder_r)dlsym(dlHandle,
                                              "GEOSWKBWriter_setByteOrder_r");
  if ((error = dlerror()) != NULL) {
    return error;
  }
  ret->CR_GEOS_WKBWriter_write_r =
      (CR_GEOS_WKBWriter_write_r)dlsym(dlHandle, "GEOSWKBWriter_write_r");
  if ((error = dlerror()) != NULL) {
    return error;
  }

  *lib = ret.release();

  return NULL;
}

CR_GEOS_String CR_GEOS_WKTToWKB(CR_GEOS *lib, CR_GEOS_String wktString) {
  CR_GEOS_String result = {.data = NULL, .len = 0};

  CR_GEOS_Handle handle = lib->CR_GEOS_init_r();
  CR_GEOS_WKTReader wktReader = lib->CR_GEOS_WKTReader_create_r(handle);
  auto wktStr = CR_GEOS_StringToString(wktString);
  CR_GEOS_Geometry geom =
      lib->CR_GEOS_WKTReader_read_r(handle, wktReader, wktStr.c_str());
  lib->CR_GEOS_WKTReader_destroy_r(handle, wktReader);

  if (geom != NULL) {
    CR_GEOS_WKBWriter wkbWriter = lib->CR_GEOS_WKBWriter_create_r(handle);
    lib->CR_GEOS_WKBWriter_setByteOrder_r(handle, wkbWriter, 1);
    result.data =
        lib->CR_GEOS_WKBWriter_write_r(handle, wkbWriter, geom, &result.len);
    lib->CR_GEOS_WKBWriter_destroy_r(handle, wkbWriter);
    lib->CR_GEOS_Geom_destroy_r(handle, geom);
  }

  lib->CR_GEOS_finish_r(handle);
  return result;
}
