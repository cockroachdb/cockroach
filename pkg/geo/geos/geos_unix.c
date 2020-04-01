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
#include <dlfcn.h>

#include "geos_unix.h"

char* bootstrap(GEOSLib *geos_lib, char *lib_location) {
  char *error;

  void *dlHandle = dlopen(lib_location, RTLD_LAZY);
  if (!dlHandle) {
    return "geos bindings not found";
  }
  geos_lib->dlHandle = dlHandle;

  // TODO(otan): autogenerate all of this.
  geos_lib->GEOS_init_r = (GEOS_init_r) dlsym(dlHandle, "GEOS_init_r");
  if ((error = dlerror()) != NULL) {
    return error;
  }
  geos_lib->GEOS_finish_r = (GEOS_finish_r) dlsym(dlHandle, "GEOS_finish_r");
  if ((error = dlerror()) != NULL) {
    return error;
  }

  geos_lib->GEOSGeom_destroy_r = (GEOSGeom_destroy_r) dlsym(dlHandle, "GEOSGeom_destroy_r");
  if ((error = dlerror()) != NULL) {
    return error;
  }

  geos_lib->GEOSWKTReader_create_r = (GEOSWKTReader_create_r) dlsym(dlHandle, "GEOSWKTReader_create_r");
  if ((error = dlerror()) != NULL) {
    return error;
  }
  geos_lib->GEOSWKTReader_destroy_r = (GEOSWKTReader_destroy_r) dlsym(dlHandle, "GEOSWKTReader_destroy_r");
  if ((error = dlerror()) != NULL) {
    return error;
  }
  geos_lib->GEOSWKTReader_read_r = (GEOSWKTReader_read_r) dlsym(dlHandle, "GEOSWKTReader_read_r");
  if ((error = dlerror()) != NULL) {
    return error;
  }

  geos_lib->GEOSWKBWriter_create_r = (GEOSWKBWriter_create_r) dlsym(dlHandle, "GEOSWKBWriter_create_r");
  if ((error = dlerror()) != NULL) {
    return error;
  }
  geos_lib->GEOSWKBWriter_destroy_r = (GEOSWKBWriter_destroy_r) dlsym(dlHandle, "GEOSWKBWriter_destroy_r");
  if ((error = dlerror()) != NULL) {
    return error;
  }
  geos_lib->GEOSWKBWriter_write_r = (GEOSWKBWriter_write_r) dlsym(dlHandle, "GEOSWKBWriter_write_r");
  if ((error = dlerror()) != NULL) {
    return error;
  }

  return NULL;
}

unsigned char* wkt_to_wkb(GEOSLib *geos_lib, char *wkt, size_t *size) {
  unsigned char *result = NULL;
  GEOSHandle handle = geos_lib->GEOS_init_r();
  GEOSWKTReader wktReader = geos_lib->GEOSWKTReader_create_r(handle);
  GEOSGeometry geom = geos_lib->GEOSWKTReader_read_r(handle, wktReader, wkt);
  geos_lib->GEOSWKTReader_destroy_r(handle, wktReader);

  if (geom != NULL) {
    GEOSWKBWriter wkbWriter = geos_lib->GEOSWKBWriter_create_r(handle);
    result = geos_lib->GEOSWKBWriter_write_r(handle, wkbWriter, geom, size);
    geos_lib->GEOSWKBWriter_destroy_r(handle, wkbWriter);
    geos_lib->GEOSGeom_destroy_r(handle, geom);
  }

  geos_lib->GEOS_finish_r(handle);
  return result;
}
