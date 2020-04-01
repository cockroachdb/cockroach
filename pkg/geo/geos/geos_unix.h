// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package geos is a wrapper around the spatial data types in the geo package
// and the GEOS C library. The GEOS library is dynamically loaded at init time.
// Operations will error if the GEOS library was not found.

#include <stdlib.h>

// Data Types adapted from `capi/geos_c.h.in` in GEOS.
typedef void* GEOSHandle;
typedef void* GEOSGeometry;
typedef void* GEOSWKTReader;
typedef void* GEOSWKBWriter;

// Function declarations from `capi/geos_c.h.in` in GEOS.
typedef GEOSHandle (*GEOS_init_r)();
typedef void (*GEOS_finish_r)(GEOSHandle);

typedef void (*GEOSGeom_destroy_r)(GEOSHandle, GEOSGeometry);

typedef GEOSWKTReader (*GEOSWKTReader_create_r)(GEOSHandle);
typedef GEOSGeometry (*GEOSWKTReader_read_r)(GEOSHandle, GEOSWKTReader, char*);
typedef void (*GEOSWKTReader_destroy_r)(GEOSHandle, GEOSWKTReader);

typedef GEOSWKBWriter (*GEOSWKBWriter_create_r)(GEOSHandle);
typedef unsigned char* (*GEOSWKBWriter_write_r)(GEOSHandle, GEOSWKBWriter, GEOSGeometry, size_t*);
typedef void (*GEOSWKBWriter_destroy_r)(GEOSHandle, GEOSWKBWriter);

// GEOSLib contains all the functions loaded by GEOS.
typedef struct {
  void *dlHandle;

  GEOS_init_r GEOS_init_r;
  GEOS_finish_r GEOS_finish_r;

  GEOSGeom_destroy_r GEOSGeom_destroy_r;

  GEOSWKTReader_create_r GEOSWKTReader_create_r;
  GEOSWKTReader_destroy_r GEOSWKTReader_destroy_r;
  GEOSWKTReader_read_r GEOSWKTReader_read_r;

  GEOSWKBWriter_create_r GEOSWKBWriter_create_r;
  GEOSWKBWriter_destroy_r GEOSWKBWriter_destroy_r;
  GEOSWKBWriter_write_r GEOSWKBWriter_write_r;
} GEOSLib;

// bootstrap initializes the provided GEOSLib with GEOS using dlopen/dlsym.
// Returns a string containing an error if an error was found.
char* bootstrap(GEOSLib *geos_lib, char *lib_location);

// wkt_to_wkb converts a given WKT into it's WKB form.
unsigned char* wkt_to_wkb(GEOSLib *geos_lib, char *wkt, size_t *size);
