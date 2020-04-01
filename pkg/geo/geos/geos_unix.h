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
typedef void* CockroachGEOSGeometry;

// CockroachGEOSSlice is a wrapper around a Go slice.
typedef struct {
  char* data;
  size_t len;
} CockroachGEOSSlice;

// CockroachGEOSString is a wrapper around a Go string.
typedef struct {
  char* data;
  size_t len;
} CockroachGEOSString;

// CockroachGEOSLib contains all the functions loaded by GEOS.
typedef struct CockroachGEOSLib CockroachGEOSLib;

// CockroachGEOSInitLib initializes the provided GEOSLib with GEOS using dlopen/dlsym.
// Returns a string containing an error if an error was found.
CockroachGEOSLib *CockroachGEOSInitLib(CockroachGEOSString loc);

// CockroachGEOSWKTToWKB converts a given WKT into it's WKB form.
CockroachGEOSSlice CockroachGEOSWKTToWKB(CockroachGEOSLib *lib, CockroachGEOSString wkt);
