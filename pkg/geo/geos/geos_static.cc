// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build !libgeos_dynamic
// +build !libgeos_dynamic

#include "geos.h"
#include "geos_cc.hh"

CR_GEOS::~CR_GEOS() {}

char *CR_GEOS::Init() {
  return nullptr;
}

// NB: The library locations are ignored since we've statically linked.
CR_GEOS_Status CR_GEOS_Init(CR_GEOS_Slice geoscLoc, CR_GEOS_Slice geosLoc, CR_GEOS** lib) {
  std::string error;
  std::unique_ptr<CR_GEOS> ret(new CR_GEOS(nullptr, nullptr));
  auto initError = ret->Init();
  if (initError != nullptr) {
    errorHandler(initError, &error);
    return toGEOSString(error.data(), error.length());
  }
  *lib = ret.release();
  return toGEOSString(error.data(), error.length());
}
