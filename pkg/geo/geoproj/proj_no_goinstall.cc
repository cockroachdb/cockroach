// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build go_install

// All stub functions in this file will error out.

#include "proj.h"
#include <assert.h>

CR_PROJ_Status CR_PROJ_Transform(char* fromSpec, char* toSpec, long point_count, double* x,
                                 double* y, double* z) {
  assert(0);
}
