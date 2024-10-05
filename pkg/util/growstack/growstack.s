// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

#include "funcdata.h"

TEXT ·Grow(SB),0,$32768-0
        NO_LOCAL_POINTERS
        RET
