// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package a

var (
	fneg  float64 = -1
	uineg         = uint(fneg) // want `do not convert a floating point number to an unsigned integer type`

	fpos  float64 = 1
	uipos         = uint(fpos) // want `do not convert a floating point number to an unsigned integer type`

	ineg = int(fneg)
	ipos = int(fpos)
)
