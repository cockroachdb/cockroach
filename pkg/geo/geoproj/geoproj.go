// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package geoproj contains functions that interface with the PROJ library.
package geoproj

// #cgo CXXFLAGS: -std=c++14
// #cgo CPPFLAGS: -I../../../c-deps/proj/src
// #cgo LDFLAGS: -lproj
// #cgo linux LDFLAGS: -lrt -lm -lpthread
// #cgo windows LDFLAGS: -lshlwapi -lrpcrt4
//
// #include "proj.h"
import "C"
import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/cockroachdb/errors"
)

// maxArrayLen is the maximum safe length for this architecture.
const maxArrayLen = 1<<31 - 1

func cStatusToUnsafeGoBytes(s C.CR_PROJ_Status) []byte {
	if s.data == nil {
		return nil
	}
	// Interpret the C pointer as a pointer to a Go array, then slice.
	return (*[maxArrayLen]byte)(unsafe.Pointer(s.data))[:s.len:s.len]
}

// Project projects the given xCoords, yCoords and zCoords from one
// coordinate system to another using proj4text.
// Array elements are edited in place.
func Project(
	from geoprojbase.Proj4Text,
	to geoprojbase.Proj4Text,
	xCoords []float64,
	yCoords []float64,
	zCoords []float64,
) error {
	if len(xCoords) != len(yCoords) || len(xCoords) != len(zCoords) {
		return errors.Newf(
			"len(xCoords) != len(yCoords) != len(zCoords): %d != %d != %d",
			len(xCoords),
			len(yCoords),
			len(zCoords),
		)
	}
	if len(xCoords) == 0 {
		return nil
	}
	if err := cStatusToUnsafeGoBytes(C.CR_PROJ_Transform(
		(*C.char)(unsafe.Pointer(&from.Bytes()[0])),
		(*C.char)(unsafe.Pointer(&to.Bytes()[0])),
		C.long(len(xCoords)),
		(*C.double)(unsafe.Pointer(&xCoords[0])),
		(*C.double)(unsafe.Pointer(&yCoords[0])),
		(*C.double)(unsafe.Pointer(&zCoords[0])),
	)); err != nil {
		return errors.Newf("error from PROJ: %s", string(err))
	}
	return nil
}
