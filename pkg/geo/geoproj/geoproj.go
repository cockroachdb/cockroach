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
// #cgo !windows LDFLAGS: -lproj
// #cgo linux LDFLAGS: -lrt -lm -lpthread
// #cgo windows LDFLAGS: -lproj_4_9 -lshlwapi -lrpcrt4
//
// #include "proj.h"
import "C"
import (
	"math"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/geo/geographiclib"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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

// GetProjMetadata returns metadata about the given projection.
// The return arguments are a bool representing whether it is a latlng, a spheroid
// object and an error if anything was erroneous was found.
func GetProjMetadata(b geoprojbase.Proj4Text) (bool, *geographiclib.Spheroid, error) {
	var majorAxis, eccentricitySquared C.double
	var isLatLng C.int
	if err := cStatusToUnsafeGoBytes(
		C.CR_PROJ_GetProjMetadata(
			(*C.char)(unsafe.Pointer(&b.Bytes()[0])),
			(*C.int)(unsafe.Pointer(&isLatLng)),
			(*C.double)(unsafe.Pointer(&majorAxis)),
			(*C.double)(unsafe.Pointer(&eccentricitySquared)),
		),
	); err != nil {
		return false, nil, pgerror.Newf(pgcode.InvalidParameterValue, "error from PROJ: %s", string(err))
	}
	// flattening = e^2 / 1 + sqrt(1-e^2).
	// See: https://en.wikipedia.org/wiki/Eccentricity_(mathematics), derived from
	// e = sqrt(f(2-f))
	flattening := float64(eccentricitySquared) / (1 + math.Sqrt(1-float64(eccentricitySquared)))
	return isLatLng != 0, geographiclib.NewSpheroid(float64(majorAxis), flattening), nil
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
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
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
		return pgerror.Newf(pgcode.InvalidParameterValue, "error from PROJ: %s", string(err))
	}
	return nil
}
