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
// #include <proj_api.h>
import "C"
import "unsafe"

// ProjPJ is the projPJ wrapper around the PROJ library's projPJ object.
type ProjPJ struct {
	projPJ C.projPJ
}

// IsLatLng returns whether the underlying ProjPJ is a latlng system.
// TODO(otan): store this metadata in the projPJ struct.
func (p *ProjPJ) IsLatLng() bool {
	return C.pj_is_latlong(p.projPJ) != 0
}

// NewProjPJFromText initializes a ProjPJ from text.
// TODO(otan): thread through thread contexts and retrieve error messages.
// TODO(otan): use slice management mechanisms.
// TODO(otan): free after creation.
func NewProjPJFromText(proj4text string) (*ProjPJ, error) {
	str := C.CString(proj4text)
	defer C.free(unsafe.Pointer(str))
	projPJ := C.pj_init_plus(str)
	return &ProjPJ{projPJ: projPJ}, nil
}
