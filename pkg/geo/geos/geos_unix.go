// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !windows

package geos

import (
	"os"
	"path"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/errors"
)

// #include "geos_unix.h"
import "C"

// geosHolder is the struct that contains all dlsym loaded functions.
type geosHolder struct {
	geosLib C.GEOSLib
	valid   bool
}

// validOrError returns an error if the geosHolder is not valid.
func (gh *geosHolder) validOrError() error {
	if !gh.valid {
		return errors.Newf("could not find GEOS library")
	}
	return nil
}

// geos is the global instance of geosHolder, which is initialized at init time.
var holder = geosHolder{}

// defaultGEOSLocations contains a list of locations where GEOS is expected to exist.
// TODO(otan): make this configurable by flags.
var defaultGEOSLocations = []string{
	// TODO: put mac / linux locations
}

func init() {
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	for _, extraLoc := range []string{
		"../../../lib/libgeos_c.dylib",
		"../../../lib/libgeos_c.so",
	} {
		defaultGEOSLocations = append(defaultGEOSLocations, path.Join(cwd, extraLoc))
	}
	holder = bootstrap(defaultGEOSLocations)
}

// bootstrap initializes a geosHolder by attempting to dlopen all the
// paths as parsed in by locs.
func bootstrap(locs []string) geosHolder {
	var h geosHolder
	for _, loc := range locs {
		cLoc := C.CString(loc)
		errStr := C.bootstrap(&h.geosLib, cLoc)
		C.free(unsafe.Pointer(cLoc))
		if unsafe.Pointer(errStr) == C.NULL {
			h.valid = true
			break
		}
	}
	return h
}

// WKTToWKB parses a WKT into WKB using the GEOS library.
func WKTToWKB(wkt geo.WKT) (geo.WKB, error) {
	if err := holder.validOrError(); err != nil {
		return nil, err
	}
	var dataLen C.size_t
	cWKT := C.CString(string(wkt))
	defer C.free(unsafe.Pointer(cWKT))
	cWKB := C.wkt_to_wkb(&holder.geosLib, cWKT, &dataLen)
	if unsafe.Pointer(cWKB) == C.NULL {
		return nil, errors.Newf("error decoding WKT: %s", wkt)
	}
	defer C.free(unsafe.Pointer(cWKB))

	wkb := geo.WKB(C.GoBytes(unsafe.Pointer(cWKB), C.int(dataLen)))
	return wkb, nil
}
