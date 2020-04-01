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
	"runtime"
	"strings"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/errors"
)

// #cgo LDFLAGS: -ldl
// #include "geos_unix.h"
import "C"

// maxArrayLen is the maximum safe length for this architecture.
const maxArrayLen = 1<<31 - 1

// validOrError returns an error if the CockroachGEOSLib  is not valid.
func validOrError(c *C.CockroachGEOSLib) error {
	if c == nil {
		// TODO(otan): be more helpful in this error message.
		return errors.Newf("could not load GEOS library")
	}
	return nil
}

// defaultGEOSLocations contains a list of locations where GEOS is expected to exist.
// TODO(otan): make this configurable by flags.
// TODO(otan): put mac / linux locations
var defaultGEOSLocations []string

var globalCockroachGEOSLib *C.CockroachGEOSLib

func init() {
	// Add the CI path by trying to find all parenting paths.
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	split := strings.Split(cwd, "/")
	for i := 0; i < len(split); i++ {
		nextPath := "/" + path.Join(split[:i]...)
		if runtime.GOOS == "darwin" {
			nextPath = path.Join(nextPath, "lib", "libgeos_c.dylib")
		} else {
			nextPath = path.Join(nextPath, "lib", "libgeos_c.so")
		}
		if _, err := os.Stat(nextPath); err == nil {
			defaultGEOSLocations = append(defaultGEOSLocations, nextPath)
		}
	}
	globalCockroachGEOSLib = initCockroachGEOSLib(defaultGEOSLocations)
}

// initCockroachGEOSLib initializes the CockroachGEOSLib by attempting to dlopen all
// the paths as parsed in by locs.
func initCockroachGEOSLib(locs []string) *C.CockroachGEOSLib {
	for _, loc := range locs {
		lib := C.CockroachGEOSInitLib(goToCString(loc))
		if lib != nil {
			return lib
		}
	}
	return nil
}

// goToCString returns a CockroachGEOSSlice from a given Go string.
func goToCString(str string) C.CockroachGEOSString {
	if len(str) == 0 {
		return C.CockroachGEOSSlice{data: nil, len: 0}
	}
	b := []byte(str)
	return C.CockroachGEOSSlice{
		data: (*C.char)(unsafe.Pointer(&b[0])),
		len:  C.size_t(len(b)),
	}
}

// cSliceToGo converts a CockroachGEOSSlice to go bytes.
func cSliceToUnsafeGoBytes(s C.CockroachGEOSSlice) []byte {
	if s.data == nil {
		return nil
	}
	// Interpret the C pointer as a pointer to a Go array, then slice.
	return (*[maxArrayLen]byte)(unsafe.Pointer(s.data))[:s.len:s.len]
}

// WKTToWKB parses a WKT into WKB using the GEOS library.
func WKTToWKB(wkt geopb.WKT) (geopb.WKB, error) {
	if err := validOrError(globalCockroachGEOSLib); err != nil {
		return nil, err
	}
	cWKB := C.CockroachGEOSWKTToWKB(
		globalCockroachGEOSLib,
		goToCString(string(wkt)),
	)
	if cWKB.data == nil {
		return nil, errors.Newf("error decoding WKT: %s", wkt)
	}
	unsafeWKB := cSliceToUnsafeGoBytes(cWKB)
	wkb := make([]byte, len(unsafeWKB))
	copy(wkb, unsafeWKB)
	defer C.free(unsafe.Pointer(cWKB.data))
	return wkb, nil
}
