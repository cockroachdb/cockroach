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
	"path/filepath"
	"runtime"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/errors"
)

// #cgo CXXFLAGS: -std=c++14
// #cgo LDFLAGS: -ldl
//
// #include "geos_unix.h"
import "C"

// maxArrayLen is the maximum safe length for this architecture.
const maxArrayLen = 1<<31 - 1

// validOrError returns an error if the CR_GEOS  is not valid.
func validOrError(c *C.CR_GEOS) error {
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

// crGEOS contains the global instance of CR_GEOS, to be initialized
// during init time.
var crGEOS *C.CR_GEOS

func init() {
	// Add the CI path by trying to find all parenting paths and appending
	// `lib/lib_geos.<ext>`.
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	for {
		var nextPath string
		if runtime.GOOS == "darwin" {
			nextPath = filepath.Join(cwd, "lib", "libgeos_c.dylib")
		} else {
			nextPath = filepath.Join(cwd, "lib", "libgeos_c.so")
		}
		if _, err := os.Stat(nextPath); err == nil {
			defaultGEOSLocations = append(defaultGEOSLocations, nextPath)
		}
		nextCWD := filepath.Dir(cwd)
		if nextCWD == cwd {
			break
		}
		cwd = nextCWD
	}
	crGEOS = initCRGEOS(defaultGEOSLocations)
}

// initCRGEOS initializes the CR_GEOS by attempting to dlopen all
// the paths as parsed in by locs.
func initCRGEOS(locs []string) *C.CR_GEOS {
	for _, loc := range locs {
		var ret *C.CR_GEOS
		errStr := C.CR_GEOS_Init(goToCString(loc), &ret)
		if errStr == nil {
			return ret
		}
		// TODO(otan): thread the error message somewhere.
	}
	return nil
}

// goToCString returns a CR_GEOS_String from a given Go string.
func goToCString(str string) C.CR_GEOS_String {
	if len(str) == 0 {
		return C.CR_GEOS_String{data: nil, len: 0}
	}
	b := []byte(str)
	return C.CR_GEOS_String{
		data: (*C.char)(unsafe.Pointer(&b[0])),
		len:  C.size_t(len(b)),
	}
}

// goToCSlice returns a CR_GEOS_Slice from a given Go byte slice.
func goToCSlice(b []byte) C.CR_GEOS_Slice {
	if len(b) == 0 {
		return C.CR_GEOS_Slice{data: nil, len: 0}
	}
	return C.CR_GEOS_Slice{
		data: (*C.char)(unsafe.Pointer(&b[0])),
		len:  C.size_t(len(b)),
	}
}

// cSliceToUnsafeGoBytes converts a CR_GEOS_Slice to a Go byte slice that
// refer to the underlying C memory.
func cSliceToUnsafeGoBytes(s C.CR_GEOS_Slice) []byte {
	if s.data == nil {
		return nil
	}
	// Interpret the C pointer as a pointer to a Go array, then slice.
	return (*[maxArrayLen]byte)(unsafe.Pointer(s.data))[:s.len:s.len]
}

// cSliceToSafeGoBytes converts a CR_GEOS_Slice to a Go byte slice.
// Additionally, it frees the memory in the C slice.
func cSliceToSafeGoBytes(s C.CR_GEOS_Slice) []byte {
	unsafeBytes := cSliceToUnsafeGoBytes(s)
	b := make([]byte, len(unsafeBytes))
	copy(b, unsafeBytes)
	C.free(unsafe.Pointer(s.data))
	return b
}

// WKTToWKB parses a WKT into WKB using the GEOS library.
func WKTToWKB(wkt geopb.WKT) (geopb.WKB, error) {
	if err := validOrError(crGEOS); err != nil {
		return nil, err
	}
	cWKB := C.CR_GEOS_WKTToWKB(
		crGEOS,
		goToCString(string(wkt)),
	)
	if cWKB.data == nil {
		return nil, errors.Newf("error decoding WKT: %s", wkt)
	}
	return cSliceToSafeGoBytes(cWKB), nil
}

// ClipWKBByRect clips a WKB to the specified rectangle.
func ClipWKBByRect(
	wkb geopb.WKB, xMin float64, yMin float64, xMax float64, yMax float64,
) (geopb.WKB, error) {
	if err := validOrError(crGEOS); err != nil {
		return nil, err
	}
	cWKB := C.CR_GEOS_ClipWKBByRect(
		crGEOS, goToCSlice(wkb), C.double(xMin), C.double(yMin), C.double(xMax), C.double(yMax))
	if cWKB.data == nil {
		return nil, nil
	}
	return cSliceToSafeGoBytes(cWKB), nil
}
