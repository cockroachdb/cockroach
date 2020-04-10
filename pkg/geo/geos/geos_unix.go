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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
)

// #cgo CXXFLAGS: -std=c++14
// #cgo LDFLAGS: -ldl
//
// #include "geos_unix.h"
import "C"

// maxArrayLen is the maximum safe length for this architecture.
const maxArrayLen = 1<<31 - 1

// crGEOSOnce contains the global instance of CR_GEOS, to be initialized
// during at a maximum of once.
// If it has failed to open, the error will be populated in "err".
// This should only be touched by "fetchGEOSOrError".
type crGEOSOnce struct {
	crGEOS *C.CR_GEOS
	err    error
	once   sync.Once
}

var crGEOS crGEOSOnce

// FetchGEOSOrErrorDisplay is used to control the error message displayed by
// FetchGEOSOrError.
type FetchGEOSOrErrorDisplay int

const (
	// FetchGEOSOrErrorDisplayPrivate displays the full error message, including
	// path info. It is intended for log messages.
	FetchGEOSOrErrorDisplayPrivate FetchGEOSOrErrorDisplay = iota
	// FetchGEOSOrErrorDisplayPublic displays a redacted error message, excluding
	// path info. It is intended for errors to display for the client.
	FetchGEOSOrErrorDisplayPublic
)

// FetchGEOSOrError attempts to start GEOS if it has not been opened already
// and returns an error if the CR_GEOS  is not valid.
func FetchGEOSOrError(errDisplay FetchGEOSOrErrorDisplay) (*C.CR_GEOS, error) {
	crGEOS.once.Do(func() {
		crGEOS.crGEOS, crGEOS.err = initCRGEOS(defaultGEOSLocations)
	})
	if crGEOS.err != nil && errDisplay == FetchGEOSOrErrorDisplayPublic {
		return nil, fmt.Errorf("geos: this operation is not available")
	}
	return crGEOS.crGEOS, crGEOS.err
}

// defaultGEOSLocations contains a list of locations where GEOS is expected to exist.
// TODO(otan): make this configurable by flags.
// TODO(otan): put mac / linux locations
var defaultGEOSLocations []string

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
}

// initCRGEOS initializes the CR_GEOS by attempting to dlopen all
// the paths as parsed in by locs.
func initCRGEOS(locs []string) (*C.CR_GEOS, error) {
	for _, loc := range locs {
		var ret *C.CR_GEOS
		errStr := C.CR_GEOS_Init(goToCSlice([]byte(loc)), &ret)
		if errStr.data == nil {
			return ret, nil
		}
		return nil, fmt.Errorf(
			"geos: cannot load GEOS from %s: %s",
			loc,
			string(cSliceToUnsafeGoBytes(errStr)),
		)
	}
	return nil, fmt.Errorf("geos: could not find location to init GEOS")
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

// c{String,Slice}ToUnsafeGoBytes convert a CR_GEOS_{String,Slice} to a Go
// byte slice that refer to the underlying C memory.
func cStringToUnsafeGoBytes(s C.CR_GEOS_String) []byte {
	return cToUnsafeGoBytes(s.data, s.len)
}

func cSliceToUnsafeGoBytes(s C.CR_GEOS_Slice) []byte {
	return cToUnsafeGoBytes(s.data, s.len)
}

func cToUnsafeGoBytes(data *C.char, len C.size_t) []byte {
	if data == nil {
		return nil
	}
	// Interpret the C pointer as a pointer to a Go array, then slice.
	return (*[maxArrayLen]byte)(unsafe.Pointer(data))[:len:len]
}

// cStringToSafeGoBytes converts a CR_GEOS_String to a Go byte slice.
// Additionally, it frees the C memory.
func cStringToSafeGoBytes(s C.CR_GEOS_String) []byte {
	unsafeBytes := cStringToUnsafeGoBytes(s)
	b := make([]byte, len(unsafeBytes))
	copy(b, unsafeBytes)
	C.free(unsafe.Pointer(s.data))
	return b
}

// A Error wraps an error returned from a GEOS operation.
type Error struct {
	msg string
}

// Error implements the error interface.
func (err *Error) Error() string {
	return err.msg
}

func statusToError(s C.CR_GEOS_Status) error {
	if s.data == nil {
		return nil
	}
	return &Error{msg: string(cStringToSafeGoBytes(s))}
}

// WKTToWKB parses a WKT into WKB using the GEOS library.
func WKTToWKB(wkt geopb.WKT) (geopb.WKB, error) {
	crGEOS, err := FetchGEOSOrError(FetchGEOSOrErrorDisplayPrivate)
	if err != nil {
		return nil, err
	}
	var cWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_WKTToWKB(crGEOS, goToCSlice([]byte(wkt)), &cWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cWKB), nil
}

// ClipWKBByRect clips a WKB to the specified rectangle.
func ClipWKBByRect(
	wkb geopb.WKB, xMin float64, yMin float64, xMax float64, yMax float64,
) (geopb.WKB, error) {
	crGEOS, err := FetchGEOSOrError(FetchGEOSOrErrorDisplayPrivate)
	if err != nil {
		return nil, err
	}
	var cWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_ClipWKBByRect(crGEOS, goToCSlice(wkb), C.double(xMin),
		C.double(yMin), C.double(xMax), C.double(yMax), &cWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cWKB), nil
}
