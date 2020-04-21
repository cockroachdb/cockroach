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
	"sync"
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

// geosOnce contains the global instance of CR_GEOS, to be initialized
// during at a maximum of once.
// If it has failed to open, the error will be populated in "err".
// This should only be touched by "fetchGEOSOrError".
var geosOnce struct {
	geos *C.CR_GEOS
	err  error
	once sync.Once
}

// EnsureInit attempts to start GEOS if it has not been opened already
// and returns an error if the CR_GEOS  is not valid.
func EnsureInit(errDisplay EnsureInitErrorDisplay) error {
	_, err := ensureInit(errDisplay)
	return err
}

// ensureInit behaves as described in EnsureInit, but also returns the GEOS
// C object which should be hidden from the public eye.
func ensureInit(errDisplay EnsureInitErrorDisplay) (*C.CR_GEOS, error) {
	geosOnce.once.Do(func() {
		geosOnce.geos, geosOnce.err = initGEOS(findGEOSLocations())
	})
	if geosOnce.err != nil && errDisplay == EnsureInitErrorDisplayPublic {
		return nil, errors.Newf("geos: this operation is not available")
	}
	return geosOnce.geos, geosOnce.err
}

// findGEOSLocations returns the default locations where GEOS is installed.
func findGEOSLocations() []string {
	var ext string
	switch runtime.GOOS {
	case "darwin":
		ext = "dylib"
	default:
		ext = "so"
	}
	if *GEOSLocation != "" {
		// Try both the location itself, as well as adding the libgeos_c extension
		// at the end in case our user assumes it's just a directory.
		return []string{
			*GEOSLocation,
			filepath.Join(*GEOSLocation, "libgeos_c."+ext),
		}
	}
	var locDirs []string
	switch runtime.GOOS {
	case "Darwin":
		locDirs = []string{
			"/usr/lib",
			"/lib",
			"/usr/local/lib",
		}
	default:
		locDirs = []string{
			"/lib/x86_64-linux-gnu",
			"/usr/lib/x86_64-linux-gnu",
			"/lib/i386-linux-gnu",
			"/usr/lib/i386-linux-gnu",
			"/usr/lib",
			"/lib",
			"/usr/local/lib",
		}
	}
	var locs []string
	locs = append(locs, findGEOSLocationsInParentingDirectories(ext)...)
	for _, loc := range locDirs {
		locs = append(locs, filepath.Join(loc, "libgeos_c."+ext))
	}
	return locs
}

// findGEOSLocationsInParentingDirectories attempts to find GEOS by looking at
// parenting folders and looking inside `lib/lib_geos_c.*`.
func findGEOSLocationsInParentingDirectories(ext string) []string {
	locs := []string{}

	// Add the CI path by trying to find all parenting paths and appending
	// `lib/lib_geos.<ext>`.
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	for {
		nextPath := filepath.Join(cwd, "lib", "libgeos_c."+ext)
		if _, err := os.Stat(nextPath); err == nil {
			locs = append(locs, nextPath)
		}
		nextCWD := filepath.Dir(cwd)
		if nextCWD == cwd {
			break
		}
		cwd = nextCWD
	}
	return locs
}

// initGEOS initializes the CR_GEOS by attempting to dlopen all
// the paths as parsed in by locs.
func initGEOS(locs []string) (*C.CR_GEOS, error) {
	var err error
	for _, loc := range locs {
		var ret *C.CR_GEOS
		errStr := C.CR_GEOS_Init(goToCSlice([]byte(loc)), &ret)
		if errStr.data == nil {
			return ret, nil
		}
		err = errors.CombineErrors(
			err,
			errors.Newf(
				"geos: cannot load GEOS from %s: %s",
				loc,
				string(cSliceToUnsafeGoBytes(errStr)),
			),
		)
	}
	if err != nil {
		return nil, errors.Wrap(err, "geos: could not init GEOS")
	}
	return nil, errors.Newf("geos: no valid locations to init GEOS")
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

// WKTToEWKB parses a WKT into WKB using the GEOS library.
func WKTToEWKB(wkt geopb.WKT, srid geopb.SRID) (geopb.EWKB, error) {
	g, err := ensureInit(EnsureInitErrorDisplayPrivate)
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_WKTToEWKB(g, goToCSlice([]byte(wkt)), C.int(srid), &cEWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

// ClipEWKBByRect clips a WKB to the specified rectangle.
func ClipEWKBByRect(
	wkb geopb.EWKB, xMin float64, yMin float64, xMax float64, yMax float64,
) (geopb.EWKB, error) {
	g, err := ensureInit(EnsureInitErrorDisplayPrivate)
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_ClipEWKBByRect(g, goToCSlice(wkb), C.double(xMin),
		C.double(yMin), C.double(xMax), C.double(yMax), &cEWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

// Covers returns whether the EWKB provided by A covers the EWKB provided by B.
func Covers(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	g, err := ensureInit(EnsureInitErrorDisplayPrivate)
	if err != nil {
		return false, err
	}
	var ret C.char
	if err := statusToError(C.CR_GEOS_Covers(g, goToCSlice(a), goToCSlice(b), &ret)); err != nil {
		return false, err
	}
	return ret == 1, nil
}

// CoveredBy returns whether the EWKB provided by A is covered by the EWKB provided by B.
func CoveredBy(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	g, err := ensureInit(EnsureInitErrorDisplayPrivate)
	if err != nil {
		return false, err
	}
	var ret C.char
	if err := statusToError(C.CR_GEOS_CoveredBy(g, goToCSlice(a), goToCSlice(b), &ret)); err != nil {
		return false, err
	}
	return ret == 1, nil
}

// Contains returns whether the EWKB provided by A contains the EWKB provided by B.
func Contains(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	g, err := ensureInit(EnsureInitErrorDisplayPrivate)
	if err != nil {
		return false, err
	}
	var ret C.char
	if err := statusToError(C.CR_GEOS_Contains(g, goToCSlice(a), goToCSlice(b), &ret)); err != nil {
		return false, err
	}
	return ret == 1, nil
}

// Crosses returns whether the EWKB provided by A crosses the EWKB provided by B.
func Crosses(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	g, err := ensureInit(EnsureInitErrorDisplayPrivate)
	if err != nil {
		return false, err
	}
	var ret C.char
	if err := statusToError(C.CR_GEOS_Crosses(g, goToCSlice(a), goToCSlice(b), &ret)); err != nil {
		return false, err
	}
	return ret == 1, nil
}

// Equals returns whether the EWKB provided by A equals the EWKB provided by B.
func Equals(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	g, err := ensureInit(EnsureInitErrorDisplayPrivate)
	if err != nil {
		return false, err
	}
	var ret C.char
	if err := statusToError(C.CR_GEOS_Equals(g, goToCSlice(a), goToCSlice(b), &ret)); err != nil {
		return false, err
	}
	return ret == 1, nil
}

// Intersects returns whether the EWKB provided by A intersects the EWKB provided by B.
func Intersects(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	g, err := ensureInit(EnsureInitErrorDisplayPrivate)
	if err != nil {
		return false, err
	}
	var ret C.char
	if err := statusToError(C.CR_GEOS_Intersects(g, goToCSlice(a), goToCSlice(b), &ret)); err != nil {
		return false, err
	}
	return ret == 1, nil
}

// Overlaps returns whether the EWKB provided by A overlaps the EWKB provided by B.
func Overlaps(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	g, err := ensureInit(EnsureInitErrorDisplayPrivate)
	if err != nil {
		return false, err
	}
	var ret C.char
	if err := statusToError(C.CR_GEOS_Overlaps(g, goToCSlice(a), goToCSlice(b), &ret)); err != nil {
		return false, err
	}
	return ret == 1, nil
}

// Touches returns whether the EWKB provided by A touches the EWKB provided by B.
func Touches(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	g, err := ensureInit(EnsureInitErrorDisplayPrivate)
	if err != nil {
		return false, err
	}
	var ret C.char
	if err := statusToError(C.CR_GEOS_Touches(g, goToCSlice(a), goToCSlice(b), &ret)); err != nil {
		return false, err
	}
	return ret == 1, nil
}

// Within returns whether the EWKB provided by A is within the EWKB provided by B.
func Within(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	g, err := ensureInit(EnsureInitErrorDisplayPrivate)
	if err != nil {
		return false, err
	}
	var ret C.char
	if err := statusToError(C.CR_GEOS_Within(g, goToCSlice(a), goToCSlice(b), &ret)); err != nil {
		return false, err
	}
	return ret == 1, nil
}
