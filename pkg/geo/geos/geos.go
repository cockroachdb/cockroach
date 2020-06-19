// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package geos is a wrapper around the spatial data types between the geo
// package and the GEOS C library. The GEOS library is dynamically loaded
// at init time.
// Operations will error if the GEOS library was not found.
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
// #cgo !windows LDFLAGS: -ldl
//
// #include "geos.h"
import "C"

// EnsureInitErrorDisplay is used to control the error message displayed by
// EnsureInit.
type EnsureInitErrorDisplay int

const (
	// EnsureInitErrorDisplayPrivate displays the full error message, including
	// path info. It is intended for log messages.
	EnsureInitErrorDisplayPrivate EnsureInitErrorDisplay = iota
	// EnsureInitErrorDisplayPublic displays a redacted error message, excluding
	// path info. It is intended for errors to display for the client.
	EnsureInitErrorDisplayPublic
)

// maxArrayLen is the maximum safe length for this architecture.
const maxArrayLen = 1<<31 - 1

// geosOnce contains the global instance of CR_GEOS, to be initialized
// during at a maximum of once.
// If it has failed to open, the error will be populated in "err".
// This should only be touched by "fetchGEOSOrError".
var geosOnce struct {
	geos *C.CR_GEOS
	loc  string
	err  error
	once sync.Once
}

// EnsureInit attempts to start GEOS if it has not been opened already
// and returns the location if found, and an error if the CR_GEOS is not valid.
func EnsureInit(
	errDisplay EnsureInitErrorDisplay, flagLibraryDirectoryValue string,
) (string, error) {
	_, err := ensureInit(errDisplay, flagLibraryDirectoryValue)
	return geosOnce.loc, err
}

// ensureInitInternal ensures initialization has been done, always displaying
// errors privately and not assuming a flag has been set if initialized
// for the first time.
func ensureInitInternal() (*C.CR_GEOS, error) {
	return ensureInit(EnsureInitErrorDisplayPrivate, "")
}

// ensureInits behaves as described in EnsureInit, but also returns the GEOS
// C object which should be hidden from the public eye.
func ensureInit(
	errDisplay EnsureInitErrorDisplay, flagLibraryDirectoryValue string,
) (*C.CR_GEOS, error) {
	geosOnce.once.Do(func() {
		geosOnce.geos, geosOnce.loc, geosOnce.err = initGEOS(findLibraryDirectories(flagLibraryDirectoryValue))
	})
	if geosOnce.err != nil && errDisplay == EnsureInitErrorDisplayPublic {
		return nil, errors.Newf("geos: this operation is not available")
	}
	return geosOnce.geos, geosOnce.err
}

// appendLibraryExt appends the extension expected for the running OS.
func getLibraryExt(base string) string {
	switch runtime.GOOS {
	case "darwin":
		return base + ".dylib"
	case "windows":
		return base + ".dll"
	default:
		return base + ".so"
	}
}

const (
	libgeosFileName  = "libgeos"
	libgeoscFileName = "libgeos_c"
)

// findLibraryDirectories returns the default locations where GEOS is installed.
func findLibraryDirectories(flagLibraryDirectoryValue string) []string {
	// For CI, they are always in a parenting directory where libgeos_c is set.
	// For now, this will need to look at every given location
	// TODO(otan): fix CI to always use a fixed location OR initialize GEOS
	// correctly for each test suite that may need GEOS.
	locs := append(findLibraryDirectoriesInParentingDirectories(), flagLibraryDirectoryValue)
	return locs
}

// findLibraryDirectoriesInParentingDirectories attempts to find GEOS by looking at
// parenting folders and looking inside `lib/libgeos_c.*`.
// This is basically only useful for CI runs.
func findLibraryDirectoriesInParentingDirectories() []string {
	locs := []string{}

	// Add the CI path by trying to find all parenting paths and appending
	// `lib/libgeos_c.<ext>`.
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	for {
		dir := filepath.Join(cwd, "lib")
		found := true
		for _, file := range []string{
			filepath.Join(dir, getLibraryExt(libgeoscFileName)),
			filepath.Join(dir, getLibraryExt(libgeosFileName)),
		} {
			if _, err := os.Stat(file); err != nil {
				found = false
				break
			}
		}
		if found {
			locs = append(locs, dir)
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
func initGEOS(dirs []string) (*C.CR_GEOS, string, error) {
	var err error
	for _, dir := range dirs {
		var ret *C.CR_GEOS
		errStr := C.CR_GEOS_Init(
			goToCSlice([]byte(filepath.Join(dir, getLibraryExt(libgeoscFileName)))),
			goToCSlice([]byte(filepath.Join(dir, getLibraryExt(libgeosFileName)))),
			&ret,
		)
		if errStr.data == nil {
			return ret, dir, nil
		}
		err = errors.CombineErrors(
			err,
			errors.Newf(
				"geos: cannot load GEOS from dir %q: %s",
				dir,
				string(cSliceToUnsafeGoBytes(errStr)),
			),
		)
	}
	if err != nil {
		return nil, "", errors.Wrap(err, "geos: error during GEOS init")
	}
	return nil, "", errors.Newf("geos: no locations to init GEOS")
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
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_WKTToEWKB(g, goToCSlice([]byte(wkt)), C.int(srid), &cEWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

// BufferParamsJoinStyle maps to the GEOSBufJoinStyles enum in geos_c.h.in.
type BufferParamsJoinStyle int

// These should be kept in sync with the geos_c.h.in corresponding enum definition.
const (
	BufferParamsJoinStyleRound = 1
	BufferParamsJoinStyleMitre = 2
	BufferParamsJoinStyleBevel = 3
)

// BufferParamsEndCapStyle maps to the GEOSBufCapStyles enum in geos_c.h.in.
type BufferParamsEndCapStyle int

// These should be kept in sync with the geos_c.h.in corresponding enum definition.
const (
	BufferParamsEndCapStyleRound  = 1
	BufferParamsEndCapStyleFlat   = 2
	BufferParamsEndCapStyleSquare = 3
)

// BufferParams are parameters to provide into the GEOS buffer function.
type BufferParams struct {
	JoinStyle        BufferParamsJoinStyle
	EndCapStyle      BufferParamsEndCapStyle
	SingleSided      bool
	QuadrantSegments int
	MitreLimit       float64
}

// Buffer buffers the given geometry by the given distance and params.
func Buffer(ewkb geopb.EWKB, params BufferParams, distance float64) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	singleSided := 0
	if params.SingleSided {
		singleSided = 1
	}
	cParams := C.CR_GEOS_BufferParamsInput{
		endCapStyle:      C.int(params.EndCapStyle),
		joinStyle:        C.int(params.JoinStyle),
		singleSided:      C.int(singleSided),
		quadrantSegments: C.int(params.QuadrantSegments),
		mitreLimit:       C.double(params.MitreLimit),
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_Buffer(g, goToCSlice(ewkb), cParams, C.double(distance), &cEWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

// Area returns the area of an EWKB.
func Area(ewkb geopb.EWKB) (float64, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return 0, err
	}
	var area C.double
	if err := statusToError(C.CR_GEOS_Area(g, goToCSlice(ewkb), &area)); err != nil {
		return 0, err
	}
	return float64(area), nil
}

// Length returns the length of an EWKB.
func Length(ewkb geopb.EWKB) (float64, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return 0, err
	}
	var length C.double
	if err := statusToError(C.CR_GEOS_Length(g, goToCSlice(ewkb), &length)); err != nil {
		return 0, err
	}
	return float64(length), nil
}

// Centroid returns the centroid of an EWKB.
func Centroid(ewkb geopb.EWKB) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_Centroid(g, goToCSlice(ewkb), &cEWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

// PointOnSurface returns an EWKB with a point that is on the surface of the given EWKB.
func PointOnSurface(ewkb geopb.EWKB) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_PointOnSurface(g, goToCSlice(ewkb), &cEWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

// Intersection returns an EWKB which contains the geometries of intersection between A and B.
func Intersection(a geopb.EWKB, b geopb.EWKB) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_Intersection(g, goToCSlice(a), goToCSlice(b), &cEWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

// Union returns an EWKB which is a union of shapes A and B.
func Union(a geopb.EWKB, b geopb.EWKB) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_Union(g, goToCSlice(a), goToCSlice(b), &cEWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

// InterpolateLine returns the point along the given LineString which is at
// a given distance from starting point.
// Note: For distance less than 0 it returns start point similarly for distance
// greater LineString's length.
// InterpolateLine also works with (Multi)LineString. However, the result is
// not appropriate as it combines all the LineString present in (MULTI)LineString,
// considering all the corner points of LineString overlaps each other.
func InterpolateLine(ewkb geopb.EWKB, distance float64) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_Interpolate(g, goToCSlice(ewkb), C.double(distance), &cEWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

// MinDistance returns the minimum distance between two EWKBs.
func MinDistance(a geopb.EWKB, b geopb.EWKB) (float64, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return 0, err
	}
	var distance C.double
	if err := statusToError(C.CR_GEOS_Distance(g, goToCSlice(a), goToCSlice(b), &distance)); err != nil {
		return 0, err
	}
	return float64(distance), nil
}

// ClipEWKBByRect clips a EWKB to the specified rectangle.
func ClipEWKBByRect(
	ewkb geopb.EWKB, xMin float64, yMin float64, xMax float64, yMax float64,
) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_ClipEWKBByRect(g, goToCSlice(ewkb), C.double(xMin),
		C.double(yMin), C.double(xMax), C.double(yMax), &cEWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

// Covers returns whether the EWKB provided by A covers the EWKB provided by B.
func Covers(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	g, err := ensureInitInternal()
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
	g, err := ensureInitInternal()
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
	g, err := ensureInitInternal()
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
	g, err := ensureInitInternal()
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
	g, err := ensureInitInternal()
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
	g, err := ensureInitInternal()
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
	g, err := ensureInitInternal()
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
	g, err := ensureInitInternal()
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
	g, err := ensureInitInternal()
	if err != nil {
		return false, err
	}
	var ret C.char
	if err := statusToError(C.CR_GEOS_Within(g, goToCSlice(a), goToCSlice(b), &ret)); err != nil {
		return false, err
	}
	return ret == 1, nil
}

//
// DE-9IM related
//

// Relate returns the DE-9IM relation between A and B.
func Relate(a geopb.EWKB, b geopb.EWKB) (string, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return "", err
	}
	var ret C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_Relate(g, goToCSlice(a), goToCSlice(b), &ret)); err != nil {
		return "", err
	}
	if ret.data == nil {
		return "", errors.Newf("expected DE-9IM string but found nothing")
	}
	return string(cStringToSafeGoBytes(ret)), nil
}

// RelatePattern whether A and B have a DE-9IM relation matching the given pattern.
func RelatePattern(a geopb.EWKB, b geopb.EWKB, pattern string) (bool, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return false, err
	}
	var ret C.char
	if err := statusToError(
		C.CR_GEOS_RelatePattern(g, goToCSlice(a), goToCSlice(b), goToCSlice([]byte(pattern)), &ret),
	); err != nil {
		return false, err
	}
	return ret == 1, nil
}
