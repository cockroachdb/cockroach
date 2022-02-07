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
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

// #cgo CXXFLAGS: -std=c++14
// #cgo !windows LDFLAGS: -ldl -lm
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

// PreparedGeometry is an instance of a GEOS PreparedGeometry.
type PreparedGeometry *C.CR_GEOS_PreparedGeometry

// EnsureInit attempts to start GEOS if it has not been opened already
// and returns the location if found, and an error if the CR_GEOS is not valid.
func EnsureInit(
	errDisplay EnsureInitErrorDisplay, flagLibraryDirectoryValue string,
) (string, error) {
	crdbBinaryLoc := ""
	if len(os.Args) > 0 {
		crdbBinaryLoc = os.Args[0]
	}
	_, err := ensureInit(errDisplay, flagLibraryDirectoryValue, crdbBinaryLoc)
	return geosOnce.loc, err
}

// ensureInitInternal ensures initialization has been done, always displaying
// errors privately and not assuming a flag has been set if initialized
// for the first time.
func ensureInitInternal() (*C.CR_GEOS, error) {
	return ensureInit(EnsureInitErrorDisplayPrivate, "", "")
}

// ensureInits behaves as described in EnsureInit, but also returns the GEOS
// C object which should be hidden from the public eye.
func ensureInit(
	errDisplay EnsureInitErrorDisplay, flagLibraryDirectoryValue string, crdbBinaryLoc string,
) (*C.CR_GEOS, error) {
	geosOnce.once.Do(func() {
		geosOnce.geos, geosOnce.loc, geosOnce.err = initGEOS(
			findLibraryDirectories(flagLibraryDirectoryValue, crdbBinaryLoc),
		)
	})
	if geosOnce.err != nil && errDisplay == EnsureInitErrorDisplayPublic {
		return nil, pgerror.Newf(pgcode.System, "geos: this operation is not available")
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
func findLibraryDirectories(flagLibraryDirectoryValue string, crdbBinaryLoc string) []string {
	// Try path by trying to find all parenting paths and appending
	// `lib/libgeos_c.<ext>` to the current working directory, as well
	// as the directory in which the cockroach binary is initialized.
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	locs := []string{}
	if flagLibraryDirectoryValue != "" {
		locs = append(locs, flagLibraryDirectoryValue)
	}
	// Account for the libraries to be in a bazel runfile path.
	if bazel.BuiltWithBazel() {
		if p, err := bazel.Runfile(path.Join("c-deps", "libgeos", "lib")); err == nil {
			locs = append(locs, p)
		}
	}
	locs = append(
		append(
			locs,
			findLibraryDirectoriesInParentingDirectories(crdbBinaryLoc)...,
		),
		findLibraryDirectoriesInParentingDirectories(cwd)...,
	)
	return locs
}

// findLibraryDirectoriesInParentingDirectories attempts to find GEOS by looking at
// parenting folders and looking inside `lib/libgeos_c.*`.
// This is basically only useful for CI runs.
func findLibraryDirectoriesInParentingDirectories(dir string) []string {
	locs := []string{}

	for {
		checkDir := filepath.Join(dir, "lib")
		found := true
		for _, file := range []string{
			filepath.Join(checkDir, getLibraryExt(libgeoscFileName)),
			filepath.Join(checkDir, getLibraryExt(libgeosFileName)),
		} {
			if _, err := os.Stat(file); err != nil {
				found = false
				break
			}
		}
		if found {
			locs = append(locs, checkDir)
		}
		parentDir := filepath.Dir(dir)
		if parentDir == dir {
			break
		}
		dir = parentDir
	}
	return locs
}

// initGEOS initializes the CR_GEOS by attempting to dlopen all
// the paths as parsed in by locs.
func initGEOS(dirs []string) (*C.CR_GEOS, string, error) {
	var err error
	for _, dir := range dirs {
		var ret *C.CR_GEOS
		newErr := statusToError(
			C.CR_GEOS_Init(
				goToCSlice([]byte(filepath.Join(dir, getLibraryExt(libgeoscFileName)))),
				goToCSlice([]byte(filepath.Join(dir, getLibraryExt(libgeosFileName)))),
				&ret,
			),
		)
		if newErr == nil {
			return ret, dir, nil
		}
		err = errors.CombineErrors(
			err,
			errors.Wrapf(
				newErr,
				"geos: cannot load GEOS from dir %q",
				dir,
			),
		)
	}
	if err != nil {
		return nil, "", wrapGEOSInitError(errors.Wrap(err, "geos: error during GEOS init"))
	}
	return nil, "", wrapGEOSInitError(errors.Newf("geos: no locations to init GEOS"))
}

func wrapGEOSInitError(err error) error {
	page := "linux"
	switch runtime.GOOS {
	case "darwin":
		page = "mac"
	case "windows":
		page = "windows"
	}
	return errors.WithHintf(
		err,
		"Ensure you have the spatial libraries installed as per the instructions in %s",
		docs.URL("install-cockroachdb-"+page),
	)
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

// cStringToUnsafeGoBytes convert a CR_GEOS_String to a Go
// byte slice that refer to the underlying C memory.
func cStringToUnsafeGoBytes(s C.CR_GEOS_String) []byte {
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

// Boundary returns the boundary of an EWKB.
func Boundary(ewkb geopb.EWKB) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_Boundary(g, goToCSlice(ewkb), &cEWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

// Difference returns the difference between two EWKB.
func Difference(ewkb1 geopb.EWKB, ewkb2 geopb.EWKB) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var diffEWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_Difference(g, goToCSlice(ewkb1), goToCSlice(ewkb2), &diffEWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(diffEWKB), nil
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

// Normalize returns the geometry in its normalized form.
func Normalize(a geopb.EWKB) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_Normalize(g, goToCSlice(a), &cEWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

// LineMerge merges multilinestring constituents.
func LineMerge(a geopb.EWKB) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_LineMerge(g, goToCSlice(a), &cEWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

// IsSimple returns whether the EWKB is simple.
func IsSimple(ewkb geopb.EWKB) (bool, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return false, err
	}
	var ret C.char
	if err := statusToError(C.CR_GEOS_IsSimple(g, goToCSlice(ewkb), &ret)); err != nil {
		return false, err
	}
	return ret == 1, nil
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

// MinimumBoundingCircle returns minimum bounding circle of an EWKB
func MinimumBoundingCircle(ewkb geopb.EWKB) (geopb.EWKB, geopb.EWKB, float64, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, nil, 0, err
	}
	var centerEWKB C.CR_GEOS_String
	var polygonEWKB C.CR_GEOS_String
	var radius C.double

	if err := statusToError(C.CR_GEOS_MinimumBoundingCircle(g, goToCSlice(ewkb), &radius, &centerEWKB, &polygonEWKB)); err != nil {
		return nil, nil, 0, err
	}
	return cStringToSafeGoBytes(polygonEWKB), cStringToSafeGoBytes(centerEWKB), float64(radius), nil

}

// ConvexHull returns an EWKB which returns the convex hull of the given EWKB.
func ConvexHull(ewkb geopb.EWKB) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_ConvexHull(g, goToCSlice(ewkb), &cEWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

// Simplify returns an EWKB which returns the simplified EWKB.
func Simplify(ewkb geopb.EWKB, tolerance float64) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(
		C.CR_GEOS_Simplify(g, goToCSlice(ewkb), &cEWKB, C.double(tolerance)),
	); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

// TopologyPreserveSimplify returns an EWKB which returns the simplified EWKB
// with the topology preserved.
func TopologyPreserveSimplify(ewkb geopb.EWKB, tolerance float64) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(
		C.CR_GEOS_TopologyPreserveSimplify(g, goToCSlice(ewkb), &cEWKB, C.double(tolerance)),
	); err != nil {
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

// UnaryUnion Returns an EWKB which is a union of input geometry components.
func UnaryUnion(a geopb.EWKB) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var unionEWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_UnaryUnion(g, goToCSlice(a), &unionEWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(unionEWKB), nil
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

// SymDifference returns an EWKB which is the symmetric difference of shapes A and B.
func SymDifference(a geopb.EWKB, b geopb.EWKB) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_SymDifference(g, goToCSlice(a), goToCSlice(b), &cEWKB)); err != nil {
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

// MinimumClearance returns the minimum distance a vertex can move to result in an
// invalid geometry.
func MinimumClearance(ewkb geopb.EWKB) (float64, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return 0, err
	}
	var distance C.double
	if err := statusToError(C.CR_GEOS_MinimumClearance(g, goToCSlice(ewkb), &distance)); err != nil {
		return 0, err
	}
	return float64(distance), nil
}

// MinimumClearanceLine returns the line spanning the minimum clearance a vertex can
// move before producing an invalid geometry.
func MinimumClearanceLine(ewkb geopb.EWKB) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var clearanceEWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_MinimumClearanceLine(g, goToCSlice(ewkb), &clearanceEWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(clearanceEWKB), nil
}

// ClipByRect clips a EWKB to the specified rectangle.
func ClipByRect(
	ewkb geopb.EWKB, xMin float64, yMin float64, xMax float64, yMax float64,
) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_ClipByRect(g, goToCSlice(ewkb), C.double(xMin),
		C.double(yMin), C.double(xMax), C.double(yMax), &cEWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

//
// PreparedGeometry
//

// PrepareGeometry prepares a geometry in GEOS.
func PrepareGeometry(a geopb.EWKB) (PreparedGeometry, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var ret *C.CR_GEOS_PreparedGeometry
	if err := statusToError(C.CR_GEOS_Prepare(g, goToCSlice(a), &ret)); err != nil {
		return nil, err
	}
	return PreparedGeometry(ret), nil
}

// PreparedGeomDestroy destroys a prepared geometry.
func PreparedGeomDestroy(a PreparedGeometry) {
	g, err := ensureInitInternal()
	if err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "trying to destroy PreparedGeometry with no GEOS"))
	}
	ap := (*C.CR_GEOS_PreparedGeometry)(unsafe.Pointer(a))
	if err := statusToError(C.CR_GEOS_PreparedGeometryDestroy(g, ap)); err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "PreparedGeometryDestroy returned an error"))
	}
}

//
// Binary predicates.
//

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

// Disjoint returns whether the EWKB provided by A is disjoint from the EWKB provided by B.
func Disjoint(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return false, err
	}
	var ret C.char
	if err := statusToError(C.CR_GEOS_Disjoint(g, goToCSlice(a), goToCSlice(b), &ret)); err != nil {
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

// PreparedIntersects returns whether the EWKB provided by A intersects the EWKB provided by B.
func PreparedIntersects(a PreparedGeometry, b geopb.EWKB) (bool, error) {
	// Double check - since PreparedGeometry is actually a pointer to C type.
	if a == nil {
		return false, errors.New("provided PreparedGeometry is nil")
	}
	g, err := ensureInitInternal()
	if err != nil {
		return false, err
	}
	var ret C.char
	ap := (*C.CR_GEOS_PreparedGeometry)(unsafe.Pointer(a))
	if err := statusToError(C.CR_GEOS_PreparedIntersects(g, ap, goToCSlice(b), &ret)); err != nil {
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

// FrechetDistance returns the Frechet distance between the geometries.
func FrechetDistance(a, b geopb.EWKB) (float64, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return 0, err
	}
	var distance C.double
	if err := statusToError(
		C.CR_GEOS_FrechetDistance(g, goToCSlice(a), goToCSlice(b), &distance),
	); err != nil {
		return 0, err
	}
	return float64(distance), nil
}

// FrechetDistanceDensify returns the Frechet distance between the geometries.
func FrechetDistanceDensify(a, b geopb.EWKB, densifyFrac float64) (float64, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return 0, err
	}
	var distance C.double
	if err := statusToError(
		C.CR_GEOS_FrechetDistanceDensify(g, goToCSlice(a), goToCSlice(b), C.double(densifyFrac), &distance),
	); err != nil {
		return 0, err
	}
	return float64(distance), nil
}

// HausdorffDistance returns the Hausdorff distance between the geometries.
func HausdorffDistance(a, b geopb.EWKB) (float64, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return 0, err
	}
	var distance C.double
	if err := statusToError(
		C.CR_GEOS_HausdorffDistance(g, goToCSlice(a), goToCSlice(b), &distance),
	); err != nil {
		return 0, err
	}
	return float64(distance), nil
}

// HausdorffDistanceDensify returns the Hausdorff distance between the geometries.
func HausdorffDistanceDensify(a, b geopb.EWKB, densifyFrac float64) (float64, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return 0, err
	}
	var distance C.double
	if err := statusToError(
		C.CR_GEOS_HausdorffDistanceDensify(g, goToCSlice(a), goToCSlice(b), C.double(densifyFrac), &distance),
	); err != nil {
		return 0, err
	}
	return float64(distance), nil
}

// EqualsExact returns whether two geometry objects are equal with some epsilon
func EqualsExact(lhs, rhs geopb.EWKB, epsilon float64) (bool, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return false, err
	}
	var ret C.char
	if err := statusToError(
		C.CR_GEOS_EqualsExact(g, goToCSlice(lhs), goToCSlice(rhs), C.double(epsilon), &ret),
	); err != nil {
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

// RelateBoundaryNodeRule returns the DE-9IM relation between A and B given a boundary node rule.
func RelateBoundaryNodeRule(a geopb.EWKB, b geopb.EWKB, bnr int) (string, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return "", err
	}
	var ret C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_RelateBoundaryNodeRule(g, goToCSlice(a), goToCSlice(b), C.int(bnr), &ret)); err != nil {
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

//
// Validity checking.
//

// IsValid returns whether the given geometry is valid.
func IsValid(ewkb geopb.EWKB) (bool, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return false, err
	}
	var ret C.char
	if err := statusToError(
		C.CR_GEOS_IsValid(g, goToCSlice(ewkb), &ret),
	); err != nil {
		return false, err
	}
	return ret == 1, nil
}

// IsValidReason the reasoning for whether the Geometry is valid or invalid.
func IsValidReason(ewkb geopb.EWKB) (string, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return "", err
	}
	var ret C.CR_GEOS_String
	if err := statusToError(
		C.CR_GEOS_IsValidReason(g, goToCSlice(ewkb), &ret),
	); err != nil {
		return "", err
	}

	return string(cStringToSafeGoBytes(ret)), nil
}

// IsValidDetail returns information regarding whether a geometry is valid or invalid.
// It takes in a flag parameter which behaves the same as the GEOS module, where 1
// means that self-intersecting rings forming holes are considered valid.
// It returns a bool representing whether it is valid, a string giving a reason for
// invalidity and an EWKB representing the location things are invalid at.
func IsValidDetail(ewkb geopb.EWKB, flags int) (bool, string, geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return false, "", nil, err
	}
	var retIsValid C.char
	var retReason C.CR_GEOS_String
	var retLocationEWKB C.CR_GEOS_String
	if err := statusToError(
		C.CR_GEOS_IsValidDetail(
			g,
			goToCSlice(ewkb),
			C.int(flags),
			&retIsValid,
			&retReason,
			&retLocationEWKB,
		),
	); err != nil {
		return false, "", nil, err
	}
	return retIsValid == 1,
		string(cStringToSafeGoBytes(retReason)),
		cStringToSafeGoBytes(retLocationEWKB),
		nil
}

// MakeValid returns a valid form of the EWKB.
func MakeValid(ewkb geopb.EWKB) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(C.CR_GEOS_MakeValid(g, goToCSlice(ewkb), &cEWKB)); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

// SharedPaths Returns a EWKB containing paths shared by the two given EWKBs.
func SharedPaths(a geopb.EWKB, b geopb.EWKB) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(
		C.CR_GEOS_SharedPaths(g, goToCSlice(a), goToCSlice(b), &cEWKB),
	); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

// Node returns a EWKB containing a set of linestrings using the least possible number of nodes while preserving all of the input ones.
func Node(a geopb.EWKB) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	err = statusToError(C.CR_GEOS_Node(g, goToCSlice(a), &cEWKB))
	if err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

// VoronoiDiagram Computes the Voronoi Diagram from the vertices of the supplied EWKBs.
func VoronoiDiagram(a, env geopb.EWKB, tolerance float64, onlyEdges bool) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	flag := 0
	if onlyEdges {
		flag = 1
	}
	if err := statusToError(
		C.CR_GEOS_VoronoiDiagram(g, goToCSlice(a), goToCSlice(env), C.double(tolerance), C.int(flag), &cEWKB),
	); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

// MinimumRotatedRectangle Returns a minimum rotated rectangle enclosing a geometry
func MinimumRotatedRectangle(ewkb geopb.EWKB) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(
		C.CR_GEOS_MinimumRotatedRectangle(g, goToCSlice(ewkb), &cEWKB),
	); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}

// Snap returns the input EWKB with the vertices snapped to the target
// EWKB. Tolerance is used to control where snapping is performed.
// If no snapping occurs then the input geometry is returned unchanged.
func Snap(input, target geopb.EWKB, tolerance float64) (geopb.EWKB, error) {
	g, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cEWKB C.CR_GEOS_String
	if err := statusToError(
		C.CR_GEOS_Snap(g, goToCSlice(input), goToCSlice(target), C.double(tolerance), &cEWKB),
	); err != nil {
		return nil, err
	}
	return cStringToSafeGoBytes(cEWKB), nil
}
