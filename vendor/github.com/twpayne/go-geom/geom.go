// Package geom implements efficient geometry types for geospatial
// applications.
package geom

//go:generate goderive .

import (
	"errors"
	"fmt"
	"math"
)

// A Layout describes the meaning of an N-dimensional coordinate. Layout(N) for
// N > 4 is a valid layout, in which case the first dimensions are interpreted
// to be X, Y, Z, and M and extra dimensions have no special meaning.  M values
// are considered part of a linear referencing system (e.g. classical time or
// distance along a path). 1-dimensional layouts are not supported.
type Layout int

const (
	// NoLayout is an unknown layout.
	NoLayout Layout = iota
	// XY is a 2D layout (X and Y).
	XY
	// XYZ is 3D layout (X, Y, and Z).
	XYZ
	// XYM is a 2D layout with an M value.
	XYM
	// XYZM is a 3D layout with an M value.
	XYZM
)

// An ErrLayoutMismatch is returned when geometries with different layouts
// cannot be combined.
type ErrLayoutMismatch struct {
	Got  Layout
	Want Layout
}

func (e ErrLayoutMismatch) Error() string {
	return fmt.Sprintf("geom: layout mismatch, got %s, want %s", e.Got, e.Want)
}

// An ErrStrideMismatch is returned when the stride does not match the expected
// stride.
type ErrStrideMismatch struct {
	Got  int
	Want int
}

func (e ErrStrideMismatch) Error() string {
	return fmt.Sprintf("geom: stride mismatch, got %d, want %d", e.Got, e.Want)
}

// An ErrUnsupportedLayout is returned when the requested layout is not
// supported.
type ErrUnsupportedLayout Layout

func (e ErrUnsupportedLayout) Error() string {
	return fmt.Sprintf("geom: unsupported layout %s", Layout(e))
}

// An ErrUnsupportedType is returned when the requested type is not supported.
type ErrUnsupportedType struct {
	Value interface{}
}

func (e ErrUnsupportedType) Error() string {
	return fmt.Sprintf("geom: unsupported type %T", e.Value)
}

// A Coord represents an N-dimensional coordinate.
type Coord []float64

// Clone returns a deep copy of c.
func (c Coord) Clone() Coord {
	return deriveCloneCoord(c)
}

// X returns the x coordinate of c. X is assumed to be the first ordinate.
func (c Coord) X() float64 {
	return c[0]
}

// Y returns the y coordinate of c. Y is assumed to be the second ordinate.
func (c Coord) Y() float64 {
	return c[1]
}

// Set copies the ordinate data from the other coord to this coord.
func (c Coord) Set(other Coord) {
	copy(c, other)
}

// Equal compares that all ordinates are the same in this and the other coords.
// It is assumed that this coord and other coord both have the same (provided)
// layout.
func (c Coord) Equal(layout Layout, other Coord) bool {
	numOrds := len(c)

	if layout.Stride() < numOrds {
		numOrds = layout.Stride()
	}

	if (len(c) < layout.Stride() || len(other) < layout.Stride()) && len(c) != len(other) {
		return false
	}

	for i := 0; i < numOrds; i++ {
		if math.IsNaN(c[i]) || math.IsNaN(other[i]) {
			if !math.IsNaN(c[i]) || !math.IsNaN(other[i]) {
				return false
			}
		} else if c[i] != other[i] {
			return false
		}
	}

	return true
}

// T is a generic interface implemented by all geometry types.
type T interface {
	Layout() Layout
	Stride() int
	Bounds() *Bounds
	FlatCoords() []float64
	Ends() []int
	Endss() [][]int
	SRID() int
	Empty() bool
}

// MIndex returns the index of the M dimension, or -1 if the l does not have an
// M dimension.
func (l Layout) MIndex() int {
	switch l {
	case NoLayout, XY, XYZ:
		return -1
	case XYM:
		return 2
	case XYZM:
		return 3
	default:
		return 3
	}
}

// Stride returns l's number of dimensions.
func (l Layout) Stride() int {
	switch l {
	case NoLayout:
		return 0
	case XY:
		return 2
	case XYZ:
		return 3
	case XYM:
		return 3
	case XYZM:
		return 4
	default:
		return int(l)
	}
}

// String returns a human-readable string representing l.
func (l Layout) String() string {
	switch l {
	case NoLayout:
		return "NoLayout"
	case XY:
		return "XY"
	case XYZ:
		return "XYZ"
	case XYM:
		return "XYM"
	case XYZM:
		return "XYZM"
	default:
		return fmt.Sprintf("Layout(%d)", int(l))
	}
}

// ZIndex returns the index of l's Z dimension, or -1 if l does not have a Z
// dimension.
func (l Layout) ZIndex() int {
	switch l {
	case NoLayout, XY, XYM:
		return -1
	default:
		return 2
	}
}

// SetSRID sets the SRID of an arbitrary geometry.
func SetSRID(g T, srid int) (T, error) {
	switch g := g.(type) {
	case *Point:
		return g.SetSRID(srid), nil
	case *LineString:
		return g.SetSRID(srid), nil
	case *LinearRing:
		return g.SetSRID(srid), nil
	case *Polygon:
		return g.SetSRID(srid), nil
	case *MultiPoint:
		return g.SetSRID(srid), nil
	case *MultiLineString:
		return g.SetSRID(srid), nil
	case *MultiPolygon:
		return g.SetSRID(srid), nil
	case *GeometryCollection:
		return g.SetSRID(srid), nil
	default:
		return g, &ErrUnsupportedType{
			Value: g,
		}
	}
}

// TransformInPlace replaces all coordinates in g using f.
func TransformInPlace(g T, f func(Coord)) T {
	var (
		flatCoords = g.FlatCoords()
		stride     = g.Stride()
	)
	for i, n := 0, len(flatCoords); i < n; i += stride {
		f(flatCoords[i : i+stride])
	}
	return g
}

// Must panics if err is not nil, otherwise it returns g.
func Must(g T, err error) T {
	if err != nil {
		panic(err)
	}
	return g
}

var (
	errIncorrectEnd         = errors.New("geom: incorrect end")
	errLengthStrideMismatch = errors.New("geom: length/stride mismatch")
	errMisalignedEnd        = errors.New("geom: misaligned end")
	errNonEmptyEnds         = errors.New("geom: non-empty ends")
	errNonEmptyEndss        = errors.New("geom: non-empty endss")
	errNonEmptyFlatCoords   = errors.New("geom: non-empty flatCoords")
	errOutOfOrderEnd        = errors.New("geom: out-of-order end")
	errStrideLayoutMismatch = errors.New("geom: stride/layout mismatch")
)
