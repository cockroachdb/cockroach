// Copyright 2018 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s2

import (
	"math"

	"github.com/golang/geo/r2"
	"github.com/golang/geo/s1"
)

const (
	// MinTessellationTolerance is the minimum supported tolerance (which
	// corresponds to a distance less than 1 micrometer on the Earth's
	// surface, but is still much larger than the expected projection and
	// interpolation errors).
	MinTessellationTolerance s1.Angle = 1e-13
)

// EdgeTessellator converts an edge in a given projection (e.g., Mercator) into
// a chain of spherical geodesic edges such that the maximum distance between
// the original edge and the geodesic edge chain is at most the requested
// tolerance. Similarly, it can convert a spherical geodesic edge into a chain
// of edges in a given 2D projection such that the maximum distance between the
// geodesic edge and the chain of projected edges is at most the requested tolerance.
//
//   Method      | Input                  | Output
//   ------------|------------------------|-----------------------
//   Projected   | S2 geodesics           | Planar projected edges
//   Unprojected | Planar projected edges | S2 geodesics
type EdgeTessellator struct {
	projection   Projection
	tolerance    s1.ChordAngle
	wrapDistance r2.Point
}

// NewEdgeTessellator creates a new edge tessellator for the given projection and tolerance.
func NewEdgeTessellator(p Projection, tolerance s1.Angle) *EdgeTessellator {
	return &EdgeTessellator{
		projection:   p,
		tolerance:    s1.ChordAngleFromAngle(maxAngle(tolerance, MinTessellationTolerance)),
		wrapDistance: p.WrapDistance(),
	}
}

// AppendProjected converts the spherical geodesic edge AB to a chain of planar edges
// in the given projection and returns the corresponding vertices.
//
// If the given projection has one or more coordinate axes that wrap, then
// every vertex's coordinates will be as close as possible to the previous
// vertex's coordinates. Note that this may yield vertices whose
// coordinates are outside the usual range. For example, tessellating the
// edge (0:170, 0:-170) (in lat:lng notation) yields (0:170, 0:190).
func (e *EdgeTessellator) AppendProjected(a, b Point, vertices []r2.Point) []r2.Point {
	pa := e.projection.Project(a)
	if len(vertices) == 0 {
		vertices = []r2.Point{pa}
	} else {
		pa = e.wrapDestination(vertices[len(vertices)-1], pa)
	}

	pb := e.wrapDestination(pa, e.projection.Project(b))
	return e.appendProjected(pa, a, pb, b, vertices)
}

// appendProjected splits a geodesic edge AB as necessary and returns the
// projected vertices appended to the given vertices.
//
// The maximum recursion depth is (math.Pi / MinTessellationTolerance) < 45
func (e *EdgeTessellator) appendProjected(pa r2.Point, a Point, pb r2.Point, b Point, vertices []r2.Point) []r2.Point {
	// It's impossible to robustly test whether a projected edge is close enough
	// to a geodesic edge without knowing the details of the projection
	// function, but the following heuristic works well for a wide range of map
	// projections. The idea is simply to test whether the midpoint of the
	// projected edge is close enough to the midpoint of the geodesic edge.
	//
	// This measures the distance between the two edges by treating them as
	// parametric curves rather than geometric ones. The problem with
	// measuring, say, the minimum distance from the projected midpoint to the
	// geodesic edge is that this is a lower bound on the value we want, because
	// the maximum separation between the two curves is generally not attained
	// at the midpoint of the projected edge. The distance between the curve
	// midpoints is at least an upper bound on the distance from either midpoint
	// to opposite curve. It's not necessarily an upper bound on the maximum
	// distance between the two curves, but it is a powerful requirement because
	// it demands that the two curves stay parametrically close together. This
	// turns out to be much more robust with respect for projections with
	// singularities (e.g., the North and South poles in the rectangular and
	// Mercator projections) because the curve parameterization speed changes
	// rapidly near such singularities.
	mid := Point{a.Add(b.Vector).Normalize()}
	testMid := e.projection.Unproject(e.projection.Interpolate(0.5, pa, pb))

	if ChordAngleBetweenPoints(mid, testMid) < e.tolerance {
		return append(vertices, pb)
	}

	pmid := e.wrapDestination(pa, e.projection.Project(mid))
	vertices = e.appendProjected(pa, a, pmid, mid, vertices)
	return e.appendProjected(pmid, mid, pb, b, vertices)
}

// AppendUnprojected converts the planar edge AB in the given projection to a chain of
// spherical geodesic edges and returns the vertices.
//
// Note that to construct a Loop, you must eliminate the duplicate first and last
// vertex. Note also that if the given projection involves coordinate wrapping
// (e.g. across the 180 degree meridian) then the first and last vertices may not
// be exactly the same.
func (e *EdgeTessellator) AppendUnprojected(pa, pb r2.Point, vertices []Point) []Point {
	pb2 := e.wrapDestination(pa, pb)
	a := e.projection.Unproject(pa)
	b := e.projection.Unproject(pb)

	if len(vertices) == 0 {
		vertices = []Point{a}
	}

	// Note that coordinate wrapping can create a small amount of error. For
	// example in the edge chain "0:-175, 0:179, 0:-177", the first edge is
	// transformed into "0:-175, 0:-181" while the second is transformed into
	// "0:179, 0:183". The two coordinate pairs for the middle vertex
	// ("0:-181" and "0:179") may not yield exactly the same S2Point.
	return e.appendUnprojected(pa, a, pb2, b, vertices)
}

// appendUnprojected interpolates a projected edge and appends the corresponding
// points on the sphere.
func (e *EdgeTessellator) appendUnprojected(pa r2.Point, a Point, pb r2.Point, b Point, vertices []Point) []Point {
	pmid := e.projection.Interpolate(0.5, pa, pb)
	mid := e.projection.Unproject(pmid)
	testMid := Point{a.Add(b.Vector).Normalize()}

	if ChordAngleBetweenPoints(mid, testMid) < e.tolerance {
		return append(vertices, b)
	}

	vertices = e.appendUnprojected(pa, a, pmid, mid, vertices)
	return e.appendUnprojected(pmid, mid, pb, b, vertices)
}

// wrapDestination returns the coordinates of the edge destination wrapped if
// necessary to obtain the shortest edge.
func (e *EdgeTessellator) wrapDestination(pa, pb r2.Point) r2.Point {
	x := pb.X
	y := pb.Y
	// The code below ensures that pb is unmodified unless wrapping is required.
	if e.wrapDistance.X > 0 && math.Abs(x-pa.X) > 0.5*e.wrapDistance.X {
		x = pa.X + math.Remainder(x-pa.X, e.wrapDistance.X)
	}
	if e.wrapDistance.Y > 0 && math.Abs(y-pa.Y) > 0.5*e.wrapDistance.Y {
		y = pa.Y + math.Remainder(y-pa.Y, e.wrapDistance.Y)
	}
	return r2.Point{x, y}
}
