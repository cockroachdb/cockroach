// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geomfn

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// AffineMatrix defines an affine transformation matrix for a geom object.
// It is expected to be of the form:
//     a  b  c  x_off
//     d  e  f  y_off
//     g  h  i  z_off
//     0  0  0  1
// Which gets applies onto a coordinate of form:
//     (x y z 0)^T
// With the following transformation:
//   x' = a*x + b*y + c*z + x_off
//   y' = d*x + e*y + f*z + y_off
//   z' = g*x + h*y + i*z + z_off
type AffineMatrix [][]float64

// Affine applies a 3D affine transformation onto the given geometry.
// See: https://en.wikipedia.org/wiki/Affine_transformation.
func Affine(g geo.Geometry, m AffineMatrix) (geo.Geometry, error) {
	if g.Empty() {
		return g, nil
	}

	t, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}

	newT, err := affine(t, m)
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.MakeGeometryFromGeomT(newT)
}

func affine(t geom.T, m AffineMatrix) (geom.T, error) {
	return applyOnCoordsForGeomT(t, func(l geom.Layout, dst, src []float64) error {
		var z float64
		if l.ZIndex() != -1 {
			z = src[l.ZIndex()]
		}
		newX := m[0][0]*src[0] + m[0][1]*src[1] + m[0][2]*z + m[0][3]
		newY := m[1][0]*src[0] + m[1][1]*src[1] + m[1][2]*z + m[1][3]
		newZ := m[2][0]*src[0] + m[2][1]*src[1] + m[2][2]*z + m[2][3]

		dst[0] = newX
		dst[1] = newY
		if l.ZIndex() != -1 {
			dst[2] = newZ
		}
		if l.MIndex() != -1 {
			dst[l.MIndex()] = src[l.MIndex()]
		}
		return nil
	})
}

// Translate returns a modified Geometry whose coordinates are incremented
// or decremented by the deltas.
func Translate(g geo.Geometry, deltas []float64) (geo.Geometry, error) {
	if g.Empty() {
		return g, nil
	}

	t, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}

	newT, err := translate(t, deltas)
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.MakeGeometryFromGeomT(newT)
}

func translate(t geom.T, deltas []float64) (geom.T, error) {
	if t.Layout().Stride() != len(deltas) {
		err := geom.ErrStrideMismatch{
			Got:  len(deltas),
			Want: t.Layout().Stride(),
		}
		return nil, pgerror.WithCandidateCode(
			errors.Wrap(err, "translating coordinates"),
			pgcode.InvalidParameterValue,
		)
	}
	var zOff float64
	if t.Layout().ZIndex() != -1 {
		zOff = deltas[t.Layout().ZIndex()]
	}
	return affine(
		t,
		AffineMatrix([][]float64{
			{1, 0, 0, deltas[0]},
			{0, 1, 0, deltas[1]},
			{0, 0, 1, zOff},
			{0, 0, 0, 1},
		}),
	)
}

// Scale returns a modified Geometry whose coordinates are multiplied by the factors.
// REQUIRES: len(factors) >= 2.
func Scale(g geo.Geometry, factors []float64) (geo.Geometry, error) {
	var zFactor float64
	if len(factors) > 2 {
		zFactor = factors[2]
	}
	return Affine(
		g,
		AffineMatrix([][]float64{
			{factors[0], 0, 0, 0},
			{0, factors[1], 0, 0},
			{0, 0, zFactor, 0},
			{0, 0, 0, 1},
		}),
	)
}

// ScaleRelativeToOrigin returns a modified Geometry whose coordinates are
// multiplied by the factors relative to the origin.
func ScaleRelativeToOrigin(
	g geo.Geometry, factor geo.Geometry, origin geo.Geometry,
) (geo.Geometry, error) {
	if g.Empty() {
		return g, nil
	}

	t, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}

	factorG, err := factor.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}

	factorPointG, ok := factorG.(*geom.Point)
	if !ok {
		return geo.Geometry{}, pgerror.Newf(pgcode.InvalidParameterValue, "the scaling factor must be a Point")
	}

	originG, err := origin.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}

	originPointG, ok := originG.(*geom.Point)
	if !ok {
		return geo.Geometry{}, pgerror.Newf(pgcode.InvalidParameterValue, "the false origin must be a Point")
	}

	if factorG.Stride() != originG.Stride() {
		err := geom.ErrStrideMismatch{
			Got:  factorG.Stride(),
			Want: originG.Stride(),
		}
		return geo.Geometry{}, pgerror.WithCandidateCode(
			errors.Wrap(err, "number of dimensions for the scaling factor and origin must be equal"),
			pgcode.InvalidParameterValue,
		)
	}

	// This is inconsistent with PostGIS, which allows a POINT EMPTY, but whose
	// behavior seems to depend on previous queries in the session, and not
	// desirable to reproduce.
	if len(originPointG.FlatCoords()) < 2 {
		return geo.Geometry{}, pgerror.Newf(pgcode.InvalidParameterValue, "the origin must have at least 2 coordinates")
	}

	// Offset by the origin, scale, and translate it back to the origin.
	offsetDeltas := make([]float64, 0, 3)
	offsetDeltas = append(offsetDeltas, -originPointG.X(), -originPointG.Y())
	if originG.Layout().ZIndex() != -1 {
		offsetDeltas = append(offsetDeltas, -originPointG.Z())
	}
	retT, err := translate(t, offsetDeltas)
	if err != nil {
		return geo.Geometry{}, err
	}

	var xFactor, yFactor, zFactor float64
	zFactor = 1
	if len(factorPointG.FlatCoords()) < 2 {
		// POINT EMPTY results in factors of 0, similar to PostGIS.
		xFactor, yFactor = 0, 0
	} else {
		xFactor, yFactor = factorPointG.X(), factorPointG.Y()
		if factorPointG.Layout().ZIndex() != -1 {
			zFactor = factorPointG.Z()
		}
	}

	retT, err = affine(
		retT,
		AffineMatrix([][]float64{
			{xFactor, 0, 0, 0},
			{0, yFactor, 0, 0},
			{0, 0, zFactor, 0},
			{0, 0, 0, 1},
		}),
	)
	if err != nil {
		return geo.Geometry{}, err
	}

	for i := range offsetDeltas {
		offsetDeltas[i] = -offsetDeltas[i]
	}
	retT, err = translate(retT, offsetDeltas)
	if err != nil {
		return geo.Geometry{}, err
	}

	return geo.MakeGeometryFromGeomT(retT)
}

// Rotate returns a modified Geometry whose coordinates are rotated
// around the origin by a rotation angle.
func Rotate(g geo.Geometry, rotRadians float64) (geo.Geometry, error) {
	return Affine(
		g,
		AffineMatrix([][]float64{
			{math.Cos(rotRadians), -math.Sin(rotRadians), 0, 0},
			{math.Sin(rotRadians), math.Cos(rotRadians), 0, 0},
			{0, 0, 1, 0},
			{0, 0, 0, 1},
		}),
	)
}

// ErrPointOriginEmpty is an error to compare point origin is empty.
var ErrPointOriginEmpty = pgerror.Newf(pgcode.InvalidParameterValue, "origin is an empty point")

// RotateWithPointOrigin returns a modified Geometry whose coordinates are rotated
// around the pointOrigin by a rotRadians.
func RotateWithPointOrigin(
	g geo.Geometry, rotRadians float64, pointOrigin geo.Geometry,
) (geo.Geometry, error) {
	if pointOrigin.ShapeType() != geopb.ShapeType_Point {
		return g, pgerror.Newf(pgcode.InvalidParameterValue, "origin is not a POINT")
	}
	t, err := pointOrigin.AsGeomT()
	if err != nil {
		return g, err
	}
	if t.Empty() {
		return g, ErrPointOriginEmpty
	}

	return RotateWithXY(g, rotRadians, t.FlatCoords()[0], t.FlatCoords()[1])
}

// RotateWithXY returns a modified Geometry whose coordinates are rotated
// around the X and Y by a rotRadians.
func RotateWithXY(g geo.Geometry, rotRadians, x, y float64) (geo.Geometry, error) {
	cos, sin := math.Cos(rotRadians), math.Sin(rotRadians)
	return Affine(
		g,
		AffineMatrix{
			{cos, -sin, 0, x - x*cos + y*sin},
			{sin, cos, 0, y - x*sin - y*cos},
			{0, 0, 1, 0},
			{0, 0, 0, 1},
		},
	)
}

// RotateX returns a modified Geometry whose coordinates are rotated about
// the X axis by rotRadians
func RotateX(g geo.Geometry, rotRadians float64) (geo.Geometry, error) {
	cos, sin := math.Cos(rotRadians), math.Sin(rotRadians)
	return Affine(
		g,
		AffineMatrix{
			{1, 0, 0, 0},
			{0, cos, -sin, 0},
			{0, sin, cos, 0},
			{0, 0, 0, 1},
		},
	)
}

// RotateY returns a modified Geometry whose coordinates are rotated about
// the Y axis by rotRadians
func RotateY(g geo.Geometry, rotRadians float64) (geo.Geometry, error) {
	cos, sin := math.Cos(rotRadians), math.Sin(rotRadians)
	return Affine(
		g,
		AffineMatrix{
			{cos, 0, sin, 0},
			{0, 1, 0, 0},
			{-sin, 0, cos, 0},
			{0, 0, 0, 1},
		},
	)
}

// RotateZ returns a modified Geometry whose coordinates are rotated about
// the Z axis by rotRadians
func RotateZ(g geo.Geometry, rotRadians float64) (geo.Geometry, error) {
	cos, sin := math.Cos(rotRadians), math.Sin(rotRadians)
	return Affine(
		g,
		AffineMatrix{
			{cos, -sin, 0, 0},
			{sin, cos, 0, 0},
			{0, 0, 1, 0},
			{0, 0, 0, 1},
		},
	)
}

// TransScale returns a modified Geometry whose coordinates are
// translate by deltaX and deltaY and scale by xFactor and yFactor
func TransScale(g geo.Geometry, deltaX, deltaY, xFactor, yFactor float64) (geo.Geometry, error) {
	return Affine(g,
		AffineMatrix([][]float64{
			{xFactor, 0, 0, xFactor * deltaX},
			{0, yFactor, 0, yFactor * deltaY},
			{0, 0, 1, 0},
			{0, 0, 0, 1},
		}))
}
