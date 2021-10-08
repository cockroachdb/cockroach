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
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// Angle calculates the clockwise angle between two vectors given by points
// p1,p2 and p3,p4. If p4 is an empty geometry (of any type, to follow PostGIS
// behavior), it instead calculates the clockwise angle between p2,p1 and p2,p3.
func Angle(g1, g2, g3, g4 geo.Geometry) (*float64, error) {
	if g4.Empty() {
		g1, g2, g3, g4 = g2, g1, g2, g3
	}

	if g1.SRID() != g2.SRID() {
		return nil, geo.NewMismatchingSRIDsError(g1.SpatialObject(), g2.SpatialObject())
	}
	if g1.SRID() != g3.SRID() {
		return nil, geo.NewMismatchingSRIDsError(g1.SpatialObject(), g3.SpatialObject())
	}
	if g1.SRID() != g4.SRID() {
		return nil, geo.NewMismatchingSRIDsError(g1.SpatialObject(), g4.SpatialObject())
	}

	t1, err := g1.AsGeomT()
	if err != nil {
		return nil, err
	}
	t2, err := g2.AsGeomT()
	if err != nil {
		return nil, err
	}
	t3, err := g3.AsGeomT()
	if err != nil {
		return nil, err
	}
	t4, err := g4.AsGeomT()
	if err != nil {
		return nil, err
	}

	p1, p1ok := t1.(*geom.Point)
	p2, p2ok := t2.(*geom.Point)
	p3, p3ok := t3.(*geom.Point)
	p4, p4ok := t4.(*geom.Point)

	if !p1ok || !p2ok || !p3ok || !p4ok {
		return nil, errors.New("arguments must be POINT geometries")
	}
	if p1.Empty() || p2.Empty() || p3.Empty() || p4.Empty() {
		return nil, errors.New("received EMPTY geometry")
	}

	return angleFromCoords(p1.Coords(), p2.Coords(), p3.Coords(), p4.Coords()), nil
}

// AngleLineString calculates the clockwise angle between two linestrings,
// treating them as vectors between the start- and endpoints. Type conflicts
// or empty geometries return nil (as opposed to Angle which errors), to
// follow PostGIS behavior.
func AngleLineString(g1, g2 geo.Geometry) (*float64, error) {
	if g1.SRID() != g2.SRID() {
		return nil, geo.NewMismatchingSRIDsError(g1.SpatialObject(), g2.SpatialObject())
	}
	t1, err := g1.AsGeomT()
	if err != nil {
		return nil, err
	}
	t2, err := g2.AsGeomT()
	if err != nil {
		return nil, err
	}
	l1, l1ok := t1.(*geom.LineString)
	l2, l2ok := t2.(*geom.LineString)
	if !l1ok || !l2ok || l1.Empty() || l2.Empty() {
		return nil, nil // follow PostGIS behavior
	}
	return angleFromCoords(
		l1.Coord(0), l1.Coord(l1.NumCoords()-1), l2.Coord(0), l2.Coord(l2.NumCoords()-1)), nil
}

// angleFromCoords returns the clockwise angle between the vectors c1,c2 and
// c3,c4. For compatibility with PostGIS, it returns nil if any vectors have
// length 0.
func angleFromCoords(c1, c2, c3, c4 geom.Coord) *float64 {
	a := coordSub(c2, c1)
	b := coordSub(c4, c3)
	if (a.X() == 0 && a.Y() == 0) || (b.X() == 0 && b.Y() == 0) {
		return nil
	}
	// We want the clockwise angle, not the smallest interior angle, so can't use cosine formula.
	angle := math.Atan2(-coordDet(a, b), coordDot(a, b))
	// We want the angle in the interval [0,2π), while Atan2 returns [-π,π]
	if angle == -0.0 {
		angle = 0.0
	} else if angle < 0 {
		angle += 2 * math.Pi
	}
	return &angle
}
