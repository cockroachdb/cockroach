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
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
)

// LineInterpolatePoints returns one or more points along the given
// LineString which are at an integral multiples of given fraction of
// LineString's total length. When repeat is set to false, it returns
// the first point.
func LineInterpolatePoints(g *geo.Geometry, fraction float64, repeat bool) (*geo.Geometry, error) {
	if fraction < 0 || fraction > 1 {
		return nil, errors.Newf("fraction %f should be within [0 1] range", fraction)
	}
	geomRepr, err := g.AsGeomT()
	if err != nil {
		return nil, err
	}
	switch geomRepr := geomRepr.(type) {
	case *geom.LineString:
		// In case fraction is greater than 0.5 or equal to 0 or repeat is false,
		// then we will have only one interpolated point.
		lengthOfLineString := geomRepr.Length()
		if repeat && fraction <= 0.5 && fraction != 0 {
			numberOfInterpolatedPoints := int(1 / fraction)
			interpolatedPoints := geom.NewMultiPoint(geom.XY).SetSRID(geomRepr.SRID())
			for pointInserted := 1; pointInserted <= numberOfInterpolatedPoints; pointInserted++ {
				pointEWKB, err := geos.InterpolateLine(g.EWKB(), float64(pointInserted)*fraction*lengthOfLineString)
				if err != nil {
					return nil, err
				}
				point, err := ewkb.Unmarshal(pointEWKB)
				if err != nil {
					return nil, err
				}
				err = interpolatedPoints.Push(point.(*geom.Point))
				if err != nil {
					return nil, err
				}
			}
			return geo.NewGeometryFromGeom(interpolatedPoints)
		}
		interpolatedPointEWKB, err := geos.InterpolateLine(g.EWKB(), fraction*lengthOfLineString)
		if err != nil {
			return nil, err
		}
		return geo.ParseGeometryFromEWKB(interpolatedPointEWKB)
	default:
		return nil, errors.Newf("geometry %s should be LineString", g.ShapeType())
	}
}
