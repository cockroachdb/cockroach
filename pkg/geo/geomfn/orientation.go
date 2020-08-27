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
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// Orientation defines an orientation of a shape.
type Orientation int

const (
	// OrientationCW denotes a clockwise orientation.
	OrientationCW Orientation = iota
	// OrientationCCW denotes a counter-clockwise orientation
	OrientationCCW
)

// HasPolygonOrientation checks whether a given Geometry have polygons
// that matches the given Orientation.
// Non-Polygon objects
func HasPolygonOrientation(g geo.Geometry, o Orientation) (bool, error) {
	t, err := g.AsGeomT()
	if err != nil {
		return false, err
	}
	return hasPolygonOrientation(t, o)
}

func hasPolygonOrientation(g geom.T, o Orientation) (bool, error) {
	switch g := g.(type) {
	case *geom.Polygon:
		for i := 0; i < g.NumLinearRings(); i++ {
			isCCW := geo.IsLinearRingCCW(g.LinearRing(i))
			// Interior rings should be the reverse orientation of the exterior ring.
			if i > 0 {
				isCCW = !isCCW
			}
			switch o {
			case OrientationCW:
				if isCCW {
					return false, nil
				}
			case OrientationCCW:
				if !isCCW {
					return false, nil
				}
			default:
				return false, errors.Newf("unexpected orientation: %v", o)
			}
		}
		return true, nil
	case *geom.MultiPolygon:
		for i := 0; i < g.NumPolygons(); i++ {
			if ret, err := hasPolygonOrientation(g.Polygon(i), o); !ret || err != nil {
				return ret, err
			}
		}
		return true, nil
	case *geom.GeometryCollection:
		for i := 0; i < g.NumGeoms(); i++ {
			if ret, err := hasPolygonOrientation(g.Geom(i), o); !ret || err != nil {
				return ret, err
			}
		}
		return true, nil
	case *geom.Point, *geom.MultiPoint, *geom.LineString, *geom.MultiLineString:
		return true, nil
	default:
		return false, errors.Newf("unhandled geometry type: %T", g)
	}
}

// ForcePolygonOrientation forces orientations within polygons
// to be oriented the prescribed way.
func ForcePolygonOrientation(g geo.Geometry, o Orientation) (geo.Geometry, error) {
	t, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}

	if err := forcePolygonOrientation(t, o); err != nil {
		return geo.Geometry{}, err
	}
	return geo.MakeGeometryFromGeomT(t)
}

func forcePolygonOrientation(g geom.T, o Orientation) error {
	switch g := g.(type) {
	case *geom.Polygon:
		for i := 0; i < g.NumLinearRings(); i++ {
			isCCW := geo.IsLinearRingCCW(g.LinearRing(i))
			// Interior rings should be the reverse orientation of the exterior ring.
			if i > 0 {
				isCCW = !isCCW
			}
			reverse := false
			switch o {
			case OrientationCW:
				if isCCW {
					reverse = true
				}
			case OrientationCCW:
				if !isCCW {
					reverse = true
				}
			default:
				return errors.Newf("unexpected orientation: %v", o)
			}

			if reverse {
				// Reverse coordinates from both ends.
				// Do this by swapping up to the middle of the array of elements, which guarantees
				// each end get swapped. This works for an odd number of elements as well as
				// the middle element ends swapping with itself, which is ok.
				coords := g.LinearRing(i).FlatCoords()
				for cIdx := 0; cIdx < len(coords)/2; cIdx += g.Stride() {
					for sIdx := 0; sIdx < g.Stride(); sIdx++ {
						coords[cIdx+sIdx], coords[len(coords)-cIdx-g.Stride()+sIdx] = coords[len(coords)-cIdx-g.Stride()+sIdx], coords[cIdx+sIdx]
					}
				}
			}
		}
		return nil
	case *geom.MultiPolygon:
		for i := 0; i < g.NumPolygons(); i++ {
			if err := forcePolygonOrientation(g.Polygon(i), o); err != nil {
				return err
			}
		}
		return nil
	case *geom.GeometryCollection:
		for i := 0; i < g.NumGeoms(); i++ {
			if err := forcePolygonOrientation(g.Geom(i), o); err != nil {
				return err
			}
		}
		return nil
	case *geom.Point, *geom.MultiPoint, *geom.LineString, *geom.MultiLineString:
		return nil
	default:
		return errors.Newf("unhandled geometry type: %T", g)
	}
}
