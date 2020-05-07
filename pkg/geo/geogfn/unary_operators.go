// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geogfn

import (
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geographiclib"
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
)

// Area returns the area of a given Geography.
func Area(g *geo.Geography, useSphereOrSpheroid UseSphereOrSpheroid) (float64, error) {
	regions, err := g.AsS2()
	if err != nil {
		return 0, err
	}
	spheroid := geographiclib.WGS84Spheroid

	var totalArea float64
	for _, region := range regions {
		switch region := region.(type) {
		case s2.Point, *s2.Polyline:
		case *s2.Polygon:
			if useSphereOrSpheroid == UseSpheroid {
				for _, loop := range region.Loops() {
					points := loop.Vertices()
					area, _ := spheroid.AreaAndPerimeter(points[:len(points)-1])
					totalArea += float64(loop.Sign()) * area
				}
			} else {
				totalArea += region.Area()
			}
		default:
			return 0, errors.Newf("unknown type: %T", region)
		}
	}
	if useSphereOrSpheroid == UseSphere {
		totalArea *= spheroid.SphereRadius * spheroid.SphereRadius
	}
	return totalArea, nil
}

// Perimeter returns the perimeter of a given Geography.
func Perimeter(g *geo.Geography, useSphereOrSpheroid UseSphereOrSpheroid) (float64, error) {
	gt, err := g.AsGeomT()
	if err != nil {
		return 0, err
	}
	// This check mirrors PostGIS behavior, where GeometryCollections
	// of LineStrings include the length for perimeters.
	switch gt.(type) {
	case *geom.Polygon, *geom.MultiPolygon, *geom.GeometryCollection:
	default:
		return 0, nil
	}
	return length(geo.S2RegionsFromGeom(gt), useSphereOrSpheroid)
}

// Length returns length of a given Geography.
func Length(g *geo.Geography, useSphereOrSpheroid UseSphereOrSpheroid) (float64, error) {
	gt, err := g.AsGeomT()
	if err != nil {
		return 0, err
	}
	// This check mirrors PostGIS behavior, where GeometryCollections
	// of Polygons include the perimeters for polygons.
	switch gt.(type) {
	case *geom.LineString, *geom.MultiLineString, *geom.GeometryCollection:
	default:
		return 0, nil
	}
	return length(geo.S2RegionsFromGeom(gt), useSphereOrSpheroid)
}

// length returns the sum of the lengtsh and perimeters in the shapes of the Geography.
// In OGC parlance, length returns both LineString lengths _and_ Polygon perimeters.
func length(regions []s2.Region, useSphereOrSpheroid UseSphereOrSpheroid) (float64, error) {
	spheroid := geographiclib.WGS84Spheroid

	var totalLength float64
	for _, region := range regions {
		switch region := region.(type) {
		case s2.Point:
		case *s2.Polyline:
			if useSphereOrSpheroid == UseSpheroid {
				totalLength += spheroid.InverseBatch((*region))
			} else {
				for edgeIdx := 0; edgeIdx < region.NumEdges(); edgeIdx++ {
					edge := region.Edge(edgeIdx)
					totalLength += s2.ChordAngleBetweenPoints(edge.V0, edge.V1).Angle().Radians()
				}
			}
		case *s2.Polygon:
			for _, loop := range region.Loops() {
				if useSphereOrSpheroid == UseSpheroid {
					totalLength += spheroid.InverseBatch(loop.Vertices())
				} else {
					for edgeIdx := 0; edgeIdx < loop.NumEdges(); edgeIdx++ {
						edge := loop.Edge(edgeIdx)
						totalLength += s2.ChordAngleBetweenPoints(edge.V0, edge.V1).Angle().Radians()
					}
				}
			}
		default:
			return 0, errors.Newf("unknown type: %T", region)
		}
	}
	if useSphereOrSpheroid == UseSphere {
		totalLength *= spheroid.SphereRadius
	}
	return totalLength, nil
}
