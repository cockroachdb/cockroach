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
	"math"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geographiclib"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
)

// Area returns the area of a given Geography.
func Area(g geo.Geography, useSphereOrSpheroid UseSphereOrSpheroid) (float64, error) {
	regions, err := g.AsS2(geo.EmptyBehaviorOmit)
	if err != nil {
		return 0, err
	}
	spheroid, err := g.Spheroid()
	if err != nil {
		return 0, err
	}

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
			return 0, pgerror.Newf(pgcode.InvalidParameterValue, "unknown type: %T", region)
		}
	}
	if useSphereOrSpheroid == UseSphere {
		totalArea *= spheroid.SphereRadius * spheroid.SphereRadius
	}
	return totalArea, nil
}

// Perimeter returns the perimeter of a given Geography.
func Perimeter(g geo.Geography, useSphereOrSpheroid UseSphereOrSpheroid) (float64, error) {
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
	regions, err := geo.S2RegionsFromGeomT(gt, geo.EmptyBehaviorOmit)
	if err != nil {
		return 0, err
	}
	spheroid, err := g.Spheroid()
	if err != nil {
		return 0, err
	}
	return length(regions, spheroid, useSphereOrSpheroid)
}

// Length returns length of a given Geography.
func Length(g geo.Geography, useSphereOrSpheroid UseSphereOrSpheroid) (float64, error) {
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
	regions, err := geo.S2RegionsFromGeomT(gt, geo.EmptyBehaviorOmit)
	if err != nil {
		return 0, err
	}
	spheroid, err := g.Spheroid()
	if err != nil {
		return 0, err
	}
	return length(regions, spheroid, useSphereOrSpheroid)
}

// Project returns calculate a projected point given a source point, a distance and a azimuth.
func Project(g geo.Geography, distance float64, azimuth s1.Angle) (geo.Geography, error) {
	geomT, err := g.AsGeomT()
	if err != nil {
		return geo.Geography{}, err
	}

	point, ok := geomT.(*geom.Point)
	if !ok {
		return geo.Geography{}, pgerror.Newf(pgcode.InvalidParameterValue, "ST_Project(geography) is only valid for point inputs")
	}

	spheroid, err := g.Spheroid()
	if err != nil {
		return geo.Geography{}, err
	}

	// Normalize distance to be positive.
	if distance < 0.0 {
		distance = -distance
		azimuth += math.Pi
	}

	// Normalize azimuth
	azimuth = azimuth.Normalized()

	// Check the distance validity.
	if distance > (math.Pi * spheroid.Radius) {
		return geo.Geography{}, pgerror.Newf(pgcode.InvalidParameterValue, "distance must not be greater than %f", math.Pi*spheroid.Radius)
	}

	if point.Empty() {
		return geo.Geography{}, pgerror.Newf(pgcode.InvalidParameterValue, "cannot project POINT EMPTY")
	}

	// Convert to ta geodetic point.
	x := point.X()
	y := point.Y()

	projected := spheroid.Project(
		s2.LatLngFromDegrees(x, y),
		distance,
		azimuth,
	)

	ret := geom.NewPointFlat(
		geom.XY,
		[]float64{
			geo.NormalizeLongitudeDegrees(projected.Lng.Degrees()),
			geo.NormalizeLatitudeDegrees(projected.Lat.Degrees()),
		},
	).SetSRID(point.SRID())
	return geo.MakeGeographyFromGeomT(ret)
}

// length returns the sum of the lengths and perimeters in the shapes of the Geography.
// In OGC parlance, length returns both LineString lengths _and_ Polygon perimeters.
func length(
	regions []s2.Region, spheroid *geographiclib.Spheroid, useSphereOrSpheroid UseSphereOrSpheroid,
) (float64, error) {
	var totalLength float64
	for _, region := range regions {
		switch region := region.(type) {
		case s2.Point:
		case *s2.Polyline:
			if useSphereOrSpheroid == UseSpheroid {
				totalLength += spheroid.InverseBatch((*region))
			} else {
				for edgeIdx, regionNumEdges := 0, region.NumEdges(); edgeIdx < regionNumEdges; edgeIdx++ {
					edge := region.Edge(edgeIdx)
					totalLength += s2.ChordAngleBetweenPoints(edge.V0, edge.V1).Angle().Radians()
				}
			}
		case *s2.Polygon:
			for _, loop := range region.Loops() {
				if useSphereOrSpheroid == UseSpheroid {
					totalLength += spheroid.InverseBatch(loop.Vertices())
				} else {
					for edgeIdx, loopNumEdges := 0, loop.NumEdges(); edgeIdx < loopNumEdges; edgeIdx++ {
						edge := loop.Edge(edgeIdx)
						totalLength += s2.ChordAngleBetweenPoints(edge.V0, edge.V1).Angle().Radians()
					}
				}
			}
		default:
			return 0, pgerror.Newf(pgcode.InvalidParameterValue, "unknown type: %T", region)
		}
	}
	if useSphereOrSpheroid == UseSphere {
		totalLength *= spheroid.SphereRadius
	}
	return totalLength, nil
}
