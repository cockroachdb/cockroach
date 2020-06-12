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
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
)

// Area returns the area of a given Geography.
func Area(g *geo.Geography, useSphereOrSpheroid UseSphereOrSpheroid) (float64, error) {
	regions, err := g.AsS2(geo.EmptyBehaviorOmit)
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
	regions, err := geo.S2RegionsFromGeom(gt, geo.EmptyBehaviorOmit)
	if err != nil {
		return 0, err
	}
	return length(regions, useSphereOrSpheroid)
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
	regions, err := geo.S2RegionsFromGeom(gt, geo.EmptyBehaviorOmit)
	if err != nil {
		return 0, err
	}
	return length(regions, useSphereOrSpheroid)
}

// Project returns calculate a projected point given a source point, a distance and a azimuth.
func Project(point *geom.Point, distance float64, azimuth s1.Angle) (*geom.Point, error) {
	spheroid := geographiclib.WGS84Spheroid

	// Normalize distance to be positive.
	if distance < 0.0 {
		distance = -distance
		azimuth += math.Pi
	}

	// Normalize azimuth
	azimuth = azimuth.Normalized()

	// Check the distance validity.
	if distance > (math.Pi * spheroid.Radius) {
		return nil, errors.Newf("distance must not be greater than %f", math.Pi*spheroid.Radius)
	}

	// Convert to ta geodetic point.
	x := point.X()
	y := point.Y()

	projected := spheroid.Project(
		s2.LatLngFromDegrees(x, y),
		distance,
		azimuth,
	)

	return geom.NewPointFlat(
		geom.XY,
		[]float64{
			float64(projected.Lng.Normalized()) * 180.0 / math.Pi,
			normalizeLatitude(float64(projected.Lat)) * 180.0 / math.Pi,
		},
	).SetSRID(point.SRID()), nil
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

// normalizeLatitude convert a latitude to the range of -Pi/2, Pi/2.
func normalizeLatitude(lat float64) float64 {
	if lat > 2.0*math.Pi {
		lat = math.Remainder(lat, 2.0*math.Pi)
	}

	if lat < -2.0*math.Pi {
		lat = math.Remainder(lat, -2.0*math.Pi)
	}

	if lat > math.Pi {
		lat = math.Pi - lat
	}

	if lat < -1.0*math.Pi {
		lat = -1.0*math.Pi - lat
	}

	if lat > math.Pi*2 {
		lat = math.Pi - lat
	}

	if lat < -1.0*math.Pi*2 {
		lat = -1.0*math.Pi - lat
	}

	return lat
}
