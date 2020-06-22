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
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/r3"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
)

// Centroid returns the Centroid of a given Geography.
//
// Note: In the case of (Multi)Polygon Centroid result, don't mirror with
// `PostGIS`'s result. Though we are using the same algorithm. Their
// implementation differs, as in the `PostGIS` reference point for all the
// triangles remains same, which changes the centroid significantly as we
// change the starting point of the outermost ring.
func Centroid(g *geo.Geography, useSphereOrSpheroid UseSphereOrSpheroid) (*geo.Geography, error) {
	geomRepr, err := g.AsGeomT()
	if err != nil {
		return nil, err
	}
	if geomRepr.Empty() {
		// Return Empty GeometryCollection in case of Empty Geographical Objects
		return geo.NewGeographyFromGeom(geom.NewGeometryCollection().SetSRID(geomRepr.SRID()))
	}
	switch geomRepr.(type) {
	case *geom.Point, *geom.LineString, *geom.Polygon, *geom.MultiPoint, *geom.MultiLineString, *geom.MultiPolygon:
	default:
		// GeometryCollection is among the unhandled Geography type.
		return nil, errors.Newf("unhandled geography type %T", geomRepr)
	}

	regions, err := geo.S2RegionsFromGeom(geomRepr, geo.EmptyBehaviorOmit)
	if err != nil {
		return nil, err
	}
	spheroid, err := g.Spheroid()
	if err != nil {
		return nil, err
	}

	// `localWgtCentroids` is the collection of all the centroid corresponds to
	// various small regions in which we divide the given region for calculation
	// of centroid. The magnitude of each vector(`s2.Point.Vector`) represents
	// the weight corresponding to its region.
	var localWgtCentroids []s2.Point
	for _, region := range regions {
		switch region := region.(type) {
		case s2.Point:
			localWgtCentroids = append(localWgtCentroids, region)
		case *s2.Polyline:
			// The algorithm used for the calculation of centroid for (Multi)LineString:
			//  * Split (Multi)LineString in the set of individual edges.
			//  * Calculate the mid-points and length/angle for all the edges.
			//  * The centroid of (Multi)LineString will be a weighted average of mid-points
			//    of all the edges, where each mid-points is weighted by its length/angle.
			for edgeIdx := 0; edgeIdx < region.NumEdges(); edgeIdx++ {
				var edgeWgt float64
				eV0 := region.Edge(edgeIdx).V0
				eV1 := region.Edge(edgeIdx).V1
				if useSphereOrSpheroid == UseSpheroid {
					edgeWgt = spheroidDistance(spheroid, eV0, eV1)
				} else {
					edgeWgt = float64(s2.ChordAngleBetweenPoints(eV0, eV1).Angle())
				}
				localWgtCentroids = append(localWgtCentroids, s2.Point{Vector: eV0.Add(eV1.Vector).Mul(edgeWgt)})
			}
		case *s2.Polygon:
			// The algorithm used for the calculation of centroid for (Multi)Polygon:
			//  * Split (Multi)Polygon in the set of individual triangles.
			//  * Calculate the centroid and singed area (negative area for triangle inside
			//    the hole) for all the triangle.
			//  * The centroid of (Multi)Polygon will be a weighted average of the centroid
			//    of all the triangle, where each centroid is weighted by its area.
			for _, loop := range region.Loops() {
				triangleVertices := make([]s2.Point, 4)
				triangleVertices[0] = loop.Vertex(0)
				triangleVertices[3] = loop.Vertex(0)

				for pointIdx := 1; pointIdx+2 < loop.NumVertices(); pointIdx++ {
					triangleVertices[1] = loop.Vertex(pointIdx)
					triangleVertices[2] = loop.Vertex(pointIdx + 1)
					triangleCentroid := s2.PlanarCentroid(triangleVertices[0], triangleVertices[1], triangleVertices[2])
					var area float64
					if useSphereOrSpheroid == UseSpheroid {
						area, _ = spheroid.AreaAndPerimeter(triangleVertices[:3])
					} else {
						area = s2.LoopFromPoints(triangleVertices).Area()
					}
					area = area * float64(loop.Sign())
					localWgtCentroids = append(localWgtCentroids, s2.Point{Vector: triangleCentroid.Mul(area)})
				}
			}
		}
	}
	var centroidVector r3.Vector
	for _, point := range localWgtCentroids {
		centroidVector = centroidVector.Add(point.Vector)
	}
	latLng := s2.LatLngFromPoint(s2.Point{Vector: centroidVector.Normalize()})
	centroid := geom.NewPointFlat(geom.XY, []float64{latLng.Lng.Degrees(), latLng.Lat.Degrees()}).SetSRID(int(g.SRID()))
	return geo.NewGeographyFromGeom(centroid)
}
