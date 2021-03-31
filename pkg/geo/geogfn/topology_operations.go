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
// NOTE: In the case of (Multi)Polygon Centroid result, it doesn't mirror with
// PostGIS's result. We are using the same algorithm of dividing into triangles.
// However, The PostGIS implementation differs as it cuts triangles in a different
// way - namely, it fixes the first point in the exterior ring as the first point
// of the triangle, whereas we always update the reference point to be
// the first point of the ring when moving from one ring to another.
//
// See: http://jennessent.com/downloads/Graphics_Shapes_Manual_A4.pdf#page=49
// for more details.
//
// Ideally, both implementations should provide the same result. However, the
// centroid of the triangles is the vectorized mean of all the points, not the
// actual projection in the Spherical surface, which causes a small inaccuracies.
// This inaccuracy will eventually grow if there is a substantial
// number of a triangle with a larger area.
func Centroid(g geo.Geography, useSphereOrSpheroid UseSphereOrSpheroid) (geo.Geography, error) {
	geomRepr, err := g.AsGeomT()
	if err != nil {
		return geo.Geography{}, err
	}
	if geomRepr.Empty() {
		return geo.MakeGeographyFromGeomT(geom.NewGeometryCollection().SetSRID(geomRepr.SRID()))
	}
	switch geomRepr.(type) {
	case *geom.Point, *geom.LineString, *geom.Polygon, *geom.MultiPoint, *geom.MultiLineString, *geom.MultiPolygon:
	default:
		return geo.Geography{}, errors.Newf("unhandled geography type %s", g.ShapeType().String())
	}

	regions, err := geo.S2RegionsFromGeomT(geomRepr, geo.EmptyBehaviorOmit)
	if err != nil {
		return geo.Geography{}, err
	}
	spheroid, err := g.Spheroid()
	if err != nil {
		return geo.Geography{}, err
	}

	// localWeightedCentroids is the collection of all the centroid corresponds to
	// various small regions in which we divide the given region for calculation
	// of centroid. The magnitude of each s2.Point.Vector represents
	// the weight corresponding to its region.
	var localWeightedCentroids []s2.Point
	for _, region := range regions {
		switch region := region.(type) {
		case s2.Point:
			localWeightedCentroids = append(localWeightedCentroids, region)
		case *s2.Polyline:
			// The algorithm used for the calculation of centroid for (Multi)LineString:
			//  * Split (Multi)LineString in the set of individual edges.
			//  * Calculate the mid-points and length/angle for all the edges.
			//  * The centroid of (Multi)LineString will be a weighted average of mid-points
			//    of all the edges, where each mid-points is weighted by its length/angle.
			for edgeIdx, regionNumEdges := 0, region.NumEdges(); edgeIdx < regionNumEdges; edgeIdx++ {
				var edgeWeight float64
				eV0 := region.Edge(edgeIdx).V0
				eV1 := region.Edge(edgeIdx).V1
				if useSphereOrSpheroid == UseSpheroid {
					edgeWeight = spheroidDistance(spheroid, eV0, eV1)
				} else {
					edgeWeight = float64(s2.ChordAngleBetweenPoints(eV0, eV1).Angle())
				}
				localWeightedCentroids = append(localWeightedCentroids, s2.Point{Vector: eV0.Add(eV1.Vector).Mul(edgeWeight)})
			}
		case *s2.Polygon:
			// The algorithm used for the calculation of centroid for (Multi)Polygon:
			//  * Split (Multi)Polygon in the set of individual triangles.
			//  * Calculate the centroid and signed area (negative area for triangle inside
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
					localWeightedCentroids = append(localWeightedCentroids, s2.Point{Vector: triangleCentroid.Mul(area)})
				}
			}
		}
	}
	var centroidVector r3.Vector
	for _, point := range localWeightedCentroids {
		centroidVector = centroidVector.Add(point.Vector)
	}
	latLng := s2.LatLngFromPoint(s2.Point{Vector: centroidVector.Normalize()})
	centroid := geom.NewPointFlat(geom.XY, []float64{latLng.Lng.Degrees(), latLng.Lat.Degrees()}).SetSRID(int(g.SRID()))
	return geo.MakeGeographyFromGeomT(centroid)
}
