// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package geogen provides utilities for generating various geospatial types.
package geogen

import (
	"math"
	"math/rand"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

var validShapeTypes = []geopb.ShapeType{
	geopb.ShapeType_Point,
	geopb.ShapeType_LineString,
	geopb.ShapeType_Polygon,
	geopb.ShapeType_MultiPoint,
	geopb.ShapeType_MultiLineString,
	geopb.ShapeType_MultiPolygon,
	geopb.ShapeType_GeometryCollection,
}

// RandomCoord generates a random coord in the given bounds.
func RandomCoord(rng *rand.Rand, min float64, max float64) float64 {
	return rng.Float64()*(max-min) + min
}

// RandomValidLinearRingCoords generates a flat float64 array of coordinates that represents
// a completely closed shape that can represent a simple LinearRing. This shape is always valid.
// A LinearRing must have at least 3 points. A point is added at the end to close the ring.
// Implements the algorithm in https://observablehq.com/@tarte0/generate-random-simple-polygon.
func RandomValidLinearRingCoords(
	rng *rand.Rand, numPoints int, minX float64, maxX float64, minY float64, maxY float64,
) []geom.Coord {
	if numPoints < 3 {
		panic(errors.Newf("need at least 3 points, got %d", numPoints))
	}
	// Generate N random points, and find the center.
	coords := make([]geom.Coord, numPoints+1)
	var centerX, centerY float64
	for i := 0; i < numPoints; i++ {
		coords[i] = geom.Coord{
			RandomCoord(rng, minX, maxX),
			RandomCoord(rng, minY, maxY),
		}
		centerX += coords[i].X()
		centerY += coords[i].Y()
	}

	centerX /= float64(numPoints)
	centerY /= float64(numPoints)

	// Sort by the angle of all the points relative to the center.
	// Use ascending order of angle to get a CCW loop.
	sort.Slice(coords[:numPoints], func(i, j int) bool {
		angleI := math.Atan2(coords[i].Y()-centerY, coords[i].X()-centerX)
		angleJ := math.Atan2(coords[j].Y()-centerY, coords[j].X()-centerX)
		return angleI < angleJ
	})

	// Append the first coordinate to the end.
	coords[numPoints] = coords[0]
	return coords
}

// RandomPoint generates a random Point.
func RandomPoint(
	rng *rand.Rand, minX float64, maxX float64, minY float64, maxY float64, srid geopb.SRID,
) *geom.Point {
	return geom.NewPointFlat(geom.XY, []float64{
		RandomCoord(rng, minX, maxX),
		RandomCoord(rng, minY, maxY),
	}).SetSRID(int(srid))
}

// RandomLineString generates a random LineString.
func RandomLineString(
	rng *rand.Rand, minX float64, maxX float64, minY float64, maxY float64, srid geopb.SRID,
) *geom.LineString {
	numCoords := 3 + rand.Intn(10)
	randCoords := RandomValidLinearRingCoords(rng, numCoords, minX, maxX, minY, maxY)

	// Extract a random substring from the LineString by truncating at the ends.
	var minTrunc, maxTrunc int
	// Ensure we always have at least two points.
	for maxTrunc-minTrunc < 2 {
		minTrunc, maxTrunc = rand.Intn(numCoords+1), rand.Intn(numCoords+1)
		// Ensure maxTrunc >= minTrunc.
		if minTrunc > maxTrunc {
			minTrunc, maxTrunc = maxTrunc, minTrunc
		}
	}
	return geom.NewLineString(geom.XY).MustSetCoords(randCoords[minTrunc:maxTrunc]).SetSRID(int(srid))
}

// RandomPolygon generates a random Polygon.
func RandomPolygon(
	rng *rand.Rand, minX float64, maxX float64, minY float64, maxY float64, srid geopb.SRID,
) *geom.Polygon {
	// TODO(otan): generate random holes inside the Polygon.
	// Ideas:
	// * We can do something like use 4 arbitrary points in the LinearRing to generate a BoundingBox,
	//   and re-use "PointInLinearRing" to generate N random points inside the 4 points to form
	//   a "sub" linear ring inside.
	// * Generate a random set of polygons, see which ones they fully cover and use that.
	return geom.NewPolygon(geom.XY).MustSetCoords([][]geom.Coord{
		RandomValidLinearRingCoords(rng, 3+rng.Intn(10), minX, maxX, minY, maxY),
	}).SetSRID(int(srid))
}

// RandomGeomT generates a random geom.T object within the given bounds and SRID.
func RandomGeomT(
	rng *rand.Rand, minX float64, maxX float64, minY float64, maxY float64, srid geopb.SRID,
) geom.T {
	shapeType := validShapeTypes[rng.Intn(len(validShapeTypes))]
	switch shapeType {
	case geopb.ShapeType_Point:
		return RandomPoint(rng, minX, maxX, minY, maxY, srid)
	case geopb.ShapeType_LineString:
		return RandomLineString(rng, minX, maxX, minY, maxY, srid)
	case geopb.ShapeType_Polygon:
		return RandomPolygon(rng, minX, maxX, minY, maxY, srid)
	case geopb.ShapeType_MultiPoint:
		// TODO(otan): add empty points.
		ret := geom.NewMultiPoint(geom.XY).SetSRID(int(srid))
		num := 1 + rng.Intn(10)
		for i := 0; i < num; i++ {
			if err := ret.Push(RandomPoint(rng, minX, maxX, minY, maxY, srid)); err != nil {
				panic(err)
			}
		}
		return ret
	case geopb.ShapeType_MultiLineString:
		// TODO(otan): add empty LineStrings.
		ret := geom.NewMultiLineString(geom.XY).SetSRID(int(srid))
		num := 1 + rng.Intn(10)
		for i := 0; i < num; i++ {
			if err := ret.Push(RandomLineString(rng, minX, maxX, minY, maxY, srid)); err != nil {
				panic(err)
			}
		}
		return ret
	case geopb.ShapeType_MultiPolygon:
		// TODO(otan): add empty Polygons.
		ret := geom.NewMultiPolygon(geom.XY).SetSRID(int(srid))
		num := 1 + rng.Intn(10)
		for i := 0; i < num; i++ {
			if err := ret.Push(RandomPolygon(rng, minX, maxX, minY, maxY, srid)); err != nil {
				panic(err)
			}
		}
		return ret
	case geopb.ShapeType_GeometryCollection:
		ret := geom.NewGeometryCollection().SetSRID(int(srid))
		num := 1 + rng.Intn(10)
		for i := 0; i < num; i++ {
			var shape geom.T
			needShape := true
			// Keep searching for a non GeometryCollection.
			for needShape {
				shape = RandomGeomT(rng, minX, maxX, minY, maxY, srid)
				_, needShape = shape.(*geom.GeometryCollection)
			}
			if err := ret.Push(shape); err != nil {
				panic(err)
			}
		}
		return ret
	}
	panic(errors.Newf("unknown shape type: %v", shapeType))
}

// RandomGeometry generates a random Geometry with the given SRID.
func RandomGeometry(rng *rand.Rand, srid geopb.SRID) geo.Geometry {
	minX, maxX := -math.MaxFloat32, math.MaxFloat32
	minY, maxY := -math.MaxFloat32, math.MaxFloat32
	proj, ok := geoprojbase.Projections[srid]
	if ok {
		minX, maxX = proj.Bounds.MinX, proj.Bounds.MaxX
		minY, maxY = proj.Bounds.MinY, proj.Bounds.MaxY
	}
	ret, err := geo.MakeGeometryFromGeomT(RandomGeomT(rng, minX, maxX, minY, maxY, srid))
	if err != nil {
		panic(err)
	}
	return ret
}

// RandomGeography generates a random Geometry with the given SRID.
func RandomGeography(rng *rand.Rand, srid geopb.SRID) geo.Geography {
	// TODO(otan): generate geographies that traverse latitude/longitude boundaries.
	minX, maxX := -180.0, 180.0
	minY, maxY := -90.0, 90.0
	ret, err := geo.MakeGeographyFromGeomT(RandomGeomT(rng, minX, maxX, minY, maxY, srid))
	if err != nil {
		panic(err)
	}
	return ret
}
