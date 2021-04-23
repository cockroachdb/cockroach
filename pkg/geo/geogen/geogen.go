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

// RandomShapeType returns a random geometry type.
func RandomShapeType(rng *rand.Rand) geopb.ShapeType {
	return validShapeTypes[rng.Intn(len(validShapeTypes))]
}

var validLayouts = []geom.Layout{
	geom.XY,
	geom.XYM,
	geom.XYZ,
	geom.XYZM,
}

// RandomLayout randomly chooses a layout if no layout is provided.
func RandomLayout(rng *rand.Rand, layout geom.Layout) geom.Layout {
	if layout != geom.NoLayout {
		return layout
	}
	return validLayouts[rng.Intn(len(validLayouts))]
}

// RandomGeomBounds is a struct for storing the random bounds for each dimension.
type RandomGeomBounds struct {
	minX, maxX float64
	minY, maxY float64
	minZ, maxZ float64
	minM, maxM float64
}

// MakeRandomGeomBoundsForGeography creates a RandomGeomBounds struct with
// bounds corresponding to a geography.
func MakeRandomGeomBoundsForGeography() RandomGeomBounds {
	randomBounds := MakeRandomGeomBounds()
	randomBounds.minX, randomBounds.maxX = -180.0, 180.0
	randomBounds.minY, randomBounds.maxY = -90.0, 90.0
	return randomBounds
}

// MakeRandomGeomBounds creates a RandomGeomBounds struct with
// bounds corresponding to a geometry.
func MakeRandomGeomBounds() RandomGeomBounds {
	return RandomGeomBounds{
		minX: -10e9, maxX: 10e9,
		minY: -10e9, maxY: 10e9,
		minZ: -10e9, maxZ: 10e9,
		minM: -10e9, maxM: 10e9,
	}
}

// RandomCoordSlice generates a slice of random coords of length corresponding
// to the given layout.
func RandomCoordSlice(rng *rand.Rand, randomBounds RandomGeomBounds, layout geom.Layout) []float64 {
	if layout == geom.NoLayout {
		panic(errors.Newf("must specify a layout for RandomCoordSlice"))
	}
	var coords []float64
	coords = append(coords, RandomCoord(rng, randomBounds.minX, randomBounds.maxX))
	coords = append(coords, RandomCoord(rng, randomBounds.minY, randomBounds.maxY))

	if layout.ZIndex() != -1 {
		coords = append(coords, RandomCoord(rng, randomBounds.minZ, randomBounds.maxZ))
	}
	if layout.MIndex() != -1 {
		coords = append(coords, RandomCoord(rng, randomBounds.minM, randomBounds.maxM))
	}

	return coords
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
	rng *rand.Rand, numPoints int, randomBounds RandomGeomBounds, layout geom.Layout,
) []geom.Coord {
	layout = RandomLayout(rng, layout)
	if numPoints < 3 {
		panic(errors.Newf("need at least 3 points, got %d", numPoints))
	}
	// Generate N random points, and find the center.
	coords := make([]geom.Coord, numPoints+1)
	var centerX, centerY float64
	for i := 0; i < numPoints; i++ {
		coords[i] = RandomCoordSlice(rng, randomBounds, layout)
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
	rng *rand.Rand, randomBounds RandomGeomBounds, srid geopb.SRID, layout geom.Layout,
) *geom.Point {
	layout = RandomLayout(rng, layout)
	// 10% chance to generate an empty point.
	if rng.Intn(10) == 0 {
		return geom.NewPointEmpty(layout).SetSRID(int(srid))
	}
	return geom.NewPointFlat(layout, RandomCoordSlice(rng, randomBounds, layout)).SetSRID(int(srid))
}

// RandomLineString generates a random LineString.
func RandomLineString(
	rng *rand.Rand, randomBounds RandomGeomBounds, srid geopb.SRID, layout geom.Layout,
) *geom.LineString {
	layout = RandomLayout(rng, layout)
	// 10% chance to generate an empty linestring.
	if rng.Intn(10) == 0 {
		return geom.NewLineString(layout).SetSRID(int(srid))
	}
	numCoords := 3 + rand.Intn(10)
	randCoords := RandomValidLinearRingCoords(rng, numCoords, randomBounds, layout)

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
	return geom.NewLineString(layout).MustSetCoords(randCoords[minTrunc:maxTrunc]).SetSRID(int(srid))
}

// RandomPolygon generates a random Polygon.
func RandomPolygon(
	rng *rand.Rand, randomBounds RandomGeomBounds, srid geopb.SRID, layout geom.Layout,
) *geom.Polygon {
	layout = RandomLayout(rng, layout)
	// 10% chance to generate an empty polygon.
	if rng.Intn(10) == 0 {
		return geom.NewPolygon(layout).SetSRID(int(srid))
	}
	// TODO(otan): generate random holes inside the Polygon.
	// Ideas:
	// * We can do something like use 4 arbitrary points in the LinearRing to generate a BoundingBox,
	//   and re-use "PointIntersectsLinearRing" to generate N random points inside the 4 points to form
	//   a "sub" linear ring inside.
	// * Generate a random set of polygons, see which ones they fully cover and use that.
	return geom.NewPolygon(layout).MustSetCoords([][]geom.Coord{
		RandomValidLinearRingCoords(rng, 3+rng.Intn(10), randomBounds, layout),
	}).SetSRID(int(srid))
}

// RandomMultiPoint generates a random MultiPoint.
func RandomMultiPoint(
	rng *rand.Rand, randomBounds RandomGeomBounds, srid geopb.SRID, layout geom.Layout,
) *geom.MultiPoint {
	layout = RandomLayout(rng, layout)
	ret := geom.NewMultiPoint(layout).SetSRID(int(srid))
	// 10% chance to generate an empty multipoint (when num is 0).
	num := rng.Intn(10)
	for i := 0; i < num; i++ {
		// TODO(#62184): permit EMPTY collection item once GEOS is patched
		var point *geom.Point
		for point == nil || point.Empty() {
			point = RandomPoint(rng, randomBounds, srid, layout)
		}
		if err := ret.Push(point); err != nil {
			panic(err)
		}
	}
	return ret
}

// RandomMultiLineString generates a random MultiLineString.
func RandomMultiLineString(
	rng *rand.Rand, randomBounds RandomGeomBounds, srid geopb.SRID, layout geom.Layout,
) *geom.MultiLineString {
	layout = RandomLayout(rng, layout)
	ret := geom.NewMultiLineString(layout).SetSRID(int(srid))
	// 10% chance to generate an empty multilinestring (when num is 0).
	num := rng.Intn(10)
	for i := 0; i < num; i++ {
		// TODO(#62184): permit EMPTY collection item once GEOS is patched
		var lineString *geom.LineString
		for lineString == nil || lineString.Empty() {
			lineString = RandomLineString(rng, randomBounds, srid, layout)
		}
		if err := ret.Push(lineString); err != nil {
			panic(err)
		}
	}
	return ret
}

// RandomMultiPolygon generates a random MultiPolygon.
func RandomMultiPolygon(
	rng *rand.Rand, randomBounds RandomGeomBounds, srid geopb.SRID, layout geom.Layout,
) *geom.MultiPolygon {
	layout = RandomLayout(rng, layout)
	ret := geom.NewMultiPolygon(layout).SetSRID(int(srid))
	// 10% chance to generate an empty multipolygon (when num is 0).
	num := rng.Intn(10)
	for i := 0; i < num; i++ {
		// TODO(#62184): permit EMPTY collection item once GEOS is patched
		var polygon *geom.Polygon
		for polygon == nil || polygon.Empty() {
			polygon = RandomPolygon(rng, randomBounds, srid, layout)
		}
		if err := ret.Push(polygon); err != nil {
			panic(err)
		}
	}
	return ret
}

// RandomGeometryCollection generates a random GeometryCollection.
func RandomGeometryCollection(
	rng *rand.Rand, randomBounds RandomGeomBounds, srid geopb.SRID, layout geom.Layout,
) *geom.GeometryCollection {
	layout = RandomLayout(rng, layout)
	ret := geom.NewGeometryCollection().SetSRID(int(srid))
	if err := ret.SetLayout(layout); err != nil {
		panic(err)
	}
	// 10% chance to generate an empty geometrycollection (when num is 0).
	num := rng.Intn(10)
	for i := 0; i < num; i++ {
		var shape geom.T
		needShape := true
		// Keep searching for a non GeometryCollection.
		for needShape {
			shape = RandomGeomT(rng, randomBounds, srid, layout)
			_, needShape = shape.(*geom.GeometryCollection)
			// TODO(#62184): permit EMPTY collection item once GEOS is patched
			if shape.Empty() {
				needShape = true
			}
		}
		if err := ret.Push(shape); err != nil {
			panic(err)
		}
	}
	return ret
}

// RandomGeomT generates a random geom.T object with the given layout within the
// given bounds and SRID.
func RandomGeomT(
	rng *rand.Rand, randomBounds RandomGeomBounds, srid geopb.SRID, layout geom.Layout,
) geom.T {
	layout = RandomLayout(rng, layout)
	shapeType := RandomShapeType(rng)
	switch shapeType {
	case geopb.ShapeType_Point:
		return RandomPoint(rng, randomBounds, srid, layout)
	case geopb.ShapeType_LineString:
		return RandomLineString(rng, randomBounds, srid, layout)
	case geopb.ShapeType_Polygon:
		return RandomPolygon(rng, randomBounds, srid, layout)
	case geopb.ShapeType_MultiPoint:
		return RandomMultiPoint(rng, randomBounds, srid, layout)
	case geopb.ShapeType_MultiLineString:
		return RandomMultiLineString(rng, randomBounds, srid, layout)
	case geopb.ShapeType_MultiPolygon:
		return RandomMultiPolygon(rng, randomBounds, srid, layout)
	case geopb.ShapeType_GeometryCollection:
		return RandomGeometryCollection(rng, randomBounds, srid, layout)
	}
	panic(errors.Newf("unknown shape type: %v", shapeType))
}

// RandomGeometry generates a random Geometry with the given SRID.
func RandomGeometry(rng *rand.Rand, srid geopb.SRID) geo.Geometry {
	return RandomGeometryWithLayout(rng, srid, geom.NoLayout)
}

// RandomGeometryWithLayout generates a random Geometry of a given layout with
// the given SRID.
func RandomGeometryWithLayout(rng *rand.Rand, srid geopb.SRID, layout geom.Layout) geo.Geometry {
	randomBounds := MakeRandomGeomBounds()
	if srid != 0 {
		proj, err := geoprojbase.Projection(srid)
		if err != nil {
			panic(err)
		}
		randomBounds.minX, randomBounds.maxX = proj.Bounds.MinX, proj.Bounds.MaxX
		randomBounds.minY, randomBounds.maxY = proj.Bounds.MinY, proj.Bounds.MaxY
	}
	ret, err := geo.MakeGeometryFromGeomT(RandomGeomT(rng, randomBounds, srid, layout))
	if err != nil {
		panic(err)
	}
	return ret
}

// RandomGeography generates a random Geometry with the given SRID.
func RandomGeography(rng *rand.Rand, srid geopb.SRID) geo.Geography {
	return RandomGeographyWithLayout(rng, srid, geom.NoLayout)
}

// RandomGeographyWithLayout generates a random Geometry of a given layout with
// the given SRID.
func RandomGeographyWithLayout(rng *rand.Rand, srid geopb.SRID, layout geom.Layout) geo.Geography {
	// TODO(otan): generate geographies that traverse latitude/longitude boundaries.
	randomBoundsGeography := MakeRandomGeomBoundsForGeography()
	ret, err := geo.MakeGeographyFromGeomT(RandomGeomT(rng, randomBoundsGeography, srid, layout))
	if err != nil {
		panic(err)
	}
	return ret
}
