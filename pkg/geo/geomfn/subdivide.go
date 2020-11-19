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

// Subdivide returns a geometry divided into parts, where each part contains no more than the number of vertices provided.
func Subdivide(g geo.Geometry, maxVertices int) (geo.Geometry, error) {
	if g.Empty() {
		return g, nil
	}
	const minMaxVertices = 5
	if maxVertices < minMaxVertices {
		return geo.Geometry{}, errors.Newf("max_vertices number cannot be less than %v", minMaxVertices)
	}

	gt, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, errors.Newf("could not transform input geometry into geom.T: %v", err)
	}
	col := geom.NewGeometryCollection()
	dim, err := dimensionFromGeomT(gt)
	if err != nil {
		return geo.Geometry{}, errors.Newf("could not calculate geometry dimension: %v", err)
	}
	const startDepth = 0
	err = subdivideRecursive(gt, maxVertices, startDepth, col, dim)
	if err != nil {
		return geo.Geometry{}, err
	}
	col.SetSRID(gt.SRID())
	output, err := geo.MakeGeometryFromGeomT(col)
	if err != nil {
		return geo.Geometry{}, errors.Newf("could not transform output geom.T into geometry: %v", err)
	}
	return output, nil
}

func subdivideRecursive(
	gt geom.T, maxVertices int, depth int, col *geom.GeometryCollection, dim int,
) error {
	const geometryCollectionPushError = "could not add geometry to results collection: %v"
	// maxDepth 50 => 2^50 ~= 10^15 subdivisions.
	const maxDepth = 50
	clip := geo.BoundingBoxFromGeomTGeometryType(gt)
	width := clip.HiX - clip.LoX
	height := clip.HiY - clip.LoY
	if width == 0 && height == 0 {
		if gt, ok := gt.(*geom.Point); ok && dim == 0 {
			err := col.Push(gt)
			if err != nil {
				return errors.Newf(geometryCollectionPushError, err)
			}
		}
		return nil
	}
	// Boundaries needs to be widen slightly for intersection to work properly.
	tolerance := 1e-12
	if width == 0 {
		clip.HiX += tolerance
		clip.LoX -= tolerance
		width = 2 * tolerance
	}
	if height == 0 {
		clip.HiY += tolerance
		clip.LoY -= tolerance
		height = 2 * tolerance
	}
	// Recurse into collections other than MultiPoint.
	isCollection, err := isGeomTCollection(gt)
	if err != nil {
		return errors.Newf("error checking if geom.T is collection: %v", err)
	}
	_, isMultiPoint := gt.(*geom.MultiPoint)
	if isCollection && !isMultiPoint {
		err := forEachGeomTInCollection(gt, func(gi geom.T) error {
			// It's not a subdivision yet, so depth stays the same
			return subdivideRecursive(gi, maxVertices, depth, col, dim)
		})
		if err != nil {
			return err
		}
		return nil
	}
	currDim, err := dimensionFromGeomT(gt)
	if err != nil {
		return errors.Newf("error checking geom.T dimension: %v", err)
	}
	if currDim < dim {
		// Ignore lower dimension object produced by clipping at a shallower recursion level.
		return nil
	}
	if depth > maxDepth {
		err = col.Push(gt)
		if err != nil {
			return errors.Newf(geometryCollectionPushError, err)
		}
		return nil
	}
	nVertices, err := countVertices(gt)
	if err != nil {
		return errors.Newf("error counting vertices: %v", err)
	}
	if nVertices == 0 {
		return nil
	}
	if nVertices <= maxVertices {
		err = col.Push(gt)
		if err != nil {
			return errors.Newf(geometryCollectionPushError, err)
		}
		return nil
	}

	splitHorizontally := width > height
	splitPoint, err := calculateSplitPoint(gt, nVertices, splitHorizontally, clip)
	if err != nil {
		return err
	}
	subBox1 := *clip
	subBox2 := *clip
	if splitHorizontally {
		subBox1.HiX = splitPoint
		subBox2.LoX = splitPoint
	} else {
		subBox1.HiY = splitPoint
		subBox2.LoY = splitPoint
	}

	clipped1, err := clipGeomTByBoundingBox(gt, &subBox1)
	if err != nil {
		return errors.Newf("error clipping geom.T: %v", err)
	}
	clipped2, err := clipGeomTByBoundingBox(gt, &subBox2)
	if err != nil {
		return errors.Newf("error clipping geom.T: %v", err)
	}
	depth++
	err = subdivideRecursive(clipped1, maxVertices, depth, col, dim)
	if err != nil {
		return err
	}
	err = subdivideRecursive(clipped2, maxVertices, depth, col, dim)
	if err != nil {
		return err
	}
	return nil
}

// isGeomTCollection returns whether the given geom.T is of a collection type (wrapper for a geometry function).
func isGeomTCollection(gt geom.T) (bool, error) {
	g, err := geo.MakeGeometryFromGeomT(gt)
	if err != nil {
		return false, err
	}
	return IsCollection(g)
}

// forEachGeomTInCollection calls the provided function for each geometry in the passed in collection.
// Ignores MultiPoint input.
func forEachGeomTInCollection(gt geom.T, fn func(geom.T) error) error {
	switch gt := gt.(type) {
	case *geom.GeometryCollection:
		for i := 0; i < gt.NumGeoms(); i++ {
			err := fn(gt.Geom(i))
			if err != nil {
				return err
			}
		}
		return nil
	case *geom.MultiPolygon:
		for i := 0; i < gt.NumPolygons(); i++ {
			err := fn(gt.Polygon(i))
			if err != nil {
				return err
			}
		}
	case *geom.MultiLineString:
		for i := 0; i < gt.NumLineStrings(); i++ {
			err := fn(gt.LineString(i))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func calculateSplitPoint(
	gt geom.T, nVertices int, splitHorizontally bool, clip *geo.CartesianBoundingBox,
) (float64, error) {
	var pivot float64
	switch gt := gt.(type) {
	case *geom.Polygon:
		var err error
		pivot, err = findPolygonsMostCentralPointValue(gt, nVertices, splitHorizontally, clip)
		if err != nil {
			return 0, errors.Newf("error finding most central point for polygon: %v", err)
		}
		if splitHorizontally {
			// Ignore point on the boundaries
			if clip.LoX == pivot || clip.HiX == pivot {
				pivot = (clip.LoX + clip.HiX) / 2
			}
		} else {
			// Ignore point on the boundaries
			if clip.LoY == pivot || clip.HiY == pivot {
				pivot = (clip.LoY + clip.HiY) / 2
			}
		}
	default:
		if splitHorizontally {
			pivot = (clip.LoX + clip.HiX) / 2
		} else {
			pivot = (clip.LoY + clip.HiY) / 2
		}
	}
	return pivot, nil
}

// findPolygonsMostCentralPointValue finds the value of the most central point for a geom.Polygon.
// Can be X or Y value, depending on the splitHorizontally argument.
func findPolygonsMostCentralPointValue(
	poly *geom.Polygon, nVertices int, splitHorizontally bool, clip *geo.CartesianBoundingBox,
) (float64, error) {
	pivot := math.MaxFloat64
	ringToTrimIndex := 0
	ringArea := float64(0)
	pivotEps := math.MaxFloat64
	var ptEps float64
	nVerticesOuterRing, err := countVertices(poly.LinearRing(0))
	if err != nil {
		return 0, errors.Newf("error counting vertices: %v", err)
	}
	// if there are more points in holes than in outer ring
	if nVertices >= 2*nVerticesOuterRing {
		// trim holes starting from biggest
		for i := 1; i < poly.NumLinearRings(); i++ {
			currentRingArea := poly.LinearRing(i).Area()
			if currentRingArea >= ringArea {
				ringArea = currentRingArea
				ringToTrimIndex = i
			}
		}
	}
	pa := poly.LinearRing(ringToTrimIndex)
	// Find the most central point in the chosen ring
	for j := 0; j < pa.NumCoords(); j++ {
		var pt float64
		if splitHorizontally {
			pt = pa.Coord(j).X()
			xHalf := (clip.LoX + clip.HiX) / 2
			ptEps = math.Abs(pt - xHalf)
		} else {
			pt = pa.Coord(j).Y()
			yHalf := (clip.LoY + clip.HiY) / 2
			ptEps = math.Abs(pt - yHalf)
		}
		if ptEps < pivotEps {
			pivot = pt
			pivotEps = ptEps
		}
	}
	return pivot, nil
}

// clipGeomTByBoundingBox returns a result of intersection and simplification of the geom.T and bounding box provided.
// The result is also a geom.T.
func clipGeomTByBoundingBox(gt geom.T, clip *geo.CartesianBoundingBox) (geom.T, error) {
	g, err := geo.MakeGeometryFromGeomT(gt)
	if err != nil {
		return geom.NewGeometryCollection(), errors.Newf("error transforming geom.T to geometry: %v", err)
	}
	clipgt := clip.ToGeomT(g.SRID())
	clipg, err := geo.MakeGeometryFromGeomT(clipgt)
	if err != nil {
		return geom.NewGeometryCollection(), errors.Newf("error transforming geom.T to geometry: %v", err)
	}
	out, err := Intersection(g, clipg)
	if err != nil {
		return geom.NewGeometryCollection(), errors.Newf("error applying intersection: %v", err)
	}
	// Simplify is required to remove the unnecessary points. Otherwise vertices count is altered and too many subdivision may be returned.
	out, err = Simplify(out, 0)
	if err != nil {
		return geom.NewGeometryCollection(), errors.Newf("simplifying error: %v", err)
	}
	gt, err = out.AsGeomT()
	if err != nil {
		return geom.NewGeometryCollection(), errors.Newf("error transforming geometry to geom.T: %v", err)
	}
	return gt, nil
}

func countVertices(gt geom.T) (int, error) {
	if gt.Empty() {
		return 0, nil
	}
	switch gt := gt.(type) {
	case *geom.GeometryCollection:
		sum := 0
		for _, gti := range gt.Geoms() {
			v, err := countVertices(gti)
			if err != nil {
				return 0, err
			}
			sum += v
		}
		return sum, nil
	case *geom.LineString:
		return gt.NumCoords(), nil
	case *geom.MultiPoint:
		return gt.NumCoords(), nil
	case *geom.LinearRing:
		return gt.NumCoords(), nil
	case *geom.Point:
		return 1, nil
	case *geom.MultiLineString:
		sum := 0
		for i := 0; i < gt.NumLineStrings(); i++ {
			v, err := countVertices(gt.LineString(i))
			if err != nil {
				return 0, err
			}
			sum += v
		}
		return sum, nil
	case *geom.MultiPolygon:
		sum := 0
		for i := 0; i < gt.NumPolygons(); i++ {
			v, err := countVertices(gt.Polygon(i))
			if err != nil {
				return 0, err
			}
			sum += v
		}
		return sum, nil
	case *geom.Polygon:
		sum := 0
		for i := 0; i < gt.NumLinearRings(); i++ {
			v, err := countVertices(gt.LinearRing(i))
			if err != nil {
				return 0, err
			}
			sum += v
		}
		return sum, nil
	}
	return 0, errors.New("unsupported geom.T type")
}
