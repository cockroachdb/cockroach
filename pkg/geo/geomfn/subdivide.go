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
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// Subdivide returns a geometry divided into parts, where each part contains no more than the number of vertices provided.
func Subdivide(g geo.Geometry, maxVertices int) ([]geo.Geometry, error) {
	if g.Empty() {
		return []geo.Geometry{g}, nil
	}
	const minMaxVertices = 5
	if maxVertices < minMaxVertices {
		return []geo.Geometry{}, errors.Newf("max_vertices number cannot be less than %v", minMaxVertices)
	}

	gt, err := g.AsGeomT()
	if err != nil {
		return []geo.Geometry{}, errors.Newf("could not transform input geometry into geom.T: %v", err)
	}
	geomTs := []geom.T{}
	dim, err := dimensionFromGeomT(gt)
	if err != nil {
		return []geo.Geometry{}, errors.Newf("could not calculate geometry dimension: %v", err)
	}
	// maxDepth 50 => 2^50 ~= 10^15 subdivisions.
	const maxDepth = 50
	const startDepth = 0
	if err = subdivideRecursive(gt, maxVertices, startDepth, dim, maxDepth, &geomTs); err != nil {
		return []geo.Geometry{}, err
	}

	output := []geo.Geometry{}
	for _, cg := range geomTs {
		g, err := geo.MakeGeometryFromGeomT(cg)
		if err != nil {
			return []geo.Geometry{}, errors.Newf("could not transform output geom.T into geometry: %v", err)
		}
		g, err = g.CloneWithSRID(geopb.SRID(gt.SRID()))
		if err != nil {
			return []geo.Geometry{}, errors.Newf("could not clone and set SRID on geometry: %v", err)
		}
		output = append(output, g)
	}
	return output, nil
}

// subdivideRecursive performs the recursive subdivision on the gt provided.
// Subdivided geom.T's that have less vertices than maxVertices are added to the results.
// depth is compared with maxDepth, so that maximum number of subdivisions is never exceeded.
// If gt passed is a GeometryCollection, some of the types may be shallower than the others.
// We need dim to make sure that those geometries are ignored.
func subdivideRecursive(
	gt geom.T, maxVertices int, depth int, dim int, maxDepth int, results *[]geom.T,
) error {
	clip := geo.BoundingBoxFromGeomTGeometryType(gt)
	width := clip.HiX - clip.LoX
	height := clip.HiY - clip.LoY
	if width == 0 && height == 0 {
		// Point is a special case, because actual dimension is checked later in the code
		if gt, ok := gt.(*geom.Point); ok && dim == 0 {
			*results = append(*results, gt)
		}
		return nil
	}
	// Boundaries needs to be widen slightly for intersection to work properly.
	const tolerance = 1e-12
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
		err := forEachGeomTInCollectionForSubdivide(gt, func(gi geom.T) error {
			// It's not a subdivision yet, so depth stays the same
			return subdivideRecursive(gi, maxVertices, depth, dim, maxDepth, results)
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
		*results = append(*results, gt)
		return nil
	}
	nVertices := CountVertices(gt)
	if nVertices == 0 {
		return nil
	}
	if nVertices <= maxVertices {
		*results = append(*results, gt)
		return nil
	}

	splitHorizontally := width > height
	splitPoint, err := calculateSplitPointCoordForSubdivide(gt, nVertices, splitHorizontally, *clip)
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

	clipped1, err := clipGeomTByBoundingBoxForSubdivide(gt, &subBox1)
	if err != nil {
		return errors.Newf("error clipping geom.T: %v", err)
	}
	clipped2, err := clipGeomTByBoundingBoxForSubdivide(gt, &subBox2)
	if err != nil {
		return errors.Newf("error clipping geom.T: %v", err)
	}
	depth++
	if err = subdivideRecursive(clipped1, maxVertices, depth, dim, maxDepth, results); err != nil {
		return err
	}
	if err = subdivideRecursive(clipped2, maxVertices, depth, dim, maxDepth, results); err != nil {
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

// forEachGeomTInCollectionForSubdivide calls the provided function for each geometry in the passed in collection.
// Ignores MultiPoint input.
func forEachGeomTInCollectionForSubdivide(gt geom.T, fn func(geom.T) error) error {
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

// calculateSplitPointCoordForSubdivide calculates the coords that can be used for splitting
// the geom.T provided and returns their X or Y value, depending on the splitHorizontally argument.
// These coords will be coords of the closest point to the center in case of a Polygon
// and simply the center coords in case of any other geometry type or
// closest point to the center being on the boundaries.
// Specialized to be used by the Subdivide function.
func calculateSplitPointCoordForSubdivide(
	gt geom.T, nVertices int, splitHorizontally bool, clip geo.CartesianBoundingBox,
) (float64, error) {
	var pivot float64
	switch gt := gt.(type) {
	case *geom.Polygon:
		var err error
		pivot, err = findMostCentralPointValueForPolygon(gt, nVertices, splitHorizontally, &clip)
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

// findMostCentralPointValueForPolygon finds the value of the most central point for a geom.Polygon.
// Can be X or Y value, depending on the splitHorizontally argument.
func findMostCentralPointValueForPolygon(
	poly *geom.Polygon, nVertices int, splitHorizontally bool, clip *geo.CartesianBoundingBox,
) (float64, error) {
	pivot := math.MaxFloat64
	ringToTrimIndex := 0
	ringArea := float64(0)
	pivotEps := math.MaxFloat64
	var ptEps float64
	nVerticesOuterRing := CountVertices(poly.LinearRing(0))
	if nVertices >= 2*nVerticesOuterRing {
		// When there are more points in holes than in outer ring, trim holes starting from biggest.
		for i := 1; i < poly.NumLinearRings(); i++ {
			currentRingArea := poly.LinearRing(i).Area()
			if currentRingArea >= ringArea {
				ringArea = currentRingArea
				ringToTrimIndex = i
			}
		}
	}
	pa := poly.LinearRing(ringToTrimIndex)
	// Find the most central point in the chosen ring.
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

// clipGeomTByBoundingBoxForSubdivide returns a result of intersection and simplification of the geom.T and bounding box provided.
// The result is also a geom.T.
// Specialized to be used in the Subdivide function.
func clipGeomTByBoundingBoxForSubdivide(gt geom.T, clip *geo.CartesianBoundingBox) (geom.T, error) {
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
