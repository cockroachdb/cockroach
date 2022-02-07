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
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// maxAllowedGridSize is the upper bound limit for a generated grid size.
const maxAllowedGridSize = 100 * geo.MaxAllowedSplitPoints

// ErrGenerateRandomPointsInvalidPoints is returned if we have a negative number of points
// or an empty geometry.
var ErrGenerateRandomPointsInvalidPoints = pgerror.Newf(
	pgcode.InvalidParameterValue,
	"points must be positive and geometry must not be empty",
)

// GenerateRandomPoints generates provided number of pseudo-random points for the input area.
func GenerateRandomPoints(g geo.Geometry, nPoints int, rng *rand.Rand) (geo.Geometry, error) {
	var generateRandomPointsFunction func(g geo.Geometry, nPoints int, rng *rand.Rand) (*geom.MultiPoint, error)
	switch g.ShapeType() {
	case geopb.ShapeType_Polygon:
		generateRandomPointsFunction = generateRandomPointsFromPolygon
	case geopb.ShapeType_MultiPolygon:
		generateRandomPointsFunction = generateRandomPointsFromMultiPolygon
	default:
		return geo.Geometry{}, pgerror.Newf(pgcode.InvalidParameterValue, "unsupported type: %v", g.ShapeType().String())
	}
	if nPoints <= 0 {
		return geo.Geometry{}, ErrGenerateRandomPointsInvalidPoints
	}
	if nPoints > geo.MaxAllowedSplitPoints {
		return geo.Geometry{}, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"failed to generate random points, too many points to generate: requires %d points, max %d",
			nPoints,
			geo.MaxAllowedSplitPoints,
		)
	}
	empty, err := IsEmpty(g)
	if err != nil {
		return geo.Geometry{}, errors.Wrap(err, "could not check if geometry is empty")
	}
	if empty {
		return geo.Geometry{}, ErrGenerateRandomPointsInvalidPoints
	}
	mpt, err := generateRandomPointsFunction(g, nPoints, rng)
	if err != nil {
		return geo.Geometry{}, err
	}
	srid := g.SRID()
	mpt.SetSRID(int(srid))
	out, err := geo.MakeGeometryFromGeomT(mpt)
	if err != nil {
		return geo.Geometry{}, errors.Wrap(err, "could not transform geom.T into geometry")
	}
	return out, nil
}

// generateRandomPointsFromPolygon returns a given number of randomly generated points that are within the Polygon provided.
// In order to get more uniformed number of points, it's generating a grid, and then generating a random point within each grid cell.
// Read more: http://lin-ear-th-inking.blogspot.com/2010/05/more-random-points-in-jts.html.
// Rough description of the algorithm:
// 1. Generate the grid cells. The higher number of points to generate and bounding box area/polygon area ratio, the smaller cell size.
// 2. Shuffle the array of grid cells indexes. This way the order in which grid cells are chosen for point generation is random.
// 3. For each grid cell, generate a random point within it. If the point intersects with our geometry, add it to the results.
// 4. When there are enough points in the results, stop. Otherwise go to step 3.
func generateRandomPointsFromPolygon(
	g geo.Geometry, nPoints int, rng *rand.Rand,
) (*geom.MultiPoint, error) {
	area, err := Area(g)
	if err != nil {
		return nil, errors.Wrap(err, "could not calculate Polygon area")
	}
	if area == 0.0 {
		return nil, pgerror.Newf(pgcode.InvalidParameterValue, "zero area input Polygon")
	}
	bbox := g.CartesianBoundingBox()
	bboxWidth := bbox.HiX - bbox.LoX
	bboxHeight := bbox.HiY - bbox.LoY
	bboxArea := bboxHeight * bboxWidth
	// Gross up our test set a bit to increase odds of getting coverage in one pass.
	sampleNPoints := float64(nPoints) * bboxArea / area

	sampleSqrt := math.Round(math.Sqrt(sampleNPoints))
	if sampleSqrt == 0 {
		sampleSqrt = 1
	}
	var sampleHeight, sampleWidth int
	var sampleCellSize float64
	// Calculate the grids we're going to randomize within.
	if bboxWidth > bboxHeight {
		sampleWidth = int(sampleSqrt)
		sampleHeight = int(math.Ceil(sampleNPoints / float64(sampleWidth)))
		sampleCellSize = bboxWidth / float64(sampleWidth)
	} else {
		sampleHeight = int(sampleSqrt)
		sampleWidth = int(math.Ceil(sampleNPoints / float64(sampleHeight)))
		sampleCellSize = bboxHeight / float64(sampleHeight)
	}
	n := sampleHeight * sampleWidth
	if n > maxAllowedGridSize {
		return nil, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"generated area is too large: %d, max %d",
			n,
			maxAllowedGridSize,
		)
	}

	// Prepare the polygon for fast true/false testing.
	gPrep, err := geos.PrepareGeometry(g.EWKB())
	if err != nil {
		return nil, errors.Wrap(err, "could not prepare geometry")
	}
	defer geos.PreparedGeomDestroy(gPrep)

	// Generate a slice of points - for every cell on a grid store coordinates.
	cells := make([]geom.Coord, n)
	for i := 0; i < sampleWidth; i++ {
		for j := 0; j < sampleHeight; j++ {
			cells[i*sampleHeight+j] = geom.Coord{float64(i), float64(j)}
		}
	}
	// Shuffle the points. Without shuffling, the generated point will
	// always be adjacent to the previous one (in terms of grid cells).
	if n > 1 {
		rng.Shuffle(n, func(i int, j int) {
			temp := cells[j]
			cells[j] = cells[i]
			cells[i] = temp
		})
	}
	results := geom.NewMultiPoint(geom.XY)
	// Generate points and test them.
	for nPointsGenerated, iterations := 0, 0; nPointsGenerated < nPoints && iterations <= 100; iterations++ {
		for _, cell := range cells {
			y := bbox.LoY + cell.X()*sampleCellSize
			x := bbox.LoX + cell.Y()*sampleCellSize
			x += rng.Float64() * sampleCellSize
			y += rng.Float64() * sampleCellSize
			if x > bbox.HiX || y > bbox.HiY {
				continue
			}
			gpt, err := geo.MakeGeometryFromPointCoords(x, y)
			if err != nil {
				return nil, errors.Wrap(err, "could not create geometry Point")
			}
			intersects, err := geos.PreparedIntersects(gPrep, gpt.EWKB())
			if err != nil {
				return nil, errors.Wrap(err, "could not check prepared intersection")
			}
			if intersects {
				nPointsGenerated++
				p := geom.NewPointFlat(geom.XY, []float64{x, y})
				srid := g.SRID()
				p.SetSRID(int(srid))
				err = results.Push(p)
				if err != nil {
					return nil, errors.Wrap(err, "could not add point to the results")
				}
				if nPointsGenerated == nPoints {
					return results, nil
				}
			}
		}
	}
	return results, nil
}

func generateRandomPointsFromMultiPolygon(
	g geo.Geometry, nPoints int, rng *rand.Rand,
) (*geom.MultiPoint, error) {
	results := geom.NewMultiPoint(geom.XY)

	area, err := Area(g)
	if err != nil {
		return nil, errors.Wrap(err, "could not calculate MultiPolygon area")
	}

	gt, err := g.AsGeomT()
	if err != nil {
		return nil, errors.Wrap(err, "could not transform MultiPolygon into geom.T")
	}

	gmp := gt.(*geom.MultiPolygon)
	for i := 0; i < gmp.NumPolygons(); i++ {
		poly := gmp.Polygon(i)
		subarea := poly.Area()
		subNPoints := int(math.Round(float64(nPoints) * subarea / area))
		if subNPoints > 0 {
			g, err := geo.MakeGeometryFromGeomT(poly)
			if err != nil {
				return nil, errors.Wrap(err, "could not transform geom.T into Geometry")
			}
			subMPT, err := generateRandomPointsFromPolygon(g, subNPoints, rng)
			if err != nil {
				return nil, errors.Wrap(err, "error generating points for Polygon")
			}
			for j := 0; j < subMPT.NumPoints(); j++ {
				if err := results.Push(subMPT.Point(j)); err != nil {
					return nil, errors.Wrap(err, "could not push point to the results")
				}
			}
		}
	}
	return results, nil
}
