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
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// GenerateRandomPoints generates provided number of pseudo-random points for the input area.
func GenerateRandomPoints(g geo.Geometry, nPoints int, rng *rand.Rand) (geo.Geometry, error) {
	if nPoints < 0 {
		return geo.Geometry{}, nil
	}
	pointsAsGeometry, err := generateRandomPoints(g, nPoints, rng)
	if err != nil {
		return geo.Geometry{}, errors.Newf("generating random points error: %v", err)
	}
	return pointsAsGeometry, nil
}

// generateRandomPoints returns a MultiPoint geometry consisting of randomly generated points
// that are covered by the geometry provided.
// nPoints is the number of points to return.
// rng is the random numbers generator.
func generateRandomPoints(g geo.Geometry, nPoints int, rng *rand.Rand) (geo.Geometry, error) {
	var generateRandomPointsFunction func(g geo.Geometry, nPoints int, rng *rand.Rand) (*geom.MultiPoint, error)
	switch g.ShapeType() {
	case geopb.ShapeType_Polygon:
		generateRandomPointsFunction = generateRandomPointsFromPolygon
	case geopb.ShapeType_MultiPolygon:
		generateRandomPointsFunction = generateRandomPointsFromMultiPolygon
	default:
		return geo.Geometry{}, errors.Newf("unsupported type: %v", g.ShapeType().String())
	}
	// This is to be checked once we know Geometry type is supported,
	// so that we can keep consistency with PostGIS implementation.
	if nPoints == 0 {
		return geo.Geometry{}, nil
	}
	empty, err := IsEmpty(g)
	if err != nil {
		return geo.Geometry{}, errors.Newf("could not check if geometry is empty: %v", err)
	}
	if empty {
		return geo.Geometry{}, nil
	}
	mpt, err := generateRandomPointsFunction(g, nPoints, rng)
	if err != nil {
		return geo.Geometry{}, err
	}
	srid := g.SRID()
	mpt.SetSRID(int(srid))
	out, err := geo.MakeGeometryFromGeomT(mpt)
	if err != nil {
		return geo.Geometry{}, errors.Newf("could not transform geom.T into geometry: %v", err)
	}
	return out, nil
}

func generateRandomPointsFromPolygon(
	g geo.Geometry, nPoints int, rng *rand.Rand,
) (*geom.MultiPoint, error) {
	area, err := Area(g)
	if err != nil {
		return nil, errors.Newf("could not calculate Polygon area: %v", err)
	}
	if area == 0.0 {
		return nil, errors.New("zero area input Polygon")
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
	// Prepare the polygon for fast true/false testing.
	gPrep, err := geos.PrepareGeometry(g.EWKB())
	if err != nil {
		return nil, errors.Newf("could not prepare geometry: %v", err)
	}
	res, err := func() (*geom.MultiPoint, error) {
		// Generate a slice of points - for every cell on a grid store coordinates.
		n := sampleHeight * sampleWidth
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
		nPointsGenerated := 0
		iterations := 0
		for nPointsGenerated < nPoints {
			iterations++
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
					return nil, errors.Newf("could not create geometry Point: %v", err)
				}
				intersects, err := geos.PreparedIntersects(gPrep, gpt.EWKB())
				if err != nil {
					return nil, errors.Newf("could not check prepared intersection: %v", err)
				}
				if intersects {
					nPointsGenerated++
					p := geom.NewPointFlat(geom.XY, []float64{x, y})
					srid := g.SRID()
					p.SetSRID(int(srid))
					err = results.Push(p)
					if err != nil {
						return nil, errors.Newf("could not add point to the results: %v", err)
					}
					if nPointsGenerated == nPoints {
						break
					}
				}
			}
			if nPointsGenerated == nPoints || iterations > 100 {
				break
			}
		}
		return results, nil
	}()
	if err != nil {
		destroyErr := geos.PreparedGeomDestroy(gPrep)
		return nil, errors.CombineErrors(destroyErr, err)
	}
	return res, nil
}

func generateRandomPointsFromMultiPolygon(
	g geo.Geometry, nPoints int, rng *rand.Rand,
) (*geom.MultiPoint, error) {
	results := geom.NewMultiPoint(geom.XY)

	area, err := Area(g)
	if err != nil {
		return nil, errors.Newf("could not calculate MultiPolygon area: %v", err)
	}

	gt, err := g.AsGeomT()
	if err != nil {
		return nil, errors.Newf("could not transform MultiPolygon into geom.T: %v", err)
	}

	gmp := gt.(*geom.MultiPolygon)
	for i := 0; i < gmp.NumPolygons(); i++ {
		poly := gmp.Polygon(i)
		subarea := poly.Area()
		subNPoints := int(math.Round(float64(nPoints) * subarea / area))
		if subNPoints > 0 {
			g, err := geo.MakeGeometryFromGeomT(poly)
			if err != nil {
				return nil, errors.Newf("could not transform geom.T into Geometry: %v", err)
			}
			subMPT, err := generateRandomPointsFromPolygon(g, subNPoints, rng)
			if err != nil {
				return nil, errors.Newf("error generating points for Polygon: %v", err)
			}
			for j := 0; j < subMPT.NumPoints(); j++ {
				if err := results.Push(subMPT.Point(j)); err != nil {
					return nil, errors.Newf("could not push point to the results: %v", err)
				}
			}
		}
	}
	return results, nil
}
