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

// GeneratePoints generates provided number of pseudo-random points for the input area.
func GeneratePoints(g geo.Geometry, npoints int, seed int64) (geo.Geometry, error) {
	if npoints < 0 {
		return geo.Geometry{}, nil
	}
	if seed < 1 {
		return geo.Geometry{}, errors.New("seed must be greater than zero")
	}
	pointsGeometry, err := geometryToPoints(g, npoints, seed)
	if err != nil {
		return geo.Geometry{}, errors.Newf("generating random points error: %v", err)
	}
	return pointsGeometry, nil
}

// geometryToPoints returns a MultiPoint geometry consisting of randomly generated points
// that are covered by the geometry provided.
// npoints is the number of points to return.
// seed is the seed used by the random numbers generator.
func geometryToPoints(g geo.Geometry, npoints int, seed int64) (geo.Geometry, error) {
	var generatePointsFunction func(g geo.Geometry, npoints int, seed int64) (*geom.MultiPoint, error)
	switch g.ShapeType() {
	case geopb.ShapeType_Polygon:
		generatePointsFunction = polygonGeometryToPoints
	case geopb.ShapeType_MultiPolygon:
		generatePointsFunction = multipolygonGeometryToPoints
	default:
		return geo.Geometry{}, errors.New("unsupported type")
	}
	// This is to be checked once we know Geometry type is supported,
	// so that we can keep consistency with PostGIS implementation.
	if npoints == 0 {
		return geo.Geometry{}, nil
	}
	empty, err := IsEmpty(g)
	if err != nil {
		return geo.Geometry{}, errors.Newf("could not check if Geometry is empty: %v", err)
	}
	if empty {
		return geo.Geometry{}, nil
	}
	mpt, err := generatePointsFunction(g, npoints, seed)
	if err != nil {
		return geo.Geometry{}, err
	}
	srid := g.SRID()
	mpt.SetSRID(int(srid))
	out, err := geo.MakeGeometryFromGeomT(mpt)
	if err != nil {
		return geo.Geometry{}, errors.Newf("could not transform geom.T into Geometry: %v", err)
	}
	return out, nil
}

func polygonGeometryToPoints(g geo.Geometry, npoints int, seed int64) (*geom.MultiPoint, error) {
	area, err := Area(g)
	if err != nil {
		return nil, errors.Newf("could not calculate Polygon area: %v", err)
	}
	bbox := g.CartesianBoundingBox()
	bboxWidth := bbox.HiX - bbox.LoX
	bboxHeight := bbox.HiY - bbox.LoY
	bboxArea := bboxHeight * bboxWidth
	if area == 0.0 || bboxArea == 0.0 {
		return nil, errors.New("zero area input Polygon")
	}
	// Gross up our test set a bit to increase odds of getting coverage in one pass.
	sampleNpoints := float64(npoints) * bboxArea / area

	sampleSqrt := math.Round(math.Sqrt(sampleNpoints))
	if sampleSqrt == 0 {
		sampleSqrt = 1
	}
	var sampleHeight, sampleWidth int
	var sampleCellSize float64
	// Calculate the grids we're going to randomize within.
	if bboxWidth > bboxHeight {
		sampleWidth = int(sampleSqrt)
		sampleHeight = int(math.Ceil(sampleNpoints / float64(sampleWidth)))
		sampleCellSize = bboxWidth / float64(sampleWidth)
	} else {
		sampleHeight = int(sampleSqrt)
		sampleWidth = int(math.Ceil(sampleNpoints / float64(sampleHeight)))
		sampleCellSize = bboxHeight / float64(sampleHeight)
	}
	// Prepare the polygon for fast true/false testing.
	gprep, err := geos.PrepareGeometry(g.EWKB())
	if err != nil {
		return nil, errors.Newf("could not prepare Geometry: %v", err)
	}
	defer func() {
		err = geos.PreparedGeomDestroy(gprep)
	}()

	// Initiate random number generator.
	randomNumberGenerator := rand.New(rand.NewSource(seed))
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
		randomNumberGenerator.Shuffle(n, func(i int, j int) {
			temp := cells[j]
			cells[j] = cells[i]
			cells[i] = temp
		})
	}
	results := geom.NewMultiPoint(geom.XY)
	// Generate points and test them.
	npointsGenerated := 0
	iterations := 0
	for npointsGenerated < npoints {
		iterations++
		for _, cell := range cells {
			y := bbox.LoY + cell.X()*sampleCellSize
			x := bbox.LoX + cell.Y()*sampleCellSize
			x += randomNumberGenerator.Float64() * sampleCellSize
			y += randomNumberGenerator.Float64() * sampleCellSize
			if x > bbox.HiX || y > bbox.HiY {
				continue
			}
			gseq, err := geos.CoordSequenceCreate(1, 2)
			if err != nil {
				return nil, errors.Newf("could not create coord sequence: %v", err)
			}
			gseq, err = geos.CoordSequenceSetXY(gseq, 0, x, y)
			if err != nil {
				return nil, errors.Newf("could not set X and Y for coord sequence: %v", err)
			}
			defer func() {
				// err = geos.CoordSequenceDestroy(gseq)
			}()
			gpt, err := geos.GeometryPointCreate(gseq)
			if err != nil {
				return nil, errors.Newf("could not create Geometry Point: %v", err)
			}
			intersects, err := geos.PreparedIntersects(gprep, gpt)
			if err != nil {
				return nil, errors.Newf("could not check prepared intersection: %v", err)
			}
			if intersects {
				npointsGenerated++
				p := geom.NewPointFlat(geom.XY, []float64{x, y})
				srid := g.SRID()
				p.SetSRID(int(srid))
				err = results.Push(p)
				if err != nil {
					return nil, errors.Newf("could not add point to the results: %v", err)
				}
				if npointsGenerated == npoints {
					break
				}
			}
		}
		if npointsGenerated == npoints || iterations > 100 {
			break
		}
	}
	return results, nil
}

func multipolygonGeometryToPoints(
	g geo.Geometry, npoints int, seed int64,
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
		subNpoints := int(math.Round(float64(npoints) * subarea / area))
		if subNpoints > 0 {
			g, err := geo.MakeGeometryFromGeomT(poly)
			if err != nil {
				return nil, errors.Newf("could not transform geom.T into Geometry: %v", err)
			}
			subMpt, err := polygonGeometryToPoints(g, subNpoints, seed)
			if err != nil {
				return nil, errors.Newf("error generating points for Polygon: %v", err)
			}
			for j := 0; j < subMpt.NumPoints(); j++ {
				if err := results.Push(subMpt.Point(j)); err != nil {
					return nil, errors.Newf("could not push point to the results: %v", err)
				}
			}
		}
	}
	return results, nil
}
