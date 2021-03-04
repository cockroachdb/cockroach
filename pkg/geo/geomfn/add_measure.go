// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

func AddMeasureToLineString(
	geometry geo.Geometry, start float64, end float64,
) (geo.Geometry, error) {
	t, err := geometry.AsGeomT()
	if err != nil {
		return geometry, err
	}

	switch t := t.(type) {
	case *geom.LineString: //, *geom.MultiLineString: TODO have a function that calls this multiple times
		// Add an M dimension to the linestring if it doesn't have one already.
		geometryWithM, err := ForceLayout(geometry, augmentLayoutWithM(t.Layout()))
		if err != nil {
			return geometry, nil
		}
		newT, err := geometryWithM.AsGeomT()
		if err != nil {
			return geometry, err
		}
		newLineString, ok := newT.(*geom.LineString)
		if !ok {
			return geometry, errors.Newf("ForceLayout unexpectedly transformed a LineString to a different geometry")
		}

		// Handle special case where all the measure values will be the same.
		if start == end {
			newT, err := applyOnCoordsForGeomT(newLineString, func(l geom.Layout, dst []float64, src []float64) error {
				copy(dst, src)
				dst[newLineString.Layout().MIndex()] = start
				return nil
			})
			if err != nil {
				return geometry, err
			}
			return geo.MakeGeometryFromGeomT(newT)
		}

		// Compute 2D length of linestring.
		lineLength, err := Length(geometry)
		if err != nil {
			return geometry, err
		}

		// Compute new coords slice.
		lineCoords := newLineString.Coords()
		if lineLength == 0 {
			for i := 0; i < newLineString.NumCoords(); i++ {
				lineCoords[i][newLineString.Layout().MIndex()] =
					start + (end-start)*float64(i)/float64(newLineString.NumCoords()-1)
			}
		} else {
			lengthSum := float64(0)
			prevPoint, err := geo.MakeGeometryFromGeomT(geom.NewPointFlat(geom.XY, lineCoords[0][:2]))
			if err != nil {
				return geometry, err
			}
			for i := 0; i < newLineString.NumCoords(); i++ {
				curPoint, err := geo.MakeGeometryFromGeomT(geom.NewPointFlat(geom.XY, lineCoords[i][:2]))
				if err != nil {
					return geometry, err
				}
				distBetweenPoints, err := MinDistance(prevPoint, curPoint)
				if err != nil {
					return geometry, err
				}
				lengthSum += distBetweenPoints
				lineCoords[i][newLineString.Layout().MIndex()] = start + (end-start)*lengthSum/lineLength
				prevPoint = curPoint
			}
		}
		newLineString.SetCoords(lineCoords)
		return geo.MakeGeometryFromGeomT(newLineString)
	default:
		// Ideally we should return NULL here, but following PostGIS on this.
		return geometry, errors.Newf("input geometry must be LINESTRING or MULTILINESTRING")
	}
}

func augmentLayoutWithM(layout geom.Layout) geom.Layout {
	switch layout {
	case geom.XY, geom.XYM:
		return geom.XYM
	case geom.XYZ, geom.XYZM:
		return geom.XYZM
	default:
		return layout
	}
}
