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

// AddMeasure takes a LineString or MultiLineString and linearly interpolates measure values for each line.
func AddMeasure(geometry geo.Geometry, start float64, end float64) (geo.Geometry, error) {
	t, err := geometry.AsGeomT()
	if err != nil {
		return geometry, err
	}

	switch t := t.(type) {
	case *geom.LineString:
		newLineString, err := addMeasureToLineString(t, start, end)
		if err != nil {
			return geometry, err
		}
		return geo.MakeGeometryFromGeomT(newLineString)
	case *geom.MultiLineString:
		newMultiLineString, err := addMeasureToMultiLineString(t, start, end)
		if err != nil {
			return geometry, err
		}
		return geo.MakeGeometryFromGeomT(newMultiLineString)
	default:
		// Ideally we should return NULL here, but following PostGIS on this.
		return geometry, errors.Newf("input geometry must be LINESTRING or MULTILINESTRING")
	}
}

// addMeasureToMultiLineString takes a MultiLineString and linearly interpolates measure values for each component line.
func addMeasureToMultiLineString(
	multiLineString *geom.MultiLineString, start float64, end float64,
) (*geom.MultiLineString, error) {
	newMultiLineString :=
		geom.NewMultiLineString(augmentLayoutWithM(multiLineString.Layout())).SetSRID(multiLineString.SRID())

	// Create a copy of the MultiLineString with measures added to each component LineString.
	for i := 0; i < multiLineString.NumLineStrings(); i++ {
		newLineString, err := addMeasureToLineString(multiLineString.LineString(i), start, end)
		if err != nil {
			return multiLineString, err
		}
		err = newMultiLineString.Push(newLineString)
		if err != nil {
			return multiLineString, err
		}
	}

	return newMultiLineString, nil
}

// addMeasureToLineString takes a LineString and linearly interpolates measure values.
func addMeasureToLineString(
	lineString *geom.LineString, start float64, end float64,
) (*geom.LineString, error) {
	newLineString := geom.NewLineString(augmentLayoutWithM(lineString.Layout())).SetSRID(lineString.SRID())

	if lineString.Empty() {
		return newLineString, nil
	}

	// Extract the line's current points.
	lineCoords := lineString.Coords()

	// Compute the length of the line as the sum of the distances between each pair of points.
	// Also, fill in pointMeasures with the partial sums.
	prevPoint := lineCoords[0]
	lineLength := float64(0)
	pointMeasures := make([]float64, lineString.NumCoords())
	for i := 0; i < lineString.NumCoords(); i++ {
		curPoint := lineCoords[i]
		distBetweenPoints := coordNorm(coordSub(prevPoint, curPoint))
		lineLength += distBetweenPoints
		pointMeasures[i] = lineLength
		prevPoint = curPoint
	}

	// Compute the measures for each point.
	for i := 0; i < lineString.NumCoords(); i++ {
		// Handle special case where line is zero length.
		if lineLength == 0 {
			pointMeasures[i] = start + (end-start)*(float64(i)/float64(lineString.NumCoords()-1))
		} else {
			pointMeasures[i] = start + (end-start)*(pointMeasures[i]/lineLength)
		}
	}

	// Replace M value if it exists, otherwise append it to each Coord.
	for i := 0; i < lineString.NumCoords(); i++ {
		if lineString.Layout().MIndex() == -1 {
			lineCoords[i] = append(lineCoords[i], pointMeasures[i])
		} else {
			lineCoords[i][lineString.Layout().MIndex()] = pointMeasures[i]
		}
	}

	// Create a new LineString with the measures tacked on.
	_, err := newLineString.SetCoords(lineCoords)
	if err != nil {
		return lineString, err
	}

	return newLineString, nil
}

// augmentLayoutWithM takes a layout and returns a layout with the M dimension added.
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
