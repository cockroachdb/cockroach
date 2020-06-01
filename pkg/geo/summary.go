// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geo

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// Summary returns a text summary of the contents of the geometry type.
//
// Flags shown square brackets after the geometry type have the following meaning:
// M: has M coordinate
// Z: has Z coordinate
// B: has a cached bounding box
// G: is geography
// S: has spatial reference system
func Summary(t geom.T, shape geopb.Shape, isGeography bool) (string, error) {
	return summary(t, shape, isGeography, 0)
}

func summary(
	t geom.T, shape geopb.Shape, isGeography bool, offset int,
) (summaryLine string, err error) {
	f, err := summaryFlag(t, isGeography)
	if err != nil {
		return "", err
	}

	summaryLine += strings.Repeat(" ", offset)
	switch t := t.(type) {
	case *geom.Point:
		return summaryLine + fmt.Sprintf("%s[%s]", shape.String(), f), nil
	case *geom.LineString:
		return summaryLine + fmt.Sprintf("%s[%s] with %d points", shape.String(), f, t.NumCoords()), nil
	case *geom.Polygon:
		numLinearRings := t.NumLinearRings()

		summaryLine += fmt.Sprintf("%s[%s] with %d ring", shape.String(), f, t.NumLinearRings())
		if numLinearRings > 1 {
			summaryLine += "s"
		}

		for i := 0; i < numLinearRings; i++ {
			ring := t.LinearRing(i)
			summaryLine += fmt.Sprintf("\n   ring %d has %d points", i, ring.NumCoords())
		}

		return summaryLine, nil
	case *geom.MultiPoint:
		numPoints := t.NumPoints()

		summaryLine += fmt.Sprintf("%s[%s] with %d element", shape.String(), f, numPoints)
		if 1 < numPoints {
			summaryLine += "s"
		}

		for i := 0; i < numPoints; i++ {
			point := t.Point(i)
			line, err := summary(point, geopb.Shape_Point, isGeography, offset+2)
			if err != nil {
				return "", err
			}

			summaryLine += "\n" + line
		}

		return summaryLine, nil
	case *geom.MultiLineString:
		numLineStrings := t.NumLineStrings()

		summaryLine += fmt.Sprintf("%s[%s] with %d element", shape.String(), f, numLineStrings)
		if 1 < numLineStrings {
			summaryLine += "s"
		}

		for i := 0; i < numLineStrings; i++ {
			lineString := t.LineString(i)
			line, err := summary(lineString, geopb.Shape_LineString, isGeography, offset+2)
			if err != nil {
				return "", err
			}

			summaryLine += "\n" + line
		}

		return summaryLine, nil
	case *geom.MultiPolygon:
		numPolygons := t.NumPolygons()

		summaryLine += fmt.Sprintf("%s[%s] with %d element", shape.String(), f, numPolygons)
		if 1 < numPolygons {
			summaryLine += "s"
		}

		for i := 0; i < numPolygons; i++ {
			polygon := t.Polygon(i)
			line, err := summary(polygon, geopb.Shape_Polygon, isGeography, offset+2)
			if err != nil {
				return "", err
			}

			summaryLine += "\n" + line
		}

		return summaryLine, nil
	case *geom.GeometryCollection:
		numGeoms := t.NumGeoms()

		summaryLine += fmt.Sprintf("%s[%s] with %d element", shape.String(), f, numGeoms)
		if 1 < numGeoms {
			summaryLine += "s"
		}

		for i := 0; i < numGeoms; i++ {
			g := t.Geom(i)
			gShape, err := geopbShape(g)
			if err != nil {
				return "", err
			}

			line, err := summary(g, gShape, isGeography, offset+2)
			if err != nil {
				return "", err
			}

			summaryLine += "\n" + line
		}

		return summaryLine, nil
	default:
		return "", errors.Newf("unsupported geom type: %T", t)
	}
}

func summaryFlag(t geom.T, isGeography bool) (f string, err error) {
	layout := t.Layout()
	if layout.MIndex() != -1 {
		f += "M"
	}

	if layout.ZIndex() != -1 {
		f += "Z"
	}

	bbox, err := BoundingBoxFromGeom(t)
	if err != nil {
		return "", err
	}

	if bbox != nil {
		f += "B"
	}

	if geopb.SRID(t.SRID()) != geopb.UnknownSRID {
		f += "S"
	}

	if isGeography {
		f += "G"
	}

	return f, nil
}
