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
func Summary(t geom.T, isGeography bool, offset int) (summary string, err error) {
	shape, err := geopbShape(t)
	if err != nil {
		return "", err
	}

	f, err := flag(t, isGeography)
	if err != nil {
		return "", err
	}

	summary += strings.Repeat(" ", offset)
	switch t := t.(type) {
	case *geom.Point:
		return summary + fmt.Sprintf("%s[%s]", shape.String(), f), nil
	case *geom.LineString:
		return summary + fmt.Sprintf("%s[%s] with %d points", shape.String(), f, t.NumCoords()), nil
	case *geom.Polygon:
		numLinearRings := t.NumLinearRings()

		summary += fmt.Sprintf("%s[%s] with %d ring", shape.String(), f, t.NumLinearRings())
		if 1 < numLinearRings {
			summary += "s"
		}

		for i := 0; i < numLinearRings; i++ {
			ring := t.LinearRing(i)
			summary += fmt.Sprintf("\n   ring %d has %d points", i, ring.NumCoords())
		}

		return summary, nil
	case *geom.MultiPoint:
		numPoints := t.NumPoints()

		summary += fmt.Sprintf("%s[%s] with %d element", shape.String(), f, numPoints)
		if 1 < numPoints {
			summary += "s"
		}

		for i := 0; i < numPoints; i++ {
			point := t.Point(i)
			tmp, err := Summary(point, isGeography, offset+2)
			if err != nil {
				return "", err
			}

			summary += "\n" + tmp
		}

		return summary, nil
	case *geom.MultiLineString:
		numLineStrings := t.NumLineStrings()

		summary += fmt.Sprintf("%s[%s] with %d element", shape.String(), f, numLineStrings)
		if 1 < numLineStrings {
			summary += "s"
		}

		for i := 0; i < numLineStrings; i++ {
			lineString := t.LineString(i)
			tmp, err := Summary(lineString, isGeography, offset+2)
			if err != nil {
				return "", err
			}

			summary += "\n" + tmp
		}

		return summary, nil
	case *geom.MultiPolygon:
		numPolygons := t.NumPolygons()

		summary += fmt.Sprintf("%s[%s] with %d element", shape.String(), f, numPolygons)
		if 1 < numPolygons {
			summary += "s"
		}

		for i := 0; i < numPolygons; i++ {
			polygon := t.Polygon(i)
			tmp, err := Summary(polygon, isGeography, offset+2)
			if err != nil {
				return "", err
			}

			summary += "\n" + tmp
		}

		return summary, nil
	case *geom.GeometryCollection:
		numGeoms := t.NumGeoms()

		summary += fmt.Sprintf("%s[%s] with %d element", shape.String(), f, numGeoms)
		if 1 < numGeoms {
			summary += "s"
		}

		for i := 0; i < numGeoms; i++ {
			g := t.Geom(i)
			tmp, err := Summary(g, isGeography, offset+2)
			if err != nil {
				return "", err
			}

			summary += "\n" + tmp
		}

		return summary, nil
	default:
		return "", errors.Newf("unspport geom type: %T", t)
	}
}

func flag(t geom.T, isGeography bool) (f string, err error) {
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
