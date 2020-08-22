// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geotransform

import (
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geoproj"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// Transform projects a given Geometry from a given Proj4Text to another Proj4Text.
func Transform(
	g geo.Geometry, from geoprojbase.Proj4Text, to geoprojbase.Proj4Text, newSRID geopb.SRID,
) (geo.Geometry, error) {
	if from.Equal(to) {
		return g.CloneWithSRID(newSRID)
	}

	t, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}

	newT, err := transform(t, from, to, newSRID)
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.MakeGeometryFromGeomT(newT)
}

// transform performs the projection operation on a geom.T object.
func transform(
	t geom.T, from geoprojbase.Proj4Text, to geoprojbase.Proj4Text, newSRID geopb.SRID,
) (geom.T, error) {
	switch t := t.(type) {
	case *geom.Point:
		newCoords, err := projectFlatCoords(t.FlatCoords(), t.Layout(), from, to)
		if err != nil {
			return nil, err
		}
		return geom.NewPointFlat(t.Layout(), newCoords).SetSRID(int(newSRID)), nil
	case *geom.LineString:
		newCoords, err := projectFlatCoords(t.FlatCoords(), t.Layout(), from, to)
		if err != nil {
			return nil, err
		}
		return geom.NewLineStringFlat(t.Layout(), newCoords).SetSRID(int(newSRID)), nil
	case *geom.Polygon:
		newCoords, err := projectFlatCoords(t.FlatCoords(), t.Layout(), from, to)
		if err != nil {
			return nil, err
		}
		return geom.NewPolygonFlat(t.Layout(), newCoords, t.Ends()).SetSRID(int(newSRID)), nil
	case *geom.MultiPoint:
		newCoords, err := projectFlatCoords(t.FlatCoords(), t.Layout(), from, to)
		if err != nil {
			return nil, err
		}
		return geom.NewMultiPointFlat(t.Layout(), newCoords).SetSRID(int(newSRID)), nil
	case *geom.MultiLineString:
		newCoords, err := projectFlatCoords(t.FlatCoords(), t.Layout(), from, to)
		if err != nil {
			return nil, err
		}
		return geom.NewMultiLineStringFlat(t.Layout(), newCoords, t.Ends()).SetSRID(int(newSRID)), nil
	case *geom.MultiPolygon:
		newCoords, err := projectFlatCoords(t.FlatCoords(), t.Layout(), from, to)
		if err != nil {
			return nil, err
		}
		return geom.NewMultiPolygonFlat(t.Layout(), newCoords, t.Endss()).SetSRID(int(newSRID)), nil
	case *geom.GeometryCollection:
		g := geom.NewGeometryCollection().SetSRID(int(newSRID))
		for _, subG := range t.Geoms() {
			subGeom, err := transform(subG, from, to, 0)
			if err != nil {
				return nil, err
			}
			if err := g.Push(subGeom); err != nil {
				return nil, err
			}
		}
		return g, nil
	default:
		return nil, errors.Newf("unhandled type; %T", t)
	}
}

// projectFlatCoords projects a given flatCoords array and returns an array with the projected
// coordinates.
// Note M coordinates are not touched.
func projectFlatCoords(
	flatCoords []float64, layout geom.Layout, from geoprojbase.Proj4Text, to geoprojbase.Proj4Text,
) ([]float64, error) {
	numCoords := len(flatCoords) / layout.Stride()
	// Allocate the map once and partition the arrays to store xCoords, yCoords and zCoords in order.
	coords := make([]float64, numCoords*3)
	xCoords := coords[numCoords*0 : numCoords*1]
	yCoords := coords[numCoords*1 : numCoords*2]
	zCoords := coords[numCoords*2 : numCoords*3]
	for i := 0; i < numCoords; i++ {
		base := i * layout.Stride()
		xCoords[i] = flatCoords[base+0]
		yCoords[i] = flatCoords[base+1]
		// If ZIndex is != -1, it is 2 and forms part of our z coords.
		if layout.ZIndex() != -1 {
			zCoords[i] = flatCoords[base+layout.ZIndex()]
		}
	}

	if err := geoproj.Project(from, to, xCoords, yCoords, zCoords); err != nil {
		return nil, err
	}
	newCoords := make([]float64, numCoords*layout.Stride())
	for i := 0; i < numCoords; i++ {
		base := i * layout.Stride()
		newCoords[base+0] = xCoords[i]
		newCoords[base+1] = yCoords[i]

		if layout.ZIndex() != -1 {
			newCoords[base+layout.ZIndex()] = zCoords[i]
		}
		if layout.MIndex() != -1 {
			newCoords[base+layout.MIndex()] = flatCoords[i*layout.Stride()+layout.MIndex()]
		}
	}

	return newCoords, nil
}
