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
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/twpayne/go-geom"
)

// ForceLayout forces a geometry into the given layout.
// If a Z dimension is added, 0 is added as the Z value.
// If an M dimension is added, 0 is added as the M value.
func ForceLayout(g geo.Geometry, layout geom.Layout) (geo.Geometry, error) {
	return ForceLayoutWithDefaultZM(g, layout, 0, 0)
}

// ForceLayoutWithDefaultZ forces a geometry into the given layout.
// If a Z dimension is added, defaultZ is added as the Z value.
// If an M dimension is added, 0 is added as the M value.
func ForceLayoutWithDefaultZ(
	g geo.Geometry, layout geom.Layout, defaultZ float64,
) (geo.Geometry, error) {
	return ForceLayoutWithDefaultZM(g, layout, defaultZ, 0)
}

// ForceLayoutWithDefaultM forces a geometry into the given layout.
// If a Z dimension is added, 0 is added as the Z value.
// If an M dimension is added, defaultM is added as the M value.
func ForceLayoutWithDefaultM(
	g geo.Geometry, layout geom.Layout, defaultM float64,
) (geo.Geometry, error) {
	return ForceLayoutWithDefaultZM(g, layout, 0, defaultM)
}

// ForceLayoutWithDefaultZM forces a geometry into the given layout.
// If a Z dimension is added, defaultZ is added as the Z value.
// If an M dimension is added, defaultM is added as the M value.
func ForceLayoutWithDefaultZM(
	g geo.Geometry, layout geom.Layout, defaultZ float64, defaultM float64,
) (geo.Geometry, error) {
	geomT, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}
	retGeomT, err := forceLayout(geomT, layout, defaultZ, defaultM)
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.MakeGeometryFromGeomT(retGeomT)
}

// forceLayout forces a geom.T into the given layout and uses
// the specified default Z and M values as needed.
func forceLayout(t geom.T, layout geom.Layout, defaultZ float64, defaultM float64) (geom.T, error) {
	if t.Layout() == layout {
		return t, nil
	}
	switch t := t.(type) {
	case *geom.GeometryCollection:
		ret := geom.NewGeometryCollection().SetSRID(t.SRID())
		if err := ret.SetLayout(layout); err != nil {
			return nil, err
		}
		for i := 0; i < t.NumGeoms(); i++ {
			toPush, err := forceLayout(t.Geom(i), layout, defaultZ, defaultM)
			if err != nil {
				return nil, err
			}
			if err := ret.Push(toPush); err != nil {
				return nil, err
			}
		}
		return ret, nil
	case *geom.Point:
		return geom.NewPointFlat(
			layout, forceFlatCoordsLayout(t, layout, defaultZ, defaultM)).SetSRID(t.SRID()), nil
	case *geom.LineString:
		return geom.NewLineStringFlat(
			layout, forceFlatCoordsLayout(t, layout, defaultZ, defaultM)).SetSRID(t.SRID()), nil
	case *geom.Polygon:
		return geom.NewPolygonFlat(
			layout,
			forceFlatCoordsLayout(t, layout, defaultZ, defaultM),
			forceEnds(t.Ends(), t.Layout(), layout),
		).SetSRID(t.SRID()), nil
	case *geom.MultiPoint:
		return geom.NewMultiPointFlat(
			layout,
			forceFlatCoordsLayout(t, layout, defaultZ, defaultM),
			geom.NewMultiPointFlatOptionWithEnds(forceEnds(t.Ends(), t.Layout(), layout)),
		).SetSRID(t.SRID()), nil
	case *geom.MultiLineString:
		return geom.NewMultiLineStringFlat(
			layout,
			forceFlatCoordsLayout(t, layout, defaultZ, defaultM),
			forceEnds(t.Ends(), t.Layout(), layout),
		).SetSRID(t.SRID()), nil
	case *geom.MultiPolygon:
		endss := make([][]int, len(t.Endss()))
		for i := range t.Endss() {
			endss[i] = forceEnds(t.Endss()[i], t.Layout(), layout)
		}
		return geom.NewMultiPolygonFlat(
			layout,
			forceFlatCoordsLayout(t, layout, defaultZ, defaultM),
			endss,
		).SetSRID(t.SRID()), nil
	default:
		return nil, pgerror.Newf(pgcode.InvalidParameterValue, "unknown geom.T type: %T", t)
	}
}

// forceEnds forces the Endss layout of a geometry into the new layout.
func forceEnds(ends []int, oldLayout geom.Layout, newLayout geom.Layout) []int {
	if oldLayout.Stride() == newLayout.Stride() {
		return ends
	}
	newEnds := make([]int, len(ends))
	for i := range ends {
		newEnds[i] = (ends[i] / oldLayout.Stride()) * newLayout.Stride()
	}
	return newEnds
}

// forceFlatCoordsLayout forces the flatCoords layout of a geometry into the new layout
// and uses the specified default Z and M values as needed.
func forceFlatCoordsLayout(
	t geom.T, layout geom.Layout, defaultZ float64, defaultM float64,
) []float64 {
	oldFlatCoords := t.FlatCoords()
	newFlatCoords := make([]float64, (len(oldFlatCoords)/t.Stride())*layout.Stride())
	for coordIdx := 0; coordIdx < len(oldFlatCoords)/t.Stride(); coordIdx++ {
		newFlatCoords[coordIdx*layout.Stride()] = oldFlatCoords[coordIdx*t.Stride()]
		newFlatCoords[coordIdx*layout.Stride()+1] = oldFlatCoords[coordIdx*t.Stride()+1]
		if layout.ZIndex() != -1 {
			z := defaultZ
			if t.Layout().ZIndex() != -1 {
				z = oldFlatCoords[coordIdx*t.Stride()+t.Layout().ZIndex()]
			}
			newFlatCoords[coordIdx*layout.Stride()+layout.ZIndex()] = z
		}
		if layout.MIndex() != -1 {
			m := defaultM
			if t.Layout().MIndex() != -1 {
				m = oldFlatCoords[coordIdx*t.Stride()+t.Layout().MIndex()]
			}
			newFlatCoords[coordIdx*layout.Stride()+layout.MIndex()] = m
		}
	}
	return newFlatCoords
}
