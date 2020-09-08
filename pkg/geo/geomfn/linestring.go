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
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// LineStringFromMultiPoint generates a linestring from a multipoint.
func LineStringFromMultiPoint(g geo.Geometry) (geo.Geometry, error) {
	t, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}
	mp, ok := t.(*geom.MultiPoint)
	if !ok {
		return geo.Geometry{}, errors.Wrap(geom.ErrUnsupportedType{Value: t},
			"geometry must be a MultiPoint")
	}
	if mp.NumPoints() == 1 {
		return geo.Geometry{}, errors.Newf("a LineString must have at least 2 points")
	}
	flatCoords := make([]float64, 0, mp.NumCoords()*mp.Stride())
	var prevPoint *geom.Point
	for i := 0; i < mp.NumPoints(); i++ {
		p := mp.Point(i)
		// Empty points in multipoints double count the previous coordiante.
		// If i == 0, then add 0's.
		if p.Empty() {
			if prevPoint == nil {
				prevPoint = geom.NewPointFlat(mp.Layout(), make([]float64, mp.Stride()))
			}
			flatCoords = append(flatCoords, prevPoint.FlatCoords()...)
			continue
		}
		flatCoords = append(flatCoords, p.FlatCoords()...)
		prevPoint = p
	}
	lineString := geom.NewLineStringFlat(mp.Layout(), flatCoords).SetSRID(mp.SRID())
	return geo.MakeGeometryFromGeomT(lineString)
}

// LineMerge merges multilinestring constituents.
func LineMerge(g geo.Geometry) (geo.Geometry, error) {
	// Mirrors PostGIS behavior
	if g.Empty() {
		return g, nil
	}
	ret, err := geos.LineMerge(g.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(ret)
}

// AddPoint adds a point to a LineString at the given 0-based index. -1 appends.
func AddPoint(lineString geo.Geometry, index int, point geo.Geometry) (geo.Geometry, error) {
	g, err := lineString.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}
	lineStringG, ok := g.(*geom.LineString)
	if !ok {
		e := geom.ErrUnsupportedType{Value: g}
		return geo.Geometry{}, errors.Wrap(e, "geometry to be modified must be a LineString")
	}

	g, err = point.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}

	pointG, ok := g.(*geom.Point)
	if !ok {
		e := geom.ErrUnsupportedType{Value: g}
		return geo.Geometry{}, errors.Wrapf(e, "invalid geometry used to add a Point to a LineString")
	}

	g, err = addPoint(lineStringG, index, pointG)
	if err != nil {
		return geo.Geometry{}, err
	}

	return geo.MakeGeometryFromGeomT(g)
}

func addPoint(lineString *geom.LineString, index int, point *geom.Point) (*geom.LineString, error) {
	if lineString.Layout() != point.Layout() {
		return nil, geom.ErrLayoutMismatch{Got: point.Layout(), Want: lineString.Layout()}
	}
	if point.Empty() {
		point = geom.NewPointFlat(point.Layout(), make([]float64, point.Stride()))
	}

	coords := lineString.Coords()

	if index > len(coords) {
		return nil, errors.Newf("index %d out of range of LineString with %d coordinates",
			index, len(coords))
	} else if index == -1 {
		index = len(coords)
	} else if index < 0 {
		return nil, errors.Newf("invalid index %v", index)
	}

	// Shift the slice right by one element, then replace the element at the index, to avoid
	// allocating an additional slice.
	coords = append(coords, geom.Coord{})
	copy(coords[index+1:], coords[index:])
	coords[index] = point.Coords()

	return lineString.SetCoords(coords)
}

// SetPoint sets the point at the given index of lineString; index is 0-based.
func SetPoint(lineString geo.Geometry, index int, point geo.Geometry) (geo.Geometry, error) {
	g, err := lineString.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}

	lineStringG, ok := g.(*geom.LineString)
	if !ok {
		e := geom.ErrUnsupportedType{Value: g}
		return geo.Geometry{}, errors.Wrap(e, "geometry to be modified must be a LineString")
	}

	g, err = point.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}

	pointG, ok := g.(*geom.Point)
	if !ok {
		e := geom.ErrUnsupportedType{Value: g}
		return geo.Geometry{}, errors.Wrapf(e, "invalid geometry used to replace a Point on a LineString")
	}

	g, err = setPoint(lineStringG, index, pointG)
	if err != nil {
		return geo.Geometry{}, err
	}

	return geo.MakeGeometryFromGeomT(g)
}

func setPoint(lineString *geom.LineString, index int, point *geom.Point) (*geom.LineString, error) {
	if lineString.Layout() != point.Layout() {
		return nil, geom.ErrLayoutMismatch{Got: point.Layout(), Want: lineString.Layout()}
	}
	if point.Empty() {
		point = geom.NewPointFlat(point.Layout(), make([]float64, point.Stride()))
	}

	coords := lineString.Coords()
	hasNegIndex := index < 0

	if index >= len(coords) || (hasNegIndex && index*-1 > len(coords)) {
		return nil, errors.Newf("index %d out of range of LineString with %d coordinates", index, len(coords))
	}

	if hasNegIndex {
		index = len(coords) + index
	}

	coords[index].Set(point.Coords())

	return lineString.SetCoords(coords)
}

// RemovePoint removes the point at the given index of lineString; index is 0-based.
func RemovePoint(lineString geo.Geometry, index int) (geo.Geometry, error) {
	g, err := lineString.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}

	lineStringG, ok := g.(*geom.LineString)
	if !ok {
		e := geom.ErrUnsupportedType{Value: g}
		return geo.Geometry{}, errors.Wrap(e, "geometry to be modified must be a LineString")
	}

	if lineStringG.NumCoords() == 2 {
		return geo.Geometry{}, errors.Newf("cannot remove a point from a LineString with only two Points")
	}

	g, err = removePoint(lineStringG, index)
	if err != nil {
		return geo.Geometry{}, err
	}

	return geo.MakeGeometryFromGeomT(g)
}

func removePoint(lineString *geom.LineString, index int) (*geom.LineString, error) {
	coords := lineString.Coords()

	if index >= len(coords) || index < 0 {
		return nil, errors.Newf("index %d out of range of LineString with %d coordinates", index, len(coords))
	}

	coords = append(coords[:index], coords[index+1:]...)

	return lineString.SetCoords(coords)
}
